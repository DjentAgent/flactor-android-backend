import asyncio
import base64
import binascii
import hashlib
import html
import json
import logging
import os
import re
import socket
import tempfile
import time
import urllib.parse
import shutil
from typing import List, Optional, Tuple, Dict, Any, Set
from urllib.parse import urlparse, parse_qs
from contextlib import asynccontextmanager
from functools import lru_cache
from dataclasses import dataclass
from enum import Enum

import aiohttp
from aiohttp import ClientTimeout, ClientSession, TCPConnector
from aiohttp.client_exceptions import ContentTypeError, ClientError, ServerTimeoutError
from redis.asyncio import Redis, ConnectionPool
from fastapi import HTTPException
from rapidfuzz import fuzz

try:
    import bencodepy
except ImportError:
    try:
        import bencode as bencodepy
    except ImportError:
        try:
            import bencoder as bencodepy
        except ImportError:
            from . import bencode_fallback as bencodepy

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# РљРѕРЅСЃС‚Р°РЅС‚С‹ РґР»СЏ С„РёР»СЊС‚СЂР°С†РёРё
_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_BTIH_RE = re.compile(r"btih:([A-Za-z0-9]{32,40})", re.IGNORECASE)


@dataclass
class PerformanceMetrics:
    """РњРµС‚СЂРёРєРё РїСЂРѕРёР·РІРѕРґРёС‚РµР»СЊРЅРѕСЃС‚Рё."""
    total_requests: int = 0
    cache_hits: int = 0
    api_calls: int = 0
    avg_response_time: float = 0.0


def _human_size(nbytes: int) -> str:
    """РљРѕРЅРІРµСЂС‚РёСЂСѓРµС‚ Р±Р°Р№С‚С‹ РІ С‡РµР»РѕРІРµРєРѕС‡РёС‚Р°РµРјС‹Р№ С„РѕСЂРјР°С‚."""
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    v = float(nbytes)
    for suf in suffixes:
        if v < 1024.0:
            return f"{v:.2f} {suf}"
        v /= 1024.0
    return f"{v:.2f} PB"


def _hex_upper_from_btih(btih: str) -> Optional[str]:
    """РќРѕСЂРјР°Р»РёР·СѓРµС‚ BTIH РІ РІРµСЂС…РЅРёР№ СЂРµРіРёСЃС‚СЂ HEX."""
    v = (btih or "").strip()

    if v.lower().startswith("urn:btih:"):
        v = v.split(":", 2)[-1]
    if v.lower().startswith("urn:btmh:"):
        return None

    # HEX С„РѕСЂРјР°С‚ (40 СЃРёРјРІРѕР»РѕРІ)
    if len(v) == 40 and all(c in "0123456789abcdefABCDEF" for c in v):
        return v.upper()

    # Base32 С„РѕСЂРјР°С‚ (32 СЃРёРјРІРѕР»Р°)
    if len(v) == 32:
        try:
            raw = base64.b32decode(v.upper())
            return binascii.hexlify(raw).decode("ascii").upper()
        except Exception:
            return None

    return None


def _slug(s: str) -> str:
    """РЎРѕР·РґР°РµС‚ slug РёР· СЃС‚СЂРѕРєРё РґР»СЏ URL."""
    t = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-")
    return t or "torrent"


def _title_maybe_contains_track(title: str, track: str) -> bool:
    """Р‘С‹СЃС‚СЂР°СЏ РїСЂРѕРІРµСЂРєР°, РјРѕР¶РµС‚ Р»Рё Р·Р°РіРѕР»РѕРІРѕРє СЃРѕРґРµСЂР¶Р°С‚СЊ С‚СЂРµРє."""
    if not track:
        return True
    t = (title or "").lower()
    q = track.lower()
    if q in t:
        return True
    return fuzz.partial_ratio(q, t) >= 75


def _norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\W_]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _compact_text(s: str) -> str:
    return re.sub(r"\s+", "", _norm_text(s))


def _track_aliases(track: str) -> List[str]:
    tr = _norm_text(track)
    if not tr:
        return []
    aliases = {
        tr,
        re.sub(r"\bfeat\.?.*$", "", tr).strip(),
        re.sub(r"\s*\(.*?\)\s*$", "", tr).strip(),
        re.sub(r"\s*\[.*?\]\s*$", "", tr).strip(),
    }
    return sorted([a for a in aliases if a], key=len, reverse=True)


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for v in values:
        key = (v or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


class FastHTTPSession:
    """Р‘С‹СЃС‚СЂР°СЏ HTTP СЃРµСЃСЃРёСЏ СЃ РјРёРЅРёРјР°Р»СЊРЅС‹РјРё РЅР°СЃС‚СЂРѕР№РєР°РјРё."""

    def __init__(self):
        # РџСЂРѕСЃС‚РµР№С€Р°СЏ РєРѕРЅС„РёРіСѓСЂР°С†РёСЏ Р±РµР· РєРѕРЅС„Р»РёРєС‚РѕРІ
        self.session: Optional[ClientSession] = None

    async def _ensure_session(self) -> ClientSession:
        if not self.session or self.session.closed:
            connector = TCPConnector(limit=50, limit_per_host=15, family=socket.AF_INET)
            self.session = ClientSession(
                connector=connector,
                trust_env=True,
                timeout=ClientTimeout(total=3, connect=1, sock_read=2),
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip",
                }
            )
        return self.session

    async def get_fast(self, url: str, timeout: float = 2.0) -> Optional[bytes]:
        """Р‘С‹СЃС‚СЂС‹Р№ GET Р·Р°РїСЂРѕСЃ."""
        try:
            session = await self._ensure_session()
            custom_timeout = ClientTimeout(total=timeout)

            async with session.get(url, timeout=custom_timeout, allow_redirects=False) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if len(data) > 50 * 1024 * 1024:  # Р‘РѕР»СЊС€Рµ 50MB
                        return None
                    return data
                elif resp.status in (301, 302, 303, 307, 308):
                    location = resp.headers.get('Location')
                    if location and not location.startswith('magnet:'):
                        async with session.get(location, timeout=custom_timeout) as resp2:
                            if resp2.status == 200:
                                return await resp2.read()

        except (asyncio.TimeoutError, ClientError):
            pass
        except Exception as e:
            log.debug(f"Fast GET error for {url[:50]}: {e}")

        return None

    async def get_json_fast(
            self,
            url: str,
            params: dict = None,
            *,
            timeout_total: float = 9.0,
            timeout_connect: float = 3.0,
            timeout_read: float = 7.0,
    ) -> Optional[Any]:
        """Р‘С‹СЃС‚СЂС‹Р№ GET РґР»СЏ JSON API."""
        try:
            session = await self._ensure_session()
            timeout = ClientTimeout(total=timeout_total, connect=timeout_connect, sock_read=timeout_read)

            async with session.get(url, params=params, timeout=timeout) as resp:
                if resp.status == 200:
                    try:
                        return await resp.json(content_type=None)
                    except Exception:
                        text = await resp.text()
                        return json.loads(text) if text.strip() else {}

        except Exception as e:
            log.debug(f"Fast JSON error for {url}: {e}")

        return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


class SpeedyCache:
    """РЈР»СЊС‚СЂР°Р±С‹СЃС‚СЂС‹Р№ РєРµС€."""

    def __init__(self, redis_pool: ConnectionPool):
        self.redis = Redis(connection_pool=redis_pool)
        self.memory: Dict[str, Tuple[Any, float]] = {}
        self.memory_lock = asyncio.Lock()
        self.redis_write_block_until = 0.0
        self._readonly_last_log_at = 0.0
        self._readonly_backoff_sec = 300.0

    @staticmethod
    def _is_redis_readonly_error(exc: Exception) -> bool:
        msg = str(exc or "").lower()
        return ("read only replica" in msg) or ("readonly" in msg)

    async def get_fast(self, key: str, ttl: int = 300) -> Optional[Any]:
        """Р‘С‹СЃС‚СЂРѕРµ РїРѕР»СѓС‡РµРЅРёРµ."""
        # Memory cache
        now = time.time()
        if key in self.memory:
            value, timestamp = self.memory[key]
            if now - timestamp < ttl:
                return value
            else:
                asyncio.create_task(self._cleanup_memory_key(key))

        # Redis cache
        try:
            data = await self.redis.get(key)
            if data:
                if isinstance(data, bytes):
                    result = json.loads(data.decode())
                else:
                    result = json.loads(data)

                await self.set_memory(key, result)
                return result
        except Exception:
            pass

        return None

    async def set_fast(self, key: str, value: Any, ex: int):
        """Р‘С‹СЃС‚СЂРѕРµ СЃРѕС…СЂР°РЅРµРЅРёРµ."""
        await self.set_memory(key, value)
        asyncio.create_task(self._set_redis_async(key, value, ex))

    async def set_memory(self, key: str, value: Any):
        """РЈСЃС‚Р°РЅРѕРІРєР° РІ memory cache."""
        async with self.memory_lock:
            if len(self.memory) > 2000:
                cutoff = time.time() - 1800
                self.memory = {
                    k: (v, t) for k, (v, t) in self.memory.items()
                    if t > cutoff
                }

            self.memory[key] = (value, time.time())

    async def _cleanup_memory_key(self, key: str):
        """РђСЃРёРЅС…СЂРѕРЅРЅР°СЏ РѕС‡РёСЃС‚РєР° memory key."""
        async with self.memory_lock:
            self.memory.pop(key, None)

    async def _set_redis_async(self, key: str, value: Any, ex: int):
        now = time.time()
        if now < float(self.redis_write_block_until or 0.0):
            return
        try:
            await self.redis.set(key, json.dumps(value), ex=ex)
        except Exception as e:
            if self._is_redis_readonly_error(e):
                self.redis_write_block_until = now + self._readonly_backoff_sec
                if (now - float(self._readonly_last_log_at or 0.0)) >= 60.0:
                    self._readonly_last_log_at = now
                    log.warning(
                        "PB cache Redis is READONLY. Disabling writes for %.0fs: %s",
                        self._readonly_backoff_sec,
                        e,
                    )
                return
            log.debug("Redis set error for %s: %s", key, e)

class PirateBayService:
    """Р‘С‹СЃС‚СЂС‹Р№ СЃРµСЂРІРёСЃ РґР»СЏ СЂР°Р±РѕС‚С‹ СЃ PirateBay API."""

    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
            self,
            api_base_url: Optional[str] = None,
            redis_url: Optional[str] = None,
    ) -> None:
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        # Р‘Р°Р·РѕРІС‹Рµ РЅР°СЃС‚СЂРѕР№РєРё
        base = api_base_url or getattr(settings, "piratebay_api_base", "https://apibay.org")
        self.api_base = base.rstrip("/")

        # HTTP СЃРµСЃСЃРёСЏ
        self.http = FastHTTPSession()

        # Redis
        r_url = redis_url or getattr(settings, "redis_url", "redis://localhost:6379/0")
        self.redis_pool = ConnectionPool.from_url(
            r_url,
            max_connections=20,
            encoding="utf-8",
            decode_responses=True
        )

        # Р‘С‹СЃС‚СЂС‹Р№ РєРµС€
        self.cache = SpeedyCache(self.redis_pool)

        # РЎРѕРєСЂР°С‰РµРЅРЅС‹Рµ TTL
        self.search_ttl = 180  # 3 minutes
        self.filelist_ttl = 3600  # 1 hour
        self.torrent_ttl = 7200  # 2 hours
        self.id2hash_ttl = 1800  # 30 minutes
        self.track_filecheck_top_n = int(getattr(settings, "piratebay_track_filecheck_top_n", 12))
        self.track_verify_timeout_sec = float(getattr(settings, "piratebay_track_verify_timeout_sec", 3.0))
        self.min_seeders = int(getattr(settings, "piratebay_min_seeders", 1))
        self.magnet_fallback_timeout_sec = float(getattr(settings, "piratebay_magnet_fallback_timeout_sec", 45.0))
        self.magnet_tool_path = str(getattr(settings, "piratebay_magnet_tool_path", "aria2c"))

        self._fast_mirrors = [
            "https://itorrents.org/torrent/{HEX}.torrent",
            "https://itorrents.net/torrent/{HEX}.torrent",
            "https://itorrents.xyz/torrent/{HEX}.torrent",
            "https://itorrents.ch/torrent/{HEX}.torrent",
            "https://itorrents.se/torrent/{HEX}.torrent",
            "https://btcache.me/torrent/{HEX}.torrent",
            "https://torrage.info/torrent.php?h={HEX}",
            "https://torcache.net/torrent/{HEX}.torrent",
        ]
        extra_mirrors = getattr(settings, "piratebay_extra_torrent_mirrors", None)
        if extra_mirrors:
            for mirror in str(extra_mirrors).split(","):
                m = mirror.strip()
                if m:
                    self._fast_mirrors.append(m)

        self._api_mirrors = _dedupe_preserve_order(
            [
                self.api_base,
                "https://apibay.org",
            ]
        )
        extra_api_mirrors = getattr(settings, "piratebay_extra_api_mirrors", None)
        if extra_api_mirrors:
            for mirror in str(extra_api_mirrors).split(","):
                m = mirror.strip().rstrip("/")
                if m:
                    self._api_mirrors.append(m)
        self._api_mirrors = _dedupe_preserve_order(self._api_mirrors)

        self._page_mirrors = [
            "https://tpb.party/torrent/{ID}",
            "https://pirateproxy.live/torrent/{ID}",
            "https://thepiratebay.party/torrent/{ID}",
            "https://thepiratebay.org/?t={ID}",
            "https://thepiratebay.party/description.php?id={ID}",
            "https://tpb.party/description.php?id={ID}",
            "https://pirateproxy.live/description.php?id={ID}",
        ]
        extra_page_mirrors = getattr(settings, "piratebay_extra_page_mirrors", None)
        if extra_page_mirrors:
            for mirror in str(extra_page_mirrors).split(","):
                m = mirror.strip()
                if m:
                    self._page_mirrors.append(m)
        self._page_mirrors = _dedupe_preserve_order(self._page_mirrors)

        self._id_download_mirrors = [
            "https://thepiratebay.org/download/{ID}",
            "https://thepiratebay.party/download/{ID}",
            "https://tpb.party/download/{ID}",
            "https://pirateproxy.live/download/{ID}",
        ]
        # РўСЂРµРєРµСЂС‹
        self._trackers = [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://open.stealth.si:80/announce",
            "udp://tracker.torrent.eu.org:451/announce",
        ]

        # РњРµС‚СЂРёРєРё
        self.metrics = PerformanceMetrics()

        log.info(f"PirateBayService initialized: api={self.api_base}")

    async def _record_timing(self, operation: str, start_time: float):
        """Р—Р°РїРёСЃС‹РІР°РµС‚ РјРµС‚СЂРёРєРё."""
        duration = time.time() - start_time
        self.metrics.total_requests += 1

        alpha = 0.1
        self.metrics.avg_response_time = (
                alpha * duration + (1 - alpha) * self.metrics.avg_response_time
        )

    async def search(
            self,
            query: str,
            only_lossless: Optional[bool] = None,
            track: Optional[str] = None,
    ) -> List[TorrentInfo]:
        """Р‘С‹СЃС‚СЂС‹Р№ РїРѕРёСЃРє С‚РѕСЂСЂРµРЅС‚РѕРІ."""
        start_time = time.time()

        # РљРµС€
        cache_key = f"pb:search:{query}:{only_lossless}:{track}"
        if cached := await self.cache.get_fast(cache_key, ttl=self.search_ttl):
            self.metrics.cache_hits += 1
            await self._record_timing("search_cached", start_time)
            return [TorrentInfo(**d) for d in cached]

        # API Р·Р°РїСЂРѕСЃ
        items, api_reachable = await self._search_api_items(query)
        page_reachable = False
        if not items:
            items, page_reachable = await self._search_page_items(query)
        if not items:
            if not api_reachable and not page_reachable:
                raise HTTPException(status_code=502, detail="PirateBay API unavailable")
            await self._record_timing("search_empty", start_time)
            return []

        self.metrics.api_calls += 1

        # РћР±СЂР°Р±РѕС‚РєР° СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ
        results = await self._process_search_results(items, only_lossless, track)

        # РљРµС€
        cache_data = [r.__dict__ for r in results]
        await self.cache.set_fast(cache_key, cache_data, ex=self.search_ttl)

        await self._record_timing("search_api", start_time)
        return results

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return 0

    @staticmethod
    def _strip_html(value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value or "")
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _size_to_bytes(value: Any) -> int:
        if isinstance(value, (int, float)):
            return max(0, int(value))
        text = str(value or "").strip()
        if not text:
            return 0
        if text.isdigit():
            return max(0, int(text))
        m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*([KMGTPE]?i?B)", text, re.IGNORECASE)
        if not m:
            return 0
        num = float(m.group(1).replace(",", "."))
        unit = m.group(2).upper()
        unit_map = {
            "B": 1,
            "KB": 1000,
            "MB": 1000 ** 2,
            "GB": 1000 ** 3,
            "TB": 1000 ** 4,
            "PB": 1000 ** 5,
            "KIB": 1024,
            "MIB": 1024 ** 2,
            "GIB": 1024 ** 3,
            "TIB": 1024 ** 4,
            "PIB": 1024 ** 5,
        }
        mult = unit_map.get(unit, 1)
        return max(0, int(num * mult))

    def _normalize_search_items(self, payload: Any) -> List[dict]:
        if isinstance(payload, dict):
            for key in ("items", "results", "data", "torrents"):
                nested = payload.get(key)
                if isinstance(nested, list):
                    payload = nested
                    break
            else:
                payload = [payload]
        if not isinstance(payload, list):
            return []

        out: List[dict] = []
        for raw in payload:
            if not isinstance(raw, dict):
                continue
            tid = str(
                raw.get("id")
                or raw.get("torrentid")
                or raw.get("torrent_id")
                or ""
            ).strip()
            if not tid.isdigit() or tid == "0":
                continue

            name = str(raw.get("name") or raw.get("title") or "").strip()
            if not name or name.lower().startswith("no results"):
                continue

            item: Dict[str, Any] = {
                "id": tid,
                "name": name,
                "seeders": max(
                    0,
                    self._safe_int(raw.get("seeders") or raw.get("seeds") or raw.get("seed")),
                ),
                "leechers": max(
                    0,
                    self._safe_int(raw.get("leechers") or raw.get("peers")),
                ),
                "size": max(0, self._size_to_bytes(raw.get("size") or raw.get("sz") or 0)),
            }
            ih_raw = raw.get("info_hash") or raw.get("infohash") or raw.get("hash") or ""
            ih_norm = _hex_upper_from_btih(str(ih_raw))
            if ih_norm:
                item["info_hash"] = ih_norm
            out.append(item)
        return out

    def _parse_search_rows_from_html(self, html_text: str) -> List[dict]:
        out_by_id: Dict[str, dict] = {}
        for m_row in re.finditer(r"<tr[^>]*>(.*?)</tr>", html_text or "", re.IGNORECASE | re.DOTALL):
            row = m_row.group(1)
            m_tid = re.search(r"/torrent/(\d+)", row, re.IGNORECASE)
            if not m_tid:
                m_tid = re.search(r"(?:description\.php\?id=|[?&]id=)(\d+)", row, re.IGNORECASE)
            if not m_tid:
                continue
            tid = m_tid.group(1)

            m_title = re.search(
                r'class=["\']detName["\'][^>]*>.*?<a[^>]*>(.*?)</a>',
                row,
                re.IGNORECASE | re.DOTALL,
            )
            if not m_title:
                m_title = re.search(r'class=["\']detLink["\'][^>]*>(.*?)</a>', row, re.IGNORECASE | re.DOTALL)
            title = self._strip_html(m_title.group(1)) if m_title else ""
            if not title:
                m_slug = re.search(r"/torrent/\d+/([^\"'<>/]+)", row, re.IGNORECASE)
                if m_slug:
                    title = urllib.parse.unquote(m_slug.group(1)).replace("_", " ").strip()
            if not title:
                continue

            right_nums = [
                self._safe_int(x)
                for x in re.findall(r'<td[^>]*align=["\']right["\'][^>]*>\s*(\d+)\s*</td>', row, re.IGNORECASE)
            ]
            seeders = right_nums[0] if len(right_nums) > 0 else 0
            leechers = right_nums[1] if len(right_nums) > 1 else 0

            size_bytes = 0
            m_size = re.search(r"Size\s*([0-9]+(?:[.,][0-9]+)?)\s*([KMGTPE]?i?B)", row, re.IGNORECASE)
            if m_size:
                size_bytes = self._size_to_bytes(f"{m_size.group(1)} {m_size.group(2)}")
            else:
                row_text = self._strip_html(row)
                m_size2 = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*([KMGTPE]?i?B)", row_text, re.IGNORECASE)
                if m_size2:
                    size_bytes = self._size_to_bytes(f"{m_size2.group(1)} {m_size2.group(2)}")

            current = {
                "id": tid,
                "name": title,
                "seeders": max(0, seeders),
                "leechers": max(0, leechers),
                "size": max(0, size_bytes),
            }
            prev = out_by_id.get(tid)
            if not prev or current["seeders"] > int(prev.get("seeders", 0)):
                out_by_id[tid] = current

        out = list(out_by_id.values())
        out.sort(key=lambda x: (int(x.get("seeders", 0)), int(x.get("size", 0))), reverse=True)
        return out

    async def _search_api_items(self, query: str) -> Tuple[List[dict], bool]:
        params = {"q": query, "cat": 100}
        reachable = False
        for api_base in self._api_mirrors:
            for ep in ("q.php",):
                url = f"{api_base.rstrip('/')}/{ep}"
                payload = await self.http.get_json_fast(
                        url,
                        params=params,
                        timeout_total=6.0,
                        timeout_connect=2.0,
                        timeout_read=4.0,
                    )
                if payload is not None:
                    reachable = True
                items = self._normalize_search_items(payload)
                if items:
                    return items, True
        return [], reachable

    async def _search_page_items(self, query: str) -> Tuple[List[dict], bool]:
        domains: List[str] = []
        for tmpl in self._page_mirrors:
            probe = tmpl.replace("{ID}", "1")
            parsed = urlparse(probe)
            if not parsed.scheme or not parsed.netloc:
                continue
            domains.append(f"{parsed.scheme}://{parsed.netloc}")
        domains = _dedupe_preserve_order(domains)
        if domains:
            def _prio(base: str) -> int:
                host = (urlparse(base).netloc or "").lower()
                if host == "tpb.party":
                    return 0
                if host == "thepiratebay.party":
                    return 1
                if host == "pirateproxy.live":
                    return 2
                if host == "thepiratebay.org":
                    return 9
                return 5
            domains = sorted(domains, key=_prio)

        q_enc = urllib.parse.quote(query, safe="")
        urls: List[str] = []
        for base in domains:
            urls.append(f"{base}/search/{q_enc}/1/99/100")

        reachable = False
        for url in urls:
            html_data = await self.http.get_fast(url, timeout=4.0)
            if not html_data:
                continue
            reachable = True
            items = self._parse_search_rows_from_html(html_data.decode("utf-8", "ignore"))
            if items:
                return items, True
        return [], reachable

    async def _process_search_results(
            self,
            items: List[dict],
            only_lossless: Optional[bool],
            track: Optional[str]
    ) -> List[TorrentInfo]:
        """РћР±СЂР°Р±РѕС‚РєР° СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ РїРѕРёСЃРєР°."""

        # РћС‡РёСЃС‚РєР°
        clean_items = []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue

            tid = str(it.get("id", "")).strip()
            name = (it.get("name") or "").strip()

            if not tid.isdigit() or tid == "0" or not name or name.lower().startswith("no results"):
                continue

            it["name"] = name
            it["seeders"] = max(0, int(it.get("seeders", 0) or 0))
            it["leechers"] = max(0, int(it.get("leechers", 0) or 0))
            it["size"] = max(0, int(it.get("size", 0) or 0))
            if it["seeders"] < self.min_seeders:
                continue

            # Save id->hash mapping early when API returns it, so /download/{id} works reliably.
            ih_raw = it.get("info_hash") or it.get("infohash") or ""
            ih_norm = _hex_upper_from_btih(str(ih_raw))
            if ih_norm:
                await self.cache.set_fast(f"pb:id2hash:{tid}", ih_norm, ex=self.id2hash_ttl)
            clean_items.append(it)

        # Lossless С„РёР»СЊС‚СЂР°С†РёСЏ
        if only_lossless is not None:
            filtered = []
            for it in clean_items:
                name = it.get("name", "")
                is_lossless = bool(_LOSSLESS_RE.search(name))
                is_lossy = bool(_LOSSY_RE.search(name))

                if only_lossless is True and (not is_lossless or is_lossy):
                    continue
                if only_lossless is False and is_lossless:
                    continue

                filtered.append(it)
        else:
            filtered = clean_items

        # Track С„РёР»СЊС‚СЂР°С†РёСЏ
        if track:
            def candidate_score(it: dict) -> tuple[int, int, int]:
                name = it.get("name", "")
                title_score = 1 if _title_maybe_contains_track(name, track) else 0
                return (title_score, int(it.get("seeders", 0)), int(it.get("size", 0)))

            candidates = sorted(filtered, key=candidate_score, reverse=True)[: self.track_filecheck_top_n]
            verified: List[dict] = []
            target_verified = min(8, self.track_filecheck_top_n)

            async def verify_candidate(it: dict):
                tid = str(it.get("id"))
                ok = await self._verify_track_presence(tid, track)
                return it, ok

            tasks = [asyncio.create_task(verify_candidate(it)) for it in candidates]
            try:
                for fut in asyncio.as_completed(tasks, timeout=self.track_verify_timeout_sec):
                    try:
                        it, ok = await fut
                    except Exception:
                        continue
                    if ok:
                        verified.append(it)
                        if len(verified) >= target_verified:
                            break
            except asyncio.TimeoutError:
                log.debug("Track verification timed out")
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()

            if verified:
                filtered = verified
            else:
                # Fallback: keep title-matching candidates when filelist verification
                # could not prove presence in time.
                title_candidates = [it for it in candidates if _title_maybe_contains_track(it.get("name", ""), track)]
                if title_candidates:
                    filtered = title_candidates[:target_verified]
                else:
                    filtered = []

        # Р РµР·СѓР»СЊС‚Р°С‚С‹
        results: List[TorrentInfo] = []
        for it in filtered:
            size_bytes = it.get("size", 0)
            page_url = f"https://thepiratebay.org/?t={it['id']}"

            info = TorrentInfo(
                title=it.get("name", ""),
                url=page_url,
                size=_human_size(size_bytes),
                seeders=it.get("seeders", 0),
                leechers=it.get("leechers", 0),
            )
            results.append(info)

        return results

    def _track_presence_cache_key(self, torrent_id: str, track: str) -> str:
        return f"pb:v2:hastrack:{torrent_id}:{_norm_text(track)}"

    async def _verify_track_presence(self, torrent_id: str, track: str) -> bool:
        key = self._track_presence_cache_key(torrent_id, track)
        cached = await self.cache.get_fast(key, ttl=7 * 24 * 3600)
        if cached is not None:
            if isinstance(cached, bool):
                return cached
            return str(cached).lower() in {"1", "true", "yes"}

        ok, checked = await self._quick_track_check(torrent_id, track)
        if checked:
            await self.cache.set_fast(key, ok, ex=7 * 24 * 3600)
        return ok

    async def _quick_track_check(self, torrent_id: str, track: str) -> Tuple[bool, bool]:
        """Quick check by title/filelist using cached or fetched torrent metadata."""
        try:
            cache_key = f"pb:fl:{torrent_id}"
            filelist = await self.cache.get_fast(cache_key, ttl=self.filelist_ttl)
            if not filelist:
                filelist = await self._fetch_and_cache_filelist(torrent_id)
            if not filelist:
                return False, False

            aliases = _track_aliases(track)
            for fname in filelist:
                fn = _norm_text(str(fname))
                fn_compact = _compact_text(fn)
                for alias in aliases:
                    if alias in fn:
                        return True, True
                    alias_compact = _compact_text(alias)
                    if alias_compact and alias_compact in fn_compact:
                        return True, True
            return False, True
        except Exception as e:
            log.debug("Quick track check failed for %s: %s", torrent_id, e)
        return False, False

    async def _extract_filelist_from_torrent_bytes(self, data: bytes) -> List[str]:
        try:
            meta = bencodepy.decode(data)
            info = meta.get(b"info") if isinstance(meta, dict) else None
            if not isinstance(info, dict):
                return []

            file_list: List[str] = []
            files = info.get(b"files")
            if isinstance(files, list):
                for f in files:
                    if not isinstance(f, dict):
                        continue
                    path = f.get(b"path")
                    if isinstance(path, list):
                        file_list.append("/".join(p.decode("utf-8", "ignore") for p in path if isinstance(p, (bytes, bytearray))))
            else:
                name = info.get(b"name")
                if isinstance(name, (bytes, bytearray)):
                    file_list.append(name.decode("utf-8", "ignore"))
            return [x for x in file_list if x]
        except Exception:
            return []

    async def _fetch_and_cache_filelist(self, torrent_id: str) -> List[str]:
        info_hash = await self._extract_hash_fast(f"https://thepiratebay.org/?t={torrent_id}")
        if not info_hash:
            return []
        data = await self._fetch_from_fast_mirrors(info_hash)
        if not data:
            return []
        files = await self._extract_filelist_from_torrent_bytes(data)
        if files:
            await self.cache.set_fast(f"pb:fl:{torrent_id}", files, ex=self.filelist_ttl)
        return files
    async def download(self, info: TorrentInfo) -> bytes:
        """Download real .torrent payload from mirror network."""
        start_time = time.time()

        info_hash_hex = await self._extract_hash_fast(info.url)
        if not info_hash_hex:
            raise HTTPException(status_code=422, detail="Cannot resolve info hash")

        cache_key = f"pb:blob:{info_hash_hex}"
        if cached := await self.cache.get_fast(cache_key, ttl=self.torrent_ttl):
            data = base64.b64decode(cached) if isinstance(cached, str) else cached
            await self._record_timing("download_cached", start_time)
            return data

        data = await self._fetch_from_fast_mirrors(info_hash_hex)
        if not data:
            data = await self._fetch_from_magnet_metadata(info_hash_hex)
        if not data:
            raise HTTPException(status_code=404, detail="Torrent payload is unavailable")

        b64_data = base64.b64encode(data).decode("ascii")
        await self.cache.set_fast(cache_key, b64_data, ex=self.torrent_ttl)
        await self._record_timing("download_resolved", start_time)
        return data

    def _torrent_info_hash(self, data: bytes) -> Optional[str]:
        def parse_next(buf: bytes, i: int) -> int:
            if i >= len(buf):
                raise ValueError("Unexpected end")
            c = buf[i:i + 1]
            if c == b"i":
                j = buf.find(b"e", i + 1)
                if j == -1:
                    raise ValueError("Invalid integer token")
                return j + 1
            if c == b"l":
                i += 1
                while i < len(buf) and buf[i:i + 1] != b"e":
                    i = parse_next(buf, i)
                if i >= len(buf):
                    raise ValueError("Unterminated list")
                return i + 1
            if c == b"d":
                i += 1
                while i < len(buf) and buf[i:i + 1] != b"e":
                    i = parse_next(buf, i)  # key
                    i = parse_next(buf, i)  # value
                if i >= len(buf):
                    raise ValueError("Unterminated dict")
                return i + 1
            if 48 <= c[0] <= 57:
                j = i
                while j < len(buf) and 48 <= buf[j] <= 57:
                    j += 1
                if j >= len(buf) or buf[j:j + 1] != b":":
                    raise ValueError("Invalid bytes length token")
                n = int(buf[i:j].decode("ascii"))
                start = j + 1
                end = start + n
                if end > len(buf):
                    raise ValueError("Bytes overflow")
                return end
            raise ValueError("Unknown token")

        def read_bstr(buf: bytes, i: int) -> Tuple[bytes, int]:
            j = i
            while j < len(buf) and 48 <= buf[j] <= 57:
                j += 1
            if j >= len(buf) or buf[j:j + 1] != b":":
                raise ValueError("Invalid string token")
            n = int(buf[i:j].decode("ascii"))
            start = j + 1
            end = start + n
            if end > len(buf):
                raise ValueError("String overflow")
            return buf[start:end], end

        def extract_info_slice(buf: bytes) -> Optional[bytes]:
            if not buf or buf[:1] != b"d":
                return None
            i = 1
            while i < len(buf) and buf[i:i + 1] != b"e":
                key, i = read_bstr(buf, i)
                value_start = i
                value_end = parse_next(buf, value_start)
                if key == b"info":
                    return buf[value_start:value_end]
                i = value_end
            return None

        try:
            info_raw = extract_info_slice(data)
            if info_raw:
                return hashlib.sha1(info_raw).hexdigest().upper()
        except Exception:
            pass

        try:
            meta = bencodepy.decode(data)
            info = meta.get(b"info") if isinstance(meta, dict) else None
            if isinstance(info, dict):
                return hashlib.sha1(bencodepy.encode(info)).hexdigest().upper()
        except Exception:
            pass
        return None

    def _is_valid_torrent_payload(self, data: bytes, expected_hash: str) -> bool:
        if not data or not data.startswith(b"d"):
            return False
        got = self._torrent_info_hash(data)
        return bool(got and got == expected_hash.upper())
    async def _extract_hash_fast(self, url: str) -> Optional[str]:
        """Р‘С‹СЃС‚СЂРѕРµ РёР·РІР»РµС‡РµРЅРёРµ hash."""
        if url.startswith("magnet:"):
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)

            for xt in qs.get("xt", []):
                if xt.lower().startswith("urn:btih:"):
                    return _hex_upper_from_btih(xt)
        else:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)

            tid = None
            for key in ("t", "id"):
                if key in qs and qs[key]:
                    tid = qs[key][0]
                    break

            if tid:
                cache_key = f"pb:id2hash:{tid}"
                cached_hash = await self.cache.get_fast(cache_key, ttl=self.id2hash_ttl)
                if cached_hash:
                    return cached_hash

                for api_base in self._api_mirrors:
                    api_url = f"{api_base}/t.php"
                    meta = await self.http.get_json_fast(
                        api_url,
                        {"id": tid},
                        timeout_total=12.0,
                        timeout_connect=4.0,
                        timeout_read=9.0,
                    )
                    if not meta:
                        meta = await self.http.get_json_fast(
                            f"{api_base}/t.php?id={tid}",
                            timeout_total=12.0,
                            timeout_connect=4.0,
                            timeout_read=9.0,
                        )
                    if not meta:
                        continue

                    ih_raw = meta.get("info_hash") or meta.get("infohash") or ""
                    ih_norm = _hex_upper_from_btih(str(ih_raw))
                    if ih_norm:
                        await self.cache.set_fast(cache_key, ih_norm, ex=self.id2hash_ttl)
                        return ih_norm

                # Fallback: parse page mirrors and extract btih from magnet link.
                for tmpl in self._page_mirrors:
                    page_url = tmpl.replace("{ID}", tid)
                    html = await self.http.get_fast(page_url, timeout=3.0)
                    if not html:
                        continue
                    text = html.decode("utf-8", "ignore")
                    m = _BTIH_RE.search(text)
                    if not m:
                        continue
                    ih_norm = _hex_upper_from_btih(m.group(1))
                    if ih_norm:
                        await self.cache.set_fast(cache_key, ih_norm, ex=self.id2hash_ttl)
                        return ih_norm

                # Last fallback: try search APIs with numeric query and match exact id.
                for api_base in self._api_mirrors:
                    for ep in ("q.php", "search.php"):
                        items = await self.http.get_json_fast(
                            f"{api_base}/{ep}",
                            {"q": tid, "cat": 100},
                            timeout_total=12.0,
                            timeout_connect=4.0,
                            timeout_read=9.0,
                        )
                        if not items or not isinstance(items, list):
                            continue
                        for it in items:
                            if not isinstance(it, dict):
                                continue
                            if str(it.get("id", "")).strip() != tid:
                                continue
                            ih_raw = it.get("info_hash") or it.get("infohash") or ""
                            ih_norm = _hex_upper_from_btih(str(ih_raw))
                            if ih_norm:
                                await self.cache.set_fast(cache_key, ih_norm, ex=self.id2hash_ttl)
                                return ih_norm

        return None

    async def _fetch_from_fast_mirrors(self, info_hash_hex: str) -> Optional[bytes]:
        """Fetch torrent payload from mirrors and verify expected info-hash."""

        async def fetch_one(mirror_url: str) -> Optional[bytes]:
            url = mirror_url.replace("{HEX}", info_hash_hex)
            data = await self.http.get_fast(url, timeout=3.0)
            if data and self._is_valid_torrent_payload(data, info_hash_hex):
                return data
            return None

        tasks = [asyncio.create_task(fetch_one(m)) for m in self._fast_mirrors]
        try:
            done, pending = await asyncio.wait(tasks, timeout=6.0, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    result = t.result()
                    if result:
                        for p in pending:
                            p.cancel()
                        return result
                except Exception:
                    continue

            if pending:
                done2, pending2 = await asyncio.wait(pending, timeout=4.0)
                for t in done2:
                    try:
                        result = t.result()
                        if result:
                            for p in pending2:
                                p.cancel()
                            return result
                    except Exception:
                        continue
                for p in pending2:
                    p.cancel()
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

        return None

    async def _fetch_from_magnet_metadata(
        self,
        info_hash_hex: str,
        timeout_sec: Optional[float] = None,
    ) -> Optional[bytes]:
        timeout = float(timeout_sec if timeout_sec is not None else self.magnet_fallback_timeout_sec)
        magnet = f"magnet:?xt=urn:btih:{info_hash_hex}"
        for tr in self._trackers:
            magnet += "&tr=" + urllib.parse.quote(tr, safe="")

        tmpdir = tempfile.mkdtemp(prefix="pbmeta_")
        cmd = [
            self.magnet_tool_path,
            "--bt-metadata-only=true",
            "--bt-save-metadata=true",
            "--follow-torrent=mem",
            "--seed-time=0",
            "--enable-dht=true",
            "--enable-peer-exchange=true",
            "--summary-interval=0",
            "--console-log-level=warn",
            "--dir",
            tmpdir,
            magnet,
        ]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                await asyncio.wait_for(proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                return None

            for root, _, files in os.walk(tmpdir):
                for name in files:
                    if not name.lower().endswith(".torrent"):
                        continue
                    fpath = os.path.join(root, name)
                    try:
                        with open(fpath, "rb") as fh:
                            data = fh.read()
                    except Exception:
                        continue
                    if self._is_valid_torrent_payload(data, info_hash_hex):
                        return data
            return None
        except FileNotFoundError:
            log.warning("Magnet fallback tool not found: %s", self.magnet_tool_path)
            return None
        except Exception as e:
            log.debug("Magnet metadata fallback failed for %s: %s", info_hash_hex, e)
            return None
        finally:
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass

    async def _fetch_from_id_download_mirrors(
        self,
        torrent_id: str,
        expected_hash: Optional[str] = None,
    ) -> Optional[bytes]:
        for tmpl in self._id_download_mirrors:
            url = tmpl.replace("{ID}", str(torrent_id))
            data = await self.http.get_fast(url, timeout=4.0)
            if not data:
                continue
            if expected_hash and not self._is_valid_torrent_payload(data, expected_hash):
                continue
            if not expected_hash and not data.startswith(b"d"):
                continue
            return data
        return None

    async def _fetch_from_page_torrent_links(
        self,
        torrent_id: str,
        expected_hash: Optional[str] = None,
    ) -> Optional[bytes]:
        href_re = re.compile(r'href=[\"\']([^\"\']+\\.torrent[^\"\']*|/download/\\d+|download\\.php\\?id=\\d+)[\"\']', re.IGNORECASE)
        for page_tmpl in self._page_mirrors:
            page_url = page_tmpl.replace("{ID}", str(torrent_id))
            html = await self.http.get_fast(page_url, timeout=4.0)
            if not html:
                continue
            text = html.decode("utf-8", "ignore")
            links = [m.group(1) for m in href_re.finditer(text)]
            if not links:
                continue
            for href in links[:8]:
                link = href
                if link.startswith("/"):
                    parsed = urlparse(page_url)
                    link = f"{parsed.scheme}://{parsed.netloc}{link}"
                elif not link.startswith("http://") and not link.startswith("https://"):
                    continue
                data = await self.http.get_fast(link, timeout=4.0)
                if not data:
                    continue
                if expected_hash and not self._is_valid_torrent_payload(data, expected_hash):
                    continue
                if not expected_hash and not data.startswith(b"d"):
                    continue
                return data
        return None
    async def download_by_id(self, torrent_id: str) -> bytes:
        """РЎРєР°С‡РёРІР°РЅРёРµ РїРѕ ID."""
        info_hash = await self._extract_hash_fast(f"https://thepiratebay.org/?t={torrent_id}")
        if not info_hash:
            raise HTTPException(status_code=404, detail="Torrent not found")

        # Prefer direct by-id torrent endpoints first; often available even when hash mirrors are flaky.
        by_id_data = await self._fetch_from_id_download_mirrors(torrent_id, expected_hash=info_hash)
        if by_id_data:
            cache_key = f"pb:blob:{info_hash}"
            await self.cache.set_fast(cache_key, base64.b64encode(by_id_data).decode("ascii"), ex=self.torrent_ttl)
            return by_id_data

        # Then try torrent links scraped from page mirrors.
        page_data = await self._fetch_from_page_torrent_links(torrent_id, expected_hash=info_hash)
        if page_data:
            cache_key = f"pb:blob:{info_hash}"
            await self.cache.set_fast(cache_key, base64.b64encode(page_data).decode("ascii"), ex=self.torrent_ttl)
            return page_data

        fake_info = TorrentInfo(
            title="",
            url=f"https://thepiratebay.org/?t={torrent_id}",
            size="0 B",
            seeders=0,
            leechers=0
        )
        return await self.download(fake_info)

    async def download_by_hash(self, info_hash: str) -> bytes:
        """РЎРєР°С‡РёРІР°РЅРёРµ РїРѕ hash."""
        ih = _hex_upper_from_btih(info_hash)
        if not ih:
            raise HTTPException(status_code=422, detail="Invalid hash format")

        fake_info = TorrentInfo(
            title="",
            url=f"magnet:?xt=urn:btih:{ih}",
            size="0 B",
            seeders=0,
            leechers=0
        )
        return await self.download(fake_info)

    async def diagnose_download_by_id(self, torrent_id: str) -> Dict[str, Any]:
        """Detailed diagnostics for PB download pipeline."""
        report: Dict[str, Any] = {
            "torrent_id": str(torrent_id),
            "cached_info_hash": None,
            "resolved_info_hash": None,
            "tphp_checks": [],
            "page_btih_checks": [],
            "id_download_checks": [],
            "hash_mirror_checks": [],
            "page_torrent_link_checks": [],
            "magnet_fallback": {},
        }

        tid = str(torrent_id).strip()
        cache_key = f"pb:id2hash:{tid}"
        cached_hash = await self.cache.get_fast(cache_key, ttl=self.id2hash_ttl)
        if cached_hash:
            report["cached_info_hash"] = str(cached_hash)

        info_hash = await self._extract_hash_fast(f"https://thepiratebay.org/?t={tid}")
        report["resolved_info_hash"] = info_hash
        if not info_hash:
            report["result"] = "failed: cannot resolve info hash"
            return report

        session = await self.http._ensure_session()

        async def probe(url: str, timeout: float = 4.0) -> Tuple[Dict[str, Any], Optional[bytes]]:
            item: Dict[str, Any] = {"url": url}
            try:
                async with session.get(url, timeout=ClientTimeout(total=timeout), allow_redirects=True) as resp:
                    data = await resp.read()
                    item["status"] = resp.status
                    item["bytes"] = len(data)
                    item["content_type"] = resp.headers.get("Content-Type", "")
                    return item, data
            except Exception as e:
                item["error"] = str(e)
                return item, None

        for api_base in self._api_mirrors:
            url = f"{api_base}/t.php?id={tid}"
            item, data = await probe(url, timeout=5.0)
            if data:
                try:
                    payload = json.loads(data.decode("utf-8", "ignore"))
                    ih_raw = payload.get("info_hash") or payload.get("infohash") or ""
                    item["info_hash"] = _hex_upper_from_btih(str(ih_raw))
                except Exception as e:
                    item["parse_error"] = str(e)
            report["tphp_checks"].append(item)

        for tmpl in self._page_mirrors:
            url = tmpl.replace("{ID}", tid)
            item, data = await probe(url, timeout=5.0)
            if data:
                text = data.decode("utf-8", "ignore")
                m = _BTIH_RE.search(text)
                item["btih_in_html"] = bool(m)
                if m:
                    item["btih_value"] = _hex_upper_from_btih(m.group(1))
            report["page_btih_checks"].append(item)

        for tmpl in self._id_download_mirrors:
            url = tmpl.replace("{ID}", tid)
            item, data = await probe(url, timeout=5.0)
            if data:
                item["is_bencode"] = data.startswith(b"d")
                item["payload_info_hash"] = self._torrent_info_hash(data)
                item["valid_info_hash"] = bool(self._is_valid_torrent_payload(data, info_hash))
            report["id_download_checks"].append(item)

        for tmpl in self._fast_mirrors:
            url = tmpl.replace("{HEX}", info_hash)
            item, data = await probe(url, timeout=5.0)
            if data:
                item["is_bencode"] = data.startswith(b"d")
                item["payload_info_hash"] = self._torrent_info_hash(data)
                item["valid_info_hash"] = bool(self._is_valid_torrent_payload(data, info_hash))
            report["hash_mirror_checks"].append(item)

        md = {"tool": self.magnet_tool_path, "timeout_sec": 25.0}
        data = await self._fetch_from_magnet_metadata(info_hash, timeout_sec=25.0)
        md["found"] = bool(data)
        if data:
            md["bytes"] = len(data)
            md["payload_info_hash"] = self._torrent_info_hash(data)
            md["valid_info_hash"] = bool(self._is_valid_torrent_payload(data, info_hash))
        report["magnet_fallback"] = md

        href_re = re.compile(
            r'href=[\"\']([^\"\']+\\.torrent[^\"\']*|/download/\\d+|download\\.php\\?id=\\d+)[\"\']',
            re.IGNORECASE,
        )
        for tmpl in self._page_mirrors:
            page_url = tmpl.replace("{ID}", tid)
            item: Dict[str, Any] = {"page_url": page_url, "torrent_links": []}
            p_item, p_data = await probe(page_url, timeout=5.0)
            item["page_status"] = p_item.get("status")
            item["page_error"] = p_item.get("error")
            if p_data:
                text = p_data.decode("utf-8", "ignore")
                links = [m.group(1) for m in href_re.finditer(text)][:8]
                for href in links:
                    link = href
                    if link.startswith("/"):
                        parsed = urlparse(page_url)
                        link = f"{parsed.scheme}://{parsed.netloc}{link}"
                    elif not link.startswith("http://") and not link.startswith("https://"):
                        continue
                    l_item, l_data = await probe(link, timeout=5.0)
                    l_item["resolved_url"] = link
                    if l_data:
                        l_item["is_bencode"] = l_data.startswith(b"d")
                        l_item["payload_info_hash"] = self._torrent_info_hash(l_data)
                        l_item["valid_info_hash"] = bool(self._is_valid_torrent_payload(l_data, info_hash))
                    item["torrent_links"].append(l_item)
            report["page_torrent_link_checks"].append(item)

        report["result"] = "ok"
        return report

    def get_performance_stats(self) -> Dict[str, Any]:
        """РЎС‚Р°С‚РёСЃС‚РёРєР° РїСЂРѕРёР·РІРѕРґРёС‚РµР»СЊРЅРѕСЃС‚Рё."""
        total = self.metrics.total_requests
        cache_rate = (self.metrics.cache_hits / total * 100) if total > 0 else 0

        return {
            "total_requests": total,
            "cache_hit_rate": f"{cache_rate:.1f}%",
            "avg_response_time": f"{self.metrics.avg_response_time:.3f}s",
            "api_calls": self.metrics.api_calls,
        }

    async def clear_cache(self, pattern: str = "pb:*"):
        """РћС‡РёСЃС‚РєР° РєРµС€Р°."""
        try:
            redis = Redis(connection_pool=self.redis_pool)
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
            log.info(f"Cleared {len(keys)} cache keys")
        except Exception as e:
            log.error(f"Cache clear error: {e}")

    async def close(self) -> None:
        """Р—Р°РєСЂС‹С‚РёРµ СЃРµСЂРІРёСЃР°."""
        await self.http.close()
        log.info("PirateBayService closed")


@lru_cache()
def get_piratebay_service() -> PirateBayService:
    """Р’РѕР·РІСЂР°С‰Р°РµС‚ singleton СЌРєР·РµРјРїР»СЏСЂ СЃРµСЂРІРёСЃР°."""
    return PirateBayService()








