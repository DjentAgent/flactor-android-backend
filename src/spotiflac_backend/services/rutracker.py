# -*- coding: utf-8 -*-
import json
import logging
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache, wraps
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import cloudscraper
import lxml.html
import redis
import requests
from fastapi import HTTPException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo
from spotiflac_backend.services.metadata_resolver import resolve_track

log = logging.getLogger(__name__)

# ---------------- tuning ----------------
_LOSSLESS_RE = re.compile(
    r"(?:\b(flac|alac|ape|wv|wavpack|aiff|pcm|tta|lossless|dsd|dsf|dff)\b|"
    r"\btr24\b|"
    r"\b24/\d{2,3}\b|"
    r"\b24bit\b|"
    r"hi[-\s]?res)",
    re.I,
)
_LOSSY_RE = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.I)
_FORM_TOKEN_RE = re.compile(r"form_token\s*:\s*'([0-9a-f]+)'", re.I)
_TOTAL_RE = re.compile(r"(?:Результатов\s+поиска|Результат(?:ов)?\s+поиска)\s*:\s*(\d+)", re.I)

_AUDIO_EXTS = {
    ".flac", ".ape", ".wav", ".wv", ".alac", ".aiff", ".aif", ".tta", ".m4a",
    ".mp3", ".aac", ".ogg", ".opus", ".mka", ".dsf", ".dff"
}
_IGNORE_FILE_EXTS = {
    ".txt", ".cue", ".log", ".nfo", ".sfv", ".md5", ".jpg", ".jpeg", ".png", ".gif",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".m3u", ".m3u8", ".pls", ".gpx", ".ini",
    ".zip", ".rar", ".7z", ".iso", ".torrent", ".url"
}
_VIDEO_EXTS = {".avi", ".mkv", ".mp4", ".ts", ".m2ts", ".vob", ".mov", ".webm", ".wmv", ".mpg", ".mpeg"}
_NON_AUDIO_HINT_RE = re.compile(
    r"(?:\bvideo\b|видео|hdrip|bdrip|dvdrip|tvrip|satrip|клип|видеоурок|обучени|lesson|tutorial|караоке)",
    re.I,
)
_GENERIC_ALBUM_HINT_RE = re.compile(
    r"(?:\bgreatest\s+hits\b|\bhits\s+and\s+unreleased\b|\bkaraoke\b|\bthe\s+best\b)",
    re.I,
)

DEFAULT_TIME_BUDGET_SEC = getattr(settings, "rutracker_time_budget_sec", 6.0)
DEFAULT_WANT_RESULTS = getattr(settings, "rutracker_want_results", 3)
DEFAULT_MAX_PAGES_WIDE = getattr(settings, "rutracker_search_max_pages_wide", 3)
DEFAULT_MAX_CANDIDATES = getattr(settings, "rutracker_max_candidates_per_pass", 25)
DEFAULT_FILELIST_TTL = getattr(settings, "rutracker_filelist_ttl", 24 * 3600)
DEFAULT_SEARCH_TTL = getattr(settings, "rutracker_search_ttl", 10 * 60)
DEFAULT_COOKIE_TTL = getattr(settings, "rutracker_cookie_ttl", 24 * 3600)

MUSIC_FORUM_IDS: tuple[int, ...] = (
    1872, 675, 1874,
    792, 435, 560, 794, 556, 2307, 557, 2308, 558, 793, 1395, 1396, 436, 2309, 2310, 2311, 969,
    1130, 1131, 1132, 1133, 2084, 1128, 1129, 1856, 2430, 1283, 2085, 1282, 1284, 1285, 1138, 1136, 1137,
    1126, 1127, 1134, 1135, 2018, 855,
    441, 1173, 1486, 1172, 446, 909, 1665,
    1764, 1767, 1769, 1765, 1771, 1770, 1768, 1774, 1772, 2233,
    2377, 468, 691, 469, 786, 785, 1631, 1499, 715, 1388, 282, 796, 784, 783, 2331, 2431, 880,
    1220, 1221, 1334, 1216, 1223, 1224, 1225, 1226,
    577, 1842, 1648, 134, 965,

    424, 1361, 425, 1635, 1634,
    428, 1362, 429, 735, 1753, 2232, 714, 1331, 1330, 1219, 1452, 2275, 2270, 1351,
    2503, 2504, 2502, 2501, 2505, 2500,

    2299, 2277, 2278, 2279, 2280, 2281, 2282, 2353, 2284, 2285, 2283, 2286, 2287,
    2300, 2293, 2292, 2290, 2289, 2288,
    2297, 2295, 2296, 2298,

    1702, 1703, 1704, 1705, 1706, 1707, 2329, 2330, 1708, 1709, 1710, 1711, 1712, 1713, 731, 1799, 1714, 1715,
    1718, 1796, 1797, 1719, 1778, 1779, 1780, 1720, 798, 1724, 1725, 1730, 1731, 1726, 1727, 1815, 1816, 1728, 1729,
    2230, 2231,
    1736, 1737, 1738, 1739, 1740, 1741, 1773, 202, 172, 236, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 2175, 2174,
    737, 738, 464, 463, 739, 740, 951, 952,

    1844, 1822, 1894, 1895, 460, 1818, 1819, 1847, 1824,
    1829, 1830, 1831, 1857, 1859, 1858, 840, 1860, 1825, 1826, 1827, 1828,
    797, 1805, 1832, 1833, 1834, 1836, 1837, 1839, 454, 1838, 1840, 1841, 2229,
    1861, 1862, 1947, 1946, 1945, 1944,
    1864, 1865, 1871, 1867, 1869, 1873,
    1868, 1875, 1877, 1878, 1907, 1880, 1881, 466, 465, 1866, 406,

    1115,
    1884, 1164, 2513, 1397, 2512, 1885, 1163, 2302, 2303, 1755, 1757, 1893, 1890,
    1660, 506, 1835, 1625, 1217, 974, 1444, 2401, 239, 450, 2301, 123, 1756, 1758, 1766, 1754,
    453, 1170, 1759, 1852,

    1909, 1927, 2240, 2248, 2244
)


def _music_forums_csv() -> str:
    return ",".join(str(fid) for fid in MUSIC_FORUM_IDS)


def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_brackets(s: str) -> str:
    return re.sub(r"\s*[\(\[].*?[\)\]]\s*", " ", s or "")


def _compact_no_space(s: str) -> str:
    return re.sub(r"\s+", "", _norm(s))


def _track_aliases(track: str) -> List[str]:
    tr = _norm(track)
    if not tr:
        return []
    variants = {
        tr,
        re.sub(r"\bfeat\.?.*$", "", tr).strip(),
        re.sub(r"\s*\(.*?\)\s*$", "", tr).strip(),
        re.sub(r"\s*\[.*?\]\s*$", "", tr).strip(),
    }
    if " - " in tr:
        variants.add(tr.split(" - ", 1)[0].strip())
    aliases = [v for v in variants if v]
    aliases.sort(key=len, reverse=True)
    return aliases


def _derive_artist_from_query(query: str, track: Optional[str]) -> str:
    q = (query or "").strip()
    if not q:
        return q
    tr = (track or "").strip()
    if not tr:
        return q
    q_norm = _norm(q)
    tr_norm = _norm(tr)
    if not q_norm or not tr_norm:
        return q

    # remove full track phrase from query if present
    reduced = re.sub(rf"(?<!\w){re.escape(tr_norm)}(?!\w)", " ", q_norm, flags=re.I)
    reduced = re.sub(r"\s+", " ", reduced).strip(" -:;,")
    # fallback to original if reduction made it empty/unusable
    if not reduced or len(reduced) < 2:
        return q
    return reduced


def _sanitize_album_hints(albums: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for a in albums or []:
        raw = (a or "").strip()
        if not raw:
            continue
        n = _norm(raw)
        if not n:
            continue
        if len(n) < 3:
            continue
        if _GENERIC_ALBUM_HINT_RE.search(n):
            continue
        if n in seen:
            continue
        seen.add(n)
        out.append(raw)
        if len(out) >= 3:
            break
    return out


def _normalized_text(resp: requests.Response) -> str:
    try:
        return resp.content.decode("cp1251", errors="ignore")
    except Exception:
        return resp.text or ""


def _file_ext(path: str) -> str:
    base = os.path.basename((path or "").strip()).lower()
    dot = base.rfind(".")
    if dot < 0:
        return ""
    return base[dot:]


def _looks_non_audio_release(forum_txt: str, title_txt: str) -> bool:
    src = f"{forum_txt} {title_txt}".lower()
    if _NON_AUDIO_HINT_RE.search(src):
        return True
    return any(ext[1:] in src for ext in _VIDEO_EXTS)


class InMemoryKVStore:
    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            return self._data.get(key)

    def set(self, key: str, value: Any, ex: Optional[int] = None):
        # TTL is ignored for local fallback; good enough for degraded mode.
        with self._lock:
            self._data[key] = value
        return True

    def delete(self, key: str):
        with self._lock:
            self._data.pop(key, None)
        return 1


def adaptive_retry(max_attempts=3, base_delay=1.0, max_delay=10.0, exponential_base=2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (requests.Timeout, requests.ConnectionError) as e:
                    last_exc = e
                    if attempt < max_attempts - 1:
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        log.debug("Retry %d/%d after %.1fs: %s", attempt + 1, max_attempts, delay, e)
                        time.sleep(delay)
                except Exception:
                    raise
            if last_exc:
                raise last_exc

        return wrapper

    return decorator


# ---------------- circuit breaker ----------------
class ServiceState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=120, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = ServiceState.CLOSED
        self._lock = threading.Lock()

    def _should_attempt_reset(self) -> bool:
        return bool(self.last_failure_time) and (time.time() - self.last_failure_time) >= self.recovery_timeout

    def _on_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == ServiceState.HALF_OPEN:
                self.state = ServiceState.CLOSED
                log.info("Circuit breaker: recovered (CLOSED)")

    def _on_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == ServiceState.HALF_OPEN or self.failure_count >= self.failure_threshold:
                self.state = ServiceState.OPEN
                log.warning("Circuit breaker: OPEN (failures=%d)", self.failure_count)

    def call(self, fn, *args, **kwargs):
        with self._lock:
            if self.state == ServiceState.OPEN:
                if self._should_attempt_reset():
                    self.state = ServiceState.HALF_OPEN
                    log.info("Circuit breaker: HALF_OPEN (test)")
                else:
                    raise HTTPException(status_code=503, detail="Service unavailable (circuit open)")
        try:
            res = fn(*args, **kwargs)
            self._on_success()
            return res
        except self.expected_exception:
            self._on_failure()
            raise
        except Exception:
            self._on_failure()
            raise


# ---------------- errors ----------------
class CaptchaRequired(Exception):
    def __init__(self, session_id: str, img_url: str):
        super().__init__(f"Captcha required: {session_id}")
        self.session_id = session_id
        self.img_url = img_url


@dataclass
class RutrackerAccount:
    index: int
    login: str
    password: str
    scraper: Any
    cb: CircuitBreaker
    cookie_key: str
    lock: threading.RLock
    recent_errors: int = 0
    last_error_at: float = 0.0
    last_success_at: float = 0.0
    blocked_until: float = 0.0
    last_keepalive_at: float = 0.0


# ---------------- service ----------------
class RutrackerService:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, base_url: Optional[str] = None):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        self.base_url = (base_url or settings.rutracker_base).rstrip("/")

        self.redis: Any = InMemoryKVStore()
        if getattr(settings, "redis_enabled", True):
            try:
                self.redis = self._build_redis_client()
            except Exception as e:
                log.warning("Redis unavailable for RutrackerService, using in-memory store: %s", e)
        self.cookie_ttl = DEFAULT_COOKIE_TTL
        self.search_ttl = DEFAULT_SEARCH_TTL
        self.filelist_ttl = DEFAULT_FILELIST_TTL
        self.dump_dir = getattr(settings, "debug_html_dir", "/app/debug_html")
        os.makedirs(self.dump_dir, exist_ok=True)

        # perf knobs
        self.time_budget_sec = DEFAULT_TIME_BUDGET_SEC
        self.want_results = DEFAULT_WANT_RESULTS
        self.max_pages_wide = DEFAULT_MAX_PAGES_WIDE
        self.max_candidates = DEFAULT_MAX_CANDIDATES
        # РЅРѕРІС‹Р№ Р»РёРјРёС‚ РґР»СЏ artist-only СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ (РєРѕРіРґР° track is None)
        self.artist_results_limit = int(getattr(settings, "rutracker_artist_results_limit", 50))
        self.artist_search_ttl = int(getattr(settings, "rutracker_artist_search_ttl", max(self.search_ttl, 15 * 60)))
        self.track_search_ttl = int(getattr(settings, "rutracker_track_search_ttl", self.search_ttl))
        self.artist_time_budget_sec = float(getattr(settings, "rutracker_artist_time_budget_sec", 10.0))
        self.track_time_budget_sec = float(getattr(settings, "rutracker_track_time_budget_sec", self.time_budget_sec))
        self.artist_phase1_pages = max(1, int(getattr(settings, "rutracker_artist_phase1_pages", 2)))
        self.artist_phase1_results = max(1, int(getattr(settings, "rutracker_artist_phase1_results", 20)))
        self.artist_max_candidates_per_pass = max(
            1, int(getattr(settings, "rutracker_artist_max_candidates_per_pass", 80))
        )
        self.track_max_candidates_per_pass = max(
            1, int(getattr(settings, "rutracker_track_max_candidates_per_pass", self.max_candidates))
        )
        self.track_max_pages_wide = max(
            1, int(getattr(settings, "rutracker_track_search_max_pages_wide", self.max_pages_wide))
        )
        self.track_max_pages_narrow = max(
            1, int(getattr(settings, "rutracker_track_search_max_pages_narrow", 10))
        )
        self.track_relaxed_reserve_ratio = float(getattr(settings, "rutracker_track_relaxed_reserve_ratio", 0.4))
        if self.track_relaxed_reserve_ratio < 0.0:
            self.track_relaxed_reserve_ratio = 0.0
        if self.track_relaxed_reserve_ratio > 0.8:
            self.track_relaxed_reserve_ratio = 0.8
        self.track_strict_album_limit_nohit = max(
            0, int(getattr(settings, "rutracker_track_strict_album_limit_nohit", 1))
        )
        self.track_force_relaxed_filematch_grace_sec = max(
            0.0, float(getattr(settings, "rutracker_track_force_relaxed_filematch_grace_sec", 1.5))
        )
        self.track_force_relaxed_filematch_candidates = max(
            1, int(getattr(settings, "rutracker_track_force_relaxed_filematch_candidates", 12))
        )
        self.download_timeout_sec = float(getattr(settings, "rutracker_download_timeout_sec", 12.0))
        self.download_retries = max(1, int(getattr(settings, "rutracker_download_retries", 1)))
        self.download_retry_delay_sec = float(getattr(settings, "rutracker_download_retry_delay_sec", 0.6))
        self.track_presence_ttl_hit_sec = max(
            600, int(getattr(settings, "rutracker_track_presence_ttl_hit_sec", 7 * 24 * 3600))
        )
        self.track_presence_ttl_miss_sec = max(
            60, int(getattr(settings, "rutracker_track_presence_ttl_miss_sec", 4 * 3600))
        )
        self.cookie_keepalive_enabled = bool(getattr(settings, "rutracker_cookie_keepalive_enabled", True))
        self.cookie_keepalive_interval_sec = float(
            getattr(settings, "rutracker_cookie_keepalive_interval_sec", 600.0)
        )

        self._thread_ctx = threading.local()
        self._rr_lock = threading.Lock()
        self._rr_index = 0
        self.accounts = self._build_accounts()
        if not self.accounts:
            raise RuntimeError("RuTracker credentials are not configured")
        self._restore_cookies()

        self._executor = ThreadPoolExecutor(max_workers=10)
        log.debug("Init RutrackerService base_url=%s accounts=%d", self.base_url, len(self.accounts))

    def _mk_scraper(self):
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        adapter = HTTPAdapter(
            pool_connections=12,
            pool_maxsize=24,
            max_retries=Retry(
                total=2,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                respect_retry_after_header=True,
            ),
        )
        scraper.mount("http://", adapter)
        scraper.mount("https://", adapter)
        scraper.timeout = 25
        return scraper

    def _parse_accounts(self) -> List[Tuple[str, str]]:
        parsed: List[Tuple[str, str]] = []

        raw_accounts = getattr(settings, "rutracker_accounts", None) or ""
        for chunk in re.split(r"[;\n]+", str(raw_accounts)):
            part = chunk.strip()
            if not part:
                continue
            if ":" not in part:
                log.warning("Skip malformed RUTRACKER_ACCOUNTS entry: %r", part)
                continue
            login, password = part.split(":", 1)
            login = login.strip()
            password = password.strip()
            if not login or not password:
                continue
            parsed.append((login, password))

        single_login = (settings.rutracker_login or "").strip()
        single_pass = (settings.rutracker_password or "").strip()
        if single_login and single_pass and (single_login, single_pass) not in parsed:
            parsed.insert(0, (single_login, single_pass))

        return parsed

    def _build_accounts(self) -> List[RutrackerAccount]:
        creds = self._parse_accounts()
        accounts: List[RutrackerAccount] = []
        for idx, (login, password) in enumerate(creds):
            safe_login = re.sub(r"[^a-zA-Z0-9_.-]+", "_", login)[:64]
            cookie_key = f"rutracker:cookiejar:{idx}:{safe_login}"
            accounts.append(
                RutrackerAccount(
                    index=idx,
                    login=login,
                    password=password,
                    scraper=self._mk_scraper(),
                    cb=CircuitBreaker(expected_exception=(requests.RequestException, HTTPException)),
                    cookie_key=cookie_key,
                    lock=threading.RLock(),
                )
            )
        return accounts

    def _restore_cookies(self):
        for acc in self.accounts:
            try:
                raw = self.redis.get(acc.cookie_key)
                if raw:
                    acc.scraper.cookies.update(json.loads(raw))
                    log.debug(
                        "Restored cookies for account[%d]=%s bb_session=%s",
                        acc.index,
                        acc.login,
                        acc.scraper.cookies.get("bb_session"),
                    )
            except Exception:
                log.exception("Cookie restore failed for account[%d]=%s", acc.index, acc.login)

    def _choose_account(self) -> RutrackerAccount:
        with self._rr_lock:
            idx = self._rr_index % len(self.accounts)
            self._rr_index += 1
            return self.accounts[idx]

    def _ordered_accounts(self) -> List[RutrackerAccount]:
        if not self.accounts:
            return []
        with self._rr_lock:
            start = self._rr_index % len(self.accounts)
            self._rr_index += 1
        rotated = self.accounts[start:] + self.accounts[:start]
        now = time.time()

        def score(acc: RutrackerAccount) -> tuple[int, int, int, float]:
            state_rank = {
                ServiceState.CLOSED: 0,
                ServiceState.HALF_OPEN: 1,
                ServiceState.OPEN: 2,
            }.get(acc.cb.state, 2)
            blocked = 1 if acc.blocked_until > now else 0
            no_session = 0 if acc.scraper.cookies.get("bb_session") else 1
            return (blocked, state_rank, no_session, float(acc.recent_errors))

        return sorted(rotated, key=score)

    def _mark_account_success(self, acc: RutrackerAccount):
        acc.recent_errors = 0
        acc.last_success_at = time.time()
        if acc.cb.state != ServiceState.CLOSED:
            acc.cb.state = ServiceState.CLOSED
            acc.cb.failure_count = 0

    def _mark_account_error(self, acc: RutrackerAccount, exc: Exception):
        acc.recent_errors = min(100, acc.recent_errors + 1)
        acc.last_error_at = time.time()
        if isinstance(exc, CaptchaRequired):
            acc.blocked_until = time.time() + 120.0
        elif isinstance(exc, HTTPException) and int(getattr(exc, "status_code", 0)) in (429, 503):
            acc.blocked_until = time.time() + 60.0

    def _get_current_account(self) -> RutrackerAccount:
        acc = getattr(self._thread_ctx, "account", None)
        if acc is not None:
            return acc
        acc = self._choose_account()
        self._thread_ctx.account = acc
        return acc

    @contextmanager
    def _account_context(self, account: RutrackerAccount):
        prev = getattr(self._thread_ctx, "account", None)
        self._thread_ctx.account = account
        try:
            yield
        finally:
            if prev is None:
                try:
                    del self._thread_ctx.account
                except Exception:
                    pass
            else:
                self._thread_ctx.account = prev

    def _build_redis_client(self):
        attempts = max(1, int(getattr(settings, "redis_startup_retries", 8)))
        delay = float(getattr(settings, "redis_startup_retry_delay_sec", 1.0))
        for attempt in range(1, attempts + 1):
            try:
                candidate = redis.Redis.from_url(settings.redis_url, decode_responses=True)
                candidate.ping()
                return candidate
            except Exception as e:
                log.warning("RuTracker redis connect attempt %s/%s failed: %s", attempt, attempts, e)
                if attempt < attempts:
                    time.sleep(delay)
        raise RuntimeError(f"RuTracker redis unavailable after {attempts} attempts")

    # -------- session / token helpers ----------
    def _extract_token(self, html: str) -> str:
        m = _FORM_TOKEN_RE.search(html)
        return m.group(1) if m else ""

    def _is_login_page(self, text: str) -> bool:
        snip = (text or "")[:4000]
        return 'id="login-form-full"' in snip or 'name="login_username"' in snip

    @adaptive_retry(max_attempts=2, base_delay=1.5)
    def _login_sync(self):
        acc = self._get_current_account()

        url = f"{self.base_url}/forum/login.php"
        with acc.lock:
            r = acc.scraper.get(url, timeout=25)
        r.raise_for_status()
        doc = lxml.html.fromstring(r.text)
        form = doc.get_element_by_id("login-form-full")
        data = {i.get("name"): i.get("value", "") for i in form.xpath(".//input[@type='hidden']") if i.get("name")}
        tok = self._extract_token(r.text)
        if tok:
            data["form_token"] = tok
        data.update({
            "login_username": acc.login,
            "login_password": acc.password,
            "login": (form.xpath(".//input[@type='submit']/@value") or [""])[0],
        })
        with acc.lock:
            r2 = acc.scraper.post(url, data=data, allow_redirects=True, timeout=25)
        r2.raise_for_status()
        if not acc.scraper.cookies.get("bb_session"):
            self._handle_login_captcha()
        self.redis.set(acc.cookie_key, json.dumps(acc.scraper.cookies.get_dict()), ex=self.cookie_ttl)
        acc.last_keepalive_at = time.time()
        self._mark_account_success(acc)
        log.debug("Login succeeded account[%d]=%s bb_session=%s", acc.index, acc.login, acc.scraper.cookies.get("bb_session"))

    def _handle_login_captcha(self):
        acc = self._get_current_account()
        url = f"{self.base_url}/forum/login.php"
        with acc.lock:
            r = acc.scraper.get(url, timeout=25)
        r.raise_for_status()
        doc = lxml.html.fromstring(r.text)
        hidden = {i.get("name"): i.get("value", "") for i in doc.xpath(".//input[@type='hidden']") if i.get("name")}
        sid = hidden.get("cap_sid", uuid.uuid4().hex)
        img = (doc.xpath("//img[contains(@src,'/captcha/')]/@src") or [""])[0]
        if not img:
            path = os.path.join(self.dump_dir, f"captcha_parse_error_{sid}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(r.text)
            raise RuntimeError(f"Captcha parse failed вЂ” dumped {path}")
        raise CaptchaRequired(sid, urljoin(self.base_url + "/forum/", img))

    def _ensure_login(self):
        acc = self._get_current_account()
        if acc.scraper.cookies.get("bb_session"):
            self._maybe_keepalive(acc)
            return
        return acc.cb.call(self._login_sync)

    def _maybe_keepalive(self, acc: RutrackerAccount):
        if not self.cookie_keepalive_enabled:
            return
        now = time.time()
        if (now - float(acc.last_keepalive_at or 0.0)) < self.cookie_keepalive_interval_sec:
            return
        url = f"{self.base_url}/forum/tracker.php"
        try:
            with acc.lock:
                r = acc.scraper.get(url, params={"nm": "flac"}, allow_redirects=False, timeout=12)
            if r.status_code in (301, 302) and "login.php" in (r.headers.get("Location") or ""):
                acc.scraper.cookies.clear()
                self.redis.delete(acc.cookie_key)
                return
            if self._is_login_page(r.text):
                acc.scraper.cookies.clear()
                self.redis.delete(acc.cookie_key)
                return
            self.redis.set(acc.cookie_key, json.dumps(acc.scraper.cookies.get_dict()), ex=self.cookie_ttl)
            acc.last_keepalive_at = now
        except Exception as e:
            log.debug("Keepalive failed account[%d]=%s: %s", acc.index, acc.login, e)

    @adaptive_retry(max_attempts=2, base_delay=1.0)
    def _search_request(self, params: dict, data: dict | None = None) -> requests.Response:
        acc = self._get_current_account()
        url = f"{self.base_url}/forum/tracker.php"
        with acc.lock:
            if data is None:
                r = acc.scraper.get(url, params=params, allow_redirects=False, timeout=25)
            else:
                r = acc.scraper.post(url, data=data, allow_redirects=False, timeout=25)
        r.encoding = "cp1251"

        if r.status_code in (301, 302) and "login.php" in (r.headers.get("Location") or ""):
            acc.scraper.cookies.clear()
            self.redis.delete(acc.cookie_key)
            self._login_sync()
            with acc.lock:
                r = acc.scraper.get(url, params=params, timeout=25) if data is None else acc.scraper.post(
                    url, data=data, timeout=25
                )
            r.encoding = "cp1251"
            return r

        if r.status_code >= 500 or r.status_code in (429,):
            raise requests.RequestException(f"Transient HTTP {r.status_code}")

        r.raise_for_status()
        if self._is_login_page(_normalized_text(r)):
            acc.scraper.cookies.clear()
            self.redis.delete(acc.cookie_key)
            self._login_sync()
            with acc.lock:
                r = acc.scraper.get(url, params=params, timeout=25) if data is None else acc.scraper.post(
                    url, data=data, timeout=25
                )
            r.encoding = "cp1251"
            r.raise_for_status()
        return r

    # -------- redis cache helpers ----------
    def _cache_get_json(self, key: str):
        raw = self.redis.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _cache_set_json(self, key: str, obj: Any, ttl: int):
        try:
            self.redis.set(key, json.dumps(obj), ex=ttl)
        except Exception:
            pass

    def _has_track_cache_key(self, tid: int, track: str) -> str:
        return f"rt:v3:hastrack:{tid}:{_norm(track)}"

    def _get_track_presence(self, tid: int, track: str, allow_cached_miss: bool = True) -> Optional[bool]:
        try:
            v = self.redis.get(self._has_track_cache_key(tid, track))
            if v is None:
                return None
            if v == "1":
                return True
            if v == "0" and allow_cached_miss:
                return False
            return None
        except Exception:
            return None

    def _set_track_presence(self, tid: int, track: str, present: bool, ttl: Optional[int] = None) -> None:
        try:
            if ttl is None:
                ttl = self.track_presence_ttl_hit_sec if present else self.track_presence_ttl_miss_sec
            self.redis.set(self._has_track_cache_key(tid, track), "1" if present else "0", ex=int(ttl))
        except Exception:
            pass

    # -------- filelists ----------
    @adaptive_retry(max_attempts=2, base_delay=1.0)
    def _fetch_filelist_html(self, tid: int) -> Optional[str]:
        self._ensure_login()
        acc = self._get_current_account()
        url = f"{self.base_url}/forum/viewtorrent.php"
        headers = {"X-Requested-With": "XMLHttpRequest", "Accept": "*/*"}
        data = {"t": str(tid)}
        with acc.lock:
            r = acc.scraper.post(url, data=data, headers=headers, allow_redirects=True, timeout=25)
        r.encoding = "cp1251"
        r.raise_for_status()
        body = _normalized_text(r)
        if not body.strip():
            return None
        log.debug("viewtorrent POST with %s returned body length %d for tid %s", data, len(body), tid)
        return body

    def _parse_filetree_html(self, html_fragment: str) -> List[str]:
        res: List[str] = []
        doc = lxml.html.fromstring(f"<div>{html_fragment}</div>")
        files = doc.xpath(
            ".//ul[contains(@class,'ftree')]//li[not(.//ul)]//b/text() | "
            ".//ul[contains(@class,'ftree')]//li[not(.//ul)]//div[not(.//b)]/text()"
        )
        for t in files:
            t = (t or "").strip()
            if t:
                res.append(t)
        return res

    def _get_filelist(self, tid: int) -> List[str]:
        key = f"rt:filelist:{tid}"
        cached = self._cache_get_json(key)
        if cached is not None:
            return cached
        html = self._fetch_filelist_html(tid)
        if not html:
            self._cache_set_json(key, [], ttl=self.filelist_ttl)
            return []
        files = self._parse_filetree_html(html)
        log.debug("viewtorrent.php returned %d file entries for tid %s", len(files), tid)
        self._cache_set_json(key, files, ttl=self.filelist_ttl)
        return files

    def _get_filelist_for_account(self, tid: int, account: RutrackerAccount) -> List[str]:
        with self._account_context(account):
            return self._get_filelist(tid)

    # -------- matching ----------
    def _artist_ok(self, query_artist: str, forum_txt: str, title_txt: str) -> bool:
        """
        Р‘РѕР»РµРµ В«РјСЏРіРєРёР№В» РјР°С‚С‡ Р°СЂС‚РёСЃС‚Р°:
        - РїСЂРѕРІРµСЂСЏРµРј РїРѕР»РЅСѓСЋ С„СЂР°Р·Сѓ Р°СЂС‚РёСЃС‚Р° (Р° РЅРµ С‚РѕР»СЊРєРѕ РїРµСЂРІРѕРµ СЃР»РѕРІРѕ);
        - РґРѕРїСѓСЃРєР°РµРј РІР°СЂРёР°С†РёРё СЃ СЂР°Р·РґРµР»РёС‚РµР»СЏРјРё (' - ', ' вЂ” ', '/', '&', ',');
        - РёРіРЅРѕСЂРёСЂСѓРµРј СЃРєРѕР±РєРё.
        """
        artist = _norm(_strip_brackets(query_artist))
        if not artist:
            return True

        title_norm = _norm(_strip_brackets(title_txt))

        # РїРѕР»РЅРѕРµ РІС…РѕР¶РґРµРЅРёРµ С„СЂР°Р·С‹ Р°СЂС‚РёСЃС‚Р°
        if re.search(rf"(?<!\w){re.escape(artist)}(?!\w)", title_norm):
            return True

        # С‡Р°СЃС‚Рѕ РїРёС€СѓС‚ В«Artist - ...В» РёР»Рё В«Artist вЂ” ...В»
        if title_norm.startswith(artist + " -") or title_norm.startswith(artist + " вЂ”"):
            return True

        # Р±СЊРµРј РїРѕ СЃСѓР±-С‚РѕРєРµРЅР°Рј (РґР»СЏ В«A / BВ», В«A & BВ» Рё С‚.Рї.)
        tokens = [t.strip() for t in re.split(r"[\/&+,]", artist) if t.strip()]
        tokens = [t for t in tokens if len(t) >= 3]
        if tokens and all(re.search(rf"(?<!\w){re.escape(t)}(?!\w)", title_norm) for t in tokens):
            return True

        log.debug("Artist miss in title: artist=%r title=%r forum=%r", artist, title_txt, forum_txt)
        return False

    def _track_in_files(
        self,
        files: List[str],
        track: str,
        album_hints: List[str],
        relax_file_match: bool = False,
    ) -> Tuple[bool, Dict[str, Any]]:
        if not files:
            return False, {"reason": "empty_filelist"}

        aliases = _track_aliases(track)
        if not aliases:
            return False, {"reason": "empty_track"}

        album_hints_norm = [_norm(a) for a in (album_hints or []) if a]

        for f in files:
            ext = _file_ext(f)
            if not relax_file_match:
                if ext in _VIDEO_EXTS:
                    continue
                if ext in _IGNORE_FILE_EXTS:
                    continue
                if ext and ext not in _AUDIO_EXTS:
                    continue
            f_norm = _norm(f)
            f_compact = _compact_no_space(f_norm)
            for idx, alias in enumerate(aliases):
                if alias in f_norm:
                    album_bonus = 10 if (album_hints_norm and any(h and h in f_norm for h in album_hints_norm)) else 0
                    return True, {
                        "file": f,
                        "album_bonus": album_bonus,
                        "match_quality": "exact" if idx == 0 else "alias",
                    }
                alias_compact = _compact_no_space(alias)
                if alias_compact and alias_compact in f_compact:
                    album_bonus = 10 if (album_hints_norm and any(h and h in f_norm for h in album_hints_norm)) else 0
                    return True, {
                        "file": f,
                        "album_bonus": album_bonus,
                        "match_quality": "compact",
                    }

        return False, {"reason": "no_track_match"}

    def _score_candidate(self, ti: TorrentInfo, seeders: int, hit_details: Dict[str, Any]) -> int:
        album_bonus = int(hit_details.get("album_bonus", 0) or 0)
        quality = str(hit_details.get("match_quality") or "")
        score = 60
        if quality == "exact":
            score += 15
        elif quality in {"alias", "compact"}:
            score += 8
        score += min(20, album_bonus)
        score += int(max(0, seeders) ** 0.5) * 5
        if _LOSSLESS_RE.search(ti.title):
            score += 5
        return score

    # -------- search parsing & pagination ----------
    def _parse_search_rows(self, html: str) -> Tuple[List[Tuple[TorrentInfo, int, int, str, str]], int]:
        doc = lxml.html.fromstring(html)
        rows = doc.xpath("//table[@id='tor-tbl']//tr[@data-topic_id]")
        total_hint = None
        m = _TOTAL_RE.search(html)
        if m:
            try:
                total_hint = int(m.group(1))
            except Exception:
                total_hint = None

        parsed: List[Tuple[TorrentInfo, int, int, str, str]] = []
        for row in rows:
            try:
                tid = int(row.get("data-topic_id"))
                size = (row.xpath(".//td[contains(@class,'tor-size')]//a/text()") or [""])[0].strip()
                seeders = int((row.xpath(".//b[contains(@class,'seedmed')]/text()") or ["0"])[0].strip())
                leechers = int((row.xpath(".//td[contains(@class,'leechmed')]/text()") or ["0"])[0].strip())
                hrefs = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")
                url_dl = urljoin(self.base_url + "/forum/", hrefs[0]) if hrefs else ""
                forum_a = (row.xpath(".//td[contains(@class,'f-name-col')]//a") or [None])[0]
                forum_txt = forum_a.text_content().strip() if forum_a is not None else ""
                forum_href = forum_a.get("href") if forum_a is not None else ""
                mm = re.search(r"[?&]f=(\d+)", forum_href or "")
                forum_id = int(mm.group(1)) if mm else 0
                title_txt = (row.xpath(".//td[contains(@class,'t-title-col')]//a/text()") or [""])[0].strip()

                ti = TorrentInfo(title=f"{forum_txt} {title_txt}".strip(), url=url_dl, size=size, seeders=seeders,
                                 leechers=leechers)
                parsed.append((ti, tid, forum_id, forum_txt, title_txt))
            except Exception as e:
                log.debug("Parse row error: %s", e)
        return parsed, (total_hint or 0)

    def _perform_search_pages(
            self,
            query: str,
            max_pages: int,
            t0: float,
            budget: float,
            want_candidates: int,
            artist_only: bool,
    ) -> Tuple[List[Tuple[TorrentInfo, int, int, str, str]], bool]:
        """
        Р’РѕР·РІСЂР°С‰Р°РµС‚:
          - РІСЃРµ СЃС‚СЂРѕРєРё СЃ С‚РµРєСѓС‰РёС… СЃС‚СЂР°РЅРёС†
          - С„Р»Р°Рі used_whitelist: True, РµСЃР»Рё РёСЃРєР°Р»Рё СЃ f=<whitelist>, РёРЅР°С‡Рµ False
        """
        self._ensure_login()

        # В«СЂР°Р·Р±СѓРґРёРјВ» С‚РѕРєРµРЅРѕРј
        r0 = self._search_request({"nm": query})
        token = self._extract_token(r0.text)

        # СЃС‚Р°СЂС‚РѕРІС‹Р№ POST
        post: Dict[str, Any] = {"nm": query}
        if token:
            post["form_token"] = token
        if artist_only:
            post["f"] = _music_forums_csv()  # <-- whitelist РІРєР»СЋС‡РµРЅ
        else:
            post["f"] = "-1"
        r1 = self._search_request(params={}, data=post)

        page0, total_hint = self._parse_search_rows(r1.text)
        all_rows = list(page0)
        used_whitelist = bool(artist_only)
        log.debug("Search page 0: rows=%d (query=%r, total_hint=%s, artist_only=%s)", len(page0), query,
                  total_hint or "?", artist_only)

        if len(all_rows) >= want_candidates or (time.time() - t0) >= budget:
            return all_rows, used_whitelist

        # РїР°РіРёРЅР°С†РёСЏ GET
        for page in range(1, max_pages):
            if (time.time() - t0) >= budget:
                break
            start = page * 50
            params: Dict[str, Any] = {"nm": query, "start": str(start)}
            params["f"] = _music_forums_csv() if artist_only else "-1"
            r = self._search_request(params=params, data=None)
            rows, _ = self._parse_search_rows(r.text)
            log.debug("Search page %d: rows=%d (start=%d)", page, len(rows), start)
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < 50:
                break
            if len(all_rows) >= want_candidates:
                break

        return all_rows, used_whitelist

    # -------- core pass ----------
    def _pass_once(
            self,
            query_str: str,
            artist_filter: str,
            only_lossless: Optional[bool],
            track: Optional[str],
            album_hints: Optional[List[str]],
            t0: float,
            budget: float,
            want: int,
            max_pages_override: Optional[int] = None,
            max_candidates_override: Optional[int] = None,
            strict_release_filter: bool = True,
            relax_file_match: bool = False,
            debug_info: Optional[Dict[str, Any]] = None,
    ) -> List[TorrentInfo]:
        is_wide = ('"' not in query_str) and (' ' not in query_str or track is None)
        if max_pages_override is not None:
            max_pages = max(1, int(max_pages_override))
        else:
            # Р”Р°РґРёРј artist-only РїРѕР±РѕР»СЊС€Рµ СЃС‚СЂР°РЅРёС† (С€РёСЂРµ РѕР±С…РѕРґ)
            max_pages = 10 if (track is None) else (self.track_max_pages_wide if is_wide else self.track_max_pages_narrow)
        artist_only = track is None
        max_candidates = (
            max(1, int(max_candidates_override))
            if max_candidates_override is not None
            else (self.artist_max_candidates_per_pass if artist_only else self.track_max_candidates_per_pass)
        )

        parsed_all, used_whitelist = self._perform_search_pages(
            query=query_str,
            max_pages=max_pages,
            t0=t0,
            budget=budget,
            want_candidates=max(max_candidates, want * 5),
            artist_only=artist_only,
        )

        # Fallback: РµСЃР»Рё СЃ whitelist С„РѕСЂСѓРјРѕРІ РЅРёС‡РµРіРѕ РЅРµ РЅР°С€Р»Рё вЂ” РїРѕРІС‚РѕСЂ Р±РµР· С„РёР»СЊС‚СЂР°
        if not parsed_all and artist_only:
            log.debug("Artist-only search returned 0 rows, retrying without forum filter...")
            parsed_all, used_whitelist = self._perform_search_pages(
                query=query_str,
                max_pages=max_pages,
                t0=t0,
                budget=budget,
                want_candidates=max(max_candidates, want * 5),
                artist_only=False,
            )

        log.debug("Search rows found: %d (query=%r, lossless=%s, track=%r, total_hint=?)",
                  len(parsed_all), query_str, only_lossless, track)

        skip_lossless = 0
        skip_release = 0
        skip_whitelist = 0
        skip_artist = 0
        items_pre: List[Tuple[TorrentInfo, int, int, str, str]] = []
        for ti, tid, forum_id, forum_txt, title_txt in parsed_all:
            is_lossless = bool(_LOSSLESS_RE.search(ti.title))
            is_lossy = bool(_LOSSY_RE.search(ti.title))
            if only_lossless is True and (not is_lossless or is_lossy):
                skip_lossless += 1
                continue
            if only_lossless is False and is_lossless:
                skip_lossless += 1
                continue
            if track and strict_release_filter and _looks_non_audio_release(forum_txt, title_txt):
                skip_release += 1
                continue
            # Р•СЃР»Рё СѓР¶Рµ РёСЃРєР°Р»Рё СЃ whitelist (f=...), РїРѕРІС‚РѕСЂРЅРѕ РЅРµ СЂРµР¶РµРј
            if track is None and artist_only and (not used_whitelist) and forum_id not in MUSIC_FORUM_IDS:
                skip_whitelist += 1
                continue
            # РјР°С‚С‡ РїРѕ РїРѕР»РЅРѕР№ С„СЂР°Р·Рµ Р°СЂС‚РёСЃС‚Р°
            if not self._artist_ok(artist_filter, forum_txt, title_txt):
                skip_artist += 1
                continue
            items_pre.append((ti, tid, forum_id, forum_txt, title_txt))

        items_pre.sort(key=lambda x: x[0].seeders, reverse=True)
        if len(items_pre) > max_candidates:
            items_pre = items_pre[:max_candidates]

        log.debug("Parsed items: %d (pre track-filter)", len(items_pre))
        if not track:
            # Р”Р»СЏ artist-only РІРѕР·РІСЂР°С‰Р°РµРј Р±РѕР»СЊС€Рµ, С‡РµРј self.want_results
            limit = max(want, self.artist_results_limit)
            out = [ti for (ti, _, __, ___, ____) in items_pre[:limit]]
            if debug_info is not None:
                debug_info.update(
                    {
                        "query": query_str,
                        "artist_filter": artist_filter,
                        "artist_only": True,
                        "max_pages": max_pages,
                        "max_candidates": max_candidates,
                        "rows_total": len(parsed_all),
                        "rows_after_prefilter": len(items_pre),
                        "skip_lossless": skip_lossless,
                        "skip_release": skip_release,
                        "skip_whitelist": skip_whitelist,
                        "skip_artist": skip_artist,
                        "used_forum_whitelist": bool(used_whitelist),
                        "returned_count": len(out),
                        "returned_sample": [
                            {"title": x.title, "seeders": x.seeders, "url": x.url}
                            for x in out[: min(8, len(out))]
                        ],
                    }
                )
            return out

        hints = [a for a in (album_hints or []) if a]

        results: List[Tuple[int, TorrentInfo]] = []
        need_check = items_pre
        account = self._get_current_account()
        track_presence_cache_true = 0
        track_presence_cache_false = 0
        track_presence_cache_miss = 0
        filelist_checks_started = 0
        filelist_checks_failed = 0
        filelist_track_hits = 0
        check_debug: List[Dict[str, Any]] = []

        max_workers = min(12, max(2, len(need_check)))
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for ti, tid, _, __, ___ in need_check:
                pres = self._get_track_presence(tid, track, allow_cached_miss=(not relax_file_match))
                if pres is True:
                    track_presence_cache_true += 1
                    details = {"album_bonus": 0, "match_quality": "cached"}
                    score = self._score_candidate(ti, ti.seeders, details)
                    results.append((score, ti))
                    if debug_info is not None and len(check_debug) < 24:
                        check_debug.append(
                            {
                                "topic_id": tid,
                                "title": ti.title,
                                "seeders": ti.seeders,
                                "from_presence_cache": True,
                                "track_hit": True,
                                "details": details,
                            }
                        )
                    if len(results) >= want or (time.time() - t0) >= budget:
                        break
                    continue
                if pres is False:
                    track_presence_cache_false += 1
                    if debug_info is not None and len(check_debug) < 24:
                        check_debug.append(
                            {
                                "topic_id": tid,
                                "title": ti.title,
                                "seeders": ti.seeders,
                                "from_presence_cache": True,
                                "track_hit": False,
                                "details": {"reason": "cached_miss"},
                            }
                        )
                    continue
                track_presence_cache_miss += 1
                filelist_checks_started += 1
                futures[ex.submit(self._get_filelist_for_account, tid, account)] = (ti, tid)

            for fut in as_completed(futures):
                if (time.time() - t0) >= budget:
                    break
                ti, tid = futures[fut]
                try:
                    files = fut.result()
                    hit, details = self._track_in_files(files, track, hints, relax_file_match=relax_file_match)
                    self._set_track_presence(tid, track, hit)
                    if not hit:
                        if debug_info is not None and len(check_debug) < 24:
                            check_debug.append(
                                {
                                    "topic_id": tid,
                                    "title": ti.title,
                                    "seeders": ti.seeders,
                                    "from_presence_cache": False,
                                    "track_hit": False,
                                    "file_count": len(files),
                                    "details": details,
                                }
                            )
                        continue
                    filelist_track_hits += 1
                    score = self._score_candidate(ti, ti.seeders, details)
                    results.append((score, ti))
                    if debug_info is not None and len(check_debug) < 24:
                        check_debug.append(
                            {
                                "topic_id": tid,
                                "title": ti.title,
                                "seeders": ti.seeders,
                                "from_presence_cache": False,
                                "track_hit": True,
                                "file_count": len(files),
                                "details": details,
                            }
                        )
                    if len(results) >= want:
                        break
                except Exception as e:
                    filelist_checks_failed += 1
                    log.debug("Filelist check failed for tid=%s: %s", tid, e)

        results.sort(key=lambda x: x[0], reverse=True)
        final = [ti for score, ti in results[:want]]
        log.debug("Final items after track-filter: %d", len(final))
        if debug_info is not None:
            debug_info.update(
                {
                    "query": query_str,
                    "artist_filter": artist_filter,
                    "artist_only": False,
                    "max_pages": max_pages,
                    "max_candidates": max_candidates,
                    "rows_total": len(parsed_all),
                    "rows_after_prefilter": len(items_pre),
                    "skip_lossless": skip_lossless,
                    "skip_release": skip_release,
                    "skip_whitelist": skip_whitelist,
                    "skip_artist": skip_artist,
                    "used_forum_whitelist": bool(used_whitelist),
                    "track_presence_cache_true": track_presence_cache_true,
                    "track_presence_cache_false": track_presence_cache_false,
                    "track_presence_cache_miss": track_presence_cache_miss,
                    "filelist_checks_started": filelist_checks_started,
                    "filelist_checks_failed": filelist_checks_failed,
                    "filelist_track_hits": filelist_track_hits,
                    "checks_sample": check_debug,
                    "returned_count": len(final),
                    "returned_sample": [
                        {"title": x.title, "seeders": x.seeders, "url": x.url}
                        for x in final[: min(8, len(final))]
                    ],
                }
            )
        return final

    def _search_sync(
        self,
        artist: str,
        only_lossless: Optional[bool],
        track: Optional[str],
        forced_account: Optional[RutrackerAccount] = None,
    ) -> List[TorrentInfo]:
        self._thread_ctx.account = forced_account or self._choose_account()
        # --- artist-only mode: wider recall with two phases ---
        if track is None:
            cache_key = f"search3:artist:{artist}:{only_lossless}"
            cached = self._cache_get_json(cache_key)
            if cached is not None:
                log.debug("Artist-only cache hit: %r", cache_key)
                return [TorrentInfo(**x) for x in cached]

            t0 = time.time()
            budget = self.artist_time_budget_sec
            want = max(self.artist_results_limit, self.want_results)
            items: List[TorrentInfo] = []

            def _merge_unique(base: List[TorrentInfo], extra: List[TorrentInfo], limit: int) -> List[TorrentInfo]:
                seen = {x.url for x in base}
                for it in extra:
                    if it.url in seen:
                        continue
                    base.append(it)
                    seen.add(it.url)
                    if len(base) >= limit:
                        break
                return base

            phase1_want = min(want, self.artist_phase1_results)
            phase1 = self._get_current_account().cb.call(
                self._pass_once,
                artist,
                artist,
                only_lossless,
                None,
                None,
                t0,
                budget,
                phase1_want,
                self.artist_phase1_pages,
                self.artist_max_candidates_per_pass,
            )
            items = _merge_unique(items, phase1, want)

            if (time.time() - t0) < budget and len(items) < want:
                phase2 = self._get_current_account().cb.call(
                    self._pass_once,
                    artist,
                    artist,
                    only_lossless,
                    None,
                    None,
                    t0,
                    budget,
                    want,
                    None,
                    self.artist_max_candidates_per_pass,
                )
                items = _merge_unique(items, phase2, want)

            if items:
                to_cache = [
                    dict(title=x.title, url=x.url, size=x.size, seeders=x.seeders, leechers=x.leechers)
                    for x in items
                ]
                self._cache_set_json(cache_key, to_cache, ttl=self.artist_search_ttl)
            else:
                log.debug("Artist-only search produced 0 items -> skip caching to avoid sticky empty cache")

            log.debug(
                "Search metrics (artist-only): success=%s, duration=%.3fs, query=%r, items=%d",
                bool(items), time.time() - t0, artist, len(items)
            )
            return items

        # --- track mode: aggressive precision/speed ---
        artist_base = _derive_artist_from_query(artist, track)
        cache_key = f"search3:track:{artist}:{only_lossless}:{track}"
        cached = self._cache_get_json(cache_key)
        if cached is not None:
            log.debug("Search cache hit: %r", cache_key)
            return [TorrentInfo(**x) for x in cached]

        t0 = time.time()
        budget = self.track_time_budget_sec
        want = max(1, self.want_results)
        items: List[TorrentInfo] = []
        try:
            meta = None
            albums: List[str] = []
            conf = 0.0

            if track:
                try:
                    meta = resolve_track(artist_base, track)
                    albums = [a for a in ([meta.primary_album] if getattr(meta, "primary_album", None) else []) +
                              ((getattr(meta, "albums", []) or [])) if a]
                    albums = _sanitize_album_hints(albums)
                    conf = float(getattr(meta, "confidence", 0.0) or 0.0)
                except Exception as e:
                    log.debug("Track resolver error (ignored): %s", e)
                    albums = []
                    conf = 0.0

            do_album_passes = bool(track) and conf >= 0.60 and bool(albums)
            strict_budget = max(0.8, budget * (1.0 - self.track_relaxed_reserve_ratio))
            strict_deadline = t0 + strict_budget

            def _merge_more(more: List[TorrentInfo]) -> int:
                seen = {x.url for x in items}
                added = 0
                for m in more:
                    if m.url in seen:
                        continue
                    items.append(m)
                    seen.add(m.url)
                    added += 1
                    if len(items) >= want:
                        break
                return added

            def _run_phase(
                phase_name: str,
                strict_release_filter: bool,
                relax_file_match: bool,
                phase_only_lossless: Optional[bool],
                deadline: Optional[float] = None,
                album_limit_if_nohit: Optional[int] = None,
            ) -> None:
                phase_budget = budget if deadline is None else max(0.1, deadline - t0)
                album_attempts = 0

                if track and (time.time() - t0) < budget and len(items) < want:
                    if deadline is not None and time.time() >= deadline:
                        return
                    q3 = f'{artist_base} "{track}"'
                    log.debug(
                        "track phase=%s track-quoted query=%r strict_release=%s relax_file=%s",
                        phase_name,
                        q3,
                        strict_release_filter,
                        relax_file_match,
                    )
                    more = self._get_current_account().cb.call(
                        self._pass_once,
                        q3,
                        artist_base,
                        phase_only_lossless,
                        track,
                        albums,
                        t0,
                        phase_budget,
                        want,
                        None,
                        self.track_max_candidates_per_pass,
                        strict_release_filter,
                        relax_file_match,
                    )
                    _merge_more(more)

                if track and (time.time() - t0) < budget and len(items) < want:
                    if deadline is not None and time.time() >= deadline:
                        return
                    q2 = f"{artist_base} {track}"
                    log.debug(
                        "track phase=%s track query=%r strict_release=%s relax_file=%s",
                        phase_name,
                        q2,
                        strict_release_filter,
                        relax_file_match,
                    )
                    more = self._get_current_account().cb.call(
                        self._pass_once,
                        q2,
                        artist_base,
                        phase_only_lossless,
                        track,
                        albums,
                        t0,
                        phase_budget,
                        want,
                        None,
                        self.track_max_candidates_per_pass,
                        strict_release_filter,
                        relax_file_match,
                    )
                    _merge_more(more)

                if (time.time() - t0) < budget and len(items) < want:
                    if deadline is not None and time.time() >= deadline:
                        return
                    more = self._get_current_account().cb.call(
                        self._pass_once,
                        artist_base,
                        artist_base,
                        phase_only_lossless,
                        track,
                        albums,
                        t0,
                        phase_budget,
                        want,
                        None,
                        self.track_max_candidates_per_pass,
                        strict_release_filter,
                        relax_file_match,
                    )
                    _merge_more(more)

                if do_album_passes and (time.time() - t0) < budget and len(items) < want:
                    for alb in albums:
                        if (time.time() - t0) >= budget or len(items) >= want:
                            break
                        if deadline is not None and time.time() >= deadline:
                            break
                        if (
                            album_limit_if_nohit is not None
                            and album_limit_if_nohit >= 0
                            and len(items) == 0
                            and album_attempts >= album_limit_if_nohit
                        ):
                            break
                        q = f'{artist_base} "{alb}"'
                        log.debug(
                            "track phase=%s album-quoted query=%r strict_release=%s relax_file=%s",
                            phase_name,
                            q,
                            strict_release_filter,
                            relax_file_match,
                        )
                        more = self._get_current_account().cb.call(
                            self._pass_once,
                            q,
                            artist_base,
                            phase_only_lossless,
                            track,
                            albums,
                            t0,
                            phase_budget,
                            want,
                            None,
                            self.track_max_candidates_per_pass,
                            strict_release_filter,
                            relax_file_match,
                        )
                        _merge_more(more)
                        album_attempts += 1

                    for alb in albums:
                        if (time.time() - t0) >= budget or len(items) >= want:
                            break
                        if deadline is not None and time.time() >= deadline:
                            break
                        if (
                            album_limit_if_nohit is not None
                            and album_limit_if_nohit >= 0
                            and len(items) == 0
                            and album_attempts >= album_limit_if_nohit
                        ):
                            break
                        q = f"{artist_base} {alb}"
                        log.debug(
                            "track phase=%s album query=%r strict_release=%s relax_file=%s",
                            phase_name,
                            q,
                            strict_release_filter,
                            relax_file_match,
                        )
                        more = self._get_current_account().cb.call(
                            self._pass_once,
                            q,
                            artist_base,
                            phase_only_lossless,
                            track,
                            albums,
                            t0,
                            phase_budget,
                            want,
                            None,
                            self.track_max_candidates_per_pass,
                            strict_release_filter,
                            relax_file_match,
                        )
                        _merge_more(more)
                        album_attempts += 1

            _run_phase(
                "strict",
                strict_release_filter=True,
                relax_file_match=False,
                phase_only_lossless=only_lossless,
                deadline=strict_deadline,
                album_limit_if_nohit=self.track_strict_album_limit_nohit if do_album_passes else None,
            )
            if not items:
                _run_phase(
                    "relaxed_release",
                    strict_release_filter=False,
                    relax_file_match=False,
                    phase_only_lossless=only_lossless,
                )
            if not items:
                _run_phase(
                    "relaxed_filematch",
                    strict_release_filter=False,
                    relax_file_match=True,
                    phase_only_lossless=only_lossless,
                )
            if not items and only_lossless is True:
                _run_phase(
                    "relaxed_lossless",
                    strict_release_filter=False,
                    relax_file_match=True,
                    phase_only_lossless=None,
                )
            if not items and time.time() < (t0 + budget + self.track_force_relaxed_filematch_grace_sec):
                qf = f"{artist_base} {track}" if track else artist_base
                log.debug("track phase=forced_relaxed_filematch query=%r", qf)
                forced_more = self._get_current_account().cb.call(
                    self._pass_once,
                    qf,
                    artist_base,
                    only_lossless,
                    track,
                    albums,
                    t0,
                    budget + self.track_force_relaxed_filematch_grace_sec,
                    want,
                    1,
                    self.track_force_relaxed_filematch_candidates,
                    False,
                    True,
                )
                _merge_more(forced_more)
            if not items:
                log.debug("track phase=artist_fallback query=%r", artist_base)
                fallback_artist = self._get_current_account().cb.call(
                    self._pass_once,
                    artist_base,
                    artist_base,
                    only_lossless,
                    None,  # artist-only fallback if track-based phases found nothing
                    albums,
                    t0,
                    budget,
                    max(want, 5),
                    self.artist_phase1_pages,
                    self.artist_max_candidates_per_pass,
                    False,
                    False,
                )
                _merge_more(fallback_artist)

            if items:
                to_cache = [dict(title=x.title, url=x.url, size=x.size, seeders=x.seeders, leechers=x.leechers) for x in
                            items]
                self._cache_set_json(cache_key, to_cache, ttl=self.track_search_ttl)
            else:
                log.debug("Search produced 0 items -> skip caching to avoid sticky empty cache")

            log.debug(
                "Search metrics: success=True, duration=%.3fs, query=%r, lossless=%s, track=%r (conf=%.2f)",
                time.time() - t0, artist, only_lossless, track, conf
            )
            return items
        except Exception as e:
            log.debug("Search metrics: success=False, duration=%.3fs, query=%r, err=%s",
                      time.time() - t0, artist, e)
            raise

    def _dump_search_debug_html(self, account: RutrackerAccount, page_no: int, html: str) -> str:
        ts = int(time.time() * 1000)
        name = f"rt_search_acc{account.index}_p{page_no}_{ts}.html"
        path = os.path.join(self.dump_dir, name)
        with open(path, "w", encoding="utf-8", errors="ignore") as f:
            f.write(html or "")
        return path

    def _rows_sample(self, rows: List[Tuple[TorrentInfo, int, int, str, str]], limit: int = 10) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for ti, tid, forum_id, forum_txt, title_txt in rows[:limit]:
            out.append(
                {
                    "topic_id": tid,
                    "forum_id": forum_id,
                    "forum": forum_txt,
                    "title": title_txt,
                    "seeders": ti.seeders,
                    "leechers": ti.leechers,
                    "size": ti.size,
                    "download_url": ti.url,
                }
            )
        return out

    def _debug_search_probe_sync(
        self,
        query: str,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None,
        max_pages: int = 2,
        save_html: bool = True,
        verify_track: bool = False,
        verify_top_n: int = 5,
        forced_account: Optional[RutrackerAccount] = None,
    ) -> Dict[str, Any]:
        self._thread_ctx.account = forced_account or self._choose_account()
        account = self._get_current_account()
        started = time.time()

        with self._account_context(account):
            self._ensure_login()
            pages: List[Dict[str, Any]] = []

            warm = self._search_request({"nm": query})
            form_token = self._extract_token(warm.text)
            warm_dump = self._dump_search_debug_html(account, -1, warm.text) if save_html else None
            pages.append(
                {
                    "page": -1,
                    "kind": "warmup_get",
                    "status_code": int(warm.status_code),
                    "bytes": len((warm.text or "").encode("utf-8", errors="ignore")),
                    "has_form_token": bool(form_token),
                    "dump_path": warm_dump,
                }
            )

            post_payload: Dict[str, Any] = {"nm": query, "f": "-1"}
            if form_token:
                post_payload["form_token"] = form_token
            p0 = self._search_request(params={}, data=post_payload)
            p0_rows, p0_total = self._parse_search_rows(p0.text)
            p0_dump = self._dump_search_debug_html(account, 0, p0.text) if save_html else None
            pages.append(
                {
                    "page": 0,
                    "kind": "search_post",
                    "status_code": int(p0.status_code),
                    "bytes": len((p0.text or "").encode("utf-8", errors="ignore")),
                    "total_hint": p0_total,
                    "rows_parsed": len(p0_rows),
                    "sample": self._rows_sample(p0_rows, limit=12),
                    "dump_path": p0_dump,
                }
            )

            collected = list(p0_rows)
            for page in range(1, max(1, int(max_pages))):
                params = {"nm": query, "start": str(page * 50), "f": "-1"}
                rp = self._search_request(params=params, data=None)
                rows, total_hint = self._parse_search_rows(rp.text)
                page_dump = self._dump_search_debug_html(account, page, rp.text) if save_html else None
                pages.append(
                    {
                        "page": page,
                        "kind": "search_get",
                        "status_code": int(rp.status_code),
                        "bytes": len((rp.text or "").encode("utf-8", errors="ignore")),
                        "total_hint": total_hint,
                        "rows_parsed": len(rows),
                        "sample": self._rows_sample(rows, limit=12),
                        "dump_path": page_dump,
                    }
                )
                if not rows:
                    break
                collected.extend(rows)
                if len(rows) < 50:
                    break

            raw_skip_lossless = 0
            raw_skip_release = 0
            filtered_rows: List[Tuple[TorrentInfo, int, int, str, str]] = []
            for ti, tid, forum_id, forum_txt, title_txt in collected:
                is_lossless = bool(_LOSSLESS_RE.search(ti.title))
                is_lossy = bool(_LOSSY_RE.search(ti.title))
                if only_lossless is True and (not is_lossless or is_lossy):
                    raw_skip_lossless += 1
                    continue
                if only_lossless is False and is_lossless:
                    raw_skip_lossless += 1
                    continue
                if track and _looks_non_audio_release(forum_txt, title_txt):
                    raw_skip_release += 1
                    continue
                filtered_rows.append((ti, tid, forum_id, forum_txt, title_txt))

            filtered_count = len(filtered_rows)

            def _merge_unique(base: List[TorrentInfo], extra: List[TorrentInfo], limit: int) -> int:
                seen = {x.url for x in base}
                added = 0
                for it in extra:
                    if it.url in seen:
                        continue
                    base.append(it)
                    seen.add(it.url)
                    added += 1
                    if len(base) >= limit:
                        break
                return added

            pipeline_items: List[TorrentInfo] = []
            phase_traces: List[Dict[str, Any]] = []
            resolver_payload: Dict[str, Any] = {}
            pipeline_mode = "artist_only" if not track else "track"

            if not track:
                t0 = time.time()
                budget = self.artist_time_budget_sec
                want = max(self.artist_results_limit, self.want_results)
                phase1_want = min(want, self.artist_phase1_results)

                dbg1: Dict[str, Any] = {}
                phase1_items = self._get_current_account().cb.call(
                    self._pass_once,
                    query,
                    query,
                    only_lossless,
                    None,
                    None,
                    t0,
                    budget,
                    phase1_want,
                    self.artist_phase1_pages,
                    self.artist_max_candidates_per_pass,
                    debug_info=dbg1,
                )
                added1 = _merge_unique(pipeline_items, phase1_items, want)
                phase_traces.append(
                    {
                        "phase": "artist_phase1",
                        "query": query,
                        "added_count": added1,
                        "items_count": len(phase1_items),
                        "debug": dbg1,
                    }
                )

                if (time.time() - t0) < budget and len(pipeline_items) < want:
                    dbg2: Dict[str, Any] = {}
                    phase2_items = self._get_current_account().cb.call(
                        self._pass_once,
                        query,
                        query,
                        only_lossless,
                        None,
                        None,
                        t0,
                        budget,
                        want,
                        None,
                        self.artist_max_candidates_per_pass,
                        debug_info=dbg2,
                    )
                    added2 = _merge_unique(pipeline_items, phase2_items, want)
                    phase_traces.append(
                        {
                            "phase": "artist_phase2",
                            "query": query,
                            "added_count": added2,
                            "items_count": len(phase2_items),
                            "debug": dbg2,
                        }
                    )
            else:
                t0 = time.time()
                budget = self.track_time_budget_sec
                want = max(1, self.want_results)
                artist_base = _derive_artist_from_query(query, track)

                albums: List[str] = []
                conf = 0.0
                resolver_error: Optional[str] = None
                try:
                    meta = resolve_track(artist_base, track)
                    albums = [a for a in ([meta.primary_album] if getattr(meta, "primary_album", None) else []) +
                              ((getattr(meta, "albums", []) or [])) if a]
                    albums = _sanitize_album_hints(albums)
                    conf = float(getattr(meta, "confidence", 0.0) or 0.0)
                except Exception as e:
                    resolver_error = str(e)

                resolver_payload = {
                    "effective_artist": artist_base,
                    "track": track,
                    "albums": albums,
                    "confidence": conf,
                    "error": resolver_error,
                }
                do_album_passes = bool(track) and conf >= 0.60 and bool(albums)
                resolver_payload["do_album_passes"] = bool(do_album_passes)
                strict_budget = max(0.8, budget * (1.0 - self.track_relaxed_reserve_ratio))
                strict_deadline = t0 + strict_budget

                def _run_pass(
                    phase_name: str,
                    pass_kind: str,
                    query_str: str,
                    strict_release_filter: bool,
                    relax_file_match: bool,
                    phase_only_lossless: Optional[bool],
                    track_for_pass: Optional[str] = track,
                    max_pages_override: Optional[int] = None,
                    max_candidates_override: Optional[int] = None,
                    deadline: Optional[float] = None,
                    budget_override: Optional[float] = None,
                    allow_over_budget: bool = False,
                ) -> None:
                    if len(pipeline_items) >= want:
                        return
                    now = time.time()
                    if deadline is not None and now >= deadline:
                        return
                    if not allow_over_budget and (now - t0) >= budget:
                        return
                    dbg: Dict[str, Any] = {}
                    before = len(pipeline_items)
                    started_pass = time.time()
                    call_budget = budget if budget_override is None else float(budget_override)
                    more = self._get_current_account().cb.call(
                        self._pass_once,
                        query_str,
                        artist_base,
                        phase_only_lossless,
                        track_for_pass,
                        albums,
                        t0,
                        call_budget,
                        want if track_for_pass else max(want, 5),
                        max_pages_override,
                        max_candidates_override or self.track_max_candidates_per_pass,
                        strict_release_filter,
                        relax_file_match,
                        debug_info=dbg,
                    )
                    added = _merge_unique(pipeline_items, more, max(want, 5) if track_for_pass is None else want)
                    phase_traces.append(
                        {
                            "phase": phase_name,
                            "pass_kind": pass_kind,
                            "query": query_str,
                            "strict_release_filter": strict_release_filter,
                            "relax_file_match": relax_file_match,
                            "only_lossless": phase_only_lossless,
                            "track_filter_enabled": bool(track_for_pass),
                            "duration_ms": int((time.time() - started_pass) * 1000),
                            "items_count": len(more),
                            "added_count": added,
                            "items_before": before,
                            "items_after": len(pipeline_items),
                            "debug": dbg,
                        }
                    )

                def _run_phase(
                    phase_name: str,
                    strict_release_filter: bool,
                    relax_file_match: bool,
                    phase_only_lossless: Optional[bool],
                    deadline: Optional[float] = None,
                    album_limit_if_nohit: Optional[int] = None,
                ) -> None:
                    phase_budget = budget if deadline is None else max(0.1, deadline - t0)
                    album_attempts = 0
                    _run_pass(
                        phase_name,
                        "track_quoted",
                        f'{artist_base} "{track}"',
                        strict_release_filter,
                        relax_file_match,
                        phase_only_lossless,
                        deadline=deadline,
                        budget_override=phase_budget,
                    )
                    _run_pass(
                        phase_name,
                        "track_plain",
                        f"{artist_base} {track}",
                        strict_release_filter,
                        relax_file_match,
                        phase_only_lossless,
                        deadline=deadline,
                        budget_override=phase_budget,
                    )
                    _run_pass(
                        phase_name,
                        "artist_only_query",
                        artist_base,
                        strict_release_filter,
                        relax_file_match,
                        phase_only_lossless,
                        deadline=deadline,
                        budget_override=phase_budget,
                    )

                    if do_album_passes:
                        for alb in albums:
                            if (
                                album_limit_if_nohit is not None
                                and album_limit_if_nohit >= 0
                                and len(pipeline_items) == 0
                                and album_attempts >= album_limit_if_nohit
                            ):
                                break
                            _run_pass(
                                phase_name,
                                "album_quoted",
                                f'{artist_base} "{alb}"',
                                strict_release_filter,
                                relax_file_match,
                                phase_only_lossless,
                                deadline=deadline,
                                budget_override=phase_budget,
                            )
                            album_attempts += 1
                        for alb in albums:
                            if (
                                album_limit_if_nohit is not None
                                and album_limit_if_nohit >= 0
                                and len(pipeline_items) == 0
                                and album_attempts >= album_limit_if_nohit
                            ):
                                break
                            _run_pass(
                                phase_name,
                                "album_plain",
                                f"{artist_base} {alb}",
                                strict_release_filter,
                                relax_file_match,
                                phase_only_lossless,
                                deadline=deadline,
                                budget_override=phase_budget,
                            )
                            album_attempts += 1

                _run_phase(
                    "strict",
                    True,
                    False,
                    only_lossless,
                    deadline=strict_deadline,
                    album_limit_if_nohit=self.track_strict_album_limit_nohit if do_album_passes else None,
                )
                if not pipeline_items:
                    _run_phase("relaxed_release", False, False, only_lossless)
                if not pipeline_items:
                    _run_phase("relaxed_filematch", False, True, only_lossless)
                if not pipeline_items and only_lossless is True:
                    _run_phase("relaxed_lossless", False, True, None)
                if not pipeline_items and time.time() < (t0 + budget + self.track_force_relaxed_filematch_grace_sec):
                    _run_pass(
                        "forced_relaxed_filematch",
                        "forced_track_plain",
                        f"{artist_base} {track}",
                        False,
                        True,
                        only_lossless,
                        max_pages_override=1,
                        max_candidates_override=self.track_force_relaxed_filematch_candidates,
                        budget_override=budget + self.track_force_relaxed_filematch_grace_sec,
                        allow_over_budget=True,
                    )
                if not pipeline_items:
                    _run_pass(
                        "artist_fallback",
                        "artist_fallback",
                        artist_base,
                        False,
                        False,
                        only_lossless,
                        track_for_pass=None,
                        max_pages_override=self.artist_phase1_pages,
                        max_candidates_override=self.artist_max_candidates_per_pass,
                    )

            track_probe: List[Dict[str, Any]] = []
            if track and verify_track:
                checks: List[Dict[str, Any]] = []
                for phase in phase_traces:
                    dbg = phase.get("debug") or {}
                    for c in (dbg.get("checks_sample") or []):
                        if isinstance(c, dict):
                            checks.append(c)

                seen_tid: set[int] = set()
                ranked_checks = sorted(
                    [c for c in checks if c.get("topic_id")],
                    key=lambda x: int(x.get("seeders") or 0),
                    reverse=True,
                )
                for c in ranked_checks:
                    tid = int(c.get("topic_id"))
                    if tid in seen_tid:
                        continue
                    seen_tid.add(tid)
                    track_probe.append(
                        {
                            "topic_id": tid,
                            "title": c.get("title"),
                            "seeders": int(c.get("seeders") or 0),
                            "file_count": int(c.get("file_count") or 0),
                            "track_hit": bool(c.get("track_hit")),
                            "from_presence_cache": bool(c.get("from_presence_cache")),
                            "details": c.get("details") or {},
                        }
                    )
                    if len(track_probe) >= max(1, int(verify_top_n)):
                        break

                if not track_probe and filtered_rows:
                    top = sorted(filtered_rows, key=lambda x: x[0].seeders, reverse=True)[: max(1, int(verify_top_n))]
                    for ti, tid, _, __, ___ in top:
                        files = self._get_filelist_for_account(tid, account)
                        hit, details = self._track_in_files(files, track, [])
                        track_probe.append(
                            {
                                "topic_id": tid,
                                "title": ti.title,
                                "seeders": ti.seeders,
                                "file_count": len(files),
                                "track_hit": bool(hit),
                                "from_presence_cache": False,
                                "details": details,
                            }
                        )

            duration_ms = int((time.time() - started) * 1000)
            return {
                "account": {"index": account.index, "login": account.login},
                "query": query,
                "only_lossless": only_lossless,
                "track": track,
                "max_pages": max_pages,
                "save_html": save_html,
                "duration_ms": duration_ms,
                "rows_collected": len(collected),
                "rows_after_lossless_filter": filtered_count,
                "raw_filter_stats": {
                    "skip_lossless": raw_skip_lossless,
                    "skip_non_audio_release": raw_skip_release,
                },
                "pages": pages,
                "pipeline_mode": pipeline_mode,
                "pipeline_final_count": len(pipeline_items),
                "pipeline_final_items": [
                    {
                        "title": ti.title,
                        "url": ti.url,
                        "size": ti.size,
                        "seeders": ti.seeders,
                        "leechers": ti.leechers,
                    }
                    for ti in pipeline_items[: max(8, self.want_results)]
                ],
                "resolver": resolver_payload,
                "pipeline_phases": phase_traces,
                "track_probe": track_probe,
            }

    # -------- async wrappers ----------
    async def search(self, query: str, only_lossless: Optional[bool] = None, track: Optional[str] = None) -> List[TorrentInfo]:
        import asyncio
        errors: List[Exception] = []
        for acc in self._ordered_accounts():
            try:
                result = await asyncio.to_thread(self._search_sync, query, only_lossless, track, acc)
                self._mark_account_success(acc)
                return result
            except Exception as e:
                self._mark_account_error(acc, e)
                errors.append(e)
                log.warning("RuTracker search failed for account[%d]=%s: %s", acc.index, acc.login, e)
        if errors:
            raise errors[-1]
        return []

    async def debug_search_probe(
        self,
        query: str,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None,
        max_pages: int = 2,
        save_html: bool = True,
        verify_track: bool = False,
        verify_top_n: int = 5,
    ) -> Dict[str, Any]:
        import asyncio

        errors: List[Exception] = []
        for acc in self._ordered_accounts():
            try:
                payload = await asyncio.to_thread(
                    self._debug_search_probe_sync,
                    query,
                    only_lossless,
                    track,
                    max_pages,
                    save_html,
                    verify_track,
                    verify_top_n,
                    acc,
                )
                self._mark_account_success(acc)
                return payload
            except Exception as e:
                self._mark_account_error(acc, e)
                errors.append(e)
                log.warning("RuTracker debug probe failed for account[%d]=%s: %s", acc.index, acc.login, e)

        if errors:
            raise errors[-1]
        raise RuntimeError("RuTracker debug probe failed for all accounts")

    async def prelogin_all_accounts(self) -> Dict[str, Any]:
        import asyncio

        results: List[Dict[str, Any]] = []
        for acc in self._ordered_accounts():
            try:
                await asyncio.to_thread(self._prelogin_single_sync, acc)
                self._mark_account_success(acc)
                results.append(
                    {
                        "index": acc.index,
                        "login": acc.login,
                        "ok": True,
                        "session": bool(acc.scraper.cookies.get("bb_session")),
                    }
                )
            except Exception as e:
                self._mark_account_error(acc, e)
                results.append(
                    {
                        "index": acc.index,
                        "login": acc.login,
                        "ok": False,
                        "error": str(e),
                        "session": bool(acc.scraper.cookies.get("bb_session")),
                    }
                )
        return {"accounts": results}

    def _prelogin_single_sync(self, account: RutrackerAccount) -> None:
        with self._account_context(account):
            self._ensure_login()

    def _download_sync(self, topic_id: int, forced_account: Optional[RutrackerAccount] = None) -> bytes:
        self._thread_ctx.account = forced_account or self._choose_account()
        acc = self._get_current_account()
        self._ensure_login()
        url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        r: Optional[requests.Response] = None
        last_net_err: Optional[Exception] = None
        for attempt in range(self.download_retries):
            try:
                with acc.lock:
                    r = acc.scraper.get(url, allow_redirects=True, timeout=self.download_timeout_sec)
                break
            except (requests.Timeout, requests.ConnectionError) as e:
                last_net_err = e
                if attempt < self.download_retries - 1:
                    time.sleep(self.download_retry_delay_sec)
                else:
                    raise
        if r is None:
            if last_net_err:
                raise last_net_err
            raise RuntimeError("RuTracker download failed before response")
        r.encoding = "cp1251"
        r.raise_for_status()
        if "application/x-bittorrent" in (r.headers.get("Content-Type") or ""):
            return r.content

        html = _normalized_text(r)
        path = os.path.join(self.dump_dir, f"download_error_{topic_id}_{int(time.time())}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        log.error("Download failed, dumped HTML to %s", path)

        if "исчерпали суточный лимит" in html.lower():
            raise HTTPException(status_code=429, detail="Daily download limit exceeded")

        doc = lxml.html.fromstring(html)
        hidden = {inp.get("name"): inp.get("value", "") for inp in doc.xpath(".//input[@type='hidden']") if
                  inp.get("name")}
        imgs = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not imgs:
            raise RuntimeError(f"Download parsing failed вЂ” dumped {path}")
        sid = hidden.get("cap_sid", uuid.uuid4().hex)
        img_url = urljoin(self.base_url + "/forum/", imgs[0])
        raise CaptchaRequired(sid, img_url)

    async def download(self, topic_id: int) -> bytes:
        import asyncio
        errors: List[Exception] = []
        for acc in self._ordered_accounts():
            try:
                payload = await asyncio.to_thread(self._download_sync, topic_id, acc)
                self._mark_account_success(acc)
                return payload
            except Exception as e:
                self._mark_account_error(acc, e)
                errors.append(e)
                log.warning("RuTracker download failed for account[%d]=%s topic=%s: %s", acc.index, acc.login, topic_id, e)
        if errors:
            raise errors[-1]
        raise RuntimeError("RuTracker download failed for all accounts")

    # -------- health/ops ----------
    def get_health_status(self) -> Dict[str, Any]:
        acc_states = [
            {
                "index": acc.index,
                "login": acc.login,
                "circuit_state": acc.cb.state.value,
                "failures": acc.cb.failure_count,
                "recent_errors": acc.recent_errors,
                "blocked_for_sec": max(0, int(acc.blocked_until - time.time())),
                "session": bool(acc.scraper.cookies.get("bb_session")),
            }
            for acc in self.accounts
        ]
        return {
            "accounts": acc_states,
            "time": datetime.utcnow().isoformat() + "Z",
        }

    def reset_circuit_breaker(self):
        for acc in self.accounts:
            acc.cb.failure_count = 0
            acc.cb.state = ServiceState.CLOSED
            acc.cb.last_failure_time = None

    def force_session_refresh(self):
        for acc in self.accounts:
            acc.scraper.cookies.clear()
            self.redis.delete(acc.cookie_key)
        log.info("All RuTracker sessions cleared; will re-login on next request")

    async def close(self):
        self._executor.shutdown(wait=True)

    def __del__(self):
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass


@lru_cache()
def get_rutracker_service() -> RutrackerService:
    return RutrackerService()





