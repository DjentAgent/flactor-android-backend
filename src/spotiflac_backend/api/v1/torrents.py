import io
import logging
import re
import time
import urllib.parse
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.pirate_bay_service import get_piratebay_service
from spotiflac_backend.services.rutracker import CaptchaRequired, get_rutracker_service
from spotiflac_backend.services.trackers.adapters import (
    PirateBaySearchAdapter,
    RuTrackerSearchAdapter,
)
from spotiflac_backend.services.usecases.torrent_search import TorrentSearchUseCase

log = logging.getLogger(__name__)
router = APIRouter(prefix="")
CAPTCHA_REQUIRED = 428
DEBUG_RUTRACKER_ENDPOINT_VERSION = "rt-debug-http-2026-02-24-03"
DEBUG_PIRATEBAY_SEARCH_ENDPOINT_VERSION = "pb-debug-http-2026-02-24-02"
DEBUG_UNIFIED_SEARCH_ENDPOINT_VERSION = "search-debug-http-2026-02-24-02"


class TorrentInfoResponse(BaseModel):
    title: str
    url: str
    size: str
    seeders: int
    leechers: int
    source: Optional[str] = None


class CaptchaInitResponse(BaseModel):
    session_id: str
    captcha_image: str


class CaptchaCompleteRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=100)
    solution: str = Field(..., min_length=1, max_length=20)


class TorrentSource(str, Enum):
    RUTRACKER = "rutracker"
    PIRATEBAY = "piratebay"


def get_torrent_search_usecase(request: Request) -> TorrentSearchUseCase:
    uc = getattr(request.app.state, "torrent_search_usecase", None)
    if uc is not None:
        return uc
    rt_svc = get_rutracker_service()
    pb_svc = get_piratebay_service()
    return TorrentSearchUseCase(
        rutracker=RuTrackerSearchAdapter(
            rt_svc,
            max_retries=int(getattr(settings, "rutracker_search_retries", 3)),
        ),
        piratebay=PirateBaySearchAdapter(pb_svc),
    )


def is_info_hash(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-fA-F]{40}", value))


def normalize_topic_id(topic_id: str) -> str:
    if is_info_hash(topic_id):
        return topic_id.upper()
    return topic_id.strip()


def extract_topic_id(url: str) -> Optional[str]:
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        return params.get("t", [None])[0]
    except Exception:
        return None


def make_cache_key(source: TorrentSource, topic_id: str) -> str:
    normalized_id = normalize_topic_id(topic_id)
    if source == TorrentSource.PIRATEBAY:
        if is_info_hash(normalized_id):
            return f"torrent:pb:hash:{normalized_id}"
        return f"torrent:pb:id:{normalized_id}"
    return f"torrent:rt:{normalized_id}"


def make_source_key(topic_id: str) -> str:
    return f"torrent:source:{normalize_topic_id(topic_id)}"


async def cache_search_results_text(
    redis_text,
    results: List[TorrentInfoResponse],
    ttl: int = 24 * 3600,
) -> None:
    if not results:
        return
    pipe = redis_text.pipeline()
    for r in results:
        topic_id = extract_topic_id(r.url)
        if topic_id:
            pipe.setex(make_source_key(topic_id), ttl, (r.source or TorrentSource.PIRATEBAY))
    try:
        await pipe.execute()
    except Exception as e:
        log.warning("Failed to cache search results: %s", e)


async def download_from_piratebay(pb_svc, topic_id: str, redis_text, strict: bool = False) -> Optional[bytes]:
    try:
        data = await (pb_svc.download_by_hash(topic_id) if is_info_hash(topic_id) else pb_svc.download_by_id(topic_id))
        await redis_text.setex(make_source_key(topic_id), 24 * 3600, TorrentSource.PIRATEBAY)
        return data
    except HTTPException as e:
        if strict:
            raise
        if e.status_code in (404, 422):
            return None
        raise
    except Exception as e:
        if strict:
            raise HTTPException(status_code=502, detail=f"PirateBay error: {str(e)}")
        log.warning("PirateBay download failed for %s: %s", topic_id, e)
        return None


async def download_from_rutracker(rt_svc, topic_id: str, redis_text, strict: bool = False) -> Optional[bytes]:
    if not topic_id.isdigit():
        if strict:
            raise HTTPException(status_code=400, detail="Invalid RuTracker ID format")
        return None
    try:
        data = await rt_svc.download(int(topic_id))
        await redis_text.setex(make_source_key(topic_id), 24 * 3600, TorrentSource.RUTRACKER)
        return data
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    except Exception as e:
        if strict:
            raise HTTPException(status_code=502, detail=f"RuTracker error: {str(e)}")
        log.warning("RuTracker download failed for %s: %s", topic_id, e)
        return None


async def get_cached_torrent_bytes(redis_bytes, source: Optional[TorrentSource], topic_id: str) -> Optional[bytes]:
    if not source:
        return None
    try:
        return await redis_bytes.get(make_cache_key(source, topic_id))
    except Exception as e:
        log.warning("Cache read failed for %s: %s", topic_id, e)
        return None


async def get_cached_torrent_bytes_any(redis_bytes, topic_id: str) -> tuple[Optional[TorrentSource], Optional[bytes]]:
    for source in (TorrentSource.RUTRACKER, TorrentSource.PIRATEBAY):
        data = await get_cached_torrent_bytes(redis_bytes, source, topic_id)
        if data:
            return source, data
    return None, None


async def cache_torrent_bytes(
    redis_bytes,
    source: TorrentSource,
    topic_id: str,
    data: bytes,
    ttl: int = 7 * 24 * 3600,
) -> None:
    try:
        await redis_bytes.setex(make_cache_key(source, topic_id), ttl, data)
    except Exception as e:
        log.warning("Cache write failed for %s: %s", topic_id, e)


async def detect_source_text(redis_text, topic_id: str) -> Optional[TorrentSource]:
    try:
        value = await redis_text.get(make_source_key(topic_id))
        if value:
            source_str = str(value).lower()
            if source_str == TorrentSource.RUTRACKER:
                return TorrentSource.RUTRACKER
            if source_str == TorrentSource.PIRATEBAY:
                return TorrentSource.PIRATEBAY
    except Exception as e:
        log.warning("Failed to detect source for %s: %s", topic_id, e)
    return None


@router.post(
    "/login/initiate",
    response_model=CaptchaInitResponse,
    responses={CAPTCHA_REQUIRED: {"model": CaptchaInitResponse}},
    summary="Инициировать процесс логина с CAPTCHA",
)
async def login_initiate(request: Request):
    rt_svc = get_rutracker_service()
    try:
        rt_svc.initiate_login()
        return {"session_id": "", "captcha_image": ""}
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )


@router.post("/login/complete", summary="Завершить логин с решением CAPTCHA")
async def login_complete(body: CaptchaCompleteRequest):
    rt_svc = get_rutracker_service()
    try:
        rt_svc.complete_login(body.session_id, body.solution)
        return {"status": "ok", "message": "Login successful"}
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("Login completion error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/search",
    response_model=List[TorrentInfoResponse],
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
    summary="Поиск торрентов",
)
async def search_torrents(
    request: Request,
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
):
    redis_text = request.app.state.cache_client
    usecase = get_torrent_search_usecase(request)
    try:
        all_results = await usecase.search_all(q, only_lossless=lossless, track=track)
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    except Exception as e:
        log.error("Search failed: %s", e)
        raise HTTPException(status_code=502, detail=str(e))

    result_models = [
        TorrentInfoResponse(
            title=r.title,
            url=r.url,
            size=r.size,
            seeders=r.seeders,
            leechers=r.leechers,
            source=r.source,
        )
        for r in all_results
    ]
    if result_models:
        await cache_search_results_text(redis_text, result_models)
    return result_models


@router.get(
    "/search/piratebay",
    response_model=List[TorrentInfoResponse],
    summary="Поиск только в PirateBay",
)
async def search_piratebay(
    request: Request,
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
):
    redis_text = request.app.state.cache_client
    usecase = get_torrent_search_usecase(request)
    try:
        usecase_results = await usecase.search_piratebay_only(q, only_lossless=lossless, track=track)
        result_models = [
            TorrentInfoResponse(
                title=r.title,
                url=r.url,
                size=r.size,
                seeders=r.seeders,
                leechers=r.leechers,
                source=r.source,
            )
            for r in usecase_results
        ]
        if result_models:
            await cache_search_results_text(redis_text, result_models)
        return result_models
    except HTTPException:
        raise
    except Exception as e:
        log.error("PirateBay search error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@router.get(
    "/debug/piratebay/{topic_id}",
    summary="Debug PirateBay download pipeline",
)
async def debug_piratebay_download(
    topic_id: int,
):
    pb_svc = get_piratebay_service()
    try:
        return await pb_svc.diagnose_download_by_id(str(topic_id))
    except HTTPException:
        raise
    except Exception as e:
        log.error("PirateBay debug error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@router.get(
    "/debug/rutracker/search",
    summary="Debug RuTracker search parsing pipeline",
)
async def debug_rutracker_search(
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
    pages: int = Query(2, ge=1, le=20, description="How many pages to inspect"),
    save_html: bool = Query(True, description="Dump raw HTML pages into debug_html dir"),
    verify_track: bool = Query(False, description="Verify track hit by checking filelists"),
    verify_top_n: int = Query(5, ge=1, le=100, description="How many top torrents to verify for track"),
):
    rt_svc = get_rutracker_service()
    try:
        rt_payload = await rt_svc.debug_search_probe(
            query=q,
            only_lossless=lossless,
            track=track,
            max_pages=pages,
            save_html=save_html,
            verify_track=verify_track,
            verify_top_n=verify_top_n,
        )
        rt_payload["endpoint_version"] = DEBUG_RUTRACKER_ENDPOINT_VERSION
        return rt_payload
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error("RuTracker debug search error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


def _as_search_item(
    *,
    title: str,
    url: str,
    size: str,
    seeders: int,
    leechers: int,
    source: str,
) -> Dict[str, Any]:
    return {
        "title": title or "",
        "url": url or "",
        "size": size or "",
        "seeders": int(seeders or 0),
        "leechers": int(leechers or 0),
        "source": source,
    }


async def _run_piratebay_search_probe(
    pb_svc,
    *,
    query: str,
    only_lossless: Optional[bool],
    track: Optional[str],
    enabled: bool = True,
) -> tuple[list, Dict[str, Any]]:
    pb_probe: Dict[str, Any] = {
        "enabled": bool(enabled),
        "ok": False,
        "duration_ms": 0,
        "error": "",
        "items_count": 0,
        "items_sample": [],
    }
    if not enabled:
        return [], pb_probe

    pb_started = time.time()
    try:
        pb_items = await pb_svc.search(query, only_lossless=only_lossless, track=track)
        pb_probe.update(
            {
                "ok": True,
                "duration_ms": int((time.time() - pb_started) * 1000),
                "items_count": len(pb_items),
                "items_sample": [
                    _as_search_item(
                        title=x.title,
                        url=x.url,
                        size=x.size,
                        seeders=x.seeders,
                        leechers=x.leechers,
                        source="piratebay",
                    )
                    for x in pb_items[: min(8, len(pb_items))]
                ],
            }
        )
        return pb_items, pb_probe
    except HTTPException as e:
        pb_probe.update(
            {
                "ok": False,
                "duration_ms": int((time.time() - pb_started) * 1000),
                "error": f"{e.status_code}: {e.detail}",
            }
        )
        return [], pb_probe
    except Exception as e:
        pb_probe.update(
            {
                "ok": False,
                "duration_ms": int((time.time() - pb_started) * 1000),
                "error": str(e),
            }
        )
        return [], pb_probe


@router.get(
    "/debug/search/piratebay",
    summary="Debug PirateBay search pipeline",
)
async def debug_piratebay_search(
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
    limit: int = Query(50, ge=1, le=200, description="Max PB items to include in response"),
):
    pb_svc = get_piratebay_service()
    started = time.time()
    try:
        pb_items, pb_probe = await _run_piratebay_search_probe(
            pb_svc,
            query=q,
            only_lossless=lossless,
            track=track,
            enabled=True,
        )
        pb_only_items = [
            _as_search_item(
                title=x.title,
                url=x.url,
                size=x.size,
                seeders=x.seeders,
                leechers=x.leechers,
                source="piratebay",
            )
            for x in pb_items
        ]
        pb_only_items.sort(key=lambda x: x["seeders"], reverse=True)
        pb_only_items = pb_only_items[: max(1, int(limit))]
        return {
            "endpoint_version": DEBUG_PIRATEBAY_SEARCH_ENDPOINT_VERSION,
            "query": q,
            "only_lossless": lossless,
            "track": track,
            "mode": "piratebay_only",
            "duration_ms": int((time.time() - started) * 1000),
            "limit": int(limit),
            "search_like_final_count": len(pb_only_items),
            "search_like_source_counts": {"rutracker": 0, "piratebay": len(pb_only_items)},
            "search_like_final_items": pb_only_items,
            "piratebay_probe": pb_probe,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("PirateBay debug search error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@router.get(
    "/debug/search",
    summary="Unified debug search (RuTracker + PirateBay, merged like /search)",
)
async def debug_unified_search(
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
    pages: int = Query(2, ge=1, le=20, description="How many RuTracker pages to inspect"),
    save_html: bool = Query(False, description="Dump RuTracker raw HTML pages into debug_html dir"),
    verify_track: bool = Query(False, description="Verify RuTracker track hit by checking filelists"),
    verify_top_n: int = Query(5, ge=1, le=100, description="How many top RuTracker torrents to verify"),
    include_pb: bool = Query(True, description="Include PirateBay in merged debug output"),
    merged_limit: int = Query(50, ge=1, le=200, description="Max merged items to include in response"),
):
    rt_svc = get_rutracker_service()
    pb_svc = get_piratebay_service()
    started = time.time()
    try:
        rt_started = time.time()
        rt_payload = await rt_svc.debug_search_probe(
            query=q,
            only_lossless=lossless,
            track=track,
            max_pages=pages,
            save_html=save_html,
            verify_track=verify_track,
            verify_top_n=verify_top_n,
        )
        rt_payload["endpoint_version"] = DEBUG_RUTRACKER_ENDPOINT_VERSION
        rt_probe = {
            "ok": True,
            "duration_ms": int((time.time() - rt_started) * 1000),
            "error": "",
            "pipeline_final_count": int(rt_payload.get("pipeline_final_count") or 0),
        }

        pb_items, pb_probe = await _run_piratebay_search_probe(
            pb_svc,
            query=q,
            only_lossless=lossless,
            track=track,
            enabled=include_pb,
        )

        rt_items = [
            _as_search_item(
                title=x.get("title", ""),
                url=x.get("url", ""),
                size=x.get("size", ""),
                seeders=int(x.get("seeders", 0) or 0),
                leechers=int(x.get("leechers", 0) or 0),
                source="rutracker",
            )
            for x in (rt_payload.get("pipeline_final_items") or [])
        ]
        merged_items = list(rt_items)
        merged_items.extend(
            _as_search_item(
                title=x.title,
                url=x.url,
                size=x.size,
                seeders=x.seeders,
                leechers=x.leechers,
                source="piratebay",
            )
            for x in pb_items
        )
        merged_items.sort(key=lambda x: x["seeders"], reverse=True)
        merged_items = merged_items[: max(1, int(merged_limit))]

        source_counts = {"rutracker": 0, "piratebay": 0}
        for x in merged_items:
            src = str(x.get("source") or "")
            if src in source_counts:
                source_counts[src] += 1

        return {
            "endpoint_version": DEBUG_UNIFIED_SEARCH_ENDPOINT_VERSION,
            "query": q,
            "only_lossless": lossless,
            "track": track,
            "mode": "rutracker+piratebay" if include_pb else "rutracker_only",
            "combined_duration_ms": int((time.time() - started) * 1000),
            "merged_limit": int(merged_limit),
            "search_like_final_count": len(merged_items),
            "search_like_source_counts": source_counts,
            "search_like_final_items": merged_items,
            "rutracker_probe": rt_probe,
            "piratebay_probe": pb_probe,
            "rutracker_debug": rt_payload,
        }
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error("Unified debug search error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@router.get(
    "/download/{topic_id}",
    response_class=StreamingResponse,
    responses={
        CAPTCHA_REQUIRED: {"description": "Captcha required"},
        404: {"description": "Torrent not found"},
        502: {"description": "Tracker error"},
    },
    summary="Скачать торрент файл",
)
async def download_torrent(
    request: Request,
    topic_id: str,
    tracker: Optional[TorrentSource] = Query(
        None,
        description="Source tracker. If omitted, auto-detected.",
    ),
):
    redis_bytes = request.app.state.redis_client
    redis_text = request.app.state.cache_client
    topic_id = normalize_topic_id(topic_id)

    source = tracker or await detect_source_text(redis_text, topic_id)
    if source:
        cached = await get_cached_torrent_bytes(redis_bytes, source, topic_id)
        if cached:
            return StreamingResponse(
                io.BytesIO(cached),
                media_type="application/x-bittorrent",
                headers={
                    "Content-Disposition": f'attachment; filename="{topic_id}.torrent"',
                    "Cache-Control": "public, max-age=3600",
                },
            )
    else:
        guessed_source, cached_any = await get_cached_torrent_bytes_any(redis_bytes, topic_id)
        if cached_any:
            if guessed_source:
                await redis_text.setex(make_source_key(topic_id), 24 * 3600, guessed_source.value)
            return StreamingResponse(
                io.BytesIO(cached_any),
                media_type="application/x-bittorrent",
                headers={
                    "Content-Disposition": f'attachment; filename="{topic_id}.torrent"',
                    "Cache-Control": "public, max-age=3600",
                },
            )

    rt_svc = get_rutracker_service()
    pb_svc = get_piratebay_service()

    data: Optional[bytes] = None
    actual_source: Optional[TorrentSource] = None
    if source == TorrentSource.RUTRACKER:
        data = await download_from_rutracker(rt_svc, topic_id, redis_text, strict=True)
        actual_source = TorrentSource.RUTRACKER
    elif source == TorrentSource.PIRATEBAY:
        data = await download_from_piratebay(pb_svc, topic_id, redis_text, strict=True)
        actual_source = TorrentSource.PIRATEBAY
    else:
        # Source is unknown: prefer RuTracker first for numeric topic IDs, PB first for info-hash IDs.
        if is_info_hash(topic_id):
            data = await download_from_piratebay(pb_svc, topic_id, redis_text, strict=False)
            if data:
                actual_source = TorrentSource.PIRATEBAY
            else:
                data = await download_from_rutracker(rt_svc, topic_id, redis_text, strict=False)
                if data:
                    actual_source = TorrentSource.RUTRACKER
        else:
            data = await download_from_rutracker(rt_svc, topic_id, redis_text, strict=False)
            if data:
                actual_source = TorrentSource.RUTRACKER
            else:
                data = await download_from_piratebay(pb_svc, topic_id, redis_text, strict=False)
                if data:
                    actual_source = TorrentSource.PIRATEBAY

    if not data:
        raise HTTPException(status_code=404, detail="Torrent not found")

    if actual_source:
        await cache_torrent_bytes(redis_bytes, actual_source, topic_id, data)

    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/x-bittorrent",
        headers={
            "Content-Disposition": f'attachment; filename="{topic_id}.torrent"',
            "Cache-Control": "public, max-age=3600",
        },
    )
