import io
import logging
import re
import urllib.parse
from enum import Enum
from typing import List, Optional

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
    topic_id: str,
):
    pb_svc = get_piratebay_service()
    try:
        return await pb_svc.diagnose_download_by_id(topic_id)
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
        return await rt_svc.debug_search_probe(
            query=q,
            only_lossless=lossless,
            track=track,
            max_pages=pages,
            save_html=save_html,
            verify_track=verify_track,
            verify_top_n=verify_top_n,
        )
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
