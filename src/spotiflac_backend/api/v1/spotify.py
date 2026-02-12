import logging

from fastapi import APIRouter, HTTPException, Query, Request

from spotiflac_backend.models.spotify import (
    SearchTracksPageDto,
    SearchTracksResponse,
    TrackDetailDto,
)
from spotiflac_backend.services.spotify_public import SpotifyPublicService

log = logging.getLogger(__name__)
router = APIRouter(prefix="")


def _resolve_spotify_service(request: Request) -> tuple[SpotifyPublicService, bool]:
    svc = getattr(request.app.state, "spotify_service", None)
    if svc is not None:
        return svc, False
    return SpotifyPublicService(), True


@router.get(
    "/search",
    response_model=SearchTracksResponse,
    summary="Public Spotify search (no user login)",
)
async def public_search(
    request: Request,
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
):
    svc, owned = _resolve_spotify_service(request)
    try:
        data = await svc.search_tracks(q=q, limit=limit, offset=offset)
        return SearchTracksResponse(tracksPage=SearchTracksPageDto(**data["tracksPage"]))
    except Exception as e:
        log.exception("Spotify public search failed")
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        if owned:
            await svc.close()


@router.get(
    "/tracks/{track_id}",
    response_model=TrackDetailDto,
    summary="Public Spotify track detail (no user login)",
)
async def public_track_detail(request: Request, track_id: str):
    svc, owned = _resolve_spotify_service(request)
    try:
        data = await svc.get_track_detail(track_id)
        return TrackDetailDto(**data)
    except Exception as e:
        log.exception("Spotify public track detail failed")
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        if owned:
            await svc.close()
