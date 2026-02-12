import asyncio
import logging

from fastapi import HTTPException

from spotiflac_backend.services.pirate_bay_service import PirateBayService
from spotiflac_backend.services.rutracker import CaptchaRequired, RutrackerService
from spotiflac_backend.services.trackers.contracts import (
    TrackerSearchClient,
    TrackerSearchResult,
)

log = logging.getLogger(__name__)


class RuTrackerSearchAdapter(TrackerSearchClient):
    source = "rutracker"

    def __init__(self, service: RutrackerService, max_retries: int = 3):
        self.service = service
        self.max_retries = max_retries

    async def search(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        for attempt in range(self.max_retries + 1):
            try:
                results = await self.service.search(query, only_lossless=only_lossless, track=track)
                if results:
                    return [TrackerSearchResult.from_torrent(r, self.source) for r in results]
                if attempt < self.max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
            except CaptchaRequired:
                raise
            except Exception as e:
                if attempt == self.max_retries:
                    raise
                log.warning("RuTracker search attempt %s failed: %s", attempt + 1, e)
                await asyncio.sleep(0.5 * (attempt + 1))
        return []


class PirateBaySearchAdapter(TrackerSearchClient):
    source = "piratebay"

    def __init__(self, service: PirateBayService):
        self.service = service

    async def search(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        try:
            results = await self.service.search(query, only_lossless=only_lossless, track=track)
            return [TrackerSearchResult.from_torrent(r, self.source) for r in results]
        except HTTPException:
            raise
        except Exception as e:
            log.error("PirateBay search failed: %s", e)
            return []
