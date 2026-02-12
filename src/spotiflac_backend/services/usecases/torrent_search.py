import asyncio

from spotiflac_backend.services.rutracker import CaptchaRequired
from spotiflac_backend.services.trackers.contracts import (
    TrackerSearchClient,
    TrackerSearchResult,
)


class TorrentSearchUseCase:
    def __init__(
        self,
        rutracker: TrackerSearchClient,
        piratebay: TrackerSearchClient,
    ):
        self.rutracker = rutracker
        self.piratebay = piratebay

    async def search_all(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        rt_task = asyncio.create_task(
            self.rutracker.search(query, only_lossless=only_lossless, track=track)
        )
        pb_task = asyncio.create_task(
            self.piratebay.search(query, only_lossless=only_lossless, track=track)
        )
        rt_results, pb_results = await asyncio.gather(rt_task, pb_task, return_exceptions=True)

        if isinstance(rt_results, CaptchaRequired):
            raise rt_results
        if isinstance(rt_results, Exception):
            rt_results = []
        if isinstance(pb_results, Exception):
            pb_results = []

        merged = list(rt_results) + list(pb_results)
        merged.sort(key=lambda x: x.seeders, reverse=True)
        return merged

    async def search_piratebay_only(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        results = await self.piratebay.search(query, only_lossless=only_lossless, track=track)
        results.sort(key=lambda x: x.seeders, reverse=True)
        return results
