import asyncio
from contextlib import suppress

from spotiflac_backend.core.config import settings
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
        pb_soft_timeout_sec: float | None = None,
        pb_track_min_rt_results_to_skip: int | None = None,
    ):
        self.rutracker = rutracker
        self.piratebay = piratebay
        self.pb_soft_timeout_sec = float(
            pb_soft_timeout_sec
            if pb_soft_timeout_sec is not None
            else getattr(settings, "search_pb_soft_timeout_sec", 2.5)
        )
        self.pb_track_min_rt_results_to_skip = max(
            0,
            int(
                pb_track_min_rt_results_to_skip
                if pb_track_min_rt_results_to_skip is not None
                else getattr(settings, "search_pb_track_min_rt_results_to_skip", 1)
            ),
        )

    async def search_all(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        is_track_query = bool((track or "").strip())
        pb_task = asyncio.create_task(
            self.piratebay.search(query, only_lossless=only_lossless, track=track)
        )

        try:
            rt_results = await self.rutracker.search(query, only_lossless=only_lossless, track=track)
        except CaptchaRequired:
            await self._cancel_task(pb_task)
            raise
        except Exception:
            rt_results = []

        # Policy:
        # - artist-only queries: PB is always best-effort (adds breadth).
        # - track queries: PB only when RT result count is low.
        if not is_track_query:
            pb_results = await self._wait_pb_best_effort(pb_task)
        elif len(rt_results) >= self.pb_track_min_rt_results_to_skip:
            await self._cancel_task(pb_task)
            pb_results = []
        else:
            pb_results = await self._wait_pb_best_effort(pb_task)

        merged = list(rt_results) + list(pb_results)
        merged.sort(key=lambda x: x.seeders, reverse=True)
        return merged

    async def _wait_pb_best_effort(self, pb_task: asyncio.Task) -> list[TrackerSearchResult]:
        try:
            timeout = max(0.0, float(self.pb_soft_timeout_sec))
            if timeout == 0.0:
                pb_results = await pb_task
            else:
                pb_results = await asyncio.wait_for(pb_task, timeout=timeout)
            if isinstance(pb_results, Exception):
                return []
            return list(pb_results)
        except asyncio.TimeoutError:
            await self._cancel_task(pb_task)
            return []
        except Exception:
            return []

    async def _cancel_task(self, task: asyncio.Task) -> None:
        if task.done():
            with suppress(asyncio.CancelledError, Exception):
                await task
            return
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task

    async def search_piratebay_only(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        results = await self.piratebay.search(query, only_lossless=only_lossless, track=track)
        results.sort(key=lambda x: x.seeders, reverse=True)
        return results
