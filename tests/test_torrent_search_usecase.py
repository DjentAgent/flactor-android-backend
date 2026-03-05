import asyncio
import time

import pytest

from spotiflac_backend.services.trackers.contracts import TrackerSearchResult
from spotiflac_backend.services.usecases.torrent_search import TorrentSearchUseCase


class _FakeTracker:
    def __init__(self, source: str, results: list[TrackerSearchResult]):
        self.source = source
        self._results = results

    async def search(self, query: str, only_lossless=None, track=None):
        return self._results


class _SlowFakeTracker:
    def __init__(self, results: list[TrackerSearchResult], delay_sec: float):
        self._results = results
        self._delay_sec = delay_sec
        self.started = False
        self.cancelled = False

    async def search(self, query: str, only_lossless=None, track=None):
        self.started = True
        try:
            await asyncio.sleep(self._delay_sec)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return self._results


@pytest.mark.asyncio
async def test_search_all_uses_pb_when_rutracker_empty():
    rt = _FakeTracker(
        "rutracker",
        [],
    )
    pb = _FakeTracker(
        "piratebay",
        [TrackerSearchResult("B", "u2", "2 MB", 50, 2, "piratebay")],
    )

    usecase = TorrentSearchUseCase(
        rutracker=rt,
        piratebay=pb,
        pb_soft_timeout_sec=2.5,
    )
    results = await usecase.search_all("q", only_lossless=True, track="t")

    assert [r.title for r in results] == ["B"]


@pytest.mark.asyncio
async def test_search_all_rt_first_skips_pb_when_enough_rt_results():
    rt = _FakeTracker(
        "rutracker",
        [
            TrackerSearchResult("R1", "u1", "1 MB", 100, 1, "rutracker"),
            TrackerSearchResult("R2", "u2", "1 MB", 90, 1, "rutracker"),
            TrackerSearchResult("R3", "u3", "1 MB", 80, 1, "rutracker"),
        ],
    )
    pb = _SlowFakeTracker(
        [TrackerSearchResult("PB", "up", "1 MB", 999, 1, "piratebay")],
        delay_sec=0.5,
    )

    usecase = TorrentSearchUseCase(
        rutracker=rt,
        piratebay=pb,
        pb_soft_timeout_sec=2.5,
    )
    results = await usecase.search_all("q")

    assert [r.title for r in results] == ["R1", "R2", "R3"]
    assert pb.cancelled is True or pb.started is False


@pytest.mark.asyncio
async def test_search_all_pb_soft_timeout_when_rt_insufficient():
    rt = _FakeTracker("rutracker", [])
    pb = _SlowFakeTracker(
        [TrackerSearchResult("PB", "up", "1 MB", 50, 1, "piratebay")],
        delay_sec=0.4,
    )

    usecase = TorrentSearchUseCase(
        rutracker=rt,
        piratebay=pb,
        pb_soft_timeout_sec=0.05,
    )
    started = time.perf_counter()
    results = await usecase.search_all("q")
    elapsed = time.perf_counter() - started

    assert results == []
    assert pb.cancelled is True
    assert elapsed < 0.3
