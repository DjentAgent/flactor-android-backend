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
async def test_search_all_track_skips_pb_when_rt_enough():
    rt = _FakeTracker(
        "rutracker",
        [
            TrackerSearchResult("R1", "u1", "1 MB", 100, 1, "rutracker"),
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
        pb_track_min_rt_results_to_skip=1,
    )
    results = await usecase.search_all("q", track="t")

    assert [r.title for r in results] == ["R1"]
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


@pytest.mark.asyncio
async def test_search_all_artist_only_merges_pb_even_with_rt_results():
    rt = _FakeTracker(
        "rutracker",
        [TrackerSearchResult("R1", "u1", "1 MB", 100, 1, "rutracker")],
    )
    pb = _SlowFakeTracker(
        [TrackerSearchResult("PB1", "up1", "1 MB", 150, 1, "piratebay")],
        delay_sec=0.01,
    )

    usecase = TorrentSearchUseCase(
        rutracker=rt,
        piratebay=pb,
        pb_soft_timeout_sec=2.5,
        pb_track_min_rt_results_to_skip=1,
    )
    results = await usecase.search_all("artist query")

    assert [r.title for r in results] == ["PB1", "R1"]
    assert pb.started is True
