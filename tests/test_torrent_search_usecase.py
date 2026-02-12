import pytest

from spotiflac_backend.services.trackers.contracts import TrackerSearchResult
from spotiflac_backend.services.usecases.torrent_search import TorrentSearchUseCase


class _FakeTracker:
    def __init__(self, source: str, results: list[TrackerSearchResult]):
        self.source = source
        self._results = results

    async def search(self, query: str, only_lossless=None, track=None):
        return self._results


@pytest.mark.asyncio
async def test_search_all_merges_and_sorts():
    rt = _FakeTracker(
        "rutracker",
        [TrackerSearchResult("A", "u1", "1 MB", 10, 1, "rutracker")],
    )
    pb = _FakeTracker(
        "piratebay",
        [TrackerSearchResult("B", "u2", "2 MB", 50, 2, "piratebay")],
    )

    usecase = TorrentSearchUseCase(rutracker=rt, piratebay=pb)
    results = await usecase.search_all("q", only_lossless=True, track="t")

    assert [r.title for r in results] == ["B", "A"]
