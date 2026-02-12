from dataclasses import dataclass
from typing import Protocol

from spotiflac_backend.models.torrent import TorrentInfo


@dataclass
class TrackerSearchResult:
    title: str
    url: str
    size: str
    seeders: int
    leechers: int
    source: str

    @classmethod
    def from_torrent(cls, torrent: TorrentInfo, source: str) -> "TrackerSearchResult":
        return cls(
            title=torrent.title,
            url=torrent.url,
            size=torrent.size,
            seeders=torrent.seeders,
            leechers=torrent.leechers,
            source=source,
        )


class TrackerSearchClient(Protocol):
    source: str

    async def search(
        self,
        query: str,
        only_lossless: bool | None = None,
        track: str | None = None,
    ) -> list[TrackerSearchResult]:
        ...
