from spotiflac_backend.services.rutracker import RutrackerService
from spotiflac_backend.models.torrent import TorrentInfo

SAMPLE_HTML = """
<table id="tor-tbl">
  <tr data-topic_id="12345">
    <td class="f-name-col"><a href="tracker.php?f=123">Rock</a></td>
    <td class="t-title-col"><a href="viewtopic.php?t=12345">Test Album FLAC</a></td>
    <td class="tor-size"><a>55.2 MB</a></td>
    <td><b class="seedmed">123</b></td>
    <td class="leechmed">45</td>
    <td><a href="dl.php?t=12345">DL</a></td>
  </tr>
</table>
"""


def test_parse_search_rows():
    svc = object.__new__(RutrackerService)
    svc.base_url = "https://rutracker.example"

    rows, total = svc._parse_search_rows(SAMPLE_HTML)

    assert total == 0
    assert len(rows) == 1

    info, tid, forum_id, forum_txt, title_txt = rows[0]
    assert isinstance(info, TorrentInfo)
    assert tid == 12345
    assert forum_id == 123
    assert forum_txt == "Rock"
    assert title_txt == "Test Album FLAC"
    assert info.url == "https://rutracker.example/forum/dl.php?t=12345"
    assert info.size == "55.2 MB"
    assert info.seeders == 123
    assert info.leechers == 45
