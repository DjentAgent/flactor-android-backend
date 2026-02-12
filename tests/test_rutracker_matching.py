from spotiflac_backend.services.rutracker import RutrackerService


def _svc() -> RutrackerService:
    # Use a raw instance for pure matching methods without network/redis init.
    return object.__new__(RutrackerService)


def test_track_match_does_not_accept_album_only():
    svc = _svc()
    files = [
        "Artist - Great Album (2024)/01 - Intro.flac",
        "Artist - Great Album (2024)/02 - Another Song.flac",
    ]
    hit, details = svc._track_in_files(files, "Target Song", ["Great Album"])
    assert hit is False
    assert details.get("reason") == "no_track_match"


def test_track_match_exact():
    svc = _svc()
    files = ["Artist - Great Album/03 - Target Song.flac"]
    hit, details = svc._track_in_files(files, "Target Song", ["Great Album"])
    assert hit is True
    assert details.get("match_quality") in {"exact", "alias"}


def test_track_match_compact_filename():
    svc = _svc()
    files = ["Artist - Great Album/03-TargetSong.flac"]
    hit, details = svc._track_in_files(files, "Target Song", ["Great Album"])
    assert hit is True
    assert details.get("match_quality") == "compact"

