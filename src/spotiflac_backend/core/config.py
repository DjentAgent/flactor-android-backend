from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # Runtime
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    debug: bool = Field(default=False, alias="DEBUG")
    host: str = Field(default="0.0.0.0", alias="HOST")
    port: int = Field(default=8000, alias="PORT")
    workers: int = Field(default=1, alias="WORKERS")
    debug_html_dir: str = Field(default="debug_html", alias="DEBUG_HTML_DIR")

    # Rate limit / security
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")
    rate_limit_rpm: int = Field(default=60, alias="RATE_LIMIT_RPM")
    allowed_hosts: list[str] = Field(default_factory=lambda: ["*"], alias="ALLOWED_HOSTS")

    # Cache / redis
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    redis_max_connections: int = Field(default=100, alias="REDIS_MAX_CONNECTIONS")
    cache_ttl: int = Field(default=300, alias="CACHE_TTL")
    redis_enabled: bool = Field(default=True, alias="REDIS_ENABLED")
    redis_startup_retries: int = Field(default=8, alias="REDIS_STARTUP_RETRIES")
    redis_startup_retry_delay_sec: float = Field(default=1.0, alias="REDIS_STARTUP_RETRY_DELAY_SEC")

    # RuTracker
    rutracker_login: Optional[str] = Field(default=None, alias="RUTRACKER_LOGIN")
    rutracker_password: Optional[str] = Field(default=None, alias="RUTRACKER_PASSWORD")
    rutracker_accounts: Optional[str] = Field(default=None, alias="RUTRACKER_ACCOUNTS")
    rutracker_base: str = Field(default="https://rutracker.org", alias="RUTRACKER_BASE")
    rutracker_cookie_ttl: int = Field(default=86400, alias="RUTRACKER_COOKIE_TTL")
    rutracker_time_budget_sec: float = Field(default=6.0, alias="RUTRACKER_TIME_BUDGET_SEC")
    rutracker_want_results: int = Field(default=3, alias="RUTRACKER_WANT_RESULTS")
    rutracker_search_max_pages_wide: int = Field(default=3, alias="RUTRACKER_SEARCH_MAX_PAGES_WIDE")
    rutracker_max_candidates_per_pass: int = Field(default=25, alias="RUTRACKER_MAX_CANDIDATES_PER_PASS")
    rutracker_filelist_ttl: int = Field(default=24 * 3600, alias="RUTRACKER_FILELIST_TTL")
    rutracker_search_ttl: int = Field(default=10 * 60, alias="RUTRACKER_SEARCH_TTL")
    rutracker_artist_results_limit: int = Field(default=50, alias="RUTRACKER_ARTIST_RESULTS_LIMIT")
    rutracker_artist_search_ttl: int = Field(default=15 * 60, alias="RUTRACKER_ARTIST_SEARCH_TTL")
    rutracker_track_search_ttl: int = Field(default=10 * 60, alias="RUTRACKER_TRACK_SEARCH_TTL")
    rutracker_artist_time_budget_sec: float = Field(default=10.0, alias="RUTRACKER_ARTIST_TIME_BUDGET_SEC")
    rutracker_track_time_budget_sec: float = Field(default=6.0, alias="RUTRACKER_TRACK_TIME_BUDGET_SEC")
    rutracker_artist_phase1_pages: int = Field(default=2, alias="RUTRACKER_ARTIST_PHASE1_PAGES")
    rutracker_artist_phase1_results: int = Field(default=20, alias="RUTRACKER_ARTIST_PHASE1_RESULTS")
    rutracker_artist_max_candidates_per_pass: int = Field(
        default=80, alias="RUTRACKER_ARTIST_MAX_CANDIDATES_PER_PASS"
    )
    rutracker_track_max_candidates_per_pass: int = Field(
        default=25, alias="RUTRACKER_TRACK_MAX_CANDIDATES_PER_PASS"
    )
    rutracker_track_search_max_pages_wide: int = Field(default=3, alias="RUTRACKER_TRACK_SEARCH_MAX_PAGES_WIDE")
    rutracker_track_search_max_pages_narrow: int = Field(default=10, alias="RUTRACKER_TRACK_SEARCH_MAX_PAGES_NARROW")
    rutracker_track_relaxed_reserve_ratio: float = Field(default=0.4, alias="RUTRACKER_TRACK_RELAXED_RESERVE_RATIO")
    rutracker_track_strict_album_limit_nohit: int = Field(default=1, alias="RUTRACKER_TRACK_STRICT_ALBUM_LIMIT_NOHIT")
    rutracker_track_force_relaxed_filematch_grace_sec: float = Field(
        default=1.5, alias="RUTRACKER_TRACK_FORCE_RELAXED_FILEMATCH_GRACE_SEC"
    )
    rutracker_track_force_relaxed_filematch_candidates: int = Field(
        default=12, alias="RUTRACKER_TRACK_FORCE_RELAXED_FILEMATCH_CANDIDATES"
    )
    rutracker_track_presence_ttl_hit_sec: int = Field(default=7 * 24 * 3600, alias="RUTRACKER_TRACK_PRESENCE_TTL_HIT_SEC")
    rutracker_track_presence_ttl_miss_sec: int = Field(default=4 * 3600, alias="RUTRACKER_TRACK_PRESENCE_TTL_MISS_SEC")
    rutracker_cookie_keepalive_enabled: bool = Field(default=True, alias="RUTRACKER_COOKIE_KEEPALIVE_ENABLED")
    rutracker_cookie_keepalive_interval_sec: float = Field(
        default=600.0, alias="RUTRACKER_COOKIE_KEEPALIVE_INTERVAL_SEC"
    )
    rutracker_prelogin_on_startup: bool = Field(default=True, alias="RUTRACKER_PRELOGIN_ON_STARTUP")

    # Spotify
    spotify_client_id: Optional[str] = Field(default=None, alias="SPOTIFY_CLIENT_ID")
    spotify_client_secret: Optional[str] = Field(default=None, alias="SPOTIFY_CLIENT_SECRET")
    spotify_market: str = Field(default="US", alias="SPOTIFY_MARKET")
    spotify_timeout: float = Field(default=8.0, alias="SPOTIFY_TIMEOUT")
    spotify_cache_ttl: int = Field(default=120, alias="SPOTIFY_CACHE_TTL")
    spotify_http_proxy: Optional[str] = Field(default=None, alias="SPOTIFY_HTTP_PROXY")

    # PirateBay
    piratebay_track_filecheck_top_n: int = Field(default=12, alias="PIRATEBAY_TRACK_FILECHECK_TOP_N")
    piratebay_track_verify_timeout_sec: float = Field(default=3.0, alias="PIRATEBAY_TRACK_VERIFY_TIMEOUT_SEC")
    piratebay_min_seeders: int = Field(default=1, alias="PIRATEBAY_MIN_SEEDERS")
    piratebay_extra_torrent_mirrors: Optional[str] = Field(default=None, alias="PIRATEBAY_EXTRA_TORRENT_MIRRORS")


settings = Settings()
