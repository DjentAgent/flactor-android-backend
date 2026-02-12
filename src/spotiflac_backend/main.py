"""
SpotiFlac Backend - Optimized FastAPI Application
"""

import os
# Глобально отключаем hiredis, чтобы не видеть _AsyncHiredisParser баги
os.environ.setdefault("REDIS_DISABLE_HIREDIS", "1")

import logging
import sys
import time
import asyncio
import socket
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

try:
    import uvloop  # type: ignore
    UVLOOP_AVAILABLE = True
except Exception:
    uvloop = None
    UVLOOP_AVAILABLE = False
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis, ConnectionPool
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from starlette.middleware.base import BaseHTTPMiddleware

from spotiflac_backend.core.config import settings
from spotiflac_backend.api.v1.health import router as health_router
from spotiflac_backend.api.v1.torrents import router as torrents_router
from spotiflac_backend.api.v1.spotify import router as spotify_router

# Импортируем сервисы для pre-warming
from spotiflac_backend.services.rutracker import get_rutracker_service
from spotiflac_backend.services.pirate_bay_service import get_piratebay_service
from spotiflac_backend.services.spotify_public import SpotifyPublicService
from spotiflac_backend.services.trackers.adapters import (
    PirateBaySearchAdapter,
    RuTrackerSearchAdapter,
)
from spotiflac_backend.services.usecases.torrent_search import TorrentSearchUseCase

# --- Кросс-версийный импорт PythonParser (если доступен — используем)
try:
    from redis.connection import PythonParser as RedisPythonParser
except Exception:
    try:
        from redis._parsers import PythonParser as RedisPythonParser
    except Exception:
        RedisPythonParser: Optional[type] = None


# ========================= Logging Configuration =========================

class ColoredFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    blue = "\x1b[34m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    COLORS = {
        logging.DEBUG: grey,
        logging.INFO: blue,
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.CRITICAL: bold_red
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelno, self.grey)
        record.levelname = f"{log_color}{record.levelname}{self.reset}"
        return super().format(record)


def setup_logging():
    log_level = getattr(settings, "log_level", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[]
    )

    console_handler = logging.StreamHandler(sys.stdout)
    if sys.stdout.isatty():
        console_handler.setFormatter(ColoredFormatter(
            "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
    else:
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)

    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger("spotiflac_backend").setLevel(logging.DEBUG)


# ========================= Middleware =========================

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        request_id = request.headers.get("X-Request-ID", str(time.time()))
        request.state.request_id = request_id
        try:
            response = await call_next(request)
            process_time = time.perf_counter() - start_time
            response.headers["X-Process-Time"] = f"{process_time:.3f}"
            response.headers["X-Request-ID"] = request_id
            if process_time > 1.0:
                logging.warning(
                    f"Slow request: {request.method} {request.url.path} took {process_time:.3f}s"
                )
            return response
        except Exception as e:
            process_time = time.perf_counter() - start_time
            logging.error(
                f"Request failed: {request.method} {request.url.path} after {process_time:.3f}s: {e}"
            )
            raise


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests: Dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        now = time.time()
        minute_ago = now - 60

        self.requests = {
            ip: [t for t in times if t > minute_ago]
            for ip, times in self.requests.items()
            if any(t > minute_ago for t in times)
        }

        ip_requests = self.requests.get(client_ip, [])
        recent_requests = [t for t in ip_requests if t > minute_ago]

        if len(recent_requests) >= self.requests_per_minute:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Too many requests. Please try again later."},
                headers={"Retry-After": "60"}
            )

        recent_requests.append(now)
        self.requests[client_ip] = recent_requests
        return await call_next(request)


class InMemoryPipeline:
    def __init__(self, store: "InMemoryRedis"):
        self.store = store
        self.ops: list[tuple[str, str, Any]] = []

    def setex(self, key: str, ttl: int, value: Any):
        self.ops.append(("setex", key, value))
        return self

    async def execute(self):
        for op, key, value in self.ops:
            if op == "setex":
                await self.store.setex(key, 0, value)
        self.ops.clear()
        return True


class InMemoryRedis:
    def __init__(self, decode_responses: bool):
        self.decode_responses = decode_responses
        self.data: Dict[str, Any] = {}

    async def ping(self):
        return True

    async def get(self, key: str):
        value = self.data.get(key)
        if value is None:
            return None
        if self.decode_responses and isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore")
        if not self.decode_responses and isinstance(value, str):
            return value.encode("utf-8")
        return value

    async def setex(self, key: str, ttl: int, value: Any):
        self.data[key] = value
        return True

    def pipeline(self):
        return InMemoryPipeline(self)

    async def close(self):
        return None


# ========================= Redis Manager =========================

class RedisManager:
    """Centralized Redis connection management with in-memory fallback."""
    _pool_bytes: ConnectionPool | None = None
    _pool_text: ConnectionPool | None = None
    _client: Redis | InMemoryRedis | None = None
    _cache_client: Redis | InMemoryRedis | None = None

    @classmethod
    async def initialize(cls):
        if cls._client is not None and cls._cache_client is not None:
            return cls._client, cls._cache_client

        if not getattr(settings, "redis_enabled", True):
            logging.warning("Redis disabled by config. Using in-memory backend.")
            cls._client = InMemoryRedis(decode_responses=False)
            cls._cache_client = InMemoryRedis(decode_responses=True)
            return cls._client, cls._cache_client

        try:
            attempts = max(1, int(getattr(settings, "redis_startup_retries", 8)))
            delay = float(getattr(settings, "redis_startup_retry_delay_sec", 1.0))
            last_error: Exception | None = None

            for attempt in range(1, attempts + 1):
                try:
                    base_kwargs: Dict[str, Any] = dict(
                        max_connections=getattr(settings, "redis_max_connections", 100),
                        socket_keepalive=True,
                        health_check_interval=30,
                        socket_timeout=5,
                        retry_on_timeout=True,
                    )
                    if RedisPythonParser is not None:
                        base_kwargs["parser_class"] = RedisPythonParser

                    cls._pool_bytes = ConnectionPool.from_url(settings.redis_url, **base_kwargs)
                    cls._pool_text = ConnectionPool.from_url(settings.redis_url, **base_kwargs)

                    cls._client = Redis(
                        connection_pool=cls._pool_bytes,
                        encoding="utf-8",
                        decode_responses=False,
                    )
                    cls._cache_client = Redis(
                        connection_pool=cls._pool_text,
                        encoding="utf-8",
                        decode_responses=True,
                    )
                    await cls._client.ping()
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    logging.warning(
                        "Redis connect attempt %s/%s failed: %s",
                        attempt,
                        attempts,
                        e,
                    )
                    if cls._pool_bytes:
                        await cls._pool_bytes.disconnect()
                    if cls._pool_text:
                        await cls._pool_text.disconnect()
                    cls._pool_bytes = None
                    cls._pool_text = None
                    cls._client = None
                    cls._cache_client = None
                    if attempt < attempts:
                        await asyncio.sleep(delay)

            if last_error is not None:
                raise last_error
        except Exception as e:
            logging.warning("Redis unavailable. Using in-memory backend: %s", e)
            cls._pool_bytes = None
            cls._pool_text = None
            cls._client = InMemoryRedis(decode_responses=False)
            cls._cache_client = InMemoryRedis(decode_responses=True)

        return cls._client, cls._cache_client

    @classmethod
    async def close(cls):
        if cls._client:
            await cls._client.close()
        if cls._cache_client:
            await cls._cache_client.close()
        if cls._pool_bytes:
            await cls._pool_bytes.disconnect()
        if cls._pool_text:
            await cls._pool_text.disconnect()
        cls._client = None
        cls._cache_client = None
        cls._pool_bytes = None
        cls._pool_text = None


# ========================= Lifespan Manager =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting SpotiFlac Backend...")
    try:
        logging.info("Initializing Redis connections...")
        redis_client, cache_client = await RedisManager.initialize()
        await redis_client.ping()
        logging.info("Redis backend initialized")

        if isinstance(cache_client, Redis):
            FastAPICache.init(
                RedisBackend(cache_client),
                prefix="spotiflac:cache:",
                expire=getattr(settings, "cache_ttl", 300),
            )
            logging.info("FastAPI Cache initialized")
            app.state.cache_enabled = True
        else:
            logging.warning("FastAPI Cache disabled (in-memory backend in use)")
            app.state.cache_enabled = False

        logging.info("Pre-warming services...")
        rt_service = get_rutracker_service()
        pb_service = get_piratebay_service()
        spotify_service = SpotifyPublicService()
        if bool(getattr(settings, "rutracker_prelogin_on_startup", True)):
            try:
                prelogin_result = await rt_service.prelogin_all_accounts()
                for item in prelogin_result.get("accounts", []):
                    if item.get("ok"):
                        logging.info(
                            "RuTracker prelogin ok account[%s] login=%s session=%s",
                            item.get("index"),
                            item.get("login"),
                            item.get("session"),
                        )
                    else:
                        logging.warning(
                            "RuTracker prelogin failed account[%s] login=%s error=%s session=%s",
                            item.get("index"),
                            item.get("login"),
                            item.get("error"),
                            item.get("session"),
                        )
            except Exception as e:
                logging.warning("RuTracker prelogin stage failed: %s", e)
        try:
            rt_health = rt_service.get_health_status()
            for acc in rt_health.get("accounts", []):
                logging.info(
                    "RuTracker account[%s] login=%s session=%s circuit=%s failures=%s recent_errors=%s blocked_for_sec=%s",
                    acc.get("index"),
                    acc.get("login"),
                    acc.get("session"),
                    acc.get("circuit_state"),
                    acc.get("failures"),
                    acc.get("recent_errors"),
                    acc.get("blocked_for_sec"),
                )
        except Exception as e:
            logging.warning("Failed to log RuTracker account startup status: %s", e)
        torrent_search_usecase = TorrentSearchUseCase(
            rutracker=RuTrackerSearchAdapter(
                rt_service,
                max_retries=int(getattr(settings, "rutracker_search_retries", 3)),
            ),
            piratebay=PirateBaySearchAdapter(pb_service),
        )
        logging.info("Services pre-warmed")

        app.state.redis_client = redis_client
        app.state.cache_client = cache_client
        app.state.rt_service = rt_service
        app.state.pb_service = pb_service
        app.state.spotify_service = spotify_service
        app.state.torrent_search_usecase = torrent_search_usecase

        logging.info("Application startup complete")
        yield

    except Exception as e:
        logging.error(f"Startup failed: {e}")
        raise

    finally:
        logging.info("Shutting down SpotiFlac Backend...")
        try:
            if hasattr(app.state, "rt_service"):
                await app.state.rt_service.close()
                logging.info("Closed RuTracker service")
            if hasattr(app.state, "pb_service"):
                await app.state.pb_service.close()
                logging.info("Closed PirateBay service")
            if hasattr(app.state, "spotify_service"):
                await app.state.spotify_service.close()
                logging.info("Closed Spotify service")

            await RedisManager.close()
            logging.info("Redis connections closed")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
        logging.info("Application shutdown complete")


# ========================= Application Factory =========================

def create_application() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title="SpotiFlac Backend",
        description="High-performance torrent search and download API",
        version="2.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
        default_response_class=JSONResponse,
        openapi_tags=[
            {"name": "health", "description": "Health check endpoints"},
            {"name": "torrents", "description": "Torrent search and download"},
            {"name": "spotify", "description": "Spotify integration"},
        ],
    )

    # CORS: нативный клиент, без credentials
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Process-Time", "X-Request-ID"],
        max_age=86400,
    )

    # Security: проверка Host
    if hasattr(settings, "allowed_hosts"):
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=settings.allowed_hosts
        )

    # Compression
    app.add_middleware(
        GZipMiddleware,
        minimum_size=1000,
        compresslevel=6,
    )

    # Rate limiting
    if getattr(settings, "rate_limit_enabled", True):
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_minute=getattr(settings, "rate_limit_rpm", 60),
        )

    # Timing (последний для измерения полного времени)
    app.add_middleware(TimingMiddleware)

    # Routers
    app.include_router(health_router, prefix="/health", tags=["health"])
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    app.include_router(torrents_router, prefix="/api/v1/torrents", tags=["torrents"])
    app.include_router(spotify_router, prefix="/api/v1/spotify", tags=["spotify"])

    # Exception Handlers
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": str(exc)}
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logging.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"}
        )

    # Root
    @app.get("/", include_in_schema=False)
    async def root():
        return {
            "name": "SpotiFlac Backend",
            "version": "2.0.0",
            "status": "running",
            "docs": "/api/docs"
        }

    return app


# ========================= Main =========================

if sys.platform != "win32" and UVLOOP_AVAILABLE:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = create_application()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=getattr(settings, "host", "0.0.0.0"),
        port=getattr(settings, "port", 8000),
        reload=getattr(settings, "debug", False),
        log_config=None,
        access_log=False,
        workers=1 if getattr(settings, "debug", False) else None,
        loop="uvloop" if (sys.platform != "win32" and UVLOOP_AVAILABLE) else "asyncio",
    )





