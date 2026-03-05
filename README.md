 # FlacTor Backend

  FlacTor Backend — FastAPI-сервис, который агрегирует поиск музыкальных torrent-раздач, работает с RuTracker через веб-
  интерфейс (HTML + cookies + сессии) и предоставляет стабильный API для поиска и загрузки `.torrent`.

  ## Назначение

  Сервис решает три ключевые задачи:
  - единый API поверх нескольких источников (`RuTracker`, `PirateBay`);
  - точный поиск конкретного трека (а не только релиза);
  - устойчивость к нестабильности внешних сайтов и сессий.

  ## Что реализовано

  ### 1) Интеграция с RuTracker
  - Web-login через `forum/login.php` с разбором скрытых полей и `form_token`.
  - Валидация успешной авторизации по `bb_session`.
  - Обработка CAPTCHA-флоу с возвратом `HTTP 428` (`session_id`, `captcha_image`).
  - Поиск и скачивание через HTTP + HTML parsing.

  ### 2) Сессии и мультиаккаунтинг
  - Пул аккаунтов через `RUTRACKER_ACCOUNTS`.
  - Изолированное состояние на аккаунт: HTTP session, cookies, lock, fail limiter, health-метрики.
  - Сохранение cookie jar в Redis (`rutracker:cookiejar:*`) и восстановление при старте.
  - Pre-login аккаунтов при запуске.
  - Keep-alive сессий и auto-relogin при инвалидировании.
  - Переключение между аккаунтами при ошибках.

  ### 3) Точность поиска треков
  - Два сценария: поиск по артисту и поиск по треку.
  - Многоэтапный алгоритм поиска трека: `strict`, `relaxed_release`, `relaxed_filematch`, `relaxed_lossless`,
  `artist_fallback`.
  - Подтверждение трека по `filelist` релиза, а не только по заголовку темы.
  - Ранжирование кандидатов по релевантности, формату и сидерам.

  ### 4) Производительность и отказоустойчивость
  - Кэш поиска, `filelist` и проверки присутствия трека.
  - Раздельные TTL для `presence hit` и `presence miss`.
  - Ограничение времени фаз и числа кандидатов.
  - Retry + fail limiter для внешних запросов.
  - Fallback на in-memory режим при недоступности Redis.

  ### 5) Диагностика и наблюдаемость
  - Диагностический endpoint: `GET /api/v1/torrents/debug/rutracker/search`.
  - Трассировка фаз, фильтрации, проверок `filelist`, финального ранжирования.
  - Опциональные HTML-дампы для анализа изменений верстки источника.

  ## API

  - `GET /api/v1/torrents/search`
  - `GET /api/v1/torrents/search/piratebay`
  - `GET /api/v1/torrents/download/{topic_id}`
  - `POST /api/v1/torrents/login/initiate`
  - `POST /api/v1/torrents/login/complete`
  - `GET /api/v1/torrents/debug/rutracker/search`
  - `GET /api/v1/spotify/search`
  - `GET /api/v1/spotify/tracks/{track_id}`
  - `GET /api/v1/health`

  ## Архитектура

  - `src/spotiflac_backend/api/v1/` — REST endpoints.
  - `src/spotiflac_backend/services/rutracker.py` — основной алгоритм RuTracker.
  - `src/spotiflac_backend/services/pirate_bay_service.py` — интеграция PirateBay.
  - `src/spotiflac_backend/services/usecases/torrent_search.py` — координация поиска и merge результатов.
  - `src/spotiflac_backend/services/trackers/` — tracker contracts и adapters.
  - `src/spotiflac_backend/core/config.py` — runtime-конфигурация.

  ## Стек

  - Python 3.10+
  - FastAPI, Uvicorn
  - requests, aiohttp, cloudscraper
  - lxml, BeautifulSoup
  - Redis
  - Pydantic v2
  - pytest

  ## Локальный запуск

  ```bash
  poetry install
  poetry run uvicorn spotiflac_backend.main:app --host 0.0.0.0 --port 8000

  или через Docker:

  docker compose up --build

  ## Связанный клиент

  Android-клиент: https://github.com/DjentAgent/flactor-android

  ## Disclaimer

  Проект предоставлен в образовательных и исследовательских целях. Пользователь несет ответственность за соблюдение
  законодательства своей юрисдикции.