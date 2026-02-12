# SpotiFlac Backend

Серверный сервис на FastAPI для поиска музыкальных торрент-релизов и выдачи `.torrent` файлов клиентскому приложению.

## Кратко о проекте

- Агрегация поиска из нескольких источников (`RuTracker`, `PirateBay`) через единый API.
- Специализированный алгоритм поиска для RuTracker без публичного API.
- Приоритет: точность поиска конкретного трека, устойчивость при сетевой нестабильности и прогнозируемое время ответа.

## Задачи, которые решает сервис

### 1. Работа с RuTracker без API

- Авторизация через веб-сценарий входа (`forum/login.php`) с парсингом скрытых полей формы и `form_token`.
- Проверка успешного входа по cookie `bb_session`.
- Обработка CAPTCHA сценариев и возврат `HTTP 428` с `session_id` и `captcha_image`.
- Поисковые запросы и скачивание выполняются через HTTP с разбором HTML ответов.

### 2. Управление сессиями и мультиаккаунтинг

- Поддержка пула аккаунтов (`RUTRACKER_ACCOUNTS`).
- Для каждого аккаунта хранится изолированное состояние:
- HTTP сессия
- набор cookies
- блокировка
- ограничитель сбоев
- метрики состояния
- Cookie сессии сохраняются в Redis (`rutracker:cookiejar:*`) и восстанавливаются при старте.
- Предварительный вход для всех аккаунтов при запуске сервиса.
- Поддержание активности сессий через периодический запрос к `forum/tracker.php`.
- При редиректе на `login.php` сессия инвалидируется и выполняется повторная авторизация.
- Запросы распределяются между аккаунтами с учетом состояния, при ошибке выполняется переключение на следующий аккаунт.

### 3. Точность поиска трека

- Два режима поиска:
- режим по артисту
- режим по треку
- В режиме по треку реализован фазный алгоритм:
- `strict`
- `relaxed_release`
- `relaxed_filematch`
- `relaxed_lossless`
- `artist_fallback`
- Наличие трека подтверждается по `filelist` релиза, а не только по заголовку темы.
- Кандидаты ранжируются по качеству совпадения, формату и количеству сидов.

### 4. Производительность и устойчивость

- Кэширование поиска, `filelist` и результатов проверки трека.
- Раздельные TTL для `track presence hit` и `track presence miss`.
- Ограничение времени выполнения фаз и числа кандидатов на фазу.
- Повторные попытки запросов и ограничитель сбоев для внешних вызовов.
- Резервный режим хранения в памяти при недоступности Redis.

### 5. Диагностика и наблюдаемость

- Диагностический эндпоинт: `GET /api/v1/torrents/debug/rutracker/search`.
- Трассировка фаз алгоритма, счетчиков фильтрации, статистики проверок `filelist` и итоговых кандидатов.
- Опциональные HTML дампы для анализа изменений верстки источника.

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

- `src/spotiflac_backend/api/v1/` — REST эндпоинты
- `src/spotiflac_backend/services/rutracker.py` — основной алгоритм RuTracker
- `src/spotiflac_backend/services/pirate_bay_service.py` — сервис PirateBay
- `src/spotiflac_backend/services/usecases/torrent_search.py` — координация поиска и объединение результатов
- `src/spotiflac_backend/services/trackers/` — контракты и адаптеры
- `src/spotiflac_backend/core/config.py` — конфигурация времени выполнения

## Стек

- Python 3.10+
- FastAPI, Uvicorn
- requests, aiohttp, cloudscraper
- lxml, BeautifulSoup
- Redis
- Pydantic v2
- pytest, pytest async

## Запуск

```bash
poetry install
poetry run uvicorn spotiflac_backend.main:app --host 0.0.0.0 --port 8000
```

или

```bash
docker compose up --build
```
