# MarketDataCollector

Telegram-бот для сбора рыночных данных Binance Futures (OI, Funding, L/S, Taker Ratio) по всем парам.

## Структура

```
main.py              — запуск, cache hydration, background loops, graceful shutdown
config.py            — все настройки из .env + константы
database/db.py       — SQLite: схема, батчевые вставки, запросы, cache hydration
services/
  binance_api.py     — REST запросы к Binance Futures API, retry, прокси
  collector.py       — 5-мин цикл сбора + watchdog + дедупликация
  anomaly.py         — детекция аномалий (адаптивные пороги из symbol_stats)
  symbols.py         — обновление списка пар + is_hot фильтрация
  stats_worker.py    — пересчёт symbol_stats раз в сутки (Python statistics)
  notifier.py        — очередь Telegram с flood control + mass alert grouping
handlers/
  commands.py        — /start, /status, /stats, /anomalies, /top, /pair, /hot
  keyboards.py       — клавиатуры
```

## Запуск

```bash
pip install -r requirements.txt
# Заполнить .env: TELEGRAM_BOT_TOKEN, ADMIN_ID
python main.py
```

## Ключевые решения

- **Дедупликация**: кэш last_values в памяти, загружается при старте (cache hydration)
- **cycle_ts**: единый timestamp цикла = `time.time() // 300 * 300`
- **is_hot фильтр**: L/S и Taker только для пар с объёмом > $1M, OI — для всех
- **Адаптивные пороги**: symbol_stats пересчитывается раз в сутки в 04:00 UTC
- **SQLite WAL mode**: PRAGMA journal_mode=WAL, synchronous=NORMAL
- **Flood control**: asyncio.PriorityQueue, 0.5с между сообщениями, mass alert группировка
- **Прокси**: PROXY_URL в .env для обхода блокировки Binance из РФ

## Команды для разработки

```bash
# Проверка синтаксиса
python -m py_compile main.py
python -m py_compile config.py
python -m py_compile database/db.py

# Запуск
python main.py
```

## .env параметры

| Параметр | Описание | Default |
|---|---|---|
| TELEGRAM_BOT_TOKEN | Токен бота | — |
| ADMIN_ID | ID админа для алертов | — |
| PROXY_URL | socks5/http прокси | — |
| COLLECT_INTERVAL | Интервал сбора (сек) | 300 |
| HOT_VOLUME_THRESHOLD | Порог объёма для hot | 1000000 |
| MIN_HISTORY_FOR_ANOMALY | Мин. точек для аномалий | 12 |
| ALERT_COOLDOWN | Cooldown алерта (сек) | 3600 |
| MIN_ALERT_SEVERITY | Мин. severity для алерта | high |
| ARCHIVE_AFTER_DAYS | Архивация данных старше N дней | 30 |
