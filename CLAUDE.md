# MarketDataCollector

Telegram-бот для сбора рыночных данных Binance Futures (OI, Funding, L/S, Taker Ratio) по всем парам (~540 пар).

Stack: Python 3.10+ | asyncio | aiohttp | aiosqlite | aiogram 3.15

## Структура

```
main.py              — запуск, cache hydration, background loops, graceful shutdown
config.py            — все настройки из .env + константы
database/db.py       — SQLite: схема, батчевые вставки, запросы, cache hydration
services/
  binance_api.py     — REST запросы к Binance Futures API, retry, прокси
  collector.py       — 5-мин цикл сбора + watchdog + дедупликация
  anomaly.py         — детекция аномалий (адаптивные + фиксированные пороги, OI Flush)
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

# Или через systemd (автозапуск):
systemctl start market-data-collector
systemctl status market-data-collector
journalctl -u market-data-collector -f
```

## Ключевые решения

- **Дедупликация**: кэш last_values в памяти, загружается при старте через `db.load_last_values()` (cache hydration). Предотвращает дубликаты после рестарта
- **cycle_ts**: единый timestamp цикла = `time.time() // 300 * 300` — позволяет JOIN между таблицами по timestamp
- **is_hot фильтр**: L/S и Taker только для пар с quoteVolume > $1M, OI — для всех
- **Адаптивные пороги**: symbol_stats пересчитывается раз в сутки в 04:00 UTC. Статистика считается в Python (`statistics.mean()/stdev()`), т.к. SQLite не поддерживает STDDEV
- **Flood control**: asyncio.PriorityQueue, 0.5с между сообщениями, mass alert группировка (>5 одного типа за 60с)
- **OI Flush**: паттерн "накопление OI → резкий сброс" (buildup >3% дольше 1ч, drop >=2% от пика). Отдельный cooldown 30 мин, всегда уведомляет независимо от MIN_ALERT_SEVERITY
- **DB передаётся в handlers** через `commands.db = db` (module-level var), не через `bot["db"]`

## Gotchas

### Config
**[2026-02-17]** `int(os.getenv("KEY", "0"))` падает на пустой строке

- **Симптом:** `ValueError: invalid literal for int()` при `KEY=` в .env
- **Причина:** `os.getenv("KEY", "0")` возвращает `""` (пустая строка), а не `"0"`
- **Решение:** `int(os.getenv("KEY") or "0")` — `or` заменяет пустую строку на default
- **При повторении:** всегда использовать `or` вместо второго аргумента getenv для числовых значений

### API
**[2026-02-17]** aiogram 3.x Bot не поддерживает dict-like доступ

- **Симптом:** `TypeError: 'Bot' object does not support item assignment` на `bot["db"] = db`
- **Причина:** aiogram 3.x убрал dict mixin из Bot
- **Решение:** передавать через module-level переменную: `commands.db = db`
- **При повторении:** для shared state использовать module vars или middleware, не bot[]

### Build
**[2026-02-17]** aiogram 3.15 конфликтует с aiohttp >= 3.11

- **Симптом:** `ResolutionImpossible` при pip install
- **Причина:** aiogram 3.15 требует `aiohttp<3.11,>=3.9.0`
- **Решение:** не пинить aiohttp в requirements.txt, пусть pip resolve через aiogram
- **При повторении:** в requirements.txt указывать только aiogram, aiohttp подтянется как зависимость

## Anomaly Types

| Тип | Триггер | Cooldown |
|-----|---------|----------|
| funding_spike | \|funding\| > adaptive/fixed threshold | ALERT_COOLDOWN (1h) |
| oi_surge | \|OI change 1h\| > adaptive/10% | ALERT_COOLDOWN |
| ls_extreme | L/S ratio > adaptive/3.0 | ALERT_COOLDOWN |
| taker_extreme | Taker ratio > adaptive/2.0 | ALERT_COOLDOWN |
| combined_overheat | funding + OI + L/S одновременно | ALERT_COOLDOWN |
| combined_capitulation | OI drop + funding flip | ALERT_COOLDOWN |
| oi_flush | Buildup >3% (1ч+) → drop >=2% от пика | OI_FLUSH_COOLDOWN (30мин) |

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
| OI_BUILDUP_THRESHOLD | Мин. % роста OI для накопления | 3.0 |
| OI_BUILDUP_MIN_POINTS | Мин. точек подряд (×5мин) | 12 |
| OI_FLUSH_DROP_PCT | Мин. % падения от пика | 2.0 |
| OI_FLUSH_COOLDOWN | Cooldown OI Flush (сек) | 1800 |
| ARCHIVE_AFTER_DAYS | Архивация данных старше N дней | 30 |

## Systemd

Unit file: `/etc/systemd/system/market-data-collector.service`

```bash
systemctl restart market-data-collector  # перезапуск после изменений кода
systemctl stop market-data-collector     # остановка
journalctl -u market-data-collector -f   # логи
```
