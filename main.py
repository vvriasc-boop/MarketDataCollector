import asyncio
import csv
import gzip
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import aiohttp
from aiogram import Bot, Dispatcher

import config
from database.db import Database
from handlers import commands
from handlers.commands import router
from services.collector import run_collector
from services.notifier import Notifier
from services.stats_worker import run_stats_loop
from services import symbols as symbols_svc

logging.basicConfig(
    level=logging.INFO,
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


async def _daily_summary(db: Database, notifier: Notifier) -> None:
    while True:
        now_utc = datetime.now(timezone.utc)
        target = now_utc.replace(
            hour=config.DAILY_SUMMARY_HOUR_UTC, minute=0, second=0, microsecond=0
        )
        if now_utc >= target:
            target = target.replace(day=target.day + 1)
        await asyncio.sleep((target - now_utc).total_seconds())

        try:
            lines = ["<b>Daily Summary</b>"]
            top_f = await db.get_daily_top_funding(10)
            if top_f:
                lines.append("\n<b>TOP Funding:</b>")
                for r in top_f:
                    lines.append(f"  {r['symbol']}: {r['rate']:.6f}")
            top_oi = await db.get_daily_top_oi_change(10)
            if top_oi:
                lines.append("\n<b>TOP OI change:</b>")
                for r in top_oi:
                    lines.append(f"  {r['symbol']}: {r.get('pct', 0):.1f}%")
            top_ls = await db.get_daily_top_ls(10)
            if top_ls:
                lines.append("\n<b>TOP L/S ratio:</b>")
                for r in top_ls:
                    lines.append(f"  {r['symbol']}: {r.get('max_ratio', 0):.2f}")
            counts = await db.get_anomaly_counts_24h()
            if counts:
                lines.append("\n<b>Anomalies 24h:</b>")
                for sev in ("critical", "high", "medium", "low"):
                    cnt = counts.get(sev, 0)
                    if cnt:
                        lines.append(f"  {sev}: {cnt}")
            await notifier.send("\n".join(lines), priority="medium")
        except Exception:
            logger.exception("Daily summary error")


async def _archive_loop(db: Database) -> None:
    while True:
        now_utc = datetime.now(timezone.utc)
        if now_utc.day == 1 and now_utc.hour == 3:
            await _run_archive(db)
        await asyncio.sleep(3600)


async def _run_archive(db: Database) -> None:
    before_ts = int(time.time()) - config.ARCHIVE_AFTER_DAYS * 86400
    tables = ["open_interest", "funding_rate", "long_short_ratio", "taker_ratio"]
    os.makedirs("archives", exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y_%m")
    total_rows = 0

    for table in tables:
        rows = await db.get_archive_rows(table, before_ts)
        if not rows:
            continue
        path = f"archives/{table}_{stamp}.csv.gz"
        with gzip.open(path, "wt", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        deleted = await db.delete_old_rows(table, before_ts)
        total_rows += deleted
        logger.info("Archived %s: %d rows -> %s", table, deleted, path)

    if total_rows:
        await db.vacuum()
        size = await db.get_db_size_mb()
        logger.info("Archive done: %d rows, DB now %.1f MB", total_rows, size)


def _create_session() -> aiohttp.ClientSession:
    if config.PROXY_URL:
        from aiohttp_socks import ProxyConnector
        connector = ProxyConnector.from_url(config.PROXY_URL)
        logger.info("Using proxy: %s", config.PROXY_URL[:20] + "...")
    else:
        connector = aiohttp.TCPConnector(limit=20)
    return aiohttp.ClientSession(connector=connector)


async def main() -> None:
    if not config.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set in .env")
        sys.exit(1)

    # 1. Database
    db = Database()
    await db.connect()

    # 2. Cache hydration
    last_values = await db.load_last_values()

    # 3. Symbol stats
    symbol_stats = await db.load_symbol_stats()

    # 4. aiohttp session
    session = _create_session()

    # 5. Initial symbol load
    await symbols_svc.refresh_symbols(session, db)

    # 6. Bot + notifier
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    commands.db = db
    dp = Dispatcher()
    dp.include_router(router)
    notifier = Notifier(bot)
    await notifier.start()

    # 7. Background tasks
    tasks = [
        asyncio.create_task(run_collector(session, db, notifier, last_values, symbol_stats)),
        asyncio.create_task(run_stats_loop(db, symbol_stats)),
        asyncio.create_task(_daily_summary(db, notifier)),
        asyncio.create_task(_archive_loop(db)),
    ]

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    # Start polling in background
    polling_task = asyncio.create_task(dp.start_polling(bot))

    await shutdown_event.wait()

    # Cleanup
    logger.info("Shutting down...")
    for t in tasks:
        t.cancel()
    polling_task.cancel()
    await notifier.stop()
    await session.close()
    await db.close()
    await bot.session.close()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
