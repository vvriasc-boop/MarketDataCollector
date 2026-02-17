import asyncio
import logging
import statistics
import time
from datetime import datetime, timezone

import config
from database.db import Database

logger = logging.getLogger(__name__)


async def run_stats_loop(db: Database, symbol_stats: dict[int, dict]) -> None:
    """Run daily at STATS_WORKER_HOUR_UTC. Updates symbol_stats table."""
    while True:
        now_utc = datetime.now(timezone.utc)
        target = now_utc.replace(
            hour=config.STATS_WORKER_HOUR_UTC, minute=0, second=0, microsecond=0
        )
        if now_utc >= target:
            target = target.replace(day=target.day + 1)
        wait_sec = (target - now_utc).total_seconds()
        logger.info("Stats worker: next run in %.0f sec", wait_sec)
        await asyncio.sleep(wait_sec)

        try:
            await compute_stats(db, symbol_stats)
        except Exception:
            logger.exception("Stats worker error")


async def compute_stats(db: Database, symbol_stats: dict[int, dict]) -> None:
    logger.info("Stats worker: computing symbol_stats...")
    all_symbols = await db.get_all_active_symbols()
    since = int(time.time()) - config.STATS_LOOKBACK_DAYS * 86400
    now = int(time.time())
    rows: list[tuple] = []

    for sym in all_symbols:
        sid = sym["id"]
        try:
            funding_data = await db.get_funding_data(sid, since)
            oi_changes = await db.get_oi_changes_1h(sid, since)
            ls_data = await db.get_ls_data(sid, since)
            taker_data = await db.get_taker_data(sid, since)
            avg_oi = await db.get_avg_oi_usd(sid, since)

            mean_f = _safe_mean(funding_data)
            std_f = _safe_stdev(funding_data)
            mean_oi_c = _safe_mean(oi_changes)
            std_oi_c = _safe_stdev(oi_changes)
            mean_ls = _safe_mean(ls_data)
            std_ls = _safe_stdev(ls_data)
            mean_tk = _safe_mean(taker_data)
            std_tk = _safe_stdev(taker_data)

            total_pts = len(funding_data) + len(oi_changes) + len(ls_data) + len(taker_data)
            if total_pts < config.STATS_MIN_POINTS:
                continue

            rows.append((
                sid, now,
                mean_f, std_f,
                mean_oi_c, std_oi_c,
                mean_ls, std_ls,
                mean_tk, std_tk,
                avg_oi or 0.0,
            ))
        except Exception:
            logger.exception("Stats error for symbol %d", sid)
            continue

    if rows:
        await db.save_symbol_stats(rows)

    # update in-memory cache
    fresh = await db.load_symbol_stats()
    symbol_stats.clear()
    symbol_stats.update(fresh)

    logger.info("Stats worker: updated %d symbols", len(rows))


def _safe_mean(data: list[float]) -> float | None:
    if not data:
        return None
    return statistics.mean(data)


def _safe_stdev(data: list[float]) -> float | None:
    if len(data) < 2:
        return None
    return statistics.stdev(data)
