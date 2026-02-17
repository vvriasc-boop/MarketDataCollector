import logging
import time

import aiohttp

import config
from database.db import Database
from services import binance_api

logger = logging.getLogger(__name__)

_last_refresh: float = 0.0


async def refresh_symbols(
    session: aiohttp.ClientSession, db: Database
) -> dict[str, int]:
    """Fetch exchange info + ticker, update symbols & hot status. Return symbol map."""
    global _last_refresh

    info = await binance_api.get_exchange_info(session)
    if info is None:
        logger.error("Failed to fetch exchangeInfo")
        return await db.get_symbol_map()

    sym_map = await db.upsert_symbols(info)
    logger.info("Symbols updated: %d active pairs", len(sym_map))

    ticker = await binance_api.get_ticker_24h(session)
    if ticker:
        hot_map: dict[str, tuple[bool, float]] = {}
        for sym_name in sym_map:
            vol = ticker.get(sym_name, 0.0)
            is_hot = vol > config.HOT_VOLUME_THRESHOLD
            hot_map[sym_name] = (is_hot, vol)
        await db.update_hot_status(hot_map)
        hot_count = sum(1 for v in hot_map.values() if v[0])
        logger.info("Hot filter: %d / %d pairs", hot_count, len(hot_map))

    _last_refresh = time.time()
    return sym_map


def needs_refresh() -> bool:
    return (time.time() - _last_refresh) >= config.SYMBOLS_REFRESH_INTERVAL
