import asyncio
import logging

import aiohttp

import config

logger = logging.getLogger(__name__)


async def _request(
    session: aiohttp.ClientSession,
    url: str,
    params: dict | None = None,
) -> dict | list | None:
    """GET with exponential back-off on retryable errors."""
    wait = 1.0
    for attempt in range(5):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 403:
                    logger.warning(
                        "403 Forbidden for %s — set PROXY_URL in .env", url
                    )
                    return None
                if resp.status == 404:
                    return None
                if resp.status in config.RETRY_CODES:
                    retry_after = float(
                        resp.headers.get("Retry-After", wait)
                    )
                    logger.warning(
                        "HTTP %d for %s, retry in %.1fs",
                        resp.status, url, retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    wait = min(wait * 2, config.RETRY_MAX_WAIT)
                    continue
                logger.error("HTTP %d for %s", resp.status, url)
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Request error %s: %s, retry %d", url, exc, attempt)
            await asyncio.sleep(wait)
            wait = min(wait * 2, config.RETRY_MAX_WAIT)
    logger.error("Max retries for %s", url)
    return None


async def get_exchange_info(
    session: aiohttp.ClientSession,
) -> list[dict] | None:
    """Return list of active USDT-M futures symbols."""
    base = config.BINANCE_FAPI_BASE
    data = await _request(session, f"{base}/fapi/v1/exchangeInfo")
    if data is None:
        return None
    symbols = []
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            if s.get("quoteAsset") == "USDT":
                symbols.append(
                    {"symbol": s["symbol"], "baseAsset": s.get("baseAsset", "")}
                )
    return symbols


async def get_ticker_24h(
    session: aiohttp.ClientSession,
) -> dict[str, float] | None:
    """Return {symbol: quoteVolume_24h}."""
    base = config.BINANCE_FAPI_BASE
    data = await _request(session, f"{base}/fapi/v1/ticker/24hr")
    if data is None:
        return None
    result = {}
    for item in data:
        sym = item.get("symbol", "")
        try:
            result[sym] = float(item.get("quoteVolume", 0))
        except (ValueError, TypeError):
            continue
    return result


async def get_premium_index(
    session: aiohttp.ClientSession,
) -> list[dict] | None:
    """One request for all symbols: markPrice, lastFundingRate, nextFundingTime."""
    base = config.BINANCE_FAPI_BASE
    return await _request(session, f"{base}/fapi/v1/premiumIndex")


async def get_open_interest(
    session: aiohttp.ClientSession, symbol: str
) -> dict | None:
    base = config.BINANCE_FAPI_BASE
    return await _request(
        session, f"{base}/fapi/v1/openInterest", {"symbol": symbol}
    )


async def get_long_short_ratio(
    session: aiohttp.ClientSession, symbol: str, period: str = "5m"
) -> list[dict] | None:
    base = config.BINANCE_FAPI_BASE
    data = await _request(
        session,
        f"{base}/futures/data/topLongShortPositionRatio",
        {"symbol": symbol, "period": period, "limit": 1},
    )
    if isinstance(data, list):
        return data
    return None


async def get_taker_ratio(
    session: aiohttp.ClientSession, symbol: str, period: str = "5m"
) -> list[dict] | None:
    base = config.BINANCE_FAPI_BASE
    data = await _request(
        session,
        f"{base}/futures/data/takerlongshortRatio",
        {"symbol": symbol, "period": period, "limit": 1},
    )
    if isinstance(data, list):
        return data
    return None
