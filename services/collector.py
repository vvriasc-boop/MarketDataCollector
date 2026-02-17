import asyncio
import logging
import time

import aiohttp

import config
from database.db import Database
from services import binance_api, symbols as symbols_svc
from services.anomaly import detect_anomalies
from services.notifier import Notifier

logger = logging.getLogger(__name__)


async def run_collector(
    session: aiohttp.ClientSession,
    db: Database,
    notifier: Notifier,
    last_values: dict[tuple, float],
    symbol_stats: dict[int, dict],
) -> None:
    """Main collection loop with watchdog."""
    while True:
        cycle_ts = int(time.time()) // config.COLLECT_INTERVAL * config.COLLECT_INTERVAL
        start = time.monotonic()
        try:
            await asyncio.wait_for(
                _five_min_loop(
                    session, db, notifier, last_values, symbol_stats, cycle_ts, start
                ),
                timeout=config.WATCHDOG_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("Cycle timed out (%ds)", config.WATCHDOG_TIMEOUT)
        except Exception:
            logger.exception("Cycle error")

        elapsed = time.monotonic() - start
        sleep_for = max(0, config.COLLECT_INTERVAL - elapsed)
        logger.info(
            "Cycle done in %.1fs, sleeping %.1fs", elapsed, sleep_for
        )
        await asyncio.sleep(sleep_for)


async def _five_min_loop(
    session: aiohttp.ClientSession,
    db: Database,
    notifier: Notifier,
    last_values: dict[tuple, float],
    symbol_stats: dict[int, dict],
    cycle_ts: int,
    cycle_start: float = 0.0,
) -> None:
    requests_ok = 0
    requests_fail = 0

    # refresh symbols if needed
    if symbols_svc.needs_refresh():
        await symbols_svc.refresh_symbols(session, db)

    sym_map = await db.get_symbol_map()
    hot_symbols = await db.get_hot_symbols()
    all_symbols = await db.get_all_active_symbols()
    hot_ids = {s["id"] for s in hot_symbols}

    # step 1: premium index (one request for all)
    premium_data = await binance_api.get_premium_index(session)
    mark_prices: dict[str, float] = {}
    funding_map: dict[str, tuple[float, int]] = {}

    if premium_data:
        requests_ok += 1
        for item in premium_data:
            sym = item.get("symbol", "")
            try:
                mark_prices[sym] = float(item.get("markPrice", 0))
            except (ValueError, TypeError):
                continue
            try:
                rate = float(item.get("lastFundingRate", 0))
                nft = int(item.get("nextFundingTime", 0))
                funding_map[sym] = (rate, nft)
            except (ValueError, TypeError):
                continue
    else:
        requests_fail += 1

    # prepare batches
    oi_rows: list[tuple] = []
    funding_rows: list[tuple] = []
    ls_rows: list[tuple] = []
    taker_rows: list[tuple] = []
    anomaly_rows: list[tuple] = []

    sem = asyncio.Semaphore(config.MAX_CONCURRENT)

    # compute top OI symbol IDs for severity
    top_oi_ids = _compute_top_oi_ids(symbol_stats)

    # funding — deduplicate and batch
    for sym_name, (rate, nft) in funding_map.items():
        sid = sym_map.get(sym_name)
        if sid is None:
            continue
        cached = last_values.get((sid, "funding"))
        if cached is not None and cached == rate:
            continue
        funding_rows.append((cycle_ts, sid, rate, nft))
        last_values[(sid, "funding")] = rate

    # step 2: collect OI + L/S + Taker in parallel
    tasks = []
    for sym in all_symbols:
        sid = sym["id"]
        sym_name = sym["symbol"]
        tasks.append(
            _collect_symbol(
                sem, session, cycle_ts, sid, sym_name,
                mark_prices, hot_ids, last_values,
            )
        )
        await asyncio.sleep(config.REQUEST_DELAY)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, Exception):
            requests_fail += 1
            logger.error("Symbol task error: %s", res)
            continue
        if res is None:
            continue
        r_ok, r_fail, oi_r, ls_r, tk_r = res
        requests_ok += r_ok
        requests_fail += r_fail
        if oi_r:
            oi_rows.append(oi_r)
        if ls_r:
            ls_rows.append(ls_r)
        if tk_r:
            taker_rows.append(tk_r)

    # batch insert
    await db.insert_open_interest(oi_rows)
    await db.insert_funding_rate(funding_rows)
    await db.insert_long_short_ratio(ls_rows)
    await db.insert_taker_ratio(taker_rows)

    # anomaly detection
    for sym in all_symbols:
        sid = sym["id"]
        sym_name = sym["symbol"]
        current_oi = None
        current_funding = None
        current_ls = None
        current_taker = None

        for r in oi_rows:
            if r[1] == sid:
                current_oi = r[3]  # oi_usd
                break
        current_funding = last_values.get((sid, "funding"))
        current_ls = last_values.get((sid, "ls"))
        current_taker = last_values.get((sid, "taker"))

        try:
            anom = await detect_anomalies(
                db, cycle_ts, sid, sym_name,
                current_oi, current_funding, current_ls, current_taker,
                symbol_stats, top_oi_ids,
            )
            anomaly_rows.extend(anom)
        except Exception:
            logger.exception("Anomaly detection error for %s", sym_name)

    if anomaly_rows:
        await db.insert_anomalies(anomaly_rows)
        for anom in anomaly_rows:
            sev = anom[4]
            atype = anom[3]
            # oi_flush always notified (severity >= medium)
            should_notify = (
                atype == "oi_flush"
                or config.SEVERITY_ORDER.get(sev, 0)
                >= config.SEVERITY_ORDER.get(config.MIN_ALERT_SEVERITY, 0)
            )
            if should_notify:
                sid = anom[2]
                sym_name = next(
                    (s["symbol"] for s in all_symbols if s["id"] == sid), "?"
                )
                text = (
                    anom[6] if atype == "oi_flush"
                    else _format_alert(sym_name, anom)
                )
                await notifier.send(
                    text, priority=sev, msg_type=atype,
                )

    # collector stats
    pairs_collected = len(oi_rows)
    elapsed = time.monotonic() - cycle_start if cycle_start else 0
    await db.insert_collector_stats((
        cycle_ts, elapsed,
        requests_ok, requests_fail, pairs_collected, len(anomaly_rows),
    ))

    logger.info(
        "Cycle %d: OI=%d, FR=%d, LS=%d, TK=%d, anomalies=%d",
        cycle_ts, len(oi_rows), len(funding_rows),
        len(ls_rows), len(taker_rows), len(anomaly_rows),
    )


async def _collect_symbol(
    sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    cycle_ts: int,
    sid: int,
    symbol: str,
    mark_prices: dict[str, float],
    hot_ids: set[int],
    last_values: dict[tuple, float],
) -> tuple[int, int, tuple | None, tuple | None, tuple | None]:
    ok = 0
    fail = 0
    oi_row = None
    ls_row = None
    tk_row = None

    async with sem:
        # OI for ALL symbols
        data = await binance_api.get_open_interest(session, symbol)
        if data:
            ok += 1
            try:
                oi_contracts = float(data.get("openInterest", 0))
                mp = mark_prices.get(symbol, 0)
                oi_usd = oi_contracts * mp

                cached = last_values.get((sid, "oi"))
                if cached is None or cached != oi_contracts:
                    oi_row = (cycle_ts, sid, oi_contracts, oi_usd, mp)
                    last_values[(sid, "oi")] = oi_contracts
            except (ValueError, TypeError):
                fail += 1
        else:
            fail += 1

        # L/S and Taker only for hot
        if sid in hot_ids:
            ls_data = await binance_api.get_long_short_ratio(session, symbol)
            if ls_data and len(ls_data) > 0:
                ok += 1
                try:
                    item = ls_data[0]
                    ratio = float(item.get("longShortRatio", 0))
                    long_pct = float(item.get("longAccount", 0))
                    short_pct = float(item.get("shortAccount", 0))

                    cached = last_values.get((sid, "ls"))
                    if cached is None or cached != ratio:
                        ls_row = (cycle_ts, sid, ratio, long_pct, short_pct)
                        last_values[(sid, "ls")] = ratio
                except (ValueError, TypeError):
                    fail += 1
            else:
                # 404 or empty — skip silently
                pass

            tk_data = await binance_api.get_taker_ratio(session, symbol)
            if tk_data and len(tk_data) > 0:
                ok += 1
                try:
                    item = tk_data[0]
                    bsr = float(item.get("buySellRatio", 0))
                    buy_vol = float(item.get("buyVol", 0))
                    sell_vol = float(item.get("sellVol", 0))

                    cached = last_values.get((sid, "taker"))
                    if cached is None or cached != bsr:
                        tk_row = (cycle_ts, sid, bsr, buy_vol, sell_vol)
                        last_values[(sid, "taker")] = bsr
                except (ValueError, TypeError):
                    fail += 1
            else:
                pass

    return ok, fail, oi_row, ls_row, tk_row


def _compute_top_oi_ids(symbol_stats: dict[int, dict]) -> list[int]:
    items = [
        (sid, s.get("avg_oi_usd", 0) or 0)
        for sid, s in symbol_stats.items()
    ]
    items.sort(key=lambda x: x[1], reverse=True)
    return [sid for sid, _ in items[: config.SEVERITY_TOP_N]]


def _format_alert(symbol: str, anom: tuple) -> str:
    _, _, _, atype, severity, value, desc = anom
    icons = {
        "critical": "\U0001f534",
        "high": "\U0001f7e0",
        "medium": "\U0001f7e1",
        "low": "\u26aa",
    }
    icon = icons.get(severity, "\u26aa")
    label = atype.upper().replace("_", " ")
    return f"{icon} <b>{label}: {symbol}</b>\n{desc}"
