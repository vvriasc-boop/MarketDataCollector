import time

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

import config
from database.db import Database
from handlers.keyboards import main_keyboard, top_keyboard

router = Router()

# Set from main.py after db init
db: Database | None = None


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    text = (
        "<b>Market Data Collector</b>\n\n"
        "Collects Open Interest, Funding Rate, Long/Short Ratio, "
        "Taker Ratio for all Binance Futures pairs.\n\n"
        "Commands:\n"
        "/status — current collection status\n"
        "/stats — daily/weekly stats\n"
        "/anomalies — last 20 anomalies\n"
        "/top — top 10 by metric\n"
        "/pair BTCUSDT — pair details\n"
        "/hot — hot filter status"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=main_keyboard())


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    hot, total = await db.get_symbol_count()
    last = await db.get_last_collector_stats()
    size = await db.get_db_size_mb()

    if last:
        ts = time.strftime("%H:%M:%S UTC", time.gmtime(last["timestamp"]))
        dur = last.get("cycle_duration_sec") or 0
        ok = last.get("requests_ok", 0)
        fail = last.get("requests_failed", 0)
        pairs = last.get("pairs_collected", 0)
        anom = last.get("anomalies_found", 0)
        text = (
            f"<b>Status</b>\n"
            f"Pairs: {hot} hot / {total} total\n"
            f"Last cycle: {ts}\n"
            f"Duration: {dur:.1f}s\n"
            f"Requests: {ok} ok / {fail} fail\n"
            f"Pairs collected: {pairs}\n"
            f"Anomalies: {anom}\n"
            f"DB size: {size:.1f} MB"
        )
    else:
        text = (
            f"<b>Status</b>\n"
            f"Pairs: {hot} hot / {total} total\n"
            f"No collection cycles yet\n"
            f"DB size: {size:.1f} MB"
        )
    await message.answer(text, parse_mode="HTML")


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    counts = await db.get_anomaly_counts_24h()
    total = sum(counts.values())
    lines = [f"<b>Stats (24h)</b>", f"Total anomalies: {total}"]
    for sev in ("critical", "high", "medium", "low"):
        cnt = counts.get(sev, 0)
        if cnt:
            lines.append(f"  {sev}: {cnt}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("anomalies"))
async def cmd_anomalies(message: Message) -> None:
    rows = await db.get_recent_anomalies(20)
    if not rows:
        await message.answer("No anomalies yet.")
        return

    lines = ["<b>Last 20 anomalies:</b>"]
    for r in rows:
        ts = time.strftime("%m-%d %H:%M", time.gmtime(r["timestamp"]))
        lines.append(
            f"{ts} [{r['severity']}] {r['symbol']} — {r['type']}: "
            f"{r.get('description', '')[:60]}"
        )
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("top"))
async def cmd_top(message: Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "Choose metric:", reply_markup=top_keyboard()
        )
        return

    metric = args[1].lower().strip()

    if metric == "oi":
        rows = await db.get_top_oi_change(10)
        lines = ["<b>TOP-10 OI change (1h):</b>"]
        for r in rows:
            lines.append(
                f"{r['symbol']}: {r.get('change_pct', 0):+.2f}% "
                f"(${r.get('current_oi', 0):,.0f})"
            )
    elif metric == "funding":
        rows = await db.get_top_funding(10)
        lines = ["<b>TOP-10 Funding Rate:</b>"]
        for r in rows:
            lines.append(f"{r['symbol']}: {r['rate']:.6f}")
    elif metric == "ls":
        rows = await db.get_top_ls(10)
        lines = ["<b>TOP-10 L/S Ratio:</b>"]
        for r in rows:
            long_pct = r.get("long_pct", 0) or 0
            lines.append(
                f"{r['symbol']}: {r['ratio']:.2f} ({long_pct*100:.0f}% long)"
            )
    else:
        await message.answer("Use: /top oi | /top funding | /top ls")
        return

    await message.answer("\n".join(lines) if lines else "No data.", parse_mode="HTML")


@router.callback_query(F.data.startswith("top_"))
async def cb_top(callback: CallbackQuery) -> None:
    metric = callback.data.replace("top_", "")

    if metric == "oi":
        rows = await db.get_top_oi_change(10)
        lines = ["<b>TOP-10 OI change (1h):</b>"]
        for r in rows:
            lines.append(
                f"{r['symbol']}: {r.get('change_pct', 0):+.2f}% "
                f"(${r.get('current_oi', 0):,.0f})"
            )
    elif metric == "funding":
        rows = await db.get_top_funding(10)
        lines = ["<b>TOP-10 Funding Rate:</b>"]
        for r in rows:
            lines.append(f"{r['symbol']}: {r['rate']:.6f}")
    elif metric == "ls":
        rows = await db.get_top_ls(10)
        lines = ["<b>TOP-10 L/S Ratio:</b>"]
        for r in rows:
            long_pct = r.get("long_pct", 0) or 0
            lines.append(
                f"{r['symbol']}: {r['ratio']:.2f} ({long_pct*100:.0f}% long)"
            )
    else:
        await callback.answer("Unknown metric")
        return

    await callback.message.edit_text(
        "\n".join(lines) if lines else "No data.", parse_mode="HTML"
    )
    await callback.answer()


@router.message(Command("pair"))
async def cmd_pair(message: Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Usage: /pair BTCUSDT")
        return

    symbol = args[1].upper().strip()
    data = await db.get_pair_data(symbol)
    if not data:
        await message.answer(f"Symbol {symbol} not found.")
        return

    lines = [f"<b>{data['symbol']}</b>"]

    oi = data.get("oi")
    if oi:
        oi_usd = oi.get("oi_usd", 0) or 0
        line = f"OI: ${oi_usd:,.0f}"
        for label, key in [("1h", "oi_1h"), ("24h", "oi_24h"), ("7d", "oi_7d")]:
            prev = data.get(key)
            if prev and prev > 0:
                pct = (oi_usd - prev) / prev * 100
                line += f" ({pct:+.1f}% {label})"
        lines.append(line)

    funding = data.get("funding")
    if funding is not None:
        avg_7d = data.get("funding_7d_avg")
        line = f"Funding: {funding:.6f}"
        if avg_7d is not None:
            line += f" (7d avg: {avg_7d:.6f})"
        lines.append(line)

    ls = data.get("ls")
    if ls:
        long_pct = (ls.get("long_pct", 0) or 0) * 100
        lines.append(f"L/S: {ls['ratio']:.2f} ({long_pct:.0f}% long)")

    taker = data.get("taker")
    if taker is not None:
        lines.append(f"Taker: {taker:.2f} buy/sell")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("hot"))
async def cmd_hot(message: Message) -> None:
    hot, total = await db.get_symbol_count()
    threshold = config.HOT_VOLUME_THRESHOLD
    await message.answer(
        f"<b>Hot Filter</b>\n"
        f"Hot pairs: {hot} / {total} total\n"
        f"Volume threshold: ${threshold:,.0f}",
        parse_mode="HTML",
    )
