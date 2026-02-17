import logging
import time

import config
from database.db import Database

logger = logging.getLogger(__name__)

# In-memory cooldown: {(symbol_id, anomaly_type): last_alert_ts}
_alert_cooldowns: dict[tuple[int, str], float] = {}


def _severity_for_symbol(
    symbol_id: int,
    symbol_stats: dict[int, dict],
    top_oi_ids: list[int],
) -> str:
    stats = symbol_stats.get(symbol_id)
    if stats is None:
        return "medium"
    avg_oi = stats.get("avg_oi_usd") or 0
    if avg_oi > config.SEVERITY_CRITICAL_OI:
        return "critical"
    if symbol_id in top_oi_ids:
        return "high"
    if avg_oi > config.SEVERITY_MEDIUM_OI:
        return "medium"
    return "low"


def _check_cooldown(symbol_id: int, atype: str) -> bool:
    key = (symbol_id, atype)
    last = _alert_cooldowns.get(key, 0)
    cooldown = (
        config.OI_FLUSH_COOLDOWN if atype == "oi_flush"
        else config.ALERT_COOLDOWN
    )
    return (time.time() - last) >= cooldown


def _set_cooldown(symbol_id: int, atype: str) -> None:
    _alert_cooldowns[(symbol_id, atype)] = time.time()


async def detect_anomalies(
    db: Database,
    cycle_ts: int,
    symbol_id: int,
    symbol_name: str,
    current_oi_usd: float | None,
    current_funding: float | None,
    current_ls_ratio: float | None,
    current_taker_ratio: float | None,
    symbol_stats: dict[int, dict],
    top_oi_ids: list[int],
) -> list[tuple]:
    """Return list of anomaly tuples for batch insert."""
    count = await db.count_recent_oi(symbol_id)
    if count < config.MIN_HISTORY_FOR_ANOMALY:
        return []

    now = int(time.time())
    severity = _severity_for_symbol(symbol_id, symbol_stats, top_oi_ids)
    stats = symbol_stats.get(symbol_id)
    anomalies: list[tuple] = []

    has_funding_spike = False
    has_oi_surge = False
    has_ls_extreme = False

    # 1. Funding spike
    if current_funding is not None:
        if stats and stats.get("std_funding"):
            threshold = abs(stats["mean_funding"]) + 3 * stats["std_funding"]
        else:
            threshold = config.FUNDING_SPIKE_THRESHOLD

        if abs(current_funding) > threshold:
            has_funding_spike = True
            if _check_cooldown(symbol_id, "funding_spike"):
                desc = (
                    f"Funding {current_funding:.6f} "
                    f"(threshold {threshold:.6f})"
                )
                anomalies.append((
                    now, cycle_ts, symbol_id, "funding_spike",
                    severity, current_funding, desc,
                ))
                _set_cooldown(symbol_id, "funding_spike")

    # 2. OI surge
    if current_oi_usd is not None and current_oi_usd > 0:
        prev_oi = await db.get_oi_hour_ago(symbol_id, cycle_ts - 3600)
        if prev_oi and prev_oi > 0:
            change = (current_oi_usd - prev_oi) / prev_oi

            if stats and stats.get("std_oi_change_1h"):
                threshold = abs(stats["mean_oi_change_1h"]) + 3 * stats["std_oi_change_1h"]
            else:
                threshold = config.OI_SURGE_THRESHOLD

            if abs(change) > threshold:
                has_oi_surge = True
                direction = "surge" if change > 0 else "drop"
                if _check_cooldown(symbol_id, "oi_surge"):
                    desc = (
                        f"OI {direction} {change:+.2%} "
                        f"(${current_oi_usd:,.0f} -> prev ${prev_oi:,.0f})"
                    )
                    anomalies.append((
                        now, cycle_ts, symbol_id, "oi_surge",
                        severity, change, desc,
                    ))
                    _set_cooldown(symbol_id, "oi_surge")

    # 3. L/S extreme
    if current_ls_ratio is not None:
        if stats and stats.get("std_ls_ratio"):
            threshold = stats["mean_ls_ratio"] + 3 * stats["std_ls_ratio"]
        else:
            threshold = config.LS_EXTREME_THRESHOLD

        if current_ls_ratio > threshold:
            has_ls_extreme = True
            if _check_cooldown(symbol_id, "ls_extreme"):
                desc = f"L/S ratio {current_ls_ratio:.2f} (threshold {threshold:.2f})"
                anomalies.append((
                    now, cycle_ts, symbol_id, "ls_extreme",
                    severity, current_ls_ratio, desc,
                ))
                _set_cooldown(symbol_id, "ls_extreme")

    # 4. Taker extreme
    if current_taker_ratio is not None:
        if stats and stats.get("std_taker_ratio"):
            threshold = stats["mean_taker_ratio"] + 3 * stats["std_taker_ratio"]
        else:
            threshold = config.TAKER_EXTREME_THRESHOLD

        if current_taker_ratio > threshold:
            if _check_cooldown(symbol_id, "taker_extreme"):
                desc = (
                    f"Taker ratio {current_taker_ratio:.2f} "
                    f"(threshold {threshold:.2f})"
                )
                anomalies.append((
                    now, cycle_ts, symbol_id, "taker_extreme",
                    severity, current_taker_ratio, desc,
                ))
                _set_cooldown(symbol_id, "taker_extreme")

    # 5. Combined: overheat
    if has_funding_spike and has_oi_surge and has_ls_extreme:
        if _check_cooldown(symbol_id, "combined_overheat"):
            desc = (
                f"OVERHEAT: funding={current_funding:.6f}, "
                f"OI surge, L/S={current_ls_ratio:.2f}"
            )
            anomalies.append((
                now, cycle_ts, symbol_id, "combined_overheat",
                severity, 0.0, desc,
            ))
            _set_cooldown(symbol_id, "combined_overheat")

    # 6. Combined: capitulation (OI drop + funding flip)
    if has_oi_surge and has_funding_spike:
        prev_funding = await db.get_latest_funding(symbol_id)
        if (
            prev_funding is not None
            and current_funding is not None
            and prev_funding * current_funding < 0
        ):
            if _check_cooldown(symbol_id, "combined_capitulation"):
                desc = (
                    f"CAPITULATION: funding flipped "
                    f"{prev_funding:.6f} -> {current_funding:.6f}, OI dropping"
                )
                anomalies.append((
                    now, cycle_ts, symbol_id, "combined_capitulation",
                    severity, 0.0, desc,
                ))
                _set_cooldown(symbol_id, "combined_capitulation")

    # 7. OI Flush
    flush = await check_oi_flush(
        db, cycle_ts, symbol_id, symbol_name,
        symbol_stats, top_oi_ids,
    )
    if flush:
        anomalies.append(flush)

    return anomalies


async def check_oi_flush(
    db: Database,
    cycle_ts: int,
    symbol_id: int,
    symbol_name: str,
    symbol_stats: dict[int, dict],
    top_oi_ids: list[int],
) -> tuple | None:
    """Detect OI buildup followed by flush."""
    if not _check_cooldown(symbol_id, "oi_flush"):
        return None

    since = cycle_ts - config.OI_FLUSH_LOOKBACK * config.COLLECT_INTERVAL
    history = await db.get_oi_history(symbol_id, since)
    if len(history) < config.OI_FLUSH_LOOKBACK:
        return None

    base_oi = history[0][1]
    if base_oi <= 0:
        return None

    pct_changes = [
        (ts, (oi - base_oi) / base_oi * 100) for ts, oi in history
    ]

    # Find longest consecutive run above buildup threshold
    best_start = 0
    best_len = 0
    cur_start = 0
    cur_len = 0
    for i, (_, pct) in enumerate(pct_changes):
        if pct >= config.OI_BUILDUP_THRESHOLD:
            if cur_len == 0:
                cur_start = i
            cur_len += 1
            if cur_len > best_len:
                best_len = cur_len
                best_start = cur_start
        else:
            cur_len = 0

    if best_len < config.OI_BUILDUP_MIN_POINTS:
        return None

    # The buildup series must reach into recent points (not ancient history)
    best_end = best_start + best_len - 1
    if best_end < len(pct_changes) - config.OI_BUILDUP_MIN_POINTS:
        return None

    # Peak pct during buildup
    peak_pct = max(pct_changes[best_start][1], *(
        pct_changes[i][1] for i in range(best_start, best_start + best_len)
    ))

    # Current pct must be below flush threshold
    current_pct = pct_changes[-1][1]
    if current_pct >= config.OI_FLUSH_CURRENT_MAX:
        return None

    drop = peak_pct - current_pct
    if drop < config.OI_FLUSH_DROP_PCT:
        return None

    # Confirmed OI Flush — gather context
    now = int(time.time())
    severity = _severity_for_symbol(symbol_id, symbol_stats, top_oi_ids)

    funding = await db.get_latest_funding(symbol_id)
    ls_data = await db.get_latest_ls_ratio(symbol_id)
    taker = await db.get_latest_taker_ratio(symbol_id)

    ls_ratio = ls_data[0] if ls_data else None
    long_pct = ls_data[1] if ls_data else None

    peak_oi = history[0][1] * (1 + peak_pct / 100)
    current_oi = history[-1][1]
    buildup_minutes = best_len * 5
    # How many points since peak to now
    flush_points = len(pct_changes) - 1 - best_end
    flush_minutes = max(flush_points * 5, 5)

    # Interpretation
    if funding and funding > 0 and ls_ratio and ls_ratio > 2.0:
        interpretation = "\u26a0\ufe0f \u041c\u0430\u0441\u0441\u043e\u0432\u043e\u0435 \u0437\u0430\u043a\u0440\u044b\u0442\u0438\u0435 \u043b\u043e\u043d\u0433\u043e\u0432"
    elif funding and funding < 0 and ls_ratio and ls_ratio < 0.5:
        interpretation = "\u26a0\ufe0f \u041c\u0430\u0441\u0441\u043e\u0432\u043e\u0435 \u0437\u0430\u043a\u0440\u044b\u0442\u0438\u0435 \u0448\u043e\u0440\u0442\u043e\u0432"
    elif taker and taker < 0.8:
        interpretation = "\u26a0\ufe0f \u0410\u0433\u0440\u0435\u0441\u0441\u0438\u0432\u043d\u044b\u0435 \u043f\u0440\u043e\u0434\u0430\u0436\u0438 \u043d\u0430 \u0441\u043f\u043e\u0442\u0435"
    else:
        interpretation = "\u26a0\ufe0f \u0420\u0435\u0437\u043a\u0438\u0439 \u0441\u0431\u0440\u043e\u0441 \u043f\u043e\u0437\u0438\u0446\u0438\u0439"

    # Format description for DB + Telegram
    funding_str = f"{funding:+.3%}" if funding else "n/a"
    funding_label = (
        " (\u043b\u043e\u043d\u0433\u0438 \u043f\u043b\u0430\u0442\u044f\u0442)"
        if funding and funding > 0
        else " (\u0448\u043e\u0440\u0442\u044b \u043f\u043b\u0430\u0442\u044f\u0442)"
        if funding and funding < 0
        else ""
    )
    ls_str = (
        f"{ls_ratio:.1f} ({long_pct * 100:.0f}% \u043b\u043e\u043d\u0433)"
        if ls_ratio and long_pct is not None
        else "n/a"
    )
    taker_str = f"{taker:.1f} buy/sell" if taker else "n/a"

    def _fmt_usd(v: float) -> str:
        if v >= 1_000_000_000:
            return f"${v / 1_000_000_000:.1f}B"
        if v >= 1_000_000:
            return f"${v / 1_000_000:.1f}M"
        return f"${v:,.0f}"

    def _fmt_duration(minutes: int) -> str:
        if minutes >= 60:
            h = minutes // 60
            m = minutes % 60
            return f"{h}\u0447 {m}\u043c\u0438\u043d" if m else f"{h}\u0447"
        return f"{minutes}\u043c\u0438\u043d"

    desc = (
        f"\u26a1 OI FLUSH: {symbol_name}\n"
        f"\n"
        f"\U0001f4ca OI \u043d\u0430\u043a\u043e\u043f\u043b\u0435\u043d\u0438\u0435 \u2192 \u0441\u0431\u0440\u043e\u0441:\n"
        f"   \u041f\u0438\u043a: {_fmt_usd(peak_oi)} (+{peak_pct:.1f}% \u0437\u0430 {_fmt_duration(buildup_minutes)})\n"
        f"   \u0421\u0435\u0439\u0447\u0430\u0441: {_fmt_usd(current_oi)} ({current_pct:+.1f}%)\n"
        f"   \u0421\u0431\u0440\u043e\u0441: -{drop:.1f}% \u0437\u0430 {_fmt_duration(flush_minutes)}\n"
        f"\n"
        f"\U0001f4b0 Funding: {funding_str}{funding_label}\n"
        f"\U0001f4c8 L/S Ratio: {ls_str}\n"
        f"\U0001f504 Taker: {taker_str}\n"
        f"\n"
        f"{interpretation}"
    )

    _set_cooldown(symbol_id, "oi_flush")
    return (now, cycle_ts, symbol_id, "oi_flush", severity, drop, desc)
