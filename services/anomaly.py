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
    return (time.time() - last) >= config.ALERT_COOLDOWN


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

    return anomalies
