import logging
import time
from typing import Any

import aiosqlite

import config

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS symbols (
    id INTEGER PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL,
    base_asset TEXT,
    status TEXT DEFAULT 'active',
    is_hot INTEGER DEFAULT 1,
    quote_volume_24h REAL DEFAULT 0,
    first_seen INTEGER,
    last_seen INTEGER
);

CREATE TABLE IF NOT EXISTS open_interest (
    timestamp INTEGER NOT NULL,
    symbol_id INTEGER NOT NULL,
    oi_contracts REAL,
    oi_usd REAL,
    mark_price REAL,
    PRIMARY KEY (timestamp, symbol_id)
);

CREATE TABLE IF NOT EXISTS funding_rate (
    timestamp INTEGER NOT NULL,
    symbol_id INTEGER NOT NULL,
    rate REAL,
    next_funding_time INTEGER,
    PRIMARY KEY (timestamp, symbol_id)
);

CREATE TABLE IF NOT EXISTS long_short_ratio (
    timestamp INTEGER NOT NULL,
    symbol_id INTEGER NOT NULL,
    ratio REAL,
    long_pct REAL,
    short_pct REAL,
    PRIMARY KEY (timestamp, symbol_id)
);

CREATE TABLE IF NOT EXISTS taker_ratio (
    timestamp INTEGER NOT NULL,
    symbol_id INTEGER NOT NULL,
    buy_sell_ratio REAL,
    buy_vol REAL,
    sell_vol REAL,
    PRIMARY KEY (timestamp, symbol_id)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    cycle_ts INTEGER NOT NULL,
    symbol_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'medium',
    value REAL,
    description TEXT,
    notified INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS symbol_stats (
    symbol_id INTEGER PRIMARY KEY,
    updated_at INTEGER,
    mean_funding REAL,
    std_funding REAL,
    mean_oi_change_1h REAL,
    std_oi_change_1h REAL,
    mean_ls_ratio REAL,
    std_ls_ratio REAL,
    mean_taker_ratio REAL,
    std_taker_ratio REAL,
    avg_oi_usd REAL
);

CREATE TABLE IF NOT EXISTS collector_stats (
    timestamp INTEGER PRIMARY KEY,
    cycle_duration_sec REAL,
    requests_ok INTEGER,
    requests_failed INTEGER,
    pairs_collected INTEGER,
    anomalies_found INTEGER
);

CREATE INDEX IF NOT EXISTS idx_oi_symbol ON open_interest(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_funding_symbol ON funding_rate(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_ls_symbol ON long_short_ratio(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_taker_symbol ON taker_ratio(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_cycle ON anomalies(cycle_ts);
CREATE INDEX IF NOT EXISTS idx_stats_time ON collector_stats(timestamp);
"""


class Database:
    def __init__(self, path: str = config.DB_PATH):
        self._path = path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        logger.info("Database initialized: %s", self._path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    # ── symbols ──────────────────────────────────────────

    async def upsert_symbols(
        self, symbols: list[dict[str, Any]]
    ) -> dict[str, int]:
        """Insert or update symbols, return {symbol_name: id}."""
        now = int(time.time())
        for s in symbols:
            await self._db.execute(
                """INSERT INTO symbols (symbol, base_asset, status, first_seen, last_seen)
                   VALUES (?, ?, 'active', ?, ?)
                   ON CONFLICT(symbol) DO UPDATE SET
                       status='active', last_seen=excluded.last_seen""",
                (s["symbol"], s.get("baseAsset", ""), now, now),
            )
        await self._db.commit()
        return await self.get_symbol_map()

    async def get_symbol_map(self) -> dict[str, int]:
        cur = await self._db.execute("SELECT symbol, id FROM symbols")
        rows = await cur.fetchall()
        return {r["symbol"]: r["id"] for r in rows}

    async def update_hot_status(
        self, hot_map: dict[str, tuple[bool, float]]
    ) -> None:
        """hot_map: {symbol: (is_hot, volume_24h)}."""
        sym_map = await self.get_symbol_map()
        for symbol, (is_hot, vol) in hot_map.items():
            sid = sym_map.get(symbol)
            if sid is None:
                continue
            await self._db.execute(
                "UPDATE symbols SET is_hot=?, quote_volume_24h=? WHERE id=?",
                (int(is_hot), vol, sid),
            )
        await self._db.commit()

    async def get_hot_symbols(self) -> list[dict]:
        cur = await self._db.execute(
            "SELECT id, symbol FROM symbols WHERE is_hot=1 AND status='active'"
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_all_active_symbols(self) -> list[dict]:
        cur = await self._db.execute(
            "SELECT id, symbol FROM symbols WHERE status='active'"
        )
        return [dict(r) for r in await cur.fetchall()]

    # ── batch inserts ────────────────────────────────────

    async def insert_open_interest(
        self, rows: list[tuple]
    ) -> None:
        """rows: [(timestamp, symbol_id, oi_contracts, oi_usd, mark_price)]."""
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO open_interest
               (timestamp, symbol_id, oi_contracts, oi_usd, mark_price)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()

    async def insert_funding_rate(self, rows: list[tuple]) -> None:
        """rows: [(timestamp, symbol_id, rate, next_funding_time)]."""
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO funding_rate
               (timestamp, symbol_id, rate, next_funding_time)
               VALUES (?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()

    async def insert_long_short_ratio(self, rows: list[tuple]) -> None:
        """rows: [(timestamp, symbol_id, ratio, long_pct, short_pct)]."""
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO long_short_ratio
               (timestamp, symbol_id, ratio, long_pct, short_pct)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()

    async def insert_taker_ratio(self, rows: list[tuple]) -> None:
        """rows: [(timestamp, symbol_id, buy_sell_ratio, buy_vol, sell_vol)]."""
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO taker_ratio
               (timestamp, symbol_id, buy_sell_ratio, buy_vol, sell_vol)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()

    async def insert_anomalies(self, rows: list[tuple]) -> None:
        """rows: [(timestamp, cycle_ts, symbol_id, type, severity, value, desc)]."""
        if not rows:
            return
        await self._db.executemany(
            """INSERT INTO anomalies
               (timestamp, cycle_ts, symbol_id, type, severity, value, description)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()

    async def insert_collector_stats(self, row: tuple) -> None:
        await self._db.execute(
            """INSERT OR REPLACE INTO collector_stats
               (timestamp, cycle_duration_sec, requests_ok,
                requests_failed, pairs_collected, anomalies_found)
               VALUES (?, ?, ?, ?, ?, ?)""",
            row,
        )
        await self._db.commit()

    # ── cache hydration ──────────────────────────────────

    async def load_last_values(self) -> dict[tuple, float]:
        last: dict[tuple, float] = {}

        cur = await self._db.execute(
            """SELECT oi.symbol_id, oi.oi_contracts
               FROM open_interest oi
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM open_interest GROUP BY symbol_id
               ) t ON oi.symbol_id = t.symbol_id
                  AND oi.timestamp = t.max_ts"""
        )
        for r in await cur.fetchall():
            last[(r["symbol_id"], "oi")] = r["oi_contracts"]

        cur = await self._db.execute(
            """SELECT fr.symbol_id, fr.rate
               FROM funding_rate fr
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM funding_rate GROUP BY symbol_id
               ) t ON fr.symbol_id = t.symbol_id
                  AND fr.timestamp = t.max_ts"""
        )
        for r in await cur.fetchall():
            last[(r["symbol_id"], "funding")] = r["rate"]

        cur = await self._db.execute(
            """SELECT ls.symbol_id, ls.ratio
               FROM long_short_ratio ls
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM long_short_ratio GROUP BY symbol_id
               ) t ON ls.symbol_id = t.symbol_id
                  AND ls.timestamp = t.max_ts"""
        )
        for r in await cur.fetchall():
            last[(r["symbol_id"], "ls")] = r["ratio"]

        cur = await self._db.execute(
            """SELECT tr.symbol_id, tr.buy_sell_ratio
               FROM taker_ratio tr
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM taker_ratio GROUP BY symbol_id
               ) t ON tr.symbol_id = t.symbol_id
                  AND tr.timestamp = t.max_ts"""
        )
        for r in await cur.fetchall():
            last[(r["symbol_id"], "taker")] = r["buy_sell_ratio"]

        logger.info("Cache hydration: loaded %d last values", len(last))
        return last

    # ── symbol_stats ─────────────────────────────────────

    async def load_symbol_stats(self) -> dict[int, dict]:
        cur = await self._db.execute("SELECT * FROM symbol_stats")
        rows = await cur.fetchall()
        return {r["symbol_id"]: dict(r) for r in rows}

    async def save_symbol_stats(self, rows: list[tuple]) -> None:
        """rows: [(symbol_id, updated_at, mean_funding, std_funding, ...)]."""
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR REPLACE INTO symbol_stats
               (symbol_id, updated_at, mean_funding, std_funding,
                mean_oi_change_1h, std_oi_change_1h,
                mean_ls_ratio, std_ls_ratio,
                mean_taker_ratio, std_taker_ratio, avg_oi_usd)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await self._db.commit()

    # ── queries for stats_worker (raw data for Python stats) ─

    async def get_funding_data(
        self, symbol_id: int, since: int
    ) -> list[float]:
        cur = await self._db.execute(
            "SELECT rate FROM funding_rate WHERE symbol_id=? AND timestamp>=?",
            (symbol_id, since),
        )
        return [r["rate"] for r in await cur.fetchall() if r["rate"] is not None]

    async def get_oi_changes_1h(
        self, symbol_id: int, since: int
    ) -> list[float]:
        cur = await self._db.execute(
            """SELECT a.oi_usd, b.oi_usd AS prev_oi
               FROM open_interest a
               INNER JOIN open_interest b
                   ON a.symbol_id = b.symbol_id
                   AND b.timestamp = a.timestamp - 3600
               WHERE a.symbol_id=? AND a.timestamp>=?
                   AND a.oi_usd IS NOT NULL AND b.oi_usd IS NOT NULL
                   AND b.oi_usd > 0""",
            (symbol_id, since),
        )
        rows = await cur.fetchall()
        return [(r["oi_usd"] - r["prev_oi"]) / r["prev_oi"] for r in rows]

    async def get_ls_data(
        self, symbol_id: int, since: int
    ) -> list[float]:
        cur = await self._db.execute(
            "SELECT ratio FROM long_short_ratio WHERE symbol_id=? AND timestamp>=?",
            (symbol_id, since),
        )
        return [r["ratio"] for r in await cur.fetchall() if r["ratio"] is not None]

    async def get_taker_data(
        self, symbol_id: int, since: int
    ) -> list[float]:
        cur = await self._db.execute(
            "SELECT buy_sell_ratio FROM taker_ratio WHERE symbol_id=? AND timestamp>=?",
            (symbol_id, since),
        )
        return [
            r["buy_sell_ratio"]
            for r in await cur.fetchall()
            if r["buy_sell_ratio"] is not None
        ]

    async def get_avg_oi_usd(
        self, symbol_id: int, since: int
    ) -> float | None:
        cur = await self._db.execute(
            "SELECT AVG(oi_usd) AS avg_oi FROM open_interest WHERE symbol_id=? AND timestamp>=?",
            (symbol_id, since),
        )
        row = await cur.fetchone()
        return row["avg_oi"] if row else None

    # ── queries for anomaly detection ────────────────────

    async def count_recent_oi(self, symbol_id: int) -> int:
        cur = await self._db.execute(
            "SELECT COUNT(*) AS cnt FROM open_interest WHERE symbol_id=?",
            (symbol_id,),
        )
        row = await cur.fetchone()
        return row["cnt"]

    async def get_oi_hour_ago(
        self, symbol_id: int, ts_hour_ago: int
    ) -> float | None:
        cur = await self._db.execute(
            """SELECT oi_usd FROM open_interest
               WHERE symbol_id=? AND timestamp<=?
               ORDER BY timestamp DESC LIMIT 1""",
            (symbol_id, ts_hour_ago),
        )
        row = await cur.fetchone()
        return row["oi_usd"] if row else None

    async def get_oi_history(
        self, symbol_id: int, since: int
    ) -> list[tuple[int, float]]:
        """Return [(timestamp, oi_usd)] sorted ASC for OI flush detection."""
        cur = await self._db.execute(
            """SELECT timestamp, oi_usd FROM open_interest
               WHERE symbol_id=? AND timestamp>=? AND oi_usd IS NOT NULL
               ORDER BY timestamp ASC""",
            (symbol_id, since),
        )
        return [(r["timestamp"], r["oi_usd"]) for r in await cur.fetchall()]

    async def get_latest_ls_ratio(
        self, symbol_id: int
    ) -> tuple[float, float] | None:
        """Return (ratio, long_pct) or None."""
        cur = await self._db.execute(
            """SELECT ratio, long_pct FROM long_short_ratio
               WHERE symbol_id=?
               ORDER BY timestamp DESC LIMIT 1""",
            (symbol_id,),
        )
        row = await cur.fetchone()
        if row and row["ratio"] is not None:
            return (row["ratio"], row["long_pct"] or 0.0)
        return None

    async def get_latest_taker_ratio(
        self, symbol_id: int
    ) -> float | None:
        cur = await self._db.execute(
            """SELECT buy_sell_ratio FROM taker_ratio
               WHERE symbol_id=?
               ORDER BY timestamp DESC LIMIT 1""",
            (symbol_id,),
        )
        row = await cur.fetchone()
        return row["buy_sell_ratio"] if row else None

    async def get_latest_funding(
        self, symbol_id: int
    ) -> float | None:
        cur = await self._db.execute(
            """SELECT rate FROM funding_rate
               WHERE symbol_id=?
               ORDER BY timestamp DESC LIMIT 1""",
            (symbol_id,),
        )
        row = await cur.fetchone()
        return row["rate"] if row else None

    # ── queries for handlers ─────────────────────────────

    async def get_recent_anomalies(self, limit: int = 20) -> list[dict]:
        cur = await self._db.execute(
            """SELECT a.*, s.symbol FROM anomalies a
               JOIN symbols s ON a.symbol_id = s.id
               ORDER BY a.timestamp DESC LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_last_collector_stats(self) -> dict | None:
        cur = await self._db.execute(
            "SELECT * FROM collector_stats ORDER BY timestamp DESC LIMIT 1"
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_symbol_count(self) -> tuple[int, int]:
        cur = await self._db.execute(
            "SELECT COUNT(*) AS total FROM symbols WHERE status='active'"
        )
        total = (await cur.fetchone())["total"]
        cur = await self._db.execute(
            "SELECT COUNT(*) AS hot FROM symbols WHERE status='active' AND is_hot=1"
        )
        hot = (await cur.fetchone())["hot"]
        return hot, total

    async def get_db_size_mb(self) -> float:
        import os
        try:
            return os.path.getsize(self._path) / (1024 * 1024)
        except OSError:
            return 0.0

    async def get_top_oi_change(self, limit: int = 10) -> list[dict]:
        now = int(time.time())
        hour_ago = now - 3600
        cur = await self._db.execute(
            """SELECT s.symbol, a.oi_usd AS current_oi, b.oi_usd AS prev_oi,
                      (a.oi_usd - b.oi_usd) / b.oi_usd * 100 AS change_pct
               FROM open_interest a
               INNER JOIN open_interest b
                   ON a.symbol_id = b.symbol_id
               INNER JOIN symbols s ON a.symbol_id = s.id
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM open_interest GROUP BY symbol_id
               ) latest ON a.symbol_id = latest.symbol_id
                       AND a.timestamp = latest.max_ts
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM open_interest WHERE timestamp <= ? GROUP BY symbol_id
               ) prev ON b.symbol_id = prev.symbol_id
                     AND b.timestamp = prev.max_ts
               WHERE b.oi_usd > 0
               ORDER BY ABS(change_pct) DESC LIMIT ?""",
            (hour_ago, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_top_funding(self, limit: int = 10) -> list[dict]:
        cur = await self._db.execute(
            """SELECT s.symbol, fr.rate
               FROM funding_rate fr
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM funding_rate GROUP BY symbol_id
               ) t ON fr.symbol_id = t.symbol_id AND fr.timestamp = t.max_ts
               INNER JOIN symbols s ON fr.symbol_id = s.id
               ORDER BY ABS(fr.rate) DESC LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_top_ls(self, limit: int = 10) -> list[dict]:
        cur = await self._db.execute(
            """SELECT s.symbol, ls.ratio, ls.long_pct, ls.short_pct
               FROM long_short_ratio ls
               INNER JOIN (
                   SELECT symbol_id, MAX(timestamp) AS max_ts
                   FROM long_short_ratio GROUP BY symbol_id
               ) t ON ls.symbol_id = t.symbol_id AND ls.timestamp = t.max_ts
               INNER JOIN symbols s ON ls.symbol_id = s.id
               ORDER BY ABS(ls.ratio - 1.0) DESC LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_pair_data(self, symbol: str) -> dict | None:
        cur = await self._db.execute(
            "SELECT id, symbol, is_hot, quote_volume_24h FROM symbols WHERE symbol=?",
            (symbol.upper(),),
        )
        sym = await cur.fetchone()
        if not sym:
            return None
        sid = sym["id"]
        result: dict[str, Any] = {"symbol": sym["symbol"], "is_hot": sym["is_hot"]}

        cur = await self._db.execute(
            """SELECT oi_contracts, oi_usd, mark_price FROM open_interest
               WHERE symbol_id=? ORDER BY timestamp DESC LIMIT 1""",
            (sid,),
        )
        row = await cur.fetchone()
        result["oi"] = dict(row) if row else None

        now = int(time.time())
        for label, delta in [("1h", 3600), ("24h", 86400), ("7d", 604800)]:
            ts = now - delta
            cur = await self._db.execute(
                """SELECT oi_usd FROM open_interest
                   WHERE symbol_id=? AND timestamp<=?
                   ORDER BY timestamp DESC LIMIT 1""",
                (sid, ts),
            )
            row = await cur.fetchone()
            result[f"oi_{label}"] = row["oi_usd"] if row else None

        cur = await self._db.execute(
            """SELECT rate FROM funding_rate
               WHERE symbol_id=? ORDER BY timestamp DESC LIMIT 1""",
            (sid,),
        )
        row = await cur.fetchone()
        result["funding"] = row["rate"] if row else None

        cur = await self._db.execute(
            "SELECT AVG(rate) AS avg_rate FROM funding_rate WHERE symbol_id=? AND timestamp>=?",
            (sid, now - 604800),
        )
        row = await cur.fetchone()
        result["funding_7d_avg"] = row["avg_rate"] if row else None

        cur = await self._db.execute(
            """SELECT ratio, long_pct, short_pct FROM long_short_ratio
               WHERE symbol_id=? ORDER BY timestamp DESC LIMIT 1""",
            (sid,),
        )
        row = await cur.fetchone()
        result["ls"] = dict(row) if row else None

        cur = await self._db.execute(
            """SELECT buy_sell_ratio FROM taker_ratio
               WHERE symbol_id=? ORDER BY timestamp DESC LIMIT 1""",
            (sid,),
        )
        row = await cur.fetchone()
        result["taker"] = row["buy_sell_ratio"] if row else None

        return result

    async def get_anomaly_counts_24h(self) -> dict[str, int]:
        since = int(time.time()) - 86400
        cur = await self._db.execute(
            "SELECT severity, COUNT(*) AS cnt FROM anomalies WHERE timestamp>=? GROUP BY severity",
            (since,),
        )
        return {r["severity"]: r["cnt"] for r in await cur.fetchall()}

    async def get_daily_top_funding(
        self, limit: int = 10
    ) -> list[dict]:
        since = int(time.time()) - 86400
        cur = await self._db.execute(
            """SELECT s.symbol, fr.rate
               FROM funding_rate fr
               INNER JOIN symbols s ON fr.symbol_id = s.id
               WHERE fr.timestamp >= ?
               ORDER BY ABS(fr.rate) DESC LIMIT ?""",
            (since, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_daily_top_oi_change(
        self, limit: int = 10
    ) -> list[dict]:
        since = int(time.time()) - 86400
        cur = await self._db.execute(
            """SELECT s.symbol,
                      MAX(oi.oi_usd) - MIN(oi.oi_usd) AS oi_range,
                      (MAX(oi.oi_usd) - MIN(oi.oi_usd)) / MIN(oi.oi_usd) * 100 AS pct
               FROM open_interest oi
               INNER JOIN symbols s ON oi.symbol_id = s.id
               WHERE oi.timestamp >= ? AND oi.oi_usd > 0
               GROUP BY oi.symbol_id
               ORDER BY pct DESC LIMIT ?""",
            (since, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_daily_top_ls(self, limit: int = 10) -> list[dict]:
        since = int(time.time()) - 86400
        cur = await self._db.execute(
            """SELECT s.symbol, MAX(ls.ratio) AS max_ratio
               FROM long_short_ratio ls
               INNER JOIN symbols s ON ls.symbol_id = s.id
               WHERE ls.timestamp >= ?
               GROUP BY ls.symbol_id
               ORDER BY max_ratio DESC LIMIT ?""",
            (since, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    # ── archive ──────────────────────────────────────────

    async def get_archive_rows(
        self, table: str, before_ts: int
    ) -> list[dict]:
        cur = await self._db.execute(
            f"SELECT * FROM {table} WHERE timestamp < ?", (before_ts,)
        )
        return [dict(r) for r in await cur.fetchall()]

    async def delete_old_rows(self, table: str, before_ts: int) -> int:
        cur = await self._db.execute(
            f"DELETE FROM {table} WHERE timestamp < ?", (before_ts,)
        )
        await self._db.commit()
        return cur.rowcount

    async def vacuum(self) -> None:
        await self._db.execute("VACUUM")
