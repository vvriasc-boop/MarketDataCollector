import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_ID: int = int(os.getenv("ADMIN_ID") or "0")

# Proxy
PROXY_URL: str = os.getenv("PROXY_URL", "")

# Collection
COLLECT_INTERVAL: int = int(os.getenv("COLLECT_INTERVAL", "300"))
REQUEST_DELAY: float = float(os.getenv("REQUEST_DELAY", "0.05"))
MAX_CONCURRENT: int = 10
WATCHDOG_TIMEOUT: int = 240

# Hot filter
HOT_VOLUME_THRESHOLD: float = float(os.getenv("HOT_VOLUME_THRESHOLD", "1000000"))
SYMBOLS_REFRESH_INTERVAL: int = 4 * 3600  # 4 hours

# Anomaly detection
MIN_HISTORY_FOR_ANOMALY: int = int(os.getenv("MIN_HISTORY_FOR_ANOMALY", "12"))
FUNDING_SPIKE_THRESHOLD: float = float(os.getenv("FUNDING_SPIKE_THRESHOLD", "0.001"))
OI_SURGE_THRESHOLD: float = float(os.getenv("OI_SURGE_THRESHOLD", "0.10"))
LS_EXTREME_THRESHOLD: float = float(os.getenv("LS_EXTREME_THRESHOLD", "3.0"))
TAKER_EXTREME_THRESHOLD: float = float(os.getenv("TAKER_EXTREME_THRESHOLD", "2.0"))

# OI Flush detection
OI_BUILDUP_THRESHOLD: float = float(os.getenv("OI_BUILDUP_THRESHOLD", "3.0"))
OI_BUILDUP_MIN_POINTS: int = int(os.getenv("OI_BUILDUP_MIN_POINTS", "12"))
OI_FLUSH_DROP_PCT: float = float(os.getenv("OI_FLUSH_DROP_PCT", "2.0"))
OI_FLUSH_CURRENT_MAX: float = 2.0
OI_FLUSH_LOOKBACK: int = 24
OI_FLUSH_COOLDOWN: int = int(os.getenv("OI_FLUSH_COOLDOWN", "1800"))

# Notifications
ALERT_COOLDOWN: int = int(os.getenv("ALERT_COOLDOWN", "3600"))
MIN_ALERT_SEVERITY: str = os.getenv("MIN_ALERT_SEVERITY", "high")
NOTIFIER_DELAY: float = 0.5
NOTIFIER_MAX_QUEUE: int = 100
MASS_ALERT_THRESHOLD: int = 5
MASS_ALERT_WINDOW: int = 60

# Severity thresholds (avg_oi_usd)
SEVERITY_CRITICAL_OI: float = 10_000_000_000  # $10B
SEVERITY_MEDIUM_OI: float = 100_000_000       # $100M
SEVERITY_TOP_N: int = 20

# Archive
ARCHIVE_AFTER_DAYS: int = int(os.getenv("ARCHIVE_AFTER_DAYS", "30"))

# Database
DB_PATH: str = os.getenv("DB_PATH", "market_data.db")

# Stats worker
STATS_WORKER_HOUR_UTC: int = 4   # 04:00 UTC
STATS_MIN_POINTS: int = 100
STATS_LOOKBACK_DAYS: int = 7

# Daily summary
DAILY_SUMMARY_HOUR_UTC: int = 9  # 09:00 UTC

# Logging
LOG_FILE: str = "market_data.log"
LOG_FORMAT: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

# Binance API base
BINANCE_FAPI_BASE: str = "https://fapi.binance.com"

# Retry
RETRY_CODES: set = {429, 500, 502, 503, 504}
RETRY_MAX_WAIT: int = 30

# Severity ordering
SEVERITY_ORDER: dict = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
}
