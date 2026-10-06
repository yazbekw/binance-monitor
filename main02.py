"""
Binance Monitor Bot — تقاطع EMA (نفس استراتيجية بوت Bybit) + مراقبة TP/SL ذاتية
المراقبة عبر WebSocket bookTicker (خفيف، لا يسبب حظر)

🆕 التحديثات:
- استراتيجية تقاطعات ثلاثية: pre-cross / live-cross / confirmed cross
- متعدد الفريمات: 5m, 15m, 1h
- نظام العلامة Score 0-100 مع مؤشرات داعمة (Volume/ADX/Gap/MACD/RSI/EMA50/ATR)
- فلتر إلزامي (حجم/ADX/ATR/EMA50/15m)
- حماية Rate Limit: retry + backoff + cooldown للرموز + إيقاف عالمي عند الحظر
"""
import os
import sys
import time
import re
import asyncio
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import pandas as pd
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import NetworkError, TimedOut
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# الإعدادات الأساسية
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID")
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

REQUIRED = {
    "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
    "TELEGRAM_CHAT_ID": CHAT_ID_RAW,
    "BINANCE_API_KEY": API_KEY,
    "BINANCE_API_SECRET": API_SECRET,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    print(f"❌ متغيرات ناقصة: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

CHAT_ID = int(CHAT_ID_RAW)
HOURLY_MIN = 60
WS_RECONNECT_DELAY = 10
MARK_WS_RECONNECT_DELAY = 5


# ============================================================
# أدوات مساعدة للقراءة من البيئة
# ============================================================
def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    m = re.search(r"-?\d+(\.\d+)?", str(raw))
    return float(m.group(0)) if m else default


def _env_int(name: str, default: int) -> int:
    return int(_env_float(name, default))


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("true", "1", "yes", "on")


# ============================================================
# الاستراتيجية الأساسية
# ============================================================
STRATEGY_ENABLED = _env_bool("STRATEGY_ENABLED", False)

_default_symbols = "DOGEUSDT, SOLUSDT, AVAXUSDT, XRPUSDT, LINKUSDT, ETHUSDT, ADAUSDT, BNBUSDT"
STRATEGY_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("STRATEGY_SYMBOLS", _default_symbols).split(",")
    if s.strip()
]

# 🆕 فريمات مراقبة التقاطع (متعددة)
CROSS_TIMEFRAMES = [
    t.strip() for t in os.getenv("CROSS_TIMEFRAMES", "5m,15m,1h").split(",") if t.strip()
]

# الفريم الذي تُفتح عليه الصفقات (يبقى كما هو)
STRATEGY_INTERVAL     = os.getenv("STRATEGY_INTERVAL", "5m")
EMA_FAST              = int(os.getenv("EMA_FAST", "7"))
EMA_SLOW              = int(os.getenv("EMA_SLOW", "25"))

TREND_FILTER_ENABLED  = _env_bool("TREND_FILTER_ENABLED", True)
TREND_TIMEFRAME       = os.getenv("TREND_TIMEFRAME", "1h")
TREND_EMA_PERIOD      = int(os.getenv("TREND_EMA_PERIOD", "200"))

MARGIN_USDT           = float(os.getenv("MARGIN_USDT", "5"))
LEVERAGE              = int(os.getenv("LEVERAGE", "20"))
TP_USDT               = float(os.getenv("TP_USDT", "1"))
SL_USDT               = float(os.getenv("SL_USDT", "10"))

MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
MAX_DAILY_LOSS_USDT   = float(os.getenv("MAX_DAILY_LOSS_USDT", "10"))
STRATEGY_JOB_INTERVAL = int(os.getenv("STRATEGY_JOB_INTERVAL", "30"))
SL_CAP_RATIO          = float(os.getenv("SL_CAP_RATIO", "0.8"))


# ============================================================
# 🆕 إعدادات التقاطع الثلاثي (نفس بوت Bybit)
# ============================================================
ENABLE_PRE_CROSS   = _env_bool("ENABLE_PRE_CROSS", True)
ENABLE_LIVE_CROSS  = _env_bool("ENABLE_LIVE_CROSS", True)
ENABLE_CONFIRMED   = _env_bool("ENABLE_CONFIRMED", True)

PRE_CROSS_GAP      = _env_float("PRE_CROSS_GAP", 0.05)
PRE_CROSS_LOOKBACK = _env_int("PRE_CROSS_LOOKBACK", 3)
PRE_CROSS_COOLDOWN = _env_int("PRE_CROSS_COOLDOWN", 3)

MIN_EMA_GAP        = _env_float("MIN_EMA_GAP", 0.10)
STRONG_GAP_5M      = _env_float("STRONG_GAP_5M", 0.25)
STRONG_GAP_15M     = _env_float("STRONG_GAP_15M", 0.20)
STRONG_GAP_1H      = _env_float("STRONG_GAP_1H", 0.30)


# ============================================================
# 🆕 نظام العلامة (Score)
# ============================================================
SCORE_MODE   = os.getenv("SCORE_MODE", "silent").lower()
MIN_SCORE    = _env_float("MIN_SCORE", 55)
GOLD_SCORE   = _env_float("GOLD_SCORE", 80)


# ============================================================
# 🆕 الفلتر الإلزامي
# ============================================================
ENABLE_HARD_FILTER = _env_bool("ENABLE_HARD_FILTER", True)
MIN_VOL_RATIO      = _env_float("MIN_VOL_RATIO", 1.0)
MIN_ADX_HARD       = _env_float("MIN_ADX_HARD", 18.0)
MAX_ATR_PCT_HARD   = _env_float("MAX_ATR_PCT_HARD", 0.70)
BLOCK_AGAINST_HTF  = _env_bool("BLOCK_AGAINST_HTF", True)
BLOCK_TF_15M_LOW   = _env_bool("BLOCK_TF_15M_LOW", True)
MIN_SCORE_15M      = _env_float("MIN_SCORE_15M", 65)


# ============================================================
# 🆕 حماية Rate Limit (Binance)
# ============================================================
RATE_LIMIT_BACKOFF_BASE = _env_float("RATE_LIMIT_BACKOFF_BASE", 2.0)
RATE_LIMIT_MAX_RETRIES  = _env_int("RATE_LIMIT_MAX_RETRIES", 3)
SYMBOL_COOLDOWN_SEC     = _env_int("SYMBOL_COOLDOWN_SEC", 60)
SYMBOL_DELAY_MS         = _env_int("SYMBOL_DELAY_MS", 200)
GLOBAL_PAUSE_SEC        = _env_int("GLOBAL_PAUSE_SEC", 120)
GLOBAL_PAUSE_TRIGGER    = _env_int("GLOBAL_PAUSE_TRIGGER", 3)
OHLCV_CACHE_SECONDS     = _env_int("OHLCV_CACHE_SECONDS", 90)


# ============================================================
# الحماية ومراقبة TP/SL
# ============================================================
CLOSE_MODE = os.getenv("CLOSE_MODE", "monitor").lower()
if CLOSE_MODE not in ("monitor", "orders", "both"):
    CLOSE_MODE = "monitor"

AUTO_PROTECT_ENABLED = _env_bool("AUTO_PROTECT_ENABLED", True)
AUTO_PROTECT_INTERVAL = _env_int("AUTO_PROTECT_INTERVAL", 60)
AUTO_PROTECT_MIN_NOTIONAL = _env_float("AUTO_PROTECT_MIN_NOTIONAL", 20)

_notional_ref = MARGIN_USDT * LEVERAGE
_effective_sl_ref = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
_default_tp_pct = round(TP_USDT / _notional_ref * 100, 3)
_default_sl_pct = round(_effective_sl_ref / _notional_ref * 100, 3)

AUTO_PROTECT_TP_PCT = _env_float("AUTO_PROTECT_TP_PCT", _default_tp_pct)
AUTO_PROTECT_SL_PCT = _env_float("AUTO_PROTECT_SL_PCT", _default_sl_pct)
MAX_SLIPPAGE_PCT = _env_float("MAX_SLIPPAGE_PCT", 0.5)


# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
for noisy in ("httpx", "telegram", "telegram.ext", "binance", "websockets"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger(__name__)


# ============================================================
# حالة عامة
# ============================================================
binance_client: AsyncClient | None = None
notifications_enabled = True
_app: Application | None = None
_user_stream_task: asyncio.Task | None = None
_mark_watcher_task: asyncio.Task | None = None

_http_cache: dict = {}
_banned_until_ms: int = 0

_last_ema_states: dict = {}          # {(symbol, tf): "FAST_ABOVE"/"FAST_BELOW"}
_last_candle_times: dict = {}        # {(symbol, tf): candle_ts}
_symbol_filters_cache: dict = {}
_trend_cache: dict = {}

_last_strategy_candle: int = 0

_watched_positions: dict = {}
_daily_pnl_cache = {"date": None, "pnl": 0.0, "last_fetch": 0.0}
_close_locks: dict = {}

# 🆕 حالات التقاطع الثلاثي
_crossover_cache: dict = {}          # {(symbol, tf, kind): {"direction":..., "candle_ts":...}}
_ohlcv_cache: dict = {}              # {(symbol, tf, limit): {"data":..., "ts":...}}
_symbol_cooldown: dict = {}          # {symbol: timestamp_until}
_rate_limit_global_until: float = 0.0

# 🆕 إحصائيات
_filter_stats = {
    "sent": 0,
    "hidden": 0,
    "filtered_vol": 0,
    "filtered_adx": 0,
    "filtered_atr": 0,
    "filtered_htf": 0,
    "filtered_15m_score": 0,
    "filtered_other": 0,
    "rate_limit_hits": 0,
    "global_pauses": 0,
}


# ============================================================
# Health Server
# ============================================================
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Binance Monitor Bot")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


def _run_health():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever()
    log.info(f"🩺 Health server على المنفذ {port}")


# ============================================================
# أدوات مساعدة عامة
# ============================================================
def is_banned() -> bool:
    return _banned_until_ms > int(time.time() * 1000)


def ban_remaining_sec() -> int:
    if not is_banned():
        return 0
    return max(0, (_banned_until_ms - int(time.time() * 1000)) // 1000)


def is_globally_paused() -> bool:
    return time.time() < _rate_limit_global_until


def global_pause_remaining() -> int:
    if not is_globally_paused():
        return 0
    return max(0, int(_rate_limit_global_until - time.time()))


def _register_ban(exc: Exception):
    global _banned_until_ms
    m = re.search(r"banned until (\d+)", str(exc))
    if m:
        _banned_until_ms = int(m.group(1))
        log.warning(
            f"⛔ Binance IP banned until "
            f"{datetime.fromtimestamp(_banned_until_ms/1000, timezone.utc)}"
        )


def _symbol_in_cooldown(symbol: str) -> bool:
    return time.time() < _symbol_cooldown.get(symbol, 0)


def _set_symbol_cooldown(symbol: str):
    _symbol_cooldown[symbol] = time.time() + SYMBOL_COOLDOWN_SEC
    log.warning(f"⏸️ {symbol} في كولداون {SYMBOL_COOLDOWN_SEC}ث")

    # إذا كان هناك عدة رموز في كولداون → إيقاف عالمي
    active = sum(1 for ts in _symbol_cooldown.values() if ts > time.time())
    if active >= GLOBAL_PAUSE_TRIGGER:
        global _rate_limit_global_until
        _rate_limit_global_until = time.time() + GLOBAL_PAUSE_SEC
        _filter_stats["global_pauses"] += 1
        log.error(
            f"🚨 إيقاف عالمي {GLOBAL_PAUSE_SEC}ث — "
            f"Binance يرفض الطلبات ({active} رموز في كولداون)"
        )


async def cached_http(key: str, coro_factory, ttl: int = 30):
    if is_banned():
        raise RuntimeError(
            f"⛔ Binance حظر IP. المتبقي: ~{ban_remaining_sec()//60} دقيقة."
        )
    now = time.time()
    hit = _http_cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        val = await coro_factory()
        _http_cache[key] = (now, val)
        return val
    except BinanceAPIException as e:
        if e.code == -1003:
            _register_ban(e)
            raise RuntimeError(
                f"⛔ Binance حظر IP. المتبقي: ~{ban_remaining_sec()//60} دقيقة."
            )
        raise


async def send(text: str, context: ContextTypes.DEFAULT_TYPE | None = None):
    if not notifications_enabled:
        return
    try:
        if context is not None:
            await context.bot.send_message(
                chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML
            )
        elif _app is not None:
            await _app.bot.send_message(
                chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML
            )
    except Exception as e:
        log.error(f"send error: {e}")


def round_step(value: float, step: str) -> float:
    try:
        d_val = Decimal(str(value))
        d_step = Decimal(str(step))
        if d_step <= 0:
            return value
        return float((d_val / d_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * d_step)
    except Exception:
        return value


def short(symbol: str) -> str:
    # DOGEUSDT -> DOGE, BTCUSDT -> BTC
    for quote in ("USDT", "BUSD", "USDC", "USD"):
        if symbol.endswith(quote):
            return symbol[: -len(quote)]
    return symbol


def syria_now_str() -> str:
    # عرض بتوقيت UTC (يمكن تعديله لاحقاً إن أردت)
    now = datetime.now(timezone.utc)
    days_ar = {
        "Monday": "الاثنين", "Tuesday": "الثلاثاء", "Wednesday": "الأربعاء",
        "Thursday": "الخميس", "Friday": "الجمعة", "Saturday": "السبت",
        "Sunday": "الأحد",
    }
    return (
        f"{days_ar.get(now.strftime('%A'), now.strftime('%A'))} "
        f"{now.strftime('%Y-%m-%d')} — {now.strftime('%H:%M:%S')} UTC"
    )


def ts_to_str(ts_ms: int) -> str:
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "?"


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:.2f}"
    if value >= 100:
        return f"{value:.3f}"
    if value >= 1:
        return f"{value:.4f}"
    if value >= 0.01:
        return f"{value:.5f}"
    return f"{value:.8f}"


def calc_ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def get_last_closed_candle_ts(interval: str) -> int:
    interval_ms = {
        "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
        "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
        "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000,
        "12h": 43_200_000, "1d": 86_400_000,
    }.get(interval)
    if not interval_ms:
        return 0
    now_ms = int(time.time() * 1000)
    current_open = (now_ms // interval_ms) * interval_ms
    return current_open - interval_ms


# ============================================================
# 🆕 المؤشرات الفنية (نفس بوت Bybit)
# ============================================================
def calc_ema_series(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    tr = pd.concat([
        df["h"] - df["l"],
        (df["h"] - df["c"].shift()).abs(),
        (df["l"] - df["c"].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def calc_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["h"], df["l"], df["c"]
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
    tr = pd.concat([
        high - low, (high - close.shift()).abs(), (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, 1e-9)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, 1e-9)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-9)
    return dx.ewm(alpha=1 / period, adjust=False).mean()


def calc_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_f = series.ewm(span=fast, adjust=False).mean()
    ema_s = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_f - ema_s
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


# ============================================================
# 🆕 تصنيف قوة التقاطع
# ============================================================
def classify_strength(timeframe: str, gap_pct: float) -> str:
    if timeframe == "1h":
        if gap_pct >= STRONG_GAP_1H:
            return "🔥 قوية جداً"
        if gap_pct >= MIN_EMA_GAP:
            return "🟢 قوية"
        return "🟡 متوسطة"
    if timeframe == "15m":
        if gap_pct >= STRONG_GAP_15M:
            return "🟢 قوية"
        if gap_pct >= MIN_EMA_GAP:
            return "🟡 متوسطة"
        return "⚪ ضعيفة"
    if timeframe == "5m":
        if gap_pct >= STRONG_GAP_5M:
            return "🟢 قوية"
        if gap_pct >= MIN_EMA_GAP:
            return "🟡 متوسطة"
        return "⚪ ضعيفة"
    if gap_pct >= STRONG_GAP_15M:
        return "🟢 قوية"
    if gap_pct >= MIN_EMA_GAP:
        return "🟡 متوسطة"
    return "⚪ ضعيفة"


# ============================================================
# 🆕 دوال العلامة (Score) — نفس أوزان بوت Bybit
# ============================================================
def _score_volume(vol_ratio: float) -> tuple:
    if vol_ratio >= 4.0:   return 30, "قوي جداً 🔥"
    if vol_ratio >= 2.5:   return 25, "قوي"
    if vol_ratio >= 1.5:   return 18, "جيد"
    if vol_ratio >= 1.0:   return 10, "طبيعي"
    if vol_ratio >= 0.5:   return 5,  "ضعيف"
    return 0, "ضعيف جداً ⚠️"


def _score_adx(adx: float) -> tuple:
    if adx >= 50:  return 18, "اتجاه متطرف"
    if adx >= 35:  return 20, "اتجاه قوي جداً"
    if adx >= 25:  return 18, "اتجاه واضح"
    if adx >= 20:  return 12, "اتجاه ضعيف"
    if adx >= 15:  return 5,  "عرضي"
    return 0, "عرضي جداً"


def _score_gap(gap_pct: float) -> tuple:
    if gap_pct >= 0.60:  return 12, "كبير جداً"
    if gap_pct >= 0.30:  return 15, "كبير"
    if gap_pct >= 0.10:  return 10, "متوسط"
    if gap_pct >= 0.05:  return 6,  "صغير"
    if gap_pct >= 0.02:  return 3,  "صغير جداً"
    return 0, "طفيلي"


def _score_macd(hist_now: float, hist_prev: float) -> tuple:
    rising = hist_now > hist_prev
    if hist_now > 0 and rising:     return 10, "يتسارع صعوداً ↗"
    if hist_now > 0 and not rising: return 5,  "يتباطأ صعوداً ↘"
    if hist_now < 0 and rising:     return 7,  "يتباطأ هبوطاً ↗"
    return 5, "يتسارع هبوطاً ↘"


def _score_rsi(rsi: float, direction: str) -> tuple:
    if direction == "bullish":
        if 55 <= rsi <= 70:   return 10, "زخم صاعد صحي ⭐"
        if rsi > 70:          return 5,  "تشبع شرائي (قد ينعكس)"
        if rsi >= 45:         return 7,  "محايد"
        if rsi >= 30:         return 3,  "زخم هابط (ضد الإشارة)"
        return 0, "تشبع بيعي"
    else:
        if 30 <= rsi <= 45:   return 10, "زخم هابط صحي ⭐"
        if rsi < 30:          return 5,  "تشبع بيعي (قد ينعكس)"
        if rsi <= 55:         return 7,  "محايد"
        if rsi <= 70:         return 3,  "زخم صاعد (ضد الإشارة)"
        return 0, "تشبع شرائي"


def _score_htf(price: float, ema50: float, direction: str) -> tuple:
    above = price > ema50
    if direction == "bullish" and above:      return 10, "مع الاتجاه الأكبر ✅"
    if direction == "bearish" and not above:  return 10, "مع الاتجاه الأكبر ✅"
    return 0, "ضد الاتجاه الأكبر ⚠️"


def _score_atr(atr_pct: float) -> tuple:
    if atr_pct >= 0.30:  return 5, "نشاط جيد"
    if atr_pct >= 0.15:  return 3, "طبيعي"
    return 1, "خمول"


def analyze_with_score(df: pd.DataFrame, direction: str, curr_idx: int = -2) -> dict:
    close = df["c"]
    vol = df["v"]
    current_price = float(close.iloc[curr_idx])

    # Volume
    try:
        start = max(0, len(df) + curr_idx - 20)
        end = len(df) + curr_idx
        vol_ma20 = float(vol.iloc[start:end].mean())
        vol_curr = float(vol.iloc[curr_idx])
        vol_ratio = vol_curr / vol_ma20 if vol_ma20 > 0 else 0
    except Exception:
        vol_ratio = 0
    vol_pts, vol_label = _score_volume(vol_ratio)

    # ADX
    try:
        adx = float(calc_adx(df).iloc[curr_idx])
    except Exception:
        adx = 0
    adx_pts, adx_label = _score_adx(adx)

    # Gap EMA
    try:
        ef = float(calc_ema_series(close, EMA_FAST).iloc[curr_idx])
        es = float(calc_ema_series(close, EMA_SLOW).iloc[curr_idx])
        gap_pct = abs(ef - es) / es * 100 if es else 0
    except Exception:
        gap_pct = 0
    gap_pts, gap_label = _score_gap(gap_pct)

    # MACD
    try:
        _, _, hist = calc_macd(close)
        macd_pts, macd_label = _score_macd(
            float(hist.iloc[curr_idx]), float(hist.iloc[curr_idx - 1])
        )
    except Exception:
        macd_pts, macd_label = 0, "غير محدد"

    # RSI
    try:
        rsi = float(calc_rsi(close).iloc[curr_idx])
    except Exception:
        rsi = 50
    rsi_pts, rsi_label = _score_rsi(rsi, direction)

    # EMA50
    try:
        ema50 = float(calc_ema_series(close, 50).iloc[curr_idx])
        htf_pts, htf_label = _score_htf(current_price, ema50, direction)
    except Exception:
        ema50 = current_price
        htf_pts, htf_label = 0, "?"

    # ATR
    try:
        atr = float(calc_atr(df).iloc[curr_idx])
        atr_pct = (atr / current_price) * 100 if current_price > 0 else 0
    except Exception:
        atr_pct = 0
    atr_pts, atr_label = _score_atr(atr_pct)

    total = vol_pts + adx_pts + gap_pts + macd_pts + rsi_pts + htf_pts + atr_pts

    if total >= 80:
        grade = "🌟 ذهبية"
    elif total >= 65:
        grade = "⭐ قوية"
    elif total >= 55:
        grade = "✅ جيدة"
    elif total >= 45:
        grade = "🟡 متوسطة"
    else:
        grade = "⚪ ضعيفة"

    return {
        "score": total,
        "grade": grade,
        "adx": round(adx, 1), "adx_label": adx_label, "adx_pts": adx_pts,
        "rsi": round(rsi, 1), "rsi_label": rsi_label, "rsi_pts": rsi_pts,
        "macd_label": macd_label, "macd_pts": macd_pts,
        "vol_ratio": round(vol_ratio, 2), "vol_label": vol_label, "vol_pts": vol_pts,
        "atr_pct": round(atr_pct, 2), "atr_label": atr_label, "atr_pts": atr_pts,
        "htf_trend": "صاعد" if current_price > ema50 else "هابط",
        "htf_label": htf_label, "htf_pts": htf_pts,
        "gap_pct": round(gap_pct, 3), "gap_label": gap_label, "gap_pts": gap_pts,
    }


# ============================================================
# 🆕 الفلتر الإلزامي
# ============================================================
def passes_hard_filter(cross: dict) -> tuple:
    if not ENABLE_HARD_FILTER:
        return True, "", ""

    s = cross.get("support", {})
    if not s:
        return False, "لا توجد مؤشرات داعمة", "other"

    tf    = cross["timeframe"]
    vol   = s.get("vol_ratio", 0)
    adx   = s.get("adx", 0)
    atr   = s.get("atr_pct", 0)
    htf   = s.get("htf_pts", 0)
    score = s.get("score", 0)

    if vol < MIN_VOL_RATIO:
        return False, f"الحجم ضعيف ({vol}× < {MIN_VOL_RATIO})", "vol"

    if adx < MIN_ADX_HARD:
        return False, f"ADX منخفض ({adx} < {MIN_ADX_HARD})", "adx"

    if atr > MAX_ATR_PCT_HARD:
        return False, f"ATR مرتفع ({atr}% > {MAX_ATR_PCT_HARD}%)", "atr"

    if BLOCK_AGAINST_HTF and cross.get("alert_type") != "pre":
        if htf == 0:
            return False, "ضد الاتجاه الأكبر (EMA50)", "htf"

    if BLOCK_TF_15M_LOW and tf == "15m" and score < MIN_SCORE_15M:
        return False, f"15m بعلامة منخفضة ({score} < {MIN_SCORE_15M})", "15m_score"

    return True, "", ""


# ============================================================
# 🆕 جلب الشموع مع Retry + Backoff + Cooldown
# ============================================================
async def fetch_klines_safe(symbol: str, interval: str, limit: int = 200):
    """يُرجع DataFrame أو None. مع حماية Rate Limit."""
    global _rate_limit_global_until

    if binance_client is None:
        return None

    now = time.time()

    if now < _rate_limit_global_until:
        log.debug(f"⏸️ إيقاف عالمي — باقي {int(_rate_limit_global_until - now)}ث")
        return None

    if _symbol_in_cooldown(symbol):
        log.debug(f"⏸️ {symbol} في كولداون")
        return None

    last_err = None
    for attempt in range(RATE_LIMIT_MAX_RETRIES):
        try:
            klines = await binance_client.futures_klines(
                symbol=symbol, interval=interval, limit=limit
            )
            _symbol_cooldown.pop(symbol, None)
            return klines

        except BinanceAPIException as e:
            last_err = e
            if e.code == -1003:
                _filter_stats["rate_limit_hits"] += 1
                _register_ban(e)
                _set_symbol_cooldown(symbol)
                return None
            wait = RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
            log.warning(
                f"⚠️ klines {symbol} {interval} (محاولة {attempt+1}) — "
                f"{type(e).__name__}: {e} — انتظار {wait}ث"
            )
            await asyncio.sleep(wait)
            if attempt == RATE_LIMIT_MAX_RETRIES - 1:
                _set_symbol_cooldown(symbol)
                return None

        except Exception as e:
            last_err = e
            wait = RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
            log.warning(f"⚠️ klines {symbol}: {type(e).__name__} — انتظار {wait}ث")
            await asyncio.sleep(wait)

    log.error(f"❌ فشل klines {symbol} {interval} بعد {RATE_LIMIT_MAX_RETRIES} محاولات")
    return None


async def fetch_df_cached(symbol: str, interval: str, limit: int = 200) -> pd.DataFrame | None:
    """يجلب شموع Binance ويحوّلها إلى DataFrame مع كاش."""
    key = (symbol, interval, limit)
    now = time.time()
    cached = _ohlcv_cache.get(key)
    if cached and (now - cached["ts"]) < OHLCV_CACHE_SECONDS:
        return cached["df"]

    klines = await fetch_klines_safe(symbol, interval, limit)
    if not klines or len(klines) < 60:
        return None

    df = pd.DataFrame(klines, columns=[
        "ts", "o", "h", "l", "c", "v",
        "close_time", "quote_vol", "trades",
        "taker_base", "taker_quote", "ignore",
    ])
    # تحويل الأنواع إلى float
    for col in ("o", "h", "l", "c", "v"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce")

    if df["c"].isna().all():
        return None

    _ohlcv_cache[key] = {"df": df, "ts": now}
    return df


def clear_ohlcv_cache():
    _ohlcv_cache.clear()


# ============================================================
# 🆕 دوال الكشف الثلاثي (pre / live / confirmed)
# ============================================================
async def _do_detect(symbol: str, timeframe: str, kind: str) -> dict | None:
    """
    kind = "confirmed" | "live" | "pre"
    يعيد dict الإشارة أو None
    """
    df = await fetch_df_cached(symbol, timeframe, EMA_SLOW + 80)
    if df is None or len(df) < EMA_SLOW + 20:
        return None

    df["ef"] = calc_ema_series(df["c"], EMA_FAST)
    df["es"] = calc_ema_series(df["c"], EMA_SLOW)

    if kind == "confirmed":
        curr, prev = -2, -3
        cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
        pf = float(df["ef"].iloc[prev]); ps = float(df["es"].iloc[prev])
        bullish = (cf > cs) and (pf <= ps)
        bearish = (cf < cs) and (pf >= ps)
        if not (bullish or bearish):
            return None
        direction = "bullish" if bullish else "bearish"
        candle_ts = int(df["ts"].iloc[curr])
        cache_key = (symbol, timeframe, "confirmed")
        last = _crossover_cache.get(cache_key)
        if last and last.get("candle_ts") == candle_ts and last.get("direction") == direction:
            return None
        _crossover_cache[cache_key] = {"direction": direction, "candle_ts": candle_ts}
        strength = classify_strength(timeframe, abs(cf - cs) / cs * 100)
        alert_type = "confirmed"

    elif kind == "live":
        curr, prev = -1, -2
        cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
        pf = float(df["ef"].iloc[prev]); ps = float(df["es"].iloc[prev])
        bullish = (cf > cs) and (pf <= ps)
        bearish = (cf < cs) and (pf >= ps)
        if not (bullish or bearish):
            return None
        direction = "bullish" if bullish else "bearish"
        candle_ts = int(df["ts"].iloc[curr])
        cache_key = (symbol, timeframe, "live")
        last = _crossover_cache.get(cache_key)
        if last and last.get("candle_ts") == candle_ts:
            return None
        _crossover_cache[cache_key] = {"direction": direction, "candle_ts": candle_ts}
        strength = "⚡ مبدئي (قابل للتغير)"
        alert_type = "live"

    elif kind == "pre":
        curr = -2
        cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
        if cs == 0:
            return None
        gap_pct = abs(cf - cs) / cs * 100
        if gap_pct >= PRE_CROSS_GAP:
            return None

        gaps = []
        for i in range(PRE_CROSS_LOOKBACK):
            idx = curr - i
            f = float(df["ef"].iloc[idx]); s = float(df["es"].iloc[idx])
            if s == 0:
                return None
            gaps.append(abs(f - s) / s * 100)
        gaps_chrono = list(reversed(gaps))
        if not all(gaps_chrono[i] >= gaps_chrono[i + 1] for i in range(len(gaps_chrono) - 1)):
            return None

        if abs(cf - cs) < 1e-12:
            return None

        direction = "bullish" if cf < cs else "bearish"
        candle_ts = int(df["ts"].iloc[curr])
        cache_key = (symbol, timeframe, "pre")
        last = _crossover_cache.get(cache_key)
        if last:
            tf_ms = {
                "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
                "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
                "4h": 14_400_000, "1d": 86_400_000,
            }.get(timeframe, 900_000)
            if candle_ts - last.get("candle_ts", 0) < tf_ms * PRE_CROSS_COOLDOWN:
                return None
        _crossover_cache[cache_key] = {"direction": direction, "candle_ts": candle_ts}
        strength = "🔔 تقارب وشيك"
        alert_type = "pre"
        cf = float(df["ef"].iloc[curr])
        cs = float(df["es"].iloc[curr])

    else:
        return None

    support = analyze_with_score(df, direction, curr)

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": direction,
        "ema_fast": round(cf, 8),
        "ema_slow": round(cs, 8),
        "price": round(float(df["c"].iloc[curr]), 8),
        "candle_ts": candle_ts,
        "gap_pct": round(abs(cf - cs) / cs * 100, 3) if cs else 0,
        "strength": strength,
        "exchange": "BINANCE",
        "market_type": "swap",
        "alert_type": alert_type,
        "support": support,
    }


async def detect_confirmed(symbol: str, timeframe: str) -> dict | None:
    if not ENABLE_CONFIRMED:
        return None
    return await _do_detect(symbol, timeframe, "confirmed")


async def detect_live(symbol: str, timeframe: str) -> dict | None:
    if not ENABLE_LIVE_CROSS:
        return None
    return await _do_detect(symbol, timeframe, "live")


async def detect_pre(symbol: str, timeframe: str) -> dict | None:
    if not ENABLE_PRE_CROSS:
        return None
    return await _do_detect(symbol, timeframe, "pre")


# ============================================================
# 🆕 قواعد الإرسال
# ============================================================
def _should_send(cross: dict) -> tuple:
    passed, reason, _cat = passes_hard_filter(cross)
    if not passed:
        return False, False, reason

    score = cross.get("support", {}).get("score", 0)
    mode = SCORE_MODE

    if mode == "silent":
        return True, False, ""

    if mode == "strict":
        if score >= MIN_SCORE:
            return True, False, ""
        return False, False, f"أقل من العتبة ({score} < {MIN_SCORE})"

    if mode == "hybrid":
        if score >= GOLD_SCORE:
            return True, False, ""
        if score >= MIN_SCORE:
            return True, False, ""
        return False, True, "علامة منخفضة"

    return True, False, ""


# ============================================================
# 🆕 بناء رسالة الإشارة (بنفس تنسيق بوت Bybit)
# ============================================================
def _stars(points: int, max_points: int) -> str:
    if max_points == 0:
        return ""
    ratio = points / max_points
    if ratio >= 0.9:  return "⭐⭐⭐"
    if ratio >= 0.6:  return "⭐⭐"
    if ratio >= 0.3:  return "⭐"
    return "▫️"


def build_cross_message(cross: dict) -> str:
    symbol = cross["symbol"]
    tf = cross["timeframe"]
    is_bull = (cross["direction"] == "bullish")
    alert_type = cross.get("alert_type", "confirmed")
    s = cross.get("support", {})
    score = s.get("score", 0)
    grade = s.get("grade", "?")

    if alert_type == "pre":
        emoji = "🔔"
        title = "تقارب وشيك — تحذير مبكر"
        type_label = "🔔 <b>تحذير مبكر</b> — لم يحدث التقاطع بعد"
        dir_label = "🟢 اتجاه محتمل: صاعد" if is_bull else "🔴 اتجاه محتمل: هابط"
    elif alert_type == "live":
        emoji = "⚡"
        title = "تقاطع مبدئي"
        type_label = "⚡ <b>تقاطع مبدئي</b> — على الشمعة الجارية"
        dir_label = "🟢 صاعد" if is_bull else "🔴 هابط"
    else:
        emoji = "🚀" if is_bull else "🔻"
        title = "تقاطع صاعد 🟢" if is_bull else "تقاطع هابط 🔴"
        type_label = "✅ <b>تقاطع مؤكد</b> — على شمعة مغلقة"
        dir_label = "🟢 صاعد" if is_bull else "🔴 هابط"

    candle_time = ts_to_str(cross["candle_ts"])

    filter_note = ""
    if s and SCORE_MODE == "silent" and ENABLE_HARD_FILTER:
        notes = []
        if s.get("vol_ratio", 0) < MIN_VOL_RATIO:
            notes.append(f"الحجم ضعيف ({s.get('vol_ratio')}×)")
        if s.get("adx", 0) < MIN_ADX_HARD:
            notes.append(f"ADX منخفض ({s.get('adx')})")
        if s.get("atr_pct", 0) > MAX_ATR_PCT_HARD:
            notes.append(f"تقلب مرتفع ({s.get('atr_pct')}%)")
        if BLOCK_AGAINST_HTF and s.get("htf_pts", 0) == 0 and alert_type != "pre":
            notes.append("ضد الاتجاه الأكبر (EMA50)")
        if notes:
            filter_note = "\n⚠️ <i>" + " | ".join(notes) + "</i>\n"

    support_block = ""
    if s:
        support_block = (
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📌 مؤشرات داعمة (النقاط):</b>\n"
            f"• الحجم: <b>{s.get('vol_ratio','?')}×</b> "
            f"{_stars(s.get('vol_pts',0),30)} {s.get('vol_label','?')} "
            f"<i>({s.get('vol_pts',0)}/30)</i>\n"
            f"• ADX: <b>{s.get('adx','?')}</b> "
            f"{_stars(s.get('adx_pts',0),20)} {s.get('adx_label','?')} "
            f"<i>({s.get('adx_pts',0)}/20)</i>\n"
            f"• فرق EMA: <b>{s.get('gap_pct','?')}%</b> "
            f"{_stars(s.get('gap_pts',0),15)} "
            f"<i>({s.get('gap_pts',0)}/15)</i>\n"
            f"• MACD: {s.get('macd_label','?')} "
            f"{_stars(s.get('macd_pts',0),10)} <i>({s.get('macd_pts',0)}/10)</i>\n"
            f"• RSI: <b>{s.get('rsi','?')}</b> — {s.get('rsi_label','?')} "
            f"{_stars(s.get('rsi_pts',0),10)} <i>({s.get('rsi_pts',0)}/10)</i>\n"
            f"• EMA50: {s.get('htf_label','?')} "
            f"{_stars(s.get('htf_pts',0),10)} <i>({s.get('htf_pts',0)}/10)</i>\n"
            f"• ATR: {s.get('atr_pct','?')}% — {s.get('atr_label','?')} "
            f"{_stars(s.get('atr_pts',0),5)} <i>({s.get('atr_pts',0)}/5)</i>\n"
        )

    score_line = f"\n🎯 <b>العلامة النهائية: {score}/100 {grade}</b>\n" if s else ""

    return (
        f"{emoji} <b>{title} — {short(symbol)} [{tf}]</b>\n"
        f"🕒 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{type_label}\n"
        f"📊 <b>{dir_label}</b>\n"
        f"⚡ القوة الوصفية: <b>{cross['strength']}</b>\n"
        f"📏 فرق EMA: <b>{cross['gap_pct']:.3f}%</b>"
        f"{score_line}"
        f"{filter_note}"
        f"{support_block}"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• EMA{EMA_FAST}: {fmt_price(cross['ema_fast'])}\n"
        f"• EMA{EMA_SLOW}: {fmt_price(cross['ema_slow'])}\n"
        f"• السعر: {fmt_price(cross['price'])}\n"
        f"• الفريم: <b>{tf}</b>\n"
        f"• وقت الشمعة: {candle_time}\n"
        f"• المصدر: BINANCE (swap)"
    )


# ============================================================
# جلب البيانات العامة (positions / account / filters / trend)
# ============================================================
async def fetch_positions(force: bool = False) -> list:
    if force:
        _http_cache.pop("positions", None)
    data = await cached_http(
        "positions",
        lambda: binance_client.futures_position_information(),
        ttl=20,
    )
    return [p for p in data if float(p["positionAmt"]) != 0]


async def fetch_account(force: bool = False) -> dict:
    if force:
        _http_cache.pop("account", None)
    return await cached_http(
        "account",
        lambda: binance_client.futures_account(),
        ttl=30,
    )


async def get_symbol_filters(symbol: str) -> tuple[str, str]:
    if symbol in _symbol_filters_cache:
        return _symbol_filters_cache[symbol]
    info = await binance_client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            step, tick = "0.001", "0.01"
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = f["stepSize"]
                elif f["filterType"] == "PRICE_FILTER":
                    tick = f["tickSize"]
            _symbol_filters_cache[symbol] = (step, tick)
            return step, tick
    raise ValueError(f"الرمز {symbol} غير موجود")


async def get_daily_realized_pnl() -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if _daily_pnl_cache["date"] == today and now - _daily_pnl_cache["last_fetch"] < 300:
        return _daily_pnl_cache["pnl"]

    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(start.timestamp() * 1000)

    try:
        income = await binance_client.futures_income_history(
            startTime=start_ms, limit=1000
        )
        total = 0.0
        for i in income:
            itype = i.get("incomeType", "")
            if itype in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
                try:
                    total += float(i["income"])
                except (KeyError, ValueError):
                    pass
        _daily_pnl_cache["date"] = today
        _daily_pnl_cache["pnl"] = total
        _daily_pnl_cache["last_fetch"] = now
        return total
    except Exception as e:
        log.warning(f"daily pnl: {e}")
        return _daily_pnl_cache.get("pnl", 0.0)


async def get_trend_direction(symbol: str) -> str | None:
    """يستخدم TREND_TIMEFRAME + TREND_EMA_PERIOD. لا علاقة له بالتقاطع الثلاثي."""
    if not TREND_FILTER_ENABLED:
        return None

    now = time.time()
    cached = _trend_cache.get(symbol)
    if cached and now - cached[0] < 300:
        return cached[1]

    df = await fetch_df_cached(symbol, TREND_TIMEFRAME, TREND_EMA_PERIOD + 50)
    if df is None or len(df) < TREND_EMA_PERIOD + 5:
        return None

    # استخدم الشمعة المغلقة
    closes = df["c"].iloc[:-1].tolist()
    ema = calc_ema(closes, TREND_EMA_PERIOD)
    if not ema:
        return None

    trend = "UP" if closes[-1] > ema[-1] else "DOWN"
    _trend_cache[symbol] = (now, trend)
    return trend


# ============================================================
# TP/SL على Binance (وضع orders)
# ============================================================
async def _try_order(
    symbol: str, side: str, order_type: str,
    stop_price: float, qty: float,
) -> tuple[bool, str]:
    try:
        await binance_client.futures_create_order(
            symbol=symbol, side=side, type=order_type,
            stopPrice=stop_price, quantity=qty,
            reduceOnly=True, workingType="MARK_PRICE",
        )
        return True, "ok"
    except BinanceAPIException as e:
        return False, f"{e.code}"
    except Exception as e:
        return False, str(e)[:50]


async def try_place_tp_sl_on_binance(
    symbol: str, close_side: str, qty: float,
    tp_price: float, sl_price: float,
) -> tuple[bool, bool]:
    tp_ok, tp_err = await _try_order(
        symbol, close_side, "TAKE_PROFIT_MARKET", tp_price, qty
    )
    sl_ok, sl_err = await _try_order(
        symbol, close_side, "STOP_MARKET", sl_price, qty
    )
    if not tp_ok:
        log.info(f"TP على Binance فشل لـ {symbol}: {tp_err}")
    if not sl_ok:
        log.info(f"SL على Binance فشل لـ {symbol}: {sl_err}")
    return tp_ok, sl_ok


# ============================================================
# المراقبة الذاتية TP/SL
# ============================================================
def _get_lock(symbol: str) -> asyncio.Lock:
    if symbol not in _close_locks:
        _close_locks[symbol] = asyncio.Lock()
    return _close_locks[symbol]


async def sync_positions_and_watch():
    if binance_client is None or is_banned():
        return

    try:
        positions = await fetch_positions(force=True)
    except Exception as e:
        log.debug(f"sync positions: {e}")
        return

    current_symbols = {p["symbol"] for p in positions}

    for sym in list(_watched_positions.keys()):
        if sym not in current_symbols:
            log.info(f"👁️ توقف عن مراقبة {sym} (المركز أُغلق)")
            _watched_positions.pop(sym, None)
            _close_locks.pop(sym, None)

    for p in positions:
        symbol = p["symbol"]
        try:
            amt = float(p["positionAmt"])
        except (KeyError, ValueError):
            continue
        if amt == 0:
            continue

        entry = float(p.get("entryPrice", 0) or 0)
        if entry <= 0:
            continue

        abs_amt = abs(amt)
        notional = abs_amt * entry
        if notional < AUTO_PROTECT_MIN_NOTIONAL:
            continue

        is_long = amt > 0
        if is_long:
            tp = entry * (1 + AUTO_PROTECT_TP_PCT / 100)
            sl = entry * (1 - AUTO_PROTECT_SL_PCT / 100)
        else:
            tp = entry * (1 - AUTO_PROTECT_TP_PCT / 100)
            sl = entry * (1 + AUTO_PROTECT_SL_PCT / 100)

        existing = _watched_positions.get(symbol)
        if existing:
            same_amt = abs(existing["qty"] - abs_amt) / max(abs_amt, 1e-9) < 1e-4
            same_entry = abs(existing["entry"] - entry) / max(entry, 1e-9) < 1e-4
            if same_amt and same_entry:
                continue

        _watched_positions[symbol] = {
            "side": "LONG" if is_long else "SHORT",
            "entry": entry,
            "qty": abs_amt,
            "tp": tp,
            "sl": sl,
            "closed": False,
            "closing": False,
            "notified": False,
            "created": time.time(),
            "last_ws_update": 0.0,
        }
        log.info(f"👁️ جديد: {symbol} {('LONG' if is_long else 'SHORT')} "
                 f"| TP={tp:.4f} SL={sl:.4f}")

        side_emoji = "🟢 LONG" if is_long else "🔴 SHORT"
        try:
            await send(
                f"👁️ <b>مراقبة صفقة جديدة</b>\n\n"
                f"{side_emoji} | <b>{symbol}</b>\n"
                f"الدخول: {entry}\n"
                f"الكمية: {abs_amt}\n"
                f"Notional: ~{notional:.0f}$\n\n"
                f"🎯 TP: {tp:.4f} (+{AUTO_PROTECT_TP_PCT:.2f}%)\n"
                f"🛑 SL: {sl:.4f} (-{AUTO_PROTECT_SL_PCT:.2f}%)\n\n"
                f"<i>سأراقب السعر وأُغلق تلقائياً عند الوصول.</i>"
            )
        except Exception:
            pass


async def check_price_and_close(symbol: str, price: float, source: str = "ws"):
    pos = _watched_positions.get(symbol)
    if not pos or pos.get("closed") or pos.get("closing"):
        return

    pos["last_ws_update"] = time.time()
    side = pos["side"]
    tp = pos["tp"]
    sl = pos["sl"]

    hit = None
    if side == "LONG":
        if price >= tp:
            hit = "TP"
        elif price <= sl:
            hit = "SL"
    else:
        if price <= tp:
            hit = "TP"
        elif price >= sl:
            hit = "SL"

    if hit:
        log.info(f"⚡ {symbol} وصل {hit} @ {price} (source={source})")
        await close_position_market(symbol, pos, hit, price)


async def close_position_market(symbol: str, pos: dict, reason: str, price: float):
    lock = _get_lock(symbol)
    async with lock:
        if pos.get("closed") or pos.get("closing"):
            return
        pos["closing"] = True

        try:
            side = "SELL" if pos["side"] == "LONG" else "BUY"
            try:
                await binance_client.futures_cancel_all_open_orders(symbol=symbol)
            except Exception as e:
                log.debug(f"cancel orders {symbol}: {e}")

            order = await binance_client.futures_create_order(
                symbol=symbol, side=side, type="MARKET",
                quantity=pos["qty"], reduceOnly=True,
            )

            pos["closed"] = True
            pos["close_time"] = time.time()

            emoji = "🎯" if reason == "TP" else "🛑"
            title = "جني أرباح" if reason == "TP" else "وقف خسارة"
            status = order.get("status", "?")
            avg = order.get("avgPrice", "0")

            try:
                await send(
                    f"{emoji} <b>{title} (مراقبة ذاتية)</b>\n\n"
                    f"<b>{symbol}</b> | {pos['side']}\n"
                    f"الكمية: {pos['qty']}\n"
                    f"سعر التنفيذ: {avg or price}\n"
                    f"الدخول: {pos['entry']}\n"
                    f"الحالة: {status}"
                )
            except Exception:
                pass

            log.info(f"✅ {reason} {symbol} — أُغلق @ {avg or price}")

            await asyncio.sleep(10)
            _watched_positions.pop(symbol, None)
            _close_locks.pop(symbol, None)

        except Exception as e:
            log.exception(f"close {symbol}: {e}")
            pos["closing"] = False
            try:
                await send(
                    f"🚨 <b>فشل إغلاق {symbol}</b>\n\n"
                    f"السبب: {reason}\n"
                    f"السعر: {price}\n"
                    f"❌ {e}\n\n"
                    f"<b>أغلق يدوياً من التطبيق فوراً!</b>"
                )
            except Exception:
                pass


# ============================================================
# WebSocket bookTicker
# ============================================================
async def mark_price_watcher(app: Application):
    while True:
        try:
            symbols = list(_watched_positions.keys())
            if not symbols:
                await asyncio.sleep(5)
                continue

            active = [
                s for s in symbols
                if _watched_positions.get(s)
                and not _watched_positions[s].get("closed")
                and not _watched_positions[s].get("closing")
            ]
            if not active:
                await asyncio.sleep(5)
                continue

            streams = [f"{s.lower()}@bookTicker" for s in active]
            bsm = BinanceSocketManager(binance_client)

            log.info(f"📡 bookTicker watcher: {len(active)} رموز")
            try:
                async with bsm.futures_multiplex_socket(streams) as stream:
                    while True:
                        msg = await stream.recv()
                        data = msg.get("data", msg)

                        symbol = data.get("s")
                        if not symbol:
                            continue

                        bid = data.get("b")
                        ask = data.get("a")
                        if not bid or not ask:
                            continue

                        try:
                            price = (float(bid) + float(ask)) / 2.0
                        except (ValueError, TypeError):
                            continue

                        await check_price_and_close(symbol, price, "ws")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"bookTicker ws inner: {e}")
                await asyncio.sleep(MARK_WS_RECONNECT_DELAY)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"bookTicker watcher: {e}")
            await asyncio.sleep(MARK_WS_RECONNECT_DELAY)


# ============================================================
# Job الدوري: مزامنة TP/SL
# ============================================================
async def watch_sync_job(context: ContextTypes.DEFAULT_TYPE | None = None):
    if not AUTO_PROTECT_ENABLED or binance_client is None or is_banned():
        return

    if CLOSE_MODE == "orders":
        await try_place_orders_for_all()
        return

    try:
        await sync_positions_and_watch()
    except Exception as e:
        log.exception(f"sync: {e}")
        return

    if CLOSE_MODE == "both":
        await try_place_orders_for_all()

    now = time.time()
    for symbol, pos in list(_watched_positions.items()):
        if pos.get("closed") or pos.get("closing"):
            continue
        last = pos.get("last_ws_update", 0)
        if now - last < 90:
            continue
        try:
            ticker = await binance_client.futures_symbol_ticker(symbol=symbol)
            price = float(ticker["price"])
            await check_price_and_close(symbol, price, "rest")
        except Exception as e:
            log.debug(f"rest price {symbol}: {e}")
        await asyncio.sleep(1)


async def try_place_orders_for_all():
    try:
        positions = await fetch_positions()
    except Exception:
        return
    for p in positions:
        symbol = p["symbol"]
        try:
            amt = float(p["positionAmt"])
        except (KeyError, ValueError):
            continue
        if amt == 0:
            continue
        entry = float(p.get("entryPrice", 0) or 0)
        if entry <= 0:
            continue
        abs_amt = abs(amt)
        if abs_amt * entry < AUTO_PROTECT_MIN_NOTIONAL:
            continue

        watched = _watched_positions.get(symbol)
        if not watched:
            continue
        if watched.get("orders_tried"):
            continue
        watched["orders_tried"] = True

        is_long = amt > 0
        close_side = "SELL" if is_long else "BUY"

        tp_ok, sl_ok = await try_place_tp_sl_on_binance(
            symbol, close_side, abs_amt,
            watched["tp"], watched["sl"],
        )
        log.info(f"{symbol}: orders mode TP={tp_ok} SL={sl_ok}")
        await asyncio.sleep(1)


# ============================================================
# التنسيق
# ============================================================
def fmt_position(p: dict) -> str | None:
    try:
        amt = float(p["positionAmt"])
    except (KeyError, ValueError):
        return None
    if amt == 0:
        return None

    side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
    entry = float(p.get("entryPrice", 0) or 0)
    mark = float(p.get("markPrice", 0) or 0)
    pnl = float(p.get("unRealizedProfit", 0) or 0)
    lev = p.get("leverage", "?")
    icon = "📈" if pnl >= 0 else "📉"

    lines = [
        f"{side} | <b>{p.get('symbol', '?')}</b>",
        f"  الحجم: {abs(amt)}",
        f"  الدخول: {entry}",
    ]
    if mark:
        lines.append(f"  الحالي: {mark}")
    lines.append(f"  الرافعة: x{lev}")
    lines.append(f"  {icon} PnL: <b>{pnl:+.4f} USDT</b>")

    watched = _watched_positions.get(p["symbol"])
    if watched and not watched.get("closed"):
        lines.append(f"  🎯 TP: {watched['tp']:.4f}")
        lines.append(f"  🛑 SL: {watched['sl']:.4f}")
    return "\n".join(lines)


HELP_TEXT = (
    "🤖 <b>بوت Binance — تقاطع EMA ثلاثي + مراقبة ذاتية</b>\n"
    "━━━━━━━━━━━━━━━━━━━\n\n"
    "<b>الأوامر:</b>\n"
    "/positions — الصفقات + TP/SL\n"
    "/balance — الرصيد\n"
    "/pnl — الربح/الخسارة\n"
    "/watch — الصفقات المراقَبة\n"
    "/status — حالة البوت\n"
    "/strategy — الاستراتيجية\n"
    "/rescan — إعادة مزامنة الآن\n"
    "/close SYMBOL — إغلاق صفقة\n"
    "/mute /unmute — الإشعارات"
)


# ============================================================
# الأوامر
# ============================================================
def authorized(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.id == CHAT_ID


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        positions = await fetch_positions(force=True)
        if not positions:
            await update.message.reply_text("لا توجد صفقات مفتوحة. ✨")
            return
        body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
        total_pnl = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        await update.message.reply_text(
            f"📊 <b>الصفقات المفتوحة ({len(positions)})</b>\n\n"
            f"{body}\n\n──────────────\n"
            f"📈 إجمالي PnL: <b>{total_pnl:+.4f} USDT</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        account = await fetch_account(force=True)
        wallet = float(account.get("totalWalletBalance", 0) or 0)
        unrealized = float(account.get("totalUnrealizedProfit", 0) or 0)
        available = float(account.get("availableBalance", 0) or 0)
        used = float(account.get("totalPositionInitialMargin", 0) or 0)
        daily = await get_daily_realized_pnl()

        await update.message.reply_text(
            f"💰 <b>الرصيد</b>\n\n"
            f"المحفظة: <b>{wallet:.2f}</b> USDT\n"
            f"PnL عائم: <b>{unrealized:+.4f}</b>\n"
            f"هامش مستخدم: <b>{used:.2f}</b>\n"
            f"متاح: <b>{available:.2f}</b>\n\n"
            f"📅 PnL اليوم: <b>{daily:+.4f}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    try:
        positions = await fetch_positions(force=True)
        unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        daily = await get_daily_realized_pnl()
        lines = []
        for p in positions:
            pnl = float(p.get("unRealizedProfit", 0) or 0)
            icon = "📈" if pnl >= 0 else "📉"
            lines.append(f"{icon} <b>{p.get('symbol', '?')}</b>: {pnl:+.4f}")
        await update.message.reply_text(
            f"📊 <b>PnL</b>\n\n"
            f"عائم: <b>{unrealized:+.4f}</b>\n"
            f"محقق اليوم: <b>{daily:+.4f}</b>\n\n"
            + ("\n".join(lines) if lines else "—"),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    if not _watched_positions:
        await update.message.reply_text(
            "👁️ لا توجد صفقات تحت المراقبة.\n\n"
            "<i>استخدم /rescan لإعادة المزامنة.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = [f"👁️ <b>تحت المراقبة ({len(_watched_positions)})</b>\n"]
    for sym, pos in _watched_positions.items():
        status = "🟢 نشط"
        if pos.get("closed"):
            status = "✅ أُغلق"
        elif pos.get("closing"):
            status = "⏳ جارٍ الإغلاق"

        side_emoji = "🟢 LONG" if pos["side"] == "LONG" else "🔴 SHORT"
        lines.append(
            f"\n<b>{sym}</b> {side_emoji}\n"
            f"  الحالة: {status}\n"
            f"  الدخول: {pos['entry']:.4f}\n"
            f"  🎯 TP: {pos['tp']:.4f}\n"
            f"  🛑 SL: {pos['sl']:.4f}"
        )

    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML
    )


async def cmd_rescan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text("🔄 جاري المزامنة...")
    await sync_positions_and_watch()
    await update.message.reply_text(
        f"✅ {len(_watched_positions)} صفقة تحت المراقبة.\n"
        f"أرسل /watch للتفاصيل."
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    ws_status = "🟢" if _user_stream_task and not _user_stream_task.done() else "🔴"
    mark_status = "🟢" if _mark_watcher_task and not _mark_watcher_task.done() else "🔴"
    ban = (f"\n⛔ محظور — ~{ban_remaining_sec()//60} دقيقة"
           if is_banned() else "")
    gp = (f"\n⏸️ إيقاف عالمي — ~{global_pause_remaining()}ث"
          if is_globally_paused() else "")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # 🆕 إحصائيات الفلتر
    total_filtered = (
        _filter_stats["filtered_vol"]
        + _filter_stats["filtered_adx"]
        + _filter_stats["filtered_atr"]
        + _filter_stats["filtered_htf"]
        + _filter_stats["filtered_15m_score"]
        + _filter_stats["filtered_other"]
    )

    active_cd = {s: int(ts_ - time.time())
                 for s, ts_ in _symbol_cooldown.items() if ts_ > time.time()}
    cd_lines = "\n".join(
        [f"   - {short(s)}: {v}ث" for s, v in list(active_cd.items())[:5]]
    ) or "   — لا شيء"

    await update.message.reply_text(
        f"🤖 <b>حالة البوت</b>\n\n"
        f"Binance: {'🟢' if binance_client else '🔴'}\n"
        f"WebSocket أوامر: {ws_status}\n"
        f"WebSocket أسعار: {mark_status}\n"
        f"الإشعارات: {'🔔' if notifications_enabled else '🔕'}\n"
        f"الحماية: {'🟢' if AUTO_PROTECT_ENABLED else '🔴'}\n"
        f"الوضع: <b>{CLOSE_MODE}</b>\n"
        f"تحت المراقبة: {len(_watched_positions)}\n"
        f"الوقت: {ts}{ban}{gp}\n\n"
        f"<b>🎯 التقاطع الثلاثي:</b>\n"
        f"• pre: {'✅' if ENABLE_PRE_CROSS else '❌'} | "
        f"live: {'✅' if ENABLE_LIVE_CROSS else '❌'} | "
        f"confirmed: {'✅' if ENABLE_CONFIRMED else '❌'}\n"
        f"• الفريمات: {', '.join(CROSS_TIMEFRAMES)}\n"
        f"• العتبة: {MIN_SCORE} | ذهبية: {GOLD_SCORE} | الوضع: {SCORE_MODE}\n\n"
        f"<b>🔒 الفلتر الإلزامي:</b>\n"
        f"• {'✅ مفعل' if ENABLE_HARD_FILTER else '❌ معطل'}\n"
        f"• حجم ≥ {MIN_VOL_RATIO}× | ADX ≥ {MIN_ADX_HARD} | "
        f"ATR ≤ {MAX_ATR_PCT_HARD}%\n"
        f"• ضد EMA50: {'محجوب' if BLOCK_AGAINST_HTF else 'مسموح'} | "
        f"15m ≥ {MIN_SCORE_15M}\n\n"
        f"<b>📊 إحصائيات الإشارات:</b>\n"
        f"• مُرسَلة: {_filter_stats['sent']}\n"
        f"• محجوبة: {total_filtered}\n"
        f"   - حجم: {_filter_stats['filtered_vol']}\n"
        f"   - ADX: {_filter_stats['filtered_adx']}\n"
        f"   - ATR: {_filter_stats['filtered_atr']}\n"
        f"   - ضد EMA50: {_filter_stats['filtered_htf']}\n"
        f"   - 15m ضعيفة: {_filter_stats['filtered_15m_score']}\n\n"
        f"<b>🛡️ Rate Limit:</b>\n"
        f"• مرات تجاوز: {_filter_stats['rate_limit_hits']}\n"
        f"• إيقاف عالمي: {_filter_stats['global_pauses']}\n"
        f"• كولداون الرموز ({len(active_cd)}):\n{cd_lines}\n\n"
        f"<b>💾 كاش:</b> {len(_ohlcv_cache)} OHLCV",
        parse_mode=ParseMode.HTML,
    )


async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    state_lines = []
    for s in STRATEGY_SYMBOLS:
        # اعرض حالات التقاطع على الفريم الأساسي
        st = _last_ema_states.get((s, STRATEGY_INTERVAL), "—")
        st_str = {"FAST_ABOVE": "🟢", "FAST_BELOW": "🔴"}.get(st, "⚪")
        trend = _trend_cache.get(s)
        trend_str = ("📈" if trend[1] == "UP" else "📉") if trend else "—"
        state_lines.append(f"  {st_str} <b>{s}</b> {trend_str}")

    effective_sl = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
    notional = MARGIN_USDT * LEVERAGE
    daily = await get_daily_realized_pnl()

    await update.message.reply_text(
        f"📈 <b>الاستراتيجية</b>\n\n"
        f"{'🟢 مفعلة' if STRATEGY_ENABLED else '🔴 معطلة'}\n"
        f"EMA {EMA_FAST}/{EMA_SLOW}\n"
        f"فريمات التقاطع: {', '.join(CROSS_TIMEFRAMES)}\n"
        f"فريم فتح الصفقات: {STRATEGY_INTERVAL}\n\n"
        f"<b>الرموز:</b>\n" + "\n".join(state_lines) + "\n\n"
        f"الهامش: {MARGIN_USDT}$ × x{LEVERAGE} = {notional:.0f}$\n"
        f"🎯 +{TP_USDT}$ | 🛑 -{effective_sl:.2f}$\n"
        f"PnL اليوم: {daily:+.4f}\n\n"
        f"🛡️ المراقبة: TP +{AUTO_PROTECT_TP_PCT}% / SL -{AUTO_PROTECT_SL_PCT}%\n"
        f"الوضع: {CLOSE_MODE}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    if not context.args:
        await update.message.reply_text("الاستخدام: /close BTCUSDT")
        return
    symbol = context.args[0].upper()
    try:
        positions = await fetch_positions(force=True)
        target = next((p for p in positions if p["symbol"] == symbol), None)
        if not target:
            await update.message.reply_text(f"لا توجد صفقة على {symbol}.")
            return

        amt = float(target["positionAmt"])
        side = "SELL" if amt > 0 else "BUY"

        try:
            await binance_client.futures_cancel_all_open_orders(symbol=symbol)
        except Exception:
            pass

        order = await binance_client.futures_create_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=abs(amt), reduceOnly=True,
        )
        _watched_positions.pop(symbol, None)
        await update.message.reply_text(
            f"✅ أُغلق {symbol}\n{order.get('status', '')}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = False
    await update.message.reply_text("🔕")


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = True
    await update.message.reply_text("🔔")


# ============================================================
# 🆕 Job التقاطع الثلاثي (بديل check_crossover القديم)
# ============================================================
async def run_crossovers_for_symbol(symbol: str, context=None) -> list:
    """
    يفحص كل الفريمات ويُرجع قائمة الإشارات المُرسلة
    (يُستخدم من strategy_job و يمكن إضافة أمر /cross لاحقاً)
    """
    sent_signals = []
    detectors = []
    if ENABLE_PRE_CROSS:  detectors.append(("pre", detect_pre))
    if ENABLE_LIVE_CROSS: detectors.append(("live", detect_live))
    if ENABLE_CONFIRMED:  detectors.append(("confirmed", detect_confirmed))

    for tf in CROSS_TIMEFRAMES:
        for kind, detector in detectors:
            try:
                cross = await detector(symbol, tf)
                if not cross:
                    continue

                send_now, to_digest, reason = _should_send(cross)
                score = cross.get("support", {}).get("score", 0)

                if send_now:
                    try:
                        if context is not None:
                            await context.bot.send_message(
                                chat_id=CHAT_ID,
                                text=build_cross_message(cross),
                                parse_mode=ParseMode.HTML,
                            )
                        else:
                            await send(build_cross_message(cross))
                        log.info(
                            f"📤 [{kind}] {symbol} [{tf}] {cross['direction']} "
                            f"| Score={score} | vol={cross['support'].get('vol_ratio')}× "
                            f"| adx={cross['support'].get('adx')} "
                            f"| htf={cross['support'].get('htf_trend')}"
                        )
                        _filter_stats["sent"] += 1
                        sent_signals.append(cross)
                    except Exception as e:
                        log.error(f"send {symbol} {tf}: {e}")

                else:
                    # محجوب — إما فلتر إلزامي أو علامة منخفضة
                    if to_digest:
                        _filter_stats["hidden"] += 1
                        log.debug(f"💤 محجوب (علامة): {symbol} [{tf}] Score={score}")
                    else:
                        filtered = True
                        if "الحجم" in reason:
                            _filter_stats["filtered_vol"] += 1
                        elif "ADX" in reason:
                            _filter_stats["filtered_adx"] += 1
                        elif "ATR" in reason:
                            _filter_stats["filtered_atr"] += 1
                        elif "EMA50" in reason:
                            _filter_stats["filtered_htf"] += 1
                        elif "15m" in reason:
                            _filter_stats["filtered_15m_score"] += 1
                        else:
                            _filter_stats["filtered_other"] += 1
                        log.debug(f"🚫 محجوب (فلتر): {symbol} [{tf}] — {reason}")

            except Exception as e:
                log.exception(f"detect {kind} {symbol} {tf}: {e}")

    return sent_signals


# ============================================================
# 🆕 استراتيجية فتح الصفقات (تعتمد على confirmed cross مع الفلتر)
# ============================================================
async def check_trade_signal(symbol: str) -> str | None:
    """
    يفحص التقاطع المؤكد على STRATEGY_INTERVAL فقط (للاتجاه).
    يُرجع "LONG"/"SHORT" أو None.
    يخضع للفلتر الإلزامي.
    """
    cross = await detect_confirmed(symbol, STRATEGY_INTERVAL)
    if not cross:
        return None

    # تحديث حالة EMA (للعرض في /strategy)
    cf = cross["ema_fast"]
    cs = cross["ema_slow"]
    _last_ema_states[(symbol, STRATEGY_INTERVAL)] = (
        "FAST_ABOVE" if cf > cs else "FAST_BELOW"
    )

    # الفلتر الإلزامي
    passed, reason, _cat = passes_hard_filter(cross)
    if not passed:
        log.info(f"🚫 {symbol}: إشارة محجوبة — {reason}")
        return None

    score = cross.get("support", {}).get("score", 0)
    # في strict/hybrid: اشترط العتبة أيضاً
    if SCORE_MODE == "strict" and score < MIN_SCORE:
        log.info(f"🚫 {symbol}: علامة منخفضة ({score} < {MIN_SCORE})")
        return None

    return "LONG" if cross["direction"] == "bullish" else "SHORT"


async def strategy_job(context: ContextTypes.DEFAULT_TYPE):
    global _last_strategy_candle
    if not STRATEGY_ENABLED or binance_client is None or is_banned() or is_globally_paused():
        return

    last_candle = get_last_closed_candle_ts(STRATEGY_INTERVAL)
    if _last_strategy_candle == last_candle:
        return
    _last_strategy_candle = last_candle

    try:
        daily = await get_daily_realized_pnl()
        if daily <= -MAX_DAILY_LOSS_USDT:
            return
    except Exception:
        pass

    try:
        positions = await fetch_positions()
        open_symbols = {p["symbol"] for p in positions}
        if len(positions) >= MAX_CONCURRENT_TRADES:
            return

        for symbol in STRATEGY_SYMBOLS:
            if len(positions) >= MAX_CONCURRENT_TRADES:
                break
            if symbol in open_symbols:
                continue
            if is_globally_paused() or is_banned():
                break

            # 1) إرسال إشارات التقاطع الثلاثي (كل الفريمات)
            await run_crossovers_for_symbol(symbol, context)
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)

            # 2) فتح صفقة إن وُجد تقاطع مؤكد يستحق
            signal = await check_trade_signal(symbol)
            if signal is None:
                continue

            log.info(f"📶 {symbol}: {signal}")
            success = await place_trade(symbol, signal)
            if success:
                positions = positions + [{"symbol": symbol}]
                open_symbols.add(symbol)
    except Exception as e:
        log.exception(f"strategy_job: {e}")


# ============================================================
# فتح صفقة (منقول كما هو)
# ============================================================
async def place_trade(symbol: str, side: str) -> bool:
    if TREND_FILTER_ENABLED:
        trend = await get_trend_direction(symbol)
        if trend is not None:
            if side == "LONG" and trend != "UP":
                return False
            if side == "SHORT" and trend != "DOWN":
                return False

    try:
        account = await fetch_account()
        available = float(account.get("availableBalance", 0) or 0)
        if available < MARGIN_USDT * 1.1:
            await send(f"⚠️ رصيد غير كافٍ لـ {symbol}")
            return False
    except Exception:
        pass

    try:
        await binance_client.futures_change_leverage(
            symbol=symbol, leverage=LEVERAGE
        )
    except Exception:
        pass

    try:
        step, tick = await get_symbol_filters(symbol)
    except Exception:
        return False

    try:
        ticker = await binance_client.futures_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
    except Exception:
        return False

    notional = MARGIN_USDT * LEVERAGE
    qty = round_step(notional / price, step)
    if qty <= 0:
        return False

    order_side = "BUY" if side == "LONG" else "SELL"
    try:
        order = await binance_client.futures_create_order(
            symbol=symbol, side=order_side, type="MARKET", quantity=qty,
        )
    except Exception as e:
        await send(f"❌ فتح {symbol}: {e}")
        return False

    fill_price = float(order.get("avgPrice") or 0) or price
    if fill_price <= 0:
        await asyncio.sleep(1)
        try:
            positions = await binance_client.futures_position_information(symbol=symbol)
            fill_price = float(positions[0].get("entryPrice", 0) or price)
        except Exception:
            fill_price = price

    await send(
        f"🎯 <b>فتح صفقة</b>\n\n"
        f"{'🟢 LONG' if side == 'LONG' else '🔴 SHORT'} | <b>{symbol}</b>\n"
        f"الدخول: {fill_price}\n"
        f"الكمية: {qty}\n"
        f"الهامش: {MARGIN_USDT}$ | x{LEVERAGE}"
    )

    await sync_positions_and_watch()
    return True


# ============================================================
# تقرير كل ساعة
# ============================================================
async def hourly_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        if is_banned():
            return

        positions = await fetch_positions()
        try:
            account = await fetch_account()
            balance = float(account.get("totalWalletBalance", 0) or 0)
        except Exception:
            balance = 0.0

        unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        daily = await get_daily_realized_pnl()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        if not positions:
            msg = (
                f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n"
                f"لا صفقات.\n"
                f"💰 الرصيد: {balance:.2f}$\n"
                f"📅 PnL اليوم: {daily:+.4f}$"
            )
        else:
            body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
            msg = (
                f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n{body}\n\n"
                f"──────────────\n"
                f"💰 الرصيد: {balance:.2f}$\n"
                f"📊 عائم: {unrealized:+.4f}$\n"
                f"📅 اليوم: {daily:+.4f}$\n"
                f"👁️ مراقبة: {len(_watched_positions)}"
            )

        await context.bot.send_message(
            chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML
        )
    except Exception as e:
        log.exception(f"hourly_job: {e}")


# ============================================================
# WebSocket أوامر المستخدم
# ============================================================
async def user_stream_task(app: Application):
    bsm = BinanceSocketManager(binance_client)
    while True:
        try:
            async with bsm.futures_user_socket() as stream:
                log.info("🔌 WebSocket الأوامر متصل")
                while True:
                    try:
                        msg = await stream.recv()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.warning(f"recv: {e}")
                        break

                    if msg.get("e") == "ACCOUNT_UPDATE":
                        for pos in msg["a"]["P"]:
                            amt = float(pos["pa"])
                            if amt != 0 and pos.get("bc", "0") == "0":
                                asyncio.create_task(sync_positions_and_watch())

                    elif msg.get("e") == "ORDER_TRADE_UPDATE":
                        o = msg["o"]
                        if o["X"] == "FILLED":
                            otype = o.get("ot", "")
                            rp_val = float(o.get("rp", "0") or 0)
                            pnl_txt = (
                                f"\n💰 PnL: <b>{rp_val:+.4f}$</b>"
                                if rp_val != 0 else ""
                            )

                            if "TAKE_PROFIT" in otype:
                                emoji, title = "🎯", "جني أرباح"
                            elif "STOP" in otype:
                                emoji, title = "🛑", "وقف خسارة"
                            else:
                                emoji, title = "⚡", "تنفيذ"

                            if "TAKE_PROFIT" in otype or "STOP" in otype:
                                _watched_positions.pop(o["s"], None)

                            if notifications_enabled:
                                try:
                                    await app.bot.send_message(
                                        chat_id=CHAT_ID,
                                        text=(
                                            f"{emoji} <b>{title}</b>\n"
                                            f"<b>{o['s']}</b> | {o['S']}\n"
                                            f"الكمية: {o['q']}\n"
                                            f"السعر: {o.get('ap') or '0'}{pnl_txt}"
                                        ),
                                        parse_mode=ParseMode.HTML,
                                    )
                                except Exception:
                                    pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"WS: {e}")
            await asyncio.sleep(WS_RECONNECT_DELAY)


# ============================================================
# Lifecycle
# ============================================================
async def post_init(app: Application):
    global binance_client, _app, _user_stream_task, _mark_watcher_task
    _app = app

    binance_client = await AsyncClient.create(API_KEY, API_SECRET)
    log.info("Binance client جاهز")

    _user_stream_task = app.create_task(user_stream_task(app))

    if AUTO_PROTECT_ENABLED:
        _mark_watcher_task = app.create_task(mark_price_watcher(app))

    try:
        await asyncio.sleep(2)
        await sync_positions_and_watch()
    except Exception as e:
        log.warning(f"initial sync: {e}")

    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"🤖 <b>بدأ البوت</b>\n\n"
                f"🛡️ الوضع: <b>{CLOSE_MODE}</b>\n"
                f"TP: +{AUTO_PROTECT_TP_PCT}% | SL: -{AUTO_PROTECT_SL_PCT}%\n"
                f"👁️ تحت المراقبة: {len(_watched_positions)}\n"
                f"🎯 التقاطع الثلاثي: {', '.join(CROSS_TIMEFRAMES)}\n"
                f"🧪 SCORE_MODE: {SCORE_MODE}\n\n"
                f"/help للأوامر"
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.error(f"startup: {e}")


async def post_shutdown(app: Application):
    global binance_client, _user_stream_task, _mark_watcher_task
    for task in (_user_stream_task, _mark_watcher_task):
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=3)
            except Exception:
                pass
    if binance_client:
        try:
            await binance_client.close_connection()
        except Exception:
            pass
        binance_client = None
    log.info("Bot أُغلق")


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        return
    log.error(f"❌ {err}")


# ============================================================
# Main
# ============================================================
def main():
    threading.Thread(target=_run_health, daemon=True).start()
    print(f"🚀 Binance Bot — وضع: {CLOSE_MODE}")

    app = Application.builder() \
        .token(TELEGRAM_TOKEN) \
        .post_init(post_init) \
        .post_shutdown(post_shutdown) \
        .build()

    handlers = [
        ("start", cmd_start), ("help", cmd_help),
        ("positions", cmd_positions), ("balance", cmd_balance),
        ("pnl", cmd_pnl), ("watch", cmd_watch),
        ("status", cmd_status), ("strategy", cmd_strategy),
        ("rescan", cmd_rescan), ("close", cmd_close),
        ("mute", cmd_mute), ("unmute", cmd_unmute),
    ]
    for name, h in handlers:
        app.add_handler(CommandHandler(name, h))
    app.add_error_handler(error_handler)

    if app.job_queue:
        app.job_queue.run_repeating(
            hourly_job, interval=HOURLY_MIN * 60, first=300, name="hourly"
        )

        if STRATEGY_ENABLED:
            app.job_queue.run_repeating(
                strategy_job, interval=STRATEGY_JOB_INTERVAL,
                first=15, name="strategy",
            )

        if AUTO_PROTECT_ENABLED:
            app.job_queue.run_repeating(
                watch_sync_job, interval=AUTO_PROTECT_INTERVAL,
                first=30, name="watch_sync",
            )

    print("✅ جاهز")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        bootstrap_retries=5,
        read_timeout=30, write_timeout=30,
        connect_timeout=30, pool_timeout=30,
    )


if __name__ == "__main__":
    main()
