"""
Unified Smart Bot {__version_full__} — Binance Futures Execution (Fixed Env + Callback)
============================================================================
  🆕 {__version_full__} — إصلاح قراءة مفاتيح البيئة (_read_env_clean)
  🆕 {__version_full__} — إصلاح AttributeError في Callback Queries (_reply)
  🆕 {__version_full__} — أمر /bcheck للتشخيص
  🆕 {__version_full__} — BinanceExecutor.error_msg واضح
  ✓ v6.6.0: تنفيذ حقيقي + إدارة داخلية + مزامنة يدوية
  ✓ v6.5.0: كل الميزات محفوظة
"""
import os, io, asyncio, logging, threading, time as _time, math
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, time as dt_time
from zoneinfo import ZoneInfo
from collections import deque, defaultdict

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
for _n in ("httpx", "telegram", "telegram.ext", "ccxt", "apscheduler",
           "matplotlib", "binance", "urllib3"):
    logging.getLogger(_n).setLevel(logging.WARNING)
log = logging.getLogger("unified")

import ccxt
import pandas as pd
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          CallbackQueryHandler)
from telegram.error import NetworkError, TimedOut
import sys as _sys_6h
_sys_6h.path.insert(0, "/root/bot2")
try:
    import six_hour_report as _6h
except Exception:
    _6h = None

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import matplotlib.ticker as mticker

try:
    from binance.client import Client as BinanceClient
    from binance.exceptions import BinanceAPIException, BinanceRequestException
    _BINANCE_LIB_OK = True
except Exception:
    _BINANCE_LIB_OK = False

# 🆕 v7.9.29 — WebSocket manager
try:
    from binance_ws import BinanceWSManager
    _WS_AVAILABLE = True
except Exception as _we:
    _WS_AVAILABLE = False
    BinanceWSManager = None

load_dotenv("/root/bot2/.env", override=True)  # 🆕 v8.0.32: override=True


# ═══════════════════════════════════════════════════════════
# 0) أدوات قراءة البيئة (يجب أن تكون قبل الاستخدام)
# ═══════════════════════════════════════════════════════════
def _env_float(key, default):
    try: return float(os.getenv(key, default))
    except: return default

def _env_int(key, default):
    try: return int(os.getenv(key, default))
    except: return default


def _read_env_clean(key: str, default: str = "") -> str:
    """
    قراءة متغير بيئة بشكل نظيف:
    - يحذف BOM / whitespace
    - يحذف علامات التنصيص المحيطة
    """
    raw = os.getenv(key)
    if raw is None:
        return default
    raw = raw.replace("\ufeff", "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
        raw = raw[1:-1].strip()
    return raw or default


def _diagnose_env_keys():
    """فحص مفاتيح البيئة وإرجاع تقرير."""
    report = {}
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET"):
        raw = os.getenv(k)
        if raw is None:
            report[k] = "❌ not found"
        else:
            cleaned = _read_env_clean(k)
            if not cleaned:
                report[k] = f"❌ empty (raw len={len(raw)})"
            else:
                report[k] = f"✅ len={len(cleaned)} ***hidden***"
    return report


# ═══════════════════════════════════════════════════════════
# 1) الإعدادات العامة
# ═══════════════════════════════════════════════════════════
__version__ = "8.0.87d"
__version_full__ = f"v{__version__}"

BOT_TOKEN = os.getenv("TELEGRAM_REPORT_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_REPORT_CHAT_ID")

RANGE_PCT = 6.0
RANGE_REPORT_INTERVAL_HOURS = 4
SEND_STARTUP_REPORT = True

PRIMARY_EXCHANGE = "binance"  # 🆕 v8.0.26 — Binance كمصدر أساسي
FALLBACK_EXCHANGES = ["okx"]  # v7.9.9 memory opt
MARKET_TYPE = "swap"

TIMEFRAMES = ["1m"]  # v9.24.0: 1m فقط (لاختبار Supertrend)  # 🆕 v7.9.50 — 15m للـPaper فقط
EMA_FAST, EMA_SLOW = 7, 25
JOB_INTERVAL_MIN = 5

ENABLE_PRE_CROSS  = False  # 🆕 v7.9.10 — معطّل (0 إشارات في 24 ساعة، يستهلك 47s/دورة)
ENABLE_LIVE_CROSS = False  # v9.3.1: معطل
ENABLE_CONFIRMED  = False  # v9.3.1: معطل
PRE_CROSS_GAP = 0.10  # v7.4.0 relaxed (was 0.05)
PRE_CROSS_LOOKBACK = 5  # v7.4.0 relaxed (was 3)
PRE_CROSS_COOLDOWN = 2  # v7.4.0 relaxed (was 3)

ENABLE_RSI_DIVERGENCE = False  # 🆕 v7.9.30 — disabled (Paper: -3.04%)
ENABLE_BB_SQUEEZE     = True  # v9.7.0: مفعل (مع فلاتر جديدة)
ENABLE_SR_BREAKOUT    = True

RSI_DIV_LOOKBACK = 50  # v7.4.0 relaxed (was 35)
RSI_DIV_COOLDOWN_MIN = 20  # v7.4.0 relaxed
BB_SQUEEZE_PCTL = 0.35  # v7.4.0 relaxed (was 0.20)
BB_SQUEEZE_COOLDOWN_MIN = 20  # v7.4.0
SR_BREAKOUT_LOOKBACK = 40  # v7.4.0 (كان 50)
SR_BREAKOUT_COOLDOWN_MIN = 5  # 🆕 v8.0.66: 30→5

MTF_ENABLED = True
SESSION_ADJUST_ENABLED = True

OHLCV_CACHE_SECONDS = 5  # v9.27.0: فحص سريع
CONFLICT_WINDOW_SEC = 300

HTF_ENABLED = True
HTF_TIMEFRAME = "1h"
HTF_BLOCK_OPPOSITE = True
VWAP_SLOPE_LOOKBACK = 5

STRICTNESS = "auto"
STRICTNESS_OVERRIDE: dict = {}

PRICE_ALERT_INTERVAL_MIN = 1
MAX_PRICE_ALERTS = 3
DEFAULT_THRESHOLD = 1.5

RANGE_MONITOR_INTERVAL_MIN = 3
RANGE_APPROACH_ENABLED = True
RANGE_APPROACH_PCT = 25.0
RANGE_APPROACH_COOLDOWN_MIN = 45
RANGE_BREAKOUT_ENABLED = True
RANGE_BREAKOUT_COOLDOWN_MIN = 60

SYMBOL_DELAY_MS = 20  # v9.2.0: 80→20 (تسريع)
SYRIA_TZ = ZoneInfo("Asia/Damascus")

MORNING_REPORT_ENABLED = True
MORNING_REPORT_HOUR_SYRIA = 10
MORNING_REPORT_TF = "1h"
MORNING_REPORT_LIMIT = 200
SWING_LOOKBACK = 3
SWING_MAX_COUNT = 4

SIGNAL_TRACK_MAX_MIN = 90  # legacy (used as fallback)
# 🆕 v7.9.20 — Dynamic expiry per TF (Paper only, more time to reach TP)
SIGNAL_TRACK_MAX_MIN_BY_TF = {"5m": 90, "15m": 180, "1h": 360}
SIGNAL_TRACK_INTERVAL_MIN = 2
MIN_SL_PCT = {"5m": 0.70, "15m": 0.60, "1h": 0.80}

# ═══ 🆕 Smart S/R Zones Engine ═══
SMART_ZONES_ENABLED   = _env_int("SMART_ZONES_ENABLED", 1) == 1
SZ_SWING_LOOKBACK     = _env_int("SZ_SWING_LOOKBACK", 3)
SZ_CLUSTER_PCT        = _env_float("SZ_CLUSTER_PCT", 0.5)
SZ_MIN_TOUCHES        = _env_int("SZ_MIN_TOUCHES", 2)
SZ_MIN_VOL_RATIO      = _env_float("SZ_MIN_VOL_RATIO", 1.2)
SZ_ENTRY_TOLERANCE    = _env_float("SZ_ENTRY_TOLERANCE", 1.0)
SZ_TP_BEFORE_PCT      = _env_float("SZ_TP_BEFORE_PCT", 0.15)
SZ_SL_BEHIND_PCT      = _env_float("SZ_SL_BEHIND_PCT", 0.25)
SZ_SL_SAFETY_PCT      = _env_float("SZ_SL_SAFETY_PCT", 0.30)
SZ_MIN_RR             = _env_float("SZ_MIN_RR", 1.5)

# 🆕 v6.8.9 — حدود SL صارمة + R:R filter
SZ_MAX_SL_PCT         = _env_float("SZ_MAX_SL_PCT", 2.0)       # حد مطلق % على SL
SZ_MIN_RR_FILTER      = _env_float("SZ_MIN_RR_FILTER", 0.0)  # 🆕 v8.0.65 — معطل (TP1 = 0.7% ثابت)  # v7.3.0: 1.3→1.0    # حد R:R الأدنى للفلترة
SZ_ENFORCE_TP1_RR     = _env_float("SZ_ENFORCE_TP1_RR", 0.0)  # 🆕 v8.0.68 — معطل (TP1 = 0.6% ثابت)   # TP1 يجب أن يكون ≥ هذا الرقم

# ═══ 🆕 v7.2.0 — TP1 ATR Cap (reachable target) ═══
TP1_ATR_MULT_15M = _env_float("TP1_ATR_MULT_15M", 2.0)   # ATR(15m) × 2 = max TP1
TP1_MIN_PCT      = _env_float("TP1_MIN_PCT", 0.4)         # minimum TP1 %
TP1_MAX_PCT      = _env_float("TP1_MAX_PCT", 1.0)  # 🆕 v8.0.23: 1.5→1.0 (target closer)         # absolute cap
TRADE_STATS_FILE = "/root/bot2/trade_stats.jsonl"        # append-only

# ═══ 🆕 v7.2.0 — TP1 Dynamic Cap ═══
TP1_MIN_PCT         = _env_float("TP1_MIN_PCT", 0.4)
TP1_MAX_PCT         = _env_float("TP1_MAX_PCT", 1.5)
TP1_TP2_SPLIT_TP1   = _env_float("TP1_TP2_SPLIT_TP1", 1.0)  # 100%   # 60% عند TP1
TRADE_STATS_FILE    = "/root/bot2/trade_stats.jsonl"
SHADOW_TRADES_FILE  = "/root/bot2/shadow_trades.jsonl"  # 🆕 v7.9.23 — shadow reversal

# per-symbol base multipliers (من التحليل التاريخي)

# ═══ 🆕 v7.9.0 — TP1 based on median MFE(1h) ═══
# القيم من تحليل 30 يوم على 15m
# TP1 = القيمة هذه (أو الحافة إذا أقرب)
SYMBOL_MFE_TARGET = {
    "BNBUSDT": 0.24,   # 🆕 v8.0.23: 0.32→0.24 (-25%)
    "ETHUSDT": 0.25,   # 🆕 v8.0.23: 0.33→0.25 (-24%)
    "SOLUSDT": 0.41,   # 🆕 v8.0.23: 0.55→0.41 (-25%)
    "XRPUSDT": 0.31,   # 🆕 v8.0.23: 0.41→0.31 (-24%)
    "LINKUSDT": 0.35,   # v8.0.86
    "AVAXUSDT": 0.45,   # v8.0.86
}

# Bounds (لضمان حدود معقولة)
SYMBOL_TP1_MIN = {
    "BNBUSDT": 0.25,   # لا تقل عن 0.25%
    "ETHUSDT": 0.25,
    "SOLUSDT": 0.40,
    "XRPUSDT": 0.30,
}

SYMBOL_TP1_MAX = {
    "BNBUSDT": 0.50,   # 🆕 v8.0.23: 0.65→0.50
    "ETHUSDT": 0.50,   # 🆕 v8.0.23: 0.65→0.50
    "SOLUSDT": 0.70,   # 🆕 v8.0.23: 0.95→0.70
    "XRPUSDT": 0.60,   # 🆕 v8.0.23: 0.80→0.60
}

SYMBOL_BASE_MULT = {
    "BNBUSDT": 0.70,
    "ETHUSDT": 0.85,
    "SOLUSDT": 1.00,
    "XRPUSDT": 1.10,
}

# ═══ 🆕 v6.9.0 — Smart TP2 (Bot-Managed) ═══
# ═══ 🆕 v7.0.0 — Smart TP2 (Bot-Managed, Static SL) ═══
SMART_TP2_ENABLED        = _env_int("SMART_TP2_ENABLED", 1) == 1
TP2_ENTRY_RETURN_PCT     = _env_float("TP2_ENTRY_RETURN_PCT", 0.2)
TP2_ENTRY_ATR_MULT       = _env_float("TP2_ENTRY_ATR_MULT", 0.4)
TP2_ZONE_EDGE_BUFFER_PCT = _env_float("TP2_ZONE_EDGE_BUFFER_PCT", 0.2)
TP2_TWO_CANDLE_AFTER_TP1 = _env_int("TP2_TWO_CANDLE_AFTER_TP1", 1) == 1

# ═══ 🆕 v6.8.1 — لمسة ATR + حدود ذكية ═══
SZ_CLUSTER_ATR_MULT   = _env_float("SZ_CLUSTER_ATR_MULT", 0.5)   # تسامح اللمس = 0.5 ATR
SZ_MAX_SL_ATR         = _env_float("SZ_MAX_SL_ATR", 3.0)          # أقصى SL عادي (ATR)
SZ_SL_DOUBLE_ATR      = _env_float("SZ_SL_DOUBLE_ATR", 6.0)       # الحد المضاعف (ATR)
SZ_MAX_TP1_ATR        = _env_float("SZ_MAX_TP1_ATR", 4.0)         # أقصى TP1 منفرد (ATR)

# ═══ 🆕 v6.8.0 — إدارة الtrade بعد الدخول ═══
SZ_RISK_PCT           = _env_float("SZ_RISK_PCT", 1.5)          # 1-2% من الرصيد
# 🆕 v7.0.0 — SL ثابت: تعطيل الميزات الديناميكية
SZ_BREAKEVEN_ENABLED  = 0
SZ_TRAILING_ENABLED   = 0
SZ_TWO_CANDLE_ENABLED = 0
SZ_STRUCT_BREAK_ENABLED = 0
SZ_TRAIL_EMA_PERIOD   = _env_int("SZ_TRAIL_EMA_PERIOD", 20)
SZ_TRAIL_ATR_MULT     = _env_float("SZ_TRAIL_ATR_MULT", 1.5)
SZ_TRAIL_MIN_STEP_PCT = _env_float("SZ_TRAIL_MIN_STEP_PCT", 0.05)  # 0.05% min move

LAST_SIGNALS_MAX = 20

CHART_LOOKBACK = 120
CHART_DPI      = 110
CHART_ENABLED  = True
SEND_SR_CHARTS = True


# ═══════════════════════════════════════════════════════════
# 1b) ⚙️ إعدادات Binance Futures
# ═══════════════════════════════════════════════════════════
BINANCE_ENABLED       = _env_int("BINANCE_ENABLED", 1) == 1
BINANCE_TESTNET       = _env_int("BINANCE_TESTNET", 1) == 1  # v7.0.0 TESTNET
BINANCE_API_KEY       = _read_env_clean("BINANCE_API_KEY")
BINANCE_API_SECRET    = _read_env_clean("BINANCE_API_SECRET")
NOTIONAL_USDT         = _env_float("NOTIONAL_USDT", 150.0)  # v9.24.0: 100→150  # v8.2.0: 85→100
LEVERAGE              = _env_int("LEVERAGE", 150)  # v9.24.0: 20→150          # v8.2.0: 13→20
MARGIN_TYPE           = (_read_env_clean("MARGIN_TYPE") or "ISOLATED").upper()
STRONG_SCORE_FOR_ADD  = _env_int("STRONG_SCORE_FOR_ADD", 80)
POSITION_MONITOR_SEC  = _env_int("POSITION_MONITOR_INTERVAL_SEC", 60)
MAX_HOLD_MINUTES      = _env_int("MAX_HOLD_MINUTES", 240)  # 🆕 v7.9.13 — 4 hours max
# 🆕 v7.9.35 — Slippage protection
MAX_SLIPPAGE_PCT      = _env_float("MAX_SLIPPAGE_PCT", 0.2)  # reject if |fill-signal|/signal > 0.2%
# 🆕 v6.7.0 — حدود الحماية
MAX_OPEN_TRADES       = _env_int("MAX_OPEN_TRADES", 10)  # v8.2.0: 5→10
FAST_MONITOR_ENABLED  = _env_int("FAST_MONITOR_ENABLED", 1) == 1
FAST_MONITOR_SEC      = _env_int("FAST_MONITOR_SEC", 60)  # 🆕 v7.9.29: 30→60 (WS prices)
FAST_MONITOR_DIST_PCT = _env_float("FAST_MONITOR_DIST_PCT", 0.25)
CLOSE_REASON_TP       = "TP1"
CLOSE_REASON_SL       = "SL"


# ── إعدادات الفلترة ─────────────────────────────────
MIN_EMA_GAP_PCT = {
    "5m":  _env_float("MIN_EMA_GAP_5M",  0.2),   # 🆕 v7.9.39: 0.008→0.2 (anti-fakeout)
    "15m": _env_float("MIN_EMA_GAP_15M", 0.006),
    "1h":  _env_float("MIN_EMA_GAP_1H",  0.004),
}
MIN_ATR_PCT = {
    "5m":  _env_float("MIN_ATR_5M",  0.05),
    "15m": _env_float("MIN_ATR_15M", 0.10),
    "1h":  _env_float("MIN_ATR_1H",  0.20),
}
MIN_ADX_FOR_BB       = _env_float("MIN_ADX_FOR_BB", 8.0)  # v7.4.0
SYMBOL_COOLDOWN_SEC  = 300  # 🆕 v8.0.66: 900→300 (5 min)

# ═══ حزمة 4: Funding Rate Filter ═══
FUNDING_FILTER_ENABLED   = _env_int("FUNDING_FILTER_ENABLED", 1) == 1
FUNDING_BLOCK_LONG_ABOVE = _env_float("FUNDING_BLOCK_LONG_ABOVE", 0.03)
FUNDING_BLOCK_SHORT_BELOW = _env_float("FUNDING_BLOCK_SHORT_BELOW", -0.03)

# ═══ حزمة 3: VWAP Breakout ═══
ENABLE_VWAP_BREAKOUT     = _env_int("ENABLE_VWAP_BREAKOUT", 1) == 1
VWAP_BREAKOUT_LOOKBACK   = _env_int("VWAP_BREAKOUT_LOOKBACK", 3)  # v7.4.0
VWAP_BREAKOUT_MIN_SLOPE  = _env_float("VWAP_BREAKOUT_MIN_SLOPE", 0.001)  # 🆕 v7.9.20: 0.002→0.001
VWAP_BREAKOUT_COOLDOWN_MIN = _env_int("VWAP_BREAKOUT_COOLDOWN_MIN", 5)  # 🆕 v8.0.66: 10→5

# ═══ حزمة 6: Ichimoku Cloud ═══
ENABLE_ICHIMOKU          = True  # 🆕 v8.0.54 — enabled (with ADX+Score filters)
ICHIMOKU_COOLDOWN_MIN    = _env_int("ICHIMOKU_COOLDOWN_MIN", 5)  # 🆕 v8.0.66: 60→5

# ═══ حزمة 7: Voting System ═══
VOTING_ENABLED           = _env_int("VOTING_ENABLED", 1) == 1
VOTING_MIN_AGREEMENT     = _env_int("VOTING_MIN_AGREEMENT", 2)
VOTING_WINDOW_SEC        = _env_int("VOTING_WINDOW_SEC", 180)

# ═══ حماية إضافية ═══
MAX_MARGIN_USAGE_PCT     = _env_float("MAX_MARGIN_USAGE_PCT", 70.0)  # v8.2.0: 60→70
STRICTNESS_MAX_FINAL = _env_int("STRICTNESS_MAX_FINAL", 80)

ABS_MIN_SCORE        = _env_int("ABS_MIN_SCORE", 60)  # v7.1.1: lowered from 75

# 🆕 Paper Mode — إشارات ضعيفة للتقييم بدون تنفيذ
PAPER_MODE_ENABLED   = _env_int("PAPER_MODE_ENABLED", 1) == 1
PAPER_MIN_SCORE      = _env_int("PAPER_MIN_SCORE", 40)

# 🆕 v7.9.46 — Paper: تصنيف أسباب الرفض + expiry + digest
PAPER_EXPIRY_MIN          = _env_int("PAPER_EXPIRY_MIN", 180)  # 3 ساعات
PAPER_DIGEST_INTERVAL_SEC = _env_int("PAPER_DIGEST_INTERVAL_SEC", 3 * 3600)

# الفلاتر التشغيلية (رفضت لأن التنفيذ غير ممكن، ليس لأن الإشارة ضعيفة)
_PAPER_OPERATIONAL_CATS = {
    "existing_position", "opposite_position",
    "max_concurrent", "max_same_dir",
    "slippage", "qty",
}
# استثناءات مؤقتة — لا نُسجّل Paper لها (ستظهر مجدداً في الدورة القادمة)
_PAPER_EXCLUDE_CATS = {"cooldown", "rate_limit"}

# 🆕 تفعيل المؤشرات المعطّلة للـPaper فقط (Ichimoku + BB Squeeze)
PAPER_FORCE_DISABLED_DETECTORS = _env_int("PAPER_FORCE_DISABLED_DETECTORS", 1) == 1
BB_MIN_VOL           = _env_float("BB_MIN_VOL", 0.6)  # v7.4.0
BB_MIN_ADX_IN_RANGE  = _env_float("BB_MIN_ADX_IN_RANGE", 15)
BB_CONFIRM_ENABLED   = True

FOLLOWUP_ENABLED = True
FOLLOWUP_TELEGRAM_ENABLED = False  # 🆕 v7.9.26 — tracking only
FOLLOWUP_ON_TP1 = True
FOLLOWUP_ON_TP2 = True
FOLLOWUP_ON_TP3 = True
FOLLOWUP_ON_SL = True
FOLLOWUP_ON_EXPIRE = True
FOLLOWUP_THROTTLE_SEC = 120


# ═══════════════════════════════════════════════════════════
# 2) النطاقات اليدوية
# ═══════════════════════════════════════════════════════════
def _normalize_symbol_from_var(name):
    name = name.strip().upper()
    if "/" in name: return name
    if name.endswith("_USDT"): name = name[:-5]
    if ":" in name:
        return name if "/" in name else name.replace("_", "/", 1)
    return f"{name}/USDT:USDT"


def _detect_manual_ranges():
    env = dict(os.environ)
    out = {}
    for key, upper_val in env.items():
        if not key.upper().endswith("_UPPER"): continue
        coin_part = key[:-6]
        lower_key = f"{coin_part}_LOWER"
        lower_val = env.get(lower_key)
        if lower_val is None:
            for k in env:
                if k.upper() == lower_key.upper():
                    lower_val = env[k]; break
        if lower_val is None:
            log.warning(f"⚠️ {key} without {lower_key} — skip"); continue
        try:
            upper = float(str(upper_val).strip())
            lower = float(str(lower_val).strip())
        except Exception as e:
            log.warning(f"⚠️ Convert {key}: {e}"); continue
        if upper <= lower:
            log.warning(f"⚠️ Invalid range {coin_part}"); continue
        sym = _normalize_symbol_from_var(coin_part)
        out[sym] = {"lower": lower, "upper": upper}
        log.info(f"📌 {sym}: [{lower} – {upper}]")
    return out


# 🆕 v7.9.30 — Production: hardcoded 4 symbols, no ranges
_MANUAL_RANGES = {}
ALL_SYMBOLS = [
    "ETH/USDT:USDT",
]  # v9.24.0: ETH فقط (لاختبار Supertrend)

SCALPER_SYMBOLS = [
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "DOGE/USDT:USDT",
]

log.info(f"✅ Symbols (hardcoded): {[s.split('/')[0] for s in ALL_SYMBOLS]}")


# ═══════════════════════════════════════════════════════════
# 3) منصات ccxt
# ═══════════════════════════════════════════════════════════
_EXCHANGE_NAMES = ["binance", "bybit", "okx", "kucoin", "kraken", "gate",
                   "bitget", "mexc", "htx", "coinex", "bitmart"]
_EXCHANGE_CLASSES = {n: getattr(ccxt, n) for n in _EXCHANGE_NAMES if hasattr(ccxt, n)}
log.info(f"📦 ccxt {ccxt.__version__}")


class MultiExchange:
    def __init__(self):
        self.primary = None
        self.fallbacks = []
        self._init_all()

    def _make(self, name):
        if name not in _EXCHANGE_CLASSES: return None  # 🆕 v8.0.26: binance allowed
        try:
            opts = {"enableRateLimit": True, "timeout": 20000,
                    "options": {"defaultType": MARKET_TYPE, "fetchOHLCV": {"maxLimit": 200}}}
            if name == "bybit":
                opts["rateLimit"] = 500
                opts["options"]["unifiedMargin"] = False
            ex = _EXCHANGE_CLASSES[name](opts)
            ex.load_markets()
            log.info(f"✅ {ex.name} | {len(ex.markets)} markets")
            return ex
        except Exception as e:
            log.warning(f"❌ {name}: {type(e).__name__}: {e}")
            return None

    def _init_all(self):
        self.primary = self._make(PRIMARY_EXCHANGE)
        for name in FALLBACK_EXCHANGES:
            if name == PRIMARY_EXCHANGE: continue
            ex = self._make(name)
            if ex: self.fallbacks.append(ex)
        if not self.primary and self.fallbacks:
            self.primary = self.fallbacks.pop(0)

    def _chain(self):
        out = []
        if self.primary: out.append(self.primary)
        out.extend(self.fallbacks)
        return out

    async def fetch_ohlcv(self, symbol, tf, limit=200):
        for ex in self._chain():
            try:
                # 🆕 v6.9.3 — timeout 15s
                data = await asyncio.wait_for(
                    asyncio.to_thread(ex.fetch_ohlcv, symbol, tf, None, limit),
                    timeout=15.0)
                if data and len(data) > 0:
                    return data, ex.id
            except asyncio.TimeoutError:
                log.warning(f"ccxt timeout {ex.id} {symbol} {tf}")
                continue
            except ccxt.BadSymbol:
                continue
            except (ccxt.RateLimitExceeded, ccxt.NetworkError, ccxt.ExchangeError):
                await asyncio.sleep(0.3); continue
            except Exception:
                continue
        return None, None

    @property
    def primary_name(self): return self.primary.name if self.primary else "none"

    @property
    def chain_names(self): return [e.name for e in self._chain()]

    def ok(self): return self.primary is not None


_exchange = MultiExchange()


# ═══════════════════════════════════════════════════════════
# 4) التشدد الذكي
# ═══════════════════════════════════════════════════════════
STATIC_PROFILES = {
    "balanced": {"min_score": 60, "min_vol_ratio": 1.00, "min_adx": 18.0,
                 "max_atr_pct": 0.70, "vwap_slope_pct": 0.05,
                 "block_against_htf": True, "score_15m_override": 65},
    "strict":   {"min_score": 70, "min_vol_ratio": 1.30, "min_adx": 22.0,
                 "max_atr_pct": 0.60, "vwap_slope_pct": 0.07,
                 "block_against_htf": True, "score_15m_override": 75},
}
TF_PROFILES = {
    "5m":  {"score_mult": 1.00, "vol_mult": 1.00, "atr_max": 0.70, "min_hold_min": 5},
    "15m": {"score_mult": 1.00, "vol_mult": 1.00, "atr_max": 0.90, "min_hold_min": 15},
    "1h":  {"score_mult": 0.85, "vol_mult": 0.75, "atr_max": 1.80, "min_hold_min": 60},
}
SESSIONS = [
    (0, 6,  "🌏 Asia (quiet)",   +8),
    (6, 10, "🌅 Asia/Europe",     +2),
    (10, 13, "🇪🇺 Europe",        0),
    (13, 16, "🔥 Peak",           -5),
    (16, 21, "🇺🇸 America",        0),
    (21, 24, "🌙 Night quiet",  +5),
]


class SmartStrictness:
    def __init__(self):
        self.history = deque(maxlen=200)
        self.results = deque(maxlen=200)
        self.source_stats = defaultdict(lambda: {"wins": 0, "losses": 0,
                                                  "expired": 0, "sent": 0,
                                                  "filtered": 0})
        self._lock = threading.Lock()
        self._cached_profile = None
        self._cached_ts = 0.0
        self._cached_atr = None
        self._cached_adx = None
        self._cache_ttl = 60.0

    def record(self, passed, score, source="unknown"):
        with self._lock:
            self.history.append((passed, score, _time.time(), source))
            if passed: self.source_stats[source]["sent"] += 1
            else:      self.source_stats[source]["filtered"] += 1

    def record_result(self, result, direction, source="unknown"):
        # 🩹 FIX: handle None
        if not result:
            return
        win = result.startswith("win"); expired = result == "expired"
        with self._lock:
            self.results.append({"win": win, "result": result,
                                 "direction": direction, "ts": _time.time(),
                                 "source": source, "expired": expired})
            if win: self.source_stats[source]["wins"] += 1
            elif expired: self.source_stats[source]["expired"] += 1
            else: self.source_stats[source]["losses"] += 1

    def _feedback(self):
        with self._lock:
            results = list(self.results)[-50:]
        non_expired = [r for r in results if not r.get("expired")]
        if len(non_expired) < 15:
            return 0, None, len(non_expired)
        wins = sum(1 for r in non_expired if r["win"])
        ratio = wins / len(non_expired)
        n = len(non_expired)
        if ratio > 0.65:   d = +10
        elif ratio > 0.50: d = +5
        elif ratio < 0.25: d = -18
        elif ratio < 0.40: d = -8
        else:              d = 0
        return d, ratio, n

    def compute(self, atr_pct=None, adx=None):
        now = _time.time()
        if (self._cached_profile
            and (now - self._cached_ts) < self._cache_ttl
            and self._cached_atr is not None
            and abs(self._cached_atr - (atr_pct or 0)) < 0.05
            and abs(self._cached_adx - (adx or 0)) < 2.0):
            return self._cached_profile
        if atr_pct is None or atr_pct <= 0: atr_pct = 0.5
        if atr_pct < 0.15:   base = 50
        elif atr_pct < 0.30: base = 55
        elif atr_pct < 0.60: base = 62
        elif atr_pct < 1.00: base = 70
        else:                base = 78
        adx_delta = 0
        if adx is not None:
            if adx > 35:   adx_delta = -3
            elif adx > 25: adx_delta = -1
            elif adx < 18: adx_delta = +5
            elif adx < 22: adx_delta = +2
        fb_delta, pass_ratio, sample_n = self._feedback()
        final = max(40, min(STRICTNESS_MAX_FINAL, base + adx_delta + fb_delta))
        prof = {"min_score": final,
                "min_vol_ratio": round(max(0.7, (final - 30) / 30), 2),
                "min_adx": round(max(12, final * 0.28), 1),
                "max_atr_pct": round(max(0.35, 1.2 - final / 100), 2),
                "vwap_slope_pct": round(0.02 + final / 1000, 3),
                "block_against_htf": final >= 55,
                "score_15m_override": min(80, final + 10),
                "_debug": {"base": base, "adx_delta": adx_delta,
                           "fb_delta": fb_delta, "atr": atr_pct, "adx": adx}}
        self._cached_profile = prof
        self._cached_ts = now
        self._cached_atr = atr_pct
        self._cached_adx = adx
        return prof

    def stats(self):
        with self._lock:
            n = len(self.results)
            if n == 0: return {"n": 0, "wr": 0, "wins": 0, "losses": 0, "expired": 0}
            wins = sum(1 for r in self.results if r["win"])
            expired = sum(1 for r in self.results if r.get("expired"))
            losses = n - wins - expired
        decisive = wins + losses
        wr = round(wins/decisive*100, 1) if decisive > 0 else 0
        return {"n": n, "wr": wr, "wins": wins, "losses": losses, "expired": expired}

    def source_report(self):
        with self._lock: return dict(self.source_stats)

    def reset_feedback(self):
        with self._lock:
            self.history.clear(); self.results.clear(); self.source_stats.clear()
        self._cached_profile = None; self._cached_ts = 0.0
        self._cached_atr = None; self._cached_adx = None


_smart = SmartStrictness()


def get_session_info():
    h = datetime.now(timezone.utc).hour
    for start, end, name, delta in SESSIONS:
        if start <= h < end: return delta, name
    return 0, "❓"


def get_thresholds(atr_pct=None, adx=None, tf="15m"):
    if STRICTNESS == "auto":
        prof = dict(_smart.compute(atr_pct=atr_pct, adx=adx))
    else:
        prof = dict(STATIC_PROFILES.get(STRICTNESS, STATIC_PROFILES["balanced"]))
        prof["_debug"] = {"mode": STRICTNESS}
    tf_p = TF_PROFILES.get(tf, TF_PROFILES["15m"])
    prof["min_score"] = prof["min_score"] * tf_p["score_mult"]
    prof["min_vol_ratio"] = prof["min_vol_ratio"] * tf_p["vol_mult"]
    prof["max_atr_pct"] = tf_p["atr_max"]
    prof["_tf"] = tf
    if SESSION_ADJUST_ENABLED:
        sd, sn = get_session_info()
        prof["min_score"] += sd
        prof["_session"] = sn
        prof["_session_delta"] = sd
    prof.update(STRICTNESS_OVERRIDE)
    return prof


# ═══════════════════════════════════════════════════════════
# 5) أدوات
# ═══════════════════════════════════════════════════════════
def short(symbol): return symbol.split("/")[0].split(":")[0].upper()


def syria_now_str():
    now = datetime.now(SYRIA_TZ)
    days_ar = {"Monday": "الاثنين", "Tuesday": "الثلاثاء", "Wednesday": "الأربعاء",
               "Thursday": "الخميس", "Friday": "الجمعة", "Saturday": "السبت",
               "Sunday": "الأحد"}
    return f"{days_ar.get(now.strftime('%A'), now.strftime('%A'))} " \
           f"{now.strftime('%Y-%m-%d')} — {now.strftime('%H:%M:%S')}"


def syria_from_ts(ts_ms, with_time=True):
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=SYRIA_TZ)
        return dt.strftime("%Y-%m-%d %H:%M") if with_time else dt.strftime("%Y-%m-%d")
    except Exception:
        return "?"


def fmt_price(v):
    if v >= 1000: return f"{v:.2f}"
    if v >= 100:  return f"{v:.3f}"
    if v >= 1:    return f"{v:.4f}"
    if v >= 0.01: return f"{v:.5f}"
    return f"{v:.8f}"


def find_symbol_by_name(name):
    if not name: return None
    name = name.upper().strip()
    for sym in ALL_SYMBOLS:
        if short(sym) == name: return sym
    for sym in ALL_SYMBOLS:
        if name in sym.upper(): return sym
    if "/" not in name:
        return f"{name}/USDT:USDT"
    return None


# ═══════════════════════════════════════════════════════════
# 🔧 أدوات الرد الآمنة (حل AttributeError)
# ═══════════════════════════════════════════════════════════
def _get_target_message(update: Update):
    """يُرجع message من أي نوع update (رسالة أو callback)."""
    if update is None:
        return None
    if update.message:
        return update.message
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message
    return None


async def _reply(update: Update, text: str, **kwargs):
    """ردّ آمن يعمل مع الرسائل و اfor Callbacks."""
    msg = _get_target_message(update)
    if msg is None:
        log.warning("⚠️ _reply: no message in update")
        return None
    try:
        return await msg.reply_text(text, **kwargs)
    except Exception as e:
        log.error(f"_reply error: {e}")
        return None


async def _reply_photo(update: Update, photo, caption: str = "", **kwargs):
    """ردّ صورة آمن."""
    msg = _get_target_message(update)
    if msg is None:
        return None
    try:
        return await msg.reply_photo(photo=photo, caption=caption, **kwargs)
    except Exception as e:
        log.error(f"_reply_photo error: {e}")
        return None


def _get_chat_id(update: Update):
    """يُرجع chat_id من أي نوع update."""
    if update is None:
        return CHAT_ID
    if update.effective_chat:
        return update.effective_chat.id
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message.chat_id
    return CHAT_ID


# ═══════════════════════════════════════════════════════════
# 6) المؤشرات
# ═══════════════════════════════════════════════════════════
def calc_ema(s, p): return s.ewm(span=p, adjust=False).mean()


def calc_rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0); l = -d.clip(upper=0)
    ag = g.ewm(alpha=1/p, adjust=False).mean()
    al = l.ewm(alpha=1/p, adjust=False).mean()
    rs = ag / al.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))


def calc_atr(df, p=14):
    tr = pd.concat([df["h"] - df["l"], (df["h"] - df["c"].shift()).abs(),
                    (df["l"] - df["c"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()


def calc_adx(df, p=14):
    h, l, c = df["h"], df["l"], df["c"]
    pdm = h.diff(); ndm = -l.diff()
    pdm = pdm.where((pdm > ndm) & (pdm > 0), 0)
    ndm = ndm.where((ndm > pdm) & (ndm > 0), 0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100 * pdm.ewm(alpha=1/p, adjust=False).mean() / atr.replace(0, 1e-9)
    ndi = 100 * ndm.ewm(alpha=1/p, adjust=False).mean() / atr.replace(0, 1e-9)
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, 1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean()


def calc_macd(s, f=12, sl=26, sig=9):
    ef = s.ewm(span=f, adjust=False).mean()
    es = s.ewm(span=sl, adjust=False).mean()
    dif = ef - es; dea = dif.ewm(span=sig, adjust=False).mean()
    return dif, dea, dif - dea


def calc_vwap_session(df, session="day"):
    if df.empty: return df["c"]
    try:
        ts = pd.to_datetime(df["ts"], unit="ms", utc=True)
        if session == "day":
            skey = ts.dt.floor("D")
        elif session == "week":
            skey = ts.dt.strftime("%Y-W%W")
        else:
            skey = ts.dt.floor("H")
        tp = (df["h"] + df["l"] + df["c"]) / 3
        pv = tp * df["v"]
        cum_pv = pv.groupby(skey, sort=False).cumsum()
        cum_v = df["v"].groupby(skey, sort=False).cumsum()
        return cum_pv / cum_v.replace(0, 1e-9)
    except Exception:
        tp = (df["h"] + df["l"] + df["c"]) / 3
        return (tp * df["v"]).cumsum() / df["v"].cumsum()


def calc_bollinger(df, period=20, std_mult=2):
    close = df["c"]
    ma = close.rolling(period).mean()
    sd = close.rolling(period).std()
    upper = ma + std_mult * sd
    lower = ma - std_mult * sd
    width = (upper - lower) / ma * 100
    return {"ma": ma, "upper": upper, "lower": lower, "width": width}


def find_swing_levels(df, lookback=SWING_LOOKBACK, max_count=SWING_MAX_COUNT):
    if len(df) < lookback * 2 + 2:
        return {"highs": [], "lows": []}
    highs, lows = [], []
    end = len(df) - lookback - 1
    for i in range(lookback, end):
        h = float(df["h"].iloc[i]); l = float(df["l"].iloc[i]); ts = int(df["ts"].iloc[i])
        nh = [float(df["h"].iloc[i+j]) for j in range(-lookback, lookback+1) if j != 0]
        nl = [float(df["l"].iloc[i+j]) for j in range(-lookback, lookback+1) if j != 0]
        if all(h > x for x in nh): highs.append((h, ts))
        if all(l < x for x in nl): lows.append((l, ts))
    highs.sort(key=lambda x: x[1], reverse=True)
    lows.sort(key=lambda x: x[1], reverse=True)
    return {"highs": highs[:max_count], "lows": lows[:max_count]}


def calc_pivots_classic(df):
    if len(df) < 2: return None
    last = df.iloc[-2]
    h, l, c = float(last["h"]), float(last["l"]), float(last["c"])
    pivot = (h + l + c) / 3
    return {"pivot": pivot, "r1": 2*pivot - l, "r2": pivot + (h - l),
            "r3": h + 2*(pivot - l), "s1": 2*pivot - h,
            "s2": pivot - (h - l), "s3": l - 2*(h - pivot),
            "h": h, "l": l, "c": c}


def classify_market(df, curr_idx=-2):
    if len(df) < 60: return "UNKNOWN"
    try:
        adx = float(calc_adx(df).iloc[curr_idx])
        close = df["c"]; bb = calc_bollinger(df)
        bb_w = float(bb["width"].iloc[curr_idx])
        e50 = calc_ema(close, 50)
        e50_slope = (float(e50.iloc[curr_idx]) - float(e50.iloc[curr_idx-5])) \
                    / float(e50.iloc[curr_idx-5]) * 100
        atr = float(calc_atr(df).iloc[curr_idx])
        price = float(close.iloc[curr_idx])
        atr_pct = (atr / price) * 100 if price else 0
        if adx > 25 and abs(e50_slope) > 0.15:
            return "TREND_UP" if e50_slope > 0 else "TREND_DOWN"
        if bb_w < 2.0 and adx < 18: return "RANGE"
        if atr_pct > 1.2: return "VOLATILE"
        return "RANGE" if adx < 20 else ("TREND_UP" if e50_slope > 0 else "TREND_DOWN")
    except Exception:
        return "UNKNOWN"


async def classify_market_safe(symbol, tf, df):
    # 🆕 v7.9.9 — Cache
    _cache_key = (symbol, tf)
    _now = _time.time()
    _cached = _market_cache.get(_cache_key)
    if _cached and (_now - _cached["ts"]) < _MARKET_CACHE_TTL:
        return _cached["market"]
    
    if df is not None and len(df) >= 60:
        m = classify_market(df)
        if m != "UNKNOWN":
            _market_cache[_cache_key] = {"market": m, "ts": _now}
            return m
    try:
        data = await fetch_ohlcv_cached(symbol, "15m", 80)
        if data and len(data) >= 60:
            df2 = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
            m = classify_market(df2)
            _market_cache[_cache_key] = {"market": m, "ts": _now}
            return m
    except Exception: pass
    _market_cache[_cache_key] = {"market": "UNKNOWN", "ts": _now}
    return "UNKNOWN"



# ═══════════════════════════════════════════════════════════
# 🆕 Smart S/R Zones Engine
# ═══════════════════════════════════════════════════════════
def sz_find_swings(df, lb=None):
    """كشف القمم/القيعان الحقيقية"""
    if lb is None: lb = SZ_SWING_LOOKBACK
    if len(df) < lb * 2 + 5:
        return {"highs": [], "lows": []}
    v_avg = df["v"].rolling(20).mean()
    H, L = [], []
    for i in range(lb, len(df) - lb):
        h = float(df["h"].iloc[i]); l = float(df["l"].iloc[i])
        vol_ok = float(df["v"].iloc[i]) >= float(v_avg.iloc[i]) * SZ_MIN_VOL_RATIO
        if (h > float(df["h"].iloc[i-lb:i].max()) and
            h > float(df["h"].iloc[i+1:i+lb+1].max()) and vol_ok):
            H.append({"price": h, "vr": round(float(df["v"].iloc[i]) /
                       float(v_avg.iloc[i]), 2), "idx": i})
        if (l < float(df["l"].iloc[i-lb:i].min()) and
            l < float(df["l"].iloc[i+1:i+lb+1].min()) and vol_ok):
            L.append({"price": l, "vr": round(float(df["v"].iloc[i]) /
                       float(v_avg.iloc[i]), 2), "idx": i})
    return {"highs": H, "lows": L}


def sz_cluster(points, atr=None):
    """
    تجميع النقاط في عناقيد (Zones)
    - إذا atr مُعطى: التسامح = SZ_CLUSTER_ATR_MULT × ATR (لمسة مرنة)
    - وإلا: التسامح = SZ_CLUSTER_PCT % (fallback)
    """
    if not points: return []
    pts = sorted(points, key=lambda x: x["price"])
    groups, cur = [], [pts[0]]
    
    # 🆕 ATR-based tolerance
    if atr is not None and atr > 0:
        tol_abs = atr * SZ_CLUSTER_ATR_MULT
        _use_atr = True
    else:
        _use_atr = False
    
    for p in pts[1:]:
        if _use_atr:
            close_enough = abs(p["price"] - cur[-1]["price"]) <= tol_abs
        else:
            close_enough = abs(p["price"] - cur[-1]["price"]) / cur[-1]["price"] * 100 <= SZ_CLUSTER_PCT
        if close_enough:
            cur.append(p)
        else:
            if len(cur) >= SZ_MIN_TOUCHES: groups.append(cur)
            cur = [p]
    if len(cur) >= SZ_MIN_TOUCHES: groups.append(cur)
    out = []
    for g in groups:
        prices = [p["price"] for p in g]
        vrs = [p["vr"] for p in g]
        hi, lo = max(prices), min(prices)
        ctr = (hi + lo) / 2; w = hi - lo
        out.append({
            "high": hi, "low": lo, "center": ctr, "width": w,
            "width_pct": round(w / ctr * 100, 3),
            "touches": len(g),
            "strength": round(len(g) + (sum(vrs) / len(vrs)) * 0.5, 2),
        })
    out.sort(key=lambda z: z["strength"], reverse=True)
    return out


def sz_1h_direction(df_1h):
    """اتجاه 1H: UP / DOWN / NEUTRAL"""
    if len(df_1h) < 100: return "NEUTRAL"
    e20 = calc_ema(df_1h["c"], 20)
    e50 = calc_ema(df_1h["c"], 50)
    e20_now = float(e20.iloc[-2]); e50_now = float(e50.iloc[-2])
    price = float(df_1h["c"].iloc[-2])
    e20_10ago = float(e20.iloc[-10])
    slope = (e20_now - e20_10ago) / e20_10ago * 100
    if e20_now > e50_now and price > e20_now and slope > 0.1: return "UP"
    if e20_now < e50_now and price < e20_now and slope < -0.1: return "DOWN"
    return "NEUTRAL"


def sz_combined_direction(df_4h, df_1h):
    """
    4H + 1H قرار مشترك 50/50:
    - كلاهما نفس الاتجاه → نفس الاتجاه
    - أحدهما NEUTRAL → الآخر يحكم
    - تعارض → NEUTRAL (يسمح بالاتجاهين)
    """
    h4 = sz_4h_direction(df_4h)
    h1 = sz_1h_direction(df_1h)
    if h4 == h1: return h4, h4, h1
    if h4 == "NEUTRAL": return h1, h4, h1
    if h1 == "NEUTRAL": return h4, h4, h1
    # تعارض → NEUTRAL
    return "NEUTRAL", h4, h1


def sz_4h_direction(df_4h):
    """اتجاه 4H: UP / DOWN / NEUTRAL"""
    if len(df_4h) < 210: return "NEUTRAL"
    e50 = calc_ema(df_4h["c"], 50); e200 = calc_ema(df_4h["c"], 200)
    e50_now = float(e50.iloc[-2]); e200_now = float(e200.iloc[-2])
    price = float(df_4h["c"].iloc[-2])
    e50_10ago = float(e50.iloc[-10])
    slope = (e50_now - e50_10ago) / e50_10ago * 100
    if e50_now > e200_now and price > e50_now and slope > 0.1: return "UP"
    if e50_now < e200_now and price < e50_now and slope < -0.1: return "DOWN"
    return "NEUTRAL"


def sz_detect_reversal(df, curr=-2):
    """كشف شمعة انعكاس مغلقة"""
    if len(df) < 25: return None
    o = float(df["o"].iloc[curr]); h = float(df["h"].iloc[curr])
    l = float(df["l"].iloc[curr]); c = float(df["c"].iloc[curr])
    po = float(df["o"].iloc[curr-1]); pc = float(df["c"].iloc[curr-1])
    body = abs(c - o); rng = h - l
    if rng == 0: return None
    ls = min(o, c) - l; us = h - max(o, c); br = body / rng
    v_avg = df["v"].iloc[curr-20:curr].mean()
    vr = float(df["v"].iloc[curr]) / float(v_avg) if v_avg > 0 else 0
    if vr < SZ_MIN_VOL_RATIO: return None
    if ls >= 2*body and us < body*0.5 and br < 0.35 and c >= o:
        return {"type": "hammer", "dir": "bullish", "vr": round(vr, 2)}
    if us >= 2*body and ls < body*0.5 and br < 0.35 and c <= o:
        return {"type": "shooting_star", "dir": "bearish", "vr": round(vr, 2)}
    if c > o and pc < po and c > po and o < pc and body > abs(pc - po):
        return {"type": "bull_engulf", "dir": "bullish", "vr": round(vr, 2)}
    if c < o and pc > po and c < po and o > pc and body > abs(pc - po):
        return {"type": "bear_engulf", "dir": "bearish", "vr": round(vr, 2)}

    # 🆕 v6.8.0 — تسلسل 3 شموع (هبوط → رفض → تأكيد)
    if len(df) >= curr - 3 and curr - 3 > -len(df):
        try:
            # 🩹 FIX #18: align 3-candle pattern with vr (curr = last closed)
            c1 = df.iloc[curr-2]; c2 = df.iloc[curr-1]; c3 = df.iloc[curr]
            o1,h1,l1,cl1 = [float(c1[k]) for k in ("o","h","l","c")]
            o2,h2,l2,cl2 = [float(c2[k]) for k in ("o","h","l","c")]
            o3,h3,l3,cl3 = [float(c3[k]) for k in ("o","h","l","c")]

            # Bullish: c1 red → c2 hammer → c3 green close > c2.high
            if (cl1 < o1 and
                (min(o2,cl2) - l2) > abs(cl2-o2) * 1.5 and
                cl3 > o3 and cl3 > h2):
                return {"type": "3candle_bull", "dir": "bullish", "vr": round(vr, 2)}
            # Bearish: c1 green → c2 star → c3 red close < c2.low
            if (cl1 > o1 and
                (h2 - max(o2,cl2)) > abs(cl2-o2) * 1.5 and
                cl3 < o3 and cl3 < l2):
                return {"type": "3candle_bear", "dir": "bearish", "vr": round(vr, 2)}
        except Exception:
            pass

    return None





# 🩹 FIX v7.9.3: dedup set
_LOGGED_TRADE_IDS = set()

def _log_trade_stat(ticket, close_price, reason, mfe_pct=None, mae_pct=None):
    """v7.9.3: append one JSON line per closed trade (with dedup)."""
    try:
        import json as _json
        _tid = ticket.get("id", "")
        if _tid and _tid in _LOGGED_TRADE_IDS:
            log.debug(f"_log_trade_stat: skip duplicate {_tid}")
            return
        entry = float(ticket.get("entry", 0))
        if entry <= 0:
            return
        qty = float(ticket.get("qty", 0))
        if ticket.get("side") == "LONG":
            pnl_pct = (close_price - entry) / entry * 100
        else:
            pnl_pct = (entry - close_price) / entry * 100

        # TP distances at entry
        tp1 = float(ticket.get("tp", 0) or 0)
        sl = float(ticket.get("sl", 0) or 0)
        tp1_dist_pct = abs(tp1 - entry) / entry * 100 if tp1 else 0
        sl_dist_pct = abs(sl - entry) / entry * 100 if sl else 0

        rec = {
            "ts": _time.time(),
            "symbol": ticket.get("symbol", "?"),
            "side": ticket.get("side", "?"),
            "tf": ticket.get("tf", "?"),
            "score": ticket.get("score", 0),
            "source": ticket.get("source", "?"),
            "entry": entry,
            "close": close_price,
            "tp1": tp1,
            "tp1_dist_pct": round(tp1_dist_pct, 3),
            "tp1_source": ticket.get("tp1_source", "?"),
            "sl": sl,
            "sl_dist_pct": round(sl_dist_pct, 3),
            "atr_15m_pct": ticket.get("atr_15m_pct", 0),
            "pnl_pct": round(pnl_pct, 3),
            "reason": reason,
            "duration_min": int((_time.time() - ticket.get("opened_at", _time.time())) / 60),
            "mfe_pct": round(mfe_pct, 3) if mfe_pct is not None else None,
            "mae_pct": round(mae_pct, 3) if mae_pct is not None else None,
        }
        with io.open(TRADE_STATS_FILE, "a", encoding="utf-8") as f:
            f.write(_json.dumps(rec, ensure_ascii=False) + "\n")
        if _tid:
            _LOGGED_TRADE_IDS.add(_tid)
            if len(_LOGGED_TRADE_IDS) > 10000:
                _LOGGED_TRADE_IDS.clear()
    except Exception as e:
        log.warning(f"_log_trade_stat: {e}")




def _get_session_tp1_mult():
    """v7.2.0: session multiplier for TP1."""
    h = datetime.now(timezone.utc).hour
    if 0 <= h < 6:   return 0.85   # Asia
    if 6 <= h < 10:  return 1.00   # Asia/EU
    if 10 <= h < 13: return 1.00   # EU
    if 13 <= h < 16: return 1.10   # LondonNY
    if 16 <= h < 21: return 1.05   # NY
    return 0.80                     # Late


def _get_htf_tp1_mult(direction, htf_combined):
    """v7.2.0: alignment multiplier."""
    if htf_combined == "NEUTRAL":
        return 1.00
    if (htf_combined == "UP" and direction == "bullish") or \
       (htf_combined == "DOWN" and direction == "bearish"):
        return 1.10   # aligned
    return 0.65        # against



# ═══ 🆕 v8.0.28 — Time Factor (Study v2, UTC) ═══
_HOUR_FACTOR_UTC = {
    0: 1.0, 1: 1.0, 2: 0.8, 3: 0.8, 4: 0.8, 5: 0.8, 6: 0.7, 7: 0.8,
    8: 1.0, 9: 1.0, 10: 0.7, 11: 1.0, 12: 1.0, 13: 0.7,
    14: 1.3, 15: 1.1, 16: 1.1, 17: 1.3, 18: 1.1, 19: 1.1,
    20: 1.3, 21: 1.3, 22: 1.1, 23: 1.0,
}
_DAY_FACTOR = {0: 0.95, 1: 1.05, 2: 1.10, 3: 1.05, 4: 0.90, 5: 0.50, 6: 0.45}


def _get_time_factor():
    now = datetime.now(timezone.utc)
    h = now.hour
    dow = now.weekday()
    hour_f = _HOUR_FACTOR_UTC.get(h, 1.0)
    day_f = _DAY_FACTOR.get(dow, 1.0)
    interaction = 1.0
    is_weekend = dow in (5, 6)
    is_golden = hour_f >= 1.3
    if is_weekend and is_golden: interaction = 0.70
    elif dow == 4 and h >= 17: interaction = 0.80
    elif dow == 6 and h >= 22: interaction = 0.75
    elif dow == 0 and h < 7: interaction = 0.85
    factor = hour_f * day_f * interaction
    if factor < 0.6: return factor, 0.0
    elif factor < 0.85: return factor, 0.8
    elif factor < 1.15: return factor, 1.0
    else: return factor, 1.2


def _apply_tp1_atr_cap(price, tp1, direction, symbol="?", htf_combined="NEUTRAL", sl_pct=None, range_mid=None):
    """v9.6.1 — TP ديناميكي: 2×SL للترند، منتصف النطاق للعرضي"""
    if range_mid is not None and range_mid > 0:
        # السوق العرضي: استخدم منتصف النطاق
        if direction == "bullish":
            tp_pct = (range_mid - price) / price * 100
        else:
            tp_pct = (price - range_mid) / price * 100
        tp_pct = max(0.2, min(abs(tp_pct), 1.5))
        if direction == "bullish":
            new_tp1 = price * (1 + tp_pct / 100)
        else:
            new_tp1 = price * (1 - tp_pct / 100)
        log.info(f"🔒 TP1 = range_mid: {new_tp1:.6f} ({tp_pct:.2f}%)")
        return new_tp1, tp_pct, "range_mid", {"range_mid": range_mid}
    
    # السوق الترند: 2×SL
    if sl_pct is not None and sl_pct > 0:
        tp_pct = sl_pct * 2.0
        tp_pct = max(0.35, min(tp_pct, 2.5))
    else:
        tp_pct = 0.70
    
    if direction == "bullish":
        new_tp1 = price * (1 + tp_pct / 100)
    else:
        new_tp1 = price * (1 - tp_pct / 100)
    log.info(f"🔒 TP1 = 2×SL: {new_tp1:.6f} ({tp_pct:.2f}%)")
    return new_tp1, tp_pct, "rr_2x", {"rr_mult": 2.0, "sl_pct": sl_pct}
    """v7.9.0: TP1 = min(zone, MFE_target). Simple, honest, achievable."""
    try:
        if price <= 0:
            return tp1, 0.0, "no_price", {}

        b = short(symbol) + "USDT"
        zone_dist_pct = abs(tp1 - price) / price * 100

        # MFE-based target
        mfe_target = SYMBOL_MFE_TARGET.get(b, 0.40)
        tp1_min = SYMBOL_TP1_MIN.get(b, 0.25)
        tp1_max = SYMBOL_TP1_MAX.get(b, 0.65)

        # session adjustment (light)
        sess = _get_session_tp1_mult()
        # htf adjustment (light)
        htf = _get_htf_tp1_mult(direction, htf_combined)

        # cap = mfe × session × htf
        cap_pct = 0.6
        _tf_factor, _tf_mult = 1.0, 1.0
        if _tf_mult > 0:
            cap_pct *= _tf_mult
        cap_pct = max(cap_pct, tp1_min)
        cap_pct = min(cap_pct, tp1_max)

        info = {
            "mfe_target": mfe_target,
            "session": round(sess, 2),
            "htf": round(htf, 2),
            "time_factor": round(_tf_factor, 2),
            "time_mult": _tf_mult,
            "cap_pct": round(cap_pct, 3),
            "zone_dist_pct": round(zone_dist_pct, 3),
        }

        # 🆕 v7.9.43 — enforce minimum TP1 distance BEFORE zone
        if zone_dist_pct < tp1_min:
            if direction == "bullish":
                new_tp1 = price * (1 + tp1_min / 100)
            else:
                new_tp1 = price * (1 - tp1_min / 100)
            log.info(f"TP1=min_floor {tp1_min:.2f}% (zone {zone_dist_pct:.2f}% too close)")
            return new_tp1, tp1_min, "min_floor", info

        # 🆕 v8.0.69 — فرض الحد الأدنى 0.6% (حتى لو كانت zone أقرب)
        if zone_dist_pct < cap_pct:
            # zone أقرب من 0.6% → نفرض 0.6%
            if direction == "bullish":
                new_tp1 = price * (1 + cap_pct / 100)
            else:
                new_tp1 = price * (1 - cap_pct / 100)
            log.info(f"TP1=floor {cap_pct:.2f}% (zone was {zone_dist_pct:.2f}%)")
            return new_tp1, cap_pct, "floor_06", info

        # prefer zone if between 0.6% and cap (0.6 = 0.6)
        if zone_dist_pct <= cap_pct:
            log.info(f"TP1=zone {zone_dist_pct:.2f}% (cap {cap_pct:.2f}%)")
            return tp1, zone_dist_pct, "zone", info

        # else use MFE cap
        if direction == "bullish":
            new_tp1 = price * (1 + cap_pct / 100)
        else:
            new_tp1 = price * (1 - cap_pct / 100)
        log.info(f"TP1=MFE {cap_pct:.2f}% (zone was {zone_dist_pct:.2f}%)")
        return new_tp1, cap_pct, "mfe_cap", info
    except Exception as e:
        log.warning(f"_apply_tp1_atr_cap: {e}")
        return tp1, 0.0, "error", {"error": str(e)}


def _log_trade_stat(ticket, close_price, reason, mfe_pct=None, mae_pct=None):
    """v7.9.3: append one JSON line per closed trade (with dedup)."""
    try:
        import json as _json
        _tid = ticket.get("id", "")
        if _tid and _tid in _LOGGED_TRADE_IDS:
            log.debug(f"_log_trade_stat: skip duplicate {_tid}")
            return
        entry = float(ticket.get("entry", 0))
        if entry <= 0:
            return
        if ticket.get("side") == "LONG":
            pnl_pct = (close_price - entry) / entry * 100
        else:
            pnl_pct = (entry - close_price) / entry * 100

        tp1 = float(ticket.get("tp", 0) or 0)
        sl = float(ticket.get("sl", 0) or 0)
        tp1_dist = abs(tp1 - entry) / entry * 100 if tp1 else 0
        sl_dist = abs(sl - entry) / entry * 100 if sl else 0

        info = ticket.get("tp1_calc_info") or {}
        rec = {
            "ts": _time.time(),
            "symbol": ticket.get("symbol", "?"),
            "side": ticket.get("side", "?"),
            "tf": ticket.get("tf", "?"),
            "score": ticket.get("score", 0),
            "source": ticket.get("source", "?"),
            "entry": entry,
            "close": close_price,
            "tp1": tp1,
            "tp1_dist_pct": round(tp1_dist, 3),
            "tp1_source": ticket.get("tp1_source", "?"),
            "tp1_calc_info": info,
            "sl": sl,
            "sl_dist_pct": round(sl_dist, 3),
            "pnl_pct": round(pnl_pct, 3),
            "reason": reason,
            "duration_min": int((_time.time() - ticket.get("opened_at", _time.time())) / 60),
            "mfe_pct": round(mfe_pct, 3) if mfe_pct is not None else None,
            "mae_pct": round(mae_pct, 3) if mae_pct is not None else None,
        }
        with io.open(TRADE_STATS_FILE, "a", encoding="utf-8") as f:
            f.write(_json.dumps(rec, ensure_ascii=False) + "\n")
        if _tid:
            _LOGGED_TRADE_IDS.add(_tid)
            if len(_LOGGED_TRADE_IDS) > 10000:
                _LOGGED_TRADE_IDS.clear()
    except Exception as e:
        log.warning(f"_log_trade_stat: {e}")


def sz_find_setup(price, atr, highs_z, lows_z, direction,
                  symbol=None, htf_combined="NEUTRAL"):
    """
    🎯 محرك حساب SL/TP ذكي — no يرفض أبداً.
    يستخدم أقرب منطقة S/R لحساب SL و TP.
    """
    sl_zone = None
    if direction == "bullish":
        candidates = [z for z in lows_z if z["center"] < price]
        candidates.sort(key=lambda z: price - z["center"])
    else:
        candidates = [z for z in highs_z if z["center"] > price]
        candidates.sort(key=lambda z: z["center"] - price)
    
    if candidates:
        sl_zone = candidates[0]
        if direction == "bullish":
            sl = (sl_zone["low"] - sl_zone["width"] * SZ_SL_BEHIND_PCT
                  - price * SZ_SL_SAFETY_PCT / 100)
        else:
            sl = (sl_zone["high"] + sl_zone["width"] * SZ_SL_BEHIND_PCT
                  + price * SZ_SL_SAFETY_PCT / 100)
    else:
        sl = price - atr * 1.5 if direction == "bullish" else price + atr * 1.5
    
        # ═══ v7.7.0 — TP1 = zone1, TP2 = zone2 ═══
    tp_zone = None
    if direction == "bullish":
        candidates = [z for z in highs_z if z["center"] > price]
        candidates.sort(key=lambda z: z["center"] - price)
    else:
        candidates = [z for z in lows_z if z["center"] < price]
        candidates.sort(key=lambda z: price - z["center"])

    if candidates:
        tp_zone = candidates[0]
        if direction == "bullish":
            tp1 = tp_zone["low"] - tp_zone["width"] * SZ_TP_BEFORE_PCT
        else:
            tp1 = tp_zone["high"] + tp_zone["width"] * SZ_TP_BEFORE_PCT
    else:
        risk_ = abs(price - sl)
        if direction == "bullish":
            tp1 = price + risk_
        else:
            tp1 = price - risk_

    # TP2 = second zone in same direction
    tp2_zone = None
    if direction == "bullish":
        candidates2 = [z for z in highs_z if z["low"] > tp1]
        candidates2.sort(key=lambda z: z["low"])
    else:
        candidates2 = [z for z in lows_z if z["high"] < tp1]
        candidates2.sort(key=lambda z: -z["high"])

    if candidates2:
        tp2_zone = candidates2[0]
        if direction == "bullish":
            tp2 = tp2_zone["low"] - tp2_zone["width"] * SZ_TP_BEFORE_PCT
        else:
            tp2 = tp2_zone["high"] + tp2_zone["width"] * SZ_TP_BEFORE_PCT
        log.info(f"TP2=zone2 {tp2:.6f} ({abs(tp2-price)/price*100:.2f}%)")
    else:
        # fallback: 3× TP1 distance (🆕 v7.9.15: was 2×)
        tp1_dist = abs(tp1 - price)
        if direction == "bullish":
            tp2 = price + tp1_dist * 3
        else:
            tp2 = price - tp1_dist * 3
        log.info(f"TP2=3×TP1 {tp2:.6f} ({abs(tp2-price)/price*100:.2f}%)")

    tp3 = tp2  # keep tp3 = tp2 for compatibility

    # 🩹 FIX: risk was used before being defined (NameError)
    risk = abs(price - sl)

    if risk == 0:
        return {"ok": False, "reason": "risk=0"}
    
    min_sl_pct = 0.3
    if risk / price * 100 < min_sl_pct:
        risk = price * min_sl_pct / 100
        sl = price - risk if direction == "bullish" else price + risk
    
    # ═══ 🆕 v7.3.0 — SL on ATR (يُصلح R:R) ═══
    if atr > 0:
        sl_atr_max = atr * 1.5  # 1.5 × ATR(15m)
        sl_dist = abs(price - sl)
        if sl_dist > sl_atr_max:
            if direction == "bullish":
                sl = price - sl_atr_max
            else:
                sl = price + sl_atr_max
            log.info(f"SL set to 1.5×ATR: {sl_atr_max/price*100:.2f}%")
            risk = abs(price - sl)

    # ═══ 🆕 v6.8.9 — حدود SL صارمة ═══
    if atr > 0:
        max_normal = atr * SZ_MAX_SL_ATR
        max_doubled = atr * SZ_SL_DOUBLE_ATR
        sl_dist = abs(price - sl)
        
        if sl_dist > max_doubled:
            # 🔴 أكبر من المضاعف → cap عند المضاعف
            if direction == "bullish":
                sl = price - max_doubled
            else:
                sl = price + max_doubled
            log.info(f"⚠️ SL capped at {SZ_SL_DOUBLE_ATR} ATR (was {sl_dist/atr:.1f})")
        elif sl_dist > max_normal:
            log.info(f"🔶 SL doubled: {sl_dist/atr:.1f} ATR")
        
        # 🆕 cap مطلق %
        sl_dist = abs(price - sl)
        max_pct_abs = price * SZ_MAX_SL_PCT / 100
        if sl_dist > max_pct_abs:
            if direction == "bullish":
                sl = price - max_pct_abs
            else:
                sl = price + max_pct_abs
            log.info(f"⚠️ SL capped at {SZ_MAX_SL_PCT}% (absolute)")
        
        risk = abs(price - sl)
    
    # ═══ 🆕 v6.8.9 — ضمان TP1 R:R ≥ 1.5 ═══
    target_tp1_dist = risk * SZ_ENFORCE_TP1_RR
    if direction == "bullish":
        if tp1 < price + target_tp1_dist:
            tp1 = price + target_tp1_dist
            log.info(f"📏 TP1 raised to R:R={SZ_ENFORCE_TP1_RR}")
    else:
        if tp1 > price - target_tp1_dist:
            tp1 = price - target_tp1_dist
            log.info(f"📏 TP1 raised to R:R={SZ_ENFORCE_TP1_RR}")
    
    # ═══ 🆕 v6.8.1 — TP fallback إذا بعيد جداً ═══
    if atr > 0:
        tp1_dist = abs(tp1 - price)
        if tp1_dist > atr * SZ_MAX_TP1_ATR:
            # ↩️ نعود لإعدادات البوت الكلاسيكية (1:1، 1:2، 1:3)
            if direction == "bullish":
                tp1 = price + risk
                tp2 = price + risk * 2
                tp3 = price + risk * 3
            else:
                tp1 = price - risk
                tp2 = price - risk * 2
                tp3 = price - risk * 3
            log.info(f"↩️ TP1 far ({tp1_dist/atr:.1f} ATR) — Classic 1:1")
    
    # 🩹 FIX #10: TP2 only raised to RR=1.0 (MFE cap wins over RR for TP1)
    if risk > 0:
        if abs(tp2 - price) / risk < 1.0:
            tp2 = price + risk if direction == "bullish" else price - risk
            log.info(f"TP2 raised to RR=1.0")

    # 🩹 FIX #2+#3: single MFE cap call (no atr_15m)
    # v8.0.89 — sl_pct (لحساب TP ديناميكي)
    _sl_pct_computed = abs(price - sl) / price * 100 if price > 0 else 0
    # 🆕 v9.6.1 — تمرير range_mid إذا كان متوفراً
    _range_mid = None
    try:
        if symbol and "range_mid" in str(htf_combined):
            pass  # لا يوجد range_mid هنا
    except Exception:
        pass
    
    tp1, tp1_dist_pct, tp1_source, tp1_calc_info = _apply_tp1_atr_cap(
        price, tp1, direction,
        symbol=symbol or "?", htf_combined=htf_combined,
        sl_pct=_sl_pct_computed, range_mid=_range_mid)
    log.info(f"TP1 final: {tp1:.6f} ({tp1_dist_pct:.2f}%) src={tp1_source}")

    # 🆕 v8.2.1 — TP2 يبقى كما حسبه Smart Zones (لا نفرض 1.5×)

    # 🆕 v7.9.49 — إجبار TP2 ليكون ضعف مسافة TP1 (إذا كانا متداخلين)
    _tp1_dist_abs = abs(tp1 - price)
    _tp2_dist_abs = abs(tp2 - price)
    if _tp2_dist_abs <= _tp1_dist_abs * 1.05:  # إذا TP2 قريب جداً من TP1
        if direction == "bullish":
            tp2 = price + (_tp1_dist_abs * 2)
        else:
            tp2 = price - (_tp1_dist_abs * 2)
        log.info(f"📏 TP2 forced to 2×TP1: {tp2:.6f}")

    # 🎯 Option C: TP1 = max(MFE_cap, risk × SZ_MIN_RR_FILTER)
    # MFE study sets achievable target, but RR_min is a hard floor.
    if risk > 0 and SZ_MIN_RR_FILTER > 0:
        rr1_before = abs(tp1 - price) / risk
        min_tp1_dist = risk * SZ_MIN_RR_FILTER
        cur_tp1_dist = abs(tp1 - price)
        if cur_tp1_dist < min_tp1_dist:
            if direction == "bullish":
                tp1 = price + min_tp1_dist
            else:
                tp1 = price - min_tp1_dist
            rr1_after = abs(tp1 - price) / risk
            log.info(f"📏 TP1 raised to RR={SZ_MIN_RR_FILTER} "
                     f"({rr1_before:.2f} → {rr1_after:.2f})")
            # recompute tp1_dist_pct for downstream (TP2 floor)
            tp1_dist_pct = min_tp1_dist / price * 100
            tp1_source = "rr_floor"

    # 🩹 FIX v7.9.3: enforce TP1/TP2 direction
    if direction == "bullish":
        if tp1 <= price:
            log.warning(f"⚠️ TP1 ({tp1}) <= price ({price}) for bullish — fixing")
            tp1 = price * 1.001
        if tp2 <= price:
            tp2 = price * 1.002
    elif direction == "bearish":
        if tp1 >= price:
            log.warning(f"⚠️ TP1 ({tp1}) >= price ({price}) for bearish — fixing")
            tp1 = price * 0.999
        if tp2 >= price:
            tp2 = price * 0.998

    # 🆕 v8.0.9 — تأكيد أن TP2 أبعد من TP1 دائمًا
    if direction == "bullish":
        if tp2 <= tp1:
            tp2 = tp1 + (tp1 - price) * 0.5  # TP2 = TP1 + 50% من المسافة
            log.info(f"🛠️ TP2 forced above TP1: {tp2:.6f}")
    else:
        if tp2 >= tp1:
            tp2 = tp1 - (price - tp1) * 0.5  # TP2 = TP1 - 50%
            log.info(f"🛠️ TP2 forced below TP1: {tp2:.6f}")

    # 🆕 v8.2.1 — إزالة فرض RR=1.0 (TP = 0.35% دائماً)
    
    # tp3 = tp2 (strategy: TP1 + smart TP2 only, no third target)
    tp3 = tp2
    
    # v7.4.4 — FINAL: recalc rr1/rr2 after all adjustments
    rr1 = abs(tp1 - price) / risk if risk > 0 else 0
    rr2 = abs(tp2 - price) / risk if risk > 0 else 0
    sl_atr = risk / atr if atr > 0 else 0
    log.info(f"FINAL R:R  rr1={rr1:.2f}  rr2={rr2:.2f}  sl={sl_atr:.2f}ATR")
    return {
        "ok": True,
        "entry": price, "sl": sl,
        "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "risk": risk,
        "rr1": round(rr1, 2), "rr2": round(rr2, 2),
        "sl_atr": round(sl_atr, 2), "sl_pct": round(risk / price * 100, 3),
        "entry_zone": sl_zone, "tp_zone": tp_zone,
        "tp1_source": tp1_source,  # v7.2.0
        "tp1_dist_pct": round(tp1_dist_pct, 3),
    }


async def sz_compute_levels(symbol, direction, tf="15m"):
    """
    🎯 حساب SL/TP من مناطق S/R — بلا فلاتر رفض.
    🆕 v7.9.8: cache 5 دقائق لتسريع crossover_job
    """
    if not SMART_ZONES_ENABLED:
        return None
    
    # 🆕 v7.9.8 — Cache check (يجب أن يكون هنا!)
    _cache_key = (symbol, direction)
    _now = _time.time()
    _cached = _sz_cache.get(_cache_key)
    if _cached and (_now - _cached["ts"]) < _SZ_CACHE_TTL:
        return _cached["data"]
    
    try:
        d4 = await fetch_ohlcv_cached(symbol, "4h", 300)
        d1 = await fetch_ohlcv_cached(symbol, "1h", 300)
        d15 = await fetch_ohlcv_cached(symbol, "15m", 200)
        if not d4 or not d1 or not d15:
            return None
        df4 = pd.DataFrame(d4, columns=["ts","o","h","l","c","v"])
        df1 = pd.DataFrame(d1, columns=["ts","o","h","l","c","v"])
        df15 = pd.DataFrame(d15, columns=["ts","o","h","l","c","v"])
        
        combined, h4, h1 = sz_combined_direction(df4, df1)
        
        sw = sz_find_swings(df1)
        # 🆕 ATR من 15m (نطاق الدخول الفعلي)
        price = float(df15["c"].iloc[-2])
        atr = float(calc_atr(df15).iloc[-2])
        highs_z = sz_cluster(sw["highs"], atr=atr)
        lows_z = sz_cluster(sw["lows"], atr=atr)
        
        setup = sz_find_setup(price, atr, highs_z, lows_z, direction,
                              symbol=symbol, htf_combined=combined)
        if not setup["ok"]:
            return None
        
        candle = sz_detect_reversal(df15)
        candle_type = candle["type"] if candle else "—"
        candle_dir = candle["dir"] if candle else "—"
        
        # 🆕 v6.9.0 — zone_edge (أقرب حافة من منطقة TP)
        _tp_zone = setup.get("tp_zone")
        _zone_edge = None
        if _tp_zone:
            if direction == "bullish":
                _zone_edge = _tp_zone["low"]
            else:
                _zone_edge = _tp_zone["high"]
        
        _result = {
            "entry": setup["entry"],
            "stop_loss": setup["sl"],
            "tp1": setup["tp1"], "tp2": setup["tp2"], "tp3": setup["tp3"],
            "sl_pct": setup["sl_pct"], "atr": atr,
            "rr1": setup["rr1"], "rr2": setup["rr2"],
            "sl_atr": setup["sl_atr"],
            "h4": h4, "h1": h1, "combined": combined,
            "candle": candle_type, "candle_dir": candle_dir,
            "entry_zone": setup["entry_zone"], "tp_zone": setup["tp_zone"],
            "zone_edge": _zone_edge,
            "source": "smart_zones",
        }
        _sz_cache[_cache_key] = {"data": _result, "ts": _now}
        return _result
    except Exception as e:
        log.warning(f"sz_compute_levels {symbol}: {e}")
        return None


async def compute_levels_smart_aware(symbol, df, direction, curr_idx=-2, tf="15m", source="ema"):
    """
    🎯 محرك حساب SL/TP ذكي:
    1. جرّب Smart Zones
    2. إذا فشل → fallback Classic
    3. no رفض
    """
    if SMART_ZONES_ENABLED:
        try:
            smart = await sz_compute_levels(symbol, direction, tf)
            if smart and "entry" in smart:
                log.info(f"✅ Smart Zones {short(symbol)} [{source}]: "
                         f"R:R={smart['rr1']} | SL={smart['sl_pct']}% | "
                         f"4H={smart.get('h4','?')} 1H={smart.get('h1','?')} "
                         f"candle={smart.get('candle','—')}")
                return smart
        except Exception as e:
            log.warning(f"smart_zones {symbol}: {e}")
    
    result = _compute_levels_classic(df, direction, curr_idx, tf)
    if result:
        log.info(f"↩️ {short(symbol)} [{source}]: Classic SL/TP")
    return result


def _compute_levels_classic(df, direction, curr_idx=-2, tf="15m"):
    """الطريقة القديمة (ATR + EMA)"""
    try:
        price = float(df["c"].iloc[curr_idx])
        atr = float(calc_atr(df).iloc[curr_idx])
        ema_slow = float(calc_ema(df["c"], EMA_SLOW).iloc[curr_idx])
        min_sl_pct = MIN_SL_PCT.get(tf, 0.50)
        lookback = min(20, len(df) + curr_idx - 1)
        if lookback < 2: return None
        recent_high = float(df["h"].iloc[curr_idx-lookback:curr_idx].max())
        recent_low = float(df["l"].iloc[curr_idx-lookback:curr_idx].min())
        if direction == "bullish":
            sl_atr = price - atr * 1.5
            sl_struct = min(recent_low, ema_slow) * 0.998
            stop_loss = max(sl_atr, sl_struct)
            min_dist = price * (min_sl_pct / 100)
            if (price - stop_loss) < min_dist: stop_loss = price - min_dist
            risk = price - stop_loss
            tp1, tp2, tp3 = price + risk, price + risk*2, price + risk*3
        else:
            sl_atr = price + atr * 1.5
            sl_struct = max(recent_high, ema_slow) * 1.002
            stop_loss = min(sl_atr, sl_struct)
            min_dist = price * (min_sl_pct / 100)
            if (stop_loss - price) < min_dist: stop_loss = price + min_dist
            risk = stop_loss - price
            tp1, tp2, tp3 = price - risk, price - risk*2, price - risk*3
        risk_pct = abs(price - stop_loss) / price * 100 if price else 0
        if risk_pct < min_sl_pct * 0.9 or risk_pct > 6.0: return None
        _risk = abs(price - stop_loss)
        _tp1_dist = abs(tp1 - price)
        if _tp1_dist < _risk * 0.95:
            return None
        return {"entry": price, "stop_loss": stop_loss, "sl_pct": round(risk_pct, 2),
                "tp1": tp1, "tp2": tp2, "tp3": tp3, "atr": atr,
                "source": "classic"}
    except Exception as e:
        log.warning(f"_compute_levels_classic: {e}")
        return None




# ═══════════════════════════════════════════════════════════
# 7) الكاشات
# ═══════════════════════════════════════════════════════════
_ohlcv_cache = {}
_crossover_cache = {}
# 🆕 v7.9.10 — batch symbols for 5m
_crossover_batch_idx = {"5m": 0}
_BATCH_SIZE_5M = 8  # v8.0.86: 4→8 (كل العملات في دورة 5m واحدة)
_price_state = {}
_recent_signals = {}
_last_range = {}
_last_6pct_report = {}
_pending_signals = {}
_divergence_cache = {}
_bb_cache = {}
_sr_cache = {}
_vwap_breakout_cache = {}
_ichimoku_cache = {}
_sz_cache = {}  # 🆕 v7.9.8 — Smart Zones cache
_SZ_CACHE_TTL = 600  # 🆕 v7.9.9 B — 10 دقائق (zones 1h/4h بطيئة التغير)
_market_cache = {}  # 🆕 v7.9.9 — classify_market_safe
_htf_cache = {}     # 🆕 v7.9.9 — get_htf_trend
_mtf_cache = {}     # 🆕 v7.9.9 — get_mtf_alignment
_MARKET_CACHE_TTL = 60  # دقيقة
_HTF_CACHE_TTL = 60     # دقيقة
_MTF_CACHE_TTL = 60     # دقيقة
_voting_history = {}
_last_signal_time = {}
_last_signals = deque(maxlen=LAST_SIGNALS_MAX)

_filter_stats = defaultdict(int)

_detector_stats = defaultdict(lambda: {
    "calls": 0, "none": 0, "cross": 0, "filtered": 0, "sent": 0
})
# 🩹 FIX #4: map cleaned detector names → canonical source keys
_DETECTOR_SOURCE_MAP = {
    "pre": "ema", "live": "ema", "cross": "ema",
    "rsi": "rsi_div", "bb_squeeze": "bb_squeeze",
    "sr": "sr_breakout", "vwap": "vwap_breakout",
    "ichimoku": "ichimoku",
}
_filter_stats.update({
    "sent": 0, "filtered_score": 0, "filtered_score_abs": 0,
    "filtered_vol": 0, "filtered_adx": 0,
    "filtered_atr": 0, "filtered_atr_low": 0, "filtered_htf": 0,
    "filtered_vwap": 0, "filtered_conflict": 0, "filtered_range": 0,
    "filtered_mtf": 0, "filtered_trend": 0, "filtered_ema_gap": 0,
    "filtered_cooldown": 0, "filtered_bb_confirm": 0, "filtered_other": 0,
    "range_breakouts": 0, "range_approaches": 0,
    "range_6pct_reports": 0, "morning_reports": 0, "signals_tracked": 0,
    "signals_divergence": 0, "signals_bb": 0, "signals_sr": 0,
    "followups_sent": 0, "charts_sent": 0,
    "trades_opened": 0, "trades_closed": 0, "trades_failed": 0,
    "trades_tp": 0, "trades_sl": 0, "trades_manual_sync": 0,
    "trades_manual_close": 0, "paper_signals": 0,
    "tp2_skipped_no_edge": 0,
})


# ═══════════════════════════════════════════════════════════
# 8) جلب الشموع
# ═══════════════════════════════════════════════════════════

# 🆕 v8.0.86 — فحص عمر الشمعة (يمنع إرجاع شمعة مغلقة قديمة)
_TF_MS_CACHE = {"1m": 60_000, "5m": 300_000, "15m": 900_000,
                "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

def _cached_data_is_fresh(cached, tf):
    """هل الشمعة الأخيرة في الكاش ما زالت مفتوحة/حديثة؟"""
    try:
        data = cached.get("data") or []
        if not data:
            return False
        last_ts = int(data[-1][0])
        tf_ms = _TF_MS_CACHE.get(tf, 300_000)
        # إذا مرّ أكثر من فترتين على فتح الشمعة الأخيرة → stale
        age_ms = int(_time.time() * 1000) - last_ts
        return age_ms < tf_ms * 2
    except Exception:
        return False
async def fetch_ohlcv_cached(symbol, tf, limit=150):
    """🆕 v7.9.9 — Unified cache key (symbol, tf) — no limit
    Always fetches MAX_FETCH=200, slices to requested limit.
    Reduces ~6.5 fetches/call to 1 fetch/call.
    """
    _MAX_FETCH = 200
    key = (symbol, tf)  # ← v7.9.9: NO limit in key
    now = _time.time()
    cached = _ohlcv_cache.get(key)
    if (cached
            and (now - cached["ts"]) < OHLCV_CACHE_SECONDS
            and _cached_data_is_fresh(cached, tf)):  # v8.0.86
        data = cached["data"]
        if len(data) >= limit:
            return data[-limit:] if limit < len(data) else data
    try:
        result = await asyncio.wait_for(
            _exchange.fetch_ohlcv(symbol, tf, _MAX_FETCH),
            timeout=20.0)
        data, ex_id = result
    except asyncio.TimeoutError:
        log.warning(f"fetch_ohlcv_cached timeout {symbol} {tf}")
        return None
    except Exception as e:
        log.warning(f"fetch_ohlcv_cached {symbol} {tf}: {e}")
        return None
    if data:
        _ohlcv_cache[key] = {"data": data, "ts": now, "ex": ex_id}
        return data[-limit:] if limit < len(data) else data
    return data


def clear_caches():
    _ohlcv_cache.clear(); _crossover_cache.clear()
    _recent_signals.clear(); _divergence_cache.clear()
    _bb_cache.clear(); _sr_cache.clear()
    _sz_cache.clear()       # 🆕 v7.9.8
    _market_cache.clear()   # 🆕 v7.9.9
    _htf_cache.clear()      # 🆕 v7.9.9
    _mtf_cache.clear()      # 🆕 v7.9.9


# ═══════════════════════════════════════════════════════════
# 9) HTF / VWAP / MTF
# ═══════════════════════════════════════════════════════════
async def check_1m_candles(symbol, direction, n=5, min_agree=4):
    """
    🆕 v7.9.40 — تأكيد 1m: على الأقل min_agree من n شمعات في الاتجاه.
    Returns: (passed, count, total)
    """
    try:
        data = await fetch_ohlcv_cached(symbol, "1m", n + 2)
        if not data or len(data) < n:
            return False, 0, 0
        # نأخذ آخر n شمعة مغلقة
        candles = data[-n-1:-1]  # exclude current unclosed
        if direction == "bullish":
            agree = sum(1 for c in candles if c[4] > c[1])  # close > open
        else:
            agree = sum(1 for c in candles if c[4] < c[1])
        return agree >= min_agree, agree, n
    except Exception as e:
        log.debug(f"check_1m_candles {symbol}: {e}")
        return False, 0, 0


async def get_tf_trend(symbol, tf):
    """🆕 v8.7.0 — نستخدم 1h فقط كإطار أعلى"""
    if not HTF_ENABLED: return "NEUTRAL"
    if tf not in ("1h", "4h"):
        tf = "1h"  # 🆕 نفرض 1h دائماً
    data = await fetch_ohlcv_cached(symbol, tf, 100)
    if not data or len(data) < 50: return "NEUTRAL"
    df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
    e_fast = float(calc_ema(df["c"], EMA_FAST).iloc[-2])
    e_slow = float(calc_ema(df["c"], EMA_SLOW).iloc[-2])
    price = float(df["c"].iloc[-2])
    if price > e_fast > e_slow: return "UP"
    if price < e_fast < e_slow: return "DOWN"
    return "NEUTRAL"


async def get_htf_trend(symbol):
    if not HTF_ENABLED: return "NEUTRAL"
    # 🆕 v7.9.9 — Cache
    _now = _time.time()
    _cached = _htf_cache.get(symbol)
    if _cached and (_now - _cached["ts"]) < _HTF_CACHE_TTL:
        return _cached["trend"]
    
    data = await fetch_ohlcv_cached(symbol, HTF_TIMEFRAME, 250)
    if not data or len(data) < 200:
        _htf_cache[symbol] = {"trend": "NEUTRAL", "ts": _now}
        return "NEUTRAL"
    df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
    e50 = float(calc_ema(df["c"], 50).iloc[-1])
    e200 = float(calc_ema(df["c"], 200).iloc[-1])
    price = float(df["c"].iloc[-1])
    if price > e50 > e200:
        trend = "UP"
    elif price < e50 < e200:
        trend = "DOWN"
    else:
        trend = "NEUTRAL"
    _htf_cache[symbol] = {"trend": trend, "ts": _now}
    return trend


async def get_mtf_alignment(symbol, direction):
    if not MTF_ENABLED: return 0, "—"
    # 🆕 v7.9.9 — Cache
    _cache_key = (symbol, direction)
    _now = _time.time()
    _cached = _mtf_cache.get(_cache_key)
    if _cached and (_now - _cached["ts"]) < _MTF_CACHE_TTL:
        return _cached["bonus"], _cached["desc"]
    
    score = 0
    for tf in ["5m", "15m", "1h"]:
        try:
            data = await fetch_ohlcv_cached(symbol, tf, 60)
            if not data or len(data) < 30: continue
            df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
            ef = float(calc_ema(df["c"], EMA_FAST).iloc[-2])
            es = float(calc_ema(df["c"], EMA_SLOW).iloc[-2])
            if direction == "bullish" and ef > es: score += 1
            elif direction == "bearish" and ef < es: score += 1
        except Exception: continue
    if score >= 3:   bonus, desc = +8, "Full alignment (3/3) 🌟"
    elif score == 2: bonus, desc = +3, "Partial alignment (2/3) ✅"
    elif score == 1: bonus, desc = -5, "Weak alignment (1/3) ⚠️"
    else:            bonus, desc = -15, "Full conflict (0/3) ❌"
    
    _mtf_cache[_cache_key] = {"bonus": bonus, "desc": desc, "ts": _now}
    return bonus, desc


def check_vwap_slope(df, direction, thresholds, tf="15m"):
    min_pct = thresholds.get("vwap_slope_pct", 0.05)
    if len(df) < VWAP_SLOPE_LOOKBACK + 1: return True, 0.0
    session = "week" if tf == "1h" else "day"
    vwap = calc_vwap_session(df, session)
    v_now = float(vwap.iloc[-1]); v_prev = float(vwap.iloc[-VWAP_SLOPE_LOOKBACK - 1])
    if v_prev == 0: return True, 0.0
    slope = (v_now - v_prev) / v_prev * 100
    if direction == "bullish" and slope < -min_pct: return False, slope
    if direction == "bearish" and slope > min_pct:  return False, slope
    return True, slope


def has_conflicting_signal(symbol, direction):
    prev = _recent_signals.get(symbol)
    if not prev: return False
    if _time.time() - prev[1] < CONFLICT_WINDOW_SEC and prev[0] != direction:
        return True
    return False


def record_signal(symbol, direction):
    _recent_signals[symbol] = (direction, _time.time())


def check_symbol_cooldown(symbol):
    if SYMBOL_COOLDOWN_SEC <= 0: return True, 0
    now = _time.time()
    last = _last_signal_time.get(symbol, 0)
    if now - last < SYMBOL_COOLDOWN_SEC:
        return False, int(SYMBOL_COOLDOWN_SEC - (now - last))
    return True, 0


def mark_symbol_sent(symbol):
    _last_signal_time[symbol] = _time.time()


# ═══════════════════════════════════════════════════════════
# 10) Score
# ═══════════════════════════════════════════════════════════
def compute_score(df, direction, curr_idx=-2):
    close = df["c"]; vol = df["v"]
    price = float(close.iloc[curr_idx])
    try:
        s = max(0, len(df) + curr_idx - 20); e = len(df) + curr_idx
        vma = float(vol.iloc[s:e].mean())
        vr = float(vol.iloc[curr_idx]) / vma if vma > 0 else 0
    except Exception: vr = 0
    v_pts = 30 if vr >= 4 else 25 if vr >= 2.5 else 18 if vr >= 1.5 else 10 if vr >= 1 else 5 if vr >= 0.5 else 0
    try: adx = float(calc_adx(df).iloc[curr_idx])
    except Exception: adx = 0
    a_pts = 20 if adx >= 50 else 18 if adx >= 35 else 14 if adx >= 25 else 10 if adx >= 20 else 5 if adx >= 15 else 0
    try:
        ef = float(calc_ema(close, EMA_FAST).iloc[curr_idx])
        es = float(calc_ema(close, EMA_SLOW).iloc[curr_idx])
        gap = abs(ef - es) / es * 100 if es else 0
    except Exception: gap = 0
    g_pts = 15 if gap >= 0.6 else 12 if gap >= 0.3 else 8 if gap >= 0.1 else 5 if gap >= 0.05 else 3 if gap >= 0.02 else 0
    try:
        _, _, hist = calc_macd(close)
        hn = float(hist.iloc[curr_idx]); hp = float(hist.iloc[curr_idx - 1])
        rising = hn > hp
        m_pts = 10 if (hn > 0 and rising) else 5 if hn > 0 else 7 if (hn < 0 and rising) else 5
    except Exception: m_pts = 0
    try: rsi = float(calc_rsi(close).iloc[curr_idx])
    except Exception: rsi = 50
    if direction == "bullish":
        r_pts = 10 if 55 <= rsi <= 70 else 5 if rsi > 70 else 7 if rsi >= 45 else 3 if rsi >= 30 else 0
    else:
        r_pts = 10 if 30 <= rsi <= 45 else 5 if rsi < 30 else 7 if rsi <= 55 else 3 if rsi <= 70 else 0
    try: e50 = float(calc_ema(close, 50).iloc[curr_idx])
    except Exception: e50 = price
    above = price > e50
    h_pts = 10 if (direction == "bullish" and above) or (direction == "bearish" and not above) else 0
    try:
        atr = float(calc_atr(df).iloc[curr_idx])
        atr_pct = (atr / price) * 100 if price > 0 else 0
    except Exception: atr_pct = 0
    t_pts = 5 if atr_pct >= 0.30 else 3 if atr_pct >= 0.15 else 1
    total = v_pts + a_pts + g_pts + m_pts + r_pts + h_pts + t_pts
    grade = ("🌟 ذهبية" if total >= 80 else "⭐ قوية" if total >= 65 else
             "✅ جيدة" if total >= 55 else "🟡 متوسطة" if total >= 45 else "⚪ ضعيفة")
    return {"score": total, "grade": grade,
            "adx": round(adx, 1), "adx_pts": a_pts,
            "rsi": round(rsi, 1), "rsi_pts": r_pts, "macd_pts": m_pts,
            "vol_ratio": round(vr, 2), "vol_pts": v_pts,
            "atr_pct": round(atr_pct, 2), "atr_pts": t_pts, "htf_pts": h_pts,
            "gap_pct": round(gap, 3), "gap_pts": g_pts}


# ═══════════════════════════════════════════════════════════
# 11) الفلتر
# ═══════════════════════════════════════════════════════════
async def passes_filter(cross, htf_trend=None):
    # 🆕 v9.25.0 — تجاوز الفلاتر لإشارات Supertrend
    if cross.get("source") == "supertrend":
        return True, "", ""
    """🩹 FIX #19: wrapper records rejections to _smart."""
    # 🆕 v9.25.0 — تجاوز كل الفلاتر لإشارات Supertrend (لاختبار الكاشف فقط)
    if cross.get("source") == "supertrend":
        return True, "", ""
    result = await _passes_filter_inner(cross, htf_trend)
    passed, reason, cat = result
    if not passed and STRICTNESS == "auto":
        source = cross.get("source", "unknown")
        score = cross.get("final_score", cross.get("base_score", 0))
        _smart.record(False, score, source=source)
    return result


def _paper_or_reject(cross, reason, cat, score):
    """
    🆕 v7.9.46 — يحوّل الرفض إلى Paper إذا كان يستحق المراقبة.
    - score >= PAPER_MIN_SCORE → Paper (مع تصنيف السبب)
    - cat في _PAPER_EXCLUDE_CATS → رفض نهائي (مؤقت)
    - غير ذلك → رفض نهائي
    Returns: (passed, reason, cat) — متوافق مع الـcaller
    """
    # v8.0.112 — تتبع الرفض
    try:
        _rej = cross.get("rejections", [])
        _fname = (cat or "?").upper()
        import re as _re2
        _m2 = _re2.search(r'(\d+\.?\d*)', str(reason))
        _val = float(_m2.group(1)) if _m2 else 0
        _t2 = _re2.search(r'[<>]\s*(\d+\.?\d*)', str(reason))
        _thr = float(_t2.group(1)) if _t2 else 0
        _dist = abs(_val - _thr) if _thr > 0 else 0
        _rej.append({"filter": _fname, "value": _val, "threshold": _thr, "dist": round(_dist, 3)})
        cross["rejections"] = _rej
    except Exception:
        pass
    
    # 🆕 v8.4.7 — كل رفض يذهب للورقي (بدون استثناء Score)
    # 🆕 v9.12.0 — حفظ الإشارات في الورقي حتى لو لم تكن هناك مستويات
    if PAPER_MODE_ENABLED:
        cross["paper_signal"] = True
        cross["paper_reason"] = f"{cat}: {reason}"
        cross["paper_type"] = ("operational"
                                if cat in _PAPER_OPERATIONAL_CATS
                                else "technical")
        log.info(f"📄 Paper [forced] [{cat}] {short(cross.get('symbol','?'))} "
                 f"score={score} — {reason}")
        return True, "", "paper"
    return False, reason, cat



def _get_higher_tf(tf):
    """🆕 v8.7.2 — الإطار الأعلى ديناميكياً"""
    mapping = {
        "1m": "5m",
        "5m": "15m",
        "15m": "1h",
        "1h": "4h",
        "4h": "1d",
    }
    return mapping.get(tf, "1h")

async def _get_tf_direction(symbol, tf):
    """🆕 v8.7.2 — اتجاه الإطار الأعلى (UP/DOWN/NEUTRAL)"""
    try:
        data = await fetch_ohlcv_cached(symbol, tf, 100)
        if not data or len(data) < 50:
            return "NEUTRAL"
        df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
        e7 = calc_ema(df["c"], 7)
        e25 = calc_ema(df["c"], 25)
        price = float(df["c"].iloc[-2])
        e7_val = float(e7.iloc[-2])
        e25_val = float(e25.iloc[-2])
        e7_prev = float(e7.iloc[-5])
        slope = (e7_val - e7_prev) / e7_prev * 100 if e7_prev > 0 else 0
        if price > e7_val > e25_val and slope > 0.05:
            return "UP"
        if price < e7_val < e25_val and slope < -0.05:
            return "DOWN"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"

async def _passes_filter_inner(cross, htf_trend=None):
    s = cross.get("support", {})
    if not s: return False, "no مؤشرات", "other"
    tf = cross["timeframe"]
    vol = s.get("vol_ratio", 0); adx = s.get("adx", 0)
    atr = s.get("atr_pct", 0); gap_pct = s.get("gap_pct", 0)
    source = cross.get("source", "ema")
    direction = cross["direction"]
    base_score = cross.get("base_score", s.get("score", 0))

    if SYMBOL_COOLDOWN_SEC > 0:
        ok_cd, remaining = check_symbol_cooldown(cross["symbol"])
        if not ok_cd: return False, f"cooldown {remaining}s", "cooldown"

    min_gap = MIN_EMA_GAP_PCT.get(tf, 0.02)
    if gap_pct < min_gap and source == "ema":
        # 🆕 v6.7.2: استثناء confirmed و live
        if cross.get("alert_type") not in ("confirmed", "live"):
            return _paper_or_reject(cross, f"EMA gap {gap_pct:.3f}% < {min_gap}%", "ema_gap", base_score)

    min_atr = MIN_ATR_PCT.get(tf, 0.05)
    if atr < min_atr: return _paper_or_reject(cross, f"ATR {atr}% < {min_atr}%", "atr_low", base_score)

    # 🆕 v8.7.0 — MTF معلّق
    mtf_bonus, mtf_desc = 0, "MTF معلق"
    cross["mtf"] = mtf_desc; cross["mtf_bonus"] = mtf_bonus
    score = base_score + mtf_bonus
    s["score"] = score; cross["final_score"] = score

    # v7.1.0: determine market early for contextual MTF
    _df_early = cross.get("_df")
    market_early = cross.get("market") or "UNKNOWN"
    if market_early == "UNKNOWN":
        market_early = await classify_market_safe(cross["symbol"], tf, _df_early)
        cross["market"] = market_early

    # v7.1.0: 0/3 rejected always
    # 🆕 v7.9.11 — ema: allow 0/3 if score>=70
    if mtf_bonus <= -15:
        # 🆕 v7.9.21 — ema/vwap allowed at MTF 0/3 if score >= 40 (Paper: 71.4%/100% WR)
        if source in ("ema", "vwap_breakout") and score >= 40:
            log.info(f"🔓 [{source}] MTF 0/3 but score={score} — allowed")
        else:
            return _paper_or_reject(cross, mtf_desc, "mtf", base_score)
    # v7.1.0 contextual: trend needs >=2/3, range accepts >=1/3
    if market_early in ("RANGE", "UNKNOWN"):
        mtf_reject = mtf_bonus <= -10
    else:
        mtf_reject = mtf_bonus <= -5
    if mtf_reject and cross.get("alert_type") == "confirmed":
        return _paper_or_reject(cross, f"MTF weak {mtf_desc}", "mtf_weak", score)

    # v7.1.0 Trend Filter for reversion detectors (ichimoku, rsi_div)
    if source in ("ichimoku", "rsi_div") and cross.get("levels"):
        _sz_dir = cross["levels"].get("combined")
        if _sz_dir == "DOWN" and direction == "bullish":
            return _paper_or_reject(cross, f"reversion vs 4H/1H {_sz_dir}", "trend_filter", score)
        if _sz_dir == "UP" and direction == "bearish":
            return _paper_or_reject(cross, f"reversion vs 4H/1H {_sz_dir}", "trend_filter", score)

    # 🆕 v7.9.12 — 1h/15m agreement for ichimoku (avoid trading against 15m recovery)
    if source == "ichimoku":
        _t15 = await get_tf_trend(cross["symbol"], "15m")
        if direction == "bearish" and _t15 == "UP":
            return _paper_or_reject(cross, f"15m UP — bearish blocked", "tf_align", score)
        if direction == "bullish" and _t15 == "DOWN":
            return _paper_or_reject(cross, f"15m DOWN — bullish blocked", "tf_align", score)

    # 🆕 v7.9.14 — bb_squeeze: also respect 15m direction (prevent LONG against 15m DOWN)
    if source == "bb_squeeze":
        _t15_bb = await get_tf_trend(cross["symbol"], "15m")
        if direction == "bullish" and _t15_bb == "DOWN":
            return _paper_or_reject(cross, f"bb_squeeze 15m DOWN — LONG blocked", "tf_align", score)
        if direction == "bearish" and _t15_bb == "UP":
            return _paper_or_reject(cross, f"bb_squeeze 15m UP — SHORT blocked", "tf_align", score)

    # 🆕 v7.9.16 — rsi_div: also respect 15m direction
    if source == "rsi_div":
        _t15_rsi = await get_tf_trend(cross["symbol"], "15m")
        if direction == "bullish" and _t15_rsi == "DOWN":
            return _paper_or_reject(cross, f"rsi_div 15m DOWN — LONG blocked", "tf_align", score)
        if direction == "bearish" and _t15_rsi == "UP":
            return _paper_or_reject(cross, f"rsi_div 15m UP — SHORT blocked", "tf_align", score)

    # 🩹 FIX v7.9.5: combined_trend (with ema/vwap exception)
    # 🆕 v8.7.3 — الترند من الإطار الأعلى ديناميكياً
    _htf = _get_higher_tf(cross["timeframe"])
    _htf_dir = await _get_tf_direction(cross["symbol"], _htf)
    if _htf_dir == "DOWN" and direction == "bullish":
        # v8.9.6 — شرط صارم للصفقات ضد الترند: score >= 75 + EMA7 صاعد بقوة
        _e7_strong = False
        try:
            _df_e7 = cross.get("_df")
            if _df_e7 is not None and len(_df_e7) >= 10:
                _e7_series = calc_ema(_df_e7["c"], 7)
                _e7_curr = float(_e7_series.iloc[-1])
                _e7_prev = float(_e7_series.iloc[-2])
                _pr7_curr = float(_df_e7["c"].iloc[-1])
                # EMA7 يجب أن يكون صاعداً (ميل إيجابي) والسعر فوقه
                if _pr7_curr > _e7_curr and _e7_curr > _e7_prev:
                    _e7_strong = True
        except Exception:
            pass
        if score >= 75 and _e7_strong:
            log.info(f"🔓 [trend_htf] LONG vs {_htf} DOWN but score={score} and EMA7 strong — allowed")
        else:
            return _paper_or_reject(cross, f"LONG vs {_htf} DOWN (score={score}, e7_strong={_e7_strong})", "trend_htf", score)
            
    if _htf_dir == "UP" and direction == "bearish":
        _e7_strong = False
        try:
            _df_e7 = cross.get("_df")
            if _df_e7 is not None and len(_df_e7) >= 10:
                _e7_series = calc_ema(_df_e7["c"], 7)
                _e7_curr = float(_e7_series.iloc[-1])
                _e7_prev = float(_e7_series.iloc[-2])
                _pr7_curr = float(_df_e7["c"].iloc[-1])
                # EMA7 يجب أن يكون هابطاً (ميل سلبي) والسعر تحته
                if _pr7_curr < _e7_curr and _e7_curr < _e7_prev:
                    _e7_strong = True
        except Exception:
            pass
        if score >= 75 and _e7_strong:
            log.info(f"🔓 [trend_htf] SHORT vs {_htf} UP but score={score} and EMA7 strong — allowed")
        else:
            return _paper_or_reject(cross, f"SHORT vs {_htf} UP (score={score}, e7_strong={_e7_strong})", "trend_htf", score)

    # v7.1.0: alias for compatibility
    market = market_early
    df = _df_early
    # 🆕 v7.9.11 — ema ABS_MIN softer (55 vs 60)
    # 🆕 v8.0.28 — Time-based filter (Study v2, UTC)
    _tf_factor, _tf_mult = _get_time_factor()
    if False and _tf_mult == 0.0:  # TEST
        return _paper_or_reject(cross, f"Dead time f={_tf_factor:.2f}", "dead_time", score)
    cross["_time_factor"] = round(_tf_factor, 2)
    cross["_time_tp1_mult"] = _tf_mult

    # 🆕 v8.0.24 — النطاق الإلزامي 40-60 للمصادر الثلاثة
    # 🆕 v8.0.80 — ema معطل (Paper فقط، PnL -2.13%)
    if source == "ema":
        # v8.0.98 — ema مُفعّل + شرط اتجاه (ADX ≥ 20 + EMA50)
        if adx < 20:
            return _paper_or_reject(cross, f"ema ADX {adx:.1f} < 20", "ema_weak", score)
        try:
            _df_ema = cross.get("_df")
            if _df_ema is not None and len(_df_ema) >= 55:
                _e50 = float(calc_ema(_df_ema["c"], 50).iloc[-2])
                _pr = float(_df_ema["c"].iloc[-2])
                if direction == "bearish" and _pr > _e50:
                    return _paper_or_reject(cross, f"ema SHORT above EMA50", "ema_ema50", score)
                if direction == "bullish" and _pr < _e50:
                    return _paper_or_reject(cross, f"ema LONG below EMA50", "ema_ema50", score)
        except Exception:
            pass
        
        # 🆕 v9.1.3 — شرط توافق 5m مع اتجاه الإشارة (لـ ema فقط)
        if cross["timeframe"] != "5m":
            try:
                _t5 = await get_tf_trend(cross["symbol"], "5m")
                if direction == "bullish" and _t5 == "DOWN":
                    return _paper_or_reject(cross, f"ema 5m DOWN (block LONG)", "ema_5m_align", score)
                if direction == "bearish" and _t5 == "UP":
                    return _paper_or_reject(cross, f"ema 5m UP (block SHORT)", "ema_5m_align", score)
            except Exception:
                pass
        
        log.info(f"✅ [ema] {short(cross['symbol'])} score={score} ADX={adx:.1f} — strong trend")
        # 🆕 v8.9.0 — السكور الأدنى 46 لـ ema أيضاً
        if score < 46:
            return _paper_or_reject(cross, f"Score {score} < 46", "score", score)
    
    # v8.0.90 — ranges مخصصة لكل مصدر
    # v8.0.104 — لا حد أعلى (Score ≥ min فقط)
    # 🆕 v8.7.0 — كل المصادر تحتاج 46+
    _ranges = {
        "sr_breakout": (46, 100),
        "vwap_breakout": (46, 100),
        "bb_squeeze": (46, 100),
    }
    
    if source in _ranges:
        _min, _max = _ranges[source]
        if score < _min or score > _max:
            if PAPER_MODE_ENABLED and score >= PAPER_MIN_SCORE:
                cross["paper_signal"] = True
                cross["paper_reason"] = f"Score {score} outside {_min}-{_max}"
                log.info(f"📄 Paper (out of range): {short(cross['symbol'])} [{source}] score={score}")
                return True, "", "paper"
            else:
                return False, f"Score {score} outside {_min}-{_max}", "score_out_of_range"
        else:
            # 🆕 v8.6.0 — لا نعود هنا! نكمل للفلاتر التالية (الترند، MTF، إلخ)
            log.info(f"🚀 Live: {short(cross['symbol'])} [{source}] score={score}")
    
    # v8.0.89c — توحيد ABS_MIN مع passes_filter
    # v8.0.98 — Score 46-75 لكل المصادر
    # 🆕 v8.7.0 — السكور الأدنى 46 لكل المصادر
    _abs_min_for_source = 46
    
    if score < _abs_min_for_source:
        if PAPER_MODE_ENABLED and score >= PAPER_MIN_SCORE:
            cross["paper_signal"] = True
            cross["paper_reason"] = f"Score {score} < {ABS_MIN_SCORE}"
            log.info(f"📄 Paper: {short(cross['symbol'])} [{source}] score={score}")
            return True, "", "paper"
        else:
            return False, f"Score {score} < {_abs_min_for_source}", "score_abs"


    th = get_thresholds(atr_pct=atr, adx=adx, tf=tf)
    min_score = th["min_score"]
    # 🆕 v7.9.18 — 5m: min_score in [40, 54] (Paper: 58.3% WR)
    if tf == "5m":
        min_score = max(40, min(min_score, 54))
    if tf == "15m":
        _override = th.get("score_15m_override", min_score)
        # 🆕 v7.9.12 — ema: cap at 62 (was 74-77)
        if source == "ema":
            _override = min(_override, 62)
        min_score = max(min_score, _override)
    
    # 🆕 v8.0.17 — فلتر صارم لـ ichimoku (ADX > 30 + Score >= 65)
    if source == "ichimoku":
        # v8.0.98 — ichimoku مُفعّل + شرط ترند قوي (ADX ≥ 20 + EMA50)
        if adx < 20:
            return _paper_or_reject(cross, f"ichimoku ADX {adx:.1f} < 20 (not trending)", "ichimoku_weak", score)
        # EMA50 filter
        try:
            _df_ichi = cross.get("_df")
            if _df_ichi is not None and len(_df_ichi) >= 55:
                _e50 = float(calc_ema(_df_ichi["c"], 50).iloc[-2])
                _pr = float(_df_ichi["c"].iloc[-2])
                if direction == "bearish" and _pr > _e50:
                    return _paper_or_reject(cross, f"ichimoku SHORT above EMA50", "ichimoku_ema50", score)
                if direction == "bullish" and _pr < _e50:
                    return _paper_or_reject(cross, f"ichimoku LONG below EMA50", "ichimoku_ema50", score)
        except Exception:
            pass
        log.info(f"✅ [ichimoku] {short(cross['symbol'])} score={score} ADX={adx:.1f} — strong trend")
        # 🆕 v8.0.72 — إضافة فلتر "قوة الحركة" (آخر 3 شمعات)
        # 1. ADX > 15
        # 2. Score 40-70
        # 3. آخر 3 شمعات: مدى الحركة > 0.2% (ليس ثابتاً)
        
        if adx < 15:
            return _paper_or_reject(cross, f"ichimoku ADX {adx:.1f} < 15", "ichimoku_adx", score)
        
        # 🆕 v8.0.83 — ichimoku: Score 56-70 تنفيذ، >70 أو <56 Paper
        if score < 54:  # v9.3.0: رفع السكور إلى 54
            cross["paper_signal"] = True
            cross["paper_reason"] = f"ichimoku_score: {score} < 54 (weak)"
            cross["paper_type"] = "technical"
            log.info(f"📄 Paper [ichimoku] {short(cross['symbol'])} score={score}")
            return True, "", "paper"

        
        # 🆕 v8.0.72 — فلتر قوة الحركة (آخر 3 شمعات)
        try:
            _df = cross.get("_df")
            if _df is not None and len(_df) >= 4:
                _last3 = _df.iloc[-4:-1]  # آخر 3 شمعات مغلقة
                _range_pct = ((_last3["h"].max() - _last3["l"].min()) / _last3["c"].iloc[-1]) * 100
                if _range_pct < 0.2:
                    return _paper_or_reject(cross, f"ichimoku weak move {_range_pct:.2f}% < 0.2%", "ichimoku_weak", score)
                log.info(f"✅ [ichimoku] strength {_range_pct:.2f}%")
        except Exception:
            pass
        
        # 🆕 v8.0.79 — Score 40-50 + ADX > 15 + strength > 0.2%: تنفيذ حقيقي
        log.info(f"✅ [ichimoku] OK {short(cross['symbol'])} score={score} ADX={adx:.1f}")
        # 🆕 v8.0.62 — تجاوز min_score الديناميكي (ichimoku له فلاتره الخاصة)
        return True, "", ""
    elif source == "rsi_div":
        min_score = max(min_score, 75)

    # 🆕 v7.9.21 — ema/vwap: min_score cap [40, 50] all TFs
    if source in ("ema", "vwap_breakout"):
        min_score = max(40, min(min_score, 50))
    
    # 🆕 v7.9.50 — تم إلغاء شرط 1m confirmation لـ ema 5m (كان يحجب تقاطعات قوية)
    # الاعتماد الآن على MIN_EMA_GAP + MTF فقط

    if score < min_score: return _paper_or_reject(cross, f"Score {score} < {min_score:.0f}", "score", score)
    
    # 🆕 v6.8.9 — فلتر R:R خفيف
    # v7.4.2 — check R:R vs TP2 (not TP1)
    if SMART_ZONES_ENABLED and cross.get("levels"):
        _rr1 = cross["levels"].get("rr1", 0)
        _rr2 = cross["levels"].get("rr2", 0)
        # Rule:
        #   TP1 R:R must be >= 0.6 (loose)
        #   TP2 R:R must be >= 1.0 (proper)
        if _rr2 > 0:
            # use TP2 as main filter
            if _rr2 < 1.0:
                return _paper_or_reject(cross, f"TP2 R:R {_rr2:.2f} < 1.0", "rr_weak", score)
        elif _rr1 > 0 and _rr1 < 0.6:
            # fallback to TP1 if no TP2
            return _paper_or_reject(cross, f"TP1 R:R {_rr1:.2f} < 0.6", "rr_weak", score)
        
        # 🆕 v8.7.3 — rr_smart محذوف
    
    # v7.1.1: volume exception for strong trend (ADX>35 AND gap>0.5%)
    _strong_trend = (adx > 35 and gap_pct > 0.5)
    _vol_needed = th["min_vol_ratio"]
    if _strong_trend:
        _vol_needed = min(_vol_needed, 0.6)
    if vol < _vol_needed:
        _tag = " (trend-exempt)" if _strong_trend else ""
        return _paper_or_reject(cross, f"Vol {vol}× < {_vol_needed}×{_tag}", "vol", score)
    # 🆕 v9.11.0 — حدود RSI صارمة (40-60) لمنع الدخول في القمم/القيعان
    try:
        _rsi_val = s.get("rsi", 50)
        if direction == "bullish" and _rsi_val > 60:
            return _paper_or_reject(cross, f"RSI {_rsi_val:.1f} > 60 (LONG)", "rsi_high", score)
        if direction == "bearish" and _rsi_val < 40:
            return _paper_or_reject(cross, f"RSI {_rsi_val:.1f} < 40 (SHORT)", "rsi_low", score)
    except Exception:
        pass

    # 🆕 v9.11.0 — فلتر مكافحة المطاردة (Anti-Chase)
    try:
        _df_ac = cross.get("_df")
        if _df_ac is not None and len(_df_ac) >= 5:
            # آخر 3 شمعات
            _c_last3 = float(_df_ac["c"].iloc[-2])
            _c_3ago = float(_df_ac["c"].iloc[-5])
            _move_15m = abs(_c_last3 - _c_3ago) / _c_3ago * 100
            
            if _move_15m > 0.3:
                _dir = "UP" if _c_last3 > _c_3ago else "DOWN"
                return _paper_or_reject(cross, f"Anti-Chase: تحرك {_dir} {_move_15m:.2f}% في 15m", "anti_chase", score)
    except Exception:
        pass

    # 🆕 v9.6.0 — ADX ديناميكي حسب نوع السوق
    _mkt = cross.get("market", "UNKNOWN")
    if source == "range_reversal":
        # السوق العرضي: ADX منخفض مسموح (10)
        if adx < 10:
            return _paper_or_reject(cross, f"range_rev ADX {adx:.1f} < 10", "adx", score)
    elif source == "ichimoku":
        if adx < 20:
            return _paper_or_reject(cross, f"ADX {adx:.1f} < 20", "adx", score)
    else:
        # الكواشف الأخرى: 25 في الترند، 10 في العرضي
        if _mkt == "RANGE":
            if adx < 10:
                return _paper_or_reject(cross, f"range ADX {adx:.1f} < 10", "adx", score)
        else:
            if adx < 25:
                return _paper_or_reject(cross, f"ADX {adx:.1f} < 25", "adx", score)
    if atr > th["max_atr_pct"]: return _paper_or_reject(cross, f"ATR {atr}% > {th['max_atr_pct']}%", "atr", score)

    if source == "bb_squeeze":
        if adx < MIN_ADX_FOR_BB: return _paper_or_reject(cross, f"ADX {adx} < {MIN_ADX_FOR_BB} for BB", "adx", score)
        if vol < BB_MIN_VOL: return _paper_or_reject(cross, f"BB Vol {vol}× < {BB_MIN_VOL}×", "vol", score)
        if market == "RANGE" and adx < BB_MIN_ADX_IN_RANGE:
            return _paper_or_reject(cross, f"BB في رينج يحتاج ADX ≥ {BB_MIN_ADX_IN_RANGE}", "range", score)
        if BB_CONFIRM_ENABLED:
            confirmed = (mtf_bonus >= 8) or (adx > 22) or (vol > 1.5)
            if not confirmed: return _paper_or_reject(cross, "BB without confirmation", "bb_confirm", score)
        # 🩹 FIX v7.9.5: bb_squeeze يحتاج score ≥ 70 (WR 33% قبل التعديل)
        if score < 70:
            return _paper_or_reject(cross, f"BB score {score} < 70", "bb_quality", score)

    # 🆕 v9.14.1 — فلتر التوافق (Confluence) قبل Supertrend
    try:
        _conf_score = _calc_confluence(cross, direction)
        _htf_15m = await get_tf_trend(cross["symbol"], "15m")
        
        if _htf_15m != "NEUTRAL":
            _is_against = (
                (_htf_15m == "DOWN" and direction == "bullish") or
                (_htf_15m == "UP" and direction == "bearish")
            )
            if _is_against and _conf_score < 3:
                return _paper_or_reject(cross, f"Confluence {_conf_score}/5 < 3 (against {_htf_15m})", "confluence", score)
            elif _is_against and _conf_score >= 3:
                log.info(f"🔓 [Confluence] {short(cross['symbol'])} {_conf_score}/5 (against {_htf_15m}) — allowed")
    except Exception:
        pass

    # 🆕 v9.22.0 — فلتر Supertrend معطل (لأن الكاشف نفسه هو Supertrend)

    # v8.0.111 — فلتر السوق العرضي لـ sr_breakout (ADX 20 + 1m confirm)
    if source == "sr_breakout":
        # 🆕 v9.4.0 — فلتر SR الذكي (يعتمد على Smart Zones)
        _sr_ok, _sr_reason = await _smart_sr_filter(cross, direction, score)
        if not _sr_ok:
            return _paper_or_reject(cross, f"smart_sr: {_sr_reason}", "smart_sr", score)
        log.info(f"✅ [sr] {short(cross['symbol'])} {_sr_reason}")

    # 🛡️ فحص الاتجاه for EMA + BB + S/R
    # 🆕 v8.7.3 — فلتر market محذوف (الترند الديناميكي يكفي)

    # 🆕 v8.7.3 — rsi_div لا يحتاج فلتر ترند إضافي

    if market == "RANGE" and source == "ema" and cross.get("alert_type") == "confirmed":
        # 🆕 v7.9.11 — allow ema in RANGE if ADX > 22
        if adx > 10:  # 🆕 v7.9.21: 15→10
            log.info(f"🔓 [ema] RANGE but ADX={adx:.1f}>10 — allowed")
        else:
            return _paper_or_reject(cross, "Ranging market - false cross", "range", score)

    if th["block_against_htf"] and cross.get("alert_type") != "pre" and htf_trend:
        # 🆕 v7.9.11 — ema: allow if mtf_bonus >= 8 (3/3 alignment compensates)
        # 🆕 v7.9.21 — ema/vwap always exempt from HTF (Paper: 71.4%/100% WR)
        _ema_htf_exempt = (source in ("ema", "vwap_breakout"))
        if direction == "bullish" and htf_trend == "DOWN":
            if not _ema_htf_exempt:
                return _paper_or_reject(cross, "HTF=DOWN against LONG", "htf", score)
            else:
                log.info(f"🔓 [ema] HTF=DOWN but MTF 3/3 — allowed")
        elif direction == "bearish" and htf_trend == "UP":
            if not _ema_htf_exempt:
                return _paper_or_reject(cross, "HTF=UP against SHORT", "htf", score)
            else:
                log.info(f"🔓 [ema] HTF=UP but MTF 3/3 — allowed")

    if df is not None:
        ok, slope = check_vwap_slope(df, direction, th, tf=tf)
        if not ok: return _paper_or_reject(cross, f"VWAP {slope:+.2f}% against", "vwap", score)

    # 🆕 v9.11.0 — تأكيد 1m: الصفقات القوية (>= 70) تُعفى
    try:
        if score < 70:
            _ok_1m, _cnt_1m = await check_live_confirmation(cross["symbol"], direction, n=3, min_agree=2)
            if not _ok_1m and _cnt_1m > 0:
                return _paper_or_reject(cross, f"1m confirm {_cnt_1m}/3", "confirm_1m", score)
        else:
            log.info(f"🔓 [1m exempt] {short(cross['symbol'])} score={score} >= 70")
    except Exception:
        pass

    if cross.get("alert_type") == "confirmed":
        if has_conflicting_signal(cross["symbol"], direction):
            return _paper_or_reject(cross, "Conflict", "conflict", score)

    # 🗳️ Voting System (v6.7.2 — مُصلَّح)
    if VOTING_ENABLED and cross.get("alert_type") == "confirmed":
        _sym_key = cross["symbol"]
        _now = _time.time()
        _hist = _voting_history.setdefault(_sym_key, [])
        _hist[:] = [(d, t) for d, t in _hist if _now - t < VOTING_WINDOW_SEC]
        
        _same = sum(1 for d, _ in _hist if d == direction)
        _opp = sum(1 for d, _ in _hist if d != direction)
        
        # 🆕 رفض إذا تعارض قوي (2+ معاكس، 0 نفس)
        if _opp >= 2 and _same == 0:
            return _paper_or_reject(cross, f"Voting: {_same}s/{_opp}o", "voting", score)
        
        # سجّل الإشارة
        _hist.append((direction, _now))
        
        # خفّض score إذا تعارض
        if _opp > 0:
            cross["final_score"] = score - 5
            log.info(f"🗳️ {cross['symbol']}: voting {_same}s/{_opp}o — score -5")

    if STRICTNESS == "auto": _smart.record(True, score, source=source)
    return True, "", ""


# ═══════════════════════════════════════════════════════════
# 12) كواشف الإشارات
# ═══════════════════════════════════════════════════════════
async def _df_for(symbol, tf, extra=60):
    data = await fetch_ohlcv_cached(symbol, tf, EMA_SLOW + extra)
    if not data or len(data) < EMA_SLOW + 5: return None
    return pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])


async def _make_cross(symbol, tf, df, direction, alert_type, strength, curr,
                     source="ema", extra=None):
    cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
    cts = int(df["ts"].iloc[curr])
    levels = await compute_levels_smart_aware(symbol, df, direction, curr, tf=tf, source=source)
    market = await classify_market_safe(symbol, tf, df)
    support = compute_score(df, direction, curr)
    out = {"symbol": symbol, "timeframe": tf, "direction": direction,
           "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
           "rejections": [],  # v8.0.112 — تتبع الرفض
           "price": round(float(df["c"].iloc[curr]), 8), "candle_ts": cts,
           "gap_pct": round(abs(cf - cs) / cs * 100, 3) if cs else 0,
           "strength": strength, "alert_type": alert_type,
           "support": support, "base_score": support["score"],
           "final_score": support["score"], "_df": df,
           "exchange": _exchange.primary_name, "levels": levels,
           "source": source, "market": market, "created_at": _time.time()}
    if extra: out.update(extra)
    return out


async def check_live_confirmation(symbol, direction, n=3, min_agree=2):
    """
    🆕 v8.0.30 — تأكيد الإشارات المبدئية (live) بـ 3 شمعات 1m.
    - n = 3 شمعات.
    - min_agree = 2 (على الأقل 2 من 3 في الاتجاه).
    """
    try:
        data = await fetch_ohlcv_cached(symbol, "1m", n + 2)
        if not data or len(data) < n:
            return False, 0
        candles = data[-n-1:-1]  # آخر n شمعة مغلقة
        if direction == "bullish":
            agree = sum(1 for c in candles if c[4] > c[1])
        else:
            agree = sum(1 for c in candles if c[4] < c[1])
        return agree >= min_agree, agree
    except Exception:
        return False, 0


async def detect_crossover(symbol, tf):
    if not ENABLE_CONFIRMED: return None
    df = await _df_for(symbol, tf)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr, prev = -2, -3
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])
    bull = (cf > cs) and (pf <= ps); bear = (cf < cs) and (pf >= ps)
    if not (bull or bear): return None
    d = "bullish" if bull else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf)
    last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts and last.get("direction") == d: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "confirmed", "✅ مؤكد", curr)


async def detect_live_crossover(symbol, tf):
    if not ENABLE_LIVE_CROSS: return None
    df = await _df_for(symbol, tf)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr, prev = -2, -3  # 🆕 v8.0.51 — العودة إلى الشمعة المغلقة (دقة أعلى)
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])
    bull = (cf > cs) and (pf <= ps); bear = (cf < cs) and (pf >= ps)
    if not (bull or bear): return None
    d = "bullish" if bull else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "live")
    last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "confirmed", "✅ مؤكد", curr)


_TF_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "1d": 86_400_000}


async def detect_pre_crossover(symbol, tf):
    if not ENABLE_PRE_CROSS: return None
    df = await _df_for(symbol, tf, extra=60)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr = -2
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    gap = abs(cf - cs) / cs * 100
    if gap >= PRE_CROSS_GAP: return None
    gaps = []
    for i in range(PRE_CROSS_LOOKBACK):
        idx = curr - i
        f = float(df["ef"].iloc[idx]); s = float(df["es"].iloc[idx])
        gaps.append(abs(f - s) / s * 100)
    gch = list(reversed(gaps))
    if not all(gch[i] >= gch[i+1] for i in range(len(gch)-1)): return None
    if abs(cf - cs) < 1e-9: return None
    d = "bullish" if cf < cs else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "pre")
    last = _crossover_cache.get(key)
    if last:
        tf_ms = _TF_MS.get(tf, 900_000)
        if cts - last.get("candle_ts", 0) < tf_ms * PRE_CROSS_COOLDOWN: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "pre", "🔔 تقارب وشيك", curr)


async def detect_rsi_divergence(symbol, tf):
    if not ENABLE_RSI_DIVERGENCE: return None
    df = await _df_for(symbol, tf, extra=RSI_DIV_LOOKBACK + 10)
    if df is None or len(df) < RSI_DIV_LOOKBACK + 5: return None
    rsi = calc_rsi(df["c"]); close = df["c"]
    curr = -2
    key = (symbol, tf, "div")
    last = _divergence_cache.get(key)
    if last and (_time.time() - last) < RSI_DIV_COOLDOWN_MIN * 60: return None
    win = RSI_DIV_LOOKBACK; best = None
    for i in range(3, win // 2):
        for j in range(i + 3, win):
            idx1 = curr - i; idx2 = curr - j
            if idx1 >= 0 or idx2 >= 0: continue
            c1 = float(close.iloc[idx1]); c2 = float(close.iloc[idx2])
            r1 = float(rsi.iloc[idx1]);   r2 = float(rsi.iloc[idx2])
            if c2 < c1 and r2 > r1 and r1 < 40:
                s = (r2 - r1) / max(1, 40 - r1)
                if not best or s > best["strength"]:
                    best = {"dir": "bullish", "strength": s,
                            "idx1": idx1, "idx2": idx2, "r1": r1, "r2": r2}
            if c2 > c1 and r2 < r1 and r1 > 60:
                s = (r1 - r2) / max(1, r1 - 60)
                if not best or s > best["strength"]:
                    best = {"dir": "bearish", "strength": s,
                            "idx1": idx1, "idx2": idx2, "r1": r1, "r2": r2}
    if not best or best["strength"] < 0.08: return None  # v7.4.0 relaxed
    d = best["dir"]
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _divergence_cache[key] = _time.time()
    _filter_stats["signals_divergence"] += 1
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "🔄 دايفرجنس RSI", curr,
                              source="rsi_div")
    cross["div_info"] = {"r1": round(best["r1"], 1), "r2": round(best["r2"], 1),
                         "ts1": int(df["ts"].iloc[best["idx1"]]),
                         "ts2": int(df["ts"].iloc[best["idx2"]])}
    return cross


async def _detect_macd_setup(df, curr=-2):
    """
    🆕 v8.0.34 — فحص ذكي لـ MACD قبل انفجار BB:
    1. حجم القضيب السابق مقبول (> 0.5x متوسط 20).
    2. تدرج في الهبوط/الارتفاع (3 قضبان).
    3. الشمعة الأخيرة (التي تسبق الانفجار) تعكس الاتجاه.
    """
    try:
        _, _, hist = calc_macd(df["c"])
        _h = hist.iloc[-10:].values  # آخر 10 قضبان
        
        # 1) حجم القضيب (القمة/القاع) مقبول
        _peak_val = abs(_h[-2])  # القضيب الذي يسبق الانفجار
        _avg_hist = abs(hist.iloc[-30:-10]).mean()
        if _avg_hist <= 0 or _peak_val < _avg_hist * 0.5:
            return False, "MACD peak too small"
        
        # 2) تدرج (3 قضبان متتالية في اتجاه واحد)
        _seq_up = all(_h[i] < _h[i+1] for i in range(-4, -1))
        _seq_down = all(_h[i] > _h[i+1] for i in range(-4, -1))
        if not (_seq_up or _seq_down):
            return False, "No MACD sequence"
        
        # 3) الشمعة الأخيرة تعكس الاتجاه (بداية الانفجار)
        _last = _h[-1]
        _prev = _h[-2]
        if _seq_down and _last > _prev:
            # كان هابطاً، بدأ يصعد
            if _last > 0: return True, "MACD reversal UP"
        elif _seq_up and _last < _prev:
            # كان صاعداً، بدأ يهبط
            if _last < 0: return True, "MACD reversal DOWN"
        
        return False, "No MACD reversal"
    except Exception as e:
        return False, f"MACD error: {e}"


async def _detect_macd_setup(df, curr=-2):
    """🆕 v8.0.34 — فحص ذكي لـ MACD قبل انفجار BB."""
    try:
        _, _, hist = calc_macd(df["c"])
        _h = hist.iloc[-10:].values
        
        _peak_val = abs(_h[-2])
        _avg_hist = abs(hist.iloc[-30:-10]).mean()
        if _avg_hist <= 0 or _peak_val < _avg_hist * 0.5:
            return False, "MACD peak too small"
        
        _seq_up = all(_h[i] < _h[i+1] for i in range(-4, -1))
        _seq_down = all(_h[i] > _h[i+1] for i in range(-4, -1))
        if not (_seq_up or _seq_down):
            return False, "No MACD sequence"
        
        _last = _h[-1]; _prev = _h[-2]
        if _seq_down and _last > _prev and _last > 0:
            return True, "MACD reversal UP"
        elif _seq_up and _last < _prev and _last < 0:
            return True, "MACD reversal DOWN"
        
        return False, "No MACD reversal"
    except Exception as e:
        return False, f"MACD error: {e}"


async def detect_bb_squeeze_breakout(symbol, tf):
    """🆕 v7.9.19 — BB Squeeze مع شروط زاوية (width slope, breakout distance, body ratio, EMA)"""
    if not ENABLE_BB_SQUEEZE and not PAPER_FORCE_DISABLED_DETECTORS: return None
    df = await _df_for(symbol, tf, extra=80)
    if df is None or len(df) < 60: return None
    bb = calc_bollinger(df); curr = -1  # 🆕 v8.0.49 — الشمعة الحالية (أسرع)
    key = (symbol, tf, "bb")
    last = _bb_cache.get(key)
    if last and (_time.time() - last) < BB_SQUEEZE_COOLDOWN_MIN * 60: return None
    
    # شرط 1: squeeze
    hist_width = bb["width"].iloc[curr-50:curr].dropna()
    if len(hist_width) < 20: return None
    if float(bb["width"].iloc[curr]) > hist_width.quantile(BB_SQUEEZE_PCTL): return None
    
    price = float(df["c"].iloc[curr])
    up = float(bb["upper"].iloc[curr]); lo = float(bb["lower"].iloc[curr])
    d = None
    if price > up:   d = "bullish"
    elif price < lo: d = "bearish"
    if not d: return None
    
    # 🆕 v7.9.19 — شروط الزاوية والقوة
    
    # 🆕 v9.7.0 — شرط 2: عرض البولينجر يجب أن يكون < 2% (سوق عرضي)
    _bb_w_now = float(bb["width"].iloc[curr])
    if _bb_w_now > 2.0:
        log.debug(f"bb_squeeze {symbol} {tf}: width {_bb_w_now:.2f}% > 2% (not range)")
        return None
    
    # شرط 2ب: توسع البولينجر (width slope > 2%)
    bb_width_prev = float(bb["width"].iloc[curr - 3])
    if bb_width_prev <= 0: return None
    width_slope_pct = (float(bb["width"].iloc[curr]) - bb_width_prev) / bb_width_prev * 100
    if width_slope_pct < 2.0:
        log.debug(f"bb_squeeze {symbol} {tf}: width_slope {width_slope_pct:.2f}% < 2%")
        return None
    
    # شرط 3: كسر حقيقي (breakout > 0.1%)
    if d == "bullish":
        breakout_pct = (price - up) / up * 100
    else:
        breakout_pct = (lo - price) / lo * 100
    if breakout_pct < 0.1:
        log.debug(f"bb_squeeze {symbol} {tf}: breakout {breakout_pct:.2f}% < 0.1%")
        return None
    
    # 🆕 v9.7.0 — شرط 4: قوة الشمعة + حجم قوي
    o = float(df["o"].iloc[curr]); c_ = float(df["c"].iloc[curr])
    h = float(df["h"].iloc[curr]); l = float(df["l"].iloc[curr])
    body = abs(c_ - o); rng = h - l
    body_ratio = body / rng if rng > 0 else 0
    if body_ratio < 0.5:
        log.debug(f"bb_squeeze {symbol} {tf}: body_ratio {body_ratio:.2f} < 0.5")
        return None
    
    # شرط 4ب: حجم > 1.5× (زخم قوي)
    try:
        _vma_bb = df["v"].iloc[curr-20:curr].mean()
        _vr_bb = float(df["v"].iloc[curr]) / float(_vma_bb) if _vma_bb > 0 else 0
        if _vr_bb < 1.5:
            log.debug(f"bb_squeeze {symbol} {tf}: vol {_vr_bb:.2f}x < 1.5x")
            return None
    except Exception:
        pass

    # 🆕 v8.0.34 — فحص MACD
    _macd_ok, _macd_reason = await _detect_macd_setup(df, curr)
    if not _macd_ok:
        log.debug(f"bb_squeeze {symbol} {tf}: {_macd_reason}")
        return None
    log.info(f"✅ BB Squeeze + MACD: {short(symbol)} {_macd_reason}")

    # 🆕 v8.0.34 — فحص MACD (تدرج + حجم + انعكاس)
    _macd_ok, _macd_reason = await _detect_macd_setup(df, curr)
    if not _macd_ok:
        log.debug(f"bb_squeeze {symbol} {tf}: {_macd_reason}")
        return None
    log.info(f"✅ BB Squeeze + MACD: {short(symbol)} {_macd_reason}")
    
    # شرط 5: توافق EMA
    ema_fast = float(calc_ema(df["c"], EMA_FAST).iloc[curr])
    ema_slow = float(calc_ema(df["c"], EMA_SLOW).iloc[curr])
    if d == "bullish" and ema_fast <= ema_slow:
        log.debug(f"bb_squeeze {symbol} {tf}: EMA ضد bullish")
        return None
    if d == "bearish" and ema_fast >= ema_slow:
        log.debug(f"bb_squeeze {symbol} {tf}: EMA ضد bearish")
        return None
    
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _bb_cache[key] = _time.time()
    _filter_stats["signals_bb"] += 1
    cross = await _make_cross(symbol, tf, df, d, "live", "💥 كسر BB Squeeze", curr,
                              source="bb_squeeze")
    if cross: cross["_needs_confirm"] = True
    cross["bb_info"] = {
        "width": round(float(bb["width"].iloc[curr]), 2),
        "upper": up, "lower": lo,
        "width_slope": round(width_slope_pct, 2),
        "breakout_pct": round(breakout_pct, 3),
        "body_ratio": round(body_ratio, 2),
    }
    return cross


async def detect_vwap_breakout(symbol, tf):
    """🆕 حزمة 3: كسر VWAP اليومي مع تأكيد"""
    if not ENABLE_VWAP_BREAKOUT: return None
    df = await _df_for(symbol, tf, extra=60)
    if df is None or len(df) < 30: return None
    curr = -1  # 🆕 v8.0.49 — الشمعة الحالية (أسرع)
    key = (symbol, tf, "vwap_b")
    last = _vwap_breakout_cache.get(key)
    if last and (_time.time() - last) < VWAP_BREAKOUT_COOLDOWN_MIN * 60:
        return None

    session = "week" if tf == "1h" else "day"
    try:
        vwap = calc_vwap_session(df, session)
    except Exception:
        return None
    if vwap is None or len(vwap) < 10: return None

    price_now = float(df["c"].iloc[curr])
    price_prev = float(df["c"].iloc[curr - 1])
    vwap_now = float(vwap.iloc[curr])
    vwap_prev = float(vwap.iloc[curr - VWAP_BREAKOUT_LOOKBACK])

    # ميل VWAP
    if vwap_prev <= 0: return None
    slope = (vwap_now - vwap_prev) / vwap_prev * 100

    # حجم
    try:
        vr = float(df["v"].iloc[curr]) / float(df["v"].iloc[curr-20:curr].mean())
    except Exception:
        vr = 0

    d = None
    if price_prev <= vwap_prev and price_now > vwap_now and slope > VWAP_BREAKOUT_MIN_SLOPE:
        d = "bullish"
    elif price_prev >= vwap_prev and price_now < vwap_now and slope < -VWAP_BREAKOUT_MIN_SLOPE:
        d = "bearish"
    else:
        return None

    if vr < 0.5: return None  # 🆕 v7.9.20: 0.7→0.5

    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _vwap_breakout_cache[key] = _time.time()
    cross = await _make_cross(symbol, tf, df, d, "live",  # 🆕 live (أسرع)
                              "📊 كسر VWAP", curr, source="vwap_breakout")
    if cross: cross["_needs_confirm"] = True
    cross["vwap_info"] = {"slope": round(slope, 3), "vr": round(vr, 2)}
    return cross


def calc_poc(df, bins=20):
    """v8.0.99 — Point of Control (Volume Profile)"""
    try:
        if df is None or len(df) < 30:
            return None
        _recent = df.iloc[-30:]
        _prices = (_recent["h"] + _recent["l"]) / 2
        _volumes = _recent["v"]
        _min = float(_prices.min())
        _max = float(_prices.max())
        if _max <= _min:
            return None
        _bin_size = (_max - _min) / bins
        _profile = {}
        for _p, _v in zip(_prices, _volumes):
            _bin = int((_p - _min) / _bin_size)
            _profile[_bin] = _profile.get(_bin, 0) + float(_v)
        if not _profile:
            return None
        _poc_bin = max(_profile, key=_profile.get)
        _poc = _min + (_poc_bin + 0.5) * _bin_size
        return _poc
    except Exception:
        return None


async def detect_ichimoku_breakout(symbol, tf):
    """🆕 حزمة 6: Ichimoku Cloud Breakout"""
    if not ENABLE_ICHIMOKU and not PAPER_FORCE_DISABLED_DETECTORS: return None
    df = await _df_for(symbol, tf, extra=100)
    if df is None or len(df) < 80: return None
    curr = -2
    key = (symbol, tf, "ichi")
    last = _ichimoku_cache.get(key)
    if last and (_time.time() - last) < ICHIMOKU_COOLDOWN_MIN * 60:
        return None

    try:
        h, l, c = df["h"], df["l"], df["c"]
        tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
        kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
        senkou_a = ((tenkan + kijun) / 2).shift(26)
        senkou_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)

        t_now = float(tenkan.iloc[curr]); k_now = float(kijun.iloc[curr])
        sa = float(senkou_a.iloc[curr]); sb = float(senkou_b.iloc[curr])
        price = float(c.iloc[curr])
        price_26 = float(c.iloc[curr - 26])
        c_now = float(c.iloc[curr])
    except Exception:
        return None

    if any(pd.isna(x) for x in [t_now, k_now, sa, sb]):
        return None

    cloud_top = max(sa, sb)
    cloud_bot = min(sa, sb)

    d = None
    # 🆕 شروط مبسّطة (3 من 4 كافية)
    # v7.3.0 — stricter Ichimoku (3/4 instead of 2/4)
    bull_score = sum([price > cloud_top, t_now > k_now, c_now > price_26, sa > sb])
    bear_score = sum([price < cloud_bot, t_now < k_now, c_now < price_26, sa < sb])
    if bull_score >= 3:
        d = "bullish"
    elif bear_score >= 3:
        d = "bearish"
    else:
        return None

    # v9.3.0 — نطاق RSI 25-75 (توازن بين الحماية والمرونة)
    try:
        _rsi_now = float(calc_rsi(df["c"]).iloc[curr])
        if d == "bearish" and _rsi_now < 25:
            log.info(f"🚫 [ichimoku] {short(symbol)} RSI {_rsi_now:.1f} < 25 (oversold) — reject SHORT")
            return None
        if d == "bearish" and _rsi_now > 75:
            log.info(f"🚫 [ichimoku] {short(symbol)} RSI {_rsi_now:.1f} > 75 (overbought) — reject SHORT")
            return None
        if d == "bullish" and _rsi_now > 75:
            log.info(f"🚫 [ichimoku] {short(symbol)} RSI {_rsi_now:.1f} > 75 (overbought) — reject LONG")
            return None
        if d == "bullish" and _rsi_now < 25:
            log.info(f"🚫 [ichimoku] {short(symbol)} RSI {_rsi_now:.1f} < 25 (oversold) — reject LONG")
            return None
    except Exception:
        pass

    # v8.0.99 — فلتر 3 شمعات (بدل 2)
    try:
        _last3 = df.iloc[curr-2:curr+1]
        if d == "bearish":
            _bull_count = sum(
                1 for _, c in _last3.iterrows()
                if c["c"] > c["o"] and abs(c["c"] - c["o"]) / c["o"] * 100 > 0.1
            )
            if _bull_count >= 3:
                log.info(f"🚫 [ichimoku] {short(symbol)} 3 strong bullish candles — reject SHORT")
                return None
        else:  # bullish
            _bear_count = sum(
                1 for _, c in _last3.iterrows()
                if c["c"] < c["o"] and abs(c["c"] - c["o"]) / c["o"] * 100 > 0.1
            )
            if _bear_count >= 3:
                log.info(f"🚫 [ichimoku] {short(symbol)} 3 strong bearish candles — reject LONG")
                return None
    except Exception:
        pass

    # v9.0.2 — RSI extreme (20-80 بدلاً من 25-75)
    try:
        _rsi_f = float(calc_rsi(df["c"]).iloc[curr])
        if _rsi_f < 20:
            log.info(f"🚫 [ichimoku] {short(symbol)} RSI {_rsi_f:.1f} < 20 (extreme oversold) — reject")
            return None
        if _rsi_f > 80:
            log.info(f"🚫 [ichimoku] {short(symbol)} RSI {_rsi_f:.1f} > 80 (extreme overbought) — reject")
            return None
    except Exception:
        pass

    # v9.3.0 — Volume > 1.0× (زخم أقوى)
    try:
        _vma = df["v"].iloc[curr-20:curr].mean()
        _vr = float(df["v"].iloc[curr]) / float(_vma) if _vma > 0 else 0
        if _vr < 1.0:
            log.info(f"🚫 [ichimoku] {short(symbol)} Vol {_vr:.2f}× < 1.0 — reject")
            return None
    except Exception:
        pass

    # v8.0.109 — MACD histogram (هامش 10% تباطؤ مسموح)
    try:
        _, _, _hist = calc_macd(df["c"])
        _h_now = float(_hist.iloc[curr])
        _h_prev = float(_hist.iloc[curr-1])
        _h_prev2 = float(_hist.iloc[curr-2])
        if d == "bearish":
            # شرط 1: القيمة سالبة
            # شرط 2: القيمة الحالية أعمق من الشمعة قبل السابقة (الاتجاه العام هابط)
            # شرط 3: إما أعمق من السابقة، أو تباطؤ ≤ 10%
            _h_tolerance = abs(_h_prev) * 0.1
            _is_ok = (
                _h_now < 0
                and _h_now < _h_prev2
                and (_h_now < _h_prev or abs(_h_now - _h_prev) <= _h_tolerance)
            )
            if not _is_ok:
                log.info(f"🚫 [ichimoku] {short(symbol)} MACD hist not bearish "
                         f"(now={_h_now:.4f}, prev={_h_prev:.4f}, prev2={_h_prev2:.4f}, "
                         f"tol={_h_tolerance:.4f}) — reject")
                return None
        else:
            # LONG: نفس المنطق بالعكس
            _h_tolerance = abs(_h_prev) * 0.1
            _is_ok = (
                _h_now > 0
                and _h_now > _h_prev2
                and (_h_now > _h_prev or abs(_h_now - _h_prev) <= _h_tolerance)
            )
            if not _is_ok:
                log.info(f"🚫 [ichimoku] {short(symbol)} MACD hist not bullish "
                         f"(now={_h_now:.4f}, prev={_h_prev:.4f}, prev2={_h_prev2:.4f}) — reject")
                return None
    except Exception:
        pass

    # v8.0.99 — Chikou Span (صارم)
    try:
        _low_26 = float(df["l"].iloc[curr-26:curr].min())
        _high_26 = float(df["h"].iloc[curr-26:curr].max())
        _close_now = float(df["c"].iloc[curr])
        if d == "bearish" and _close_now >= _low_26:
            log.info(f"🚫 [ichimoku] {short(symbol)} Chikou strict: close>=low_26 — reject SHORT")
            return None
        if d == "bullish" and _close_now <= _high_26:
            log.info(f"🚫 [ichimoku] {short(symbol)} Chikou strict: close<=high_26 — reject LONG")
            return None
    except Exception:
        pass

    # v8.0.99 — POC (Volume Profile)
    try:
        _poc = calc_poc(df)
        if _poc is not None:
            _close_now = float(df["c"].iloc[curr])
            _poc_dist = abs(_close_now - _poc) / _close_now * 100
            if _poc_dist < 0.1:
                log.info(f"🚫 [ichimoku] {short(symbol)} at POC {_poc:.5f} (dist {_poc_dist:.3f}%) — reject")
                return None
    except Exception:
        pass

    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _ichimoku_cache[key] = _time.time()
    cross = await _make_cross(symbol, tf, df, d, "confirmed",
                              "☁️ Ichimoku", curr, source="ichimoku")
    cross["ichi_info"] = {"tenkan": t_now, "kijun": k_now,
                          "cloud_top": cloud_top, "cloud_bot": cloud_bot}
    return cross


async def detect_sr_breakout(symbol, tf):
    if not ENABLE_SR_BREAKOUT: return None
    # v8.0.96 — extra=100 (ADX دقيق)
    df = await _df_for(symbol, tf, extra=100)
    if df is None or len(df) < SR_BREAKOUT_LOOKBACK + 5: return None
    curr = -1  # 🆕 v8.0.49 — الشمعة الحالية (أسرع)
    key = (symbol, tf, "sr")
    last = _sr_cache.get(key)
    if last and (_time.time() - last) < SR_BREAKOUT_COOLDOWN_MIN * 60: return None
    window = df.iloc[curr - SR_BREAKOUT_LOOKBACK:curr]
    high = float(window["h"].max()); low = float(window["l"].min())
    close_prev = float(window["c"].iloc[-1])
    pivot = (high + low + close_prev) / 3
    r1 = 2*pivot - low; s1 = 2*pivot - high
    price_now = float(df["c"].iloc[curr]); price_prev = float(df["c"].iloc[curr - 1])
    d = None; level = None
    # v7.4.0 — add pivot breakout as additional condition
    if price_now > r1 and price_prev <= r1: d, level = "bullish", r1
    elif price_now < s1 and price_prev >= s1: d, level = "bearish", s1
    elif price_now > pivot and price_prev <= pivot: d, level = "bullish", pivot
    elif price_now < pivot and price_prev >= pivot: d, level = "bearish", pivot
    if not d: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _sr_cache[key] = _time.time()
    _filter_stats["signals_sr"] += 1
    cross = await _make_cross(symbol, tf, df, d, "live", "🎯 كسر S/R", curr,
                              source="sr_breakout")
    if cross: cross["_needs_confirm"] = True
    cross["sr_info"] = {"level": level, "r1": r1, "s1": s1, "pivot": pivot}
    return cross


# ═══════════════════════════════════════════════════════════
# 13) التنبيه المفاجئ
# ═══════════════════════════════════════════════════════════
async def detect_sudden_change(symbol):
    data, _ = await _exchange.fetch_ohlcv(symbol, "1m", 3)
    if not data or len(data) < 2: return None
    prev = float(data[-2][4]); cur = float(data[-1][4])
    if prev == 0: return None
    pct = ((cur - prev) / prev) * 100
    st = _price_state.setdefault(symbol, {"direction": None, "count": 0, "alerting": False})
    if abs(pct) >= DEFAULT_THRESHOLD:
        d = "up" if pct > 0 else "down"
        if st["direction"] != d:
            st["direction"] = d; st["count"] = 0; st["alerting"] = False
        if not st["alerting"]:
            st["alerting"] = True; st["count"] = 1
        elif st["count"] < MAX_PRICE_ALERTS: st["count"] += 1
        else: return None
        return {"symbol": symbol, "direction": d, "change_pct": round(pct, 2),
                "current_price": round(cur, 8), "prev_price": round(prev, 8),
                "threshold": DEFAULT_THRESHOLD, "alert_count": st["count"],
                "max_alerts": MAX_PRICE_ALERTS}
    else:
        st["direction"] = None; st["count"] = 0; st["alerting"] = False
    return None


# ═══════════════════════════════════════════════════════════
# 14) تخزين النطاقات
# ═══════════════════════════════════════════════════════════
def _store_range(symbol, lower, upper, source="manual"):
    _last_range[symbol] = {
        "lower": float(lower), "upper": float(upper),
        "computed_at": _time.time(), "state": "inside",
        "last_alert_at": 0.0, "last_approach_alert_at": 0.0,
        "last_near_side": None, "source": source,
    }
    log.info(f"📌 Range [{source}]: {symbol} [{lower} – {upper}]")


def _load_manual_ranges():
    n = 0
    for sym, r in _MANUAL_RANGES.items():
        _store_range(sym, r["lower"], r["upper"], source="manual")
        n += 1
    return n


# ═══════════════════════════════════════════════════════════
# 15) نطاق 6%
# ═══════════════════════════════════════════════════════════
async def _get_current_price(symbol):
    data, _ = await _exchange.fetch_ohlcv(symbol, "1m", 2)
    if not data: return 0.0
    return float(data[-1][4])


async def compute_6pct_ranges():
    global _last_6pct_report
    results = []; half = RANGE_PCT / 200.0
    for symbol in ALL_SYMBOLS:
        try:
            price = await _get_current_price(symbol)
            if price <= 0: continue
            lower = price * (1 - half); upper = price * (1 + half)
            r = {"symbol": symbol, "price": price, "lower": lower, "upper": upper,
                 "width": upper - lower, "pct": RANGE_PCT, "ts": _time.time()}
            results.append(r); _last_6pct_report[symbol] = r
            _filter_stats["range_6pct_reports"] += 1
        except Exception as e: log.exception(f"compute_6pct {symbol}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    return results


def build_6pct_report(results, title="📊 نطاقات 6% المرجعية"):
    if not results: return f"{title}\n📭 no بيانات."
    header = (
        f"{title}\n🇸🇾 {syria_now_str()}\n"
        f"📐 النسبة: <b>{RANGE_PCT}%</b>\n"
        f"🔌 المصدر: <b>{_exchange.primary_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n")
    body = ""; env_block = "\n<b>📋 Environment:</b>\n<code>"
    for r in results:
        body += (f"\n🔹 <b>{short(r['symbol'])}</b>\n"
                 f"   💰 السعر: <b>{fmt_price(r['price'])}</b>\n"
                 f"   📉 الأدنى: <b>{fmt_price(r['lower'])}</b>\n"
                 f"   📈 الأعلى: <b>{fmt_price(r['upper'])}</b>\n")
        sym_up = short(r["symbol"])
        env_block += f"{sym_up}_LOWER={r['lower']:.6f}\n{sym_up}_UPPER={r['upper']:.6f}\n"
    env_block += "</code>\n"
    return header + body + env_block


# ═══════════════════════════════════════════════════════════
# 15b) تقرير الصباح
# ═══════════════════════════════════════════════════════════
async def collect_sr_data(symbol):
    try:
        data = await fetch_ohlcv_cached(symbol, MORNING_REPORT_TF, MORNING_REPORT_LIMIT)
        if not data or len(data) < 60: return None
        df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
        price = float(df["c"].iloc[-2])
        return {"symbol": symbol, "price": price,
                "pivots": calc_pivots_classic(df),
                "highs": find_swing_levels(df)["highs"],
                "lows": find_swing_levels(df)["lows"],
                "tf": MORNING_REPORT_TF}
    except Exception as e:
        log.exception(f"collect_sr_data {symbol}: {e}"); return None


def build_morning_report(items, title=None):
    if not title: title = "🌅 تقرير الصباح"
    if not items: return f"{title}\n🇸🇾 {syria_now_str()}\n📭 no بيانات."
    out = [f"{title}", f"🇸🇾 <b>{syria_now_str()}</b>",
           f"📊 الإطار: <b>{MORNING_REPORT_TF}</b>", ""]
    for it in items:
        if not it: continue
        sym = short(it["symbol"]); price = it["price"]; p = it["pivots"]
        block = [f"━━━━━━━━━━━━━━━━━━━", f"🔹 <b>{sym}</b>",
                 f"💰 السعر: <b>{fmt_price(price)}</b>"]
        if p:
            block += ["", "📊 <b>Pivot Points:</b>",
                     f"   <b>R3</b>: {fmt_price(p['r3'])}",
                     f"   <b>R2</b>: {fmt_price(p['r2'])}",
                     f"   <b>R1</b>: {fmt_price(p['r1'])}",
                     f"   ⚪ <b>Pivot</b>: {fmt_price(p['pivot'])}",
                     f"   <b>S1</b>: {fmt_price(p['s1'])}",
                     f"   <b>S2</b>: {fmt_price(p['s2'])}",
                     f"   <b>S3</b>: {fmt_price(p['s3'])}"]
        if it["highs"]:
            block += ["", "📈 <b>القمم السابقة:</b>"]
            for h, ts in it["highs"]:
                block.append(f"   • {fmt_price(h)} — <i>{syria_from_ts(ts)}</i>")
        if it["lows"]:
            block += ["", "📉 <b>القيعان السابقة:</b>"]
            for l, ts in it["lows"]:
                block.append(f"   • {fmt_price(l)} — <i>{syria_from_ts(ts)}</i>")
        out.append("\n".join(block))
    out.append("━━━━━━━━━━━━━━━━━━━")
    return "\n".join(out)


# ═══════════════════════════════════════════════════════════
# 16) مراقبة النطاق
# ═══════════════════════════════════════════════════════════
async def check_range_breakout(symbol):
    if not RANGE_BREAKOUT_ENABLED: return None
    r = _last_range.get(symbol)
    if not r: return None
    price = await _get_current_price(symbol)
    if price <= 0: return None
    now = _time.time(); cooldown = RANGE_BREAKOUT_COOLDOWN_MIN * 60
    width = r["upper"] - r["lower"]
    if r["lower"] <= price <= r["upper"]:
        r["state"] = "inside"; return None
    if price > r["upper"]:
        if r["state"] == "above" or now - r["last_alert_at"] < cooldown: return None
        r["state"] = "above"; r["last_alert_at"] = now
        return {"symbol": symbol, "direction": "above", "price": price,
                "boundary": r["upper"], "lower": r["lower"], "upper": r["upper"],
                "width": width, "deviation_pct": ((price - r["upper"]) / width) * 100,
                "age_min": int((now - r["computed_at"]) / 60),
                "source": r.get("source", "manual")}
    if price < r["lower"]:
        if r["state"] == "below" or now - r["last_alert_at"] < cooldown: return None
        r["state"] = "below"; r["last_alert_at"] = now
        return {"symbol": symbol, "direction": "below", "price": price,
                "boundary": r["lower"], "lower": r["lower"], "upper": r["upper"],
                "width": width, "deviation_pct": ((r["lower"] - price) / width) * 100,
                "age_min": int((now - r["computed_at"]) / 60),
                "source": r.get("source", "manual")}
    return None


async def check_range_approach(symbol):
    if not RANGE_APPROACH_ENABLED: return None
    r = _last_range.get(symbol)
    if not r or r["state"] != "inside": return None
    price = await _get_current_price(symbol)
    if price <= 0: return None
    width = r["upper"] - r["lower"]
    if width <= 0: return None
    position_pct = ((price - r["lower"]) / width) * 100
    dist_up = 100 - position_pct; dist_down = position_pct
    if dist_up <= RANGE_APPROACH_PCT: side, dist = "upper", dist_up
    elif dist_down <= RANGE_APPROACH_PCT: side, dist = "lower", dist_down
    else: r["last_near_side"] = None; return None
    now = _time.time(); cooldown = RANGE_APPROACH_COOLDOWN_MIN * 60
    if r["last_near_side"] == side and now - r["last_approach_alert_at"] < cooldown:
        return None
    r["last_near_side"] = side; r["last_approach_alert_at"] = now
    mid = (r["lower"] + r["upper"]) / 2; shift = price - mid
    shift_pct = (shift / mid) * 100 if mid else 0
    width_ratio = width / price if price > 0 else 0
    if width_ratio > 0.40:
        new_width = price * 0.08
        sug_lower = price - new_width / 2; sug_upper = price + new_width / 2
        stype, snote = "rebuild", f"⚠️ العرض ضخم ({width_ratio*100:.1f}%)"
    else:
        sug_lower = price - width / 2; sug_upper = price + width / 2
        stype, snote = "shift", "إزاحة النطاق"
    return {"symbol": symbol, "side": side, "price": price,
            "lower": r["lower"], "upper": r["upper"], "width": width,
            "position_pct": round(position_pct, 1), "distance_pct": round(dist, 1),
            "suggested_lower": sug_lower, "suggested_upper": sug_upper,
            "shift": shift, "shift_pct": shift_pct,
            "age_min": int((now - r["computed_at"]) / 60),
            "source": r.get("source", "manual"),
            "suggestion_type": stype, "suggestion_note": snote,
            "width_ratio": round(width_ratio * 100, 1)}


def build_breakout_msg(b):
    up = b["direction"] == "above"
    return (f"{'🚨' if up else '🔻'} <b>خروج عن النطاق — {short(b['symbol'])}</b>\n"
            f"🇸🇾 <b>{syria_now_str()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{'⬆️' if up else '⬇️'} السعر: <b>{fmt_price(b['price'])}</b>\n"
            f"🎯 الحد المخترق: <b>{fmt_price(b['boundary'])}</b>\n"
            f"📏 المسافة: <b>{b['deviation_pct']:.2f}%</b>\n"
            f"• النطاق: {fmt_price(b['lower'])} – {fmt_price(b['upper'])}\n"
            f"• عمر: {b['age_min']} د")


def build_approach_msg(a):
    up = a["side"] == "upper"
    return (f"{'⚠️' if up else '🔔'} <b>اقتراب — {short(a['symbol'])}</b>\n"
            f"🇸🇾 <b>{syria_now_str()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{'⬆️' if up else '⬇️'} السعر: <b>{fmt_price(a['price'])}</b>\n"
            f"📏 المسافة: <b>{a['distance_pct']}%</b> | الموضع: {a['position_pct']}%\n"
            f"📍 النطاق: {fmt_price(a['lower'])} – {fmt_price(a['upper'])}\n"
            f"💡 الاقتراح: <b>{fmt_price(a['suggested_lower'])} – {fmt_price(a['suggested_upper'])}</b>")


# ═══════════════════════════════════════════════════════════
# 17) رسائل الإشارات
# ═══════════════════════════════════════════════════════════
def _source_label(src):
    return {
        "ema": "📈 تقاطع EMA",
        "rsi_div": "🔄 دايفرجنس RSI",
        "bb_squeeze": "💥 BB Squeeze",
        "sr_breakout": "🎯 كسر S/R",
        "vwap_breakout": "📊 كسر VWAP",
        "ichimoku": "☁️ Ichimoku",
        "manual_sync": "🔧 يدوي",
        "pre": "🔔 تقارب مبكر",
        "live": "⚡ مبدئي",
    }.get(src, f"❓{src}" if src else "—")


def build_signal_message(cross):
    sym = cross["symbol"]; tf = cross["timeframe"]
    bull = cross["direction"] == "bullish"
    at = cross.get("alert_type", "confirmed")
    s = cross.get("support", {})
    score = cross.get("final_score", s.get("score", 0))
    grade = s.get("grade", "?")
    market = cross.get("market", "?")
    levels = cross.get("levels")
    source = cross.get("source", "ema")
    mtf = cross.get("mtf", "—")

    # 🆕 Paper Mode
    paper = cross.get("paper_signal", False)
    if paper:
        emoji = "📄"
        title = f"إشارة ضعيفة (Paper) — {short(sym)}"
        type_lbl = "📄 <b>Paper — no تنفيذ</b>"
        dir_lbl = "🟢 محتمل صاعد" if bull else "🔴 محتمل هابط"
    elif at == "pre":
        emoji, title = "🔔", "تقارب وشيك"
        type_lbl = "🔔 <b>تحذير مبكر</b>"
        dir_lbl = "🟢 محتمل صاعد" if bull else "🔴 محتمل هابط"
    elif at == "live":
        emoji, title = "⚡", "تقاطع مبدئي"
        type_lbl = "⚡ <b>مبدئي</b>"
        dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"
    else:
        emoji, title = ("🚀" if bull else "🔻"), "إشارة مؤكدة"
        type_lbl = "✅ <b>مؤكد</b>"
        dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"
    market_lbl = {"TREND_UP": "📈 ترند صاعد", "TREND_DOWN": "📉 ترند هابط",
                  "RANGE": "↔️ عرضي", "VOLATILE": "🌊 متقلب",
                  "UNKNOWN": "❓"}.get(market, market)
    msg = (f"{emoji} <b>{title} — {short(sym)} [{tf}]</b>\n"
           f"🇸🇾 <b>{syria_now_str()}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"{type_lbl} | {_source_label(source)}\n"
           f"📊 <b>{dir_lbl}</b>\n"
           f"🏛️ السوق: <b>{market_lbl}</b>\n"
           f"🔗 MTF: <b>{mtf}</b>\n")
    if source == "rsi_div" and cross.get("div_info"):
        di = cross["div_info"]
        msg += (f"🔄 الدايفرجنس: RSI <b>{di['r1']}</b> → <b>{di['r2']}</b>\n")
    elif source == "bb_squeeze" and cross.get("bb_info"):
        bi = cross["bb_info"]; msg += f"💥 عرض BB: <b>{bi['width']}%</b>\n"
    elif source == "sr_breakout" and cross.get("sr_info"):
        si = cross["sr_info"]; msg += f"🎯 المستوى المكسور: <b>{fmt_price(si['level'])}</b>\n"
    elif source == "range_reversal":
        _rh = cross.get("range_high", 0)
        _rl = cross.get("range_low", 0)
        _rm = cross.get("range_mid", 0)
        msg += f"↔️ النطاق: <b>{fmt_price(_rl)} — {fmt_price(_rh)}</b>\n"
        msg += f"🎯 الهدف (المنتصف): <b>{fmt_price(_rm)}</b>\n"
    elif source == "macd_rsi" and cross.get("macd_info"):
        _mi = cross["macd_info"]
        msg += f"📊 MACD: DIF={_mi['dif']} | DEA={_mi['dea']} | RSI={_mi['rsi']}\n"
    elif source == "live_momentum" and cross.get("momentum_info"):
        _mo = cross["momentum_info"]
        msg += f"⚡ الزخم: {_mo['change_pct']:+.2f}% | الحجم: {_mo['vol_ratio']}x\n"
    msg += (f"📏 فرق EMA: <b>{cross['gap_pct']:.3f}%</b>\n"
            f"🎯 <b>العلامة: {score}/100 {grade}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"• الحجم: {s.get('vol_ratio','?')}×\n"
            f"• ADX: {s.get('adx','?')}\n"
            f"• RSI: {s.get('rsi','?')}\n"
            f"• ATR: {s.get('atr_pct','?')}%\n")
    if levels:
        msg += (f"━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 <b>خطة التنفيذ:</b>\n"
                f"• الدخول: <b>{fmt_price(levels['entry'])}</b>\n"
                f"• وقف الخسارة: <b>{fmt_price(levels['stop_loss'])}</b> (-{levels['sl_pct']}%)\n"
                f"• هدف 1 (1:1): {fmt_price(levels['tp1'])}\n"
                f"• هدف 2 (1:2): {fmt_price(levels['tp2'])}\n")
    msg += (f"━━━━━━━━━━━━━━━━━━━\n"
            f"• السعر: {fmt_price(cross['price'])}\n"
            f"• المصدر: {cross.get('exchange','?')} ({MARKET_TYPE})")
    return msg


def build_price_alert_msg(a):
    up = a["direction"] == "up"
    return (f"{'🚀' if up else '🔻'} <b>تغير مفاجئ — {short(a['symbol'])}</b>\n"
            f"🇸🇾 <b>{syria_now_str()}</b>\n"
            f"📈 التغير: <b>{a['change_pct']}%</b>\n"
            f"🔔 {a['alert_count']} من {a['max_alerts']}\n"
            f"• الحالي: {fmt_price(a['current_price'])}\n"
            f"• السابق: {fmt_price(a['prev_price'])}")


def build_ranges_report():
    if not _last_range: return "📭 no نطاقات."
    lines = ["<b>📊 النطاقات المُراقَبة:</b>\n"]
    now = _time.time()
    for sym, r in _last_range.items():
        age = int((now - r["computed_at"]) / 60)
        icon = {"inside": "🟢", "above": "🔺", "below": "🔻"}.get(r["state"], "⚪")
        lines.append(f"{icon} <b>{short(sym)}</b>\n"
                     f"   {fmt_price(r['lower'])} | {fmt_price(r['upper'])}\n"
                     f"   عمر: {age} د\n")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# 17b) رسائل المتابعة
# ═══════════════════════════════════════════════════════════
def build_followup_msg(sig, result, price, age_min):
    cross = sig["cross"]
    sym = short(cross["symbol"]); tf = cross["timeframe"]
    bull = sig["direction"] == "bullish"
    entry = sig["levels"]["entry"]
    pnl_pct = ((price - entry) / entry * 100) * (1 if bull else -1)
    if result == "win_tp2": emoji, title, color = "✅", "هدف 2 (1:2)", "🟢"
    elif result == "win_tp1": emoji, title, color = "🎯", "هدف 1 (1:1)", "🟢"
    elif result == "loss_sl": emoji, title, color = "❌", "وقف الخسارة", "🔴"
    elif result == "expired": emoji, title, color = "⏰", "انتهت الصلاحية", "🟡"
    else: return None
    return (f"{emoji} <b>{title}</b>\n"
            f"📌 متابعة: {sym} [{tf}]\n"
            f"🇸🇾 {syria_now_str()}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{color} {'🟢 صاعد' if bull else '🔴 هابط'} | "
            f"{_source_label(cross.get('source','ema'))}\n"
            f"💰 الدخول: <b>{fmt_price(entry)}</b>\n"
            f"💵 الحالي: <b>{fmt_price(price)}</b>\n"
            f"📊 P&L: <b>{pnl_pct:+.2f}%</b>\n"
            f"⏱️ المدة: <b>{int(age_min)} دقيقة</b>\n")


async def send_followup(bot, sig, result, price, age_min):
    if not FOLLOWUP_ENABLED: return
    if result == "win_tp1" and not FOLLOWUP_ON_TP1: return
    if result == "win_tp2" and not FOLLOWUP_ON_TP2: return
    if result == "loss_sl" and not FOLLOWUP_ON_SL: return
    if result == "expired" and not FOLLOWUP_ON_EXPIRE: return
    now = _time.time()
    if now - sig.get("last_followup_ts", 0) < FOLLOWUP_THROTTLE_SEC: return
    sig["last_followup_ts"] = now
    text = build_followup_msg(sig, result, price, age_min)
    if not text: return
    # 🆕 v7.9.26 — tracking only, no Telegram
    if not FOLLOWUP_TELEGRAM_ENABLED:
        _filter_stats["followups_sent"] += 1
        log.debug(f"followup suppressed: {result} {sig['cross']['symbol']}")
        return
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        _filter_stats["followups_sent"] += 1
    except Exception as e:
        log.error(f"send_followup: {e}")


# ═══════════════════════════════════════════════════════════
# 18) تتبّع الإشارات (Paper)
# ═══════════════════════════════════════════════════════════
def register_signal_for_tracking(cross):
    levels = cross.get("levels")
    if not levels: return
    sid = f"{cross['symbol']}_{cross['timeframe']}_{int(_time.time())}"
    _pending_signals[sid] = {
        "cross": cross, "levels": levels,
        "entry_price": levels["entry"], "entry_ts": _time.time(),
        "direction": cross["direction"], "source": cross.get("source", "ema"),
        "checked": False, "result": None,
        "max_favorable": levels["entry"], "max_adverse": levels["entry"],
        "last_followup_ts": 0.0,
    }
    _filter_stats["signals_tracked"] += 1


async def evaluate_pending_signals(bot=None):
    if not _pending_signals: return
    now = _time.time()
    for sid, sig in list(_pending_signals.items()):
        if sig["checked"]: continue
        age_min = (now - sig["entry_ts"]) / 60
        _tf = sig["cross"].get("timeframe", "15m")
        _max_min = SIGNAL_TRACK_MAX_MIN_BY_TF.get(_tf, SIGNAL_TRACK_MAX_MIN)
        if age_min > _max_min:
            sig["checked"] = True; sig["result"] = "expired"
            _smart.record_result("expired", sig["direction"], sig.get("source", "unknown"))
            _record_history(sig, "expired", sig["levels"]["entry"], age_min)
            if bot and CHAT_ID:
                await send_followup(bot, sig, "expired", sig["levels"]["entry"], age_min)
            continue
        try:
            data, _ = await _exchange.fetch_ohlcv(sig["cross"]["symbol"], "1m", 3)
            if not data: continue
            price = float(data[-1][4]); lv = sig["levels"]
            new_result = None
            if sig["direction"] == "bullish":
                if price <= lv["stop_loss"]: sig["checked"] = True; new_result = "loss_sl"
                elif price >= lv["tp2"] and sig.get("result") not in ("win_tp2",):
                    new_result = "win_tp2"; sig["result"] = "win_tp2"
                elif price >= lv["tp1"] and not sig.get("result"):
                    new_result = "win_tp1"; sig["result"] = "win_tp1"
            else:
                if price >= lv["stop_loss"]: sig["checked"] = True; new_result = "loss_sl"
                elif price <= lv["tp2"] and sig.get("result") not in ("win_tp2",):
                    new_result = "win_tp2"; sig["result"] = "win_tp2"
                elif price <= lv["tp1"] and not sig.get("result"):
                    new_result = "win_tp1"; sig["result"] = "win_tp1"
            if new_result:
                if sig["checked"]:
                    _smart.record_result(sig["result"], sig["direction"],
                                         sig.get("source", "unknown"))
                    _record_history(sig, sig["result"], price, age_min)
                if bot and CHAT_ID:
                    await send_followup(bot, sig, new_result, price, age_min)
        except Exception as e:
            log.warning(f"track {sid}: {e}")
    cutoff = now - 3600
    for sid in [k for k, v in _pending_signals.items()
                if v.get("checked") and v.get("entry_ts", 0) < cutoff]:
        _pending_signals.pop(sid, None)


def _record_history(sig, result, price, age_min):
    cross = sig["cross"]; entry = sig["levels"]["entry"]
    bull = sig["direction"] == "bullish"
    pnl = ((price - entry) / entry * 100) * (1 if bull else -1)
    _last_signals.append({
        "symbol": cross["symbol"], "timeframe": cross["timeframe"],
        "direction": cross["direction"], "source": cross.get("source", "ema"),
        "score": cross.get("final_score", 0),
        "entry": entry, "exit": price, "result": result,
        "pnl_pct": round(pnl, 2), "duration_min": int(age_min), "ts": _time.time(),
    })


# ═══════════════════════════════════════════════════════════
# 19) 📈 الرسوم البيانية
# ═══════════════════════════════════════════════════════════
C_BG = "#0d1117"; C_PANEL = "#131722"; C_GRID = "#1f2430"
C_UP = "#26a69a"; C_DOWN = "#ef5350"; C_TEXT = "#d1d4dc"; C_MUTED = "#7a7f8a"
C_PIVOT = "#ffeb3b"; C_R1 = "#ff9800"; C_R2 = "#ff5722"; C_R3 = "#f44336"
C_S1 = "#8bc34a"; C_S2 = "#4caf50"; C_S3 = "#00bcd4"
C_PRICE = "#ffffff"; C_ENTRY = "#00e5ff"


def _chart_generate_sync(symbol, df, sr_data, signal=None, lookback=CHART_LOOKBACK):
    df = df.iloc[-lookback:].reset_index(drop=True)
    if df.empty: return None
    fig = plt.figure(figsize=(14, 7.5), dpi=CHART_DPI, facecolor=C_BG)
    ax = fig.add_axes([0.06, 0.10, 0.76, 0.82]); ax.set_facecolor(C_PANEL)
    n = len(df)
    for i, row in df.iterrows():
        is_up = row["c"] >= row["o"]
        color = C_UP if is_up else C_DOWN
        ax.plot([i, i], [row["l"], row["h"]], color=color, linewidth=0.8,
                zorder=2, solid_capstyle="round")
        low_body = min(row["o"], row["c"])
        body_h = abs(row["c"] - row["o"])
        if body_h <= 0: body_h = (row["h"] - row["l"]) * 0.01 or row["c"] * 0.0001
        ax.add_patch(Rectangle((i - 0.35, low_body), 0.7, body_h,
                                facecolor=color, edgecolor=color, linewidth=0, zorder=3))
    price_now = float(df["c"].iloc[-1])
    ax.axhline(price_now, color=C_PRICE, linestyle="-", linewidth=0.9, alpha=0.55, zorder=1)
    ax.text(n + 0.3, price_now, f"  {price_now:.6f}", color=C_PRICE, fontsize=10,
            va="center", ha="left", fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.25", facecolor=C_PANEL,
                      edgecolor=C_PRICE, linewidth=0.6))
    p = (sr_data or {}).get("pivots")
    if p:
        for name, lvl, col in [("R3", p["r3"], C_R3), ("R2", p["r2"], C_R2),
                                ("R1", p["r1"], C_R1), ("P", p["pivot"], C_PIVOT),
                                ("S1", p["s1"], C_S1), ("S2", p["s2"], C_S2),
                                ("S3", p["s3"], C_S3)]:
            ax.axhline(lvl, color=col, linestyle="--", linewidth=1.0, alpha=0.75, zorder=1)
            ax.text(n + 0.3, lvl, f"  {name}  {lvl:.6f}", color=col, fontsize=9,
                    va="center", ha="left", fontweight="bold")
    for h, _ts in (sr_data or {}).get("highs", []):
        ax.axhline(h, color=C_DOWN, linestyle=":", linewidth=0.9, alpha=0.55, zorder=1)
        ax.scatter([n - 1], [h], marker="v", s=42, color=C_DOWN, zorder=4,
                   edgecolors="white", linewidths=0.5)
    for l, _ts in (sr_data or {}).get("lows", []):
        ax.axhline(l, color=C_UP, linestyle=":", linewidth=0.9, alpha=0.55, zorder=1)
        ax.scatter([n - 1], [l], marker="^", s=42, color=C_UP, zorder=4,
                   edgecolors="white", linewidths=0.5)
    if signal:
        lv = signal.get("levels") or {}
        entry = lv.get("entry"); sl = lv.get("stop_loss")
        tp1 = lv.get("tp1"); tp2 = lv.get("tp2"); tp3 = lv.get("tp3")
        if entry: ax.axhline(entry, color=C_ENTRY, linestyle="-",
                             linewidth=1.4, alpha=0.9, zorder=5)
        if sl: ax.axhline(sl, color=C_DOWN, linestyle="-",
                          linewidth=1.2, alpha=0.85, zorder=5)
        for tp in (tp1, tp2, tp3):
            if tp: ax.axhline(tp, color=C_UP, linestyle="-",
                              linewidth=1.0, alpha=0.7, zorder=5)
    tf = (sr_data or {}).get("tf", "1h")
    ax.set_title(f"  {symbol}   ·   {tf}", color=C_TEXT, fontsize=13,
                 fontweight="bold", loc="left", pad=14)
    ax.tick_params(axis="x", colors=C_MUTED, labelsize=9)
    ax.tick_params(axis="y", colors=C_MUTED, labelsize=9)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.5f"))
    ax.grid(True, color=C_GRID, alpha=0.55, linewidth=0.5)
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    for sp in ("bottom", "left"): ax.spines[sp].set_color(C_GRID)
    ax.set_xlim(-3, n + 18)
    fig.text(0.06, 0.025, f"SY {syria_now_str()}", color=C_MUTED, fontsize=9)
    fig.text(0.94, 0.025, f"{_exchange.primary_name.upper()}  ·  {MARKET_TYPE}",
             color=C_MUTED, fontsize=9, ha="right")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", facecolor=C_BG); plt.close(fig)
    buf.seek(0); return buf


async def generate_sr_chart(symbol, signal=None, tf=None):
    if not CHART_ENABLED: return None
    try:
        tf = tf or MORNING_REPORT_TF
        data = await fetch_ohlcv_cached(symbol, tf, MORNING_REPORT_LIMIT)
        if not data or len(data) < 60: return None
        df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
        sr_data = {"pivots": calc_pivots_classic(df),
                   "highs": find_swing_levels(df)["highs"],
                   "lows": find_swing_levels(df)["lows"], "tf": tf}
        return await asyncio.to_thread(_chart_generate_sync, symbol, df, sr_data, signal)
    except Exception as e:
        log.exception(f"generate_sr_chart {symbol}: {e}"); return None


# ═══════════════════════════════════════════════════════════
# 20) 💰 طبقة تنفيذ Binance Futures
# ═══════════════════════════════════════════════════════════
def _to_binance_symbol(ccxt_symbol: str) -> str:
    return ccxt_symbol.split(":")[0].replace("/", "").upper()


log.info(f"💰 Config: NOTIONAL_USDT={NOTIONAL_USDT}$ | LEVERAGE={LEVERAGE}x | MARGIN={MARGIN_TYPE}")
log.info("🔐 Binance diagnosis:")
for _k, _v in _diagnose_env_keys().items():
    log.info(f"   • {_k}: {_v}")





BINANCE_API_TIMEOUT = _env_int("BINANCE_API_TIMEOUT", 25)  # 🆕 v8.0.5: 15→25
_rate_limit_until = 0.0  # 🆕 v7.9.28 — global rate limit cooldown
RATE_LIMIT_BACKOFF_SEC = 300  # 5 min


async def _binance_call(func, *args, timeout=None, **kwargs):
    """v7.9.28 — cooldown on rate limit"""
    global _rate_limit_until
    _now = _time.time()
    if _now < _rate_limit_until:
        _rem = int(_rate_limit_until - _now)
        raise Exception(f"Rate limit cooldown ({_rem}s)")
    t = timeout or BINANCE_API_TIMEOUT
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(func, *args, **kwargs), timeout=t)
    except asyncio.TimeoutError:
        log.warning(f"Binance timeout: {getattr(func, '__name__', 'call')}")
        raise
    except Exception as e:
        _es = str(e)
        if "-1003" in _es or "Way too many requests" in _es:
            _rate_limit_until = _time.time() + RATE_LIMIT_BACKOFF_SEC
            log.warning(f"⏸️ Rate limit — cooldown {RATE_LIMIT_BACKOFF_SEC}s")
        raise


USE_BINANCE_WS = _env_int("USE_BINANCE_WS", 1) == 1  # 🆕 v7.9.29


class BinanceExecutor:
    def __init__(self):
        self.client = None
        self.symbol_info = {}
        self.ok = False
        self.error_msg = None
        self.ws = None  # 🆕 v7.9.29 — WebSocket manager
        self._init()

    def _init(self):
        if not BINANCE_ENABLED:
            self.error_msg = "BINANCE_ENABLED=0"
            log.info(f"ℹ️ {self.error_msg} — execution disabled")
            return
        if not _BINANCE_LIB_OK:
            self.error_msg = "python-binance غير مثبت"
            log.error(f"❌ {self.error_msg} — pip install python-binance")
            return
        if not BINANCE_API_KEY:
            self.error_msg = "BINANCE_API_KEY فارغة أو مفقودة"
            log.error(f"❌ {self.error_msg}")
            return
        if not BINANCE_API_SECRET:
            self.error_msg = "BINANCE_API_SECRET فارغة أو مفقودة"
            log.error(f"❌ {self.error_msg}")
            return
        try:
            self.client = BinanceClient(
                BINANCE_API_KEY, BINANCE_API_SECRET,
                testnet=BINANCE_TESTNET,
                requests_params={"timeout": 30},
            )
            self.client.futures_ping()
            acc = self.client.futures_account()
            bal = 0.0
            for a in acc.get("assets", []):
                if a.get("asset") == "USDT":
                    bal = float(a.get("availableBalance", 0) or 0); break
            net = "TESTNET" if BINANCE_TESTNET else "MAINNET"
            log.info(f"✅ Binance {net} | USDT balance: {bal:.2f}")
            self._load_exchange_info()
            self.ok = True
            self.error_msg = None
            # 🆕 v7.9.29 — Start WebSocket (non-blocking; falls back to REST if fails)
            if USE_BINANCE_WS and _WS_AVAILABLE:
                try:
                    _syms = list(self.symbol_info.keys())[:50]
                    self.ws = BinanceWSManager(
                        BINANCE_API_KEY, BINANCE_API_SECRET,
                        testnet=BINANCE_TESTNET)
                    self.ws.start(_syms)
                    log.info(f"✅ WS: {len(_syms)} symbols (prices + user data)")
                except Exception as _we:
                    log.warning(f"⚠️ WS failed, REST only: {_we}")
                    self.ws = None
        except BinanceAPIException as e:
            self.error_msg = f"Binance API {e.code}: {e.message}"
            log.error(f"❌ {self.error_msg}")
            if e.code == -2015:
                log.error("   ⚠️ Wrong key / missing permissions / IP not allowed")
            self.client = None; self.ok = False
        except Exception as e:
            self.error_msg = f"{type(e).__name__}: {e}"
            log.error(f"❌ Binance init: {self.error_msg}")
            self.client = None; self.ok = False

    def _load_exchange_info(self):
        try:
            info = self.client.futures_exchange_info()
            for s in info.get("symbols", []):
                sym = s["symbol"]
                filters = {f["filterType"]: f for f in s.get("filters", [])}
                step = float(filters.get("LOT_SIZE", {}).get("stepSize", "0.001"))
                min_qty = float(filters.get("LOT_SIZE", {}).get("minQty", "0"))
                tick = float(filters.get("PRICE_FILTER", {}).get("tickSize", "0.0001"))
                min_notional = float(filters.get("MIN_NOTIONAL", {}).get("notional", "5"))
                self.symbol_info[sym] = {
                    "qty_precision": s.get("quantityPrecision", 3),
                    "price_precision": s.get("pricePrecision", 4),
                    "step_size": step, "min_qty": min_qty,
                    "tick_size": tick, "min_notional": min_notional,
                }
            log.info(f"📦 Binance: {len(self.symbol_info)} symbols")
        except Exception as e:
            log.error(f"load_exchange_info: {e}")

    def info(self, bsym): return self.symbol_info.get(bsym)

    def round_qty(self, qty, step):
        if step <= 0: return qty
        return math.floor(qty / step) * step

    def fmt_qty(self, qty, precision):
        s = f"{qty:.{precision}f}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"

    def _prepare_symbol(self, bsym):
        try:
            self.client.futures_change_leverage(symbol=bsym, leverage=LEVERAGE)
        except BinanceAPIException as e:
            log.warning(f"set_leverage {bsym}: {e.message}")
        except Exception as e:
            log.warning(f"set_leverage {bsym}: {e}")
        try:
            self.client.futures_change_margin_type(symbol=bsym, marginType=MARGIN_TYPE)
        except BinanceAPIException as e:
            if e.code != -4046:
                log.warning(f"set_margin {bsym}: {e.message}")
        except Exception as e:
            log.warning(f"set_margin {bsym}: {e}")

    def price_to_precision(self, bsym, price):
        """تقريب السعر حسب tick_size"""
        info = self.info(bsym)
        if not info: return str(price)
        tick = info.get("tick_size", 0.0001)
        precision = info.get("price_precision", 4)
        rounded = round(math.floor(price / tick) * tick, precision)
        return f"{rounded:.{precision}f}".rstrip("0").rstrip(".") or "0"

    def place_stop_loss(self, bsym, side, sl_price, qty_str=None):
        """v7.9.1 — Algo Order API (conditional SL) with legacy fallback"""
        sl_side = "SELL" if side == "BUY" else "BUY"
        sl_str = self.price_to_precision(bsym, sl_price)
        params = {
            "symbol": bsym, "side": sl_side, "type": "STOP_MARKET",
            "stopPrice": sl_str, "workingType": "MARK_PRICE",
        }
        if qty_str:
            params["quantity"] = qty_str
            params["reduceOnly"] = True
        else:
            params["closePosition"] = True
        # 🆕 v7.9.28 — legacy only (Testnet algo_order fails -1102)
        params["newClientOrderId"] = f"bot_sl_{bsym}_{int(_time.time()*1000)%1000000}"
        order = self.client.futures_create_order(**params)
        oid = (order.get("algoId") or order.get("orderId")
               or order.get("clientAlgoId") or order.get("clientOrderId") or "placed")
        log.info(f"SL @ {sl_str} qty={qty_str or 'ALL'} oid={oid}")
        return oid

    def place_take_profit(self, bsym, side, tp_price, qty_str, label="tp1"):
        """v7.9.1 — Algo Order API with legacy fallback"""
        tp_side = "SELL" if side == "BUY" else "BUY"
        tp_str = self.price_to_precision(bsym, tp_price)
        params = {
            "symbol": bsym, "side": tp_side, "type": "TAKE_PROFIT_MARKET",
            "stopPrice": tp_str, "quantity": qty_str, "reduceOnly": True,
            "workingType": "MARK_PRICE",
        }
        # 🆕 v7.9.28 — legacy only
        params["newClientOrderId"] = f"bot_{label}_{bsym}_{int(_time.time()*1000)%1000000}"
        order = self.client.futures_create_order(**params)
        oid = (order.get("algoId") or order.get("orderId")
               or order.get("clientAlgoId") or order.get("clientOrderId") or "placed")
        log.info(f"✅ TP on Binance @ {tp_str} qty={qty_str} (oid={oid})")
        return oid

    def open_market(self, bsym, side, qty):
        order = self.client.futures_create_order(
            symbol=bsym, side=side, type="MARKET", quantity=qty)
        try:
            detail = self.client.futures_get_order(symbol=bsym,
                                                   orderId=order["orderId"])
        except Exception:
            detail = order
        avg_price = float(detail.get("avgPrice") or 0)
        executed = float(detail.get("executedQty") or 0)
        orig = float(detail.get("origQty") or qty)
        # 🆕 Partial Fill Protection
        if executed <= 0:
            raise Exception(f"فشل التنفيذ: executedQty=0 (orig={orig})")
        if executed < orig * 0.99:
            log.warning(f"⚠️ Partial Fill {bsym}: {executed}/{orig} "
                        f"({executed/orig*100:.1f}%)")
        if avg_price == 0:
            t = self.client.futures_symbol_ticker(symbol=bsym)
            avg_price = float(t.get("price", 0))
        return {"order_id": detail.get("orderId"), "avg_price": avg_price,
                "executed_qty": executed, "orig_qty": orig, "raw": detail}

    def close_market(self, bsym, side, qty):
        order = self.client.futures_create_order(
            symbol=bsym, side=side, type="MARKET",
            quantity=qty, reduceOnly=True)
        try:
            detail = self.client.futures_get_order(symbol=bsym,
                                                   orderId=order["orderId"])
        except Exception:
            detail = order
        avg_price = float(detail.get("avgPrice") or 0)
        if avg_price == 0:
            t = self.client.futures_symbol_ticker(symbol=bsym)
            avg_price = float(t.get("price", 0))
        return {"order_id": detail.get("orderId"), "avg_price": avg_price, "raw": detail}

    def get_open_positions(self):
        # 🆕 v7.9.29 — WS first
        if self.ws and self.ws.is_healthy():
            try:
                out = self.ws.get_positions()
                return out
            except Exception as _we:
                log.debug(f"WS get_positions: {_we}")
        # REST fallback
        try:
            positions = self.client.futures_position_information()
            out = []
            for p in positions:
                amt = float(p.get("positionAmt", 0) or 0)
                if abs(amt) < 1e-9: continue
                out.append({"symbol": p["symbol"], "amount": amt,
                            "entry_price": float(p.get("entryPrice", 0) or 0),
                            "mark_price": float(p.get("markPrice", 0) or 0),
                            "unrealized_pnl": float(p.get("unRealizedProfit", 0) or 0),
                            "leverage": p.get("leverage")})
            return out
        except Exception as e:
            log.error(f"get_open_positions: {e}")
            raise  # 🆕 v7.9.25 — let caller decide (don't treat as empty)

    def usdt_balance(self):
        try:
            acc = self.client.futures_account()
            for a in acc.get("assets", []):
                if a.get("asset") == "USDT":
                    return {"available": float(a.get("availableBalance", 0) or 0),
                            "wallet": float(a.get("walletBalance", 0) or 0),
                            "unrealized": float(a.get("unrealizedProfit", 0) or 0)}
            return None
        except Exception as e:
            log.error(f"usdt_balance: {e}"); return None

    def last_price(self, bsym):
        # 🆕 v7.9.29 — WS first
        if self.ws and self.ws.is_healthy():
            _p = self.ws.get_price(bsym)
            if _p and _p > 0:
                return _p
        try:
            t = self.client.futures_symbol_ticker(symbol=bsym)
            return float(t["price"])
        except Exception as e:
            log.debug(f"last_price {bsym}: {e}"); return 0.0

    def get_klines(self, bsym, interval="1m", limit=2):
        """🆕 v6.9.1 — قراءة الشموع مع timeout + retry"""
        import time as _t
        for attempt in range(2):
            try:
                return self.client.futures_klines(
                    symbol=bsym, interval=interval, limit=limit)
            except Exception as e:
                log.warning(f"klines {bsym} attempt {attempt+1}: {e}")
                if attempt < 1:
                    _t.sleep(0.5)
        return None



_binance = BinanceExecutor()


# ═══════════════════════════════════════════════════════════
# 20b) 💰 تتبع الtrades
# ═══════════════════════════════════════════════════════════
TRADE_TICKETS = {}
_symbol_ticket_index = defaultdict(list)
# 🩹 FIX #12: keep threading.Lock (sync-only critical sections)
# ⚠️ NEVER add `await` inside any `with _ticket_lock:` or `with _paper_lock:` block
_ticket_lock = threading.Lock()


def is_paused():
    """🆕 v6.8.8 — يبقى بعد restart"""
    try:
        return os.path.exists(PAUSE_FILE)
    except Exception:
        return False



async def _safe_close_market(bsym, side, qty):
    """v7.0.0 safe close with -2022 handling"""
    try:
        pos = await _binance_call(_binance.get_open_positions)
    except Exception:
        pos = []
    actual = next((p for p in pos if p["symbol"] == bsym), None)
    if not actual:
        for _t in _tickets_for(bsym):
            _remove_ticket(_t["id"])
        raise Exception(f"no position on {bsym}")
    real_qty = abs(float(actual["amount"]))
    if real_qty <= 0:
        raise Exception(f"qty=0 on {bsym}")
    safe_qty = min(float(qty), real_qty)
    info = _binance.info(bsym)
    if info:
        safe_qty = _binance.round_qty(safe_qty, info["step_size"])
        qty_s = _binance.fmt_qty(safe_qty, info["qty_precision"])
    else:
        qty_s = f"{safe_qty}"
    try:
        # 🩹 FIX: was recursive self-call (RecursionError)
        return await asyncio.to_thread(_binance.close_market, bsym, side, qty_s)
    except BinanceAPIException as e:
        if e.code == -2022:
            log.warning(f"-2022 on {bsym}: cleaning tickets")
            for _t in _tickets_for(bsym):
                _remove_ticket(_t["id"])
        raise


def _mk_ticket_id(bsym, side):
    return f"{bsym}_{side}_{int(_time.time()*1000)}"


STATE_FILE = "/root/bot2/state_tickets.json"
RISK_STATE_FILE = "/root/bot2/risk_state.json"  # 🩹 FIX #11
PAUSE_FILE = "/root/bot2/pause.flag"
PAPER_FILE = "/root/bot2/paper_signals.json"  # legacy
PAPER_DB   = "/root/bot2/paper_signals.db"

# 🆕 v6.8.8 — منع loop
_closed_binance_ids = deque(maxlen=500)
_RECENT_BOT_TRADES = {}  # 🩹 FIX v7.9.3
_global_ticket_lock = asyncio.Lock()  # v7.0.0
_paper_signals = {}
_paper_lock = threading.Lock()


def _save_state():
    """🆕 {__version_full__} — حفظ TRADE_TICKETS في ملف."""
    try:
        with _ticket_lock:
            # 🩹 FIX #14: strip _df (DataFrame) before saving
            tickets_copy = {}
            for tid, t in TRADE_TICKETS.items():
                tc = dict(t)
                sig = tc.get("signal")
                if sig and isinstance(sig, dict) and "_df" in sig:
                    sig = dict(sig)
                    sig.pop("_df", None)
                    tc["signal"] = sig
                tickets_copy[tid] = tc
            data = {"tickets": tickets_copy, "ts": _time.time()}
        import json
        with io.open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, default=str)
    except Exception as e:
        log.warning(f"save_state: {e}")


def _cleanup_dead_tickets():
    """🆕 v6.7.4: احذف tickets التي no يوجد لها مركز فعلي.
    🩹 FIX v7.9.2: آمن — لا يحذف عند فشل API."""
    try:
        positions = _binance.get_open_positions()
        if positions is None:
            log.warning("🧹 cleanup: get_open_positions returned None — skip")
            return
        # إذا رجعت [] — افحص مرة ثانية للتأكد
        if len(positions) == 0:
            import time as _t
            _t.sleep(1.5)
            positions2 = _binance.get_open_positions()
            if positions2 is None or len(positions2) == 0:
                with _ticket_lock:
                    count = len(TRADE_TICKETS)
                    if count > 0:
                        log.warning(f"🧹 Cleanup {count} tickets — no actual positions (confirmed)")
                        TRADE_TICKETS.clear()
                        _symbol_ticket_index.clear()
                _save_state()
                return
            else:
                log.warning(f"🧹 cleanup: first=[], second={len(positions2)} — skip")
                positions = positions2
        active_symbols = {p["symbol"] for p in positions}
        with _ticket_lock:
            to_remove = [tid for tid, t in TRADE_TICKETS.items()
                         if t.get("binance_symbol") not in active_symbols]
            for tid in to_remove:
                t = TRADE_TICKETS.pop(tid, None)
                if t:
                    lst = _symbol_ticket_index.get(t["binance_symbol"], [])
                    if tid in lst: lst.remove(tid)
            if to_remove:
                log.warning(f"🧹 Deleted {len(to_remove)} missing tickets")
        _save_state()
    except Exception as e:
        log.warning(f"cleanup_dead: {e}")


def _load_state():
    """🆕 {__version_full__} — استعادة TRADE_TICKETS من ملف."""
    try:
        import json
        if not os.path.exists(STATE_FILE):
            return 0
        with io.open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        tickets = data.get("tickets", {})
        # 🆕 v6.7.4: الاحتفاظ بكل tickets
        cutoff = _time.time() - (30 * 86400)  # 30 يوم
        restored = 0
        with _ticket_lock:
            for tid, t in tickets.items():
                if t.get("opened_at", 0) < cutoff:
                    continue
                TRADE_TICKETS[tid] = t
                _symbol_ticket_index[t["binance_symbol"]].append(tid)
                restored += 1
        return restored
    except Exception as e:
        log.warning(f"load_state: {e}")
        return 0




async def _recover_tickets_after_restart():
    """v7.0.0 recovery: refresh max/min from 1m klines after restart."""
    if not _binance.ok:
        return
    restored = 0
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    for t in tickets:
        if not t.get("tp1_hit"):
            continue
        if t.get("tp2_moved"):
            try:
                cur = await _binance_call(_binance.last_price, t["binance_symbol"])
                target = t["tp2_current"]
                hit = ((t["side"] == "SHORT" and cur <= target) or
                       (t["side"] == "LONG"  and cur >= target))
                if hit:
                    log.info(f"recovery: {t['binance_symbol']} hit TP2 during downtime")
                    await _close_remaining_tp2(t, bot=None, reason="TP2 (recovery)")
            except Exception as e:
                log.warning(f"recovery tp2_moved: {e}")
            continue
        try:
            since_ts = int(t.get("tp1_hit_ts", 0) * 1000)
            if since_ts <= 0:
                continue
            klines = await _binance_call(_binance.get_klines, t["binance_symbol"], "1m", 60)
            if not klines:
                continue
            max_p = t.get("max_price_after_tp1") or 0
            min_p = t.get("min_price_after_tp1") or 1e18
            for k in klines:
                if int(k[0]) < since_ts:
                    continue
                h = float(k[2]); l = float(k[3])
                if h > max_p: max_p = h
                if l < min_p: min_p = l
            t["max_price_after_tp1"] = max_p if max_p > 0 else None
            t["min_price_after_tp1"] = min_p if min_p < 1e18 else None
            zedge = t.get("zone_edge")
            if zedge:
                buf = 1 + TP2_ZONE_EDGE_BUFFER_PCT / 100
                if t["side"] == "LONG" and max_p >= zedge * buf:
                    t["serious_breakout_flag"] = True
                elif t["side"] == "SHORT" and min_p <= zedge / buf:
                    t["serious_breakout_flag"] = True
            restored += 1
        except Exception as e:
            log.warning(f"recovery: {e}")
    if restored:
        _save_state()
        log.info(f"recovery: updated {restored} tickets")


def _add_ticket(t):
    # 🩹 FIX #13: single lock acquisition (was TOCTOU)
    with _ticket_lock:
        # منع تكرار الtrades اليدوية
        if t.get("source") == "manual_sync":
            existing = [x for x in TRADE_TICKETS.values()
                        if x.get("binance_symbol") == t.get("binance_symbol")
                        and x.get("side") == t.get("side")
                        and x.get("source") == "manual_sync"]
            if existing:
                total_qty = sum(x["qty"] for x in existing)
                if abs(total_qty - t["qty"]) < 1e-6:
                    log.info(f"⏭️ Rejected duplicate manual ticket: {t['binance_symbol']} {t['side']}")
                    return False
                if abs(total_qty - t["qty"]) / t["qty"] < 0.02:
                    log.info(f"⏭️ Minor duplicate, rejected: {t['binance_symbol']}")
                    return False
        # 🩹 FIX v7.9.3: if bot ticket, remove any "_M" dupes for same bsym+side
        if not t.get("manual"):
            _bsym = t.get("binance_symbol")
            _side = t.get("side")
            _to_del = []
            for _tid in list(TRADE_TICKETS.keys()):
                _old = TRADE_TICKETS[_tid]
                if (_old.get("binance_symbol") == _bsym and
                    _old.get("side") == _side and
                    _old.get("manual")):
                    _to_del.append(_tid)
            for _tid in _to_del:
                log.info(f"🗑️ Removing dup manual ticket: {_tid}")
                del TRADE_TICKETS[_tid]
                try:
                    _symbol_ticket_index[_bsym].remove(_tid)
                except ValueError:
                    pass
        TRADE_TICKETS[t["id"]] = t
        _symbol_ticket_index[t["binance_symbol"]].append(t["id"])
    _save_state()
    return True


def _remove_ticket(tid):
    with _ticket_lock:
        t = TRADE_TICKETS.pop(tid, None)
        if not t: return
        # 🩹 FIX v7.9.5: cascade remove linked _M
        if not t.get("manual"):
            _bsym = t.get("binance_symbol")
            _side = t.get("side")
            orphans = [k for k, v in TRADE_TICKETS.items()
                       if v.get("manual") and
                       v.get("binance_symbol") == _bsym and
                       v.get("side") == _side]
            for _k in orphans:
                TRADE_TICKETS.pop(_k, None)
                lst2 = _symbol_ticket_index.get(_bsym, [])
                if _k in lst2: lst2.remove(_k)
                log.info(f"🗑️ Cascade remove _M: {_k}")
        lst = _symbol_ticket_index.get(t["binance_symbol"], [])
        if tid in lst: lst.remove(tid)
    _save_state()


def _tickets_for(bsym):
    with _ticket_lock:
        ids = list(_symbol_ticket_index.get(bsym, []))
        return [TRADE_TICKETS[i] for i in ids if i in TRADE_TICKETS]


def _count_all_tickets():
    with _ticket_lock:
        return len(TRADE_TICKETS)


def _get_actual_position(bsym):
    """يُرجع المركز الفعلي على Binance أو None."""
    try:
        positions = _binance.get_open_positions()
        for p in positions:
            if p["symbol"] == bsym:
                return p
        return None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════
# 20c) 💰 تنفيذ trade
# ═══════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════
# 🛡️ Risk Manager — إدارة المخاطرة
# ═══════════════════════════════════════════════════════════
class RiskManager:
    """يدير:
    - 5 trades متزامنة كحد أقصى
    - 3 trades في نفس الاتجاه كحد أقصى
    - إيقاف عند 3 خسائر متتالية
    - حد خسارة يومي $20
    """
    def __init__(self):
        self.max_concurrent = int(os.getenv("MAX_CONCURRENT_POSITIONS", "10"))  # v8.2.0: 6→10
        self.max_same_dir = int(os.getenv("MAX_SAME_DIRECTION", "4"))  # v8.0.99: 3→4  # v8.0.86: 3→2
        self.max_consec_losses = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "6"))
        self.max_daily_loss = float(os.getenv("MAX_DAILY_LOSS_USD", "20"))
        self.consecutive_losses = 0
        self.daily_pnl_usd = 0.0
        self.daily_reset_ts = _time.time()
        self.paused = False
        self.paused_reason = ""
        # 🩹 FIX #11: load persisted state
        self._load_daily()
        log.info(f"🛡️ RiskManager: max={self.max_concurrent} | same_dir={self.max_same_dir} | consec={self.max_consec_losses} | daily=${self.max_daily_loss} | pnl=${self.daily_pnl_usd:+.2f}")

    def _load_daily(self):
        """🩹 FIX #11: restore daily PnL from disk."""
        try:
            import json as _j
            if not os.path.exists(RISK_STATE_FILE):
                return
            with io.open(RISK_STATE_FILE, "r", encoding="utf-8") as f:
                data = _j.load(f)
            if _time.time() - data.get("reset_ts", 0) < 86400:
                self.daily_pnl_usd = float(data.get("pnl", 0.0))
                self.daily_reset_ts = float(data.get("reset_ts", _time.time()))
                self.consecutive_losses = int(data.get("consec", 0))
                log.info(f"🔄 Restored RiskManager: pnl=${self.daily_pnl_usd:+.2f} consec={self.consecutive_losses}")
        except Exception as e:
            log.warning(f"RiskManager._load_daily: {e}")

    def _save_daily(self):
        """🩹 FIX #11: persist daily PnL to disk."""
        try:
            import json as _j
            data = {
                "pnl": self.daily_pnl_usd,
                "reset_ts": self.daily_reset_ts,
                "consec": self.consecutive_losses,
                "saved_at": _time.time(),
            }
            with io.open(RISK_STATE_FILE, "w", encoding="utf-8") as f:
                _j.dump(data, f)
        except Exception as e:
            log.warning(f"RiskManager._save_daily: {e}")

    def _reset_daily(self):
        if _time.time() - self.daily_reset_ts > 86400:
            self.daily_pnl_usd = 0.0
            self.daily_reset_ts = _time.time()
            log.info("🔄 Daily loss reset")
            self._save_daily()

    def can_open(self, all_positions, new_side):
        """فحص شامل قبل فتح trade جديدة"""
        self._reset_daily()

        if self.paused:
            return {"ok": False, "error": f"🛑 البوت موقوف: {self.paused_reason}"}

        # حد الخسارة اليومي
        if self.daily_pnl_usd <= -self.max_daily_loss:
            self.paused = True
            self.paused_reason = f"خسارة يومية ${abs(self.daily_pnl_usd):.2f}"
            return {"ok": False, "error": f"🛑 {self.paused_reason} (استخدم /resume)"}

        # خسائر متتالية
        if self.consecutive_losses >= self.max_consec_losses:
            self.paused = True
            self.paused_reason = f"{self.consecutive_losses} خسائر متتالية"
            return {"ok": False, "error": f"🛑 {self.paused_reason} (استخدم /resume)"}

        # v8.0.86 — حماية ديناميكية: قد يكون الهامش أقل من الحد الثابت
        _margin_per_trade = NOTIONAL_USDT / max(LEVERAGE, 1)  # 75/10 = 7.5$
        try:
            if _binance.ok:
                _bal_dyn = _binance.usdt_balance()
                if _bal_dyn and _bal_dyn.get("available", 0) > 0:
                    _max_by_margin = int(_bal_dyn["available"] * 0.8 / _margin_per_trade)
                    _effective_max = min(self.max_concurrent, max(1, _max_by_margin))
                    if len(all_positions) >= _effective_max:
                        return {"ok": False,
                                "error": f"⚠️ هامش غير كافٍ ({len(all_positions)}/{_effective_max}) — متاح ${_bal_dyn['available']:.2f}"}
        except Exception as _de:
            log.debug(f"dynamic margin check: {_de}")
        # trades متزامنة (ثابت)
        if len(all_positions) >= self.max_concurrent:
            return {"ok": False, "error": f"⚠️ وصلت للحد الأقصى ({self.max_concurrent})"}

        # 🛡️ حماية الهامش
        try:
            total_notional = sum(
                abs(float(p.get("amount", 0))) * float(p.get("mark_price", 0))
                for p in all_positions
            )
            if total_notional > 0 and _binance.ok:
                bal = _binance.usdt_balance()
                if bal and bal.get("wallet", 0) > 0:
                    usage_pct = (total_notional / (bal["wallet"] * LEVERAGE)) * 100
                    if usage_pct >= MAX_MARGIN_USAGE_PCT:
                        return {"ok": False,
                                "error": f"⚠️ هامش مستخدم {usage_pct:.1f}% ≥ {MAX_MARGIN_USAGE_PCT}%"}
        except Exception as _he:
            log.debug(f"margin check: {_he}")

        # 🆕 v6.8.8 — احتساب من amount (يدوي + بوت)
        same_dir = 0
        for p in all_positions:
            try:
                amt = float(p.get("amount", 0))
                if amt > 0 and new_side == "LONG": same_dir += 1
                elif amt < 0 and new_side == "SHORT": same_dir += 1
            except Exception:
                continue
        if same_dir >= self.max_same_dir:
            return {"ok": False,
                    "error": f"⚠️ {same_dir}/{self.max_same_dir} trades {new_side} (يدوي+بوت)"}

        return {"ok": True, "error": ""}

    def record_result(self, pnl_usd, result):
        """تسجيل نتيجة trade"""
        self.daily_pnl_usd += pnl_usd
        if result == "loss":
            self.consecutive_losses += 1
            log.warning(f"📉 loss: consec={self.consecutive_losses} | daily=${self.daily_pnl_usd:.2f}")
        elif result == "win":
            self.consecutive_losses = 0
            log.info(f"📈 profit: daily=${self.daily_pnl_usd:.2f}")
        self._save_daily()  # 🩹 FIX #11

    def resume(self):
        """استئناف بعد التوقف"""
        self.paused = False
        self.paused_reason = ""
        self.consecutive_losses = 0
        self._save_daily()  # 🩹 FIX #11
        log.info("▶️ Risk resumed")

    def status(self):
        return {
            "consec_losses": self.consecutive_losses,
            "daily_pnl": self.daily_pnl_usd,
            "paused": self.paused,
            "reason": self.paused_reason,
        }


_risk_mgr = RiskManager()
_last_pause_notified = 0.0  # 🆕 v7.9.24 — last pause notify timestamp
PAUSE_NOTIFY_COOLDOWN = 3600  # 1 hour between pause notifications
_last_ratelimit_notified = 0.0  # 🆕 v7.9.25 — last rate limit notify
RATELIMIT_NOTIFY_COOLDOWN = 1800  # 30 min between rate limit notifications


async def execute_binance_trade(cross, bot=None):
    global PAPER_ONLY_MODE
    if PAPER_ONLY_MODE:
        return {"ok": False, "error": "paper_only: كل الصفقات إلى Paper"}
    if is_paused():
        return {"ok": False, "error": "⏸️ البوت موقوف — يُسجل في الورقي فقط"}
    if _time.time() < _rate_limit_until:
        _rem = int(_rate_limit_until - _time.time())
        return {"ok": False, "error": f"Rate limit cooldown ({_rem}s)"}
    if not _binance.ok:
        return {"ok": False, "error": _binance.error_msg or "Binance not connected"}
    # 🆕 v8.0.30 — السماح لـ live بالتنفيذ (بعد تأكيد 3 شمعات)
    if cross.get("alert_type") not in ("confirmed", "live"):
        return {"ok": False, "error": "Unconfirmed signal"}
    levels = cross.get("levels")
    if not levels:
        return {"ok": False, "error": "no مستويات SL/TP"}

    bsym = _to_binance_symbol(cross["symbol"])
    
    # 🆕 v8.9.5 — استبعاد BTC من التنفيذ (Paper فقط + إشعار)
    if bsym == "BTCUSDT" and not cross.get("_btc_force_exec"):
        cross["paper_signal"] = True
        cross["paper_reason"] = "btc_paper_only: مؤشر فقط"
        cross["paper_type"] = "operational"
        if bot and CHAT_ID:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"📄 <b>BTC — Paper فقط (مؤشر السوق)</b>\n"
                          f"🇸🇾 {syria_now_str()}\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"📌 {cross['symbol']} [{cross['timeframe']}] "
                          f"{'🟢' if cross['direction']=='bullish' else '🔴'} "
                          f"{cross['direction']}\n"
                          f"🎯 Score: {cross.get('final_score', 0)}\n"
                          f"📊 السعر: {fmt_price(cross['price'])}\n"
                          f"⚡ لن يتم التنفيذ (BTC = مؤشر فقط)"),
                    parse_mode="HTML")
            except Exception: pass
        return {"ok": False, "error": "btc_paper_only (مؤشر السوق)"}
    
    info = _binance.info(bsym)
    if not info:
        return {"ok": False, "error": f"{bsym} غير مدعوم"}

    side_long = cross["direction"] == "bullish"
    new_side = "LONG" if side_long else "SHORT"
    score = cross.get("final_score", 0)

    # 🩹 FIX v7.9.4: reserve BEFORE opening (async race)
    _RECENT_BOT_TRADES[bsym] = _time.time() + 3600  # future = "in progress"
    log.debug(f"🔒 Reserved {bsym} for bot opening")

    # 🛡️ Risk Management Check
    try:
        _all_positions = _binance.get_open_positions()
        if _all_positions is None:
            _all_positions = []
    except Exception as _e:
        log.warning(f"⚠️ get_open_positions failed: {_e}")
        _all_positions = []

    # 🆕 v8.0.3 — حماية: حد أقصى 2 صفقات حية من Micro-Live
    _live_count = sum(1 for t in TRADE_TICKETS.values() if t.get("source") in ("vwap_breakout", "ema"))
    if cross.get("source") in ("vwap_breakout", "ema") and _live_count >= 2:
        log.warning(f"⛔ Micro-Live limit reached ({_live_count}/2) for {cross['source']}")
        return {"ok": False, "error": "Micro-Live limit (2 trades max)"}
    
    # v8.0.99 — 1m confirm (3 شمعات، 2/3)
    try:
        if cross.get("source") == "ichimoku":
            _ok_1m, _cnt_1m = await check_live_confirmation(
                cross["symbol"], cross["direction"], n=3, min_agree=2)
            if not _ok_1m:
                log.info(f"🚫 1m confirm: {short(cross['symbol'])} {_cnt_1m}/3 — reject")
                return {"ok": False, "error": f"1m confirm {_cnt_1m}/3"}
            log.info(f"✅ 1m confirm: {short(cross['symbol'])} {_cnt_1m}/3")
    except Exception as _e1m:
        log.debug(f"1m confirm: {_e1m}")

    _risk_check = _risk_mgr.can_open(_all_positions, new_side)
    if not _risk_check.get("ok"):
        log.warning(f"⛔ Risk block: {_risk_check.get('error')}")
        # 🆕 v7.9.24 — notify user (throttled)
        global _last_pause_notified
        _now_pause = _time.time()
        if _risk_mgr.paused and (_now_pause - _last_pause_notified) > PAUSE_NOTIFY_COOLDOWN:
            _last_pause_notified = _now_pause
            if bot and CHAT_ID:
                try:
                    _st = _risk_mgr.status()
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(f"🛑 <b>البوت موقوف تلقائياً</b>\n"
                              f"🇸🇾 {syria_now_str()}\n"
                              f"━━━━━━━━━━━━━━━━━━━\n"
                              f"⚠️ <b>السبب:</b> {_st['reason']}\n"
                              f"━━━━━━━━━━━━━━━━━━━\n"
                              f"📉 الخسائر المتتالية: <b>{_st['consec_losses']}/{_risk_mgr.max_consec_losses}</b>\n"
                              f"💰 PnL اليوم: <b>${_st['daily_pnl']:+.2f}</b> / -${_risk_mgr.max_daily_loss}\n"
                              f"━━━━━━━━━━━━━━━━━━━\n"
                              f"⚡ <i>للاستئناف:</i>\n"
                              f"<code>/resume_risk</code>"),
                        parse_mode="HTML")
                except Exception as _ne:
                    log.warning(f"pause notify: {_ne}")
        return _risk_check

    # 🆕 v7.9.30 — 1 trade per symbol (any direction)
    existing = _tickets_for(bsym)
    if existing:
        sides = set(t["side"] for t in existing)
        return {"ok": False,
                "error": f"صفقة مفتوحة على {short(cross['symbol'])} ({'/'.join(sides)})"}

    # 🆕 v9.21.0 — الدخول بسعر السوق للإشارات السريعة (Live Momentum)
    _cur_price = _binance.last_price(bsym)
    if cross.get("source") == "live_momentum" and _cur_price > 0:
        _plan_price = float(levels["entry"])
        _diff_pct = abs(_cur_price - _plan_price) / _plan_price * 100
        if _diff_pct < 0.2:
            log.info(f"⚡ live_momentum: Market entry (diff={_diff_pct:.3f}%)")
            levels["entry"] = _cur_price
        else:
            log.info(f"⏳ live_momentum: Limit entry (diff={_diff_pct:.3f}%)")
            cross["_use_limit"] = _plan_price

    # 🆕 v8.0.9 — الدخول الهجين لـ sr_breakout
    if cross.get("source") == "sr_breakout" and _cur_price > 0:
        _plan_price = float(levels["entry"])
        if side_long:
            # LONG: السعر الأفضل هو الأقل
            if _cur_price <= _plan_price:
                log.info(f"⚡ sr_breakout: Market entry (cur={_cur_price:.6f} <= plan={_plan_price:.6f})")
                levels["entry"] = _cur_price  # استخدام سعر السوق
            else:
                log.info(f"⏳ sr_breakout: Limit entry (cur={_cur_price:.6f} > plan={_plan_price:.6f})")
                # نضع أمر Limit (سيتم لاحقاً في التنفيذ)
                cross["_use_limit"] = _plan_price
        else:
            # SHORT: السعر الأفضل هو الأعلى
            if _cur_price >= _plan_price:
                log.info(f"⚡ sr_breakout: Market entry (cur={_cur_price:.6f} >= plan={_plan_price:.6f})")
                levels["entry"] = _cur_price
            else:
                log.info(f"⏳ sr_breakout: Limit entry (cur={_cur_price:.6f} < plan={_plan_price:.6f})")
                cross["_use_limit"] = _plan_price

    price_for_qty = float(levels["entry"])
    sl_for_size = float(levels["stop_loss"])

    # 🆕 v9.30.0 — إعدادات Supertrend المثالية (TP=2.0%, SL=0.5%)
    _source = cross.get("source", "")
    if _source == "supertrend":
        _tp_pct = 2.0
        _sl_pct = 0.5
        if side_long:
            levels["tp1"] = price_for_qty * (1 + _tp_pct / 100)
            levels["stop_loss"] = price_for_qty * (1 - _sl_pct / 100)
        else:
            levels["tp1"] = price_for_qty * (1 - _tp_pct / 100)
            levels["stop_loss"] = price_for_qty * (1 + _sl_pct / 100)
        log.info(f"🔒 [v9.30.0] Supertrend: TP={levels['tp1']:.2f}, SL={levels['stop_loss']:.2f} (TP=2%, SL=0.5%)")
    else:
        _range_mid = cross.get("range_mid")
        if _range_mid and _source == "range_reversal":
            _forced_tp = _range_mid
            levels["tp1"] = _forced_tp
            log.info(f"🔒 [v9.11.0] TP = range_mid: {_forced_tp:.6f}")
        else:
            _sl_pct = abs(price_for_qty - sl_for_size) / price_for_qty * 100 if price_for_qty > 0 else 0
            _forced_tp_pct = max(0.35, min(_sl_pct * 2, 2.5))
            if side_long:
                _forced_tp = price_for_qty * (1 + _forced_tp_pct / 100)
            else:
                _forced_tp = price_for_qty * (1 - _forced_tp_pct / 100)
            levels["tp1"] = _forced_tp
            log.info(f"🔒 [v9.11.0] TP = 2×SL: {_forced_tp:.6f} ({_forced_tp_pct:.2f}%)")

    # 🆕 v8.6.0 — إجبار SL من Smart Zones (إذا لم يكن موجوداً)
    if not sl_for_size or sl_for_size <= 0:
        try:
            _sz = await sz_compute_levels(cross["symbol"], cross["direction"], cross["timeframe"])
            if _sz and _sz.get("stop_loss"):
                sl_for_size = float(_sz["stop_loss"])
                levels["stop_loss"] = sl_for_size
                log.info(f"✅ [v8.6.0] SL from Smart Zones: {sl_for_size:.6f}")
        except Exception as e:
            log.warning(f"⚠️ [v8.6.0] SL fallback: {e}")

    # v8.8.0 — Slippage handling
    _cur_mkt_price = _binance.last_price(bsym)
    if _cur_mkt_price > 0:
        if side_long:
            _expected_slip = (_cur_mkt_price - price_for_qty) / price_for_qty * 100
        else:
            _expected_slip = (price_for_qty - _cur_mkt_price) / price_for_qty * 100

        if _expected_slip > MAX_SLIPPAGE_PCT:
            log.warning("[v8.8.0] Slippage against position: " + str(round(_expected_slip, 3)) + " pct - doubling TP")
            _new_entry = _cur_mkt_price
            if side_long:
                _new_tp = _new_entry * (1 + 0.0035 * 2.0)
                _new_sl = _new_entry * (1 - abs(price_for_qty - sl_for_size) / price_for_qty)
            else:
                _new_tp = _new_entry * (1 - 0.0035 * 2.0)
                _new_sl = _new_entry * (1 + abs(price_for_qty - sl_for_size) / price_for_qty)
            levels["entry"] = _new_entry
            levels["tp1"] = _new_tp
            levels["stop_loss"] = _new_sl
            price_for_qty = _new_entry
            sl_for_size = _new_sl
        else:
            log.info("[v8.8.0] Slippage OK: " + str(round(_expected_slip, 3)))
    # 🆕 v6.8.0: حساب الحجم على أساس المخاطرة 1-2%
    _bal = None
    try:
        _bal = _binance.usdt_balance()
    except Exception:
        pass
    # 🆕 v8.0.39 — استخدام NOTIONAL_USDT مباشرة (بدون risk-based)
    qty_raw = NOTIONAL_USDT / price_for_qty
    log.info(f"💡 Fixed notional: {qty_raw:.6f} ({NOTIONAL_USDT}$)")
    qty = _binance.round_qty(qty_raw, info["step_size"])
    if qty < info["min_qty"]:
        return {"ok": False, "error": f"qty {qty} < minQty {info['min_qty']}"}
    notional_actual = qty * price_for_qty
    if notional_actual < info["min_notional"]:
        return {"ok": False,
                "error": f"notional {notional_actual:.2f} < {info['min_notional']}"}
    qty_s = _binance.fmt_qty(qty, info["qty_precision"])

    try:
        await _binance_call(_binance._prepare_symbol, bsym)
    except Exception as e:
        log.warning(f"prepare {bsym}: {e}")

    order_side = "BUY" if side_long else "SELL"
    try:
        result = await _binance_call(_binance.open_market, bsym, order_side, qty_s)
    except BinanceAPIException as e:
        _filter_stats["trades_failed"] += 1
        err = f"Binance {e.code}: {e.message}"
        log.error(f"❌ open {bsym}: {err}")
        if bot and CHAT_ID:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"⚠️ <b>فشل تنفيذ {short(cross['symbol'])}</b>\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"❌ {err}\n"
                          f"💰 notional: {NOTIONAL_USDT}$ | ⚙️ {LEVERAGE}x"),
                    parse_mode="HTML")
            except Exception: pass
        return {"ok": False, "error": err}
    except Exception as e:
        _filter_stats["trades_failed"] += 1
        log.exception(f"open {bsym}: {e}")
        return {"ok": False, "error": str(e)}

    fill_price = result["avg_price"] or price_for_qty
    filled_qty = result["executed_qty"] or float(qty_s)

    # ═══════════════════════════════════════════════
    # 🆕 v7.9.35 — Slippage check + SL/TP recalculation
    # ═══════════════════════════════════════════════
    _signal_price = float(levels["entry"])
    # 🆕 v8.0.6 — حساب الانزلاق الاتجاهي (LONG/SHORT)
    if side_long:
        # LONG: ملء أعلى من المتوقع = خسارة
        _slippage_pct = (fill_price - _signal_price) / _signal_price * 100
    else:
        # SHORT: ملء أقل من المتوقع = خسارة
        _slippage_pct = (_signal_price - fill_price) / _signal_price * 100
    
    log.info(f"📊 Slippage: signal={_signal_price:.6f} → fill={fill_price:.6f} = {_slippage_pct:+.3f}%")
    
    if _slippage_pct > MAX_SLIPPAGE_PCT:
        log.warning(f"⚠️ Slippage {_slippage_pct:.3f}% > {MAX_SLIPPAGE_PCT}% — استمرار بدون Break-even")
        # 🆕 v8.1.5 — إلغاء رسالة Break-even الوهمية
        if bot and CHAT_ID:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"⚠️ <b>تنبيه انزلاق — الصفقة مستمرة</b>\n"
                          f"📌 {short(cross['symbol'])} [{cross['timeframe']}]\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"📊 السعر المتوقع: {fmt_price(_signal_price)}\n"
                          f"💵 السعر الفعلي: {fmt_price(fill_price)}\n"
                          f"📉 الانزلاق: <b>{_slippage_pct:.3f}%</b>\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"🛑 SL: {fmt_price(levels.get('stop_loss', 0))}\n"
                          f"🎯 TP: {fmt_price(levels.get('tp1', 0))}\n"
                          f"⚡ الصفقة تكمل مسارها الطبيعي"),
                    parse_mode="HTML")
            except Exception: pass
    
    # Recalculate SL/TP from actual fill_price
    _orig_sl = float(levels["stop_loss"])
    _orig_tp1 = float(levels["tp1"])
    _orig_tp2 = float(levels.get("tp2", 0))
    _orig_risk = abs(_signal_price - _orig_sl)
    
    # v8.0.89b — نُعدّل SL فقط
    _new_sl = fill_price - _orig_risk if side_long else fill_price + _orig_risk
    levels["stop_loss"] = _new_sl
    
    # 🆕 v8.9.4 — إعادة حساب TP1 من سعر الملء الفعلي (لضمان 0.35%)
    if side_long:
        _new_tp1 = fill_price * (1 + 0.0035)
    else:
        _new_tp1 = fill_price * (1 - 0.0035)
    levels["tp1"] = _new_tp1
    log.info(f"🔄 TP1 on fill_price={fill_price:.6f}: {_orig_tp1:.6f} → {_new_tp1:.6f} (0.35%)")
    log.info(f"🔄 SL on fill_price={fill_price:.6f}: {_orig_sl:.6f} → {_new_sl:.6f}")

    # v8.0.89a — TP1 محسوب مسبقاً (RR=1.0) — لا نُعيد فرض 0.6%
    
    # 🆕 v6.7.0 — تحقق من المركز الفعلي بعد الفتح
    await asyncio.sleep(1.5)
    try:
        actual_after = _get_actual_position(bsym)
        if actual_after:
            real_total = abs(actual_after["amount"])
            tracked = sum(t["qty"] for t in _tickets_for(bsym))
            # إذا هناك qty إضافي لم نتتبعه
            unaccounted = real_total - tracked - filled_qty
            if unaccounted > filled_qty * 0.02:
                log.warning(f"⚠️ Qty diff {bsym}: "
                            f"tracked={tracked} + filled={filled_qty} "
                            f"< real={real_total}")
                # أضف الفرق for ticket الحالي
                filled_qty += unaccounted
                log.info(f"🔧 corrected qty to {filled_qty}")
    except Exception as e:
        log.warning(f"post-open verify: {e}")

    tid = _mk_ticket_id(bsym, new_side)

    # ═══ 🛡️ الحزمة A: SL/TP على Binance ═══
    _sl_status = "none"
    _tp_status = "none"
    _tp1_placed = False
    _tp2_placed = False

    # 🆕 v8.1.4 — فلتر RR ملغى (كان يسبب إغلاق مركز بعد فتحه)
    # يُحسب فقط لأغراض التسجيل
    try:
        _tp1_chk = float(levels.get("tp1", 0))
        _sl_chk = float(levels.get("stop_loss", 0))
        if _tp1_chk > 0 and _sl_chk > 0:
            _rr_ratio = abs(_tp1_chk - fill_price) / abs(fill_price - _sl_chk) if abs(fill_price - _sl_chk) > 0 else 0
            log.info(f"📊 RR = {_rr_ratio:.2f} (v8.1.4 — لا يُوقف التنفيذ)")
    except Exception as _rr_e:
        log.debug(f"RR log: {_rr_e}")

    # 🆕 v9.1.0 — وضع التتبع: لا SL، مراقبة عبر _trailing_monitor
    if _trailing_mode["enabled"]:
        if _trailing_mode["active_ticket"]:
            log.info(f"[Trailing] يوجد صفقة نشطة بالفعل — رفض {bsym}")
            return {"ok": False, "error": "Trailing: صفقة نشطة بالفعل"}
        
        # نضع SL احتياطي (Hard Stop) لكن نترك المراقبة اللحظية
        _hard_sl = fill_price * (1 - 0.025) if side_long else fill_price * (1 + 0.025)
        try:
            _sl_qty_s = _binance.fmt_qty(_binance.round_qty(filled_qty, info["step_size"]), info["qty_precision"])
            await _binance_call(_binance.place_stop_loss, bsym, order_side, _hard_sl, _sl_qty_s)
            _sl_status = "binance"
            log.info(f"[Trailing] Hard SL @ {_hard_sl} (2.5%)")
        except Exception as e:
            log.warning(f"[Trailing] SL failed: {e}")
        
        # نضع TP بعيد جداً (اختياري، لن يُنفذ لأنه سيُغلق قبل ذلك)
        _tp_dist = 0.05  # 5%
        _new_tp = fill_price * (1 + _tp_dist) if side_long else fill_price * (1 - _tp_dist)
        
        tid = _mk_ticket_id(bsym, new_side)
        ticket = {
            "id": tid, "symbol": cross["symbol"], "binance_symbol": bsym,
            "side": new_side, "qty": filled_qty, "entry": fill_price,
            "sl": _hard_sl, "tp": _new_tp,
            "sl_status": _sl_status, "tp_status": "none",
            "half_sl_mode": False, "needs_manual_monitor": False,
            "sl_pct": 2.5, "score": score, "source": cross.get("source", "ema"),
            "tf": cross["timeframe"], "market": cross.get("market"),
            "opened_at": _time.time(), "manual": False,
            "order_id": result["order_id"],
            "notional": filled_qty * fill_price,
            "leverage": LEVERAGE, "signal": cross,
            "trailing_mode": True,
        }
        _add_ticket(ticket)
        _trailing_mode["active_ticket"] = tid
        _trailing_mode["highest_profit"] = 0.0
        _save_trailing_state()
        
        if bot and CHAT_ID:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(f"🎯 <b>صفقة تتبع (Trailing) — {short(cross['symbol'])}</b>\n"
                      f"🇸🇾 {syria_now_str()}\n"
                      f"━━━━━━━━━━━━━━━━━━━\n"
                      f"{'🟢 LONG' if side_long else '🔴 SHORT'}\n"
                      f"💵 الدخول: {fmt_price(fill_price)}\n"
                      f"🛑 Hard Stop: {fmt_price(_hard_sl)} (2.5%)\n"
                      f"🎯 الربح: مفتوح (حتى انعكاس BTC)\n"
                      f"⚡ المراقبة: كل 30 ثانية"),
                parse_mode="HTML")
        
        return {"ok": True, "ticket": ticket}

    # 1. SL إلزامي (🆕 v8.0.11 — نصف الكمية فقط)
    try:
        _sl_price = float(levels["stop_loss"])
        # v8.0.87r — SL واحد 100% (لا تقسيم، لا SL2)
        _sl_qty_s = _binance.fmt_qty(
            _binance.round_qty(filled_qty, info["step_size"]),
            info["qty_precision"])
        await _binance_call(_binance.place_stop_loss, bsym, order_side, _sl_price, _sl_qty_s)
        _sl_status = "binance"
        log.info(f"✅ SL (100%) @ {_sl_price} qty={_sl_qty_s}")

    except Exception as e:
        log.error(f"❌ SL failed — immediate close: {e}")
        try:
            await _safe_close_market(bsym, "SELL" if order_side == "BUY" else "BUY", _binance.fmt_qty(
                    _binance.round_qty(filled_qty, info["step_size"]),
                    info["qty_precision"]))
            log.warning(f"🆘 Trade closed due to failed SL")
        except Exception as _ex:
            log.error(f"❌ Close failed: {_ex}")
        return {"ok": False, "error": "SL فشل — تم الإغلاق"}

    # 2. TP1 (60%) — MANDATORY (close if fails, like SL)
    try:
        _tp1_qty = filled_qty  # v8.0.87c: TP1 100%
        _tp1_qty_s = _binance.fmt_qty(
            _binance.round_qty(_tp1_qty, info["step_size"]),
            info["qty_precision"])
        _tp1_id = await _binance_call(_binance.place_take_profit, bsym, order_side,
            float(levels["tp1"]), _tp1_qty_s)
        _tp1_placed = bool(_tp1_id)
        if _tp1_placed:
            log.info(f"✅ TP1 (60%) @ {levels['tp1']}")
    except Exception as e:
        log.error(f"❌ TP1 failed: {e}")
    
    # 🆕 v7.9.36 — TP1 fallback: bot-managed if placement fails
    if not _tp1_placed:
        log.warning(f"⚠️ TP1 not placed on Binance — switching to BOT-MANAGED mode")
        _tp_status = "bot_managed"  # ← البوت سيدير TP1 و SL يدوياً
        _tp1_placed = True  # ← نعتبرها "مُدارة" لكي لا يتم رفض الصفقة
        # ملاحظة: سيتم التعامل مع TP1 عبر _manage_smart_tp2 و fast_tp_sl_job
    # ═══════════════════════════════════════════════

    # 3. v8.0.87o — TP2 محذوف نهائياً (لا كتلة if False/else)
    _tp_status = "binance" if _tp1_placed else "bot_managed"
    log.info(f"v8.0.87o: TP1 (100%) only, no TP2")

    _needs_manual = (_tp_status != "binance")

    ticket = {
        "id": tid, "symbol": cross["symbol"], "binance_symbol": bsym,
        "side": new_side, "qty": filled_qty, "entry": fill_price,
        "sl": levels["stop_loss"], "tp": levels["tp1"],
        "tp2": levels.get("tp2"), "tp3": levels.get("tp3"),
        "sl_status": _sl_status, "tp_status": _tp_status,
        "half_sl_mode": True, # 🆕 تفعيل وضع النصف
        "needs_manual_monitor": _needs_manual,
        "sl_pct": levels["sl_pct"],
        "score": score, "source": cross.get("source", "ema"),
        "tf": cross["timeframe"], "market": cross.get("market"),
        "opened_at": _time.time(), "manual": False,
        "order_id": result["order_id"],
        "notional": filled_qty * fill_price,
        "leverage": LEVERAGE, "signal": cross,
        # 🆕 v6.9.0 — Smart TP2 state
        "tp1_hit": False,
        "tp1_hit_ts": None,
        "tp1_level": levels["tp1"],
        "tp1_source": levels.get("tp1_source", "?"),
        "tp1_calc_info": levels.get("tp1_calc_info", {}),
        "tp1_source": levels.get("tp1_source", "?"),      # v7.2.0
        "atr_15m_pct": round(float(levels.get("atr", 0)) / float(levels.get("entry", 1)) * 100, 3),  # v7.2.0
        "tp2_target": levels.get("tp2"),
        "tp2_current": levels.get("tp2"),
        "tp2_moved": False,
        "zone_edge": levels.get("zone_edge"),
        "min_price_after_tp1": None,
        "max_price_after_tp1": None,
        "atr_pct_at_tp1": None,
        "serious_breakout_flag": False,
    }
    _add_ticket(ticket)
    _RECENT_BOT_TRADES[bsym] = _time.time()  # 🩹 FIX v7.9.3/4: reset reservation
    _filter_stats["trades_opened"] += 1
    log.info(f"✅ Opened {new_side} {bsym} qty={filled_qty} @ {fill_price} "
             f"SL={ticket['sl']} TP={ticket['tp']}")

    if bot and CHAT_ID:
        try:
            await bot.send_message(chat_id=CHAT_ID,
                text=build_execution_msg(ticket), parse_mode="HTML")
        except Exception as e: log.error(f"send exec msg: {e}")

    return {"ok": True, "ticket": ticket}


async def _cancel_all_open_orders(bsym):
    """🆕 v7.9.44 — إلغاء شامل مع retry (Regular + Algo)."""
    cancelled = 0

    # 1) Regular orders
    try:
        await asyncio.to_thread(
            _binance.client.futures_cancel_all_open_orders, symbol=bsym)
        log.info(f"🧹 cancel_all regular: {bsym}")
        cancelled += 1
    except Exception as e:
        log.debug(f"cancel_all regular {bsym}: {e}")
    
    # 2) Algo orders with 3 retries
    for _attempt in range(3):
        try:
            algo_orders = await asyncio.to_thread(
                _binance.client.futures_get_open_algo_orders, symbol=bsym)
            if not algo_orders:
                break
            _batch_cancelled = 0
            for o in algo_orders:
                aid = o.get("algoId")
                if not aid:
                    continue
                try:
                    await asyncio.to_thread(
                        _binance.client.futures_cancel_algo_order,
                        symbol=bsym, algoId=aid)
                    cancelled += 1
                    _batch_cancelled += 1
                    log.info(f"🧹 cancelled algo: {bsym} algoId={aid}")
                except Exception as e:
                    log.warning(f"cancel algo {bsym}/{aid} (attempt {_attempt+1}): {e}")
            if _batch_cancelled == 0:
                # no progress → stop retrying
                break
            await asyncio.sleep(0.5)
        except AttributeError:
            break
        except Exception as e:
            log.debug(f"get algo orders {bsym} attempt {_attempt+1}: {e}")
            await asyncio.sleep(1.0)
    
    # 3) Regular individual fallback
    try:
        orders = await asyncio.to_thread(
            _binance.client.futures_get_open_orders, symbol=bsym)
        for o in orders:
            try:
                await asyncio.to_thread(
                    _binance.client.futures_cancel_order,
                    symbol=bsym, orderId=o["orderId"])
                cancelled += 1
            except Exception as e:
                log.debug(f"cancel regular {o.get('orderId')}: {e}")
    except Exception:
        pass

    if cancelled:
        log.info(f"🧹 Total {cancelled} cancelled for {bsym}")
    return cancelled


def _is_managed_by_binance(t):
    """🆕 v6.8.3 — هل Binance تدير الtrade؟"""
    return (t.get("sl_status") == "binance" and
            t.get("tp_status") == "binance")



def build_execution_msg(t):
    bull = t["side"] == "LONG"
    cross = t.get("signal") or {}
    s = cross.get("support", {}) or {}
    market_raw = cross.get("market", "?")
    market_lbl = {"TREND_UP": "📈 ترند صاعد", "TREND_DOWN": "📉 ترند هابط",
                  "RANGE": "↔️ عرضي", "VOLATILE": "🌊 متقلب",
                  "UNKNOWN": "❓"}.get(market_raw, market_raw)
    mtf = cross.get("mtf", "—")
    _entry = float(t.get("entry", 0))
    _tp = float(t.get("tp", 0))
    _sl = float(t.get("sl", 0))
    tp_pct = ((_tp - _entry) / _entry * 100) if _entry > 0 else 0
    sl_pct = ((_sl - _entry) / _entry * 100) if _entry > 0 else 0
    rr = abs(tp_pct / sl_pct) if sl_pct != 0 else 0
    return (
        f"💸 <b>تم تنفيذ صفقة — {short(t['symbol'])}</b>\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>{short(t['symbol'])}</b> [{t['tf']}] "
        f"{'🟢 LONG' if bull else '🔴 SHORT'} | {_source_label(t['source'])}\n"
        f"🏛️ السوق: <b>{market_lbl}</b>\n"
        f"🔗 MTF: <b>{mtf}</b>\n"
        f"🎯 Score: <b>{t['score']}/100</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💵 الدخول: <b>{fmt_price(t['entry'])}</b>\n"
        f"📦 الكمية: {t['qty']} (≈{t['notional']:.2f}$)\n"
        f"⚙️ الرافعة: {t['leverage']}x | {MARGIN_TYPE}\n"
        f"🎯 TP: <b>{fmt_price(t['tp'])}</b> ({tp_pct:+.2f}%)\n"
        f"🛑 SL: <b>{fmt_price(t['sl'])}</b> ({sl_pct:+.2f}%)\n"
        f"📊 R:R: <b>{rr:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• الحجم: {s.get('vol_ratio','?')}×\n"
        f"• ADX: {s.get('adx','?')}\n"
        f"• RSI: {s.get('rsi','?')}\n"
        f"• ATR: {s.get('atr_pct','?')}%\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <code>{t['id']}</code>\n"
        f"⚡ <i>إدارة داخلية — البوت يراقب و يُغلق</i>")


def build_close_msg(t, close_price, pnl_usdt, pnl_pct, reason, age_min):
    bull = t["side"] == "LONG"
    if reason == CLOSE_REASON_TP: emoji, title = "🎯", "TP1 مُحقَّق"
    elif reason == CLOSE_REASON_SL: emoji, title = "🛑", "وقف الخسارة"
    else: emoji, title = "ℹ️", "إغلاق"
    # 🆕 v7.9.26 — always calculate real pnl_usdt
    _e = float(t.get("entry", 0))
    _q = float(t.get("qty", 0))
    if _e > 0 and _q > 0 and close_price > 0:
        pnl_usdt = ((close_price - _e) if bull else (_e - close_price)) * _q
    pnl_icon = "🟢" if pnl_usdt >= 0 else "🔴"
    return (
        f"{emoji} <b>{title} — {short(t['symbol'])}</b>\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 LONG' if bull else '🔴 SHORT'} | {_source_label(t['source'])}\n"
        f"💰 دخول: {fmt_price(t['entry'])}\n"
        f"💵 خروج: {fmt_price(close_price)}\n"
        f"{pnl_icon} <b>PnL: {pnl_usdt:+.3f} USDT ({pnl_pct:+.2f}%)</b>\n"
        f"⏱️ {int(age_min)} دقيقة | 📦 {t['qty']}\n"
        f"🆔 <code>{t['id']}</code>")


# ═══════════════════════════════════════════════════════════
# 20d) 💰 المزامنة + المراقبة
# ═══════════════════════════════════════════════════════════
async def _compute_levels_for_manual(ccxt_symbol, side):
    try:
        data = await fetch_ohlcv_cached(ccxt_symbol, "15m", 100)
        if not data or len(data) < 30: return None
        df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
        direction = "bullish" if side == "LONG" else "bearish"
        return await compute_levels_smart_aware(ccxt_symbol, df, direction, -2, tf="15m", source="manual_sync")
    except Exception as e:
        log.warning(f"compute_levels_manual {ccxt_symbol}: {e}"); return None


async def _sync_manual_positions(bot):
    try:
        positions = await _binance_call(_binance.get_open_positions)
    except Exception as e:
        log.error(f"sync get_positions: {e}"); return
    if not positions: return

    for p in positions:
        bsym = p["symbol"]; amt = p["amount"]
        _abs_amt = abs(float(amt))
        # 🩹 FIX A: skip dust positions
        _info = _binance.info(bsym)
        if _info:
            _min_qty = _info.get("min_qty", 0)
            _min_notional = _info.get("min_notional", 0)
            _mark = p.get("mark_price", 0) or 0
            # v8.0.91d — DUST: qty < min_qty فقط (بدون *1.01)
            if _abs_amt < _min_qty:
                log.debug(f"⏭️ Sync skip {bsym}: dust qty={_abs_amt} < min={_min_qty}")
                continue
            if _mark > 0 and (_abs_amt * _mark) < _min_notional:
                log.debug(f"⏭️ Sync skip {bsym}: dust notional < {_min_notional}")
                continue
        # 🩹 FIX v7.9.4: check reservation (future timestamp = opening in progress)
        _reserved = _RECENT_BOT_TRADES.get(bsym, 0)
        _now = _time.time()
        if _reserved > _now:  # opening in progress
            log.debug(f"⏭️ Sync skip {bsym}: opening in progress (reserved)")
            continue
        if _now - _reserved < 300:  # bot opened recently
            log.debug(f"⏭️ Sync skip {bsym}: bot opened < 300s ago")
            continue
        side = "LONG" if amt > 0 else "SHORT"
        tickets = _tickets_for(bsym)
        
        # 🆕 v6.7.4: فحص مزدوج قبل أي إضافة
        same_side_tickets = [t for t in tickets if t["side"] == side]
        if same_side_tickets:
            total_tracked = sum(t["qty"] for t in same_side_tickets)
            # إذا الكمية المُتتبَّعة تساوي الكمية الفعلية (±2%)
            if abs(total_tracked - abs(amt)) <= abs(amt) * 0.02:
                log.debug(f"✓ {bsym} {side} tracked {total_tracked} = {abs(amt)}")
                continue
        
        tracked_qty = sum(t["qty"] for t in tickets)
        diff = abs(amt) - tracked_qty

        if abs(diff) < abs(amt) * 0.02:  # 🆕 إذا الفرق < 2% → تجاهل
            continue

        # 🩹 FIX v7.9.2: لا نُعدّل tickets البوت (source != manual_sync)
        # لأن البوت يُديرها بنفسه عبر _manage_smart_tp2
        # أي تعديل من _sync سيُفسد تتبع TP1/TP2
        bot_managed = [t for t in tickets if t.get("source") != "manual_sync"]
        if bot_managed:
            log.debug(f"✓ {bsym} {side} has bot-managed tickets — skip sync fix")
            continue
        
        # 🆕 إذا كانت هناك trades يدوية بنفس الرمز+الاتجاه → تجاهل
        if any(t.get("source") == "manual_sync" 
               for t in same_side_tickets):
            log.debug(f"✓ {bsym} {side} tracked as manual trade")
            continue

        if diff > 1e-6:
            # 🆕 v6.7.4: فحص التكرار قبل الإضافة
            existing_same = [t for t in tickets if t["side"] == side]
            if existing_same:
                total_existing = sum(t["qty"] for t in existing_same)
                if abs(total_existing - abs(amt)) < abs(amt) * 0.02:
                    log.debug(f"✓ {bsym} {side} tracked ({total_existing} ≈ {abs(amt)})")
                    continue
            
            ccxt_sym = None
            for s in ALL_SYMBOLS:
                if _to_binance_symbol(s) == bsym: ccxt_sym = s; break
            if not ccxt_sym: ccxt_sym = f"{bsym[:-4]}/USDT:USDT"

            levels = await _compute_levels_for_manual(ccxt_sym, side)
            if not levels:
                log.warning(f"⚠️ manual {bsym}: cannot compute levels"); continue

            # 🆕 v6.7.5b: مراقبة فقط — no نضع أو نلغي أوامر
            log.info(f"🔍 Manual monitoring only: {bsym}")

            tid = _mk_ticket_id(bsym, side) + "_M"
            
            # 🆕 v6.7.5b: احترم القيم الأصلية
            existing_same = [t for t in tickets if t["side"] == side]
            if existing_same:
                _sl = existing_same[0]["sl"]
                _tp = existing_same[0]["tp"]
                _sl_pct = existing_same[0].get("sl_pct", 0)
                _source = existing_same[0].get("source", "manual_sync")
                log.info(f"♻️ Using original SL/TP: {bsym} ({_source})")
            else:
                _sl = levels["stop_loss"]
                _tp = levels["tp1"]
                _sl_pct = levels["sl_pct"]
                _source = "manual_sync"
            
            # 🆕 v6.7.5b: فحص clientOrderId
            # 🆕 v7.9.32 — فحص Algo + Regular orders
            _is_bot_opened = False
            # 1) فحص Algo orders (في Production، SL/TP تُحفظ هنا)
            try:
                _algo = await asyncio.to_thread(
                    _binance.client.futures_get_open_algo_orders, symbol=bsym)
                if _algo and len(_algo) > 0:
                    _is_bot_opened = True
            except Exception:
                pass
            # 2) فحص Regular orders (احتياطي)
            if not _is_bot_opened:
                try:
                    _orders = await asyncio.to_thread(
                        _binance.client.futures_get_open_orders, symbol=bsym)
                    for _o in _orders:
                        _cid = _o.get("clientOrderId", "")
                        if any(x in _cid for x in ["bot_sl", "bot_tp1", "bot_tp2"]):
                            _is_bot_opened = True
                            break
                except Exception:
                    pass
            
            # 🆕 v8.0.35 — حماية مطلقة: الصفقات اليدوية لا تُلمس
            # تصنيف يدوي دائماً (إلا إذا كان هناك تطابق كامل مع صفقة بوت)
            _existing_bot = [t for t in tickets if t.get("source") != "manual_sync"
                            and t.get("binance_symbol") == bsym
                            and t.get("side") == side
                            and abs(t.get("qty", 0) - abs(amt)) < abs(amt) * 0.02]
            if _existing_bot:
                # موجود بالفعل كصفقة بوت، لا نضيف تذكرة جديدة
                log.info(f"⏭️ {bsym} {side} matches existing bot ticket, skipping")
                continue
            
            # 🆕 التصنيف الافتراضي: صفقة يدوية (آمنة)
            _final_source = "manual_sync"
            _final_manual = True
            try:
                await _cancel_all_open_orders(bsym)
                log.info(f"🧹 {bsym}: cleared old orders before sync")
            except Exception as _coe:
                log.debug(f"cancel before sync {bsym}: {_coe}")
            log.info(f"👤 Manual trade (protected): {bsym} {side} qty={abs(amt)}")
            
            ticket = {
                "id": tid, "symbol": ccxt_sym, "binance_symbol": bsym,
                "side": side, "qty": diff,
                "entry": p["entry_price"] or p["mark_price"],
                "sl": _sl, "tp": _tp,
                "sl_pct": _sl_pct,
                "score": 0, "source": _final_source,
                "tf": "15m", "market": None,
                "opened_at": _time.time(), "manual": _final_manual, "order_id": None,
                "notional": diff * (p["entry_price"] or p["mark_price"]),
                "leverage": LEVERAGE, "signal": None,
                # 🆕 v6.7.5b: مراقبة فقط
                "sl_status": "manual",
                "tp_status": "manual",
                "needs_manual_monitor": False,
            }
            # 🆕 v8.0.37 — إضافة التذكرة بدون فحص MAX_CONCURRENT
            # (لأن الاكتشاف لا يستهلك فتح صفقة جديدة)
            _add_ticket(ticket)
            _filter_stats["trades_manual_sync"] += 1
            _lbl = "Manual" if _final_manual else "Recovered"
            log.info(f"🔍 {_lbl} {side} {bsym} qty={diff} @ {ticket['entry']} (bypass max_concurrent)")
            # 🆕 v7.9.41 — Send manual confirm request (only once)
            if bot and CHAT_ID and _final_manual and bsym not in _pending_manual:
                try:
                    await _send_manual_confirm(bot, ticket, levels, cur_price=p.get("mark_price"))
                except Exception as e: log.warning(f"manual confirm: {e}")

        elif diff < -1e-6:
            remaining = abs(diff)
            for t in sorted(tickets, key=lambda x: x["opened_at"]):
                if remaining <= 0: break
                if t["qty"] <= remaining + 1e-9:
                    remaining -= t["qty"]; _remove_ticket(t["id"])
                    _filter_stats["trades_manual_close"] += 1
                    log.info(f"🔎 manual close {t['id']} full")
                else:
                    t["qty"] -= remaining; remaining = 0
                    log.info(f"🔎 manual close {t['id']} partial")


async def _check_ticket_close(t, last_candle):
    hi = last_candle["h"]; lo = last_candle["l"]
    _entry = float(t.get("entry", 0))
    _tp_dist_pct = abs(float(t.get("tp", 0)) - _entry) / _entry * 100 if _entry > 0 else 0
    # 🆕 v7.9.43 — require tp_dist >= 0.2% before allowing TP close
    _tp_valid = _tp_dist_pct >= 0.2
    # 🆕 v7.9.43 — if bot_managed and tp not placed on Binance, don't close by tp
    _tp_managed = t.get("tp_status") == "bot_managed"
    
    if t["side"] == "LONG":
        if lo <= t["sl"] and not t.get("half_sl_mode"): return CLOSE_REASON_SL, t["sl"]
        if hi >= t["tp"] and _tp_valid and not _tp_managed: 
            return CLOSE_REASON_TP, t["tp"]
    else:
        if hi >= t["sl"] and not t.get("half_sl_mode"): return CLOSE_REASON_SL, t["sl"]
        if lo <= t["tp"] and _tp_valid and not _tp_managed:
            return CLOSE_REASON_TP, t["tp"]
    return None, None


# ═══════════════════════════════════════════════════════════
# 🆕 v6.8.0 — إدارة الtrade الذكية بعد الدخول
# ═══════════════════════════════════════════════════════════
def _sz_calc_qty_by_risk(entry, sl, balance_usdt, leverage):
    """
    مخاطرة 1-2% من الرصيد بدلاً من notional ثابت.
    qty = (balance × risk%) / (|entry - sl|) ، محدود بـ (balance × leverage × 0.5)
    """
    risk_amount = balance_usdt * (SZ_RISK_PCT / 100)
    dist = abs(entry - sl)
    if dist <= 0:
        return 0
    qty_risk = risk_amount / dist
    qty_max = (balance_usdt * leverage * 0.5) / entry
    return min(qty_risk, qty_max)


def _sz_move_sl(bsym, new_sl, order_side):
    """v7.9.1 move SL only, keep TP1 — handles algo orders"""
    import time as _t
    # Cancel regular SL
    try:
        orders = _binance.client.futures_get_open_orders(symbol=bsym)
        for o in orders:
            if o.get("type") in ("STOP_MARKET", "STOP"):
                try:
                    _binance.client.futures_cancel_order(
                        symbol=bsym, orderId=o["orderId"])
                    log.info(f"cancelled old regular SL: {o.get('orderId')}")
                except Exception as e:
                    log.warning(f"cancel regular SL {o.get('orderId')}: {e}")
    except Exception as e:
        log.warning(f"get regular orders {bsym}: {e}")
    # Cancel algo SL
    try:
        algo = _binance.client.futures_get_open_algo_orders(symbol=bsym)
        for o in algo:
            if o.get("orderType") in ("STOP_MARKET", "STOP"):
                aid = o.get("algoId")
                if not aid:
                    continue
                try:
                    _binance.client.futures_cancel_algo_order(
                        symbol=bsym, algoId=aid)
                    log.info(f"cancelled old algo SL: {aid}")
                except Exception as e:
                    log.warning(f"cancel algo SL {aid}: {e}")
    except (AttributeError, Exception) as e:
        log.debug(f"get algo orders {bsym}: {e}")
    _t.sleep(0.6)
    try:
        positions = _binance.client.futures_position_information(symbol=bsym)
        cur_qty = 0.0
        for p in positions:
            amt = abs(float(p.get("positionAmt", 0) or 0))
            if amt > 0:
                cur_qty = amt
                break
        info = _binance.info(bsym)
        qty_s = None
        if cur_qty > 0 and info:
            rq = _binance.round_qty(cur_qty, info["step_size"])
            qty_s = _binance.fmt_qty(rq, info["qty_precision"])
        _binance.place_stop_loss(bsym, order_side, new_sl, qty_s)
        return True
    except Exception as e:
        log.warning(f"place new SL {bsym}: {e}")
        return False



async def _sz_manage_single(ticket, bot=None):
    """v7.0.0 disabled: static SL, managed via _manage_smart_tp2."""
    return


async def _sz_execute_force_close(ticket, reason, bot=None):
    """ينفذ إغلاق الtrade (2candle أو struct)"""
    if is_paused():
        log.debug(f"_sz_execute_force_close {ticket.get('binance_symbol')}: paused — skip")
        return
    bsym = ticket["binance_symbol"]
    side = ticket["side"]
    close_side = "SELL" if side == "LONG" else "BUY"
    info = _binance.info(bsym)
    if not info:
        return
    # 🩹 FIX #16: use actual position qty
    _actual = _get_actual_position(bsym)
    if not _actual:
        log.warning(f"force_close: no position {bsym} — removing ticket")
        _remove_ticket(ticket["id"])
        return
    _real_qty = abs(_actual["amount"])
    qty_s = _binance.fmt_qty(
        _binance.round_qty(_real_qty, info["step_size"]),
        info["qty_precision"])
    try:
        res = await _safe_close_market(bsym, close_side, qty_s)
        close_price = res["avg_price"] or 0
        entry = float(ticket["entry"])
        if side == "LONG":
            pnl_pct = (close_price - entry) / entry * 100
        else:
            pnl_pct = (entry - close_price) / entry * 100
        age_min = (_time.time() - ticket["opened_at"]) / 60
        # 🩹 FIX: log trade_stat
        try:
            _log_trade_stat(ticket, close_price, reason)
        except Exception as _e:
            log.warning(f"_log_trade_stat in force_close: {_e}")
        # 🩹 FIX v7.9.6: record risk
        _record_risk_from_ticket(ticket, close_price, reason)
        _remove_ticket(ticket["id"])
        _filter_stats["trades_closed"] += 1
        await _cancel_all_open_orders(bsym)
        log.info(f"🚪 Closed {reason} {bsym} @ {close_price} | {pnl_pct:+.2f}%")
        if bot and CHAT_ID:
            reason_lbl = {"2candle": "🕯️ شمعتان معاكستان",
                          "struct": "💥 كسر بنية 1H"}.get(reason, reason)
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"{reason_lbl} — {short(ticket['symbol'])}\n"
                          f"🇸🇾 {syria_now_str()}\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"💰 دخول: {fmt_price(entry)}\n"
                          f"💵 خروج: {fmt_price(close_price)}\n"
                          f"📊 P&L: <b>{pnl_pct:+.2f}%</b>\n"
                          f"⏱️ {int(age_min)} دقيقة"),
                    parse_mode="HTML")
            except Exception: pass
    except Exception as e:
        log.warning(f"force_close {bsym}: {e}")


async def sz_manage_open_positions(bot=None):
    """يدير كل الtrades المفتوحة — يُستدعى من job"""
    if not SMART_ZONES_ENABLED or not _binance.ok:
        return
    if is_paused():
        log.debug("sz_manage: paused — skip")
        return
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    for t in tickets:
        if t.get("manual"):
            continue
        # 🆕 v6.9.0 — Smart TP2 أولاً (للtrades التي ضُرب فيها TP1)
        if t.get("tp_status") == "tp1_bot":
            try:
                await _manage_smart_tp2(t, bot=bot)
            except Exception as e:
                log.warning(f"smart_tp2 {t.get('id')}: {e}")
            continue
        try:
            if t.get("force_close"):
                reason = t.pop("force_close")
                await _sz_execute_force_close(t, reason, bot=bot)
        except Exception as e:
            log.warning(f"sz_manage {t.get('id')}: {e}")


async def _get_total_open_orders(bsym):
    """🆕 v7.9.32 — إجمالي الأوامر (Regular + Algo)"""
    total = 0
    try:
        r = await asyncio.to_thread(
            _binance.client.futures_get_open_orders, symbol=bsym)
        total += len(r or [])
    except Exception:
        pass
    try:
        a = await asyncio.to_thread(
            _binance.client.futures_get_open_algo_orders, symbol=bsym)
        total += len(a or [])
    except Exception:
        pass
    return total


async def _detect_binance_closes(bot=None):
    """🆕 v6.8.8 — يكتشف إغلاق Binance + إشعار + تنظيف"""
    if not _binance.ok:
        return
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    if not tickets:
        return
    
    # 🆕 v7.9.13 — MAX_HOLD_MINUTES check (force close stale trades)
    _now = _time.time()
    for t in tickets:
        _age_min = (_now - t.get("opened_at", _now)) / 60
        if _age_min > MAX_HOLD_MINUTES and not t.get("tp1_hit"):
            if is_paused():
                log.debug(f"MAX_HOLD {t.get('binance_symbol')}: paused — skip force close")
                continue
            bsym = t["binance_symbol"]
            _side = t["side"]
            _close_side = "SELL" if _side == "LONG" else "BUY"
            try:
                _pos = await _binance_call(_binance.get_open_positions)
                _actual = next((p for p in _pos if p["symbol"] == bsym), None)
                if _actual:
                    _qty = abs(float(_actual["amount"]))
                    _info = _binance.info(bsym)
                    _qty_s = _binance.fmt_qty(
                        _binance.round_qty(_qty, _info["step_size"]),
                        _info["qty_precision"]) if _info else f"{_qty}"
                    _res = await _safe_close_market(bsym, _close_side, _qty_s)
                    _cp = _res["avg_price"] or 0
                    log.warning(f"⏰ MAX_HOLD {MAX_HOLD_MINUTES}min — force close {bsym} @ {_cp}")
                    _log_trade_stat(t, _cp, "MAX_HOLD")
                    _record_risk_from_ticket(t, _cp, "MAX_HOLD")
                    _remove_ticket(t["id"])
                    _filter_stats["trades_closed"] += 1
                    if bot and CHAT_ID:
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=(f"⏰ <b>إغلاق (تجاوُز الحد) — {short(t['symbol'])}</b>\n"
                                      f"📌 صفقة مفتوحة > {MAX_HOLD_MINUTES} دقيقة\n"
                                      f"💰 الدخول: {fmt_price(t['entry'])}\n"
                                      f"💵 الخروج: {fmt_price(_cp)}"),
                                parse_mode="HTML")
                        except Exception: pass
            except Exception as _e:
                log.warning(f"max_hold close {bsym}: {_e}")
    
    try:
        positions = await asyncio.wait_for(
            asyncio.to_thread(_binance.get_open_positions), timeout=10.0)
    except asyncio.TimeoutError:
        log.warning("detect_closes: API timeout, retry next cycle")
        return
    except Exception as e:
        log.warning(f"detect_closes: {e}")
        # 🆕 v7.9.25 — notify user on rate limit
        global _last_ratelimit_notified
        if "-1003" in str(e) or "Way too many requests" in str(e):
            _now_rl = _time.time()
            if (_now_rl - _last_ratelimit_notified) > RATELIMIT_NOTIFY_COOLDOWN:
                _last_ratelimit_notified = _now_rl
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(f"⚠️ <b>Binance Rate Limit</b>\n"
                                  f"🇸🇾 {syria_now_str()}\n"
                                  f"━━━━━━━━━━━━━━━━━━━\n"
                                  f"🚫 IP محظور مؤقتاً من Binance API\n"
                                  f"⚡ البوت يعمل بحذر — يتجنب إغلاق التذاكر\n"
                                  f"⏱️ إعادة المحاولة: {RATELIMIT_NOTIFY_COOLDOWN//60} دقيقة"),
                            parse_mode="HTML")
                    except Exception: pass
        return
    
    # 🆕 v7.9.25 — double-check on empty (prevent rate limit misfire)
    if not positions:
        await asyncio.sleep(0.5)  # 🆕 v8.0.5: 2s → 0.5s لتسريع المراقبة
        try:
            positions = await asyncio.wait_for(
                asyncio.to_thread(_binance.get_open_positions), timeout=10.0)
        except Exception:
            return
        if not positions:
            log.warning("detect_closes: positions empty twice — skip")
            return
    
    # 🆕 v7.9.49 — التحقق من أن كل صفقة مغلقة فعلاً قبل تسجيل EXTERNAL
    for t in tickets:
        bsym = t["binance_symbol"]
        if bsym not in {p["symbol"] for p in positions}:
            # المركز غير موجود، لكن لنأخذ نفساً ونعيد الفحص للتأكد (Race condition)
            await asyncio.sleep(1)
            try:
                _recheck = await _binance_call(_binance.get_open_positions)
                if _recheck and any(p["symbol"] == bsym for p in _recheck):
                    log.warning(f"⏭️ {bsym}: false EXTERNAL detected — still open")
                    continue
            except Exception:
                pass
    
    active_syms = {p["symbol"] for p in positions}

    for t in tickets:
        # v8.0.87h — include "partial" (legacy)
        if not (t.get("sl_status") == "binance" and
                t.get("tp_status") in ("binance", "tp1_bot", "partial")):
            continue
        # age check
        age_sec = _time.time() - t.get("opened_at", 0)
        if age_sec < 90:
            continue
        # closed check
        if t["id"] in _closed_binance_ids:
            continue
        bsym = t["binance_symbol"]
        if bsym not in active_syms:
            # v8.0.87h — double-check before skipping
            _pending_orders = await _get_total_open_orders(bsym)
            if _pending_orders > 0:
                _has_position = False
                try:
                    _recheck = await _binance_call(_binance.get_open_positions)
                    _has_position = any(
                        p["symbol"] == bsym and abs(float(p.get("amount", 0))) > 1e-9
                        for p in _recheck
                    )
                except Exception:
                    pass
                if _has_position:
                    log.debug(f"⏭️ {bsym}: still has position, skip")
                    continue
                else:
                    log.warning(f"⚠️ {bsym}: no position but {_pending_orders} orphan orders — proceeding")
                    try:
                        await _cancel_all_open_orders(bsym)
                    except Exception:
                        pass
            _closed_binance_ids.append(t["id"])
            log.info(f"🔍 Binance closed {bsym} (ticket {t['id'][-20:]})")
            # 🆕 v7.9.45 — determine TP1 vs SL by price (with tolerance)
            try:
                _cur = await _binance_call(_binance.last_price, bsym)
            except Exception:
                _cur = 0
            _sl = float(t.get("sl", 0))
            _tp = float(t.get("tp", 0))
            _entry = float(t.get("entry", 0))  # v8.0.87v: needed for P&L
            _side = t.get("side", "LONG")
            # v8.0.87v — P&L-based classification (لا SL مع ربح)
            _reason = "external_close"
            if _sl > 0 and _tp > 0 and _cur > 0 and _entry > 0:
                _is_profit = (_cur > _entry) if _side == "LONG" else (_cur < _entry)
                if _side == "LONG":
                    _hit_sl = _cur <= _sl * 1.002
                    _hit_tp = _cur >= _tp * 0.998
                else:
                    _hit_sl = _cur >= _sl * 0.998
                    _hit_tp = _cur <= _tp * 1.002
                if _hit_sl and not _is_profit:
                    _reason = "SL"
                elif _hit_tp and _is_profit:
                    _reason = "TP1"
                # else: external_close (بين SL و TP، أو SL مع ربح، أو TP مع خسارة)
            log.info(f"🔍 {bsym} closed: reason={_reason} cur={_cur} sl={_sl} tp={_tp}")
            # log + risk
            try:
                _log_trade_stat(t, _cur, _reason)
                _record_risk_from_ticket(t, _cur, _reason)
            except Exception as _e:
                log.warning(f"_log_trade_stat in detect_closes: {_e}")
            # تنظيف
            try:
                await _cancel_all_open_orders(bsym)
            except Exception:
                pass
            # احذف ticket
            try:
                _remove_ticket(t["id"])
                _filter_stats["trades_closed"] += 1
            except Exception:
                pass
            # إشعار
            if bot and CHAT_ID:
                entry = float(t.get("entry", 0))
                qty = float(t.get("qty", 0))
                if _side == "LONG":
                    _pnl = (_cur - entry) * qty
                    _pnl_pct = (_cur - entry) / entry * 100 if entry else 0
                else:
                    _pnl = (entry - _cur) * qty
                    _pnl_pct = (entry - _cur) / entry * 100 if entry else 0
                _age_min = age_sec / 60
                _is_manual = t.get("manual") or t.get("source") == "manual_sync"
                _tag = " (يدوي)" if _is_manual else ""
                try:
                    if _reason == "TP1":
                        # v8.0.87i — TP2 ملغى: إغلاق 100% عند TP1
                        _msg = (
                            f"🎯 <b>TP1 محقق — {short(t['symbol'])}{_tag}</b>\n"
                            f"🇸🇾 {syria_now_str()}\n"
                            f"━━━━━━━━━━━━━━━━━━━\n"
                            f"{'🟢 LONG' if _side=='LONG' else '🔴 SHORT'} | 📦 {qty}\n"
                            f"💰 دخول: {fmt_price(entry)}\n"
                            f"💵 خروج: {fmt_price(_cur)}\n"
                            f"{'🟢' if _pnl>=0 else '🔴'} <b>PnL: {_pnl:+.3f} USDT ({_pnl_pct:+.2f}%)</b>\n"
                            f"⏱️ {int(_age_min)} دقيقة\n"
                            f"✅ الصفقة مغلقة بالكامل (100%)")
                    elif _reason == "SL":
                        _msg = (
                            f"🛑 <b>وقف الخسارة — {short(t['symbol'])}{_tag}</b>\n"
                            f"🇸🇾 {syria_now_str()}\n"
                            f"━━━━━━━━━━━━━━━━━━━\n"
                            f"{'🟢 LONG' if _side=='LONG' else '🔴 SHORT'} | 📦 {qty}\n"
                            f"💰 دخول: {fmt_price(entry)}\n"
                            f"💵 خروج: {fmt_price(_cur)}\n"
                            f"{'🟢' if _pnl>=0 else '🔴'} <b>PnL: {_pnl:+.3f} USDT ({_pnl_pct:+.2f}%)</b>\n"
                            f"⏱️ {int(_age_min)} دقيقة")
                    else:
                        _msg = (
                            f"🔔 <b>إغلاق من Binance — {short(t['symbol'])}{_tag}</b>\n"
                            f"🇸🇾 {syria_now_str()}\n"
                            f"━━━━━━━━━━━━━━━━━━━\n"
                            f"{'🟢 LONG' if _side=='LONG' else '🔴 SHORT'}\n"
                            f"💰 دخول: {fmt_price(entry)}\n"
                            f"💵 خروج: {fmt_price(_cur)}\n"
                            f"{'🟢' if _pnl>=0 else '🔴'} <b>PnL: {_pnl:+.3f} USDT ({_pnl_pct:+.2f}%)</b>\n"
                            f"⏱️ {int(_age_min)} دقيقة")
                    await bot.send_message(chat_id=CHAT_ID, text=_msg, parse_mode="HTML")
                except Exception as e:
                    log.error(f"detect notify: {e}")


# ═══════════════════════════════════════════════════════════
# 🆕 v6.9.0 — Smart TP2 Manager
# ═══════════════════════════════════════════════════════════
def _should_skip_monitor(t):
    """تخطى في fast_tp_sl و manage_open_positions"""
    # SL و TP1 على Binance
    if t.get("sl_status") == "binance" and t.get("tp_status") == "binance":
        return True
    # Smart TP2 (TP1 على Binance، TP2 bot-managed)
    if (t.get("sl_status") == "binance" and
        t.get("tp_status") == "tp1_bot"):
        return True
    return False


def _record_shadow_reversal(ticket, close_price, pnl_pct):
    """🆕 v7.9.23 — log shadow trade when closed by reversal signal"""
    try:
        import json as _json
        rec = {
            "ts": _time.time(),
            "symbol": ticket.get("symbol", "?"),
            "binance_symbol": ticket.get("binance_symbol", "?"),
            "side": ticket.get("side", "?"),
            "tf": ticket.get("tf", "?"),
            "source": ticket.get("source", "?"),
            "score": ticket.get("score", 0),
            "entry": ticket.get("entry", 0),
            "close_at_reversal": close_price,
            "pnl_pct_at_reversal": round(pnl_pct, 3),
            "sl": ticket.get("sl", 0),
            "tp1": ticket.get("tp", 0),
            "tp2": ticket.get("tp2", 0),
            "tp1_hit": ticket.get("tp1_hit", False),
            "tp2_moved": ticket.get("tp2_moved", False),
            "reversal_ts": _time.time(),
            "shadow_result": None,
            "shadow_close": None,
            "shadow_pnl_pct": None,
            "shadow_closed_at": None,
            "max_price": ticket.get("entry", 0),
            "min_price": ticket.get("entry", 0),
        }
        with io.open(SHADOW_TRADES_FILE, "a", encoding="utf-8") as f:
            f.write(_json.dumps(rec, ensure_ascii=False) + "\n")
        log.info(f"👻 Shadow reversal logged: {ticket.get('binance_symbol')} {ticket.get('side')}")
    except Exception as e:
        log.warning(f"_record_shadow_reversal: {e}")


async def _evaluate_shadow_trades():
    """🆕 v7.9.23 — evaluate shadow trades (after 60 min or TP/SL hit)"""
    try:
        import json as _json
        if not os.path.exists(SHADOW_TRADES_FILE):
            return
        lines = []
        with io.open(SHADOW_TRADES_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        changed = False
        new_lines = []
        for line in lines:
            try:
                rec = _json.loads(line.strip())
            except Exception:
                new_lines.append(line)
                continue
            if rec.get("shadow_result") is not None:
                new_lines.append(line)
                continue
            # check timeout (60 min)
            _age = (_time.time() - rec.get("reversal_ts", 0)) / 60
            _bsym = rec.get("binance_symbol", "")
            _side = rec.get("side", "")
            _entry = rec.get("entry", 0)
            _tp1 = rec.get("tp1", 0)
            _tp2 = rec.get("tp2", 0)
            _sl = rec.get("sl", 0)
            try:
                _cur = await _binance_call(_binance.last_price, _bsym)
            except Exception:
                _cur = 0
            if _cur <= 0:
                new_lines.append(line)
                continue
            # update max/min
            if _side == "LONG":
                rec["max_price"] = max(rec.get("max_price", _entry), _cur)
                rec["min_price"] = min(rec.get("min_price", _entry), _cur)
            else:
                rec["max_price"] = max(rec.get("max_price", _entry), _cur)
                rec["min_price"] = min(rec.get("min_price", _entry), _cur)
            # check TP1/TP2/SL/timeout
            _res = None
            _close = _cur
            if _side == "LONG":
                if _sl > 0 and _cur <= _sl:
                    _res = "shadow_sl"
                elif _tp2 > 0 and _cur >= _tp2:
                    _res = "shadow_tp2"
                elif _tp1 > 0 and _cur >= _tp1:
                    _res = "shadow_tp1"
            else:
                if _sl > 0 and _cur >= _sl:
                    _res = "shadow_sl"
                elif _tp2 > 0 and _cur <= _tp2:
                    _res = "shadow_tp2"
                elif _tp1 > 0 and _cur <= _tp1:
                    _res = "shadow_tp1"
            if _age > 60 and _res is None:
                _res = "shadow_expired"
            if _res:
                if _entry > 0:
                    _pnl = ((_close - _entry) / _entry * 100) if _side == "LONG" else ((_entry - _close) / _entry * 100)
                else:
                    _pnl = 0
                rec["shadow_result"] = _res
                rec["shadow_close"] = _close
                rec["shadow_pnl_pct"] = round(_pnl, 3)
                rec["shadow_closed_at"] = _time.time()
                changed = True
                log.info(f"👻 Shadow closed: {_bsym} {_side} {_res} ({_pnl:+.2f}%)")
            new_lines.append(_json.dumps(rec, ensure_ascii=False) + "\n")
        if changed:
            with io.open(SHADOW_TRADES_FILE, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
    except Exception as e:
        log.warning(f"_evaluate_shadow_trades: {e}")


def _record_risk_from_ticket(ticket, close_price, reason=""):
    """🩹 FIX v7.9.6: unified risk recording."""
    try:
        # تجاهل صفقات manual
        if ticket.get("source") == "manual_sync" or ticket.get("manual"):
            return
        entry = float(ticket.get("entry", 0))
        qty = float(ticket.get("qty", 0))
        if entry <= 0 or qty <= 0:
            return
        side = ticket.get("side", "")
        if side == "LONG":
            pnl_usd = (close_price - entry) * qty
        else:
            pnl_usd = (entry - close_price) * qty
        # تحديد النتيجة
        if "SL" in reason or "وقف" in reason or "خسارة" in reason:
            result = "loss"
        elif "TP" in reason or "هدف" in reason:
            result = "win"
        elif pnl_usd > 0:
            result = "win"
        elif pnl_usd < 0:
            result = "loss"
        else:
            return
        _risk_mgr.record_result(pnl_usd, result)
        log.info(f"📊 Risk record: {ticket.get('binance_symbol')} {result} ${pnl_usd:+.2f}")
    except Exception as e:
        log.warning(f"_record_risk_from_ticket: {e}")


async def _close_remaining_tp2(ticket, bot, reason="TP2"):
    """يُغلق اfor 50% المتبقية عند TP2"""
    if is_paused():
        log.debug(f"_close_remaining_tp2 {ticket.get('binance_symbol')}: paused — skip")
        return
    bsym = ticket["binance_symbol"]
    side = ticket["side"]
    close_side = "SELL" if side == "LONG" else "BUY"

    try:
        pos = await _binance_call(_binance.get_open_positions)
    except Exception:
        return
    current_pos = next((p for p in pos if p["symbol"] == bsym), None)
    if not current_pos:
        return
    qty = abs(float(current_pos["amount"]))
    if qty <= 0:
        return

    info = _binance.info(bsym)
    qty_s = _binance.fmt_qty(
        _binance.round_qty(qty, info["step_size"]),
        info["qty_precision"]) if info else f"{qty}"

    try:
        res = await _safe_close_market(bsym, close_side, qty_s)
    except Exception as e:
        log.warning(f"_close_remaining_tp2 {bsym}: {e}")
        return

    close_price = res["avg_price"] or 0
    entry = float(ticket["entry"])
    if side == "LONG":
        pnl_pct = (close_price - entry) / entry * 100
    else:
        pnl_pct = (entry - close_price) / entry * 100

    await _cancel_all_open_orders(bsym)

    # v7.2.0: log stats before removal
    _mfe = None; _mae = None
    try:
        _e = float(ticket.get("entry", 0))
        _side = ticket.get("side", "")
        _mn = ticket.get("min_price_after_tp1")
        _mx = ticket.get("max_price_after_tp1")
        if _mn is None and _mx is None:
            _mn = close_price; _mx = close_price
        elif _mn is None:
            _mn = _mx
        elif _mx is None:
            _mx = _mn
        if _e > 0:
            if _side == "SHORT":
                _mfe = abs(_e - _mn) / _e * 100
                _mae = abs(_mx - _e) / _e * 100
            else:
                _mfe = abs(_mx - _e) / _e * 100
                _mae = abs(_e - _mn) / _e * 100
    except Exception:
        pass
    _log_trade_stat(ticket, close_price, reason, mfe_pct=_mfe, mae_pct=_mae)

    _remove_ticket(ticket["id"])
    _filter_stats["trades_closed"] += 1
    _filter_stats["trades_tp"] = _filter_stats.get("trades_tp", 0) + 1
    # 🩹 FIX v7.9.6: record in risk manager
    _record_risk_from_ticket(ticket, close_price, reason)
    log.info(f"🎯 {reason} hit: {bsym} @ {close_price} | {pnl_pct:+.2f}%")

    if bot and CHAT_ID:
        try:
            _pnl_usdt = ((close_price - entry) if side == "LONG" else (entry - close_price)) * float(ticket.get("qty", 0))
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(f"🎯 <b>{reason} — {short(ticket['symbol'])}</b>\n"
                      f"🇸🇾 {syria_now_str()}\n"
                      f"━━━━━━━━━━━━━━━━━━━\n"
                      f"💰 دخول: {fmt_price(entry)}\n"
                      f"💵 خروج: {fmt_price(close_price)}\n"
                      f"{'🟢' if _pnl_usdt >= 0 else '🔴'} <b>PnL: {_pnl_usdt:+.3f} USDT ({pnl_pct:+.2f}%)</b>\n"
                      f"⚡ 50% المتبقي أُغلق — الtrade انتهت"),
                parse_mode="HTML")
        except Exception:
            pass


def _calc_entry_tolerance(ticket, atr_pct):
    """v7.0.0 entry tolerance (ATR-based)"""
    if atr_pct is None or atr_pct <= 0:
        return TP2_ENTRY_RETURN_PCT
    return max(TP2_ENTRY_RETURN_PCT, atr_pct * TP2_ENTRY_ATR_MULT)


async def _manage_smart_tp2(ticket, bot=None):
    # v8.0.87c — معطّل: TP2 ملغى، إغلاق 100% عند TP1
    return
    """v7.0.0 manage remaining 50% after TP1: static SL + smart TP2 move."""
    if not SMART_TP2_ENABLED:
        return
    # 🆕 v8.0.35 — الصفقات اليدوية لا تُدار أبداً
    if ticket.get("manual") or ticket.get("source") == "manual_sync":
        return

    bsym = ticket["binance_symbol"]
    side = ticket["side"]

    try:
        pos = await _binance_call(_binance.get_open_positions)
    except Exception:
        return
    current_pos = next((p for p in pos if p["symbol"] == bsym), None)

    if not current_pos:
        await _cancel_all_open_orders(bsym)
        _remove_ticket(ticket["id"])
        _filter_stats["trades_closed"] += 1
        # 🩹 FIX: detect actual reason
        entry = float(ticket.get("entry", 0))
        sl = float(ticket.get("sl", 0))
        side = ticket["side"]
        try:
            klines = await _binance_call(_binance.get_klines, bsym, "1m", 5)
            last_close = float(klines[-2][4]) if klines and len(klines) >= 2 else sl
        except Exception:
            last_close = sl
        # تحديد السبب
        if not ticket.get("tp1_hit"):
            # لم يضرب TP1 → SL أو إغلاق خارجي
            hit_sl = ((side == "LONG" and last_close <= sl * 1.001) or
                      (side == "SHORT" and last_close >= sl * 0.999))
            if hit_sl:
                reason_label = "🛑 وقف الخسارة (SL)"
                result_kind = "SL"
            else:
                reason_label = "❓ إغلاق خارجي"
                result_kind = "EXTERNAL"
        else:
            reason_label = "🎯 TP2 (بعد TP1)"
            result_kind = "TP2"
        # PnL
        if entry > 0:
            pnl_pct = ((last_close - entry) / entry * 100) if side == "LONG" else ((entry - last_close) / entry * 100)
        else:
            pnl_pct = 0
        log.info(f"🚪 {bsym} closed: {reason_label} | last={last_close} | {pnl_pct:+.2f}%")
        # 🩹 FIX: log trade stat
        try:
            _log_trade_stat(ticket, last_close, reason_label)
        except Exception as _e:
            log.warning(f"_log_trade_stat in smart_tp2: {_e}")
        # 🩹 FIX v7.9.6: record in risk manager
        _record_risk_from_ticket(ticket, last_close, reason_label)
        if bot and CHAT_ID:
            try:
                pnl_icon = "🟢" if pnl_pct >= 0 else "🔴"
                _pnl_usdt = ((last_close - entry) if side == "LONG" else (entry - last_close)) * float(ticket.get("qty", 0))
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"{reason_label} — {short(ticket['symbol'])}\n"
                          f"🇸🇾 {syria_now_str()}\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"💰 دخول: {fmt_price(entry)}\n"
                          f"💵 خروج: {fmt_price(last_close)}\n"
                          f"{pnl_icon} <b>PnL: {_pnl_usdt:+.3f} USDT ({pnl_pct:+.2f}%)</b>\n"
                          f"📊 النتيجة: {result_kind}"),
                    parse_mode="HTML")
            except Exception:
                pass
        return

    current_qty = abs(float(current_pos["amount"]))
    initial_qty = float(ticket["qty"])

    try:
        current_price = await _binance_call(_binance.last_price, bsym)
    except Exception:
        return
    if current_price <= 0:
        return

    # 🆕 v7.9.38 — Handle bot_managed TP1 (check by price)
    if not ticket.get("tp1_hit") and ticket.get("tp_status") == "bot_managed":
        _tp1 = float(ticket.get("tp", 0))
        if _tp1 > 0:
            _hit = ((side == "LONG" and current_price >= _tp1) or
                    (side == "SHORT" and current_price <= _tp1))
            if _hit:
                log.info(f"🎯 TP1 (bot-managed) hit: {bsym} @ {current_price}")
                _qty_to_close = initial_qty * 0.6
                _info = _binance.info(bsym)
                _qty_s = _binance.fmt_qty(
                    _binance.round_qty(_qty_to_close, _info["step_size"]),
                    _info["qty_precision"])
                try:
                    await _safe_close_market(bsym, "SELL" if side == "LONG" else "BUY", _qty_s)
                    ticket["tp1_hit"] = True
                    ticket["tp1_hit_ts"] = _time.time()
                    ticket["tp_status"] = "tp1_bot"
                    ticket["min_price_after_tp1"] = current_price
                    ticket["max_price_after_tp1"] = current_price
                    _save_state()
                    if bot and CHAT_ID:
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=(f"🎯 <b>TP1 (bot-managed) — {short(ticket['symbol'])}</b>\n"
                                      f"💰 60% أُغلق @ {fmt_price(current_price)}\n"
                                      f"🎯 TP2 target: {fmt_price(ticket['tp2_target'] or 0)}\n"
                                      f"🛑 SL: {fmt_price(ticket['sl'])}"),
                                parse_mode="HTML")
                        except Exception: pass
                except Exception as _e:
                    log.warning(f"Manual TP1 close: {_e}")
                return
        return  # still waiting for TP1
    
    # 🆕 v7.9.43 — Bot-managed TP1: verify price >= tp (not just qty)
    if not ticket.get("tp1_hit") and ticket.get("tp_status") == "bot_managed":
        _tp1 = float(ticket.get("tp", 0))
        _entry = float(ticket.get("entry", 0))
        _tp_dist_pct = abs(_tp1 - _entry) / _entry * 100 if _entry > 0 else 0
        _tp_valid = _tp_dist_pct >= 0.2
        
        if _tp1 > 0 and _tp_valid:
            _hit = ((side == "LONG" and current_price >= _tp1) or
                    (side == "SHORT" and current_price <= _tp1))
            if _hit:
                log.info(f"🎯 TP1 (bot-managed) hit: {bsym} @ {current_price}")
                _qty_to_close = initial_qty * 0.6
                _info = _binance.info(bsym)
                _qty_s = _binance.fmt_qty(
                    _binance.round_qty(_qty_to_close, _info["step_size"]),
                    _info["qty_precision"])
                try:
                    await _safe_close_market(bsym, "SELL" if side == "LONG" else "BUY", _qty_s)
                    ticket["tp1_hit"] = True
                    ticket["tp1_hit_ts"] = _time.time()
                    ticket["tp_status"] = "tp1_bot"
                    ticket["min_price_after_tp1"] = current_price
                    ticket["max_price_after_tp1"] = current_price
                    _save_state()
                    log.info(f"✅ bot-managed TP1 closed 60% of {bsym}")
                except Exception as _e:
                    log.warning(f"bot-managed TP1 close: {_e}")
                return
        return  # still waiting for TP1
    
    if not ticket.get("tp1_hit") and not ticket.get("half_sl_hit"):
        # 🆕 v8.0.12 — اكتشاف دقيق للـ TP1 أو Half SL
        if current_qty < initial_qty * 0.95:
            _sl = float(ticket.get("sl", 0))
            _tp = float(ticket.get("tp", 0))
            _side = ticket["side"]
            _is_sl_hit = False
            _is_tp_hit = False
            
            if _side == "LONG":
                if current_price <= _sl * 1.005: _is_sl_hit = True
                elif current_price >= _tp * 0.995: _is_tp_hit = True
            else:
                if current_price >= _sl * 0.995: _is_sl_hit = True
                elif current_price <= _tp * 1.005: _is_tp_hit = True
                
            if _is_tp_hit:
                ticket["tp1_hit"] = True
                ticket["tp1_hit_ts"] = _time.time()
                ticket["min_price_after_tp1"] = current_price
                ticket["max_price_after_tp1"] = current_price
                ticket["qty"] = current_qty
                _save_state()
                log.info(f"TP1 hit: {bsym} remaining={current_qty}/{initial_qty}")
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(f"🎯 <b>TP1 حُقق — {short(ticket['symbol'])}</b>\n"
                                  f"🇸🇾 {syria_now_str()}\n"
                                  f"━━━━━━━━━━━━━━━━━━━\n"
                                  f"💰 60% أُغلق على Binance\n"
                                  f"🎯 TP2 target: {fmt_price(ticket['tp2_target'] or 0)}\n"
                                  f"🛑 SL ثابت: {fmt_price(ticket['sl'])}\n"
                                  f"⚡ البوت يتابع 40% المتبقي"),
                            parse_mode="HTML")
                    except Exception:
                        pass
            elif _is_sl_hit:
                ticket["half_sl_hit"] = True
                ticket["qty"] = current_qty
                ticket["tp_status"] = "bot_managed" # البوت يدير النصف المتبقي
                _save_state()
                log.info(f"Half SL hit: {bsym} remaining={current_qty}/{initial_qty}")
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(f"🛑 <b>وقف الخسارة (50%) — {short(ticket['symbol'])}</b>\n"
                                  f"🇸🇾 {syria_now_str()}\n"
                                  f"━━━━━━━━━━━━━━━━━━━\n"
                                  f"💰 أُغلق 50% على Binance\n"
                                  f"⚡ البوت يتابع الـ 50% المتبقي لعل السعر يعود."),
                            parse_mode="HTML")
                    except Exception:
                        pass
            else:
                # Partial close (manual or unknown)
                ticket["qty"] = current_qty
                _save_state()
                log.info(f"Partial close detected: {bsym} remaining={current_qty}/{initial_qty}")
        else:
            return

    if side == "SHORT":
        if ticket["min_price_after_tp1"] is None or current_price < ticket["min_price_after_tp1"]:
            ticket["min_price_after_tp1"] = current_price
    else:
        if ticket["max_price_after_tp1"] is None or current_price > ticket["max_price_after_tp1"]:
            ticket["max_price_after_tp1"] = current_price

    if TP2_TWO_CANDLE_AFTER_TP1 and ticket.get("tp2_moved"):
        try:
            d15 = await fetch_ohlcv_cached(ticket["symbol"], "15m", 20)
            if d15 and len(d15) >= 5:
                df15 = pd.DataFrame(d15, columns=["ts","o","h","l","c","v"])
                c1 = df15.iloc[-3]
                c2 = df15.iloc[-2]
                _entry = float(ticket.get("entry", 0))
                _cur = float(c2["c"])
                # 🩹 FIX v7.9.5: 2-candle فقط إذا السعر عاد للخسارة
                _in_loss = False
                if side == "LONG" and _entry > 0 and _cur < _entry:
                    _in_loss = True
                elif side == "SHORT" and _entry > 0 and _cur > _entry:
                    _in_loss = True

                # 🆕 v7.9.15 — MFE protection: if MFE > 1%, don't close
                _mfe_pct = 0.0
                if side == "LONG":
                    _mfe_check = ticket.get("max_price_after_tp1") or _cur
                    _mfe_pct = (_mfe_check - _entry) / _entry * 100 if _entry > 0 else 0
                else:
                    _mfe_check = ticket.get("min_price_after_tp1") or _cur
                    _mfe_pct = (_entry - _mfe_check) / _entry * 100 if _entry > 0 else 0
                
                if _mfe_pct > 1.0:
                    # 🆕 MFE > 1% — لا تُغلق، اترك TP2
                    log.info(f"2candle {bsym}: skip (MFE {_mfe_pct:.2f}% > 1%)")
                    return
                
                if not _in_loss:
                    # السعر لا يزال في ربح — لا تُغلق، اترك TP2
                    log.debug(f"2candle {bsym}: skip (still profitable)")
                elif side == "LONG":
                    both_red = (c1["c"] < c1["o"]) and (c2["c"] < c2["o"])
                    below = float(c2["c"]) < float(c1["l"])
                    if both_red and below:
                        log.info(f"2 candles + loss after TP1: {bsym}")
                        await _close_remaining_tp2(ticket, bot, "شمعتان معاكستان + خسارة")
                        return
                else:
                    both_green = (c1["c"] > c1["o"]) and (c2["c"] > c2["o"])
                    above = float(c2["c"]) > float(c1["h"])
                    if both_green and above:
                        log.info(f"2 candles + loss after TP1: {bsym}")
                        await _close_remaining_tp2(ticket, bot, "شمعتان معاكستان + خسارة")
                        return
        except Exception as e:
            log.debug(f"2candle {bsym}: {e}")

    if ticket.get("tp2_moved"):
        target = ticket["tp2_current"]
        hit = ((side == "SHORT" and current_price <= target) or
               (side == "LONG"  and current_price >= target))
        if hit:
            await _close_remaining_tp2(ticket, bot, "TP2 (بعد النقل)")
        return

    target_orig = ticket.get("tp2_target")
    if target_orig:
        hit = ((side == "SHORT" and current_price <= target_orig) or
               (side == "LONG"  and current_price >= target_orig))
        if hit:
            await _close_remaining_tp2(ticket, bot, "TP2")
            return

    zone_edge = ticket.get("zone_edge")
    if zone_edge is None:
        if not ticket.get("_no_edge_logged"):
            _filter_stats["tp2_skipped_no_edge"] += 1
            ticket["_no_edge_logged"] = True
            log.info(f"no zone_edge for {bsym}: skip TP2 move")
        return

    buffer = 1 + TP2_ZONE_EDGE_BUFFER_PCT / 100
    if side == "LONG":
        max_p = ticket["max_price_after_tp1"] or current_price
        seriously_broke = max_p >= zone_edge * buffer
    else:
        min_p = ticket["min_price_after_tp1"] or current_price
        seriously_broke = min_p <= zone_edge / buffer

    if seriously_broke:
        if not ticket.get("serious_breakout_flag"):
            ticket["serious_breakout_flag"] = True
            _save_state()
            log.info(f"{bsym}: serious breakout — wait original TP2")
        return

    entry = float(ticket["entry"])
    atr_pct = ticket.get("atr_pct_at_tp1") or 0
    tolerance = _calc_entry_tolerance(ticket, atr_pct)
    diff_pct = abs(current_price - entry) / entry * 100

    if diff_pct <= tolerance:
        ticket["tp2_moved"] = True
        ticket["tp2_current"] = ticket["tp1_level"]
        ticket["moved_at"] = _time.time()
        _save_state()
        log.info(f"TP2->TP1 @ {ticket['tp1_level']} for {bsym}")
        if bot and CHAT_ID:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"🔄 <b>TP2 نُقل — {short(ticket['symbol'])}</b>\n"
                          f"🇸🇾 {syria_now_str()}\n"
                          f"━━━━━━━━━━━━━━━━━━━\n"
                          f"💡 فشل اختراق <b>{fmt_price(zone_edge)}</b>\n"
                          f"💰 السعر عاد للدخول: {fmt_price(entry)}\n"
                          f"🎯 TP2 الجديد = TP1: <b>{fmt_price(ticket['tp1_level'])}</b>\n"
                          f"🛑 SL ثابت: {fmt_price(ticket['sl'])}\n"
                          f"⚡ انتظار الارتداد للهدف الجديد"),
                    parse_mode="HTML")
            except Exception:
                pass


async def manage_open_positions(bot=None):
    if not _binance.ok: return
    if is_paused():
        log.debug("manage_open_positions: paused — skip closing")
        return

    try: await _sync_manual_positions(bot)
    except Exception as e: log.exception(f"sync: {e}")
    try: await _detect_binance_closes(bot)
    except Exception as e: log.warning(f"detect_closes: {e}")

    symbols = set()
    with _ticket_lock:
        for bsym in _symbol_ticket_index.keys():
            if _symbol_ticket_index[bsym]: symbols.add(bsym)
    if not symbols: return

    for bsym in symbols:
        tickets = _tickets_for(bsym)
        if not tickets: continue
        try:
            klines = await _binance_call(_binance.get_klines, bsym, "1m", 2)
        except Exception as e:
            log.warning(f"klines {bsym}: {e}"); continue
        if not klines or len(klines) < 2: continue
        k = klines[-2]
        last_candle = {"o": float(k[1]), "h": float(k[2]),
                       "l": float(k[3]), "c": float(k[4])}

        closes = defaultdict(list)
        for t in tickets:
            # 🛡️ تخطى إذا Binance تُدير الtrade
            if _should_skip_monitor(t):
                continue
            # 🆕 v6.9.0 — تخطى TP2 bot-managed
            if t.get("tp_status") == "tp1_bot":
                continue
            # 🆕 v7.9.36 — إذا كان TP1 bot-managed (فشل الإرسال)، أدره يدوياً
            if t.get("tp_status") == "bot_managed" and not t.get("tp1_hit"):
                # تحقق من TP1 يدوياً
                _tp1 = float(t.get("tp", 0))
                if _tp1 > 0:
                    if (t["side"] == "LONG" and last_candle["h"] >= _tp1) or \
                       (t["side"] == "SHORT" and last_candle["l"] <= _tp1):
                        log.info(f"🎯 TP1 (bot-managed) hit: {bsym} @ {_tp1}")
                        # تحديث حالة TP1 وبدء إدارة TP2
                        t["tp1_hit"] = True
                        t["tp1_hit_ts"] = _time.time()
                        # إغلاق 60% يدوياً
                        _qty_to_close = t["qty"] * 0.6
                        _info = _binance.info(bsym)
                        _qty_s = _binance.fmt_qty(
                            _binance.round_qty(_qty_to_close, _info["step_size"]),
                            _info["qty_precision"])
                        try:
                            await _safe_close_market(bsym, "SELL" if t["side"] == "LONG" else "BUY", _qty_s)
                            t["qty"] -= _qty_to_close
                            t["tp_status"] = "tp1_bot"  # الآن البوت يدير الـ40% المتبقي
                            _save_state()
                            log.info(f"✅ Closed 60% of {bsym} manually")
                        except Exception as _e:
                            log.warning(f"❌ Manual close 60% failed: {_e}")
                        continue  # انتقل للصفقة التالية

            reason, hit_price = await _check_ticket_close(t, last_candle)
            if reason:
                closes[(t["side"], reason)].append((t, hit_price))
                try:
                    _e = float(t.get("entry", 0))
                    _q = float(t.get("qty", 0))
                    _m = 1 if t.get("side") == "LONG" else -1
                    _pnl = (float(hit_price) - _e) * _q * _m
                    _res = "win" if reason == CLOSE_REASON_TP else "loss"
                    _risk_mgr.record_result(_pnl, _res)
                except Exception as _ex:
                    log.warning(f"⚠️ risk record: {_ex}")

        for (side, reason), lst in closes.items():
            total_qty = sum(t["qty"] for t, _ in lst)
            if total_qty <= 0: continue

            # 🆕 {__version_full__} — استخدام الكمية الفعلية من Binance
            try:
                _actual = _get_actual_position(bsym)
                if _actual:
                    real_qty = abs(_actual["amount"])
                    if total_qty > real_qty * 1.02:
                        log.warning(f"⚠️ {__version_full__}: tickets={total_qty} > real={real_qty} "
                                    f"— نستخدم الفعلي")
                        total_qty = real_qty
            except Exception as _e:
                log.warning(f"actual qty check: {_e}")
            info = _binance.info(bsym)
            qty_s = _binance.fmt_qty(
                _binance.round_qty(total_qty, info["step_size"]),
                info["qty_precision"]) if info else f"{total_qty}"

            close_side = "SELL" if side == "LONG" else "BUY"
            try:
                res = await _safe_close_market(bsym, close_side, qty_s)
            except BinanceAPIException as e:
                log.error(f"❌ close {bsym} {side}: {e.code} {e.message}")
                # 🆕 {__version_full__} — إذا reduce-only failed, امسح tickets للرمز
                if e.code == -2022:
                    log.warning(f"🧹 Cleared all {bsym} tickets (position gone)")
                    for _t in _tickets_for(bsym):
                        _remove_ticket(_t["id"])
                continue
            except Exception as e:
                # 🩹 FIX #9: track failures, abandon after 5
                for _t, _ in lst:
                    _n = _t.get("_close_fails", 0) + 1
                    _t["_close_fails"] = _n
                    if _n >= 5:
                        log.error(f"🚨 abandon {_t['id']}: 5 failed close attempts")
                        _remove_ticket(_t["id"])
                        if bot and CHAT_ID:
                            try:
                                await bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=(f"🚨 <b>فشل إغلاق متكرر</b>\n"
                                          f"📌 {short(_t['symbol'])} ({_t['side']})\n"
                                          f"❌ {_n} محاولات فاشلة"),
                                    parse_mode="HTML")
                            except Exception: pass
                log.exception(f"close {bsym}: {e}")
                continue

            close_price = res["avg_price"] or lst[0][1]

            # 🆕 v6.8.3 — تنظيف (helper)
            await _cancel_all_open_orders(bsym)

            for t, _hit in lst:
                entry = t["entry"]; qty = t["qty"]
                if t["side"] == "LONG":
                    pnl_usdt = (close_price - entry) * qty
                    pnl_pct = (close_price - entry) / entry * 100
                else:
                    pnl_usdt = (entry - close_price) * qty
                    pnl_pct = (entry - close_price) / entry * 100
                fees = t["notional"] * 0.0008
                pnl_usdt_net = pnl_usdt - fees
                age_min = (_time.time() - t["opened_at"]) / 60
                # v7.2.0: log stats
                _log_trade_stat(t, close_price, reason)

                _remove_ticket(t["id"])
                _filter_stats["trades_closed"] += 1
                if reason == CLOSE_REASON_TP: _filter_stats["trades_tp"] += 1
                else: _filter_stats["trades_sl"] += 1
                _last_signals.append({
                    "symbol": t["symbol"], "timeframe": t["tf"],
                    "direction": "bullish" if t["side"] == "LONG" else "bearish",
                    "source": t["source"], "score": t["score"],
                    "entry": entry, "exit": close_price,
                    "result": "win_tp1" if reason == CLOSE_REASON_TP else "loss_sl",
                    "pnl_pct": round(pnl_pct, 2),
                    "duration_min": int(age_min), "ts": _time.time()})
                _smart.record_result(
                    "win_tp1" if reason == CLOSE_REASON_TP else "loss_sl",
                    "bullish" if t["side"] == "LONG" else "bearish", t["source"])
                log.info(f"💰 Closed {t['id']} | {reason} | PnL {pnl_usdt_net:+.3f} USDT")
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=build_close_msg(t, close_price, pnl_usdt_net,
                                                 pnl_pct, reason, age_min),
                            parse_mode="HTML")
                    except Exception as e: log.error(f"close notify: {e}")


# ═══════════════════════════════════════════════════════════
# 20e) 💰 أوامر الإدارة (مُصلَّحة لfor callbacks)
# ═══════════════════════════════════════════════════════════
async def cmd_bcheck(update, context):
    """🔐 تشخيص حالة Binance API."""
    lines = ["🔐 <b>تشخيص Binance</b>", "━━━━━━━━━━━━━━━━━━━"]
    lines.append(f"• python-binance: "
                 f"{'✅' if _BINANCE_LIB_OK else '❌ غير مثبت'}")
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET"):
        raw = os.getenv(k)
        if raw is None:
            lines.append(f"• {k}: ❌ <b>not found in .env</b>")
        else:
            cleaned = _read_env_clean(k)
            if not cleaned:
                lines.append(f"• {k}: ❌ <b>empty</b> (raw len={len(raw)})")
            else:
                lines.append(f"• {k}: ✅ len={len(cleaned)} | "
                             f"<code>***hidden***</code>")
    lines.append(f"• TESTNET: {'✅' if BINANCE_TESTNET else '❌ MAINNET'}")
    lines.append(f"• ENABLED: {'✅' if BINANCE_ENABLED else '❌'}")
    lines.append(f"• Notional: {NOTIONAL_USDT}$ | Lev: {LEVERAGE}x")
    lines.append(f"• Margin: {MARGIN_TYPE}")
    lines.append("━━━━━━━━━━━━━━━━━━━")
    if _binance.ok:
        try:
            bal = await _binance_call(_binance.usdt_balance)
            if bal:
                lines.append(f"✅ <b>متصل</b>")
                lines.append(f"• رصيد: {bal['wallet']:.3f} USDT")
                lines.append(f"• متاح: {bal['available']:.3f} USDT")
                lines.append(f"• Unrealized: {bal['unrealized']:+.3f} USDT")
            else:
                lines.append("⚠️ متصل لكن فشل جلب الرصيد")
        except Exception as e:
            lines.append(f"⚠️ {e}")
    else:
        lines.append(f"❌ <b>غير متصل</b>")
        lines.append(f"<b>السبب:</b> <code>{_binance.error_msg or 'غير معروف'}</code>")
        if _binance.error_msg and "فارغة" in _binance.error_msg:
            lines.append("\n<b>💡 الحل:</b>")
            lines.append("1. <code>nano /root/bot2/.env</code>")
            lines.append("2. أضف بدون مسافات أو quotes:")
            lines.append("<code>BINANCE_API_KEY=xxxx</code>")
            lines.append("<code>BINANCE_API_SECRET=xxxx</code>")
            lines.append("3. <code>systemctl restart signals-bot</code>")
        elif _binance.error_msg and "-2015" in _binance.error_msg:
            lines.append("\n<b>💡 الحل:</b>")
            lines.append("1. تأكد من تفعيل Futures على المفتاح")
            lines.append("2. تأكد أن IP السيرفر مسموح")
            lines.append("3. تأكد أن المفتاح لم يُحذف من Binance")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_positions(update, context):
    if not _binance.ok:
        await _reply(update,
            f"❌ Binance غير متصل.\n"
            f"السبب: <code>{_binance.error_msg or 'غير معروف'}</code>\n"
            f"جرّب: /bcheck", parse_mode="HTML")
        return
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    if not tickets:
        await _reply(update, "📭 no trades مفتوحة (متتبع)."); return
    lines = [f"💼 <b>الtrades المفتوحة ({len(tickets)})</b>", "━━━━━━━━━━━━━━━━━━━"]
    now = _time.time()
    for t in tickets:
        try:
            cur = await _binance_call(_binance.last_price, t["binance_symbol"])
        except Exception:
            cur = t["entry"]
        if t["side"] == "LONG": pnl_pct = (cur - t["entry"]) / t["entry"] * 100
        else: pnl_pct = (t["entry"] - cur) / t["entry"] * 100
        pnl_icon = "🟢" if pnl_pct >= 0 else "🔴"
        age = int((now - t["opened_at"]) / 60)
        lines.append(
            f"\n{'🟢 LONG' if t['side']=='LONG' else '🔴 SHORT'} "
            f"<b>{short(t['symbol'])}</b> [{t['tf']}]"
            f"{' 🔧' if t.get('manual') else ''}\n"
            f"   💰 {fmt_price(t['entry'])} → {fmt_price(cur)}\n"
            f"   {pnl_icon} {pnl_pct:+.2f}% | ⏱️ {age} د\n"
            f"   🎯 TP {fmt_price(t['tp'])} | 🛑 SL {fmt_price(t['sl'])}\n"
            f"   📦 {t['qty']} | 🆔 <code>{t['id'][-20:]}</code>")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_close(update, context):
    if not _binance.ok:
        await _reply(update,
            f"❌ Binance غير متصل.\n"
            f"السبب: <code>{_binance.error_msg or 'غير معروف'}</code>",
            parse_mode="HTML")
        return
    args = context.args or []
    if not args:
        await _reply(update, "الاستخدام: <code>/close XRP</code>", parse_mode="HTML")
        return
    coin = args[0].upper()
    target_sym = None
    for s in ALL_SYMBOLS:
        if short(s) == coin: target_sym = _to_binance_symbol(s); break
    if not target_sym: target_sym = coin + "USDT"
    tickets = _tickets_for(target_sym)
    if not tickets:
        await _reply(update, f"⚠️ no trades على {coin}."); return
    total_qty = sum(t["qty"] for t in tickets)
    side = tickets[0]["side"]
    close_side = "SELL" if side == "LONG" else "BUY"
    info = _binance.info(target_sym)
    qty_s = _binance.fmt_qty(_binance.round_qty(total_qty, info["step_size"]),
                             info["qty_precision"]) if info else f"{total_qty}"
    try:
        res = await _safe_close_market(target_sym, close_side, qty_s)
    except Exception as e:
        await _reply(update, f"❌ فشل: {e}"); return
    close_price = res["avg_price"] or 0
    # 🆕 v6.8.2 — تنظيف
    await _cancel_all_open_orders(target_sym)
    for t in tickets:
        if t["side"] == "LONG": pnl_pct = (close_price - t["entry"]) / t["entry"] * 100
        else: pnl_pct = (t["entry"] - close_price) / t["entry"] * 100
        age_min = (_time.time() - t["opened_at"]) / 60
        # 🩹 FIX: log trade_stat
        try:
            _log_trade_stat(t, close_price, "MANUAL_CLOSE")
        except Exception as _e:
            log.warning(f"_log_trade_stat in cmd_close: {_e}")
        _remove_ticket(t["id"])
        _filter_stats["trades_closed"] += 1
        _filter_stats["trades_manual_close"] += 1
        try:
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=build_close_msg(t, close_price, 0.0, pnl_pct,
                                     "MANUAL", age_min),
                parse_mode="HTML")
        except Exception: pass


async def cmd_closeall(update, context):
    if not _binance.ok:
        await _reply(update,
            f"❌ Binance غير متصل.\n"
            f"السبب: <code>{_binance.error_msg or 'غير معروف'}</code>",
            parse_mode="HTML")
        return
    with _ticket_lock:
        symbols = [s for s, ids in _symbol_ticket_index.items() if ids]
    if not symbols:
        await _reply(update, "📭 no trades مفتوحة."); return
    await _reply(update, f"🔒 جارٍ إغلاق {len(symbols)} رمز...")
    for bsym in symbols:
        tickets = _tickets_for(bsym)
        if not tickets: continue
        total_qty = sum(t["qty"] for t in tickets)
        side = tickets[0]["side"]
        close_side = "SELL" if side == "LONG" else "BUY"
        info = _binance.info(bsym)
        qty_s = _binance.fmt_qty(_binance.round_qty(total_qty, info["step_size"]),
                                 info["qty_precision"]) if info else f"{total_qty}"
        try:
            res = await _safe_close_market(bsym, close_side, qty_s)
            cp = res["avg_price"] or 0
            # 🆕 v6.8.2 — تنظيف
            await _cancel_all_open_orders(bsym)
            for t in tickets:
                age_min = (_time.time() - t["opened_at"]) / 60
                # 🩹 FIX: log trade_stat
                try:
                    _log_trade_stat(t, cp, "CLOSE_ALL")
                except Exception as _e:
                    log.warning(f"_log_trade_stat in closeall: {_e}")
                _remove_ticket(t["id"])
                _filter_stats["trades_closed"] += 1
                _filter_stats["trades_manual_close"] += 1
                try:
                    await context.bot.send_message(
                        chat_id=CHAT_ID,
                        text=build_close_msg(t, cp, 0.0, 0.0, "MANUAL", age_min),
                        parse_mode="HTML")
                except Exception: pass
        except Exception as e:
            await _reply(update, f"❌ {bsym}: {e}")
        await asyncio.sleep(0.3)


async def cmd_pnl(update, context):
    if not _binance.ok:
        await _reply(update,
            f"❌ Binance غير متصل.\n"
            f"السبب: <code>{_binance.error_msg or 'غير معروف'}</code>",
            parse_mode="HTML")
        return
    bal = await _binance_call(_binance.usdt_balance)
    if not bal:
        await _reply(update, "❌ فشل جلب الرصيد."); return
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    total_unrealized = 0.0
    lines = [f"💵 <b>حالة الحساب</b>", "━━━━━━━━━━━━━━━━━━━",
             f"• رصيد المحفظة: <b>{bal['wallet']:.3f} USDT</b>",
             f"• متاح: <b>{bal['available']:.3f} USDT</b>"]
    if tickets:
        lines.append(f"\n<b>📊 trades مفتوحة ({len(tickets)}):</b>")
        for t in tickets:
            try:
                cur = await _binance_call(_binance.last_price, t["binance_symbol"])
            except Exception:
                cur = t["entry"]
            if t["side"] == "LONG": pu = (cur - t["entry"]) * t["qty"]
            else: pu = (t["entry"] - cur) * t["qty"]
            total_unrealized += pu
            ico = "🟢" if pu >= 0 else "🔴"
            lines.append(f"{ico} {short(t['symbol'])} "
                         f"{'L' if t['side']=='LONG' else 'S'} {pu:+.3f} USDT")
    lines.append("━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📈 Unrealized: <b>{total_unrealized:+.3f} USDT</b>")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_syncnow(update, context):
    if not _binance.ok:
        await _reply(update,
            f"❌ Binance غير متصل.\n"
            f"السبب: <code>{_binance.error_msg or 'غير معروف'}</code>",
            parse_mode="HTML")
        return
    await _reply(update, "🔄 مزامنة...")
    await _sync_manual_positions(context.bot)
    with _ticket_lock:
        n = len(TRADE_TICKETS)
    await _reply(update, f"✅ انتهت. Tickets: {n}")


# ═══════════════════════════════════════════════════════════
# 21) الواجهة
# ═══════════════════════════════════════════════════════════
def main_menu_kb() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("💼 الtrades",   callback_data="menu:positions"),
         InlineKeyboardButton("💵 الرصيد",     callback_data="menu:pnl")],
        [InlineKeyboardButton("🔄 مزامنة",     callback_data="menu:syncnow"),
         InlineKeyboardButton("🔐 فحص Binance", callback_data="menu:bcheck")],
        [InlineKeyboardButton("🧹 إغلاق الكل", callback_data="menu:closeall"),
         InlineKeyboardButton("📊 S/R",         callback_data="menu:sr")],
        [InlineKeyboardButton("📈 فحص الإشارات", callback_data="menu:cross"),
         InlineKeyboardButton("🎯 تشخيص",       callback_data="menu:why")],
        [InlineKeyboardButton("📋 آخر الإشارات", callback_data="menu:last"),
         InlineKeyboardButton("⏳ النشطة",       callback_data="menu:pending")],
        [InlineKeyboardButton("📐 نطاقات 6%", callback_data="menu:ref6"),
         InlineKeyboardButton("🗺️ النطاقات",   callback_data="menu:ranges")],
        [InlineKeyboardButton("🎯 مناطق S/R",  callback_data="menu:zones")],
        [InlineKeyboardButton("🔍 فحص النطاقات", callback_data="menu:checkranges"),
         InlineKeyboardButton("📊 الأداء",      callback_data="menu:stats")],
        [InlineKeyboardButton("⚙️ الحالة",      callback_data="menu:status"),
         InlineKeyboardButton("🔧 التشدد",      callback_data="menu:strict")],
        [InlineKeyboardButton("🔄 تحديث",       callback_data="menu:refresh")],
    ]
    return InlineKeyboardMarkup(kb)


def coin_menu_kb(symbol: str) -> InlineKeyboardMarkup:
    s = short(symbol)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📈 رسم {s}", callback_data=f"chart:{s}"),
         InlineKeyboardButton(f"🔍 تشخيص {s}", callback_data=f"why:{s}")],
        [InlineKeyboardButton("📊 كل العملات", callback_data="menu:why"),
         InlineKeyboardButton("⬅️ القائمة", callback_data="menu:main")],
    ])


def back_kb(target="menu:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ رجوع", callback_data=target)
    ]])


async def cmd_menu(update, context):
    txt = (f"🎛️ <b>القائمة الرئيسية — {__version_full__}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"🤖 Unified Smart Bot\n"
           f"💸 Binance Futures: {'✅' if _binance.ok else '❌'}\n"
           f"🔌 {_exchange.primary_name} · {len(ALL_SYMBOLS)} عملة\n"
           f"🇸🇾 {syria_now_str()}\n\n"
           f"👇 اختر:")
    if update.callback_query:
        q = update.callback_query
        try:
            await q.edit_message_text(
                txt, parse_mode="HTML", reply_markup=main_menu_kb())
        except Exception:
            # fallback: photo message cannot be edited to text
            try:
                await q.message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(
                    chat_id=q.message.chat_id,
                    text=txt, parse_mode="HTML",
                    reply_markup=main_menu_kb())
            except Exception as e:
                log.error(f"cmd_menu callback fallback: {e}")
    else:
        await _reply(update, txt, parse_mode="HTML",
                     reply_markup=main_menu_kb())


# 🆕 v8.0.0 — Scalper (Standalone Range Radar)
_scalper_session = {
    "active": False, "symbol": None, "range_high": 0, "range_low": 0,
    "midpoint": 0, "target_pct": 0, "notional": 0, "leverage": 20,
    "long_ticket": None, "short_ticket": None,
    "long_closed": False, "short_closed": False
}

async def _get_btc_filter():
    """فلتر BTC: (allowed_long, allowed_short, btc_breakout)"""
    try:
        data = await fetch_ohlcv_cached("BTC/USDT:USDT", "5m", 50)
        if not data: return True, True, False
        df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
        adx = float(calc_adx(df).iloc[-2])
        h = df["h"].iloc[-50:-2].max(); l = df["l"].iloc[-50:-2].min()
        price = float(df["c"].iloc[-2])
        
        if adx > 30:
            e50 = calc_ema(df["c"], 50).iloc[-2]
            if price > e50: return True, False, False
            else: return False, True, False
        
        if price > h * 1.002 or price < l * 0.998:
            return True, True, True  # Breakout
        
        return True, True, False  # Range
    except:
        return True, True, False

# 🆕 v8.0.25 — Scalper: دعم إطارين + نافذة أقصر
_scalper_tf = "5m"  # افتراضي
_SCALPER_WINDOW = {"5m": 15, "15m": 6, "1h": 5}  # 🆕 v8.0.48: 1h = 5 شمعات

async def _detect_range_radar(symbol, tf=None):
    """يبحث عن نطاق ضيق في عملة محددة"""
    try:
        _tf = tf or _scalper_tf
        _win = _SCALPER_WINDOW.get(_tf, 15)
        data = await fetch_ohlcv_cached(symbol, _tf, 50)
        if not data or len(data) < _win + 5: return None
        df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
        
        adx = float(calc_adx(df).iloc[-2])
        atr = float(calc_atr(df).iloc[-2]) / float(df["c"].iloc[-2]) * 100
        
        if adx > 25 or atr < 0.3: return None
        
        window = df.iloc[-_win-2:-2]
        high = float(window["h"].max()); low = float(window["l"].min())
        mid = (high + low) / 2
        price = float(df["c"].iloc[-2])
        
        width_pct = (high - low) / mid * 100
        if width_pct < 0.5 or width_pct > 2.5: return None
        
        if abs(price - mid) / mid * 100 > 0.1: return None
        
        target_pct = max(0.25, min(width_pct / 2, 0.5))
        notional = min(200, max(50, 0.5 / (target_pct / 100)))
        
        return {
            "symbol": symbol, "adx": adx, "atr": atr, "high": high, "low": low,
            "mid": mid, "price": price, "width_pct": width_pct,
            "target_pct": target_pct, "notional": notional, "tf": _tf
        }
    except: return None

async def _scalper_monitor_job(context):
    if not _scalper_session["active"]: return
    s = _scalper_session
    
    _, _, btc_breakout = await _get_btc_filter()
    if btc_breakout:
        log.info("🛑 BTC Breakout — Closing scalper session")
        await _close_scalper_session(context.bot, "BTC Breakout")
        return
    
    try:
        price = _binance.last_price(_to_binance_symbol(s["symbol"]))
        if price > s["high"] * 1.002 and not s["short_closed"]:
            await _close_scalper_side("short", context.bot, "Range Breakout Up")
        elif price < s["low"] * 0.998 and not s["long_closed"]:
            await _close_scalper_side("long", context.bot, "Range Breakout Down")
    except: pass

async def _close_scalper_side(side, bot, reason):
    tid = _scalper_session.get(f"{side}_ticket")
    if not tid: return
    t = TRADE_TICKETS.get(tid)
    if not t: return
    bsym = t["binance_symbol"]
    close_side = "SELL" if t["side"] == "LONG" else "BUY"
    info = _binance.info(bsym)
    qty_s = _binance.fmt_qty(_binance.round_qty(t["qty"], info["step_size"]), info["qty_precision"])
    try:
        res = await _safe_close_market(bsym, close_side, qty_s)
        _remove_ticket(t["id"] if isinstance(tid, dict) else tid)
        _scalper_session[f"{side}_closed"] = True
        log.info(f"🔒 Closed {side} ({reason})")
    except Exception as e: log.warning(f"scalper close {side}: {e}")

async def _close_scalper_session(bot, reason):
    await _close_scalper_side("long", bot, reason)
    await _close_scalper_side("short", bot, reason)
    _scalper_session["active"] = False
    if bot and CHAT_ID:
        await bot.send_message(CHAT_ID, f"🛑 <b>انتهت جلسة Scalping</b>\nالسبب: {reason}", parse_mode="HTML")

async def on_callback(update, context):
    q = update.callback_query
    data = q.data or ""
    try: await q.answer()
    except Exception: pass
    
    # 🆕 v7.9.42 — Close now (2c-opposite)
    if data.startswith("scalper:start:"):
        sym = data.split(":")[2]
        r = await _detect_range_radar(sym, tf=_scalper_tf)
        if not r: 
            await q.edit_message_text("⚠️ الفرصة اختفت")
            return
        await q.edit_message_text(f"⏳ جاري فتح صفقتين على {short(sym)}...")
        
        bsym = _to_binance_symbol(sym)
        info = _binance.info(bsym)
        price = _binance.last_price(bsym)
        
        try:
            _binance.client.futures_change_leverage(symbol=bsym, leverage=20)
        except: pass
        try:
            _binance.client.futures_change_position_mode(dualSidePosition=True)
        except: pass
        
        qty = r["notional"] / price
        qty_s = _binance.fmt_qty(_binance.round_qty(qty, info["step_size"]), info["qty_precision"])
        
        _scalper_session.update({
            "active": True, "symbol": sym, "high": r["high"], "low": r["low"],
            "mid": r["mid"], "target_pct": r["target_pct"], "notional": r["notional"],
            "leverage": 20, "long_closed": False, "short_closed": False
        })
        
        try:
            res_l = await _binance_call(_binance.open_market, bsym, "BUY", qty_s)
            _scalper_session["long_ticket"] = f"SCALP_L_{int(time.time())}"
            TRADE_TICKETS[_scalper_session["long_ticket"]] = {
                "id": _scalper_session["long_ticket"], "symbol": sym, "binance_symbol": bsym,
                "side": "LONG", "qty": float(qty_s), "entry": res_l["avg_price"],
                "tp": price * (1 + r["target_pct"]/100), "sl": 0, "source": "scalper",
                "leverage": 20
            }
            res_s = await _binance_call(_binance.open_market, bsym, "SELL", qty_s)
            _scalper_session["short_ticket"] = f"SCALP_S_{int(time.time())}"
            TRADE_TICKETS[_scalper_session["short_ticket"]] = {
                "id": _scalper_session["short_ticket"], "symbol": sym, "binance_symbol": bsym,
                "side": "SHORT", "qty": float(qty_s), "entry": res_s["avg_price"],
                "tp": price * (1 - r["target_pct"]/100), "sl": 0, "source": "scalper",
                "leverage": 20
            }
            await q.edit_message_text(f"✅ <b>بدأت الجلسة</b> (20x)\nLONG: {res_l['avg_price']}\nSHORT: {res_s['avg_price']}", parse_mode="HTML")
        except Exception as e:
            await q.edit_message_text(f"❌ فشل فتح الصفقات: {e}")
        return

    if data == "scalper:cancel":
        await q.edit_message_text("❌ تم الإلغاء")
        return

    if data.startswith("close_now:"):
        bsym = data.split(":", 1)[1]
        ticket = next((t for t in _tickets_for(bsym) if not t.get("manual")), None)
        if not ticket:
            try:
                await q.edit_message_text("⚠️ لا صفقة مفتوحة لهذا الرمز")
            except: pass
            return
        try:
            await q.edit_message_text(f"⏳ جارٍ إغلاق {short(bsym)}...")
        except: pass
        try:
            _side = ticket["side"]
            _close_side = "SELL" if _side == "LONG" else "BUY"
            _info = _binance.info(bsym)
            _qty_s = _binance.fmt_qty(
                _binance.round_qty(float(ticket["qty"]), _info["step_size"]),
                _info["qty_precision"])
            _res = await _safe_close_market(bsym, _close_side, _qty_s)
            _cp = _res.get("avg_price", 0)
            _e = float(ticket["entry"])
            if _side == "LONG":
                _pnl_pct = (_cp - _e) / _e * 100
                _pnl_usdt = (_cp - _e) * float(ticket["qty"])
            else:
                _pnl_pct = (_e - _cp) / _e * 100
                _pnl_usdt = (_e - _cp) * float(ticket["qty"])
            _log_trade_stat(ticket, _cp, "2candle_manual")
            _record_risk_from_ticket(ticket, _cp, "2candle_manual")
            _remove_ticket(ticket["id"])
            _filter_stats["trades_closed"] += 1
            await _cancel_all_open_orders(bsym)
            try:
                await q.edit_message_text(
                    f"✅ <b>أُغلقت {short(bsym)}</b>\n"
                    f"💵 الدخول: {fmt_price(_e)}\n"
                    f"💵 الخروج: {fmt_price(_cp)}\n"
                    f"{'🟢' if _pnl_usdt >= 0 else '🔴'} "
                    f"PnL: <b>{_pnl_usdt:+.3f} USDT ({_pnl_pct:+.2f}%)</b>",
                    parse_mode="HTML")
            except: pass
        except Exception as e:
            log.error(f"close_now {bsym}: {e}")
            try:
                await q.edit_message_text(f"❌ فشل الإغلاق: {e}")
            except: pass
        return
    
    if data.startswith("ignore_2c:"):
        bsym = data.split(":", 1)[1]
        try:
            await q.edit_message_text(
                f"✅ <b>{short(bsym)}</b> — تجاهل الإشعار (الصفقة مفتوحة)",
                parse_mode="HTML")
        except: pass
        return
    
    # 🆕 v7.9.41 — Manual trade confirmation
    # v8.0.112 — Near-Miss
    if data.startswith("nm:exec:"):
        sym = data.split(":", 2)[2]
        pending = _pending_manual.get(sym)
        if not pending:
            try:
                await q.edit_message_text("⚠️ لا اقتراح معلق لهذا الرمز")
            except: pass
            return
        # تنفيذ بسعر السوق الحالي
        try:
            cross = pending.get("cross")
            if not cross:
                await q.edit_message_text("⚠️ بيانات الإشارة مفقودة")
                return
            # تحديث السعر الحالي
            from signals import _binance
            _bsym = _to_binance_symbol(sym)
            _price = _binance.last_price(_bsym)
            if _price <= 0:
                await q.edit_message_text("❌ فشل جلب السعر الحالي")
                return
            _levels = cross.get("levels", {})
            _levels["entry"] = _price
            # حساب TP1 من السعر الحالي (0.48% ثابت)
            _direction = cross.get("direction")
            if _direction == "bullish":
                _levels["tp1"] = _price * 1.0035
            else:
                _levels["tp1"] = _price * 0.9965

            # 🆕 v8.1.3 — SL من Smart Zones
            try:
                _sz = await sz_compute_levels(sym, _direction, cross.get("timeframe", "5m"))
                if _sz and _sz.get("stop_loss"):
                    _levels["stop_loss"] = _sz["stop_loss"]
                    log.info(f"✅ nm:exec SL from Smart Zones: {_sz['stop_loss']:.6f}")
                else:
                    raise Exception("Smart Zones returned None")
            except Exception as _sze:
                if _direction == "bullish":
                    _levels["stop_loss"] = _price * (1 - 0.007)
                else:
                    _levels["stop_loss"] = _price * (1 + 0.007)
                log.warning(f"⚠️ nm:exec SL fallback 0.7%: {_levels['stop_loss']:.6f} ({_sze})")

            # 🆕 v8.1.3 — تجاهل فلتر RR (المستخدم وافق)
            cross["_skip_rr_check"] = True
            cross["levels"] = _levels
            # تنفيذ
            exe = await execute_binance_trade(cross, bot=context.bot)
            if exe.get("ok"):
                try:
                    await q.edit_message_text(
                        f"✅ <b>تم التنفيذ — {short(sym)}</b>\n"
                        f"💵 السعر: {fmt_price(_price)}\n"
                        f"🎯 TP: {fmt_price(_levels['tp1'])}\n"
                        f"🛑 SL: {fmt_price(_levels['stop_loss'])}",
                        parse_mode="HTML")
                except: pass
            else:
                try:
                    await q.edit_message_text(f"❌ فشل التنفيذ: {exe.get('error')}")
                except: pass
        except Exception as _e:
            try:
                await q.edit_message_text(f"❌ خطأ: {_e}")
            except: pass
        return
    
    if data.startswith("nm:paper:"):
        sym = data.split(":", 2)[2]
        _pending_manual.pop(sym, None)
        try:
            await q.edit_message_text(
                f"📄 <b>{short(sym)}</b> — Paper فقط\n"
                f"⚡ الإشارة تُراقب في الورقي",
                parse_mode="HTML")
        except: pass
        return

    if data.startswith("mc:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        action = parts[1]
        bsym = parts[2]
        pending = _pending_manual.get(bsym)
        if not pending:
            try:
                await q.edit_message_text("⚠️ لا اقتراح معلق لهذا الرمز")
            except: pass
            return
        
        if action == "ok":
            try:
                await q.edit_message_text("⏳ جارٍ وضع SL/TP...")
            except: pass
            await _apply_manual_sl_tp(bsym, context.bot)
            return
        
        if action == "no":
            _pending_manual.pop(bsym, None)
            try:
                await q.edit_message_text(
                    f"✅ <b>{short(bsym)}</b> — مراقبة فقط (بدون أوامر على Binance)\n"
                    f"⚡ البوت يراقب داخلياً", parse_mode="HTML")
            except: pass
            return
        
        if action == "manual":
            # نطلب SL/TP من المستخدم
            _manual_waiting_input[bsym] = {"ticket": pending["ticket"], "ts": _time.time()}
            try:
                await q.edit_message_text(
                    f"✏️ <b>{short(bsym)}</b> — أرسل SL و TP بالشكل:\n"
                    f"<code>{bsym} SL TP</code>\n"
                    f"مثال: <code>{bsym} 1.29 1.32</code>\n"
                    f"⏱️ خلال 5 دقائق.",
                    parse_mode="HTML")
            except: pass
            return
        
        if action == "quick":
            # 🆕 v7.9.52 — وضع الربح السريع
            _pending_manual.pop(bsym, None)
            _quick_profit_targets[bsym] = {"ticket": pending["ticket"], "target_usd": 0.5}
            try:
                await q.edit_message_text(
                    f"💰 <b>{short(bsym)}</b> — وضع الربح السريع (هدف: +0.5 USDT)\n"
                    f"⚡ البوت يراقب الصفقة وسيغلقها فوراً عند تحقيق +$0.5.",
                    parse_mode="HTML")
            except: pass
            return
        
        if action == "mine":
            # v8.0.92 — سأديرها بنفسي
            _pending_manual.pop(bsym, None)
            _n_removed = 0
            for _t in list(_tickets_for(bsym)):
                _remove_ticket(_t["id"])
                _n_removed += 1
            log.info(f"🚫 {bsym}: user manages manually — {_n_removed} tickets removed")
            try:
                await q.edit_message_text(
                    f"🚫 <b>{short(bsym)}</b> — ستديرها بنفسك\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"✅ البوت لن يضع SL/TP\n"
                    f"✅ البوت لن يراقب أو يُغلق\n"
                    f"✅ التذكرة حُذفت ({_n_removed})\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <b>أنت مسؤول عن الإدارة</b>\n"
                    f"💡 ضع SL/TP بنفسك على Binance",
                    parse_mode="HTML")
            except: pass
            return

        return

    if data == "menu:main": return await cmd_menu(update, context)
    if data == "menu:positions": return await cmd_positions(update, context)
    if data == "menu:pnl": return await cmd_pnl(update, context)
    if data == "menu:syncnow": return await cmd_syncnow(update, context)
    if data == "menu:closeall": return await cmd_closeall(update, context)
    if data == "menu:bcheck": return await cmd_bcheck(update, context)

    if data == "menu:cross":
        try: await q.edit_message_text("🔍 فحص...")
        except Exception: pass
        for tf in ["5m", "15m", "1h"]: await _run_cross(update, [tf])
        return
    if data == "menu:sr":
        try: await q.edit_message_text("📊 توليد رسوم...")
        except Exception: pass
        return await _send_sr_charts(update, context)
    if data == "menu:why":
        try: await q.edit_message_text("🔍 فحص شامل...")
        except Exception: pass
        return await _diagnose_all(update)
    if data == "menu:last": return await cmd_last(update, context)
    if data == "menu:pending": return await cmd_pending(update, context)
    if data == "menu:ref6": return await cmd_ref6(update, context)
    if data == "menu:ranges": return await cmd_ranges(update, context)
    if data == "menu:checkranges": return await cmd_checkranges(update, context)
    if data == "menu:stats": return await cmd_stats(update, context)
    if data == "menu:status": return await cmd_status(update, context)
    if data == "menu:strict": return await cmd_strict(update, context)
    if data == "menu:zones": return await cmd_zones(update, context)
    if data == "menu:refresh": return await cmd_refresh(update, context)

    if data.startswith("chart:"):
        coin = data.split(":", 1)[1]
        target = find_symbol_by_name(coin)
        if not target:
            try: return await q.edit_message_text(f"❌ {coin} غير معروفة")
            except Exception: return
        try: await q.edit_message_text(f"📈 جارٍ رسم {coin}...")
        except Exception: pass
        buf = await generate_sr_chart(target)
        if not buf:
            try: return await q.message.reply_text("⚪ no بيانات")
            except Exception: return
        try:
            await q.message.reply_photo(photo=buf,
                caption=f"📈 <b>{short(target)}</b>\n🇸🇾 {syria_now_str()}",
                parse_mode="HTML", reply_markup=back_kb("menu:main"))
            _filter_stats["charts_sent"] += 1
        except Exception as e: log.error(f"chart send: {e}")
        return

    if data.startswith("why:"):
        coin = data.split(":", 1)[1]
        target = find_symbol_by_name(coin)
        if not target:
            try: return await q.edit_message_text(f"❌ {coin} غير معروفة")
            except Exception: return
        return await _diagnose_coin(update, target)


async def _send_sr_charts(update, context):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    chat_id = _get_chat_id(update); ok = 0
    for sym in ALL_SYMBOLS:
        buf = await generate_sr_chart(sym)
        if buf:
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=buf,
                    caption=f"📊 <b>{short(sym)}</b>",
                    parse_mode="HTML", reply_markup=coin_menu_kb(sym))
                ok += 1; _filter_stats["charts_sent"] += 1
            except Exception as e: log.error(f"send_sr {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if ok == 0:
        try: await context.bot.send_message(chat_id=chat_id, text="⚪ no بيانات.")
        except Exception: pass


# ═══════════════════════════════════════════════════════════
# 22) أوامر عامة
# ═══════════════════════════════════════════════════════════
async def cmd_start(update, context):
    sd, sn = get_session_info()
    ranges_txt = "\n".join(
        f"• <b>{short(s)}</b>: {fmt_price(r['lower'])} – {fmt_price(r['upper'])}"
        for s, r in _last_range.items()) or "  (no نطاقات)"
    binance_status = "✅" if _binance.ok else ("❌ " + (_binance.error_msg or ""))
    await _reply(update,
        f"🔀 <b>Unified Smart Bot {__version_full__}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💸 Binance Futures: <b>{binance_status}</b>\n"
        f"• الرافعة: {LEVERAGE}x | Notional: {NOTIONAL_USDT}$\n"
        f"• الهامش: {MARGIN_TYPE}\n"
        f"• trade ثانية: score ≥ {STRONG_SCORE_FOR_ADD}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🔌 {_exchange.primary_name}\n"
        f"🕐 {sn} ({sd:+d})\n\n"
        f"<b>📌 النطاقات ({len(_last_range)}):</b>\n{ranges_txt}\n\n"
        f"👇",
        parse_mode="HTML", reply_markup=main_menu_kb())


async def _run_cross(update, tfs=None):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    tfs = tfs or TIMEFRAMES
    await _reply(update, f"🔍 فحص: {', '.join(tfs)} ...")
    clear_caches()
    detectors = _get_detectors()
    found = filtered = 0
    chat_id = _get_chat_id(update)
    bot = update.get_bot() if update else None
    for sym in ALL_SYMBOLS:
        for tf in tfs:
            for k in [(sym, tf), (sym, tf, "pre"), (sym, tf, "live"),
                      (sym, tf, "div"), (sym, tf, "bb"), (sym, tf, "sr")]:
                _crossover_cache.pop(k, None)
            _divergence_cache.pop((sym, tf, "div"), None)
            _bb_cache.pop((sym, tf, "bb"), None)
            _sr_cache.pop((sym, tf, "sr"), None)
            for det in detectors:
                # 🆕 v6.9.3 — timeout 10s لكل كاشف
                try:
                    cross = await asyncio.wait_for(det(sym, tf), timeout=8.0)  # v8.0.86
                except asyncio.TimeoutError:
                    log.warning(f"detector timeout {det.__name__} {sym} {tf}")
                    continue
                if not cross: continue
                htf = await get_htf_trend(sym)
                passed, _r, _c = await passes_filter(cross, htf)
                if not passed: filtered += 1; continue
                
                # 🆕 v7.9.17 — Paper: silent (save only, no Telegram)
                if cross.get("paper_signal"):
                    _filter_stats["paper_signals"] = _filter_stats.get("paper_signals", 0) + 1
                    if cross.get("levels"):
                        _paper_track_add(cross, cross["levels"])
                    log.debug(f"📄 Paper saved (silent): {cross['symbol']} [{cross.get('source')}]")
                    continue  # no تنفيذ
                
                try:
                    if bot:
                        await bot.send_message(chat_id=chat_id,
                            text=build_signal_message(cross), parse_mode="HTML")
                    mark_symbol_sent(sym)
                    if cross["alert_type"] == "confirmed":
                        record_signal(sym, cross["direction"])
                        register_signal_for_tracking(cross)
                        _src = cross.get("source", "?")
                        _sc = cross.get("final_score", 0)
                        log.info(f"🎬 Executing: {short(sym)} [{_src}] "
                                 f"score={_sc} dir={cross.get('direction','?')}")
                        exe = await execute_binance_trade(cross, bot=bot)
                        if exe.get("ok"):
                            log.info(f"🚀 Opened: {short(sym)} [{_src}]")
                        else:
                            log.info(f"exec skip {sym}: {exe.get('error')}")
                            try:
                                await _notify_exec_skip(
                                    bot, sym, cross, exe.get("error", ""))
                            except Exception: pass
                    found += 1
                except Exception as e: log.error(f"send {sym} {tf}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    msg = f"✅ {found} إشارة."
    if filtered: msg += f"\n🚫 {filtered} محجوبة."
    await _reply(update, msg)


def _get_detectors():
    # 🆕 v9.22.0 — تفعيل Supertrend فقط (باقي الكواشف معطلة)
    return [detect_supertrend_signal]

async def detect_supertrend_signal(symbol, tf):
    """🆕 v9.24.0 — Supertrend مع تأكيد الشمعة الثانية"""
    df = await _df_for(symbol, tf, extra=40)
    if df is None or len(df) < 25: return None
    
    try:
        # نحسب اتجاه Supertrend للشمعة الحالية
        st_curr = calc_supertrend(df)
        # نحسب اتجاه الشمعة السابقة
        st_prev = calc_supertrend(df.iloc[:-1])
        # نحسب اتجاه الشمعة قبل السابقة (لتأكيد الانعكاس)
        st_prev2 = calc_supertrend(df.iloc[:-2])
        
        d = None
        # انعكاس صعودي: كان هابطاً (-1)، وأصبح صاعداً (1)
        if st_curr["trend"] == 1 and st_prev["trend"] == -1 and st_prev2["trend"] == -1:
            d = "bullish"
        # انعكاس هبوطي: كان صاعداً (1)، وأصبح هابطاً (-1)
        elif st_curr["trend"] == -1 and st_prev["trend"] == 1 and st_prev2["trend"] == 1:
            d = "bearish"
            
        if not d: return None
        
        # 🆕 v9.26.0 — تأكيد الشمعة الثانية (نصف الشمعة): نفحص الشمعة الحالية (-1)
        c_now = float(df["c"].iloc[-1])
        o_now = float(df["o"].iloc[-1])
        
        if d == "bullish" and c_now <= o_now: return None
        if d == "bearish" and c_now >= o_now: return None
        
        # 🆕 v9.27.0 — cooldown 5 ثوانٍ فقط
        key = (symbol, tf, "supertrend")
        last = _crossover_cache.get(key)
        if last and (_time.time() - last.get("ts", 0)) < 5:
            return None
        _crossover_cache[key] = {"ts": _time.time()}
        
        df["ef"] = calc_ema(df["c"], EMA_FAST)
        df["es"] = calc_ema(df["c"], EMA_SLOW)
        # 🆕 v9.32.0 — فلتر ADX > 20 (لتجنب السوق العرضي)
        try:
            _adx = float(calc_adx(df).iloc[-1])
            if _adx <= 20:
                log.debug(f"supertrend {symbol}: ADX {_adx:.1f} <= 20 (range) — reject")
                return None
        except Exception:
            pass
            
        cross = await _make_cross(symbol, tf, df, d, "live", "📈 تغير Supertrend", -2, source="supertrend")
        return cross
    except Exception:
        return None


async def cmd_cross(u, c):   await _run_cross(u)
async def cmd_cross5(u, c):  await _run_cross(u, ["5m"])
async def cmd_cross15(u, c): await _run_cross(u, ["15m"])
async def cmd_cross1h(u, c): await _run_cross(u, ["1h"])


async def _get_diagnosis(symbol, tf):
    try:
        df = await _df_for(symbol, tf)
        if df is None: return None
        df["ef"] = calc_ema(df["c"], EMA_FAST)
        df["es"] = calc_ema(df["c"], EMA_SLOW)
        curr = -2
        cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
        gap = abs(cf - cs) / cs * 100 if cs else 0
        atr = float(calc_atr(df).iloc[curr])
        price = float(df["c"].iloc[curr])
        atr_pct = (atr / price) * 100 if price else 0
        adx = float(calc_adx(df).iloc[curr])
        rsi = float(calc_rsi(df["c"]).iloc[curr])
        try: vol_ratio = float(df["v"].iloc[curr]) / float(df["v"].iloc[curr-20:curr].mean())
        except: vol_ratio = 0
        th = get_thresholds(atr_pct=atr_pct, adx=adx, tf=tf)
        sb = compute_score(df, "bullish", curr)["score"]
        sr = compute_score(df, "bearish", curr)["score"]
        best = max(sb, sr)
        min_gap = MIN_EMA_GAP_PCT.get(tf, 0.02)
        min_atr = MIN_ATR_PCT.get(tf, 0.05)
        checks = {"EMA gap": gap >= min_gap, "ATR": atr_pct >= min_atr,
                  "ADX": adx >= th["min_adx"],
                  "Volume": vol_ratio >= th["min_vol_ratio"],
                  "Score مطلق": best >= ABS_MIN_SCORE,
                  "Score ديناميكي": best >= th["min_score"]}
        values = {"EMA gap": f"{gap:.3f}%", "ATR": f"{atr_pct:.3f}%",
                  "ADX": f"{adx:.1f}", "Volume": f"{vol_ratio:.2f}×",
                  "Score مطلق": f"{best}", "Score ديناميكي": f"{best}"}
        thresholds = {"EMA gap": f"{min_gap}%", "ATR": f"{min_atr}%",
                      "ADX": f"{th['min_adx']:.1f}",
                      "Volume": f"{th['min_vol_ratio']}×",
                      "Score مطلق": f"{ABS_MIN_SCORE}",
                      "Score ديناميكي": f"{th['min_score']:.0f}"}
        return {"symbol": symbol, "tf": tf, "score": best, "checks": checks,
                "values": values, "thresholds": thresholds,
                "price": price, "rsi": rsi}
    except Exception as e:
        log.warning(f"_get_diagnosis {symbol} {tf}: {e}"); return None


async def _diagnose_coin(update, target):
    lines = [f"📊 <b>تشخيص {short(target)}</b>", "━━━━━━━━━━━━━━━━━━━"]
    for tf in ["5m", "15m", "1h"]:
        r = await _get_diagnosis(target, tf)
        if r is None: lines.append(f"\n<b>[{tf}]</b> ❌ no بيانات"); continue
        lines.append(f"\n<b>[{tf}]</b>  💰 {fmt_price(r['price'])}")
        for k in r["checks"]:
            ok = "✅" if r["checks"][k] else "❌"
            lines.append(f"{ok} {k}: <b>{r['values'][k]}</b> / {r['thresholds'][k]}")
        lines.append(f"📉 RSI: {r['rsi']:.1f}")
    ok_cd, rem = check_symbol_cooldown(target)
    lines.append(f"\n⏳ Cooldown: {'✅' if ok_cd else f'❌ {rem}s'}")
    await _reply(update, "\n".join(lines), parse_mode="HTML",
                 reply_markup=coin_menu_kb(target))


async def _diagnose_all(update):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    await _reply(update, f"🔍 فحص {len(ALL_SYMBOLS)} عملة ...")
    results = []
    for sym in ALL_SYMBOLS:
        r = await _get_diagnosis(sym, "15m")
        if r: results.append(r)
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if not results:
        await _reply(update, "⚪ no بيانات."); return
    results.sort(key=lambda r: (sum(1 for v in r["checks"].values() if not v), -r["score"]))
    sd, sn = get_session_info()
    lines = [f"📊 <b>تشخيص شامل ({len(results)})</b>", f"🕐 {sn}",
             "━━━━━━━━━━━━━━━━━━━"]
    for r in results:
        fails = [k for k, v in r["checks"].items() if not v]
        icon = "🌟" if not fails else ("⚠️" if len(fails)==1 else "❌" if len(fails)==2 else "🚫")
        lines.append(f"\n{icon} <b>{short(r['symbol'])}</b>  Score={r['score']}  "
                     f"💰{fmt_price(r['price'])}")
        if not fails: lines.append("   ✅ كل الفلاتر مرّت")
        else:
            for k in fails:
                lines.append(f"   ❌ {k}: {r['values'][k]} / {r['thresholds'][k]}")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_why(update, context):
    args = context.args or []
    if args:
        target = find_symbol_by_name(args[0])
        if not target:
            await _reply(update, f"❌ {args[0]} غير معروف.", parse_mode="HTML"); return
        await _diagnose_coin(update, target)
    else:
        await _diagnose_all(update)


async def cmd_last(update, context):
    if not _last_signals:
        await _reply(update, "📭 no إشارات بعد."); return
    lines = ["📋 <b>آخر الإشارات</b>", "━━━━━━━━━━━━━━━━━━━"]
    for s in reversed(list(_last_signals)):
        icon = {"win_tp2": "✅", "win_tp1": "🎯",
                "loss_sl": "❌", "expired": "⏰", None: "⏳"}.get(s["result"], "?")
        ts_str = datetime.fromtimestamp(s["ts"], tz=SYRIA_TZ).strftime("%H:%M")
        dir_icon = "🟢" if s["direction"] == "bullish" else "🔴"
        lines.append(f"\n{icon} <b>{short(s['symbol'])}</b> [{s['timeframe']}] "
                     f"{dir_icon} {_source_label(s['source'])}")
        lines.append(f"   🕐 {ts_str} | Score={s['score']}")
        lines.append(f"   💰 {fmt_price(s['entry'])} → {fmt_price(s['exit'])} "
                     f"<b>{s['pnl_pct']:+.2f}%</b>")
        lines.append(f"   ⏱️ {s['duration_min']} د | <b>{s['result']}</b>")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_pending(update, context):
    active = {sid: s for sid, s in _pending_signals.items() if not s["checked"]}
    if not active:
        await _reply(update, "📭 no إشارات نشطة."); return
    lines = [f"⏳ <b>إشارات نشطة ({len(active)})</b>", "━━━━━━━━━━━━━━━━━━━"]
    now = _time.time()
    for sid, sig in active.items():
        cross = sig["cross"]; lv = sig["levels"]
        bull = sig["direction"] == "bullish"
        try:
            data, _ = await _exchange.fetch_ohlcv(cross["symbol"], "1m", 2)
            price = float(data[-1][4]) if data else lv["entry"]
        except Exception: price = lv["entry"]
        pnl = ((price - lv["entry"]) / lv["entry"] * 100) * (1 if bull else -1)
        age = int((now - sig["entry_ts"]) / 60)
        lines.append(f"\n{'🟢' if bull else '🔴'} <b>{short(cross['symbol'])}</b> "
                     f"[{cross['timeframe']}]")
        lines.append(f"   💰 {fmt_price(lv['entry'])} → {fmt_price(price)}")
        lines.append(f"   <b>{pnl:+.2f}%</b> | ⏱️ {age} د")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_checkprice(update, context):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    await _reply(update, "🔍 ...")
    found = 0
    for sym in ALL_SYMBOLS:
        a = await detect_sudden_change(sym)
        if a:
            await _reply(update, build_price_alert_msg(a), parse_mode="HTML")
            found += 1
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if found == 0: await _reply(update, "⚪ no تغيرات.")


async def cmd_ranges(update, context):
    await _reply(update, build_ranges_report(), parse_mode="HTML")


async def cmd_refresh(update, context):
    # 🆕 v7.9.30 — ranges removed
    await _reply(update, "ℹ️ النطاقات معطلة (v7.9.30)")


async def cmd_ref6(update, context):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    await _reply(update, "🔍 حساب 6% ...")
    results = await compute_6pct_ranges()
    if not results:
        await _reply(update, "⚪ no بيانات."); return
    await _reply(update, build_6pct_report(results), parse_mode="HTML")


async def cmd_sr(update, context):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    await _reply(update, f"📊 {len(ALL_SYMBOLS)} رسم ...")
    chat_id = _get_chat_id(update); ok = 0
    for sym in ALL_SYMBOLS:
        buf = await generate_sr_chart(sym)
        if buf:
            try:
                await context.bot.send_photo(chat_id=chat_id, photo=buf,
                    caption=f"📊 <b>{short(sym)}</b>", parse_mode="HTML",
                    reply_markup=coin_menu_kb(sym))
                ok += 1; _filter_stats["charts_sent"] += 1
            except Exception as e: log.error(f"sr {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if ok == 0:
        try: await context.bot.send_message(chat_id=chat_id, text="⚪ no بيانات.")
        except Exception: pass


async def cmd_checkranges(update, context):
    if not _exchange.ok():
        await _reply(update, "❌ no منصة."); return
    await _reply(update, "🔍 ...")
    alerts = 0
    for symbol in list(_last_range.keys()):
        b = await check_range_breakout(symbol)
        if b:
            await _reply(update, build_breakout_msg(b), parse_mode="HTML")
            _filter_stats["range_breakouts"] += 1; alerts += 1; continue
        a = await check_range_approach(symbol)
        if a:
            await _reply(update, build_approach_msg(a), parse_mode="HTML")
            _filter_stats["range_approaches"] += 1; alerts += 1
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if alerts == 0:
        await _reply(update, "✅ كل الأسعار داخل النطاقات.")


async def cmd_status(update, context):
    total = sum(v for k, v in _filter_stats.items() if k.startswith("filtered_"))
    jq = context.application.job_queue
    jobs_n = len(jq.jobs()) if jq else 0
    sd, sn = get_session_info()
    st = _smart.stats()
    active = len([s for s in _pending_signals.values() if not s["checked"]])
    with _ticket_lock:
        open_tickets = len(TRADE_TICKETS)
    await _reply(update,
        f"🤖 <b>الحالة {__version_full__}</b>\n"
        f"💸 Binance: {'✅' if _binance.ok else ('❌ ' + (_binance.error_msg or ''))}\n"
        f"🔌 {_exchange.primary_name} | 🕐 {sn}\n"
        f"⚙️ Jobs: <b>{jobs_n}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💼 trades مفتوحة: <b>{open_tickets}</b>\n"
        f"💸 فُتحت: {_filter_stats['trades_opened']} | "
        f"أُغلقت: {_filter_stats['trades_closed']}\n"
        f"🎯 TP: {_filter_stats['trades_tp']} | "
        f"🛑 SL: {_filter_stats['trades_sl']}\n"
        f"❌ فشل: {_filter_stats['trades_failed']}\n"
        f"🔍 يدوي: {_filter_stats['trades_manual_sync']}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 إشارات: {_filter_stats['sent']}\n"
        f"🚫 محجوبة: {total}\n"
        f"📈 WR: <b>{st['wr']}%</b> "
        f"(✅{st['wins']} ❌{st['losses']} ⏰{st['expired']})",
        parse_mode="HTML")


async def cmd_stats(update, context):
    st = _smart.stats()
    if st["n"] == 0:
        await _reply(update, "📊 no نتائج بعد."); return
    bar = "█" * int(st["wr"] / 5) + "░" * (20 - int(st["wr"] / 5))
    await _reply(update,
        f"📊 <b>الأداء</b>\n📈 WR: <b>{st['wr']}%</b>\n"
        f"<code>{bar}</code>\n"
        f"✅ {st['wins']} | ❌ {st['losses']} | ⏰ {st['expired']}\n"
        f"📝 {st['n']} إشارة", parse_mode="HTML")


async def cmd_perf(update, context):
    rep = _smart.source_report()
    if not rep:
        await _reply(update, "📊 no بيانات."); return
    lines = ["📊 <b>أداء المصادر</b>\n━━━━━━━━━━━━━━━━━━━"]
    for src, d in rep.items():
        tot = d["wins"] + d["losses"]
        wr = round(d["wins"] / tot * 100, 1) if tot else 0
        lines.append(f"\n🔹 <b>{_source_label(src) if src != 'unknown' else '—'}</b>\n"
                     f"   📤 {d['sent']} | 🚫 {d['filtered']}\n"
                     f"   ✅ {d['wins']} | ❌ {d['losses']}\n"
                     f"   📈 WR: <b>{wr}%</b>")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_jobs(update, context):
    jq = context.application.job_queue
    if jq is None:
        await _reply(update, "❌ غير متوفر."); return
    jobs = jq.jobs()
    if not jobs:
        await _reply(update, "⚠️ no مهام."); return
    lines = [f"⚙️ <b>Jobs ({len(jobs)}):</b>\n"]
    for j in jobs:
        nxt = getattr(j, "next_t", None)
        nxt_str = nxt.strftime("%H:%M:%S") if nxt else "—"
        lines.append(f"• <b>{j.name or 'unnamed'}</b> → {nxt_str}")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_strict(update, context):
    th = get_thresholds(tf="15m")
    sd, sn = get_session_info()
    await _reply(update,
        f"🎯 <b>التشدد {__version_full__}</b>\n🕐 {sn}\n━━━━━━━━━━━━━━━━━━━\n"
        f"<b>ديناميكي (15m):</b>\n"
        f"• Score ≥ {th['min_score']:.0f}\n"
        f"• Vol ≥ {th['min_vol_ratio']}×\n"
        f"• ADX ≥ {th['min_adx']}\n"
        f"• ATR ≤ {th['max_atr_pct']}%\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🛡️ ثوابت:</b>\n"
        f"• Score مطلق ≥ {ABS_MIN_SCORE}\n"
        f"• cooldown: {SYMBOL_COOLDOWN_SEC}s\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"<b>💸 Binance:</b>\n"
        f"• Notional: {NOTIONAL_USDT}$\n"
        f"• الرافعة: {LEVERAGE}x\n"
        f"• الهامش: {MARGIN_TYPE}\n"
        f"• trade ثانية: score ≥ {STRONG_SCORE_FOR_ADD}\n"
        f"• فحص كل: {POSITION_MONITOR_SEC}s",
        parse_mode="HTML")


async def cmd_clearcache(update, context):
    clear_caches(); _smart.reset_feedback(); _last_signal_time.clear()
    await _reply(update, "✅ تم تفريغ الكاش.")



async def cmd_zones(update, context):
    """🎯 عرض مناطق S/R لكل رمز"""
    if not SMART_ZONES_ENABLED:
        await _reply(update, "⚠️ Smart Zones معطّلة"); return
    await _reply(update, f"🔍 فحص {len(ALL_SYMBOLS)} رمز...")
    lines = ["🎯 <b>Smart S/R Zones</b>", "━━━━━━━━━━━━━━━━━━━"]
    for sym in ALL_SYMBOLS:
        try:
            d4 = await fetch_ohlcv_cached(sym, "4h", 300)
            d1 = await fetch_ohlcv_cached(sym, "1h", 300)
            d15 = await fetch_ohlcv_cached(sym, "15m", 200)
            if not d4 or not d1 or not d15:
                lines.append(f"\n❌ <b>{short(sym)}</b>: no بيانات")
                continue
            df4 = pd.DataFrame(d4, columns=["ts","o","h","l","c","v"])
            df1 = pd.DataFrame(d1, columns=["ts","o","h","l","c","v"])
            df15 = pd.DataFrame(d15, columns=["ts","o","h","l","c","v"])
            combined, h4, h1 = sz_combined_direction(df4, df1)
            sw = sz_find_swings(df1)
            price = float(df15["c"].iloc[-2])
            atr = float(calc_atr(df15).iloc[-2])
            highs_z = sz_cluster(sw["highs"], atr=atr)
            lows_z = sz_cluster(sw["lows"], atr=atr)
            candle = sz_detect_reversal(df15)
            lines.append(f"\n🔹 <b>{short(sym)}</b> | 4H={h4} 1H={h1} → <b>{combined}</b>")
            lines.append(f"   💰 {fmt_price(price)}")
            if highs_z:
                z = highs_z[0]
                dist = (z["low"] - price) / price * 100
                lines.append(f"   📈 [{fmt_price(z['low'])}—{fmt_price(z['high'])}] s={z['strength']} | {dist:+.2f}%")
            if lows_z:
                z = lows_z[0]
                dist = (price - z["high"]) / price * 100
                lines.append(f"   📉 [{fmt_price(z['low'])}—{fmt_price(z['high'])}] s={z['strength']} | {dist:+.2f}%")
            if candle:
                lines.append(f"   🕯️ {candle['type']} ({candle['dir']})")
            for direction in ["bullish", "bearish"]:
                if direction == "bullish" and combined == "DOWN": continue
                if direction == "bearish" and combined == "UP": continue
                setup = sz_find_setup(price, atr, highs_z, lows_z, direction,
                              symbol=sym, htf_combined=combined)
                if setup.get("ok"):
                    icon = "🟢" if direction == "bullish" else "🔴"
                    lines.append(f"   {icon} <b>Setup {direction}</b>:")
                    lines.append(f"      SL={fmt_price(setup['sl'])} ({setup['sl_pct']}% | {setup['sl_atr']} ATR)")
                    lines.append(f"      TP1={fmt_price(setup['tp1'])} (R:R={setup['rr1']})")
                    lines.append(f"      TP2={fmt_price(setup['tp2'])} (R:R={setup['rr2']})")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
        except Exception as e:
            lines.append(f"\n❌ <b>{short(sym)}</b>: {e}")
    await _reply(update, "\n".join(lines), parse_mode="HTML")



async def cmd_pause(update, context):
    """⏸️ إيقاف البوت"""
    try:
        with open(PAUSE_FILE, "w") as f:
            f.write(str(_time.time()))
        await _reply(update,
            "⏸️ <b>البوت موقوف</b>\n"
            "• no فتح trades جديدة\n"
            "• الtrades الحالية تُدار (SL/TP)\n"
            "• الاستئناف: /resume\n"
            "⚠️ يبقى بعد restart",
            parse_mode="HTML")
    except Exception as e:
        await _reply(update, f"❌ {e}")


async def cmd_resume(update, context):
    """▶️ استئناف البوت"""
    try:
        if os.path.exists(PAUSE_FILE):
            os.remove(PAUSE_FILE)
            msg = "▶️ <b>تم الاستئناف</b>"
        else:
            msg = "ℹ️ البوت يعمل أصلاً"
        await _reply(update,
            msg + "\n• الإيقاف: /pause",
            parse_mode="HTML")
    except Exception as e:
        await _reply(update, f"❌ {e}")





# v7.0.0 Paper SQLite
import sqlite3

def _paper_db_init():
    try:
        conn = sqlite3.connect(PAPER_DB)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS paper (
                id TEXT PRIMARY KEY,
                symbol TEXT, direction TEXT, source TEXT,
                score INTEGER, entry REAL, sl REAL,
                tp1 REAL, tp2 REAL, tf TEXT,
                ts REAL, result TEXT, closed_at REAL, pnl_pct REAL
            )
        """)
        # 🆕 v7.9.51 — إضافة أعمدة Paper الجديدة (slippage, fee, tags)
        for _col, _typ in [
            ("paper_reason", "TEXT"), ("paper_type", "TEXT"),
            ("slippage_pct", "REAL"), ("fee_pct", "REAL"), ("tags", "TEXT")
        ]:
            try:
                conn.execute(f"ALTER TABLE paper ADD COLUMN {_col} {_typ}")
                log.info(f"✅ paper: added column {_col}")
            except sqlite3.OperationalError:
                pass  # already exists
        conn.commit(); conn.close()
        log.info(f"Paper SQLite ready: {PAPER_DB}")
    except Exception as e:
        log.error(f"_paper_db_init: {e}")

def _paper_save(sig):
    try:
        conn = sqlite3.connect(PAPER_DB)
        # 🆕 v7.9.46 — أعمدة صريحة + الحقلان الجديدان
        conn.execute(
            "INSERT OR REPLACE INTO paper "
            "(id, symbol, direction, source, score, entry, sl, tp1, tp2, tf, "
            " ts, result, closed_at, pnl_pct, paper_reason, paper_type, "
            " slippage_pct, fee_pct, tags) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sig.get("id"), sig.get("symbol"), sig.get("direction"),
             sig.get("source"), int(sig.get("score") or 0),
             sig.get("entry"), sig.get("sl"), sig.get("tp1"),
             sig.get("tp2"), sig.get("tf"), sig.get("ts"),
             sig.get("result"), sig.get("closed_at"), sig.get("pnl_pct"),
             sig.get("paper_reason"), sig.get("paper_type"),
             sig.get("slippage_pct", 0.15), sig.get("fee_pct", 0.08), sig.get("tags")))
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f"_paper_save: {e}")

def _paper_load_all():
    try:
        conn = sqlite3.connect(PAPER_DB)
        # 🆕 v7.9.46 — أعمدة صريحة (تتضمن الجديدة)
        keys = ["id","symbol","direction","source","score","entry","sl",
                "tp1","tp2","tf","ts","result","closed_at","pnl_pct",
                "paper_reason","paper_type","slippage_pct","fee_pct","tags"]
        cols = ",".join(keys)
        cur = conn.execute(f"SELECT {cols} FROM paper ORDER BY ts DESC")
        rows = cur.fetchall()
        conn.close()
        return [dict(zip(keys, r)) for r in rows]
    except Exception as e:
        log.warning(f"_paper_load_all: {e}")
        return []

_paper_db_init()


def _paper_track_add(cross, levels):
    try:
        if not levels:
            levels = {
                "entry": cross.get("price", 0),
                "stop_loss": 0, "tp1": 0, "tp2": 0
            }
        sid = f"{short(cross['symbol'])}_{int(_time.time())}"
        sig = {
            "id": sid, "symbol": cross["symbol"],
            "direction": cross["direction"],
            "source": cross.get("source", "?"),
            "score": cross.get("final_score", 0),
            "entry": levels["entry"], "sl": levels["stop_loss"],
            "tp1": levels["tp1"], "tp2": levels.get("tp2"),
            "tf": cross["timeframe"], "ts": _time.time(),
            "result": None, "closed_at": None, "pnl_pct": None,
            # 🆕 v7.9.46
            "paper_reason": cross.get("paper_reason"),
            "paper_type": cross.get("paper_type"),
            # 🆕 v7.9.51 — Slippage + Fee
            "slippage_pct": 0.15,  # افتراضي 0.15%
            "fee_pct": 0.08,       # افتراضي 0.08%
            "tags": f"TF_{cross['timeframe']}|src_{cross.get('source', '?')}|sym_{short(cross['symbol'])}",
        }
        with _paper_lock:
            _paper_signals[sid] = sig
        _paper_save(sig)
    except Exception as e:
        log.warning(f"paper_add: {e}")



async def _paper_track_eval():
    try:
        with _paper_lock:
            active = [s for s in _paper_signals.values()
                      if s.get("result") is None]
        if not active:
            return
        for s in active:
            age_min = (_time.time() - s["ts"]) / 60
            if age_min > PAPER_EXPIRY_MIN:
                s["result"] = "expired"
                s["closed_at"] = _time.time()
                _paper_save(s)
                continue
            try:
                data, _ = await _exchange.fetch_ohlcv(s["symbol"], "1m", 3)
                if not data:
                    continue
                price = float(data[-1][4])
            except Exception:
                continue
            # 🆕 v8.0.22 — تطبيق الانزلاق + العمولة على النتيجة
            _slippage = s.get("slippage_pct", 0.15) / 100
            _fee = s.get("fee_pct", 0.08) / 100
            _total_fees_pct = _fee * 2 * 100  # entry + exit as %
            if s["direction"] == "bullish":
                _entry_adj = s["entry"] * (1 + _slippage)
                _mult = 1
            else:
                _entry_adj = s["entry"] * (1 - _slippage)
                _mult = -1

            res = None
            if s["direction"] == "bullish":
                if price <= s["sl"]:
                    res = "loss"
                elif s.get("tp2") and price >= s["tp2"]:
                    res = "win_tp2"
                elif price >= s["tp1"]:
                    res = "win_tp1"
            else:
                if price >= s["sl"]:
                    res = "loss"
                elif s.get("tp2") and price <= s["tp2"]:
                    res = "win_tp2"
                elif price <= s["tp1"]:
                    res = "win_tp1"
            if res:
                s["result"] = res
                s["closed_at"] = _time.time()
                s["pnl_pct"] = round(((price - _entry_adj) / _entry_adj * 100 * _mult) - _total_fees_pct, 3)
                _paper_save(s)
    except Exception as e:
        log.warning(f"paper_eval: {e}")



def _tg_escape(text):
    """🆕 v7.9.47 — Escape HTML entities for Telegram"""
    if text is None:
        return ""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


async def paper_digest_job(context):
    """
    🆕 v7.9.46 — كل 3 ساعات: تقرير الإشارات المحجوبة.
    الهدف: كشف فلتر يحجب نجاحاً.
    """
    if not PAPER_MODE_ENABLED or not CHAT_ID:
        return
    try:
        import sqlite3 as _sq, time as _t
        _since = _t.time() - PAPER_DIGEST_INTERVAL_SEC
        conn = _sq.connect(PAPER_DB)
        cur = conn.execute(
            "SELECT symbol, direction, source, score, paper_reason, "
            "       paper_type, result, pnl_pct FROM paper "
            "WHERE ts >= ? ORDER BY ts DESC", (_since,))
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=(f"📄 <b>Paper Digest</b>\n"
                      f"⏱️ آخر 3 ساعات\n"
                      f"📭 لا إشارات محجوبة."),
                parse_mode="HTML")
            return

        total = len(rows)
        closed = [r for r in rows if r[6]]
        wins = [r for r in closed if r[6] and r[6].startswith("win")]
        losses = [r for r in closed if r[6] == "loss"]
        expired = [r for r in closed if r[6] == "expired"]
        pending = total - len(closed)

        from collections import Counter
        by_reason = Counter()
        win_by_reason = Counter()
        for r in rows:
            _rsn = (r[4] or "unknown").split(":")[0].strip()
            by_reason[_rsn] += 1
            if r[6] and r[6].startswith("win"):
                win_by_reason[_rsn] += 1

        top5 = sorted(
            [r for r in wins if r[3] is not None],
            key=lambda r: -int(r[3] or 0))[:5]

        _m = (f"📄 <b>Paper Digest</b> — آخر 3 ساعات\n"
              f"🇸🇾 {syria_now_str()}\n"
              f"━━━━━━━━━━━━━━━━━━━\n"
              f"📊 إجمالي: <b>{total}</b> إشارة محجوبة\n"
              f"✅ نجحت: <b>{len(wins)}</b> | "
              f"❌ فشلت: <b>{len(losses)}</b> | "
              f"⏳ لم تُحسم: <b>{pending}</b> | "
              f"⌛ انتهت: <b>{len(expired)}</b>\n"
              f"━━━━━━━━━━━━━━━━━━━\n"
              f"🎯 <b>حسب سبب الحجب:</b>\n")

        _sorted_reasons = sorted(
            by_reason.items(),
            key=lambda kv: (win_by_reason.get(kv[0], 0) / max(1, kv[1])),
            reverse=True)
        for rsn, cnt in _sorted_reasons[:10]:
            _w = win_by_reason.get(rsn, 0)
            _wr = _w / cnt * 100 if cnt else 0
            if _w >= 2:
                _icon = "⚠️"
            elif _w == 0 and cnt >= 3:
                _icon = "✅"
            else:
                _icon = "·"
            _rsn_esc = _tg_escape(rsn[:22])
            _m += (f"{_icon} <code>{_rsn_esc:22s}</code> "
                   f"{cnt:3d} → ✅{_w} ({_wr:.0f}%)\n")

        if top5:
            _m += f"\n━━━━━━━━━━━━━━━━━━━\n"
            _m += f"🔥 <b>أقوى إشارات نجحت:</b>\n"
            for r in top5:
                _sym = r[0] or "?"
                _dir = r[1] or "?"
                _src = r[2] or "?"
                _score = int(r[3] or 0)
                _res = r[6]
                _pnl = float(r[7] or 0)
                _dir_icon = "🟢" if _dir == "bullish" else "🔴"
                _m += (f"{_dir_icon} {_tg_escape(short(_sym))} "
                       f"<b>{_score}</b> [{_tg_escape(_src)}] "
                       f"— {_tg_escape(_res)} ({_pnl:+.2f}%)\n")

        _m += (f"\n💡 <i>⚠️ = فلتر يحجب نجاحاً — يستحق مراجعة</i>\n"
               f"💡 <i>✅ = فلتر يحجب فشلاً — يعمل جيداً</i>")

        # 🆕 v7.9.51 — أفضل التركيبات (Combinations)
        from collections import defaultdict
        _combos = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
        for r in rows:
            _tag = r[4] or "unknown"
            _combos[_tag]["total"] += 1
            if r[6] and r[6].startswith("win"):
                _combos[_tag]["wins"] += 1
                _combos[_tag]["pnl"] += float(r[7] or 0)

        _best_combos = sorted(
            [(k, v) for k, v in _combos.items() if v["total"] >= 2],
            key=lambda x: (x[1]["wins"] / x[1]["total"]),
            reverse=True
        )[:3]
        
        if _best_combos:
            _m += f"\n\n━━━━━━━━━━━━━━━━━━━\n🏆 <b>أفضل التركيبات (Combinations):</b>\n"
            for combo, stats in _best_combos:
                _wr = stats["wins"] / stats["total"] * 100
                _m += f"· <code>{combo}</code> → ✅ {stats['wins']}/{stats['total']} ({_wr:.0f}%)\n"

        await context.bot.send_message(
            chat_id=CHAT_ID, text=_m, parse_mode="HTML")
    except Exception as e:
        log.warning(f"paper_digest_job: {e}")


async def cmd_paper(update, context):
    try:
        data = _paper_load_all()
        if not data:
            await _reply(update, "no Paper signals")
            return
        total = len(data)
        closed = [s for s in data if s.get("result")]
        wins = sum(1 for s in closed if s["result"].startswith("win"))
        losses = sum(1 for s in closed if s["result"] == "loss")
        expired = sum(1 for s in closed if s["result"] == "expired")
        d = wins + losses
        wr = round(wins / d * 100, 1) if d > 0 else 0
        lines = ["Paper Mode", "---",
                 f"total: {total} | closed: {len(closed)}",
                 f"W: {wins} | L: {losses} | exp: {expired}",
                 f"WR: {wr}%", ""]
        recent = sorted(closed, key=lambda x: x.get("closed_at") or 0,
                        reverse=True)[:20]
        for s in recent:
            icon = {"win_tp1": "[TP1]", "win_tp2": "[TP2]",
                    "loss": "[LOSS]", "expired": "[EXP]"}.get(s["result"], "[?]")
            pnl = s.get("pnl_pct") or 0
            src = s.get("source", "?")
            rsn = (s.get("paper_reason") or "—")[:40]
            lines.append(f"{icon} {short(s['symbol'])} [{s['tf']}] "
                         f"{src} -> {pnl:+.2f}% | {rsn}")
        await _reply(update, chr(10).join(lines))
    except Exception as e:
        await _reply(update, f"Error: {e}")





async def cmd_trade_stats(update, context):
    """v7.2.0: analyze logged trades per symbol."""
    try:
        import json as _json
        if not os.path.exists(TRADE_STATS_FILE):
            await _reply(update, "no trades logged yet")
            return
        rows = []
        with io.open(TRADE_STATS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(_json.loads(line))
                except Exception:
                    continue
        if not rows:
            await _reply(update, "no trades logged yet")
            return

        # aggregate per symbol
        from collections import defaultdict
        by_sym = defaultdict(list)
        for r in rows:
            by_sym[r.get("symbol", "?")].append(r)

        lines = [f"📊 <b>Trade Stats</b>  ({len(rows)} trades)",
                 "━━━━━━━━━━━━━━━━━━━"]
        for sym, items in sorted(by_sym.items()):
            wins = sum(1 for x in items if x.get("pnl_pct", 0) > 0)
            losses = sum(1 for x in items if x.get("pnl_pct", 0) <= 0)
            wr = round(wins / len(items) * 100, 1) if items else 0
            avg_pnl = sum(x.get("pnl_pct", 0) for x in items) / len(items)
            avg_tp1_d = sum(x.get("tp1_dist_pct", 0) for x in items) / len(items)
            avg_mfe = [x.get("mfe_pct") for x in items if x.get("mfe_pct")]
            avg_mfe_v = sum(avg_mfe) / len(avg_mfe) if avg_mfe else 0
            hit_tp1 = sum(1 for x in items if x.get("reason", "").startswith("TP"))
            hit_tp1_pct = round(hit_tp1 / len(items) * 100, 1) if items else 0
            by_src = defaultdict(int)
            for x in items:
                by_src[x.get("tp1_source", "?")] += 1
            lines.append(f"\n<b>{short(sym)}</b> ({len(items)})")
            lines.append(f"  WR: <b>{wr}%</b> | avg PnL: {avg_pnl:+.2f}%")
            lines.append(f"  TP1 hit: <b>{hit_tp1_pct}%</b>")
            lines.append(f"  avg TP1 dist: {avg_tp1_d:.2f}%")
            lines.append(f"  avg MFE: {avg_mfe_v:.2f}%")
            lines.append(f"  tp1 source: {dict(by_src)}")
        await _reply(update, "\n".join(lines), parse_mode="HTML")
    except Exception as e:
        await _reply(update, f"❌ {e}")




async def cmd_trade_stats(update, context):
    """v7.2.0: analyze logged trades."""
    try:
        import json as _json
        from collections import defaultdict
        if not os.path.exists(TRADE_STATS_FILE):
            await _reply(update, "no trades logged yet")
            return
        rows = []
        with io.open(TRADE_STATS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(_json.loads(line))
                except Exception:
                    continue
        if not rows:
            await _reply(update, "no trades logged yet")
            return

        by_sym = defaultdict(list)
        for r in rows:
            by_sym[r.get("symbol", "?")].append(r)

        lines = [f"📊 <b>Trade Stats</b>  ({len(rows)} trades)",
                 "━━━━━━━━━━━━━━━━━━━"]
        for sym, items in sorted(by_sym.items()):
            wins = sum(1 for x in items if x.get("pnl_pct", 0) > 0)
            losses = sum(1 for x in items if x.get("pnl_pct", 0) <= 0)
            wr = round(wins / len(items) * 100, 1) if items else 0
            avg_pnl = sum(x.get("pnl_pct", 0) for x in items) / len(items)
            avg_tp1_d = sum(x.get("tp1_dist_pct", 0) for x in items) / len(items)
            mfes = [x.get("mfe_pct") for x in items if x.get("mfe_pct") is not None]
            avg_mfe = sum(mfes) / len(mfes) if mfes else 0
            hit_tp1 = sum(1 for x in items if x.get("reason", "").startswith("TP"))
            hit_tp1_pct = round(hit_tp1 / len(items) * 100, 1) if items else 0
            by_src = defaultdict(int)
            for x in items:
                by_src[x.get("tp1_source", "?")] += 1
            lines.append(f"\n<b>{short(sym)}</b> ({len(items)})")
            lines.append(f"  WR: <b>{wr}%</b> | avg PnL: {avg_pnl:+.2f}%")
            lines.append(f"  TP hit: <b>{hit_tp1_pct}%</b>")
            lines.append(f"  avg TP1 dist: {avg_tp1_d:.2f}%")
            lines.append(f"  avg MFE: {avg_mfe:.2f}%")
            lines.append(f"  src: {dict(by_src)}")
        await _reply(update, "\n".join(lines), parse_mode="HTML")
    except Exception as e:
        await _reply(update, f"❌ {e}")




async def cmd_detector_stats(update, context):
    """v7.3.1: show per-detector call statistics."""
    try:
        if not _detector_stats:
            await _reply(update, "no detector stats yet")
            return
        lines = ["📊 <b>Detector Stats</b>", "━━━━━━━━━━━━━━━━━━━",
                 "<code>detector        calls   none   cross  filt  sent</code>"]
        total_calls = 0
        for name, s in sorted(_detector_stats.items(),
                              key=lambda x: -x[1]["calls"]):
            lines.append(
                f"<code>{name:15s} {s['calls']:6d} {s['none']:6d} "
                f"{s['cross']:6d} {s['filtered']:5d} {s['sent']:5d}</code>")
            total_calls += s["calls"]
        lines.append("━━━━━━━━━━━━━━━━━━━")
        lines.append(f"Total calls: <b>{total_calls}</b>")
        await _reply(update, "\n".join(lines), parse_mode="HTML")
    except Exception as e:
        await _reply(update, f"❌ {e}")


async def cmd_manual_sl_tp_input(update, context):
    """🆕 v7.9.41 — handle text like 'XRPUSDT 1.29 1.32' for manual SL/TP."""
    try:
        if not update.message or not update.message.text:
            return
        text = update.message.text.strip()
        parts = text.split()
        if len(parts) < 3:
            return
        bsym = parts[0].upper()
        if bsym not in _manual_waiting_input:
            return
        sl = float(parts[1])
        tp = float(parts[2])
        # Delegate to _apply
        await _apply_manual_sl_tp(bsym, context.bot, sl=sl, tp=tp)
        _manual_waiting_input.pop(bsym, None)
    except Exception as e:
        log.debug(f"cmd_manual_sl_tp_input: {e}")


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        log.warning(f"⚠️ network: {err}"); return
    log.error(f"❌ error: {err}", exc_info=err)


# ═══════════════════════════════════════════════════════════
# 23) Jobs
# ═══════════════════════════════════════════════════════════

# ═══ Job timing helper (v7.9.2) ═══
def _timed(job_name):
    """Decorator to log job duration."""
    def decorator(fn):
        async def wrapper(*args, **kwargs):
            import time as _t
            _t0 = _t.time()
            try:
                return await fn(*args, **kwargs)
            finally:
                _elapsed = _t.time() - _t0
                log.info(f"⏱️ {job_name}: {_elapsed:.2f}s")
        return wrapper
    return decorator


_last_skip_notify = {}
SKIP_NOTIFY_COOLDOWN = 300  # 5 min per (symbol, category)


def _categorize_skip_reason(reason):
    """Returns category or None (silent)."""
    if not reason:
        return None
    r = reason.lower()
    # silent (temporary/routine)
    if "rate limit" in r or "cooldown" in r:
        return None
    if "unconfirmed signal" in r or "no مستويات" in reason:
        return None
    # important
    if "خسارة يومية" in reason or "daily loss" in r:
        return "daily_loss"
    if "خسائر متتالية" in reason or "consecutive" in r:
        return "consec_losses"
    if "صفقة مفتوحة" in reason or "مفتوحة على" in reason:
        return "existing_position"
    if "معاكسة" in reason:
        return "opposite_position"
    if "الحد الأقصى" in reason or "وصلت للحد" in reason:
        return "max_concurrent"
    if "trades" in r and ("SHORT" in reason or "LONG" in reason):
        return "max_same_dir"
    if "qty" in r or "notional" in r:
        return "size"
    return "other"


_paper_reject_notify_ts = {}
PAPER_REJECT_COOLDOWN = 300  # 5 دقائق لكل (رمز + سبب)


# v8.0.112 — Near-Miss Alert
_NEAR_MISS_MARGINS = {
    "RANGE_SR": 3.0,  # v8.0.113 — Near-Miss لـ range_sr
    "ADX": 3.0,
    "Volume": 0.2,
    "RSI": 5.0,
    "Score": 5.0,
    "EMA7": 0.05,
    "MACD": 0.10,
}

def _is_near_miss(cross):
    """v8.0.113 — فحص إذا الإشارة قريبة من القبول"""
    rejects = cross.get("rejections", [])
    if len(rejects) != 1:  # 1 فلتر فقط
        return False, None
    r = rejects[0]
    _f = r.get("filter", "")
    _d = r.get("dist", 999)
    # v8.0.113 — PRE_EXEC = قريب دائماً (تشغيلي/جودة)
    if _f == "PRE_EXEC":
        return True, r
    _margin = _NEAR_MISS_MARGINS.get(_f, 0)
    if _margin > 0 and _d <= _margin:
        return True, r
    return False, None


def _build_near_miss_kb(sym, cross):
    """v8.0.112 — أزرار Near-Miss"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ نفذ الآن (السعر الحالي)", callback_data=f"nm:exec:{sym}")],
        [InlineKeyboardButton("📄 Paper فقط", callback_data=f"nm:paper:{sym}")],
    ])


async def _notify_near_miss(bot, sym, cross, reason):
    """v8.0.112 — إشعار Near-Miss"""
    if not bot or not CHAT_ID:
        return
    # v8.0.113 — تخزين للإشارة (لـ nm:exec)
    try:
        _pending_manual[sym] = {"cross": cross, "reason": reason, "ts": _time.time()}
    except Exception:
        pass
    _s = cross.get("support", {})
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(f"🟡 <b>إشارة قريبة — {short(sym)}</b>\n"
                  f"🇸🇾 {syria_now_str()}\n"
                  f"━━━━━━━━━━━━━━━━━━━\n"
                  f"📌 {short(sym)} [{cross.get('timeframe','?')}] "
                  f"{'🟢' if cross.get('direction')=='bullish' else '🔴'} "
                  f"{cross.get('direction','?')}\n"
                  f"🎯 Score: {cross.get('final_score', 0)} | "
                  f"{_source_label(cross.get('source','?'))}\n"
                  f"━━━━━━━━━━━━━━━━━━━\n"
                  f"⚠️ <b>رُفض بسبب:</b> {reason}\n"
                  f"💡 <b>اقتراح:</b> راقبها أو نفذها يدوياً"),
            parse_mode="HTML",
            reply_markup=_build_near_miss_kb(sym, cross))
    except Exception as e:
        log.debug(f"_notify_near_miss: {e}")


async def _notify_paper_reject(bot, sym, cross, reason):
    """v8.0.95d — إشعار Paper مع throttle (5 min)"""
    if not bot or not CHAT_ID:
        return
    _key = (sym, reason[:30])
    _now = _time.time()
    if _now - _paper_reject_notify_ts.get(_key, 0) < PAPER_REJECT_COOLDOWN:
        return
    _paper_reject_notify_ts[_key] = _now
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(f"📄 <b>إشارة مرفوضة (Paper)</b>\n"
                  f"📌 {short(sym)} [{cross.get('timeframe','?')}] "
                  f"{'🟢' if cross.get('direction')=='bullish' else '🔴'} "
                  f"{cross.get('direction','?')}\n"
                  f"🎯 Score: {cross.get('final_score', 0)} | "
                  f"{_source_label(cross.get('source','?'))}\n"
                  f"━━━━━━━━━━━━━━━━━━━\n"
                  f"❌ <b>السبب:</b> {reason}\n"
                  f"⏱️ <i>throttle 5 min</i>"),
            parse_mode="HTML")
    except Exception as e:
        log.debug(f"_notify_paper_reject: {e}")


async def _notify_exec_skip(bot, sym, cross, reason):
    """🆕 v7.9.38 — Notify user when signal fails at execution."""
    if not bot or not CHAT_ID:
        return
    cat = _categorize_skip_reason(reason)
    if cat is None:
        return
    key = (sym, cat)
    now = _time.time()
    if now - _last_skip_notify.get(key, 0) < SKIP_NOTIFY_COOLDOWN:
        return
    _last_skip_notify[key] = now
    
    extra = ""
    try:
        if cat == "max_same_dir":
            extra = f"\n💡 الحد: MAX_SAME_DIRECTION = {_risk_mgr.max_same_dir}"
        elif cat == "max_concurrent":
            extra = f"\n💡 الحد: MAX_CONCURRENT = {_risk_mgr.max_concurrent}"
        elif cat == "daily_loss":
            st = _risk_mgr.status()
            extra = f"\n💡 PnL اليوم: ${st.get('daily_pnl', 0):+.2f} / -${_risk_mgr.max_daily_loss}"
        elif cat == "consec_losses":
            st = _risk_mgr.status()
            extra = f"\n💡 الخسائر المتتالية: {st.get('consec_losses', 0)} / {_risk_mgr.max_consec_losses}"
    except Exception:
        pass
    
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=(f"🚫 <b>إشارة لم تُنفَّذ</b>\n"
                  f"📌 {short(sym)} [{cross.get('timeframe','?')}] "
                  f"{'🟢' if cross.get('direction')=='bullish' else '🔴'} "
                  f"{cross.get('direction','?')}\n"
                  f"🎯 Score: {cross.get('final_score', 0)} | {_source_label(cross.get('source','?'))}\n"
                  f"━━━━━━━━━━━━━━━━━━━\n"
                  f"❌ <b>السبب:</b> {reason}{extra}"),
            parse_mode="HTML")
        log.info(f"📤 Skip notify: {sym} [{cat}]")
    except Exception as e:
        log.debug(f"_notify_exec_skip: {e}")


# 🆕 v7.9.41 — Manual trade confirmation
_pending_manual = {}  # {bsym: {"ticket": t, "levels": lv, "msg_ts": ts}}
_quick_profit_targets = {} # 🆕 {bsym: {"ticket": t, "target_usd": 0.5}}
_orphan_cleanup_enabled = True  # 🆕 v7.9.53 — يمكن تعطيله من تلغرام
PAPER_ONLY_MODE = False  # v9.24.0: تنفيذ حقيقي
_global_bot = None  # v8.0.87c — كل الصفقات إلى Paper (بدون تنفيذ)
_manual_waiting_input = {}  # {bsym: {"ticket": t, "ts": ts}} for /manual SL TP entry


def _build_manual_confirm_kb(bsym):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ موافق (ضع SL/TP)", callback_data=f"mc:ok:{bsym}")],
        [InlineKeyboardButton("✏️ يدوي (SL/TP مخصص)", callback_data=f"mc:manual:{bsym}")],
        [InlineKeyboardButton("❌ مراقبة فقط", callback_data=f"mc:no:{bsym}")],
        [InlineKeyboardButton("💰 ربح سريع (+$0.5)", callback_data=f"mc:quick:{bsym}")],
        [InlineKeyboardButton("🚫 سأديرها بنفسي (لا تتدخل)", callback_data=f"mc:mine:{bsym}")],  # v8.0.92
    ])


def _build_manual_confirm_msg(ticket, levels):
    sym = ticket["symbol"]
    side = ticket["side"]
    is_long = side == "LONG"
    entry = float(ticket["entry"])
    cur_price = entry  # fallback
    try:
        cur_price = float(ticket.get("_cur_price") or entry)
    except Exception: pass
    if is_long:
        pnl_pct = (cur_price - entry) / entry * 100 if entry else 0
    else:
        pnl_pct = (entry - cur_price) / entry * 100 if entry else 0
    pnl_usdt = pnl_pct / 100 * entry * float(ticket.get("qty", 0)) / 100 if entry else 0
    icon = "🟢" if pnl_pct >= 0 else "🔴"
    sl = float(levels.get("stop_loss", 0))
    tp = float(levels.get("tp1", 0))
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = reward / risk if risk > 0 else 0
    return (
        f"🔍 <b>صفقة يدوية — {short(sym)}</b>\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢' if is_long else '🔴'} <b>{side}</b> | 📦 {ticket.get('qty')}\n"
        f"💰 الدخول: <b>{fmt_price(entry)}</b>\n"
        f"💵 الحالي: <b>{fmt_price(cur_price)}</b>\n"
        f"{icon} PnL: <b>{pnl_pct:+.2f}%</b> (≈{pnl_usdt:+.2f} USDT)\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>الاقتراح (Smart Zones):</b>\n"
        f"• SL: <b>{fmt_price(sl)}</b> ({-abs(sl-entry)/entry*100:.2f}%)\n"
        f"• TP1: <b>{fmt_price(tp)}</b> ({abs(tp-entry)/entry*100:+.2f}%)\n"
        f"• R:R = <b>{rr:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"❓ <b>ماذا تريد؟</b>"
    )


async def _send_manual_confirm(bot, ticket, levels, cur_price=None):
    """🆕 v7.9.41 — send manual trade confirmation with buttons."""
    if not bot or not CHAT_ID:
        return
    bsym = ticket["binance_symbol"]
    if bsym in _pending_manual:
        # Already sent recently → skip
        return
    try:
        ticket["_cur_price"] = cur_price or ticket.get("entry", 0)
        text = _build_manual_confirm_msg(ticket, levels)
        msg = await bot.send_message(
            chat_id=CHAT_ID, text=text, parse_mode="HTML",
            reply_markup=_build_manual_confirm_kb(bsym))
        _pending_manual[bsym] = {
            "ticket": ticket, "levels": levels,
            "msg_ts": _time.time(),
            "chat_id": msg.chat_id, "msg_id": msg.message_id,
        }
        log.info(f"📩 Manual confirm sent: {bsym}")
    except Exception as e:
        log.warning(f"_send_manual_confirm: {e}")


def _detect_2c_opposite(df, curr=-2):
    """
    🆕 v8.1.2 — يكتشف شمعتين متتاليتين متعاكستين بجودة عالية.
    الشروط:
      1) جسم كل شمعة ≥ 30% من ATR(20)
      2) نسبة الحجم بين 0.5 و 2.0
      3) اختراق حقيقي (close2 > high1 أو close2 < low1)
      4) الشمعة الثانية ≥ 80% من حجم الأولى
    Returns: dict or None
    """
    if df is None or len(df) < 22:
        return None
    try:
        c1 = df.iloc[curr - 1]
        c2 = df.iloc[curr]
        o1, cl1, h1, l1 = float(c1["o"]), float(c1["c"]), float(c1["h"]), float(c1["l"])
        o2, cl2, h2, l2 = float(c2["o"]), float(c2["c"]), float(c2["h"]), float(c2["l"])
        b1 = abs(cl1 - o1)
        b2 = abs(cl2 - o2)
        
        if b1 <= 0 or b2 <= 0:
            return None
        
        color1 = "bull" if cl1 > o1 else "bear"
        color2 = "bull" if cl2 > o2 else "bear"
        
        if color1 == color2:
            return None
        
        # 🆕 شرط 1: حجم جسم الشمعة ≥ 30% من ATR(20)
        try:
            tr = pd.concat([
                df["h"] - df["l"],
                (df["h"] - df["c"].shift()).abs(),
                (df["l"] - df["c"].shift()).abs()
            ], axis=1).max(axis=1)
            atr = tr.ewm(alpha=1/14, adjust=False).mean()
            avg_atr = float(atr.iloc[curr-20:curr].mean())
            if avg_atr <= 0:
                return None
            min_body = avg_atr * 0.3
            if b1 < min_body or b2 < min_body:
                return None
        except Exception:
            return None
        
        # 🆕 شرط 2: نسبة الحجم بين 0.5 و 2.0
        ratio = b2 / b1
        if ratio < 0.5 or ratio > 2.0:
            return None
        
        # 🆕 شرط 3: اختراق حقيقي
        if color2 == "bull":
            # تأكيد انعكاس صاعد: يجب أن يُغلق فوق قمة الشمعة 1
            if cl2 <= h1:
                return None
        else:
            # تأكيد انعكاس هابط: يجب أن يُغلق تحت قاع الشمعة 1
            if cl2 >= l1:
                return None
        
        # 🆕 شرط 4: الشمعة الثانية ≥ 80% من حجم الأولى
        if b2 < b1 * 0.8:
            return None
        
        return {
            "c1": {"o": o1, "c": cl1, "h": h1, "l": l1, "body": b1, "color": color1},
            "c2": {"o": o2, "c": cl2, "h": h2, "l": l2, "body": b2, "color": color2},
            "ratio": round(ratio, 3),
            "body_atr_ratio": round(min(b1, b2) / avg_atr, 3),
        }
    except Exception:
        return None


def _build_2c_msg(ticket, price, pnl_pct, pnl_usdt, det):
    sym = ticket["symbol"]
    side = ticket["side"]
    is_long = side == "LONG"
    icon = "🟢" if pnl_usdt >= 0 else "🔴"
    c1 = det["c1"]
    c2 = det["c2"]
    return (
        f"🚨 <b>شمعتان متعاكستان — {short(sym)}</b>\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢' if is_long else '🔴'} <b>{side}</b> [{ticket.get('tf')}] | "
        f"{_source_label(ticket.get('source','?'))}\n"
        f"💰 الدخول: <b>{fmt_price(ticket['entry'])}</b>\n"
        f"💵 الحالي: <b>{fmt_price(price)}</b>\n"
        f"{icon} PnL: <b>{pnl_pct:+.2f}%</b> ({pnl_usdt:+.2f} USDT)\n"
        f"🛑 SL: {fmt_price(ticket.get('sl',0))}\n"
        f"🎯 TP1: {fmt_price(ticket.get('tp',0))}\n"
        f"🎯 TP2: {fmt_price(ticket.get('tp2', 0) or 0)}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🕯️ <b>الشمعة 1:</b> {c1['color'].upper()} (body={c1['body']:.6f})\n"
        f"🕯️ <b>الشمعة 2:</b> {c2['color'].upper()} (body={c2['body']:.6f})\n"
        f"📊 النسبة: {det['ratio']}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ إشارة انعكاس محتملة"
    )


async def _shooting_star_watcher(bot=None):
    """🆕 v7.9.42 — يرسل إشعار عند شمعتين متعاكستين (مرة لكل صفقة)."""
    if not _binance.ok or not bot or not CHAT_ID:
        return
    with _ticket_lock:
        tickets = [t for t in TRADE_TICKETS.values()
                   if not t.get("manual") and not t.get("2c_notified")]
    for t in tickets:
        if t.get("2c_notified"):
            continue
        if t.get("manual"):
            continue
        try:
            sym = t["symbol"]
            tf = t.get("tf", "5m")
            data = await fetch_ohlcv_cached(sym, tf, 10)
            if not data or len(data) < 5:
                continue
            df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
            det = _detect_2c_opposite(df, curr=-2)
            if not det:
                continue
            # We have 2 opposite candles — check direction
            side = t["side"]
            c2_color = det["c2"]["color"]
            # signal must be AGAINST our position (opposite)
            is_against = (
                (side == "LONG" and c2_color == "bear") or
                (side == "SHORT" and c2_color == "bull")
            )
            if not is_against:
                continue
            # Get current price
            try:
                cur = await _binance_call(_binance.last_price, t["binance_symbol"])
            except Exception:
                cur = t["entry"]
            if cur <= 0:
                continue
            entry = float(t["entry"])
            qty = float(t["qty"])
            if side == "LONG":
                pnl_pct = (cur - entry) / entry * 100
                pnl_usdt = (cur - entry) * qty
            else:
                pnl_pct = (entry - cur) / entry * 100
                pnl_usdt = (entry - cur) * qty
            # Send message with close button
            text = _build_2c_msg(t, cur, pnl_pct, pnl_usdt, det)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚨 أغلق الآن", callback_data=f"close_now:{t['binance_symbol']}")],
                [InlineKeyboardButton("✅ تجاهل", callback_data=f"ignore_2c:{t['binance_symbol']}")],
            ])
            try:
                await bot.send_message(chat_id=CHAT_ID, text=text,
                                       parse_mode="HTML", reply_markup=kb)
                t["2c_notified"] = True
                _save_state()
                log.info(f"🚨 2c-opposite sent: {t['binance_symbol']}")
            except Exception as e:
                log.warning(f"_shooting_star_watcher send: {e}")
        except Exception as e:
            log.debug(f"_shooting_star_watcher {t.get('binance_symbol')}: {e}")


async def _apply_manual_sl_tp(bsym, bot, sl=None, tp=None):
    """🆕 v7.9.41 — place SL/TP on Binance for manual trade."""
    pending = _pending_manual.get(bsym)
    if not pending:
        if bot:
            try:
                await bot.send_message(chat_id=CHAT_ID,
                    text=f"⚠️ لا اقتراح معلق لـ {bsym}")
            except: pass
        return
    ticket = pending["ticket"]
    levels = pending["levels"]
    side = ticket["side"]
    qty = float(ticket["qty"])
    info = _binance.info(bsym)
    if not info:
        if bot:
            try:
                await bot.send_message(chat_id=CHAT_ID,
                    text=f"❌ {bsym} غير مدعوم على Binance")
            except: pass
        return
    
    # 🆕 v8.2.3 — SL من Smart Zones + TP = 0.35%
    _actual_entry = float(ticket.get("entry", 0))
    _direction = "bullish" if side == "LONG" else "bearish"
    
    # 1. حساب SL من Smart Zones
    if sl:
        final_sl = float(sl)
    else:
        try:
            _sz = await sz_compute_levels(ticket["symbol"], _direction, "5m")
            if _sz and _sz.get("stop_loss"):
                final_sl = float(_sz["stop_loss"])
                log.info(f"✅ Manual SL from Smart Zones: {final_sl:.6f}")
            else:
                raise Exception("Smart Zones returned None")
        except Exception as e:
            final_sl = float(levels.get("stop_loss", 0))
            log.warning(f"⚠️ Manual SL fallback to levels: {final_sl:.6f} ({e})")

    # 2. حساب TP (0.35%)
    if tp:
        final_tp = float(tp)
    else:
        if _actual_entry > 0:
            if side == "SHORT":
                final_tp = _actual_entry * (1 - 0.0035)
            else:
                final_tp = _actual_entry * (1 + 0.0035)
            log.info(f"🔒 Manual TP fixed 0.35% from entry: {final_tp:.6f}")
        else:
            final_tp = float(levels.get("tp1", 0))
    
    _order_side = "BUY" if side == "LONG" else "SELL"
    _qty_s = _binance.fmt_qty(
        _binance.round_qty(qty, info["step_size"]),
        info["qty_precision"])
    
    sl_ok = False
    try:
        # v8.0.87v — SL 100%
        _full_qty_s = _binance.fmt_qty(
            _binance.round_qty(qty, info["step_size"]),
            info["qty_precision"])
        await _binance_call(_binance.place_stop_loss, bsym, _order_side, final_sl, _full_qty_s)
        sl_ok = True
        ticket["sl"] = final_sl
        ticket["sl_status"] = "binance"
        ticket["half_sl_mode"] = False  # 🆕 تفعيل وضع النصف
        
        if bot and CHAT_ID:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"✅ <b>تم وضع SL (100%) — {short(bsym)}</b>\n"
                          f"🛑 SL: {fmt_price(final_sl)} (كامل)\n"
                          f"⚡ الصفقة محمية بالكامل."),
                    parse_mode="HTML")
            except Exception: pass
    except Exception as e:
        log.error(f"Manual SL failed {bsym}: {e}")
    
    tp_ok = False
    if final_tp > 0:
        try:
            # v8.0.87v — TP 100%
            _tp_full_s = _binance.fmt_qty(
                _binance.round_qty(qty, info["step_size"]),
                info["qty_precision"])
            await _binance_call(_binance.place_take_profit, bsym, _order_side,
                                final_tp, _tp_full_s, label="manual")
            tp_ok = True
            ticket["tp"] = final_tp
            ticket["tp_status"] = "tp1_bot"
        except Exception as e:
            log.error(f"Manual TP1 failed {bsym}: {e}")
    
    _save_state()
    _pending_manual.pop(bsym, None)
    
    if bot:
        try:
            _msg = (f"{'✅' if (sl_ok and tp_ok) else '⚠️'} <b>نتيجة الصفقة اليدوية — {short(bsym)}</b>\n"
                    f"🇸🇾 {syria_now_str()}\n"
                    f"━━━━━━━━━━━━━━━━━━━\n")
            if sl_ok:
                _msg += f"✅ SL: <b>{fmt_price(final_sl)}</b>\n"
            else:
                _msg += f"❌ SL فشل\n"
            if tp_ok:
                _msg += f"✅ TP1 (100%): <b>{fmt_price(final_tp)}</b>\n"
            elif final_tp > 0:
                _msg += f"❌ TP1 فشل\n"
            await bot.send_message(chat_id=CHAT_ID, text=_msg, parse_mode="HTML")
        except: pass


def _cleanup_stale_caches():
    """🆕 v7.9.9 — يمنع تسرب الذاكرة بحذف الكاشات القديمة."""
    now = _time.time()
    removed = 0
    # OHLCV: احتفظ فقط بالحديث (< 10 دقائق)
    for k in [k for k, v in _ohlcv_cache.items() if now - v.get("ts", 0) > 600]:
        _ohlcv_cache.pop(k, None); removed += 1
    # New caches: < 10 دقائق
    for cache in (_sz_cache, _market_cache, _htf_cache, _mtf_cache):
        for k in [k for k, v in cache.items() if now - v.get("ts", 0) > 600]:
            cache.pop(k, None); removed += 1
    # Crossover: < 30 دقيقة
    for k in [k for k, v in _crossover_cache.items() 
              if now - v.get("candle_ts", 0)/1000 > 1800]:
        _crossover_cache.pop(k, None); removed += 1
    # Divergence/BB/SR/VWAP/Ichimoku: < 60 دقيقة
    for cache in (_divergence_cache, _bb_cache, _sr_cache, 
                  _vwap_breakout_cache, _ichimoku_cache):
        for k in [k for k, ts in cache.items() if now - ts > 3600]:
            cache.pop(k, None); removed += 1
    # _recent_signals: احتفظ بـ 100 فقط
    if len(_recent_signals) > 100:
        keys = list(_recent_signals.keys())
        for k in keys[:-100]:
            _recent_signals.pop(k, None); removed += 1
    # _last_signal_time: احتفظ بـ 200 فقط
    if len(_last_signal_time) > 200:
        keys = list(_last_signal_time.keys())
        for k in keys[:-200]:
            _last_signal_time.pop(k, None); removed += 1
    # _pending_signals: احذف المنتهية (>90 دقيقة)
    for sid in [k for k, v in _pending_signals.items()
                if now - v.get("entry_ts", 0) > 5400]:
        _pending_signals.pop(sid, None); removed += 1
    # _paper_signals: احتفظ بـ 200 فقط
    with _paper_lock:
        if len(_paper_signals) > 200:
            keys = list(_paper_signals.keys())
            for k in keys[:-200]:
                _paper_signals.pop(k, None); removed += 1
    # _voting_history: احذف الرموز القديمة
    for sym in list(_voting_history.keys()):
        hist = _voting_history[sym]
        hist[:] = [(d, t) for d, t in hist if now - t < 600]
        if not hist:
            _voting_history.pop(sym, None); removed += 1
    if removed:
        log.info(f"🧹 Cache cleanup: {removed} entries removed")


async def _can_execute_now(cross):
    """🆕 v8.6.1 — كل رفض يذهب للورقي مع سبب واضح (الترتيب الصحيح للفلاتر)"""
    if not _binance.ok:
        return False, "Binance غير متصل"
    if "rejections" not in cross:
        cross["rejections"] = []
    if is_paused():
        return False, "البوت في pause"
    
    bsym = _to_binance_symbol(cross["symbol"])
    side = "LONG" if cross["direction"] == "bullish" else "SHORT"

    # 🆕 v9.0.1 — فحص حالة BTC Sentinel
    _btc_ok, _btc_reason = await _can_execute_now_btc(cross)
    if not _btc_ok:
        return False, _btc_reason

    # 🆕 v8.7.3 — فلتر market محذوف (استخدمنا الفلتر الديناميكي في passes_filter)

    # ═══ المرحلة 1: الفلاتر الفنية ═══
    # 1. EMA7    """🆕 v8.6.0 — كل رفض يذهب للورقي مع سبب واضح (الترتيب الصحيح للفلاتر)"""
    if not _binance.ok:
        return False, "Binance غير متصل"
    if "rejections" not in cross:
        cross["rejections"] = []
    if is_paused():
        return False, "البوت في pause"
    
    bsym = _to_binance_symbol(cross["symbol"])
    side = "LONG" if cross["direction"] == "bullish" else "SHORT"

    # ═══ المرحلة 0: فلتر الترند (v8.6.0) ═══
    _market = cross.get("market", "UNKNOWN")
    # v8.9.6 — تشديد في _can_execute_now: ضد الترند يحتاج score >= 75
    _score_cx = cross.get("final_score", 0)
    if _market == "TREND_UP" and side == "SHORT":
        if _score_cx < 75:
            return False, "SHORT against TREND_UP (score " + str(_score_cx) + " < 75)"
    if _market == "TREND_DOWN" and side == "LONG":
        if _score_cx < 75:
            return False, "LONG against TREND_DOWN (score " + str(_score_cx) + " < 75)"

    # ═══ المرحلة 1: الفلاتر الفنية ═══
    # 1. EMA7 (v8.9.3 — مشدد)
    try:
        _df_ema7 = cross.get("_df")
        if _df_ema7 is not None and len(_df_ema7) >= 10:
            _e7_series = calc_ema(_df_ema7["c"], 7)
            _e7_curr = float(_e7_series.iloc[-1])
            _e7_prev = float(_e7_series.iloc[-2])
            _pr7_curr = float(_df_ema7["c"].iloc[-1])
            _pr7_prev = float(_df_ema7["c"].iloc[-2])
            _margin = 0.0005
            if side == "SHORT":
                if _pr7_curr > _e7_curr * (1 + _margin) or _pr7_prev > _e7_prev * (1 + _margin):
                    return False, "EMA7: price above"
                if _e7_curr > _e7_prev:
                    return False, "EMA7: slope UP"
            if side == "LONG":
                if _pr7_curr < _e7_curr * (1 - _margin) or _pr7_prev < _e7_prev * (1 - _margin):
                    return False, "EMA7: price below"
                if _e7_curr < _e7_prev:
                    return False, "EMA7: slope DOWN"
    except Exception:
        pass

    # 2. VWAP / SR
    if cross.get("source") == "vwap_breakout":
        _s_v = cross.get("support", {})
        if _s_v.get("vol_ratio", 0) < 0.8 or _s_v.get("adx", 0) < 20:
            return False, "vwap weak"

    if cross.get("source") == "sr_breakout":
        _s = cross.get("support", {})
        if _s.get("adx", 0) < 20 and _s.get("vol_ratio", 0) < 3.0:
            return False, "range_sr"

    # ═══ المرحلة 2: الفلاتر التشغيلية ═══
    if _tickets_for(bsym):
        return False, "صفقة مفتوحة على " + str(short(cross["symbol"]))
    
    try:
        positions = await _binance_call(_binance.get_open_positions)
    except Exception:
        positions = []
    
    if len(positions) >= _risk_mgr.max_concurrent:
        return False, "الحد الأقصى " + str(_risk_mgr.max_concurrent)
    
    same_dir = sum(1 for p in positions 
                   if (p.get("amount", 0) > 0 and side == "LONG")
                   or (p.get("amount", 0) < 0 and side == "SHORT"))
    if same_dir >= _risk_mgr.max_same_dir:
        return False, str(same_dir) + "/" + str(_risk_mgr.max_same_dir) + " " + side
    
    try:
        total_notional = sum(
            abs(float(p.get("amount", 0))) * float(p.get("mark_price", 0))
            for p in positions
        )
        bal = _binance.usdt_balance()
        if bal and bal.get("wallet", 0) > 0:
            usage_pct = (total_notional / (bal["wallet"] * LEVERAGE)) * 100
            if usage_pct >= MAX_MARGIN_USAGE_PCT:
                return False, "هامش " + str(round(usage_pct, 1)) + "%"
    except Exception:
        pass

    try:
        _info = _binance.info(bsym)
        if _info:
            _price = _binance.last_price(bsym)
            if _price > 0:
                _qty_raw = NOTIONAL_USDT / _price
                _qty = _binance.round_qty(_qty_raw, _info["step_size"])
                if _qty < _info["min_qty"]:
                    return False, "qty < min"
                if _qty * _price < _info["min_notional"]:
                    return False, "notional < min"
    except Exception:
        pass
    
    return True, "OK"

async def crossover_job(context, tfs=None):
    import time as _t
    _t0 = _t.time()
    if not _exchange.ok(): return
    # 🆕 v9.13.0 — pause يمنع التنفيذ الحقيقي فقط، وليس الورقي
    _is_bot_paused = is_paused()
    if _is_bot_paused:
        log.info("⏸️ crossover_job: Bot paused — إشارات ورقية فقط")
    _tfs = tfs if tfs else TIMEFRAMES
    _label = "/".join(_tfs)
    
    # 🆕 v7.9.10 — batch 5m symbols (4 per cycle)
    _batch_tf = _tfs[0] if (len(_tfs) == 1 and _tfs[0] == "5m") else None
    if _batch_tf:
        _idx = _crossover_batch_idx.get(_batch_tf, 0)
        _start = _idx * _BATCH_SIZE_5M
        _end = _start + _BATCH_SIZE_5M
        _symbols_to_scan = ALL_SYMBOLS[_start:_end]
        _total_batches = (len(ALL_SYMBOLS) + _BATCH_SIZE_5M - 1) // _BATCH_SIZE_5M
        _crossover_batch_idx[_batch_tf] = (_idx + 1) % _total_batches
        log.info(f"🔄 crossover_job[{_label}] START batch={_idx}/{_total_batches} symbols={[short(s) for s in _symbols_to_scan]}")
    else:
        _symbols_to_scan = ALL_SYMBOLS
        log.info(f"🔄 crossover_job[{_label}] START (all {len(_symbols_to_scan)} symbols)")
    
    detectors = _get_detectors()
    total = 0
    # 🆕 v7.9.9 — profiling
    _prof = {"det_total": 0.0, "det_calls": 0, "det_slow": {},
             "filter_total": 0.0, "filter_calls": 0,
             "htf_total": 0.0, "htf_calls": 0}
    for sym in _symbols_to_scan:
        for tf in _tfs:
            for det in detectors:
                _dn = det.__name__.replace("detect_","").replace("_breakout","").replace("_crossover","").replace("_divergence","")
                _key = _DETECTOR_SOURCE_MAP.get(_dn, _dn)  # 🩹 FIX #4
                _detector_stats[_key]["calls"] += 1
                try:
                    # 🆕 v6.9.3 — timeout 10s
                    _td = _t.time()
                    cross = await asyncio.wait_for(det(sym, tf), timeout=10.0)
                    _dt = _t.time() - _td
                    _prof["det_total"] += _dt
                    _prof["det_calls"] += 1
                    _prof["det_slow"][det.__name__] = \
                        _prof["det_slow"].get(det.__name__, 0) + _dt
                    if not cross:
                        _detector_stats[_key]["none"] += 1
                        continue
                    _detector_stats[_key]["cross"] += 1
                except asyncio.TimeoutError:
                    log.warning(f"det timeout {det.__name__} {sym} {tf}")
                    continue
                except Exception as e:
                    log.warning(f"det error {det.__name__} {sym} {tf}: {e}")
                    continue
                try:
                    _th = _t.time()
                    htf = await get_htf_trend(sym)
                    _prof["htf_total"] += _t.time() - _th
                    _prof["htf_calls"] += 1
                    _tf_ = _t.time()
                    passed, _r, cat = await passes_filter(cross, htf)
                    _prof["filter_total"] += _t.time() - _tf_
                    _prof["filter_calls"] += 1
                    if not passed:
                        key = f"filtered_{cat}"
                        _filter_stats[key] += 1
                        _detector_stats[_key]["filtered"] += 1  # 🩹 FIX #4
                        log.info(f"🚫 [{cross.get('source','?')}] {cross['symbol']} {tf}: {cat} — {_r}")
                        continue
                    _detector_stats[_key]["sent"] += 1  # 🩹 FIX #4
                    
                    # 🆕 v7.9.17 — Paper: silent (no Telegram)
                    # v8.0.98 — 15m مُفعّل (كل المؤشرات)

                    if cross.get("paper_signal"):
                        _filter_stats["paper_signals"] = _filter_stats.get("paper_signals", 0) + 1
                        if cross.get("levels"):
                            _paper_track_add(cross, cross["levels"])
                        log.debug(f"📄 Paper silent: {cross['symbol']} [{cross.get('source')}]")
                        continue  # no تنفيذ
                    if CHAT_ID:
                        # 🆕 v8.0.60 — فحص paperonly قبل الإرسال
                        if PAPER_ONLY_MODE:
                            if PAPER_MODE_ENABLED and cross.get("levels"):
                                cross["paper_signal"] = True
                                cross["paper_reason"] = "paperonly_mode"
                                cross["paper_type"] = "operational"
                                _paper_track_add(cross, cross["levels"])
                                log.info(f"📄 Paper (paperonly): {short(cross['symbol'])}")
                            continue
                        
                        # 🆕 v9.18.0 — إذا كان البوت في pause، سجل الإشارة في الورقي وتخطى التنفيذ
                        if _is_bot_paused:
                            cross["paper_signal"] = True
                            cross["paper_reason"] = "pause: البوت موقوف"
                            cross["paper_type"] = "operational"
                            if cross.get("levels"):
                                _paper_track_add(cross, cross["levels"])
                            log.info(f"📄 Paper (pause): {short(cross['symbol'])}")
                            continue

                        # 🆕 v8.0.58 — فحص مسبق قبل الإرسال
                        _can_exec, _exec_reason = await _can_execute_now(cross)
                        if not _can_exec:
                            if PAPER_MODE_ENABLED and cross.get("levels"):
                                cross["paper_signal"] = True
                                cross["paper_reason"] = f"pre_exec: {_exec_reason}"
                                cross["paper_type"] = "operational"
                                _paper_track_add(cross, cross["levels"])
                                log.info(f"📄 Paper (pre-exec): {short(cross['symbol'])} — {_exec_reason}")
                            
                            # v8.0.113 — إضافة PRE_EXEC rejections (إذا لم تكن موجودة)
                            if not cross.get("rejections"):
                                try:
                                    cross["rejections"] = [{"filter": "PRE_EXEC", "value": 0, "threshold": 0, "dist": 0, "reason": _exec_reason}]
                                except Exception:
                                    pass
                            
                            # v8.0.113 — Near-Miss check
                            _nm, _nm_rej = _is_near_miss(cross)
                            if _nm and _nm_rej:
                                try:
                                    await _notify_near_miss(
                                        context.bot, sym, cross,
                                        f"{_nm_rej['filter']} {_nm_rej['value']} (dist {_nm_rej['dist']})")
                                except Exception: pass
                            else:
                                try:
                                    await _notify_paper_reject(
                                        context.bot, sym, cross,
                                        f"pre-exec: {_exec_reason}")
                                except Exception: pass
                            continue
                        
                        # 🆕 v8.0.60 — فحص live_confirm قبل الإرسال (إذا live)
                        if cross.get("alert_type") == "live":
                            _live_ok, _agree = await check_live_confirmation(
                                cross["symbol"], cross["direction"], n=3, min_agree=2)
                            if not _live_ok:
                                log.info(f"⚠️ live confirm failed: {short(cross['symbol'])} ({_agree}/3)")
                                if PAPER_MODE_ENABLED and cross.get("levels"):
                                    cross["paper_signal"] = True
                                    cross["paper_reason"] = f"live_confirm_failed: {_agree}/3"
                                    cross["paper_type"] = "operational"
                                    _paper_track_add(cross, cross["levels"])
                                    log.info(f"📄 Paper (live confirm failed): {short(cross['symbol'])}")
                                continue
                        
                        try:
                            # v8.0.94 — لا نرسل الإشارة قبل التنفيذ
                            total += 1
                            _filter_stats["sent"] += 1
                            # 🆕 v8.0.57 — لا cooldown للإشارات live (فقط confirmed)
                            if cross.get("alert_type") == "confirmed":
                                mark_symbol_sent(sym)
                            # 🆕 v8.0.49 — تأكيد كل الإشارات (live + المؤكدة الجديدة)
                            _needs_confirm = cross["alert_type"] == "live" or cross.get("_needs_confirm", False)
                            if _needs_confirm:
                                _live_ok, _agree = await check_live_confirmation(
                                    cross["symbol"], cross["direction"], n=3, min_agree=2)
                                if not _live_ok:
                                    log.info(f"⚠️ live confirm failed: {short(cross['symbol'])} ({_agree}/3)")
                                    # 🆕 v8.0.57 — لا cooldown عند رفض live
                                    # (نسمح بإشارة مؤكدة فوراً)
                                    try:
                                        await _notify_exec_skip(
                                            context.bot, sym, cross,
                                            f"Live confirm {_agree}/3")
                                    except Exception: pass
                                    continue
                                else:
                                    log.info(f"✅ live confirm passed: {short(cross['symbol'])} ({_agree}/3)")

                            if cross["alert_type"] in ("confirmed", "live"):
                                # 🆕 v7.9.22 — REVERSAL CLOSE (score >= 80)
                                _sc = cross.get("final_score", 0)
                                _opp_dir = "bearish" if cross["direction"] == "bullish" else "bullish"
                                if _sc >= 80:
                                    _bsym = _to_binance_symbol(cross["symbol"])
                                    _opp_tickets = [t for t in _tickets_for(_bsym) if t["side"] != ("LONG" if cross["direction"] == "bullish" else "SHORT")]
                                    for _ot in _opp_tickets:
                                        try:
                                            log.warning(f"🔄 Reversal signal (score={_sc}) — closing {_ot['side']} on {_bsym}")
                                            _close_side = "SELL" if _ot["side"] == "LONG" else "BUY"
                                            _pos = await _binance_call(_binance.get_open_positions)
                                            _actual = next((p for p in _pos if p["symbol"] == _bsym), None)
                                            if _actual:
                                                _qty = abs(float(_actual["amount"]))
                                                _info = _binance.info(_bsym)
                                                _qty_s = _binance.fmt_qty(_binance.round_qty(_qty, _info["step_size"]), _info["qty_precision"]) if _info else f"{_qty}"
                                                _res = await _safe_close_market(_bsym, _close_side, _qty_s)
                                                _cp = _res["avg_price"] or 0
                                                _entry = float(_ot.get("entry", 0))
                                                _pnl_pct = ((_cp - _entry) / _entry * 100) if _ot["side"] == "LONG" else ((_entry - _cp) / _entry * 100)
                                                log.warning(f"🔄 Reversal close: {_bsym} {_ot['side']} @ {_cp} ({_pnl_pct:+.2f}%)")
                                                _log_trade_stat(_ot, _cp, "REVERSAL_SIGNAL")
                                                _record_shadow_reversal(_ot, _cp, _pnl_pct)
                                                _record_risk_from_ticket(_ot, _cp, "REVERSAL_SIGNAL")
                                                _remove_ticket(_ot["id"])
                                                _filter_stats["trades_closed"] += 1
                                        except Exception as _re:
                                            log.warning(f"reversal close {_bsym}: {_re}")
                                record_signal(sym, cross["direction"])
                                register_signal_for_tracking(cross)
                                _src = cross.get("source", "?")
                                log.info(f"🎬 Executing: {short(sym)} [{_src}] "
                                         f"score={_sc} dir={cross.get('direction','?')}")
                                exe = await execute_binance_trade(
                                    cross, bot=context.bot)
                                if exe.get("ok"):
                                    log.info(f"🚀 Opened: {short(sym)} [{_src}]")
                                else:
                                    log.info(f"exec skip {sym}: {exe.get('error')}")
                                    # 🆕 v7.9.46 — Paper للفشل التشغيلي
                                    _exec_err = (exe.get("error") or "").lower()
                                    _ops_keys = ["slippage", "صفقة مفتوحة",
                                                 "الحد الأقصى", "trades",
                                                 "qty", "notional", "هامش"]
                                    _is_ops = any(k in _exec_err for k in _ops_keys)
                                    _is_rl = "rate limit" in _exec_err
                                    _exec_score = cross.get("final_score", 0)
                                    if (_is_ops and not _is_rl
                                            and PAPER_MODE_ENABLED
                                            and _exec_score >= PAPER_MIN_SCORE
                                            and cross.get("levels")):
                                        cross["paper_signal"] = True
                                        cross["paper_reason"] = (
                                            f"exec_reject: {exe.get('error','')[:80]}")
                                        cross["paper_type"] = "operational"
                                        _paper_track_add(cross, cross["levels"])
                                        log.info(f"📄 Paper (exec reject): "
                                                 f"{short(sym)} [{_src}] "
                                                 f"score={_exec_score}")
                                    try:
                                        await _notify_exec_skip(
                                            context.bot, sym, cross,
                                            exe.get("error", ""))
                                    except Exception: pass
                        except Exception as e:
                            log.error(f"send {sym} {tf}: {e}")
                except Exception as e:
                    log.exception(f"job {sym} {tf}: {e}")
            # 🆕 v7.9.46 — تفعيل Ichimoku + BB للـPaper فقط
            if PAPER_MODE_ENABLED and PAPER_FORCE_DISABLED_DETECTORS:
                for _pdet, _pname in [
                    (detect_bb_squeeze_breakout, "bb_squeeze"),
                    # 🆕 v8.0.54 — ichimoku removed (now in _get_detectors)
                ]:
                    try:
                        _pcross = await asyncio.wait_for(
                            _pdet(sym, tf), timeout=10.0)
                    except asyncio.TimeoutError:
                        log.debug(f"paper det timeout {_pname} {sym} {tf}")
                        continue
                    except Exception as _pe:
                        log.debug(f"paper det error {_pname} {sym} {tf}: {_pe}")
                        continue
                    if not _pcross or not _pcross.get("levels"):
                        continue
                    _psc = _pcross.get("final_score", 0)
                    if _psc < PAPER_MIN_SCORE:
                        continue
                    _pcross["paper_signal"] = True
                    _pcross["paper_reason"] = f"{_pname}_disabled"
                    _pcross["paper_type"] = "technical"
                    _paper_track_add(_pcross, _pcross["levels"])
                    log.info(f"📄 Paper (disabled det): {short(sym)} "
                             f"[{_pname}] score={_psc}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    _elapsed = _t.time() - _t0
    # v8.0.86 — نقل cleanup إلى job منفصل (يُشغَّل كل 5 دقائق)
    log.info(f"🔄 crossover_job[{_label}] END: {_elapsed:.1f}s | signals={total}")
    # 🆕 v7.9.9 — profiling report
    try:
        _det_avg = _prof["det_total"] / max(1, _prof["det_calls"]) * 1000
        _fil_avg = _prof["filter_total"] / max(1, _prof["filter_calls"]) * 1000
        _htf_avg = _prof["htf_total"] / max(1, _prof["htf_calls"]) * 1000
        _top3 = sorted(_prof["det_slow"].items(), key=lambda x: -x[1])[:3]
        _top3_str = " | ".join(f"{n.replace('detect_','')}={t:.1f}s" for n, t in _top3)
        log.info(
            f"   ⏱️ PROF: det={_prof['det_total']:.1f}s "
            f"({_prof['det_calls']}c avg={_det_avg:.0f}ms) | "
            f"filter={_prof['filter_total']:.1f}s (avg={_fil_avg:.0f}ms) | "
            f"htf={_prof['htf_total']:.1f}s (avg={_htf_avg:.0f}ms) | "
            f"TOP3: {_top3_str}"
        )
    except Exception as _pe:
        log.warning(f"prof: {_pe}")


async def shadow_trades_job(context):
    """🆕 v7.9.23 — evaluate shadow reversal trades (every 5 min)"""
    try:
        await _evaluate_shadow_trades()
    except Exception as e:
        log.warning(f"shadow_trades_job: {e}")


@_timed('tracker')
async def signal_tracker_job(context):
    if not _exchange.ok(): return
    try:
        await evaluate_pending_signals(bot=context.bot)
        await _paper_track_eval()
    except Exception as e: log.exception(f"tracker: {e}")


@_timed('price')
async def price_alert_job(context):
    try:
        if not _exchange.ok(): return
        total = 0
        for sym in ALL_SYMBOLS:
            try:
                a = await detect_sudden_change(sym)
                if a and CHAT_ID:
                    try:
                        await context.bot.send_message(
                            chat_id=CHAT_ID, text=build_price_alert_msg(a),
                            parse_mode="HTML")
                        total += 1
                    except Exception: pass
            except asyncio.CancelledError:
                raise
            except Exception as e: log.warning(f"price_alert {sym}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
        if total: log.info(f"📤 {total} alerts")
    except asyncio.CancelledError:
        log.info("price_alert_job cancelled")
        raise


async def _DISABLED_range_monitor_job(context):
    """⏸️ معطّلة بواسطة المستخدم"""
    return  # DISABLED BY USER
    if not _exchange.ok() or not _last_range: return
    for symbol in list(_last_range.keys()):
        try:
            b = await check_range_breakout(symbol)
            if b and CHAT_ID:
                await context.bot.send_message(chat_id=CHAT_ID,
                    text=build_breakout_msg(b), parse_mode="HTML")
                _filter_stats["range_breakouts"] += 1; continue
            a = await check_range_approach(symbol)
            if a and CHAT_ID:
                await context.bot.send_message(chat_id=CHAT_ID,
                    text=build_approach_msg(a), parse_mode="HTML")
                _filter_stats["range_approaches"] += 1
        except Exception as e: log.exception(f"range_monitor {symbol}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)


@_timed('six_pct')
async def six_pct_report_job(context):
    if not _exchange.ok() or not CHAT_ID: return
    results = await compute_6pct_ranges()
    if not results: return
    try:
        await context.bot.send_message(chat_id=CHAT_ID,
            text=build_6pct_report(results), parse_mode="HTML")
    except Exception as e: log.error(f"6pct: {e}")


async def morning_report_job(context):
    if not _exchange.ok() or not CHAT_ID: return
    log.info("🌅 Morning report ...")
    try:
        await context.bot.send_message(chat_id=CHAT_ID,
            text=(f"🌅 <b>تقرير الصباح</b>\n🇸🇾 {syria_now_str()}\n"
                  f"📊 {len(ALL_SYMBOLS)} عملة"), parse_mode="HTML")
    except Exception: pass
    for sym in ALL_SYMBOLS:
        try:
            buf = await generate_sr_chart(sym)
            if not buf: continue
            await context.bot.send_photo(chat_id=CHAT_ID, photo=buf,
                caption=f"📊 <b>{short(sym)}</b>", parse_mode="HTML",
                reply_markup=coin_menu_kb(sym))
            _filter_stats["charts_sent"] += 1
        except Exception as e: log.error(f"morning {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)


async def six_hour_report_job(context):
    """🆕 v7.9.17 — تقرير كل 6 ساعات (زبدة النتائج فقط)"""
    if _6h is None:
        log.warning("six_hour_report module not loaded")
        return
    if not CHAT_ID:
        return
    try:
        await _6h.send_report(context.bot, CHAT_ID)
        log.info("📤 Six-hour report sent")
    except Exception as e:
        log.error(f"six_hour_report: {e}")


async def sync_manual_positions_job(context):
    """🆕 {__version_full__} — Job منفصل للمزامنة (no يعطّل cross)."""
    if not _binance.ok: return
    try:
        await _sync_manual_positions(context.bot)
    except Exception as e:
        log.warning(f"sync_job: {e}")


async def binance_position_monitor_job(context):
    if not _binance.ok: return
    try: await manage_open_positions(bot=context.bot)
    except Exception as e: log.exception(f"position_monitor: {e}")


async def sz_manage_job(context):
    """🆕 v6.8.0 — إدارة ذكية كل 30 ثانية"""
    if not SMART_ZONES_ENABLED: return
    try:
        await sz_manage_open_positions(bot=context.bot)
    except Exception as e:
        log.warning(f"sz_manage_job: {e}")


@_timed('fast_tp_sl')
async def _manage_quick_profit():
    """🆕 v7.9.52 — مراقبة صفقات الربح السريع وإغلاقها عند +0.5 USDT"""
    if not _binance.ok or not _quick_profit_targets:
        return
    for bsym, data in list(_quick_profit_targets.items()):
        if data.get("processing"):
            continue
        data["processing"] = True
        try:
            pos = _get_actual_position(bsym)
            if not pos:
                _quick_profit_targets.pop(bsym, None)
                continue
            
            pnl = float(pos.get("unrealized_pnl", 0))
            if pnl < data["target_usd"]:
                continue
            
            t = data["ticket"]
            side = t["side"]
            close_side = "SELL" if side == "LONG" else "BUY"
            qty = abs(float(pos["amount"]))
            info = _binance.info(bsym)
            qty_s = _binance.fmt_qty(_binance.round_qty(qty, info["step_size"]), info["qty_precision"]) if info else f"{qty}"
            _cur_price = float(pos.get("mark_price", 0))
            
            log.info(f"💰 Quick Profit hit: {bsym} @ PnL={pnl:.3f} USDT")
            await _safe_close_market(bsym, close_side, qty_s)
            _log_trade_stat(t, _cur_price, "QUICK_PROFIT")
            _record_risk_from_ticket(t, _cur_price, "QUICK_PROFIT")
            _remove_ticket(t["id"])
            _quick_profit_targets.pop(bsym, None)
            
            # 🆕 v8.0.22 — إشعار Telegram
            if CHAT_ID:
                try:
                    import telegram
                    _bot = telegram.Bot(token=BOT_TOKEN)
                    _entry = float(t.get("entry", 0))
                    _pnl_pct = ((_cur_price - _entry) if side == "LONG" else (_entry - _cur_price)) / _entry * 100 if _entry else 0
                    await _bot.send_message(
                        chat_id=CHAT_ID,
                        text=(f"💰 <b>ربح سريع — {short(bsym)}</b>\n"
                              f"🇸🇾 {syria_now_str()}\n"
                              f"━━━━━━━━━━━━━━━━━━━\n"
                              f"🎯 الهدف: +0.5 USDT\n"
                              f"✅ النتيجة: {pnl:+.3f} USDT ({_pnl_pct:+.2f}%)\n"
                              f"💰 دخول: {fmt_price(_entry)}\n"
                              f"💵 خروج: {fmt_price(_cur_price)}"),
                        parse_mode="HTML")
                except Exception as _ne:
                    log.warning(f"quick_profit notify: {_ne}")
        except Exception as e:
            log.debug(f"quick_profit {bsym}: {e}")
        finally:
            if bsym in _quick_profit_targets:
                _quick_profit_targets[bsym]["processing"] = False


async def _check_breakeven():
    """v8.0.87d-TEMP — Break-even معطّل مؤقتاً."""
    return  # ⚠️ TEMP: disabled to protect TP1
    if not _binance.ok:
        return
    with _ticket_lock:
        tickets = [t for t in TRADE_TICKETS.values()
                   if not t.get("manual") and not t.get("breakeven_moved")]
    for t in tickets:
        try:
            _entry = float(t.get("entry", 0))
            if _entry <= 0:
                continue
            _side = t.get("side")
            _bsym = t["binance_symbol"]
            try:
                _tk = await asyncio.to_thread(
                    _binance.client.futures_symbol_ticker, symbol=_bsym)
                _cur = float(_tk["price"])
            except Exception:
                continue
            if _side == "LONG":
                _move_pct = (_cur - _entry) / _entry * 100
            else:
                _move_pct = (_entry - _cur) / _entry * 100
            if _move_pct < 0.4:
                continue
            _info = _binance.info(_bsym)
            if not _info:
                continue
            _qty_s = _binance.fmt_qty(
                _binance.round_qty(t["qty"], _info["step_size"]),
                _info["qty_precision"])
            _order_side = "BUY" if _side == "LONG" else "SELL"
            try:
                await _cancel_all_open_orders(_bsym)
            except Exception:
                pass
            await asyncio.sleep(0.5)
            try:
                await _binance_call(_binance.place_stop_loss,
                                    _bsym, _order_side, _entry, _qty_s)
                t["breakeven_moved"] = True
                t["sl"] = _entry
                _save_state()
                log.info(f"🛡️ Break-even {_bsym}: SL -> entry {_entry:.6f} (move {_move_pct:+.2f}%)")
                if CHAT_ID and _global_bot:
                    try:
                        await _global_bot.send_message(
                            chat_id=CHAT_ID,
                            text="🛡️ Break-even " + short(t['symbol']) + ": SL -> " + fmt_price(_entry),
                            parse_mode="HTML")
                    except Exception:
                        pass
            except Exception as _e:
                log.warning(f"breakeven place {_bsym}: {_e}")
        except Exception as _e:
            log.debug(f"breakeven {t.get('binance_symbol')}: {_e}")


async def fast_tp_sl_job(context):
    """v8.0.87c — فحص سريع + break-even."""
    if not FAST_MONITOR_ENABLED or not _binance.ok: return
    if is_paused():
        log.debug("fast_tp_sl: paused — skip")
        return
    # v8.0.87k — lock check removed (dead code)
    await _manage_quick_profit() # 🆕
    try:
        await _check_breakeven()
    except Exception as _be:
        log.debug(f"breakeven hook: {_be}")
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    if not tickets: return

    for t in tickets:
        # 🆕 v8.0.35 — تخطى الصفقات اليدوية
        if t.get("manual") or t.get("source") == "manual_sync":
            continue
        # 🆕 v6.8.3 — تخطى إذا Binance تُدير
        if _is_managed_by_binance(t):
            continue
        # 🆕 v7.9.36 — إذا كان bot_managed، راقب SL/TP يدوياً
        if t.get("tp_status") == "bot_managed":
            # 🆕 v8.0.85 — تعريف price قبل الاستخدام
            try:
                _ticker = await asyncio.to_thread(
                    _binance.client.futures_symbol_ticker,
                    symbol=t["binance_symbol"])
                price = float(_ticker["price"])
            except Exception:
                continue
            # تحقق من SL
            _sl = float(t.get("sl", 0))
            if _sl > 0:
                _bsym = t["binance_symbol"]
                if (t["side"] == "LONG" and price <= _sl) or \
                   (t["side"] == "SHORT" and price >= _sl):
                    log.warning(f"🛑 SL (bot-managed) hit: {_bsym} @ {_sl}")
                    try:
                        _info = _binance.info(_bsym)
                        _qty_s = _binance.fmt_qty(
                            _binance.round_qty(t["qty"], _info["step_size"]),
                            _info["qty_precision"])
                        await _safe_close_market(_bsym, "SELL" if t["side"] == "LONG" else "BUY", _qty_s)
                        _remove_ticket(t["id"])
                        _filter_stats["trades_closed"] += 1
                        _filter_stats["trades_sl"] += 1
                        _log_trade_stat(t, _sl, "SL (bot-managed)")
                        _record_risk_from_ticket(t, _sl, "SL")
                        log.info(f"✅ SL (bot-managed) closed {_bsym}")
                    except Exception as _e:
                        log.error(f"❌ Manual SL close failed: {_e}")
                    continue
            continue  # لا تتابع إذا كانت الصفقة bot_managed (تم التعامل معها أعلاه)
        # 🆕 v6.9.0 — تخطى TP2 bot-managed (يتولاه _manage_smart_tp2)
        if t.get("tp_status") == "tp1_bot":
            continue
        try:
            ticker = await asyncio.to_thread(
                _binance.client.futures_symbol_ticker,
                symbol=t["binance_symbol"])
            price = float(ticker["price"])
            tp_dist = abs(price - t["tp"]) / t["tp"] * 100
            sl_dist = abs(price - t["sl"]) / t["sl"] * 100
            if tp_dist > FAST_MONITOR_DIST_PCT and sl_dist > FAST_MONITOR_DIST_PCT:
                continue

            hit = False; reason = None
            if t["side"] == "LONG":
                if price <= t["sl"] and not t.get("half_sl_mode"): hit, reason = True, CLOSE_REASON_SL
                elif price >= t["tp"]: hit, reason = True, CLOSE_REASON_TP
            else:
                if price >= t["sl"] and not t.get("half_sl_mode"): hit, reason = True, CLOSE_REASON_SL
                elif price <= t["tp"]: hit, reason = True, CLOSE_REASON_TP
            if not hit:
                # 🆕 v9.33.4 — Split Exit (إغلاق 50% عند 2% لصفقات Supertrend)
                if t.get("source") == "supertrend" and not t.get("half_closed"):
                    try:
                        _entry_se = float(t["entry"])
                        _pnl_pct_se = ((price - _entry_se) / _entry_se * 100) if t["side"] == "LONG" else ((_entry_se - price) / _entry_se * 100)
                        if _pnl_pct_se >= 2.0:
                            _half_qty = float(t["qty"]) * 0.5
                            _info_se = _binance.info(t["binance_symbol"])
                            _qty_s_se = _binance.fmt_qty(_binance.round_qty(_half_qty, _info_se["step_size"]), _info_se["qty_precision"])
                            _close_side_se = "SELL" if t["side"] == "LONG" else "BUY"
                            await _safe_close_market(t["binance_symbol"], _close_side_se, _qty_s_se)
                            t["qty"] = float(t["qty"]) - _half_qty
                            t["half_closed"] = True
                            t["sl"] = _entry_se
                            t["breakeven_moved"] = True
                            _save_state()
                            log.info(f"💰 Split Exit: {t['binance_symbol']} أُغلق 50% عند 2% ربح")
                            if CHAT_ID:
                                try:
                                    await context.bot.send_message(chat_id=CHAT_ID, text=f"💰 Split Exit: {t['symbol']} تم جني 50% من الأرباح عند 2%")
                                except: pass
                    except Exception as e:
                        log.warning(f"split exit {t['binance_symbol']}: {e}")

                # 🆕 v9.20.0 — إدارة Break-even و Trailing Stop
                try:
                    _entry = float(t["entry"])
                    _pnl_pct = ((price - _entry) / _entry * 100) if t["side"] == "LONG" else ((_entry - price) / _entry * 100)
                    
                    # 🆕 v9.31.0 — Break-even مخصص لصفقات Supertrend عند 0.5%
                    _be_target = 0.5 if t.get("source") == "supertrend" else 0.3
                    if _pnl_pct >= _be_target and not t.get("breakeven_moved"):
                        _new_sl = _entry
                        _info = _binance.info(t["binance_symbol"])
                        _qty_s = _binance.fmt_qty(_binance.round_qty(t["qty"], _info["step_size"]), _info["qty_precision"])
                        try:
                            await _cancel_all_open_orders(t["binance_symbol"])
                            await _binance_call(_binance.place_stop_loss, t["binance_symbol"], "BUY" if t["side"] == "SHORT" else "SELL", _new_sl, _qty_s)
                            t["sl"] = _new_sl
                            t["breakeven_moved"] = True
                            _save_state()
                            log.info(f"🛡️ Break-even: {t['binance_symbol']} SL -> {_new_sl}")
                            if CHAT_ID:
                                try:
                                    await context.bot.send_message(chat_id=CHAT_ID, text=f"🛡️ Break-even: {t['symbol']} تم تأمين الصفقة عند الدخول")
                                except: pass
                        except Exception as e:
                            log.warning(f"breakeven {t['binance_symbol']}: {e}")

                    # 2. Trailing Stop: إذا وصل الربح 0.6%، نلاحق السعر
                    if _pnl_pct >= 0.6 and t.get("breakeven_moved"):
                        _trail_dist = price * 0.004  # مسافة 0.4%
                        _new_sl = (price - _trail_dist) if t["side"] == "LONG" else (price + _trail_dist)
                        _current_sl = float(t["sl"])
                        
                        # نحدث الوقف إذا كان أفضل من الحالي
                        if (t["side"] == "LONG" and _new_sl > _current_sl) or (t["side"] == "SHORT" and _new_sl < _current_sl):
                            _info = _binance.info(t["binance_symbol"])
                            _qty_s = _binance.fmt_qty(_binance.round_qty(t["qty"], _info["step_size"]), _info["qty_precision"])
                            try:
                                await _cancel_all_open_orders(t["binance_symbol"])
                                await _binance_call(_binance.place_stop_loss, t["binance_symbol"], "BUY" if t["side"] == "SHORT" else "SELL", _new_sl, _qty_s)
                                t["sl"] = _new_sl
                                _save_state()
                                log.info(f"🚀 Trailing Stop: {t['binance_symbol']} SL -> {_new_sl}")
                            except Exception:
                                pass
                except Exception as e:
                    log.debug(f"trailing {t.get('binance_symbol')}: {e}")

                continue

            # تأكيد بشمعة 1m مكتملة
            klines = await _binance_call(_binance.get_klines, t["binance_symbol"], "1m", 2)
            k = klines[-2]
            hi = float(k[2]); lo = float(k[3])
            confirmed = False
            if t["side"] == "LONG":
                if reason == CLOSE_REASON_TP and hi >= t["tp"]: confirmed = True
                if reason == CLOSE_REASON_SL and lo <= t["sl"]: confirmed = True
            else:
                if reason == CLOSE_REASON_TP and lo <= t["tp"]: confirmed = True
                if reason == CLOSE_REASON_SL and hi >= t["sl"]: confirmed = True
            if not confirmed: continue

            # 🆕 {__version_full__} — تحقق من الكمية الفعلية قبل reduce-only
            _actual = _get_actual_position(t["binance_symbol"])
            if not _actual:
                log.warning(f"⚠️ {t['id']}: no مركز فعلي — تخطي")
                _remove_ticket(t["id"])
                continue
            real_qty = abs(_actual["amount"])
            if real_qty < t["qty"] * 0.98:
                log.warning(f"⚠️ {t['id']}: qty {t['qty']} > real {real_qty}")
                t["qty"] = real_qty
            if t["qty"] <= 0:
                _remove_ticket(t["id"])
                continue

            info = _binance.info(t["binance_symbol"])
            qty_s = _binance.fmt_qty(
                _binance.round_qty(t["qty"], info["step_size"]),
                info["qty_precision"]) if info else f"{t['qty']}"
            close_side = "SELL" if t["side"] == "LONG" else "BUY"
            try:
                res = await _safe_close_market(t["binance_symbol"], close_side, qty_s)
            except Exception as _ce:
                log.warning(f"close_market {t['id']}: {_ce}")
                continue
            close_price = res["avg_price"] or price

            if t["side"] == "LONG":
                pnl_pct = (close_price - t["entry"]) / t["entry"] * 100
            else:
                pnl_pct = (t["entry"] - close_price) / t["entry"] * 100
            age_min = (_time.time() - t["opened_at"]) / 60
            # v7.2.0: log stats
            _log_trade_stat(t, close_price, reason)
            # 🩹 FIX v7.9.6: record risk
            _record_risk_from_ticket(t, close_price, reason)

            _remove_ticket(t["id"])
            _filter_stats["trades_closed"] += 1
            if reason == CLOSE_REASON_TP: _filter_stats["trades_tp"] += 1
            else: _filter_stats["trades_sl"] += 1
            await _cancel_all_open_orders(t["binance_symbol"])

            try:
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=build_close_msg(t, close_price, 0,
                                         pnl_pct, reason, age_min),
                    parse_mode="HTML")
            except Exception: pass
        except Exception as e:
            log.warning(f"fast_tp_sl {t['id']}: {e}")


async def cmd_orders(update, context):
    """v7.9.1 — عرض Regular + Algo orders."""
    if not _binance.ok:
        await _reply(update, "❌ Binance غير متصل."); return
    regular = []
    algo = []
    try:
        regular = await asyncio.to_thread(
            _binance.client.futures_get_open_orders)
    except Exception as e:
        log.warning(f"orders: {e}")
    try:
        algo = await asyncio.to_thread(
            _binance.client.futures_get_open_algo_orders)
    except (AttributeError, Exception):
        algo = []
    total = len(regular) + len(algo)
    if total == 0:
        await _reply(update, "✅ no أوامر معلقة.")
        return
    lines = [f"📋 <b>أوامر معلقة ({total})</b>",
             "━━━━━━━━━━━━━━━━━━━"]
    for o in regular:
        side_icon = "🟢" if o["side"] == "BUY" else "🔴"
        price = o.get("stopPrice") or o.get("price") or "—"
        lines.append(f"\n{side_icon} <b>{o['symbol']}</b> [R]\n"
                     f"   {o['type']} | {o['side']}\n"
                     f"   qty={o['origQty']} | price={price}\n"
                     f"   🆔 {o.get('orderId', '?')}")
    for o in algo:
        side_icon = "🟢" if o["side"] == "BUY" else "🔴"
        price = o.get("triggerPrice") or o.get("stopPrice") or "—"
        lines.append(f"\n{side_icon} <b>{o['symbol']}</b> [A]\n"
                     f"   {o.get('orderType', o.get('type'))} | {o['side']}\n"
                     f"   qty={o.get('quantity')} | trigger={price}\n"
                     f"   🆔 algo={o.get('algoId', '?')}")
    lines.append("\n<b>💡 للإلغاء:</b>")
    lines.append("<code>/cancel DOGEUSDT</code>")
    await _reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_cancel(update, context):
    """🆕 v6.7.0 — إلغاء أوامر معلقة لرمز."""
    if not _binance.ok:
        await _reply(update, "❌ Binance غير متصل."); return
    args = context.args or []
    if not args:
        await _reply(update, "الاستخدام: <code>/cancel DOGEUSDT</code>",
                     parse_mode="HTML"); return
    bsym = args[0].upper()
    try:
        orders = await asyncio.to_thread(
            _binance.client.futures_get_open_orders, symbol=bsym)
        algo = []
        try:
            algo = await asyncio.to_thread(
                _binance.client.futures_get_open_algo_orders, symbol=bsym)
        except (AttributeError, Exception):
            pass
        if not orders and not algo:
            await _reply(update, f"✅ no أوامر على {bsym}."); return
        cancelled = 0
        total = len(orders) + len(algo)
        for o in orders:
            try:
                await asyncio.to_thread(
                    _binance.client.futures_cancel_order,
                    symbol=bsym, orderId=o["orderId"])
                cancelled += 1
            except Exception as e:
                log.warning(f"cancel regular {o.get('orderId')}: {e}")
        for o in algo:
            aid = o.get("algoId")
            if not aid:
                continue
            try:
                await asyncio.to_thread(
                    _binance.client.futures_cancel_algo_order,
                    symbol=bsym, algoId=aid)
                cancelled += 1
            except Exception as e:
                log.warning(f"cancel algo {aid}: {e}")
        await _reply(update, f"✅ أُلغي {cancelled}/{total} أمر.")
    except Exception as e:
        await _reply(update, f"❌ {e}")


async def cmd_adopt(update, context):
    """🆕 v6.7.0 — تبنّي trade يدوية بأوامر مخصصة."""
    if not _binance.ok:
        await _reply(update, "❌ Binance غير متصل."); return
    args = context.args or []
    if len(args) < 3:
        await _reply(update,
            "الاستخدام: <code>/adopt XRP sl=0.50 tp=0.55</code>\n"
            "أو بدون قيم: <code>/adopt XRP</code> (حساب آلي)",
            parse_mode="HTML")
        return
    coin = args[0].upper()
    bsym = coin if coin.endswith("USDT") else coin + "USDT"
    ccxt_sym = f"{coin}/USDT:USDT"
    actual = _get_actual_position(bsym)
    if not actual:
        await _reply(update, f"❌ no مركز على {bsym}."); return

    # استخراج SL/TP من args
    sl_val = None; tp_val = None
    for a in args[1:]:
        if a.startswith("sl="):
            try: sl_val = float(a[3:])
            except: pass
        elif a.startswith("tp="):
            try: tp_val = float(a[3:])
            except: pass

    side = "LONG" if actual["amount"] > 0 else "SHORT"
    if sl_val is None or tp_val is None:
        levels = await _compute_levels_for_manual(ccxt_sym, side)
        if not levels:
            await _reply(update, "❌ فشل حساب SL/TP."); return
        sl_val = sl_val or levels["stop_loss"]
        tp_val = tp_val or levels["tp1"]

    qty = abs(actual["amount"])
    entry = actual["entry_price"] or actual["mark_price"]
    sl_pct = abs(entry - sl_val) / entry * 100

    tid = _mk_ticket_id(bsym, side) + "_ADOPT"
    ticket = {
        "id": tid, "symbol": ccxt_sym, "binance_symbol": bsym,
        "side": side, "qty": qty, "entry": entry,
        "sl": sl_val, "tp": tp_val, "sl_pct": round(sl_pct, 3),
        "score": 0, "source": "manual_sync",
        "tf": "adopt", "market": None,
        "opened_at": _time.time(), "manual": True, "order_id": None,
        "notional": qty * entry, "leverage": LEVERAGE, "signal": None,
    }
    _add_ticket(ticket)
    await _reply(update,
        f"✅ تم تبنّي <b>{coin}</b>\n"
        f"📌 {side} | qty={qty}\n"
        f"🎯 TP: {fmt_price(tp_val)}\n"
        f"🛑 SL: {fmt_price(sl_val)} (-{sl_pct:.3f}%)\n"
        f"🆔 <code>{tid[-20:]}</code>",
        parse_mode="HTML")


# ═══════════════════════════════════════════════════════════
# 24) Health
# ═══════════════════════════════════════════════════════════
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(f"OK - Unified Smart Bot {__version_full__}".encode())  # 🩹 FIX #5
    def do_HEAD(self):
        self.send_response(200); self.end_headers()
    def log_message(self, *a): pass


def run_health():
    port = int(os.getenv("PORT", "8081"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()


# ═══════════════════════════════════════════════════════════
# 25) Main
# ═══════════════════════════════════════════════════════════


async def _recovery_job(context):
    """🩹 FIX: async wrapper for run_once."""
    try:
        await _recover_tickets_after_restart()
    except Exception as e:
        log.warning(f"recovery_job: {e}")


async def cmd_resume_risk(update, context):
    """▶️ استئناف البوت بعد إيقاف RiskManager"""
    try:
        was_paused = _risk_mgr.paused
        reason = _risk_mgr.paused_reason
        _risk_mgr.resume()
        status = _risk_mgr.status()
        await _reply(update,  # 🩹 FIX #17
            f"▶️ <b>تم الاستئناف</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{'⚠️ كان موقوفاً: ' + reason if was_paused else '✅ لم يكن موقوفاً'}\n"
            f"🔄 الخسائر المتتالية: <b>{status['consec_losses']}</b> → <b>0</b>\n"
            f"💰 PnL اليوم: <b>${status['daily_pnl']:+.2f}</b>\n"
            f"🛡️ الحالة: <b>{'⏸️ موقوف' if status['paused'] else '▶️ يعمل'}</b>",
            parse_mode="HTML")
    except Exception as e:
        await _reply(update, f"❌ خطأ: {e}")


async def cmd_scalper(update, context):
    """🆕 v8.0.25 — رادار Scalping على عملة محددة"""
    global _scalper_tf
    if _scalper_session["active"]:
        await _reply(update, "⚠️ هناك جلسة نشطة. أوقفها أولاً بـ /scalper_stop")
        return
    
    args = context.args or []
    
    # 🆕 معالجة /scalper set <tf>
    if args and args[0].lower() == "set":
        if len(args) < 2 or args[1].lower() not in ("5m", "15m", "1h"):
            await _reply(update, "الاستخدام: <code>/scalper set 5m</code> أو <code>15m</code> أو <code>1h</code>", parse_mode="HTML")
            return
        _scalper_tf = args[1].lower()
        await _reply(update, f"✅ تم تغيير الإطار إلى <b>{_scalper_tf}</b>", parse_mode="HTML")
        return
    
    targets = []
    if args:
        sym = find_symbol_by_name(args[0])
        if not sym: 
            await _reply(update, f"❌ {args[0]} غير معروف")
            return
        targets = [sym]
    else:
        # 🆕 v8.0.45 — السكالبينج يستخدم SCALPER_SYMBOLS
        targets = SCALPER_SYMBOLS
    
    await _reply(update, f"🔍 جاري فحص {[short(s) for s in targets]}...")
    found = []
    for sym in targets:
        res = await _detect_range_radar(sym, tf=_scalper_tf)
        if res: found.append(res)
    
    if not found:
        await _reply(update, "📭 لا توجد فرص Scalping حالياً")
        return
    
    r = found[0]
    msg = (
        f"🎯 <b>فرصة Scalping — {short(r['symbol'])}</b>\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 ADX: {r['adx']:.1f} | ATR: {r['atr']:.2f}%\n"
        f"📈 النطاق: {fmt_price(r['low'])} — {fmt_price(r['high'])} ({r['width_pct']:.2f}%)\n"
        f"🎯 المنتصف: {fmt_price(r['mid'])}\n"
        f"💵 السعر: {fmt_price(r['price'])}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💰 الحجم: ${r['notional']:.0f} (20x)\n"
        f"🎯 الهدف: {r['target_pct']:.2f}% (≈ +$0.50)\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"<i>سيفتح صفقتي LONG + SHORT (مستقل عن البوت)</i>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ابدأ الجلسة", callback_data=f"scalper:start:{r['symbol']}")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="scalper:cancel")]
    ])
    await _reply(update, msg, parse_mode="HTML", reply_markup=kb)


async def cmd_scalper_stop(update, context):
    if not _scalper_session["active"]:
        await _reply(update, "لا توجد جلسة نشطة")
        return
    await _close_scalper_session(context.bot, "يدوي")
    await _reply(update, "✅ تم إيقاف الجلسة")


async def cmd_paperonly(update, context):
    """🆕 v8.0.59 — تفعيل/تعطيل وضع Paper Only"""
    global PAPER_ONLY_MODE
    args = context.args or []
    if not args:
        status = "✅ مفعل (كل الصفقات إلى Paper)" if PAPER_ONLY_MODE else "⏸️ معطل (تنفيذ حقيقي)"
        await _reply(update, f"📄 وضع Paper Only: {status}\n"
                              f"للتغيير: <code>/paperonly on</code> أو <code>/paperonly off</code>",
                     parse_mode="HTML")
        return
    if args[0].lower() == "on":
        PAPER_ONLY_MODE = True
        await _reply(update, "📄 <b>تم تفعيل Paper Only</b>\n"
                              "⚡ كل الإشارات ستُسجل في Paper (بدون تنفيذ حقيقي)\n"
                              "🔄 للعودة: <code>/paperonly off</code>",
                     parse_mode="HTML")
    elif args[0].lower() == "off":
        PAPER_ONLY_MODE = False
        await _reply(update, "✅ <b>تم تعطيل Paper Only</b>\n"
                              "⚡ التنفيذ الحقيقي مفعّل مرة أخرى",
                     parse_mode="HTML")


async def cmd_orphan(update, context):
    """🆕 v7.9.53 — تفعيل/تعطيل تنظيف الأوامر اليتيمة"""
    global _orphan_cleanup_enabled
    args = context.args or []
    if not args:
        status = "✅ مفعل" if _orphan_cleanup_enabled else "⏸️ معطل"
        await _reply(update, f"🧹 تنظيف الأوامر: {status}\n"
                              f"للتغيير: <code>/orphan on</code> أو <code>/orphan off</code>",
                     parse_mode="HTML")
        return
    if args[0].lower() == "on":
        _orphan_cleanup_enabled = True
        await _reply(update, "✅ تنظيف الأوامر اليتيمة مفعل")
    elif args[0].lower() == "off":
        _orphan_cleanup_enabled = False
        await _reply(update, "⏸️ تنظيف الأوامر اليتيمة معطل\n"
                              f"⚠️ أوامر البوت المعلقة لن تُلغى تلقائياً")


async def cmd_risk(update, context):
    """📊 عرض حالة RiskManager"""
    try:
        s = _risk_mgr.status()
        pause_icon = "⏸️" if s["paused"] else "▶️"
        await _reply(update,  # 🩹 FIX #17
            f"🛡️ <b>حالة إدارة المخاطرة</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{pause_icon} الحالة: <b>{'موقوف' if s['paused'] else 'يعمل'}</b>\n"
            f"{'⚠️ السبب: ' + s['reason'] if s['reason'] else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📉 الخسائر المتتالية: <b>{s['consec_losses']}</b> / {_risk_mgr.max_consec_losses}\n"
            f"💰 PnL اليوم: <b>${s['daily_pnl']:+.2f}</b> / -${_risk_mgr.max_daily_loss}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"<b>الحدود:</b>\n"
            f"• Max Positions: <b>{_risk_mgr.max_concurrent}</b>\n"
            f"• Max Same Direction: <b>{_risk_mgr.max_same_dir}</b>\n"
            f"• Max Consec Losses: <b>{_risk_mgr.max_consec_losses}</b>\n"
            f"• Max Daily Loss: <b>${_risk_mgr.max_daily_loss}</b>",
            parse_mode="HTML")
    except Exception as e:
        await _reply(update,f"❌ خطأ: {e}")



@_timed('cleanup_dust')
async def _cleanup_dust_job(context):
    """🆕 v7.9.44 — يغلق أي مركز < $5 + يلغي أوامره (كل ساعة)."""
    if not _binance.ok:
        return
    if is_paused():
        log.debug("cleanup_dust: paused — skip")
        return
    try:
        try:
            positions = await _binance_call(_binance.get_open_positions)
        except Exception as _e:
            log.error(f"❌ cleanup_dust: get_open_positions failed: {_e}")
            return
        _dust = []
        for p in positions:
            _notional = abs(float(p.get("amount", 0))) * float(p.get("mark_price", 0) or 0)
            if 0 < _notional < 5:
                _dust.append((p["symbol"], _notional))
        if not _dust:
            log.debug("cleanup_dust: no dust")
            return
        for _sym, _notional in _dust:
            _pos = next((p for p in positions if p["symbol"] == _sym), None)
            if not _pos:
                continue
            _qty = abs(float(_pos["amount"]))
            _side = "SELL" if _pos["amount"] > 0 else "BUY"
            _info = _binance.info(_sym)
            _qty_s = _binance.fmt_qty(
                _binance.round_qty(_qty, _info["step_size"]),
                _info["qty_precision"]) if _info else f"{_qty}"
            try:
                await _safe_close_market(_sym, _side, _qty_s)
                log.warning(f"🧹 DUST closed: {_sym} ${_notional:.2f}")
                await _cancel_all_open_orders(_sym)
                if context.bot and CHAT_ID:
                    try:
                        await context.bot.send_message(
                            chat_id=CHAT_ID,
                            text=(f"🧹 <b>تنظيف غبار</b>\n"
                                  f"📌 {short(_sym)} — ${_notional:.2f}\n"
                                  f"✅ أُغلق + أُلغي أوامره"),
                            parse_mode="HTML")
                    except: pass
            except Exception as e:
                log.error(f"❌ DUST close FAILED {_sym} (${_notional:.2f}): {e}")
    except Exception as e:
        log.warning(f"cleanup_dust_job: {e}")


@_timed('orphan_orders')
async def _check_orphan_orders_job(context):
    """🆕 v7.9.44 — يلغي أوامر على رموز ليس لها مركز (كل 5 دقائق)."""
    if not _binance.ok:
        return
    try:
        positions = await _binance_call(_binance.get_open_positions)
        _active = {p["symbol"] for p in positions if abs(float(p.get("amount", 0))) > 1e-9}
        
        # Regular orders
        try:
            orders = await asyncio.to_thread(_binance.client.futures_get_open_orders)
            for o in orders:
                _s = o.get("symbol")
                _cid = (o.get("clientOrderId") or "")
                # 🆕 حماية: فقط أوامر البوت
                if not _cid.startswith("bot_"):
                    continue
                if _s and _s not in _active:
                    try:
                        await asyncio.to_thread(
                            _binance.client.futures_cancel_order,
                            symbol=_s, orderId=o["orderId"])
                        log.warning(f"🧹 orphan regular cancelled: {_s} {o.get('type')}")
                    except: pass
        except Exception: pass
        
        # Algo orders
        try:
            algo = await asyncio.to_thread(_binance.client.futures_get_open_algo_orders)
            for o in algo:
                _s = o.get("symbol")
                _aid = o.get("algoId")
                if _s and _aid and _s not in _active:
                    try:
                        await asyncio.to_thread(
                            _binance.client.futures_cancel_algo_order,
                            symbol=_s, algoId=_aid)
                        log.warning(f"🧹 orphan algo cancelled: {_s} {o.get('orderType')}")
                    except: pass
        except Exception: pass
    except Exception as e:
        log.warning(f"check_orphan_orders: {e}")


@_timed('cleanup_orphans')
async def _cleanup_orphan_orders_job(context):
    """🩹 FIX #15: cancel orders on symbols without positions."""
    if not _binance.ok:
        return
    if not _orphan_cleanup_enabled:
        return  # 🆕 v7.9.53
    try:
        positions = await _binance_call(_binance.get_open_positions)
        active_syms = {p["symbol"] for p in positions}
        orders = await asyncio.to_thread(
            _binance.client.futures_get_open_orders)
        if not orders:
            return
        cancelled = 0
        for o in orders:
            sym = o.get("symbol")
            _cid = (o.get("clientOrderId") or "")
            # 🆕 v8.0.27 — فقط أوامر البوت (حماية مزدوجة)
            if not _cid.startswith("bot_"):
                log.debug(f"⏭️ User order protected: {sym} {o.get('type')} cid={_cid}")
                continue
            if sym and sym not in active_syms:
                try:
                    await asyncio.to_thread(
                        _binance.client.futures_cancel_order,
                        symbol=sym, orderId=o["orderId"])
                    cancelled += 1
                    log.info(f"🧹 orphan bot order: {sym} {o['type']} cid={_cid[:20]}")
                except Exception as e:
                    log.debug(f"orphan cancel {sym}: {e}")
        if cancelled:
            log.info(f"🧹 Cleaned {cancelled} orphan orders")
    except Exception as e:
        log.warning(f"cleanup_orphans: {e}")


@_timed('unified_pos')
async def _cleanup_dead_tickets_job(context):
    """🆕 v8.0.37 — حذف التذاكر التي ليس لها مركز فعلي على Binance (كل 5 دقائق)."""
    if not _binance.ok:
        return
    try:
        positions = await _binance_call(_binance.get_open_positions)
    except Exception:
        return
    active = {p["symbol"] for p in positions if abs(float(p.get("amount", 0))) > 1e-9}
    with _ticket_lock:
        dead = [tid for tid, t in TRADE_TICKETS.items()
                if t.get("binance_symbol") not in active]
    for tid in dead:
        # v8.0.87f — إشعار قبل الحذف
        t = TRADE_TICKETS.get(tid)
        if t and not t.get("manual") and t.get("id") not in _closed_binance_ids:
            try:
                _sym = t.get("symbol", "?")
                _side = t.get("side", "?")
                _entry = float(t.get("entry", 0))
                _bsym = t.get("binance_symbol", "")
                try:
                    _cur = _binance.last_price(_bsym) if _bsym else 0
                except Exception:
                    _cur = 0
                _pnl = 0.0
                if _entry > 0 and _cur > 0:
                    if _side == "LONG":
                        _pnl = (_cur - _entry) / _entry * 100
                    else:
                        _pnl = (_entry - _cur) / _entry * 100
                if context.bot and CHAT_ID:
                    _msg = (
                        f"🔔 <b>إغلاق مكتشَف — {short(_sym)}</b>\n"
                        f"🇸🇾 {syria_now_str()}\n"
                        f"━━━━━━━━━━━━━━━━━━━\n"
                        f"{'🟢' if _side=='LONG' else '🔴'} {_side}\n"
                        f"💰 دخول: {fmt_price(_entry)}\n"
                        f"💵 حالي: {fmt_price(_cur) if _cur else '?'}\n"
                        f"{'🟢' if _pnl>=0 else '🔴'} PnL: <b>{_pnl:+.2f}%</b>\n"
                        f"⚠️ (sync detection)"
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=CHAT_ID, text=_msg, parse_mode="HTML")
                        log.info(f"📤 Cleanup notify sent: {_sym}")
                    except Exception as _ne:
                        log.warning(f"cleanup notify send: {_ne}")
            except Exception as _e:
                log.warning(f"cleanup notify: {_e}")
        log.info(f"🧹 Dead ticket removed (no position): {tid}")
        try:
            if t and t.get("binance_symbol"):
                await _cancel_all_open_orders(t["binance_symbol"])
                log.info(f"🧹 {t['binance_symbol']}: orders cancelled")
        except Exception as _ce:
            log.debug(f"cancel orders after dead ticket: {_ce}")
        _remove_ticket(tid)


async def unified_position_manager_job(context):
    """v8.0.87h — lock removed for reliability."""
    try:
        if not _binance.ok:
            return
        for name, fn in [
            ("sync",    _sync_manual_positions),
            ("detect",  _detect_binance_closes),
            ("manage",  manage_open_positions),
            ("sz",      sz_manage_open_positions),
        ]:
            if name == "sz" and not SMART_ZONES_ENABLED:
                continue
            try:
                await asyncio.wait_for(fn(context.bot), timeout=45)  # v8.0.91d: 20→45
            except asyncio.TimeoutError:
                log.warning(f"unified.{name}: timeout 20s")
            except asyncio.CancelledError:
                log.info(f"unified.{name} cancelled")
            except Exception as e:
                log.warning(f"unified.{name}: {e}")
    except asyncio.CancelledError:
        log.info("unified_pos cancelled")
    except Exception as e:
        log.warning(f"unified_pos: {e}")



# 🆕 v9.0.0 — BTC Sentinel State
# 🆕 v9.1.0 — Trailing Mode State
# 🆕 v9.1.6 — Trailing Mode State (محفوظ في ملف)
TRAILING_STATE_FILE = "/root/bot2/trailing_state.json"

def _load_trailing_state():
    try:
        import json as _j
        if not os.path.exists(TRAILING_STATE_FILE):
            return {}
        with io.open(TRAILING_STATE_FILE, "r", encoding="utf-8") as f:
            return _j.load(f)
    except Exception:
        return {}

def _save_trailing_state():
    try:
        import json as _j
        with io.open(TRAILING_STATE_FILE, "w", encoding="utf-8") as f:
            _j.dump(_trailing_mode, f)
    except Exception as e:
        log.warning(f"_save_trailing_state: {e}")

_trailing_mode = {
    "enabled": False,
    "max_loss_pct": 2.5,      # حد أقصى للخسارة
    "active_ticket": None,    # الصفقة المفتوحة في هذا الوضع
    "highest_profit": 0.0,    # أعلى ربح تم الوصول إليه
}

# تحميل الحالة من الملف عند بدء التشغيل
_loaded_trailing = _load_trailing_state()
if _loaded_trailing:
    _trailing_mode.update(_loaded_trailing)
    log.info(f"[Trailing] Restored: enabled={_trailing_mode['enabled']}, active={_trailing_mode['active_ticket']}")

_btc_state = {
    "emergency_until": 0,     # تعليق بسبب تقلب
    "trend_until": 0,         # تعليق بسبب كسر هيكل
    "trend_dir": None,        # UP/DOWN
    "pause_until": 0,         # تعليق بسبب تحرك BTC وحيد
    "last_notify": 0,         # لمنع تكرار الإشعارات
    "last_close_action": 0,   # لمنع تكرار الإغلاق
    "btc_last_candle": 0,     # لمعرفة الشمعة الأخيرة
    "prev_emergency": False,  # 🆕 v9.0.5 — لتتبع انتهاء الطوارئ
    "prev_trend": False,      # 🆕 v9.0.5 — لتتبع انتهاء الترند
    "prev_pause": False,      # 🆕 v9.0.5 — لتتبع انتهاء التعليق
}

async def btc_sentinel_check(bot):
    """🆕 v9.0.0 — فحص BTC وإدارة الحالة"""
    global _btc_state
    now = _time.time()
    log.info("btc_sentinel: START")
    
    # 1. جلب شموع BTC
    try:
        d5 = await fetch_ohlcv_cached("BTC/USDT:USDT", "5m", 30)
        d15 = await fetch_ohlcv_cached("BTC/USDT:USDT", "15m", 20)
        d_eth = await fetch_ohlcv_cached("ETH/USDT:USDT", "5m", 3)
        d_bnb = await fetch_ohlcv_cached("BNB/USDT:USDT", "5m", 3)
        if not d5 or not d15 or not d_eth or not d_bnb:
            log.warning(f"btc_sentinel: EXIT — data missing (d5={bool(d5)}, d15={bool(d15)}, d_eth={bool(d_eth)}, d_bnb={bool(d_bnb)})")
            return
        df5 = pd.DataFrame(d5, columns=["ts","o","h","l","c","v"])
        df15 = pd.DataFrame(d15, columns=["ts","o","h","l","c","v"])
        log.info(f"btc_sentinel: data OK (BTC 5m={len(d5)}, 15m={len(d15)})")
    except Exception as e:
        log.warning(f"btc_sentinel: EXIT — exception: {e}")
        return

    # --- الطريقة 1: انفجار التقلب ---
    # نأخذ الشمعة المغلقة (index -2)
    last_close = float(df5["c"].iloc[-2])
    last_open = float(df5["o"].iloc[-2])
    change_5m = abs(last_close - last_open) / last_open * 100

    if change_5m > 0.4:
        _btc_state["emergency_until"] = now + 900  # 15 دقيقة
        log.info(f"🚨 BTC Sentinel: تقلب {change_5m:.2f}% (5m) — تعليق الصفقات المعاكسة")
        if bot and CHAT_ID and (now - _btc_state["last_notify"]) > 300:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"🚨 <b>BTC Sentinel — تقلب مفاجئ</b>\n"
                          f"🇸🇾 {syria_now_str()}\n"
                          f"📉 التغير: <b>{change_5m:.2f}%</b> في 5m\n"
                          f"⚡ الحالة: تعليق الصفقات المعاكسة لمدة 15 دقيقة"),
                    parse_mode="HTML")
                _btc_state["last_notify"] = now
            except Exception: pass

    # --- الطريقة 2: كسر الهيكل (حقيقي فقط) ---
    lookback = df15.iloc[-22:-2]
    if len(lookback) >= 10:
        high_20 = float(lookback["h"].max())
        low_20 = float(lookback["l"].min())
        current_close = float(df15["c"].iloc[-2])
        current_open = float(df15["o"].iloc[-2])
        
        # شرط 1: حجم قوي (> 2.0x متوسط 20 شمعة) — كسر حقيقي
        try:
            vma = float(df15["v"].iloc[-22:-2].mean())
            v_now = float(df15["v"].iloc[-2])
            vol_strong = v_now > (vma * 2.0) if vma > 0 else False
        except Exception:
            vol_strong = False
        
        # شرط 2: هامش أمان (0.1% على الأقل)
        margin = 0.001  # 0.1%
        
        # شرط 3: استمرارية (3 شمعات في نفس الاتجاه)
        try:
            _last3 = df15.iloc[-5:-2]  # آخر 3 شمعات مغلقة
            _bull_count = sum(1 for _, r in _last3.iterrows() if r["c"] > r["o"])
            _bear_count = sum(1 for _, r in _last3.iterrows() if r["c"] < r["o"])
        except Exception:
            _bull_count = 0
            _bear_count = 0
        
        # شرط 4: تجميع مسبق (عرض بولينجر < 2%)
        try:
            _bb = calc_bollinger(df15)
            _bb_w = float(_bb["width"].iloc[-2])
            _consolidated = _bb_w < 2.0
        except Exception:
            _consolidated = False
        
        if current_close > high_20 * (1 + margin) and vol_strong and _bull_count >= 3 and _consolidated:
            _btc_state["trend_until"] = now + 3600
            _btc_state["trend_last_kick"] = now
            # 🆕 v9.10.2 — إرسال الإشعار مرة واحدة فقط (عند بدء الترند)
            if not _btc_state["trend_active"] or _btc_state["trend_dir"] != "UP":
                _btc_state["trend_dir"] = "UP"
                _btc_state["trend_active"] = True
                log.info(f"🔄 BTC Sentinel: كسر هيكل صعودي حقيقي (vol={v_now/vma if vma else 0:.2f}x)")
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(f"🚨 <b>بدأ ترند BTC — UP</b>\n"
                                  f"🇸🇾 {syria_now_str()}\n"
                                  f"━━━━━━━━━━━━━━━━━━━\n"
                                  f"🔄 كسر هيكل صعودي حقيقي\n"
                                  f"📊 الحجم: {v_now/vma if vma else 0:.2f}x\n"
                                  f"⏱️ الصلاحية: ساعة واحدة"),
                            parse_mode="HTML")
                    except Exception: pass
            else:
                # الترند مستمر، لا نرسل إشعار
                log.debug(f"BTC Sentinel: ترند UP مستمر (vol={v_now/vma if vma else 0:.2f}x)")
                
        elif current_close < low_20 * (1 - margin) and vol_strong and _bear_count >= 3 and _consolidated:
            _btc_state["trend_until"] = now + 3600
            _btc_state["trend_last_kick"] = now
            if not _btc_state["trend_active"] or _btc_state["trend_dir"] != "DOWN":
                _btc_state["trend_dir"] = "DOWN"
                _btc_state["trend_active"] = True
                log.info(f"🔄 BTC Sentinel: كسر هبوطي حقيقي (vol={v_now/vma:.1f}x, 3c={_bear_count}, bb={_bb_w:.1f}%)")
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(f"🚨 <b>بدأ ترند BTC — DOWN</b>\n"
                                  f"🇸🇾 {syria_now_str()}\n"
                                  f"━━━━━━━━━━━━━━━━━━━\n"
                                  f"🔄 كسر هبوطي حقيقي (3 شمعات)\n"
                                  f"📊 الحجم: {v_now/vma:.1f}x\n"
                                  f"📉 عرض BB: {_bb_w:.1f}%\n"
                                  f"⏱️ الصلاحية: ساعة"),
                            parse_mode="HTML")
                    except Exception: pass
        else:
            # إذا لم تكن الشروط مكتملة، لا نغيّر الحالة
            pass

    # --- الطريقة 3: الارتباط ---
    try:
        eth_close = float(d_eth[-2][4])
        eth_open = float(d_eth[-2][1])
        eth_change = abs(eth_close - eth_open) / eth_open * 100

        bnb_close = float(d_bnb[-2][4])
        bnb_open = float(d_bnb[-2][1])
        bnb_change = abs(bnb_close - bnb_open) / bnb_open * 100

        if change_5m > 0.5 and eth_change < 0.1 and bnb_change < 0.1:
            _btc_state["pause_until"] = now + 1800  # 30 دقيقة
            log.info(f"⚠️ BTC Sentinel: تحرك وحيد (BTC={change_5m:.2f}%)")
            if bot and CHAT_ID and (now - _btc_state["last_notify"]) > 300:
                try:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(f"⚠️ <b>BTC Sentinel — تحرك وحيد</b>\n"
                              f"🇸🇾 {syria_now_str()}\n"
                              f"📊 BTC: {change_5m:.2f}% | ETH: {eth_change:.2f}% | BNB: {bnb_change:.2f}%\n"
                              f"⚡ الحالة: تعليق كل الصفقات (30 دقيقة)"),
                        parse_mode="HTML")
                    _btc_state["last_notify"] = now
                except Exception: pass
    except Exception:
        pass

    # --- دمج 1 + 2 ---
    if _btc_state["emergency_until"] > now and _btc_state["trend_until"] > now:
        if now - _btc_state["last_close_action"] > 120:
            _btc_state["last_close_action"] = now
            await btc_auto_close_opposite(bot, _btc_state["trend_dir"])

    log.info("btc_sentinel: END (no event)")
    
    # 🆕 v9.0.5 — إشعار عند انتهاء الحالات
    _now = now
    _ended = []
    
    # انتهاء الطوارئ
    _is_emergency = _btc_state["emergency_until"] > _now
    if _btc_state["prev_emergency"] and not _is_emergency:
        _ended.append("🚨 انتهت حالة الطوارئ (تقلب BTC)")
    _btc_state["prev_emergency"] = _is_emergency
    
    # انتهاء الترند
    _is_trend = _btc_state["trend_until"] > _now
    if _btc_state["prev_trend"] and not _is_trend:
        _ended.append(f"🔄 انتهى ترند BTC ({_btc_state['trend_dir'] or '?'})")
        _btc_state["trend_dir"] = None
        _btc_state["trend_active"] = False  # 🆕 v9.10.2
    _btc_state["prev_trend"] = _is_trend
    
    # انتهاء التعليق
    _is_pause = _btc_state["pause_until"] > _now
    if _btc_state["prev_pause"] and not _is_pause:
        _ended.append("⚠️ انتهى تعليق BTC (تحرك وحيد)")
    _btc_state["prev_pause"] = _is_pause
    
    if _ended and bot and CHAT_ID:
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(f"✅ <b>BTC Sentinel — انتهت الحالة</b>\n"
                      f"🇸🇾 {syria_now_str()}\n"
                      f"━━━━━━━━━━━━━━━━━━━\n" +
                      "\n".join(f"• {e}" for e in _ended) +
                      f"\n\n⚡ البوت عاد للعمل الطبيعي"),
                parse_mode="HTML")
        except Exception as e:
            log.warning(f"btc_sentinel end notify: {e}")

    # 🆕 v9.0.4 — إشعار عند بدء التشغيل (دائماً، حتى لو لا توجد حالة نشطة)
    if not _btc_state.get("startup_notified"):
        _btc_state["startup_notified"] = True
        status = []
        if now < _btc_state["emergency_until"]:
            status.append(f"🚨 طوارئ حتى {datetime.fromtimestamp(_btc_state['emergency_until'], tz=SYRIA_TZ).strftime('%H:%M')}")
        if now < _btc_state["trend_until"]:
            status.append(f"🔄 ترند {_btc_state['trend_dir']} حتى {datetime.fromtimestamp(_btc_state['trend_until'], tz=SYRIA_TZ).strftime('%H:%M')}")
        if now < _btc_state["pause_until"]:
            status.append(f"⚠️ تعليق حتى {datetime.fromtimestamp(_btc_state['pause_until'], tz=SYRIA_TZ).strftime('%H:%M')}")
        if not status:
            status.append("✅ لا توجد حالة نشطة (السوق طبيعي)")
        if bot and CHAT_ID:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=(f"🛡️ <b>BTC Sentinel بدأ</b>\n"
                          f"🇸🇾 {syria_now_str()}\n"
                          f"━━━━━━━━━━━━━━━━━━━\n" +
                          "\n".join(f"• {s}" for s in status) +
                          f"\n⏱️ الفحص: كل 60 ثانية"),
                    parse_mode="HTML")
            except Exception as e:
                log.warning(f"btc_sentinel startup notify: {e}")


async def btc_auto_close_opposite(bot, direction):
    """🆕 v9.0.0 — إغلاق الصفقات المفتوحة عكس اتجاه BTC"""
    global _btc_state
    if not direction: return
    with _ticket_lock:
        tickets = list(TRADE_TICKETS.values())
    for t in tickets:
        if t.get("manual") or t.get("source") == "manual_sync":
            continue
        if direction == "UP" and t["side"] == "SHORT":
            log.warning(f"🔄 BTC Sentinel: إغلاق {t['side']} {t['symbol']} (عكس اتجاه BTC)")
            try:
                _close_side = "BUY"
                _info = _binance.info(t["binance_symbol"])
                _qty_s = _binance.fmt_qty(_binance.round_qty(t["qty"], _info["step_size"]), _info["qty_precision"])
                await _safe_close_market(t["binance_symbol"], _close_side, _qty_s)
                _remove_ticket(t["id"])
                _filter_stats["trades_closed"] += 1
                if bot and CHAT_ID:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(f"🔄 <b>إغلاق تلقائي — {short(t['symbol'])}</b>\n"
                              f"🇸🇾 {syria_now_str()}\n"
                              f"⚡ السبب: BTC كسر الهيكل (UP)\n"
                              f"📉 الصفقة كانت {t['side']} عكس الاتجاه"),
                        parse_mode="HTML")
            except Exception as e:
                log.warning(f"btc_auto_close: {e}")
        elif direction == "DOWN" and t["side"] == "LONG":
            log.warning(f"🔄 BTC Sentinel: إغلاق {t['side']} {t['symbol']} (عكس اتجاه BTC)")
            try:
                _close_side = "SELL"
                _info = _binance.info(t["binance_symbol"])
                _qty_s = _binance.fmt_qty(_binance.round_qty(t["qty"], _info["step_size"]), _info["qty_precision"])
                await _safe_close_market(t["binance_symbol"], _close_side, _qty_s)
                _remove_ticket(t["id"])
                _filter_stats["trades_closed"] += 1
                if bot and CHAT_ID:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(f"🔄 <b>إغلاق تلقائي — {short(t['symbol'])}</b>\n"
                              f"🇸🇾 {syria_now_str()}\n"
                              f"⚡ السبب: BTC كسر الهيكل (DOWN)\n"
                              f"📉 الصفقة كانت {t['side']} عكس الاتجاه"),
                        parse_mode="HTML")
            except Exception as e:
                log.warning(f"btc_auto_close: {e}")


async def btc_sentinel_job(context):
    """🆕 v9.0.0 — يعمل كل دقيقة"""
    try:
        await btc_sentinel_check(context.bot)
    except Exception as e:
        log.warning(f"btc_sentinel_job: {e}")


async def _can_execute_now_btc(cross):
    """🆕 v9.0.0 — فحص حالة BTC قبل التنفيذ"""
    now = _time.time()
    side = "LONG" if cross["direction"] == "bullish" else "SHORT"
    
    if now < _btc_state["pause_until"]:
        return False, "btc_sentinel: BTC يتحرك وحيداً"
        
    if now < _btc_state["emergency_until"]:
        # في حالة الطوارئ: نسمح فقط بالصفقات في اتجاه BTC
        return True, "OK"
        
    if now < _btc_state["trend_until"]:
        if _btc_state["trend_dir"] == "UP" and side == "SHORT":
            return False, "btc_sentinel: عكس اتجاه BTC (UP)"
        if _btc_state["trend_dir"] == "DOWN" and side == "LONG":
            return False, "btc_sentinel: عكس اتجاه BTC (DOWN)"
            
    return True, "OK"



async def _trailing_monitor(bot):
    """🆕 v9.1.0 — مراقبة الصفقة في وضع التتبع (Trailing Mode)"""
    global _trailing_mode
    if not _trailing_mode["enabled"]:
        return
    
    _ticket_id = _trailing_mode["active_ticket"]
    if not _ticket_id:
        return
    
    with _ticket_lock:
        t = TRADE_TICKETS.get(_ticket_id)
    if not t:
        _trailing_mode["active_ticket"] = None
        return
    
    # 1. مراقبة الهامش
    try:
        _bal = _binance.usdt_balance()
        if _bal and _bal.get("available", 0) < 5.0:
            log.warning(f"[Trailing] هامش منخفض ({_bal['available']:.2f}) — إغلاق")
            await _trailing_close(t, bot, "هامش منخفض")
            return
    except Exception:
        pass
    
    # 2. حساب الخسارة/الربح الحالي
    try:
        _cur_price = await _binance_call(_binance.last_price, t["binance_symbol"])
        if _cur_price <= 0:
            return
        _entry = float(t["entry"])
        if t["side"] == "LONG":
            _pnl_pct = (_cur_price - _entry) / _entry * 100
        else:
            _pnl_pct = (_entry - _cur_price) / _entry * 100
        
        if _pnl_pct > _trailing_mode["highest_profit"]:
            _trailing_mode["highest_profit"] = _pnl_pct
    except Exception:
        return
    
    # 3. فحص الحد الأقصى للخسارة
    if _pnl_pct <= -_trailing_mode["max_loss_pct"]:
        log.warning(f"[Trailing] Hard Stop ({_pnl_pct:.2f}%) — إغلاق {t['symbol']}")
        await _trailing_close(t, bot, f"Hard Stop {_pnl_pct:.2f}%")
        return
    
    # 4. فحص انعكاس BTC (الطريقة 1+2)
    _now = _time.time()
    _emergency = _btc_state["emergency_until"] > _now
    _trend = _btc_state["trend_until"] > _now
    _trend_dir = _btc_state["trend_dir"]
    
    _reverse = False
    if t["side"] == "LONG" and _trend_dir == "DOWN":
        _reverse = True
    elif t["side"] == "SHORT" and _trend_dir == "UP":
        _reverse = True
    
    if _reverse and (_emergency or _trend):
        log.warning(f"[Trailing] انعكاس BTC ({_trend_dir}) — إغلاق {t['symbol']}")
        await _trailing_close(t, bot, f"انعكاس BTC ({_trend_dir})")
        return


async def _trailing_close(t, bot, reason):
    """🆕 v9.1.0 — إغلاق الصفقة في وضع التتبع"""
    global _trailing_mode
    _bsym = t["binance_symbol"]
    try:
        _info = _binance.info(_bsym)
        _qty_s = _binance.fmt_qty(_binance.round_qty(t["qty"], _info["step_size"]), _info["qty_precision"])
        _close_side = "SELL" if t["side"] == "LONG" else "BUY"
        _res = await _safe_close_market(_bsym, _close_side, _qty_s)
        _cp = _res["avg_price"] or 0
        _entry = float(t["entry"])
        _pnl_pct = ((_cp - _entry) / _entry * 100) if t["side"] == "LONG" else ((_entry - _cp) / _entry * 100)
        _pnl_usdt = ((_cp - _entry) if t["side"] == "LONG" else (_entry - _cp)) * float(t["qty"])
        
        _log_trade_stat(t, _cp, f"TRAILING_{reason}")
        _record_risk_from_ticket(t, _cp, f"TRAILING_{reason}")
        _remove_ticket(t["id"])
        _filter_stats["trades_closed"] += 1
        await _cancel_all_open_orders(_bsym)
        
        _trailing_mode["active_ticket"] = None
        _trailing_mode["highest_profit"] = 0.0
        _save_trailing_state()
        
        if bot and CHAT_ID:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(f"🎯 <b>إغلاق (Trailing) — {short(t['symbol'])}</b>\n"
                      f"🇸🇾 {syria_now_str()}\n"
                      f"━━━━━━━━━━━━━━━━━━━\n"
                      f"📌 السبب: {reason}\n"
                      f"💰 الدخول: {fmt_price(_entry)}\n"
                      f"💵 الخروج: {fmt_price(_cp)}\n"
                      f"{'🟢' if _pnl_usdt >= 0 else '🔴'} <b>PnL: {_pnl_usdt:+.3f} USDT ({_pnl_pct:+.2f}%)</b>\n"
                      f"⚡ أعلى ربح محقق: {_trailing_mode['highest_profit']:.2f}%"),
                parse_mode="HTML")
    except Exception as e:
        log.warning(f"[Trailing] close {_bsym}: {e}")


async def trailing_monitor_job(context):
    """🆕 v9.1.0 — Job يعمل كل 30 ثانية"""
    try:
        await _trailing_monitor(context.bot)
    except Exception as e:
        log.warning(f"trailing_monitor_job: {e}")


async def cmd_trail(update, context):
    """🆕 v9.1.0 — /trail on | off | status"""
    global _trailing_mode
    args = context.args or []
    
    if not args or args[0].lower() == "status":
        _status = "✅ مفعّل" if _trailing_mode["enabled"] else "⏸️ معطّل"
        _active = _trailing_mode["active_ticket"] or "لا شيء"
        await _reply(update,
            f"🎯 <b>وضع التتبع (Trailing Mode)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"• الحالة: <b>{_status}</b>\n"
            f"• حد الخسارة: <b>{_trailing_mode['max_loss_pct']}%</b>\n"
            f"• الصفقة النشطة: <code>{_active}</code>\n"
            f"• أعلى ربح: <b>{_trailing_mode['highest_profit']:.2f}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"<code>/trail on</code> — تفعيل\n"
            f"<code>/trail off</code> — تعطيل",
            parse_mode="HTML")
        return
    
    if args[0].lower() == "on":
        if _trailing_mode["enabled"]:
            await _reply(update, "ℹ️ وضع التتبع مفعّل بالفعل.")
            return
        _trailing_mode["enabled"] = True
        _trailing_mode["max_loss_pct"] = 2.5
        _trailing_mode["active_ticket"] = None
        _trailing_mode["highest_profit"] = 0.0
        _save_trailing_state()
        await _reply(update,
            "✅ <b>تم تفعيل وضع التتبع</b>\n"
            "• حد الخسارة: 2.5%\n"
            "• صفقة واحدة فقط\n"
            "• الإغلاق عند انعكاس BTC",
            parse_mode="HTML")
        return
    
    if args[0].lower() == "off":
        _trailing_mode["enabled"] = False
        _save_trailing_state()
        await _reply(update, "⏸️ <b>تم تعطيل وضع التتبع</b>", parse_mode="HTML")
        return



async def cmd_commands(update, context):
    """🆕 v9.1.2 — عرض كل الأوامر المتاحة"""
    text = """📋 <b>أوامر البوت</b>
━━━━━━━━━━━━━━━━━━━

<b>📊 معلومات وحالة:</b>
/start — القائمة الرئيسية
/menu — القائمة التفاعلية
/status — الحالة العامة
/stats — الأداء (WR)
/perf — أداء المصادر
/jobs — المهام المجدولة
/strict — إعدادات التشدد
/risk — حالة المخاطرة
/bcheck — فحص Binance
/commands — عرض الأوامر (هذا الأمر)

<b>🔍 إشارات:</b>
/cross — فحص كل الإطارات
/cross5 — فحص 5m
/cross15 — فحص 15m
/cross1h — فحص 1h
/why [SYMBOL] — تشخيص
/last — آخر الإشارات
/pending — الإشارات النشطة
/zones — مناطق S/R

<b>💼 صفقات:</b>
/positions — الصفقات المفتوحة
/pnl — الرصيد والأرباح
/close [SYMBOL] — إغلاق صفقة
/closeall — إغلاق الكل
/syncnow — مزامنة
/orders — الأوامر المعلقة
/cancel [SYMBOL] — إلغاء أوامر
/adopt [SYMBOL] — تبنّي صفقة

<b>🎯 Trailing Mode:</b>
/trail on — تفعيل
/trail off — تعطيل
/trail status — الحالة

<b>📄 Paper (الورقي):</b>
/paper — صفقات Paper
/trade_stats — إحصائيات حقيقية
/detector_stats — إحصائيات الكواشف

<b>⏸️ تحكم:</b>
/pause — إيقاف البوت
/resume — استئناف
/resume_risk — استئناف المخاطرة
/paperonly on|off — وضع Paper فقط

<b>⚙️ أخرى:</b>
/scalper [SYMBOL] — رادار Scalping
/scalper_stop — إيقاف Scalper
/clearcache — تفريغ الكاش
/ref6 — نطاقات 6%
/sr — رسوم S/R
/orphan on|off — تنظيف الأوامر

━━━━━━━━━━━━━━━━━━━
💡 <i>استخدم /help لأي أمر لمزيد من التفاصيل</i>"""
    await _reply(update, text, parse_mode="HTML")





def calc_supertrend(df, period=10, multiplier=3.0):
    """🆕 v9.22.0 — Supertrend بسيط ومباشر"""
    try:
        high, low, close = df["h"], df["l"], df["c"]
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
        
        hl2 = (high + low) / 2
        upper_band = hl2 + (multiplier * atr)
        lower_band = hl2 - (multiplier * atr)
        
        trend = pd.Series(index=df.index, dtype=int)
        trend.iloc[0] = 1
        
        for i in range(1, len(df)):
            if close.iloc[i] > upper_band.iloc[i-1]:
                trend.iloc[i] = 1
            elif close.iloc[i] < lower_band.iloc[i-1]:
                trend.iloc[i] = -1
            else:
                trend.iloc[i] = trend.iloc[i-1]
                
        return {"trend": int(trend.iloc[-1]), "st": 0.0}
    except Exception:
        return {"trend": 0, "st": 0.0}




async def _smart_sr_filter(cross, direction, score):
    """🆕 v9.4.0 — فلتر SR ذكي يعتمد على Smart Zones"""
    try:
        # جلب مناطق Smart Zones
        _sz = await sz_compute_levels(cross["symbol"], direction, cross["timeframe"])
        if not _sz:
            return True, "no_sz"
        
        _price = float(cross["price"])
        _entry = float(_sz.get("entry", _price))
        _sl = float(_sz.get("stop_loss", 0))
        
        # 1. التحقق من أن السعر قريب من منطقة دخول (وليس في منتصف الطريق)
        if abs(_price - _entry) / _entry > 0.002:  # 0.2%
            return False, f"price far from zone ({(abs(_price - _entry)/_entry*100):.2f}%)"
        
        # 2. التحقق من أن SL ليس بعيداً جداً
        _sl_pct = abs(_price - _sl) / _price * 100 if _price > 0 else 0
        if _sl_pct > 1.5:
            return False, f"SL too wide ({_sl_pct:.2f}%)"
        
        # 3. التحقق من أن TP (2×SL) معقول
        _tp_pct = _sl_pct * 2
        if _tp_pct > 3.0:
            return False, f"TP too far ({_tp_pct:.2f}%)"
        
        return True, f"SR OK (SL={_sl_pct:.2f}%, TP={_tp_pct:.2f}%)"
    except Exception as e:
        return True, f"error: {e}"




async def detect_range_reversal(symbol, tf):
    """🆕 v9.6.0 — كاشف الانعكاس في السوق العرضي"""
    df = await _df_for(symbol, tf, extra=40)
    if df is None or len(df) < 25: return None
    
    curr = -1
    # 🆕 v9.16.0 — رفع ADX إلى 30 (لأن القيعان قد تكون في ترند قوي)
    try:
        _adx = float(calc_adx(df).iloc[curr])
        if _adx >= 30:
            return None
    except Exception:
        return None
    
    # نطاق 20 شمعة
    window = df.iloc[curr-20:curr]
    high_20 = float(window["h"].max())
    low_20 = float(window["l"].min())
    mid = (high_20 + low_20) / 2
    width = high_20 - low_20
    if width <= 0: return None
    
    price = float(df["c"].iloc[curr])
    rsi = float(calc_rsi(df["c"]).iloc[curr])
    
    d = None
    # 🆕 v9.17.0 — التفريق بين الانعكاس وجني الأرباح (حجم الشمعة)
    try:
        _vma_rev = float(df["v"].iloc[-22:-2].mean())
        _v_now_rev = float(df["v"].iloc[-2])
        _vol_ratio_rev = _v_now_rev / _vma_rev if _vma_rev > 0 else 0
        
        # 🆕 v9.21.0 — تخفيف الحجم إلى 1.2x (للدخول المبكر)
        if _vol_ratio_rev < 1.2:
            log.debug(f"range_reversal {symbol}: vol {_vol_ratio_rev:.2f}x < 1.5x (profit taking)")
            return None
    except Exception:
        pass

    # 🆕 v9.16.0 — فحص شمعة الانعكاس (Reversal Candle)
    try:
        c_last = df.iloc[-2]
        o_last = float(c_last["o"])
        c_close = float(c_last["c"])
        h_last = float(c_last["h"])
        l_last = float(c_last["l"])
        
        body = abs(c_close - o_last)
        lower_wick = min(o_last, c_close) - l_last
        upper_wick = h_last - max(o_last, c_close)
        
        # شمعة مطرقة (Hammer) للشراء
        is_hammer = (lower_wick > body * 2) and (upper_wick < body * 0.5) and (c_close > o_last)
        
        # شمعة نجمة هابطة (Shooting Star) للبيع
        is_star = (upper_wick > body * 2) and (lower_wick < body * 0.5) and (c_close < o_last)
    except Exception:
        is_hammer = False
        is_star = False

    # شراء عند الدعم (السعر قريب من أدنى 20 شمعة) + RSI منخفض أو شمعة مطرقة
    if price <= low_20 * 1.002 and (rsi < 40 or is_hammer):
        d = "bullish"
    # بيع عند المقاومة (السعر قريب من أعلى 20 شمعة) + RSI مرتفع أو شمعة نجمة
    elif price >= high_20 * 0.998 and (rsi > 60 or is_star):
        d = "bearish"
    
    if not d: return None
    
    # cooldown
    key = (symbol, tf, "range_rev")
    last = _crossover_cache.get(key)
    if last and (_time.time() - last.get("ts", 0)) < 300:  # 5 دقائق
        return None
    _crossover_cache[key] = {"ts": _time.time()}
    
    df["ef"] = calc_ema(df["c"], EMA_FAST)
    df["es"] = calc_ema(df["c"], EMA_SLOW)
    
    cross = await _make_cross(symbol, tf, df, d, "live", "↔️ انعكاس عرضي", curr, source="range_reversal")
    if cross:
        cross["range_high"] = high_20
        cross["range_low"] = low_20
        cross["range_mid"] = mid
    return cross



async def detect_macd_rsi(symbol, tf):
    """🆕 v9.9.0 — كاشف MACD متقدم (زاوية + هيستوجرام + RSI + تأكيد الشمعة)"""
    df = await _df_for(symbol, tf, extra=50)
    if df is None or len(df) < 30: return None
    
    curr = -2
    try:
        dif, dea, hist = calc_macd(df["c"])
        rsi_series = calc_rsi(df["c"])
        atr = float(calc_atr(df).iloc[curr])
        price = float(df["c"].iloc[curr])
    except Exception:
        return None
    
    _dif_now = float(dif.iloc[curr])
    _dea_now = float(dea.iloc[curr])
    _dif_prev = float(dif.iloc[curr-1])
    _dea_prev = float(dea.iloc[curr-1])
    _hist_now = float(hist.iloc[curr])
    _hist_prev = float(hist.iloc[curr-1])
    _hist_prev2 = float(hist.iloc[curr-2])
    _rsi_now = float(rsi_series.iloc[curr])
    _rsi_prev = float(rsi_series.iloc[curr-1])
    
    # 1. اكتشاف التقاطع
    bull_cross = (_dif_now > _dea_now) and (_dif_prev <= _dea_prev)
    bear_cross = (_dif_now < _dea_now) and (_dif_prev >= _dea_prev)
    
    if not (bull_cross or bear_cross): return None
    
    d = "bullish" if bull_cross else "bearish"
    
    # 2. 🆕 زاوية/قوة التقاطع (المسافة بين DIF و DEA > 0.05% من السعر)
    _cross_gap = abs(_dif_now - _dea_now)
    _min_gap = price * 0.0005  # 0.05% من السعر
    if _cross_gap < _min_gap:
        log.debug(f"macd_rsi {symbol} {tf}: cross gap {_cross_gap:.4f} < {_min_gap:.4f} (weak)")
        return None
    
    # 3. 🆕 تأكيد الهيستوجرام (يجب أن يكون في الاتجاه الصحيح ومتزايداً)
    if d == "bullish":
        if _hist_now <= 0 or _hist_now <= _hist_prev:
            log.debug(f"macd_rsi {symbol} {tf}: hist not confirming bull")
            return None
    else:
        if _hist_now >= 0 or _hist_now >= _hist_prev:
            log.debug(f"macd_rsi {symbol} {tf}: hist not confirming bear")
            return None
    
    # 4. 🆕 تأكيد RSI (منطقة زخم + ليس في تشبع)
    if d == "bullish":
        if not (50 < _rsi_now < 70) or _rsi_now <= _rsi_prev:
            log.debug(f"macd_rsi {symbol} {tf}: RSI {_rsi_now:.1f} not confirming bull")
            return None
    else:
        if not (30 < _rsi_now < 50) or _rsi_now >= _rsi_prev:
            log.debug(f"macd_rsi {symbol} {tf}: RSI {_rsi_now:.1f} not confirming bear")
            return None
    
    # 5. 🆕 تأكيد الشمعة التالية (لتجنب الارتداد الفوري)
    # نتحقق من أن الشمعة الحالية (التي تلت التقاطع) لا تزال تؤكد الاتجاه
    if d == "bullish" and _dif_now <= _dea_now:
        return None
    if d == "bearish" and _dif_now >= _dea_now:
        return None
    
    # cooldown
    key = (symbol, tf, "macd_rsi")
    last = _crossover_cache.get(key)
    if last and (_time.time() - last.get("ts", 0)) < 300:  # 5 دقائق
        return None
    _crossover_cache[key] = {"ts": _time.time()}
    
    df["ef"] = calc_ema(df["c"], EMA_FAST)
    df["es"] = calc_ema(df["c"], EMA_SLOW)
    
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "📊 MACD+RSI", curr, source="macd_rsi")
    if cross:
        cross["macd_info"] = {
            "dif": round(_dif_now, 4), "dea": round(_dea_now, 4),
            "rsi": round(_rsi_now, 1), "gap": round(_cross_gap, 5),
        }
    return cross



def _calc_confluence(cross, direction):
    """
    🆕 v9.14.0 — حساب نقاط التوافق (0-5).
    كلما زادت النقاط، زادت قوة الإشارة.
    """
    _score = 0
    _df = cross.get("_df")
    if _df is None or len(_df) < 25:
        return 0
    
    price = float(_df["c"].iloc[-2])
    
    # 1. توافق EMA50 (5m)
    try:
        e50 = float(calc_ema(_df["c"], 50).iloc[-2])
        if direction == "bullish" and price > e50: _score += 1
        if direction == "bearish" and price < e50: _score += 1
    except Exception: pass

    # 2. توافق RSI (منطقة الزخم)
    try:
        rsi = float(calc_rsi(_df["c"]).iloc[-2])
        if direction == "bullish" and 45 <= rsi <= 65: _score += 1
        if direction == "bearish" and 35 <= rsi <= 55: _score += 1
    except Exception: pass

    # 3. توافق الحجم (أعلى من المتوسط)
    try:
        vma = float(_df["v"].iloc[-20:-2].mean())
        v_now = float(_df["v"].iloc[-2])
        if v_now > vma * 1.2: _score += 1
    except Exception: pass

    # 4. توافق Supertrend (5m)
    try:
        st = calc_supertrend(_df)
        if direction == "bullish" and st["trend"] == 1: _score += 1
        if direction == "bearish" and st["trend"] == -1: _score += 1
    except Exception: pass

    # 5. توافق الشمعة (هل الشمعة الأخيرة في اتجاه الإشارة؟)
    try:
        c_now = float(_df["c"].iloc[-2])
        o_now = float(_df["o"].iloc[-2])
        if direction == "bullish" and c_now > o_now: _score += 1
        if direction == "bearish" and c_now < o_now: _score += 1
    except Exception: pass

    return _score



async def detect_live_momentum(symbol, tf):
    """🆕 v9.15.0 — كاشف الزخم اللحظي (يدخل مع الحركة وليس بعدها)"""
    df = await _df_for(symbol, tf, extra=30)
    if df is None or len(df) < 20: return None
    
    curr = -1  # الشمعة الحالية (غير مغلقة)
    try:
        # 1. حجم قوي جداً في الشمعة الحالية
        vma = float(df["v"].iloc[curr-20:curr].mean())
        v_now = float(df["v"].iloc[curr])
        vr = v_now / vma if vma > 0 else 0
        
        if vr < 1.5:  # 🆕 v9.21.0 — تخفيف الحجم إلى 1.5x
            return None
            
        # 2. حركة سعرية قوية في الشمعة الحالية
        o_now = float(df["o"].iloc[curr])
        c_now = float(df["c"].iloc[curr])
        change_pct = (c_now - o_now) / o_now * 100
        
        if abs(change_pct) < 0.15:  # 🆕 v9.21.0 — تخفيف الحركة إلى 0.15%
            return None
            
        d = "bullish" if change_pct > 0 else "bearish"
        
        # 3. فحص RSI (لا ندخل في التشبع)
        rsi = float(calc_rsi(df["c"]).iloc[curr-1])  # RSI للشمعة السابقة المغلقة
        if d == "bullish" and rsi > 65:
            return None
        if d == "bearish" and rsi < 35:
            return None
            
        # 4. cooldown (دقيقة واحدة فقط)
        key = (symbol, tf, "live_mom")
        last = _crossover_cache.get(key)
        if last and (_time.time() - last.get("ts", 0)) < 60:
            return None
        _crossover_cache[key] = {"ts": _time.time()}
        
        # 5. بناء الإشارة
        df["ef"] = calc_ema(df["c"], EMA_FAST)
        df["es"] = calc_ema(df["c"], EMA_SLOW)
        
        cross = await _make_cross(symbol, tf, df, d, "live", "⚡ زخم لحظي", curr, source="live_momentum")
        if cross:
            cross["momentum_info"] = {"vol_ratio": round(vr, 2), "change_pct": round(change_pct, 2)}
            cross["_needs_confirm"] = False # لا ينتظر التأكيد
        return cross
        
    except Exception as e:
        log.debug(f"live_momentum {symbol}: {e}")
        return None


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ TELEGRAM_REPORT_BOT_TOKEN and CHAT_ID required"); return

    # 🩹 FIX: restore active paper signals from SQLite
    try:
        _restored = _paper_load_all()
        with _paper_lock:
            for _s in _restored:
                if _s.get("result") is None:
                    _paper_signals[_s["id"]] = _s
        print(f"♻️  Restored {len(_paper_signals)} active paper signals")
    except Exception as _e:
        print(f"⚠️  paper restore failed: {_e}")

    threading.Thread(target=run_health, daemon=True).start()

    n = _load_manual_ranges()
    n_restored = _load_state()
    if n_restored:
        print(f"♻️  Restored {n_restored} tickets from previous save")
    _cleanup_dead_tickets()  # 🆕 v6.7.4
    sd, sn = get_session_info()
    print(f"📌 {n} ranges | 🔌 {_exchange.primary_name}")
    print(f"🕐 Session: {sn} ({sd:+d})")
    print(f"💸 Binance: {'✅' if _binance.ok else '❌ ' + (str(_binance.error_msg) or '')}")
    print(f"   • Notional: {NOTIONAL_USDT}$ | Lev: {LEVERAGE}x | {MARGIN_TYPE}")
    print(f"   • Second trade: score ≥ {STRONG_SCORE_FOR_ADD}")
    print(f"   • Monitoring: every {POSITION_MONITOR_SEC}s")

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(read_timeout=30.0, write_timeout=30.0,
                           connect_timeout=30.0, pool_timeout=30.0)
    app = (Application.builder().token(BOT_TOKEN)
           .request(request).get_updates_request(request).build())

    if app.job_queue is None:
        raise RuntimeError("❌ job_queue غير متوفر! "
                           "pip install 'python-telegram-bot[job-queue]'")

    # ── Handlers ────────────────────────────────────
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("cross", cmd_cross))
    app.add_handler(CommandHandler("cross5", cmd_cross5))
    app.add_handler(CommandHandler("cross15", cmd_cross15))
    app.add_handler(CommandHandler("cross1h", cmd_cross1h))
    app.add_handler(CommandHandler("why", cmd_why))
    app.add_handler(CommandHandler("last", cmd_last))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CommandHandler("checkprice", cmd_checkprice))
    app.add_handler(CommandHandler("ranges", cmd_ranges))
    app.add_handler(CommandHandler("checkranges", cmd_checkranges))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("ref6", cmd_ref6))
    app.add_handler(CommandHandler("sr", cmd_sr))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("perf", cmd_perf))
    app.add_handler(CommandHandler("jobs", cmd_jobs))
    app.add_handler(CommandHandler("strict", cmd_strict))
    app.add_handler(CommandHandler("zones", cmd_zones))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    # 💰 Binance
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("closeall", cmd_closeall))
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CommandHandler("syncnow", cmd_syncnow))
    app.add_handler(CommandHandler("bcheck", cmd_bcheck))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("adopt", cmd_adopt))
    app.add_handler(CommandHandler("resume_risk", cmd_resume_risk))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("paper", cmd_paper))
    app.add_handler(CommandHandler("trade_stats", cmd_trade_stats))
    app.add_handler(CommandHandler("detector_stats", cmd_detector_stats))
    app.add_handler(CommandHandler("risk", cmd_risk))
    app.add_handler(CommandHandler("orphan", cmd_orphan))
    app.add_handler(CommandHandler("paperonly", cmd_paperonly))
    app.add_handler(CommandHandler("scalper", cmd_scalper))
    app.add_handler(CommandHandler("scalper_stop", cmd_scalper_stop))  # 🆕
    app.add_handler(CommandHandler("trail", cmd_trail))  # 🆕 v9.1.0
    app.add_handler(CommandHandler("commands", cmd_commands))  # 🆕 v9.1.2

    # 🆕 v7.9.41 — text handler for manual SL/TP input (only if waiting)
    from telegram.ext import MessageHandler, filters as _tg_filters
    async def _text_dispatcher(update, context):
        if update.message and update.message.text:
            _t = update.message.text.strip()
            _parts = _t.split()
            if len(_parts) >= 3:
                _bsym = _parts[0].upper()
                if _bsym in _manual_waiting_input:
                    await cmd_manual_sl_tp_input(update, context)
    app.add_handler(MessageHandler(_tg_filters.TEXT & ~_tg_filters.COMMAND,
                                    _text_dispatcher))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_error_handler(error_handler)

    # ── Jobs ────────────────────────────────────────
    # v7.9.2: split crossover_job by timeframe (avoid overlap)
    # 🆕 v9.27.0 — فحص سريع كل 10 ثوانٍ (ETH فقط)
    app.job_queue.run_repeating(
        lambda ctx: crossover_job(ctx, ["1m"]),
        interval=10, first=5, name="cross_1m")
    # 🆕 v7.9.54 — 15m Paper-only (لجمع بيانات)
    app.job_queue.run_repeating(_scalper_monitor_job, interval=5, first=10, name="scalper_monitor")
    # 🆕 v9.1.4 — إطار 15m معطّل من إنتاج الإشارات (يبقى للتحليل فقط)
    # app.job_queue.run_repeating(
    #     lambda ctx: crossover_job(ctx, ["15m"]),
    #     interval=15 * 60, first=30, name="cross_15m")
    print("🔄 crossover jobs: 5m (exec) + 15m (Paper only)")
    app.job_queue.run_repeating(signal_tracker_job,
        interval=SIGNAL_TRACK_INTERVAL_MIN * 60, first=60, name="tracker")
    app.job_queue.run_repeating(price_alert_job,
        interval=PRICE_ALERT_INTERVAL_MIN * 60, first=20, name="price")
    if FAST_MONITOR_ENABLED:
        app.job_queue.run_repeating(fast_tp_sl_job,
            interval=FAST_MONITOR_SEC, first=25, name="fast_tp_sl")
    app.job_queue.run_repeating(unified_position_manager_job,
        interval=45, first=30, name="unified_pos")
    # 🆕 v7.9.23 — shadow reversal evaluation (every 5 min)
    app.job_queue.run_repeating(shadow_trades_job,
        interval=5 * 60, first=120, name="shadow_trades")
    # 🆕 v7.9.44 — dust + orphan orders
    app.job_queue.run_repeating(_cleanup_dust_job,
        interval=3600, first=180, name="cleanup_dust")
    app.job_queue.run_repeating(_check_orphan_orders_job,
        interval=300, first=120, name="orphan_orders")
    print("🧹 Dust cleanup: every 1h | Orphan orders: every 5m")
    # 🆕 v7.9.42 — 2c opposite watcher (every 15s)
    app.job_queue.run_repeating(
        lambda ctx: _shooting_star_watcher(bot=ctx.bot),
        interval=15, first=30, name="2c_watcher")
    print("🚨 2c-opposite watcher: every 15s")
    print("👻 Shadow reversal job: every 5 min")

    # v8.0.86 — cleanup caches كل 5 دقائق (منفصل عن crossover_job)
    async def _cache_cleanup_job(context):
        try:
            _cleanup_stale_caches()
        except Exception as _e:
            log.debug(f"cache_cleanup: {_e}")
    app.job_queue.run_repeating(_cache_cleanup_job,
        interval=300, first=120, name="cache_cleanup")
    app.job_queue.run_repeating(btc_sentinel_job,
        interval=60, first=20, name="btc_sentinel")
    app.job_queue.run_repeating(trailing_monitor_job,
        interval=30, first=15, name="trailing_monitor")
    
    # v9.2.2 — Super Job ملغى (يسبب تعارض Event Loop)
    # العودة إلى الـ Jobs المنفصلة
    print("🚨 BTC Sentinel: every 60s")
    print("🧹 Cache cleanup: every 5 min (separate job)")
# DISABLED: range_monitor_job scheduling

    if _binance.ok:
        # v7.0.0 unified
        app.job_queue.run_repeating(unified_position_manager_job,
            interval=45, first=30, name="unified_pos")  # v8.0.87f: 90→45
        print("💼 Unified position manager: every 15s")
        # v8.0.87g — cleanup للميتة
        app.job_queue.run_repeating(_cleanup_dead_tickets_job,
            interval=120, first=60, name="dead_tickets_cleanup")
        print("🧹 Dead tickets cleanup: every 2 min")

        # 🩹 FIX #15: orphan orders cleanup every 90s
        app.job_queue.run_repeating(_cleanup_orphan_orders_job,
            interval=180, first=100, name="cleanup_orphans")  # 🆕 v7.9.28: 90→180
        print("🧹 Orphan cleanup: every 90s")

    # v7.0.0 recovery once after 60s
    app.job_queue.run_once(_recovery_job, when=60, name="recovery")

    # 🆕 v7.9.30 — 6pct reports disabled (ranges removed)
    pass

    if MORNING_REPORT_ENABLED:
        morning_time = dt_time(hour=MORNING_REPORT_HOUR_SYRIA, minute=0,
                                tzinfo=SYRIA_TZ)
        app.job_queue.run_daily(morning_report_job, time=morning_time,
                                name="morning_report")
    
    # 🆕 v7.9.17 — تقرير كل 6 ساعات
    app.job_queue.run_repeating(
        six_hour_report_job,
        interval=6 * 3600,
        first=300,  # أول مرة بعد 5 دقائق (للتأكد)
        name="six_hour_report")
    print("📊 Six-hour report: every 6h (first in 5 min)")

    # 🆕 v7.9.46 — Paper digest كل 3 ساعات
    app.job_queue.run_repeating(
        paper_digest_job,
        interval=PAPER_DIGEST_INTERVAL_SEC,
        first=PAPER_DIGEST_INTERVAL_SEC,
        name="paper_digest")
    print(f"📄 Paper digest: every {PAPER_DIGEST_INTERVAL_SEC//3600}h "
          f"(first in {PAPER_DIGEST_INTERVAL_SEC//3600}h)")

    if is_paused():
        print("⏸️ paused (pause.flag exists) — /resume to resume")
    global _global_bot
    _global_bot = app.bot
    print("✅ Ready - starting polling")
    app.run_polling(allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True, bootstrap_retries=5)


if __name__ == "__main__":
    main()
