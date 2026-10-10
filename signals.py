"""
Unified Smart Bot v3.3 — Final Stable Edition
==============================================
  ✓ لا Binance نهائياً
  ✓ منصات متعددة مع getattr آمن
  ✓ log مُعرَّف قبل أي استخدام
  ✓ STRICTNESS + STRICTNESS_OVERRIDE (متغيّران فقط للتشدد)
  ✓ RANGE_WIDTH_MULTIPLIER لتضييق/توسيع النطاق
  ✓ إشعارات فورية عند خروج السعر عن النطاق
  ✓ قوائم رموز منفصلة (إشارات / نطاق / مفاجئ)
"""
import os
import re
import asyncio
import logging
import threading
import time as _time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import deque

# ═══════════════════════════════════════════════════════════
# 0) Logging — أول شيء يُنفَّذ قبل أي استخدام لـ log
# ═══════════════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
for _name in ("httpx", "telegram", "telegram.ext", "ccxt"):
    logging.getLogger(_name).setLevel(logging.WARNING)
log = logging.getLogger("unified")

# ─── المكتبات الأخرى ───
import ccxt
import pandas as pd
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import NetworkError, TimedOut

load_dotenv()


# ═══════════════════════════════════════════════════════════
# 1) إعدادات عامة
# ═══════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("TELEGRAM_REPORT_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_REPORT_CHAT_ID")


def _get_float(n, d):
    raw = os.getenv(n, str(d))
    m = re.search(r"-?\d+(\.\d+)?", str(raw))
    return float(m.group(0)) if m else float(d)


def _get_int(n, d):   return int(_get_float(n, d))
def _get_bool(n, d):  return os.getenv(n, str(d)).strip().lower() in ("true", "1", "yes", "on")


# ═══════════════════════════════════════════════════════════
# 2) المنصات — بدون Binance + getattr آمن
# ═══════════════════════════════════════════════════════════
PRIMARY_EXCHANGE = os.getenv("PRIMARY_EXCHANGE", "bybit").strip().lower()
FALLBACK_EXCHANGES = [
    x.strip().lower() for x in
    os.getenv("FALLBACK_EXCHANGES", "okx,kucoin,bitget,mexc,gate").split(",")
    if x.strip() and x.strip().lower() != "binance"
]
MARKET_TYPE = os.getenv("MARKET_TYPE", "swap").strip().lower()
if MARKET_TYPE not in ("spot", "swap", "future"):
    MARKET_TYPE = "swap"

# بناء ديناميكي — يتجاهل أي منصة غير موجودة في نسخة ccxt الحالية
_EXCHANGE_NAMES = [
    "bybit", "okx", "kucoin", "kraken", "gate",
    "bitget", "mexc", "htx", "coinex", "bitmart",
]
_EXCHANGE_CLASSES = {
    name: getattr(ccxt, name)
    for name in _EXCHANGE_NAMES
    if hasattr(ccxt, name)
}
log.info(f"📦 ccxt {ccxt.__version__} | Available: {list(_EXCHANGE_CLASSES.keys())}")


class MultiExchange:
    """طبقة موحّدة: تجرب الأساسية ثم البدائل بالترتيب"""

    def __init__(self):
        self.primary = None
        self.fallbacks = []
        self._init_all()

    def _make(self, name):
        if name == "binance":
            log.warning("🚫 Binance محجوب في هذا البوت")
            return None
        if name not in _EXCHANGE_CLASSES:
            log.warning(f"⚠️ منصة غير متوفرة في ccxt: {name}")
            return None
        try:
            opts = {
                "enableRateLimit": True,
                "timeout": 60000,
                "options": {
                    "defaultType": MARKET_TYPE,
                    "fetchOHLCV": {"maxLimit": 200},
                },
            }
            if name == "bybit":
                opts["rateLimit"] = 500
                opts["options"]["unifiedMargin"] = False
            ex = _EXCHANGE_CLASSES[name](opts)
            ex.load_markets()
            log.info(f"✅ {ex.name} | {len(ex.markets)} سوق")
            return ex
        except Exception as e:
            log.warning(f"❌ فشل تهيئة {name}: {type(e).__name__}: {e}")
            return None

    def _init_all(self):
        self.primary = self._make(PRIMARY_EXCHANGE)
        for name in FALLBACK_EXCHANGES:
            if name == PRIMARY_EXCHANGE:
                continue
            ex = self._make(name)
            if ex:
                self.fallbacks.append(ex)
        if not self.primary and self.fallbacks:
            self.primary = self.fallbacks.pop(0)
            log.warning(f"🔄 استخدام {self.primary.name} كمنصة أساسية بديلة")

    def _chain(self):
        out = []
        if self.primary:
            out.append(self.primary)
        out.extend(self.fallbacks)
        return out

    async def fetch_ohlcv(self, symbol, tf, limit=200):
        last_err = None
        for ex in self._chain():
            try:
                data = await asyncio.to_thread(ex.fetch_ohlcv, symbol, tf, None, limit)
                if data and len(data) > 0:
                    return data, ex.id
            except ccxt.BadSymbol:
                continue
            except (ccxt.RateLimitExceeded, ccxt.NetworkError, ccxt.ExchangeError) as e:
                last_err = e
                await asyncio.sleep(0.3)
                continue
            except Exception as e:
                last_err = e
                continue
        if last_err:
            log.debug(f"fetch_ohlcv {symbol} {tf}: {type(last_err).__name__}")
        return None, None

    @property
    def primary_name(self):
        return self.primary.name if self.primary else "none"

    @property
    def chain_names(self):
        return [e.name for e in self._chain()]

    def ok(self):
        return self.primary is not None


_exchange = MultiExchange()


# ═══════════════════════════════════════════════════════════
# 3) قوائم الرموز — منفصلة
# ═══════════════════════════════════════════════════════════
_SIG_DEFAULT = "BTC/USDT:USDT,XRP/USDT:USDT,XAU/USDT:USDT"
SIG_SYMBOLS = [
    s.strip().upper() for s in
    (os.getenv("SYMBOLS_SIGNALS", "").strip() or _SIG_DEFAULT).split(",")
    if s.strip()
]

_RNG_DEFAULT = "BTC/USDT:USDT,XAU/USDT:USDT,XAG/USDT:USDT"
RNG_SYMBOLS = [
    s.strip().upper() for s in
    (os.getenv("SYMBOLS_RANGE", "").strip() or _RNG_DEFAULT).split(",")
    if s.strip()
]

SC_SYMBOLS = [
    s.strip().upper() for s in
    (os.getenv("SYMBOLS_SUDDEN", "").strip() or ",".join(SIG_SYMBOLS)).split(",")
    if s.strip()
]


# ═══════════════════════════════════════════════════════════
# 4) إعدادات عامة
# ═══════════════════════════════════════════════════════════
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "5m,15m,1h").split(",") if t.strip()]
EMA_FAST = _get_int("EMA_FAST", 7)
EMA_SLOW = _get_int("EMA_SLOW", 25)
JOB_INTERVAL_MIN = _get_int("JOB_INTERVAL_MIN", 2)

ENABLE_PRE_CROSS  = _get_bool("ENABLE_PRE_CROSS", True)
ENABLE_LIVE_CROSS = _get_bool("ENABLE_LIVE_CROSS", True)
ENABLE_CONFIRMED  = _get_bool("ENABLE_CONFIRMED", True)
PRE_CROSS_GAP = _get_float("PRE_CROSS_GAP", 0.05)
PRE_CROSS_LOOKBACK = _get_int("PRE_CROSS_LOOKBACK", 3)
PRE_CROSS_COOLDOWN = _get_int("PRE_CROSS_COOLDOWN", 3)

OHLCV_CACHE_SECONDS = _get_int("OHLCV_CACHE_SECONDS", 90)
CONFLICT_WINDOW_SEC = _get_int("CONFLICT_WINDOW_SEC", 300)

HTF_ENABLED = _get_bool("HTF_TREND_ENABLED", True)
HTF_TIMEFRAME = os.getenv("HTF_TIMEFRAME", "1h")
HTF_BLOCK_OPPOSITE = _get_bool("HTF_BLOCK_OPPOSITE", True)
VWAP_SLOPE_LOOKBACK = _get_int("VWAP_SLOPE_LOOKBACK", 5)

PRICE_ALERT_INTERVAL_MIN = _get_int("PRICE_ALERT_INTERVAL_MIN", 1)
MAX_PRICE_ALERTS = _get_int("MAX_PRICE_ALERTS", 3)
DEFAULT_THRESHOLD = _get_float("DEFAULT_THRESHOLD", 1.0)
PRICE_CHANGE_THRESHOLDS = {
    "BTC/USDT:USDT": _get_float("THRESHOLD_BTC", 1.0),
    "XAU/USDT:USDT": _get_float("THRESHOLD_XAU", 0.4),
    "XAG/USDT:USDT": _get_float("THRESHOLD_XAG", 0.6),
    "XRP/USDT:USDT": _get_float("THRESHOLD_XRP", 1.5),
}

MORNING_REPORT_ENABLED = _get_bool("MORNING_REPORT_ENABLED", True)
MORNING_REPORT_HOUR = _get_int("MORNING_REPORT_HOUR", 9)
MORNING_REPORT_LOOKBACK_DAYS = _get_int("MORNING_REPORT_LOOKBACK_DAYS", 10)
MORNING_REPORT_MIN_GRIDS = _get_int("MORNING_REPORT_MIN_GRIDS", 15)
MORNING_REPORT_MAX_GRIDS = _get_int("MORNING_REPORT_MAX_GRIDS", 35)
MORNING_REPORT_ATR_MULTIPLIER = _get_float("MORNING_REPORT_ATR_MULTIPLIER", 3.0)
MORNING_REPORT_MAX_RANGE_PCT = _get_float("MORNING_REPORT_MAX_RANGE_PCT", 8.0)

RANGE_WIDTH_MULTIPLIER = _get_float("RANGE_WIDTH_MULTIPLIER", 1.0)
RANGE_WIDTH_MULTIPLIER = max(0.2, min(3.0, RANGE_WIDTH_MULTIPLIER))

RANGE_BREAKOUT_ENABLED = _get_bool("RANGE_BREAKOUT_ENABLED", True)
RANGE_BREAKOUT_INTERVAL_MIN = _get_int("RANGE_BREAKOUT_INTERVAL_MIN", 5)
RANGE_BREAKOUT_COOLDOWN_MIN = _get_int("RANGE_BREAKOUT_COOLDOWN_MIN", 60)
RANGE_MAX_AGE_HOURS = _get_int("RANGE_MAX_AGE_HOURS", 24)

SYMBOL_DELAY_MS = _get_int("SYMBOL_DELAY_MS", 200)

SYRIA_TZ = ZoneInfo("Asia/Damascus")


# ═══════════════════════════════════════════════════════════
# 5) 🧠 محرك التشدد الذكي
# ═══════════════════════════════════════════════════════════
STRICTNESS = os.getenv("STRICTNESS", "balanced").strip().lower()
STRICTNESS_OVERRIDE: dict = {}

STATIC_PROFILES = {
    "relaxed": {
        "min_score": 45, "min_vol_ratio": 0.80, "min_adx": 14.0,
        "max_atr_pct": 0.95,
        "rsi_long": (32, 80), "rsi_short": (20, 68),
        "vwap_slope_pct": 0.03, "min_ema_gap": 0.03,
        "block_against_htf": False, "score_15m_override": 55,
    },
    "balanced": {
        "min_score": 60, "min_vol_ratio": 1.00, "min_adx": 18.0,
        "max_atr_pct": 0.70,
        "rsi_long": (35, 75), "rsi_short": (25, 65),
        "vwap_slope_pct": 0.05, "min_ema_gap": 0.05,
        "block_against_htf": True, "score_15m_override": 65,
    },
    "strict": {
        "min_score": 70, "min_vol_ratio": 1.30, "min_adx": 22.0,
        "max_atr_pct": 0.60,
        "rsi_long": (40, 72), "rsi_short": (28, 60),
        "vwap_slope_pct": 0.07, "min_ema_gap": 0.08,
        "block_against_htf": True, "score_15m_override": 75,
    },
    "elite": {
        "min_score": 82, "min_vol_ratio": 1.80, "min_adx": 28.0,
        "max_atr_pct": 0.50,
        "rsi_long": (45, 68), "rsi_short": (32, 55),
        "vwap_slope_pct": 0.10, "min_ema_gap": 0.12,
        "block_against_htf": True, "score_15m_override": 85,
    },
}


class SmartStrictness:
    """
    محرك تشدد ذكي بثلاث طبقات:
      1) تقلب ATR% → score أساسي
      2) ADX → تعديل
      3) Feedback loop → نسبة القبول
    """

    def __init__(self):
        self.history = deque(maxlen=200)
        self._lock = threading.Lock()
        self._cached_profile = None
        self._cached_ts = 0.0
        self._cache_ttl = 60.0
        self._window = 50
        self._min_samples = 20

    def record(self, passed: bool, score: int):
        with self._lock:
            self.history.append((passed, score, _time.time()))

    def _feedback(self):
        with self._lock:
            recent = list(self.history)[-self._window:]
        n = len(recent)
        if n < self._min_samples:
            return 0, None, n
        passed = sum(1 for p, _, _ in recent if p)
        ratio = passed / n
        if ratio > 0.60:   delta = +8
        elif ratio > 0.40: delta = +3
        elif ratio < 0.10: delta = -12
        elif ratio < 0.20: delta = -6
        else:              delta = 0
        return delta, ratio, n

    def compute(self, atr_pct=None, adx=None) -> dict:
        now = _time.time()
        if self._cached_profile and (now - self._cached_ts) < self._cache_ttl:
            return self._cached_profile

        if atr_pct is None or atr_pct <= 0:
            atr_pct = 0.5
        if atr_pct < 0.15:    base, regime = 50, "هادئ"
        elif atr_pct < 0.30:  base, regime = 55, "منخفض"
        elif atr_pct < 0.60:  base, regime = 62, "عادي"
        elif atr_pct < 1.00:  base, regime = 70, "مرتفع"
        else:                 base, regime = 78, "متطرف"

        adx_delta = 0
        if adx is not None:
            if adx > 35:     adx_delta = -3
            elif adx > 25:   adx_delta = -1
            elif adx < 18:   adx_delta = +5
            elif adx < 22:   adx_delta = +2

        fb_delta, pass_ratio, sample_n = self._feedback()
        final_score = max(40, min(90, base + adx_delta + fb_delta))

        profile = {
            "min_score": final_score,
            "min_vol_ratio": round(max(0.7, (final_score - 30) / 30), 2),
            "min_adx": round(max(12, final_score * 0.28), 1),
            "max_atr_pct": round(max(0.35, 1.2 - final_score / 100), 2),
            "rsi_long": (
                min(50, 30 + max(0, final_score - 50) // 4),
                max(70, 82 - max(0, final_score - 50) // 5),
            ),
            "rsi_short": (
                min(40, 18 + max(0, final_score - 50) // 4),
                max(55, 68 - max(0, final_score - 50) // 6),
            ),
            "vwap_slope_pct": round(0.02 + final_score / 1000, 3),
            "min_ema_gap": round(final_score / 800, 3),
            "block_against_htf": final_score >= 55,
            "score_15m_override": min(90, final_score + 10),
            "_debug": {
                "mode": "auto",
                "base_score": base,
                "adx_delta": adx_delta,
                "feedback_delta": fb_delta,
                "pass_ratio": round(pass_ratio, 2) if pass_ratio is not None else None,
                "sample_n": sample_n,
                "regime": regime,
                "atr_pct": atr_pct,
                "adx": adx,
            },
        }
        self._cached_profile = profile
        self._cached_ts = now
        return profile

    def reset_feedback(self):
        with self._lock:
            self.history.clear()
        self._cached_profile = None
        self._cached_ts = 0.0


_smart = SmartStrictness()


def get_thresholds(atr_pct: float = None, adx: float = None) -> dict:
    if STRICTNESS == "auto":
        prof = dict(_smart.compute(atr_pct=atr_pct, adx=adx))
    else:
        prof = dict(STATIC_PROFILES.get(STRICTNESS, STATIC_PROFILES["balanced"]))
        prof["_debug"] = {"mode": STRICTNESS}
    prof.update(STRICTNESS_OVERRIDE)
    return prof


# ═══════════════════════════════════════════════════════════
# 6) أدوات مساعدة
# ═══════════════════════════════════════════════════════════
def short(symbol: str) -> str:
    return symbol.split("/")[0].split(":")[0].upper()


def syria_now_str() -> str:
    now = datetime.now(SYRIA_TZ)
    days_ar = {
        "Monday": "الاثنين", "Tuesday": "الثلاثاء", "Wednesday": "الأربعاء",
        "Thursday": "الخميس", "Friday": "الجمعة", "Saturday": "السبت",
        "Sunday": "الأحد",
    }
    return f"{days_ar.get(now.strftime('%A'), now.strftime('%A'))} " \
           f"{now.strftime('%Y-%m-%d')} — {now.strftime('%H:%M:%S')}"


def syria_from_ts(ts_ms: int) -> str:
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=SYRIA_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "?"


def fmt_price(v: float) -> str:
    if v >= 1000: return f"{v:.2f}"
    if v >= 100:  return f"{v:.3f}"
    if v >= 1:    return f"{v:.4f}"
    if v >= 0.01: return f"{v:.5f}"
    return f"{v:.8f}"


# ═══════════════════════════════════════════════════════════
# 7) المؤشرات
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
    tr = pd.concat([
        df["h"] - df["l"],
        (df["h"] - df["c"].shift()).abs(),
        (df["l"] - df["c"].shift()).abs(),
    ], axis=1).max(axis=1)
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
    dif = ef - es
    dea = dif.ewm(span=sig, adjust=False).mean()
    return dif, dea, dif - dea


def calc_vwap(df):
    tp = (df["h"] + df["l"] + df["c"]) / 3
    return (tp * df["v"]).cumsum() / df["v"].cumsum()


# ═══════════════════════════════════════════════════════════
# 8) كاشات وحالات
# ═══════════════════════════════════════════════════════════
_ohlcv_cache: dict = {}
_crossover_cache: dict = {}
_price_state: dict = {}
_recent_signals: dict = {}
_last_range: dict = {}

_filter_stats = {
    "sent": 0, "filtered_score": 0, "filtered_vol": 0,
    "filtered_adx": 0, "filtered_atr": 0, "filtered_htf": 0,
    "filtered_vwap": 0, "filtered_conflict": 0,
    "filtered_15m": 0, "filtered_other": 0,
    "range_breakouts": 0,
}


# ═══════════════════════════════════════════════════════════
# 9) جلب الشموع
# ═══════════════════════════════════════════════════════════
async def fetch_ohlcv_cached(symbol, tf, limit=150):
    key = (symbol, tf, limit)
    now = _time.time()
    c = _ohlcv_cache.get(key)
    if c and (now - c["ts"]) < OHLCV_CACHE_SECONDS:
        return c["data"]
    data, ex_id = await _exchange.fetch_ohlcv(symbol, tf, limit)
    if data:
        _ohlcv_cache[key] = {"data": data, "ts": now, "ex": ex_id}
    return data


def clear_caches():
    _ohlcv_cache.clear()
    _crossover_cache.clear()
    _recent_signals.clear()


# ═══════════════════════════════════════════════════════════
# 10) HTF / VWAP / Conflict
# ═══════════════════════════════════════════════════════════
async def get_htf_trend(symbol):
    if not HTF_ENABLED:
        return "NEUTRAL"
    data = await fetch_ohlcv_cached(symbol, HTF_TIMEFRAME, 250)
    if not data or len(data) < 200:
        return "NEUTRAL"
    df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
    e50 = calc_ema(df["c"], 50)
    e200 = calc_ema(df["c"], 200)
    price = float(df["c"].iloc[-1])
    a = float(e50.iloc[-1]); b = float(e200.iloc[-1])
    if price > a > b: return "UP"
    if price < a < b: return "DOWN"
    return "NEUTRAL"


def check_vwap_slope(df, direction, thresholds):
    min_pct = thresholds.get("vwap_slope_pct", 0.05)
    if len(df) < VWAP_SLOPE_LOOKBACK + 1:
        return True, 0.0
    vwap = calc_vwap(df)
    v_now = float(vwap.iloc[-1])
    v_prev = float(vwap.iloc[-VWAP_SLOPE_LOOKBACK - 1])
    if v_prev == 0:
        return True, 0.0
    slope = (v_now - v_prev) / v_prev * 100
    if direction == "bullish" and slope < -min_pct:
        return False, slope
    if direction == "bearish" and slope > min_pct:
        return False, slope
    return True, slope


def has_conflicting_signal(symbol, direction):
    prev = _recent_signals.get(symbol)
    if not prev:
        return False
    prev_dir, prev_ts = prev
    if _time.time() - prev_ts < CONFLICT_WINDOW_SEC and prev_dir != direction:
        return True
    return False


def record_signal(symbol, direction):
    _recent_signals[symbol] = (direction, _time.time())


# ═══════════════════════════════════════════════════════════
# 11) حساب الـ Score
# ═══════════════════════════════════════════════════════════
def _stars(pts, mx):
    if mx == 0: return ""
    r = pts / mx
    if r >= 0.9:  return "⭐⭐⭐"
    if r >= 0.6:  return "⭐⭐"
    if r >= 0.3:  return "⭐"
    return "▫️"


def compute_score(df, direction, curr_idx=-2):
    close = df["c"]; vol = df["v"]
    price = float(close.iloc[curr_idx])

    # Volume
    try:
        s = max(0, len(df) + curr_idx - 20); e = len(df) + curr_idx
        vma = float(vol.iloc[s:e].mean())
        vr = float(vol.iloc[curr_idx]) / vma if vma > 0 else 0
    except Exception:
        vr = 0
    if vr >= 4.0:   v_pts, v_lbl = 30, "قوي جداً 🔥"
    elif vr >= 2.5: v_pts, v_lbl = 25, "قوي"
    elif vr >= 1.5: v_pts, v_lbl = 18, "جيد"
    elif vr >= 1.0: v_pts, v_lbl = 10, "طبيعي"
    elif vr >= 0.5: v_pts, v_lbl = 5,  "ضعيف"
    else:           v_pts, v_lbl = 0,  "ضعيف جداً ⚠️"

    # ADX
    try:  adx = float(calc_adx(df).iloc[curr_idx])
    except Exception: adx = 0
    if adx >= 50:    a_pts, a_lbl = 18, "اتجاه متطرف"
    elif adx >= 35:  a_pts, a_lbl = 20, "قوي جداً"
    elif adx >= 25:  a_pts, a_lbl = 18, "واضح"
    elif adx >= 20:  a_pts, a_lbl = 12, "ضعيف"
    elif adx >= 15:  a_pts, a_lbl = 5,  "عرضي"
    else:            a_pts, a_lbl = 0,  "عرضي جداً"

    # EMA gap
    try:
        ef = float(calc_ema(close, EMA_FAST).iloc[curr_idx])
        es = float(calc_ema(close, EMA_SLOW).iloc[curr_idx])
        gap = abs(ef - es) / es * 100 if es else 0
    except Exception:
        gap = 0
    if gap >= 0.60:   g_pts, g_lbl = 12, "كبير جداً"
    elif gap >= 0.30: g_pts, g_lbl = 15, "كبير"
    elif gap >= 0.10: g_pts, g_lbl = 10, "متوسط"
    elif gap >= 0.05: g_pts, g_lbl = 6,  "صغير"
    elif gap >= 0.02: g_pts, g_lbl = 3,  "صغير جداً"
    else:             g_pts, g_lbl = 0,  "طفيلي"

    # MACD
    try:
        _, _, hist = calc_macd(close)
        hn = float(hist.iloc[curr_idx]); hp = float(hist.iloc[curr_idx - 1])
        rising = hn > hp
        if hn > 0 and rising:    m_pts, m_lbl = 10, "يتسارع صعوداً ↗"
        elif hn > 0:             m_pts, m_lbl = 5,  "يتباطأ صعوداً ↘"
        elif hn < 0 and rising:  m_pts, m_lbl = 7,  "يتباطأ هبوطاً ↗"
        else:                    m_pts, m_lbl = 5,  "يتسارع هبوطاً ↘"
    except Exception:
        m_pts, m_lbl = 0, "غير محدد"

    # RSI
    try: rsi = float(calc_rsi(close).iloc[curr_idx])
    except Exception: rsi = 50
    if direction == "bullish":
        if 55 <= rsi <= 70:   r_pts, r_lbl = 10, "زخم صاعد صحي ⭐"
        elif rsi > 70:        r_pts, r_lbl = 5,  "تشبع شرائي"
        elif rsi >= 45:       r_pts, r_lbl = 7,  "محايد"
        elif rsi >= 30:       r_pts, r_lbl = 3,  "زخم هابط (ضد)"
        else:                 r_pts, r_lbl = 0,  "تشبع بيعي"
    else:
        if 30 <= rsi <= 45:   r_pts, r_lbl = 10, "زخم هابط صحي ⭐"
        elif rsi < 30:        r_pts, r_lbl = 5,  "تشبع بيعي"
        elif rsi <= 55:       r_pts, r_lbl = 7,  "محايد"
        elif rsi <= 70:       r_pts, r_lbl = 3,  "زخم صاعد (ضد)"
        else:                 r_pts, r_lbl = 0,  "تشبع شرائي"

    # EMA50 alignment
    try:
        e50 = float(calc_ema(close, 50).iloc[curr_idx])
    except Exception:
        e50 = price
    above = price > e50
    if direction == "bullish" and above:          h_pts, h_lbl = 10, "مع الاتجاه ✅"
    elif direction == "bearish" and not above:    h_pts, h_lbl = 10, "مع الاتجاه ✅"
    else:                                          h_pts, h_lbl = 0,  "ضد الاتجاه ⚠️"

    # ATR
    try:
        atr = float(calc_atr(df).iloc[curr_idx])
        atr_pct = (atr / price) * 100 if price > 0 else 0
    except Exception:
        atr_pct = 0
    if atr_pct >= 0.30:   t_pts, t_lbl = 5, "نشاط جيد"
    elif atr_pct >= 0.15: t_pts, t_lbl = 3, "طبيعي"
    else:                 t_pts, t_lbl = 1, "خمول"

    total = v_pts + a_pts + g_pts + m_pts + r_pts + h_pts + t_pts
    if total >= 80:   grade = "🌟 ذهبية"
    elif total >= 65: grade = "⭐ قوية"
    elif total >= 55: grade = "✅ جيدة"
    elif total >= 45: grade = "🟡 متوسطة"
    else:             grade = "⚪ ضعيفة"

    return {
        "score": total, "grade": grade,
        "adx": round(adx, 1), "adx_label": a_lbl, "adx_pts": a_pts,
        "rsi": round(rsi, 1), "rsi_label": r_lbl, "rsi_pts": r_pts,
        "macd_label": m_lbl, "macd_pts": m_pts,
        "vol_ratio": round(vr, 2), "vol_label": v_lbl, "vol_pts": v_pts,
        "atr_pct": round(atr_pct, 2), "atr_label": t_lbl, "atr_pts": t_pts,
        "htf_label": h_lbl, "htf_pts": h_pts,
        "gap_pct": round(gap, 3), "gap_label": g_lbl, "gap_pts": g_pts,
    }


# ═══════════════════════════════════════════════════════════
# 12) الفلتر الموحّد
# ═══════════════════════════════════════════════════════════
async def passes_filter(cross: dict, htf_trend: str = None) -> tuple:
    s = cross.get("support", {})
    if not s:
        return False, "لا مؤشرات", "other"

    tf = cross["timeframe"]
    vol = s.get("vol_ratio", 0)
    adx = s.get("adx", 0)
    atr = s.get("atr_pct", 0)
    score = s.get("score", 0)

    th = get_thresholds(atr_pct=atr, adx=adx)
    min_score = th["min_score"]
    if tf == "15m":
        min_score = max(min_score, th.get("score_15m_override", min_score))

    passed = True
    reason = ""
    category = ""

    if score < min_score:
        passed, reason, category = False, f"Score {score} < {min_score}", "score"
    elif vol < th["min_vol_ratio"]:
        passed, reason, category = False, f"Vol {vol}× < {th['min_vol_ratio']}×", "vol"
    elif adx < th["min_adx"]:
        passed, reason, category = False, f"ADX {adx} < {th['min_adx']}", "adx"
    elif atr > th["max_atr_pct"]:
        passed, reason, category = False, f"ATR {atr}% > {th['max_atr_pct']}%", "atr"
    elif th["block_against_htf"] and cross.get("alert_type") != "pre" and htf_trend:
        direction = cross["direction"]
        if direction == "bullish" and htf_trend == "DOWN":
            passed, reason, category = False, "HTF=DOWN ضد LONG", "htf"
        elif direction == "bearish" and htf_trend == "UP":
            passed, reason, category = False, "HTF=UP ضد SHORT", "htf"

    df = cross.get("_df")
    if passed and df is not None:
        ok, slope = check_vwap_slope(df, cross["direction"], th)
        if not ok:
            passed, reason, category = False, f"VWAP {slope:+.2f}% ضد", "vwap"

    if passed and cross.get("alert_type") == "confirmed":
        if has_conflicting_signal(cross["symbol"], cross["direction"]):
            passed, reason, category = False, "تعارض", "conflict"

    if STRICTNESS == "auto":
        _smart.record(passed, score)

    return passed, reason, category


# ═══════════════════════════════════════════════════════════
# 13) كواشف الإشارات
# ═══════════════════════════════════════════════════════════
async def _df_for(symbol, tf, extra=60):
    data = await fetch_ohlcv_cached(symbol, tf, EMA_SLOW + extra)
    if not data or len(data) < EMA_SLOW + 5:
        return None
    return pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])


def _classify_strength(tf, gap):
    if tf == "1h":
        if gap >= 0.30: return "🔥 قوية جداً"
        if gap >= 0.10: return "🟢 قوية"
        return "🟡 متوسطة"
    if tf == "15m":
        if gap >= 0.20: return "🟢 قوية"
        if gap >= 0.10: return "🟡 متوسطة"
        return "⚪ ضعيفة"
    if gap >= 0.25: return "🟢 قوية"
    if gap >= 0.10: return "🟡 متوسطة"
    return "⚪ ضعيفة"


async def detect_crossover(symbol, tf):
    if not ENABLE_CONFIRMED: return None
    df = await _df_for(symbol, tf)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST)
    df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr, prev = -2, -3
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])
    bull = (cf > cs) and (pf <= ps)
    bear = (cf < cs) and (pf >= ps)
    if not (bull or bear): return None
    direction = "bullish" if bull else "bearish"
    cts = int(df["ts"].iloc[curr])
    key = (symbol, tf)
    last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts and last.get("direction") == direction:
        return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": direction}
    support = compute_score(df, direction, curr)
    return {
        "symbol": symbol, "timeframe": tf, "direction": direction,
        "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
        "price": round(float(df["c"].iloc[curr]), 8),
        "candle_ts": cts,
        "gap_pct": round(abs(cf - cs) / cs * 100, 3),
        "strength": _classify_strength(tf, abs(cf - cs) / cs * 100),
        "alert_type": "confirmed", "support": support, "_df": df,
        "exchange": _exchange.primary_name,
    }


async def detect_live_crossover(symbol, tf):
    if not ENABLE_LIVE_CROSS: return None
    df = await _df_for(symbol, tf)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST)
    df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr, prev = -1, -2
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])
    bull = (cf > cs) and (pf <= ps)
    bear = (cf < cs) and (pf >= ps)
    if not (bull or bear): return None
    direction = "bullish" if bull else "bearish"
    cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "live")
    last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": direction}
    support = compute_score(df, direction, curr)
    return {
        "symbol": symbol, "timeframe": tf, "direction": direction,
        "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
        "price": round(float(df["c"].iloc[curr]), 8),
        "candle_ts": cts,
        "gap_pct": round(abs(cf - cs) / cs * 100, 3),
        "strength": "⚡ مبدئي",
        "alert_type": "live", "support": support, "_df": df,
        "exchange": _exchange.primary_name,
    }


_TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
    "4h": 14_400_000, "1d": 86_400_000,
}


async def detect_pre_crossover(symbol, tf):
    if not ENABLE_PRE_CROSS: return None
    df = await _df_for(symbol, tf, extra=10)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST)
    df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr = -2
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    gap = abs(cf - cs) / cs * 100
    if gap >= PRE_CROSS_GAP: return None
    gaps = []
    for i in range(PRE_CROSS_LOOKBACK):
        idx = curr - i
        f = float(df["ef"].iloc[idx]); s = float(df["es"].iloc[idx])
        gaps.append(abs(f - s) / s * 100)
    gaps_ch = list(reversed(gaps))
    if not all(gaps_ch[i] >= gaps_ch[i+1] for i in range(len(gaps_ch)-1)):
        return None
    if abs(cf - cs) < 1e-9: return None
    direction = "bullish" if cf < cs else "bearish"
    cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "pre")
    last = _crossover_cache.get(key)
    if last:
        tf_ms = _TF_MS.get(tf, 900_000)
        if cts - last.get("candle_ts", 0) < tf_ms * PRE_CROSS_COOLDOWN:
            return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": direction}
    support = compute_score(df, direction, curr)
    return {
        "symbol": symbol, "timeframe": tf, "direction": direction,
        "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
        "price": round(float(df["c"].iloc[curr]), 8),
        "candle_ts": cts, "gap_pct": round(gap, 3),
        "strength": "🔔 تقارب وشيك",
        "alert_type": "pre", "support": support, "_df": df,
        "exchange": _exchange.primary_name,
    }


# ═══════════════════════════════════════════════════════════
# 14) تنبيه التغير المفاجئ
# ═══════════════════════════════════════════════════════════
async def detect_sudden_change(symbol):
    data, _ = await _exchange.fetch_ohlcv(symbol, "1m", 3)
    if not data or len(data) < 2: return None
    prev = float(data[-2][4]); cur = float(data[-1][4])
    if prev == 0: return None
    pct = ((cur - prev) / prev) * 100
    th = PRICE_CHANGE_THRESHOLDS.get(symbol, DEFAULT_THRESHOLD)
    st = _price_state.setdefault(symbol, {"direction": None, "count": 0, "alerting": False})
    if abs(pct) >= th:
        d = "up" if pct > 0 else "down"
        if st["direction"] != d:
            st["direction"] = d; st["count"] = 0; st["alerting"] = False
        if not st["alerting"]:
            st["alerting"] = True; st["count"] = 1
        elif st["count"] < MAX_PRICE_ALERTS:
            st["count"] += 1
        else:
            return None
        return {
            "symbol": symbol, "direction": d,
            "change_pct": round(pct, 2),
            "current_price": round(cur, 8), "prev_price": round(prev, 8),
            "threshold": th, "alert_count": st["count"],
            "max_alerts": MAX_PRICE_ALERTS,
        }
    else:
        st["direction"] = None; st["count"] = 0; st["alerting"] = False
    return None


# ═══════════════════════════════════════════════════════════
# 15) تحليل النطاق + تخزينه
# ═══════════════════════════════════════════════════════════
def _store_range(symbol: str, rng: dict):
    _last_range[symbol] = {
        "lower": float(rng["suggested_lower"]),
        "upper": float(rng["suggested_upper"]),
        "computed_at": _time.time(),
        "state": "inside",
        "last_alert_at": 0.0,
        "range_pct": rng.get("range_pct", 0),
        "grids": rng.get("grids", 0),
    }
    log.info(
        f"📌 Range stored: {symbol} "
        f"[{rng['suggested_lower']:.6f} – {rng['suggested_upper']:.6f}]"
    )


async def analyze_range(symbol):
    data, _ = await _exchange.fetch_ohlcv(
        symbol, "1d", MORNING_REPORT_LOOKBACK_DAYS + 20
    )
    if not data or len(data) < 10:
        return None
    df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
    cur = float(df["c"].iloc[-1])
    if cur == 0: return None
    df["tr"] = pd.concat([
        df["h"] - df["l"],
        (df["h"] - df["c"].shift()).abs(),
        (df["l"] - df["c"].shift()).abs(),
    ], axis=1).max(axis=1)
    atr = float(df["tr"].tail(14).mean())
    atr_pct = (atr / cur) * 100
    recent = df.tail(MORNING_REPORT_LOOKBACK_DAYS)
    highest = float(recent["h"].max()); lowest = float(recent["l"].min())

    span = atr * MORNING_REPORT_ATR_MULTIPLIER * RANGE_WIDTH_MULTIPLIER
    if span <= 0: return None

    lower = cur - span / 2; upper = cur + span / 2
    rng_pct = ((upper - lower) / cur) * 100

    if rng_pct > MORNING_REPORT_MAX_RANGE_PCT:
        span = cur * (MORNING_REPORT_MAX_RANGE_PCT / 100)
        lower = cur - span / 2; upper = cur + span / 2
        rng_pct = MORNING_REPORT_MAX_RANGE_PCT
    if rng_pct <= 0: return None

    ideal = int(rng_pct / 0.10)
    grids = max(MORNING_REPORT_MIN_GRIDS, min(MORNING_REPORT_MAX_GRIDS, ideal))
    step = (upper - lower) / grids
    step_pct = (step / cur) * 100 if cur else 0

    result = {
        "symbol": symbol, "current": cur,
        "highest": highest, "lowest": lowest,
        "suggested_lower": lower, "suggested_upper": upper,
        "range_pct": round(rng_pct, 2), "atr_pct": round(atr_pct, 2),
        "atr_multiplier": MORNING_REPORT_ATR_MULTIPLIER,
        "width_multiplier": RANGE_WIDTH_MULTIPLIER,
        "max_range_pct": MORNING_REPORT_MAX_RANGE_PCT,
        "grids": grids, "grid_step": step,
        "grid_step_pct": round(step_pct, 3),
        "lookback": MORNING_REPORT_LOOKBACK_DAYS,
    }
    _store_range(symbol, result)
    return result


# ═══════════════════════════════════════════════════════════
# 16) مراقبة خروج السعر عن النطاق
# ═══════════════════════════════════════════════════════════
async def _get_current_price(symbol: str) -> float:
    data, _ = await _exchange.fetch_ohlcv(symbol, "1m", 2)
    if not data:
        return 0.0
    return float(data[-1][4])


def _range_is_fresh(r: dict) -> bool:
    return (_time.time() - r["computed_at"]) < RANGE_MAX_AGE_HOURS * 3600


async def check_range_breakout(symbol: str):
    if not RANGE_BREAKOUT_ENABLED:
        return None
    r = _last_range.get(symbol)
    if not r or not _range_is_fresh(r):
        return None

    price = await _get_current_price(symbol)
    if price <= 0:
        return None

    now = _time.time()
    cooldown_sec = RANGE_BREAKOUT_COOLDOWN_MIN * 60

    if r["lower"] <= price <= r["upper"]:
        r["state"] = "inside"
        return None

    if price > r["upper"]:
        if r["state"] == "above":
            return None
        if now - r["last_alert_at"] < cooldown_sec:
            return None
        r["state"] = "above"
        r["last_alert_at"] = now
        return {
            "symbol": symbol, "direction": "above",
            "price": price, "boundary": r["upper"],
            "lower": r["lower"], "upper": r["upper"],
            "deviation_pct": ((price - r["upper"]) / r["upper"]) * 100,
            "age_min": int((now - r["computed_at"]) / 60),
        }

    if price < r["lower"]:
        if r["state"] == "below":
            return None
        if now - r["last_alert_at"] < cooldown_sec:
            return None
        r["state"] = "below"
        r["last_alert_at"] = now
        return {
            "symbol": symbol, "direction": "below",
            "price": price, "boundary": r["lower"],
            "lower": r["lower"], "upper": r["upper"],
            "deviation_pct": ((r["lower"] - price) / r["lower"]) * 100,
            "age_min": int((now - r["computed_at"]) / 60),
        }
    return None


def build_range_breakout_msg(b: dict) -> str:
    up = b["direction"] == "above"
    emoji = "🚨" if up else "⚠️"
    title = "اختراق صعودي — فوق النطاق 🟢" if up else "اختراق هبوطي — تحت النطاق 🔴"
    arrow = "⬆️" if up else "⬇️"
    return (
        f"{emoji} <b>خروج عن النطاق — {short(b['symbol'])}</b>\n"
        f"🇸🇾 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>{title}</b>\n"
        f"{arrow} السعر الحالي: <b>{fmt_price(b['price'])}</b>\n"
        f"🎯 الحد المخترق: <b>{fmt_price(b['boundary'])}</b>\n"
        f"📏 الانحراف: <b>{b['deviation_pct']:.3f}%</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• النطاق المحدد: <b>{fmt_price(b['lower'])} – {fmt_price(b['upper'])}</b>\n"
        f"• عُمر النطاق: <b>{b['age_min']} دقيقة</b>\n"
        f"• المصدر: {_exchange.primary_name}\n\n"
        f"💡 <i>قد يكون بداية اتجاه جديد — راقب الزخم.</i>"
    )


# ═══════════════════════════════════════════════════════════
# 17) بناء رسائل الإشارات
# ═══════════════════════════════════════════════════════════
def build_signal_message(cross):
    sym = cross["symbol"]; tf = cross["timeframe"]
    bull = cross["direction"] == "bullish"
    at = cross.get("alert_type", "confirmed")
    s = cross.get("support", {})
    score = s.get("score", 0); grade = s.get("grade", "?")

    if at == "pre":
        emoji, title = "🔔", "تقارب وشيك"
        type_lbl = "🔔 <b>تحذير مبكر</b>"
        dir_lbl = "🟢 محتمل صاعد" if bull else "🔴 محتمل هابط"
    elif at == "live":
        emoji, title = "⚡", "تقاطع مبدئي"
        type_lbl = "⚡ <b>مبدئي</b> — شمعة جارية"
        dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"
    else:
        emoji, title = ("🚀" if bull else "🔻"), "تقاطع مؤكد"
        type_lbl = "✅ <b>مؤكد</b> — شمعة مغلقة"
        dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"

    candle_time = syria_from_ts(cross["candle_ts"])
    support_block = (
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📌 مؤشرات داعمة:</b>\n"
        f"• الحجم: <b>{s.get('vol_ratio','?')}×</b> "
        f"{_stars(s.get('vol_pts',0),30)} <i>({s.get('vol_pts',0)}/30)</i>\n"
        f"• ADX: <b>{s.get('adx','?')}</b> "
        f"{_stars(s.get('adx_pts',0),20)} <i>({s.get('adx_pts',0)}/20)</i>\n"
        f"• فرق EMA: <b>{s.get('gap_pct','?')}%</b> <i>({s.get('gap_pts',0)}/15)</i>\n"
        f"• MACD: {s.get('macd_label','?')} <i>({s.get('macd_pts',0)}/10)</i>\n"
        f"• RSI: <b>{s.get('rsi','?')}</b> — {s.get('rsi_label','?')} "
        f"<i>({s.get('rsi_pts',0)}/10)</i>\n"
        f"• EMA50: {s.get('htf_label','?')} <i>({s.get('htf_pts',0)}/10)</i>\n"
        f"• ATR: {s.get('atr_pct','?')}% — {s.get('atr_label','?')} "
        f"<i>({s.get('atr_pts',0)}/5)</i>\n"
    )
    return (
        f"{emoji} <b>{title} — {short(sym)} [{tf}]</b>\n"
        f"🇸🇾 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{type_lbl}\n"
        f"📊 <b>{dir_lbl}</b>\n"
        f"⚡ القوة: <b>{cross['strength']}</b>\n"
        f"📏 فرق EMA: <b>{cross['gap_pct']:.3f}%</b>\n"
        f"🎯 <b>العلامة: {score}/100 {grade}</b>\n"
        f"{support_block}"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• EMA{EMA_FAST}: {fmt_price(cross['ema_fast'])}\n"
        f"• EMA{EMA_SLOW}: {fmt_price(cross['ema_slow'])}\n"
        f"• السعر: {fmt_price(cross['price'])}\n"
        f"• وقت الشمعة: {candle_time}\n"
        f"• المصدر: {cross.get('exchange','?')} ({MARKET_TYPE})\n"
        f"• التشدد: <b>{STRICTNESS}</b>"
    )


def build_price_alert_msg(a):
    up = a["direction"] == "up"
    emoji = "🚀" if up else "🔻"
    title = "ارتفاع مفاجئ 🟢" if up else "انخفاض مفاجئ 🔴"
    return (
        f"{emoji} <b>تغير مفاجئ — {short(a['symbol'])}</b>\n"
        f"🇸🇾 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>{title}</b>\n"
        f"📈 التغير: <b>{a['change_pct']}%</b>\n"
        f"🎯 العتبة: {a['threshold']}%\n"
        f"🔔 التنبيه: <b>{a['alert_count']} من {a['max_alerts']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• الحالي: {fmt_price(a['current_price'])}\n"
        f"• السابق: {fmt_price(a['prev_price'])}\n"
        f"• المصدر: {_exchange.primary_name}"
    )


def build_range_report(analyses, title="🌅 التقرير الصباحي"):
    if not analyses:
        return f"{title}\n📭 لا بيانات كافية."
    header = (
        f"{title} — <b>النطاقات المقترحة</b>\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"📅 آخر <b>{MORNING_REPORT_LOOKBACK_DAYS}</b> أيام\n"
        f"📐 ATR× <b>{MORNING_REPORT_ATR_MULTIPLIER}</b> × "
        f"<b>{RANGE_WIDTH_MULTIPLIER}</b> | سقف: {MORNING_REPORT_MAX_RANGE_PCT}%\n"
        f"🔌 المصدر: <b>{_exchange.primary_name}</b>\n"
        f"🚨 المراقبة: {'✅ مفعلة' if RANGE_BREAKOUT_ENABLED else '❌ معطلة'}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
    )
    body = ""
    for a in analyses:
        body += (
            f"\n💠 <b>{short(a['symbol'])}</b> — الحالي: <b>{fmt_price(a['current'])}</b>\n"
            f"  📉 أدنى: {fmt_price(a['lowest'])}\n"
            f"  📈 أعلى: {fmt_price(a['highest'])}\n"
            f"  🎯 النطاق: <b>{fmt_price(a['suggested_lower'])} – {fmt_price(a['suggested_upper'])}</b>\n"
            f"  📊 العرض: {a['range_pct']}% | ATR: {a['atr_pct']}%\n"
            f"  🔢 شبكات: <b>{a['grids']}</b> | خطوة: {fmt_price(a['grid_step'])} "
            f"({a['grid_step_pct']}%)\n"
            f"  ──────────────────\n"
        )
    footer = "\n💡 <i>الخطوة ≥ 0.10% لتغطية الرسوم. سيتم إشعارك عند خروج السعر عن أي نطاق.</i>"
    return header + body + footer


# ═══════════════════════════════════════════════════════════
# 18) الأوامر
# ═══════════════════════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    th = get_thresholds()
    overrides = "\n".join(f"  • {k} = {v}" for k, v in STRICTNESS_OVERRIDE.items()) or "  (لا يوجد)"
    await update.message.reply_text(
        f"🔀 <b>Unified Smart Bot v3.3</b> — بدون Binance\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🔌 الأساسية: <b>{_exchange.primary_name}</b> ({MARKET_TYPE})\n"
        f"🔁 البدائل: {', '.join(_exchange.chain_names[1:]) or '—'}\n\n"
        f"<b>📊 إشارات ({len(SIG_SYMBOLS)}):</b>\n"
        f"{', '.join(short(s) for s in SIG_SYMBOLS)}\n\n"
        f"<b>🌅 نطاق ({len(RNG_SYMBOLS)}):</b>\n"
        f"{', '.join(short(s) for s in RNG_SYMBOLS)}\n\n"
        f"<b>⚡ مفاجئ ({len(SC_SYMBOLS)}):</b>\n"
        f"{', '.join(short(s) for s in SC_SYMBOLS)}\n\n"
        f"<b>🎯 STRICTNESS:</b> <b>{STRICTNESS.upper()}</b>\n"
        f"• Score ≥ {th['min_score']}\n"
        f"• Volume ≥ {th['min_vol_ratio']}×\n"
        f"• ADX ≥ {th['min_adx']}\n"
        f"• ATR ≤ {th['max_atr_pct']}%\n"
        f"• VWAP slope ≥ {th['vwap_slope_pct']}%\n"
        f"• HTF block: {'✅' if th['block_against_htf'] else '❌'}\n\n"
        f"<b>📐 عرض النطاق:</b> <b>{RANGE_WIDTH_MULTIPLIER}×</b>\n"
        f"<b>🚨 مراقبة الاختراق:</b> "
        f"{'✅ كل ' + str(RANGE_BREAKOUT_INTERVAL_MIN) + ' د' if RANGE_BREAKOUT_ENABLED else '❌'}\n\n"
        f"<b>التجاوزات اليدوية:</b>\n{overrides}\n\n"
        f"<b>الأوامر:</b>\n"
        f"/cross /cross5 /cross15 /cross1h\n"
        f"/checkprice /report /range\n"
        f"/ranges /clearranges\n"
        f"/symbols /status /strict /clearcache",
        parse_mode="HTML",
    )


async def _run_cross(update, tfs=None):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة متاحة.")
        return
    tfs = tfs or TIMEFRAMES
    await update.message.reply_text(f"🔍 فحص: {', '.join(tfs)} ...")
    clear_caches()

    detectors = []
    if ENABLE_PRE_CROSS:  detectors.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: detectors.append(detect_live_crossover)
    if ENABLE_CONFIRMED:  detectors.append(detect_crossover)

    found = 0; filtered = 0
    for sym in SIG_SYMBOLS:
        for tf in tfs:
            for k in [(sym, tf), (sym, tf, "pre"), (sym, tf, "live")]:
                _crossover_cache.pop(k, None)
            for det in detectors:
                cross = await det(sym, tf)
                if not cross: continue
                htf = await get_htf_trend(sym)
                passed, _r, _c = await passes_filter(cross, htf)
                if not passed:
                    filtered += 1; continue
                try:
                    await update.message.reply_text(
                        build_signal_message(cross), parse_mode="HTML"
                    )
                    found += 1
                except Exception as e:
                    log.error(f"send {sym} {tf}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)

    msg = f"✅ {found} إشارة."
    if filtered: msg += f"\n🚫 {filtered} محجوبة (STRICTNESS={STRICTNESS})."
    if found == 0 and filtered == 0:
        msg = f"⚪ لا إشارات على {', '.join(tfs)}"
    await update.message.reply_text(msg)


async def cmd_cross(u, c):   await _run_cross(u)
async def cmd_cross5(u, c):  await _run_cross(u, ["5m"])
async def cmd_cross15(u, c): await _run_cross(u, ["15m"])
async def cmd_cross1h(u, c): await _run_cross(u, ["1h"])


async def cmd_checkprice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    await update.message.reply_text("🔍 فحص ...")
    found = 0
    for sym in SC_SYMBOLS:
        a = await detect_sudden_change(sym)
        if a:
            try:
                await update.message.reply_text(build_price_alert_msg(a), parse_mode="HTML")
                found += 1
            except Exception as e:
                log.error(f"send {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if found == 0:
        await update.message.reply_text("⚪ لا تغيرات مفاجئة.")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    await update.message.reply_text(
        f"🔍 تحليل {len(RNG_SYMBOLS)} رمز على مدى {MORNING_REPORT_LOOKBACK_DAYS} أيام..."
    )
    analyses = []
    for sym in RNG_SYMBOLS:
        try:
            a = await analyze_range(sym)
            if a: analyses.append(a)
        except Exception as e:
            log.exception(f"range {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if not analyses:
        await update.message.reply_text("⚪ لا بيانات."); return
    try:
        await update.message.reply_text(
            build_range_report(analyses, "📊 تقرير النطاق"),
            parse_mode="HTML",
        )
    except Exception as e:
        log.error(f"report: {e}")


async def cmd_ranges(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _last_range:
        await update.message.reply_text(
            "📭 لا نطاقات محفوظة.\nاستخدم /report أو انتظر التقرير الصباحي."
        )
        return
    lines = ["<b>📊 النطاقات المراقبة:</b>\n"]
    now = _time.time()
    for sym, r in _last_range.items():
        age = int((now - r["computed_at"]) / 60)
        state_icon = {"inside": "🟢", "above": "🔺", "below": "🔻"}.get(r["state"], "⚪")
        fresh_icon = "" if _range_is_fresh(r) else " ⏰"
        lines.append(
            f"{state_icon} <b>{short(sym)}</b> "
            f"[{fmt_price(r['lower'])} – {fmt_price(r['upper'])}] "
            f"<i>({age} د){fresh_icon}</i>"
        )
    lines.append(f"\n🚨 الإشعارات: "
                 f"{'✅ كل ' + str(RANGE_BREAKOUT_INTERVAL_MIN) + ' د' if RANGE_BREAKOUT_ENABLED else '❌ معطلة'}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_clearranges(update: Update, context: ContextTypes.DEFAULT_TYPE):
    n = len(_last_range)
    _last_range.clear()
    await update.message.reply_text(f"✅ تم مسح {n} نطاق محفوظ.")


async def cmd_symbols(update, context):
    await update.message.reply_text(
        f"<b>📊 إشارات ({len(SIG_SYMBOLS)}):</b>\n"
        + "\n".join(f"• {s}" for s in SIG_SYMBOLS)
        + f"\n\n<b>🌅 نطاق ({len(RNG_SYMBOLS)}):</b>\n"
        + "\n".join(f"• {s}" for s in RNG_SYMBOLS)
        + f"\n\n<b>⚡ مفاجئ ({len(SC_SYMBOLS)}):</b>\n"
        + "\n".join(f"• {s}" for s in SC_SYMBOLS),
        parse_mode="HTML",
    )


async def cmd_strict(update, context):
    th = get_thresholds()
    dbg = th.get("_debug", {})
    mode_desc = STRICTNESS.upper()
    overrides = "\n".join(f"  • {k} = {v}" for k, v in STRICTNESS_OVERRIDE.items()) or "  (لا يوجد)"

    debug_block = ""
    if STRICTNESS == "auto" and dbg:
        debug_block = (
            f"\n<b>🧠 تفاصيل Auto:</b>\n"
            f"• النمط: {dbg.get('regime','?')} (ATR={dbg.get('atr_pct')}%)\n"
            f"• Score أساسي: {dbg.get('base_score')}\n"
            f"• ADX Δ: {dbg.get('adx_delta'):+d}\n"
            f"• Feedback Δ: {dbg.get('feedback_delta'):+d}\n"
            f"• نسبة القبول: {dbg.get('pass_ratio')} "
            f"({dbg.get('sample_n')} عينة)\n"
        )

    await update.message.reply_text(
        f"🎯 <b>التشدد:</b> <b>{mode_desc}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"<b>العتبات الفعالة:</b>\n"
        f"• Score ≥ <b>{th['min_score']}</b>\n"
        f"• Score 15m ≥ <b>{th.get('score_15m_override', th['min_score'])}</b>\n"
        f"• Volume ≥ <b>{th['min_vol_ratio']}×</b>\n"
        f"• ADX ≥ <b>{th['min_adx']}</b>\n"
        f"• ATR ≤ <b>{th['max_atr_pct']}%</b>\n"
        f"• VWAP slope ≥ <b>{th['vwap_slope_pct']}%</b>\n"
        f"• فرق EMA ≥ <b>{th['min_ema_gap']}%</b>\n"
        f"• RSI LONG: {th['rsi_long']}\n"
        f"• RSI SHORT: {th['rsi_short']}\n"
        f"• HTF block: {'✅' if th['block_against_htf'] else '❌'}\n"
        f"{debug_block}\n"
        f"<b>📐 عرض النطاق:</b> <b>{RANGE_WIDTH_MULTIPLIER}×</b>\n"
        f"<b>التجاوزات اليدوية:</b>\n{overrides}",
        parse_mode="HTML",
    )


async def cmd_clearcache(update, context):
    clear_caches()
    _smart.reset_feedback()
    await update.message.reply_text(
        "✅ تم تفريغ:\n"
        "• كاش الشموع\n"
        "• كاش التقاطعات\n"
        "• إشارات التعارض\n"
        "• سجل التغذية الراجعة (Auto)\n"
        "⚠️ النطاقات المحفوظة لم تُمس (استخدم /clearranges)."
    )


async def cmd_status(update, context):
    total_filtered = sum(_filter_stats.values()) - _filter_stats["sent"] - _filter_stats["range_breakouts"]
    await update.message.reply_text(
        f"🤖 <b>الحالة</b>\n"
        f"🔌 {_exchange.primary_name} | بدائل: {len(_exchange.chain_names)-1}\n"
        f"🎯 STRICTNESS: <b>{STRICTNESS}</b>\n"
        f"📐 Width Multiplier: <b>{RANGE_WIDTH_MULTIPLIER}×</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 مُرسَل: <b>{_filter_stats['sent']}</b>\n"
        f"🚨 اختراق نطاق: <b>{_filter_stats['range_breakouts']}</b>\n"
        f"🚫 محجوب:\n"
        f"  • Score: {_filter_stats['filtered_score']}\n"
        f"  • Volume: {_filter_stats['filtered_vol']}\n"
        f"  • ADX: {_filter_stats['filtered_adx']}\n"
        f"  • ATR: {_filter_stats['filtered_atr']}\n"
        f"  • HTF: {_filter_stats['filtered_htf']}\n"
        f"  • VWAP: {_filter_stats['filtered_vwap']}\n"
        f"  • Conflict: {_filter_stats['filtered_conflict']}\n"
        f"  • 15m: {_filter_stats['filtered_15m']}\n"
        f"الإجمالي: {total_filtered}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💾 كاش: {len(_ohlcv_cache)} OHLCV\n"
        f"📌 نطاقات: {len(_last_range)}\n"
        f"🧠 سجل Auto: {len(_smart.history)} عينة",
        parse_mode="HTML",
    )


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        log.warning(f"⚠️ network: {err}"); return
    log.error(f"❌ error: {err}", exc_info=err)


# ═══════════════════════════════════════════════════════════
# 19) Jobs
# ═══════════════════════════════════════════════════════════
async def crossover_job(context: ContextTypes.DEFAULT_TYPE):
    if not _exchange.ok(): return
    detectors = []
    if ENABLE_PRE_CROSS:  detectors.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: detectors.append(detect_live_crossover)
    if ENABLE_CONFIRMED:  detectors.append(detect_crossover)

    total = 0
    for sym in SIG_SYMBOLS:
        for tf in TIMEFRAMES:
            for det in detectors:
                try:
                    cross = await det(sym, tf)
                    if not cross: continue
                    htf = await get_htf_trend(sym)
                    passed, reason, cat = await passes_filter(cross, htf)
                    if not passed:
                        key = f"filtered_{cat}"
                        if key in _filter_stats:
                            _filter_stats[key] += 1
                        else:
                            _filter_stats["filtered_other"] += 1
                        continue
                    if CHAT_ID:
                        try:
                            await context.bot.send_message(
                                chat_id=CHAT_ID,
                                text=build_signal_message(cross),
                                parse_mode="HTML",
                            )
                            total += 1
                            _filter_stats["sent"] += 1
                            if cross["alert_type"] == "confirmed":
                                record_signal(sym, cross["direction"])
                        except Exception as e:
                            log.error(f"send {sym} {tf}: {e}")
                except Exception as e:
                    log.exception(f"job {sym} {tf}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if total:
        log.info(f"📤 {total} إشارة (STRICTNESS={STRICTNESS})")


async def price_alert_job(context: ContextTypes.DEFAULT_TYPE):
    if not _exchange.ok(): return
    total = 0
    for sym in SC_SYMBOLS:
        try:
            a = await detect_sudden_change(sym)
            if a and CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=CHAT_ID, text=build_price_alert_msg(a), parse_mode="HTML"
                    )
                    total += 1
                except Exception as e:
                    log.error(f"send {sym}: {e}")
        except Exception as e:
            log.exception(f"price_alert {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if total:
        log.info(f"📤 {total} تغير مفاجئ")


async def range_breakout_job(context: ContextTypes.DEFAULT_TYPE):
    if not RANGE_BREAKOUT_ENABLED or not _exchange.ok(): return
    if not _last_range: return

    total = 0
    for symbol in list(_last_range.keys()):
        try:
            b = await check_range_breakout(symbol)
            if b and CHAT_ID:
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=build_range_breakout_msg(b),
                    parse_mode="HTML",
                )
                total += 1
                _filter_stats["range_breakouts"] += 1
                log.info(f"🚨 Breakout: {symbol} {b['direction']} "
                         f"{b['deviation_pct']:.3f}%")
        except Exception as e:
            log.exception(f"range_breakout {symbol}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if total:
        log.info(f"📤 {total} اختراق نطاق")


async def morning_report_job(context: ContextTypes.DEFAULT_TYPE):
    if not MORNING_REPORT_ENABLED or not _exchange.ok(): return
    analyses = []
    for sym in RNG_SYMBOLS:
        try:
            a = await analyze_range(sym)
            if a: analyses.append(a)
        except Exception as e:
            log.exception(f"morning {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if not analyses or not CHAT_ID: return
    try:
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=build_range_report(analyses, "🌅 التقرير الصباحي"),
            parse_mode="HTML",
        )
    except Exception as e:
        log.error(f"send morning: {e}")


# ═══════════════════════════════════════════════════════════
# 20) Health
# ═══════════════════════════════════════════════════════════
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Unified Smart Bot v3.3 (No Binance)")

    def do_HEAD(self):
        self.send_response(200); self.end_headers()

    def log_message(self, *a): pass


def run_health():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()


# ═══════════════════════════════════════════════════════════
# 21) Main
# ═══════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════
# 21) Main
# ═══════════════════════════════════════════════════════════
def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ TELEGRAM_REPORT_BOT_TOKEN و TELEGRAM_REPORT_CHAT_ID مطلوبان")
        return

    threading.Thread(target=run_health, daemon=True).start()

    th = get_thresholds()
    print("🔀 Unified Smart Bot v3.3 (No Binance)")
    print(f"🔌 {_exchange.primary_name} | بدائل: {_exchange.chain_names[1:]}")
    print(f"📊 إشارات: {len(SIG_SYMBOLS)} | نطاق: {len(RNG_SYMBOLS)} | مفاجئ: {len(SC_SYMBOLS)}")
    print(f"📏 EMA {EMA_FAST}/{EMA_SLOW} | فريمات: {TIMEFRAMES}")
    print(f"🎯 STRICTNESS = {STRICTNESS}")
    print(f"   • Score ≥ {th['min_score']} | Vol ≥ {th['min_vol_ratio']}× | "
          f"ADX ≥ {th['min_adx']} | ATR ≤ {th['max_atr_pct']}%")
    print(f"📐 Range Width Multiplier = {RANGE_WIDTH_MULTIPLIER}")
    print(f"🚨 Range Breakout: {'✅ كل ' + str(RANGE_BREAKOUT_INTERVAL_MIN) + ' د' if RANGE_BREAKOUT_ENABLED else '❌'}")

    # ═══════════════════════════════════════════════════════
    # Application + HTTPXRequest with timeouts (PTB v21+)
    # ═══════════════════════════════════════════════════════
    from telegram.request import HTTPXRequest

    request = HTTPXRequest(
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=30.0,
        pool_timeout=30.0,
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(request)
        .build()
    )

    # ═══════════════════════════════════════════════════════
    # Command Handlers
    # ═══════════════════════════════════════════════════════
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cross", cmd_cross))
    app.add_handler(CommandHandler("cross5", cmd_cross5))
    app.add_handler(CommandHandler("cross15", cmd_cross15))
    app.add_handler(CommandHandler("cross1h", cmd_cross1h))
    app.add_handler(CommandHandler("checkprice", cmd_checkprice))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("range", cmd_report))
    app.add_handler(CommandHandler("ranges", cmd_ranges))
    app.add_handler(CommandHandler("clearranges", cmd_clearranges))
    app.add_handler(CommandHandler("symbols", cmd_symbols))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("strict", cmd_strict))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    app.add_error_handler(error_handler)

    # ═══════════════════════════════════════════════════════
    # Job Queue
    # ═══════════════════════════════════════════════════════
    if app.job_queue:
        app.job_queue.run_repeating(
            crossover_job,
            interval=JOB_INTERVAL_MIN * 60,
            first=15,
            name="cross",
        )
        app.job_queue.run_repeating(
            price_alert_job,
            interval=PRICE_ALERT_INTERVAL_MIN * 60,
            first=20,
            name="price",
        )

        if RANGE_BREAKOUT_ENABLED:
            app.job_queue.run_repeating(
                range_breakout_job,
                interval=RANGE_BREAKOUT_INTERVAL_MIN * 60,
                first=30,
                name="range_breakout",
            )
            print(f"🚨 Range breakout: كل {RANGE_BREAKOUT_INTERVAL_MIN} دقيقة")

        if MORNING_REPORT_ENABLED:
            from datetime import time as dt_time
            now_syr = datetime.now(SYRIA_TZ)
            target_syr = now_syr.replace(
                hour=MORNING_REPORT_HOUR,
                minute=0,
                second=0,
                microsecond=0,
            )
            target_utc = target_syr.astimezone(timezone.utc)
            app.job_queue.run_daily(
                morning_report_job,
                time=dt_time(
                    hour=target_utc.hour,
                    minute=target_utc.minute,
                    tzinfo=timezone.utc,
                ),
                name="morning",
            )
            print(f"🌅 التقرير الصباحي: {MORNING_REPORT_HOUR}:00 (سوريا)")

    # ═══════════════════════════════════════════════════════
    # Start Polling
    # ═══════════════════════════════════════════════════════
    print("✅ جاهز — بدء polling")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        bootstrap_retries=5,
    )


if __name__ == "__main__":
    main()
