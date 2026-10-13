"""
Unified Smart Bot v6.6.1 — Binance Futures Execution (Fixed Env + Callback)
============================================================================
  🆕 v6.6.1 — إصلاح قراءة مفاتيح البيئة (_read_env_clean)
  🆕 v6.6.1 — إصلاح AttributeError في Callback Queries (_reply)
  🆕 v6.6.1 — أمر /bcheck للتشخيص
  🆕 v6.6.1 — BinanceExecutor.error_msg واضح
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

load_dotenv()


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
            report[k] = "❌ غير موجودة"
        else:
            cleaned = _read_env_clean(k)
            if not cleaned:
                report[k] = f"❌ فارغة (raw len={len(raw)})"
            else:
                report[k] = (f"✅ طول={len(cleaned)} | "
                             f"بداية={cleaned[:6]}...{cleaned[-4:]}")
    return report


# ═══════════════════════════════════════════════════════════
# 1) الإعدادات العامة
# ═══════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("TELEGRAM_REPORT_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_REPORT_CHAT_ID")

RANGE_PCT = 6.0
RANGE_REPORT_INTERVAL_HOURS = 4
SEND_STARTUP_REPORT = True

PRIMARY_EXCHANGE = "bybit"
FALLBACK_EXCHANGES = ["okx", "kucoin", "bitget", "mexc", "gate"]
MARKET_TYPE = "swap"

TIMEFRAMES = ["5m", "15m", "1h"]
EMA_FAST, EMA_SLOW = 7, 25
JOB_INTERVAL_MIN = 2

ENABLE_PRE_CROSS  = True
ENABLE_LIVE_CROSS = True
ENABLE_CONFIRMED  = True
PRE_CROSS_GAP = 0.05
PRE_CROSS_LOOKBACK = 3
PRE_CROSS_COOLDOWN = 3

ENABLE_RSI_DIVERGENCE = True
ENABLE_BB_SQUEEZE     = True
ENABLE_SR_BREAKOUT    = True

RSI_DIV_LOOKBACK = 35
RSI_DIV_COOLDOWN_MIN = 30
BB_SQUEEZE_PCTL = 0.20
BB_SQUEEZE_COOLDOWN_MIN = 30
SR_BREAKOUT_LOOKBACK = 50
SR_BREAKOUT_COOLDOWN_MIN = 30

MTF_ENABLED = True
SESSION_ADJUST_ENABLED = True

OHLCV_CACHE_SECONDS = 90
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

SYMBOL_DELAY_MS = 200
SYRIA_TZ = ZoneInfo("Asia/Damascus")

MORNING_REPORT_ENABLED = True
MORNING_REPORT_HOUR_SYRIA = 10
MORNING_REPORT_TF = "1h"
MORNING_REPORT_LIMIT = 200
SWING_LOOKBACK = 3
SWING_MAX_COUNT = 4

SIGNAL_TRACK_MAX_MIN = 90
SIGNAL_TRACK_INTERVAL_MIN = 2
MIN_SL_PCT = {"5m": 0.35, "15m": 0.50, "1h": 0.80}

LAST_SIGNALS_MAX = 20

CHART_LOOKBACK = 120
CHART_DPI      = 110
CHART_ENABLED  = True
SEND_SR_CHARTS = True


# ═══════════════════════════════════════════════════════════
# 1b) ⚙️ إعدادات Binance Futures
# ═══════════════════════════════════════════════════════════
BINANCE_ENABLED       = _env_int("BINANCE_ENABLED", 1) == 1
BINANCE_TESTNET       = _env_int("BINANCE_TESTNET", 0) == 1
BINANCE_API_KEY       = _read_env_clean("BINANCE_API_KEY")
BINANCE_API_SECRET    = _read_env_clean("BINANCE_API_SECRET")
NOTIONAL_USDT         = _env_float("NOTIONAL_USDT", 400.0)
LEVERAGE              = _env_int("LEVERAGE", 20)
MARGIN_TYPE           = (_read_env_clean("MARGIN_TYPE") or "ISOLATED").upper()
STRONG_SCORE_FOR_ADD  = _env_int("STRONG_SCORE_FOR_ADD", 80)
POSITION_MONITOR_SEC  = _env_int("POSITION_MONITOR_INTERVAL_SEC", 60)
CLOSE_REASON_TP       = "TP1"
CLOSE_REASON_SL       = "SL"


# ── إعدادات الفلترة ─────────────────────────────────
MIN_EMA_GAP_PCT = {
    "5m":  _env_float("MIN_EMA_GAP_5M",  0.02),
    "15m": _env_float("MIN_EMA_GAP_15M", 0.015),
    "1h":  _env_float("MIN_EMA_GAP_1H",  0.01),
}
MIN_ATR_PCT = {
    "5m":  _env_float("MIN_ATR_5M",  0.05),
    "15m": _env_float("MIN_ATR_15M", 0.10),
    "1h":  _env_float("MIN_ATR_1H",  0.20),
}
MIN_ADX_FOR_BB       = _env_float("MIN_ADX_FOR_BB", 8.0)
SYMBOL_COOLDOWN_SEC  = _env_int("SYMBOL_COOLDOWN_SEC", 180)
STRICTNESS_MAX_FINAL = _env_int("STRICTNESS_MAX_FINAL", 80)

ABS_MIN_SCORE        = _env_int("ABS_MIN_SCORE", 52)
BB_MIN_VOL           = _env_float("BB_MIN_VOL", 1.0)
BB_MIN_ADX_IN_RANGE  = _env_float("BB_MIN_ADX_IN_RANGE", 20)
BB_CONFIRM_ENABLED   = True

FOLLOWUP_ENABLED = True
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
            log.warning(f"⚠️ {key} بدون {lower_key} — تخطي"); continue
        try:
            upper = float(str(upper_val).strip())
            lower = float(str(lower_val).strip())
        except Exception as e:
            log.warning(f"⚠️ تحويل {key}: {e}"); continue
        if upper <= lower:
            log.warning(f"⚠️ نطاق غير صالح {coin_part}"); continue
        sym = _normalize_symbol_from_var(coin_part)
        out[sym] = {"lower": lower, "upper": upper}
        log.info(f"📌 {sym}: [{lower} – {upper}]")
    return out


_MANUAL_RANGES = _detect_manual_ranges()
ALL_SYMBOLS = list(_MANUAL_RANGES.keys()) if _MANUAL_RANGES else ["XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
log.info(f"✅ {len(_MANUAL_RANGES)} نطاق يدوي | {len(ALL_SYMBOLS)} رمز")


# ═══════════════════════════════════════════════════════════
# 3) منصات ccxt
# ═══════════════════════════════════════════════════════════
_EXCHANGE_NAMES = ["bybit", "okx", "kucoin", "kraken", "gate",
                   "bitget", "mexc", "htx", "coinex", "bitmart"]
_EXCHANGE_CLASSES = {n: getattr(ccxt, n) for n in _EXCHANGE_NAMES if hasattr(ccxt, n)}
log.info(f"📦 ccxt {ccxt.__version__}")


class MultiExchange:
    def __init__(self):
        self.primary = None
        self.fallbacks = []
        self._init_all()

    def _make(self, name):
        if name == "binance" or name not in _EXCHANGE_CLASSES: return None
        try:
            opts = {"enableRateLimit": True, "timeout": 60000,
                    "options": {"defaultType": MARKET_TYPE, "fetchOHLCV": {"maxLimit": 200}}}
            if name == "bybit":
                opts["rateLimit"] = 500
                opts["options"]["unifiedMargin"] = False
            ex = _EXCHANGE_CLASSES[name](opts)
            ex.load_markets()
            log.info(f"✅ {ex.name} | {len(ex.markets)} سوق")
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
                data = await asyncio.to_thread(ex.fetch_ohlcv, symbol, tf, None, limit)
                if data and len(data) > 0:
                    return data, ex.id
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
    (0, 6,  "🌏 آسيا (هادئة)",   +8),
    (6, 10, "🌅 آسيا/أوروبا",     +2),
    (10, 13, "🇪🇺 أوروبا",        0),
    (13, 16, "🔥 ذروة",           -5),
    (16, 21, "🇺🇸 أمريكا",        0),
    (21, 24, "🌙 الهدوء الليلي",  +5),
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
            recent = list(self.history)[-50:]
        non_expired = [r for r in results if not r.get("expired")]
        if len(non_expired) >= 15:
            wins = sum(1 for r in non_expired if r["win"])
            ratio = wins / len(non_expired); n = len(non_expired)
        else:
            n = len(recent)
            if n < 20: return 0, None, 0
            ratio = sum(1 for p, _, _, _ in recent if p) / n
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
    """ردّ آمن يعمل مع الرسائل و الـ Callbacks."""
    msg = _get_target_message(update)
    if msg is None:
        log.warning("⚠️ _reply: لا message في update")
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
        if session == "day":   skey = ts.dt.floor("D")
        elif session == "week": skey = ts.dt.to_period("W").astype(str)
        else: skey = ts.dt.floor("H")
        tp = (df["h"] + df["l"] + df["c"]) / 3
        pv = tp * df["v"]
        cum_pv = pv.groupby(skey).cumsum()
        cum_v = df["v"].groupby(skey).cumsum()
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
    if df is not None and len(df) >= 60:
        m = classify_market(df)
        if m != "UNKNOWN": return m
    try:
        data = await fetch_ohlcv_cached(symbol, "15m", 80)
        if data and len(data) >= 60:
            df2 = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
            return classify_market(df2)
    except Exception: pass
    return "UNKNOWN"


def compute_levels(df, direction, curr_idx=-2, tf="15m"):
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
        return {"entry": price, "stop_loss": stop_loss, "sl_pct": round(risk_pct, 2),
                "tp1": tp1, "tp2": tp2, "tp3": tp3, "atr": atr}
    except Exception as e:
        log.warning(f"compute_levels: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# 7) الكاشات
# ═══════════════════════════════════════════════════════════
_ohlcv_cache = {}
_crossover_cache = {}
_price_state = {}
_recent_signals = {}
_last_range = {}
_last_6pct_report = {}
_pending_signals = {}
_divergence_cache = {}
_bb_cache = {}
_sr_cache = {}
_last_signal_time = {}
_last_signals = deque(maxlen=LAST_SIGNALS_MAX)

_filter_stats = {
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
    "trades_manual_close": 0,
}


# ═══════════════════════════════════════════════════════════
# 8) جلب الشموع
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
    _ohlcv_cache.clear(); _crossover_cache.clear()
    _recent_signals.clear(); _divergence_cache.clear()
    _bb_cache.clear(); _sr_cache.clear()


# ═══════════════════════════════════════════════════════════
# 9) HTF / VWAP / MTF
# ═══════════════════════════════════════════════════════════
async def get_htf_trend(symbol):
    if not HTF_ENABLED: return "NEUTRAL"
    data = await fetch_ohlcv_cached(symbol, HTF_TIMEFRAME, 250)
    if not data or len(data) < 200: return "NEUTRAL"
    df = pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])
    e50 = float(calc_ema(df["c"], 50).iloc[-1])
    e200 = float(calc_ema(df["c"], 200).iloc[-1])
    price = float(df["c"].iloc[-1])
    if price > e50 > e200: return "UP"
    if price < e50 < e200: return "DOWN"
    return "NEUTRAL"


async def get_mtf_alignment(symbol, direction):
    if not MTF_ENABLED: return 0, "—"
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
    if score >= 3:   return +8, "توافق كامل (3/3) 🌟"
    if score == 2:   return +3, "توافق جزئي (2/3) ✅"
    if score == 1:   return -5, "توافق ضعيف (1/3) ⚠️"
    return -15, "تعارض كامل (0/3) ❌"


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
    a_pts = 18 if adx >= 50 else 20 if adx >= 35 else 18 if adx >= 25 else 12 if adx >= 20 else 5 if adx >= 15 else 0
    try:
        ef = float(calc_ema(close, EMA_FAST).iloc[curr_idx])
        es = float(calc_ema(close, EMA_SLOW).iloc[curr_idx])
        gap = abs(ef - es) / es * 100 if es else 0
    except Exception: gap = 0
    g_pts = 12 if gap >= 0.6 else 15 if gap >= 0.3 else 10 if gap >= 0.1 else 6 if gap >= 0.05 else 3 if gap >= 0.02 else 0
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
    s = cross.get("support", {})
    if not s: return False, "لا مؤشرات", "other"
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
        return False, f"فرق EMA {gap_pct:.3f}% < {min_gap}%", "ema_gap"

    min_atr = MIN_ATR_PCT.get(tf, 0.05)
    if atr < min_atr: return False, f"ATR {atr}% < {min_atr}%", "atr_low"

    mtf_bonus, mtf_desc = await get_mtf_alignment(cross["symbol"], direction)
    cross["mtf"] = mtf_desc; cross["mtf_bonus"] = mtf_bonus
    score = base_score + mtf_bonus
    s["score"] = score; cross["final_score"] = score

    if mtf_bonus <= -15: return False, mtf_desc, "mtf"
    if score < ABS_MIN_SCORE: return False, f"Score {score} < {ABS_MIN_SCORE}", "score_abs"

    df = cross.get("_df")
    market = cross.get("market") or "UNKNOWN"
    if market == "UNKNOWN":
        market = await classify_market_safe(cross["symbol"], tf, df)
        cross["market"] = market

    th = get_thresholds(atr_pct=atr, adx=adx, tf=tf)
    min_score = th["min_score"]
    if tf == "15m": min_score = max(min_score, th.get("score_15m_override", min_score))

    if score < min_score: return False, f"Score {score} < {min_score:.0f}", "score"
    if vol < th["min_vol_ratio"]: return False, f"Vol {vol}× < {th['min_vol_ratio']}×", "vol"
    if adx < th["min_adx"]: return False, f"ADX {adx} < {th['min_adx']}", "adx"
    if atr > th["max_atr_pct"]: return False, f"ATR {atr}% > {th['max_atr_pct']}%", "atr"

    if source == "bb_squeeze":
        if adx < MIN_ADX_FOR_BB: return False, f"ADX {adx} < {MIN_ADX_FOR_BB} لـ BB", "adx"
        if vol < BB_MIN_VOL: return False, f"BB Vol {vol}× < {BB_MIN_VOL}×", "vol"
        if market == "RANGE" and adx < BB_MIN_ADX_IN_RANGE:
            return False, f"BB في رينج يحتاج ADX ≥ {BB_MIN_ADX_IN_RANGE}", "range"
        if BB_CONFIRM_ENABLED:
            confirmed = (mtf_bonus >= 8) or (adx > 22) or (vol > 1.5)
            if not confirmed: return False, "BB بدون تأكيد", "bb_confirm"

    if source == "ema":
        if market == "TREND_DOWN" and direction == "bullish" and mtf_bonus < 5:
            return False, "تقاطع صاعد في ترند هابط", "trend"
        elif market == "TREND_UP" and direction == "bearish" and mtf_bonus < 5:
            return False, "تقاطع هابط في ترند صاعد", "trend"

    if source == "rsi_div":
        if market == "TREND_DOWN" and direction == "bullish" and mtf_bonus < 3:
            return False, "دايفرجنس ضد الترند", "trend"
        elif market == "TREND_UP" and direction == "bearish" and mtf_bonus < 3:
            return False, "دايفرجنس ضد الترند", "trend"

    if market == "RANGE" and source == "ema" and cross.get("alert_type") == "confirmed":
        return False, "سوق عرضي — تقاطع وهمي", "range"

    if th["block_against_htf"] and cross.get("alert_type") != "pre" and htf_trend:
        if direction == "bullish" and htf_trend == "DOWN":
            return False, "HTF=DOWN ضد LONG", "htf"
        elif direction == "bearish" and htf_trend == "UP":
            return False, "HTF=UP ضد SHORT", "htf"

    if df is not None:
        ok, slope = check_vwap_slope(df, direction, th, tf=tf)
        if not ok: return False, f"VWAP {slope:+.2f}% ضد", "vwap"

    if cross.get("alert_type") == "confirmed":
        if has_conflicting_signal(cross["symbol"], direction):
            return False, "تعارض", "conflict"

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
    levels = compute_levels(df, direction, curr, tf=tf)
    market = await classify_market_safe(symbol, tf, df)
    support = compute_score(df, direction, curr)
    out = {"symbol": symbol, "timeframe": tf, "direction": direction,
           "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
           "price": round(float(df["c"].iloc[curr]), 8), "candle_ts": cts,
           "gap_pct": round(abs(cf - cs) / cs * 100, 3) if cs else 0,
           "strength": strength, "alert_type": alert_type,
           "support": support, "base_score": support["score"],
           "final_score": support["score"], "_df": df,
           "exchange": _exchange.primary_name, "levels": levels,
           "source": source, "market": market, "created_at": _time.time()}
    if extra: out.update(extra)
    return out


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
    curr, prev = -1, -2
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])
    bull = (cf > cs) and (pf <= ps); bear = (cf < cs) and (pf >= ps)
    if not (bull or bear): return None
    d = "bullish" if bull else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "live")
    last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "live", "⚡ مبدئي", curr)


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
    if not best or best["strength"] < 0.15: return None
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


async def detect_bb_squeeze_breakout(symbol, tf):
    if not ENABLE_BB_SQUEEZE: return None
    df = await _df_for(symbol, tf, extra=80)
    if df is None or len(df) < 60: return None
    bb = calc_bollinger(df); curr = -2
    key = (symbol, tf, "bb")
    last = _bb_cache.get(key)
    if last and (_time.time() - last) < BB_SQUEEZE_COOLDOWN_MIN * 60: return None
    hist_width = bb["width"].iloc[curr-50:curr].dropna()
    if len(hist_width) < 20: return None
    if float(bb["width"].iloc[curr]) > hist_width.quantile(BB_SQUEEZE_PCTL): return None
    price = float(df["c"].iloc[curr])
    up = float(bb["upper"].iloc[curr]); lo = float(bb["lower"].iloc[curr])
    d = None
    if price > up:   d = "bullish"
    elif price < lo: d = "bearish"
    if not d: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _bb_cache[key] = _time.time()
    _filter_stats["signals_bb"] += 1
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "💥 كسر BB Squeeze", curr,
                              source="bb_squeeze")
    cross["bb_info"] = {"width": round(float(bb["width"].iloc[curr]), 2),
                        "upper": up, "lower": lo}
    return cross


async def detect_sr_breakout(symbol, tf):
    if not ENABLE_SR_BREAKOUT: return None
    df = await _df_for(symbol, tf, extra=SR_BREAKOUT_LOOKBACK + 10)
    if df is None or len(df) < SR_BREAKOUT_LOOKBACK + 5: return None
    curr = -2
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
    if price_now > r1 and price_prev <= r1: d, level = "bullish", r1
    elif price_now < s1 and price_prev >= s1: d, level = "bearish", s1
    if not d: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _sr_cache[key] = _time.time()
    _filter_stats["signals_sr"] += 1
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "🎯 كسر S/R", curr,
                              source="sr_breakout")
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
    if not results: return f"{title}\n📭 لا بيانات."
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
    if not items: return f"{title}\n🇸🇾 {syria_now_str()}\n📭 لا بيانات."
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
    return {"ema": "📈 تقاطع EMA", "rsi_div": "🔄 دايفرجنس RSI",
            "bb_squeeze": "💥 BB Squeeze", "sr_breakout": "🎯 كسر S/R",
            "manual_sync": "🔧 يدوي"}.get(src, "—")


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

    if at == "pre":
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
                f"• هدف 2 (1:2): {fmt_price(levels['tp2'])}\n"
                f"• هدف 3 (1:3): {fmt_price(levels['tp3'])}\n")
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
    if not _last_range: return "📭 لا نطاقات."
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
    if result == "win_tp3": emoji, title, color = "🌟", "هدف 3 (1:3)", "🟢"
    elif result == "win_tp2": emoji, title, color = "✅", "هدف 2 (1:2)", "🟢"
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
    if result == "win_tp3" and not FOLLOWUP_ON_TP3: return
    if result == "loss_sl" and not FOLLOWUP_ON_SL: return
    if result == "expired" and not FOLLOWUP_ON_EXPIRE: return
    now = _time.time()
    if now - sig.get("last_followup_ts", 0) < FOLLOWUP_THROTTLE_SEC: return
    sig["last_followup_ts"] = now
    text = build_followup_msg(sig, result, price, age_min)
    if not text: return
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
        if age_min > SIGNAL_TRACK_MAX_MIN:
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
                elif price >= lv["tp3"]: sig["checked"] = True; new_result = "win_tp3"
                elif price >= lv["tp2"] and sig.get("result") not in ("win_tp2","win_tp3"):
                    new_result = "win_tp2"; sig["result"] = "win_tp2"
                elif price >= lv["tp1"] and not sig.get("result"):
                    new_result = "win_tp1"; sig["result"] = "win_tp1"
            else:
                if price >= lv["stop_loss"]: sig["checked"] = True; new_result = "loss_sl"
                elif price <= lv["tp3"]: sig["checked"] = True; new_result = "win_tp3"
                elif price <= lv["tp2"] and sig.get("result") not in ("win_tp2","win_tp3"):
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


log.info("🔐 تشخيص مفاتيح Binance:")
for _k, _v in _diagnose_env_keys().items():
    log.info(f"   • {_k}: {_v}")


class BinanceExecutor:
    def __init__(self):
        self.client = None
        self.symbol_info = {}
        self.ok = False
        self.error_msg = None
        self._init()

    def _init(self):
        if not BINANCE_ENABLED:
            self.error_msg = "BINANCE_ENABLED=0"
            log.info(f"ℹ️ {self.error_msg} — التنفيذ معطل")
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
            log.info(f"✅ Binance {net} | رصيد USDT: {bal:.2f}")
            self._load_exchange_info()
            self.ok = True
            self.error_msg = None
        except BinanceAPIException as e:
            self.error_msg = f"Binance API {e.code}: {e.message}"
            log.error(f"❌ {self.error_msg}")
            if e.code == -2015:
                log.error("   ⚠️ مفتاح خاطئ / صلاحيات ناقصة / IP غير مسموح")
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
            log.info(f"📦 Binance: {len(self.symbol_info)} رمز")
        except Exception as e:
            log.error(f"load_exchange_info: {e}")

    def info(self, bsym): return self.symbol_info.get(bsym)

    def round_qty(self, qty, step):
        if step <= 0: return qty
        return math.floor(qty / step) * step

    def fmt_qty(self, qty, precision):
        return f"{qty:.{precision}f}".rstrip("0").rstrip(".") or "0"

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

    def open_market(self, bsym, side, qty):
        order = self.client.futures_create_order(
            symbol=bsym, side=side, type="MARKET", quantity=qty)
        try:
            detail = self.client.futures_get_order(symbol=bsym,
                                                   orderId=order["orderId"])
        except Exception:
            detail = order
        avg_price = float(detail.get("avgPrice") or 0)
        executed = float(detail.get("executedQty") or qty)
        if avg_price == 0:
            t = self.client.futures_symbol_ticker(symbol=bsym)
            avg_price = float(t.get("price", 0))
        return {"order_id": detail.get("orderId"), "avg_price": avg_price,
                "executed_qty": executed, "raw": detail}

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
            log.error(f"get_open_positions: {e}"); return []

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
        try:
            t = self.client.futures_symbol_ticker(symbol=bsym)
            return float(t["price"])
        except Exception as e:
            log.error(f"last_price {bsym}: {e}"); return 0.0


_binance = BinanceExecutor()


# ═══════════════════════════════════════════════════════════
# 20b) 💰 تتبع الصفقات
# ═══════════════════════════════════════════════════════════
TRADE_TICKETS = {}
_symbol_ticket_index = defaultdict(list)
_ticket_lock = threading.Lock()


def _mk_ticket_id(bsym, side):
    return f"{bsym}_{side}_{int(_time.time()*1000)}"


def _add_ticket(t):
    with _ticket_lock:
        TRADE_TICKETS[t["id"]] = t
        _symbol_ticket_index[t["binance_symbol"]].append(t["id"])


def _remove_ticket(tid):
    with _ticket_lock:
        t = TRADE_TICKETS.pop(tid, None)
        if not t: return
        lst = _symbol_ticket_index.get(t["binance_symbol"], [])
        if tid in lst: lst.remove(tid)


def _tickets_for(bsym):
    with _ticket_lock:
        ids = list(_symbol_ticket_index.get(bsym, []))
        return [TRADE_TICKETS[i] for i in ids if i in TRADE_TICKETS]


# ═══════════════════════════════════════════════════════════
# 20c) 💰 تنفيذ صفقة
# ═══════════════════════════════════════════════════════════
async def execute_binance_trade(cross, bot=None):
    if not _binance.ok:
        return {"ok": False, "error": _binance.error_msg or "Binance غير متصل"}
    if cross.get("alert_type") != "confirmed":
        return {"ok": False, "error": "إشارة غير مؤكدة"}
    levels = cross.get("levels")
    if not levels:
        return {"ok": False, "error": "لا مستويات SL/TP"}

    bsym = _to_binance_symbol(cross["symbol"])
    info = _binance.info(bsym)
    if not info:
        return {"ok": False, "error": f"{bsym} غير مدعوم"}

    side_long = cross["direction"] == "bullish"
    new_side = "LONG" if side_long else "SHORT"
    score = cross.get("final_score", 0)

    existing = _tickets_for(bsym)
    if existing:
        same_side = [t for t in existing if t["side"] == new_side]
        opp_side = [t for t in existing if t["side"] != new_side]
        if opp_side:
            return {"ok": False,
                    "error": f"مركز معاكس مفتوح على {short(cross['symbol'])}"}
        if same_side and score < STRONG_SCORE_FOR_ADD:
            return {"ok": False,
                    "error": f"مركز بنفس الاتجاه (score {score} < {STRONG_SCORE_FOR_ADD})"}

    price_for_qty = float(levels["entry"])
    qty_raw = NOTIONAL_USDT / price_for_qty
    qty = _binance.round_qty(qty_raw, info["step_size"])
    if qty < info["min_qty"]:
        return {"ok": False, "error": f"qty {qty} < minQty {info['min_qty']}"}
    notional_actual = qty * price_for_qty
    if notional_actual < info["min_notional"]:
        return {"ok": False,
                "error": f"notional {notional_actual:.2f} < {info['min_notional']}"}
    qty_s = _binance.fmt_qty(qty, info["qty_precision"])

    try:
        await asyncio.to_thread(_binance._prepare_symbol, bsym)
    except Exception as e:
        log.warning(f"prepare {bsym}: {e}")

    order_side = "BUY" if side_long else "SELL"
    try:
        result = await asyncio.to_thread(_binance.open_market, bsym, order_side, qty_s)
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

    tid = _mk_ticket_id(bsym, new_side)
    ticket = {
        "id": tid, "symbol": cross["symbol"], "binance_symbol": bsym,
        "side": new_side, "qty": filled_qty, "entry": fill_price,
        "sl": levels["stop_loss"], "tp": levels["tp1"],
        "sl_pct": levels["sl_pct"],
        "score": score, "source": cross.get("source", "ema"),
        "tf": cross["timeframe"], "market": cross.get("market"),
        "opened_at": _time.time(), "manual": False,
        "order_id": result["order_id"],
        "notional": filled_qty * fill_price,
        "leverage": LEVERAGE, "signal": cross,
    }
    _add_ticket(ticket)
    _filter_stats["trades_opened"] += 1
    log.info(f"✅ فتح {new_side} {bsym} qty={filled_qty} @ {fill_price} "
             f"SL={ticket['sl']} TP={ticket['tp']}")

    if bot and CHAT_ID:
        try:
            await bot.send_message(chat_id=CHAT_ID,
                text=build_execution_msg(ticket), parse_mode="HTML")
        except Exception as e: log.error(f"send exec msg: {e}")

    return {"ok": True, "ticket": ticket}


def build_execution_msg(t):
    bull = t["side"] == "LONG"
    return (
        f"💸 <b>تم تنفيذ صفقة حقيقية</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>{short(t['symbol'])}</b> [{t['tf']}] "
        f"{'🟢 LONG' if bull else '🔴 SHORT'}\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💵 <b>الدخول:</b> {fmt_price(t['entry'])}\n"
        f"📦 <b>الكمية:</b> {t['qty']} (≈{t['notional']:.2f}$)\n"
        f"⚙️ <b>الرافعة:</b> {t['leverage']}x | {MARGIN_TYPE}\n"
        f"🎯 <b>TP:</b> {fmt_price(t['tp'])} (1:1)\n"
        f"🛑 <b>SL:</b> {fmt_price(t['sl'])} (-{t['sl_pct']}%)\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Score: <b>{t['score']}</b> | {_source_label(t['source'])}\n"
        f"🆔 <code>{t['id']}</code>\n"
        f"⚡ <i>إدارة داخلية — البوت يراقب و يُغلق</i>")


def build_close_msg(t, close_price, pnl_usdt, pnl_pct, reason, age_min):
    bull = t["side"] == "LONG"
    if reason == CLOSE_REASON_TP: emoji, title = "🎯", "TP1 مُحقَّق"
    elif reason == CLOSE_REASON_SL: emoji, title = "🛑", "وقف الخسارة"
    else: emoji, title = "ℹ️", "إغلاق"
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
        return compute_levels(df, direction, -2, tf="15m")
    except Exception as e:
        log.warning(f"compute_levels_manual {ccxt_symbol}: {e}"); return None


async def _sync_manual_positions(bot):
    try:
        positions = await asyncio.to_thread(_binance.get_open_positions)
    except Exception as e:
        log.error(f"sync get_positions: {e}"); return
    if not positions: return

    for p in positions:
        bsym = p["symbol"]; amt = p["amount"]
        side = "LONG" if amt > 0 else "SHORT"
        tickets = _tickets_for(bsym)
        tracked_qty = sum(t["qty"] for t in tickets)
        diff = abs(amt) - tracked_qty

        if abs(diff) < 1e-9: continue

        if diff > 1e-6:
            ccxt_sym = None
            for s in ALL_SYMBOLS:
                if _to_binance_symbol(s) == bsym: ccxt_sym = s; break
            if not ccxt_sym: ccxt_sym = f"{bsym[:-4]}/USDT:USDT"

            levels = await _compute_levels_for_manual(ccxt_sym, side)
            if not levels:
                log.warning(f"⚠️ manual {bsym}: cannot compute levels"); continue

            tid = _mk_ticket_id(bsym, side) + "_M"
            ticket = {
                "id": tid, "symbol": ccxt_sym, "binance_symbol": bsym,
                "side": side, "qty": diff,
                "entry": p["entry_price"] or p["mark_price"],
                "sl": levels["stop_loss"], "tp": levels["tp1"],
                "sl_pct": levels["sl_pct"],
                "score": 0, "source": "manual_sync",
                "tf": "15m", "market": None,
                "opened_at": _time.time(), "manual": True, "order_id": None,
                "notional": diff * (p["entry_price"] or p["mark_price"]),
                "leverage": LEVERAGE, "signal": None,
            }
            _add_ticket(ticket)
            _filter_stats["trades_manual_sync"] += 1
            log.info(f"🔍 Manual {side} {bsym} qty={diff} @ {ticket['entry']}")
            if bot and CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(f"🔍 <b>صفقة يدوية مرصودة</b>\n"
                              f"━━━━━━━━━━━━━━━━━━━\n"
                              f"📌 <b>{short(ccxt_sym)}</b> "
                              f"{'🟢 LONG' if side=='LONG' else '🔴 SHORT'}\n"
                              f"📦 {diff} @ {fmt_price(ticket['entry'])}\n"
                              f"🎯 TP: {fmt_price(ticket['tp'])}\n"
                              f"🛑 SL: {fmt_price(ticket['sl'])} (-{ticket['sl_pct']}%)\n"
                              f"⚡ <i>البوت يديرها الآن</i>"),
                        parse_mode="HTML")
                except Exception as e: log.error(f"manual notify: {e}")

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
    if t["side"] == "LONG":
        if lo <= t["sl"]: return CLOSE_REASON_SL, t["sl"]
        if hi >= t["tp"]: return CLOSE_REASON_TP, t["tp"]
    else:
        if hi >= t["sl"]: return CLOSE_REASON_SL, t["sl"]
        if lo <= t["tp"]: return CLOSE_REASON_TP, t["tp"]
    return None, None


async def manage_open_positions(bot=None):
    if not _binance.ok: return

    try: await _sync_manual_positions(bot)
    except Exception as e: log.exception(f"sync: {e}")

    symbols = set()
    with _ticket_lock:
        for bsym in _symbol_ticket_index.keys():
            if _symbol_ticket_index[bsym]: symbols.add(bsym)
    if not symbols: return

    for bsym in symbols:
        tickets = _tickets_for(bsym)
        if not tickets: continue
        try:
            klines = await asyncio.to_thread(
                _binance.client.futures_klines,
                symbol=bsym, interval="1m", limit=2)
        except Exception as e:
            log.warning(f"klines {bsym}: {e}"); continue
        if not klines or len(klines) < 2: continue
        k = klines[-2]
        last_candle = {"o": float(k[1]), "h": float(k[2]),
                       "l": float(k[3]), "c": float(k[4])}

        closes = defaultdict(list)
        for t in tickets:
            reason, hit_price = await _check_ticket_close(t, last_candle)
            if reason: closes[(t["side"], reason)].append((t, hit_price))

        for (side, reason), lst in closes.items():
            total_qty = sum(t["qty"] for t, _ in lst)
            if total_qty <= 0: continue
            info = _binance.info(bsym)
            qty_s = _binance.fmt_qty(
                _binance.round_qty(total_qty, info["step_size"]),
                info["qty_precision"]) if info else f"{total_qty}"

            close_side = "SELL" if side == "LONG" else "BUY"
            try:
                res = await asyncio.to_thread(_binance.close_market, bsym,
                                              close_side, qty_s)
            except BinanceAPIException as e:
                log.error(f"❌ close {bsym} {side}: {e.code} {e.message}"); continue
            except Exception as e:
                log.exception(f"close {bsym}: {e}"); continue

            close_price = res["avg_price"] or lst[0][1]
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
                log.info(f"💰 إغلاق {t['id']} | {reason} | PnL {pnl_usdt_net:+.3f} USDT")
                if bot and CHAT_ID:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=build_close_msg(t, close_price, pnl_usdt_net,
                                                 pnl_pct, reason, age_min),
                            parse_mode="HTML")
                    except Exception as e: log.error(f"close notify: {e}")


# ═══════════════════════════════════════════════════════════
# 20e) 💰 أوامر الإدارة (مُصلَّحة للـ callbacks)
# ═══════════════════════════════════════════════════════════
async def cmd_bcheck(update, context):
    """🔐 تشخيص حالة Binance API."""
    lines = ["🔐 <b>تشخيص Binance</b>", "━━━━━━━━━━━━━━━━━━━"]
    lines.append(f"• python-binance: "
                 f"{'✅' if _BINANCE_LIB_OK else '❌ غير مثبت'}")
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET"):
        raw = os.getenv(k)
        if raw is None:
            lines.append(f"• {k}: ❌ <b>غير موجودة في .env</b>")
        else:
            cleaned = _read_env_clean(k)
            if not cleaned:
                lines.append(f"• {k}: ❌ <b>فارغة</b> (raw len={len(raw)})")
            else:
                lines.append(f"• {k}: ✅ طول={len(cleaned)} | "
                             f"<code>{cleaned[:6]}...{cleaned[-4:]}</code>")
    lines.append(f"• TESTNET: {'✅' if BINANCE_TESTNET else '❌ MAINNET'}")
    lines.append(f"• ENABLED: {'✅' if BINANCE_ENABLED else '❌'}")
    lines.append(f"• Notional: {NOTIONAL_USDT}$ | Lev: {LEVERAGE}x")
    lines.append(f"• Margin: {MARGIN_TYPE}")
    lines.append("━━━━━━━━━━━━━━━━━━━")
    if _binance.ok:
        try:
            bal = await asyncio.to_thread(_binance.usdt_balance)
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
        await _reply(update, "📭 لا صفقات مفتوحة (متتبع)."); return
    lines = [f"💼 <b>الصفقات المفتوحة ({len(tickets)})</b>", "━━━━━━━━━━━━━━━━━━━"]
    now = _time.time()
    for t in tickets:
        try:
            cur = await asyncio.to_thread(_binance.last_price, t["binance_symbol"])
        except Exception:
            cur = t["entry"]
        if t["side"] == "LONG": pnl_pct = (cur - t["entry"]) / t["entry"] * 100
        else: pnl_pct = (t["entry"] - cur) / t["entry"] * 100
        pnl_icon = "🟢" if pnl_pct >= 0 else "🔴"
        age = int((now - t["opened_at"]) / 60)
        lines.append(
            f"\n{'🟢 LONG' if t['side']=='LONG' else '🔴 SHORT'} "
            f"<b>{short(t['symbol'])}</b> [{t['tf
