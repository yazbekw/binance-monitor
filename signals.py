"""
Unified Smart Bot v5.1 — Manual Ranges + 6% Reference Report
=============================================================
  ✓ متغيّرات النطاقات اليدوية للمراقبة (XRP_LOWER/XRP_UPPER ...)
  ✓ تقرير 6% مرجعي عند التشغيل + كل 4 ساعات
  ✓ إشعارات اقتراب + خروج + إشارات EMA + تنبيهات مفاجئة
  ✓ بدون Binance
"""
import os, re, asyncio, logging, threading, time as _time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import deque

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
for _n in ("httpx", "telegram", "telegram.ext", "ccxt"):
    logging.getLogger(_n).setLevel(logging.WARNING)
log = logging.getLogger("unified")

import ccxt
import pandas as pd
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import NetworkError, TimedOut

load_dotenv()


# ═══════════════════════════════════════════════════════════
# 1) الإعدادات
# ═══════════════════════════════════════════════════════════
BOT_TOKEN = os.getenv("TELEGRAM_REPORT_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_REPORT_CHAT_ID")

RANGE_PCT = 6.0                        # نسبة النطاق المرجعي
RANGE_REPORT_INTERVAL_HOURS = 4        # كل 4 ساعات
SEND_STARTUP_REPORT = True             # إرسال التقرير عند التشغيل

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


# ═══════════════════════════════════════════════════════════
# 2) اكتشاف النطاقات اليدوية من env
# ═══════════════════════════════════════════════════════════
def _normalize_symbol_from_var(name):
    name = name.strip().upper()
    if "/" in name:
        return name
    if name.endswith("_USDT"):
        name = name[:-5]
    if ":" in name:
        return name if "/" in name else name.replace("_", "/", 1)
    return f"{name}/USDT:USDT"


def _detect_manual_ranges():
    env = dict(os.environ)
    out = {}
    for key, upper_val in env.items():
        if not key.upper().endswith("_UPPER"):
            continue
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
            log.warning(f"⚠️ نطاق غير صالح {coin_part}: {lower} >= {upper}"); continue
        sym = _normalize_symbol_from_var(coin_part)
        out[sym] = {"lower": lower, "upper": upper}
        log.info(f"📌 {sym}: [{lower} – {upper}]")
    return out


_MANUAL_RANGES = _detect_manual_ranges()
ALL_SYMBOLS = list(_MANUAL_RANGES.keys()) if _MANUAL_RANGES else ["XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
log.info(f"✅ {len(_MANUAL_RANGES)} نطاق يدوي | {len(ALL_SYMBOLS)} رمز")


# ═══════════════════════════════════════════════════════════
# 3) المنصات
# ═══════════════════════════════════════════════════════════
_EXCHANGE_NAMES = ["bybit", "okx", "kucoin", "kraken", "gate",
                   "bitget", "mexc", "htx", "coinex", "bitmart"]
_EXCHANGE_CLASSES = {n: getattr(ccxt, n) for n in _EXCHANGE_NAMES if hasattr(ccxt, n)}
log.info(f"📦 ccxt {ccxt.__version__} | Available: {list(_EXCHANGE_CLASSES.keys())}")


class MultiExchange:
    def __init__(self):
        self.primary = None
        self.fallbacks = []
        self._init_all()

    def _make(self, name):
        if name == "binance" or name not in _EXCHANGE_CLASSES:
            return None
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
# 4) محرك التشدد
# ═══════════════════════════════════════════════════════════
STATIC_PROFILES = {
    "balanced": {"min_score": 60, "min_vol_ratio": 1.00, "min_adx": 18.0,
                 "max_atr_pct": 0.70, "vwap_slope_pct": 0.05,
                 "block_against_htf": True, "score_15m_override": 65},
    "strict":   {"min_score": 70, "min_vol_ratio": 1.30, "min_adx": 22.0,
                 "max_atr_pct": 0.60, "vwap_slope_pct": 0.07,
                 "block_against_htf": True, "score_15m_override": 75},
}


class SmartStrictness:
    def __init__(self):
        self.history = deque(maxlen=200)
        self._lock = threading.Lock()
        self._cached_profile = None
        self._cached_ts = 0.0
        self._cache_ttl = 60.0

    def record(self, passed, score):
        with self._lock: self.history.append((passed, score, _time.time()))

    def _feedback(self):
        with self._lock: recent = list(self.history)[-50:]
        n = len(recent)
        if n < 20: return 0, None, n
        passed = sum(1 for p, _, _ in recent if p)
        ratio = passed / n
        if ratio > 0.60:   d = +8
        elif ratio > 0.40: d = +3
        elif ratio < 0.10: d = -12
        elif ratio < 0.20: d = -6
        else:              d = 0
        return d, ratio, n

    def compute(self, atr_pct=None, adx=None):
        now = _time.time()
        if self._cached_profile and (now - self._cached_ts) < self._cache_ttl:
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
        final = max(40, min(90, base + adx_delta + fb_delta))
        prof = {"min_score": final,
                "min_vol_ratio": round(max(0.7, (final - 30) / 30), 2),
                "min_adx": round(max(12, final * 0.28), 1),
                "max_atr_pct": round(max(0.35, 1.2 - final / 100), 2),
                "vwap_slope_pct": round(0.02 + final / 1000, 3),
                "block_against_htf": final >= 55,
                "score_15m_override": min(90, final + 10),
                "_debug": {"base": base, "adx_delta": adx_delta, "fb_delta": fb_delta,
                           "pass_ratio": pass_ratio, "sample_n": sample_n}}
        self._cached_profile = prof
        self._cached_ts = now
        return prof

    def reset_feedback(self):
        with self._lock: self.history.clear()
        self._cached_profile = None
        self._cached_ts = 0.0


_smart = SmartStrictness()


def get_thresholds(atr_pct=None, adx=None):
    if STRICTNESS == "auto":
        prof = dict(_smart.compute(atr_pct=atr_pct, adx=adx))
    else:
        prof = dict(STATIC_PROFILES.get(STRICTNESS, STATIC_PROFILES["balanced"]))
        prof["_debug"] = {"mode": STRICTNESS}
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


def syria_from_ts(ts_ms):
    try:
        return datetime.fromtimestamp(ts_ms / 1000, tz=SYRIA_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "?"


def fmt_price(v):
    if v >= 1000: return f"{v:.2f}"
    if v >= 100:  return f"{v:.3f}"
    if v >= 1:    return f"{v:.4f}"
    if v >= 0.01: return f"{v:.5f}"
    return f"{v:.8f}"


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
    dif = ef - es
    dea = dif.ewm(span=sig, adjust=False).mean()
    return dif, dea, dif - dea


def calc_vwap(df):
    tp = (df["h"] + df["l"] + df["c"]) / 3
    return (tp * df["v"]).cumsum() / df["v"].cumsum()


# ═══════════════════════════════════════════════════════════
# 7) كاشات وحالات
# ═══════════════════════════════════════════════════════════
_ohlcv_cache = {}
_crossover_cache = {}
_price_state = {}
_recent_signals = {}
_last_range = {}
_last_6pct_report = {}   # symbol → {price, lower, upper, ts}

_filter_stats = {
    "sent": 0, "filtered_score": 0, "filtered_vol": 0, "filtered_adx": 0,
    "filtered_atr": 0, "filtered_htf": 0, "filtered_vwap": 0,
    "filtered_conflict": 0, "range_breakouts": 0, "range_approaches": 0,
    "range_6pct_reports": 0,
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
    _ohlcv_cache.clear()
    _crossover_cache.clear()
    _recent_signals.clear()


# ═══════════════════════════════════════════════════════════
# 9) HTF / VWAP / Conflict
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


def check_vwap_slope(df, direction, thresholds):
    min_pct = thresholds.get("vwap_slope_pct", 0.05)
    if len(df) < VWAP_SLOPE_LOOKBACK + 1: return True, 0.0
    vwap = calc_vwap(df)
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


# ═══════════════════════════════════════════════════════════
# 10) حساب الـ Score
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
    vol, adx, atr, score = s.get("vol_ratio", 0), s.get("adx", 0), s.get("atr_pct", 0), s.get("score", 0)
    th = get_thresholds(atr_pct=atr, adx=adx)
    min_score = th["min_score"]
    if tf == "15m": min_score = max(min_score, th.get("score_15m_override", min_score))

    passed, reason, cat = True, "", ""
    if score < min_score:           passed, reason, cat = False, f"Score {score} < {min_score}", "score"
    elif vol < th["min_vol_ratio"]: passed, reason, cat = False, f"Vol {vol}× < {th['min_vol_ratio']}×", "vol"
    elif adx < th["min_adx"]:       passed, reason, cat = False, f"ADX {adx} < {th['min_adx']}", "adx"
    elif atr > th["max_atr_pct"]:   passed, reason, cat = False, f"ATR {atr}% > {th['max_atr_pct']}%", "atr"
    elif th["block_against_htf"] and cross.get("alert_type") != "pre" and htf_trend:
        d = cross["direction"]
        if d == "bullish" and htf_trend == "DOWN": passed, reason, cat = False, "HTF=DOWN ضد LONG", "htf"
        elif d == "bearish" and htf_trend == "UP": passed, reason, cat = False, "HTF=UP ضد SHORT", "htf"

    df = cross.get("_df")
    if passed and df is not None:
        ok, slope = check_vwap_slope(df, cross["direction"], th)
        if not ok: passed, reason, cat = False, f"VWAP {slope:+.2f}% ضد", "vwap"
    if passed and cross.get("alert_type") == "confirmed":
        if has_conflicting_signal(cross["symbol"], cross["direction"]):
            passed, reason, cat = False, "تعارض", "conflict"
    if STRICTNESS == "auto": _smart.record(passed, score)
    return passed, reason, cat


# ═══════════════════════════════════════════════════════════
# 12) كواشف الإشارات
# ═══════════════════════════════════════════════════════════
async def _df_for(symbol, tf, extra=60):
    data = await fetch_ohlcv_cached(symbol, tf, EMA_SLOW + extra)
    if not data or len(data) < EMA_SLOW + 5: return None
    return pd.DataFrame(data, columns=["ts", "o", "h", "l", "c", "v"])


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
    return {"symbol": symbol, "timeframe": tf, "direction": d,
            "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
            "price": round(float(df["c"].iloc[curr]), 8), "candle_ts": cts,
            "gap_pct": round(abs(cf - cs) / cs * 100, 3), "strength": "✅ مؤكد",
            "alert_type": "confirmed", "support": compute_score(df, d, curr),
            "_df": df, "exchange": _exchange.primary_name}


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
    return {"symbol": symbol, "timeframe": tf, "direction": d,
            "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
            "price": round(float(df["c"].iloc[curr]), 8), "candle_ts": cts,
            "gap_pct": round(abs(cf - cs) / cs * 100, 3), "strength": "⚡ مبدئي",
            "alert_type": "live", "support": compute_score(df, d, curr),
            "_df": df, "exchange": _exchange.primary_name}


_TF_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "1d": 86_400_000}


async def detect_pre_crossover(symbol, tf):
    if not ENABLE_PRE_CROSS: return None
    df = await _df_for(symbol, tf, extra=10)
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
    return {"symbol": symbol, "timeframe": tf, "direction": d,
            "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
            "price": round(float(df["c"].iloc[curr]), 8), "candle_ts": cts,
            "gap_pct": round(gap, 3), "strength": "🔔 تقارب وشيك",
            "alert_type": "pre", "support": compute_score(df, d, curr),
            "_df": df, "exchange": _exchange.primary_name}


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
        elif st["count"] < MAX_PRICE_ALERTS:
            st["count"] += 1
        else:
            return None
        return {"symbol": symbol, "direction": d, "change_pct": round(pct, 2),
                "current_price": round(cur, 8), "prev_price": round(prev, 8),
                "threshold": DEFAULT_THRESHOLD, "alert_count": st["count"],
                "max_alerts": MAX_PRICE_ALERTS}
    else:
        st["direction"] = None; st["count"] = 0; st["alerting"] = False
    return None


# ═══════════════════════════════════════════════════════════
# 14) تخزين النطاقات اليدوية
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
# 15) 🆕 حساب نطاق 6% المرجعي
# ═══════════════════════════════════════════════════════════
async def _get_current_price(symbol):
    data, _ = await _exchange.fetch_ohlcv(symbol, "1m", 2)
    if not data: return 0.0
    return float(data[-1][4])


async def compute_6pct_ranges():
    """
    يحسب نطاق 6% لكل عملة (السعر ± 3%)
    يُخزّن النتيجة في _last_6pct_report
    """
    global _last_6pct_report
    results = []
    half = RANGE_PCT / 200.0  # 6% → 0.03

    for symbol in ALL_SYMBOLS:
        try:
            price = await _get_current_price(symbol)
            if price <= 0:
                log.warning(f"⚠️ لا سعر لـ {symbol}")
                continue
            lower = price * (1 - half)
            upper = price * (1 + half)
            width = upper - lower
            results.append({
                "symbol": symbol,
                "price": price,
                "lower": lower,
                "upper": upper,
                "width": width,
                "pct": RANGE_PCT,
                "ts": _time.time(),
            })
            _last_6pct_report[symbol] = results[-1]
            _filter_stats["range_6pct_reports"] += 1
            log.info(f"📊 6% ref: {symbol} price={price:.6f} "
                     f"range=[{lower:.6f} – {upper:.6f}]")
        except Exception as e:
            log.exception(f"compute_6pct {symbol}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)

    return results


def build_6pct_report(results, title="📊 نطاقات 6% المرجعية"):
    """يبني رسالة تقرير 6% بتنسيق يسهل نسخه لـ Render"""
    if not results:
        return f"{title}\n📭 لا بيانات."

    header = (
        f"{title}\n"
        f"🇸🇾 {syria_now_str()}\n"
        f"📐 النسبة: <b>{RANGE_PCT}%</b> (السعر ± {RANGE_PCT/2}%)\n"
        f"🔌 المصدر: <b>{_exchange.primary_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
    )

    body = ""
    env_block = "\n<b>📋 انسخ إلى Render → Environment:</b>\n<code>"
    for r in results:
        body += (
            f"\n🔹 <b>{short(r['symbol'])}</b>\n"
            f"   💰 السعر الحالي: <b>{fmt_price(r['price'])}</b>\n"
            f"   📉 الأدنى ({RANGE_PCT/2}%-): <b>{fmt_price(r['lower'])}</b>\n"
            f"   📈 الأعلى ({RANGE_PCT/2}%+): <b>{fmt_price(r['upper'])}</b>\n"
            f"   📏 العرض: {fmt_price(r['width'])}\n"
        )
        sym_up = short(r["symbol"])
        env_block += f"{sym_up}_LOWER={r['lower']:.6f}\n{sym_up}_UPPER={r['upper']:.6f}\n"

    env_block += "</code>\n"
    footer = (
        f"\n━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ التقرير القادم: بعد {RANGE_REPORT_INTERVAL_HOURS} ساعات\n"
        f"💡 <i>هذه القيم للمرجع فقط — النطاقات المُراقَبة حالياً هي ما وضعته في Render.</i>"
    )
    return header + body + env_block + footer


# ═══════════════════════════════════════════════════════════
# 16) مراقبة النطاق اليدوي: اقتراب + خروج
# ═══════════════════════════════════════════════════════════
async def check_range_breakout(symbol):
    if not RANGE_BREAKOUT_ENABLED: return None
    r = _last_range.get(symbol)
    if not r: return None
    price = await _get_current_price(symbol)
    if price <= 0: return None
    now = _time.time()
    cooldown = RANGE_BREAKOUT_COOLDOWN_MIN * 60
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
    dist_up = 100 - position_pct
    dist_down = position_pct

    if dist_up <= RANGE_APPROACH_PCT:
        side, dist = "upper", dist_up
    elif dist_down <= RANGE_APPROACH_PCT:
        side, dist = "lower", dist_down
    else:
        r["last_near_side"] = None; return None

    now = _time.time()
    cooldown = RANGE_APPROACH_COOLDOWN_MIN * 60
    if r["last_near_side"] == side and now - r["last_approach_alert_at"] < cooldown:
        return None
    r["last_near_side"] = side; r["last_approach_alert_at"] = now

    mid = (r["lower"] + r["upper"]) / 2
    shift = price - mid
    shift_pct = (shift / mid) * 100 if mid else 0
    width_ratio = width / price if price > 0 else 0

    if width_ratio > 0.40:
        new_width = price * 0.08
        sug_lower = price - new_width / 2
        sug_upper = price + new_width / 2
        stype = "rebuild"
        snote = f"⚠️ العرض ضخم ({width_ratio*100:.1f}% من السعر) — يُقترح إعادة بناء"
    else:
        sug_lower = price - width / 2
        sug_upper = price + width / 2
        stype = "shift"
        snote = "إزاحة النطاق (بنفس العرض)"

    return {"symbol": symbol, "side": side, "price": price,
            "lower": r["lower"], "upper": r["upper"], "width": width,
            "position_pct": round(position_pct, 1), "distance_pct": round(dist, 1),
            "suggested_lower": sug_lower, "suggested_upper": sug_upper,
            "shift": shift, "shift_pct": shift_pct,
            "age_min": int((now - r["computed_at"]) / 60),
            "source": r.get("source", "manual"),
            "suggestion_type": stype, "suggestion_note": snote,
            "width_ratio": round(width_ratio * 100, 1)}


# ═══════════════════════════════════════════════════════════
# 17) رسائل النطاق اليدوي
# ═══════════════════════════════════════════════════════════
def build_breakout_msg(b):
    up = b["direction"] == "above"
    emoji = "🚨" if up else "🔻"
    title = "خروج صعودي — فوق الحد الأعلى 🟢" if up else "خروج هبوطي — تحت الحد الأدنى 🔴"
    arrow = "⬆️" if up else "⬇️"
    return (
        f"{emoji} <b>خروج عن النطاق — {short(b['symbol'])}</b>\n"
        f"🇸🇾 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>{title}</b>\n"
        f"{arrow} السعر: <b>{fmt_price(b['price'])}</b>\n"
        f"🎯 الحد المخترق: <b>{fmt_price(b['boundary'])}</b>\n"
        f"📏 المسافة خارج النطاق: <b>{b['deviation_pct']:.2f}%</b> من العرض\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• النطاق المُراقَب: <b>{fmt_price(b['lower'])} – {fmt_price(b['upper'])}</b>\n"
        f"• العرض: {fmt_price(b['width'])}\n"
        f"• عمر: {b['age_min']} دقيقة"
    )


def build_approach_msg(a):
    up = a["side"] == "upper"
    emoji = "⚠️" if up else "🔔"
    title = "اقتراب من الحد الأعلى 🔺" if up else "اقتراب من الحد الأدنى 🔻"
    arrow = "⬆️" if up else "⬇️"
    shift_sign = "+" if a["shift"] > 0 else ""
    return (
        f"{emoji} <b>اقتراب من النطاق — {short(a['symbol'])}</b>\n"
        f"🇸🇾 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>{title}</b>\n"
        f"{arrow} السعر: <b>{fmt_price(a['price'])}</b>\n"
        f"📏 المسافة للحد: <b>{a['distance_pct']}%</b> من العرض\n"
        f"📍 الموضع في النطاق: <b>{a['position_pct']}%</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📍 النطاق المُراقَب:</b>\n"
        f"   • الأدنى: {fmt_price(a['lower'])}\n"
        f"   • الأعلى: {fmt_price(a['upper'])}\n"
        f"   • العرض: {fmt_price(a['width'])}\n\n"
        f"<b>💡 الاقتراح ({a.get('suggestion_type','shift')}):</b>\n"
        f"   • {a.get('suggestion_note','')}\n"
        f"   • الأدنى الجديد: <b>{fmt_price(a['suggested_lower'])}</b>\n"
        f"   • الأعلى الجديد: <b>{fmt_price(a['suggested_upper'])}</b>\n"
        f"   • عرض النطاق/السعر: {a.get('width_ratio',0)}%\n\n"
        f"⚙️ <i>عدّل في Render: {short(a['symbol'])}_LOWER / {short(a['symbol'])}_UPPER</i>"
    )


# ═══════════════════════════════════════════════════════════
# 18) رسائل الإشارات
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
        type_lbl = "⚡ <b>مبدئي</b>"
        dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"
    else:
        emoji, title = ("🚀" if bull else "🔻"), "تقاطع مؤكد"
        type_lbl = "✅ <b>مؤكد</b>"
        dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"

    return (
        f"{emoji} <b>{title} — {short(sym)} [{tf}]</b>\n"
        f"🇸🇾 <b>{syria_now_str()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{type_lbl}\n"
        f"📊 <b>{dir_lbl}</b>\n"
        f"📏 فرق EMA: <b>{cross['gap_pct']:.3f}%</b>\n"
        f"🎯 <b>العلامة: {score}/100 {grade}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• الحجم: {s.get('vol_ratio','?')}× ({s.get('vol_pts',0)}/30)\n"
        f"• ADX: {s.get('adx','?')} ({s.get('adx_pts',0)}/20)\n"
        f"• RSI: {s.get('rsi','?')} ({s.get('rsi_pts',0)}/10)\n"
        f"• ATR: {s.get('atr_pct','?')}% ({s.get('atr_pts',0)}/5)\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• السعر: {fmt_price(cross['price'])}\n"
        f"• المصدر: {cross.get('exchange','?')} ({MARKET_TYPE})"
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
        f"🔔 التنبيه: <b>{a['alert_count']} من {a['max_alerts']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"• الحالي: {fmt_price(a['current_price'])}\n"
        f"• السابق: {fmt_price(a['prev_price'])}"
    )


def build_ranges_report():
    if not _last_range:
        return "📭 لا نطاقات محددة."
    lines = ["<b>📊 النطاقات المُراقَبة:</b>\n"]
    now = _time.time()
    for sym, r in _last_range.items():
        age = int((now - r["computed_at"]) / 60)
        state_icon = {"inside": "🟢", "above": "🔺", "below": "🔻"}.get(r["state"], "⚪")
        width = r["upper"] - r["lower"]
        lines.append(
            f"{state_icon} <b>{short(sym)}</b>\n"
            f"   الأدنى: <b>{fmt_price(r['lower'])}</b>\n"
            f"   الأعلى: <b>{fmt_price(r['upper'])}</b>\n"
            f"   العرض: {fmt_price(width)} | عمر: {age} د\n"
        )
    lines.append(
        f"\n🎯 <b>المعايير:</b>\n"
        f"• تحذير الاقتراب: عند {RANGE_APPROACH_PCT}% من الحد\n"
        f"• فحص كل: {RANGE_MONITOR_INTERVAL_MIN} دقيقة\n"
        f"• كولداون اقتراب: {RANGE_APPROACH_COOLDOWN_MIN} د\n"
        f"• كولداون خروج: {RANGE_BREAKOUT_COOLDOWN_MIN} د\n\n"
        f"📊 <b>تقرير 6% المرجعي:</b> كل {RANGE_REPORT_INTERVAL_HOURS} ساعات\n"
        f"💡 استخدم /ref6 لعرضه فوراً"
    )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# 19) الأوامر
# ═══════════════════════════════════════════════════════════
async def cmd_start(update, context):
    th = get_thresholds()
    ranges_txt = "\n".join(
        f"• <b>{short(s)}</b>: {fmt_price(r['lower'])} – {fmt_price(r['upper'])}"
        for s, r in _last_range.items()
    ) or "  (لا نطاقات)"
    await update.message.reply_text(
        f"🔀 <b>Unified Smart Bot v5.1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🔌 الأساسية: <b>{_exchange.primary_name}</b>\n"
        f"🔁 البدائل: {', '.join(_exchange.chain_names[1:]) or '—'}\n\n"
        f"<b>📌 النطاقات المُراقَبة ({len(_last_range)}):</b>\n{ranges_txt}\n\n"
        f"<b>🎯 التشدد:</b> <b>{STRICTNESS.upper()}</b>\n"
        f"• Score ≥ {th['min_score']}\n"
        f"• Vol ≥ {th['min_vol_ratio']}×\n"
        f"• ADX ≥ {th['min_adx']}\n\n"
        f"<b>📊 تقرير 6%:</b> كل {RANGE_REPORT_INTERVAL_HOURS} ساعات\n\n"
        f"<b>الأوامر:</b>\n"
        f"/ranges /checkranges /refresh\n"
        f"/ref6 — تقرير 6% فوري\n"
        f"/cross /cross5 /cross15 /cross1h\n"
        f"/checkprice /status /strict /clearcache",
        parse_mode="HTML",
    )


async def _run_cross(update, tfs=None):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    tfs = tfs or TIMEFRAMES
    await update.message.reply_text(f"🔍 فحص: {', '.join(tfs)} ...")
    clear_caches()
    detectors = []
    if ENABLE_PRE_CROSS:  detectors.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: detectors.append(detect_live_crossover)
    if ENABLE_CONFIRMED:  detectors.append(detect_crossover)

    found = filtered = 0
    for sym in ALL_SYMBOLS:
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
                    await update.message.reply_text(build_signal_message(cross), parse_mode="HTML")
                    found += 1
                except Exception as e:
                    log.error(f"send {sym} {tf}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    msg = f"✅ {found} إشارة."
    if filtered: msg += f"\n🚫 {filtered} محجوبة."
    if found == 0 and filtered == 0:
        msg = f"⚪ لا إشارات على {', '.join(tfs)}"
    await update.message.reply_text(msg)


async def cmd_cross(u, c):   await _run_cross(u)
async def cmd_cross5(u, c):  await _run_cross(u, ["5m"])
async def cmd_cross15(u, c): await _run_cross(u, ["15m"])
async def cmd_cross1h(u, c): await _run_cross(u, ["1h"])


async def cmd_checkprice(update, context):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    await update.message.reply_text("🔍 فحص ...")
    found = 0
    for sym in ALL_SYMBOLS:
        a = await detect_sudden_change(sym)
        if a:
            try:
                await update.message.reply_text(build_price_alert_msg(a), parse_mode="HTML")
                found += 1
            except Exception as e:
                log.error(f"send {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if found == 0: await update.message.reply_text("⚪ لا تغيرات مفاجئة.")


async def cmd_ranges(update, context):
    await update.message.reply_text(build_ranges_report(), parse_mode="HTML")


async def cmd_refresh(update, context):
    global _MANUAL_RANGES, ALL_SYMBOLS
    _MANUAL_RANGES = _detect_manual_ranges()
    ALL_SYMBOLS = list(_MANUAL_RANGES.keys()) if _MANUAL_RANGES else ALL_SYMBOLS
    _last_range.clear()
    n = _load_manual_ranges()
    await update.message.reply_text(f"✅ تم تحديث {n} نطاق يدوي.")


async def cmd_ref6(update, context):
    """تقرير 6% فوري"""
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    await update.message.reply_text("🔍 حساب نطاقات 6% ...")
    results = await compute_6pct_ranges()
    if not results:
        await update.message.reply_text("⚪ لا بيانات."); return
    try:
        await update.message.reply_text(
            build_6pct_report(results, "📊 تقرير 6% (عند الطلب)"),
            parse_mode="HTML",
        )
    except Exception as e:
        log.error(f"ref6: {e}")


async def cmd_checkranges(update, context):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    await update.message.reply_text("🔍 فحص فوري ...")
    alerts = 0
    for symbol in list(_last_range.keys()):
        b = await check_range_breakout(symbol)
        if b:
            await update.message.reply_text(build_breakout_msg(b), parse_mode="HTML")
            _filter_stats["range_breakouts"] += 1; alerts += 1; continue
        a = await check_range_approach(symbol)
        if a:
            await update.message.reply_text(build_approach_msg(a), parse_mode="HTML")
            _filter_stats["range_approaches"] += 1; alerts += 1
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if alerts == 0:
        await update.message.reply_text("✅ كل الأسعار داخل النطاقات.")


async def cmd_status(update, context):
    total = sum(v for k, v in _filter_stats.items() if k.startswith("filtered_"))
    await update.message.reply_text(
        f"🤖 <b>الحالة</b>\n"
        f"🔌 {_exchange.primary_name}\n"
        f"🎯 STRICTNESS: <b>{STRICTNESS}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 إشارات: <b>{_filter_stats['sent']}</b>\n"
        f"🚨 خروج نطاق: <b>{_filter_stats['range_breakouts']}</b>\n"
        f"⚠️ اقتراب نطاق: <b>{_filter_stats['range_approaches']}</b>\n"
        f"📊 تقارير 6%: <b>{_filter_stats['range_6pct_reports']}</b>\n"
        f"🚫 محجوبة: {total}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💾 كاش: {len(_ohlcv_cache)}\n"
        f"📌 نطاقات مُراقَبة: {len(_last_range)}",
        parse_mode="HTML",
    )


async def cmd_strict(update, context):
    th = get_thresholds()
    await update.message.reply_text(
        f"🎯 <b>التشدد:</b> <b>{STRICTNESS.upper()}</b>\n"
        f"• Score ≥ {th['min_score']}\n"
        f"• Vol ≥ {th['min_vol_ratio']}×\n"
        f"• ADX ≥ {th['min_adx']}\n"
        f"• ATR ≤ {th['max_atr_pct']}%\n"
        f"• VWAP slope ≥ {th['vwap_slope_pct']}%",
        parse_mode="HTML",
    )


async def cmd_clearcache(update, context):
    clear_caches()
    _smart.reset_feedback()
    await update.message.reply_text("✅ تم تفريغ الكاش.")


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        log.warning(f"⚠️ network: {err}"); return
    log.error(f"❌ error: {err}", exc_info=err)


# ═══════════════════════════════════════════════════════════
# 20) Jobs
# ═══════════════════════════════════════════════════════════
async def crossover_job(context):
    if not _exchange.ok(): return
    detectors = []
    if ENABLE_PRE_CROSS:  detectors.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: detectors.append(detect_live_crossover)
    if ENABLE_CONFIRMED:  detectors.append(detect_crossover)

    total = 0
    for sym in ALL_SYMBOLS:
        for tf in TIMEFRAMES:
            for det in detectors:
                try:
                    cross = await det(sym, tf)
                    if not cross: continue
                    htf = await get_htf_trend(sym)
                    passed, _r, cat = await passes_filter(cross, htf)
                    if not passed:
                        key = f"filtered_{cat}"
                        if key in _filter_stats: _filter_stats[key] += 1
                        continue
                    if CHAT_ID:
                        try:
                            await context.bot.send_message(
                                chat_id=CHAT_ID, text=build_signal_message(cross),
                                parse_mode="HTML")
                            total += 1
                            _filter_stats["sent"] += 1
                            if cross["alert_type"] == "confirmed":
                                record_signal(sym, cross["direction"])
                        except Exception as e:
                            log.error(f"send {sym} {tf}: {e}")
                except Exception as e:
                    log.exception(f"job {sym} {tf}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if total: log.info(f"📤 {total} إشارة")


async def price_alert_job(context):
    if not _exchange.ok(): return
    total = 0
    for sym in ALL_SYMBOLS:
        try:
            a = await detect_sudden_change(sym)
            if a and CHAT_ID:
                try:
                    await context.bot.send_message(
                        chat_id=CHAT_ID, text=build_price_alert_msg(a), parse_mode="HTML")
                    total += 1
                except Exception as e:
                    log.error(f"send {sym}: {e}")
        except Exception as e:
            log.exception(f"price_alert {sym}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if total: log.info(f"📤 {total} تغير مفاجئ")


async def range_monitor_job(context):
    if not _exchange.ok() or not _last_range: return
    total = 0
    for symbol in list(_last_range.keys()):
        try:
            b = await check_range_breakout(symbol)
            if b and CHAT_ID:
                await context.bot.send_message(
                    chat_id=CHAT_ID, text=build_breakout_msg(b), parse_mode="HTML")
                _filter_stats["range_breakouts"] += 1
                total += 1
                log.info(f"🚨 Breakout: {symbol} {b['direction']}")
                continue
            a = await check_range_approach(symbol)
            if a and CHAT_ID:
                await context.bot.send_message(
                    chat_id=CHAT_ID, text=build_approach_msg(a), parse_mode="HTML")
                _filter_stats["range_approaches"] += 1
                total += 1
                log.info(f"⚠️ Approach: {symbol} {a['side']}")
        except Exception as e:
            log.exception(f"range_monitor {symbol}: {e}")
        await asyncio.sleep(SYMBOL_DELAY_MS / 1000)
    if total: log.info(f"📤 {total} إشعار نطاق")


async def six_pct_report_job(context):
    """يُرسل تقرير 6% المرجعي"""
    if not _exchange.ok() or not CHAT_ID: return
    results = await compute_6pct_ranges()
    if not results: return
    try:
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=build_6pct_report(results, "📊 نطاقات 6% المرجعية"),
            parse_mode="HTML",
        )
        log.info(f"📤 تقرير 6% ({len(results)} عملة)")
    except Exception as e:
        log.error(f"send 6pct: {e}")


# ═══════════════════════════════════════════════════════════
# 21) Health
# ═══════════════════════════════════════════════════════════
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Unified Smart Bot v5.1")
    def do_HEAD(self):
        self.send_response(200); self.end_headers()
    def log_message(self, *a): pass


def run_health():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()


# ═══════════════════════════════════════════════════════════
# 22) Main
# ═══════════════════════════════════════════════════════════
def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ TELEGRAM_REPORT_BOT_TOKEN و TELEGRAM_REPORT_CHAT_ID مطلوبان")
        return

    threading.Thread(target=run_health, daemon=True).start()

    n = _load_manual_ranges()
    print(f"📌 {n} نطاق يدوي")
    print(f"🔌 {_exchange.primary_name} | بدائل: {_exchange.chain_names[1:]}")
    print(f"📊 رموز: {ALL_SYMBOLS}")
    print(f"📊 تقرير 6%: كل {RANGE_REPORT_INTERVAL_HOURS} ساعات")

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(read_timeout=30.0, write_timeout=30.0,
                           connect_timeout=30.0, pool_timeout=30.0)

    app = (Application.builder().token(BOT_TOKEN)
           .request(request).get_updates_request(request).build())

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cross", cmd_cross))
    app.add_handler(CommandHandler("cross5", cmd_cross5))
    app.add_handler(CommandHandler("cross15", cmd_cross15))
    app.add_handler(CommandHandler("cross1h", cmd_cross1h))
    app.add_handler(CommandHandler("checkprice", cmd_checkprice))
    app.add_handler(CommandHandler("ranges", cmd_ranges))
    app.add_handler(CommandHandler("checkranges", cmd_checkranges))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("ref6", cmd_ref6))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("strict", cmd_strict))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    app.add_error_handler(error_handler)

    if app.job_queue:
        app.job_queue.run_repeating(crossover_job, interval=JOB_INTERVAL_MIN * 60,
                                     first=15, name="cross")
        app.job_queue.run_repeating(price_alert_job, interval=PRICE_ALERT_INTERVAL_MIN * 60,
                                     first=20, name="price")
        app.job_queue.run_repeating(range_monitor_job, interval=RANGE_MONITOR_INTERVAL_MIN * 60,
                                     first=30, name="range_monitor")

        # 🆕 تقرير 6% — عند التشغيل ثم كل 4 ساعات
        if SEND_STARTUP_REPORT:
            app.job_queue.run_once(six_pct_report_job, when=45, name="six_pct_startup")
            print(f"📊 تقرير 6% عند التشغيل: بعد 45 ثانية")

        app.job_queue.run_repeating(
            six_pct_report_job,
            interval=RANGE_REPORT_INTERVAL_HOURS * 3600,
            first=RANGE_REPORT_INTERVAL_HOURS * 3600,
            name="six_pct_periodic",
        )
        print(f"📊 تقرير 6% دوري: كل {RANGE_REPORT_INTERVAL_HOURS} ساعات")

    print("✅ جاهز — بدء polling")
    app.run_polling(allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True, bootstrap_retries=5)


if __name__ == "__main__":
    main()
