"""
Unified Smart Trading Bot v7.2 — Binance USDT-M Futures Auto-Trader
====================================================================
FIXED v7.2:
  ✅ Watchdog: accepts ANY STOP_MARKET as SL (fixed -4130 loop)
  ✅ _place_sl: ignores -4130 (SL already exists)
  ✅ _bump_fail: threshold 3 → 5 (less aggressive emergency close)
  ✅ /pause & /resume: rich feedback + status details
FIXED v7.1:
  ✅ fetch_open_orders per-symbol (avoid IP ban)
  ✅ IP ban detection + graceful handling
  ✅ Startup delay 30s + inter-symbol delay 1.5s
  ✅ Reconcile without fetch_open_orders(no symbol)
Strategy  : EMA cross + RSI div + BB squeeze + S/R breakout
Execution : Binance USDT-M Futures | 40x ISOLATED | $400 notional
Limits    : Max 5 positions | mandatory SL | Daily loss limit
"""
import os, sys, json, hmac, hashlib, asyncio, logging, threading, time as _time
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, time as dt_time
from zoneinfo import ZoneInfo
from collections import deque, defaultdict
from typing import Optional, Dict, Any, List

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
for _n in ("httpx", "telegram", "telegram.ext", "ccxt", "apscheduler", "websockets"):
    logging.getLogger(_n).setLevel(logging.WARNING)
log = logging.getLogger("bot")

import ccxt
import pandas as pd
import websockets
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
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

LEVERAGE                = int(os.getenv("LEVERAGE", "40"))
POSITION_NOTIONAL_USD   = float(os.getenv("POSITION_NOTIONAL_USD", "400"))
MAX_CONCURRENT_POSITIONS= int(os.getenv("MAX_CONCURRENT_POSITIONS", "5"))
MARGIN_MODE             = os.getenv("MARGIN_MODE", "isolated")
MAX_SL_PCT              = float(os.getenv("MAX_SL_PCT", "1.0"))
TRADE_ON_CONFIRMED_ONLY = os.getenv("TRADE_ON_CONFIRMED_ONLY", "true").lower() == "true"
ENABLE_LONG             = os.getenv("ENABLE_LONG", "true").lower() == "true"
ENABLE_SHORT            = os.getenv("ENABLE_SHORT", "true").lower() == "true"

MAX_DAILY_LOSS_PCT       = float(os.getenv("MAX_DAILY_LOSS_PCT", "5.0"))
MAX_CONSECUTIVE_LOSSES   = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
TP_SPLITS                = [0.50, 0.30, 0.20]
TRAIL_AFTER_TP2          = True
TRAIL_CALLBACK_RATE      = float(os.getenv("TRAIL_CALLBACK_RATE", "0.4"))
BREAKEVEN_AFTER_TP1      = True

TIMEFRAMES = ["5m", "15m", "1h"]
EMA_FAST, EMA_SLOW = 7, 25
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
HTF_ENABLED = True
HTF_TIMEFRAME = "1h"
HTF_BLOCK_OPPOSITE = True
VWAP_SLOPE_LOOKBACK = 5
STRICTNESS = "auto"
STRICTNESS_OVERRIDE: dict = {}

MIN_EMA_GAP_PCT = {"5m": 0.02, "15m": 0.015, "1h": 0.01}
MIN_ATR_PCT     = {"5m": 0.05, "15m": 0.10, "1h": 0.20}
MIN_ADX_FOR_BB       = 8.0
SYMBOL_COOLDOWN_SEC  = 180
STRICTNESS_MAX_FINAL = 80
ABS_MIN_SCORE        = 52
BB_MIN_VOL           = 1.0
BB_MIN_ADX_IN_RANGE  = 20
BB_CONFIRM_ENABLED   = True
MIN_SL_PCT = {"5m": 0.35, "15m": 0.50, "1h": 0.80}
SIGNAL_TRACK_MAX_MIN = 90
LAST_SIGNALS_MAX = 20

# Rate limit safety
STARTUP_DELAY_SEC       = int(os.getenv("STARTUP_DELAY_SEC", "30"))
INTER_SYMBOL_DELAY_SEC  = float(os.getenv("INTER_SYMBOL_DELAY_SEC", "1.5"))
WATCHDOG_INTERVAL_SEC   = int(os.getenv("WATCHDOG_INTERVAL_SEC", "180"))

SYRIA_TZ = ZoneInfo("Asia/Damascus")

# ═══════════════════════════════════════════════════════════
# 2) الرموز
# ═══════════════════════════════════════════════════════════
def _norm_sym(name: str) -> str:
    name = name.strip().upper()
    if "/" in name and ":" in name: return name
    if "/" in name: return f"{name}:USDT"
    if name.endswith("USDT"): return f"{name[:-4]}/USDT:USDT"
    return f"{name}/USDT:USDT"

_syms_env = os.getenv("TRADING_SYMBOLS", "").strip()
if _syms_env:
    ALL_SYMBOLS = [_norm_sym(s) for s in _syms_env.split(",") if s.strip()]
else:
    ALL_SYMBOLS = ["XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]

log.info(f"📊 الرموز: {ALL_SYMBOLS}")

# ═══════════════════════════════════════════════════════════
# 3) أدوات
# ═══════════════════════════════════════════════════════════
def short(symbol): return symbol.split("/")[0].split(":")[0].upper()

def syria_now_str():
    now = datetime.now(SYRIA_TZ)
    days_ar = {"Monday":"الاثنين","Tuesday":"الثلاثاء","Wednesday":"الأربعاء",
               "Thursday":"الخميس","Friday":"الجمعة","Saturday":"السبت","Sunday":"الأحد"}
    return f"{days_ar.get(now.strftime('%A'), now.strftime('%A'))} {now.strftime('%Y-%m-%d')} — {now.strftime('%H:%M:%S')}"

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
    if "/" not in name: return f"{name}/USDT:USDT"
    return None

# ═══════════════════════════════════════════════════════════
# 4) المؤشرات
# ═══════════════════════════════════════════════════════════
def calc_ema(s, p): return s.ewm(span=p, adjust=False).mean()

def calc_rsi(s, p=14):
    d = s.diff(); g = d.clip(lower=0); l = -d.clip(upper=0)
    ag = g.ewm(alpha=1/p, adjust=False).mean(); al = l.ewm(alpha=1/p, adjust=False).mean()
    rs = ag / al.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))

def calc_atr(df, p=14):
    tr = pd.concat([df["h"]-df["l"], (df["h"]-df["c"].shift()).abs(),
                    (df["l"]-df["c"].shift()).abs()], axis=1).max(axis=1)
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
    ef = s.ewm(span=f, adjust=False).mean(); es = s.ewm(span=sl, adjust=False).mean()
    dif = ef - es; dea = dif.ewm(span=sig, adjust=False).mean()
    return dif, dea, dif - dea

def calc_vwap_session(df, session="day"):
    if df.empty: return df["c"]
    try:
        ts = pd.to_datetime(df["ts"], unit="ms", utc=True)
        skey = ts.dt.floor("D") if session == "day" else ts.dt.floor("H")
        tp = (df["h"] + df["l"] + df["c"]) / 3
        pv = tp * df["v"]
        return pv.groupby(skey).cumsum() / df["v"].groupby(skey).cumsum().replace(0, 1e-9)
    except Exception:
        tp = (df["h"] + df["l"] + df["c"]) / 3
        return (tp * df["v"]).cumsum() / df["v"].cumsum()

def calc_bollinger(df, period=20, std_mult=2):
    close = df["c"]; ma = close.rolling(period).mean(); sd = close.rolling(period).std()
    upper = ma + std_mult * sd; lower = ma - std_mult * sd
    width = (upper - lower) / ma * 100
    return {"ma": ma, "upper": upper, "lower": lower, "width": width}

def compute_levels(df, direction, curr_idx=-2, tf="15m"):
    try:
        price = float(df["c"].iloc[curr_idx])
        atr = float(calc_atr(df).iloc[curr_idx])
        ema_slow = float(calc_ema(df["c"], EMA_SLOW).iloc[curr_idx])
        min_sl_pct = MIN_SL_PCT.get(tf, 0.50)
        lookback = min(20, len(df) + curr_idx - 1)
        if lookback < 2: return None
        recent_high = float(df["h"].iloc[curr_idx-lookback:curr_idx].max())
        recent_low  = float(df["l"].iloc[curr_idx-lookback:curr_idx].min())
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
        if risk_pct < min_sl_pct * 0.9: return None
        if risk_pct > MAX_SL_PCT:
            log.info(f"❌ SL {risk_pct:.2f}% > {MAX_SL_PCT}% ({tf})")
            return None
        return {"entry": price, "stop_loss": stop_loss, "sl_pct": round(risk_pct, 2),
                "tp1": tp1, "tp2": tp2, "tp3": tp3, "atr": atr}
    except Exception as e:
        log.warning(f"compute_levels: {e}")
        return None

# ═══════════════════════════════════════════════════════════
# 5) SmartStrictness
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
    "5m":  {"score_mult": 1.00, "vol_mult": 1.00, "atr_max": 0.70},
    "15m": {"score_mult": 1.00, "vol_mult": 1.00, "atr_max": 0.90},
    "1h":  {"score_mult": 0.85, "vol_mult": 0.75, "atr_max": 1.80},
}
SESSIONS = [
    (0, 6,  "🌏 آسيا",       +8),
    (6, 10, "🌅 آسيا/أوروبا",+2),
    (10,13, "🇪🇺 أوروبا",     0),
    (13,16, "🔥 ذروة",       -5),
    (16,21, "🇺🇸 أمريكا",     0),
    (21,24, "🌙 ليلي",       +5),
]

class SmartStrictness:
    def __init__(self):
        self.history = deque(maxlen=200); self.results = deque(maxlen=200)
        self.source_stats = defaultdict(lambda: {"wins":0,"losses":0,"expired":0,"sent":0,"filtered":0})
        self._lock = threading.Lock()
        self._cached_profile = None; self._cached_ts = 0.0
        self._cached_atr = None; self._cached_adx = None; self._cache_ttl = 60.0

    def record(self, passed, score, source="unknown"):
        with self._lock:
            self.history.append((passed, score, _time.time(), source))
            self.source_stats[source]["sent" if passed else "filtered"] += 1

    def record_result(self, result, direction, source="unknown"):
        win = result.startswith("win"); expired = result == "expired"
        with self._lock:
            self.results.append({"win":win,"result":result,"direction":direction,
                                 "ts":_time.time(),"source":source,"expired":expired})
            if win: self.source_stats[source]["wins"] += 1
            elif expired: self.source_stats[source]["expired"] += 1
            else: self.source_stats[source]["losses"] += 1

    def _feedback(self):
        with self._lock:
            results = list(self.results)[-50:]; recent = list(self.history)[-50:]
        non_exp = [r for r in results if not r.get("expired")]
        if len(non_exp) >= 15:
            wins = sum(1 for r in non_exp if r["win"]); ratio = wins/len(non_exp); n = len(non_exp)
        else:
            n = len(recent)
            if n < 20: return 0, None, 0
            ratio = sum(1 for p,_,_,_ in recent if p)/n
        if ratio > 0.65:   d = +10
        elif ratio > 0.50: d = +5
        elif ratio < 0.25: d = -18
        elif ratio < 0.40: d = -8
        else:              d = 0
        return d, ratio, n

    def compute(self, atr_pct=None, adx=None):
        now = _time.time()
        if (self._cached_profile and (now-self._cached_ts)<self._cache_ttl
            and self._cached_atr is not None
            and abs(self._cached_atr-(atr_pct or 0))<0.05
            and abs(self._cached_adx-(adx or 0))<2.0):
            return self._cached_profile
        if atr_pct is None or atr_pct <= 0: atr_pct = 0.5
        if atr_pct < 0.15: base = 50
        elif atr_pct < 0.30: base = 55
        elif atr_pct < 0.60: base = 62
        elif atr_pct < 1.00: base = 70
        else: base = 78
        adx_delta = 0
        if adx is not None:
            if adx > 35: adx_delta = -3
            elif adx > 25: adx_delta = -1
            elif adx < 18: adx_delta = +5
            elif adx < 22: adx_delta = +2
        fb_delta, _, _ = self._feedback()
        final = max(40, min(STRICTNESS_MAX_FINAL, base + adx_delta + fb_delta))
        prof = {"min_score": final,
                "min_vol_ratio": round(max(0.7,(final-30)/30),2),
                "min_adx": round(max(12, final*0.28),1),
                "max_atr_pct": round(max(0.35, 1.2-final/100),2),
                "vwap_slope_pct": round(0.02+final/1000,3),
                "block_against_htf": final >= 55,
                "score_15m_override": min(80, final+10),
                "_debug": {"base":base,"adx_delta":adx_delta,"fb_delta":fb_delta,"atr":atr_pct,"adx":adx}}
        self._cached_profile = prof; self._cached_ts = now
        self._cached_atr = atr_pct; self._cached_adx = adx
        return prof

    def stats(self):
        with self._lock:
            n = len(self.results)
            if n == 0: return {"n":0,"wr":0,"wins":0,"losses":0,"expired":0}
            wins = sum(1 for r in self.results if r["win"])
            exp = sum(1 for r in self.results if r.get("expired"))
            losses = n - wins - exp
        dec = wins + losses
        wr = round(wins/dec*100,1) if dec>0 else 0
        return {"n":n,"wr":wr,"wins":wins,"losses":losses,"expired":exp}

    def source_report(self):
        with self._lock: return dict(self.source_stats)

    def reset_feedback(self):
        with self._lock:
            self.history.clear(); self.results.clear(); self.source_stats.clear()
        self._cached_profile = None; self._cached_ts = 0.0

_smart = SmartStrictness()

def get_session_info():
    h = datetime.now(timezone.utc).hour
    for start,end,name,delta in SESSIONS:
        if start <= h < end: return delta, name
    return 0, "❓"

def get_thresholds(atr_pct=None, adx=None, tf="15m"):
    if STRICTNESS == "auto":
        prof = dict(_smart.compute(atr_pct=atr_pct, adx=adx))
    else:
        prof = dict(STATIC_PROFILES.get(STRICTNESS, STATIC_PROFILES["balanced"]))
        prof["_debug"] = {"mode":STRICTNESS}
    tf_p = TF_PROFILES.get(tf, TF_PROFILES["15m"])
    prof["min_score"] *= tf_p["score_mult"]
    prof["min_vol_ratio"] *= tf_p["vol_mult"]
    prof["max_atr_pct"] = tf_p["atr_max"]
    prof["_tf"] = tf
    if SESSION_ADJUST_ENABLED:
        sd, sn = get_session_info()
        prof["min_score"] += sd; prof["_session"] = sn; prof["_session_delta"] = sd
    prof.update(STRICTNESS_OVERRIDE)
    return prof

# ═══════════════════════════════════════════════════════════
# 6) BinanceExecutor — FIXED v7.2
# ═══════════════════════════════════════════════════════════
class BinanceExecutor:
    def __init__(self):
        opts = {
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "timeout": 20000,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
                "warnOnFetchOpenOrdersWithoutSymbol": False,
                "recvWindow": 10000,
            },
        }
        self.ex = ccxt.binanceusdm(opts)
        self.ex.load_markets()
        log.info(f"✅ Binance USDM متصل | {len(self.ex.markets)} سوق")

    async def _call(self, method_name, *args, **kwargs):
        """REST with ban detection + adaptive backoff"""
        delays = [1, 2, 4, 8, 15, 30]
        last_err = None
        for i, d in enumerate(delays):
            try:
                m = getattr(self.ex, method_name)
                return await asyncio.to_thread(m, *args, **kwargs)
            except ccxt.RateLimitExceeded as e:
                last_err = e
                wait = d + random.uniform(0.5, 2.0)
                log.warning(f"⚠️ RateLimit {method_name}, wait {wait:.1f}s")
                await asyncio.sleep(wait)
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
                last_err = e
                err_str = str(e)
                if "418" in err_str or "-1003" in err_str or "banned" in err_str.lower():
                    log.error(f"🚫 IP BANNED → {method_name}")
                    log.error(f"   الانتظار 5 دقائق قبل المحاولة التالية")
                    await asyncio.sleep(300)
                    continue
                wait = d + random.uniform(0.5, 2.0)
                log.warning(f"⚠️ Network {method_name}, wait {wait:.1f}s")
                await asyncio.sleep(wait)
            except ccxt.ExchangeError as e:
                log.error(f"❌ Exchange {method_name}: {e}")
                raise
            except Exception as e:
                log.exception(f"❌ {method_name}: {e}")
                raise
        raise ccxt.NetworkError(f"{method_name} failed: {last_err}")

    async def fetch_ohlcv(self, symbol, tf, limit=200):
        return await self._call("fetch_ohlcv", symbol, tf, None, limit)

    async def fetch_balance(self):
        return await self._call("fetch_balance", {"type":"future"})

    async def fetch_positions(self, symbols=None):
        return await self._call("fetch_positions", symbols)

    async def fetch_open_orders(self, symbol=None):
        return await self._call("fetch_open_orders", symbol)

    async def set_leverage(self, symbol, lev):
        try:
            return await self._call("set_leverage", lev, symbol)
        except ccxt.ExchangeError as e:
            if "No need to change" in str(e): return None
            raise

    async def set_margin_mode(self, symbol, mode="isolated"):
        try:
            return await self._call("set_margin_mode", mode, symbol)
        except ccxt.ExchangeError as e:
            if "No need to change" in str(e): return None
            raise

    async def create_order(self, symbol, order_type, side, amount, price=None, params=None):
        return await self._call("create_order", symbol, order_type, side, amount, price, params or {})

    async def cancel_order(self, order_id, symbol):
        return await self._call("cancel_order", order_id, symbol)

    async def cancel_all_orders(self, symbol):
        try:
            return await self._call("cancel_all_orders", symbol)
        except Exception as e:
            log.warning(f"cancel_all {symbol}: {e}")

    async def fetch_order(self, order_id, symbol):
        return await self._call("fetch_order", order_id, symbol)

    def amount_to_precision(self, symbol, amount):
        return self.ex.amount_to_precision(symbol, amount)

    def price_to_precision(self, symbol, price):
        return self.ex.price_to_precision(symbol, price)

    def market(self, symbol): return self.ex.market(symbol)

    async def create_listen_key(self):
        return await self._call("fapiPrivatePostListenKey")

    async def keepalive_listen_key(self):
        return await self._call("fapiPrivatePutListenKey")

_executor: Optional[BinanceExecutor] = None
_ohlcv_rest_cache: Dict[tuple, dict] = {}

async def fetch_ohlcv_cached(symbol, tf, limit=200):
    key = (symbol, tf, limit); now = _time.time()
    c = _ohlcv_rest_cache.get(key)
    if c and (now - c["ts"]) < 90: return c["data"]
    if _executor is None: return None
    try:
        data = await _executor.fetch_ohlcv(symbol, tf, limit)
    except Exception as e:
        log.warning(f"ohlcv {symbol} {tf}: {e}")
        return c["data"] if c else None
    if data:
        _ohlcv_rest_cache[key] = {"data": data, "ts": now}
    return data

# ═══════════════════════════════════════════════════════════
# 7) WebSocketManager
# ═══════════════════════════════════════════════════════════
class WebSocketManager:
    FAPI_WS_BASE = "wss://fstream.binance.com"

    def __init__(self, executor: BinanceExecutor, positions_mgr):
        self.executor = executor
        self.positions_mgr = positions_mgr
        self._running = False
        self._kline_ws = None
        self._user_ws = None
        self._listen_key = None
        self._tasks: List[asyncio.Task] = []
        self._binance_sym_cache = {}

    def _to_ws_sym(self, ccxt_sym: str) -> str:
        base = ccxt_sym.split("/")[0].lower()
        quote = ccxt_sym.split("/")[1].split(":")[0].lower()
        return f"{base}{quote}"

    def _to_ccxt_sym(self, ws_sym: str) -> str:
        ws_sym = ws_sym.upper()
        if ws_sym in self._binance_sym_cache:
            return self._binance_sym_cache[ws_sym]
        try:
            for m in self.executor.ex.markets.values():
                if m.get("id") == ws_sym and m.get("swap"):
                    self._binance_sym_cache[ws_sym] = m["symbol"]
                    return m["symbol"]
        except Exception:
            pass
        return f"{ws_sym[:-4]}/USDT:USDT"

    def _build_streams(self) -> str:
        streams = []
        for s in ALL_SYMBOLS:
            ws_s = self._to_ws_sym(s)
            for tf in TIMEFRAMES:
                streams.append(f"{ws_s}@kline_{tf}")
        return "/".join(streams)

    async def start(self):
        self._running = True
        self._tasks.append(asyncio.create_task(self._kline_loop()))
        self._tasks.append(asyncio.create_task(self._user_loop()))
        log.info(f"🔌 WebSocket بدأ | klines: {len(ALL_SYMBOLS)}×{len(TIMEFRAMES)}")

    async def stop(self):
        self._running = False
        for t in self._tasks: t.cancel()
        for ws in (self._kline_ws, self._user_ws):
            if ws:
                try: await ws.close()
                except: pass

    async def _kline_loop(self):
        backoff = 5
        while self._running:
            try:
                url = f"{self.FAPI_WS_BASE}/stream?streams={self._build_streams()}"
                async with websockets.connect(url, ping_interval=180, ping_timeout=60,
                                              close_timeout=10, max_size=2**22) as ws:
                    self._kline_ws = ws
                    log.info("🟢 Kline WS متصل")
                    backoff = 5
                    async for raw in ws:
                        if not self._running: break
                        try:
                            self._handle_kline(json.loads(raw))
                        except Exception:
                            pass
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning(f"⚠️ Kline WS: {type(e).__name__} → retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(60, backoff * 2)

    def _handle_kline(self, msg):
        data = msg.get("data") or msg
        k = data.get("k")
        if not k or not k.get("x"): return
        ws_sym = k.get("s"); tf = k.get("i")
        ccxt_sym = self._to_ccxt_sym(ws_sym)
        candle = [int(k["t"]), float(k["o"]), float(k["h"]),
                  float(k["l"]), float(k["c"]), float(k["v"])]
        key = (ccxt_sym, tf, 200)
        entry = _ohlcv_rest_cache.get(key)
        if not entry:
            _ohlcv_rest_cache[key] = {"data": [candle], "ts": _time.time()}
            return
        lst = entry["data"]
        if lst and lst[-1][0] == candle[0]:
            lst[-1] = candle
        else:
            lst.append(candle)
            if len(lst) > 300: lst[:] = lst[-200:]
        entry["ts"] = _time.time()

    async def _user_loop(self):
        backoff = 5
        while self._running:
            try:
                if not self._listen_key:
                    try:
                        resp = await self.executor.create_listen_key()
                        self._listen_key = resp.get("listenKey") or resp.get("listen_key")
                    except Exception as e:
                        log.warning(f"⚠️ listenKey فشل: {e} → retry in {backoff}s")
                        await asyncio.sleep(backoff)
                        backoff = min(120, backoff * 2)
                        continue
                    if not self._listen_key:
                        await asyncio.sleep(backoff); continue
                    log.info(f"🔑 listenKey جديد")
                    backoff = 5
                url = f"{self.FAPI_WS_BASE}/ws/{self._listen_key}"
                async with websockets.connect(url, ping_interval=180, ping_timeout=60,
                                              close_timeout=10) as ws:
                    self._user_ws = ws
                    log.info("🟢 User WS متصل")
                    ka_task = asyncio.create_task(self._listen_key_keepalive())
                    try:
                        async for raw in ws:
                            if not self._running: break
                            try:
                                await self._handle_user_event(json.loads(raw))
                            except Exception:
                                pass
                    finally:
                        ka_task.cancel()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning(f"⚠️ User WS: {type(e).__name__} → retry in {backoff}s")
                self._listen_key = None
                await asyncio.sleep(backoff)
                backoff = min(120, backoff * 2)

    async def _listen_key_keepalive(self):
        try:
            while self._running:
                await asyncio.sleep(25 * 60)
                try:
                    await self.executor.keepalive_listen_key()
                except Exception as e:
                    log.warning(f"⚠️ keepalive: {e}")
                    self._listen_key = None
                    return
        except asyncio.CancelledError:
            return

    async def _handle_user_event(self, ev):
        etype = ev.get("e")
        if etype == "ORDER_TRADE_UPDATE":
            await self.positions_mgr.on_order_update(ev.get("o", {}))
        elif etype == "MARGIN_CALL":
            await self.positions_mgr.on_margin_call(ev)
        elif etype == "listenKeyExpired":
            log.warning("⚠️ listenKey expired")
            self._listen_key = None

# ═══════════════════════════════════════════════════════════
# 8) PositionManager — FIXED v7.2
# ═══════════════════════════════════════════════════════════
class PositionManager:
    def __init__(self, executor: BinanceExecutor, bot_getter):
        self.executor = executor
        self.bot_getter = bot_getter
        self.positions: Dict[str, dict] = {}
        self.daily_pnl_usd = 0.0
        self.daily_reset_ts = _time.time()
        self.consecutive_losses = 0
        self.paused = False
        self.ws_ref = None
        self._lock = asyncio.Lock()
        self._watchdog_fails: Dict[str, int] = {}

    async def can_open_new(self) -> tuple[bool, str]:
        if self.paused: return False, "البوت موقوف"
        async with self._lock:
            if len(self.positions) >= MAX_CONCURRENT_POSITIONS:
                return False, f"وصلت للحد ({MAX_CONCURRENT_POSITIONS})"
        if _time.time() - self.daily_reset_ts > 86400:
            self.daily_reset_ts = _time.time()
            self.daily_pnl_usd = 0.0
            self.consecutive_losses = 0
        if self.daily_pnl_usd < 0:
            bal = await self._get_balance_usd()
            if bal > 0 and abs(self.daily_pnl_usd) / bal * 100 >= MAX_DAILY_LOSS_PCT:
                return False, f"حد الخسارة اليومي"
        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            self.paused = True
            return False, f"{MAX_CONSECUTIVE_LOSSES} خسائر متتالية → إيقاف"
        return True, ""

    async def open_position(self, signal: dict) -> Optional[dict]:
        sym = signal["symbol"]
        async with self._lock:
            if sym in self.positions: return None
        direction = signal["direction"]
        side = "buy" if direction == "bullish" else "sell"
        if direction == "bullish" and not ENABLE_LONG: return None
        if direction == "bearish" and not ENABLE_SHORT: return None

        lv = signal.get("levels")
        if not lv: return None
        entry_ref = lv["entry"]; sl = lv["stop_loss"]
        tp1, tp2, tp3 = lv["tp1"], lv["tp2"], lv["tp3"]
        sl_pct = abs(entry_ref - sl) / entry_ref * 100
        if sl_pct > MAX_SL_PCT:
            log.info(f"❌ {short(sym)} SL {sl_pct:.2f}% > {MAX_SL_PCT}%")
            return None

        try:
            m = self.executor.market(sym)
            qty_raw = POSITION_NOTIONAL_USD / entry_ref
            qty_str = self.executor.amount_to_precision(sym, qty_raw)
            qty = float(qty_str)
            min_amt = m["limits"]["amount"]["min"] or 0
            if qty < min_amt:
                log.warning(f"❌ {short(sym)} qty {qty} < min {min_amt}"); return None
            min_cost = m["limits"]["cost"]["min"] or 5
            if qty * entry_ref < min_cost:
                log.warning(f"❌ {short(sym)} notional < min {min_cost}"); return None
        except Exception as e:
            log.error(f"qty calc {sym}: {e}"); return None

        try: await self.executor.set_margin_mode(sym, MARGIN_MODE)
        except Exception as e: log.warning(f"margin mode {sym}: {e}")
        await asyncio.sleep(0.3)
        try: await self.executor.set_leverage(sym, LEVERAGE)
        except Exception as e: log.warning(f"leverage {sym}: {e}")
        await asyncio.sleep(0.3)

        cid_base = f"v7{int(_time.time())}{sym.split('/')[0][:6]}"[:32]

        try:
            order = await self.executor.create_order(
                sym, "market", side, qty, None,
                {"newClientOrderId": f"{cid_base}_en"[:36]})
        except Exception as e:
            log.error(f"❌ entry order {sym}: {e}"); return None

        filled_qty = 0.0; avg_price = 0.0
        for _ in range(8):
            try:
                o = await self.executor.fetch_order(order["id"], sym)
                filled_qty = float(o.get("filled") or 0)
                avg_price = float(o.get("average") or 0)
                if o.get("status") == "closed" and filled_qty > 0: break
            except Exception: pass
            await asyncio.sleep(0.6)

        if filled_qty <= 0:
            log.error(f"❌ {sym} لم تُملأ")
            try: await self.executor.cancel_order(order["id"], sym)
            except: pass
            return None

        entry_price = avg_price or entry_ref
        if direction == "bullish":
            risk = entry_price - sl
            tp1 = entry_price + risk; tp2 = entry_price + risk*2; tp3 = entry_price + risk*3
        else:
            risk = sl - entry_price
            tp1 = entry_price - risk; tp2 = entry_price - risk*2; tp3 = entry_price - risk*3

        pos = {
            "symbol": sym, "side": side, "direction": direction,
            "entry": entry_price, "qty": filled_qty, "qty_initial": filled_qty,
            "leverage": LEVERAGE, "notional": entry_price * filled_qty,
            "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "sl_pct": sl_pct, "score": signal.get("final_score", 0),
            "source": signal.get("source", "ema"), "tf": signal.get("timeframe", "?"),
            "signal_id": cid_base, "opened_at": _time.time(),
            "sl_order_id": None, "tp1_order_id": None,
            "tp2_order_id": None, "tp3_order_id": None,
            "tp1_filled": False, "tp2_filled": False, "tp3_filled": False,
            "trailing_active": False, "breakeven_moved": False,
            "sl_last_verified": 0.0,
        }
        async with self._lock:
            self.positions[sym] = pos

        await asyncio.sleep(0.5)

        sl_ok = await self._place_sl(sym, pos, sl, tag="sl")
        if not sl_ok:
            log.error(f"❌ SL فشل لـ {sym} → إغلاق فوري")
            await self._emergency_close(sym)
            return None

        await asyncio.sleep(0.4)
        await self._place_tp(sym, pos, tp1, TP_SPLITS[0], "tp1")
        await asyncio.sleep(0.4)
        await self._place_tp(sym, pos, tp2, TP_SPLITS[1], "tp2")
        await asyncio.sleep(0.4)
        await self._place_tp(sym, pos, tp3, TP_SPLITS[2], "tp3")

        pos["sl_last_verified"] = _time.time()
        log.info(f"✅ فتح {short(sym)} {direction} @ {entry_price} | SL={sl} | qty={filled_qty}")
        return pos

    async def _place_sl(self, symbol, pos, price, tag="sl") -> bool:
        """✅ v7.2: يتجاهل -4130 (SL موجود مسبقاً)"""
        sl_side = "sell" if pos["direction"] == "bullish" else "buy"
        try:
            o = await self.executor.create_order(
                symbol, "STOP_MARKET", sl_side, None, None,
                {"stopPrice": self.executor.price_to_precision(symbol, price),
                 "closePosition": True, "workingType": "MARK_PRICE",
                 "priceProtect": True,
                 "newClientOrderId": f"{pos['signal_id']}_{tag}"[:36]})
            pos["sl_order_id"] = o.get("id")
            pos["sl_last_verified"] = _time.time()
            return True
        except Exception as e:
            err_str = str(e)
            # ✅ v7.2: تجاهل إذا SL موجود مسبقاً (خطأ -4130)
            if "-4130" in err_str or "closePosition in the direction is existing" in err_str:
                log.warning(f"⚠️ SL موجود مسبقاً لـ {symbol} — تخطي")
                pos["sl_last_verified"] = _time.time()
                return True
            log.error(f"SL {symbol}: {e}")
            return False

    async def _place_tp(self, symbol, pos, price, pct, tag):
        q = float(self.executor.amount_to_precision(symbol, pos["qty_initial"] * pct))
        if q <= 0: return None
        sl_side = "sell" if pos["direction"] == "bullish" else "buy"
        try:
            o = await self.executor.create_order(
                symbol, "TAKE_PROFIT_MARKET", sl_side, q, None,
                {"stopPrice": self.executor.price_to_precision(symbol, price),
                 "reduceOnly": True, "workingType": "MARK_PRICE",
                 "newClientOrderId": f"{pos['signal_id']}_{tag}"[:36]})
            pos[f"{tag}_order_id"] = o.get("id")
            return o.get("id")
        except Exception as e:
            log.warning(f"TP {tag} {symbol}: {e}")
            return None

    async def _emergency_close(self, symbol):
        async with self._lock:
            pos = self.positions.get(symbol)
        if not pos: return
        try:
            side = "sell" if pos["direction"] == "bullish" else "buy"
            await self.executor.create_order(symbol, "market", side, pos["qty"], None, {"reduceOnly": True})
            log.warning(f"🆘 إغلاق طارئ: {short(symbol)}")
        except Exception as e:
            log.error(f"emergency_close {symbol}: {e}")
        async with self._lock:
            self.positions.pop(symbol, None)

    async def close_position(self, symbol, reason="manual"):
        async with self._lock:
            pos = self.positions.get(symbol)
        if not pos: return False
        try: await self.executor.cancel_all_orders(symbol)
        except: pass
        try:
            side = "sell" if pos["direction"] == "bullish" else "buy"
            await self.executor.create_order(symbol, "market", side, pos["qty"], None, {"reduceOnly": True})
            log.info(f"🔒 إغلاق {short(symbol)} ({reason})")
        except Exception as e:
            log.error(f"close {symbol}: {e}"); return False
        async with self._lock:
            self.positions.pop(symbol, None)
        return True

    async def close_all(self, reason="closeall"):
        for s in list(self.positions.keys()):
            await self.close_position(s, reason)
            await asyncio.sleep(0.5)

    async def on_order_update(self, o: dict):
        ws_sym = o.get("s")
        if not ws_sym: return
        try:
            ccxt_sym = self.ws_ref._to_ccxt_sym(ws_sym) if self.ws_ref else f"{ws_sym[:-4]}/USDT:USDT"
        except Exception:
            ccxt_sym = f"{ws_sym[:-4]}/USDT:USDT"
        async with self._lock:
            pos = self.positions.get(ccxt_sym)
        if not pos: return
        status = o.get("X"); cid = o.get("c") or ""
        if status != "FILLED": return
        if "_tp1" in cid and not pos["tp1_filled"]:
            pos["tp1_filled"] = True
            await self._after_tp1(ccxt_sym, pos)
        elif "_tp2" in cid and not pos["tp2_filled"]:
            pos["tp2_filled"] = True
            await self._after_tp2(ccxt_sym, pos)
        elif "_tp3" in cid and not pos["tp3_filled"]:
            pos["tp3_filled"] = True
            await self._after_close(ccxt_sym, pos, "win_tp3")
        elif "_sl" in cid:
            await self._after_close(ccxt_sym, pos, "loss_sl")

    async def _after_tp1(self, symbol, pos):
        if not BREAKEVEN_AFTER_TP1: return
        await asyncio.sleep(1)
        try: await self.executor.cancel_all_orders(symbol)
        except: pass
        await asyncio.sleep(0.5)
        ok = await self._place_sl(symbol, pos, pos["entry"], tag="sl_be")
        if ok:
            pos["breakeven_moved"] = True; pos["sl"] = pos["entry"]
        else:
            log.error(f"❌ breakeven فشل {symbol} → إغلاق طارئ")
            await self._emergency_close(symbol); return
        await asyncio.sleep(0.4)
        await self._place_tp(symbol, pos, pos["tp2"], TP_SPLITS[1], "tp2_r")
        await asyncio.sleep(0.4)
        await self._place_tp(symbol, pos, pos["tp3"], TP_SPLITS[2], "tp3_r")
        await self._notify(f"🎯 <b>TP1</b> {short(symbol)} | SL → Breakeven")

    async def _after_tp2(self, symbol, pos):
        if not TRAIL_AFTER_TP2: return
        await asyncio.sleep(1)
        try: await self.executor.cancel_all_orders(symbol)
        except: pass
        await asyncio.sleep(0.5)
        sl_side = "sell" if pos["direction"] == "bullish" else "buy"
        try:
            t = await self.executor.create_order(
                symbol, "TRAILING_STOP_MARKET", sl_side, None, None,
                {"callbackRate": TRAIL_CALLBACK_RATE, "reduceOnly": True,
                 "workingType": "MARK_PRICE",
                 "newClientOrderId": f"{pos['signal_id']}_trail"[:36]})
            pos["sl_order_id"] = t.get("id"); pos["trailing_active"] = True
            pos["sl_last_verified"] = _time.time()
        except Exception as e:
            log.error(f"trailing {symbol}: {e}")
            await self._place_sl(symbol, pos, pos["tp1"], tag="sl_tp1")
        await asyncio.sleep(0.4)
        await self._place_tp(symbol, pos, pos["tp3"], TP_SPLITS[2], "tp3_r")
        await self._notify(f"✅ <b>TP2</b> {short(symbol)} | Trailing {TRAIL_CALLBACK_RATE}%")

    async def _after_close(self, symbol, pos, result):
        async with self._lock:
            self.positions.pop(symbol, None)
        _smart.record_result(result, pos["direction"], pos.get("source", "unknown"))
        await self._notify(
            f"{'✅' if result.startswith('win') else '❌'} "
            f"<b>{result.upper()}</b> {short(symbol)}")

    async def on_margin_call(self, ev):
        await self._notify("🚨 <b>MARGIN CALL</b> — راجع حسابك فوراً!")

    async def watchdog_check(self):
        if not self.positions: return
        for sym, pos in list(self.positions.items()):
            try:
                await self._watchdog_one(sym, pos)
            except Exception as e:
                log.exception(f"watchdog {sym}: {e}")
                self._bump_fail(sym)
            await asyncio.sleep(1.0)

    async def _watchdog_one(self, symbol, pos):
        # 1) هل الصفقة ما زالت موجودة؟
        try:
            bpos_list = await self.executor.fetch_positions([symbol])
        except Exception as e:
            log.warning(f"⚠️ watchdog pos {symbol}: {e}")
            self._bump_fail(symbol); return
        active = False; real_qty = 0.0
        for p in bpos_list:
            c = abs(float(p.get("contracts") or 0))
            if c > 1e-12:
                active = True; real_qty = c; break
        if not active:
            log.info(f"♻️ watchdog: {short(symbol)} اختفت → إزالة")
            async with self._lock:
                self.positions.pop(symbol, None)
            self._reset_fail(symbol); return
        pos["qty"] = real_qty

        # 2) الأوامر المعلقة لهذا الرمز فقط
        try:
            open_orders = await self.executor.fetch_open_orders(symbol)
        except Exception as e:
            log.warning(f"⚠️ watchdog orders {symbol}: {e}")
            self._bump_fail(symbol); return

        has_sl = False; tp_ids_seen = set()
        for o in open_orders:
            otype = (o.get("type") or "").upper()
            cid = o.get("clientOrderId") or ""
            oinfo = o.get("info") or {}
            ctype = (oinfo.get("type") or "").upper()
            # ✅ v7.2: قبول أي STOP_MARKET أو TRAILING_STOP_MARKET كـ SL
            is_stop = ("STOP_MARKET" in otype or "TRAILING_STOP_MARKET" in otype
                       or ctype in ("STOP_MARKET", "TRAILING_STOP_MARKET")
                       or "STOP" in ctype or "TRAILING" in ctype)
            is_tp = ("TAKE_PROFIT" in otype or "TAKE_PROFIT" in ctype)
            if is_stop:
                has_sl = True
                pos["sl_order_id"] = o.get("id")
            elif is_tp:
                if "_tp1" in cid: tp_ids_seen.add("tp1")
                elif "_tp2" in cid: tp_ids_seen.add("tp2")
                elif "_tp3" in cid: tp_ids_seen.add("tp3")

        # 3) أعد SL إن مفقود
        if not has_sl:
            log.warning(f"🛡️ watchdog: SL مفقود لـ {short(symbol)} → إعادة")
            placed = False
            if pos.get("trailing_active"):
                sl_side_ = "sell" if pos["direction"] == "bullish" else "buy"
                try:
                    t = await self.executor.create_order(
                        symbol, "TRAILING_STOP_MARKET", sl_side_, None, None,
                        {"callbackRate": TRAIL_CALLBACK_RATE, "reduceOnly": True,
                         "workingType": "MARK_PRICE",
                         "newClientOrderId": f"{pos['signal_id']}_trail_r"[:36]})
                    pos["sl_order_id"] = t.get("id")
                    pos["sl_last_verified"] = _time.time()
                    placed = True
                    await self._notify(f"🛡️ إعادة Trailing SL لـ {short(symbol)}")
                except Exception as e:
                    log.error(f"watchdog re-trail {symbol}: {e}")
            if not placed:
                if pos.get("breakeven_moved"):
                    placed = await self._place_sl(symbol, pos, pos["entry"], tag="sl_be_r")
                else:
                    placed = await self._place_sl(symbol, pos, pos["sl"], tag="sl_r")
                if placed:
                    await self._notify(f"🛡️ إعادة SL لـ {short(symbol)}")
            if not placed:
                self._bump_fail(symbol); return

        # 4) أعد TPs إن مفقودة
        if "tp1" not in tp_ids_seen and not pos["tp1_filled"]:
            await self._place_tp(symbol, pos, pos["tp1"], TP_SPLITS[0], "tp1_r")
            await asyncio.sleep(0.3)
        if "tp2" not in tp_ids_seen and not pos["tp2_filled"] and pos["tp1_filled"]:
            await self._place_tp(symbol, pos, pos["tp2"], TP_SPLITS[1], "tp2_r")
            await asyncio.sleep(0.3)
        if "tp3" not in tp_ids_seen and not pos["tp3_filled"]:
            await self._place_tp(symbol, pos, pos["tp3"], TP_SPLITS[2], "tp3_r")

        self._reset_fail(symbol)
        pos["sl_last_verified"] = _time.time()

    def _bump_fail(self, symbol):
        """✅ v7.2: threshold 5 بدلاً من 3"""
        n = self._watchdog_fails.get(symbol, 0) + 1
        self._watchdog_fails[symbol] = n
        log.warning(f"⚠️ watchdog فشل {short(symbol)} ({n}/5)")
        if n >= 5:
            log.error(f"🆘 watchdog: 5 فشل → إغلاق طارئ {short(symbol)}")
            asyncio.create_task(self._watchdog_kill(symbol))

    async def _watchdog_kill(self, symbol):
        await self._notify(f"🆘 <b>إغلاق طارئ</b> {short(symbol)} — فشل watchdog")
        await self._emergency_close(symbol)
        self._reset_fail(symbol)

    def _reset_fail(self, symbol):
        self._watchdog_fails.pop(symbol, None)

    async def reconcile(self):
        try:
            positions = await self.executor.fetch_positions()
        except Exception as e:
            log.error(f"reconcile fetch_positions: {e}"); return
        binance_pos = {}
        for p in positions:
            contracts = float(p.get("contracts") or 0)
            if abs(contracts) < 1e-12: continue
            binance_pos[p["symbol"]] = p

        for sym, p in binance_pos.items():
            if sym in self.positions: continue
            log.warning(f"⚠️ orphan {sym} → SL طارئ")
            side = "sell" if p.get("side") == "long" else "buy"
            entry = float(p.get("entryPrice") or 0)
            if entry <= 0: continue
            sl_price = entry * (0.985 if p.get("side") == "long" else 1.015)
            try:
                await self.executor.create_order(
                    sym, "STOP_MARKET", side, None, None,
                    {"stopPrice": self.executor.price_to_precision(sym, sl_price),
                     "closePosition": True, "workingType": "MARK_PRICE"})
                log.info(f"🛡️ SL طارئ {short(sym)} @ {sl_price}")
            except Exception as e:
                log.error(f"emergency SL {sym}: {e}")
            await asyncio.sleep(0.5)

        for sym in list(self.positions.keys()):
            if sym not in binance_pos:
                log.info(f"♻️ إزالة {short(sym)} — غير موجودة")
                async with self._lock:
                    self.positions.pop(sym, None)
        log.info(f"♻️ Reconcile: {len(self.positions)} محلية | {len(binance_pos)} Binance")

    async def _get_balance_usd(self) -> float:
        try:
            b = await self.executor.fetch_balance()
            return float(b.get("USDT", {}).get("total") or 0)
        except: return 0.0

    async def _notify(self, text):
        bot = self.bot_getter()
        if bot and CHAT_ID:
            try: await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
            except Exception as e: log.error(f"notify: {e}")

_positions_mgr: Optional[PositionManager] = None
_ws_mgr: Optional[WebSocketManager] = None

# ═══════════════════════════════════════════════════════════
# 9) Strategy Detectors
# ═══════════════════════════════════════════════════════════
_crossover_cache = {}; _divergence_cache = {}; _bb_cache = {}; _sr_cache = {}
_recent_signals = {}; _last_signal_time = {}
_last_signals = deque(maxlen=LAST_SIGNALS_MAX)
_filter_stats = defaultdict(int)

async def _df_for(symbol, tf, extra=60):
    data = await fetch_ohlcv_cached(symbol, tf, EMA_SLOW + extra)
    if not data or len(data) < EMA_SLOW + 5: return None
    return pd.DataFrame(data, columns=["ts","o","h","l","c","v"])

def compute_score(df, direction, curr_idx=-2):
    close = df["c"]; vol = df["v"]
    price = float(close.iloc[curr_idx])
    try:
        s = max(0, len(df) + curr_idx - 20); e = len(df) + curr_idx
        vma = float(vol.iloc[s:e].mean())
        vr = float(vol.iloc[curr_idx]) / vma if vma > 0 else 0
    except: vr = 0
    v_pts = 30 if vr>=4 else 25 if vr>=2.5 else 18 if vr>=1.5 else 10 if vr>=1 else 5 if vr>=0.5 else 0
    try: adx = float(calc_adx(df).iloc[curr_idx])
    except: adx = 0
    a_pts = 18 if adx>=50 else 20 if adx>=35 else 18 if adx>=25 else 12 if adx>=20 else 5 if adx>=15 else 0
    try:
        ef = float(calc_ema(close, EMA_FAST).iloc[curr_idx])
        es = float(calc_ema(close, EMA_SLOW).iloc[curr_idx])
        gap = abs(ef-es)/es*100 if es else 0
    except: gap = 0
    g_pts = 12 if gap>=0.6 else 15 if gap>=0.3 else 10 if gap>=0.1 else 6 if gap>=0.05 else 3 if gap>=0.02 else 0
    try:
        _,_,hist = calc_macd(close)
        hn = float(hist.iloc[curr_idx]); hp = float(hist.iloc[curr_idx-1])
        rising = hn > hp
        m_pts = 10 if (hn>0 and rising) else 5 if hn>0 else 7 if (hn<0 and rising) else 5
    except: m_pts = 0
    try: rsi = float(calc_rsi(close).iloc[curr_idx])
    except: rsi = 50
    if direction == "bullish":
        r_pts = 10 if 55<=rsi<=70 else 5 if rsi>70 else 7 if rsi>=45 else 3 if rsi>=30 else 0
    else:
        r_pts = 10 if 30<=rsi<=45 else 5 if rsi<30 else 7 if rsi<=55 else 3 if rsi<=70 else 0
    try: e50 = float(calc_ema(close, 50).iloc[curr_idx])
    except: e50 = price
    above = price > e50
    h_pts = 10 if (direction=="bullish" and above) or (direction=="bearish" and not above) else 0
    try:
        atr = float(calc_atr(df).iloc[curr_idx]); atr_pct = (atr/price)*100 if price>0 else 0
    except: atr_pct = 0
    t_pts = 5 if atr_pct>=0.30 else 3 if atr_pct>=0.15 else 1
    total = v_pts + a_pts + g_pts + m_pts + r_pts + h_pts + t_pts
    grade = ("🌟 ذهبية" if total>=80 else "⭐ قوية" if total>=65 else
             "✅ جيدة" if total>=55 else "🟡 متوسطة" if total>=45 else "⚪ ضعيفة")
    return {"score":total,"grade":grade,"adx":round(adx,1),"adx_pts":a_pts,
            "rsi":round(rsi,1),"rsi_pts":r_pts,"macd_pts":m_pts,
            "vol_ratio":round(vr,2),"vol_pts":v_pts,"atr_pct":round(atr_pct,2),
            "atr_pts":t_pts,"htf_pts":h_pts,"gap_pct":round(gap,3),"gap_pts":g_pts}

def classify_market(df, curr_idx=-2):
    if len(df) < 60: return "UNKNOWN"
    try:
        adx = float(calc_adx(df).iloc[curr_idx])
        close = df["c"]; bb = calc_bollinger(df)
        bb_w = float(bb["width"].iloc[curr_idx])
        e50 = calc_ema(close, 50)
        e50_slope = (float(e50.iloc[curr_idx]) - float(e50.iloc[curr_idx-5])) / float(e50.iloc[curr_idx-5]) * 100
        atr = float(calc_atr(df).iloc[curr_idx]); price = float(close.iloc[curr_idx])
        atr_pct = (atr/price)*100 if price else 0
        if adx > 25 and abs(e50_slope) > 0.15:
            return "TREND_UP" if e50_slope > 0 else "TREND_DOWN"
        if bb_w < 2.0 and adx < 18: return "RANGE"
        if atr_pct > 1.2: return "VOLATILE"
        return "RANGE" if adx < 20 else ("TREND_UP" if e50_slope > 0 else "TREND_DOWN")
    except: return "UNKNOWN"

async def get_htf_trend(symbol):
    if not HTF_ENABLED: return "NEUTRAL"
    data = await fetch_ohlcv_cached(symbol, HTF_TIMEFRAME, 250)
    if not data or len(data) < 200: return "NEUTRAL"
    df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
    e50 = float(calc_ema(df["c"], 50).iloc[-1]); e200 = float(calc_ema(df["c"], 200).iloc[-1])
    price = float(df["c"].iloc[-1])
    if price > e50 > e200: return "UP"
    if price < e50 < e200: return "DOWN"
    return "NEUTRAL"

async def get_mtf_alignment(symbol, direction):
    if not MTF_ENABLED: return 0, "—"
    score = 0
    for tf in ["5m","15m","1h"]:
        try:
            data = await fetch_ohlcv_cached(symbol, tf, 60)
            if not data or len(data) < 30: continue
            df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
            ef = float(calc_ema(df["c"], EMA_FAST).iloc[-2]); es = float(calc_ema(df["c"], EMA_SLOW).iloc[-2])
            if direction == "bullish" and ef > es: score += 1
            elif direction == "bearish" and ef < es: score += 1
        except: continue
    if score >= 3: return +8, "توافق كامل (3/3) 🌟"
    if score == 2: return +3, "توافق جزئي (2/3) ✅"
    if score == 1: return -5, "توافق ضعيف (1/3) ⚠️"
    return -15, "تعارض كامل (0/3) ❌"

def check_vwap_slope(df, direction, thresholds, tf="15m"):
    min_pct = thresholds.get("vwap_slope_pct", 0.05)
    if len(df) < VWAP_SLOPE_LOOKBACK + 1: return True, 0.0
    session = "week" if tf == "1h" else "day"
    vwap = calc_vwap_session(df, session)
    v_now = float(vwap.iloc[-1]); v_prev = float(vwap.iloc[-VWAP_SLOPE_LOOKBACK-1])
    if v_prev == 0: return True, 0.0
    slope = (v_now - v_prev) / v_prev * 100
    if direction == "bullish" and slope < -min_pct: return False, slope
    if direction == "bearish" and slope > min_pct: return False, slope
    return True, slope

def has_conflicting_signal(sym, direction):
    prev = _recent_signals.get(sym)
    if not prev: return False
    if _time.time() - prev[1] < 300 and prev[0] != direction: return True
    return False

def record_signal(sym, direction): _recent_signals[sym] = (direction, _time.time())

def check_symbol_cooldown(sym):
    if SYMBOL_COOLDOWN_SEC <= 0: return True, 0
    now = _time.time(); last = _last_signal_time.get(sym, 0)
    if now - last < SYMBOL_COOLDOWN_SEC: return False, int(SYMBOL_COOLDOWN_SEC - (now - last))
    return True, 0

def mark_symbol_sent(sym): _last_signal_time[sym] = _time.time()

async def _make_cross(symbol, tf, df, direction, alert_type, strength, curr, source="ema", extra=None):
    cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
    cts = int(df["ts"].iloc[curr])
    levels = compute_levels(df, direction, curr, tf=tf)
    support = compute_score(df, direction, curr)
    out = {"symbol": symbol, "timeframe": tf, "direction": direction,
           "ema_fast": round(cf, 8), "ema_slow": round(cs, 8),
           "price": round(float(df["c"].iloc[curr]), 8), "candle_ts": cts,
           "gap_pct": round(abs(cf-cs)/cs*100, 3) if cs else 0,
           "strength": strength, "alert_type": alert_type,
           "support": support, "base_score": support["score"],
           "final_score": support["score"], "_df": df,
           "levels": levels, "source": source,
           "created_at": _time.time()}
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
    key = (symbol, tf); last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts and last.get("direction") == d: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "confirmed", "✅ مؤكد", curr)

async def detect_pre_crossover(symbol, tf):
    if not ENABLE_PRE_CROSS: return None
    df = await _df_for(symbol, tf, extra=60)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr = -2
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    if abs(cf-cs)/cs*100 >= PRE_CROSS_GAP: return None
    gaps = []
    for i in range(PRE_CROSS_LOOKBACK):
        idx = curr - i
        f = float(df["ef"].iloc[idx]); s = float(df["es"].iloc[idx])
        gaps.append(abs(f-s)/s*100)
    gch = list(reversed(gaps))
    if not all(gch[i] >= gch[i+1] for i in range(len(gch)-1)): return None
    d = "bullish" if cf < cs else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "pre"); last = _crossover_cache.get(key)
    if last and cts - last.get("candle_ts", 0) < 900_000 * PRE_CROSS_COOLDOWN: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "pre", "🔔 تقارب", curr)

async def detect_live_crossover(symbol, tf):
    if not ENABLE_LIVE_CROSS: return None
    df = await _df_for(symbol, tf)
    if df is None: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    curr, prev = -1, -2
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])
    bull = (cf>cs) and (pf<=ps); bear = (cf<cs) and (pf>=ps)
    if not (bull or bear): return None
    d = "bullish" if bull else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "live"); last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts: return None
    _crossover_cache[key] = {"candle_ts": cts, "direction": d}
    return await _make_cross(symbol, tf, df, d, "live", "⚡ مبدئي", curr)

async def detect_rsi_divergence(symbol, tf):
    if not ENABLE_RSI_DIVERGENCE: return None
    df = await _df_for(symbol, tf, extra=RSI_DIV_LOOKBACK + 10)
    if df is None or len(df) < RSI_DIV_LOOKBACK + 5: return None
    rsi = calc_rsi(df["c"]); close = df["c"]; curr = -2
    key = (symbol, tf, "div"); last = _divergence_cache.get(key)
    if last and (_time.time() - last) < RSI_DIV_COOLDOWN_MIN * 60: return None
    win = RSI_DIV_LOOKBACK; best = None
    for i in range(3, win // 2):
        for j in range(i + 3, win):
            idx1 = curr - i; idx2 = curr - j
            if idx1 >= 0 or idx2 >= 0: continue
            c1 = float(close.iloc[idx1]); c2 = float(close.iloc[idx2])
            r1 = float(rsi.iloc[idx1]);   r2 = float(rsi.iloc[idx2])
            if c2 < c1 and r2 > r1 and r1 < 40:
                s = (r2-r1)/max(1, 40-r1)
                if not best or s > best["strength"]:
                    best = {"dir":"bullish","strength":s,"idx1":idx1,"idx2":idx2,"r1":r1,"r2":r2}
            if c2 > c1 and r2 < r1 and r1 > 60:
                s = (r1-r2)/max(1, r1-60)
                if not best or s > best["strength"]:
                    best = {"dir":"bearish","strength":s,"idx1":idx1,"idx2":idx2,"r1":r1,"r2":r2}
    if not best or best["strength"] < 0.15: return None
    d = best["dir"]
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _divergence_cache[key] = _time.time()
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "🔄 دايفرجنس", curr, source="rsi_div")
    cross["div_info"] = {"r1":round(best["r1"],1),"r2":round(best["r2"],1)}
    return cross

async def detect_bb_squeeze_breakout(symbol, tf):
    if not ENABLE_BB_SQUEEZE: return None
    df = await _df_for(symbol, tf, extra=80)
    if df is None or len(df) < 60: return None
    bb = calc_bollinger(df); curr = -2
    key = (symbol, tf, "bb"); last = _bb_cache.get(key)
    if last and (_time.time() - last) < BB_SQUEEZE_COOLDOWN_MIN * 60: return None
    hist_width = bb["width"].iloc[curr-50:curr].dropna()
    if len(hist_width) < 20: return None
    if float(bb["width"].iloc[curr]) > hist_width.quantile(BB_SQUEEZE_PCTL): return None
    price = float(df["c"].iloc[curr]); up = float(bb["upper"].iloc[curr]); lo = float(bb["lower"].iloc[curr])
    if price > up: d = "bullish"
    elif price < lo: d = "bearish"
    else: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _bb_cache[key] = _time.time()
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "💥 BB", curr, source="bb_squeeze")
    cross["bb_info"] = {"width":round(float(bb["width"].iloc[curr]),2)}
    return cross

async def detect_sr_breakout(symbol, tf):
    if not ENABLE_SR_BREAKOUT: return None
    df = await _df_for(symbol, tf, extra=SR_BREAKOUT_LOOKBACK + 10)
    if df is None or len(df) < SR_BREAKOUT_LOOKBACK + 5: return None
    curr = -2; key = (symbol, tf, "sr"); last = _sr_cache.get(key)
    if last and (_time.time() - last) < SR_BREAKOUT_COOLDOWN_MIN * 60: return None
    window = df.iloc[curr - SR_BREAKOUT_LOOKBACK:curr]
    high = float(window["h"].max()); low = float(window["l"].min())
    close_prev = float(window["c"].iloc[-1])
    pivot = (high + low + close_prev) / 3
    r1 = 2*pivot - low; s1 = 2*pivot - high
    price_now = float(df["c"].iloc[curr]); price_prev = float(df["c"].iloc[curr-1])
    if price_now > r1 and price_prev <= r1: d, level = "bullish", r1
    elif price_now < s1 and price_prev >= s1: d, level = "bearish", s1
    else: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _sr_cache[key] = _time.time()
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "🎯 S/R", curr, source="sr_breakout")
    cross["sr_info"] = {"level":level}
    return cross

def _get_detectors():
    dets = []
    if ENABLE_PRE_CROSS:  dets.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: dets.append(detect_live_crossover)
    if ENABLE_CONFIRMED:  dets.append(detect_crossover)
    if ENABLE_RSI_DIVERGENCE: dets.append(detect_rsi_divergence)
    if ENABLE_BB_SQUEEZE:     dets.append(detect_bb_squeeze_breakout)
    if ENABLE_SR_BREAKOUT:    dets.append(detect_sr_breakout)
    return dets

# ═══════════════════════════════════════════════════════════
# 10) Filter
# ═══════════════════════════════════════════════════════════
async def passes_filter(cross, htf_trend=None):
    s = cross.get("support", {})
    if not s: return False, "no-ind", "other"
    tf = cross["timeframe"]; vol = s.get("vol_ratio", 0); adx = s.get("adx", 0)
    atr = s.get("atr_pct", 0); base_score = cross.get("base_score", 0)
    gap_pct = s.get("gap_pct", 0); source = cross.get("source", "ema")
    direction = cross["direction"]

    min_gap = MIN_EMA_GAP_PCT.get(tf, 0.02)
    if gap_pct < min_gap and source == "ema":
        return False, f"gap {gap_pct:.3f}%", "ema_gap"
    min_atr = MIN_ATR_PCT.get(tf, 0.05)
    if atr < min_atr: return False, f"ATR {atr}%", "atr_low"

    mtf_bonus, mtf_desc = await get_mtf_alignment(cross["symbol"], direction)
    cross["mtf"] = mtf_desc; cross["mtf_bonus"] = mtf_bonus
    score = base_score + mtf_bonus
    cross["final_score"] = score
    if mtf_bonus <= -15: return False, mtf_desc, "mtf"
    if score < ABS_MIN_SCORE: return False, f"Score {score}", "score_abs"

    th = get_thresholds(atr_pct=atr, adx=adx, tf=tf)
    min_score = th["min_score"]
    if tf == "15m": min_score = max(min_score, th.get("score_15m_override", min_score))
    if score < min_score: return False, f"Score {score}<{min_score:.0f}", "score"
    if vol < th["min_vol_ratio"]: return False, f"Vol {vol}×", "vol"
    if adx < th["min_adx"]: return False, f"ADX {adx}", "adx"
    if atr > th["max_atr_pct"]: return False, f"ATR {atr}%", "atr"

    df = cross.get("_df"); market = cross.get("market") or "UNKNOWN"
    if market == "UNKNOWN" and df is not None:
        market = classify_market(df); cross["market"] = market

    if source == "bb_squeeze":
        if adx < MIN_ADX_FOR_BB: return False, "BB ADX", "adx"
        if vol < BB_MIN_VOL: return False, "BB Vol", "vol"
        if market == "RANGE" and adx < BB_MIN_ADX_IN_RANGE: return False, "BB RANGE", "range"
        if BB_CONFIRM_ENABLED and not ((mtf_bonus >= 8) or (adx > 22) or (vol > 1.5)):
            return False, "BB no-confirm", "bb_confirm"

    if source == "ema":
        if market == "TREND_DOWN" and direction == "bullish" and mtf_bonus < 5:
            return False, "ضد الترند", "trend"
        if market == "TREND_UP" and direction == "bearish" and mtf_bonus < 5:
            return False, "ضد الترند", "trend"

    if market == "RANGE" and source == "ema" and cross.get("alert_type") == "confirmed":
        return False, "سوق عرضي", "range"

    if th["block_against_htf"] and cross.get("alert_type") != "pre" and htf_trend:
        if direction == "bullish" and htf_trend == "DOWN":
            return False, "HTF ضد LONG", "htf"
        if direction == "bearish" and htf_trend == "UP":
            return False, "HTF ضد SHORT", "htf"

    if df is not None:
        ok, slope = check_vwap_slope(df, direction, th, tf=tf)
        if not ok: return False, f"VWAP {slope:+.2f}%", "vwap"

    if STRICTNESS == "auto": _smart.record(True, score, source=source)
    return True, "", ""

# ═══════════════════════════════════════════════════════════
# 11) Trading Loop
# ═══════════════════════════════════════════════════════════
async def trading_loop(context):
    if _executor is None or _positions_mgr is None: return
    if _positions_mgr.paused: return
    detectors = _get_detectors()
    for sym in ALL_SYMBOLS:
        for tf in TIMEFRAMES:
            for det in detectors:
                try:
                    cross = await det(sym, tf)
                    if not cross: continue
                    if TRADE_ON_CONFIRMED_ONLY and cross.get("alert_type") != "confirmed":
                        continue
                    ok_cd, rem = check_symbol_cooldown(sym)
                    if not ok_cd: continue
                    if sym in _positions_mgr.positions: continue
                    ok, why = await _positions_mgr.can_open_new()
                    if not ok: continue
                    htf = await get_htf_trend(sym)
                    passed, reason, cat = await passes_filter(cross, htf)
                    if not passed:
                        _filter_stats[f"filtered_{cat}"] += 1
                        continue
                    if not cross.get("levels"): continue
                    pos = await _positions_mgr.open_position(cross)
                    if pos:
                        mark_symbol_sent(sym); record_signal(sym, cross["direction"])
                        _filter_stats["sent"] += 1
                        _last_signals.append({
                            "symbol": sym, "timeframe": tf, "direction": cross["direction"],
                            "source": cross.get("source","ema"), "score": cross.get("final_score",0),
                            "entry": pos["entry"], "ts": _time.time()})
                        text = (f"🚀 <b>تم الفتح — {short(sym)}</b> [{tf}]\n"
                                f"{'🟢 LONG' if cross['direction']=='bullish' else '🔴 SHORT'}\n"
                                f"━━━━━━━━━━━━━━━━━━━\n"
                                f"💰 Entry: <b>{fmt_price(pos['entry'])}</b>\n"
                                f"📦 Qty: <b>{pos['qty']}</b> | {LEVERAGE}x\n"
                                f"💵 Margin: <b>${pos['notional']/LEVERAGE:.2f}</b>\n"
                                f"🛑 SL: <b>{fmt_price(pos['sl'])}</b> (-{pos['sl_pct']}%)\n"
                                f"🎯 TP1: {fmt_price(pos['tp1'])}\n"
                                f"🎯 TP2: {fmt_price(pos['tp2'])}\n"
                                f"🎯 TP3: {fmt_price(pos['tp3'])}")
                        try:
                            await context.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
                        except Exception as e:
                            log.error(f"send: {e}")
                except Exception as e:
                    log.exception(f"trading {sym} {tf}: {e}")
            await asyncio.sleep(0.2)
        await asyncio.sleep(0.3)

async def watchdog_job(context):
    if _positions_mgr is None or not _positions_mgr.positions: return
    try:
        await _positions_mgr.watchdog_check()
    except Exception as e:
        log.exception(f"watchdog_job: {e}")

# ═══════════════════════════════════════════════════════════
# 12) Telegram Commands
# ═══════════════════════════════════════════════════════════
def _get_bot(): return _bot_ref[0] if _bot_ref[0] else None
_bot_ref: list = [None]

def _source_label(src):
    return {"ema":"📈 EMA","rsi_div":"🔄 RSI Div","bb_squeeze":"💥 BB",
            "sr_breakout":"🎯 S/R"}.get(src, "—")

async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    sd, sn = get_session_info()
    bal = await _positions_mgr._get_balance_usd() if _positions_mgr else 0
    txt = (f"🤖 <b>Smart Trading Bot v7.2</b>\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"💰 الرصيد: <b>${bal:.2f}</b>\n"
           f"📊 Positions: <b>{len(_positions_mgr.positions) if _positions_mgr else 0}</b>/{MAX_CONCURRENT_POSITIONS}\n"
           f"⚙️ Leverage: <b>{LEVERAGE}x ISOLATED</b>\n"
           f"📏 Notional: <b>${POSITION_NOTIONAL_USD}</b>\n"
           f"🕐 {sn} ({sd:+d})\n"
           f"{'⏸️ موقوف' if _positions_mgr and _positions_mgr.paused else '▶️ يعمل'}\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"<b>أوامر:</b>\n"
           f"/positions /close /closeall\n"
           f"/pause /resume /emergency\n"
           f"/balance /status /stats /perf\n"
           f"/last /why [SYM]\n"
           f"/strict /clearcache")
    await u.message.reply_text(txt, parse_mode="HTML")

async def cmd_positions(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _positions_mgr or not _positions_mgr.positions:
        await u.message.reply_text("📭 لا صفقات مفتوحة."); return
    lines = [f"💼 <b>الصفقات المفتوحة ({len(_positions_mgr.positions)})</b>",
             "━━━━━━━━━━━━━━━━━━━"]
    for sym, pos in _positions_mgr.positions.items():
        try:
            data = await _executor.fetch_ohlcv(sym, "1m", 2)
            price = float(data[-1][4]) if data else pos["entry"]
        except: price = pos["entry"]
        bull = pos["direction"] == "bullish"
        pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100) * (1 if bull else -1)
        pnl_usd = (price - pos["entry"]) * pos["qty"] * (1 if bull else -1)
        lines.append(
            f"\n{'🟢' if bull else '🔴'} <b>{short(sym)}</b> [{pos['tf']}]\n"
            f"   Entry: {fmt_price(pos['entry'])} → {fmt_price(price)}\n"
            f"   P&L: <b>{pnl_pct:+.2f}%</b> ({pnl_usd:+.2f}$) | Qty: {pos['qty']}\n"
            f"   SL: {fmt_price(pos['sl'])} | TP: {fmt_price(pos['tp1'])}/{fmt_price(pos['tp2'])}/{fmt_price(pos['tp3'])}\n"
            f"   {'✅TP1 ' if pos['tp1_filled'] else ''}"
            f"{'✅TP2 ' if pos['tp2_filled'] else ''}"
            f"{'✅TP3 ' if pos['tp3_filled'] else ''}"
            f"{'🔄Trail ' if pos.get('trailing_active') else ''}"
            f"{'⚖️BE ' if pos.get('breakeven_moved') else ''}")
        await asyncio.sleep(0.3)
    await u.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_close(u: Update, c: ContextTypes.DEFAULT_TYPE):
    args = c.args or []
    if not args: await u.message.reply_text("الاستخدام: /close BTC"); return
    sym = find_symbol_by_name(args[0])
    if not sym or sym not in _positions_mgr.positions:
        await u.message.reply_text(f"❌ لا صفقة على {args[0]}"); return
    await u.message.reply_text(f"⏳ إغلاق {short(sym)} ...")
    ok = await _positions_mgr.close_position(sym, "manual")
    await u.message.reply_text("✅ أُغلقت" if ok else "❌ فشل")

async def cmd_closeall(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _positions_mgr.positions:
        await u.message.reply_text("📭 لا صفقات."); return
    await u.message.reply_text(f"⚠️ إغلاق {len(_positions_mgr.positions)} صفقة ...")
    await _positions_mgr.close_all()
    await u.message.reply_text("✅ تم")

async def cmd_emergency(u: Update, c: ContextTypes.DEFAULT_TYPE):
    _positions_mgr.paused = True
    await u.message.reply_text("🆘 إغلاق طارئ + إيقاف")
    await _positions_mgr.close_all("emergency")

async def cmd_pause(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """⏸️ إيقاف فتح صفقات جديدة (v7.2 محسّن)"""
    if _positions_mgr.paused:
        await u.message.reply_text(
            "⏸️ <b>البوت موقوف بالفعل</b>\n\n"
            f"📊 Positions المفتوحة: <b>{len(_positions_mgr.positions)}</b>\n"
            "💡 للاستئناف: <code>/resume</code>",
            parse_mode="HTML")
        return
    _positions_mgr.paused = True
    log.warning("⏸️ البوت أوقف عبر /pause")
    await u.message.reply_text(
        "⏸️ <b>تم الإيقاف</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "🔒 <b>لن تُفتح صفقات جديدة</b>\n"
        "✅ <b>الصفقات الحالية تستمر</b>\n"
        f"📊 Positions: <b>{len(_positions_mgr.positions)}</b>\n"
        f"🕐 {syria_now_str()}\n\n"
        "💡 للاستئناف: <code>/resume</code>",
        parse_mode="HTML")

async def cmd_resume(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """▶️ استئناف فتح صفقات جديدة (v7.2 محسّن)"""
    if not _positions_mgr.paused:
        await u.message.reply_text(
            "▶️ <b>البوت يعمل بالفعل</b>\n\n"
            f"📊 Positions: <b>{len(_positions_mgr.positions)}</b>\n"
            "💡 للإيقاف: <code>/pause</code>",
            parse_mode="HTML")
        return
    old_losses = _positions_mgr.consecutive_losses
    _positions_mgr.paused = False
    _positions_mgr.consecutive_losses = 0
    log.info(f"▶️ البوت استؤنف عبر /resume (كان {old_losses} خسائر)")
    await u.message.reply_text(
        "▶️ <b>تم الاستئناف</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>سيبدأ فتح صفقات جديدة</b>\n"
        f"🔄 تصفير الخسائر المتتالية ({old_losses} → 0)\n"
        f"📊 Positions: <b>{len(_positions_mgr.positions)}</b>\n"
        f"🕐 {syria_now_str()}\n\n"
        "⏱️ الدورة التالية خلال ≤ 120 ثانية",
        parse_mode="HTML")

async def cmd_balance(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        b = await _executor.fetch_balance()
        usdt = b.get("USDT", {})
        await u.message.reply_text(
            f"💰 <b>الرصيد</b>\n"
            f"• Total: <b>{usdt.get('total',0):.4f}</b> USDT\n"
            f"• Free: <b>{usdt.get('free',0):.4f}</b>\n"
            f"• Used: <b>{usdt.get('used',0):.4f}</b>",
            parse_mode="HTML")
    except Exception as e:
        await u.message.reply_text(f"❌ {e}")

async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
    sd, sn = get_session_info(); st = _smart.stats()
    total = sum(v for k, v in _filter_stats.items() if k.startswith("filtered_"))
    bal = await _positions_mgr._get_balance_usd()
    jq = c.application.job_queue; jobs_n = len(jq.jobs()) if jq else 0
    status_icon = "⏸️ موقوف" if _positions_mgr.paused else "▶️ يعمل"
    status_detail = ""
    if _positions_mgr.paused:
        if _positions_mgr.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            status_detail = f"\n⚠️ بسبب: {MAX_CONSECUTIVE_LOSSES} خسائر متتالية"
        else:
            status_detail = "\n💡 أوقف يدوياً عبر /pause"
    await u.message.reply_text(
        f"🤖 <b>الحالة v7.2</b>\n"
        f"💰 ${bal:.2f} | 📊 {len(_positions_mgr.positions)}/{MAX_CONCURRENT_POSITIONS}\n"
        f"{status_icon}{status_detail}\n"
        f"🕐 {sn} ({sd:+d}) | Jobs: {jobs_n}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 إشارات: {_filter_stats['sent']}\n"
        f"🚫 محجوبة: {total}\n"
        f"📈 WR: <b>{st['wr']}%</b>\n"
        f"📉 P&L: <b>{_positions_mgr.daily_pnl_usd:+.2f}$</b>",
        parse_mode="HTML")

async def cmd_stats(u: Update, c: ContextTypes.DEFAULT_TYPE):
    st = _smart.stats()
    if st["n"] == 0:
        await u.message.reply_text("📊 لا نتائج."); return
    bar = "█" * int(st["wr"]/5) + "░" * (20 - int(st["wr"]/5))
    await u.message.reply_text(
        f"📊 <b>الأداء</b>\n<code>{bar}</code>\n"
        f"📈 WR: <b>{st['wr']}%</b>\n"
        f"✅ {st['wins']} | ❌ {st['losses']}",
        parse_mode="HTML")

async def cmd_perf(u: Update, c: ContextTypes.DEFAULT_TYPE):
    rep = _smart.source_report()
    if not rep: await u.message.reply_text("لا بيانات."); return
    lines = ["📊 <b>أداء المصادر</b>"]
    for src, d in rep.items():
        tot = d["wins"] + d["losses"]
        wr = round(d["wins"]/tot*100,1) if tot>0 else 0
        lines.append(f"\n🔹 <b>{_source_label(src)}</b>\n   ✅{d['wins']} ❌{d['losses']} | WR {wr}%")
    await u.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_last(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _last_signals: await u.message.reply_text("📭 لا إشارات."); return
    lines = ["📋 <b>آخر الإشارات</b>"]
    for s in reversed(list(_last_signals)):
        d_icon = "🟢" if s["direction"]=="bullish" else "🔴"
        ts = datetime.fromtimestamp(s["ts"], tz=SYRIA_TZ).strftime("%H:%M")
        lines.append(f"{d_icon} <b>{short(s['symbol'])}</b> [{s['timeframe']}] "
                     f"{_source_label(s['source'])} | {ts} | Score={s['score']}")
    await u.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_why(u: Update, c: ContextTypes.DEFAULT_TYPE):
    args = c.args or []
    if not args: await u.message.reply_text("الاستخدام: /why BTC"); return
    sym = find_symbol_by_name(args[0])
    if not sym: await u.message.reply_text(f"❌ {args[0]}"); return
    lines = [f"📊 <b>تشخيص {short(sym)}</b>"]
    for tf in ["5m","15m","1h"]:
        try:
            df = await _df_for(sym, tf)
            if df is None: lines.append(f"\n<b>[{tf}]</b> ❌"); continue
            df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
            curr = -2
            cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
            gap = abs(cf-cs)/cs*100
            atr_p = float(calc_atr(df).iloc[curr])/float(df["c"].iloc[curr])*100
            adx = float(calc_adx(df).iloc[curr]); rsi = float(calc_rsi(df["c"]).iloc[curr])
            lines.append(f"\n<b>[{tf}]</b> 💰{fmt_price(float(df['c'].iloc[curr]))}\n"
                         f"EMA gap {gap:.3f}% | ATR {atr_p:.2f}% | ADX {adx:.1f} | RSI {rsi:.1f}")
        except Exception as e:
            lines.append(f"\n<b>[{tf}]</b> ❌ {e}")
    await u.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_strict(u: Update, c: ContextTypes.DEFAULT_TYPE):
    th = get_thresholds(tf="15m")
    await u.message.reply_text(
        f"🎯 <b>الإعدادات v7.2</b>\n"
        f"• Leverage: <b>{LEVERAGE}x ISOLATED</b>\n"
        f"• Notional: <b>${POSITION_NOTIONAL_USD}</b>\n"
        f"• Max concurrent: <b>{MAX_CONCURRENT_POSITIONS}</b>\n"
        f"• MAX_SL_PCT: <b>{MAX_SL_PCT}%</b>\n"
        f"• Score مطلق ≥ <b>{ABS_MIN_SCORE}</b>\n"
        f"• Score ديناميكي ≥ <b>{th['min_score']:.0f}</b>\n"
        f"• Daily loss: <b>{MAX_DAILY_LOSS_PCT}%</b>\n"
        f"• Max consec losses: <b>{MAX_CONSECUTIVE_LOSSES}</b>\n"
        f"• TP splits: <b>{TP_SPLITS}</b>\n"
        f"• Trailing: <b>{TRAIL_CALLBACK_RATE}%</b>\n"
        f"• Watchdog: كل <b>{WATCHDOG_INTERVAL_SEC}s</b>\n"
        f"• Watchdog threshold: <b>5 فشل</b>",
        parse_mode="HTML")

async def cmd_clearcache(u: Update, c: ContextTypes.DEFAULT_TYPE):
    _crossover_cache.clear(); _divergence_cache.clear()
    _bb_cache.clear(); _sr_cache.clear()
    _ohlcv_rest_cache.clear()
    _smart.reset_feedback()
    await u.message.reply_text("✅ تم")

async def error_handler(u, c):
    err = c.error
    if isinstance(err, (NetworkError, TimedOut)): log.warning(f"⚠️ {err}"); return
    log.error(f"❌ {err}", exc_info=err)

# ═══════════════════════════════════════════════════════════
# 13) Health
# ═══════════════════════════════════════════════════════════
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-Type","text/plain; charset=utf-8")
        self.end_headers(); self.wfile.write(b"OK - v7.2")
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def log_message(self, *a): pass

def run_health():
    port = int(os.getenv("PORT","8080"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()

# ═══════════════════════════════════════════════════════════
# 14) Main
# ═══════════════════════════════════════════════════════════
async def _startup_async(app: Application):
    global _executor, _positions_mgr, _ws_mgr
    _executor = BinanceExecutor()
    _positions_mgr = PositionManager(_executor, _get_bot)
    _ws_mgr = WebSocketManager(_executor, _positions_mgr)
    _positions_mgr.ws_ref = _ws_mgr

    log.info(f"⏳ انتظار {STARTUP_DELAY_SEC}s قبل مزامنة Binance ...")
    await asyncio.sleep(STARTUP_DELAY_SEC)

    await _positions_mgr.reconcile()
    await asyncio.sleep(2)

    log.info("📊 جلب بيانات أولية ...")
    for sym in ALL_SYMBOLS:
        for tf in TIMEFRAMES:
            try:
                await fetch_ohlcv_cached(sym, tf, 200)
                await asyncio.sleep(INTER_SYMBOL_DELAY_SEC)
            except Exception as e:
                log.warning(f"initial {sym} {tf}: {e}")

    await _ws_mgr.start()
    log.info("🚀 جاهز — Trading Engine يعمل")

def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ TELEGRAM_REPORT_BOT_TOKEN/CHAT_ID مطلوبة"); return
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("❌ BINANCE_API_KEY/SECRET مطلوبة"); return

    threading.Thread(target=run_health, daemon=True).start()

    print(f"📊 Symbols: {ALL_SYMBOLS}")
    print(f"⚙️ Leverage: {LEVERAGE}x ISOLATED | ${POSITION_NOTIONAL_USD}/trade")
    print(f"🛡️ Max positions: {MAX_CONCURRENT_POSITIONS}")
    print(f"🛡️ Max SL: {MAX_SL_PCT}% | Daily loss: {MAX_DAILY_LOSS_PCT}%")
    print(f"⏱️  Startup delay: {STARTUP_DELAY_SEC}s")
    print(f"🛡️ Watchdog: كل {WATCHDOG_INTERVAL_SEC}s | threshold: 5 فشل")

    from telegram.request import HTTPXRequest
    request = HTTPXRequest(read_timeout=30.0, write_timeout=30.0,
                           connect_timeout=30.0, pool_timeout=30.0)

    app = (Application.builder().token(BOT_TOKEN)
           .request(request).get_updates_request(request).build())

    if app.job_queue is None:
        raise RuntimeError("job_queue غير متوفر")

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("closeall", cmd_closeall))
    app.add_handler(CommandHandler("emergency", cmd_emergency))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("perf", cmd_perf))
    app.add_handler(CommandHandler("last", cmd_last))
    app.add_handler(CommandHandler("why", cmd_why))
    app.add_handler(CommandHandler("strict", cmd_strict))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    app.add_error_handler(error_handler)

    async def _post_init(app):
        _bot_ref[0] = app.bot
        await _startup_async(app)

    app.post_init = _post_init

    app.job_queue.run_repeating(trading_loop, interval=120, first=180, name="trading")
    app.job_queue.run_repeating(watchdog_job, interval=WATCHDOG_INTERVAL_SEC,
                                first=300, name="watchdog")

    print("✅ Start polling")
    app.run_polling(allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True, bootstrap_retries=5)


if __name__ == "__main__":
    main()
