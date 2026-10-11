"""
Unified Smart Bot v7.1 — WebSocket + Binance Execution
=======================================================
  🆕 Binance Futures WebSocket (bookTicker)
  🆕 FairPrice: WS → REST → Median (تدريجي)
  🆕 SL/TP تُحسب من سعر التنفيذ الفعلي
  🆕 Webshare Proxy (اختياري للـ WS، إلزامي للـ REST)
  ✓ استراتيجية v6.4.3 كما هي
  ✓ Risk Manager (4 صفقات، Kill-switch)
"""
import os, asyncio, logging, threading, time as _time, json
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collections import deque, defaultdict

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
for _n in ("httpx", "telegram", "telegram.ext", "ccxt", "websockets"):
    logging.getLogger(_n).setLevel(logging.WARNING)
log = logging.getLogger("unified")

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
PROXY_URL = os.getenv("PROXY_URL") or None
WS_USE_PROXY = os.getenv("WS_USE_PROXY", "0") == "1"

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

EXECUTION_ENABLED        = os.getenv("EXECUTION_ENABLED", "1") == "1"
TRADE_MARGIN_USD         = float(os.getenv("TRADE_MARGIN_USD", "400"))
LEVERAGE                 = int(os.getenv("LEVERAGE", "50"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT", "4"))
MAX_DAILY_LOSS_USD       = float(os.getenv("MAX_DAILY_LOSS_USD", "600"))
MAX_PRICE_DIVERGENCE_PCT = float(os.getenv("MAX_PRICE_DIV", "0.20"))

PRIMARY_EXCHANGE = "bybit"
FALLBACK_EXCHANGES = ["okx", "kucoin", "bitget"]
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
OHLCV_CACHE_SECONDS = 120
CONFLICT_WINDOW_SEC = 300
HTF_ENABLED = True
HTF_TIMEFRAME = "1h"
HTF_BLOCK_OPPOSITE = True
VWAP_SLOPE_LOOKBACK = 5
STRICTNESS = "auto"
STRICTNESS_OVERRIDE: dict = {}
SYMBOL_DELAY_MS = 200
SYRIA_TZ = ZoneInfo("Asia/Damascus")
SIGNAL_TRACK_MAX_MIN = 90
SIGNAL_TRACK_INTERVAL_MIN = 1
MIN_SL_PCT = {"5m": 0.35, "15m": 0.50, "1h": 0.80}
LAST_SIGNALS_MAX = 20

def _env_float(k, d):
    try: return float(os.getenv(k, d))
    except: return d
def _env_int(k, d):
    try: return int(os.getenv(k, d))
    except: return d

MIN_EMA_GAP_PCT = {"5m": _env_float("MIN_EMA_GAP_5M", 0.02),
                   "15m": _env_float("MIN_EMA_GAP_15M", 0.015),
                   "1h": _env_float("MIN_EMA_GAP_1H", 0.01)}
MIN_ATR_PCT = {"5m": _env_float("MIN_ATR_5M", 0.05),
               "15m": _env_float("MIN_ATR_15M", 0.10),
               "1h": _env_float("MIN_ATR_1H", 0.20)}
MIN_ADX_FOR_BB       = _env_float("MIN_ADX_FOR_BB", 8.0)
SYMBOL_COOLDOWN_SEC  = _env_int("SYMBOL_COOLDOWN_SEC", 180)
STRICTNESS_MAX_FINAL = _env_int("STRICTNESS_MAX_FINAL", 80)
ABS_MIN_SCORE        = _env_int("ABS_MIN_SCORE", 52)
BB_MIN_VOL           = _env_float("BB_MIN_VOL", 1.0)
BB_MIN_ADX_IN_RANGE  = _env_float("BB_MIN_ADX_IN_RANGE", 20)
BB_CONFIRM_ENABLED   = True


def _to_ws_sym(symbol):
    """BTC/USDT:USDT → btcusdt (لـ Binance Futures WS)"""
    return symbol.split(":")[0].replace("/", "").lower()


def _normalize_symbol(name):
    name = name.strip().upper()
    if "/" in name: return name
    if name.endswith("_USDT"): name = name[:-5]
    return f"{name}/USDT:USDT"


def _detect_manual_ranges():
    env = dict(os.environ); out = {}
    for key, upper_val in env.items():
        if not key.upper().endswith("_UPPER"): continue
        coin = key[:-6]
        lower_val = env.get(f"{coin}_LOWER") or env.get(f"{coin}_LOWER".upper())
        if lower_val is None: continue
        try:
            u, l = float(str(upper_val).strip()), float(str(lower_val).strip())
        except: continue
        if u <= l: continue
        out[_normalize_symbol(coin)] = {"lower": l, "upper": u}
    return out


_MANUAL_RANGES = _detect_manual_ranges()
ALL_SYMBOLS = list(_MANUAL_RANGES.keys()) if _MANUAL_RANGES else \
              ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT"]
log.info(f"✅ {len(ALL_SYMBOLS)} رمز: {[s.split('/')[0] for s in ALL_SYMBOLS]}")


# ═══════════════════════════════════════════════════════════
# 2) MultiExchange (بيانات من Bybit/OKX — بدون Proxy)
# ═══════════════════════════════════════════════════════════
_EXCHANGE_NAMES = ["bybit", "okx", "kucoin", "kraken", "gate",
                   "bitget", "mexc", "htx", "coinex", "bitmart"]
_EXCHANGE_CLASSES = {n: getattr(ccxt, n) for n in _EXCHANGE_NAMES if hasattr(ccxt, n)}


class MultiExchange:
    def __init__(self):
        self.primary = None; self.fallbacks = []; self._banned = {}
        self._init_all()

    def _is_banned(self, name):
        u = self._banned.get(name, 0); return (_time.time() < u), max(0, int(u-_time.time()))

    def _mark_banned(self, name, sec):
        self._banned[name] = _time.time() + sec
        log.warning(f"🚫 {name} banned {sec}s")

    def _make(self, name):
        if name not in _EXCHANGE_CLASSES: return None
        try:
            opts = {"enableRateLimit": True, "timeout": 20000,
                    "options": {"defaultType": MARKET_TYPE, "adjustForTimeDifference": True}}
            if name == "bybit": opts["options"]["unifiedMargin"] = False
            ex = _EXCHANGE_CLASSES[name](opts)
            ex.load_markets()
            log.info(f"✅ Data: {ex.name} | {len(ex.markets)} markets")
            return ex
        except Exception as e:
            log.warning(f"❌ {name}: {type(e).__name__}: {e}"); return None

    def _init_all(self):
        self.primary = self._make(PRIMARY_EXCHANGE)
        for n in FALLBACK_EXCHANGES:
            if n == PRIMARY_EXCHANGE: continue
            ex = self._make(n)
            if ex: self.fallbacks.append(ex)
        if not self.primary and self.fallbacks: self.primary = self.fallbacks.pop(0)

    def _chain(self):
        out = []
        if self.primary: out.append(self.primary)
        out.extend(self.fallbacks); return out

    async def fetch_ohlcv(self, symbol, tf, limit=200):
        for ex in self._chain():
            banned, _ = self._is_banned(ex.id)
            if banned: continue
            try:
                data = await asyncio.to_thread(ex.fetch_ohlcv, symbol, tf, None, limit)
                if data and len(data) > 0: return data, ex.id
            except ccxt.RateLimitExceeded: self._mark_banned(ex.id, 90); continue
            except ccxt.DDoSProtection: self._mark_banned(ex.id, 300); continue
            except ccxt.ExchangeNotAvailable: self._mark_banned(ex.id, 180); continue
            except ccxt.BadSymbol: continue
            except (ccxt.NetworkError, ccxt.ExchangeError): await asyncio.sleep(0.3); continue
            except Exception: continue
        return None, None

    @property
    def primary_name(self): return self.primary.name if self.primary else "none"
    def ok(self): return self.primary is not None


_exchange = MultiExchange()


# ═══════════════════════════════════════════════════════════
# 3) 🆕 Binance WebSocket (bookTicker)
# ═══════════════════════════════════════════════════════════
class BinanceWS:
    URL = "wss://fstream.binance.com/stream?streams={streams}"

    def __init__(self):
        self.prices = {}          # {"BTCUSDT": {"mark":.., "bid":.., "ask":.., "ts":..}}
        self._task = None
        self._running = False
        self._connected = False
        self._last_msg_ts = 0.0
        self._reconnects = 0
        self._symbols = set()

    def set_symbols(self, symbols):
        self._symbols = {_to_ws_sym(s) for s in symbols}

    def get(self, symbol):
        ws = _to_ws_sym(symbol).upper()
        p = self.prices.get(ws)
        if not p: return None
        if _time.time() - p["ts"] > 15: return None
        return p

    async def start(self, symbols):
        self.set_symbols(symbols)
        if self._running: return
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        while self._running:
            try:
                if not self._symbols:
                    await asyncio.sleep(5); continue
                streams = "/".join(f"{s}@bookTicker" for s in sorted(self._symbols))
                url = self.URL.format(streams=streams)

                kwargs = {"ping_interval": 20, "ping_timeout": 10,
                          "close_timeout": 5, "max_size": 2**20}
                if WS_USE_PROXY and PROXY_URL:
                    kwargs["proxy"] = PROXY_URL

                async with websockets.connect(url, **kwargs) as ws:
                    log.info(f"🔌 WS connected ({len(self._symbols)} streams)")
                    self._connected = True
                    self._reconnects = 0
                    async for raw in ws:
                        self._last_msg_ts = _time.time()
                        try:
                            msg = json.loads(raw)
                            d = msg.get("data", msg)
                            sym = d.get("s")
                            if sym and "b" in d and "a" in d:
                                bid = float(d["b"]); ask = float(d["a"])
                                self.prices[sym] = {
                                    "mark": (bid+ask)/2.0,
                                    "bid": bid, "ask": ask, "ts": _time.time(),
                                }
                        except Exception as e:
                            log.debug(f"WS parse: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"⚠️ WS: {type(e).__name__}: {e}")

            self._connected = False
            self._reconnects += 1
            if self._running:
                delay = min(2 ** min(self._reconnects, 6), 60)
                log.info(f"🔌 WS reconnect in {delay}s")
                await asyncio.sleep(delay)

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try: await self._task
            except: pass

    def status(self):
        return {
            "connected": self._connected,
            "symbols": len(self._symbols),
            "cached": len(self.prices),
            "last_msg_age": round(_time.time() - self._last_msg_ts, 1) if self._last_msg_ts else None,
            "reconnects": self._reconnects,
        }


_ws = BinanceWS()


# ═══════════════════════════════════════════════════════════
# 4) 🆕 FairPrice (WS → REST Binance → Median)
# ═══════════════════════════════════════════════════════════
class FairPrice:
    def __init__(self):
        self.binance_rest = None
        self.others = []
        self._init()

    def _init(self):
        if PROXY_URL:
            px = {"http": PROXY_URL, "https": PROXY_URL}
        else:
            px = None
        try:
            opts = {"enableRateLimit": True, "timeout": 10000,
                    "options": {"defaultType": MARKET_TYPE}}
            if px: opts["proxies"] = px
            self.binance_rest = ccxt.binance(opts)
        except Exception as e:
            log.warning(f"binance_rest init: {e}")
        for name in ["bybit", "okx"]:
            try:
                ex = getattr(ccxt, name)({"enableRateLimit": True, "timeout": 8000})
                self.others.append(ex)
            except: pass
        log.info(f"💰 FairPrice init | rest_binance={'✅' if self.binance_rest else '❌'} "
                 f"others={len(self.others)}")

    async def get(self, symbol):
        """يُعيد (price, source, details)"""
        # 1) WS
        ws = _ws.get(symbol)
        if ws:
            return ws["mark"], "ws", [("ws", ws["mark"]), ("bid", ws["bid"]), ("ask", ws["ask"])]

        # 2) REST Binance (via proxy)
        if self.binance_rest:
            try:
                t = await asyncio.to_thread(self.binance_rest.fetch_ticker, symbol)
                p = float(t["last"])
                return p, "rest_binance", [("binance_rest", p)]
            except Exception:
                pass

        # 3) Median من منصات بديلة
        prices = []
        for ex in self.others:
            try:
                t = await asyncio.to_thread(ex.fetch_ticker, symbol)
                prices.append((ex.id, float(t["last"])))
            except: continue
        if not prices: return None, "none", []
        vals = sorted(p for _, p in prices)
        median = vals[len(vals)//2]
        return median, "median", prices


_fair = FairPrice()


# ═══════════════════════════════════════════════════════════
# 5) SmartStrictness + Sessions
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
SESSIONS = [(0,6,"🌏 آسيا",+8),(6,10,"🌅 آسيا/أوروبا",+2),(10,13,"🇪🇺 أوروبا",0),
            (13,16,"🔥 ذروة",-5),(16,21,"🇺🇸 أمريكا",0),(21,24,"🌙 هدوء",+5)]


class SmartStrictness:
    def __init__(self):
        self.history = deque(maxlen=200); self.results = deque(maxlen=200)
        self.source_stats = defaultdict(lambda: {"wins":0,"losses":0,"expired":0,"sent":0,"filtered":0})
        self._lock = threading.Lock(); self._cached = None; self._ts = 0.0

    def record(self, passed, score, source="unknown"):
        with self._lock: self.history.append((passed, score, _time.time(), source))

    def record_result(self, result, direction, source="unknown"):
        win = result.startswith("win"); expired = result == "expired"
        with self._lock:
            self.results.append({"win":win,"result":result,"source":source,"expired":expired})
            s = self.source_stats[source]
            if win: s["wins"] += 1
            elif expired: s["expired"] += 1
            else: s["losses"] += 1

    def _feedback(self):
        with self._lock:
            results = list(self.results)[-50:]; recent = list(self.history)[-50:]
        non_exp = [r for r in results if not r.get("expired")]
        if len(non_exp) >= 15:
            ratio = sum(1 for r in non_exp if r["win"]) / len(non_exp)
        else:
            if len(recent) < 20: return 0
            ratio = sum(1 for p,_,_,_ in recent if p) / len(recent)
        if ratio > 0.65: return +10
        if ratio > 0.50: return +5
        if ratio < 0.25: return -18
        if ratio < 0.40: return -8
        return 0

    def compute(self, atr_pct=None, adx=None):
        if self._cached and (_time.time() - self._ts) < 60: return self._cached
        atr_pct = atr_pct or 0.5
        if atr_pct < 0.15: base = 50
        elif atr_pct < 0.30: base = 55
        elif atr_pct < 0.60: base = 62
        elif atr_pct < 1.00: base = 70
        else: base = 78
        adx_d = 0
        if adx is not None:
            if adx > 35: adx_d = -3
            elif adx > 25: adx_d = -1
            elif adx < 18: adx_d = +5
            elif adx < 22: adx_d = +2
        final = max(40, min(STRICTNESS_MAX_FINAL, base + adx_d + self._feedback()))
        prof = {"min_score": final,
                "min_vol_ratio": round(max(0.7, (final-30)/30), 2),
                "min_adx": round(max(12, final*0.28), 1),
                "max_atr_pct": round(max(0.35, 1.2 - final/100), 2),
                "vwap_slope_pct": round(0.02 + final/1000, 3),
                "block_against_htf": final >= 55,
                "score_15m_override": min(80, final + 10)}
        self._cached = prof; self._ts = _time.time()
        return prof

    def stats(self):
        with self._lock:
            n = len(self.results)
            if n == 0: return {"n":0,"wr":0,"wins":0,"losses":0,"expired":0}
            wins = sum(1 for r in self.results if r["win"])
            exp = sum(1 for r in self.results if r.get("expired"))
            losses = n - wins - exp
        dec = wins + losses
        return {"n":n, "wr": round(wins/dec*100,1) if dec else 0,
                "wins":wins, "losses":losses, "expired":exp}

    def reset(self):
        with self._lock: self.history.clear(); self.results.clear(); self.source_stats.clear()
        self._cached = None


_smart = SmartStrictness()


def get_session_info():
    h = datetime.now(timezone.utc).hour
    for s, e, n, d in SESSIONS:
        if s <= h < e: return d, n
    return 0, "❓"


def get_thresholds(atr_pct=None, adx=None, tf="15m"):
    prof = dict(_smart.compute(atr_pct, adx)) if STRICTNESS == "auto" \
           else dict(STATIC_PROFILES.get(STRICTNESS, STATIC_PROFILES["balanced"]))
    tfp = TF_PROFILES.get(tf, TF_PROFILES["15m"])
    prof["min_score"] *= tfp["score_mult"]
    prof["min_vol_ratio"] *= tfp["vol_mult"]
    prof["max_atr_pct"] = tfp["atr_max"]
    if SESSION_ADJUST_ENABLED:
        sd, _ = get_session_info(); prof["min_score"] += sd
    prof.update(STRICTNESS_OVERRIDE)
    return prof


# ═══════════════════════════════════════════════════════════
# 6) أدوات
# ═══════════════════════════════════════════════════════════
def short(s): return s.split("/")[0].split(":")[0].upper()
def syria_now_str():
    return datetime.now(SYRIA_TZ).strftime("%Y-%m-%d %H:%M:%S")
def fmt_price(v):
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
    d = s.diff(); g = d.clip(lower=0); l = -d.clip(upper=0)
    ag = g.ewm(alpha=1/p, adjust=False).mean(); al = l.ewm(alpha=1/p, adjust=False).mean()
    return 100 - 100/(1 + ag/al.replace(0,1e-9))

def calc_atr(df, p=14):
    tr = pd.concat([df["h"]-df["l"], (df["h"]-df["c"].shift()).abs(),
                    (df["l"]-df["c"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()

def calc_adx(df, p=14):
    h, l, c = df["h"], df["l"], df["c"]
    pdm = h.diff(); ndm = -l.diff()
    pdm = pdm.where((pdm>ndm)&(pdm>0), 0); ndm = ndm.where((ndm>pdm)&(ndm>0), 0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100*pdm.ewm(alpha=1/p, adjust=False).mean()/atr.replace(0,1e-9)
    ndi = 100*ndm.ewm(alpha=1/p, adjust=False).mean()/atr.replace(0,1e-9)
    dx = 100*(pdi-ndi).abs()/(pdi+ndi).replace(0,1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean()

def calc_macd(s, f=12, sl=26, sig=9):
    ef = s.ewm(span=f, adjust=False).mean(); es = s.ewm(span=sl, adjust=False).mean()
    dif = ef - es; dea = dif.ewm(span=sig, adjust=False).mean()
    return dif, dea, dif - dea

def calc_vwap_session(df, session="day"):
    try:
        ts = pd.to_datetime(df["ts"], unit="ms", utc=True)
        skey = ts.dt.floor("D") if session == "day" else ts.dt.to_period("W").astype(str)
        tp = (df["h"]+df["l"]+df["c"])/3; pv = tp*df["v"]
        return pv.groupby(skey).cumsum() / df["v"].groupby(skey).cumsum().replace(0,1e-9)
    except:
        tp = (df["h"]+df["l"]+df["c"])/3
        return (tp*df["v"]).cumsum()/df["v"].cumsum()

def calc_bollinger(df, p=20, m=2):
    c = df["c"]; ma = c.rolling(p).mean(); sd = c.rolling(p).std()
    up = ma + m*sd; lo = ma - m*sd
    return {"ma":ma, "upper":up, "lower":lo, "width":(up-lo)/ma*100}


def classify_market(df, curr=-2):
    if len(df) < 60: return "UNKNOWN"
    try:
        adx = float(calc_adx(df).iloc[curr])
        bb_w = float(calc_bollinger(df)["width"].iloc[curr])
        e50 = calc_ema(df["c"], 50)
        slope = (float(e50.iloc[curr]) - float(e50.iloc[curr-5])) / float(e50.iloc[curr-5]) * 100
        atr = float(calc_atr(df).iloc[curr]); price = float(df["c"].iloc[curr])
        atr_pct = (atr/price)*100 if price else 0
        if adx > 25 and abs(slope) > 0.15: return "TREND_UP" if slope > 0 else "TREND_DOWN"
        if bb_w < 2.0 and adx < 18: return "RANGE"
        if atr_pct > 1.2: return "VOLATILE"
        return "RANGE" if adx < 20 else ("TREND_UP" if slope > 0 else "TREND_DOWN")
    except: return "UNKNOWN"


def compute_levels(df, direction, curr=-2, tf="15m"):
    try:
        price = float(df["c"].iloc[curr]); atr = float(calc_atr(df).iloc[curr])
        ema_slow = float(calc_ema(df["c"], EMA_SLOW).iloc[curr])
        min_sl = MIN_SL_PCT.get(tf, 0.50)
        lb = min(20, len(df)+curr-1)
        if lb < 2: return None
        rh = float(df["h"].iloc[curr-lb:curr].max()); rl = float(df["l"].iloc[curr-lb:curr].min())
        if direction == "bullish":
            sl = max(price - atr*1.5, min(rl, ema_slow)*0.998)
            min_d = price * (min_sl/100)
            if (price - sl) < min_d: sl = price - min_d
            r = price - sl
            return {"entry":price, "stop_loss":sl, "sl_pct":round((price-sl)/price*100,2),
                    "tp1":price+r, "tp2":price+r*2, "tp3":price+r*3, "atr":atr}
        else:
            sl = min(price + atr*1.5, max(rh, ema_slow)*1.002)
            min_d = price * (min_sl/100)
            if (sl - price) < min_d: sl = price + min_d
            r = sl - price
            return {"entry":price, "stop_loss":sl, "sl_pct":round((sl-price)/price*100,2),
                    "tp1":price-r, "tp2":price-r*2, "tp3":price-r*3, "atr":atr}
    except Exception as e:
        log.warning(f"compute_levels: {e}"); return None


# ═══════════════════════════════════════════════════════════
# 8) كاشات
# ═══════════════════════════════════════════════════════════
_ohlcv_cache, _crossover_cache = {}, {}
_recent_signals, _last_signal_time = {}, {}
_pending_signals, _divergence_cache, _bb_cache, _sr_cache = {}, {}, {}, {}
_last_signals = deque(maxlen=LAST_SIGNALS_MAX)
_filter_stats = defaultdict(int)


async def fetch_ohlcv_cached(symbol, tf, limit=150):
    key = (symbol, tf, limit); now = _time.time()
    c = _ohlcv_cache.get(key)
    if c and (now - c["ts"]) < OHLCV_CACHE_SECONDS: return c["data"]
    data, ex_id = await _exchange.fetch_ohlcv(symbol, tf, limit)
    if data: _ohlcv_cache[key] = {"data":data, "ts":now, "ex":ex_id}
    return data


def clear_caches():
    for c in (_ohlcv_cache, _crossover_cache, _recent_signals,
              _divergence_cache, _bb_cache, _sr_cache): c.clear()


# ═══════════════════════════════════════════════════════════
# 9) HTF / MTF / VWAP / Conflict
# ═══════════════════════════════════════════════════════════
async def get_htf_trend(symbol):
    if not HTF_ENABLED: return "NEUTRAL"
    data = await fetch_ohlcv_cached(symbol, HTF_TIMEFRAME, 250)
    if not data or len(data) < 200: return "NEUTRAL"
    df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
    e50 = float(calc_ema(df["c"], 50).iloc[-1]); e200 = float(calc_ema(df["c"], 200).iloc[-1])
    p = float(df["c"].iloc[-1])
    if p > e50 > e200: return "UP"
    if p < e50 < e200: return "DOWN"
    return "NEUTRAL"


async def get_mtf_alignment(symbol, direction):
    if not MTF_ENABLED: return 0, "—"
    score = 0
    for tf in ["5m","15m","1h"]:
        try:
            data = await fetch_ohlcv_cached(symbol, tf, 60)
            if not data or len(data) < 30: continue
            df = pd.DataFrame(data, columns=["ts","o","h","l","c","v"])
            ef = float(calc_ema(df["c"], EMA_FAST).iloc[-2])
            es = float(calc_ema(df["c"], EMA_SLOW).iloc[-2])
            if (direction=="bullish" and ef>es) or (direction=="bearish" and ef<es): score += 1
        except: continue
    if score >= 3: return +8, "توافق كامل 3/3 🌟"
    if score == 2: return +3, "توافق 2/3 ✅"
    if score == 1: return -5, "توافق 1/3 ⚠️"
    return -15, "تعارض 0/3 ❌"


def check_vwap_slope(df, direction, th, tf="15m"):
    min_pct = th.get("vwap_slope_pct", 0.05)
    if len(df) < VWAP_SLOPE_LOOKBACK+1: return True, 0.0
    session = "week" if tf == "1h" else "day"
    vwap = calc_vwap_session(df, session)
    vn, vp = float(vwap.iloc[-1]), float(vwap.iloc[-VWAP_SLOPE_LOOKBACK-1])
    if vp == 0: return True, 0.0
    slope = (vn-vp)/vp*100
    if direction == "bullish" and slope < -min_pct: return False, slope
    if direction == "bearish" and slope > min_pct: return False, slope
    return True, slope


def has_conflicting_signal(symbol, direction):
    prev = _recent_signals.get(symbol)
    if not prev: return False
    return (_time.time()-prev[1] < CONFLICT_WINDOW_SEC) and prev[0] != direction


def record_signal(symbol, direction): _recent_signals[symbol] = (direction, _time.time())


def check_symbol_cooldown(symbol):
    if SYMBOL_COOLDOWN_SEC <= 0: return True, 0
    now = _time.time(); last = _last_signal_time.get(symbol, 0)
    if now - last < SYMBOL_COOLDOWN_SEC:
        return False, int(SYMBOL_COOLDOWN_SEC - (now-last))
    return True, 0


def mark_symbol_sent(symbol): _last_signal_time[symbol] = _time.time()


# ═══════════════════════════════════════════════════════════
# 10) Score
# ═══════════════════════════════════════════════════════════
def compute_score(df, direction, curr=-2):
    close = df["c"]; vol = df["v"]; price = float(close.iloc[curr])
    try:
        s = max(0, len(df)+curr-20); vma = float(vol.iloc[s:len(df)+curr].mean())
        vr = float(vol.iloc[curr])/vma if vma > 0 else 0
    except: vr = 0
    v_pts = 30 if vr>=4 else 25 if vr>=2.5 else 18 if vr>=1.5 else 10 if vr>=1 else 5 if vr>=0.5 else 0
    try: adx = float(calc_adx(df).iloc[curr])
    except: adx = 0
    a_pts = 20 if adx>=35 else 18 if adx>=25 else 12 if adx>=20 else 5 if adx>=15 else 0
    try:
        ef = float(calc_ema(close, EMA_FAST).iloc[curr]); es = float(calc_ema(close, EMA_SLOW).iloc[curr])
        gap = abs(ef-es)/es*100 if es else 0
    except: gap = 0
    g_pts = 15 if gap>=0.3 else 10 if gap>=0.1 else 6 if gap>=0.05 else 3 if gap>=0.02 else 0
    try:
        _,_,hist = calc_macd(close)
        hn, hp = float(hist.iloc[curr]), float(hist.iloc[curr-1])
        m_pts = 10 if (hn>0 and hn>hp) else 5 if hn>0 else 7 if (hn<0 and hn>hp) else 5
    except: m_pts = 0
    try: rsi = float(calc_rsi(close).iloc[curr])
    except: rsi = 50
    if direction == "bullish":
        r_pts = 10 if 55<=rsi<=70 else 5 if rsi>70 else 7 if rsi>=45 else 3 if rsi>=30 else 0
    else:
        r_pts = 10 if 30<=rsi<=45 else 5 if rsi<30 else 7 if rsi<=55 else 3 if rsi<=70 else 0
    try: e50 = float(calc_ema(close, 50).iloc[curr])
    except: e50 = price
    above = price > e50
    h_pts = 10 if ((direction=="bullish" and above) or (direction=="bearish" and not above)) else 0
    try:
        atr = float(calc_atr(df).iloc[curr]); atr_pct = (atr/price)*100 if price else 0
    except: atr_pct = 0
    t_pts = 5 if atr_pct>=0.30 else 3 if atr_pct>=0.15 else 1
    total = v_pts + a_pts + g_pts + m_pts + r_pts + h_pts + t_pts
    grade = ("🌟 ذهبية" if total>=80 else "⭐ قوية" if total>=65 else
             "✅ جيدة" if total>=55 else "🟡 متوسطة" if total>=45 else "⚪ ضعيفة")
    return {"score":total, "grade":grade, "adx":round(adx,1), "adx_pts":a_pts,
            "rsi":round(rsi,1), "rsi_pts":r_pts, "macd_pts":m_pts,
            "vol_ratio":round(vr,2), "vol_pts":v_pts, "atr_pct":round(atr_pct,2),
            "atr_pts":t_pts, "htf_pts":h_pts, "gap_pct":round(gap,3), "gap_pts":g_pts}


# ═══════════════════════════════════════════════════════════
# 11) الفلتر
# ═══════════════════════════════════════════════════════════
async def passes_filter(cross, htf_trend=None):
    s = cross.get("support", {})
    if not s: return False, "لا مؤشرات", "other"
    tf = cross["timeframe"]
    vol = s.get("vol_ratio",0); adx = s.get("adx",0); atr = s.get("atr_pct",0)
    base = cross.get("base_score", s.get("score",0))
    gap_pct = s.get("gap_pct",0); source = cross.get("source","ema")
    direction = cross["direction"]

    if SYMBOL_COOLDOWN_SEC > 0:
        ok, rem = check_symbol_cooldown(cross["symbol"])
        if not ok: return False, f"cooldown {rem}s", "cooldown"
    if gap_pct < MIN_EMA_GAP_PCT.get(tf, 0.02) and source == "ema":
        return False, f"gap {gap_pct:.3f}%", "ema_gap"
    if atr < MIN_ATR_PCT.get(tf, 0.05):
        return False, f"ATR {atr}%", "atr_low"

    mtf_b, mtf_d = await get_mtf_alignment(cross["symbol"], direction)
    cross["mtf"] = mtf_d; cross["mtf_bonus"] = mtf_b
    score = base + mtf_b; s["score"] = score; cross["final_score"] = score

    if mtf_b <= -15: return False, mtf_d, "mtf"
    if score < ABS_MIN_SCORE: return False, f"Score {score} < {ABS_MIN_SCORE}", "score_abs"

    market = cross.get("market") or "UNKNOWN"
    th = get_thresholds(atr_pct=atr, adx=adx, tf=tf)
    min_score = th["min_score"]
    if tf == "15m": min_score = max(min_score, th.get("score_15m_override", min_score))
    if score < min_score: return False, f"Score {score} < {min_score:.0f}", "score"
    if vol < th["min_vol_ratio"]: return False, f"Vol {vol} < {th['min_vol_ratio']}", "vol"
    if adx < th["min_adx"]: return False, f"ADX {adx} < {th['min_adx']}", "adx"
    if atr > th["max_atr_pct"]: return False, f"ATR {atr}%", "atr"

    if source == "bb_squeeze":
        if adx < MIN_ADX_FOR_BB: return False, f"ADX {adx} < {MIN_ADX_FOR_BB}", "adx"
        if vol < BB_MIN_VOL: return False, f"BB Vol {vol} < {BB_MIN_VOL}", "vol"
        if market == "RANGE" and adx < BB_MIN_ADX_IN_RANGE: return False, "BB range", "range"
        if BB_CONFIRM_ENABLED and not ((mtf_b >= 8) or (adx > 22) or (vol > 1.5)):
            return False, "BB no confirm", "bb_confirm"

    if source == "ema":
        if market == "TREND_DOWN" and direction == "bullish" and mtf_b < 5:
            return False, "trend ضد", "trend"
        elif market == "TREND_UP" and direction == "bearish" and mtf_b < 5:
            return False, "trend ضد", "trend"
    if source == "rsi_div":
        if market == "TREND_DOWN" and direction == "bullish" and mtf_b < 3:
            return False, "div ضد", "trend"
        elif market == "TREND_UP" and direction == "bearish" and mtf_b < 3:
            return False, "div ضد", "trend"

    if market == "RANGE" and source == "ema" and cross.get("alert_type") == "confirmed":
        return False, "range وهمي", "range"

    if th["block_against_htf"] and cross.get("alert_type") != "pre" and htf_trend:
        if direction == "bullish" and htf_trend == "DOWN": return False, "HTF ضد", "htf"
        if direction == "bearish" and htf_trend == "UP": return False, "HTF ضد", "htf"

    df = cross.get("_df")
    if df is not None:
        ok, slope = check_vwap_slope(df, direction, th, tf)
        if not ok: return False, f"VWAP {slope:+.2f}%", "vwap"

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
    if not data or len(data) < EMA_SLOW+5: return None
    return pd.DataFrame(data, columns=["ts","o","h","l","c","v"])


async def _make_cross(symbol, tf, df, direction, alert_type, strength, curr,
                     source="ema", extra=None):
    cf = float(df["ef"].iloc[curr]); cs = float(df["es"].iloc[curr])
    cts = int(df["ts"].iloc[curr])
    levels = compute_levels(df, direction, curr, tf=tf)
    market = classify_market(df)
    support = compute_score(df, direction, curr)
    out = {"symbol":symbol, "timeframe":tf, "direction":direction,
           "ema_fast":round(cf,8), "ema_slow":round(cs,8),
           "price":round(float(df["c"].iloc[curr]),8), "candle_ts":cts,
           "gap_pct":round(abs(cf-cs)/cs*100,3) if cs else 0,
           "strength":strength, "alert_type":alert_type, "support":support,
           "base_score":support["score"], "final_score":support["score"],
           "_df":df, "exchange":_exchange.primary_name, "levels":levels,
           "source":source, "market":market, "created_at":_time.time()}
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
    _crossover_cache[key] = {"candle_ts":cts, "direction":d}
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
    key = (symbol, tf, "live"); last = _crossover_cache.get(key)
    if last and last.get("candle_ts") == cts: return None
    _crossover_cache[key] = {"candle_ts":cts, "direction":d}
    return await _make_cross(symbol, tf, df, d, "live", "⚡ مبدئي", curr)


_TF_MS = {"5m":300000, "15m":900000, "1h":3600000}
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
    if abs(cf-cs) < 1e-9: return None
    d = "bullish" if cf < cs else "bearish"; cts = int(df["ts"].iloc[curr])
    key = (symbol, tf, "pre"); last = _crossover_cache.get(key)
    if last and cts - last.get("candle_ts",0) < _TF_MS.get(tf, 900000)*PRE_CROSS_COOLDOWN:
        return None
    _crossover_cache[key] = {"candle_ts":cts, "direction":d}
    return await _make_cross(symbol, tf, df, d, "pre", "🔔 تقارب وشيك", curr)


async def detect_rsi_divergence(symbol, tf):
    if not ENABLE_RSI_DIVERGENCE: return None
    df = await _df_for(symbol, tf, extra=RSI_DIV_LOOKBACK+10)
    if df is None or len(df) < RSI_DIV_LOOKBACK+5: return None
    rsi = calc_rsi(df["c"]); close = df["c"]; curr = -2
    key = (symbol, tf, "div"); last = _divergence_cache.get(key)
    if last and (_time.time()-last) < RSI_DIV_COOLDOWN_MIN*60: return None
    win = RSI_DIV_LOOKBACK; best = None
    for i in range(3, win//2):
        for j in range(i+3, win):
            idx1 = curr - i; idx2 = curr - j
            if idx1 >= 0 or idx2 >= 0: continue
            c1 = float(close.iloc[idx1]); c2 = float(close.iloc[idx2])
            r1 = float(rsi.iloc[idx1]); r2 = float(rsi.iloc[idx2])
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
    cross["div_info"] = {"r1":round(best["r1"],1), "r2":round(best["r2"],1)}
    return cross


async def detect_bb_squeeze_breakout(symbol, tf):
    if not ENABLE_BB_SQUEEZE: return None
    df = await _df_for(symbol, tf, extra=80)
    if df is None or len(df) < 60: return None
    bb = calc_bollinger(df); curr = -2
    key = (symbol, tf, "bb"); last = _bb_cache.get(key)
    if last and (_time.time()-last) < BB_SQUEEZE_COOLDOWN_MIN*60: return None
    hw = bb["width"].iloc[curr-50:curr].dropna()
    if len(hw) < 20: return None
    if float(bb["width"].iloc[curr]) > hw.quantile(BB_SQUEEZE_PCTL): return None
    price = float(df["c"].iloc[curr]); up = float(bb["upper"].iloc[curr]); lo = float(bb["lower"].iloc[curr])
    d = "bullish" if price > up else "bearish" if price < lo else None
    if not d: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _bb_cache[key] = _time.time()
    cross = await _make_cross(symbol, tf, df, d, "confirmed", "💥 كسر BB", curr, source="bb_squeeze")
    cross["bb_info"] = {"width":round(float(bb["width"].iloc[curr]),2)}
    return cross


async def detect_sr_breakout(symbol, tf):
    if not ENABLE_SR_BREAKOUT: return None
    df = await _df_for(symbol, tf, extra=SR_BREAKOUT_LOOKBACK+10)
    if df is None or len(df) < SR_BREAKOUT_LOOKBACK+5: return None
    curr = -2; key = (symbol, tf, "sr"); last = _sr_cache.get(key)
    if last and (_time.time()-last) < SR_BREAKOUT_COOLDOWN_MIN*60: return None
    w = df.iloc[curr-SR_BREAKOUT_LOOKBACK:curr]
    high = float(w["h"].max()); low = float(w["l"].min()); cp = float(w["c"].iloc[-1])
    pivot = (high+low+cp)/3; r1 = 2*pivot-low; s1 = 2*pivot-high
    pn = float(df["c"].iloc[curr]); pp = float(df["c"].iloc[curr-1])
    d = "bullish" if (pn > r1 and pp <= r1) else "bearish" if (pn < s1 and pp >= s1) else None
    if not d: return None
    df["ef"] = calc_ema(df["c"], EMA_FAST); df["es"] = calc_ema(df["c"], EMA_SLOW)
    _sr_cache[key] = _time.time()
    return await _make_cross(symbol, tf, df, d, "confirmed", "🎯 كسر S/R", curr, source="sr_breakout")


# ═══════════════════════════════════════════════════════════
# 13) BinanceExecutor (SL/TP من سعر التنفيذ الفعلي)
# ═══════════════════════════════════════════════════════════
class BinanceExecutor:
    def __init__(self):
        self.ex = None; self.ready = False; self._init()

    def _init(self):
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            log.warning("⚠️ لا مفاتيح Binance — التنفيذ معطّل"); return
        try:
            opts = {"apiKey": BINANCE_API_KEY, "secret": BINANCE_API_SECRET,
                    "enableRateLimit": True, "timeout": 15000,
                    "options": {"defaultType":"swap", "adjustForTimeDifference":True}}
            if PROXY_URL: opts["proxies"] = {"http":PROXY_URL, "https":PROXY_URL}
            self.ex = ccxt.binance(opts)
            self.ex.load_markets()
            self.ready = True
            log.info("✅ Binance Executor ready")
        except Exception as e:
            log.error(f"❌ Binance Executor: {type(e).__name__}: {e}")
            self.ex = None; self.ready = False

    async def set_leverage(self, symbol):
        try:
            await asyncio.to_thread(self.ex.set_leverage, LEVERAGE, symbol,
                                    {"marginMode":"isolated"})
            return True
        except Exception as e:
            log.warning(f"set_leverage {symbol}: {e}"); return False

    async def get_balance(self):
        try:
            b = await asyncio.to_thread(self.ex.fetch_balance)
            u = b.get("USDT", {})
            return {"free":float(u.get("free",0)), "used":float(u.get("used",0)),
                    "total":float(u.get("total",0))}
        except Exception as e:
            log.error(f"balance: {e}"); return {"free":0,"used":0,"total":0}

    async def get_positions(self):
        try:
            pos = await asyncio.to_thread(self.ex.fetch_positions)
            return [p for p in pos if float(p.get("contracts") or 0) > 0]
        except Exception as e:
            log.error(f"positions: {e}"); return []

    async def open_position(self, symbol, direction, ref_price, sl_price, tp_price):
        """
        تُحسب SL/TP كنِسَب من ref_price، ثم تُطبَّق على سعر التنفيذ الفعلي.
        """
        try:
            # نِسَب من السعر المرجعي (سعر مصدر البيانات)
            if direction == "bullish":
                sl_pct = abs(ref_price - sl_price) / ref_price
                tp_pct = abs(tp_price - ref_price) / ref_price
            else:
                sl_pct = abs(sl_price - ref_price) / ref_price
                tp_pct = abs(ref_price - tp_price) / ref_price

            await self.set_leverage(symbol)

            # احصل على سعر Binance الحالي
            ticker = await asyncio.to_thread(self.ex.fetch_ticker, symbol)
            price = float(ticker["last"])

            # الكمية من notional
            notional = TRADE_MARGIN_USD * LEVERAGE
            qty_raw = notional / price
            qty = float(self.ex.amount_to_precision(symbol, qty_raw))

            # التحقق من الحد الأدنى
            mkt = self.ex.market(symbol)
            min_amt = (mkt.get("limits",{}).get("amount",{}) or {}).get("min") or 0
            min_cost = (mkt.get("limits",{}).get("cost",{}) or {}).get("min") or 0
            if qty < min_amt:
                return {"ok":False, "err":f"qty {qty} < min {min_amt}"}
            if qty * price < min_cost:
                return {"ok":False, "err":f"notional {qty*price:.2f} < min {min_cost}"}

            side = "buy" if direction == "bullish" else "sell"
            close_side = "sell" if side == "buy" else "buy"

            # Market entry
            entry_order = await asyncio.to_thread(
                self.ex.create_order, symbol, "market", side, qty)
            fill = float(entry_order.get("average") or entry_order.get("price") or price)

            # احسب SL/TP من fill الفعلي
            if direction == "bullish":
                actual_sl = fill * (1 - sl_pct)
                actual_tp = fill * (1 + tp_pct)
            else:
                actual_sl = fill * (1 + sl_pct)
                actual_tp = fill * (1 - tp_pct)

            # SL
            sl_p = self.ex.price_to_precision(symbol, actual_sl)
            sl_ok = False
            try:
                await asyncio.to_thread(
                    self.ex.create_order, symbol, "STOP_MARKET", close_side, None, None,
                    {"stopPrice":sl_p, "closePosition":True, "workingType":"MARK_PRICE"})
                sl_ok = True
            except Exception as e:
                log.error(f"SL order {symbol}: {e}")

            # TP
            tp_p = self.ex.price_to_precision(symbol, actual_tp)
            tp_ok = False
            try:
                await asyncio.to_thread(
                    self.ex.create_order, symbol, "TAKE_PROFIT_MARKET", close_side, None, None,
                    {"stopPrice":tp_p, "closePosition":True, "workingType":"MARK_PRICE"})
                tp_ok = True
            except Exception as e:
                log.error(f"TP order {symbol}: {e}")

            return {"ok":True, "symbol":symbol, "side":side, "qty":qty,
                    "entry":fill, "sl":actual_sl, "tp":actual_tp,
                    "sl_ok":sl_ok, "tp_ok":tp_ok,
                    "order_id":entry_order.get("id"), "notional":qty*fill,
                    "sl_pct":round(sl_pct*100,3), "tp_pct":round(tp_pct*100,3)}
        except Exception as e:
            log.exception(f"open_position {symbol}: {e}")
            return {"ok":False, "err":str(e)}

    async def close_position(self, symbol):
        try:
            positions = await self.get_positions()
            pos = next((p for p in positions if p["symbol"] == symbol), None)
            if not pos: return {"ok":False, "err":"no position"}
            side = "sell" if pos["side"] == "long" else "buy"
            qty = abs(float(pos["contracts"]))
            try: await asyncio.to_thread(self.ex.cancel_all_orders, symbol)
            except: pass
            order = await asyncio.to_thread(
                self.ex.create_order, symbol, "market", side, qty, None, {"reduceOnly":True})
            return {"ok":True, "order":order}
        except Exception as e:
            log.error(f"close {symbol}: {e}")
            return {"ok":False, "err":str(e)}


_executor = BinanceExecutor()


# ═══════════════════════════════════════════════════════════
# 14) Risk Manager
# ═══════════════════════════════════════════════════════════
_open_positions = {}
_daily_pnl = {"date": "", "realized": 0.0, "trades": 0}
_kill_switch = {"active": False, "reason": "", "until": 0}


def _check_daily_reset():
    today = datetime.now(SYRIA_TZ).strftime("%Y-%m-%d")
    if _daily_pnl["date"] != today:
        _daily_pnl["date"] = today
        _daily_pnl["realized"] = 0.0
        _daily_pnl["trades"] = 0


def can_open_new_trade():
    _check_daily_reset()
    if _kill_switch["active"] and _time.time() < _kill_switch["until"]:
        rem = int(_kill_switch["until"] - _time.time())
        return False, f"Kill ({rem}s): {_kill_switch['reason']}"
    if _kill_switch["active"]: _kill_switch["active"] = False
    if len(_open_positions) >= MAX_CONCURRENT_POSITIONS:
        return False, f"عدد {len(_open_positions)}/{MAX_CONCURRENT_POSITIONS}"
    if _daily_pnl["realized"] <= -MAX_DAILY_LOSS_USD:
        _kill_switch["active"] = True
        _kill_switch["reason"] = f"خسارة يومية {_daily_pnl['realized']:.0f}$"
        _kill_switch["until"] = _time.time() + 86400
        return False, _kill_switch["reason"]
    return True, ""


# ═══════════════════════════════════════════════════════════
# 15) التنفيذ
# ═══════════════════════════════════════════════════════════
async def execute_signal(cross, bot):
    if not EXECUTION_ENABLED or not _executor.ready: return None
    if cross["alert_type"] != "confirmed": return None
    if cross["symbol"] in _open_positions: return None

    ok, reason = can_open_new_trade()
    if not ok:
        log.warning(f"🚫 {cross['symbol']}: {reason}")
        try:
            await bot.send_message(chat_id=CHAT_ID,
                text=f"🚫 <b>تخطي {short(cross['symbol'])}</b>\n{reason}",
                parse_mode="HTML")
        except: pass
        return None

    levels = cross.get("levels")
    if not levels: return None

    fair, src, details = await _fair.get(cross["symbol"])
    if not fair:
        log.warning(f"❌ لا سعر عادل {cross['symbol']}")
        try:
            await bot.send_message(chat_id=CHAT_ID,
                text=f"❌ <b>تخطي {short(cross['symbol'])}</b>\nلا سعر عادل من أي مصدر",
                parse_mode="HTML")
        except: pass
        return None

    entry_ref = levels["entry"]
    div = abs(fair - entry_ref) / entry_ref * 100
    if div > MAX_PRICE_DIVERGENCE_PCT:
        log.warning(f"⚠️ انحراف {cross['symbol']} {div:.3f}%")
        try:
            await bot.send_message(chat_id=CHAT_ID,
                text=f"⚠️ <b>تخطي {short(cross['symbol'])}</b>\n"
                     f"انحراف {div:.2f}% > {MAX_PRICE_DIVERGENCE_PCT}%\n"
                     f"• مرجع: {fmt_price(entry_ref)}\n"
                     f"• عادل ({src}): {fmt_price(fair)}",
                parse_mode="HTML")
        except: pass
        return None

    log.info(f"🚀 تنفيذ {cross['symbol']} | src={src} fair={fair:.6f} "
             f"ref={entry_ref:.6f} div={div:.3f}%")

    result = await _executor.open_position(
        cross["symbol"], cross["direction"],
        entry_ref, levels["stop_loss"], levels["tp2"])

    if not result.get("ok"):
        log.error(f"❌ فشل {cross['symbol']}: {result.get('err')}")
        try:
            await bot.send_message(chat_id=CHAT_ID,
                text=f"❌ <b>فشل التنفيذ</b>\n{short(cross['symbol'])}\n{result.get('err')}",
                parse_mode="HTML")
        except: pass
        return None

    _open_positions[cross["symbol"]] = {
        "cross":cross, "levels":levels, "exec":result,
        "opened_at":_time.time(), "source":cross.get("source","ema"),
        "fair_price":fair, "fair_source":src, "divergence_pct":round(div,3),
    }
    _daily_pnl["trades"] += 1
    mark_symbol_sent(cross["symbol"])
    register_signal_for_tracking(cross)

    msg = (f"🟢 <b>تنفيذ ناجح — {short(cross['symbol'])}</b>\n"
           f"🇸🇾 {syria_now_str()}\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"📊 {cross['direction']} | {cross.get('source','ema')}\n"
           f"⚙️ {LEVERAGE}x | Margin ${TRADE_MARGIN_USD}\n"
           f"📦 Qty: <b>{result['qty']}</b> | Notional: <b>${result['notional']:.0f}</b>\n"
           f"💰 دخول فعلي: <b>{fmt_price(result['entry'])}</b>\n"
           f"🛑 SL: <b>{fmt_price(result['sl'])}</b> (-{result['sl_pct']}%) "
           f"{'✅' if result['sl_ok'] else '⚠️'}\n"
           f"🎯 TP (1:2): <b>{fmt_price(result['tp'])}</b> (+{result['tp_pct']}%) "
           f"{'✅' if result['tp_ok'] else '⚠️'}\n"
           f"📉 انحراف: <b>{div:.3f}%</b> | 📡 سعر: {src}\n"
           f"🏦 نشطة: <b>{len(_open_positions)}/{MAX_CONCURRENT_POSITIONS}</b>")
    try: await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
    except: pass
    return result


async def sync_positions(bot):
    if not _executor.ready or not _open_positions: return
    try:
        positions = await _executor.get_positions()
        open_syms = {p["symbol"] for p in positions}
        for sym in list(_open_positions.keys()):
            if sym not in open_syms:
                info = _open_positions.pop(sym)
                pnl = 0.0
                try:
                    trades = await asyncio.to_thread(
                        _executor.ex.fetch_my_trades, sym, None, 20)
                    pnl = sum(float(t.get("info",{}).get("realizedPnl",0)) for t in trades[-5:])
                except: pass
                _daily_pnl["realized"] += pnl
                emoji = "✅" if pnl >= 0 else "❌"
                msg = (f"{emoji} <b>صفقة مغلقة — {short(sym)}</b>\n"
                       f"💰 PnL: <b>{pnl:+.2f} USDT</b>\n"
                       f"📉 اليوم: <b>{_daily_pnl['realized']:+.2f}$</b>\n"
                       f"🏦 نشطة: <b>{len(_open_positions)}/{MAX_CONCURRENT_POSITIONS}</b>")
                try: await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                except: pass
    except Exception as e:
        log.error(f"sync: {e}")


# ═══════════════════════════════════════════════════════════
# 16) تتبّع الإشارات
# ═══════════════════════════════════════════════════════════
def register_signal_for_tracking(cross):
    levels = cross.get("levels")
    if not levels: return
    sid = f"{cross['symbol']}_{cross['timeframe']}_{int(_time.time())}"
    _pending_signals[sid] = {
        "cross":cross, "levels":levels, "entry_price":levels["entry"],
        "entry_ts":_time.time(), "direction":cross["direction"],
        "source":cross.get("source","ema"), "checked":False, "result":None,
    }


async def evaluate_pending():
    if not _pending_signals: return
    now = _time.time()
    for sid, sig in list(_pending_signals.items()):
        if sig["checked"]: continue
        age_min = (now - sig["entry_ts"]) / 60
        if age_min > SIGNAL_TRACK_MAX_MIN:
            sig["checked"] = True; sig["result"] = "expired"
            _smart.record_result("expired", sig["direction"], sig.get("source","unknown"))
            _record_history(sig, "expired", sig["levels"]["entry"], age_min); continue
        try:
            # استخدم WS إن أمكن
            ws = _ws.get(sig["cross"]["symbol"])
            price = ws["mark"] if ws else None
            if not price:
                data, _ = await _exchange.fetch_ohlcv(sig["cross"]["symbol"], "1m", 3)
                if not data: continue
                price = float(data[-1][4])
            lv = sig["levels"]; new_r = None
            if sig["direction"] == "bullish":
                if price <= lv["stop_loss"]: sig["checked"]=True; new_r="loss_sl"
                elif price >= lv["tp2"]: sig["checked"]=True; new_r="win_tp2"
                elif price >= lv["tp1"] and not sig.get("result"):
                    new_r="win_tp1"; sig["result"]="win_tp1"
            else:
                if price >= lv["stop_loss"]: sig["checked"]=True; new_r="loss_sl"
                elif price <= lv["tp2"]: sig["checked"]=True; new_r="win_tp2"
                elif price <= lv["tp1"] and not sig.get("result"):
                    new_r="win_tp1"; sig["result"]="win_tp1"
            if new_r and sig["checked"]:
                _smart.record_result(sig["result"], sig["direction"], sig.get("source","unknown"))
                _record_history(sig, sig["result"], price, age_min)
        except Exception as e:
            log.warning(f"track {sid}: {e}")
    cutoff = now - 3600
    for sid in [k for k,v in _pending_signals.items() if v.get("checked") and v["entry_ts"] < cutoff]:
        _pending_signals.pop(sid, None)


def _record_history(sig, result, price, age_min):
    cross = sig["cross"]; entry = sig["levels"]["entry"]
    bull = sig["direction"] == "bullish"
    pnl = ((price-entry)/entry*100) * (1 if bull else -1)
    _last_signals.append({"symbol":cross["symbol"], "timeframe":cross["timeframe"],
        "direction":cross["direction"], "source":cross.get("source","ema"),
        "score":cross.get("final_score",0), "entry":entry, "exit":price,
        "result":result, "pnl_pct":round(pnl,2),
        "duration_min":int(age_min), "ts":_time.time()})


# ═══════════════════════════════════════════════════════════
# 17) رسائل
# ═══════════════════════════════════════════════════════════
def _source_label(s):
    return {"ema":"📈 EMA","rsi_div":"🔄 RSI","bb_squeeze":"💥 BB","sr_breakout":"🎯 S/R"}.get(s,"—")


def build_signal_message(cross):
    sym = cross["symbol"]; tf = cross["timeframe"]
    bull = cross["direction"] == "bullish"
    at = cross.get("alert_type","confirmed")
    s = cross.get("support",{})
    score = cross.get("final_score", s.get("score",0))
    grade = s.get("grade","?"); market = cross.get("market","?")
    levels = cross.get("levels"); source = cross.get("source","ema")
    mtf = cross.get("mtf","—")

    if at == "pre": emoji, title, tl = "🔔", "تقارب وشيك", "🔔 مبكر"
    elif at == "live": emoji, title, tl = "⚡", "تقاطع مبدئي", "⚡ مبدئي"
    else: emoji, title, tl = ("🚀" if bull else "🔻"), "إشارة مؤكدة", "✅ مؤكد"
    dir_lbl = "🟢 صاعد" if bull else "🔴 هابط"
    market_lbl = {"TREND_UP":"📈 ترند صاعد","TREND_DOWN":"📉 ترند هابط",
                  "RANGE":"↔️ عرضي","VOLATILE":"🌊 متقلب","UNKNOWN":"❓"}.get(market, market)

    msg = (f"{emoji} <b>{title} — {short(sym)} [{tf}]</b>\n"
           f"🇸🇾 {syria_now_str()}\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"{tl} | {_source_label(source)}\n"
           f"📊 <b>{dir_lbl}</b>\n"
           f"🏛️ السوق: <b>{market_lbl}</b>\n"
           f"🔗 MTF: <b>{mtf}</b>\n"
           f"📏 فرق EMA: <b>{cross['gap_pct']:.3f}%</b>\n"
           f"🎯 <b>{score}/100 {grade}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"• Vol: {s.get('vol_ratio','?')}× ({s.get('vol_pts',0)}/30)\n"
           f"• ADX: {s.get('adx','?')} ({s.get('adx_pts',0)}/20)\n"
           f"• RSI: {s.get('rsi','?')} ({s.get('rsi_pts',0)}/10)\n"
           f"• ATR: {s.get('atr_pct','?')}% ({s.get('atr_pts',0)}/5)\n")
    if levels:
        msg += (f"━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 <b>الخطة:</b>\n"
                f"• الدخول: <b>{fmt_price(levels['entry'])}</b>\n"
                f"• SL: <b>{fmt_price(levels['stop_loss'])}</b> (-{levels['sl_pct']}%)\n"
                f"• TP1: {fmt_price(levels['tp1'])}\n"
                f"• TP2: {fmt_price(levels['tp2'])}\n"
                f"• TP3: {fmt_price(levels['tp3'])}\n")
    msg += f"• 📦 {cross.get('exchange','?')}"
    return msg


# ═══════════════════════════════════════════════════════════
# 18) الأوامر
# ═══════════════════════════════════════════════════════════
async def cmd_start(update, context):
    ws = _ws.status()
    await update.message.reply_text(
        f"🤖 <b>Bot v7.1 — WS + Execution</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Data: <b>{_exchange.primary_name}</b>\n"
        f"💼 Exec: <b>{'Binance ✅' if _executor.ready else '❌'}</b>\n"
        f"🔌 WS: <b>{'🟢' if ws['connected'] else '🔴'} ({ws['cached']}/{ws['symbols']})</b>\n"
        f"🔐 Proxy: <b>{'✅' if PROXY_URL else '❌'}</b>\n"
        f"⚙️ {LEVERAGE}x | ${TRADE_MARGIN_USD} | Max {MAX_CONCURRENT_POSITIONS}\n"
        f"🚨 Kill: -${MAX_DAILY_LOSS_USD:.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"/ws /balance /positions /close SYM /closeall\n"
        f"/emergency /resume /status /last /pending /perf\n"
        f"/cross /cross5 /cross15 /cross1h /clearcache",
        parse_mode="HTML")


async def cmd_ws(update, context):
    st = _ws.status()
    lines = [f"🔌 <b>WebSocket Status</b>", "━━━━━━━━━━━━━━━━━━━",
             f"• متصل: {'🟢 نعم' if st['connected'] else '🔴 لا'}",
             f"• رموز مشتركة: {st['symbols']}",
             f"• أسعار مخزّنة: {st['cached']}",
             f"• آخر رسالة: {st['last_msg_age']}s" if st['last_msg_age'] else "• آخر رسالة: —",
             f"• إعادة اتصال: {st['reconnects']}"]
    if _ws.prices:
        lines.append("\n<b>أسعار حالية:</b>")
        for sym, p in list(_ws.prices.items())[:10]:
            lines.append(f"• {sym}: <b>{fmt_price(p['mark'])}</b> (bid={fmt_price(p['bid'])})")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_balance(update, context):
    if not _executor.ready:
        await update.message.reply_text("❌ التنفيذ معطّل."); return
    b = await _executor.get_balance(); _check_daily_reset()
    await update.message.reply_text(
        f"💰 <b>رصيد Binance</b>\n"
        f"• متاح: <b>{b['free']:.2f}$</b>\n"
        f"• مستخدم: <b>{b['used']:.2f}$</b>\n"
        f"• إجمالي: <b>{b['total']:.2f}$</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 PnL اليوم: <b>{_daily_pnl['realized']:+.2f}$</b>\n"
        f"🎯 صفقات: {_daily_pnl['trades']}\n"
        f"🏦 نشطة: {len(_open_positions)}/{MAX_CONCURRENT_POSITIONS}",
        parse_mode="HTML")


async def cmd_positions(update, context):
    if not _executor.ready:
        await update.message.reply_text("❌ التنفيذ معطّل."); return
    positions = await _executor.get_positions()
    if not positions:
        await update.message.reply_text("📭 لا صفقات."); return
    lines = [f"🏦 <b>الصفقات ({len(positions)})</b>", "━━━━━━━━━━━━━━━━━━━"]
    total = 0
    for p in positions:
        pnl = float(p.get("unrealizedPnl") or 0); total += pnl
        emoji = "🟢" if p["side"] == "long" else "🔴"
        lines.append(
            f"\n{emoji} <b>{short(p['symbol'])}</b> {p['side'].upper()}\n"
            f"   Qty: {p['contracts']} | Entry: {fmt_price(float(p.get('entryPrice') or 0))}\n"
            f"   Mark: {fmt_price(float(p.get('markPrice') or 0))}\n"
            f"   PnL: <b>{pnl:+.2f}$</b>")
    lines.append(f"\n━━━━━━━━━━━━━━━━━━━\n💰 إجمالي: <b>{total:+.2f}$</b>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_close(update, context):
    if not _executor.ready:
        await update.message.reply_text("❌ التنفيذ معطّل."); return
    args = context.args or []
    if not args:
        await update.message.reply_text("استخدام: <code>/close BTC</code>", parse_mode="HTML"); return
    name = args[0].upper()
    target = next((s for s in _open_positions if short(s) == name), None)
    if not target:
        await update.message.reply_text(f"❌ لا صفقة على {name}"); return
    await update.message.reply_text(f"🔄 إغلاق {name}...")
    r = await _executor.close_position(target)
    if r.get("ok"):
        _open_positions.pop(target, None)
        await update.message.reply_text(f"✅ أُغلقت {name}")
    else:
        await update.message.reply_text(f"❌ {r.get('err')}")


async def cmd_closeall(update, context):
    if not _executor.ready:
        await update.message.reply_text("❌ التنفيذ معطّل."); return
    if not _open_positions:
        await update.message.reply_text("📭 لا صفقات."); return
    n = len(_open_positions)
    await update.message.reply_text(f"🔄 إغلاق {n} صفقة...")
    closed = 0
    for sym in list(_open_positions.keys()):
        r = await _executor.close_position(sym)
        if r.get("ok"): _open_positions.pop(sym, None); closed += 1
    await update.message.reply_text(f"✅ {closed}/{n}")


async def cmd_emergency(update, context):
    if not _executor.ready:
        await update.message.reply_text("❌ التنفيذ معطّل."); return
    _kill_switch["active"] = True
    _kill_switch["reason"] = "يدوي"
    _kill_switch["until"] = _time.time() + 86400
    closed = 0
    for sym in list(_open_positions.keys()):
        r = await _executor.close_position(sym)
        if r.get("ok"): _open_positions.pop(sym, None); closed += 1
    await update.message.reply_text(
        f"🚨 <b>KILL SWITCH</b>\n• أُغلقت {closed}\n• معطّل 24h\n• /resume",
        parse_mode="HTML")


async def cmd_resume(update, context):
    _kill_switch["active"] = False; _kill_switch["until"] = 0
    await update.message.reply_text("✅ استُؤنف التنفيذ.")


async def cmd_status(update, context):
    _check_daily_reset()
    st = _smart.stats(); ws = _ws.status()
    await update.message.reply_text(
        f"🤖 <b>Status</b>\n"
        f"📦 {_exchange.primary_name} | 💼 {'✅' if _executor.ready else '❌'}\n"
        f"🔌 WS: {'🟢' if ws['connected'] else '🔴'} ({ws['cached']})\n"
        f"🔐 Proxy: {'✅' if PROXY_URL else '❌'}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 {len(_open_positions)}/{MAX_CONCURRENT_POSITIONS} | "
        f"💰 {_daily_pnl['realized']:+.2f}$\n"
        f"🎯 {_daily_pnl['trades']} | WR: {st['wr']}%\n"
        f"🚨 Kill: {'🔴' if _kill_switch['active'] else '🟢'}",
        parse_mode="HTML")


async def cmd_last(update, context):
    if not _last_signals:
        await update.message.reply_text("📭 لا إشارات."); return
    lines = ["📋 <b>آخر الإشارات</b>"]
    for s in reversed(list(_last_signals)):
        ic = {"win_tp2":"✅","win_tp1":"🎯","loss_sl":"❌","expired":"⏰"}.get(s["result"],"⏳")
        t = datetime.fromtimestamp(s["ts"], tz=SYRIA_TZ).strftime("%H:%M")
        d = "🟢" if s["direction"]=="bullish" else "🔴"
        lines.append(f"\n{ic} <b>{short(s['symbol'])}</b> [{s['timeframe']}] {d}")
        lines.append(f"   {t} | Score={s['score']} | {s['pnl_pct']:+.2f}%")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_pending(update, context):
    active = {sid:s for sid,s in _pending_signals.items() if not s["checked"]}
    if not active:
        await update.message.reply_text("📭 لا نشطة."); return
    lines = [f"⏳ <b>نشطة ({len(active)})</b>"]
    for sid, sig in active.items():
        c = sig["cross"]; lv = sig["levels"]
        lines.append(f"• <b>{short(c['symbol'])}</b> [{c['timeframe']}] {c['direction']}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_perf(update, context):
    rep = _smart.source_stats
    if not rep:
        await update.message.reply_text("📊 لا بيانات."); return
    lines = ["📊 <b>الأداء</b>"]
    for src, d in rep.items():
        tot = d["wins"] + d["losses"]
        wr = round(d["wins"]/tot*100,1) if tot else 0
        lines.append(f"• {_source_label(src)}: ✅{d['wins']} ❌{d['losses']} WR={wr}%")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def _run_cross(update, tfs=None):
    if not _exchange.ok():
        await update.message.reply_text("❌ لا منصة."); return
    tfs = tfs or TIMEFRAMES
    await update.message.reply_text(f"🔍 فحص {', '.join(tfs)}...")
    clear_caches()
    dets = []
    if ENABLE_PRE_CROSS: dets.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: dets.append(detect_live_crossover)
    if ENABLE_CONFIRMED: dets.append(detect_crossover)
    if ENABLE_RSI_DIVERGENCE: dets.append(detect_rsi_divergence)
    if ENABLE_BB_SQUEEZE: dets.append(detect_bb_squeeze_breakout)
    if ENABLE_SR_BREAKOUT: dets.append(detect_sr_breakout)
    found = 0
    for sym in ALL_SYMBOLS:
        for tf in tfs:
            for det in dets:
                cross = await det(sym, tf)
                if not cross: continue
                htf = await get_htf_trend(sym)
                passed, _, _ = await passes_filter(cross, htf)
                if not passed: continue
                try:
                    await update.message.reply_text(build_signal_message(cross), parse_mode="HTML")
                    found += 1
                    if cross["alert_type"] == "confirmed":
                        record_signal(sym, cross["direction"])
                        await execute_signal(cross, context.bot)
                except Exception as e:
                    log.error(f"send: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS/1000)
    await update.message.reply_text(f"✅ {found} إشارة.")


async def cmd_cross(u,c): await _run_cross(u)
async def cmd_cross5(u,c): await _run_cross(u, ["5m"])
async def cmd_cross15(u,c): await _run_cross(u, ["15m"])
async def cmd_cross1h(u,c): await _run_cross(u, ["1h"])


async def cmd_clearcache(update, context):
    clear_caches(); _smart.reset()
    await update.message.reply_text("✅ تم.")


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)): return
    log.error(f"❌ {err}", exc_info=err)


# ═══════════════════════════════════════════════════════════
# 19) Jobs
# ═══════════════════════════════════════════════════════════
async def crossover_job(context):
    if not _exchange.ok(): return
    dets = []
    if ENABLE_PRE_CROSS: dets.append(detect_pre_crossover)
    if ENABLE_LIVE_CROSS: dets.append(detect_live_crossover)
    if ENABLE_CONFIRMED: dets.append(detect_crossover)
    if ENABLE_RSI_DIVERGENCE: dets.append(detect_rsi_divergence)
    if ENABLE_BB_SQUEEZE: dets.append(detect_bb_squeeze_breakout)
    if ENABLE_SR_BREAKOUT: dets.append(detect_sr_breakout)
    total = 0
    for sym in ALL_SYMBOLS:
        for tf in TIMEFRAMES:
            for det in dets:
                try:
                    cross = await det(sym, tf)
                    if not cross: continue
                    htf = await get_htf_trend(sym)
                    passed, _, cat = await passes_filter(cross, htf)
                    if not passed:
                        _filter_stats[f"f_{cat}"] += 1; continue
                    if CHAT_ID:
                        try:
                            await context.bot.send_message(chat_id=CHAT_ID,
                                text=build_signal_message(cross), parse_mode="HTML")
                            total += 1; _filter_stats["sent"] += 1
                            if cross["alert_type"] == "confirmed":
                                record_signal(sym, cross["direction"])
                                await execute_signal(cross, context.bot)
                        except Exception as e:
                            log.error(f"send: {e}")
                except Exception as e:
                    log.exception(f"job {sym} {tf}: {e}")
            await asyncio.sleep(SYMBOL_DELAY_MS/1000)
    if total: log.info(f"📤 {total} إشارة")


async def tracker_job(context):
    try: await evaluate_pending()
    except Exception as e: log.exception(f"tracker: {e}")


async def sync_job(context):
    try: await sync_positions(bot=context.bot)
    except Exception as e: log.exception(f"sync: {e}")


# ═══════════════════════════════════════════════════════════
# 20) Health
# ═══════════════════════════════════════════════════════════
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - v7.1")
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def log_message(self, *a): pass

def run_health():
    port = int(os.getenv("PORT","8080"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()


# ═══════════════════════════════════════════════════════════
# 21) Main
# ═══════════════════════════════════════════════════════════
async def post_init(app):
    await _ws.start(ALL_SYMBOLS)
    log.info(f"🔌 WS started: {len(ALL_SYMBOLS)} symbols")


async def post_shutdown(app):
    await _ws.stop()


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ مفاتيح Telegram مطلوبة"); return

    threading.Thread(target=run_health, daemon=True).start()

    print(f"📦 Data: {_exchange.primary_name}")
    print(f"💼 Executor: {'✅' if _executor.ready else '❌'}")
    print(f"🔐 Proxy: {'✅' if PROXY_URL else '❌'} | WS proxy: {WS_USE_PROXY}")
    print(f"⚙️ {LEVERAGE}x | ${TRADE_MARGIN_USD} | Max {MAX_CONCURRENT_POSITIONS}")
    print(f"🚨 Kill: -${MAX_DAILY_LOSS_USD}")
    print(f"📊 Symbols: {[s.split('/')[0] for s in ALL_SYMBOLS]}")

    from telegram.request import HTTPXRequest
    req = HTTPXRequest(read_timeout=30.0, write_timeout=30.0,
                       connect_timeout=30.0, pool_timeout=30.0)

    app = (Application.builder().token(BOT_TOKEN)
           .request(req).get_updates_request(req)
           .post_init(post_init).post_shutdown(post_shutdown)
           .build())

    if app.job_queue is None:
        raise RuntimeError("pip install 'python-telegram-bot[job-queue]'")

    handlers = [
        ("start", cmd_start), ("ws", cmd_ws),
        ("balance", cmd_balance), ("positions", cmd_positions),
        ("close", cmd_close), ("closeall", cmd_closeall),
        ("emergency", cmd_emergency), ("resume", cmd_resume),
        ("cross", cmd_cross), ("cross5", cmd_cross5),
        ("cross15", cmd_cross15), ("cross1h", cmd_cross1h),
        ("status", cmd_status), ("last", cmd_last), ("pending", cmd_pending),
        ("perf", cmd_perf), ("clearcache", cmd_clearcache),
    ]
    for name, fn in handlers:
        app.add_handler(CommandHandler(name, fn))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(crossover_job, interval=JOB_INTERVAL_MIN*60, first=20, name="cross")
    app.job_queue.run_repeating(tracker_job, interval=SIGNAL_TRACK_INTERVAL_MIN*60, first=50, name="tracker")
    app.job_queue.run_repeating(sync_job, interval=60, first=30, name="sync")

    print("✅ polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True, bootstrap_retries=5)


if __name__ == "__main__":
    main()
