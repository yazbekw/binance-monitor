"""
Unified Trading Bot v1.0
========================
Two strategies in parallel:
  A) Momentum (15m) - EMA/MACD/VWAP scoring
  B) EMA Cross (5m/15m/1h) - EMA crossover detection
Shared position management, max 3 concurrent trades.
"""
import os, re, json, time, logging, threading, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np
from flask import Flask, jsonify
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

SYMBOLS            = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", 200))
LEVERAGE           = int(os.getenv("LEVERAGE", 20))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT_TRADES", 3))
TESTNET            = os.getenv("TESTNET", "false").lower() == "true"

# Momentum strategy
MOMENTUM_TF        = os.getenv("MOMENTUM_TF", "15m")
MOMENTUM_MIN_SCORE = int(os.getenv("MOMENTUM_MIN_SCORE", 6))

# EMA Cross strategy
EMA_FAST           = int(os.getenv("EMA_FAST", 7))
EMA_SLOW           = int(os.getenv("EMA_SLOW", 25))
EMA_TFS            = [t.strip() for t in os.getenv("EMA_TFS", "5m,15m,1h").split(",") if t.strip()]
EMA_MIN_SCORE      = float(os.getenv("EMA_MIN_SCORE", 55))
EMA_MIN_VOL        = float(os.getenv("EMA_MIN_VOL", 1.0))
EMA_MIN_ADX        = float(os.getenv("EMA_MIN_ADX", 18.0))
EMA_MAX_ATR        = float(os.getenv("EMA_MAX_ATR", 0.7))
EMA_BLOCK_HTF      = os.getenv("EMA_BLOCK_HTF", "true").lower() == "true"

# Risk management
SL_ATR_MULT        = float(os.getenv("SL_ATR_MULT", 1.5))
TP1_ATR_MULT       = float(os.getenv("TP1_ATR_MULT", 1.5))
TP2_ATR_MULT       = float(os.getenv("TP2_ATR_MULT", 3.0))
TRAILING_ENABLED   = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
FALLBACK_ENABLED   = os.getenv("FALLBACK_ENABLED", "true").lower() == "true"
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "true").lower() == "true"

# Session filter (UTC hours)
SESSION_START      = int(os.getenv("SESSION_START_UTC", 7))
SESSION_END        = int(os.getenv("SESSION_END_UTC", 21))

# Rate limiting
MAX_WEIGHT_PER_MIN = int(os.getenv("MAX_WEIGHT_PER_MIN", 1000))
MIN_INTERVAL_SEC   = float(os.getenv("MIN_INTERVAL_SEC", 0.2))
OHLCV_CACHE_SEC    = int(os.getenv("OHLCV_CACHE_SEC", 60))
SYMBOL_COOLDOWN    = int(os.getenv("SYMBOL_COOLDOWN_SEC", 60))
GLOBAL_PAUSE       = int(os.getenv("GLOBAL_PAUSE_SEC", 120))
GLOBAL_PAUSE_TRIG  = int(os.getenv("GLOBAL_PAUSE_TRIGGER", 3))
BACKOFF_BASE       = float(os.getenv("BACKOFF_BASE", 2.0))
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", 3))

# Monitoring
MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))
HEARTBEAT_HOURS    = int(os.getenv("HEARTBEAT_HOURS", 4))
HISTORY_FILE       = os.getenv("HISTORY_FILE", "/tmp/trade_history.json")

# ============================================================
# TIMEZONE
# ============================================================
SYRIA_TZ = timezone(timedelta(hours=3))
def syr_now(): return datetime.now(SYRIA_TZ)
def syr_str(dt=None):
    if dt is None: dt = syr_now()
    elif dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(SYRIA_TZ).strftime("%Y-%m-%d %H:%M:%S")

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("UnifiedBot")
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# ============================================================
# STATE
# ============================================================
open_positions = {}          # symbol -> info dict
state_lock = threading.RLock()
_symbol_locks = defaultdict(threading.Lock)
trade_history = []

# ============================================================
# RATE LIMITER
# ============================================================
class RateLimiter:
    def __init__(self, max_weight=1000, min_interval=0.2):
        self.max_weight = max_weight
        self.min_interval = min_interval
        self.window = []
        self.lock = threading.Lock()
        self.last_req = 0

    def add(self, weight):
        with self.lock:
            now = time.time()
            self.window = [(t, w) for t, w in self.window if now - t < 60]
            self.window.append((now, weight))
            total = sum(w for _, w in self.window)
            if total > self.max_weight:
                wait = 60 - (now - self.window[0][0])
                if wait > 0:
                    log.warning(f"⚠️ weight={total}/{self.max_weight} — انتظار {wait:.0f}s")
                    time.sleep(wait)

    def throttle(self):
        with self.lock:
            now = time.time()
            d = now - self.last_req
            if d < self.min_interval:
                time.sleep(self.min_interval - d)
            self.last_req = time.time()

rate_limiter = RateLimiter(MAX_WEIGHT_PER_MIN, MIN_INTERVAL_SEC)

# ============================================================
# CACHES
# ============================================================
_ohlcv_cache = {}
_ohlcv_cache_lock = threading.Lock()

_exchange_info = None
_exchange_info_lock = threading.Lock()

def get_exchange_info():
    global _exchange_info
    with _exchange_info_lock:
        if _exchange_info is None:
            rate_limiter.add(1)
            _exchange_info = client.futures_exchange_info()
        return _exchange_info

# ============================================================
# COOLDOWN / PAUSE / STATS
# ============================================================
_symbol_cooldown = {}
_global_pause_until = 0.0

_stats = {
    "sent": 0, "trades_opened": 0, "trades_closed": 0,
    "momentum_trades": 0, "ema_trades": 0,
    "filtered_vol": 0, "filtered_adx": 0, "filtered_atr": 0,
    "filtered_score": 0, "filtered_htf": 0, "filtered_other": 0,
    "rate_limit_hits": 0, "global_pauses": 0,
}
_stats_lock = threading.Lock()

def bump_stat(key, amount=1):
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount

# ============================================================
# TELEGRAM
# ============================================================
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

def tg_send(text):
    if not TG_BASE or not TELEGRAM_CHAT_ID:
        return
    try:
        r = requests.post(
            f"{TG_BASE}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        if r.status_code != 200:
            log.error(f"TG error: {r.text[:200]}")
    except Exception as e:
        log.error(f"TG exception: {e}")

def tg_log(title, body, emoji="ℹ️"):
    msg = f"{emoji} <b>{title}</b>\n\n🕐 <b>دمشق:</b> {syr_str()}\n\n{body}"
    tg_send(msg)
    log.info(f"{title} | {body.replace(chr(10), ' | ')[:200]}")

# ============================================================
# BINANCE CLIENT
# ============================================================
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY, testnet=TESTNET)

# ============================================================
# SAFE API CALL
# ============================================================
def safe_api_call(func, *args, weight=1, retries=MAX_RETRIES, symbol=None, **kwargs):
    global _global_pause_until
    now = time.time()

    # Global pause check
    if now < _global_pause_until:
        wait = int(_global_pause_until - now)
        log.warning(f"⏸️ إيقاف عالمي — باقي {wait}s")
        time.sleep(min(wait, 30))
        return None

    # Symbol cooldown check
    if symbol:
        cd_until = _symbol_cooldown.get(symbol, 0)
        if now < cd_until:
            log.debug(f"⏸️ {symbol} في كولداون")
            return None

    for attempt in range(retries):
        try:
            rate_limiter.throttle()
            rate_limiter.add(weight)
            result = func(*args, **kwargs)
            if symbol:
                _symbol_cooldown.pop(symbol, None)
            return result
        except AttributeError:
            raise
        except BinanceAPIException as e:
            bump_stat("rate_limit_hits")
            if e.code == -1003:
                msg = str(e.message)
                m = re.search(r"banned until (\d+)", msg)
                if m:
                    ban_until = datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc)
                    wait = (ban_until - datetime.now(timezone.utc)).total_seconds()
                    if wait > 0:
                        log.error(f"🚫 IP محظور حتى {syr_str(ban_until)}")
                        tg_log("🚫 IP محظور", f"ينتهي: {syr_str(ban_until)}", "🚫")
                        time.sleep(min(wait + 5, 7200))
                        continue
                wait = BACKOFF_BASE * (2 ** attempt)
                log.warning(f"⚠️ Rate limit — {wait}s")
                time.sleep(wait)
            elif e.code == -1021:
                time.sleep(1)
            elif e.code in (-4120, -1102, -1111, -2021):
                raise
            else:
                raise
        except BinanceRequestException:
            time.sleep(BACKOFF_BASE * (2 ** attempt))
    raise Exception(f"فشل بعد {retries} محاولات")

# ============================================================
# FETCH OHLCV (cached)
# ============================================================
def fetch_ohlcv(symbol, tf, limit=300):
    try:
        raw = safe_api_call(client.futures_klines, symbol=symbol, interval=tf,
                           limit=limit, weight=5, symbol=symbol)
        if not raw:
            return None
        df = pd.DataFrame(raw, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","qav","trades","tbbav","tbqav","ignore"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        return df
    except Exception as e:
        log.error(f"fetch_ohlcv {symbol} {tf}: {e}")
        return None

def fetch_ohlcv_cached(symbol, tf, limit=300):
    key = (symbol, tf, limit)
    with _ohlcv_cache_lock:
        c = _ohlcv_cache.get(key)
        if c and (time.time() - c["ts"]) < OHLCV_CACHE_SEC:
            return c["data"]
    data = fetch_ohlcv(symbol, tf, limit)
    if data is not None:
        with _ohlcv_cache_lock:
            _ohlcv_cache[key] = {"data": data, "ts": time.time()}
    return data

def clear_all_caches():
    with _ohlcv_cache_lock:
        _ohlcv_cache.clear()
    _symbol_cooldown.clear()
    global _global_pause_until
    _global_pause_until = 0.0

# ============================================================
# HELPERS
# ============================================================
def get_balance():
    try:
        bals = safe_api_call(client.futures_account_balance, weight=5)
        for b in bals or []:
            if b["asset"] == "USDT":
                return {"balance": float(b["balance"]),
                        "available": float(b["availableBalance"]),
                        "pnl": float(b.get("crossUnPnl", 0))}
    except Exception as e:
        log.error(f"balance: {e}")
    return {"balance": 0.0, "available": 0.0, "pnl": 0.0}

def get_price(symbol):
    try:
        t = safe_api_call(client.futures_symbol_ticker, symbol=symbol, weight=1, symbol=symbol)
        return float(t["price"]) if t else 0.0
    except Exception:
        return 0.0

def has_open_position(symbol):
    with state_lock:
        if symbol in open_positions:
            return True
    return False

def get_active_count():
    with state_lock:
        return len(open_positions)

def round_step(symbol, qty):
    for s in get_exchange_info()["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                    prec = int(round(-np.log10(step)))
                    return round(np.floor(qty / step) * step, prec)
    return round(qty, 3)

def round_price(symbol, price):
    for s in get_exchange_info()["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                    prec = int(round(-np.log10(tick)))
                    return round(round(price / tick) * tick, prec)
    return round(price, 4)

# ============================================================
# INDICATORS
# ============================================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()

def macd_series(s, f=12, sl=26, sig=9):
    dif = ema(s, f) - ema(s, sl)
    dea = ema(dif, sig)
    return dif, dea, dif - dea

def rsi_series(s, p=14):
    d = s.diff()
    g = d.where(d > 0, 0).rolling(p).mean()
    l = (-d.where(d < 0, 0)).rolling(p).mean()
    rs = g / l.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))

def atr_series(df, p=14):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(p).mean()

def vwap_series(df):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    return (tp * df["volume"]).cumsum() / df["volume"].cumsum()

def adx_series(df, p=14):
    h, l, c = df["high"], df["low"], df["close"]
    pdm = h.diff()
    ndm = -l.diff()
    pdm = pdm.where((pdm > ndm) & (pdm > 0), 0)
    ndm = ndm.where((ndm > pdm) & (ndm > 0), 0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100 * pdm.ewm(alpha=1/p, adjust=False).mean() / atr.replace(0, 1e-9)
    ndi = 100 * ndm.ewm(alpha=1/p, adjust=False).mean() / atr.replace(0, 1e-9)
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, 1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    df["ema7"] = ema(df["close"], 7)
    df["ema25"] = ema(df["close"], 25)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["ef"] = ema(df["close"], EMA_FAST)
    df["es"] = ema(df["close"], EMA_SLOW)
    df["dif"], df["dea"], df["hist"] = macd_series(df["close"])
    df["rsi"] = rsi_series(df["close"])
    df["atr"] = atr_series(df)
    df["vwap"] = vwap_series(df)
    df["vol_ma5"] = df["volume"].rolling(5).mean()
    df["adx"] = adx_series(df)
    return df

# ============================================================
# MOMENTUM STRATEGY (from v3.1)
# ============================================================
def evaluate_momentum(df, direction):
    last, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
    score, reasons = 0, []
    price = last["close"]

    if direction == "LONG":
        checks = [
            (price > last["ema200"], 2, "فوق EMA200"),
            (price > last["ema50"], 1, "فوق EMA50"),
            (prev["dif"] <= prev["dea"] and last["dif"] > last["dea"], 2, "تقاطع MACD صاعد"),
            (prev2["hist"] < prev["hist"] < 0 and last["hist"] > prev["hist"], 1, "تحول الهيستوجرام للأخضر"),
            (last["volume"] > last["vol_ma5"], 2, "حجم فوق المتوسط"),
            (price > last["vwap"], 1, "فوق VWAP"),
            (last["rsi"] > prev["rsi"] and price < prev["close"], 1, "انحراف RSI صاعد"),
        ]
    else:
        checks = [
            (price < last["ema200"], 2, "تحت EMA200"),
            (price < last["ema50"], 1, "تحت EMA50"),
            (prev["dif"] >= prev["dea"] and last["dif"] < last["dea"], 2, "تقاطع MACD هابط"),
            (prev2["hist"] > prev["hist"] > 0 and last["hist"] < prev["hist"], 1, "تحول الهيستوجرام للأحمر"),
            (last["volume"] > last["vol_ma5"], 2, "حجم فوق المتوسط"),
            (price < last["vwap"], 1, "تحت VWAP"),
            (last["rsi"] < prev["rsi"] and price > prev["close"], 1, "انحراف RSI هابط"),
        ]
    for ok, pts, label in checks:
        if ok:
            score += pts
            reasons.append(f"✅ {label} +{pts}")

    atr = float(last["atr"])
    if direction == "LONG":
        entry, sl = price, price - SL_ATR_MULT * atr
        tp1, tp2 = price + TP1_ATR_MULT * atr, price + TP2_ATR_MULT * atr
    else:
        entry, sl = price, price + SL_ATR_MULT * atr
        tp1, tp2 = price - TP1_ATR_MULT * atr, price - TP2_ATR_MULT * atr

    return {
        "score": score, "max_score": 10, "reasons": reasons,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "atr": atr, "rsi": float(last["rsi"]),
        "ema50": float(last["ema50"]), "ema200": float(last["ema200"]),
        "vwap": float(last["vwap"]), "adx": float(last["adx"]),
        "vol_ratio": float(last["volume"]/last["vol_ma5"]) if last["vol_ma5"]>0 else 0,
        "dif": float(last["dif"]), "dea": float(last["dea"]),
    }

def momentum_scan():
    log.info(f"🔍 [Momentum] مسح {len(SYMBOLS)} عملات")
    for symbol in SYMBOLS:
        try:
            if get_active_count() >= MAX_CONCURRENT:
                break
            if has_open_position(symbol):
                continue

            df = fetch_ohlcv_cached(symbol, MOMENTUM_TF, 300)
            if df is None or len(df) < 200:
                continue
            df = add_indicators(df)
            closed = df.iloc[:-1]

            longs = evaluate_momentum(closed, "LONG")
            shorts = evaluate_momentum(closed, "SHORT")
            log.info(f"[M] {symbol}: L={longs['score']} S={shorts['score']}")

            if longs["score"] >= MOMENTUM_MIN_SCORE:
                open_trade(symbol, "LONG", longs, "MOMENTUM")
            elif shorts["score"] >= MOMENTUM_MIN_SCORE:
                open_trade(symbol, "SHORT", shorts, "MOMENTUM")
        except Exception as e:
            log.error(f"[M] {symbol}: {e}")

# ============================================================
# EMA CROSS STRATEGY (from app01)
# ============================================================
_last_cross_candle = {}  # (symbol, tf) -> candle_ts

def evaluate_ema_cross(df, direction, trigger_tf):
    last = df.iloc[-1]
    price = float(last["close"])
    score, reasons = 0, []

    # Volume (30)
    try:
        start = max(0, len(df) - 21)
        vma = float(df["volume"].iloc[start:-1].mean())
        vr = float(last["volume"]) / vma if vma > 0 else 0
    except Exception:
        vr = 0
    if vr >= 2.5: score += 25; reasons.append(f"✅ حجم قوي {vr:.2f}× +25")
    elif vr >= 1.5: score += 18; reasons.append(f"✅ حجم جيد {vr:.2f}× +18")
    elif vr >= 1.0: score += 10; reasons.append(f"✅ حجم طبيعي {vr:.2f}× +10")
    elif vr >= 0.5: score += 5
    else: score += 0

    # ADX (20)
    adx = float(last["adx"])
    if adx >= 35: score += 20; reasons.append(f"✅ ADX {adx:.1f} قوي +20")
    elif adx >= 25: score += 18; reasons.append(f"✅ ADX {adx:.1f} واضح +18")
    elif adx >= 20: score += 12
    elif adx >= 15: score += 5

    # Gap (15)
    gap = abs(last["ef"] - last["es"]) / last["es"] * 100 if last["es"] else 0
    if gap >= 0.3: score += 15; reasons.append(f"✅ فرق EMA {gap:.3f}% +15")
    elif gap >= 0.1: score += 10; reasons.append(f"✅ فرق EMA {gap:.3f}% +10")
    elif gap >= 0.05: score += 6
    else: score += 3

    # MACD (10)
    hist_now = float(last["hist"])
    hist_prev = float(df["hist"].iloc[-2])
    rising = hist_now > hist_prev
    if hist_now > 0 and rising: score += 10; reasons.append("✅ MACD صاعد ومتسارع +10")
    elif hist_now < 0 and rising: score += 7
    else: score += 5

    # RSI (10)
    rsi = float(last["rsi"])
    if direction == "LONG":
        if 55 <= rsi <= 70: score += 10; reasons.append(f"✅ RSI {rsi:.1f} صحي +10")
        elif rsi >= 45: score += 7
        elif rsi > 70: score += 5
    else:
        if 30 <= rsi <= 45: score += 10; reasons.append(f"✅ RSI {rsi:.1f} صحي +10")
        elif rsi <= 55: score += 7
        elif rsi < 30: score += 5

    # EMA50 (10)
    ema50 = float(last["ema50"])
    htf_ok = (direction == "LONG" and price > ema50) or (direction == "SHORT" and price < ema50)
    if htf_ok: score += 10; reasons.append("✅ مع EMA50 +10")

    # ATR (5)
    atr = float(last["atr"])
    atr_pct = (atr / price * 100) if price > 0 else 0
    if 0.15 <= atr_pct <= 0.5: score += 5; reasons.append(f"✅ ATR {atr_pct:.2f}% +5")
    elif atr_pct < 0.15: score += 2
    else: score += 1

    if direction == "LONG":
        entry, sl = price, price - SL_ATR_MULT * atr
        tp1, tp2 = price + TP1_ATR_MULT * atr, price + TP2_ATR_MULT * atr
    else:
        entry, sl = price, price + SL_ATR_MULT * atr
        tp1, tp2 = price - TP1_ATR_MULT * atr, price - TP2_ATR_MULT * atr

    return {
        "score": score, "max_score": 100, "reasons": reasons,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "atr": atr, "rsi": rsi, "adx": adx, "gap": gap,
        "vol_ratio": vr, "atr_pct": atr_pct, "htf_ok": htf_ok,
        "ema50": ema50, "ema_fast": float(last["ef"]), "ema_slow": float(last["es"]),
        "price": price, "trigger_tf": trigger_tf,
    }

def detect_ema_cross(symbol, tf):
    df = fetch_ohlcv_cached(symbol, tf, EMA_SLOW + 60)
    if df is None or len(df) < EMA_SLOW + 5:
        return None
    df = add_indicators(df)

    curr, prev = -2, -3
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])

    bullish = (cf > cs) and (pf <= ps)
    bearish = (cf < cs) and (pf >= ps)
    if not (bullish or bearish):
        return None

    direction = "LONG" if bullish else "SHORT"
    candle_ts = int(pd.Timestamp(df["open_time"].iloc[curr]).timestamp() * 1000)

    key = (symbol, tf)
    if _last_cross_candle.get(key) == candle_ts:
        return None
    _last_cross_candle[key] = candle_ts

    # Evaluate on the closed candle (use index -2)
    eval_df = df.iloc[:-1]  # exclude current live candle
    signal = evaluate_ema_cross(eval_df, direction, tf)

    # Hard filter
    if signal["vol_ratio"] < EMA_MIN_VOL:
        bump_stat("filtered_vol"); return None
    if signal["adx"] < EMA_MIN_ADX:
        bump_stat("filtered_adx"); return None
    if signal["atr_pct"] > EMA_MAX_ATR:
        bump_stat("filtered_atr"); return None
    if EMA_BLOCK_HTF and not signal["htf_ok"]:
        bump_stat("filtered_htf"); return None
    if signal["score"] < EMA_MIN_SCORE:
        bump_stat("filtered_score"); return None

    return signal

def ema_cross_scan():
    log.info(f"🔍 [EMA Cross] مسح {len(SYMBOLS)} عملات × {len(EMA_TFS)} فريمات")
    for symbol in SYMBOLS:
        for tf in EMA_TFS:
            try:
                if get_active_count() >= MAX_CONCURRENT:
                    return
                if has_open_position(symbol):
                    continue
                signal = detect_ema_cross(symbol, tf)
                if signal:
                    log.info(f"[E] {symbol} {tf} {signal['entry']:.4f} score={signal['score']}")
                    open_trade(symbol, "LONG" if signal["ema_fast"] > signal["ema_slow"] else "SHORT",
                              signal, f"EMA_{tf}")
            except Exception as e:
                log.error(f"[E] {symbol} {tf}: {e}")

# ============================================================
# ORDER PLACEMENT
# ============================================================
def place_stop_loss(symbol, side, price, qty):
    attempts = [
        dict(symbol=symbol, side=side, type="STOP_MARKET",
             stopPrice=price, closePosition=True),
        dict(symbol=symbol, side=side, type="STOP_MARKET",
             stopPrice=price, closePosition=True, workingType="MARK_PRICE"),
        dict(symbol=symbol, side=side, type="STOP_MARKET",
             stopPrice=price, quantity=qty, reduceOnly=True),
    ]
    for i, p in enumerate(attempts, 1):
        try:
            return safe_api_call(client.futures_create_order, weight=1, **p)
        except BinanceAPIException as e:
            log.warning(f"SL attempt {i} failed: {e.code} {e.message}")
    return None

def place_take_profit(symbol, side, price, qty):
    attempts = [
        dict(symbol=symbol, side=side, type="TAKE_PROFIT_MARKET",
             stopPrice=price, quantity=qty, reduceOnly=True),
        dict(symbol=symbol, side=side, type="TAKE_PROFIT_MARKET",
             stopPrice=price, quantity=qty, reduceOnly=True, workingType="MARK_PRICE"),
        dict(symbol=symbol, side=side, type="TAKE_PROFIT_MARKET",
             stopPrice=price, quantity=qty),
    ]
    for i, p in enumerate(attempts, 1):
        try:
            return safe_api_call(client.futures_create_order, weight=1, **p)
        except BinanceAPIException as e:
            log.warning(f"TP attempt {i} failed: {e.code} {e.message}")
    return None

def close_position_market(symbol, side, qty):
    opp = "SELL" if side == "LONG" else "BUY"
    try:
        return safe_api_call(client.futures_create_order, weight=1,
                            symbol=symbol, side=opp, type="MARKET",
                            quantity=qty, reduceOnly=True)
    except BinanceAPIException as e:
        log.error(f"close {symbol}: {e.code} {e.message}")
        return None

# ============================================================
# OPEN TRADE (shared)
# ============================================================
def open_trade(symbol, direction, signal, strategy):
    with _symbol_locks[symbol]:
        # Double-check
        if has_open_position(symbol):
            return
        if get_active_count() >= MAX_CONCURRENT:
            return

        bal = get_balance()
        margin_needed = POSITION_SIZE_USDT / LEVERAGE
        if bal["available"] < margin_needed * 1.3:
            tg_log("⚠️ رصيد غير كافٍ",
                   f"💠 {symbol}\n💰 متاح: {bal['available']:.2f}\n"
                   f"📊 مطلوب: {margin_needed*1.3:.2f}", "⚠️")
            return

        try:
            safe_api_call(client.futures_change_leverage, symbol=symbol,
                         leverage=LEVERAGE, weight=1)

            price = signal["entry"]
            qty = round_step(symbol, POSITION_SIZE_USDT / price)
            if qty <= 0:
                return

            side = "BUY" if direction == "LONG" else "SELL"
            opp = "SELL" if direction == "LONG" else "BUY"

            order = safe_api_call(client.futures_create_order, weight=1,
                                 symbol=symbol, side=side, type="MARKET", quantity=qty)
            if not order:
                return
            fill = float(order.get("avgPrice", price)) or price
            notional = qty * fill

            atr = signal["atr"]
            if direction == "LONG":
                sl = round_price(symbol, fill - SL_ATR_MULT * atr)
                tp1 = round_price(symbol, fill + TP1_ATR_MULT * atr)
                tp2 = round_price(symbol, fill + TP2_ATR_MULT * atr)
                min_tp1 = fill * 1.001
                if tp1 < min_tp1: tp1 = round_price(symbol, min_tp1)
                if tp2 <= tp1: tp2 = round_price(symbol, tp1 * 1.001)
            else:
                sl = round_price(symbol, fill + SL_ATR_MULT * atr)
                tp1 = round_price(symbol, fill - TP1_ATR_MULT * atr)
                tp2 = round_price(symbol, fill - TP2_ATR_MULT * atr)
                max_tp1 = fill * 0.999
                if tp1 > max_tp1: tp1 = round_price(symbol, max_tp1)
                if tp2 >= tp1: tp2 = round_price(symbol, tp1 * 0.999)

            sl_order = place_stop_loss(symbol, opp, sl, qty)
            half = round_step(symbol, qty / 2)
            tp1_order = place_take_profit(symbol, opp, tp1, half)
            tp2_order = place_take_profit(symbol, opp, tp2, qty - half)

            with state_lock:
                open_positions[symbol] = {
                    "side": direction, "entry": fill, "qty": qty,
                    "atr": atr, "current_sl": sl,
                    "sl_order_id": sl_order["orderId"] if sl_order else None,
                    "tp1_order_id": tp1_order["orderId"] if tp1_order else None,
                    "tp2_order_id": tp2_order["orderId"] if tp2_order else None,
                    "tp1_price": tp1, "tp2_price": tp2,
                    "sl_on_exchange": sl_order is not None,
                    "tp1_on_exchange": tp1_order is not None,
                    "tp2_on_exchange": tp2_order is not None,
                    "tp1_executed": False, "tp2_executed": False,
                    "trailing_stage": 0,
                    "opened_at": datetime.now(timezone.utc),
                    "strategy": strategy,
                    "source": "BOT",
                }

            bump_stat("trades_opened")
            if strategy == "MOMENTUM":
                bump_stat("momentum_trades")
            else:
                bump_stat("ema_trades")

            # Notifications
            if not sl_order:
                tg_log("🚨 صفقة بدون SL على Binance",
                       f"💠 {symbol}\n🛑 SL: {sl}\n📝 البوت سيراقب يدوياً",
                       "🚨")
            if not tp1_order:
                tg_log("⚠️ فشل TP1", f"{symbol}: {tp1}", "⚠️")
            if not tp2_order:
                tg_log("⚠️ فشل TP2", f"{symbol}: {tp2}", "⚠️")

            sl_pct = abs(fill - sl) / fill * 100
            tp1_pct = abs(tp1 - fill) / fill * 100
            tp2_pct = abs(tp2 - fill) / fill * 100
            rr1 = abs(tp1 - fill) / abs(fill - sl) if sl != fill else 0
            rr2 = abs(tp2 - fill) / abs(fill - sl) if sl != fill else 0

            reasons_txt = "\n".join(signal["reasons"])
            strategy_emoji = "📈" if strategy == "MOMENTUM" else "🔀"

            body = (
                f"{strategy_emoji} <b>الاستراتيجية:</b> {strategy}\n"
                f"💠 <b>العملة:</b> {symbol}\n"
                f"📊 <b>الاتجاه:</b> {direction}\n"
                f"💵 <b>الدخول:</b> {fill}\n"
                f"📦 <b>الكمية:</b> {qty}\n"
                f"💼 <b>الاسمي:</b> {notional:.2f} USDT\n"
                f"💰 <b>الهامش:</b> {notional/LEVERAGE:.2f} USDT\n\n"
                f"🛑 <b>SL:</b> {sl} ({sl_pct:.2f}%)\n"
                f"🎯 <b>TP1:</b> {tp1} ({tp1_pct:.2f}%) R:R 1:{rr1:.2f}\n"
                f"🎯 <b>TP2:</b> {tp2} ({tp2_pct:.2f}%) R:R 1:{rr2:.2f}\n\n"
                f"📊 <b>Score:</b> {signal['score']}/{signal['max_score']}\n"
                f"📝 <b>الأسباب:</b>\n{reasons_txt}"
            )
            tg_log("🚀 فتح صفقة", body, "🚀")

        except BinanceAPIException as e:
            tg_log("❌ فشل فتح صفقة", f"{symbol}\nCode: {e.code}\n{e.message}", "❌")
        except Exception as e:
            log.exception(f"open_trade {symbol}")
            tg_log("❌ خطأ", f"{symbol}: {e}", "❌")

# ============================================================
# TRAILING STOP
# ============================================================
def update_trailing(symbol, info, price):
    if not TRAILING_ENABLED or not info.get("current_sl"):
        return
    side = info["side"]
    entry = info["entry"]
    atr = info["atr"]
    stage = info["trailing_stage"]
    cur_sl = info["current_sl"]
    opp = "SELL" if side == "LONG" else "BUY"
    if atr <= 0:
        return

    new_sl, new_stage = None, stage
    if side == "LONG":
        move = price - entry
        if stage == 0 and move >= atr: new_sl, new_stage = round_price(symbol, entry), 1
        elif stage == 1 and move >= 2*atr: new_sl, new_stage = round_price(symbol, entry + atr), 2
        elif stage == 2 and move >= 3*atr: new_sl, new_stage = round_price(symbol, entry + 2*atr), 3
        elif stage >= 3:
            ts = round_price(symbol, price - atr)
            if ts > cur_sl: new_sl, new_stage = ts, stage + 1
    else:
        move = entry - price
        if stage == 0 and move >= atr: new_sl, new_stage = round_price(symbol, entry), 1
        elif stage == 1 and move >= 2*atr: new_sl, new_stage = round_price(symbol, entry - atr), 2
        elif stage == 2 and move >= 3*atr: new_sl, new_stage = round_price(symbol, entry - 2*atr), 3
        elif stage >= 3:
            ts = round_price(symbol, price + atr)
            if ts < cur_sl: new_sl, new_stage = ts, stage + 1

    if new_sl is None or new_sl == cur_sl:
        return

    if info.get("sl_on_exchange") and info.get("sl_order_id"):
        try:
            safe_api_call(client.futures_cancel_order, symbol=symbol,
                         orderId=info["sl_order_id"], weight=1)
        except Exception:
            pass

    new_order = place_stop_loss(symbol, opp, new_sl, info["qty"])
    with state_lock:
        info["current_sl"] = new_sl
        info["trailing_stage"] = new_stage
        if new_order:
            info["sl_order_id"] = new_order["orderId"]
            info["sl_on_exchange"] = True
        else:
            info["sl_on_exchange"] = False
            info["sl_order_id"] = None

    tg_log("🔄 تحديث الوقف",
           f"💠 {symbol} | {side}\n💰 السعر: {price}\n🛑 جديد: {new_sl}\n📈 مرحلة: {new_stage}",
           "🔄")

# ============================================================
# MANUAL IMPORT
# ============================================================
def import_manual():
    try:
        positions = safe_api_call(client.futures_position_information, weight=5)
        for p in positions or []:
            symbol = p["symbol"]
            amt = float(p["positionAmt"])
            if amt == 0 or symbol not in SYMBOLS:
                continue
            with state_lock:
                if symbol in open_positions:
                    continue

            entry = float(p["entryPrice"])
            side = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            notional = entry * qty
            margin_type = p.get("marginType", "isolated").lower()
            unrl = float(p.get("unRealizedProfit", 0))
            mark = float(p.get("markPrice", entry))

            df = fetch_ohlcv_cached(symbol, MOMENTUM_TF, 100)
            if df is not None:
                df = add_indicators(df)
                atr = float(df.iloc[-1]["atr"])
            else:
                atr = entry * 0.01

            sl = None
            sl_id = None
            try:
                orders = safe_api_call(client.futures_get_open_orders, symbol=symbol, weight=5)
                for o in orders or []:
                    if o["type"] in ("STOP_MARKET", "STOP"):
                        sl = float(o["stopPrice"])
                        sl_id = o["orderId"]
                        break
            except Exception:
                pass

            with state_lock:
                open_positions[symbol] = {
                    "side": side, "entry": entry, "qty": qty, "atr": atr,
                    "current_sl": sl, "sl_order_id": sl_id,
                    "tp1_order_id": None, "tp2_order_id": None,
                    "tp1_price": None, "tp2_price": None,
                    "sl_on_exchange": sl_id is not None,
                    "tp1_on_exchange": False, "tp2_on_exchange": False,
                    "tp1_executed": False, "tp2_executed": False,
                    "trailing_stage": 0,
                    "opened_at": datetime.now(timezone.utc),
                    "strategy": "MANUAL", "source": "MANUAL",
                }

            tg_log("📥 استيراد صفقة يدوية",
                   f"💠 {symbol}\n📊 {side}\n💵 {entry}\n📦 {qty}\n"
                   f"💰 {unrl:+.2f} USDT\n🛡️ SL: {sl or 'لا يوجد'}",
                   "📥")

            if sl is None and AUTO_SL_MANUAL:
                auto_sl = round_price(symbol, entry - 1.5*atr if side=="LONG" else entry + 1.5*atr)
                opp = "SELL" if side == "LONG" else "BUY"
                sl_order = place_stop_loss(symbol, opp, auto_sl, qty)
                with state_lock:
                    open_positions[symbol]["current_sl"] = auto_sl
                    if sl_order:
                        open_positions[symbol]["sl_order_id"] = sl_order["orderId"]
                        open_positions[symbol]["sl_on_exchange"] = True
                if sl_order:
                    tg_log("🛡️ SL تلقائي", f"{symbol}: {auto_sl}", "🛡️")
    except Exception as e:
        log.error(f"import_manual: {e}")

# ============================================================
# MONITOR
# ============================================================
def monitor_loop():
    last_prices = {}
    while True:
        try:
            with state_lock:
                symbols = list(open_positions.keys())
            if not symbols:
                time.sleep(MONITOR_INTERVAL); continue

            for symbol in symbols:
                with state_lock:
                    info = open_positions.get(symbol)
                if not info: continue

                price = get_price(symbol)
                if price <= 0: continue

                update_trailing(symbol, info, price)

                # Fallback SL/TP
                if FALLBACK_ENABLED:
                    side = info["side"]
                    cur_sl = info.get("current_sl")
                    tp1p = info.get("tp1_price")
                    tp2p = info.get("tp2_price")

                    if not info.get("sl_on_exchange") and cur_sl:
                        hit = (side=="LONG" and price <= cur_sl) or (side=="SHORT" and price >= cur_sl)
                        if hit:
                            tg_log("🚨 SL احتياطي", f"{symbol} @ {price}", "🚨")
                            close_position_market(symbol, side, info["qty"])
                            continue

                    if not info.get("tp1_on_exchange") and tp1p and not info.get("tp1_executed"):
                        hit = (side=="LONG" and price >= tp1p) or (side=="SHORT" and price <= tp1p)
                        if hit:
                            half = round_step(symbol, info["qty"]/2)
                            if half > 0:
                                tg_log("🎯 TP1 احتياطي", f"{symbol} @ {price}", "🎯")
                                if close_position_market(symbol, side, half):
                                    with state_lock: info["tp1_executed"] = True

                    if not info.get("tp2_on_exchange") and tp2p and not info.get("tp2_executed"):
                        hit = (side=="LONG" and price >= tp2p) or (side=="SHORT" and price <= tp2p)
                        if hit:
                            rem = round_step(symbol, info["qty"]/2) if info.get("tp1_executed") else info["qty"]
                            if rem > 0:
                                tg_log("🎯 TP2 احتياطي", f"{symbol} @ {price}", "🎯")
                                if close_position_market(symbol, side, rem):
                                    with state_lock: info["tp2_executed"] = True

                # Check closed
                lp = last_prices.get(symbol, 0)
                moved = lp == 0 or abs(price - lp) / price > 0.001
                last_prices[symbol] = price
                if not moved: continue

                try:
                    pos = safe_api_call(client.futures_position_information,
                                       symbol=symbol, weight=5)
                except Exception:
                    continue

                for pp in pos or []:
                    if float(pp["positionAmt"]) == 0:
                        with state_lock:
                            closed = open_positions.pop(symbol, None)
                        if closed:
                            for k in ["sl_order_id", "tp1_order_id", "tp2_order_id"]:
                                oid = closed.get(k)
                                if oid:
                                    try:
                                        safe_api_call(client.futures_cancel_order, symbol=symbol,
                                                     orderId=oid, weight=1)
                                    except Exception:
                                        pass
                            record_trade(symbol, closed)
            time.sleep(MONITOR_INTERVAL)
        except Exception as e:
            log.error(f"monitor: {e}")
            time.sleep(MONITOR_INTERVAL)

# ============================================================
# TRADE HISTORY
# ============================================================
def load_history():
    global trade_history
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE) as f:
                trade_history = json.load(f)
            log.info(f"📚 تحميل {len(trade_history)} صفقة تاريخية")
    except Exception as e:
        log.warning(f"load_history: {e}")

def save_history():
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(trade_history[-500:], f, indent=2, default=str)
    except Exception as e:
        log.warning(f"save_history: {e}")

def record_trade(symbol, info):
    opened = info["opened_at"]
    closed = datetime.now(timezone.utc)
    duration = (closed - opened).total_seconds() / 60

    # Try to fetch realized pnl from Binance
    pnl = 0.0
    try:
        opened_ms = int(opened.timestamp() * 1000)
        trades = safe_api_call(client.futures_account_trades, symbol=symbol,
                              startTime=opened_ms, weight=5)
        for t in trades or []:
            if int(t["time"]) >= opened_ms:
                pnl += float(t["realizedPnl"]) + float(t.get("commission", 0))
    except Exception:
        pnl = 0.0

    record = {
        "symbol": symbol, "strategy": info.get("strategy", "?"),
        "side": info["side"], "entry": info["entry"],
        "qty": info["qty"], "pnl": round(pnl, 4),
        "opened_at": opened.isoformat(), "closed_at": closed.isoformat(),
        "duration_min": round(duration, 1),
        "final_sl": info.get("current_sl"), "stage": info.get("trailing_stage", 0),
    }
    trade_history.append(record)
    save_history()
    bump_stat("trades_closed")

    sign = "+" if pnl >= 0 else ""
    tg_log("✅ إغلاق صفقة",
           f"💠 <b>{symbol}</b>\n"
           f"📊 {info['side']} | {info.get('strategy','?')}\n"
           f"💵 الدخول: {info['entry']}\n"
           f"💰 <b>الربح/الخسارة:</b> {sign}{pnl:.2f} USDT\n"
           f"⏱️ المدة: {duration:.1f} دقيقة",
           "✅")

# ============================================================
# FLASK DASHBOARD
# ============================================================
app = Flask(__name__)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>Trading Bot Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,system-ui,sans-serif;background:#0a0e1a;color:#e6edf3;padding:20px;line-height:1.6}
h1{color:#58a6ff;margin-bottom:20px;font-size:24px}
h2{color:#79c0ff;margin:24px 0 12px;font-size:18px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;margin-bottom:24px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.card .label{color:#8b949e;font-size:12px;text-transform:uppercase}
.card .value{font-size:24px;font-weight:700;margin-top:6px}
.green{color:#3fb950}.red{color:#f85149}.blue{color:#58a6ff}.yellow{color:#d29922}
table{width:100%;border-collapse:collapse;background:#161b22;border-radius:8px;overflow:hidden}
th,td{padding:10px 12px;text-align:right;border-bottom:1px solid #30363d;font-size:14px}
th{background:#21262d;color:#79c0ff;font-weight:600}
tr:last-child td{border-bottom:none}
.tag{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.tag.momentum{background:#1f6feb33;color:#58a6ff}
.tag.ema{background:#a371f733;color:#bc8cff}
.tag.manual{background:#db6d2833;color:#f0883e}
.empty{text-align:center;color:#8b949e;padding:20px}
</style>
</head>
<body>
<h1>🤖 Trading Bot Dashboard</h1>
<div class="cards">
<div class="card"><div class="label">الرصيد</div><div class="value blue">{{balance}} <span style="font-size:14px">USDT</span></div></div>
<div class="card"><div class="label">المتاح</div><div class="value">{{available}}</div></div>
<div class="card"><div class="label">غير محقق</div><div class="value {{pnl_class}}">{{pnl}}</div></div>
<div class="card"><div class="label">صفقات نشطة</div><div class="value yellow">{{active_count}}/{{max_concurrent}}</div></div>
<div class="card"><div class="label">صفقات تاريخية</div><div class="value">{{total_trades}}</div></div>
<div class="card"><div class="label">ربح تراكمي</div><div class="value {{total_pnl_class}}">{{total_pnl}}</div></div>
</div>

<h2>📊 الصفقات النشطة</h2>
<table>
<tr><th>العملة</th><th>الاستراتيجية</th><th>الاتجاه</th><th>الدخول</th><th>الكمية</th><th>SL الحالي</th><th>المرحلة</th></tr>
{% for p in positions %}
<tr>
<td><b>{{p.symbol}}</b></td>
<td><span class="tag {{p.strategy_class}}">{{p.strategy}}</span></td>
<td class="{{'green' if p.side=='LONG' else 'red'}}">{{p.side}}</td>
<td>{{p.entry}}</td>
<td>{{p.qty}}</td>
<td>{{p.current_sl or '—'}}</td>
<td>{{p.trailing_stage}}</td>
</tr>
{% endfor %}
{% if not positions %}<tr><td colspan="7" class="empty">لا صفقات نشطة</td></tr>{% endif %}
</table>

<h2>📜 آخر 20 صفقة</h2>
<table>
<tr><th>الوقت</th><th>العملة</th><th>الاستراتيجية</th><th>الاتجاه</th><th>الدخول</th><th>P&L</th><th>المدة (د)</th></tr>
{% for t in history %}
<tr>
<td>{{t.opened_at[:16]}}</td>
<td><b>{{t.symbol}}</b></td>
<td><span class="tag {{t.strategy_class}}">{{t.strategy}}</span></td>
<td class="{{'green' if t.side=='LONG' else 'red'}}">{{t.side}}</td>
<td>{{t.entry}}</td>
<td class="{{'green' if t.pnl >= 0 else 'red'}}">{{'%+.2f' % t.pnl}}</td>
<td>{{t.duration_min}}</td>
</tr>
{% endfor %}
{% if not history %}<tr><td colspan="7" class="empty">لا تاريخ بعد</td></tr>{% endif %}
</table>

<h2>🎯 إحصائيات الاستراتيجيات</h2>
<table>
<tr><th>الاستراتيجية</th><th>صفقات مفتوحة</th><th>صفقات مغلقة</th><th>P&L تراكمي</th><th>Win Rate</th></tr>
{% for s in strategy_stats %}
<tr>
<td><span class="tag {{s.class}}">{{s.name}}</span></td>
<td>{{s.opened}}</td>
<td>{{s.closed}}</td>
<td class="{{'green' if s.pnl >= 0 else 'red'}}">{{'%+.2f' % s.pnl}}</td>
<td>{{s.winrate}}%</td>
</tr>
{% endfor %}
</table>
</body>
</html>"""

@app.route("/")
def dashboard():
    from flask import render_template_string
    bal = get_balance()
    with state_lock:
        pos = []
        for s, p in open_positions.items():
            strat = p.get("strategy", "?")
            strat_class = "momentum" if "MOMENTUM" in strat else ("ema" if "EMA" in strat else "manual")
            pos.append({
                "symbol": s, "side": p["side"], "entry": p["entry"],
                "qty": p["qty"], "current_sl": p.get("current_sl"),
                "trailing_stage": p.get("trailing_stage", 0),
                "strategy": strat, "strategy_class": strat_class,
            })
        ac = len(open_positions)

    history = list(reversed(trade_history[-20:]))
    for t in history:
        strat = t.get("strategy", "?")
        t["strategy_class"] = "momentum" if "MOMENTUM" in strat else ("ema" if "EMA" in strat else "manual")

    # Strategy stats
    strat_stats = {}
    for t in trade_history:
        s = t.get("strategy", "?")
        if s not in strat_stats:
            strat_stats[s] = {"trades": [], "opened": 0}
        strat_stats[s]["trades"].append(t.get("pnl", 0))
    stat_list = []
    for s, data in strat_stats.items():
        pnls = data["trades"]
        wins = sum(1 for p in pnls if p > 0)
        closed = len(pnls)
        stat_list.append({
            "name": s,
            "class": "momentum" if "MOMENTUM" in s else ("ema" if "EMA" in s else "manual"),
            "opened": sum(1 for p in pos if p["strategy"] == s),
            "closed": closed,
            "pnl": sum(pnls),
            "winrate": round(wins / closed * 100, 1) if closed else 0,
        })

    total_pnl = sum(t.get("pnl", 0) for t in trade_history)

    return render_template_string(
        DASHBOARD_HTML,
        balance=f"{bal['balance']:.2f}",
        available=f"{bal['available']:.2f} USDT",
        pnl=f"{bal['pnl']:+.2f}",
        pnl_class="green" if bal["pnl"] >= 0 else "red",
        active_count=ac, max_concurrent=MAX_CONCURRENT,
        total_trades=len(trade_history),
        total_pnl=f"{total_pnl:+.2f}",
        total_pnl_class="green" if total_pnl >= 0 else "red",
        positions=pos, history=history, strategy_stats=stat_list,
    )

@app.route("/api/stats")
def api_stats():
    with state_lock:
        pos = {s: {k: str(v) if isinstance(v, datetime) else v
                   for k, v in p.items() if not k.endswith("_id")}
               for s, p in open_positions.items()}
    return jsonify({
        "balance": get_balance(), "active": pos,
        "history_count": len(trade_history),
        "stats": _stats,
    })

@app.route("/health")
def health():
    return {"status": "alive", "time": syr_str(), "active": get_active_count()}

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ============================================================
# TELEGRAM COMMANDS (polling)
# ============================================================
def handle_command(text, chat_id):
    text = text.strip().lower()
    if text == "/status" or text == "/start":
        bal = get_balance()
        with state_lock:
            active = list(open_positions.keys())
        stats = _stats.copy()
        msg = (
            f"🤖 <b>حالة البوت</b>\n"
            f"🕐 {syr_str()}\n\n"
            f"💰 الرصيد: {bal['balance']:.2f}\n"
            f"💵 المتاح: {bal['available']:.2f}\n"
            f"📈 غير محقق: {bal['pnl']:+.2f}\n\n"
            f"📊 صفقات نشطة: {len(active)}/{MAX_CONCURRENT}\n"
            f"• {', '.join(active) if active else 'لا يوجد'}\n\n"
            f"📈 إحصائيات:\n"
            f"• فتحت: {stats['trades_opened']}\n"
            f"• أغلقت: {stats['trades_closed']}\n"
            f"• Momentum: {stats['momentum_trades']}\n"
            f"• EMA Cross: {stats['ema_trades']}\n"
            f"• Rate Limit: {stats['rate_limit_hits']}\n"
            f"• Global Pauses: {stats['global_pauses']}\n\n"
            f"🔍 فلاتر:\n"
            f"• حجم: {stats['filtered_vol']}\n"
            f"• ADX: {stats['filtered_adx']}\n"
            f"• ATR: {stats['filtered_atr']}\n"
            f"• Score: {stats['filtered_score']}\n"
            f"• HTF: {stats['filtered_htf']}"
        )
        tg_send(msg)
    elif text == "/positions":
        with state_lock:
            if not open_positions:
                tg_send("📭 لا صفقات نشطة")
                return
            lines = ["<b>📊 الصفقات النشطة:</b>\n"]
            for s, p in open_positions.items():
                lines.append(
                    f"💠 <b>{s}</b> [{p.get('strategy','?')}]\n"
                    f"   {p['side']} @ {p['entry']}\n"
                    f"   SL: {p.get('current_sl')} | Stage: {p.get('trailing_stage',0)}\n"
                )
            tg_send("\n".join(lines))
    elif text == "/clearcache":
        clear_all_caches()
        tg_send("✅ تم تفريغ الكاش والكولداون والإيقاف العالمي")
    elif text == "/balance":
        bal = get_balance()
        tg_send(f"💰 الرصيد: {bal['balance']:.2f}\n💵 المتاح: {bal['available']:.2f}\n📈 غير محقق: {bal['pnl']:+.2f}")
    elif text == "/history":
        if not trade_history:
            tg_send("📭 لا تاريخ بعد")
            return
        recent = trade_history[-10:]
        lines = ["<b>📜 آخر 10 صفقات:</b>\n"]
        for t in reversed(recent):
            sign = "+" if t["pnl"] >= 0 else ""
            lines.append(f"• {t['symbol']} [{t['strategy']}] {t['side']} → {sign}{t['pnl']:.2f}")
        tg_send("\n".join(lines))
    elif text == "/stats":
        with _stats_lock:
            s = dict(_stats)
        total_pnl = sum(t.get("pnl", 0) for t in trade_history)
        msg = f"📊 <b>إحصائيات شاملة</b>\n\n"
        msg += f"💰 P&L تراكمي: {total_pnl:+.2f} USDT\n"
        msg += f"📈 فتحت: {s['trades_opened']} | أغلقت: {s['trades_closed']}\n"
        msg += f"• Momentum: {s['momentum_trades']}\n"
        msg += f"• EMA: {s['ema_trades']}\n"
        msg += f"🚫 Filtered: V={s['filtered_vol']} ADX={s['filtered_adx']} ATR={s['filtered_atr']} Score={s['filtered_score']} HTF={s['filtered_htf']}\n"
        msg += f"⚠️ Rate limits: {s['rate_limit_hits']} | Pauses: {s['global_pauses']}"
        tg_send(msg)
    elif text == "/help":
        tg_send("📖 <b>الأوامر:</b>\n"
                "/status - حالة البوت\n"
                "/positions - الصفقات النشطة\n"
                "/balance - الرصيد\n"
                "/stats - إحصائيات\n"
                "/history - آخر 10 صفقات\n"
                "/clearcache - تفريغ الكاش\n"
                "/help - هذه القائمة")

def tg_polling_loop():
    last_id = 0
    while True:
        try:
            if not TG_BASE:
                time.sleep(60); continue
            r = requests.get(f"{TG_BASE}/getUpdates",
                           params={"offset": last_id + 1, "timeout": 25},
                           timeout=30)
            if r.status_code != 200:
                time.sleep(10); continue
            data = r.json()
            for u in data.get("result", []):
                last_id = u["update_id"]
                msg = u.get("message", {})
                txt = msg.get("text", "")
                chat_id = msg.get("chat", {}).get("id")
                if txt and chat_id:
                    try:
                        handle_command(txt, chat_id)
                    except Exception as e:
                        log.error(f"cmd {txt}: {e}")
        except Exception as e:
            log.debug(f"tg_poll: {e}")
            time.sleep(10)

# ============================================================
# HEARTBEAT
# ============================================================
def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_HOURS * 3600)
        bal = get_balance()
        with state_lock:
            active = list(open_positions.keys())
        s = _stats.copy()
        tg_log("💓 Heartbeat",
               f"📊 {len(active)}/{MAX_CONCURRENT} صفقات\n"
               f"💰 {bal['balance']:.2f} USDT (متاح: {bal['available']:.2f})\n"
               f"📈 P&L: {bal['pnl']:+.2f}\n"
               f"📊 مفتوحة: {s['trades_opened']} | مغلقة: {s['trades_closed']}\n"
               f"⚠️ Rate limits: {s['rate_limit_hits']}",
               "💓")

# ============================================================
# MAIN LOOPS
# ============================================================
def wait_for_candle_close(interval_min):
    now = datetime.now(timezone.utc)
    mins_to = interval_min - (now.minute % interval_min)
    nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=mins_to)
    wait_s = (nxt - now).total_seconds() + 5
    log.info(f"⏳ انتظار إغلاق ({interval_min}m): {wait_s:.0f}s | التالي: {syr_str(nxt)}")
    while wait_s > 0:
        c = min(wait_s, 30)
        time.sleep(c)
        wait_s -= c

def in_session():
    return SESSION_START <= datetime.now(timezone.utc).hour < SESSION_END

def momentum_loop():
    time.sleep(5)
    while True:
        try:
            wait_for_candle_close(int(MOMENTUM_TF.rstrip("m")))
            if in_session():
                momentum_scan()
            else:
                log.info("⏸️ خارج الجلسة — تخطي Momentum")
        except Exception as e:
            log.error(f"momentum_loop: {e}")
            time.sleep(60)

def ema_loop():
    time.sleep(10)
    while True:
        try:
            wait_for_candle_close(5)  # scan every 5m
            if in_session():
                ema_cross_scan()
            else:
                log.info("⏸️ خارج الجلسة — تخطي EMA")
        except Exception as e:
            log.error(f"ema_loop: {e}")
            time.sleep(60)

# ============================================================
# ENTRY
# ============================================================
def main():
    log.info("🚀 بدء البوت الموحّد")
    load_history()

    bal = get_balance()
    tg_log("🤖 بدء البوت الموحّد",
           f"📊 <b>الاستراتيجية:</b> Momentum + EMA Cross\n"
           f"⏱️ Momentum TF: {MOMENTUM_TF} | EMA TFs: {','.join(EMA_TFS)}\n"
           f"💼 حجم: {POSITION_SIZE_USDT} USDT | رافعة: {LEVERAGE}x\n"
           f"🔢 حد الصفقات: {MAX_CONCURRENT}\n"
           f"📋 العملات: {', '.join(SYMBOLS)}\n"
           f"🛡️ Fallback: {'✅' if FALLBACK_ENABLED else '❌'} | "
           f"Trailing: {'✅' if TRAILING_ENABLED else '❌'}\n"
           f"🌐 الوضع: {'TESTNET' if TESTNET else 'LIVE'}\n\n"
           f"💰 الرصيد: {bal['balance']:.2f} USDT\n"
           f"💵 المتاح: {bal['available']:.2f} USDT",
           "🤖")

    import_manual()

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=tg_polling_loop, daemon=True).start()
    threading.Thread(target=momentum_loop, daemon=True).start()
    threading.Thread(target=ema_loop, daemon=True).start()

    log.info("✅ جميع المكونات تعمل")
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
