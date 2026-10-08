"""
Signal Bot v3.0 — Multi-Source Signal Notifier
================================================
- لا تداول، لا Binance، إشارات فقط
- مصادر متعددة: Bybit / OKX / KuCoin / Kraken / Gate.io مع تبديل تلقائي
- إشعارات Telegram مفصّلة
- لوحة تحكم Flask مع سجل الإشارات
"""
import os, json, time, logging, threading, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

def normalize_symbol(s):
    return s.strip().upper().replace("/", "").replace("-", "").replace("_", "").replace(":USDT", "")

_raw_symbols = [s for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
SYMBOLS = []
for _s in _raw_symbols:
    _n = normalize_symbol(_s)
    if _n and _n not in SYMBOLS:
        SYMBOLS.append(_n)

MOMENTUM_TF        = os.getenv("MOMENTUM_TF", "15m")
MOMENTUM_MIN_SCORE = int(os.getenv("MOMENTUM_MIN_SCORE", 6))

EMA_FAST           = int(os.getenv("EMA_FAST", 7))
EMA_SLOW           = int(os.getenv("EMA_SLOW", 25))
EMA_TFS            = [t.strip() for t in os.getenv("EMA_TFS", "5m,15m,1h").split(",") if t.strip()]
EMA_MIN_SCORE      = float(os.getenv("EMA_MIN_SCORE", 55))
EMA_MIN_VOL        = float(os.getenv("EMA_MIN_VOL", 1.0))
EMA_MIN_ADX        = float(os.getenv("EMA_MIN_ADX", 18.0))
EMA_MAX_ATR        = float(os.getenv("EMA_MAX_ATR", 0.7))
EMA_BLOCK_HTF      = os.getenv("EMA_BLOCK_HTF", "true").lower() == "true"

SL_ATR_MULT        = float(os.getenv("SL_ATR_MULT", 1.5))
TP1_ATR_MULT       = float(os.getenv("TP1_ATR_MULT", 1.5))
TP2_ATR_MULT       = float(os.getenv("TP2_ATR_MULT", 3.0))

SESSION_START      = int(os.getenv("SESSION_START_UTC", 7))
SESSION_END        = int(os.getenv("SESSION_END_UTC", 21))

OHLCV_CACHE_SEC    = int(os.getenv("OHLCV_CACHE_SEC", 45))
SIGNAL_COOLDOWN    = int(os.getenv("SIGNAL_COOLDOWN_SEC", 1800))  # 30 دقيقة لكل (رمز+استراتيجية)
HTTP_TIMEOUT       = int(os.getenv("HTTP_TIMEOUT_SEC", 12))
HEARTBEAT_HOURS    = int(os.getenv("HEARTBEAT_HOURS", 6))
SIGNALS_FILE       = os.getenv("SIGNALS_FILE", "/tmp/signal_history.json")

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
log = logging.getLogger("SignalBot")
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ============================================================
# STATE
# ============================================================
signal_history = []
_signal_state = {}          # (symbol, strategy_key) -> last candle_ts
_signal_state_lock = threading.Lock()
_last_cross_candle = {}

_stats = {
    "scans": 0, "signals": 0,
    "momentum_signals": 0, "ema_signals": 0,
    "filtered_vol": 0, "filtered_adx": 0, "filtered_atr": 0,
    "filtered_score": 0, "filtered_htf": 0,
    "source_bybit": 0, "source_okx": 0, "source_kucoin": 0,
    "source_kraken": 0, "source_gate": 0,
    "source_failures": 0,
}
_stats_lock = threading.Lock()

def bump(key, amount=1):
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount

def bump_source(name):
    bump(f"source_{name}")

# ============================================================
# TELEGRAM
# ============================================================
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

def tg_send(text):
    if not TG_BASE or not TELEGRAM_CHAT_ID: return
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
# DATA SOURCES — Multi-Source OHLCV
# ============================================================
class DataSourceError(Exception):
    pass

class BaseSource:
    name = "base"
    priority = 100
    def fetch(self, symbol, tf, limit):
        raise NotImplementedError

# ---- Bybit (Futures Linear) ----
class BybitSource(BaseSource):
    name = "bybit"
    priority = 1
    url = "https://api.bybit.com/v5/market/kline"
    tf_map = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30",
              "1h":"60","2h":"120","4h":"240","6h":"360","12h":"720","1d":"D"}

    def fetch(self, symbol, tf, limit=300):
        interval = self.tf_map.get(tf)
        if not interval:
            raise DataSourceError(f"tf unsupported: {tf}")
        r = requests.get(self.url, params={
            "category": "linear", "symbol": symbol,
            "interval": interval, "limit": min(limit, 1000),
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("retCode") != 0:
            raise DataSourceError(j.get("retMsg", "bybit error"))
        rows = j["result"]["list"][::-1]  # oldest first
        df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","turnover","_"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
        df["open_time"] = pd.to_datetime(df["ts"].astype(float), unit="ms")
        return df[["open_time","open","high","low","close","volume"]].reset_index(drop=True)

# ---- OKX (SWAP) ----
class OKXSource(BaseSource):
    name = "okx"
    priority = 2
    url = "https://www.okx.com/api/v5/market/candles"
    tf_map = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
              "1h":"1H","2h":"2H","4h":"4H","6h":"6H","12h":"12H","1d":"1D"}

    def _sym(self, symbol):
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}-USDT-SWAP"
        return symbol

    def fetch(self, symbol, tf, limit=300):
        bar = self.tf_map.get(tf)
        if not bar:
            raise DataSourceError(f"tf unsupported: {tf}")
        r = requests.get(self.url, params={
            "instId": self._sym(symbol), "bar": bar, "limit": min(limit, 300),
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("code") != "0":
            raise DataSourceError(j.get("msg", "okx error"))
        rows = j["data"][::-1]
        df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","volCcy","volCcyQuote","confirm"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
        df["open_time"] = pd.to_datetime(df["ts"].astype(float), unit="ms")
        return df[["open_time","open","high","low","close","volume"]].reset_index(drop=True)

# ---- KuCoin (Spot) ----
class KuCoinSource(BaseSource):
    name = "kucoin"
    priority = 3
    url = "https://api.kucoin.com/api/v1/market/candles"
    tf_map = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min",
              "1h":"1hour","2h":"2hour","4h":"4hour","1d":"1day"}
    sec_map = {"1m":60,"5m":300,"15m":900,"30m":1800,"1h":3600,"2h":7200,"4h":14400,"1d":86400}

    def _sym(self, symbol):
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}-USDT"
        return symbol

    def fetch(self, symbol, tf, limit=300):
        typ = self.tf_map.get(tf)
        if not typ:
            raise DataSourceError(f"tf unsupported: {tf}")
        end = int(time.time())
        start = end - self.sec_map[tf] * (limit + 10)
        r = requests.get(self.url, params={
            "type": typ, "symbol": self._sym(symbol),
            "startAt": start, "endAt": end,
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("code") != "200000":
            raise DataSourceError(j.get("msg", "kucoin error"))
        rows = j["data"][::-1]
        # KuCoin format: [ts, open, close, high, low, volume, turnover]
        df = pd.DataFrame(rows, columns=["ts","open","close","high","low","volume","turnover"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
        df["open_time"] = pd.to_datetime(df["ts"].astype(float), unit="s")
        return df[["open_time","open","high","low","close","volume"]].reset_index(drop=True)

# ---- Kraken (Spot) ----
class KrakenSource(BaseSource):
    name = "kraken"
    priority = 4
    url = "https://api.kraken.com/0/public/OHLC"
    tf_map = {"1m":1,"5m":5,"15m":15,"30m":30,"1h":60,"4h":240,"1d":1440}

    def _sym(self, symbol):
        if symbol == "BTCUSDT": return "XBTUSDT"
        return symbol

    def fetch(self, symbol, tf, limit=300):
        interval = self.tf_map.get(tf)
        if not interval:
            raise DataSourceError(f"tf unsupported: {tf}")
        r = requests.get(self.url, params={
            "pair": self._sym(symbol), "interval": interval,
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("error"):
            raise DataSourceError(str(j["error"]))
        result = j.get("result", {})
        key = next((k for k in result if k != "last"), None)
        if not key:
            raise DataSourceError("kraken empty result")
        rows = result[key][-limit:]
        df = pd.DataFrame(rows, columns=["ts","open","high","low","close","vwap","volume","count"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
        df["open_time"] = pd.to_datetime(df["ts"].astype(float), unit="s")
        return df[["open_time","open","high","low","close","volume"]].reset_index(drop=True)

# ---- Gate.io (Spot) ----
class GateSource(BaseSource):
    name = "gate"
    priority = 5
    url = "https://api.gateio.ws/api/v4/spot/candlesticks"
    tf_map = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}

    def _sym(self, symbol):
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}_USDT"
        return symbol

    def fetch(self, symbol, tf, limit=300):
        interval = self.tf_map.get(tf)
        if not interval:
            raise DataSourceError(f"tf unsupported: {tf}")
        r = requests.get(self.url, params={
            "currency_pair": self._sym(symbol),
            "interval": interval, "limit": min(limit, 1000),
        }, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        rows = r.json()
        # [ts, quote_vol, close, high, low, open, base_vol, window]
        df = pd.DataFrame(rows, columns=["ts","quote_vol","close","high","low","open","volume","window"])
        for c in ["open","high","low","close","volume"]:
            df[c] = df[c].astype(float)
        df["open_time"] = pd.to_datetime(df["ts"].astype(float), unit="s")
        return df[["open_time","open","high","low","close","volume"]].reset_index(drop=True)

SOURCES = sorted(
    [BybitSource(), OKXSource(), KuCoinSource(), KrakenSource(), GateSource()],
    key=lambda s: s.priority,
)

# ============================================================
# MULTI-SOURCE FETCH WITH FAILOVER
# ============================================================
_ohlcv_cache = {}
_ohlcv_lock = threading.Lock()

def fetch_ohlcv(symbol, tf, limit=300):
    key = (symbol, tf, limit)
    with _ohlcv_lock:
        c = _ohlcv_cache.get(key)
        if c and (time.time() - c["ts"]) < OHLCV_CACHE_SEC:
            return c["data"], c["source"]

    last_err = None
    for src in SOURCES:
        try:
            df = src.fetch(symbol, tf, limit)
            if df is None or len(df) < 30:
                raise DataSourceError("insufficient data")
            with _ohlcv_lock:
                _ohlcv_cache[key] = {"data": df, "source": src.name, "ts": time.time()}
            bump_source(src.name)
            return df, src.name
        except Exception as e:
            last_err = f"{src.name}: {e}"
            log.debug(f"⚠️ {last_err}")
            continue

    bump("source_failures")
    log.warning(f"❌ كل المصادر فشلت لـ {symbol} {tf} — {last_err}")
    return None, None

def clear_caches():
    with _ohlcv_lock:
        _ohlcv_cache.clear()

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
    pdm = h.diff(); ndm = -l.diff()
    pdm = pdm.where((pdm > ndm) & (pdm > 0), 0)
    ndm = ndm.where((ndm > pdm) & (ndm > 0), 0)
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100 * pdm.ewm(alpha=1/p, adjust=False).mean() / atr.replace(0, 1e-9)
    ndi = 100 * ndm.ewm(alpha=1/p, adjust=False).mean() / atr.replace(0, 1e-9)
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, 1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    df = df.copy()
    df["ema7"]   = ema(df["close"], 7)
    df["ema25"]  = ema(df["close"], 25)
    df["ema50"]  = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["ef"]     = ema(df["close"], EMA_FAST)
    df["es"]     = ema(df["close"], EMA_SLOW)
    df["dif"], df["dea"], df["hist"] = macd_series(df["close"])
    df["rsi"]    = rsi_series(df["close"])
    df["atr"]    = atr_series(df)
    df["vwap"]   = vwap_series(df)
    df["vol_ma5"]= df["volume"].rolling(5).mean()
    df["adx"]    = adx_series(df)
    return df

# ============================================================
# MOMENTUM EVALUATION
# ============================================================
def evaluate_momentum(df, direction):
    last, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
    score, reasons = 0, []
    price = float(last["close"])

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
        "price": price,
    }

# ============================================================
# EMA CROSS EVALUATION
# ============================================================
def evaluate_ema_cross(df, direction, trigger_tf):
    last = df.iloc[-1]
    price = float(last["close"])
    score, reasons = 0, []

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

    adx = float(last["adx"])
    if adx >= 35: score += 20; reasons.append(f"✅ ADX {adx:.1f} +20")
    elif adx >= 25: score += 18; reasons.append(f"✅ ADX {adx:.1f} +18")
    elif adx >= 20: score += 12
    elif adx >= 15: score += 5

    gap = abs(last["ef"] - last["es"]) / last["es"] * 100 if last["es"] else 0
    if gap >= 0.3: score += 15; reasons.append(f"✅ فرق EMA {gap:.3f}% +15")
    elif gap >= 0.1: score += 10; reasons.append(f"✅ فرق EMA {gap:.3f}% +10")
    elif gap >= 0.05: score += 6
    else: score += 3

    hist_now = float(last["hist"])
    hist_prev = float(df["hist"].iloc[-2])
    if hist_now > 0 and hist_now > hist_prev: score += 10; reasons.append("✅ MACD صاعد +10")
    elif hist_now < 0 and hist_now > hist_prev: score += 7
    else: score += 5

    rsi = float(last["rsi"])
    if direction == "LONG":
        if 55 <= rsi <= 70: score += 10; reasons.append(f"✅ RSI {rsi:.1f} +10")
        elif rsi >= 45: score += 7
        elif rsi > 70: score += 5
    else:
        if 30 <= rsi <= 45: score += 10; reasons.append(f"✅ RSI {rsi:.1f} +10")
        elif rsi <= 55: score += 7
        elif rsi < 30: score += 5

    ema50 = float(last["ema50"])
    htf_ok = (direction == "LONG" and price > ema50) or (direction == "SHORT" and price < ema50)
    if htf_ok: score += 10; reasons.append("✅ مع EMA50 +10")

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

def detect_ema_cross(df, direction_hint=None):
    curr, prev = -2, -3
    cf, cs = float(df["ef"].iloc[curr]), float(df["es"].iloc[curr])
    pf, ps = float(df["ef"].iloc[prev]), float(df["es"].iloc[prev])

    bullish = (cf > cs) and (pf <= ps)
    bearish = (cf < cs) and (pf >= ps)
    if not (bullish or bearish):
        return None
    return "LONG" if bullish else "SHORT"

# ============================================================
# SIGNAL DEDUP + COOLDOWN
# ============================================================
def should_emit(symbol, strategy_key, candle_ts):
    key = (symbol, strategy_key)
    with _signal_state_lock:
        last_ts = _signal_state.get(key, 0)
        if candle_ts <= last_ts:
            return False
        _signal_state[key] = candle_ts
        return True

# ============================================================
# NOTIFY SIGNAL
# ============================================================
def emit_signal(symbol, direction, signal, strategy, source_name):
    bump("signals")
    if strategy == "MOMENTUM":
        bump("momentum_signals")
        s_emoji = "📈"
    else:
        bump("ema_signals")
        s_emoji = "🔀"

    price = signal["entry"]
    sl_pct = abs(price - signal["sl"]) / price * 100
    tp1_pct = abs(signal["tp1"] - price) / price * 100
    tp2_pct = abs(signal["tp2"] - price) / price * 100
    rr1 = abs(signal["tp1"] - price) / abs(price - signal["sl"]) if signal["sl"] != price else 0
    rr2 = abs(signal["tp2"] - price) / abs(price - signal["sl"]) if signal["sl"] != price else 0

    reasons_txt = "\n".join(signal["reasons"]) if signal["reasons"] else "—"

    body = (
        f"{s_emoji} <b>الاستراتيجية:</b> {strategy}\n"
        f"💠 <b>الرمز:</b> {symbol}\n"
        f"📊 <b>الاتجاه:</b> {direction}\n"
        f"💵 <b>السعر:</b> {price}\n\n"
        f"🛑 <b>SL:</b> {signal['sl']:.6g} ({sl_pct:.2f}%)\n"
        f"🎯 <b>TP1:</b> {signal['tp1']:.6g} ({tp1_pct:.2f}%) R:R 1:{rr1:.2f}\n"
        f"🎯 <b>TP2:</b> {signal['tp2']:.6g} ({tp2_pct:.2f}%) R:R 1:{rr2:.2f}\n\n"
        f"📊 <b>Score:</b> {signal['score']}/{signal['max_score']}\n"
        f"📉 RSI: {signal.get('rsi', 0):.1f} | ADX: {signal.get('adx', 0):.1f}\n"
        f"📦 ATR: {signal.get('atr', 0):.6g}\n"
        f"🔌 <b>المصدر:</b> {source_name}\n\n"
        f"📝 <b>الأسباب:</b>\n{reasons_txt}"
    )
    tg_log("🚨 إشارة جديدة", body, "🚨")

    rec = {
        "symbol": symbol, "strategy": strategy, "direction": direction,
        "price": price, "sl": signal["sl"], "tp1": signal["tp1"], "tp2": signal["tp2"],
        "score": signal["score"], "max_score": signal["max_score"],
        "rsi": signal.get("rsi"), "adx": signal.get("adx"),
        "atr": signal.get("atr"), "atr_pct": signal.get("atr_pct"),
        "vol_ratio": signal.get("vol_ratio"),
        "reasons": signal["reasons"],
        "source": source_name,
        "time": syr_now().isoformat(),
    }
    signal_history.append(rec)
    if len(signal_history) > 1000:
        del signal_history[:500]
    save_signals()

# ============================================================
# SCANNERS
# ============================================================
def scan_momentum():
    bump("scans")
    log.info(f"🔍 [Momentum] مسح {len(SYMBOLS)} × {MOMENTUM_TF}")
    for symbol in SYMBOLS:
        try:
            df, source_name = fetch_ohlcv(symbol, MOMENTUM_TF, 300)
            if df is None or len(df) < 200:
                continue
            df = add_indicators(df)
            closed = df.iloc[:-1]
            candle_ts = int(pd.Timestamp(df["open_time"].iloc[-2]).timestamp())

            longs = evaluate_momentum(closed, "LONG")
            shorts = evaluate_momentum(closed, "SHORT")
            log.info(f"[M] {symbol} ({source_name}): L={longs['score']} S={shorts['score']}")

            if longs["score"] >= MOMENTUM_MIN_SCORE:
                if should_emit(symbol, "MOMENTUM_LONG", candle_ts):
                    emit_signal(symbol, "LONG", longs, "MOMENTUM", source_name)
            elif shorts["score"] >= MOMENTUM_MIN_SCORE:
                if should_emit(symbol, "MOMENTUM_SHORT", candle_ts):
                    emit_signal(symbol, "SHORT", shorts, "MOMENTUM", source_name)
        except Exception as e:
            log.error(f"[M] {symbol}: {e}")

def scan_ema_cross():
    bump("scans")
    log.info(f"🔍 [EMA Cross] مسح {len(SYMBOLS)} × {len(EMA_TFS)}")
    for symbol in SYMBOLS:
        for tf in EMA_TFS:
            try:
                df, source_name = fetch_ohlcv(symbol, tf, EMA_SLOW + 100)
                if df is None or len(df) < EMA_SLOW + 20:
                    continue
                df = add_indicators(df)

                direction = detect_ema_cross(df)
                if not direction:
                    continue

                candle_ts = int(pd.Timestamp(df["open_time"].iloc[-2]).timestamp())
                cross_key = (symbol, tf)
                if _last_cross_candle.get(cross_key) == candle_ts:
                    continue
                _last_cross_candle[cross_key] = candle_ts

                eval_df = df.iloc[:-1]
                sig = evaluate_ema_cross(eval_df, direction, tf)

                if sig["vol_ratio"] < EMA_MIN_VOL:
                    bump("filtered_vol"); continue
                if sig["adx"] < EMA_MIN_ADX:
                    bump("filtered_adx"); continue
                if sig["atr_pct"] > EMA_MAX_ATR:
                    bump("filtered_atr"); continue
                if EMA_BLOCK_HTF and not sig["htf_ok"]:
                    bump("filtered_htf"); continue
                if sig["score"] < EMA_MIN_SCORE:
                    bump("filtered_score"); continue

                strategy_label = f"EMA_{tf}"
                if should_emit(symbol, f"{strategy_label}_{direction}", candle_ts):
                    log.info(f"[E] {symbol} {tf} score={sig['score']} ({source_name})")
                    emit_signal(symbol, direction, sig, strategy_label, source_name)
            except Exception as e:
                log.error(f"[E] {symbol} {tf}: {e}")

# ============================================================
# STORAGE
# ============================================================
def load_signals():
    global signal_history
    try:
        if os.path.exists(SIGNALS_FILE):
            with open(SIGNALS_FILE) as f:
                signal_history = json.load(f)
            log.info(f"📚 تحميل {len(signal_history)} إشارة سابقة")
    except Exception as e:
        log.warning(f"load_signals: {e}")

def save_signals():
    try:
        with open(SIGNALS_FILE, "w") as f:
            json.dump(signal_history[-500:], f, indent=2, default=str)
    except Exception as e:
        log.warning(f"save_signals: {e}")

# ============================================================
# FLASK DASHBOARD
# ============================================================
app = Flask(__name__)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="25">
<title>Signal Bot Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,system-ui,sans-serif;background:#0a0e1a;color:#e6edf3;padding:20px;line-height:1.6}
h1{color:#58a6ff;margin-bottom:10px;font-size:24px}
h2{color:#79c0ff;margin:24px 0 12px;font-size:18px}
.status{display:inline-block;padding:4px 10px;border-radius:12px;font-size:12px;margin-right:8px}
.status.ok{background:#3fb95033;color:#3fb950}
.status.info{background:#58a6ff33;color:#58a6ff}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin-bottom:24px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.card .label{color:#8b949e;font-size:12px;text-transform:uppercase}
.card .value{font-size:22px;font-weight:700;margin-top:6px}
.green{color:#3fb950}.red{color:#f85149}.blue{color:#58a6ff}.yellow{color:#d29922}.purple{color:#bc8cff}
table{width:100%;border-collapse:collapse;background:#161b22;border-radius:8px;overflow:hidden;margin-bottom:20px}
th,td{padding:10px 12px;text-align:right;border-bottom:1px solid #30363d;font-size:13px}
th{background:#21262d;color:#79c0ff;font-weight:600}
tr:last-child td{border-bottom:none}
.tag{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.tag.momentum{background:#1f6feb33;color:#58a6ff}
.tag.ema{background:#a371f733;color:#bc8cff}
.tag.src{background:#30363d;color:#8b949e;font-family:monospace}
.empty{text-align:center;color:#8b949e;padding:20px}
.reason{color:#8b949e;font-size:11px;display:block}
</style>
</head>
<body>
<h1>📡 Signal Bot Dashboard
<span class="status ok">● متصل</span>
<span class="status info">{{ source_count }} مصادر بيانات</span>
</h1>

<div class="cards">
<div class="card"><div class="label">إشارات اليوم</div><div class="value blue">{{ today_signals }}</div></div>
<div class="card"><div class="label">إجمالي الإشارات</div><div class="value">{{ total_signals }}</div></div>
<div class="card"><div class="label">Momentum</div><div class="value purple">{{ stats.momentum_signals }}</div></div>
<div class="card"><div class="label">EMA Cross</div><div class="value purple">{{ stats.ema_signals }}</div></div>
<div class="card"><div class="label">عدد المسحات</div><div class="value">{{ stats.scans }}</div></div>
<div class="card"><div class="label">فشل المصادر</div><div class="value {{ 'red' if stats.source_failures else 'green' }}">{{ stats.source_failures }}</div></div>
</div>

<h2>🔌 حالة المصادر</h2>
<table>
<tr><th>المصدر</th><th>الطلبات الناجحة</th><th>الحالة</th></tr>
{% for s in sources %}
<tr>
<td><b>{{ s.name }}</b></td>
<td>{{ s.count }}</td>
<td>{% if s.count > 0 %}<span class="green">✅ نشط</span>{% else %}<span class="yellow">⏳ لم يُستخدم</span>{% endif %}</td>
</tr>
{% endfor %}
</table>

<h2>🎯 آخر 30 إشارة</h2>
<table>
<tr><th>الوقت</th><th>الرمز</th><th>الاستراتيجية</th><th>الاتجاه</th><th>السعر</th><th>SL</th><th>TP1</th><th>TP2</th><th>Score</th><th>المصدر</th></tr>
{% for s in signals %}
<tr>
<td>{{ s.time[5:16] }}</td>
<td><b>{{ s.symbol }}</b></td>
<td><span class="tag {{ s.strategy_class }}">{{ s.strategy }}</span></td>
<td class="{{ 'green' if s.direction=='LONG' else 'red' }}">{{ s.direction }}</td>
<td>{{ s.price }}</td>
<td class="red">{{ s.sl }}</td>
<td class="green">{{ s.tp1 }}</td>
<td class="green">{{ s.tp2 }}</td>
<td>{{ s.score }}/{{ s.max_score }}</td>
<td><span class="tag src">{{ s.source }}</span></td>
</tr>
{% endfor %}
{% if not signals %}<tr><td colspan="10" class="empty">لا إشارات بعد</td></tr>{% endif %}
</table>

<h2>📊 إحصائيات الفلاتر</h2>
<table>
<tr><th>الفلتر</th><th>العدد</th></tr>
<tr><td>حجم ضعيف</td><td>{{ stats.filtered_vol }}</td></tr>
<tr><td>ADX منخفض</td><td>{{ stats.filtered_adx }}</td></tr>
<tr><td>ATR مرتفع</td><td>{{ stats.filtered_atr }}</td></tr>
<tr><td>Score منخفض</td><td>{{ stats.filtered_score }}</td></tr>
<tr><td>ضد HTF</td><td>{{ stats.filtered_htf }}</td></tr>
</table>
</body>
</html>"""

@app.route("/")
def dashboard():
    with _stats_lock:
        stats = dict(_stats)

    signals_view = []
    for s in reversed(signal_history[-30:]):
        strat = s.get("strategy", "?")
        scls = "momentum" if "MOMENTUM" in strat else "ema"
        signals_view.append({
            "time": s.get("time", ""),
            "symbol": s.get("symbol"),
            "strategy": strat, "strategy_class": scls,
            "direction": s.get("direction"),
            "price": f"{s.get('price', 0):.6g}",
            "sl": f"{s.get('sl', 0):.6g}",
            "tp1": f"{s.get('tp1', 0):.6g}",
            "tp2": f"{s.get('tp2', 0):.6g}",
            "score": s.get("score"),
            "max_score": s.get("max_score"),
            "source": s.get("source", "?"),
        })

    today = syr_now().date().isoformat()
    today_signals = sum(1 for s in signal_history if s.get("time", "").startswith(today))

    sources_view = [
        {"name": "bybit",  "count": stats.get("source_bybit", 0)},
        {"name": "okx",    "count": stats.get("source_okx", 0)},
        {"name": "kucoin", "count": stats.get("source_kucoin", 0)},
        {"name": "kraken", "count": stats.get("source_kraken", 0)},
        {"name": "gate",   "count": stats.get("source_gate", 0)},
    ]

    return render_template_string(
        DASHBOARD_HTML,
        stats=stats,
        signals=signals_view,
        sources=sources_view,
        source_count=len(SOURCES),
        total_signals=len(signal_history),
        today_signals=today_signals,
    )

@app.route("/api/stats")
def api_stats():
    with _stats_lock:
        stats = dict(_stats)
    return jsonify({
        "stats": stats,
        "total_signals": len(signal_history),
        "recent": signal_history[-20:],
        "symbols": SYMBOLS,
        "sources": [s.name for s in SOURCES],
    })

@app.route("/health")
def health():
    return {"status": "alive", "time": syr_str(), "signals": len(signal_history)}

def run_flask():
    port = int(os.getenv("PORT", 10000))
    log.info(f"🌐 Flask على المنفذ {port}")
    try:
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        log.error(f"❌ Flask: {e}")

# ============================================================
# TELEGRAM COMMANDS
# ============================================================
def handle_command(text, chat_id):
    text = text.strip().lower()
    if text in ("/start", "/status"):
        with _stats_lock:
            s = dict(_stats)
        msg = (
            f"📡 <b>Signal Bot v3.0</b>\n"
            f"🕐 {syr_str()}\n\n"
            f"📊 <b>الرموز:</b> {len(SYMBOLS)}\n"
            f"🔍 <b>المسحات:</b> {s['scans']}\n"
            f"🚨 <b>إشارات:</b> {s['signals']}\n"
            f"  • Momentum: {s['momentum_signals']}\n"
            f"  • EMA Cross: {s['ema_signals']}\n\n"
            f"🔌 <b>المصادر:</b>\n"
            f"  • Bybit: {s.get('source_bybit', 0)}\n"
            f"  • OKX: {s.get('source_okx', 0)}\n"
            f"  • KuCoin: {s.get('source_kucoin', 0)}\n"
            f"  • Kraken: {s.get('source_kraken', 0)}\n"
            f"  • Gate: {s.get('source_gate', 0)}\n"
            f"  ⚠️ فشل: {s.get('source_failures', 0)}"
        )
        tg_send(msg)
    elif text == "/signals":
        if not signal_history:
            tg_send("📭 لا إشارات بعد"); return
        lines = ["<b>🚨 آخر 10 إشارات:</b>\n"]
        for s in reversed(signal_history[-10:]):
            lines.append(
                f"• <b>{s['symbol']}</b> [{s['strategy']}] {s['direction']}\n"
                f"  💵 {s['price']} | Score: {s['score']}/{s['max_score']}\n"
                f"  🔌 {s['source']} | {s['time'][11:16]}\n"
            )
        tg_send("\n".join(lines))
    elif text == "/stats":
        with _stats_lock:
            s = dict(_stats)
        tg_send(
            f"📊 <b>إحصائيات</b>\n\n"
            f"🔍 المسحات: {s['scans']}\n"
            f"🚨 الإشارات: {s['signals']}\n"
            f"📉 فلاتر:\n"
            f"  • حجم: {s['filtered_vol']}\n"
            f"  • ADX: {s['filtered_adx']}\n"
            f"  • ATR: {s['filtered_atr']}\n"
            f"  • Score: {s['filtered_score']}\n"
            f"  • HTF: {s['filtered_htf']}"
        )
    elif text == "/sources":
        with _stats_lock:
            s = dict(_stats)
        tg_send(
            f"🔌 <b>حالة المصادر</b>\n\n"
            f"• Bybit:  {s.get('source_bybit', 0)} ✅\n"
            f"• OKX:    {s.get('source_okx', 0)} ✅\n"
            f"• KuCoin: {s.get('source_kucoin', 0)} ✅\n"
            f"• Kraken: {s.get('source_kraken', 0)} ✅\n"
            f"• Gate:   {s.get('source_gate', 0)} ✅\n"
            f"⚠️ فشل: {s.get('source_failures', 0)}"
        )
    elif text == "/clearcache":
        clear_caches()
        tg_send("✅ تم تفريغ كاش OHLCV")
    elif text == "/symbols":
        tg_send(f"📊 <b>الرموز ({len(SYMBOLS)}):</b>\n" + ", ".join(SYMBOLS))
    elif text == "/help":
        tg_send(
            "📖 <b>الأوامر:</b>\n"
            "/status - حالة البوت\n"
            "/signals - آخر 10 إشارات\n"
            "/stats - إحصائيات\n"
            "/sources - حالة المصادر\n"
            "/symbols - قائمة الرموز\n"
            "/clearcache - تفريغ الكاش\n"
            "/help - هذه القائمة"
        )

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
        with _stats_lock:
            s = dict(_stats)
        tg_log(
            "💓 Heartbeat",
            f"🔍 مسحات: {s['scans']}\n"
            f"🚨 إشارات: {s['signals']}\n"
            f"  • Momentum: {s['momentum_signals']}\n"
            f"  • EMA: {s['ema_signals']}\n"
            f"🔌 مصادر: Bybit={s.get('source_bybit',0)} "
            f"OKX={s.get('source_okx',0)} "
            f"KuCoin={s.get('source_kucoin',0)} "
            f"Kraken={s.get('source_kraken',0)} "
            f"Gate={s.get('source_gate',0)}\n"
            f"⚠️ فشل: {s.get('source_failures', 0)}",
            "💓"
        )

# ============================================================
# MAIN LOOPS
# ============================================================
def in_session():
    return SESSION_START <= datetime.now(timezone.utc).hour < SESSION_END

def wait_for_candle_close(interval_min):
    now = datetime.now(timezone.utc)
    mins_to = interval_min - (now.minute % interval_min)
    nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=mins_to)
    wait_s = (nxt - now).total_seconds() + 5
    log.info(f"⏳ انتظار إغلاق ({interval_min}m): {wait_s:.0f}s | {syr_str(nxt)}")
    while wait_s > 0:
        c = min(wait_s, 30)
        time.sleep(c)
        wait_s -= c

def momentum_loop():
    time.sleep(5)
    while True:
        try:
            interval = int(MOMENTUM_TF.rstrip("m")) if MOMENTUM_TF.endswith("m") else 15
            wait_for_candle_close(interval)
            if in_session():
                scan_momentum()
            else:
                log.info("⏸️ خارج الجلسة — Momentum")
        except Exception as e:
            log.error(f"momentum_loop: {e}")
            time.sleep(60)

def ema_loop():
    time.sleep(10)
    while True:
        try:
            wait_for_candle_close(5)
            if in_session():
                scan_ema_cross()
            else:
                log.info("⏸️ خارج الجلسة — EMA")
        except Exception as e:
            log.error(f"ema_loop: {e}")
            time.sleep(60)

# ============================================================
# MAIN
# ============================================================
def main():
    log.info("🚀 بدء Signal Bot v3.0 (Multi-Source)")
    log.info(f"📋 SYMBOLS: {SYMBOLS}")
    log.info(f"🔌 SOURCES: {[s.name for s in SOURCES]}")

    threading.Thread(target=run_flask, daemon=True).start()
    load_signals()

    tg_log(
        "📡 بدء Signal Bot v3.0",
        f"🔍 <b>الاستراتيجيات:</b> Momentum + EMA Cross\n"
        f"⏱️ Momentum: {MOMENTUM_TF} | EMA: {','.join(EMA_TFS)}\n"
        f"📋 <b>الرموز ({len(SYMBOLS)}):</b> {', '.join(SYMBOLS)}\n\n"
        f"🔌 <b>المصادر (بالترتيب):</b>\n"
        + "\n".join(f"  {i+1}. {s.name}" for i, s in enumerate(SOURCES)) +
        f"\n\n🌐 الجلسة: {SESSION_START}:00 → {SESSION_END}:00 UTC",
        "📡"
    )

    threading.Thread(target=tg_polling_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=momentum_loop, daemon=True).start()
    threading.Thread(target=ema_loop, daemon=True).start()

    log.info("✅ جميع المكونات تعمل")

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
