"""
Unified Trading Bot v2.5 — Supabase-First Dashboard
======================================================
v2.5 changes (vs v2.4):
  1. Dashboard reads EVERYTHING from Supabase during ban
  2. Stats/winrate computed from full trade history, not in-memory
  3. Data-source indicator in UI (Live / Supabase / Local)
  4. sb_error surface: silent failures replaced with visible errors
  5. Supabase cache 30s (was 5s) — reduces DB load
  6. Balance cache holds last-known value during ban (not zero)
  7. All v2.4 ban-proof features preserved
"""
import os, re, json, time, logging, threading, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string
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

SUPABASE_URL       = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY       = os.getenv("SUPABASE_KEY", "").strip()

def normalize_binance_symbol(s):
    s = s.strip().upper()
    s = s.replace("/", "").replace(":USDT", "").replace("-", "").replace("_", "")
    return s

_raw_symbols = [s for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
SYMBOLS = []
for _s in _raw_symbols:
    _n = normalize_binance_symbol(_s)
    if _n and _n not in SYMBOLS:
        SYMBOLS.append(_n)

POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", 200))
LEVERAGE           = int(os.getenv("LEVERAGE", 20))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT_TRADES", 3))
TESTNET            = os.getenv("TESTNET", "false").lower() == "true"

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
TRAILING_ENABLED   = os.getenv("TRAILING_ENABLED", "true").lower() == "true"
FALLBACK_ENABLED   = os.getenv("FALLBACK_ENABLED", "true").lower() == "true"
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "true").lower() == "true"

SESSION_START      = int(os.getenv("SESSION_START_UTC", 7))
SESSION_END        = int(os.getenv("SESSION_END_UTC", 21))

MAX_WEIGHT_PER_MIN = int(os.getenv("MAX_WEIGHT_PER_MIN", 1000))
MIN_INTERVAL_SEC   = float(os.getenv("MIN_INTERVAL_SEC", 0.2))
OHLCV_CACHE_SEC    = int(os.getenv("OHLCV_CACHE_SEC", 60))
SYMBOL_COOLDOWN    = int(os.getenv("SYMBOL_COOLDOWN_SEC", 60))
GLOBAL_PAUSE       = int(os.getenv("GLOBAL_PAUSE_SEC", 120))
GLOBAL_PAUSE_TRIG  = int(os.getenv("GLOBAL_PAUSE_TRIGGER", 3))
BACKOFF_BASE       = float(os.getenv("BACKOFF_BASE", 2.0))
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", 3))

MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))
HEARTBEAT_HOURS    = int(os.getenv("HEARTBEAT_HOURS", 4))
HISTORY_FILE       = os.getenv("HISTORY_FILE", "/tmp/trade_history.json")

BAN_STATE_FILE     = os.getenv("BAN_STATE_FILE", "/tmp/ban_state.json")
BALANCE_CACHE_SEC  = int(os.getenv("BALANCE_CACHE_SEC", 30))
DASHBOARD_CACHE_SEC = int(os.getenv("DASHBOARD_CACHE_SEC", 5))
SUPABASE_CACHE_SEC = int(os.getenv("SUPABASE_CACHE_SEC", 30))

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
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ============================================================
# STATE
# ============================================================
open_positions = {}
state_lock = threading.RLock()
_symbol_locks = defaultdict(threading.Lock)
trade_history = []

# ============================================================
# GLOBAL BAN LOCK — PERSISTENT
# ============================================================
_ban_lock = threading.Lock()
_ban_until_ts = 0.0
_notified_ban_ts = 0.0
_ban_state_loaded = False

def _load_ban_state():
    global _ban_until_ts, _notified_ban_ts, _ban_state_loaded
    if _ban_state_loaded:
        return
    _ban_state_loaded = True
    try:
        if os.path.exists(BAN_STATE_FILE):
            with open(BAN_STATE_FILE) as f:
                d = json.load(f)
            _ban_until_ts = float(d.get("until", 0.0))
            _notified_ban_ts = float(d.get("notified", 0.0))
            if _ban_until_ts > time.time():
                remaining = (_ban_until_ts - time.time()) / 60
                log.warning(f"🚫 حظر محفوظ من جلسة سابقة — باقي {remaining:.1f} دقيقة")
            else:
                _ban_until_ts = 0.0
                _notified_ban_ts = 0.0
    except Exception as e:
        log.warning(f"load_ban_state: {e}")

def _save_ban_state():
    try:
        tmp = BAN_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"until": _ban_until_ts, "notified": _notified_ban_ts,
                       "saved_at": time.time()}, f)
        os.replace(tmp, BAN_STATE_FILE)
    except Exception as e:
        log.warning(f"save_ban_state: {e}")

def sb_save_ban_state(until_ts):
    if not _supabase: return
    try:
        _supabase.table("bot_state").upsert({
            "key": "ban_until", "value": str(until_ts),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        log.debug(f"sb_save_ban_state: {e}")

def sb_load_ban_state():
    if not _supabase: return 0.0
    try:
        r = _supabase.table("bot_state").select("value").eq("key", "ban_until").execute()
        if r.data and len(r.data) > 0:
            return float(r.data[0]["value"])
    except Exception as e:
        log.debug(f"sb_load_ban_state: {e}")
    return 0.0

def is_banned():
    global _notified_ban_ts
    with _ban_lock:
        if time.time() >= _ban_until_ts:
            if _notified_ban_ts > 0:
                _notified_ban_ts = 0.0
                _save_ban_state()
            return False
        return True

def ban_remaining():
    with _ban_lock:
        return max(0.0, _ban_until_ts - time.time())

def set_ban(ban_until_dt):
    global _ban_until_ts, _notified_ban_ts
    ban_ts = ban_until_dt.timestamp()
    with _ban_lock:
        extended = ban_ts > _ban_until_ts
        if extended:
            _ban_until_ts = ban_ts
        should_notify = ban_ts > _notified_ban_ts
        if should_notify:
            _notified_ban_ts = ban_ts
    if extended:
        _save_ban_state()
        threading.Thread(target=sb_save_ban_state, args=(ban_ts,), daemon=True).start()
    return should_notify

def clear_ban():
    global _ban_until_ts, _notified_ban_ts
    with _ban_lock:
        _ban_until_ts = 0.0
        _notified_ban_ts = 0.0
    _save_ban_state()

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
# BINANCE CLIENT — SafeClient
# ============================================================
class SafeClient(Client):
    def ping(self):
        return {"serverTime": int(time.time() * 1000)}

def _create_client():
    try:
        c = SafeClient(BINANCE_API_KEY, BINANCE_SECRET_KEY, testnet=TESTNET)
        log.info("✅ Binance SafeClient initialized (no ping)")
        return c
    except Exception as e:
        log.error(f"❌ SafeClient init failed: {e}")
        return None

client = _create_client()

# ============================================================
# SUPABASE
# ============================================================
_supabase = None
_supabase_status = {"ok": False, "error": None, "last_check": 0}
try:
    from supabase import create_client
    if SUPABASE_URL and SUPABASE_KEY:
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        _supabase_status["ok"] = True
        log.info("✅ Supabase متصل")
    else:
        log.warning("⚠️ Supabase غير مهيأ (URL/KEY مفقود)")
except Exception as e:
    log.error(f"❌ Supabase init: {e}")
    _supabase_status["error"] = str(e)

def sb_insert_trade(data):
    if not _supabase: return None
    try:
        r = _supabase.table("trades").insert(data).execute()
        if r.data and len(r.data) > 0:
            return r.data[0].get("id")
    except Exception as e:
        log.error(f"sb_insert_trade: {e}")
    return None

def sb_update_trade(trade_id, data):
    if not _supabase or not trade_id: return False
    try:
        _supabase.table("trades").update(data).eq("id", trade_id).execute()
        return True
    except Exception as e:
        log.error(f"sb_update_trade: {e}")
        return False

def sb_fetch_trades(limit=100, status=None, strategy=None):
    """Raises on error so we can distinguish empty from failure."""
    if not _supabase:
        raise RuntimeError("Supabase not configured")
    q = _supabase.table("trades").select("*").order("opened_at", desc=True).limit(limit)
    if status: q = q.eq("status", status)
    if strategy: q = q.eq("strategy", strategy)
    r = q.execute()
    return r.data or []

def sb_log_event(event_type, message, data=None):
    if not _supabase: return
    try:
        _supabase.table("bot_events").insert({
            "event_type": event_type, "message": message, "data": data or {},
        }).execute()
    except Exception as e:
        log.debug(f"sb_log_event: {e}")

def sb_get_stats():
    """Try strategy_stats view; return [] if missing (not an error)."""
    if not _supabase: return []
    try:
        r = _supabase.table("strategy_stats").select("*").execute()
        return r.data or []
    except Exception as e:
        log.debug(f"sb_get_stats (view may not exist): {e}")
        return []

def sb_get_equity_curve(days=30):
    if not _supabase: return []
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        r = (_supabase.table("trades")
             .select("closed_at,pnl")
             .eq("status", "CLOSED")
             .gte("closed_at", since)
             .order("closed_at")
             .execute())
        trades = r.data or []
        cumulative = 0.0
        curve = []
        for t in trades:
            cumulative += float(t.get("pnl") or 0)
            curve.append({"date": t["closed_at"], "cumulative": round(cumulative, 2)})
        return curve
    except Exception as e:
        log.debug(f"sb_get_equity_curve: {e}")
        return []

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

_balance_cache = {"data": None, "ts": 0}
_balance_lock = threading.Lock()

_dashboard_cache = {"data": None, "ts": 0}
_dashboard_lock = threading.Lock()

# v2.5: Supabase-backed dashboard cache
_supabase_dash_cache = {"data": None, "ts": 0}
_supabase_dash_lock = threading.Lock()

# ============================================================
# COOLDOWN / PAUSE / STATS
# ============================================================
_symbol_cooldown = {}
_global_pause_until = 0.0

_stats = {
    "trades_opened": 0, "trades_closed": 0,
    "momentum_trades": 0, "ema_trades": 0,
    "filtered_vol": 0, "filtered_adx": 0, "filtered_atr": 0,
    "filtered_score": 0, "filtered_htf": 0, "filtered_other": 0,
    "rate_limit_hits": 0, "global_pauses": 0,
    "bans": 0,
}
_stats_lock = threading.Lock()

def bump_stat(key, amount=1):
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount

# ============================================================
# SAFE API CALL
# ============================================================
def safe_api_call(func, *args, weight=1, retries=MAX_RETRIES, cooldown_symbol=None, **kwargs):
    global _global_pause_until

    if is_banned():
        log.debug(f"⛔ محظور — رفض الطلب (باقي {ban_remaining()/60:.1f} د)")
        return None

    now = time.time()
    if now < _global_pause_until:
        log.debug(f"⏸️ إيقاف عالمي — رفض الطلب")
        return None

    if cooldown_symbol:
        if now < _symbol_cooldown.get(cooldown_symbol, 0):
            log.debug(f"⏸️ {cooldown_symbol} في كولداون")
            return None

    if client is None:
        log.error("safe_api_call: client is None")
        return None

    for attempt in range(retries):
        if is_banned():
            return None
        try:
            rate_limiter.throttle()
            rate_limiter.add(weight)
            result = func(*args, **kwargs)
            if cooldown_symbol:
                _symbol_cooldown.pop(cooldown_symbol, None)
            return result

        except AttributeError:
            raise

        except BinanceAPIException as e:
            if e.code == -1003:
                bump_stat("rate_limit_hits")
                msg = str(e.message)
                m = re.search(r"banned until (\d+)", msg)
                if m:
                    ban_until = datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc)
                    should_notify = set_ban(ban_until)
                    if should_notify:
                        bump_stat("bans")
                        wait = ban_until.timestamp() - time.time()
                        log.error(f"🚫 IP محظور حتى {syr_str(ban_until)} (باقي {wait/60:.0f} دقيقة)")
                        tg_log("🚫 IP محظور",
                               f"ينتهي: {syr_str(ban_until)}\n"
                               f"الوقت المتبقي: {wait/60:.0f} دقيقة\n"
                               f"<i>الواجهة تعمل من Supabase خلال الحظر</i>",
                               "🚫")
                        sb_log_event("rate_limit", "IP banned", {"until": ban_until.isoformat()})
                    return None
                else:
                    wait = BACKOFF_BASE * (2 ** attempt)
                    log.warning(f"⚠️ Rate limit — انتظار {wait}s")
                    time.sleep(wait)
                    continue
            elif e.code == -1021:
                time.sleep(1)
                continue
            elif e.code in (-4120, -1102, -1111, -2021):
                raise
            else:
                raise

        except BinanceRequestException:
            wait = BACKOFF_BASE * (2 ** attempt)
            log.warning(f"🌐 Network error — انتظار {wait}s")
            time.sleep(wait)

    raise Exception(f"فشل بعد {retries} محاولات")

# ============================================================
# EXCHANGE INFO
# ============================================================
def get_exchange_info():
    global _exchange_info
    with _exchange_info_lock:
        if _exchange_info is not None:
            return _exchange_info
        if is_banned() or client is None:
            return None
        try:
            _exchange_info = safe_api_call(
                client.futures_exchange_info, weight=1
            )
            return _exchange_info
        except Exception as e:
            log.error(f"get_exchange_info: {e}")
            return None

# ============================================================
# BALANCE — v2.5 keeps last-known value during ban
# ============================================================
def get_balance(use_cache=True):
    """
    v2.5: during ban, returns last cached balance (not zero),
    so dashboard doesn't look dead.
    """
    with _balance_lock:
        if use_cache and _balance_cache["data"] and \
           (time.time() - _balance_cache["ts"]) < BALANCE_CACHE_SEC:
            return _balance_cache["data"]

    # During ban: return stale cache if available, else zeros (marked as stale)
    if is_banned() or client is None:
        if _balance_cache["data"]:
            return _balance_cache["data"]
        return {"balance": 0.0, "available": 0.0, "pnl": 0.0,
                "stale": True}

    try:
        bals = safe_api_call(client.futures_account_balance, weight=5)
        for b in bals or []:
            if b["asset"] == "USDT":
                result = {
                    "balance": float(b["balance"]),
                    "available": float(b["availableBalance"]),
                    "pnl": float(b.get("crossUnPnl", 0)),
                    "stale": False,
                }
                with _balance_lock:
                    _balance_cache["data"] = result
                    _balance_cache["ts"] = time.time()
                return result
    except Exception as e:
        log.error(f"balance: {e}")
    if _balance_cache["data"]:
        return _balance_cache["data"]
    return {"balance": 0.0, "available": 0.0, "pnl": 0.0, "stale": True}

# ============================================================
# FETCH
# ============================================================
def fetch_ohlcv(symbol, tf, limit=200):
    try:
        raw = safe_api_call(client.futures_klines, symbol=symbol, interval=tf,
                           limit=limit, weight=5, cooldown_symbol=symbol)
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

def fetch_ohlcv_cached(symbol, tf, limit=200):
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
    with _balance_lock:
        _balance_cache["data"] = None
        _balance_cache["ts"] = 0
    with _dashboard_lock:
        _dashboard_cache["data"] = None
        _dashboard_cache["ts"] = 0
    with _supabase_dash_lock:
        _supabase_dash_cache["data"] = None
        _supabase_dash_cache["ts"] = 0
    _symbol_cooldown.clear()
    global _global_pause_until
    _global_pause_until = 0.0

# ============================================================
# HELPERS
# ============================================================
def get_price(symbol):
    try:
        t = safe_api_call(client.futures_symbol_ticker, symbol=symbol, weight=1, cooldown_symbol=symbol)
        return float(t["price"]) if t else 0.0
    except Exception:
        return 0.0

def has_open_position(symbol):
    with state_lock:
        return symbol in open_positions

def get_active_count():
    with state_lock:
        return len(open_positions)

def round_step(symbol, qty):
    info = get_exchange_info()
    if not info:
        return round(qty, 3)
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                    prec = int(round(-np.log10(step)))
                    return round(np.floor(qty / step) * step, prec)
    return round(qty, 3)

def round_price(symbol, price):
    info = get_exchange_info()
    if not info:
        return round(price, 4)
    for s in info["symbols"]:
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
# MOMENTUM
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
    if is_banned() or client is None:
        return
    log.info(f"🔍 [Momentum] مسح {len(SYMBOLS)} عملات")
    for symbol in SYMBOLS:
        try:
            if is_banned():
                return
            if get_active_count() >= MAX_CONCURRENT:
                break
            if has_open_position(symbol):
                continue

            df = fetch_ohlcv_cached(symbol, MOMENTUM_TF, 200)
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
# EMA CROSS
# ============================================================
_last_cross_candle = {}

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

    eval_df = df.iloc[:-1]
    signal = evaluate_ema_cross(eval_df, direction, tf)

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
    if is_banned() or client is None:
        return
    log.info(f"🔍 [EMA Cross] مسح {len(SYMBOLS)} × {len(EMA_TFS)}")
    for symbol in SYMBOLS:
        for tf in EMA_TFS:
            try:
                if is_banned():
                    return
                if get_active_count() >= MAX_CONCURRENT:
                    return
                if has_open_position(symbol):
                    continue
                signal = detect_ema_cross(symbol, tf)
                if signal:
                    dir_ = "LONG" if signal["ema_fast"] > signal["ema_slow"] else "SHORT"
                    log.info(f"[E] {symbol} {tf} score={signal['score']}")
                    open_trade(symbol, dir_, signal, f"EMA_{tf}")
            except Exception as e:
                log.error(f"[E] {symbol} {tf}: {e}")

# ============================================================
# ORDERS
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
            log.warning(f"SL attempt {i}: {e.code} {e.message}")
        except Exception as e:
            log.warning(f"SL attempt {i} error: {e}")
            return None
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
            log.warning(f"TP attempt {i}: {e.code} {e.message}")
        except Exception as e:
            log.warning(f"TP attempt {i} error: {e}")
            return None
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
    except Exception as e:
        log.error(f"close {symbol}: {e}")
        return None

# ============================================================
# OPEN TRADE
# ============================================================
def open_trade(symbol, direction, signal, strategy):
    if is_banned() or client is None:
        return
    with _symbol_locks[symbol]:
        if has_open_position(symbol):
            return
        if get_active_count() >= MAX_CONCURRENT:
            return

        bal = get_balance(use_cache=False)
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

            opened_at = datetime.now(timezone.utc)

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
                    "opened_at": opened_at,
                    "strategy": strategy,
                    "source": "BOT",
                    "notional": notional,
                    "in_watchlist": True,
                }

            sb_id = sb_insert_trade({
                "symbol": symbol, "strategy": strategy, "side": direction,
                "entry_price": fill, "qty": qty, "notional": notional,
                "leverage": LEVERAGE, "sl_price": sl,
                "tp1_price": tp1, "tp2_price": tp2,
                "opened_at": opened_at.isoformat(),
                "status": "OPEN", "source": "BOT",
            })
            if sb_id:
                with state_lock:
                    open_positions[symbol]["sb_id"] = sb_id

            bump_stat("trades_opened")
            if strategy == "MOMENTUM":
                bump_stat("momentum_trades")
            else:
                bump_stat("ema_trades")

            if not sl_order:
                tg_log("🚨 صفقة بدون SL على Binance",
                       f"💠 {symbol}\n🛑 SL: {sl}\n📝 البوت سيراقب يدوياً", "🚨")

            sl_pct = abs(fill - sl) / fill * 100
            tp1_pct = abs(tp1 - fill) / fill * 100
            tp2_pct = abs(tp2 - fill) / fill * 100
            rr1 = abs(tp1 - fill) / abs(fill - sl) if sl != fill else 0
            rr2 = abs(tp2 - fill) / abs(fill - sl) if sl != fill else 0

            reasons_txt = "\n".join(signal["reasons"])
            s_emoji = "📈" if strategy == "MOMENTUM" else "🔀"

            body = (
                f"{s_emoji} <b>الاستراتيجية:</b> {strategy}\n"
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
            sb_log_event("trade_failed", f"{symbol} {e.code}", {"message": e.message})
            tg_log("❌ فشل فتح صفقة", f"{symbol}\nCode: {e.code}\n{e.message}", "❌")
        except Exception as e:
            log.exception(f"open_trade {symbol}")
            tg_log("❌ خطأ", f"{symbol}: {e}", "❌")

# ============================================================
# TRAILING
# ============================================================
def update_trailing(symbol, info, price):
    if not TRAILING_ENABLED or not info.get("current_sl"):
        return
    if is_banned() or client is None:
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
# ============================================================
# MANUAL IMPORT — v2.5.1 (with Supabase duplicate guard)
# ============================================================
def import_manual():
    """
    v2.5.1 improvements:
      1. Pre-check Supabase for existing OPEN trades — skip import if found
         (prevents duplicate rows on every restart)
      2. Per-symbol Supabase check — skip symbols already tracked
      3. Better logging so you can see exactly what happened
    """
    if is_banned() or client is None:
        log.warning("import_manual: محظور — تخطي")
        return

    # ────────────────────────────────────────────────────────
    # v2.5.1 STEP 1: Global guard — are there OPEN trades in Supabase?
    # ────────────────────────────────────────────────────────
    if _supabase:
        try:
            existing = (_supabase.table("trades")
                        .select("id", count="exact")
                        .eq("status", "OPEN")
                        .execute())
            existing_count = existing.count or 0
            if existing_count > 0:
                log.info(f"import_manual: يوجد {existing_count} صفقة OPEN في Supabase — "
                         f"تخطي الاستيراد لمنع التكرار")
                # Load the symbols from Supabase so we still know what's tracked
                try:
                    rows = (_supabase.table("trades")
                            .select("symbol,strategy,side,entry_price,qty,sl_price,tp1_price,tp2_price,opened_at,source")
                            .eq("status", "OPEN")
                            .execute())
                    for r in (rows.data or []):
                        sym = r.get("symbol")
                        if not sym or sym in open_positions:
                            continue
                        opened_at_str = r.get("opened_at")
                        try:
                            opened_at = (datetime.fromisoformat(
                                str(opened_at_str).replace("Z", "+00:00"))
                                if opened_at_str else datetime.now(timezone.utc))
                        except Exception:
                            opened_at = datetime.now(timezone.utc)

                        # Rebuild in-memory state from Supabase
                        with state_lock:
                            open_positions[sym] = {
                                "side": r.get("side", "LONG"),
                                "entry": float(r.get("entry_price") or 0),
                                "qty": float(r.get("qty") or 0),
                                "atr": 0.0,                      # unknown from SB
                                "current_sl": (float(r["sl_price"])
                                               if r.get("sl_price") else None),
                                "sl_order_id": None,
                                "tp1_order_id": None,
                                "tp2_order_id": None,
                                "tp1_price": (float(r["tp1_price"])
                                              if r.get("tp1_price") else None),
                                "tp2_price": (float(r["tp2_price"])
                                              if r.get("tp2_price") else None),
                                "sl_on_exchange": False,
                                "tp1_on_exchange": False,
                                "tp2_on_exchange": False,
                                "tp1_executed": False,
                                "tp2_executed": False,
                                "trailing_stage": 0,
                                "opened_at": opened_at,
                                "strategy": r.get("strategy", "MANUAL"),
                                "source": r.get("source", "MANUAL"),
                                "notional": float(r.get("qty") or 0) * float(r.get("entry_price") or 0),
                                "in_watchlist": sym in SYMBOLS,
                                "sb_id": r.get("id"),
                                "restored_from_sb": True,
                            }
                    log.info(f"import_manual: استعيدت {len(open_positions)} صفقة من Supabase إلى الذاكرة")
                except Exception as e:
                    log.warning(f"import_manual: فشل استعادة التفاصيل من Supabase: {e}")
                return
        except Exception as e:
            log.debug(f"import_manual Supabase pre-check: {e}")

    # ────────────────────────────────────────────────────────
    # v2.5.1 STEP 2: No OPEN trades in Supabase → proceed with Binance
    # ────────────────────────────────────────────────────────
    try:
        positions = safe_api_call(client.futures_position_information, weight=5)
        if not positions:
            log.info("import_manual: لا توجد positions أو الطلب فشل")
            return

        imported_count = 0
        skipped_existing = 0
        for p in positions:
            if is_banned():
                log.warning("import_manual: تم اكتشاف حظر أثناء الاستيراد — إيقاف")
                break

            symbol = p["symbol"]
            amt = float(p["positionAmt"])

            if amt == 0:
                continue

            # Skip if already in memory
            with state_lock:
                if symbol in open_positions:
                    skipped_existing += 1
                    continue

            # ────────────────────────────────────────────────────
            # v2.5.1 STEP 3: Per-symbol Supabase check
            # (in case some symbols are already tracked but global check missed)
            # ────────────────────────────────────────────────────
            if _supabase:
                try:
                    sym_row = (_supabase.table("trades")
                               .select("id")
                               .eq("symbol", symbol)
                               .eq("status", "OPEN")
                               .limit(1)
                               .execute())
                    if sym_row.data and len(sym_row.data) > 0:
                        log.info(f"import_manual: {symbol} موجود في Supabase — تخطي")
                        skipped_existing += 1
                        continue
                except Exception as e:
                    log.debug(f"import_manual per-symbol check {symbol}: {e}")

            entry = float(p["entryPrice"])
            side = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            notional = entry * qty
            unrl = float(p.get("unRealizedProfit", 0))

            # ATR for auto-SL
            try:
                df = fetch_ohlcv_cached(symbol, MOMENTUM_TF, 100)
                if df is not None and len(df) > 20:
                    df = add_indicators(df)
                    atr = float(df.iloc[-1]["atr"])
                else:
                    atr = entry * 0.01
            except Exception:
                atr = entry * 0.01

            # Existing SL/TP orders on Binance
            sl = None
            sl_id = None
            tp_orders = []
            try:
                orders = safe_api_call(client.futures_get_open_orders,
                                       symbol=symbol, weight=5)
                for o in orders or []:
                    otype = o["type"]
                    if otype in ("STOP_MARKET", "STOP"):
                        sl = float(o["stopPrice"])
                        sl_id = o["orderId"]
                    elif otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        tp_orders.append({
                            "id": o["orderId"],
                            "price": float(o["stopPrice"]),
                        })
            except Exception:
                pass

            opened_at = datetime.now(timezone.utc)

            tp1_price = tp_orders[0]["price"] if len(tp_orders) > 0 else None
            tp2_price = tp_orders[1]["price"] if len(tp_orders) > 1 else None
            tp1_id    = tp_orders[0]["id"]    if len(tp_orders) > 0 else None
            tp2_id    = tp_orders[1]["id"]    if len(tp_orders) > 1 else None

            # Register in memory
            with state_lock:
                open_positions[symbol] = {
                    "side": side, "entry": entry, "qty": qty, "atr": atr,
                    "current_sl": sl, "sl_order_id": sl_id,
                    "tp1_order_id": tp1_id, "tp2_order_id": tp2_id,
                    "tp1_price": tp1_price, "tp2_price": tp2_price,
                    "sl_on_exchange": sl_id is not None,
                    "tp1_on_exchange": tp1_id is not None,
                    "tp2_on_exchange": tp2_id is not None,
                    "tp1_executed": False, "tp2_executed": False,
                    "trailing_stage": 0,
                    "opened_at": opened_at,
                    "strategy": "MANUAL", "source": "MANUAL",
                    "notional": notional,
                    "in_watchlist": symbol in SYMBOLS,
                }

            # Insert into Supabase
            sb_id = sb_insert_trade({
                "symbol": symbol, "strategy": "MANUAL", "side": side,
                "entry_price": entry, "qty": qty, "notional": notional,
                "leverage": LEVERAGE, "sl_price": sl,
                "tp1_price": tp1_price, "tp2_price": tp2_price,
                "opened_at": opened_at.isoformat(),
                "status": "OPEN", "source": "MANUAL",
                "notes": f"imported manually, pnl={unrl:.2f}",
            })
            if sb_id:
                with state_lock:
                    open_positions[symbol]["sb_id"] = sb_id

            imported_count += 1

            wl = "✅ في القائمة" if symbol in SYMBOLS else "⚠️ خارج القائمة"
            tg_log("📥 استيراد صفقة يدوية",
                   f"💠 <b>{symbol}</b> ({wl})\n"
                   f"📊 {side}\n💵 {entry}\n📦 {qty}\n"
                   f"💰 {unrl:+.2f} USDT\n"
                   f"🛡️ SL: {sl or 'لا يوجد'}\n"
                   f"🎯 TP: {tp1_price or '—'} / {tp2_price or '—'}",
                   "📥")

            # Auto-SL if manual position has none
            if sl is None and AUTO_SL_MANUAL and not is_banned():
                auto_sl = round_price(
                    symbol,
                    entry - 1.5*atr if side == "LONG" else entry + 1.5*atr
                )
                opp = "SELL" if side == "LONG" else "BUY"
                sl_order = place_stop_loss(symbol, opp, auto_sl, qty)
                with state_lock:
                    open_positions[symbol]["current_sl"] = auto_sl
                    if sl_order:
                        open_positions[symbol]["sl_order_id"] = sl_order["orderId"]
                        open_positions[symbol]["sl_on_exchange"] = True
                if sl_order:
                    tg_log("🛡️ SL تلقائي", f"{symbol}: {auto_sl}", "🛡️")
                    sb_update_trade(sb_id, {"sl_price": auto_sl})

        log.info(f"import_manual: استُوردت {imported_count} صفقة جديدة، "
                 f"تم تخطي {skipped_existing} موجودة")

    except Exception as e:
        log.exception(f"import_manual: {e}")
        tg_log("⚠️ خطأ في استيراد الصفقات", str(e), "⚠️")

# ============================================================
# MONITOR
# ============================================================
def monitor_loop():
    last_prices = {}
    while True:
        try:
            if is_banned() or client is None:
                time.sleep(30)
                continue

            with state_lock:
                symbols = list(open_positions.keys())
            if not symbols:
                time.sleep(MONITOR_INTERVAL); continue

            for symbol in symbols:
                if is_banned():
                    break
                with state_lock:
                    info = open_positions.get(symbol)
                if not info: continue

                price = get_price(symbol)
                if price <= 0: continue

                update_trailing(symbol, info, price)

                if FALLBACK_ENABLED:
                    side = info["side"]
                    cur_sl = info.get("current_sl")
                    tp1p = info.get("tp1_price")
                    tp2p = info.get("tp2_price")

                    if not info.get("sl_on_exchange") and cur_sl:
                        hit = (side == "LONG" and price <= cur_sl) or (side == "SHORT" and price >= cur_sl)
                        if hit:
                            tg_log("🚨 SL احتياطي", f"{symbol} @ {price}", "🚨")
                            close_position_market(symbol, side, info["qty"])
                            continue

                    if not info.get("tp1_on_exchange") and tp1p and not info.get("tp1_executed"):
                        hit = (side == "LONG" and price >= tp1p) or (side == "SHORT" and price <= tp1p)
                        if hit:
                            half = round_step(symbol, info["qty"]/2)
                            if half > 0:
                                tg_log("🎯 TP1 احتياطي", f"{symbol} @ {price}", "🎯")
                                if close_position_market(symbol, side, half):
                                    with state_lock: info["tp1_executed"] = True

                    if not info.get("tp2_on_exchange") and tp2p and not info.get("tp2_executed"):
                        hit = (side == "LONG" and price >= tp2p) or (side == "SHORT" and price <= tp2p)
                        if hit:
                            rem = round_step(symbol, info["qty"]/2) if info.get("tp1_executed") else info["qty"]
                            if rem > 0:
                                tg_log("🎯 TP2 احتياطي", f"{symbol} @ {price}", "🎯")
                                if close_position_market(symbol, side, rem):
                                    with state_lock: info["tp2_executed"] = True

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
                                        safe_api_call(client.futures_cancel_order,
                                                     symbol=symbol, orderId=oid, weight=1)
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
            log.info(f"📚 تحميل {len(trade_history)} صفقة من JSON")
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

    pnl = 0.0
    exit_price = None
    try:
        opened_ms = int(opened.timestamp() * 1000)
        trades = safe_api_call(client.futures_account_trades, symbol=symbol,
                              startTime=opened_ms, weight=5)
        for t in trades or []:
            if int(t["time"]) >= opened_ms:
                pnl += float(t["realizedPnl"]) + float(t.get("commission", 0))
                exit_price = float(t["price"])
    except Exception:
        pass

    entry = info["entry"]
    pnl_pct = ((exit_price - entry) / entry * 100) if (exit_price and entry) else 0
    if info["side"] == "SHORT":
        pnl_pct = -pnl_pct

    record = {
        "symbol": symbol, "strategy": info.get("strategy", "?"),
        "side": info["side"], "entry": entry,
        "qty": info["qty"], "pnl": round(pnl, 4),
        "opened_at": opened.isoformat(), "closed_at": closed.isoformat(),
        "duration_min": round(duration, 1),
        "final_sl": info.get("current_sl"), "stage": info.get("trailing_stage", 0),
    }
    trade_history.append(record)
    save_history()
    bump_stat("trades_closed")

    sb_id = info.get("sb_id")
    update_data = {
        "status": "CLOSED",
        "exit_price": exit_price,
        "pnl": round(pnl, 4),
        "pnl_pct": round(pnl_pct, 4),
        "final_sl": info.get("current_sl"),
        "trailing_stage": info.get("trailing_stage", 0),
        "duration_min": round(duration, 1),
        "closed_at": closed.isoformat(),
    }
    if sb_id:
        sb_update_trade(sb_id, update_data)
    else:
        insert_data = {
            "symbol": symbol, "strategy": info.get("strategy", "?"),
            "side": info["side"], "entry_price": entry,
            "qty": info["qty"], "leverage": LEVERAGE,
            "opened_at": opened.isoformat(),
            "source": info.get("source", "BOT"),
        }
        insert_data.update(update_data)
        sb_insert_trade(insert_data)

    sign = "+" if pnl >= 0 else ""
    tg_log("✅ إغلاق صفقة",
           f"💠 <b>{symbol}</b> [{info.get('strategy','?')}]\n"
           f"📊 {info['side']}\n"
           f"💵 الدخول: {entry}\n"
           f"💰 <b>P&L:</b> {sign}{pnl:.2f} USDT ({sign}{pnl_pct:.2f}%)\n"
           f"⏱️ المدة: {duration:.1f} دقيقة",
           "✅")

# ============================================================
# FLASK — PROFESSIONAL DASHBOARD (v2.5 UI)
# ============================================================
app = Flask(__name__)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Trading Bot · Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#080b14; --bg-2:#0d1220; --panel:#111827cc; --panel-solid:#111827;
  --border:#1f2937; --border-2:#2d3748;
  --text:#e5e7eb; --text-dim:#94a3b8; --text-mute:#64748b;
  --brand:#3b82f6; --brand-2:#60a5fa;
  --green:#10b981; --green-bg:#10b98122;
  --red:#ef4444; --red-bg:#ef444422;
  --yellow:#f59e0b; --yellow-bg:#f59e0b22;
  --purple:#a855f7; --purple-bg:#a855f722;
  --cyan:#06b6d4;
  --shadow:0 4px 24px rgba(0,0,0,.4);
  --radius:14px;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{
  font-family:'Inter','Segoe UI',system-ui,sans-serif;
  background:
    radial-gradient(1200px 600px at 90% -10%, #1e3a8a33, transparent 60%),
    radial-gradient(900px 500px at -10% 100%, #7c3aed22, transparent 60%),
    var(--bg);
  color:var(--text);
  min-height:100vh;
  -webkit-font-smoothing:antialiased;
  padding-bottom:60px;
}
.mono{font-family:'JetBrains Mono',monospace}

.header{
  position:sticky;top:0;z-index:50;
  backdrop-filter:blur(14px);
  background:rgba(8,11,20,.75);
  border-bottom:1px solid var(--border);
  padding:14px 24px;
  display:flex;align-items:center;gap:16px;flex-wrap:wrap;
}
.brand{display:flex;align-items:center;gap:12px;font-weight:800;font-size:18px}
.brand .logo{
  width:38px;height:38px;border-radius:11px;
  background:linear-gradient(135deg,#3b82f6,#a855f7);
  display:grid;place-items:center;font-size:20px;
  box-shadow:0 4px 14px #3b82f666;
}
.brand .sub{font-size:11px;color:var(--text-mute);font-weight:500;letter-spacing:.4px}
.pills{display:flex;gap:8px;margin-right:auto;flex-wrap:wrap}
.pill{
  display:inline-flex;align-items:center;gap:6px;
  font-size:12px;font-weight:600;padding:6px 12px;border-radius:999px;
  border:1px solid var(--border);background:var(--panel);
}
.pill .dot{width:7px;height:7px;border-radius:50%;background:var(--text-mute)}
.pill.on .dot{background:var(--green);box-shadow:0 0 0 4px #10b98122}
.pill.off .dot{background:var(--red);box-shadow:0 0 0 4px #ef444422}
.pill.warn{color:var(--yellow);border-color:#f59e0b55;background:var(--yellow-bg)}
.pill.warn .dot{background:var(--yellow);box-shadow:0 0 0 4px #f59e0b22;animation:pulse 1.6s infinite}
.pill.info{color:var(--brand-2);border-color:#3b82f655;background:#3b82f61a}
.pill.info .dot{background:var(--brand-2);box-shadow:0 0 0 4px #3b82f622}
@keyframes pulse{50%{opacity:.4}}

.clock{font-size:12px;color:var(--text-dim);font-weight:600}

.tabs{
  display:flex;gap:4px;padding:16px 24px 0;
  border-bottom:1px solid var(--border);
  overflow-x:auto;scrollbar-width:none;
}
.tabs::-webkit-scrollbar{display:none}
.tab{
  padding:10px 18px;font-size:14px;font-weight:600;color:var(--text-dim);
  background:transparent;border:none;cursor:pointer;border-radius:10px 10px 0 0;
  border-bottom:2px solid transparent;transition:.2s;white-space:nowrap;
  font-family:inherit;
}
.tab:hover{color:var(--text);background:#ffffff08}
.tab.active{color:var(--brand-2);border-bottom-color:var(--brand);background:#3b82f610}

.container{max-width:1440px;margin:0 auto;padding:24px}
.grid{display:grid;gap:16px}
.kpis{grid-template-columns:repeat(auto-fit,minmax(200px,1fr))}
.cols-2{grid-template-columns:2fr 1fr}
.cols-1-1{grid-template-columns:1fr 1fr}
@media (max-width:900px){.cols-2,.cols-1-1{grid-template-columns:1fr}}

/* ---- Alert banner ---- */
.alert{
  display:flex;align-items:center;gap:12px;
  padding:14px 18px;margin-bottom:18px;border-radius:12px;
  font-size:13px;font-weight:500;
  border:1px solid;
}
.alert.info{background:#3b82f61a;border-color:#3b82f655;color:var(--brand-2)}
.alert.warn{background:var(--yellow-bg);border-color:#f59e0b55;color:var(--yellow)}
.alert.error{background:var(--red-bg);border-color:#ef444455;color:var(--red)}
.alert .ic{font-size:20px}
.alert b{font-weight:700}

.kpi{
  background:var(--panel);border:1px solid var(--border);
  border-radius:var(--radius);padding:18px 18px 16px;
  position:relative;overflow:hidden;
  backdrop-filter:blur(10px);
  transition:.25s;
}
.kpi:hover{transform:translateY(-2px);border-color:var(--border-2);box-shadow:var(--shadow)}
.kpi::before{
  content:"";position:absolute;top:0;right:0;width:80px;height:80px;
  background:radial-gradient(circle,var(--accent,#3b82f6) 0%,transparent 70%);
  opacity:.12;transform:translate(30%,-30%);
}
.kpi .label{
  font-size:11px;color:var(--text-mute);font-weight:600;
  text-transform:uppercase;letter-spacing:.8px;display:flex;align-items:center;gap:6px;
}
.kpi .value{
  font-size:26px;font-weight:800;margin-top:8px;
  font-family:'JetBrains Mono',monospace;letter-spacing:-.5px;
}
.kpi .value .unit{font-size:13px;font-weight:600;color:var(--text-dim);margin-right:4px}
.kpi .delta{font-size:12px;font-weight:600;margin-top:6px;display:flex;gap:6px;align-items:center;color:var(--text-dim)}
.kpi.green .value{color:var(--green)}
.kpi.red .value{color:var(--red)}
.kpi.blue .value{color:var(--brand-2)}
.kpi.yellow .value{color:var(--yellow)}
.kpi.purple .value{color:var(--purple)}
.kpi.stale{opacity:.75}
.kpi .stale-badge{
  position:absolute;top:10px;left:10px;font-size:10px;
  background:var(--yellow-bg);color:var(--yellow);
  padding:2px 8px;border-radius:999px;font-weight:700;
}

.panel{
  background:var(--panel);border:1px solid var(--border);
  border-radius:var(--radius);padding:20px;
  backdrop-filter:blur(10px);
}
.panel h2{
  font-size:15px;font-weight:700;margin-bottom:16px;
  display:flex;align-items:center;gap:10px;color:var(--text);
}
.panel h2 .icon{
  width:28px;height:28px;border-radius:8px;display:grid;place-items:center;
  background:#3b82f622;color:var(--brand-2);font-size:14px;
}
.panel h2 .badge{
  margin-right:auto;font-size:11px;font-weight:700;padding:3px 10px;
  border-radius:999px;background:#ffffff10;color:var(--text-dim);
}

.chart-wrap{height:300px;position:relative}
.chart-wrap.sm{height:240px}

.table-wrap{overflow-x:auto;border-radius:10px;border:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:12px 14px;text-align:right;white-space:nowrap}
th{
  background:#0f172a;color:var(--text-dim);font-weight:600;font-size:11px;
  text-transform:uppercase;letter-spacing:.6px;border-bottom:1px solid var(--border);
}
td{border-bottom:1px solid #1e293b88}
tbody tr:hover{background:#ffffff05}
tbody tr:last-child td{border-bottom:none}
.num{font-family:'JetBrains Mono',monospace;font-weight:600}

.tag{
  display:inline-flex;align-items:center;gap:5px;
  padding:3px 9px;border-radius:6px;font-size:11px;font-weight:700;
  letter-spacing:.3px;border:1px solid transparent;
}
.tag.momentum{background:var(--brand)1a;color:var(--brand-2);border-color:#3b82f644}
.tag.ema{background:var(--purple)1a;color:#c084fc;border-color:#a855f744}
.tag.manual{background:var(--yellow)1a;color:#fbbf24;border-color:#f59e0b44}
.tag.long{background:var(--green-bg);color:var(--green);border-color:#10b98144}
.tag.short{background:var(--red-bg);color:var(--red);border-color:#ef444444}

.empty{
  text-align:center;padding:48px 20px;color:var(--text-mute);
}
.empty .icon{font-size:36px;opacity:.4;margin-bottom:10px}
.empty p{font-size:13px}

.bar{
  height:6px;background:#1e293b;border-radius:999px;overflow:hidden;margin-top:10px;
}
.bar>i{display:block;height:100%;background:linear-gradient(90deg,#3b82f6,#a855f7);border-radius:999px;transition:width .5s}

.section-title{
  font-size:12px;font-weight:700;color:var(--text-mute);
  text-transform:uppercase;letter-spacing:1px;margin:24px 0 12px;
}

@keyframes flash{0%{background:#3b82f622}100%{background:transparent}}
.flash{animation:flash 1s}

.hidden{display:none !important}
</style>
</head>
<body>

<div class="header">
  <div class="brand">
    <div class="logo">⚡</div>
    <div>
      <div>Trading Bot</div>
      <div class="sub">UNIFIED v2.5 · SUPABASE-FIRST</div>
    </div>
  </div>
  <div class="pills">
    <span class="pill {{ 'on' if client_ok else 'off' }}"><span class="dot"></span>Binance</span>
    <span class="pill {{ 'on' if supabase_available else 'off' }}"><span class="dot"></span>Supabase</span>
    <span class="pill {{ 'off' if banned else 'on' }}"><span class="dot"></span>{{ 'محظور' if banned else 'نشط' }}</span>
    {% if banned %}<span class="pill warn"><span class="dot"></span>باقي {{ ban_remaining_min }} د</span>{% endif %}
    <span class="pill {{ 'on' if in_session else 'off' }}"><span class="dot"></span>{{ 'جلسة' if in_session else 'خارج الجلسة' }}</span>
  </div>
  <div class="clock mono" id="clock">--:--:--</div>
</div>

<div class="tabs">
  <button class="tab active" data-tab="overview">📊 نظرة عامة</button>
  <button class="tab" data-tab="positions">💼 الصفقات النشطة</button>
  <button class="tab" data-tab="history">📜 التاريخ</button>
  <button class="tab" data-tab="stats">📈 الإحصائيات</button>
</div>

<div class="container">

  <div id="banner-container"></div>

  <div id="tab-overview">
    <div class="grid kpis" id="kpis">
      <div class="kpi blue" id="kpi-balance-card">
        <div class="label">💰 الرصيد الكلي</div>
        <div class="value" id="kpi-balance">— <span class="unit">USDT</span></div>
      </div>
      <div class="kpi yellow">
        <div class="label">💵 المتاح</div>
        <div class="value" id="kpi-available">— <span class="unit">USDT</span></div>
      </div>
      <div class="kpi green" id="kpi-pnl-card">
        <div class="label">📈 P&L غير محقق</div>
        <div class="value" id="kpi-pnl">—</div>
      </div>
      <div class="kpi purple">
        <div class="label">💼 صفقات نشطة</div>
        <div class="value" id="kpi-active">— / {{ max_concurrent }}</div>
        <div class="bar"><i id="kpi-active-bar" style="width:0%"></i></div>
      </div>
      <div class="kpi">
        <div class="label">📊 P&L تراكمي</div>
        <div class="value" id="kpi-total-pnl">—</div>
        <div class="delta" id="kpi-total-pnl-sub">— صفقة</div>
      </div>
      <div class="kpi">
        <div class="label">🎯 نسبة الفوز</div>
        <div class="value" id="kpi-winrate">—</div>
        <div class="delta" id="kpi-winrate-sub">— صفقة</div>
      </div>
    </div>

    <div class="section-title">📊 الأداء</div>
    <div class="grid cols-2">
      <div class="panel">
        <h2><span class="icon">📈</span> منحنى P&L التراكمي <span class="badge">30 يوم</span></h2>
        <div class="chart-wrap"><canvas id="chartEquity"></canvas></div>
      </div>
      <div class="panel">
        <h2><span class="icon">🥧</span> توزيع الاستراتيجيات</h2>
        <div class="chart-wrap sm"><canvas id="chartStrategies"></canvas></div>
      </div>
    </div>
  </div>

  <div id="tab-positions" class="hidden">
    <div class="panel">
      <h2><span class="icon">💼</span> الصفقات النشطة <span class="badge" id="pos-count">0</span></h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>العملة</th><th>الاستراتيجية</th><th>الاتجاه</th>
              <th>الدخول</th><th>الكمية</th><th>الاسمي</th>
              <th>SL الحالي</th><th>المرحلة</th>
            </tr>
          </thead>
          <tbody id="positions-body">
            <tr><td colspan="8" class="empty"><div class="icon">💼</div><p>لا صفقات نشطة</p></td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div id="tab-history" class="hidden">
    <div class="panel">
      <h2><span class="icon">📜</span> آخر الصفقات المغلقة <span class="badge" id="history-badge">50</span></h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>الوقت</th><th>العملة</th><th>الاستراتيجية</th><th>الاتجاه</th>
              <th>الدخول</th><th>P&L</th><th>المدة</th>
            </tr>
          </thead>
          <tbody id="history-body">
            <tr><td colspan="7" class="empty"><div class="icon">📜</div><p>لا تاريخ بعد</p></td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div id="tab-stats" class="hidden">
    <div class="grid cols-1-1">
      <div class="panel">
        <h2><span class="icon">🎯</span> إحصائيات الاستراتيجيات</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>الاستراتيجية</th><th>مفتوحة</th><th>مغلقة</th><th>P&L</th><th>Win Rate</th></tr>
            </thead>
            <tbody id="strategy-body">
              <tr><td colspan="5" class="empty"><div class="icon">📈</div><p>لا إحصائيات</p></td></tr>
            </tbody>
          </table>
        </div>
      </div>
      <div class="panel">
        <h2><span class="icon">⚙️</span> حالة البوت</h2>
        <div class="table-wrap">
          <table>
            <thead><tr><th>المؤشر</th><th>القيمة</th></tr></thead>
            <tbody id="bot-status-body"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

</div>

<script>
const $ = (id)=>document.getElementById(id);
const fmt = (n,d=2)=>Number(n||0).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtSigned = (n,d=2)=>{const v=Number(n||0);return (v>=0?'+':'')+fmt(v,d);};

function tickClock(){
  const d=new Date();
  const s=d.toLocaleTimeString('en-GB',{hour12:false,timeZone:'Asia/Damascus'});
  $('clock').textContent = s + ' (دمشق)';
}
setInterval(tickClock,1000); tickClock();

document.querySelectorAll('.tab').forEach(t=>{
  t.addEventListener('click',()=>{
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    t.classList.add('active');
    document.querySelectorAll('[id^="tab-"]').forEach(x=>x.classList.add('hidden'));
    $('tab-'+t.dataset.tab).classList.remove('hidden');
    if(t.dataset.tab==='overview') setTimeout(renderCharts,50);
  });
});

let chartEquity=null, chartStrategies=null;
function renderCharts(){
  const eq = window.__equity || [];
  const eqCanvas = $('chartEquity');
  if(eqCanvas){
    if(chartEquity) chartEquity.destroy();
    if(eq.length>1){
      chartEquity = new Chart(eqCanvas,{
        type:'line',
        data:{
          labels:eq.map(d=>String(d.date).slice(5,16)),
          datasets:[{
            label:'P&L التراكمي',
            data:eq.map(d=>d.cumulative),
            borderColor:'#3b82f6',
            backgroundColor:(ctx)=>{
              const g=ctx.chart.ctx.createLinearGradient(0,0,0,300);
              g.addColorStop(0,'#3b82f666');g.addColorStop(1,'#3b82f600');return g;
            },
            borderWidth:2.5, fill:true, tension:.35,
            pointRadius:0, pointHoverRadius:5, pointHoverBackgroundColor:'#60a5fa',
          }]
        },
        options:{
          responsive:true, maintainAspectRatio:false,
          interaction:{mode:'index',intersect:false},
          plugins:{
            legend:{display:false},
            tooltip:{backgroundColor:'#111827',borderColor:'#1f2937',borderWidth:1,
              titleColor:'#e5e7eb',bodyColor:'#94a3b8',padding:10,displayColors:false,
              callbacks:{label:(c)=>' P&L: '+fmtSigned(c.parsed.y)+' USDT'}}
          },
          scales:{
            x:{ticks:{color:'#64748b',maxRotation:0,autoSkip:true,maxTicksLimit:8,font:{size:11}},
              grid:{color:'#1f293766',drawBorder:false}},
            y:{ticks:{color:'#64748b',font:{size:11},callback:v=>fmtSigned(v,0)},
              grid:{color:'#1f293766',drawBorder:false}}
          }
        }
      });
    }
  }
  const st = window.__strategyStats || [];
  const stCanvas = $('chartStrategies');
  if(stCanvas && st.length>0){
    if(chartStrategies) chartStrategies.destroy();
    chartStrategies = new Chart(stCanvas,{
      type:'doughnut',
      data:{
        labels:st.map(s=>s.name),
        datasets:[{
          data:st.map(s=>Math.max(s.closed,0)),
          backgroundColor:['#3b82f6','#a855f7','#f59e0b','#10b981','#ef4444','#06b6d4'],
          borderColor:'#0d1220',borderWidth:3,
        }]
      },
      options:{
        responsive:true,maintainAspectRatio:false,cutout:'65%',
        plugins:{
          legend:{position:'bottom',labels:{color:'#94a3b8',font:{size:12},padding:14,boxWidth:12,boxHeight:12,usePointStyle:true}},
          tooltip:{backgroundColor:'#111827',borderColor:'#1f2937',borderWidth:1,
            titleColor:'#e5e7eb',bodyColor:'#94a3b8',padding:10,
            callbacks:{label:(c)=>' '+c.label+': '+c.parsed+' صفقة'}}
        }
      }
    });
  }
}

function renderBanners(d){
  const c = $('banner-container');
  const banners = [];
  if(d.banned){
    banners.push(`<div class="alert warn">
      <span class="ic">🚫</span>
      <div>
        <b>البوت محظور مؤقتاً من Binance API</b> — باقي ${d.ban_remaining_min} دقيقة.<br>
        <span style="opacity:.8">الواجهة تعمل من ${d.data_source_label}. لن يتم أي اتصال بـ Binance حتى انتهاء الحظر.</span>
      </div>
    </div>`);
  }
  if(d.sb_error){
    banners.push(`<div class="alert error">
      <span class="ic">⚠️</span>
      <div>
        <b>خطأ في Supabase:</b> ${d.sb_error}<br>
        <span style="opacity:.8">يتم استخدام البيانات المحلية كبديل — قد تكون ناقصة.</span>
      </div>
    </div>`);
  }
  if(!d.supabase_available){
    banners.push(`<div class="alert info">
      <span class="ic">ℹ️</span>
      <div>
        <b>Supabase غير مهيأ</b> — البيانات معروضة من الذاكرة المحلية فقط.
      </div>
    </div>`);
  }
  c.innerHTML = banners.join('');
}

function renderKPIs(d){
  const b=d.balance||{};
  const stale = b.stale ? ' <span class="stale-badge">آخر قراءة</span>' : '';
  $('kpi-balance').innerHTML = fmt(b.balance) + ' <span class="unit">USDT</span>' + stale;
  $('kpi-available').innerHTML = fmt(b.available) + ' <span class="unit">USDT</span>';
  const pnl = Number(b.pnl||0);
  $('kpi-pnl').textContent = fmtSigned(pnl) + ' USDT';
  $('kpi-pnl-card').className = 'kpi ' + (pnl>=0?'green':'red') + (b.stale?' stale':'');
  $('kpi-balance-card').className = 'kpi blue' + (b.stale?' stale':'');

  const ac = d.active_count||0;
  $('kpi-active').textContent = ac + ' / ' + d.max_concurrent;
  $('kpi-active-bar').style.width = Math.min(100, (ac/d.max_concurrent)*100) + '%';

  $('kpi-total-pnl').textContent = fmtSigned(d.total_pnl) + ' USDT';
  $('kpi-total-pnl-sub').textContent = d.total_trades + ' صفقة مغلقة';

  const wr = d.winrate;
  $('kpi-winrate').textContent = (wr==null?'—':fmt(wr,1)+'%');
  $('kpi-winrate-sub').textContent = d.total_trades + ' صفقة';
}

function strategyTag(s){
  if(s.includes('MOMENTUM')) return '<span class="tag momentum">⚡ Momentum</span>';
  if(s.includes('EMA')) return '<span class="tag ema">🔀 '+s+'</span>';
  return '<span class="tag manual">✋ '+s+'</span>';
}

function renderPositions(positions){
  const tb = $('positions-body');
  $('pos-count').textContent = positions.length;
  if(!positions.length){
    tb.innerHTML = '<tr><td colspan="8" class="empty"><div class="icon">💼</div><p>لا صفقات نشطة</p></td></tr>';
    return;
  }
  tb.innerHTML = positions.map(p=>`
    <tr>
      <td><b>${p.symbol}</b></td>
      <td>${strategyTag(p.strategy)}</td>
      <td><span class="tag ${p.side==='LONG'?'long':'short'}">${p.side==='LONG'?'▲ LONG':'▼ SHORT'}</span></td>
      <td class="num">${p.entry}</td>
      <td class="num">${p.qty}</td>
      <td class="num">${fmt(p.notional)}</td>
      <td class="num">${p.current_sl||'—'}</td>
      <td class="num">${p.trailing_stage||0}</td>
    </tr>
  `).join('');
}

function renderHistory(history){
  const tb = $('history-body');
  $('history-badge').textContent = history.length;
  if(!history.length){
    tb.innerHTML = '<tr><td colspan="7" class="empty"><div class="icon">📜</div><p>لا تاريخ بعد</p></td></tr>';
    return;
  }
  tb.innerHTML = history.map(t=>{
    const pnl = Number(t.pnl||0);
    return `<tr>
      <td class="num" style="color:#64748b;font-size:12px">${t.opened_at}</td>
      <td><b>${t.symbol}</b></td>
      <td>${strategyTag(t.strategy)}</td>
      <td><span class="tag ${t.side==='LONG'?'long':'short'}">${t.side}</span></td>
      <td class="num">${t.entry}</td>
      <td class="num" style="color:${pnl>=0?'#10b981':'#ef4444'};font-weight:700">${fmtSigned(pnl)}</td>
      <td class="num" style="color:#64748b">${t.duration_min}د</td>
    </tr>`;
  }).join('');
}

function renderStrategyStats(stats){
  const tb = $('strategy-body');
  if(!stats.length){
    tb.innerHTML = '<tr><td colspan="5" class="empty"><div class="icon">📈</div><p>لا إحصائيات</p></td></tr>';
    return;
  }
  tb.innerHTML = stats.map(s=>{
    const pnl = Number(s.pnl||0);
    return `<tr>
      <td>${strategyTag(s.name)}</td>
      <td class="num">${s.opened}</td>
      <td class="num">${s.closed}</td>
      <td class="num" style="color:${pnl>=0?'#10b981':'#ef4444'};font-weight:700">${fmtSigned(pnl)}</td>
      <td class="num">${fmt(s.winrate,1)}%</td>
    </tr>`;
  }).join('');
}

function renderBotStatus(d){
  const srcLabel = d.data_source_label || '—';
  const rows = [
    ['🔌 Binance Client', d.client_ok?'✅ متصل':'❌ خطأ'],
    ['🗄️ Supabase', d.supabase_available?'✅ متصل':'❌ غير مهيأ'],
    ['📡 مصدر البيانات', srcLabel],
    ['🚫 حالة الحظر', d.banned?('محظور — باقي '+d.ban_remaining_min+' دقيقة'):'✅ غير محظور'],
    ['📅 داخل الجلسة', d.in_session?'✅ نعم':'⏸️ لا'],
    ['⚡ صفقات Momentum', d.stats.momentum_trades],
    ['🔀 صفقات EMA', d.stats.ema_trades],
    ['📊 مفتوحة (هذه الجلسة)', d.stats.trades_opened],
    ['✅ مغلقة (هذه الجلسة)', d.stats.trades_closed],
    ['⚠️ Rate limit hits', d.stats.rate_limit_hits],
    ['🚫 عدد حالات الحظر', d.stats.bans||0],
  ];
  $('bot-status-body').innerHTML = rows.map(r=>
    `<tr><td style="color:#94a3b8">${r[0]}</td><td class="num" style="text-align:left">${r[1]}</td></tr>`
  ).join('');
}

async function refresh(){
  try{
    const r = await fetch('/api/dashboard',{cache:'no-store'});
    if(!r.ok) return;
    const d = await r.json();
    window.__equity = d.equity_curve||[];
    window.__strategyStats = d.strategy_stats||[];
    renderBanners(d);
    renderKPIs(d);
    renderPositions(d.positions||[]);
    renderHistory(d.history||[]);
    renderStrategyStats(d.strategy_stats||[]);
    renderBotStatus(d);
    if(!document.getElementById('tab-overview').classList.contains('hidden')){
      renderCharts();
    }
  }catch(e){console.error('refresh error',e);}
}

refresh();
setInterval(refresh,10000);
</script>
</body>
</html>"""

# ============================================================
# API + ROUTES — v2.5 SUPABASE-FIRST
# ============================================================
def _strategy_class(name):
    if "MOMENTUM" in name: return "momentum"
    if "EMA" in name: return "ema"
    return "manual"

def _fetch_supabase_dashboard_data():
    """
    v2.5: Returns a dict with trades/history/stats/equity_curve
    from Supabase, plus a possible error message.
    Raises nothing — always returns a dict with 'error' set if failed.
    """
    result = {
        "closed_trades": [],
        "open_trades": [],
        "equity_curve": [],
        "strategy_stats": [],
        "error": None,
    }

    if not _supabase:
        result["error"] = "Supabase not configured"
        return result

    # Fetch CLOSED trades (for history + stats + equity curve)
    try:
        closed = sb_fetch_trades(limit=500, status="CLOSED")
        result["closed_trades"] = closed
    except Exception as e:
        result["error"] = f"trades query failed: {e}"
        log.error(f"sb dashboard trades: {e}")
        return result

    # Fetch OPEN trades (for positions fallback if memory empty)
    try:
        open_t = sb_fetch_trades(limit=100, status="OPEN")
        result["open_trades"] = open_t
    except Exception as e:
        log.debug(f"sb open trades: {e}")

    # Try strategy_stats view (optional)
    try:
        result["strategy_stats"] = sb_get_stats()
    except Exception:
        result["strategy_stats"] = []

    # Build equity curve from closed trades (server-side, no extra query)
    try:
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=30)
        cumulative = 0.0
        curve = []
        for t in sorted(closed, key=lambda x: x.get("closed_at") or ""):
            closed_at = t.get("closed_at")
            if not closed_at: continue
            try:
                dt = datetime.fromisoformat(str(closed_at).replace("Z", "+00:00"))
            except Exception:
                continue
            if dt < since: continue
            cumulative += float(t.get("pnl") or 0)
            curve.append({"date": closed_at, "cumulative": round(cumulative, 2)})
        result["equity_curve"] = curve
    except Exception as e:
        log.debug(f"equity curve build: {e}")

    return result

def _build_strategy_stats_from_trades(trades):
    """Build strategy stats from a list of closed trades."""
    agg = {}
    for t in trades:
        s = t.get("strategy") or "?"
        agg.setdefault(s, []).append(float(t.get("pnl") or 0))
    stats = []
    for s, pnls in agg.items():
        closed = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        stats.append({
            "name": s,
            "class": _strategy_class(s),
            "opened": 0,
            "closed": closed,
            "pnl": sum(pnls),
            "winrate": round(wins / closed * 100, 1) if closed else 0,
        })
    return stats

def _build_dashboard_data():
    """v2.5: Supabase-first, falls back to memory only if SB fails."""
    bal = get_balance(use_cache=True)

    # Live positions (from memory — the source of truth for what's open NOW)
    with state_lock:
        pos = []
        for s, p in open_positions.items():
            pos.append({
                "symbol": s, "side": p["side"], "entry": p["entry"],
                "qty": p["qty"], "current_sl": p.get("current_sl"),
                "trailing_stage": p.get("trailing_stage", 0),
                "strategy": p.get("strategy", "?"),
                "notional": round(p.get("notional", 0), 2),
            })
        ac = len(open_positions)

    # Initialize
    history = []
    strategy_stats = []
    equity_curve = []
    total_pnl = 0
    total_trades = 0
    winrate = None
    sb_error = None
    data_source = "memory"

    # ---------- Try Supabase FIRST ----------
    sb_data = None
    if _supabase:
        try:
            sb_data = _fetch_supabase_dashboard_data()
            if sb_data["error"]:
                sb_error = sb_data["error"]
                sb_data = None
        except Exception as e:
            sb_error = str(e)
            log.exception(f"sb dashboard fetch: {e}")
            sb_data = None

    if sb_data:
        # History from Supabase closed trades
        for t in sb_data["closed_trades"][:100]:
            strat = t.get("strategy") or "?"
            history.append({
                "symbol": t.get("symbol", "?"),
                "strategy": strat,
                "side": t.get("side", "?"),
                "entry": t.get("entry_price", 0),
                "pnl": float(t.get("pnl") or 0),
                "duration_min": float(t.get("duration_min") or 0),
                "opened_at": str(t.get("opened_at", ""))[:16].replace("T", " "),
            })

        # Stats from view, or computed from trades
        if sb_data["strategy_stats"]:
            for s in sb_data["strategy_stats"]:
                name = s.get("strategy") or "?"
                strategy_stats.append({
                    "name": name,
                    "class": _strategy_class(name),
                    "opened": int(s.get("open_trades", 0) or 0),
                    "closed": int(s.get("closed_trades", 0) or 0),
                    "pnl": float(s.get("total_pnl") or 0),
                    "winrate": float(s.get("winrate") or 0),
                })
        else:
            # Compute from closed trades (much more reliable)
            strategy_stats = _build_strategy_stats_from_trades(sb_data["closed_trades"])

        # Equity curve from Supabase
        equity_curve = sb_data["equity_curve"]

        # Totals — from ALL Supabase closed trades, not just the 100 shown
        all_closed = sb_data["closed_trades"]
        total_pnl = sum(float(t.get("pnl") or 0) for t in all_closed)
        total_trades = len(all_closed)
        wins = sum(1 for t in all_closed if float(t.get("pnl") or 0) > 0)
        winrate = round(wins / total_trades * 100, 1) if total_trades else None
        data_source = "supabase"

    # ---------- Fallback to local JSON if Supabase unavailable ----------
    if data_source != "supabase":
        for t in reversed(trade_history[-100:]):
            strat = t.get("strategy") or "?"
            history.append({
                "symbol": t.get("symbol", "?"),
                "strategy": strat,
                "side": t.get("side", "?"),
                "entry": t.get("entry", 0),
                "pnl": float(t.get("pnl") or 0),
                "duration_min": float(t.get("duration_min") or 0),
                "opened_at": t.get("opened_at", "")[:16].replace("T", " "),
            })
        strategy_stats = _build_strategy_stats_from_trades(trade_history)
        total_pnl = sum(float(t.get("pnl") or 0) for t in trade_history)
        total_trades = len(trade_history)
        wins = sum(1 for t in trade_history if float(t.get("pnl") or 0) > 0)
        winrate = round(wins / total_trades * 100, 1) if total_trades else None
        # Build equity curve locally
        cumulative = 0.0
        for t in trade_history[-200:]:
            cumulative += float(t.get("pnl") or 0)
            equity_curve.append({
                "date": t.get("closed_at") or t.get("opened_at", ""),
                "cumulative": round(cumulative, 2),
            })
        data_source = "local"

    # Data source label
    if data_source == "supabase":
        data_source_label = "🗄️ Supabase (كامل)"
    elif data_source == "local":
        data_source_label = "📁 JSON محلي"
    else:
        data_source_label = "🧠 الذاكرة الحية"

    # If banned, note it in the label
    if is_banned():
        data_source_label += " · 🚫 حظر نشط"

    return {
        "balance": bal,
        "active_count": ac,
        "max_concurrent": MAX_CONCURRENT,
        "positions": pos,
        "history": history,
        "strategy_stats": strategy_stats,
        "equity_curve": equity_curve,
        "total_pnl": total_pnl,
        "total_trades": total_trades,
        "winrate": winrate,
        "stats": dict(_stats),
        "supabase": _supabase is not None,
        "supabase_available": _supabase is not None,
        "sb_error": sb_error,
        "client_ok": client is not None,
        "banned": is_banned(),
        "ban_remaining_min": int(ban_remaining() / 60) if is_banned() else 0,
        "in_session": SESSION_START <= datetime.now(timezone.utc).hour < SESSION_END,
        "data_source": data_source,
        "data_source_label": data_source_label,
    }

@app.route("/")
def dashboard():
    data = _build_dashboard_data()
    return render_template_string(
        DASHBOARD_HTML,
        max_concurrent=MAX_CONCURRENT,
        supabase_available=data["supabase_available"],
        client_ok=data["client_ok"],
        banned=data["banned"],
        ban_remaining_min=data["ban_remaining_min"],
        in_session=data["in_session"],
    )

@app.route("/api/dashboard")
def api_dashboard():
    # v2.5: cache dashboard payload 5s, but Supabase data itself cached 30s
    with _dashboard_lock:
        if _dashboard_cache["data"] and \
           (time.time() - _dashboard_cache["ts"]) < DASHBOARD_CACHE_SEC:
            return jsonify(_dashboard_cache["data"])

    data = _build_dashboard_data()

    with _dashboard_lock:
        _dashboard_cache["data"] = data
        _dashboard_cache["ts"] = time.time()
    return jsonify(data)

@app.route("/api/stats")
def api_stats():
    with state_lock:
        pos = {s: {k: str(v) if isinstance(v, datetime) else v
                   for k, v in p.items() if not k.endswith("_id")}
               for s, p in open_positions.items()}
    return jsonify({
        "balance": get_balance(use_cache=True),
        "active": pos,
        "stats": _stats,
        "supabase": _supabase is not None,
        "banned": is_banned(),
        "ban_remaining_min": int(ban_remaining() / 60) if is_banned() else 0,
    })

@app.route("/health")
def health():
    return {"status": "alive", "time": syr_str(),
            "active": get_active_count(),
            "supabase": _supabase is not None,
            "banned": is_banned(),
            "ban_remaining_min": int(ban_remaining() / 60) if is_banned() else 0,
            "client_ok": client is not None}

def run_flask():
    port = int(os.getenv("PORT", 10000))
    log.info(f"🌐 بدء Flask على المنفذ {port}")
    try:
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        log.error(f"❌ Flask فشل: {e}")

# ============================================================
# TELEGRAM COMMANDS
# ============================================================
def handle_command(text, chat_id):
    text = text.strip().lower()
    if text in ("/status", "/start"):
        bal = get_balance(use_cache=True)
        with state_lock:
            active = list(open_positions.keys())
        stats = dict(_stats)
        ban_line = ""
        if is_banned():
            ban_line = f"🚫 <b>محظور:</b> باقي {ban_remaining()/60:.1f} دقيقة\n\n"
        msg = (
            f"🤖 <b>حالة البوت v2.5</b>\n"
            f"🕐 {syr_str()}\n\n"
            f"{ban_line}"
            f"🔌 Client: {'✅' if client else '❌'}\n"
            f"🗄️ Supabase: {'✅' if _supabase else '❌'}\n"
            f"💰 الرصيد: {bal['balance']:.2f}\n"
            f"💵 المتاح: {bal['available']:.2f}\n"
            f"📈 غير محقق: {bal['pnl']:+.2f}\n\n"
            f"📊 صفقات نشطة: {len(active)}/{MAX_CONCURRENT}\n"
            f"• {', '.join(active) if active else 'لا يوجد'}\n\n"
            f"📈 إحصائيات (هذه الجلسة):\n"
            f"• فتحت: {stats['trades_opened']}\n"
            f"• أغلقت: {stats['trades_closed']}\n"
            f"• Momentum: {stats['momentum_trades']}\n"
            f"• EMA Cross: {stats['ema_trades']}\n"
            f"• Rate Limit: {stats['rate_limit_hits']}\n"
            f"• Bans: {stats.get('bans', 0)}"
        )
        tg_send(msg)
    elif text == "/positions":
        with state_lock:
            if not open_positions:
                tg_send("📭 لا صفقات نشطة"); return
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
        tg_send("✅ تم تفريغ الكاش (بدون حظر)")
    elif text == "/clearban":
        clear_ban()
        tg_send("✅ تم مسح الحظر المحفوظ")
    elif text == "/baninfo":
        if is_banned():
            tg_send(f"🚫 محظور — باقي {ban_remaining()/60:.1f} دقيقة")
        else:
            tg_send("✅ لا يوجد حظر حالياً")
    elif text == "/balance":
        bal = get_balance(use_cache=False)
        stale = " (آخر قراءة — البوت محظور)" if bal.get("stale") else ""
        tg_send(f"💰 الرصيد: {bal['balance']:.2f}{stale}\n"
                f"💵 المتاح: {bal['available']:.2f}\n"
                f"📈 غير محقق: {bal['pnl']:+.2f}")
    elif text == "/stats":
        s = dict(_stats)
        total_pnl = sum(t.get("pnl", 0) for t in trade_history)
        msg = f"📊 <b>إحصائيات شاملة</b>\n\n"
        msg += f"💰 P&L تراكمي (محلي): {total_pnl:+.2f} USDT\n"
        msg += f"📈 فتحت (هذه الجلسة): {s['trades_opened']} | أغلقت: {s['trades_closed']}\n"
        msg += f"• Momentum: {s['momentum_trades']}\n"
        msg += f"• EMA: {s['ema_trades']}\n"
        msg += f"⚠️ Rate limits: {s['rate_limit_hits']} | Bans: {s.get('bans', 0)}"
        tg_send(msg)
    elif text == "/help":
        tg_send("📖 <b>الأوامر:</b>\n"
                "/status - حالة البوت\n"
                "/positions - الصفقات النشطة\n"
                "/balance - الرصيد\n"
                "/stats - إحصائيات عامة\n"
                "/baninfo - حالة الحظر\n"
                "/clearban - مسح الحظر المحفوظ\n"
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
        if is_banned():
            tg_log("💓 Heartbeat (محظور)",
                   f"🚫 البوت محظور — باقي {ban_remaining()/60:.1f} دقيقة\n"
                   f"<i>الواجهة تعمل من Supabase</i>", "💓")
            continue
        bal = get_balance(use_cache=False)
        with state_lock:
            active = list(open_positions.keys())
        s = dict(_stats)
        tg_log("💓 Heartbeat",
               f"📊 {len(active)}/{MAX_CONCURRENT} صفقات\n"
               f"💰 {bal['balance']:.2f} USDT (متاح: {bal['available']:.2f})\n"
               f"📈 P&L: {bal['pnl']:+.2f}\n"
               f"📊 مفتوحة: {s['trades_opened']} | مغلقة: {s['trades_closed']}\n"
               f"⚠️ Rate limits: {s['rate_limit_hits']} | Bans: {s.get('bans', 0)}",
               "💓")

# ============================================================
# MAIN LOOPS
# ============================================================
def wait_for_candle_close(interval_min):
    now = datetime.now(timezone.utc)
    mins_to = interval_min - (now.minute % interval_min)
    nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=mins_to)
    wait_s = (nxt - now).total_seconds() + 5
    log.info(f"⏳ انتظار إغلاق ({interval_min}m): {wait_s:.0f}s | {syr_str(nxt)}")
    while wait_s > 0:
        if is_banned():
            return
        c = min(wait_s, 30)
        time.sleep(c)
        wait_s -= c

def in_session():
    return SESSION_START <= datetime.now(timezone.utc).hour < SESSION_END

def momentum_loop():
    time.sleep(5)
    while True:
        try:
            if is_banned():
                time.sleep(60); continue
            interval = int(MOMENTUM_TF.rstrip("m")) if MOMENTUM_TF.endswith("m") else 15
            wait_for_candle_close(interval)
            if is_banned(): continue
            if in_session():
                momentum_scan()
            else:
                log.info("⏸️ خارج الجلسة — Momentum")
        except Exception as e:
            log.error(f"momentum_loop: {e}")
            time.sleep(60)

def ema_loop():
    time.sleep(10)
    while True:
        try:
            if is_banned():
                time.sleep(60); continue
            wait_for_candle_close(5)
            if is_banned(): continue
            if in_session():
                ema_cross_scan()
            else:
                log.info("⏸️ خارج الجلسة — EMA")
        except Exception as e:
            log.error(f"ema_loop: {e}")
            time.sleep(60)

def wait_for_ban_to_end():
    if not is_banned():
        return
    try:
        sb_until = sb_load_ban_state()
        if sb_until > 0 and sb_until > time.time():
            ban_dt = datetime.fromtimestamp(sb_until, tz=timezone.utc)
            set_ban(ban_dt)
            log.warning(f"🚫 حظر مستعاد من Supabase — حتى {syr_str(ban_dt)}")
    except Exception:
        pass

    if not is_banned():
        return

    remaining = ban_remaining()
    log.warning(f"🚫⏸️  البوت في وضع الانتظار — حظر نشط، باقي {remaining/60:.1f} دقيقة")
    tg_log("🚫⏸️ وضع انتظار الحظر",
           f"البوت لن يتصل بـ Binance حتى انتهاء الحظر\n"
           f"المتبقي: {remaining/60:.1f} دقيقة\n"
           f"<i>الواجهة تعمل بشكل كامل من Supabase</i>",
           "⏸️")

    while is_banned():
        wait = min(ban_remaining(), 300)
        if wait <= 0:
            break
        log.info(f"⏸️ نائم {wait:.0f}s — باقي {ban_remaining()/60:.1f} دقيقة للحظر")
        time.sleep(wait + 5)

    log.info("✅ انتهى الحظر — البوت جاهز للعمل")
    tg_log("✅ انتهى الحظر", "البوت يستأنف العمل الآن", "✅")

# ============================================================
# MAIN
# ============================================================
def main():
    log.info("🚀 بدء البوت الموحّد v2.5 (Supabase-First Dashboard)")

    _load_ban_state()
    log.info(f"📋 SYMBOLS (مطبّعة): {SYMBOLS}")
    log.info(f"🔌 Client: {'✅ OK' if client else '❌ فشل'}")

    # Flask FIRST — dashboard works even during ban
    threading.Thread(target=run_flask, daemon=True).start()
    log.info("✅ Flask يعمل — المنفذ مفتوح (Dashboard يعمل من Supabase)")

    load_history()

    wait_for_ban_to_end()

    bal = get_balance(use_cache=False)
    sb_status = "✅ متصل" if _supabase else "❌ غير متصل"

    tg_log("🤖 بدء البوت الموحّد v2.5",
           f"📊 <b>الاستراتيجية:</b> Momentum + EMA Cross\n"
           f"⏱️ Momentum: {MOMENTUM_TF} | EMA: {','.join(EMA_TFS)}\n"
           f"💼 حجم: {POSITION_SIZE_USDT} USDT | رافعة: {LEVERAGE}x\n"
           f"🔢 حد الصفقات: {MAX_CONCURRENT}\n"
           f"📋 العملات ({len(SYMBOLS)}): {', '.join(SYMBOLS)}\n"
           f"🌐 الوضع: {'TESTNET' if TESTNET else 'LIVE'}\n"
           f"🔌 SafeClient: {'✅' if client else '❌'}\n"
           f"🗄️ <b>Supabase:</b> {sb_status}\n"
           f"💰 الرصيد: {bal['balance']:.2f} USDT\n"
           f"💵 المتاح: {bal['available']:.2f} USDT\n"
           f"🛡️ <b>Ban-Proof + Supabase-First:</b> ✅",
           "🤖")

    if _supabase:
        sb_log_event("bot_started", "Bot started v2.5 Supabase-First",
                     {"symbols": SYMBOLS, "leverage": LEVERAGE, "mode": "LIVE"})

    if client:
        import_manual()

    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=tg_polling_loop, daemon=True).start()
    threading.Thread(target=momentum_loop, daemon=True).start()
    threading.Thread(target=ema_loop, daemon=True).start()

    log.info("✅ جميع المكونات تعمل")

    def ban_watcher():
        was_banned = is_banned()
        while True:
            time.sleep(30)
            now_banned = is_banned()
            if was_banned and not now_banned:
                tg_log("✅ الحظر انتهى",
                       f"البوت استأنف العمل\n🕐 {syr_str()}", "✅")
                time.sleep(60)
                if not is_banned() and client:
                    import_manual()
            was_banned = now_banned

    threading.Thread(target=ban_watcher, daemon=True).start()

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
