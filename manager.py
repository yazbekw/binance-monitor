"""
Unified Position Manager v3.0.0 — Position Guardian Edition
=============================================================
هذا البوت لا يفتح صفقات جديدة — فقط يراقب ويدير المفتوحة بذكاء.

Key principles:
  ✅ One API call per cycle (futures_position_information = weight 5)
  ✅ Mark price included in position data — no extra ticker calls
  ✅ Smart trailing: BE → Lock → ATR Trail
  ✅ Time-based exit for stagnant positions
  ✅ Emergency exit on adverse moves
  ✅ Auto-detect new positions & import
  ✅ Detailed Telegram notifications for every action
  ✅ Aggressive rate-limit avoidance
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

LEVERAGE           = int(os.getenv("LEVERAGE", 20))
TESTNET            = os.getenv("TESTNET", "false").lower() == "true"
AUTO_IMPORT_NEW    = os.getenv("AUTO_IMPORT_NEW", "true").lower() == "true"

# ============================================================
# MANAGEMENT ENGINE CONFIG
# ============================================================
# Trailing stages (ATR multiples from entry)
TRAIL_STAGE_1_ATR  = float(os.getenv("TRAIL_STAGE_1_ATR", 1.0))  # BE at +1 ATR
TRAIL_STAGE_2_ATR  = float(os.getenv("TRAIL_STAGE_2_ATR", 2.0))  # lock at +2 ATR (SL = entry + 1 ATR)
TRAIL_STAGE_3_ATR  = float(os.getenv("TRAIL_STAGE_3_ATR", 3.0))  # start ATR trailing
TRAIL_ATR_DISTANCE = float(os.getenv("TRAIL_ATR_DISTANCE", 1.0)) # trail distance = 1.0 × ATR

# Emergency / Time exits
EMERGENCY_ATR_MULT = float(os.getenv("EMERGENCY_ATR_MULT", 2.5))
EMERGENCY_ENABLED  = os.getenv("EMERGENCY_ENABLED", "true").lower() == "true"

TIME_EXIT_HOURS    = float(os.getenv("TIME_EXIT_HOURS", 12.0))
TIME_EXIT_MIN_PCT  = float(os.getenv("TIME_EXIT_MIN_PCT", 0.3))   # |pnl%| < 0.3 → close
TIME_EXIT_ENABLED  = os.getenv("TIME_EXIT_ENABLED", "true").lower() == "true"

# Fallback (if SL order failed on exchange)
FALLBACK_ENABLED   = os.getenv("FALLBACK_ENABLED", "true").lower() == "true"
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "true").lower() == "true"
AUTO_SL_ATR_MULT   = float(os.getenv("AUTO_SL_ATR_MULT", 1.5))

# Auto-replace missing SL/TP if detected
AUTO_REPLACE_SL    = os.getenv("AUTO_REPLACE_SL", "true").lower() == "true"

# TP behavior
TP1_CLOSE_PCT      = float(os.getenv("TP1_CLOSE_PCT", 0.5))  # 50% at TP1

# ============================================================
# RATE LIMIT / POLLING
# ============================================================
MAX_WEIGHT_PER_MIN = int(os.getenv("MAX_WEIGHT_PER_MIN", 1000))
MIN_INTERVAL_SEC   = float(os.getenv("MIN_INTERVAL_SEC", 0.25))
OHLCV_CACHE_SEC    = int(os.getenv("OHLCV_CACHE_SEC", 300))   # 5 min ATR cache
MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))
BALANCE_CACHE_SEC  = int(os.getenv("BALANCE_CACHE_SEC", 60))
DASHBOARD_CACHE_SEC = int(os.getenv("DASHBOARD_CACHE_SEC", 5))

# ============================================================
# SESSION / MISC
# ============================================================
SESSION_START      = int(os.getenv("SESSION_START_UTC", 0))
SESSION_END        = int(os.getenv("SESSION_END_UTC", 24))
GLOBAL_PAUSE       = int(os.getenv("GLOBAL_PAUSE_SEC", 120))
GLOBAL_PAUSE_TRIG  = int(os.getenv("GLOBAL_PAUSE_TRIGGER", 3))
BACKOFF_BASE       = float(os.getenv("BACKOFF_BASE", 2.0))
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", 3))

HEARTBEAT_HOURS    = int(os.getenv("HEARTBEAT_HOURS", 4))
HISTORY_FILE       = os.getenv("HISTORY_FILE", "/tmp/trade_history.json")
BAN_STATE_FILE     = os.getenv("BAN_STATE_FILE", "/tmp/ban_state.json")

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
log = logging.getLogger("PosGuardian")
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ============================================================
# STATE
# ============================================================
open_positions = {}
state_lock = threading.RLock()
_symbol_locks = defaultdict(threading.Lock)
trade_history = []

# ============================================================
# BAN STATE
# ============================================================
_ban_lock = threading.Lock()
_ban_until_ts = 0.0
_notified_ban_ts = 0.0
_ban_state_loaded = False

def _load_ban_state():
    global _ban_until_ts, _notified_ban_ts, _ban_state_loaded
    if _ban_state_loaded: return
    _ban_state_loaded = True
    try:
        if os.path.exists(BAN_STATE_FILE):
            with open(BAN_STATE_FILE) as f:
                d = json.load(f)
            _ban_until_ts = float(d.get("until", 0.0))
            _notified_ban_ts = float(d.get("notified", 0.0))
            if _ban_until_ts > time.time():
                log.warning(f"🚫 حظر محفوظ — باقي {(_ban_until_ts-time.time())/60:.1f} د")
            else:
                _ban_until_ts = 0.0; _notified_ban_ts = 0.0
    except Exception as e:
        log.warning(f"load_ban_state: {e}")

def _save_ban_state():
    try:
        tmp = BAN_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"until": _ban_until_ts, "notified": _notified_ban_ts,
                       "saved_at": time.time()}, f)
        os.replace(tmp, BAN_STATE_FILE)
    except Exception: pass

def sb_save_ban_state(until_ts):
    if not _supabase: return
    try:
        _supabase.table("bot_state").upsert({
            "key": "ban_until", "value": str(until_ts),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e: log.debug(f"sb_save_ban: {e}")

def sb_load_ban_state():
    if not _supabase: return 0.0
    try:
        r = _supabase.table("bot_state").select("value").eq("key", "ban_until").execute()
        if r.data and len(r.data) > 0:
            return float(r.data[0]["value"])
    except Exception: pass
    return 0.0

def is_banned():
    global _notified_ban_ts
    with _ban_lock:
        if time.time() >= _ban_until_ts:
            if _notified_ban_ts > 0:
                _notified_ban_ts = 0.0; _save_ban_state()
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
        if extended: _ban_until_ts = ban_ts
        should_notify = ban_ts > _notified_ban_ts
        if should_notify: _notified_ban_ts = ban_ts
    if extended:
        _save_ban_state()
        threading.Thread(target=sb_save_ban_state, args=(ban_ts,), daemon=True).start()
    return should_notify

def clear_ban():
    global _ban_until_ts, _notified_ban_ts
    with _ban_lock:
        _ban_until_ts = 0.0; _notified_ban_ts = 0.0
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
# BINANCE CLIENT
# ============================================================
class SafeClient(Client):
    def ping(self):
        return {"serverTime": int(time.time() * 1000)}

def _create_client():
    try:
        c = SafeClient(BINANCE_API_KEY, BINANCE_SECRET_KEY, testnet=TESTNET)
        log.info("✅ Binance SafeClient initialized")
        return c
    except Exception as e:
        log.error(f"❌ SafeClient init: {e}")
        return None

client = _create_client()

# ============================================================
# SUPABASE
# ============================================================
_supabase = None
try:
    from supabase import create_client
    if SUPABASE_URL and SUPABASE_KEY:
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        log.info("✅ Supabase متصل")
    else:
        log.warning("⚠️ Supabase غير مهيأ")
except Exception as e:
    log.error(f"❌ Supabase init: {e}")

def sb_insert_trade(data):
    if not _supabase: return None
    try:
        r = _supabase.table("trades").insert(data).execute()
        if r.data and len(r.data) > 0:
            return r.data[0].get("id")
    except Exception as e: log.error(f"sb_insert_trade: {e}")
    return None

def sb_update_trade(trade_id, data):
    if not _supabase or not trade_id: return False
    try:
        _supabase.table("trades").update(data).eq("id", trade_id).execute()
        return True
    except Exception as e: log.error(f"sb_update_trade: {e}")
    return False

def sb_fetch_trades(limit=100, status=None):
    if not _supabase: raise RuntimeError("Supabase not configured")
    q = _supabase.table("trades").select("*").order("opened_at", desc=True).limit(limit)
    if status: q = q.eq("status", status)
    r = q.execute()
    return r.data or []

def sb_log_event(event_type, message, data=None):
    if not _supabase: return
    try:
        _supabase.table("bot_events").insert({
            "event_type": event_type, "message": message, "data": data or {},
        }).execute()
    except Exception as e: log.debug(f"sb_log_event: {e}")

# ============================================================
# RATE LIMITER
# ============================================================
class RateLimiter:
    def __init__(self, max_weight=1000, min_interval=0.25):
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

# ============================================================
# STATS
# ============================================================
_stats = {
    "positions_tracked": 0, "positions_closed": 0,
    "trailing_updates": 0, "breakeven_hits": 0, "lock_hits": 0,
    "tp1_hits": 0, "tp2_hits": 0, "sl_hits": 0,
    "time_exits": 0, "emergency_exits": 0, "fallback_exits": 0,
    "auto_imports": 0, "auto_sl_created": 0, "auto_sl_replaced": 0,
    "rate_limit_hits": 0, "bans": 0,
}
_stats_lock = threading.Lock()

def bump_stat(key, amount=1):
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount

# ============================================================
# SAFE API CALL
# ============================================================
_global_pause_until = 0.0

def safe_api_call(func, *args, weight=1, retries=MAX_RETRIES, **kwargs):
    global _global_pause_until

    if is_banned():
        log.debug(f"⛔ محظور — رفض (باقي {ban_remaining()/60:.1f} د)")
        return None

    now = time.time()
    if now < _global_pause_until:
        log.debug(f"⏸️ إيقاف عالمي")
        return None

    if client is None:
        log.error("safe_api_call: client is None")
        return None

    for attempt in range(retries):
        if is_banned(): return None
        try:
            rate_limiter.throttle()
            rate_limiter.add(weight)
            return func(*args, **kwargs)

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
                        log.error(f"🚫 IP محظور حتى {syr_str(ban_until)}")
                        tg_log("🚫 IP محظور",
                               f"ينتهي: {syr_str(ban_until)}\n"
                               f"المتبقي: {wait/60:.0f} دقيقة\n"
                               f"<i>الواجهة تعمل من Supabase</i>", "🚫")
                        sb_log_event("rate_limit", "IP banned", {"until": ban_until.isoformat()})
                    return None
                else:
                    wait = BACKOFF_BASE * (2 ** attempt)
                    log.warning(f"⚠️ Rate limit — انتظار {wait}s")
                    time.sleep(wait); continue
            elif e.code == -1021:
                time.sleep(1); continue
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
        if _exchange_info is not None: return _exchange_info
        if is_banned() or client is None: return None
        try:
            _exchange_info = safe_api_call(client.futures_exchange_info, weight=1)
            return _exchange_info
        except Exception as e:
            log.error(f"exchange_info: {e}")
            return None

def round_step(symbol, qty):
    info = get_exchange_info()
    if not info: return round(qty, 3)
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
    if not info: return round(price, 4)
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                    prec = int(round(-np.log10(tick)))
                    return round(round(price / tick) * tick, prec)
    return round(price, 4)

# ============================================================
# BALANCE
# ============================================================
def get_balance(use_cache=True):
    with _balance_lock:
        if use_cache and _balance_cache["data"] and \
           (time.time() - _balance_cache["ts"]) < BALANCE_CACHE_SEC:
            return _balance_cache["data"]

    if is_banned() or client is None:
        if _balance_cache["data"]: return _balance_cache["data"]
        return {"balance": 0.0, "available": 0.0, "pnl": 0.0, "stale": True}

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
    if _balance_cache["data"]: return _balance_cache["data"]
    return {"balance": 0.0, "available": 0.0, "pnl": 0.0, "stale": True}

# ============================================================
# ATR — cached long (for trailing logic)
# ============================================================
def fetch_atr(symbol, tf="15m", limit=100):
    """يستخدم كاش 5 دقائق — ATR لا يتغير بشكل جذري"""
    key = (symbol, tf, "atr")
    with _ohlcv_cache_lock:
        c = _ohlcv_cache.get(key)
        if c and (time.time() - c["ts"]) < OHLCV_CACHE_SEC:
            return c["data"]

    if is_banned() or client is None:
        return None
    try:
        raw = safe_api_call(client.futures_klines, symbol=symbol,
                           interval=tf, limit=limit, weight=5)
        if not raw: return None
        df = pd.DataFrame(raw, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","qav","trades","tbbav","tbqav","ignore"])
        for c in ["high","low","close"]:
            df[c] = df[c].astype(float)
        h, l, c_ = df["high"], df["low"], df["close"]
        tr = pd.concat([h-l, (h-c_.shift()).abs(), (l-c_.shift()).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        if atr > 0:
            with _ohlcv_cache_lock:
                _ohlcv_cache[key] = {"data": atr, "ts": time.time()}
            return atr
    except Exception as e:
        log.error(f"fetch_atr {symbol}: {e}")
    return None

# ============================================================
# POSITIONS — SINGLE API CALL FOR ALL
# ============================================================
def fetch_all_live_positions():
    """
    استدعاء واحد (weight=5) يُرجع:
      symbol -> {amt, entry, mark, pnl, side, qty}
    """
    if is_banned() or client is None:
        return {}
    try:
        positions = safe_api_call(client.futures_position_information, weight=5)
        result = {}
        for p in positions or []:
            amt = float(p.get("positionAmt", 0))
            if amt != 0:
                result[p["symbol"]] = {
                    "amt": amt,
                    "entry": float(p.get("entryPrice", 0)),
                    "mark": float(p.get("markPrice", 0)),
                    "pnl": float(p.get("unRealizedProfit", 0)),
                    "side": "LONG" if amt > 0 else "SHORT",
                    "qty": abs(amt),
                }
        return result
    except Exception as e:
        log.error(f"fetch_all_positions: {e}")
        return {}

# ============================================================
# SL / TP ORDERS
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

def cancel_order(symbol, order_id):
    try:
        return safe_api_call(client.futures_cancel_order,
                            symbol=symbol, orderId=order_id, weight=1)
    except Exception as e:
        log.warning(f"cancel {symbol}/{order_id}: {e}")
        return None

def cancel_all_orders_for_symbol(symbol):
    try:
        return safe_api_call(client.futures_cancel_all_open_orders,
                            symbol=symbol, weight=1)
    except Exception as e:
        log.warning(f"cancel_all {symbol}: {e}")
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
# IMPORT POSITION
# ============================================================
def import_position(symbol, live_pos, reason="auto_detect"):
    """
    إنشاء سجل إدارة محلي لصفقة موجودة على Binance
    """
    with _symbol_locks[symbol]:
        with state_lock:
            if symbol in open_positions:
                return False

        entry = live_pos["entry"]
        side = live_pos["side"]
        qty = live_pos["qty"]
        notional = entry * qty

        atr = fetch_atr(symbol, "15m") or (entry * 0.01)

        # Fetch existing SL/TP orders
        sl_price = None; sl_id = None
        tp_orders = []
        try:
            orders = safe_api_call(client.futures_get_open_orders,
                                  symbol=symbol, weight=1)
            for o in orders or []:
                otype = o["type"]
                if otype in ("STOP_MARKET", "STOP"):
                    sl_price = float(o["stopPrice"]); sl_id = o["orderId"]
                elif otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                    tp_orders.append({"id": o["orderId"], "price": float(o["stopPrice"])})
        except Exception as e:
            log.warning(f"fetch orders {symbol}: {e}")

        tp1_price = tp_orders[0]["price"] if len(tp_orders) > 0 else None
        tp2_price = tp_orders[1]["price"] if len(tp_orders) > 1 else None
        tp1_id    = tp_orders[0]["id"]    if len(tp_orders) > 0 else None
        tp2_id    = tp_orders[1]["id"]    if len(tp_orders) > 1 else None

        opened_at = datetime.now(timezone.utc)

        with state_lock:
            open_positions[symbol] = {
                "side": side, "entry": entry, "qty": qty, "atr": atr,
                "current_sl": sl_price, "sl_order_id": sl_id,
                "tp1_order_id": tp1_id, "tp2_order_id": tp2_id,
                "tp1_price": tp1_price, "tp2_price": tp2_price,
                "sl_on_exchange": sl_id is not None,
                "tp1_on_exchange": tp1_id is not None,
                "tp2_on_exchange": tp2_id is not None,
                "tp1_executed": False, "tp2_executed": False,
                "trailing_stage": 0,
                "opened_at": opened_at,
                "strategy": "MANAGED", "source": "MANAGED",
                "notional": notional,
                "highest_price": entry, "lowest_price": entry,
                "sl_mult_used": AUTO_SL_ATR_MULT,
                "imported_from": reason,
                "last_manage_ts": 0,
            }

        sb_id = sb_insert_trade({
            "symbol": symbol, "strategy": "MANAGED", "side": side,
            "entry_price": entry, "qty": qty, "notional": notional,
            "leverage": LEVERAGE, "sl_price": sl_price,
            "tp1_price": tp1_price, "tp2_price": tp2_price,
            "opened_at": opened_at.isoformat(),
            "status": "OPEN", "source": "MANAGED",
            "notes": f"imported ({reason})",
        })
        if sb_id:
            with state_lock:
                open_positions[symbol]["sb_id"] = sb_id

        bump_stat("positions_tracked")
        if reason == "auto_detect":
            bump_stat("auto_imports")

        # Notify
        sl_str = f"{sl_price}" if sl_price else "❌ لا يوجد"
        tp1_str = f"{tp1_price}" if tp1_price else "❌"
        tp2_str = f"{tp2_price}" if tp2_price else "❌"

        tg_log("📥 بدء إدارة صفقة",
               f"💠 <b>{symbol}</b>\n"
               f"📊 <b>الاتجاه:</b> {side}\n"
               f"💵 <b>الدخول:</b> {entry}\n"
               f"📦 <b>الكمية:</b> {qty}\n"
               f"💼 <b>الاسمي:</b> {notional:.2f} USDT\n"
               f"📉 <b>ATR:</b> {atr:.6g}\n\n"
               f"🛡️ <b>SL:</b> {sl_str}\n"
               f"🎯 <b>TP1:</b> {tp1_str}\n"
               f"🎯 <b>TP2:</b> {tp2_str}\n\n"
               f"📌 <i>سبب:</i> {reason}",
               "📥")

        # Auto-create SL if missing
        if sl_price is None and AUTO_SL_MANUAL and not is_banned():
            auto_sl = round_price(symbol,
                                 entry - AUTO_SL_ATR_MULT * atr if side == "LONG"
                                 else entry + AUTO_SL_ATR_MULT * atr)
            opp = "SELL" if side == "LONG" else "BUY"
            sl_order = place_stop_loss(symbol, opp, auto_sl, qty)
            with state_lock:
                open_positions[symbol]["current_sl"] = auto_sl
                if sl_order:
                    open_positions[symbol]["sl_order_id"] = sl_order["orderId"]
                    open_positions[symbol]["sl_on_exchange"] = True
            if sl_order:
                bump_stat("auto_sl_created")
                tg_log("🛡️ إنشاء SL تلقائي",
                       f"💠 {symbol}\n"
                       f"📉 ATR: {atr:.6g}\n"
                       f"🛡️ SL: {auto_sl} ({AUTO_SL_ATR_MULT}× ATR)",
                       "🛡️")
                sb_update_trade(sb_id, {"sl_price": auto_sl})

        return True

# ============================================================
# SMART MANAGEMENT
# ============================================================
def manage_position(symbol, info, live):
    """
    إدارة صفقة واحدة بذكاء:
      - Trailing: BE → Lock → ATR Trail
      - Emergency exit
      - Time-based exit
      - Fallback (SL/TP يدوي عند فشل الأوامر)
    """
    mark = live["mark"]
    entry = info["entry"]
    side = info["side"]
    atr = info.get("atr") or 0

    if mark <= 0 or atr <= 0:
        return

    # Update highs/lows
    if side == "LONG":
        if mark > info.get("highest_price", entry):
            info["highest_price"] = mark
    else:
        if mark < info.get("lowest_price", entry):
            info["lowest_price"] = mark

    pnl_pct = ((mark - entry) / entry * 100) if side == "LONG" else ((entry - mark) / entry * 100)

    # ========================================================
    # EMERGENCY EXIT (adverse move > EMERGENCY_ATR_MULT × ATR)
    # ========================================================
    if EMERGENCY_ENABLED and not is_banned():
        adverse = (entry - mark) if side == "LONG" else (mark - entry)
        if adverse >= EMERGENCY_ATR_MULT * atr:
            tg_log("🚨 إغلاق طارئ",
                   f"💠 {symbol}\n"
                   f"📊 {side}\n"
                   f"💵 الدخول: {entry}\n"
                   f"💰 السعر: {mark}\n"
                   f"📉 خسارة: {pnl_pct:.2f}%\n"
                   f"⚠️ حركة معاكسة {adverse/atr:.2f}× ATR > {EMERGENCY_ATR_MULT}",
                   "🚨")
            if close_position_market(symbol, side, info["qty"]):
                bump_stat("emergency_exits")
                info["emergency_closed"] = True
            return

    # ========================================================
    # TRAILING LOGIC (BE → Lock → ATR Trail)
    # ========================================================
    update_trailing_smart(symbol, info, mark, atr)

    # ========================================================
    # TIME-BASED EXIT
    # ========================================================
    if TIME_EXIT_ENABLED and not is_banned():
        opened = info.get("opened_at")
        if opened:
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            hours = (datetime.now(timezone.utc) - opened).total_seconds() / 3600
            if hours >= TIME_EXIT_HOURS and abs(pnl_pct) < TIME_EXIT_MIN_PCT:
                tg_log("⏰ إغلاق بسبب الزمن",
                       f"💠 {symbol}\n"
                       f"⏱️ المدة: {hours:.1f}h ≥ {TIME_EXIT_HOURS}h\n"
                       f"📊 P&L: {pnl_pct:+.2f}% (راكد)\n"
                       f"<i>تحرير الهامش لفرص أفضل</i>",
                       "⏰")
                if close_position_market(symbol, side, info["qty"]):
                    bump_stat("time_exits")
                    info["time_closed"] = True
                return

    # ========================================================
    # FALLBACK (SL/TP يدوي إذا فشل الأمر على Binance)
    # ========================================================
    if FALLBACK_ENABLED and not is_banned():
        check_fallback_triggers(symbol, info, mark)

    # ========================================================
    # AUTO-REPLACE MISSING SL
    # ========================================================
    if AUTO_REPLACE_SL and not is_banned():
        if not info.get("sl_on_exchange") and not info.get("sl_order_id"):
            # try to recreate
            if info.get("current_sl"):
                opp = "SELL" if side == "LONG" else "BUY"
                new_order = place_stop_loss(symbol, opp, info["current_sl"], info["qty"])
                if new_order:
                    info["sl_order_id"] = new_order["orderId"]
                    info["sl_on_exchange"] = True
                    bump_stat("auto_sl_replaced")
                    tg_log("🛡️ إعادة إنشاء SL",
                           f"💠 {symbol}\n🛡️ SL: {info['current_sl']}\n"
                           f"<i>تم بنجاح بعد فشل سابق</i>", "🛡️")

def update_trailing_smart(symbol, info, price, atr):
    """
    3 مراحل:
      Stage 0: لا شيء
      Stage 1: +1× ATR → SL = entry (breakeven)
      Stage 2: +2× ATR → SL = entry + 1× ATR (locked profit)
      Stage 3: +3× ATR → trailing بـ ATR distance
    """
    if is_banned() or client is None: return

    side = info["side"]
    entry = info["entry"]
    stage = info["trailing_stage"]
    cur_sl = info.get("current_sl")
    opp = "SELL" if side == "LONG" else "BUY"
    if atr <= 0: return

    new_sl = None
    new_stage = stage
    reason = ""

    if side == "LONG":
        move = price - entry
        if stage == 0 and move >= TRAIL_STAGE_1_ATR * atr:
            new_sl = round_price(symbol, entry)
            new_stage = 1
            reason = f"🛡️ Breakeven (+{move/atr:.2f}× ATR)"
        elif stage == 1 and move >= TRAIL_STAGE_2_ATR * atr:
            new_sl = round_price(symbol, entry + TRAIL_STAGE_1_ATR * atr)
            new_stage = 2
            reason = f"🔒 تأمين ربح (+{move/atr:.2f}× ATR)"
        elif stage == 2 and move >= TRAIL_STAGE_3_ATR * atr:
            new_sl = round_price(symbol, price - TRAIL_ATR_DISTANCE * atr)
            new_stage = 3
            reason = f"📈 Trailing ATR (+{move/atr:.2f}× ATR)"
        elif stage >= 3:
            ts = round_price(symbol, price - TRAIL_ATR_DISTANCE * atr)
            if cur_sl is None or ts > cur_sl:
                new_sl = ts
                new_stage = stage + 1
                reason = f"📈 تحديث Trailing ({ts})"
    else:  # SHORT
        move = entry - price
        if stage == 0 and move >= TRAIL_STAGE_1_ATR * atr:
            new_sl = round_price(symbol, entry)
            new_stage = 1
            reason = f"🛡️ Breakeven (+{move/atr:.2f}× ATR)"
        elif stage == 1 and move >= TRAIL_STAGE_2_ATR * atr:
            new_sl = round_price(symbol, entry - TRAIL_STAGE_1_ATR * atr)
            new_stage = 2
            reason = f"🔒 تأمين ربح (+{move/atr:.2f}× ATR)"
        elif stage == 2 and move >= TRAIL_STAGE_3_ATR * atr:
            new_sl = round_price(symbol, price + TRAIL_ATR_DISTANCE * atr)
            new_stage = 3
            reason = f"📈 Trailing ATR (+{move/atr:.2f}× ATR)"
        elif stage >= 3:
            ts = round_price(symbol, price + TRAIL_ATR_DISTANCE * atr)
            if cur_sl is None or ts < cur_sl:
                new_sl = ts
                new_stage = stage + 1
                reason = f"📈 تحديث Trailing ({ts})"

    if new_sl is None or new_sl == cur_sl:
        return

    # Cancel old SL
    if info.get("sl_on_exchange") and info.get("sl_order_id"):
        cancel_order(symbol, info["sl_order_id"])

    # Place new SL
    new_order = place_stop_loss(symbol, opp, new_sl, info["qty"])

    with state_lock:
        info["current_sl"] = new_sl
        info["trailing_stage"] = new_stage
        if new_order:
            info["sl_order_id"] = new_order["orderId"]
            info["sl_on_exchange"] = True
        else:
            info["sl_order_id"] = None
            info["sl_on_exchange"] = False

    bump_stat("trailing_updates")
    if new_stage == 1: bump_stat("breakeven_hits")
    elif new_stage == 2: bump_stat("lock_hits")

    pnl_pct = ((price - entry) / entry * 100) if side == "LONG" else ((entry - price) / entry * 100)

    tg_log("🎯 تحديث ذكي للوقف",
           f"💠 <b>{symbol}</b> | {side}\n"
           f"💰 <b>السعر:</b> {price}\n"
           f"💵 <b>الدخول:</b> {entry}\n"
           f"📊 <b>P&L:</b> {pnl_pct:+.2f}%\n\n"
           f"🛡️ <b>SL جديد:</b> {new_sl}\n"
           f"🛡️ <b>SL قديم:</b> {cur_sl or '—'}\n"
           f"📈 <b>المرحلة:</b> {stage} → {new_stage}\n\n"
           f"{reason}\n"
           f"{'✅ على Binance' if new_order else '⚠️ في المراقبة المحلية فقط'}",
           "🎯")

def check_fallback_triggers(symbol, info, price):
    """
    إذا لم تكن أوامر SL/TP على Binance، البوت يتصرف يدوياً
    """
    side = info["side"]
    cur_sl = info.get("current_sl")

    if not info.get("sl_on_exchange") and cur_sl:
        hit = (side == "LONG" and price <= cur_sl) or (side == "SHORT" and price >= cur_sl)
        if hit:
            tg_log("🚨 إغلاق احتياطي (SL)",
                   f"💠 {symbol}\n💰 السعر: {price}\n🛡️ SL: {cur_sl}",
                   "🚨")
            if close_position_market(symbol, side, info["qty"]):
                bump_stat("fallback_exits")

    if not info.get("tp1_on_exchange") and info.get("tp1_price") and not info.get("tp1_executed"):
        tp1 = info["tp1_price"]
        hit = (side == "LONG" and price >= tp1) or (side == "SHORT" and price <= tp1)
        if hit:
            half = round_step(symbol, info["qty"] * TP1_CLOSE_PCT)
            if half > 0:
                tg_log("🎯 إغلاق احتياطي (TP1)",
                       f"💠 {symbol}\n💰 السعر: {price}\n🎯 TP1: {tp1}\n"
                       f"📦 إغلاق: {half} ({int(TP1_CLOSE_PCT*100)}%)",
                       "🎯")
                if close_position_market(symbol, side, half):
                    with state_lock: info["tp1_executed"] = True
                    bump_stat("tp1_hits")

    if not info.get("tp2_on_exchange") and info.get("tp2_price") and not info.get("tp2_executed"):
        tp2 = info["tp2_price"]
        hit = (side == "LONG" and price >= tp2) or (side == "SHORT" and price <= tp2)
        if hit:
            rem = round_step(symbol, info["qty"] * (1 - TP1_CLOSE_PCT)) \
                  if info.get("tp1_executed") else info["qty"]
            if rem > 0:
                tg_log("🎯 إغلاق احتياطي (TP2)",
                       f"💠 {symbol}\n💰 السعر: {price}\n🎯 TP2: {tp2}",
                       "🎯")
                if close_position_market(symbol, side, rem):
                    with state_lock: info["tp2_executed"] = True
                    bump_stat("tp2_hits")

# ============================================================
# MONITOR LOOP — HEART OF THE BOT
# ============================================================
def monitor_loop():
    """
    دورة واحدة كل MONITOR_INTERVAL:
      1. استدعاء واحد لكل الصفقات (weight 5)
      2. كشف مغلقة
      3. إدارة الباقي
    """
    log.info(f"💓 Monitor loop بدأ — كل {MONITOR_INTERVAL}s، استدعاء واحد/دورة")
    first_run = True

    while True:
        try:
            if is_banned() or client is None:
                time.sleep(30); continue

            # One call → all positions with mark prices
            live = fetch_all_live_positions()

            with state_lock:
                tracked = list(open_positions.keys())

            # ==============================================
            # 1) Detect closed positions
            # ==============================================
            for symbol in tracked:
                if symbol not in live:
                    with state_lock:
                        closed = open_positions.pop(symbol, None)
                    if closed:
                        # Cancel remaining orders (best effort)
                        cancel_all_orders_for_symbol(symbol)
                        record_trade(symbol, closed)

            # ==============================================
            # 2) Manage live positions
            # ==============================================
            for symbol, lpos in live.items():
                with state_lock:
                    info = open_positions.get(symbol)

                if not info:
                    if AUTO_IMPORT_NEW:
                        import_position(symbol, lpos, reason="auto_detect")
                    continue

                try:
                    manage_position(symbol, info, lpos)
                except Exception as e:
                    log.error(f"manage {symbol}: {e}")

            # ==============================================
            # 3) Startup summary
            # ==============================================
            if first_run:
                first_run = False
                with state_lock:
                    count = len(open_positions)
                log.info(f"✅ أول دورة اكتملت — {count} صفقة تحت الإدارة")

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
            log.info(f"📚 تحميل {len(trade_history)} صفقة")
    except Exception as e:
        log.warning(f"load_history: {e}")

def save_history():
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(trade_history[-500:], f, indent=2, default=str)
    except Exception as e:
        log.warning(f"save_history: {e}")

def record_trade(symbol, info):
    """
    يُسجَّل عند كشف إغلاق الصفقة على Binance
    """
    opened = info["opened_at"]
    if opened.tzinfo is None: opened = opened.replace(tzinfo=timezone.utc)
    closed = datetime.now(timezone.utc)
    duration = (closed - opened).total_seconds() / 60

    pnl = 0.0
    exit_price = None
    commission = 0.0
    try:
        opened_ms = int(opened.timestamp() * 1000)
        trades = safe_api_call(client.futures_account_trades, symbol=symbol,
                              startTime=opened_ms, weight=5)
        for t in trades or []:
            if int(t["time"]) >= opened_ms:
                pnl += float(t["realizedPnl"])
                commission += float(t.get("commission", 0))
                exit_price = float(t["price"])
    except Exception as e:
        log.debug(f"account_trades {symbol}: {e}")

    entry = info["entry"]
    pnl_pct = ((exit_price - entry) / entry * 100) if (exit_price and entry) else 0
    if info["side"] == "SHORT":
        pnl_pct = -pnl_pct

    # Guess close reason
    reason = "unknown"
    if info.get("emergency_closed"): reason = "emergency"
    elif info.get("time_closed"): reason = "time_exit"
    elif info.get("tp2_executed"): reason = "tp2"
    elif info.get("tp1_executed"): reason = "tp1"
    else:
        cur_sl = info.get("current_sl")
        if cur_sl and exit_price:
            if info["side"] == "LONG" and exit_price <= cur_sl * 1.001: reason = "sl"
            elif info["side"] == "SHORT" and exit_price >= cur_sl * 0.999: reason = "sl"

    if reason == "sl": bump_stat("sl_hits")

    record = {
        "symbol": symbol, "strategy": info.get("strategy", "?"),
        "side": info["side"], "entry": entry,
        "qty": info["qty"], "pnl": round(pnl, 4),
        "opened_at": opened.isoformat(), "closed_at": closed.isoformat(),
        "duration_min": round(duration, 1),
        "final_sl": info.get("current_sl"),
        "stage": info.get("trailing_stage", 0),
        "exit_price": exit_price,
        "reason": reason,
    }
    trade_history.append(record)
    save_history()
    bump_stat("positions_closed")

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
        "notes": f"close_reason={reason}",
    }
    if sb_id:
        sb_update_trade(sb_id, update_data)
    else:
        insert_data = {
            "symbol": symbol, "strategy": info.get("strategy", "?"),
            "side": info["side"], "entry_price": entry,
            "qty": info["qty"], "leverage": LEVERAGE,
            "opened_at": opened.isoformat(),
            "source": info.get("source", "MANAGED"),
        }
        insert_data.update(update_data)
        sb_insert_trade(insert_data)

    # Detailed close notification
    sign = "+" if pnl >= 0 else ""
    reason_emoji = {
        "tp1": "🎯", "tp2": "🎯", "sl": "🛑",
        "emergency": "🚨", "time_exit": "⏰", "unknown": "❓"
    }.get(reason, "✅")

    reason_label = {
        "tp1": "هدف أول", "tp2": "هدف ثانٍ", "sl": "وقف خسارة",
        "emergency": "إغلاق طارئ", "time_exit": "إغلاق بالزمن",
        "unknown": "غير معروف",
    }.get(reason, reason)

    tg_log(f"{reason_emoji} إغلاق صفقة",
           f"💠 <b>{symbol}</b>\n"
           f"📊 {info['side']}\n"
           f"💵 الدخول: {entry}\n"
           f"💰 الخروج: {exit_price or '?'}\n"
           f"📦 الكمية: {info['qty']}\n\n"
           f"💰 <b>P&L:</b> {sign}{pnl:.4f} USDT ({sign}{pnl_pct:.2f}%)\n"
           f"💸 <b>عمولة:</b> {commission:.4f} USDT\n"
           f"⏱️ <b>المدة:</b> {duration:.1f} دقيقة\n"
           f"🎯 <b>المرحلة النهائية:</b> {info.get('trailing_stage', 0)}\n"
           f"🛡️ <b>SL النهائي:</b> {info.get('current_sl') or '—'}\n"
           f"📌 <b>السبب:</b> {reason_label}",
           reason_emoji)

# ============================================================
# FLASK — DASHBOARD
# ============================================================
app = Flask(__name__)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Position Manager · Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#080b14;--panel:#111827cc;--border:#1f2937;--border-2:#2d3748;
  --text:#e5e7eb;--text-dim:#94a3b8;--text-mute:#64748b;
  --brand:#3b82f6;--brand-2:#60a5fa;
  --green:#10b981;--red:#ef4444;--yellow:#f59e0b;--purple:#a855f7;
  --radius:14px;
}
*{box-sizing:border-box;margin:0;padding:0}
body{
  font-family:'Inter',system-ui,sans-serif;
  background:radial-gradient(1200px 600px at 90% -10%,#1e3a8a33,transparent 60%),
             radial-gradient(900px 500px at -10% 100%,#7c3aed22,transparent 60%),var(--bg);
  color:var(--text);min-height:100vh;padding-bottom:60px;
}
.mono{font-family:'JetBrains Mono',monospace}
.header{
  position:sticky;top:0;z-index:50;backdrop-filter:blur(14px);
  background:rgba(8,11,20,.75);border-bottom:1px solid var(--border);
  padding:14px 24px;display:flex;align-items:center;gap:16px;flex-wrap:wrap;
}
.brand{display:flex;align-items:center;gap:12px;font-weight:800;font-size:18px}
.brand .logo{width:38px;height:38px;border-radius:11px;
  background:linear-gradient(135deg,#10b981,#3b82f6);display:grid;place-items:center;
  font-size:20px;box-shadow:0 4px 14px #10b98166;}
.brand .sub{font-size:11px;color:var(--text-mute);font-weight:500}
.pills{display:flex;gap:8px;margin-right:auto;flex-wrap:wrap}
.pill{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;
  padding:6px 12px;border-radius:999px;border:1px solid var(--border);background:var(--panel);}
.pill .dot{width:7px;height:7px;border-radius:50%;background:var(--text-mute);}
.pill.on .dot{background:var(--green);box-shadow:0 0 0 4px #10b98122}
.pill.off .dot{background:var(--red);box-shadow:0 0 0 4px #ef444422}
.pill.warn{color:var(--yellow);border-color:#f59e0b55}
.pill.warn .dot{background:var(--yellow);animation:pulse 1.6s infinite}
@keyframes pulse{50%{opacity:.4}}
.clock{font-size:12px;color:var(--text-dim);font-weight:600}
.container{max-width:1440px;margin:0 auto;padding:24px}
.grid{display:grid;gap:16px}
.kpis{grid-template-columns:repeat(auto-fit,minmax(200px,1fr))}
.cols-2{grid-template-columns:2fr 1fr}
@media (max-width:900px){.cols-2{grid-template-columns:1fr}}
.alert{display:flex;align-items:center;gap:12px;padding:14px 18px;
  margin-bottom:18px;border-radius:12px;font-size:13px;font-weight:500;border:1px solid;}
.alert.info{background:#3b82f61a;border-color:#3b82f655;color:var(--brand-2)}
.alert.warn{background:#f59e0b22;border-color:#f59e0b55;color:var(--yellow)}
.alert.error{background:#ef444422;border-color:#ef444455;color:var(--red)}
.kpi{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);
  padding:18px;position:relative;overflow:hidden;backdrop-filter:blur(10px);transition:.25s;}
.kpi:hover{transform:translateY(-2px);border-color:var(--border-2);}
.kpi .label{font-size:11px;color:var(--text-mute);font-weight:600;
  text-transform:uppercase;letter-spacing:.8px;}
.kpi .value{font-size:26px;font-weight:800;margin-top:8px;
  font-family:'JetBrains Mono',monospace;letter-spacing:-.5px;}
.kpi .value .unit{font-size:13px;font-weight:600;color:var(--text-dim);margin-right:4px;}
.kpi.green .value{color:var(--green)}
.kpi.red .value{color:var(--red)}
.kpi.blue .value{color:var(--brand-2)}
.kpi.yellow .value{color:var(--yellow)}
.kpi.purple .value{color:var(--purple)}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);
  padding:20px;backdrop-filter:blur(10px);}
.panel h2{font-size:15px;font-weight:700;margin-bottom:16px;
  display:flex;align-items:center;gap:10px;}
.panel h2 .icon{width:28px;height:28px;border-radius:8px;display:grid;place-items:center;
  background:#3b82f622;color:var(--brand-2);font-size:14px;}
.panel h2 .badge{margin-right:auto;font-size:11px;font-weight:700;padding:3px 10px;
  border-radius:999px;background:#ffffff10;color:var(--text-dim);}
.table-wrap{overflow-x:auto;border-radius:10px;border:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:12px 14px;text-align:right;white-space:nowrap}
th{background:#0f172a;color:var(--text-dim);font-weight:600;font-size:11px;
  text-transform:uppercase;letter-spacing:.6px;border-bottom:1px solid var(--border);}
td{border-bottom:1px solid #1e293b88}
tbody tr:hover{background:#ffffff05}
tbody tr:last-child td{border-bottom:none}
.num{font-family:'JetBrains Mono',monospace;font-weight:600}
.tag{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:6px;
  font-size:11px;font-weight:700;border:1px solid transparent;}
.tag.long{background:#10b98122;color:var(--green);border-color:#10b98144}
.tag.short{background:#ef444422;color:var(--red);border-color:#ef444444}
.tag.stage{background:#a855f71a;color:#c084fc;border-color:#a855f744}
.empty{text-align:center;padding:48px 20px;color:var(--text-mute)}
.empty .icon{font-size:36px;opacity:.4;margin-bottom:10px}
.chart-wrap{height:300px;position:relative}
</style>
</head>
<body>
<div class="header">
  <div class="brand">
    <div class="logo">🛡️</div>
    <div>
      <div>Position Manager</div>
      <div class="sub">v3.0.0 · GUARDIAN EDITION</div>
    </div>
  </div>
  <div class="pills">
    <span class="pill {{ 'on' if client_ok else 'off' }}"><span class="dot"></span>Binance</span>
    <span class="pill {{ 'on' if supabase_available else 'off' }}"><span class="dot"></span>Supabase</span>
    <span class="pill {{ 'off' if banned else 'on' }}"><span class="dot"></span>{{ 'محظور' if banned else 'نشط' }}</span>
    {% if banned %}<span class="pill warn"><span class="dot"></span>باقي {{ ban_remaining_min }}د</span>{% endif %}
  </div>
  <div class="clock mono" id="clock">--:--:--</div>
</div>

<div class="container">
  <div id="banner-container"></div>

  <div class="grid kpis">
    <div class="kpi blue">
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
      <div class="label">💼 تحت الإدارة</div>
      <div class="value" id="kpi-active">—</div>
    </div>
    <div class="kpi">
      <div class="label">🎯 Trailing Updates</div>
      <div class="value" id="kpi-trail">—</div>
    </div>
    <div class="kpi">
      <div class="label">📊 P&L تراكمي</div>
      <div class="value" id="kpi-total-pnl">—</div>
    </div>
  </div>

  <div class="grid cols-2" style="margin-top:16px">
    <div class="panel">
      <h2><span class="icon">💼</span> الصفقات تحت الإدارة <span class="badge" id="pos-count">0</span></h2>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>العملة</th><th>الاتجاه</th><th>الدخول</th><th>الحالي</th>
            <th>P&L%</th><th>SL</th><th>Stage</th><th>ATR</th>
          </tr></thead>
          <tbody id="positions-body">
            <tr><td colspan="8" class="empty"><div class="icon">💼</div><p>لا صفقات</p></td></tr>
          </tbody>
        </table>
      </div>
    </div>
    <div class="panel">
      <h2><span class="icon">📊</span> إحصائيات الإدارة</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>المؤشر</th><th>القيمة</th></tr></thead>
          <tbody id="stats-body"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="panel" style="margin-top:16px">
    <h2><span class="icon">📜</span> آخر الصفقات المغلقة <span class="badge" id="history-badge">—</span></h2>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>الوقت</th><th>العملة</th><th>الاتجاه</th><th>الدخول</th>
          <th>الخروج</th><th>P&L</th><th>المدة</th><th>السبب</th>
        </tr></thead>
        <tbody id="history-body">
          <tr><td colspan="8" class="empty"><div class="icon">📜</div><p>لا تاريخ</p></td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
const $ = (id)=>document.getElementById(id);
const fmt = (n,d=2)=>Number(n||0).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtSigned = (n,d=2)=>{const v=Number(n||0);return (v>=0?'+':'')+fmt(v,d);};

function tickClock(){
  $('clock').textContent = new Date().toLocaleTimeString('en-GB',{hour12:false,timeZone:'Asia/Damascus'})+' (دمشق)';
}
setInterval(tickClock,1000); tickClock();

function renderBanners(d){
  const c = $('banner-container');
  const banners = [];
  if(d.banned){
    banners.push(`<div class="alert warn"><div>
      <b>🚫 البوت محظور مؤقتاً</b> — باقي ${d.ban_remaining_min} دقيقة.<br>
      <span style="opacity:.8">الواجهة تعمل من ${d.data_source_label}.</span>
    </div></div>`);
  }
  if(!d.supabase_available){
    banners.push(`<div class="alert info"><div>
      <b>Supabase غير مهيأ</b> — البيانات من الذاكرة المحلية.
    </div></div>`);
  }
  c.innerHTML = banners.join('');
}

function renderKPIs(d){
  const b=d.balance||{};
  $('kpi-balance').innerHTML = fmt(b.balance) + ' <span class="unit">USDT</span>';
  $('kpi-available').innerHTML = fmt(b.available) + ' <span class="unit">USDT</span>';
  const pnl = Number(b.pnl||0);
  $('kpi-pnl').textContent = fmtSigned(pnl) + ' USDT';
  $('kpi-pnl-card').className = 'kpi ' + (pnl>=0?'green':'red');
  $('kpi-active').textContent = d.active_count || 0;
  $('kpi-trail').textContent = (d.stats||{}).trailing_updates || 0;
  $('kpi-total-pnl').textContent = fmtSigned(d.total_pnl) + ' USDT';
}

function renderPositions(positions){
  const tb = $('positions-body');
  $('pos-count').textContent = positions.length;
  if(!positions.length){
    tb.innerHTML = '<tr><td colspan="8" class="empty"><div class="icon">💼</div><p>لا صفقات</p></td></tr>';
    return;
  }
  tb.innerHTML = positions.map(p=>{
    const pnl = Number(p.pnl_pct||0);
    return `<tr>
      <td><b>${p.symbol}</b></td>
      <td><span class="tag ${p.side==='LONG'?'long':'short'}">${p.side==='LONG'?'▲ LONG':'▼ SHORT'}</span></td>
      <td class="num">${p.entry}</td>
      <td class="num">${p.mark}</td>
      <td class="num" style="color:${pnl>=0?'#10b981':'#ef4444'};font-weight:700">${fmtSigned(pnl)}%</td>
      <td class="num">${p.current_sl||'—'}</td>
      <td><span class="tag stage">${p.trailing_stage||0}</span></td>
      <td class="num" style="color:#94a3b8">${fmt(p.atr,4)}</td>
    </tr>`;
  }).join('');
}

function renderHistory(history){
  const tb = $('history-body');
  $('history-badge').textContent = history.length;
  if(!history.length){
    tb.innerHTML = '<tr><td colspan="8" class="empty"><div class="icon">📜</div><p>لا تاريخ</p></td></tr>';
    return;
  }
  tb.innerHTML = history.map(t=>{
    const pnl = Number(t.pnl||0);
    const re = {tp1:'🎯 هدف',tp2:'🎯 هدف',sl:'🛑 وقف',emergency:'🚨 طارئ',time_exit:'⏰ وقت',unknown:'❓'}[t.reason] || '—';
    return `<tr>
      <td class="num" style="color:#64748b;font-size:12px">${t.opened_at}</td>
      <td><b>${t.symbol}</b></td>
      <td><span class="tag ${t.side==='LONG'?'long':'short'}">${t.side}</span></td>
      <td class="num">${t.entry}</td>
      <td class="num">${t.exit_price||'—'}</td>
      <td class="num" style="color:${pnl>=0?'#10b981':'#ef4444'};font-weight:700">${fmtSigned(pnl)}</td>
      <td class="num" style="color:#64748b">${t.duration_min}د</td>
      <td>${re}</td>
    </tr>`;
  }).join('');
}

function renderStats(stats){
  const rows = [
    ['💼 صفقات تحت الإدارة', stats.positions_tracked||0],
    ['✅ صفقات مغلقة', stats.positions_closed||0],
    ['🎯 تحديثات Trailing', stats.trailing_updates||0],
    ['🛡️ Breakeven', stats.breakeven_hits||0],
    ['🔒 تأمين ربح', stats.lock_hits||0],
    ['🎯 TP1', stats.tp1_hits||0],
    ['🎯 TP2', stats.tp2_hits||0],
    ['🛑 SL', stats.sl_hits||0],
    ['⏰ إغلاق بالزمن', stats.time_exits||0],
    ['🚨 إغلاق طارئ', stats.emergency_exits||0],
    ['🔄 إغلاق احتياطي', stats.fallback_exits||0],
    ['📥 استيراد تلقائي', stats.auto_imports||0],
    ['🛡️ إنشاء SL تلقائي', stats.auto_sl_created||0],
    ['🔧 إعادة SL', stats.auto_sl_replaced||0],
    ['⚠️ Rate limit hits', stats.rate_limit_hits||0],
    ['🚫 Bans', stats.bans||0],
  ];
  $('stats-body').innerHTML = rows.map(r=>
    `<tr><td style="color:#94a3b8">${r[0]}</td><td class="num" style="text-align:left">${r[1]}</td></tr>`
  ).join('');
}

async function refresh(){
  try{
    const r = await fetch('/api/dashboard',{cache:'no-store'});
    if(!r.ok) return;
    const d = await r.json();
    renderBanners(d);
    renderKPIs(d);
    renderPositions(d.positions||[]);
    renderHistory(d.history||[]);
    renderStats(d.stats||{});
  }catch(e){console.error(e);}
}
refresh();
setInterval(refresh,10000);
</script>
</body>
</html>"""

def _build_dashboard_data():
    bal = get_balance(use_cache=True)

    positions = []
    with state_lock:
        for s, p in open_positions.items():
            mark = p.get("_last_mark", p["entry"])
            pnl_pct = ((mark - p["entry"]) / p["entry"] * 100) if p["side"] == "LONG" \
                     else ((p["entry"] - mark) / p["entry"] * 100)
            positions.append({
                "symbol": s, "side": p["side"], "entry": p["entry"],
                "mark": mark, "qty": p["qty"],
                "current_sl": p.get("current_sl"),
                "trailing_stage": p.get("trailing_stage", 0),
                "atr": round(p.get("atr", 0), 6),
                "pnl_pct": round(pnl_pct, 2),
            })
        ac = len(open_positions)

    history = []
    strategy_stats = []
    equity_curve = []
    total_pnl = 0
    total_trades = 0
    winrate = None
    data_source = "memory"

    sb_data = None
    if _supabase:
        try:
            sb_data = sb_fetch_trades(limit=500, status="CLOSED")
        except Exception as e:
            log.debug(f"sb fetch: {e}")

    if sb_data:
        for t in sb_data[:100]:
            history.append({
                "symbol": t.get("symbol", "?"),
                "side": t.get("side", "?"),
                "entry": t.get("entry_price", 0),
                "exit_price": t.get("exit_price"),
                "pnl": float(t.get("pnl") or 0),
                "duration_min": float(t.get("duration_min") or 0),
                "opened_at": str(t.get("opened_at", ""))[:16].replace("T", " "),
                "reason": (t.get("notes") or "").replace("close_reason=", "") or "unknown",
            })
        total_pnl = sum(float(t.get("pnl") or 0) for t in sb_data)
        total_trades = len(sb_data)
        wins = sum(1 for t in sb_data if float(t.get("pnl") or 0) > 0)
        winrate = round(wins / total_trades * 100, 1) if total_trades else None
        data_source = "supabase"

    if data_source != "supabase":
        for t in reversed(trade_history[-100:]):
            history.append({
                "symbol": t.get("symbol", "?"),
                "side": t.get("side", "?"),
                "entry": t.get("entry", 0),
                "exit_price": t.get("exit_price"),
                "pnl": float(t.get("pnl") or 0),
                "duration_min": float(t.get("duration_min") or 0),
                "opened_at": t.get("opened_at", "")[:16].replace("T", " "),
                "reason": t.get("reason", "unknown"),
            })
        total_pnl = sum(float(t.get("pnl") or 0) for t in trade_history)
        total_trades = len(trade_history)
        wins = sum(1 for t in trade_history if float(t.get("pnl") or 0) > 0)
        winrate = round(wins / total_trades * 100, 1) if total_trades else None
        data_source = "local"

    return {
        "balance": bal, "active_count": ac,
        "positions": positions,
        "history": history, "strategy_stats": strategy_stats,
        "equity_curve": equity_curve,
        "total_pnl": total_pnl, "total_trades": total_trades,
        "winrate": winrate, "stats": dict(_stats),
        "supabase": _supabase is not None,
        "supabase_available": _supabase is not None,
        "client_ok": client is not None,
        "banned": is_banned(),
        "ban_remaining_min": int(ban_remaining() / 60) if is_banned() else 0,
        "data_source": data_source,
        "data_source_label": "🗄️ Supabase" if data_source == "supabase" else "📁 JSON",
    }

@app.route("/")
def dashboard():
    data = _build_dashboard_data()
    return render_template_string(
        DASHBOARD_HTML,
        supabase_available=data["supabase_available"],
        client_ok=data["client_ok"],
        banned=data["banned"],
        ban_remaining_min=data["ban_remaining_min"],
    )

@app.route("/api/dashboard")
def api_dashboard():
    with _dashboard_lock:
        if _dashboard_cache["data"] and \
           (time.time() - _dashboard_cache["ts"]) < DASHBOARD_CACHE_SEC:
            return jsonify(_dashboard_cache["data"])
    data = _build_dashboard_data()
    with _dashboard_lock:
        _dashboard_cache["data"] = data
        _dashboard_cache["ts"] = time.time()
    return jsonify(data)

@app.route("/health")
def health():
    return {"status": "alive", "time": syr_str(),
            "active": get_active_count(),
            "banned": is_banned(),
            "client_ok": client is not None}

def run_flask():
    port = int(os.getenv("PORT", 10000))
    log.info(f"🌐 Flask على المنفذ {port}")
    try:
        app.run(host="0.0.0.0", port=port, debug=False,
                use_reloader=False, threaded=True)
    except Exception as e:
        log.error(f"Flask: {e}")

# ============================================================
# GET ACTIVE COUNT
# ============================================================
def get_active_count():
    with state_lock:
        return len(open_positions)

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
        tg_send(
            f"🛡️ <b>حالة Position Manager v3.0.0</b>\n"
            f"🕐 {syr_str()}\n\n{ban_line}"
            f"🔌 Client: {'✅' if client else '❌'}\n"
            f"🗄️ Supabase: {'✅' if _supabase else '❌'}\n"
            f"💰 الرصيد: {bal['balance']:.2f}\n"
            f"💵 المتاح: {bal['available']:.2f}\n"
            f"📈 غير محقق: {bal['pnl']:+.2f}\n\n"
            f"💼 <b>تحت الإدارة:</b> {len(active)}\n"
            f"• {', '.join(active) if active else 'لا يوجد'}\n\n"
            f"📊 <b>هذه الجلسة:</b>\n"
            f"• مستوردة: {stats.get('positions_tracked',0)}\n"
            f"• مغلقة: {stats.get('positions_closed',0)}\n"
            f"• Trailing: {stats.get('trailing_updates',0)}\n"
            f"• BE: {stats.get('breakeven_hits',0)}\n"
            f"• Lock: {stats.get('lock_hits',0)}\n"
            f"• TP1: {stats.get('tp1_hits',0)}\n"
            f"• SL: {stats.get('sl_hits',0)}\n"
            f"• طارئ: {stats.get('emergency_exits',0)}\n"
            f"• زمني: {stats.get('time_exits',0)}"
        )
    elif text == "/positions":
        with state_lock:
            if not open_positions:
                tg_send("📭 لا صفقات تحت الإدارة"); return
            lines = ["<b>💼 الصفقات تحت الإدارة:</b>\n"]
            for s, p in open_positions.items():
                mark = p.get("_last_mark", p["entry"])
                pnl_pct = ((mark - p["entry"]) / p["entry"] * 100) if p["side"] == "LONG" \
                         else ((p["entry"] - mark) / p["entry"] * 100)
                lines.append(
                    f"💠 <b>{s}</b> | {p['side']}\n"
                    f"   💵 دخول: {p['entry']} | 📍 حالي: {mark}\n"
                    f"   📊 P&L: {pnl_pct:+.2f}%\n"
                    f"   🛡️ SL: {p.get('current_sl') or '—'} | Stage: {p.get('trailing_stage',0)}\n"
                    f"   📉 ATR: {p.get('atr',0):.6g}\n")
            tg_send("\n".join(lines))
    elif text == "/clearban":
        clear_ban()
        tg_send("✅ تم مسح الحظر")
    elif text == "/baninfo":
        if is_banned():
            tg_send(f"🚫 محظور — باقي {ban_remaining()/60:.1f} دقيقة")
        else:
            tg_send("✅ لا يوجد حظر")
    elif text == "/balance":
        bal = get_balance(use_cache=False)
        stale = " (آخر قراءة)" if bal.get("stale") else ""
        tg_send(f"💰 {bal['balance']:.2f}{stale}\n"
                f"💵 متاح: {bal['available']:.2f}\n"
                f"📈 {bal['pnl']:+.2f}")
    elif text == "/rescan":
        # Force re-import scan
        threading.Thread(target=force_scan, daemon=True).start()
        tg_send("🔍 جاري الفحص...")
    elif text == "/help":
        tg_send("📖 <b>الأوامر:</b>\n"
                "/status - حالة البوت\n"
                "/positions - الصفقات تحت الإدارة\n"
                "/balance - الرصيد\n"
                "/rescan - فحص فوري لصفقات جديدة\n"
                "/baninfo - حالة الحظر\n"
                "/clearban - مسح الحظر\n"
                "/help")

def force_scan():
    if is_banned() or client is None: return
    live = fetch_all_live_positions()
    new_count = 0
    for symbol, pos in live.items():
        with state_lock:
            if symbol in open_positions: continue
        if import_position(symbol, pos, reason="manual_rescan"):
            new_count += 1
    tg_send(f"✅ تم الفحص — {new_count} صفقة جديدة")

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
                   f"🚫 باقي {ban_remaining()/60:.1f} دقيقة", "💓")
            continue
        bal = get_balance(use_cache=False)
        with state_lock:
            active = list(open_positions.keys())
        s = dict(_stats)
        tg_log("💓 Heartbeat",
               f"💼 <b>تحت الإدارة:</b> {len(active)}\n"
               f"• {', '.join(active) if active else 'لا يوجد'}\n\n"
               f"💰 {bal['balance']:.2f} USDT\n"
               f"📈 P&L: {bal['pnl']:+.2f}\n"
               f"🎯 Trailing: {s.get('trailing_updates',0)}\n"
               f"✅ مغلقة: {s.get('positions_closed',0)}",
               "💓")

# ============================================================
# BAN WATCHER
# ============================================================
def wait_for_ban_to_end():
    if not is_banned(): return
    try:
        sb_until = sb_load_ban_state()
        if sb_until > 0 and sb_until > time.time():
            ban_dt = datetime.fromtimestamp(sb_until, tz=timezone.utc)
            set_ban(ban_dt)
    except Exception: pass

    if not is_banned(): return

    remaining = ban_remaining()
    log.warning(f"🚫 حظر نشط — باقي {remaining/60:.1f} دقيقة")
    tg_log("🚫⏸️ وضع انتظار الحظر",
           f"البوت لن يتصل بـ Binance حتى انتهاء الحظر\n"
           f"المتبقي: {remaining/60:.1f} دقيقة", "⏸️")

    while is_banned():
        wait = min(ban_remaining(), 300)
        if wait <= 0: break
        time.sleep(wait + 5)

    log.info("✅ انتهى الحظر")
    tg_log("✅ انتهى الحظر", "البوت يستأنف الإدارة", "✅")

# ============================================================
# MAIN
# ============================================================
def main():
    log.info("🛡️ بدء Position Manager v3.0.0")
    _load_ban_state()
    log.info(f"🔌 Client: {'✅' if client else '❌'}")
    log.info(f"⏱️ Monitor Interval: {MONITOR_INTERVAL}s")
    log.info(f"📊 Trailing: BE@{TRAIL_STAGE_1_ATR}× → Lock@{TRAIL_STAGE_2_ATR}× → Trail@{TRAIL_STAGE_3_ATR}×")
    log.info(f"🚨 Emergency: {EMERGENCY_ATR_MULT}× ATR | ⏰ Time Exit: {TIME_EXIT_HOURS}h")

    threading.Thread(target=run_flask, daemon=True).start()
    log.info("✅ Flask يعمل")

    load_history()
    wait_for_ban_to_end()

    bal = get_balance(use_cache=False)
    sb_status = "✅" if _supabase else "❌"

    tg_log("🛡️ بدء Position Manager v3.0.0",
           f"<b>الوضع:</b> إدارة الصفقات المفتوحة فقط\n"
           f"<i>لا يفتح صفقات جديدة تلقائياً</i>\n\n"
           f"⏱️ فحص كل: {MONITOR_INTERVAL}s\n"
           f"📡 استدعاء واحد/دورة (weight=5)\n\n"
           f"<b>🧠 محرك الإدارة:</b>\n"
           f"• 🛡️ Breakeven عند +{TRAIL_STAGE_1_ATR}× ATR\n"
           f"• 🔒 تأمين ربح عند +{TRAIL_STAGE_2_ATR}× ATR\n"
           f"• 📈 Trailing ATR عند +{TRAIL_STAGE_3_ATR}× ATR\n"
           f"• 🚨 إغلاق طارئ عند -{EMERGENCY_ATR_MULT}× ATR\n"
           f"• ⏰ إغلاق زمني بعد {TIME_EXIT_HOURS}h (راكد)\n\n"
           f"📥 استيراد تلقائي: {'✅' if AUTO_IMPORT_NEW else '❌'}\n"
           f"🛡️ SL تلقائي للصفقات بدون وقف: {'✅' if AUTO_SL_MANUAL else '❌'}\n\n"
           f"🔌 Client: {'✅' if client else '❌'}\n"
           f"🗄️ Supabase: {sb_status}\n"
           f"💰 الرصيد: {bal['balance']:.2f} USDT\n"
           f"💵 المتاح: {bal['available']:.2f} USDT",
           "🛡️")

    if _supabase:
        sb_log_event("bot_started", "Position Manager v3.0.0",
                     {"monitor_interval": MONITOR_INTERVAL})

    # Initial scan
    if client:
        log.info("🔍 الفحص الأولي...")
        force_scan()

    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=tg_polling_loop, daemon=True).start()

    log.info("✅ جميع المكونات تعمل")

    def ban_watcher():
        was_banned = is_banned()
        while True:
            time.sleep(30)
            now_banned = is_banned()
            if was_banned and not now_banned:
                tg_log("✅ انتهى الحظر", f"استئناف\n🕐 {syr_str()}", "✅")
                time.sleep(60)
                if not is_banned() and client:
                    force_scan()
            was_banned = now_banned

    threading.Thread(target=ban_watcher, daemon=True).start()

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
