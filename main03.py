"""
Momentum Bot v3.1 — FINAL
==========================
- Syria timezone (UTC+3) in all notifications
- Full lifecycle notifications
- Real rate limiter (weight-aware)
- Multi-fallback for SL/TP orders (3 attempts each)
- TP price sanity check (avoid -2021)
- Client-side SL/TP fallback (software-based execution)
- Auto SL for manual positions
- Heartbeat + session change notifications
"""
import os
import re
import time
import logging
import threading
from datetime import datetime, timezone, timedelta

import pandas as pd
import numpy as np
import requests
from flask import Flask
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceRequestException

load_dotenv()

# ==================== CONFIG ====================
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
SYMBOLS            = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", 200))
LEVERAGE           = int(os.getenv("LEVERAGE", 20))
TIMEFRAME          = os.getenv("TIMEFRAME", "15m")
TESTNET            = os.getenv("TESTNET", "False").lower() == "true"
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", 3))
TRAILING_ENABLED   = os.getenv("TRAILING_ENABLED", "True").lower() == "true"
MIN_SCORE          = int(os.getenv("MIN_SCORE", 6))
MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))
SCAN_INTERVAL_MIN  = int(os.getenv("SCAN_INTERVAL_MIN", 15))
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "True").lower() == "true"
NOTIFY_SCAN        = os.getenv("NOTIFY_SCAN", "True").lower() == "true"
NOTIFY_HEARTBEAT   = os.getenv("NOTIFY_HEARTBEAT", "True").lower() == "true"
HEARTBEAT_HOURS    = int(os.getenv("HEARTBEAT_HOURS", 4))
FALLBACK_ENABLED   = os.getenv("FALLBACK_ENABLED", "True").lower() == "true"

# ==================== TIMEZONE (Syria UTC+3) ====================
SYRIA_TZ = timezone(timedelta(hours=3))

def syr_now():
    return datetime.now(SYRIA_TZ)

def syr_str(dt=None):
    if dt is None:
        dt = syr_now()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(SYRIA_TZ).strftime("%Y-%m-%d %H:%M:%S")

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("MomentumBot")

# ==================== STATE ====================
open_positions = {}
lock = threading.Lock()

# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self, max_weight_per_min=1200):
        self.max_weight = max_weight_per_min
        self.weight_window = []
        self.lock = threading.Lock()
        self.min_interval = 0.15
        self.last_request = 0

    def add(self, weight):
        with self.lock:
            now = time.time()
            self.weight_window = [(t, w) for t, w in self.weight_window if now - t < 60]
            self.weight_window.append((now, weight))
            total = sum(w for _, w in self.weight_window)
            if total > self.max_weight:
                wait = 60 - (now - self.weight_window[0][0])
                if wait > 0:
                    log.warning(f"⚠️ weight={total}/{self.max_weight} — انتظار {wait:.0f}s")
                    time.sleep(wait)

    def throttle(self):
        with self.lock:
            now = time.time()
            delta = now - self.last_request
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self.last_request = time.time()

rate_limiter = RateLimiter()

# ==================== CACHE ====================
_exchange_info_cache = None
_exchange_info_lock = threading.Lock()

def get_exchange_info_cached():
    global _exchange_info_cache
    with _exchange_info_lock:
        if _exchange_info_cache is None:
            rate_limiter.add(1)
            _exchange_info_cache = client.futures_exchange_info()
            log.info("✅ تم تحميل exchange_info إلى الكاش")
        return _exchange_info_cache

# ==================== FLASK ====================
app = Flask(__name__)

@app.route("/")
def health():
    with lock:
        active = list(open_positions.keys())
    return {
        "status": "alive",
        "syria_time": syr_str(),
        "symbols": SYMBOLS,
        "mode": "TESTNET" if TESTNET else "LIVE",
        "active_positions": active,
        "max_concurrent": MAX_CONCURRENT_TRADES
    }

def run_flask():
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# ==================== TELEGRAM ====================
class Telegram:
    def __init__(self):
        self.token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.base = f"https://api.telegram.org/bot{self.token}" if self.token else None

    def send(self, message):
        if not self.token or not self.chat_id:
            return
        try:
            url = f"{self.base}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code != 200:
                log.error(f"Telegram error: {r.text}")
        except Exception as e:
            log.error(f"Telegram exception: {e}")

tg = Telegram()

def tg_log(title, body, emoji="ℹ️"):
    msg = f"{emoji} <b>{title}</b>\n\n🕐 <b>الوقت (دمشق):</b> {syr_str()}\n\n{body}"
    tg.send(msg)
    log.info(f"{title} | {body.replace(chr(10), ' | ')}")

# ==================== BINANCE ====================
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY, testnet=TESTNET)

def safe_api_call(func, *args, weight=1, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            rate_limiter.throttle()
            rate_limiter.add(weight)
            return func(*args, **kwargs)
        except AttributeError:
            raise
        except BinanceAPIException as e:
            if e.code == -1003:
                msg = str(e.message)
                match = re.search(r"banned until (\d+)", msg)
                if match:
                    ban_ms = int(match.group(1))
                    ban_until = datetime.fromtimestamp(ban_ms / 1000, tz=timezone.utc)
                    wait = (ban_until - datetime.now(timezone.utc)).total_seconds()
                    if wait > 0:
                        log.error(f"🚫 IP محظور حتى {syr_str(ban_until)} | انتظار {wait:.0f}s")
                        tg_log("🚫 IP محظور", f"الحظر ينتهي: {syr_str(ban_until)}\nانتظار {wait/60:.1f} دقيقة", "🚫")
                        time.sleep(min(wait + 5, 7200))
                        continue
                wait = 2 ** attempt
                log.warning(f"⚠️ Rate limit — انتظار {wait}s")
                time.sleep(wait)
            elif e.code == -1021:
                time.sleep(1)
            elif e.code in (-4120, -1102, -1111, -2021):
                raise
            else:
                raise
        except BinanceRequestException:
            time.sleep(2)
    raise Exception(f"فشل بعد {retries} محاولات")

# ==================== HELPERS ====================
def get_klines(symbol, interval, limit=300):
    raw = safe_api_call(client.futures_klines, symbol=symbol, interval=interval, limit=limit, weight=5)
    df = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qav","trades","tbbav","tbqav","ignore"
    ])
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

def get_balance_usdt():
    try:
        balances = safe_api_call(client.futures_account_balance, weight=5)
        for b in balances:
            if b["asset"] == "USDT":
                return {
                    "balance": float(b["balance"]),
                    "available": float(b["availableBalance"]),
                    "unrealized_pnl": float(b.get("crossUnPnl", 0))
                }
    except Exception as e:
        log.error(f"Balance error: {e}")
    return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}

def get_current_price(symbol):
    try:
        ticker = safe_api_call(client.futures_symbol_ticker, symbol=symbol, weight=1)
        return float(ticker["price"])
    except Exception as e:
        log.error(f"Price fetch error {symbol}: {e}")
        return 0.0

def set_leverage_and_margin(symbol):
    try:
        safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=LEVERAGE, weight=1)
    except Exception as e:
        log.warning(f"Leverage set {symbol}: {e}")

def has_open_position(symbol):
    with lock:
        if symbol in open_positions:
            return True
    try:
        positions = safe_api_call(client.futures_position_information, symbol=symbol, weight=5)
        for p in positions:
            if float(p["positionAmt"]) != 0:
                return True
    except Exception as e:
        log.error(f"Position check error: {e}")
    return False

def get_active_count():
    with lock:
        return len(open_positions)

# ==================== INDICATORS ====================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()

def macd_series(s, fast=12, slow=26, signal=9):
    dif = ema(s, fast) - ema(s, slow)
    dea = ema(dif, signal)
    return dif, dea, dif - dea

def rsi_series(s, period=14):
    delta = s.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def atr_series(df, period=14):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def vwap_series(df):
    typical = (df["high"] + df["low"] + df["close"]) / 3
    return (typical * df["volume"]).cumsum() / df["volume"].cumsum()

def add_indicators(df):
    df["ema7"]    = ema(df["close"], 7)
    df["ema25"]   = ema(df["close"], 25)
    df["ema50"]   = ema(df["close"], 50)
    df["ema200"]  = ema(df["close"], 200)
    df["dif"], df["dea"], df["hist"] = macd_series(df["close"])
    df["rsi"]     = rsi_series(df["close"])
    df["atr"]     = atr_series(df)
    df["vwap"]    = vwap_series(df)
    df["vol_ma5"] = df["volume"].rolling(5).mean()
    return df

# ==================== SESSION ====================
def in_trading_session():
    return 7 <= datetime.now(timezone.utc).hour < 21

# ==================== ROUNDING ====================
def round_step(symbol, qty):
    info = get_exchange_info_cached()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                    precision = int(round(-np.log10(step)))
                    return round(np.floor(qty / step) * step, precision)
    return round(qty, 3)

def round_price(symbol, price):
    info = get_exchange_info_cached()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                    precision = int(round(-np.log10(tick)))
                    return round(round(price / tick) * tick, precision)
    return round(price, 4)

# ==================== SIGNAL ====================
def evaluate_signal(df, direction):
    last, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
    score = 0
    reasons = []
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

    atr_val = last["atr"]

    if direction == "LONG":
        entry = price
        sl    = entry - (1.5 * atr_val)
        tp1   = entry + (1.5 * atr_val)
        tp2   = entry + (3.0 * atr_val)
    else:
        entry = price
        sl    = entry + (1.5 * atr_val)
        tp1   = entry - (1.5 * atr_val)
        tp2   = entry - (3.0 * atr_val)

    return {
        "score": score, "reasons": reasons,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "atr": float(last["atr"]),
        "rsi": float(last["rsi"]),
        "ema50": float(last["ema50"]),
        "ema200": float(last["ema200"]),
        "vwap": float(last["vwap"]),
        "vol_ratio": float(last["volume"] / last["vol_ma5"]) if last["vol_ma5"] > 0 else 0,
        "macd_hist": float(last["hist"]),
        "dif": float(last["dif"]),
        "dea": float(last["dea"]),
    }

# ==================== ORDER PLACEMENT (with fallbacks) ====================
def place_stop_loss(symbol, side, stop_price, qty):
    """Try 3 methods to place SL. Returns order dict or None."""
    attempts = [
        dict(symbol=symbol, side=side, type="STOP_MARKET",
             stopPrice=stop_price, closePosition=True),
        dict(symbol=symbol, side=side, type="STOP_MARKET",
             stopPrice=stop_price, closePosition=True, workingType="MARK_PRICE"),
        dict(symbol=symbol, side=side, type="STOP_MARKET",
             stopPrice=stop_price, quantity=qty, reduceOnly=True),
    ]
    last_err = None
    for i, params in enumerate(attempts, 1):
        try:
            return safe_api_call(client.futures_create_order, weight=1, **params)
        except BinanceAPIException as e:
            last_err = e
            log.warning(f"SL attempt {i} failed: {e.code} {e.message}")
            continue
    log.error(f"❌ كل محاولات SL فشلت: {last_err}")
    return None

def place_take_profit(symbol, side, tp_price, qty):
    """Try 3 methods to place TP. Returns order dict or None."""
    attempts = [
        dict(symbol=symbol, side=side, type="TAKE_PROFIT_MARKET",
             stopPrice=tp_price, quantity=qty, reduceOnly=True),
        dict(symbol=symbol, side=side, type="TAKE_PROFIT_MARKET",
             stopPrice=tp_price, quantity=qty, reduceOnly=True, workingType="MARK_PRICE"),
        dict(symbol=symbol, side=side, type="TAKE_PROFIT_MARKET",
             stopPrice=tp_price, quantity=qty),
    ]
    last_err = None
    for i, params in enumerate(attempts, 1):
        try:
            return safe_api_call(client.futures_create_order, weight=1, **params)
        except BinanceAPIException as e:
            last_err = e
            log.warning(f"TP attempt {i} failed: {e.code} {e.message}")
            continue
    log.error(f"❌ كل محاولات TP فشلت: {last_err}")
    return None

def close_position_market(symbol, side, qty, reason=""):
    """Close position by market order (software fallback)."""
    opposite = "SELL" if side == "LONG" else "BUY"
    try:
        order = safe_api_call(
            client.futures_create_order, weight=1,
            symbol=symbol, side=opposite,
            type="MARKET", quantity=qty, reduceOnly=True
        )
        return order
    except BinanceAPIException as e:
        log.error(f"❌ close_position_market فشل {symbol}: {e.code} {e.message}")
        tg_log(
            "🚨 فشل إغلاق الصفقة",
            f"💠 <b>العملة:</b> {symbol}\n"
            f"📊 <b>الاتجاه:</b> {side}\n"
            f"📦 <b>الكمية:</b> {qty}\n"
            f"⚠️ <b>السبب:</b> {e.message}\n"
            f"📝 <b>يجب الإغلاق يدوياً من Binance!</b>",
            "🚨"
        )
        return None

# ==================== OPEN TRADE ====================
def open_trade(symbol, direction, signal):
    bal = get_balance_usdt()
    required_margin = POSITION_SIZE_USDT / LEVERAGE
    total_required = required_margin * 1.3

    if bal["available"] < total_required:
        tg_log(
            "⚠️ رصيد غير كافٍ",
            f"💠 <b>العملة:</b> {symbol}\n"
            f"💰 <b>المتاح:</b> {bal['available']:.2f} USDT\n"
            f"📊 <b>المطلوب:</b> {total_required:.2f} USDT\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x",
            "⚠️"
        )
        return

    try:
        set_leverage_and_margin(symbol)

        price = signal["entry"]
        qty = round_step(symbol, POSITION_SIZE_USDT / price)

        if qty <= 0:
            tg_log("⚠️ كمية غير صالحة", f"{symbol}: qty={qty}", "⚠️")
            return

        side = "BUY" if direction == "LONG" else "SELL"
        opposite = "SELL" if direction == "LONG" else "BUY"

        # Market entry
        order = safe_api_call(
            client.futures_create_order, weight=1,
            symbol=symbol, side=side, type="MARKET", quantity=qty
        )
        fill_price = float(order.get("avgPrice", price)) or price
        actual_notional = qty * fill_price

        # === TP sanity check (avoid -2021) ===
        # Re-calculate targets based on actual fill price + ATR
        atr_val = signal["atr"]

        if direction == "LONG":
            raw_sl  = fill_price - (1.5 * atr_val)
            raw_tp1 = fill_price + (1.5 * atr_val)
            raw_tp2 = fill_price + (3.0 * atr_val)
        else:
            raw_sl  = fill_price + (1.5 * atr_val)
            raw_tp1 = fill_price - (1.5 * atr_val)
            raw_tp2 = fill_price - (3.0 * atr_val)

        sl_price  = round_price(symbol, raw_sl)
        tp1_price = round_price(symbol, raw_tp1)
        tp2_price = round_price(symbol, raw_tp2)

        # Prevent immediate trigger (min 0.1% away from current)
        if direction == "LONG":
            min_dist = fill_price * 1.001
            if tp1_price < min_dist:
                tp1_price = round_price(symbol, min_dist)
            if tp2_price <= tp1_price:
                tp2_price = round_price(symbol, tp1_price * 1.001)
        else:
            max_dist = fill_price * 0.999
            if tp1_price > max_dist:
                tp1_price = round_price(symbol, max_dist)
            if tp2_price >= tp1_price:
                tp2_price = round_price(symbol, tp1_price * 0.999)

        # === Place orders ===
        sl_order = place_stop_loss(symbol, opposite, sl_price, qty)

        half_qty = round_step(symbol, qty / 2)
        tp1_order = place_take_profit(symbol, opposite, tp1_price, half_qty)
        tp2_order = place_take_profit(symbol, opposite, tp2_price, qty - half_qty)

        # === Register ===
        with lock:
            open_positions[symbol] = {
                "side": direction,
                "entry": fill_price,
                "qty": qty,
                "atr": atr_val,
                "initial_sl": sl_price,
                "current_sl": sl_price if sl_order else None,
                "sl_order_id": sl_order["orderId"] if sl_order else None,
                "tp1_order_id": tp1_order["orderId"] if tp1_order else None,
                "tp2_order_id": tp2_order["orderId"] if tp2_order else None,
                "tp1_price": tp1_price,
                "tp2_price": tp2_price,
                # --- Fallback tracking ---
                "sl_on_exchange":  sl_order  is not None,
                "tp1_on_exchange": tp1_order is not None,
                "tp2_on_exchange": tp2_order is not None,
                "tp1_executed": False,
                "tp2_executed": False,
                # ---
                "tp1_hit": False,
                "trailing_stage": 0,
                "opened_at": datetime.now(timezone.utc),
                "source": "BOT",
                "sl_price_fallback": sl_price,
            }

        # === Notifications for failures ===
        if not sl_order:
            tg_log(
                "🚨 خطر: صفقة بدون وقف خسارة على Binance",
                f"💠 <b>العملة:</b> {symbol}\n"
                f"📊 <b>الاتجاه:</b> {direction}\n"
                f"💵 <b>الدخول:</b> {fill_price}\n"
                f"🛑 <b>الوقف المقصود:</b> {sl_price}\n\n"
                f"✅ <b>البوت سيتولى المراقبة اليدوية</b>\n"
                f"سيُغلق الصفقة تلقائياً عند لمس {sl_price}",
                "🚨"
            )
        if not tp1_order:
            tg_log("⚠️ فشل TP1", f"{symbol}: البوت سيراقب وينفذ يدوياً عند {tp1_price}", "⚠️")
        if not tp2_order:
            tg_log("⚠️ فشل TP2", f"{symbol}: البوت سيراقب وينفذ يدوياً عند {tp2_price}", "⚠️")

        # === Calculations for report ===
        sl_pct = abs(fill_price - sl_price) / fill_price * 100
        tp1_pct = abs(tp1_price - fill_price) / fill_price * 100
        tp2_pct = abs(tp2_price - fill_price) / fill_price * 100
        rr1 = abs(tp1_price - fill_price) / abs(fill_price - sl_price) if sl_price != fill_price else 0
        rr2 = abs(tp2_price - fill_price) / abs(fill_price - sl_price) if sl_price != fill_price else 0

        reasons_txt = "\n".join(signal["reasons"])
        ema50_state  = "فوق" if fill_price > signal["ema50"] else "تحت"
        ema200_state = "فوق" if fill_price > signal["ema200"] else "تحت"
        vwap_state   = "فوق" if fill_price > signal["vwap"] else "تحت"
        macd_state   = "صاعد" if signal["dif"] > signal["dea"] else "هابط"

        fallback_note = ""
        if not sl_order or not tp1_order or not tp2_order:
            fallback_note = (
                "\n\n⚠️ <b>وضع الحماية الاحتياطية مُفعّل</b>\n"
                "بعض الأوامر لم توضع على Binance — سيتولى البوت التنفيذ."
            )

        body = (
            f"💠 <b>العملة:</b> {symbol}\n"
            f"📈 <b>الاتجاه:</b> {direction}\n"
            f"💵 <b>سعر الدخول:</b> {fill_price}\n"
            f"📦 <b>الكمية:</b> {qty}\n"
            f"💼 <b>القيمة الاسمية:</b> {actual_notional:.2f} USDT\n"
            f"💰 <b>الهامش المستخدم:</b> {actual_notional/LEVERAGE:.2f} USDT\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n\n"
            f"🛑 <b>وقف الخسارة:</b> {sl_price} ({sl_pct:.2f}%)\n"
            f"🎯 <b>هدف 1:</b> {tp1_price} ({tp1_pct:.2f}%) — R:R 1:{rr1:.2f}\n"
            f"🎯 <b>هدف 2:</b> {tp2_price} ({tp2_pct:.2f}%) — R:R 1:{rr2:.2f}\n"
            f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n\n"
            f"📊 <b>النقاط:</b> {signal['score']}/10\n"
            f"📝 <b>الأسباب:</b>\n{reasons_txt}\n\n"
            f"🔬 <b>قراءة المؤشرات:</b>\n"
            f"• EMA50: {ema50_state}\n"
            f"• EMA200: {ema200_state}\n"
            f"• VWAP: {vwap_state}\n"
            f"• MACD: {macd_state} (DIF={signal['dif']:.4f}, DEA={signal['dea']:.4f})\n"
            f"• RSI: {signal['rsi']:.1f}\n"
            f"• ATR: {signal['atr']:.6f}\n"
            f"• نسبة الحجم: {signal['vol_ratio']:.2f}x"
            f"{fallback_note}"
        )
        tg_log("🚀 فتح صفقة", body, "🚀")

    except (BinanceAPIException, BinanceOrderException) as e:
        msg = f"العملة: {symbol}\nCode: {e.code}\nMessage: {e.message}"
        if e.code == -2019:
            msg += "\n\n💡 الهامش غير كافٍ"
        elif e.code == -2015:
            msg += "\n\n💡 الصلاحيات غير كافية"
        elif e.code == -4164:
            msg += "\n\n💡 حجم الصفقة أقل من الحد الأدنى"
        tg_log("❌ فشل فتح صفقة", msg, "❌")
    except Exception as e:
        tg_log("❌ خطأ غير متوقع", f"العملة: {symbol}\n{e}", "❌")

# ==================== TRAILING STOP ====================
def update_trailing_stop(symbol, info, current_price):
    if not TRAILING_ENABLED:
        return
    if not info.get("current_sl"):
        return

    side = info["side"]
    entry = info["entry"]
    atr_val = info["atr"]
    stage = info["trailing_stage"]
    current_sl = info["current_sl"]
    opposite = "SELL" if side == "LONG" else "BUY"

    if atr_val <= 0:
        return

    new_sl = None
    new_stage = stage

    if side == "LONG":
        move = current_price - entry
        if stage == 0 and move >= 1 * atr_val:
            new_sl = round_price(symbol, entry); new_stage = 1
        elif stage == 1 and move >= 2 * atr_val:
            new_sl = round_price(symbol, entry + 1 * atr_val); new_stage = 2
        elif stage == 2 and move >= 3 * atr_val:
            new_sl = round_price(symbol, entry + 2 * atr_val); new_stage = 3
        elif stage >= 3:
            ts = round_price(symbol, current_price - 1 * atr_val)
            if ts > current_sl:
                new_sl = ts; new_stage = stage + 1
    else:
        move = entry - current_price
        if stage == 0 and move >= 1 * atr_val:
            new_sl = round_price(symbol, entry); new_stage = 1
        elif stage == 1 and move >= 2 * atr_val:
            new_sl = round_price(symbol, entry - 1 * atr_val); new_stage = 2
        elif stage == 2 and move >= 3 * atr_val:
            new_sl = round_price(symbol, entry - 2 * atr_val); new_stage = 3
        elif stage >= 3:
            ts = round_price(symbol, current_price + 1 * atr_val)
            if ts < current_sl:
                new_sl = ts; new_stage = stage + 1

    if new_sl is None or new_sl == current_sl:
        return

    # Try to cancel old SL (if it exists on exchange)
    if info.get("sl_on_exchange") and info.get("sl_order_id"):
        try:
            safe_api_call(client.futures_cancel_order, symbol=symbol,
                          orderId=info["sl_order_id"], weight=1)
        except BinanceAPIException as e:
            log.warning(f"Cancel SL: {e.message}")

    # Try to place new SL
    new_order = place_stop_loss(symbol, opposite, new_sl, info["qty"])

    with lock:
        info["current_sl"] = new_sl
        info["trailing_stage"] = new_stage
        if new_order:
            info["sl_order_id"] = new_order["orderId"]
            info["sl_on_exchange"] = True
        else:
            # Fallback: no exchange order, but track SL in memory
            info["sl_on_exchange"] = False
            info["sl_order_id"] = None

    if new_stage == 1:
        protection = "Break-Even (نقطة الدخول)"
    else:
        if side == "LONG":
            protection = f"ربح مضمون +{round(new_sl - entry, 6)}"
        else:
            protection = f"ربح مضمون +{round(entry - new_sl, 6)}"

    fb_note = "" if new_order else "\n⚠️ <b>سيُنفّذ يدوياً</b> (لم يوضع على Binance)"
    tg_log(
        "🔄 تحديث الوقف المتحرك",
        f"💠 <b>العملة:</b> {symbol}\n"
        f"📊 <b>الاتجاه:</b> {side}\n"
        f"💰 <b>السعر الحالي:</b> {current_price}\n"
        f"🛑 <b>الوقف الجديد:</b> {new_sl}\n"
        f"📈 <b>المرحلة:</b> {new_stage}\n"
        f"🛡️ <b>الحماية:</b> {protection}"
        f"{fb_note}",
        "🔄"
    )

# ==================== MANUAL IMPORT ====================
def import_manual_positions():
    imported = []
    try:
        positions = safe_api_call(client.futures_position_information, weight=5)
        for p in positions:
            symbol = p["symbol"]
            amt = float(p["positionAmt"])
            if amt == 0 or symbol not in SYMBOLS:
                continue
            with lock:
                if symbol in open_positions:
                    continue

            entry = float(p["entryPrice"])
            side = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            notional = entry * qty
            margin_type = p.get("marginType", "isolated").lower()
            configured_lev = int(p.get("leverage", LEVERAGE))
            unrealized_pnl = float(p.get("unRealizedProfit", 0))
            mark_price = float(p.get("markPrice", entry))
            liquidation = float(p.get("liquidationPrice", 0))

            if margin_type == "cross":
                actual_margin = get_balance_usdt()["available"]
                actual_lev_txt = "Cross Margin"
                lev_note = "\n⚠️ <b>وضع Cross Margin</b> — الهامش = الرصيد الكلي"
            else:
                im = float(p.get("isolatedMargin", 0) or 0)
                pim = float(p.get("positionInitialMargin", 0) or 0)
                actual_margin = im or pim
                actual_lev = notional / actual_margin if actual_margin > 0 else 0
                actual_lev_txt = f"{actual_lev:.2f}x"
                lev_note = ""

            sl_id = tp1_id = tp2_id = None
            sl_p = tp1_p = tp2_p = None
            pending = []
            try:
                orders = safe_api_call(client.futures_get_open_orders, symbol=symbol, weight=5)
                for o in orders:
                    o_type = o["type"]
                    o_stop = float(o.get("stopPrice", 0)) or None
                    pending.append({"type": o_type, "stopPrice": o_stop})
                    if o_type in ("STOP_MARKET", "STOP"):
                        sl_id = o["orderId"]; sl_p = o_stop
                    elif o_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        if tp1_id is None:
                            tp1_id = o["orderId"]; tp1_p = o_stop
                        else:
                            tp2_id = o["orderId"]; tp2_p = o_stop
            except Exception as e:
                log.warning(f"تعذّر جلب أوامر {symbol}: {e}")

            try:
                df = get_klines(symbol, TIMEFRAME, limit=100)
                df = add_indicators(df)
                atr_val = float(df.iloc[-1]["atr"])
            except Exception:
                atr_val = entry * 0.01

            with lock:
                open_positions[symbol] = {
                    "side": side, "entry": entry, "qty": qty, "atr": atr_val,
                    "initial_sl": sl_p if sl_p else (
                        entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                    ),
                    "current_sl": sl_p,
                    "sl_order_id": sl_id,
                    "tp1_order_id": tp1_id,
                    "tp2_order_id": tp2_id,
                    "tp1_price": tp1_p,
                    "tp2_price": tp2_p,
                    "sl_on_exchange":  sl_id  is not None,
                    "tp1_on_exchange": tp1_id is not None,
                    "tp2_on_exchange": tp2_id is not None,
                    "tp1_executed": False,
                    "tp2_executed": False,
                    "tp1_hit": False,
                    "trailing_stage": 0,
                    "opened_at": datetime.now(timezone.utc),
                    "source": "MANUAL",
                    "margin_type": margin_type,
                }
            imported.append(symbol)

            orders_txt = ""
            if pending:
                orders_txt = "\n\n📋 <b>الأوامر المعلقة:</b>\n"
                for o in pending:
                    lbl = "🛑 SL" if "STOP" in o["type"] and "TAKE" not in o["type"] else "🎯 TP"
                    orders_txt += f"  {lbl} | {o['stopPrice']} | {o['type']}\n"
            else:
                orders_txt = "\n\n⚠️ لا توجد أوامر SL/TP معلقة"

            tg_log(
                "📥 استيراد صفقة يدوية",
                f"💠 <b>العملة:</b> {symbol}\n"
                f"📊 <b>الاتجاه:</b> {side}\n"
                f"💵 <b>سعر الدخول:</b> {entry}\n"
                f"📦 <b>الكمية:</b> {qty}\n"
                f"💼 <b>القيمة الاسمية:</b> {notional:.2f} USDT\n"
                f"💵 <b>الهامش المستخدم:</b> {actual_margin:.2f} USDT\n"
                f"⚙️ <b>الرافعة الفعلية:</b> {actual_lev_txt}\n"
                f"⚙️ <b>الرافعة المُعدّة:</b> {configured_lev}x ({margin_type})\n"
                f"📈 <b>السعر الحالي:</b> {mark_price}\n"
                f"💰 <b>ربح/خسارة:</b> {unrealized_pnl:+.2f} USDT\n"
                f"💥 <b>سعر التصفية:</b> {liquidation}\n"
                f"📊 <b>ATR:</b> {atr_val:.6f}\n"
                f"🛡️ <b>SL الحالي:</b> {sl_p if sl_p else 'لا يوجد'}"
                f"{orders_txt}"
                f"{lev_note}",
                "📥"
            )

            # Auto SL if missing
            if sl_p is None and AUTO_SL_MANUAL:
                auto_sl = entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                auto_sl = round_price(symbol, auto_sl)
                opposite = "SELL" if side == "LONG" else "BUY"
                sl_order = place_stop_loss(symbol, opposite, auto_sl, qty)

                with lock:
                    open_positions[symbol]["current_sl"] = auto_sl
                    open_positions[symbol]["initial_sl"] = auto_sl
                    if sl_order:
                        open_positions[symbol]["sl_order_id"] = sl_order["orderId"]
                        open_positions[symbol]["sl_on_exchange"] = True
                    else:
                        open_positions[symbol]["sl_on_exchange"] = False
                        open_positions[symbol]["sl_order_id"] = None

                if sl_order:
                    tg_log(
                        "🛡️ إضافة وقف خسارة تلقائي",
                        f"💠 <b>العملة:</b> {symbol}\n"
                        f"🛑 <b>الوقف الجديد:</b> {auto_sl}\n"
                        f"📝 <b>السبب:</b> لا يوجد SL على الصفقة اليدوية",
                        "🛡️"
                    )
                else:
                    tg_log(
                        "🚨 فشل SL — تفعيل الحماية اليدوية",
                        f"💠 <b>العملة:</b> {symbol}\n"
                        f"🛑 <b>الوقف المقصود:</b> {auto_sl}\n"
                        f"✅ <b>البوت سيراقب وينفّذ يدوياً عند لمس هذا السعر</b>",
                        "🚨"
                    )

        if not imported:
            log.info("🔎 لا توجد صفقات يدوية")
        return imported
    except Exception as e:
        log.error(f"import_manual_positions error: {e}")
        return []

# ==================== MONITOR ====================
def monitor_positions():
    last_prices = {}
    while True:
        try:
            with lock:
                symbols = list(open_positions.keys())

            if not symbols:
                time.sleep(MONITOR_INTERVAL)
                continue

            for symbol in symbols:
                with lock:
                    info = open_positions.get(symbol)
                if not info:
                    continue

                current_price = get_current_price(symbol)
                if current_price <= 0:
                    continue

                # === 1. Update trailing stop ===
                update_trailing_stop(symbol, info, current_price)

                # === 2. Fallback SL/TP execution ===
                if FALLBACK_ENABLED:
                    side = info["side"]
                    current_sl = info.get("current_sl")
                    tp1_price = info.get("tp1_price")
                    tp2_price = info.get("tp2_price")

                    # (A) SL Fallback
                    if not info.get("sl_on_exchange") and current_sl:
                        sl_hit = (
                            (side == "LONG"  and current_price <= current_sl) or
                            (side == "SHORT" and current_price >= current_sl)
                        )
                        if sl_hit:
                            tg_log(
                                "🚨 تنفيذ SL احتياطي",
                                f"💠 <b>العملة:</b> {symbol}\n"
                                f"📊 <b>الاتجاه:</b> {side}\n"
                                f"💰 <b>السعر الحالي:</b> {current_price}\n"
                                f"🛑 <b>الوقف:</b> {current_sl}\n"
                                f"📦 <b>الكمية:</b> {info['qty']}\n"
                                f"📝 <b>السبب:</b> أمر الوقف لم يوضع على Binance",
                                "🚨"
                            )
                            order = close_position_market(symbol, side, info["qty"], "SL")
                            if order:
                                with lock:
                                    # mark as executed to avoid repeat
                                    info["current_sl"] = current_price
                            continue

                    # (B) TP1 Fallback (نصف الكمية)
                    if (not info.get("tp1_on_exchange")
                            and tp1_price
                            and not info.get("tp1_executed")):
                        tp1_hit = (
                            (side == "LONG"  and current_price >= tp1_price) or
                            (side == "SHORT" and current_price <= tp1_price)
                        )
                        if tp1_hit:
                            half_qty = round_step(symbol, info["qty"] / 2)
                            if half_qty > 0:
                                tg_log(
                                    "🎯 تنفيذ TP1 احتياطي",
                                    f"💠 <b>العملة:</b> {symbol}\n"
                                    f"📊 <b>الاتجاه:</b> {side}\n"
                                    f"💰 <b>السعر الحالي:</b> {current_price}\n"
                                    f"🎯 <b>الهدف 1:</b> {tp1_price}\n"
                                    f"📦 <b>الكمية المُغلقة:</b> {half_qty} (50%)\n"
                                    f"📝 <b>السبب:</b> أمر TP1 لم يوضع على Binance",
                                    "🎯"
                                )
                                order = close_position_market(symbol, side, half_qty, "TP1")
                                if order:
                                    with lock:
                                        info["tp1_executed"] = True

                    # (C) TP2 Fallback (الباقي)
                    if (not info.get("tp2_on_exchange")
                            and tp2_price
                            and not info.get("tp2_executed")):
                        tp2_hit = (
                            (side == "LONG"  and current_price >= tp2_price) or
                            (side == "SHORT" and current_price <= tp2_price)
                        )
                        if tp2_hit:
                            remaining = (
                                round_step(symbol, info["qty"] / 2)
                                if info.get("tp1_executed")
                                else info["qty"]
                            )
                            if remaining > 0:
                                tg_log(
                                    "🎯 تنفيذ TP2 احتياطي",
                                    f"💠 <b>العملة:</b> {symbol}\n"
                                    f"📊 <b>الاتجاه:</b> {side}\n"
                                    f"💰 <b>السعر الحالي:</b> {current_price}\n"
                                    f"🎯 <b>الهدف 2:</b> {tp2_price}\n"
                                    f"📦 <b>الكمية المُغلقة:</b> {remaining}\n"
                                    f"📝 <b>السبب:</b> أمر TP2 لم يوضع على Binance",
                                    "🎯"
                                )
                                order = close_position_market(symbol, side, remaining, "TP2")
                                if order:
                                    with lock:
                                        info["tp2_executed"] = True

                # === 3. Check if position was closed ===
                last_p = last_prices.get(symbol, 0)
                moved = last_p == 0 or abs(current_price - last_p) / current_price > 0.001
                last_prices[symbol] = current_price
                if not moved:
                    continue

                try:
                    pos = safe_api_call(client.futures_position_information, symbol=symbol, weight=5)
                except Exception:
                    continue

                for p in pos:
                    if float(p["positionAmt"]) == 0:
                        with lock:
                            closed_info = open_positions.pop(symbol, None)
                        if closed_info:
                            for k in ["sl_order_id", "tp1_order_id", "tp2_order_id"]:
                                oid = closed_info.get(k)
                                if oid:
                                    try:
                                        safe_api_call(client.futures_cancel_order, symbol=symbol,
                                                      orderId=oid, weight=1)
                                    except Exception:
                                        pass
                            duration = datetime.now(timezone.utc) - closed_info["opened_at"]
                            hours = duration.total_seconds() / 3600
                            tg_log(
                                "✅ إغلاق صفقة",
                                f"💠 <b>العملة:</b> {symbol}\n"
                                f"📊 <b>الاتجاه:</b> {closed_info['side']}\n"
                                f"💵 <b>الدخول:</b> {closed_info['entry']}\n"
                                f"🛑 <b>آخر وقف:</b> {closed_info['current_sl']}\n"
                                f"📈 <b>المرحلة النهائية:</b> {closed_info['trailing_stage']}\n"
                                f"📦 <b>المصدر:</b> {closed_info.get('source', 'BOT')}\n"
                                f"⏱️ <b>المدة:</b> {hours:.2f} ساعة",
                                "✅"
                            )
            time.sleep(MONITOR_INTERVAL)
        except Exception as e:
            log.error(f"Monitor error: {e}")
            time.sleep(MONITOR_INTERVAL)

# ==================== HEARTBEAT ====================
def heartbeat_loop():
    if not NOTIFY_HEARTBEAT:
        return
    while True:
        time.sleep(HEARTBEAT_HOURS * 3600)
        bal = get_balance_usdt()
        with lock:
            active = list(open_positions.keys())
        tg_log(
            "💓 Heartbeat",
            f"📊 <b>الصفقات النشطة:</b> {len(active)}/{MAX_CONCURRENT_TRADES}\n"
            f"📋 <b>العملات النشطة:</b> {', '.join(active) if active else 'لا يوجد'}\n"
            f"💰 <b>الرصيد:</b> {bal['balance']:.2f} USDT\n"
            f"💵 <b>المتاح:</b> {bal['available']:.2f} USDT\n"
            f"📈 <b>غير محقق:</b> {bal['unrealized_pnl']:+.2f} USDT",
            "💓"
        )

# ==================== SCANNER ====================
def wait_for_candle_close(interval_minutes):
    now = datetime.now(timezone.utc)
    minutes_to_next = interval_minutes - (now.minute % interval_minutes)
    next_close = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)
    wait_sec = (next_close - now).total_seconds() + 5
    log.info(f"⏳ انتظار إغلاق الشمعة: {wait_sec:.0f} ثانية ({wait_sec/60:.1f} دقيقة) | التالي: {syr_str(next_close)}")
    remaining = wait_sec
    while remaining > 0:
        chunk = min(remaining, 30)
        time.sleep(chunk)
        remaining -= chunk

_last_session_state = None

def scan_once():
    global _last_session_state

    now_in = in_trading_session()
    if _last_session_state is not None and _last_session_state != now_in:
        tg_log(
            "🕐 تغيير الجلسة",
            f"{'دخول جلسة التداول (London/NY)' if now_in else 'خروج من جلسة التداول'}",
            "🕐"
        )
    _last_session_state = now_in

    if not now_in:
        log.info("⏸️ خارج جلسة التداول")
        return

    active_count = get_active_count()
    if active_count >= MAX_CONCURRENT_TRADES:
        log.info(f"⛔ الحد الأقصى ({active_count}/{MAX_CONCURRENT_TRADES})")
        return

    if NOTIFY_SCAN:
        bal = get_balance_usdt()
        tg_log(
            "🔍 بدء المسح",
            f"📋 <b>العملات:</b> {', '.join(SYMBOLS)}\n"
            f"📊 <b>الصفقات النشطة:</b> {active_count}/{MAX_CONCURRENT_TRADES}\n"
            f"💰 <b>المتاح:</b> {bal['available']:.2f} USDT",
            "🔍"
        )

    results = []
    for symbol in SYMBOLS:
        try:
            if get_active_count() >= MAX_CONCURRENT_TRADES:
                break

            if has_open_position(symbol):
                log.info(f"⏭️ {symbol}: صفقة مفتوحة")
                continue

            df = get_klines(symbol, TIMEFRAME, limit=300)
            df = add_indicators(df)
            df_closed = df.iloc[:-1].copy()

            long_sig = evaluate_signal(df_closed, "LONG")
            short_sig = evaluate_signal(df_closed, "SHORT")

            results.append(f"• {symbol}: L={long_sig['score']} S={short_sig['score']}")
            log.info(f"{symbol} | LONG={long_sig['score']} | SHORT={short_sig['score']}")

            if long_sig["score"] >= MIN_SCORE:
                open_trade(symbol, "LONG", long_sig)
            elif short_sig["score"] >= MIN_SCORE:
                open_trade(symbol, "SHORT", short_sig)

        except Exception as e:
            log.error(f"Scan error {symbol}: {e}")
            tg_log("⚠️ خطأ في المسح", f"العملة: {symbol}\n{e}", "⚠️")

    if NOTIFY_SCAN and results:
        tg_log("📊 نتائج المسح", "\n".join(results), "📊")

# ==================== MAIN ====================
def main_loop():
    bal = get_balance_usdt()
    tg_log(
        "🤖 بدء تشغيل البوت",
        f"📊 <b>الاستراتيجية:</b> Early Momentum Catch v3.1\n"
        f"⏱️ <b>الفريم:</b> {TIMEFRAME}\n"
        f"💼 <b>حجم الصفقة:</b> {POSITION_SIZE_USDT} USDT\n"
        f"💰 <b>الهامش المتوقع:</b> {POSITION_SIZE_USDT/LEVERAGE:.2f} USDT\n"
        f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n"
        f"📋 <b>العملات:</b> {', '.join(SYMBOLS)}\n"
        f"🔢 <b>حد الصفقات:</b> {MAX_CONCURRENT_TRADES}\n"
        f"🎯 <b>أدنى نقاط:</b> {MIN_SCORE}/10\n"
        f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n"
        f"🛡️ <b>SL تلقائي:</b> {'مُفعّل' if AUTO_SL_MANUAL else 'معطّل'}\n"
        f"🎯 <b>الحماية الاحتياطية (Fallback):</b> {'مُفعّلة' if FALLBACK_ENABLED else 'معطّلة'}\n"
        f"🌐 <b>الوضع:</b> {'TESTNET' if TESTNET else 'LIVE'}\n\n"
        f"💰 <b>الرصيد:</b> {bal['balance']:.2f} USDT\n"
        f"💵 <b>المتاح:</b> {bal['available']:.2f} USDT\n"
        f"📈 <b>غير محقق:</b> {bal['unrealized_pnl']:+.2f} USDT",
        "🤖"
    )

    import_manual_positions()

    while True:
        try:
            wait_for_candle_close(SCAN_INTERVAL_MIN)
            scan_once()
        except Exception as e:
            log.error(f"Main loop error: {e}")
            tg_log("💥 خطأ رئيسي", str(e), "💥")
            time.sleep(60)

# ==================== ENTRY ====================
if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=monitor_positions, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    time.sleep(2)
    main_loop()
