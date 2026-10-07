"""
Momentum Bot v2.1
==================
Strategy : Early Momentum Catch
Indicators: EMA (7/25/50/200) + MACD + RSI + ATR + VWAP + Volume MA
Features :
  - Binance Futures execution (Market entry + SL + 2 TP levels)
  - Telegram notifications (full lifecycle, no scan spam)
  - Multi-stage Trailing Stop (BE → locked profit → dynamic trail)
  - Max concurrent trades limit
  - Manual position import (with full details + SL/TP detection)
  - Auto SL for manual positions without one
  - Exchange info caching + batch price fetching (rate-limit safe)
  - Session filter (London + NY only)
  - Flask health check for Render
"""
import os
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
from binance.enums import (
    SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET,
    FUTURE_ORDER_TYPE_STOP_MARKET, FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
)
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
MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))     # seconds
SCAN_INTERVAL_MIN  = int(os.getenv("SCAN_INTERVAL_MIN", 15))    # minutes
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "True").lower() == "true"

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("MomentumBot")

# ==================== STATE ====================
open_positions = {}          # symbol -> info dict
lock = threading.Lock()

# ==================== CACHING ====================
_exchange_info_cache = None
_exchange_info_lock = threading.Lock()

def get_exchange_info_cached():
    global _exchange_info_cache
    with _exchange_info_lock:
        if _exchange_info_cache is None:
            _exchange_info_cache = client.futures_exchange_info()
            log.info("✅ تم تحميل exchange_info إلى الكاش")
        return _exchange_info_cache

# ==================== FLASK (Render health check) ====================
app = Flask(__name__)

@app.route("/")
def health():
    with lock:
        active = list(open_positions.keys())
    return {
        "status": "alive",
        "time": datetime.now(timezone.utc).isoformat(),
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

    def send(self, message: str):
        if not self.token or not self.chat_id:
            log.warning("Telegram not configured.")
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

def tg_log(title: str, body: str, emoji: str = "ℹ️"):
    msg = f"{emoji} <b>{title}</b>\n\n{body}"
    tg.send(msg)
    log.info(f"{title} | {body.replace(chr(10), ' | ')}")

# ==================== BINANCE ====================
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY, testnet=TESTNET)

def safe_api_call(func, *args, retries=3, **kwargs):
    """Wrapper for API calls with retry on rate-limit / network errors."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except BinanceAPIException as e:
            if e.code == -1003:       # Too many requests
                wait = 2 ** attempt
                log.warning(f"⚠️ Rate limit - انتظار {wait}s")
                time.sleep(wait)
            elif e.code == -1021:     # Timestamp
                log.warning("⏰ Timestamp error - إعادة")
                time.sleep(1)
            else:
                raise
        except BinanceRequestException as e:
            log.warning(f"🌐 Network error: {e}")
            time.sleep(2)
    raise Exception(f"فشل بعد {retries} محاولات")

def get_klines(symbol: str, interval: str, limit: int = 300) -> pd.DataFrame:
    raw = safe_api_call(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qav","trades","tbbav","tbqav","ignore"
    ])
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

def get_balance_usdt() -> float:
    try:
        balances = safe_api_call(client.futures_account_balance)
        for b in balances:
            if b["asset"] == "USDT":
                return float(b["balance"])
    except Exception as e:
        log.error(f"Balance error: {e}")
    return 0.0

def get_all_prices() -> dict:
    """Single batch call for all symbol prices (weight-efficient)."""
    try:
        tickers = safe_api_call(client.futures_ticker_price)
        return {t["symbol"]: float(t["price"]) for t in tickers}
    except Exception as e:
        log.error(f"Batch ticker error: {e}")
        return {}

def set_leverage_and_margin(symbol: str):
    try:
        safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=LEVERAGE)
        try:
            safe_api_call(client.futures_change_margin_type, symbol=symbol, marginType="ISOLATED")
        except BinanceAPIException as e:
            if "No need to change margin type" not in str(e):
                log.warning(f"Margin type: {e}")
    except Exception as e:
        log.error(f"Leverage error {symbol}: {e}")

# ==================== INDICATORS ====================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def macd(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    dif = ema_fast - ema_slow
    dea = ema(dif, signal)
    hist = dif - dea
    return dif, dea, hist

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    return (typical * df["volume"]).cumsum() / df["volume"].cumsum()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema7"]    = ema(df["close"], 7)
    df["ema25"]   = ema(df["close"], 25)
    df["ema50"]   = ema(df["close"], 50)
    df["ema200"]  = ema(df["close"], 200)
    df["dif"], df["dea"], df["hist"] = macd(df["close"])
    df["rsi"]     = rsi(df["close"])
    df["atr"]     = atr(df)
    df["vwap"]    = vwap(df)
    df["vol_ma5"] = df["volume"].rolling(5).mean()
    return df

# ==================== SESSION FILTER ====================
def in_trading_session() -> bool:
    now = datetime.now(timezone.utc)
    # London 07:00-16:00 UTC + NY 12:00-21:00 UTC → combined 07:00-21:00 UTC
    return 7 <= now.hour < 21

# ==================== SIGNAL EVALUATION ====================
def evaluate_signal(df: pd.DataFrame, direction: str) -> dict:
    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    score = 0
    reasons = []
    price = last["close"]

    if direction == "LONG":
        if price > last["ema200"]:
            score += 2; reasons.append("✅ فوق EMA200 (سياق صاعد) +2")
        if price > last["ema50"]:
            score += 1; reasons.append("✅ فوق EMA50 +1")
        if prev["dif"] <= prev["dea"] and last["dif"] > last["dea"]:
            score += 2; reasons.append("✅ تقاطع MACD إيجابي +2")
        if prev2["hist"] < prev["hist"] < 0 and last["hist"] > prev["hist"]:
            score += 1; reasons.append("✅ تحول الهيستوجرام للأخضر +1")
        if last["volume"] > last["vol_ma5"]:
            score += 2; reasons.append("✅ حجم أعلى من المتوسط +2")
        if price > last["vwap"]:
            score += 1; reasons.append("✅ فوق VWAP +1")
        if last["rsi"] > prev["rsi"] and price < prev["close"]:
            score += 1; reasons.append("✅ انحراف RSI إيجابي +1")

        entry = price
        sl    = entry - (1.5 * last["atr"])
        tp1   = last["ema50"]
        tp2   = last["ema200"]

    else:  # SHORT
        if price < last["ema200"]:
            score += 2; reasons.append("✅ تحت EMA200 (سياق هابط) +2")
        if price < last["ema50"]:
            score += 1; reasons.append("✅ تحت EMA50 +1")
        if prev["dif"] >= prev["dea"] and last["dif"] < last["dea"]:
            score += 2; reasons.append("✅ تقاطع MACD سلبي +2")
        if prev2["hist"] > prev["hist"] > 0 and last["hist"] < prev["hist"]:
            score += 1; reasons.append("✅ تحول الهيستوجرام للأحمر +1")
        if last["volume"] > last["vol_ma5"]:
            score += 2; reasons.append("✅ حجم أعلى من المتوسط +2")
        if price < last["vwap"]:
            score += 1; reasons.append("✅ تحت VWAP +1")
        if last["rsi"] < prev["rsi"] and price > prev["close"]:
            score += 1; reasons.append("✅ انحراف RSI سلبي +1")

        entry = price
        sl    = entry + (1.5 * last["atr"])
        tp1   = last["ema50"]
        tp2   = last["ema200"]

    return {
        "score": score,
        "reasons": reasons,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "atr": last["atr"],
        "rsi": last["rsi"]
    }

# ==================== HELPERS ====================
def has_open_position(symbol: str) -> bool:
    with lock:
        if symbol in open_positions:
            return True
    try:
        positions = safe_api_call(client.futures_position_information, symbol=symbol)
        for p in positions:
            if float(p["positionAmt"]) != 0:
                return True
    except Exception as e:
        log.error(f"Position check error: {e}")
    return False

def get_active_count() -> int:
    with lock:
        return len(open_positions)

def round_step(symbol: str, qty: float) -> float:
    info = get_exchange_info_cached()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                    precision = int(round(-np.log10(step)))
                    return round(np.floor(qty / step) * step, precision)
    return round(qty, 3)

def round_price(symbol: str, price: float) -> float:
    info = get_exchange_info_cached()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                    precision = int(round(-np.log10(tick)))
                    return round(round(price / tick) * tick, precision)
    return round(price, 4)

# ==================== OPEN TRADE ====================
def open_trade(symbol: str, direction: str, signal: dict):
    try:
        set_leverage_and_margin(symbol)

        price = signal["entry"]
        qty_usdt = POSITION_SIZE_USDT * LEVERAGE
        qty = round_step(symbol, qty_usdt / price)

        side = SIDE_BUY if direction == "LONG" else SIDE_SELL
        opposite = SIDE_SELL if direction == "LONG" else SIDE_BUY

        order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=side,
            type=ORDER_TYPE_MARKET, quantity=qty
        )
        fill_price = float(order.get("avgPrice", price)) or price

        sl_price  = round_price(symbol, signal["sl"])
        tp1_price = round_price(symbol, signal["tp1"])
        tp2_price = round_price(symbol, signal["tp2"])

        # Stop Loss
        sl_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_STOP_MARKET,
            stopPrice=sl_price, closePosition=True,
            workingType="MARK_PRICE"
        )

        # TP1 (50%)
        half_qty = round_step(symbol, qty / 2)
        tp1_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
            stopPrice=tp1_price, quantity=half_qty,
            reduceOnly=True, workingType="MARK_PRICE"
        )

        # TP2 (50%)
        tp2_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
            stopPrice=tp2_price, quantity=qty - half_qty,
            reduceOnly=True, workingType="MARK_PRICE"
        )

        with lock:
            open_positions[symbol] = {
                "side": direction,
                "entry": fill_price,
                "qty": qty,
                "atr": signal["atr"],
                "initial_sl": sl_price,
                "current_sl": sl_price,
                "sl_order_id": sl_order["orderId"],
                "tp1_order_id": tp1_order["orderId"],
                "tp2_order_id": tp2_order["orderId"],
                "tp1_price": tp1_price,
                "tp2_price": tp2_price,
                "tp1_hit": False,
                "trailing_stage": 0,
                "opened_at": datetime.now(timezone.utc),
                "source": "BOT"
            }

        body = (
            f"💠 <b>العملة:</b> {symbol}\n"
            f"📈 <b>الاتجاه:</b> {direction}\n"
            f"💵 <b>سعر الدخول:</b> {fill_price}\n"
            f"📦 <b>الكمية:</b> {qty}\n"
            f"💼 <b>الحجم الفعلي:</b> {round(qty * fill_price, 2)} USDT\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n\n"
            f"🛑 <b>وقف الخسارة:</b> {sl_price} (1.5×ATR)\n"
            f"🎯 <b>هدف 1:</b> {tp1_price} (EMA50) - 50%\n"
            f"🎯 <b>هدف 2:</b> {tp2_price} (EMA200) - 50%\n"
            f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n\n"
            f"📊 <b>النقاط:</b> {signal['score']}/10\n"
            f"📝 <b>الأسباب:</b>\n" + "\n".join(signal["reasons"])
        )
        tg_log("🚀 تم فتح صفقة", body, "🚀")

    except (BinanceAPIException, BinanceOrderException) as e:
        tg_log("❌ فشل فتح صفقة", f"العملة: {symbol}\nالخطأ: {e.message}", "❌")
    except Exception as e:
        tg_log("❌ خطأ غير متوقع", f"العملة: {symbol}\n{e}", "❌")

# ==================== TRAILING STOP ====================
def update_trailing_stop(symbol: str, info: dict, current_price: float):
    """
    Stages (LONG):
      0 → initial SL (entry - 1.5 ATR)
      1 → price ≥ entry + 1 ATR → SL = entry (BE)
      2 → price ≥ entry + 2 ATR → SL = entry + 1 ATR
      3 → price ≥ entry + 3 ATR → SL = entry + 2 ATR
      4+ → every new +1 ATR move → SL = current_price - 1 ATR
    SHORT is mirrored.
    """
    if not TRAILING_ENABLED:
        return

    side = info["side"]
    entry = info["entry"]
    atr_val = info["atr"]
    stage = info["trailing_stage"]
    current_sl = info["current_sl"]
    opposite = SIDE_SELL if side == "LONG" else SIDE_BUY

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
            trailing_sl = round_price(symbol, current_price - 1 * atr_val)
            if trailing_sl > current_sl:
                new_sl = trailing_sl; new_stage = stage + 1
    else:  # SHORT
        move = entry - current_price
        if stage == 0 and move >= 1 * atr_val:
            new_sl = round_price(symbol, entry); new_stage = 1
        elif stage == 1 and move >= 2 * atr_val:
            new_sl = round_price(symbol, entry - 1 * atr_val); new_stage = 2
        elif stage == 2 and move >= 3 * atr_val:
            new_sl = round_price(symbol, entry - 2 * atr_val); new_stage = 3
        elif stage >= 3:
            trailing_sl = round_price(symbol, current_price + 1 * atr_val)
            if trailing_sl < current_sl:
                new_sl = trailing_sl; new_stage = stage + 1

    if new_sl is None or new_sl == current_sl:
        return

    try:
        # Cancel old SL order if it exists and belongs to bot
        if info.get("sl_order_id"):
            try:
                safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=info["sl_order_id"])
            except BinanceAPIException as e:
                log.warning(f"Cancel SL warning {symbol}: {e.message}")

        # Create new SL
        new_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_STOP_MARKET,
            stopPrice=new_sl, closePosition=True,
            workingType="MARK_PRICE"
        )

        with lock:
            info["sl_order_id"] = new_order["orderId"]
            info["current_sl"] = new_sl
            info["trailing_stage"] = new_stage

        # Protection description
        if new_stage == 1:
            protection = "نقطة الدخول (Break-Even)"
        else:
            if side == "LONG":
                protection = f"ربح مضمون +{round(new_sl - entry, 6)}"
            else:
                protection = f"ربح مضمون +{round(entry - new_sl, 6)}"

        tg_log(
            "🔄 تحديث الوقف المتحرك",
            f"💠 <b>العملة:</b> {symbol}\n"
            f"📊 <b>الاتجاه:</b> {side}\n"
            f"💰 <b>السعر الحالي:</b> {current_price}\n"
            f"🛑 <b>الوقف الجديد:</b> {new_sl}\n"
            f"📈 <b>المرحلة:</b> {new_stage}\n"
            f"🛡️ <b>الحماية:</b> {protection}",
            "🔄"
        )
    except Exception as e:
        log.error(f"Trailing update error {symbol}: {e}")

# ==================== MANUAL POSITION IMPORT ====================
def import_manual_positions():
    """
    On startup:
      1. Detect all open positions on SYMBOLS not tracked by bot.
      2. Fetch full details (entry, qty, leverage, PnL, liq price).
      3. Fetch existing SL/TP orders.
      4. Register into bot's management.
      5. Add auto SL if none exists.
    """
    imported = []
    try:
        positions = safe_api_call(client.futures_position_information)
        for p in positions:
            symbol = p["symbol"]
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            if symbol not in SYMBOLS:
                log.info(f"⏭️ {symbol} ليس في SYMBOLS - تخطي")
                continue
            with lock:
                if symbol in open_positions:
                    continue

            # 1. Core position data
            entry = float(p["entryPrice"])
            side = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            leverage = int(p.get("leverage", LEVERAGE))
            margin_type = p.get("marginType", "isolated")
            unrealized_pnl = float(p.get("unRealizedProfit", 0))
            mark_price = float(p.get("markPrice", entry))
            liquidation = float(p.get("liquidationPrice", 0))

            # 2. Fetch existing SL/TP orders
            sl_order_id = None
            tp1_order_id = None
            tp2_order_id = None
            sl_price = None
            tp1_price = None
            tp2_price = None
            pending_orders = []

            try:
                orders = safe_api_call(client.futures_get_open_orders, symbol=symbol)
                for o in orders:
                    o_type = o["type"]
                    o_stop = float(o.get("stopPrice", 0)) or None
                    pending_orders.append({
                        "id": o["orderId"],
                        "type": o_type,
                        "side": o["side"],
                        "stopPrice": o_stop,
                    })

                    if o_type in ("STOP_MARKET", "STOP"):
                        sl_order_id = o["orderId"]
                        sl_price = o_stop
                    elif o_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        if tp1_order_id is None:
                            tp1_order_id = o["orderId"]
                            tp1_price = o_stop
                        else:
                            tp2_order_id = o["orderId"]
                            tp2_price = o_stop
            except Exception as e:
                log.warning(f"تعذر جلب أوامر {symbol}: {e}")

            # 3. ATR for trailing
            try:
                df = get_klines(symbol, TIMEFRAME, limit=100)
                df = add_indicators(df)
                atr_val = float(df.iloc[-1]["atr"])
            except Exception:
                atr_val = entry * 0.01

            # 4. Register
            with lock:
                open_positions[symbol] = {
                    "side": side,
                    "entry": entry,
                    "qty": qty,
                    "atr": atr_val,
                    "initial_sl": sl_price if sl_price else (
                        entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                    ),
                    "current_sl": sl_price,
                    "sl_order_id": sl_order_id,
                    "tp1_order_id": tp1_order_id,
                    "tp2_order_id": tp2_order_id,
                    "tp1_price": tp1_price,
                    "tp2_price": tp2_price,
                    "tp1_hit": False,
                    "trailing_stage": 0,
                    "opened_at": datetime.now(timezone.utc),
                    "source": "MANUAL",
                    "leverage": leverage,
                    "margin_type": margin_type,
                }
            imported.append(symbol)

            # 5. Detailed notification
            orders_info = ""
            if pending_orders:
                orders_info = "\n\n📋 <b>الأوامر المعلقة:</b>\n"
                for o in pending_orders:
                    label = "🛑 SL" if "STOP" in o["type"] and "TAKE" not in o["type"] else "🎯 TP"
                    orders_info += f"  {label} | {o['stopPrice']} | {o['type']}\n"
            else:
                orders_info = "\n\n⚠️ <b>لا توجد أوامر SL/TP معلقة</b>"

            tg_log(
                "📥 استيراد صفقة يدوية",
                f"💠 <b>العملة:</b> {symbol}\n"
                f"📊 <b>الاتجاه:</b> {side}\n"
                f"💵 <b>سعر الدخول:</b> {entry}\n"
                f"📦 <b>الكمية:</b> {qty}\n"
                f"⚙️ <b>الرافعة:</b> {leverage}x ({margin_type})\n"
                f"📈 <b>السعر الحالي:</b> {mark_price}\n"
                f"💰 <b>ربح/خسارة:</b> {unrealized_pnl:.2f} USDT\n"
                f"💥 <b>سعر التصفية:</b> {liquidation}\n"
                f"📊 <b>ATR:</b> {atr_val:.6f}\n"
                f"🛡️ <b>SL الحالي:</b> {sl_price if sl_price else 'لا يوجد'}"
                f"{orders_info}",
                "📥"
            )

            # 6. Auto SL if none exists
            if sl_price is None and AUTO_SL_MANUAL:
                try:
                    opposite = SIDE_SELL if side == "LONG" else SIDE_BUY
                    auto_sl = entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                    auto_sl = round_price(symbol, auto_sl)
                    sl_order = safe_api_call(
                        client.futures_create_order,
                        symbol=symbol, side=opposite,
                        type=FUTURE_ORDER_TYPE_STOP_MARKET,
                        stopPrice=auto_sl, closePosition=True,
                        workingType="MARK_PRICE"
                    )
                    with lock:
                        open_positions[symbol]["sl_order_id"] = sl_order["orderId"]
                        open_positions[symbol]["current_sl"] = auto_sl
                    tg_log(
                        "🛡️ إضافة وقف خسارة تلقائي",
                        f"💠 <b>العملة:</b> {symbol}\n"
                        f"🛑 <b>الوقف الجديد:</b> {auto_sl}\n"
                        f"📝 <b>السبب:</b> لا يوجد SL على الصفقة اليدوية",
                        "🛡️"
                    )
                except Exception as e:
                    log.error(f"Auto SL error {symbol}: {e}")

        if not imported:
            log.info("🔎 لا توجد صفقات يدوية لاستيرادها")
        return imported

    except Exception as e:
        log.error(f"import_manual_positions error: {e}")
        return []

# ==================== MONITOR POSITIONS ====================
def monitor_positions():
    last_prices = {}
    while True:
        try:
            with lock:
                symbols = list(open_positions.keys())

            if not symbols:
                time.sleep(MONITOR_INTERVAL)
                continue

            # Batch prices (1 request for all)
            prices = get_all_prices()

            for symbol in symbols:
                with lock:
                    info = open_positions.get(symbol)
                if not info:
                    continue

                current_price = prices.get(symbol)
                if not current_price:
                    continue

                # Always run trailing
                update_trailing_stop(symbol, info, current_price)

                # Only check position info if price moved ≥ 0.1%
                last_p = last_prices.get(symbol, 0)
                price_moved = last_p == 0 or abs(current_price - last_p) / current_price > 0.001
                last_prices[symbol] = current_price

                if not price_moved:
                    continue

                pos = safe_api_call(client.futures_position_information, symbol=symbol)
                for p in pos:
                    if float(p["positionAmt"]) == 0:
                        with lock:
                            closed_info = open_positions.pop(symbol, None)
                        if closed_info:
                            # Cancel any remaining orders
                            for oid_key in ["sl_order_id", "tp1_order_id", "tp2_order_id"]:
                                oid = closed_info.get(oid_key)
                                if oid:
                                    try:
                                        safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=oid)
                                    except Exception:
                                        pass
                            duration = datetime.now(timezone.utc) - closed_info["opened_at"]
                            body = (
                                f"💠 <b>العملة:</b> {symbol}\n"
                                f"📊 <b>الاتجاه:</b> {closed_info['side']}\n"
                                f"💵 <b>الدخول:</b> {closed_info['entry']}\n"
                                f"🛑 <b>آخر وقف:</b> {closed_info['current_sl']}\n"
                                f"📈 <b>المرحلة النهائية:</b> {closed_info['trailing_stage']}\n"
                                f"📦 <b>المصدر:</b> {closed_info.get('source', 'BOT')}\n"
                                f"⏱️ <b>المدة:</b> {duration}"
                            )
                            tg_log("✅ تم إغلاق الصفقة", body, "✅")
            time.sleep(MONITOR_INTERVAL)
        except Exception as e:
            log.error(f"Monitor error: {e}")
            time.sleep(MONITOR_INTERVAL)

# ==================== SCANNER ====================
def wait_for_candle_close(interval_minutes: int = 15):
    now = datetime.now(timezone.utc)
    next_close = (now + timedelta(minutes=interval_minutes)).replace(
        minute=0, second=0, microsecond=0
    )
    wait = (next_close - now).total_seconds() + 3
    log.info(f"⏳ انتظار إغلاق الشمعة: {wait:.0f} ثانية")
    time.sleep(max(wait, 5))

def scan_once():
    if not in_trading_session():
        log.info("⏸️ خارج جلسة التداول (London/NY فقط)")
        return

    active_count = get_active_count()
    if active_count >= MAX_CONCURRENT_TRADES:
        log.info(f"⛔ وصلنا للحد الأقصى ({active_count}/{MAX_CONCURRENT_TRADES}) - تخطي المسح")
        return

    log.info(f"🔍 مسح: {len(SYMBOLS)} عملات | نشطة: {active_count}/{MAX_CONCURRENT_TRADES}")

    for symbol in SYMBOLS:
        try:
            if get_active_count() >= MAX_CONCURRENT_TRADES:
                log.info("⛔ وصلنا للحد الأقصى خلال المسح - توقف")
                break

            if has_open_position(symbol):
                log.info(f"⏭️ {symbol}: صفقة مفتوحة، تخطي")
                continue

            df = get_klines(symbol, TIMEFRAME, limit=300)
            df = add_indicators(df)

            # Candle close rule: use last CLOSED candle
            df_closed = df.iloc[:-1].copy()

            long_sig  = evaluate_signal(df_closed, "LONG")
            short_sig = evaluate_signal(df_closed, "SHORT")

            log.info(f"{symbol} | LONG={long_sig['score']} | SHORT={short_sig['score']}")

            if long_sig["score"] >= MIN_SCORE:
                open_trade(symbol, "LONG", long_sig)
            elif short_sig["score"] >= MIN_SCORE:
                open_trade(symbol, "SHORT", short_sig)

        except Exception as e:
            log.error(f"Scan error {symbol}: {e}")
            tg_log("⚠️ خطأ في المسح", f"العملة: {symbol}\n{e}", "⚠️")

# ==================== MAIN LOOP ====================
def main_loop():
    tg_log(
        "🤖 بدء تشغيل البوت",
        f"📊 <b>الاستراتيجية:</b> Early Momentum Catch v2.1\n"
        f"⏱️ <b>الفريم:</b> {TIMEFRAME}\n"
        f"💼 <b>حجم الصفقة:</b> {POSITION_SIZE_USDT} USDT\n"
        f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n"
        f"📋 <b>العملات:</b> {', '.join(SYMBOLS)}\n"
        f"🔢 <b>حد الصفقات المتزامنة:</b> {MAX_CONCURRENT_TRADES}\n"
        f"🎯 <b>الحد الأدنى للنقاط:</b> {MIN_SCORE}/10\n"
        f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n"
        f"🛡️ <b>SL تلقائي للصفقات اليدوية:</b> {'مُفعّل' if AUTO_SL_MANUAL else 'معطّل'}\n"
        f"🌐 <b>الوضع:</b> {'TESTNET' if TESTNET else 'LIVE'}\n"
        f"💰 <b>الرصيد:</b> {get_balance_usdt():.2f} USDT",
        "🤖"
    )

    # Import any pre-existing manual positions
    import_manual_positions()

    while True:
        try:
            wait_for_candle_close(SCAN_INTERVAL_MIN)
            scan_once()
        except Exception as e:
            log.error(f"Main loop error: {e}")
            tg_log("💥 خطأ رئيسي", str(e), "💥")
            time.sleep(60)

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    # Flask (health check) in background
    threading.Thread(target=run_flask, daemon=True).start()
    # Position monitor in background
    threading.Thread(target=monitor_positions, daemon=True).start()
    # Small delay to let services start
    time.sleep(2)
    # Main loop (blocking)
    main_loop()
