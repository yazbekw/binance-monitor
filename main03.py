"""
Momentum Bot v2.2
==================
Strategy : Early Momentum Catch
Indicators: EMA (7/25/50/200) + MACD + RSI + ATR + VWAP + Volume MA
Features :
  - Binance Futures execution (Market + SL + 2 TPs)
  - Balance check based on MARGIN (not full position size)
  - Multi-stage Trailing Stop (BE → locked profit → dynamic trail)
  - Max concurrent trades limit
  - Manual position import (full details + auto SL)
  - Exchange info caching + batch price fetching
  - Session filter (London + NY)
  - Detailed error reporting with Binance error codes
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
TESTNET            = os.getenv("TESTNET", "false").strip().lower() == "true"
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", 3))
TRAILING_ENABLED   = os.getenv("TRAILING_ENABLED", "True").lower() == "true"
MIN_SCORE          = int(os.getenv("MIN_SCORE", 6))
MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))
SCAN_INTERVAL_MIN  = int(os.getenv("SCAN_INTERVAL_MIN", 15))
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "True").lower() == "true"
MARGIN_SAFETY      = float(os.getenv("MARGIN_SAFETY", 1.3))  # 30% safety buffer

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("MomentumBot")

# ==================== STATE ====================
open_positions = {}
lock = threading.Lock()

# ==================== CACHING ====================
_exchange_info_cache = None
_exchange_info_lock = threading.Lock()

# ==================== FLASK ====================
app = Flask(__name__)

@app.route("/")
def health():
    with lock:
        active = list(open_positions.keys())
    return {
        "status": "alive",
        "time": datetime.now(timezone.utc).isoformat(),
        "mode": "TESTNET" if TESTNET else "LIVE",
        "symbols": SYMBOLS,
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
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except BinanceAPIException as e:
            if e.code == -1003:
                wait = 2 ** attempt
                log.warning(f"⚠️ Rate limit - انتظار {wait}s")
                time.sleep(wait)
            elif e.code == -1021:
                log.warning("⏰ Timestamp error - إعادة")
                time.sleep(1)
            else:
                raise
        except BinanceRequestException as e:
            log.warning(f"🌐 Network error: {e}")
            time.sleep(2)
    raise Exception(f"فشل بعد {retries} محاولات")

def get_exchange_info_cached():
    global _exchange_info_cache
    with _exchange_info_lock:
        if _exchange_info_cache is None:
            _exchange_info_cache = client.futures_exchange_info()
            log.info("✅ تم تحميل exchange_info إلى الكاش")
        return _exchange_info_cache

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

def get_balance() -> dict:
    """إرجاع dict فيه الرصيد الكلي، المتاح، الربح غير المحقق."""
    try:
        balances = safe_api_call(client.futures_account_balance)
        for b in balances:
            if b["asset"] == "USDT":
                return {
                    "balance": float(b["balance"]),
                    "available": float(b.get("availableBalance", b["balance"])),
                    "unrealized_pnl": float(b.get("crossUnPnl", 0))
                }
    except Exception as e:
        log.error(f"Balance error: {e}")
    return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}

def get_balance_display() -> str:
    b = get_balance()
    return (
        f"💰 <b>الرصيد الكلي:</b> {b['balance']:.2f} USDT\n"
        f"💵 <b>المتاح:</b> {b['available']:.2f} USDT\n"
        f"📈 <b>غير محقق:</b> {b['unrealized_pnl']:+.2f} USDT"
    )

def get_all_prices() -> dict:
    try:
        tickers = safe_api_call(client.futures_ticker_price)
        return {t["symbol"]: float(t["price"]) for t in tickers}
    except Exception as e:
        log.error(f"Batch ticker error: {e}")
        return {}

def set_leverage_and_margin(symbol: str):
    try:
        safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=LEVERAGE)
    except Exception as e:
        log.warning(f"Leverage warn {symbol}: {e}")
    try:
        safe_api_call(client.futures_change_margin_type, symbol=symbol, marginType="ISOLATED")
    except BinanceAPIException as e:
        if "No need to change margin type" not in str(e):
            log.warning(f"Margin type warn {symbol}: {e}")

# ==================== INDICATORS ====================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def macd(series: pd.Series, fast=12, slow=26, signal=9):
    dif = ema(series, fast) - ema(series, slow)
    dea = ema(dif, signal)
    return dif, dea, dif - dea

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
    return 7 <= now.hour < 21

# ==================== SIGNAL ====================
def evaluate_signal(df: pd.DataFrame, direction: str) -> dict:
    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    score = 0
    reasons = []
    price = last["close"]

    if direction == "LONG":
        if price > last["ema200"]:  score += 2; reasons.append("✅ فوق EMA200 +2")
        if price > last["ema50"]:   score += 1; reasons.append("✅ فوق EMA50 +1")
        if prev["dif"] <= prev["dea"] and last["dif"] > last["dea"]:
            score += 2; reasons.append("✅ تقاطع MACD إيجابي +2")
        if prev2["hist"] < prev["hist"] < 0 and last["hist"] > prev["hist"]:
            score += 1; reasons.append("✅ تحول الهيستوجرام +1")
        if last["volume"] > last["vol_ma5"]:
            score += 2; reasons.append("✅ حجم مرتفع +2")
        if price > last["vwap"]:    score += 1; reasons.append("✅ فوق VWAP +1")
        if last["rsi"] > prev["rsi"] and price < prev["close"]:
            score += 1; reasons.append("✅ انحراف RSI +1")
        entry = price
        sl    = entry - (1.5 * last["atr"])
        tp1   = last["ema50"]
        tp2   = last["ema200"]
    else:
        if price < last["ema200"]:  score += 2; reasons.append("✅ تحت EMA200 +2")
        if price < last["ema50"]:   score += 1; reasons.append("✅ تحت EMA50 +1")
        if prev["dif"] >= prev["dea"] and last["dif"] < last["dea"]:
            score += 2; reasons.append("✅ تقاطع MACD سلبي +2")
        if prev2["hist"] > prev["hist"] > 0 and last["hist"] < prev["hist"]:
            score += 1; reasons.append("✅ تحول الهيستوجرام +1")
        if last["volume"] > last["vol_ma5"]:
            score += 2; reasons.append("✅ حجم مرتفع +2")
        if price < last["vwap"]:    score += 1; reasons.append("✅ تحت VWAP +1")
        if last["rsi"] < prev["rsi"] and price > prev["close"]:
            score += 1; reasons.append("✅ انحراف RSI +1")
        entry = price
        sl    = entry + (1.5 * last["atr"])
        tp1   = last["ema50"]
        tp2   = last["ema200"]

    return {
        "score": score, "reasons": reasons,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "atr": last["atr"], "rsi": last["rsi"]
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

def get_min_notional(symbol: str) -> float:
    """الحد الأدنى لقيمة الصفقة بالدولار"""
    info = get_exchange_info_cached()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "MIN_NOTIONAL":
                    return float(f.get("notional", 5))
    return 5.0

# ==================== OPEN TRADE ====================
def open_trade(symbol: str, direction: str, signal: dict):
    try:
        # === 1. فحص الرصيد بـ MARGIN وليس الحجم الكامل ===
        bal = get_balance()
        required_margin = POSITION_SIZE_USDT / LEVERAGE
        total_required  = required_margin * MARGIN_SAFETY

        if bal["available"] < total_required:
            tg_log(
                "⚠️ رصيد غير كافٍ",
                f"💠 <b>العملة:</b> {symbol}\n"
                f"💰 <b>المتاح:</b> {bal['available']:.2f} USDT\n"
                f"📊 <b>الهامش المطلوب:</b> {required_margin:.2f} USDT\n"
                f"🛡️ <b>مع هامش الأمان:</b> {total_required:.2f} USDT\n"
                f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n"
                f"💼 <b>حجم الصفقة:</b> {POSITION_SIZE_USDT} USDT\n\n"
                f"<b>الحل:</b> حوّل USDT إلى Futures Wallet",
                "⚠️"
            )
            return

        # === 2. حساب الكمية ===
        set_leverage_and_margin(symbol)
        price = signal["entry"]
        qty = round_step(symbol, (POSITION_SIZE_USDT * LEVERAGE) / price)

        # فحص الحد الأدنى للصفقة
        notional = qty * price
        min_notional = get_min_notional(symbol)
        if notional < min_notional:
            tg_log(
                "⚠️ حجم الصفقة أقل من الحد الأدنى",
                f"💠 <b>العملة:</b> {symbol}\n"
                f"📊 <b>قيمة الصفقة:</b> {notional:.2f} USDT\n"
                f"📉 <b>الحد الأدنى:</b> {min_notional} USDT",
                "⚠️"
            )
            return

        side = SIDE_BUY if direction == "LONG" else SIDE_SELL
        opposite = SIDE_SELL if direction == "LONG" else SIDE_BUY

        # === 3. أمر الدخول Market ===
        order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=side,
            type=ORDER_TYPE_MARKET, quantity=qty
        )
        fill_price = float(order.get("avgPrice", price)) or price
        if fill_price == 0:
            fill_price = price

        # === 4. SL ===
        sl_price  = round_price(symbol, signal["sl"])
        tp1_price = round_price(symbol, signal["tp1"])
        tp2_price = round_price(symbol, signal["tp2"])

        sl_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_STOP_MARKET,
            stopPrice=sl_price, closePosition=True,
            workingType="MARK_PRICE"
        )

        # === 5. TP1 ===
        half_qty = round_step(symbol, qty / 2)
        tp1_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
            stopPrice=tp1_price, quantity=half_qty,
            reduceOnly=True, workingType="MARK_PRICE"
        )

        # === 6. TP2 ===
        remaining_qty = round_step(symbol, qty - half_qty)
        tp2_order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=opposite,
            type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
            stopPrice=tp2_price, quantity=remaining_qty,
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
            f"💼 <b>قيمة الصفقة:</b> {round(qty * fill_price, 2)} USDT\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n\n"
            f"🛑 <b>وقف الخسارة:</b> {sl_price}\n"
            f"🎯 <b>هدف 1:</b> {tp1_price} (50%)\n"
            f"🎯 <b>هدف 2:</b> {tp2_price} (50%)\n"
            f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n\n"
            f"📊 <b>النقاط:</b> {signal['score']}/10\n"
            f"📝 <b>الأسباب:</b>\n" + "\n".join(signal["reasons"])
        )
        tg_log("🚀 تم فتح صفقة", body, "🚀")

    except BinanceAPIException as e:
        # تشخيص مفصّل
        diag = f"Binance Error <b>{e.code}</b>: {e.message}\n\n"
        hints = {
            -2015: "🔍 الأسباب: صلاحية Futures غير مُفعّلة / الحساب لم يُفعّل Futures / IP غير مسموح / Testnet vs Live mismatch",
            -2019: "🔍 السبب: هامش غير كافٍ. حوّل USDT إلى Futures Wallet أو قلّل POSITION_SIZE_USDT",
            -4164: "🔍 السبب: قيمة الصفقة أقل من الحد الأدنى (5-10 USDT)",
            -2027: "🔍 السبب: تجاوزت الحد الأقصى للمركز",
            -1111: "🔍 السبب: دقة الكمية خاطئة (خطأ برمجي في round_step)",
            -4131: "🔍 السبب: أمر TP/SL غير موجود. ألغِ الأوامر المعلقة يدوياً وأعد التشغيل",
        }
        diag += hints.get(e.code, "🔍 راجع Logs في Render لتفاصيل أكثر")
        tg_log(f"❌ فشل فتح صفقة {symbol}", diag, "❌")
    except BinanceOrderException as e:
        tg_log(f"❌ فشل الأمر {symbol}", f"{e.message}", "❌")
    except Exception as e:
        tg_log(f"❌ خطأ غير متوقع {symbol}", str(e), "❌")

# ==================== TRAILING STOP ====================
def update_trailing_stop(symbol: str, info: dict, current_price: float):
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
    else:
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
        if info.get("sl_order_id"):
            try:
                safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=info["sl_order_id"])
            except BinanceAPIException as e:
                log.warning(f"Cancel SL warning {symbol}: {e.message}")

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

# ==================== MANUAL IMPORT ====================
def import_manual_positions():
    imported = []
    try:
        positions = safe_api_call(client.futures_position_information)
        for p in positions:
            symbol = p["symbol"]
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            if symbol not in SYMBOLS:
                continue
            with lock:
                if symbol in open_positions:
                    continue

            entry = float(p["entryPrice"])
            side = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            leverage = int(p.get("leverage", LEVERAGE))
            margin_type = p.get("marginType", "isolated")
            unrealized_pnl = float(p.get("unRealizedProfit", 0))
            mark_price = float(p.get("markPrice", entry))
            liquidation = float(p.get("liquidationPrice", 0))

            sl_order_id = tp1_order_id = tp2_order_id = None
            sl_price = tp1_price = tp2_price = None
            pending_orders = []

            try:
                orders = safe_api_call(client.futures_get_open_orders, symbol=symbol)
                for o in orders:
                    o_type = o["type"]
                    o_stop = float(o.get("stopPrice", 0)) or None
                    pending_orders.append({"id": o["orderId"], "type": o_type, "stopPrice": o_stop})

                    if o_type in ("STOP_MARKET", "STOP"):
                        sl_order_id = o["orderId"]; sl_price = o_stop
                    elif o_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        if tp1_order_id is None:
                            tp1_order_id = o["orderId"]; tp1_price = o_stop
                        else:
                            tp2_order_id = o["orderId"]; tp2_price = o_stop
            except Exception as e:
                log.warning(f"Orders fetch {symbol}: {e}")

            try:
                df = get_klines(symbol, TIMEFRAME, limit=100)
                df = add_indicators(df)
                atr_val = float(df.iloc[-1]["atr"])
            except Exception:
                atr_val = entry * 0.01

            with lock:
                open_positions[symbol] = {
                    "side": side, "entry": entry, "qty": qty, "atr": atr_val,
                    "initial_sl": sl_price if sl_price else (
                        entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                    ),
                    "current_sl": sl_price, "sl_order_id": sl_order_id,
                    "tp1_order_id": tp1_order_id, "tp2_order_id": tp2_order_id,
                    "tp1_price": tp1_price, "tp2_price": tp2_price,
                    "tp1_hit": False, "trailing_stage": 0,
                    "opened_at": datetime.now(timezone.utc),
                    "source": "MANUAL", "leverage": leverage, "margin_type": margin_type,
                }
            imported.append(symbol)

            orders_info = ""
            if pending_orders:
                orders_info = "\n\n📋 <b>الأوامر المعلقة:</b>\n"
                for o in pending_orders:
                    label = "🛑 SL" if "STOP" in o["type"] and "TAKE" not in o["type"] else "🎯 TP"
                    orders_info += f"  {label} | {o['stopPrice']} | {o['type']}\n"
            else:
                orders_info = "\n\n⚠️ <b>لا توجد أوامر SL/TP</b>"

            tg_log(
                "📥 استيراد صفقة يدوية",
                f"💠 <b>العملة:</b> {symbol}\n"
                f"📊 <b>الاتجاه:</b> {side}\n"
                f"💵 <b>الدخول:</b> {entry}\n"
                f"📦 <b>الكمية:</b> {qty}\n"
                f"⚙️ <b>الرافعة:</b> {leverage}x ({margin_type})\n"
                f"📈 <b>الحالي:</b> {mark_price}\n"
                f"💰 <b>PnL:</b> {unrealized_pnl:.2f} USDT\n"
                f"💥 <b>التصفية:</b> {liquidation}\n"
                f"📊 <b>ATR:</b> {atr_val:.6f}\n"
                f"🛡️ <b>SL الحالي:</b> {sl_price if sl_price else 'لا يوجد'}"
                f"{orders_info}",
                "📥"
            )

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
                        "🛡️ إضافة SL تلقائي",
                        f"💠 {symbol}\n🛑 <b>الوقف:</b> {auto_sl}",
                        "🛡️"
                    )
                except Exception as e:
                    log.error(f"Auto SL error {symbol}: {e}")

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

            prices = get_all_prices()

            for symbol in symbols:
                with lock:
                    info = open_positions.get(symbol)
                if not info:
                    continue

                current_price = prices.get(symbol)
                if not current_price:
                    continue

                update_trailing_stop(symbol, info, current_price)

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
                            for oid_key in ["sl_order_id", "tp1_order_id", "tp2_order_id"]:
                                oid = closed_info.get(oid_key)
                                if oid:
                                    try:
                                        safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=oid)
                                    except Exception:
                                        pass
                            duration = datetime.now(timezone.utc) - closed_info["opened_at"]
                            tg_log(
                                "✅ تم إغلاق الصفقة",
                                f"💠 <b>العملة:</b> {symbol}\n"
                                f"📊 <b>الاتجاه:</b> {closed_info['side']}\n"
                                f"💵 <b>الدخول:</b> {closed_info['entry']}\n"
                                f"🛑 <b>آخر وقف:</b> {closed_info['current_sl']}\n"
                                f"📈 <b>المرحلة:</b> {closed_info['trailing_stage']}\n"
                                f"📦 <b>المصدر:</b> {closed_info.get('source', 'BOT')}\n"
                                f"⏱️ <b>المدة:</b> {duration}",
                                "✅"
                            )
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
    log.info(f"⏳ انتظار إغلاق الشمعة: {wait:.0f}s")
    time.sleep(max(wait, 5))

def scan_once():
    if not in_trading_session():
        log.info("⏸️ خارج جلسة التداول")
        return

    active_count = get_active_count()
    if active_count >= MAX_CONCURRENT_TRADES:
        log.info(f"⛔ الحد الأقصى ({active_count}/{MAX_CONCURRENT_TRADES})")
        return

    log.info(f"🔍 مسح: {len(SYMBOLS)} عملات | نشطة: {active_count}/{MAX_CONCURRENT_TRADES}")

    for symbol in SYMBOLS:
        try:
            if get_active_count() >= MAX_CONCURRENT_TRADES:
                break
            if has_open_position(symbol):
                continue

            df = get_klines(symbol, TIMEFRAME, limit=300)
            df = add_indicators(df)
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
            tg_log("⚠️ خطأ في المسح", f"{symbol}: {e}", "⚠️")

# ==================== MAIN ====================
def main_loop():
    bal = get_balance()
    required_margin = POSITION_SIZE_USDT / LEVERAGE

    tg_log(
        "🤖 بدء تشغيل البوت",
        f"📊 <b>الاستراتيجية:</b> Early Momentum Catch v2.2\n"
        f"⏱️ <b>الفريم:</b> {TIMEFRAME}\n"
        f"💼 <b>حجم الصفقة:</b> {POSITION_SIZE_USDT} USDT\n"
        f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n"
        f"🛡️ <b>الهامش لكل صفقة:</b> {required_margin:.2f} USDT\n"
        f"📋 <b>العملات:</b> {', '.join(SYMBOLS)}\n"
        f"🔢 <b>حد الصفقات:</b> {MAX_CONCURRENT_TRADES}\n"
        f"🎯 <b>الحد الأدنى للنقاط:</b> {MIN_SCORE}/10\n"
        f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n"
        f"🌐 <b>الوضع:</b> {'TESTNET' if TESTNET else 'LIVE'}\n\n"
        f"{get_balance_display()}",
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
    # فحص أولي للاتصال
    log.info(f"🔍 TESTNET={TESTNET} | SYMBOLS={SYMBOLS}")
    log.info(f"🔑 API Key: {'✅' if BINANCE_API_KEY else '❌'}")
    log.info(f"🔑 Secret:  {'✅' if BINANCE_SECRET_KEY else '❌'}")
    log.info(f"📱 TG Token: {'✅' if TELEGRAM_BOT_TOKEN else '❌'}")
    log.info(f"📱 TG Chat:  {'✅' if TELEGRAM_CHAT_ID else '❌'}")

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=monitor_positions, daemon=True).start()
    time.sleep(2)
    main_loop()
