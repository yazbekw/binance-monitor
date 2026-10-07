"""
Momentum Bot v2.1 — CoinEx Edition
==================================
Strategy : Early Momentum Catch
Indicators: EMA (7/25/50/200) + MACD + RSI + ATR + VWAP + Volume MA
Exchange : CoinEx Futures (USDT-M Perpetual) — API v2
"""
import os
import json
import hmac
import time
import hashlib
import logging
import threading
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

import pandas as pd
import numpy as np
import requests
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
COINEX_API_KEY     = os.getenv("COINEX_API_KEY")
COINEX_SECRET_KEY  = os.getenv("COINEX_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

SYMBOLS            = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", 200))
LEVERAGE           = int(os.getenv("LEVERAGE", 20))
TIMEFRAME          = os.getenv("TIMEFRAME", "15m")            # 1m/3m/5m/15m/30m/1h/2h/4h/6h/12h/1d
TESTNET            = os.getenv("TESTNET", "False").lower() == "true"  # CoinEx لا يوفر testnet عام للعقود
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", 3))
TRAILING_ENABLED   = os.getenv("TRAILING_ENABLED", "True").lower() == "true"
MIN_SCORE          = int(os.getenv("MIN_SCORE", 6))
MONITOR_INTERVAL   = int(os.getenv("MONITOR_INTERVAL", 30))    # ثواني
SCAN_INTERVAL_MIN  = int(os.getenv("SCAN_INTERVAL_MIN", 15))   # دقائق
AUTO_SL_MANUAL     = os.getenv("AUTO_SL_MANUAL", "True").lower() == "true"

COINEX_BASE_URL    = "https://api.coinex.com"

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("MomentumBot")

# ==================== STATE ====================
open_positions = {}   # symbol -> info dict
lock = threading.Lock()

# ==================== TIMEFRAME MAP ====================
_TF_MAP = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1hour", "2h": "2hour", "4h": "4hour", "6h": "6hour", "12h": "12hour",
    "1d": "1day", "3d": "3day", "1w": "1week",
}
def to_coinex_period(tf: str) -> str:
    return _TF_MAP.get(tf, "15min")

# ==================== COINEX EXCEPTIONS ====================
class CoinExAPIException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")

class CoinExRequestException(Exception):
    pass

# ==================== COINEX CLIENT ====================
class CoinExClient:
    """
    عميل CoinEx V2 للعقود.
    - التوقيع: HMAC-SHA256 على نص = METHOD + URI (مع query) + TIMESTAMP + BODY
    - الهيدرات: X-COINEX-KEY / X-COINEX-SIGN / X-COINEX-TIMESTAMP
    - BODY يُرسَل كما هو (بدون مسافات زائدة) للطلبات الموقّعة.
    """
    TIMEOUT = 12

    def __init__(self, api_key: str, secret: str, testnet: bool = False):
        self.api_key = api_key or ""
        self.secret = secret or ""
        self.base = COINEX_BASE_URL
        self.session = requests.Session()
        # تخزين نوع الأوامر (stop/regular) لأجل الإلغاء الصحيح
        self._order_kind = {}

    # ---- signing ----
    def _sign(self, method: str, uri_with_query: str, body_str: str, ts: str) -> str:
        msg = f"{method}{uri_with_query}{ts}{body_str}"
        return hmac.new(
            self.secret.encode("utf-8"),
            msg.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    # ---- low level request ----
    def _request(self, method: str, path: str, params=None, body=None, auth=False):
        params = params or {}
        url = self.base + path

        # ترتيب الـ query أبجديًا (متطلب CoinEx)
        query_str = ""
        if params:
            sorted_items = sorted((k, v) for k, v in params.items() if v is not None)
            query_str = urlencode(sorted_items)
            uri_with_query = f"{path}?{query_str}"
        else:
            uri_with_query = path

        headers = {"Accept": "application/json"}
        body_str = ""
        if body is not None:
            body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
            headers["Content-Type"] = "application/json"

        if auth:
            if not self.api_key or not self.secret:
                raise CoinExAPIException("AUTH", "Missing COINEX_API_KEY/SECRET")
            ts = str(int(time.time() * 1000))
            sign = self._sign(method, uri_with_query, body_str, ts)
            headers["X-COINEX-KEY"] = self.api_key
            headers["X-COINEX-SIGN"] = sign
            headers["X-COINEX-TIMESTAMP"] = ts

        try:
            if method == "GET":
                r = self.session.get(url, params=params, headers=headers, timeout=self.TIMEOUT)
            elif method == "POST":
                r = self.session.post(url, params=params, data=body_str, headers=headers, timeout=self.TIMEOUT)
            elif method == "DELETE":
                r = self.session.delete(url, params=params, data=body_str, headers=headers, timeout=self.TIMEOUT)
            else:
                raise ValueError(f"Unsupported method {method}")
        except requests.RequestException as e:
            raise CoinExRequestException(str(e))

        try:
            payload = r.json()
        except Exception:
            raise CoinExRequestException(f"Non-JSON response ({r.status_code}): {r.text[:200]}")

        code = payload.get("code", -1)
        if code != 0:
            raise CoinExAPIException(code, payload.get("message", "Unknown error"))
        return payload.get("data")

    # ---- public market data ----
    def futures_klines(self, symbol: str, interval: str, limit: int = 300):
        """
        يرجع list of lists: [ts_ms, open, close, high, low, volume, value]
        """
        return self._request("GET", "/v2/futures/kline", params={
            "market": symbol, "period": interval, "limit": limit,
        })

    def futures_ticker_price(self):
        """يرجع قائمة كل الأسواق مع آخر سعر."""
        return self._request("GET", "/v2/futures/ticker")

    def futures_exchange_info(self):
        return self._request("GET", "/v2/futures/market")

    # ---- account ----
    def futures_account_balance(self):
        return self._request("GET", "/v2/assets/futures/balance", auth=True)

    def futures_position_information(self, symbol: str = None):
        """إن مُرّر symbol -> موضع واحد، وإلا كل المواضع."""
        if symbol:
            return self._request("GET", "/v2/futures/position",
                                 params={"market": symbol}, auth=True)
        return self._request("GET", "/v2/futures/positions", auth=True)

    # ---- trading settings ----
    def futures_change_leverage(self, symbol: str, leverage: int):
        return self._request("POST", "/v2/futures/leverage", body={
            "market": symbol, "leverage": leverage,
        }, auth=True)

    def futures_change_margin_type(self, symbol: str, marginType: str = "isolated"):
        """
        CoinEx يستخدم endpoint position-adjust لتعديل نوع الهامش.
        نتجاهل الأخطاء إذا كان النوع مُعد مسبقًا.
        """
        return self._request("POST", "/v2/futures/position-adjust", body={
            "market": symbol, "margin_mode": marginType.lower(),
        }, auth=True)

    # ---- orders ----
    def futures_create_order(self, symbol: str, side: str, type_: str,
                             quantity: float, price: float = None):
        """
        side: "buy" أو "sell"
        type_: "market" أو "limit"
        """
        body = {
            "market": symbol,
            "market_type": "futures",
            "side": side.lower(),
            "type": type_.lower(),
            "amount": _fmt_num(quantity),
        }
        if price is not None and type_.lower() == "limit":
            body["price"] = _fmt_num(price)

        data = self._request("POST", "/v2/futures/order", body=body, auth=True)
        oid = data.get("order_id")
        if oid is not None:
            self._order_kind[oid] = "order"
        return data

    def futures_create_stop_order(self, symbol: str, side: str,
                                  quantity: float, stop_price: float,
                                  trigger_type: str = "mark_price",
                                  order_type: str = "market",
                                  reduce_only: bool = True):
        """
        أمر إيقاف (SL/TP) — ينفّذ market عند لمس trigger_price.
        """
        body = {
            "market": symbol,
            "market_type": "futures",
            "side": side.lower(),
            "type": order_type,
            "amount": _fmt_num(quantity),
            "trigger_price": _fmt_num(stop_price),
            "trigger_price_type": trigger_type,
            "reduce_only": bool(reduce_only),
        }
        data = self._request("POST", "/v2/futures/stop-order", body=body, auth=True)
        oid = data.get("order_id")
        if oid is not None:
            self._order_kind[oid] = "stop"
        return data

    def futures_get_open_orders(self, symbol: str):
        """يرجع أوامر عادية + أوامر إيقاف معلقة."""
        try:
            normal = self._request("GET", "/v2/futures/pending-order",
                                   params={"market": symbol, "market_type": "futures", "side": "all"},
                                   auth=True) or []
        except Exception:
            normal = []
        try:
            stops = self._request("GET", "/v2/futures/pending-stop-order",
                                  params={"market": symbol, "market_type": "futures", "side": "all"},
                                  auth=True) or []
        except Exception:
            stops = []

        combined = []
        for o in normal:
            o["_kind"] = "order"; combined.append(o)
        for o in stops:
            o["_kind"] = "stop"; combined.append(o)
        return combined

    def futures_cancel_order(self, symbol: str, orderId):
        """يجرّب إلغاء الأمر كـ stop أولاً ثم كـ order حسب النوع المخزّن."""
        kind = self._order_kind.get(orderId, None)
        attempts = [kind] if kind else ["stop", "order"]
        last_err = None
        for k in attempts:
            if k is None:
                continue
            endpoint = "/v2/futures/stop-order" if k == "stop" else "/v2/futures/order"
            try:
                return self._request("DELETE", endpoint, body={
                    "market": symbol,
                    "market_type": "futures",
                    "order_id": orderId,
                }, auth=True)
            except CoinExAPIException as e:
                last_err = e
                continue
        if last_err:
            raise last_err
        return None


def _fmt_num(x) -> str:
    """صياغة عدد بدون أصفار زائدة."""
    if x is None:
        return "0"
    s = f"{float(x):.10f}".rstrip("0").rstrip(".")
    return s if s else "0"


# ==================== INIT CLIENT ====================
client = CoinExClient(COINEX_API_KEY, COINEX_SECRET_KEY, testnet=TESTNET)

# ==================== SAFE API WRAPPER ====================
def safe_api_call(func, *args, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except CoinExAPIException as e:
            # بعض رموز Rate limit الشائعة في CoinEx
            if e.code in (429, 4001, 4002, 5000, 3008):
                wait = 2 ** attempt
                log.warning(f"⚠️ Rate limit CoinEx [{e.code}] - انتظار {wait}s")
                time.sleep(wait)
            else:
                raise
        except CoinExRequestException as e:
            log.warning(f"🌐 Network error: {e}")
            time.sleep(2)
    raise Exception(f"فشل بعد {retries} محاولات")

# ==================== EXCHANGE INFO CACHE ====================
_exchange_info_cache = None
_exchange_info_lock = threading.Lock()

def get_exchange_info_cached():
    global _exchange_info_cache
    with _exchange_info_lock:
        if _exchange_info_cache is None:
            _exchange_info_cache = safe_api_call(client.futures_exchange_info)
            # نبني dict فوري
            mapping = {}
            for m in _exchange_info_cache or []:
                mapping[m["market"]] = m
            _exchange_info_cache = mapping
            log.info(f"✅ تم تحميل exchange_info ({len(mapping)} سوق) إلى الكاش")
        return _exchange_info_cache

# ==================== FLASK HEALTH ====================
app = Flask(__name__)

@app.route("/")
def health():
    with lock:
        active = list(open_positions.keys())
    return {
        "status": "alive",
        "exchange": "coinex",
        "time": datetime.now(timezone.utc).isoformat(),
        "symbols": SYMBOLS,
        "mode": "LIVE",       # CoinEx لا يوفر testnet رسمي للعقود
        "active_positions": active,
        "max_concurrent": MAX_CONCURRENT_TRADES,
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
            r = requests.post(
                f"{self.base}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            if r.status_code != 200:
                log.error(f"Telegram error: {r.text}")
        except Exception as e:
            log.error(f"Telegram exception: {e}")

tg = Telegram()

def tg_log(title: str, body: str, emoji: str = "ℹ️"):
    msg = f"{emoji} <b>{title}</b>\n\n{body}"
    tg.send(msg)
    log.info(f"{title} | {body.replace(chr(10), ' | ')}")

# ==================== DATA FETCH ====================
def get_klines(symbol: str, interval: str, limit: int = 300) -> pd.DataFrame:
    period = to_coinex_period(interval)
    raw = safe_api_call(client.futures_klines, symbol=symbol, interval=period, limit=limit)
    # CoinEx: [ts, open, close, high, low, volume, value]
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "close", "high", "low", "volume", "value"
    ])
    for c in ["open", "high", "low", "close", "volume", "value"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms")
    # نرتب الأعمدة للتناسق
    df = df[["open_time", "open", "high", "low", "close", "volume", "value"]]
    return df


def get_balance_usdt() -> float:
    try:
        balances = safe_api_call(client.futures_account_balance)
        for b in balances or []:
            if b.get("ccy") == "USDT":
                # CoinEx: total = available + frozen
                return float(b.get("available", b.get("total", 0)) or 0)
    except Exception as e:
        log.error(f"Balance error: {e}")
    return 0.0


def get_all_prices() -> dict:
    """طلب واحد لجميع الأسعار."""
    try:
        tickers = safe_api_call(client.futures_ticker_price)
        out = {}
        for t in tickers or []:
            m = t.get("market")
            last = t.get("last") or t.get("mark_price") or t.get("index_price")
            if m and last:
                out[m] = float(last)
        return out
    except Exception as e:
        log.error(f"Batch ticker error: {e}")
        return {}


def set_leverage_and_margin(symbol: str):
    try:
        safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=LEVERAGE)
    except CoinExAPIException as e:
        log.warning(f"Leverage {symbol}: {e.message}")
    try:
        safe_api_call(client.futures_change_margin_type, symbol=symbol, marginType="isolated")
    except CoinExAPIException as e:
        # نتجاهل إن كان النوع مفروضًا أو لا يمكن تعديله
        log.warning(f"Margin type {symbol}: {e.message}")

# ==================== INDICATORS ====================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def macd(series: pd.Series, fast=12, slow=26, signal=9):
    dif = ema(series, fast) - ema(series, slow)
    dea = ema(dif, signal)
    hist = dif - dea
    return dif, dea, hist

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    return (typical * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, np.nan)

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
    # London 07:00-16:00 UTC + NY 12:00-21:00 UTC → 07:00-21:00 UTC
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
            score += 2; reasons.append("✅ فوق EMA200 +2")
        if price > last["ema50"]:
            score += 1; reasons.append("✅ فوق EMA50 +1")
        if prev["dif"] <= prev["dea"] and last["dif"] > last["dea"]:
            score += 2; reasons.append("✅ تقاطع MACD إيجابي +2")
        if prev2["hist"] < prev["hist"] < 0 and last["hist"] > prev["hist"]:
            score += 1; reasons.append("✅ تحول الهيستوجرام +1")
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
            score += 2; reasons.append("✅ تحت EMA200 +2")
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
        "rsi": last["rsi"],
    }

# ==================== POSITION HELPERS ====================
def _extract_position_amount(p: dict) -> float:
    """يرجع كمية الموضع بإشارة (موجب=LONG، سالب=SHORT)."""
    if not p:
        return 0.0
    # CoinEx يوفّر amount كقيمة موجبة + side.
    amt_raw = p.get("amount") or p.get("position_amount") or 0
    try:
        amt = float(amt_raw)
    except Exception:
        return 0.0
    side = (p.get("side") or "").lower()
    if side == "short" and amt > 0:
        return -amt
    return amt


def has_open_position(symbol: str) -> bool:
    with lock:
        if symbol in open_positions:
            return True
    try:
        positions = safe_api_call(client.futures_position_information)
        for p in positions or []:
            if p.get("market") == symbol and _extract_position_amount(p) != 0:
                return True
    except Exception as e:
        log.error(f"Position check error: {e}")
    return False


def get_active_count() -> int:
    with lock:
        return len(open_positions)

# ==================== PRECISION ====================
def round_step(symbol: str, qty: float) -> float:
    info = get_exchange_info_cached().get(symbol)
    if info:
        # CoinEx: amount_precision أو base_amount_precision (يعتمد على الحقل الفعلي)
        for key in ("amount_precision", "base_amount_precision"):
            if key in info:
                try:
                    prec = int(info[key])
                    return float(f"{qty:.{prec}f}")
                except Exception:
                    pass
        # بديل: استخدم min_amount
        if "min_amount" in info:
            try:
                step = float(info["min_amount"])
                return float(int(qty / step) * step)
            except Exception:
                pass
    return round(qty, 6)


def round_price(symbol: str, price: float) -> float:
    info = get_exchange_info_cached().get(symbol)
    if info:
        for key in ("price_precision", "quote_precision"):
            if key in info:
                try:
                    prec = int(info[key])
                    return float(f"{price:.{prec}f}")
                except Exception:
                    pass
    return round(price, 4)

# ==================== OPEN TRADE ====================
def open_trade(symbol: str, direction: str, signal: dict):
    try:
        set_leverage_and_margin(symbol)

        price = signal["entry"]
        qty_usdt = POSITION_SIZE_USDT * LEVERAGE
        qty = round_step(symbol, qty_usdt / price)
        if qty <= 0:
            tg_log("❌ كمية غير صالحة", f"{symbol}: qty={qty}", "❌")
            return

        side = "buy" if direction == "LONG" else "sell"
        opposite = "sell" if direction == "LONG" else "buy"

        # 1) دخول ماركت
        order = safe_api_call(
            client.futures_create_order,
            symbol=symbol, side=side, type_="market", quantity=qty,
        )
        # CoinEx قد يعيد avg_price مباشرة، وإلا نستخدم سعر الإشارة
        fill_price = float(order.get("avg_price") or order.get("price") or price)

        sl_price  = round_price(symbol, signal["sl"])
        tp1_price = round_price(symbol, signal["tp1"])
        tp2_price = round_price(symbol, signal["tp2"])

        # 2) Stop Loss (reduce_only, trigger=mark_price)
        sl_order = safe_api_call(
            client.futures_create_stop_order,
            symbol=symbol, side=opposite, quantity=qty,
            stop_price=sl_price, trigger_type="mark_price",
            order_type="market", reduce_only=True,
        )

        # 3) TP1 (50%) و TP2 (50%)
        half_qty = round_step(symbol, qty / 2)
        rem_qty  = round_step(symbol, qty - half_qty)
        tp1_order = None
        tp2_order = None
        if half_qty > 0:
            tp1_order = safe_api_call(
                client.futures_create_stop_order,
                symbol=symbol, side=opposite, quantity=half_qty,
                stop_price=tp1_price, trigger_type="mark_price",
                order_type="market", reduce_only=True,
            )
        if rem_qty > 0:
            tp2_order = safe_api_call(
                client.futures_create_stop_order,
                symbol=symbol, side=opposite, quantity=rem_qty,
                stop_price=tp2_price, trigger_type="mark_price",
                order_type="market", reduce_only=True,
            )

        with lock:
            open_positions[symbol] = {
                "side": direction,
                "entry": fill_price,
                "qty": qty,
                "atr": signal["atr"],
                "initial_sl": sl_price,
                "current_sl": sl_price,
                "sl_order_id": sl_order.get("order_id") if sl_order else None,
                "tp1_order_id": tp1_order.get("order_id") if tp1_order else None,
                "tp2_order_id": tp2_order.get("order_id") if tp2_order else None,
                "tp1_price": tp1_price,
                "tp2_price": tp2_price,
                "tp1_hit": False,
                "trailing_stage": 0,
                "opened_at": datetime.now(timezone.utc),
                "source": "BOT",
            }

        body = (
            f"💠 <b>العملة:</b> {symbol}\n"
            f"📈 <b>الاتجاه:</b> {direction}\n"
            f"💵 <b>سعر الدخول:</b> {fill_price}\n"
            f"📦 <b>الكمية:</b> {qty}\n"
            f"💼 <b>الحجم الفعلي:</b> {round(qty * fill_price, 2)} USDT\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n\n"
            f"🛑 <b>وقف الخسارة:</b> {sl_price} (1.5×ATR)\n"
            f"🎯 <b>هدف 1:</b> {tp1_price} (EMA50) — 50%\n"
            f"🎯 <b>هدف 2:</b> {tp2_price} (EMA200) — 50%\n"
            f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n\n"
            f"📊 <b>النقاط:</b> {signal['score']}/10\n"
            f"📝 <b>الأسباب:</b>\n" + "\n".join(signal["reasons"])
        )
        tg_log("🚀 تم فتح صفقة", body, "🚀")

    except CoinExAPIException as e:
        tg_log("❌ فشل فتح صفقة", f"العملة: {symbol}\nالخطأ: [{e.code}] {e.message}", "❌")
    except Exception as e:
        tg_log("❌ خطأ غير متوقع", f"العملة: {symbol}\n{e}", "❌")

# ==================== TRAILING STOP ====================
def update_trailing_stop(symbol: str, info: dict, current_price: float):
    if not TRAILING_ENABLED:
        return

    side = info["side"]
    entry = info["entry"]
    atr_val = info["atr"]
    stage = info["trailing_stage"]
    current_sl = info["current_sl"]
    opposite = "sell" if side == "LONG" else "buy"

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
            if current_sl is None or trailing_sl > current_sl:
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
            if current_sl is None or trailing_sl < current_sl:
                new_sl = trailing_sl; new_stage = stage + 1

    if new_sl is None or new_sl == current_sl:
        return

    try:
        # إلغاء SL القديم
        if info.get("sl_order_id"):
            try:
                safe_api_call(client.futures_cancel_order,
                              symbol=symbol, orderId=info["sl_order_id"])
            except Exception as e:
                log.warning(f"Cancel SL warning {symbol}: {e}")

        # كمية الموضع الحالية (بعد TP1 قد تكون تغيّرت)
        try:
            pos = safe_api_call(client.futures_position_information, symbol=symbol)
            if isinstance(pos, list):
                pos = pos[0] if pos else None
            pos_qty = abs(_extract_position_amount(pos)) if pos else info["qty"]
        except Exception:
            pos_qty = info["qty"]

        if pos_qty <= 0:
            return

        new_order = safe_api_call(
            client.futures_create_stop_order,
            symbol=symbol, side=opposite, quantity=pos_qty,
            stop_price=new_sl, trigger_type="mark_price",
            order_type="market", reduce_only=True,
        )

        with lock:
            info["sl_order_id"] = new_order.get("order_id") if new_order else None
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
            "🔄",
        )
    except Exception as e:
        log.error(f"Trailing update error {symbol}: {e}")

# ==================== MANUAL POSITION IMPORT ====================
def import_manual_positions():
    imported = []
    try:
        positions = safe_api_call(client.futures_position_information) or []
        if isinstance(positions, dict):
            positions = [positions]

        for p in positions:
            symbol = p.get("market")
            amt = _extract_position_amount(p)
            if not symbol or amt == 0:
                continue
            if symbol not in SYMBOLS:
                log.info(f"⏭️ {symbol} ليس في SYMBOLS - تخطي")
                continue
            with lock:
                if symbol in open_positions:
                    continue

            side = "LONG" if amt > 0 else "SHORT"
            qty = abs(amt)
            entry = float(p.get("avg_entry_price") or p.get("entry_price") or 0)
            leverage = int(float(p.get("leverage", LEVERAGE)))
            margin_type = p.get("margin_mode", "isolated")
            unrealized_pnl = float(p.get("unrealized_pnl", 0) or 0)
            mark_price = float(p.get("mark_price", entry) or entry)
            liquidation = float(p.get("liq_price", p.get("liquidation_price", 0)) or 0)

            # أوامر معلقة
            sl_order_id = None; tp1_order_id = None; tp2_order_id = None
            sl_price = None; tp1_price = None; tp2_price = None
            pending_orders = []
            try:
                orders = safe_api_call(client.futures_get_open_orders, symbol=symbol) or []
                for o in orders:
                    o_type = (o.get("type") or "").lower()
                    o_stop = float(o.get("trigger_price") or o.get("stop_price") or 0) or None
                    pending_orders.append({
                        "id": o.get("order_id"),
                        "type": o_type,
                        "side": o.get("side"),
                        "stopPrice": o_stop,
                        "kind": o.get("_kind"),
                    })
                    # CoinEx: أوامر الإيقاف من endpoint stop-order
                    if o.get("_kind") == "stop":
                        if sl_order_id is None:
                            sl_order_id = o.get("order_id"); sl_price = o_stop
                        elif tp1_order_id is None:
                            tp1_order_id = o.get("order_id"); tp1_price = o_stop
                        else:
                            tp2_order_id = o.get("order_id"); tp2_price = o_stop
            except Exception as e:
                log.warning(f"تعذر جلب أوامر {symbol}: {e}")

            # ATR
            try:
                df = get_klines(symbol, TIMEFRAME, limit=100)
                df = add_indicators(df)
                atr_val = float(df.iloc[-1]["atr"])
            except Exception:
                atr_val = entry * 0.01 if entry else 0.0

            with lock:
                open_positions[symbol] = {
                    "side": side, "entry": entry, "qty": qty, "atr": atr_val,
                    "initial_sl": sl_price if sl_price else (
                        entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                    ),
                    "current_sl": sl_price,
                    "sl_order_id": sl_order_id,
                    "tp1_order_id": tp1_order_id,
                    "tp2_order_id": tp2_order_id,
                    "tp1_price": tp1_price, "tp2_price": tp2_price,
                    "tp1_hit": False, "trailing_stage": 0,
                    "opened_at": datetime.now(timezone.utc),
                    "source": "MANUAL",
                    "leverage": leverage, "margin_type": margin_type,
                }
            imported.append(symbol)

            # إشعار
            orders_info = ""
            if pending_orders:
                orders_info = "\n\n📋 <b>الأوامر المعلقة:</b>\n"
                for o in pending_orders:
                    label = "🛑 SL/TP" if o["kind"] == "stop" else "📌"
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
                "📥",
            )

            # SL تلقائي
            if sl_price is None and AUTO_SL_MANUAL:
                try:
                    opposite = "sell" if side == "LONG" else "buy"
                    auto_sl = entry - 1.5 * atr_val if side == "LONG" else entry + 1.5 * atr_val
                    auto_sl = round_price(symbol, auto_sl)
                    sl_order = safe_api_call(
                        client.futures_create_stop_order,
                        symbol=symbol, side=opposite, quantity=qty,
                        stop_price=auto_sl, trigger_type="mark_price",
                        order_type="market", reduce_only=True,
                    )
                    with lock:
                        open_positions[symbol]["sl_order_id"] = sl_order.get("order_id") if sl_order else None
                        open_positions[symbol]["current_sl"] = auto_sl
                    tg_log(
                        "🛡️ إضافة وقف خسارة تلقائي",
                        f"💠 <b>العملة:</b> {symbol}\n"
                        f"🛑 <b>الوقف الجديد:</b> {auto_sl}\n"
                        f"📝 <b>السبب:</b> لا يوجد SL على الصفقة اليدوية",
                        "🛡️",
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

                # هل الموضع أُغلق؟
                try:
                    pos = safe_api_call(client.futures_position_information, symbol=symbol)
                except Exception:
                    continue
                if isinstance(pos, list):
                    pos = pos[0] if pos else None

                amt = _extract_position_amount(pos) if pos else 0
                if amt == 0:
                    with lock:
                        closed_info = open_positions.pop(symbol, None)
                    if closed_info:
                        # إلغاء أي أوامر متبقية
                        for oid_key in ["sl_order_id", "tp1_order_id", "tp2_order_id"]:
                            oid = closed_info.get(oid_key)
                            if oid:
                                try:
                                    safe_api_call(client.futures_cancel_order,
                                                  symbol=symbol, orderId=oid)
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
    # نحاذي إغلاق الشمعة بدقة: نستخدم مضاعفات الفريم
    minutes_since_epoch = int(now.timestamp() // 60)
    next_boundary = ((minutes_since_epoch // interval_minutes) + 1) * interval_minutes
    next_close = datetime.fromtimestamp(next_boundary * 60, tz=timezone.utc)
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
            df_closed = df.iloc[:-1].copy()   # آخر شمعة مُغلقة

            long_sig = evaluate_signal(df_closed, "LONG")
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
        "🤖 بدء تشغيل البوت (CoinEx)",
        f"📊 <b>الاستراتيجية:</b> Early Momentum Catch v2.1\n"
        f"🏦 <b>المنصة:</b> CoinEx Futures (V2)\n"
        f"⏱️ <b>الفريم:</b> {TIMEFRAME}\n"
        f"💼 <b>حجم الصفقة:</b> {POSITION_SIZE_USDT} USDT\n"
        f"⚙️ <b>الرافعة:</b> {LEVERAGE}x\n"
        f"📋 <b>العملات:</b> {', '.join(SYMBOLS)}\n"
        f"🔢 <b>حد الصفقات المتزامنة:</b> {MAX_CONCURRENT_TRADES}\n"
        f"🎯 <b>الحد الأدنى للنقاط:</b> {MIN_SCORE}/10\n"
        f"🔄 <b>الوقف المتحرك:</b> {'مُفعّل' if TRAILING_ENABLED else 'معطّل'}\n"
        f"🛡️ <b>SL تلقائي للصفقات اليدوية:</b> {'مُفعّل' if AUTO_SL_MANUAL else 'معطّل'}\n"
        f"💰 <b>الرصيد:</b> {get_balance_usdt():.2f} USDT",
        "🤖",
    )

    try:
        import_manual_positions()
    except Exception as e:
        log.error(f"Initial import failed: {e}")

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
    time.sleep(2)
    main_loop()
