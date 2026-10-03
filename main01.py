"""
Binance Monitor Bot — تقاطع EMA + مراقبة TP/SL ذاتية
الحل: إذا فشل وضع TP/SL على Binance → يراقب السعر ويغلق بنفسه
"""
import os
import sys
import time
import re
import asyncio
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import NetworkError, TimedOut
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# الإعدادات الأساسية
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID")
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

REQUIRED = {
    "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
    "TELEGRAM_CHAT_ID": CHAT_ID_RAW,
    "BINANCE_API_KEY": API_KEY,
    "BINANCE_API_SECRET": API_SECRET,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    print(f"❌ متغيرات ناقصة: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

CHAT_ID = int(CHAT_ID_RAW)
HOURLY_MIN = 60
WS_RECONNECT_DELAY = 10
MARK_WS_RECONNECT_DELAY = 5

# ============================================================
# الاستراتيجية
# ============================================================
STRATEGY_ENABLED = os.getenv("STRATEGY_ENABLED", "false").lower() == "true"

_default_symbols = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT"
STRATEGY_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("STRATEGY_SYMBOLS", _default_symbols).split(",")
    if s.strip()
]

STRATEGY_INTERVAL     = os.getenv("STRATEGY_INTERVAL", "5m")
EMA_FAST              = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW              = int(os.getenv("EMA_SLOW", "21"))

TREND_FILTER_ENABLED  = os.getenv("TREND_FILTER_ENABLED", "true").lower() == "true"
TREND_TIMEFRAME       = os.getenv("TREND_TIMEFRAME", "1h")
TREND_EMA_PERIOD      = int(os.getenv("TREND_EMA_PERIOD", "200"))

MARGIN_USDT           = float(os.getenv("MARGIN_USDT", "5"))
LEVERAGE              = int(os.getenv("LEVERAGE", "20"))
TP_USDT               = float(os.getenv("TP_USDT", "1"))
SL_USDT               = float(os.getenv("SL_USDT", "10"))

MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
MAX_DAILY_LOSS_USDT   = float(os.getenv("MAX_DAILY_LOSS_USDT", "10"))
STRATEGY_JOB_INTERVAL = int(os.getenv("STRATEGY_JOB_INTERVAL", "30"))
SL_CAP_RATIO          = float(os.getenv("SL_CAP_RATIO", "0.8"))

# ============================================================
# الحماية ومراقبة TP/SL
# ============================================================
# monitor: مراقبة ذاتية فقط (آمن، لا -4120)
# orders:  محاولة وضع أوامر على Binance
# both:    جرّب الأوامر + راقب كحماية إضافية
CLOSE_MODE = os.getenv("CLOSE_MODE", "monitor").lower()
if CLOSE_MODE not in ("monitor", "orders", "both"):
    CLOSE_MODE = "monitor"

AUTO_PROTECT_ENABLED = os.getenv("AUTO_PROTECT_ENABLED", "true").lower() == "true"
AUTO_PROTECT_INTERVAL = int(os.getenv("AUTO_PROTECT_INTERVAL", "60"))
AUTO_PROTECT_MIN_NOTIONAL = float(os.getenv("AUTO_PROTECT_MIN_NOTIONAL", "20"))

_notional_ref = MARGIN_USDT * LEVERAGE
_effective_sl_ref = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
_default_tp_pct = round(TP_USDT / _notional_ref * 100, 3)
_default_sl_pct = round(_effective_sl_ref / _notional_ref * 100, 3)

AUTO_PROTECT_TP_PCT = float(os.getenv("AUTO_PROTECT_TP_PCT", str(_default_tp_pct)))
AUTO_PROTECT_SL_PCT = float(os.getenv("AUTO_PROTECT_SL_PCT", str(_default_sl_pct)))

# حماية من الانزلاق: لا تقبل سعراً أسوأ من X% عن مستوى TP/SL
MAX_SLIPPAGE_PCT = float(os.getenv("MAX_SLIPPAGE_PCT", "0.5"))

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
for noisy in ("httpx", "telegram", "telegram.ext", "binance", "websockets"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ============================================================
# حالة عامة
# ============================================================
binance_client: AsyncClient | None = None
notifications_enabled = True
_app: Application | None = None
_user_stream_task: asyncio.Task | None = None
_mark_watcher_task: asyncio.Task | None = None

_http_cache: dict = {}
_banned_until_ms: int = 0

_last_ema_states: dict[str, str] = {}
_last_candle_times: dict[str, int] = {}
_symbol_filters_cache: dict = {}
_trend_cache: dict = {}

_last_strategy_candle: int = 0

# الصفقات المراقَبة: {symbol: {...}}
_watched_positions: dict = {}

_daily_pnl_cache = {"date": None, "pnl": 0.0, "last_fetch": 0.0}

# قفل لمنع الإغلاق المتزامن
_close_locks: dict = {}


# ============================================================
# Health Server
# ============================================================
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Binance Monitor Bot")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


def _run_health():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever()
    log.info(f"🩺 Health server على المنفذ {port}")


# ============================================================
# أدوات مساعدة
# ============================================================
def is_banned() -> bool:
    return _banned_until_ms > int(time.time() * 1000)


def ban_remaining_sec() -> int:
    if not is_banned():
        return 0
    return max(0, (_banned_until_ms - int(time.time() * 1000)) // 1000)


def _register_ban(exc: Exception):
    global _banned_until_ms
    m = re.search(r"banned until (\d+)", str(exc))
    if m:
        _banned_until_ms = int(m.group(1))
        log.warning(
            f"⛔ Binance IP banned until "
            f"{datetime.fromtimestamp(_banned_until_ms/1000, timezone.utc)}"
        )


async def cached_http(key: str, coro_factory, ttl: int = 30):
    if is_banned():
        raise RuntimeError(
            f"⛔ Binance حظر IP. المتبقي: ~{ban_remaining_sec()//60} دقيقة."
        )
    now = time.time()
    hit = _http_cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        val = await coro_factory()
        _http_cache[key] = (now, val)
        return val
    except BinanceAPIException as e:
        if e.code == -1003:
            _register_ban(e)
            raise RuntimeError(
                f"⛔ Binance حظر IP. المتبقي: ~{ban_remaining_sec()//60} دقيقة."
            )
        raise


async def send(text: str, context: ContextTypes.DEFAULT_TYPE | None = None):
    if not notifications_enabled:
        return
    try:
        if context is not None:
            await context.bot.send_message(
                chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML
            )
        elif _app is not None:
            await _app.bot.send_message(
                chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML
            )
    except Exception as e:
        log.error(f"send error: {e}")


def round_step(value: float, step: str) -> float:
    try:
        d_val = Decimal(str(value))
        d_step = Decimal(str(step))
        if d_step <= 0:
            return value
        return float((d_val / d_step).quantize(Decimal("1"), rounding=ROUND_DOWN) * d_step)
    except Exception:
        return value


def calc_ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def get_last_closed_candle_ts(interval: str) -> int:
    interval_ms = {
        "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
        "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
        "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000,
        "12h": 43_200_000, "1d": 86_400_000,
    }.get(interval)
    if not interval_ms:
        return 0
    now_ms = int(time.time() * 1000)
    current_open = (now_ms // interval_ms) * interval_ms
    return current_open - interval_ms


# ============================================================
# جلب البيانات
# ============================================================
async def fetch_positions(force: bool = False) -> list:
    if force:
        _http_cache.pop("positions", None)
    data = await cached_http(
        "positions",
        lambda: binance_client.futures_position_information(),
        ttl=20,
    )
    return [p for p in data if float(p["positionAmt"]) != 0]


async def fetch_account(force: bool = False) -> dict:
    if force:
        _http_cache.pop("account", None)
    return await cached_http(
        "account",
        lambda: binance_client.futures_account(),
        ttl=30,
    )


async def get_symbol_filters(symbol: str) -> tuple[str, str]:
    if symbol in _symbol_filters_cache:
        return _symbol_filters_cache[symbol]
    info = await binance_client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            step, tick = "0.001", "0.01"
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = f["stepSize"]
                elif f["filterType"] == "PRICE_FILTER":
                    tick = f["tickSize"]
            _symbol_filters_cache[symbol] = (step, tick)
            return step, tick
    raise ValueError(f"الرمز {symbol} غير موجود")


async def get_daily_realized_pnl() -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if _daily_pnl_cache["date"] == today and now - _daily_pnl_cache["last_fetch"] < 45:
        return _daily_pnl_cache["pnl"]

    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(start.timestamp() * 1000)

    try:
        income = await binance_client.futures_income_history(
            startTime=start_ms, limit=1000
        )
        total = 0.0
        for i in income:
            itype = i.get("incomeType", "")
            if itype in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
                try:
                    total += float(i["income"])
                except (KeyError, ValueError):
                    pass
        _daily_pnl_cache["date"] = today
        _daily_pnl_cache["pnl"] = total
        _daily_pnl_cache["last_fetch"] = now
        return total
    except Exception as e:
        log.warning(f"daily pnl: {e}")
        return _daily_pnl_cache.get("pnl", 0.0)


async def get_trend_direction(symbol: str) -> str | None:
    if not TREND_FILTER_ENABLED:
        return None

    now = time.time()
    cached = _trend_cache.get(symbol)
    if cached and now - cached[0] < 300:
        return cached[1]

    try:
        klines = await binance_client.futures_klines(
            symbol=symbol, interval=TREND_TIMEFRAME,
            limit=TREND_EMA_PERIOD + 50,
        )
    except Exception:
        return None

    if len(klines) < TREND_EMA_PERIOD + 5:
        return None

    closes = [float(k[4]) for k in klines[:-1]]
    ema = calc_ema(closes, TREND_EMA_PERIOD)
    if not ema:
        return None

    trend = "UP" if closes[-1] > ema[-1] else "DOWN"
    _trend_cache[symbol] = (now, trend)
    return trend


# ============================================================
# وضع TP/SL على Binance (لمن يريد orders mode)
# ============================================================
async def _try_order(
    symbol: str, side: str, order_type: str,
    stop_price: float, qty: float,
) -> tuple[bool, str]:
    """محاولة واحدة — لن نُكرر أبداً لتقليل الطلبات."""
    try:
        await binance_client.futures_create_order(
            symbol=symbol, side=side, type=order_type,
            stopPrice=stop_price, quantity=qty,
            reduceOnly=True, workingType="MARK_PRICE",
        )
        return True, "ok"
    except BinanceAPIException as e:
        return False, f"{e.code}"
    except Exception as e:
        return False, str(e)[:50]


async def try_place_tp_sl_on_binance(
    symbol: str, close_side: str, qty: float,
    tp_price: float, sl_price: float,
) -> tuple[bool, bool]:
    """
    محاولة واحدة فقط لكل أمر — لا حلقات.
    """
    tp_ok, tp_err = await _try_order(
        symbol, close_side, "TAKE_PROFIT_MARKET", tp_price, qty
    )
    sl_ok, sl_err = await _try_order(
        symbol, close_side, "STOP_MARKET", sl_price, qty
    )
    if not tp_ok:
        log.info(f"TP على Binance فشل لـ {symbol}: {tp_err}")
    if not sl_ok:
        log.info(f"SL على Binance فشل لـ {symbol}: {sl_err}")
    return tp_ok, sl_ok


# ============================================================
# المراقبة الذاتية — قلب الحل الجديد
# ============================================================
def _get_lock(symbol: str) -> asyncio.Lock:
    if symbol not in _close_locks:
        _close_locks[symbol] = asyncio.Lock()
    return _close_locks[symbol]


async def sync_positions_and_watch():
    """
    يزامن قائمة المراقبة مع المراكز الفعلية.
    يُضيف الجديد، يحذف المُغلق.
    """
    if binance_client is None or is_banned():
        return

    try:
        positions = await fetch_positions(force=True)
    except Exception as e:
        log.debug(f"sync positions: {e}")
        return

    current_symbols = {p["symbol"] for p in positions}

    # احذف المراكز المُغلقة
    for sym in list(_watched_positions.keys()):
        if sym not in current_symbols:
            log.info(f"👁️ توقف عن مراقبة {sym} (المركز أُغلق)")
            _watched_positions.pop(sym, None)
            _close_locks.pop(sym, None)

    # أضف/حدّث المراكز
    for p in positions:
        symbol = p["symbol"]
        try:
            amt = float(p["positionAmt"])
        except (KeyError, ValueError):
            continue
        if amt == 0:
            continue

        entry = float(p.get("entryPrice", 0) or 0)
        if entry <= 0:
            continue

        abs_amt = abs(amt)
        notional = abs_amt * entry
        if notional < AUTO_PROTECT_MIN_NOTIONAL:
            continue

        is_long = amt > 0

        # احسب المستويات
        if is_long:
            tp = entry * (1 + AUTO_PROTECT_TP_PCT / 100)
            sl = entry * (1 - AUTO_PROTECT_SL_PCT / 100)
        else:
            tp = entry * (1 - AUTO_PROTECT_TP_PCT / 100)
            sl = entry * (1 + AUTO_PROTECT_SL_PCT / 100)

        existing = _watched_positions.get(symbol)
        if existing:
            # لا تغيير في الحجم/الدخول
            same_amt = abs(existing["qty"] - abs_amt) / max(abs_amt, 1e-9) < 1e-4
            same_entry = abs(existing["entry"] - entry) / max(entry, 1e-9) < 1e-4
            if same_amt and same_entry:
                continue

        # مركز جديد أو تغيّر
        _watched_positions[symbol] = {
            "side": "LONG" if is_long else "SHORT",
            "entry": entry,
            "qty": abs_amt,
            "tp": tp,
            "sl": sl,
            "closed": False,
            "closing": False,
            "notified": False,
            "created": time.time(),
            "last_ws_update": 0.0,
        }
        log.info(f"👁️ جديد: {symbol} {('LONG' if is_long else 'SHORT')} "
                 f"| TP={tp:.4f} SL={sl:.4f}")

        # إشعار بالمركز الجديد
        side_emoji = "🟢 LONG" if is_long else "🔴 SHORT"
        try:
            await send(
                f"👁️ <b>مراقبة صفقة جديدة</b>\n\n"
                f"{side_emoji} | <b>{symbol}</b>\n"
                f"الدخول: {entry}\n"
                f"الكمية: {abs_amt}\n"
                f"Notional: ~{notional:.0f}$\n\n"
                f"🎯 TP: {tp:.4f} (+{AUTO_PROTECT_TP_PCT:.2f}%)\n"
                f"🛑 SL: {sl:.4f} (-{AUTO_PROTECT_SL_PCT:.2f}%)\n\n"
                f"<i>سأراقب السعر وأُغلق تلقائياً عند الوصول.</i>"
            )
        except Exception:
            pass


async def check_price_and_close(symbol: str, price: float, source: str = "ws"):
    """
    يفحص السعر الحالي مقابل TP/SL ويغلق عند الحاجة.
    """
    pos = _watched_positions.get(symbol)
    if not pos or pos.get("closed") or pos.get("closing"):
        return

    pos["last_ws_update"] = time.time()

    side = pos["side"]
    tp = pos["tp"]
    sl = pos["sl"]

    hit = None
    if side == "LONG":
        if price >= tp:
            hit = "TP"
        elif price <= sl:
            hit = "SL"
    else:
        if price <= tp:
            hit = "TP"
        elif price >= sl:
            hit = "SL"

    if hit:
        log.info(f"⚡ {symbol} وصل {hit} @ {price} (source={source})")
        await close_position_market(symbol, pos, hit, price)


async def close_position_market(symbol: str, pos: dict, reason: str, price: float):
    """
    يُغلق المركز بأمر MARKET.
    """
    lock = _get_lock(symbol)
    async with lock:
        # إعادة تحقق
        if pos.get("closed") or pos.get("closing"):
            return

        pos["closing"] = True

        try:
            side = "SELL" if pos["side"] == "LONG" else "BUY"

            # حاول إلغاء أي أوامر معلقة (قد تكون TP/SL يدوي)
            try:
                await binance_client.futures_cancel_all_open_orders(symbol=symbol)
            except Exception as e:
                log.debug(f"cancel orders {symbol}: {e}")

            # أمر MARKET
            order = await binance_client.futures_create_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=pos["qty"],
                reduceOnly=True,
            )

            pos["closed"] = True
            pos["close_time"] = time.time()

            emoji = "🎯" if reason == "TP" else "🛑"
            title = "جني أرباح" if reason == "TP" else "وقف خسارة"
            status = order.get("status", "?")
            avg = order.get("avgPrice", "0")

            try:
                await send(
                    f"{emoji} <b>{title} (مراقبة ذاتية)</b>\n\n"
                    f"<b>{symbol}</b> | {pos['side']}\n"
                    f"الكمية: {pos['qty']}\n"
                    f"سعر التنفيذ: {avg or price}\n"
                    f"الدخول: {pos['entry']}\n"
                    f"الحالة: {status}"
                )
            except Exception:
                pass

            log.info(f"✅ {reason} {symbol} — أُغلق @ {avg or price}")

            # انتظر ثم احذف
            await asyncio.sleep(10)
            _watched_positions.pop(symbol, None)
            _close_locks.pop(symbol, None)

        except Exception as e:
            log.exception(f"close {symbol}: {e}")
            pos["closing"] = False
            try:
                await send(
                    f"🚨 <b>فشل إغلاق {symbol}</b>\n\n"
                    f"السبب: {reason}\n"
                    f"السعر: {price}\n"
                    f"❌ {e}\n\n"
                    f"<b>أغلق يدوياً من التطبيق فوراً!</b>"
                )
            except Exception:
                pass


# ============================================================
# Mark Price WebSocket (المراقبة اللحظية)
# ============================================================
async def mark_price_watcher(app: Application):
    """
    يستمع لأسعار markPrice لكل مركز مُراقب.
    عند وصول TP/SL → يُغلق.
    """
    while True:
        try:
            symbols = list(_watched_positions.keys())
            if not symbols:
                await asyncio.sleep(5)
                continue

            # راقب فقط الرموز التي لم تُغلق
            active = [
                s for s in symbols
                if _watched_positions.get(s)
                and not _watched_positions[s].get("closed")
                and not _watched_positions[s].get("closing")
            ]
            if not active:
                await asyncio.sleep(5)
                continue

            streams = [f"{s.lower()}@markPrice@1s" for s in active]
            bsm = BinanceSocketManager(binance_client)

            log.info(f"📡 mark watcher: {len(active)} رموز")
            try:
                async with bsm.futures_multiplex_socket(streams) as stream:
                    while True:
                        msg = await stream.recv()
                        data = msg.get("data", msg)
                        event = data.get("e")
                        if event != "markPriceUpdate":
                            continue

                        symbol = data.get("s")
                        price_str = data.get("p")
                        if not symbol or not price_str:
                            continue

                        try:
                            price = float(price_str)
                        except (ValueError, TypeError):
                            continue

                        await check_price_and_close(symbol, price, "ws")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"mark ws inner: {e}")
                await asyncio.sleep(MARK_WS_RECONNECT_DELAY)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"mark watcher: {e}")
            await asyncio.sleep(MARK_WS_RECONNECT_DELAY)


# ============================================================
# Job الدوري: مزامنة + fallback سعري
# ============================================================
async def watch_sync_job(context: ContextTypes.DEFAULT_TYPE | None = None):
    """
    كل AUTO_PROTECT_INTERVAL ثانية:
    - يزامن قائمة المراقبة
    - فحص سعري احتياطي لكل رمز لم يتحدّث مؤخراً
    """
    if not AUTO_PROTECT_ENABLED or binance_client is None or is_banned():
        return

    try:
        await sync_positions_and_watch()
    except Exception as e:
        log.exception(f"sync: {e}")
        return

    # Fallback: لو WebSocket لم يُحدّث سعراً منذ 90 ثانية → افحص عبر REST
    now = time.time()
    for symbol, pos in list(_watched_positions.items()):
        if pos.get("closed") or pos.get("closing"):
            continue
        last = pos.get("last_ws_update", 0)
        if now - last < 90:
            continue

        try:
            ticker = await binance_client.futures_symbol_ticker(symbol=symbol)
            price = float(ticker["price"])
            await check_price_and_close(symbol, price, "rest")
        except Exception as e:
            log.debug(f"rest price {symbol}: {e}")


# ============================================================
# التنسيق
# ============================================================
def fmt_position(p: dict) -> str | None:
    try:
        amt = float(p["positionAmt"])
    except (KeyError, ValueError):
        return None
    if amt == 0:
        return None

    side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
    entry = float(p.get("entryPrice", 0) or 0)
    mark = float(p.get("markPrice", 0) or 0)
    pnl = float(p.get("unRealizedProfit", 0) or 0)
    lev = p.get("leverage", "?")
    icon = "📈" if pnl >= 0 else "📉"

    lines = [
        f"{side} | <b>{p.get('symbol', '?')}</b>",
        f"  الحجم: {abs(amt)}",
        f"  الدخول: {entry}",
    ]
    if mark:
        lines.append(f"  الحالي: {mark}")
    lines.append(f"  الرافعة: x{lev}")
    lines.append(f"  {icon} PnL: <b>{pnl:+.4f} USDT</b>")

    # أضف TP/SL من قائمة المراقبة
    watched = _watched_positions.get(p["symbol"])
    if watched and not watched.get("closed"):
        lines.append(f"  🎯 TP: {watched['tp']:.4f}")
        lines.append(f"  🛑 SL: {watched['sl']:.4f}")
    return "\n".join(lines)


HELP_TEXT = (
    "🤖 <b>بوت Binance — تقاطع EMA + مراقبة ذاتية</b>\n"
    "━━━━━━━━━━━━━━━━━━━\n\n"
    "<b>الأوامر:</b>\n"
    "/positions — الصفقات + TP/SL\n"
    "/balance — الرصيد\n"
    "/pnl — الربح/الخسارة\n"
    "/watch — الصفقات المراقَبة\n"
    "/status — حالة البوت\n"
    "/strategy — الاستراتيجية\n"
    "/rescan — إعادة مزامنة الآن\n"
    "/close SYMBOL — إغلاق صفقة\n"
    "/mute /unmute — الإشعارات"
)


# ============================================================
# الأوامر
# ============================================================
def authorized(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.id == CHAT_ID


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        positions = await fetch_positions(force=True)
        if not positions:
            await update.message.reply_text("لا توجد صفقات مفتوحة. ✨")
            return
        body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
        total_pnl = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        await update.message.reply_text(
            f"📊 <b>الصفقات المفتوحة ({len(positions)})</b>\n\n"
            f"{body}\n\n──────────────\n"
            f"📈 إجمالي PnL: <b>{total_pnl:+.4f} USDT</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        account = await fetch_account(force=True)
        wallet = float(account.get("totalWalletBalance", 0) or 0)
        unrealized = float(account.get("totalUnrealizedProfit", 0) or 0)
        available = float(account.get("availableBalance", 0) or 0)
        used = float(account.get("totalPositionInitialMargin", 0) or 0)
        daily = await get_daily_realized_pnl()

        await update.message.reply_text(
            f"💰 <b>الرصيد</b>\n\n"
            f"المحفظة: <b>{wallet:.2f}</b> USDT\n"
            f"PnL عائم: <b>{unrealized:+.4f}</b>\n"
            f"هامش مستخدم: <b>{used:.2f}</b>\n"
            f"متاح: <b>{available:.2f}</b>\n\n"
            f"📅 PnL اليوم: <b>{daily:+.4f}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    try:
        positions = await fetch_positions(force=True)
        unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        daily = await get_daily_realized_pnl()
        lines = []
        for p in positions:
            pnl = float(p.get("unRealizedProfit", 0) or 0)
            icon = "📈" if pnl >= 0 else "📉"
            lines.append(f"{icon} <b>{p.get('symbol', '?')}</b>: {pnl:+.4f}")
        await update.message.reply_text(
            f"📊 <b>PnL</b>\n\n"
            f"عائم: <b>{unrealized:+.4f}</b>\n"
            f"محقق اليوم: <b>{daily:+.4f}</b>\n\n"
            + "\n".join(lines) if lines else "—",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    if not _watched_positions:
        await update.message.reply_text(
            "👁️ لا توجد صفقات تحت المراقبة.\n\n"
            "<i>استخدم /rescan لإعادة المزامنة.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = [f"👁️ <b>تحت المراقبة ({len(_watched_positions)})</b>\n"]
    for sym, pos in _watched_positions.items():
        status = "🟢 نشط"
        if pos.get("closed"):
            status = "✅ أُغلق"
        elif pos.get("closing"):
            status = "⏳ جارٍ الإغلاق"

        side_emoji = "🟢 LONG" if pos["side"] == "LONG" else "🔴 SHORT"
        lines.append(
            f"\n<b>{sym}</b> {side_emoji}\n"
            f"  الحالة: {status}\n"
            f"  الدخول: {pos['entry']:.4f}\n"
            f"  🎯 TP: {pos['tp']:.4f}\n"
            f"  🛑 SL: {pos['sl']:.4f}"
        )

    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML
    )


async def cmd_rescan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text("🔄 جاري المزامنة...")
    await sync_positions_and_watch()
    await update.message.reply_text(
        f"✅ {len(_watched_positions)} صفقة تحت المراقبة.\n"
        f"أرسل /watch للتفاصيل."
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    ws_status = "🟢" if _user_stream_task and not _user_stream_task.done() else "🔴"
    mark_status = "🟢" if _mark_watcher_task and not _mark_watcher_task.done() else "🔴"
    ban = (f"\n⛔ محظور — ~{ban_remaining_sec()//60} دقيقة"
           if is_banned() else "")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    await update.message.reply_text(
        f"🤖 <b>حالة البوت</b>\n\n"
        f"Binance: {'🟢' if binance_client else '🔴'}\n"
        f"WebSocket أوامر: {ws_status}\n"
        f"WebSocket أسعار: {mark_status}\n"
        f"الإشعارات: {'🔔' if notifications_enabled else '🔕'}\n"
        f"الحماية: {'🟢' if AUTO_PROTECT_ENABLED else '🔴'}\n"
        f"الوضع: <b>{CLOSE_MODE}</b>\n"
        f"تحت المراقبة: {len(_watched_positions)}\n"
        f"الوقت: {ts}{ban}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    state_lines = []
    for s in STRATEGY_SYMBOLS:
        st = _last_ema_states.get(s, "—")
        st_str = {"FAST_ABOVE": "🟢", "FAST_BELOW": "🔴"}.get(st, "⚪")
        trend = _trend_cache.get(s)
        trend_str = ("📈" if trend[1] == "UP" else "📉") if trend else "—"
        state_lines.append(f"  {st_str} <b>{s}</b> {trend_str}")

    effective_sl = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
    notional = MARGIN_USDT * LEVERAGE
    daily = await get_daily_realized_pnl()

    await update.message.reply_text(
        f"📈 <b>الاستراتيجية</b>\n\n"
        f"{'🟢 مفعلة' if STRATEGY_ENABLED else '🔴 معطلة'}\n"
        f"EMA {EMA_FAST}/{EMA_SLOW} @ {STRATEGY_INTERVAL}\n\n"
        f"<b>الرموز:</b>\n" + "\n".join(state_lines) + "\n\n"
        f"الهامش: {MARGIN_USDT}$ × x{LEVERAGE} = {notional:.0f}$\n"
        f"🎯 +{TP_USDT}$ | 🛑 -{effective_sl:.2f}$\n"
        f"PnL اليوم: {daily:+.4f}\n\n"
        f"🛡️ المراقبة: TP +{AUTO_PROTECT_TP_PCT}% / SL -{AUTO_PROTECT_SL_PCT}%\n"
        f"الوضع: {CLOSE_MODE}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    if not context.args:
        await update.message.reply_text("الاستخدام: /close BTCUSDT")
        return
    symbol = context.args[0].upper()
    try:
        positions = await fetch_positions(force=True)
        target = next((p for p in positions if p["symbol"] == symbol), None)
        if not target:
            await update.message.reply_text(f"لا توجد صفقة على {symbol}.")
            return

        amt = float(target["positionAmt"])
        side = "SELL" if amt > 0 else "BUY"

        try:
            await binance_client.futures_cancel_all_open_orders(symbol=symbol)
        except Exception:
            pass

        order = await binance_client.futures_create_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=abs(amt), reduceOnly=True,
        )
        _watched_positions.pop(symbol, None)
        await update.message.reply_text(
            f"✅ أُغلق {symbol}\n{order.get('status', '')}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")


async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = False
    await update.message.reply_text("🔕")


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = True
    await update.message.reply_text("🔔")


# ============================================================
# الاستراتيجية
# ============================================================
async def check_crossover(symbol: str) -> str | None:
    if is_banned():
        return None
    try:
        klines = await binance_client.futures_klines(
            symbol=symbol, interval=STRATEGY_INTERVAL,
            limit=EMA_SLOW + 50,
        )
    except Exception:
        return None

    if len(klines) < EMA_SLOW + 5:
        return None

    candle_time = int(klines[-2][0])
    if _last_candle_times.get(symbol) == candle_time:
        return None
    _last_candle_times[symbol] = candle_time

    closes = [float(k[4]) for k in klines[:-1]]
    ema_fast = calc_ema(closes, EMA_FAST)
    ema_slow = calc_ema(closes, EMA_SLOW)
    if not ema_fast or not ema_slow:
        return None

    current = "FAST_ABOVE" if ema_fast[-1] > ema_slow[-1] else "FAST_BELOW"
    prev = _last_ema_states.get(symbol)
    _last_ema_states[symbol] = current

    if prev is None or prev == current:
        return None
    return "LONG" if current == "FAST_ABOVE" else "SHORT"


async def place_trade(symbol: str, side: str) -> bool:
    if TREND_FILTER_ENABLED:
        trend = await get_trend_direction(symbol)
        if trend is not None:
            if side == "LONG" and trend != "UP":
                return False
            if side == "SHORT" and trend != "DOWN":
                return False

    try:
        account = await fetch_account()
        available = float(account.get("availableBalance", 0) or 0)
        if available < MARGIN_USDT * 1.1:
            await send(f"⚠️ رصيد غير كافٍ لـ {symbol}")
            return False
    except Exception:
        pass

    try:
        await binance_client.futures_change_leverage(
            symbol=symbol, leverage=LEVERAGE
        )
    except Exception:
        pass

    try:
        step, tick = await get_symbol_filters(symbol)
    except Exception:
        return False

    try:
        ticker = await binance_client.futures_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
    except Exception:
        return False

    notional = MARGIN_USDT * LEVERAGE
    qty = round_step(notional / price, step)
    if qty <= 0:
        return False

    order_side = "BUY" if side == "LONG" else "SELL"
    try:
        order = await binance_client.futures_create_order(
            symbol=symbol, side=order_side, type="MARKET", quantity=qty,
        )
    except Exception as e:
        await send(f"❌ فتح {symbol}: {e}")
        return False

    fill_price = float(order.get("avgPrice") or 0) or price
    if fill_price <= 0:
        await asyncio.sleep(1)
        try:
            positions = await binance_client.futures_position_information(symbol=symbol)
            fill_price = float(positions[0].get("entryPrice", 0) or price)
        except Exception:
            fill_price = price

    await send(
        f"🎯 <b>فتح صفقة</b>\n\n"
        f"{'🟢 LONG' if side == 'LONG' else '🔴 SHORT'} | <b>{symbol}</b>\n"
        f"الدخول: {fill_price}\n"
        f"الكمية: {qty}\n"
        f"الهامش: {MARGIN_USDT}$ | x{LEVERAGE}"
    )

    # أضف إلى المراقبة فوراً
    await sync_positions_and_watch()
    return True


async def strategy_job(context: ContextTypes.DEFAULT_TYPE):
    global _last_strategy_candle
    if not STRATEGY_ENABLED or binance_client is None or is_banned():
        return

    last_candle = get_last_closed_candle_ts(STRATEGY_INTERVAL)
    if _last_strategy_candle == last_candle:
        return
    _last_strategy_candle = last_candle

    try:
        daily = await get_daily_realized_pnl()
        if daily <= -MAX_DAILY_LOSS_USDT:
            return
    except Exception:
        pass

    try:
        positions = await fetch_positions()
        open_symbols = {p["symbol"] for p in positions}
        if len(positions) >= MAX_CONCURRENT_TRADES:
            return

        for symbol in STRATEGY_SYMBOLS:
            if len(positions) >= MAX_CONCURRENT_TRADES:
                break
            if symbol in open_symbols:
                continue

            signal = await check_crossover(symbol)
            if signal is None:
                continue

            log.info(f"📶 {symbol}: {signal}")
            success = await place_trade(symbol, signal)
            if success:
                positions = positions + [{"symbol": symbol}]
                open_symbols.add(symbol)
    except Exception as e:
        log.exception(f"strategy_job: {e}")


# ============================================================
# تقرير كل ساعة
# ============================================================
async def hourly_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        if is_banned():
            return

        positions = await fetch_positions()
        try:
            account = await fetch_account()
            balance = float(account.get("totalWalletBalance", 0) or 0)
        except Exception:
            balance = 0.0

        unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        daily = await get_daily_realized_pnl()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        if not positions:
            msg = (
                f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n"
                f"لا صفقات.\n"
                f"💰 الرصيد: {balance:.2f}$\n"
                f"📅 PnL اليوم: {daily:+.4f}$"
            )
        else:
            body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
            msg = (
                f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n{body}\n\n"
                f"──────────────\n"
                f"💰 الرصيد: {balance:.2f}$\n"
                f"📊 عائم: {unrealized:+.4f}$\n"
                f"📅 اليوم: {daily:+.4f}$\n"
                f"👁️ مراقبة: {len(_watched_positions)}"
            )

        await context.bot.send_message(
            chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML
        )
    except Exception as e:
        log.exception(f"hourly_job: {e}")


# ============================================================
# WebSocket أوامر
# ============================================================
async def user_stream_task(app: Application):
    bsm = BinanceSocketManager(binance_client)
    while True:
        try:
            async with bsm.futures_user_socket() as stream:
                log.info("🔌 WebSocket الأوامر متصل")
                while True:
                    try:
                        msg = await stream.recv()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.warning(f"recv: {e}")
                        break

                    if msg.get("e") == "ACCOUNT_UPDATE":
                        for pos in msg["a"]["P"]:
                            amt = float(pos["pa"])
                            if amt != 0 and pos.get("bc", "0") == "0":
                                # مركز جديد → أضف للمراقبة
                                asyncio.create_task(
                                    sync_positions_and_watch()
                                )

                    elif msg.get("e") == "ORDER_TRADE_UPDATE":
                        o = msg["o"]
                        if o["X"] == "FILLED":
                            otype = o.get("ot", "")
                            rp_val = float(o.get("rp", "0") or 0)
                            pnl_txt = (
                                f"\n💰 PnL: <b>{rp_val:+.4f}$</b>"
                                if rp_val != 0 else ""
                            )

                            if "TAKE_PROFIT" in otype:
                                emoji, title = "🎯", "جني أرباح"
                            elif "STOP" in otype:
                                emoji, title = "🛑", "وقف خسارة"
                            else:
                                emoji, title = "⚡", "تنفيذ"

                            if "TAKE_PROFIT" in otype or "STOP" in otype:
                                _watched_positions.pop(o["s"], None)

                            if notifications_enabled:
                                try:
                                    await app.bot.send_message(
                                        chat_id=CHAT_ID,
                                        text=(
                                            f"{emoji} <b>{title}</b>\n"
                                            f"<b>{o['s']}</b> | {o['S']}\n"
                                            f"الكمية: {o['q']}\n"
                                            f"السعر: {o.get('ap') or '0'}{pnl_txt}"
                                        ),
                                        parse_mode=ParseMode.HTML,
                                    )
                                except Exception:
                                    pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"WS: {e}")
            await asyncio.sleep(WS_RECONNECT_DELAY)


# ============================================================
# Lifecycle
# ============================================================
async def post_init(app: Application):
    global binance_client, _app, _user_stream_task, _mark_watcher_task
    _app = app

    binance_client = await AsyncClient.create(API_KEY, API_SECRET)
    log.info("Binance client جاهز")

    # ابدأ WebSocket الأوامر
    _user_stream_task = app.create_task(user_stream_task(app))

    # ابدأ WebSocket الأسعار (مراقبة TP/SL)
    if AUTO_PROTECT_ENABLED:
        _mark_watcher_task = app.create_task(mark_price_watcher(app))

    # مزامنة أولية
    try:
        await asyncio.sleep(2)
        await sync_positions_and_watch()
    except Exception as e:
        log.warning(f"initial sync: {e}")

    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"🤖 <b>بدأ البوت</b>\n\n"
                f"🛡️ الوضع: <b>{CLOSE_MODE}</b>\n"
                f"TP: +{AUTO_PROTECT_TP_PCT}% | SL: -{AUTO_PROTECT_SL_PCT}%\n"
                f"👁️ تحت المراقبة: {len(_watched_positions)}\n\n"
                f"/help للأوامر"
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.error(f"startup: {e}")


async def post_shutdown(app: Application):
    global binance_client, _user_stream_task, _mark_watcher_task
    for task in (_user_stream_task, _mark_watcher_task):
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=3)
            except Exception:
                pass
    if binance_client:
        try:
            await binance_client.close_connection()
        except Exception:
            pass
        binance_client = None
    log.info("Bot أُغلق")


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        return
    log.error(f"❌ {err}")


# ============================================================
# Main
# ============================================================
def main():
    threading.Thread(target=_run_health, daemon=True).start()
    print(f"🚀 Binance Bot — وضع: {CLOSE_MODE}")

    app = Application.builder() \
        .token(TELEGRAM_TOKEN) \
        .post_init(post_init) \
        .post_shutdown(post_shutdown) \
        .build()

    handlers = [
        ("start", cmd_start), ("help", cmd_help),
        ("positions", cmd_positions), ("balance", cmd_balance),
        ("pnl", cmd_pnl), ("watch", cmd_watch),
        ("status", cmd_status), ("strategy", cmd_strategy),
        ("rescan", cmd_rescan), ("close", cmd_close),
        ("mute", cmd_mute), ("unmute", cmd_unmute),
    ]
    for name, h in handlers:
        app.add_handler(CommandHandler(name, h))
    app.add_error_handler(error_handler)

    if app.job_queue:
        app.job_queue.run_repeating(
            hourly_job, interval=HOURLY_MIN * 60, first=300, name="hourly"
        )

        if STRATEGY_ENABLED:
            app.job_queue.run_repeating(
                strategy_job, interval=STRATEGY_JOB_INTERVAL,
                first=15, name="strategy",
            )

        if AUTO_PROTECT_ENABLED:
            app.job_queue.run_repeating(
                watch_sync_job, interval=AUTO_PROTECT_INTERVAL,
                first=30, name="watch_sync",
            )

    print("✅ جاهز")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        bootstrap_retries=5,
        read_timeout=30, write_timeout=30,
        connect_timeout=30, pool_timeout=30,
    )


if __name__ == "__main__":
    main()
