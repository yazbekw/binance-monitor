"""
Binance Monitor Bot — مراقبة + تداول آلي + حماية تلقائية للصفقات اليدوية
استراتيجية: تقاطع EMA + فلتر اتجاه EMA 200
تحسينات: استهلاك API منخفض + حماية تلقائية
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
print(f"❌ متغيرات ناقصة: {', '.join(missing)}", file=sys.stderr) if missing else None
if missing:
    sys.exit(1)

CHAT_ID = int(CHAT_ID_RAW)

HOURLY_MIN = 60
WS_RECONNECT_DELAY = 10

# ============================================================
# إعدادات الاستراتيجية
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

# فلتر الاتجاه العام
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
# الحماية التلقائية للصفقات اليدوية
# ============================================================
AUTO_PROTECT_ENABLED = os.getenv("AUTO_PROTECT_ENABLED", "true").lower() == "true"
AUTO_PROTECT_INTERVAL = int(os.getenv("AUTO_PROTECT_INTERVAL", "60"))
AUTO_PROTECT_MIN_NOTIONAL = float(os.getenv("AUTO_PROTECT_MIN_NOTIONAL", "20"))

# النسب الافتراضية = نفس فعالية الاستراتيجية
_notional_ref = MARGIN_USDT * LEVERAGE
_effective_sl_ref = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
_default_tp_pct = round(TP_USDT / _notional_ref * 100, 3)
_default_sl_pct = round(_effective_sl_ref / _notional_ref * 100, 3)

AUTO_PROTECT_TP_PCT = float(os.getenv("AUTO_PROTECT_TP_PCT", str(_default_tp_pct)))
AUTO_PROTECT_SL_PCT = float(os.getenv("AUTO_PROTECT_SL_PCT", str(_default_sl_pct)))

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("binance").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ============================================================
# حالة عامة
# ============================================================
binance_client: AsyncClient | None = None
notifications_enabled = True
_app: Application | None = None

_http_cache: dict = {}
_banned_until_ms: int = 0

# حالة الاستراتيجية
_last_ema_states: dict[str, str] = {}
_last_candle_times: dict[str, int] = {}
_symbol_filters_cache: dict = {}
_trend_cache: dict = {}

# منع تكرار فحص الاستراتيجية على نفس الشمعة
_last_strategy_candle: int = 0

# كاش حماية الصفقات: {symbol: (abs_amt, entry_price)}
_protected_positions: dict = {}

# كاش PnL اليومي
_daily_pnl_cache = {"date": None, "pnl": 0.0, "last_fetch": 0.0}


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
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    log.info(f"🩺 Health server على المنفذ {port}")
    server.serve_forever()


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
            f"⛔ Binance حظر IP مؤقتاً. المتبقي: ~{ban_remaining_sec()//60} دقيقة."
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
    """
    يحسب وقت فتح آخر شمعة مغلقة لفريم معين (ms).
    يعتمد على التزامن المعروف لشموع Binance.
    """
    interval_ms = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "2h": 7_200_000,
        "4h": 14_400_000,
        "6h": 21_600_000,
        "8h": 28_800_000,
        "12h": 43_200_000,
        "1d": 86_400_000,
    }.get(interval)
    if not interval_ms:
        return 0
    now_ms = int(time.time() * 1000)
    current_open = (now_ms // interval_ms) * interval_ms
    return current_open - interval_ms


# ============================================================
# جلب البيانات (مع كاش أطول)
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


async def fetch_open_orders(force: bool = False) -> list:
    if force:
        _http_cache.pop("orders", None)
    return await cached_http(
        "orders",
        lambda: binance_client.futures_get_open_orders(),
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
    raise ValueError(f"الرمز {symbol} غير موجود في Binance Futures")


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
        log.warning(f"daily pnl fetch: {e}")
        return _daily_pnl_cache.get("pnl", 0.0)


# ============================================================
# فلتر الاتجاه العام
# ============================================================
async def get_trend_direction(symbol: str) -> str | None:
    if not TREND_FILTER_ENABLED:
        return None

    now = time.time()
    cached = _trend_cache.get(symbol)
    if cached and now - cached[0] < 300:
        return cached[1]

    try:
        klines = await binance_client.futures_klines(
            symbol=symbol,
            interval=TREND_TIMEFRAME,
            limit=TREND_EMA_PERIOD + 50,
        )
    except Exception as e:
        log.warning(f"trend fetch {symbol}: {e}")
        return None

    if len(klines) < TREND_EMA_PERIOD + 5:
        return None

    closes = [float(k[4]) for k in klines[:-1]]
    ema = calc_ema(closes, TREND_EMA_PERIOD)
    if not ema:
        return None

    last_price = closes[-1]
    trend = "UP" if last_price > ema[-1] else "DOWN"
    _trend_cache[symbol] = (now, trend)
    return trend


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
    return "\n".join(lines)


HELP_TEXT = (
    "🤖 <b>بوت Binance — تقاطع EMA + حماية تلقائية</b>\n"
    "━━━━━━━━━━━━━━━━━━━\n\n"
    "<b>الأوامر:</b>\n"
    "/positions — الصفقات المفتوحة\n"
    "/balance — الرصيد والهامش\n"
    "/pnl — الربح/الخسارة\n"
    "/orders — الأوامر المعلقة\n"
    "/status — حالة البوت\n"
    "/strategy — حالة الاستراتيجية\n"
    "/protect — فحص وحماية الصفقات يدوياً الآن\n"
    "/close SYMBOL — إغلاق صفقة\n"
    "/mute — إيقاف الإشعارات\n"
    "/unmute — تشغيل الإشعارات"
)


# ============================================================
# أوامر Telegram
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
            await update.message.reply_text("لا توجد صفقات مفتوحة حالياً. ✨")
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
        log.exception("cmd_positions")
        await update.message.reply_text(f"❌ {e}")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        account = await fetch_account(force=True)
        wallet = float(account.get("totalWalletBalance", 0) or 0)
        unrealized = float(account.get("totalUnrealizedProfit", 0) or 0)
        margin_bal = float(account.get("totalMarginBalance", 0) or 0)
        available = float(account.get("availableBalance", 0) or 0)
        used = float(account.get("totalPositionInitialMargin", 0) or 0)

        daily = await get_daily_realized_pnl()

        await update.message.reply_text(
            f"💰 <b>الرصيد</b>\n\n"
            f"المحفظة: <b>{wallet:.2f}</b> USDT\n"
            f"PnL عائم: <b>{unrealized:+.4f}</b> USDT\n"
            f"رصيد الهامش: <b>{margin_bal:.2f}</b> USDT\n"
            f"هامش مستخدم: <b>{used:.2f}</b> USDT\n"
            f"متاح للتداول: <b>{available:.2f}</b> USDT\n\n"
            f"📅 PnL اليوم: <b>{daily:+.4f}</b> / -{MAX_DAILY_LOSS_USDT:.0f} USDT",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_balance")
        await update.message.reply_text(f"❌ {e}")


async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        positions = await fetch_positions(force=True)
        unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
        daily = await get_daily_realized_pnl()

        lines = []
        for p in positions:
            pnl = float(p.get("unRealizedProfit", 0) or 0)
            icon = "📈" if pnl >= 0 else "📉"
            lines.append(f"{icon} <b>{p.get('symbol', '?')}</b>: {pnl:+.4f} USDT")
        breakdown = "\n".join(lines) if lines else "—"

        await update.message.reply_text(
            f"📊 <b>PnL</b>\n\n"
            f"عائم الآن: <b>{unrealized:+.4f}</b> USDT\n"
            f"محقق اليوم: <b>{daily:+.4f}</b> USDT\n\n"
            f"<b>تفصيل:</b>\n{breakdown}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_pnl")
        await update.message.reply_text(f"❌ {e}")


async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        orders = await fetch_open_orders(force=True)
        if not orders:
            await update.message.reply_text("لا توجد أوامر معلقة.")
            return
        lines = []
        for o in orders:
            price = o.get("price") or o.get("stopPrice") or "Market"
            lines.append(
                f"• <b>{o['symbol']}</b> {o['side']} {o['type']}\n"
                f"  الكمية: {o['origQty']} @ {price}"
            )
        await update.message.reply_text(
            f"📋 <b>الأوامر المعلقة ({len(orders)})</b>\n\n" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_orders")
        await update.message.reply_text(f"❌ {e}")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    status = "🟢 متصل" if binance_client else "🔴 غير متصل"
    notif = "🔔 مفعلة" if notifications_enabled else "🔕 مكتومة"
    ban = (f"\n⛔ محظور — متبقي ~{ban_remaining_sec()//60} دقيقة"
           if is_banned() else "")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    protect = "🟢 مفعلة" if AUTO_PROTECT_ENABLED else "🔴 معطلة"

    await update.message.reply_text(
        f"🤖 <b>حالة البوت</b>\n\n"
        f"Binance: {status}\n"
        f"الإشعارات: {notif}\n"
        f"الحماية التلقائية: {protect}\n"
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
        trend_str = ("📈 UP" if trend[1] == "UP" else "📉 DOWN") if trend else "—"
        state_lines.append(f"  {st_str} <b>{s}</b> | اتجاه: {trend_str}")

    daily = await get_daily_realized_pnl()
    effective_sl = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
    sl_capped = SL_USDT > effective_sl

    notional = MARGIN_USDT * LEVERAGE
    tp_pct = TP_USDT / notional * 100
    sl_pct = effective_sl / notional * 100

    trend_filter_str = (
        f"🟢 مفعل (EMA {TREND_EMA_PERIOD} @ {TREND_TIMEFRAME})"
        if TREND_FILTER_ENABLED else "🔴 معطل"
    )

    msg = (
        f"📈 <b>حالة الاستراتيجية</b>\n\n"
        f"الحالة: {'🟢 مفعلة' if STRATEGY_ENABLED else '🔴 معطلة'}\n"
        f"الإطار: {STRATEGY_INTERVAL} | EMA {EMA_FAST}/{EMA_SLOW}\n"
        f"فلتر الاتجاه: {trend_filter_str}\n\n"
        f"<b>الرموز ({len(STRATEGY_SYMBOLS)}):</b>\n" + "\n".join(state_lines) + "\n\n"
        f"الهامش: {MARGIN_USDT} USDT | x{LEVERAGE} → ~{notional:.0f} USDT\n"
        f"🎯 TP: +{TP_USDT} (~{tp_pct:.2f}%)\n"
        f"🛑 SL: -{effective_sl:.2f}"
        + (f" (مقصوص من {SL_USDT})" if sl_capped else "")
        + f" (~{sl_pct:.2f}%)\n"
        f"حد الصفقات: {MAX_CONCURRENT_TRADES}\n"
        f"حد الخسارة اليومي: -{MAX_DAILY_LOSS_USDT} USDT\n"
        f"PnL اليوم: {daily:+.4f} USDT\n\n"
        f"🛡️ <b>الحماية التلقائية:</b> "
        f"{'🟢 ' if AUTO_PROTECT_ENABLED else '🔴 '}"
        f"TP +{AUTO_PROTECT_TP_PCT}% / SL -{AUTO_PROTECT_SL_PCT}%\n"
        f"   فحص كل {AUTO_PROTECT_INTERVAL}s | أدنى notional: {AUTO_PROTECT_MIN_NOTIONAL}$"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_protect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text("🛡️ جاري فحص الصفقات وحمايتها...")
    _protected_positions.clear()  # إعادة الفحص من الصفر
    await auto_protect_job(context)
    await update.message.reply_text("✅ تم الانتهاء من الفحص.")


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
        _protected_positions.pop(symbol, None)
        await update.message.reply_text(
            f"✅ تم إغلاق صفقة {symbol}\n{order.get('status', '')}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_close")
        await update.message.reply_text(f"❌ {e}")


async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = False
    await update.message.reply_text("🔕 تم إيقاف الإشعارات.")


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = True
    await update.message.reply_text("🔔 تم تشغيل الإشعارات.")


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
    except Exception as e:
        log.warning(f"{symbol}: klines fetch failed: {e}")
        return None

    if len(klines) < EMA_SLOW + 5:
        return None

    last_closed = klines[-2]
    candle_time = int(last_closed[0])

    if _last_candle_times.get(symbol) == candle_time:
        return None
    _last_candle_times[symbol] = candle_time

    closes = [float(k[4]) for k in klines[:-1]]
    ema_fast = calc_ema(closes, EMA_FAST)
    ema_slow = calc_ema(closes, EMA_SLOW)

    if not ema_fast or not ema_slow:
        return None

    current_state = "FAST_ABOVE" if ema_fast[-1] > ema_slow[-1] else "FAST_BELOW"
    prev_state = _last_ema_states.get(symbol)
    _last_ema_states[symbol] = current_state

    if prev_state is None or prev_state == current_state:
        return None

    return "LONG" if current_state == "FAST_ABOVE" else "SHORT"


async def place_trade(symbol: str, side: str) -> bool:
    if TREND_FILTER_ENABLED:
        trend = await get_trend_direction(symbol)
        if trend is not None:
            if side == "LONG" and trend != "UP":
                log.info(f"{symbol}: LONG مرفوض — الاتجاه {trend}")
                return False
            if side == "SHORT" and trend != "DOWN":
                log.info(f"{symbol}: SHORT مرفوض — الاتجاه {trend}")
                return False

    try:
        account = await fetch_account()
        available = float(account.get("availableBalance", 0) or 0)
        if available < MARGIN_USDT * 1.1:
            await send(
                f"⚠️ رصيد غير كافٍ لفتح صفقة على {symbol}\n"
                f"مطلوب: ~{MARGIN_USDT * 1.1:.2f} USDT\n"
                f"متاح: {available:.2f} USDT"
            )
            return False
    except Exception as e:
        log.warning(f"balance check failed: {e}")

    try:
        await binance_client.futures_change_leverage(
            symbol=symbol, leverage=LEVERAGE
        )
    except Exception as e:
        log.warning(f"set leverage {symbol}: {e}")

    try:
        step, tick = await get_symbol_filters(symbol)
    except Exception as e:
        await send(f"⚠️ فشل جلب دقة {symbol}: {e}")
        return False

    try:
        ticker = await binance_client.futures_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])
    except Exception as e:
        await send(f"⚠️ فشل جلب سعر {symbol}: {e}")
        return False

    notional = MARGIN_USDT * LEVERAGE
    qty_raw = notional / price
    qty = round_step(qty_raw, step)

    if qty <= 0:
        await send(f"⚠️ الكمية المحسوبة صفر على {symbol}")
        return False

    tp_move_pct = TP_USDT / notional
    max_sl_usdt = MARGIN_USDT * SL_CAP_RATIO
    effective_sl = SL_USDT
    sl_capped = False
    if SL_USDT > max_sl_usdt:
        effective_sl = max_sl_usdt
        sl_capped = True
    sl_move_pct = effective_sl / notional

    order_side = "BUY" if side == "LONG" else "SELL"
    try:
        order = await binance_client.futures_create_order(
            symbol=symbol, side=order_side, type="MARKET", quantity=qty,
        )
    except Exception as e:
        log.exception(f"market order {symbol}")
        await send(f"❌ فشل فتح صفقة {symbol}: {e}")
        return False

    fill_price = float(order.get("avgPrice") or 0) or price
    if fill_price <= 0:
        await asyncio.sleep(1)
        try:
            positions = await binance_client.futures_position_information(symbol=symbol)
            fill_price = float(positions[0].get("entryPrice", 0) or price)
        except Exception:
            fill_price = price

    if side == "LONG":
        tp_price = fill_price * (1 + tp_move_pct)
        sl_price = fill_price * (1 - sl_move_pct)
        close_side = "SELL"
    else:
        tp_price = fill_price * (1 - tp_move_pct)
        sl_price = fill_price * (1 + sl_move_pct)
        close_side = "BUY"

    tp_price = round_step(tp_price, tick)
    sl_price = round_step(sl_price, tick)

    tp_ok, sl_ok = True, True
    try:
        await binance_client.futures_create_order(
            symbol=symbol, side=close_side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=tp_price, closePosition=True,
            workingType="MARK_PRICE",
        )
    except Exception as e:
        tp_ok = False
        log.error(f"TP order {symbol} failed: {e}")

    try:
        await binance_client.futures_create_order(
            symbol=symbol, side=close_side,
            type="STOP_MARKET",
            stopPrice=sl_price, closePosition=True,
            workingType="MARK_PRICE",
        )
    except Exception as e:
        sl_ok = False
        log.error(f"SL order {symbol} failed: {e}")

    # سجّل الصفقة كمحمية
    if tp_ok and sl_ok:
        _protected_positions[symbol] = (qty, fill_price)

    side_emoji = "🟢 LONG" if side == "LONG" else "🔴 SHORT"
    warn_lines = []
    if sl_capped:
        warn_lines.append(
            f"⚠️ <b>SL مقصوص تلقائياً</b>\n"
            f"طلبت: -{SL_USDT} USDT | طُبّق: -{effective_sl:.2f} USDT"
        )
    if not tp_ok or not sl_ok:
        warn_lines.append(
            f"⚠️ فشل وضع {'TP ' if not tp_ok else ''}{'SL ' if not sl_ok else ''}"
            "— تابع يدوياً!"
        )

    trend_txt = ""
    if TREND_FILTER_ENABLED:
        t = _trend_cache.get(symbol)
        if t:
            trend_txt = f"\n📊 الاتجاه العام: {'📈 UP' if t[1]=='UP' else '📉 DOWN'}"

    try:
        positions = await fetch_positions()
        open_count = len(positions)
    except Exception:
        open_count = "?"

    msg = (
        f"🎯 <b>فتح صفقة — تقاطع EMA</b>\n\n"
        f"الاتجاه: {side_emoji}\n"
        f"الرمز: <b>{symbol}</b>\n"
        f"الإطار: {STRATEGY_INTERVAL} | EMA {EMA_FAST}/{EMA_SLOW}"
        f"{trend_txt}\n"
        f"سعر الدخول: <b>{fill_price}</b>\n"
        f"الكمية: {qty}\n"
        f"الهامش: {MARGIN_USDT} USDT\n"
        f"الرافعة: x{LEVERAGE} (فعلي ~{notional:.0f} USDT)\n\n"
        f"🎯 TP: {tp_price} (+{TP_USDT} ≈ +{tp_move_pct*100:.2f}%)\n"
        f"🛑 SL: {sl_price} (-{effective_sl:.2f} ≈ -{sl_move_pct*100:.2f}%)\n\n"
        f"📊 الصفقات المفتوحة: {open_count}/{MAX_CONCURRENT_TRADES}"
    )
    if warn_lines:
        msg += "\n\n" + "\n\n".join(warn_lines)

    await send(msg)
    log.info(
        f"TRADE: {symbol} {side} @ {fill_price} qty={qty} "
        f"TP={tp_price} SL={sl_price} open={open_count}"
    )
    return True


async def strategy_job(context: ContextTypes.DEFAULT_TYPE):
    """
    يعمل كل STRATEGY_JOB_INTERVAL ثانية، لكن يفحص فعلياً
    مرة واحدة فقط لكل شمعة جديدة (يقلل استهلاك API بشكل كبير).
    """
    global _last_strategy_candle

    if not STRATEGY_ENABLED or binance_client is None or is_banned():
        return

    # تحقق: هل يوجد تقاطع محتمل في هذه الدورة؟
    last_closed_candle = get_last_closed_candle_ts(STRATEGY_INTERVAL)
    if _last_strategy_candle == last_closed_candle:
        return  # نفس الشمعة، لا داعي للفحص
    _last_strategy_candle = last_closed_candle

    # Kill Switch
    try:
        daily = await get_daily_realized_pnl()
        if daily <= -MAX_DAILY_LOSS_USDT:
            log.warning(f"🛑 Kill switch: خسارة اليوم {daily:.2f}")
            return
    except Exception as e:
        log.warning(f"daily pnl check: {e}")

    try:
        positions = await fetch_positions()
        open_symbols = {p["symbol"] for p in positions}

        if len(positions) >= MAX_CONCURRENT_TRADES:
            log.info(f"وصلنا للحد الأقصى ({len(positions)}/{MAX_CONCURRENT_TRADES})")
            return

        for symbol in STRATEGY_SYMBOLS:
            if len(positions) >= MAX_CONCURRENT_TRADES:
                break
            if symbol in open_symbols:
                continue

            signal = await check_crossover(symbol)
            if signal is None:
                continue

            log.info(f"📶 إشارة {symbol}: {signal}")
            success = await place_trade(symbol, signal)
            if success:
                try:
                    positions = await fetch_positions(force=True)
                    open_symbols = {p["symbol"] for p in positions}
                except Exception:
                    open_symbols.add(symbol)
                    positions = positions + [{"symbol": symbol}]
    except Exception as e:
        log.exception(f"strategy_job: {e}")


# ============================================================
# الحماية التلقائية للصفقات اليدوية
# ============================================================
async def auto_protect_job(context: ContextTypes.DEFAULT_TYPE | None = None):
    """
    يفحص المراكز المفتوحة ويضع TP/SL لأي مركز يدوي بلا حماية.
    يستخدم بيانات bulk (طلب واحد للمراكز + طلب واحد للأوامر).
    """
    if not AUTO_PROTECT_ENABLED or binance_client is None:
        return
    if is_banned():
        return

    try:
        positions = await fetch_positions()
        if not positions:
            _protected_positions.clear()
            return

        all_orders = await fetch_open_orders()
        orders_by_symbol: dict = {}
        for o in all_orders:
            orders_by_symbol.setdefault(o["symbol"], []).append(o)

        protected_now = 0

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

            # هل عالجناها سابقاً؟
            cached = _protected_positions.get(symbol)
            if cached:
                cached_amt, cached_entry = cached
                amt_same = abs(cached_amt - abs_amt) / max(abs_amt, 1e-9) < 1e-4
                entry_same = abs(cached_entry - entry) / max(entry, 1e-9) < 1e-4
                if amt_same and entry_same:
                    continue

            # فحص الأوامر الموجودة
            symbol_orders = orders_by_symbol.get(symbol, [])
            has_tp = False
            has_sl = False
            for o in symbol_orders:
                otype = o.get("type", "")
                close_pos = o.get("closePosition", False)
                reduce_only = o.get("reduceOnly", False)
                if not (close_pos or reduce_only):
                    continue
                if otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                    has_tp = True
                elif otype in ("STOP_MARKET", "STOP"):
                    has_sl = True

            if has_tp and has_sl:
                _protected_positions[symbol] = (abs_amt, entry)
                continue

            # دقة الرمز
            try:
                step, tick = await get_symbol_filters(symbol)
            except Exception as e:
                log.warning(f"filters {symbol}: {e}")
                continue

            is_long = amt > 0
            close_side = "SELL" if is_long else "BUY"

            if is_long:
                tp_price = entry * (1 + AUTO_PROTECT_TP_PCT / 100)
                sl_price = entry * (1 - AUTO_PROTECT_SL_PCT / 100)
            else:
                tp_price = entry * (1 - AUTO_PROTECT_TP_PCT / 100)
                sl_price = entry * (1 + AUTO_PROTECT_SL_PCT / 100)

            tp_price = round_step(tp_price, tick)
            sl_price = round_step(sl_price, tick)

            placed_tp = has_tp
            placed_sl = has_sl

            if not has_tp:
                for attempt in range(3):
                    try:
                        await binance_client.futures_create_order(
                            symbol=symbol, side=close_side,
                            type="TAKE_PROFIT_MARKET",
                            stopPrice=tp_price,
                            closePosition=True,
                            workingType="MARK_PRICE",
                        )
                        placed_tp = True
                        break
                    except Exception as e:
                        log.warning(f"TP attempt {attempt+1} {symbol}: {e}")
                        await asyncio.sleep(1)

            if not has_sl:
                for attempt in range(3):
                    try:
                        await binance_client.futures_create_order(
                            symbol=symbol, side=close_side,
                            type="STOP_MARKET",
                            stopPrice=sl_price,
                            closePosition=True,
                            workingType="MARK_PRICE",
                        )
                        placed_sl = True
                        break
                    except Exception as e:
                        log.warning(f"SL attempt {attempt+1} {symbol}: {e}")
                        await asyncio.sleep(1)

            if placed_tp or placed_sl:
                _protected_positions[symbol] = (abs_amt, entry)
                protected_now += 1

                side_emoji = "🟢 LONG" if is_long else "🔴 SHORT"
                tp_line = (
                    f"🎯 TP: {tp_price} (+{AUTO_PROTECT_TP_PCT:.3f}%)"
                    if not has_tp else "🎯 TP: موجود مسبقاً"
                )
                sl_line = (
                    f"🛑 SL: {sl_price} (-{AUTO_PROTECT_SL_PCT:.3f}%)"
                    if not has_sl else "🛑 SL: موجود مسبقاً"
                )

                msg = (
                    f"🛡️ <b>حماية صفقة يدوية</b>\n\n"
                    f"الرمز: <b>{symbol}</b>\n"
                    f"الاتجاه: {side_emoji}\n"
                    f"الكمية: {abs_amt}\n"
                    f"الدخول: {entry}\n"
                    f"Notional: ~{notional:.2f} USDT\n\n"
                    f"{tp_line}\n"
                    f"{sl_line}"
                )
                await send(msg)
                log.info(f"🛡️ Protected {symbol} tp={tp_price} sl={sl_price}")

        if protected_now:
            log.info(f"🛡️ تمت حماية {protected_now} صفقة")
    except Exception as e:
        log.exception(f"auto_protect_job: {e}")


# ============================================================
# Job كل ساعة
# ============================================================
async def hourly_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        if is_banned():
            log.warning("hourly: متخطى (حظر)")
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
                f"لا توجد صفقات مفتوحة.\n"
                f"💰 الرصيد: {balance:.2f} USDT\n"
                f"📅 PnL اليوم: {daily:+.4f} USDT"
            )
        else:
            body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
            msg = (
                f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n{body}\n\n"
                f"──────────────\n"
                f"💰 الرصيد: {balance:.2f} USDT\n"
                f"📊 PnL عائم: {unrealized:+.4f} USDT\n"
                f"📅 PnL اليوم: {daily:+.4f} USDT\n"
                f"🔢 الصفقات: {len(positions)}/{MAX_CONCURRENT_TRADES}"
            )

        await context.bot.send_message(
            chat_id=CHAT_ID, text=msg, parse_mode=ParseMode.HTML
        )
        log.info("✅ Hourly report sent")
    except Exception as e:
        log.exception(f"hourly_job: {e}")


# ============================================================
# WebSocket
# ============================================================
async def user_stream_task(app: Application):
    bsm = BinanceSocketManager(binance_client)
    while True:
        try:
            async with bsm.futures_user_socket() as stream:
                log.info("🔌 WebSocket متصل")

                while True:
                    msg = await stream.recv()

                    if msg.get("e") == "ACCOUNT_UPDATE":
                        for pos in msg["a"]["P"]:
                            amt = float(pos["pa"])
                            if amt != 0 and pos.get("bc", "0") == "0":
                                side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
                                text = (
                                    f"🚀 <b>صفقة مفتوحة</b>\n\n"
                                    f"الاتجاه: {side}\n"
                                    f"الرمز: <b>{pos['s']}</b>\n"
                                    f"الحجم: {abs(amt)}\n"
                                    f"الدخول: {pos['ep']}"
                                )
                                if notifications_enabled:
                                    try:
                                        await app.bot.send_message(
                                            chat_id=CHAT_ID, text=text,
                                            parse_mode=ParseMode.HTML,
                                        )
                                    except Exception as e:
                                        log.error(f"WS send: {e}")

                    elif msg.get("e") == "ORDER_TRADE_UPDATE":
                        o = msg["o"]
                        if o["X"] == "FILLED":
                            otype = o.get("ot", "")
                            rp_val = float(o.get("rp", "0") or 0)
                            pnl_txt = (
                                f"\n💰 PnL محقق: <b>{rp_val:+.4f} USDT</b>"
                                if rp_val != 0 else ""
                            )

                            if otype == "TAKE_PROFIT_MARKET":
                                emoji, title = "🎯", "جني أرباح (TP)"
                            elif otype == "STOP_MARKET":
                                emoji, title = "🛑", "وقف خسارة (SL)"
                            else:
                                emoji, title = "⚡", "تنفيذ أمر"

                            # إذا أُغلق مركز، نظّف الكاش
                            if otype in ("TAKE_PROFIT_MARKET", "STOP_MARKET"):
                                _protected_positions.pop(o["s"], None)

                            text = (
                                f"{emoji} <b>{title}</b>\n"
                                f"الرمز: <b>{o['s']}</b>\n"
                                f"الاتجاه: {o['S']}\n"
                                f"الكمية: {o['q']}\n"
                                f"متوسط السعر: {o.get('ap') or '0'}{pnl_txt}"
                            )
                            if notifications_enabled:
                                try:
                                    await app.bot.send_message(
                                        chat_id=CHAT_ID, text=text,
                                        parse_mode=ParseMode.HTML,
                                    )
                                except Exception as e:
                                    log.error(f"WS send: {e}")
        except Exception as e:
            log.exception(f"user_stream: {e}")
            await asyncio.sleep(WS_RECONNECT_DELAY)


# ============================================================
# Lifecycle
# ============================================================
async def post_init(app: Application):
    global binance_client, _app
    _app = app

    binance_client = await AsyncClient.create(API_KEY, API_SECRET)
    log.info("Binance client جاهز")

    app.create_task(user_stream_task(app))

    if STRATEGY_ENABLED:
        notional = MARGIN_USDT * LEVERAGE
        effective_sl = min(SL_USDT, MARGIN_USDT * SL_CAP_RATIO)
        sl_note = f" (مقصوص من {SL_USDT})" if effective_sl < SL_USDT else ""
        trend_str = (
            f"🟢 EMA {TREND_EMA_PERIOD} @ {TREND_TIMEFRAME}"
            if TREND_FILTER_ENABLED else "🔴 معطل"
        )
        strategy_status = (
            f"🟢 مفعلة على {len(STRATEGY_SYMBOLS)} رموز\n"
            f"   {', '.join(STRATEGY_SYMBOLS)}\n"
            f"   EMA {EMA_FAST}/{EMA_SLOW} @ {STRATEGY_INTERVAL}\n"
            f"   فلتر الاتجاه: {trend_str}\n"
            f"   هامش {MARGIN_USDT} USDT × x{LEVERAGE} ≈ {notional:.0f} USDT\n"
            f"   🎯 TP +{TP_USDT} | 🛑 SL -{effective_sl:.2f}{sl_note}\n"
            f"   حد الصفقات: {MAX_CONCURRENT_TRADES} | "
            f"حد خسارة يومي: -{MAX_DAILY_LOSS_USDT} USDT"
        )
    else:
        strategy_status = "🔴 معطلة"

    protect_status = (
        f"🟢 حماية تلقائية للصفقات اليدوية\n"
        f"   TP +{AUTO_PROTECT_TP_PCT}% | SL -{AUTO_PROTECT_SL_PCT}%\n"
        f"   فحص كل {AUTO_PROTECT_INTERVAL}s | أدنى {AUTO_PROTECT_MIN_NOTIONAL}$"
    ) if AUTO_PROTECT_ENABLED else "🔴 الحماية معطلة"

    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=(
                f"🤖 <b>بدأ بوت Binance</b>\n\n"
                f"<b>الاستراتيجية:</b>\n{strategy_status}\n\n"
                f"<b>الحماية:</b>\n{protect_status}\n\n"
                f"أرسل /help للأوامر."
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.error(f"startup send: {e}")


async def post_shutdown(app: Application):
    if binance_client:
        await binance_client.close_connection()
        log.info("Binance client مُغلق")


async def error_handler(update, context):
    err = context.error
    if isinstance(err, (NetworkError, TimedOut)):
        log.warning(f"⚠️ network: {err}")
        return
    log.error(f"❌ error: {err}", exc_info=err)


# ============================================================
# Main
# ============================================================
def main():
    threading.Thread(target=_run_health, daemon=True).start()
    print("🚀 Binance Bot يبدأ...")

    app = Application.builder() \
        .token(TELEGRAM_TOKEN) \
        .post_init(post_init) \
        .post_shutdown(post_shutdown) \
        .build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("strategy", cmd_strategy))
    app.add_handler(CommandHandler("protect", cmd_protect))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_error_handler(error_handler)

    if app.job_queue:
        app.job_queue.run_repeating(
            hourly_job,
            interval=HOURLY_MIN * 60,
            first=300,
            name="hourly",
        )

        if STRATEGY_ENABLED:
            app.job_queue.run_repeating(
                strategy_job,
                interval=STRATEGY_JOB_INTERVAL,
                first=15,
                name="strategy",
            )
            print(
                f"📈 استراتيجية EMA: {len(STRATEGY_SYMBOLS)} رموز "
                f"| {STRATEGY_INTERVAL} | EMA {EMA_FAST}/{EMA_SLOW} "
                f"| فحص فعلي مرة لكل شمعة"
            )

        if AUTO_PROTECT_ENABLED:
            app.job_queue.run_repeating(
                auto_protect_job,
                interval=AUTO_PROTECT_INTERVAL,
                first=20,
                name="auto_protect",
            )
            print(
                f"🛡️ حماية تلقائية: TP +{AUTO_PROTECT_TP_PCT}% / "
                f"SL -{AUTO_PROTECT_SL_PCT}% كل {AUTO_PROTECT_INTERVAL}s"
            )

        print(f"⏰ تقرير كل {HOURLY_MIN} دقيقة")

    print("✅ Bot جاهز")

    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        bootstrap_retries=5,
        read_timeout=30,
        write_timeout=30,
        connect_timeout=30,
        pool_timeout=30,
    )


if __name__ == "__main__":
    main()
