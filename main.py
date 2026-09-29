import os
import re
import sys
import time
import asyncio
import logging
from datetime import datetime, timezone

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# التحقق من متغيرات البيئة
# ============================================================
REQUIRED_ENV = ["BINANCE_API_KEY", "BINANCE_API_SECRET",
                "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]
missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    print(f"❌ متغيرات ناقصة: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

API_KEY = os.environ["BINANCE_API_KEY"]
API_SECRET = os.environ["BINANCE_API_SECRET"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)  # تقليل ضجيج Telegram

# ============================================================
# حالة عامة
# ============================================================
binance_client: AsyncClient | None = None
app: Application | None = None
notifications_enabled = True

# كاش HTTP + كشف الحظر
_http_cache: dict = {}          # key -> (timestamp, value)
_banned_until_ms: int = 0

HELP_TEXT = (
    "🤖 <b>بوت مراقبة Binance</b>\n\n"
    "<b>الأوامر:</b>\n"
    "/positions — الصفقات المفتوحة\n"
    "/balance — الرصيد والهامش\n"
    "/pnl — الربح/الخسارة\n"
    "/orders — الأوامر المعلقة\n"
    "/status — حالة البوت\n"
    "/mute — إيقاف الإشعارات\n"
    "/unmute — تشغيل الإشعارات\n"
    "/help — المساعدة"
)


# ============================================================
# أدوات مساعدة
# ============================================================
def authorized(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.id == CHAT_ID


def is_banned() -> bool:
    return _banned_until_ms > int(time.time() * 1000)


def ban_remaining_sec() -> int:
    if not is_banned():
        return 0
    return max(0, (_banned_until_ms - int(time.time() * 1000)) // 1000)


def _register_ban(exc: Exception):
    """يسجّل الحظر عند رؤية خطأ -1003 مع timestamp."""
    global _banned_until_ms
    msg = str(exc)
    m = re.search(r"banned until (\d+)", msg)
    if m:
        _banned_until_ms = int(m.group(1))
        log.warning(f"⛔ Binance IP banned until "
                    f"{datetime.fromtimestamp(_banned_until_ms/1000, timezone.utc)}")


async def cached_http(key: str, coro_factory, ttl: int = 20):
    """
    تنفيذ طلب HTTP مع:
      - كاش (TTL)
      - كشف الحظر (-1003)
      - رسالة واضحة للمستخدم
    """
    # 1) محظور حالياً؟
    if is_banned():
        remaining = ban_remaining_sec()
        raise RuntimeError(
            f"⛔ Binance حظر IP مؤقتاً.\n"
            f"المتبقي: ~{remaining//60} دقيقة\n"
            f"لن أرسل طلبات جديدة حتى ينتهي الحظر."
        )

    # 2) هل الكاش صالح؟
    now = time.time()
    hit = _http_cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]

    # 3) استدعاء HTTP
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


async def send(text: str):
    if not notifications_enabled or app is None:
        return
    try:
        await app.bot.send_message(chat_id=CHAT_ID, text=text,
                                   parse_mode=ParseMode.HTML)
    except Exception as e:
        log.error(f"Telegram send error: {e}")


# ============================================================
# جلب البيانات عبر HTTP (مع كاش)
# ============================================================
async def fetch_positions() -> list:
    """يستخدم /fapi/v2/positionRisk — يعيد markPrice و unRealizedProfit دائماً."""
    data = await cached_http(
        "positions",
        lambda: binance_client.futures_position_information(),
        ttl=20,
    )
    return [p for p in data if float(p["positionAmt"]) != 0]


async def fetch_account() -> dict:
    return await cached_http(
        "account",
        lambda: binance_client.futures_account(),
        ttl=20,
    )


async def fetch_open_orders() -> list:
    return await cached_http(
        "orders",
        lambda: binance_client.futures_get_open_orders(),
        ttl=20,
    )


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


# ============================================================
# أوامر Telegram
# ============================================================
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, ctx)


async def cmd_positions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        positions = await fetch_positions()
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


async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        account = await fetch_account()
        # حقول آمنة مع .get
        wallet = float(account.get("totalWalletBalance", 0) or 0)
        unrealized = float(account.get("totalUnrealizedProfit", 0) or 0)
        margin_bal = float(account.get("totalMarginBalance", 0) or 0)
        available = float(account.get("availableBalance", 0) or 0)
        used_margin = float(account.get("totalPositionInitialMargin", 0) or 0)

        await update.message.reply_text(
            f"💰 <b>الرصيد</b>\n\n"
            f"المحفظة: <b>{wallet:.2f}</b> USDT\n"
            f"PnL عائم: <b>{unrealized:+.4f}</b> USDT\n"
            f"رصيد الهامش: <b>{margin_bal:.2f}</b> USDT\n"
            f"هامش مستخدم: <b>{used_margin:.2f}</b> USDT\n"
            f"متاح للتداول: <b>{available:.2f}</b> USDT",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_balance")
        await update.message.reply_text(f"❌ {e}")


async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        positions = await fetch_positions()

        # PnL عائم
        unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)

        # PnL محقق آخر 24 ساعة — طلب مستقل خفيف
        realized_24h = 0.0
        try:
            start_ms = int((time.time() - 86400) * 1000)
            income = await cached_http(
                "income_24h",
                lambda: binance_client.futures_income_history(
                    startTime=start_ms, incomeType="REALIZED_PNL", limit=1000
                ),
                ttl=60,
            )
            realized_24h = sum(float(i["income"]) for i in income)
        except Exception as e:
            log.warning(f"income_history failed: {e}")

        # تفصيل
        lines = []
        for p in positions:
            pnl = float(p.get("unRealizedProfit", 0) or 0)
            icon = "📈" if pnl >= 0 else "📉"
            lines.append(f"{icon} <b>{p.get('symbol', '?')}</b>: {pnl:+.4f} USDT")
        breakdown = "\n".join(lines) if lines else "—"

        await update.message.reply_text(
            f"📊 <b>PnL</b>\n\n"
            f"عائم الآن: <b>{unrealized:+.4f}</b> USDT\n"
            f"محقق (24س): <b>{realized_24h:+.4f}</b> USDT\n\n"
            f"<b>تفصيل الصفقات:</b>\n{breakdown}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_pnl")
        await update.message.reply_text(f"❌ {e}")


async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        orders = await fetch_open_orders()
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


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    status = "🟢 متصل" if binance_client else "🔴 غير متصل"
    notif = "🔔 مفعلة" if notifications_enabled else "🔕 مكتومة"
    if is_banned():
        ban_info = f"\n⛔ محظور — متبقي ~{ban_remaining_sec()//60} دقيقة"
    else:
        ban_info = ""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    await update.message.reply_text(
        f"🤖 <b>حالة البوت</b>\n\n"
        f"Binance: {status}\n"
        f"الإشعارات: {notif}\n"
        f"الوقت: {ts}{ban_info}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_mute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = False
    await update.message.reply_text("🔕 تم إيقاف الإشعارات الفورية.")


async def cmd_unmute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = True
    await update.message.reply_text("🔔 تم تشغيل الإشعارات الفورية.")


# ============================================================
# المهام الخلفية
# ============================================================
async def hourly_report():
    # انتظر 10 دقائق قبل التقرير الأول (يعطي فرصة للحظر أن يزول / أول WS update)
    await asyncio.sleep(600)
    while True:
        try:
            if is_banned():
                log.warning("hourly_report: متخطى بسبب الحظر")
                await asyncio.sleep(3600)
                continue

            positions = await fetch_positions()
            try:
                account = await fetch_account()
                balance = float(account.get("totalWalletBalance", 0) or 0)
            except Exception:
                balance = 0.0

            unrealized = sum(float(p.get("unRealizedProfit", 0) or 0) for p in positions)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            if not positions:
                msg = (f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n"
                       f"لا توجد صفقات مفتوحة.\n"
                       f"💰 الرصيد: {balance:.2f} USDT")
            else:
                body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
                msg = (f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n{body}\n\n"
                       f"──────────────\n"
                       f"💰 الرصيد: {balance:.2f} USDT\n"
                       f"📊 PnL: {unrealized:+.4f} USDT\n"
                       f"🔢 العدد: {len(positions)}")
            await send(msg)
            log.info("✅ Hourly report sent")
        except Exception as e:
            log.exception(f"hourly_report: {e}")

        await asyncio.sleep(3600)


async def user_stream():
    """WebSocket: مصدر الإشعارات الفورية — لا يستهلك طلبات HTTP."""
    bsm = BinanceSocketManager(binance_client)
    while True:
        try:
            async with bsm.futures_user_socket() as stream:
                log.info("🔌 WebSocket متصل")
                await send("🔌 تم الاتصال بـ Binance WebSocket")

                while True:
                    msg = await stream.recv()

                    # فتح / تعديل صفقات
                    if msg.get("e") == "ACCOUNT_UPDATE":
                        for pos in msg["a"]["P"]:
                            amt = float(pos["pa"])
                            if amt != 0 and pos.get("bc", "0") == "0":
                                side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
                                await send(
                                    f"🚀 <b>صفقة مفتوحة</b>\n\n"
                                    f"الاتجاه: {side}\n"
                                    f"الرمز: <b>{pos['s']}</b>\n"
                                    f"الحجم: {abs(amt)}\n"
                                    f"الدخول: {pos['ep']}"
                                )

                    # تنفيذ أوامر
                    elif msg.get("e") == "ORDER_TRADE_UPDATE":
                        o = msg["o"]
                        if o["X"] == "FILLED":
                            rp_val = float(o.get("rp", "0") or 0)
                            pnl_txt = (f"\n💰 PnL محقق: <b>{rp_val:+.4f} USDT</b>"
                                       if rp_val != 0 else "")
                            await send(
                                f"⚡ <b>تنفيذ أمر</b>\n"
                                f"الرمز: <b>{o['s']}</b>\n"
                                f"الاتجاه: {o['S']}\n"
                                f"الكمية: {o['q']}\n"
                                f"متوسط السعر: {o.get('ap') or '0'}{pnl_txt}"
                            )
        except Exception as e:
            log.exception(f"user_stream error: {e}")
            await asyncio.sleep(10)


# ============================================================
# نقطة البداية
# ============================================================
async def main():
    global binance_client, app

    binance_client = await AsyncClient.create(API_KEY, API_SECRET)
    log.info("Binance client جاهز")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CommandHandler("orders", cmd_orders))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    log.info("Telegram polling بدأ")

    await send("🤖 <b>بدأ البوت</b>\nأرسل /help للأوامر.")

    try:
        await asyncio.gather(user_stream(), hourly_report())
    finally:
        log.info("Shutting down...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await binance_client.close_connection()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
