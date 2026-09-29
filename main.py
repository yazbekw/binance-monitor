import os
import sys
import time
import asyncio
import logging
from datetime import datetime, timezone

from binance import AsyncClient, BinanceSocketManager
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
)
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# التحقق من متغيرات البيئة
# ============================================================
REQUIRED_ENV = [
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "TELEGRAM_TOKEN",
    "TELEGRAM_CHAT_ID",
]
missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    print(f"❌ متغيرات ناقصة: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

API_KEY = os.environ["BINANCE_API_KEY"]
API_SECRET = os.environ["BINANCE_API_SECRET"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# ============================================================
# حالة عامة
# ============================================================
binance_client: AsyncClient | None = None
app: Application | None = None
notifications_enabled = True

HELP_TEXT = (
    "🤖 <b>بوت مراقبة Binance</b>\n\n"
    "<b>الأوامر المتاحة:</b>\n"
    "/positions — الصفقات المفتوحة\n"
    "/balance — الرصيد والهامش\n"
    "/pnl — الربح/الخسارة\n"
    "/orders — الأوامر المعلقة\n"
    "/status — حالة البوت\n"
    "/mute — إيقاف الإشعارات الفورية\n"
    "/unmute — تشغيل الإشعارات\n"
    "/help — هذه القائمة"
)


# ============================================================
# أدوات مساعدة
# ============================================================
def authorized(update: Update) -> bool:
    """يسمح فقط لصاحب الـ CHAT_ID بإرسال الأوامر."""
    return update.effective_chat and update.effective_chat.id == CHAT_ID


async def send(text: str):
    """إرسال رسالة للقناة الأساسية (تُستخدم من المهام الخلفية)."""
    if not notifications_enabled or app is None:
        return
    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.error(f"Telegram send error: {e}")


def fmt_position(p) -> str | None:
    amt = float(p["positionAmt"])
    if amt == 0:
        return None
    side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
    entry = float(p["entryPrice"])
    mark = float(p["markPrice"])
    pnl = float(p["unRealizedProfit"])
    lev = p.get("leverage", "?")
    icon = "📈" if pnl >= 0 else "📉"
    return (
        f"{side} | <b>{p['symbol']}</b>\n"
        f"  الحجم: {abs(amt)}\n"
        f"  الدخول: {entry}\n"
        f"  الحالي: {mark}\n"
        f"  الرافعة: x{lev}\n"
        f"  {icon} PnL: <b>{pnl:+.4f} USDT</b>"
    )


async def get_open_positions() -> tuple[list, dict]:
    """يرجع (قائمة الصفقات المفتوحة، الحساب كامل)."""
    account = await binance_client.futures_account()
    positions = [p for p in account["positions"] if float(p["positionAmt"]) != 0]
    return positions, account


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
        positions, account = await get_open_positions()
        if not positions:
            await update.message.reply_text("لا توجد صفقات مفتوحة حالياً. ✨")
            return
        body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
        total_pnl = float(account["totalUnrealizedProfit"])
        await update.message.reply_text(
            f"📊 <b>الصفقات المفتوحة ({len(positions)})</b>\n\n"
            f"{body}\n\n"
            f"──────────────\n"
            f"📈 إجمالي PnL: <b>{total_pnl:+.4f} USDT</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("cmd_positions")
        await update.message.reply_text(f"❌ خطأ: <code>{e}</code>",
                                        parse_mode=ParseMode.HTML)


async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        account = await binance_client.futures_account()
        wallet = float(account["totalWalletBalance"])
        unrealized = float(account["totalUnrealizedProfit"])
        margin_bal = float(account["totalMarginBalance"])
        available = float(account["availableBalance"])
        used_margin = float(account["totalPositionInitialMargin"])
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
        await update.message.reply_text(f"❌ خطأ: <code>{e}</code>",
                                        parse_mode=ParseMode.HTML)


async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        account = await binance_client.futures_account()
        positions = [p for p in account["positions"] if float(p["positionAmt"]) != 0]
        unrealized = float(account["totalUnrealizedProfit"])

        # PnL محقق آخر 24 ساعة من income history
        start_ms = int((time.time() - 86400) * 1000)
        try:
            income = await binance_client.futures_income_history(
                startTime=start_ms, incomeType="REALIZED_PNL", limit=1000
            )
            realized_24h = sum(float(i["income"]) for i in income)
        except Exception:
            realized_24h = 0.0

        # تفصيل PnL لكل صفقة
        lines = []
        for p in positions:
            pnl = float(p["unRealizedProfit"])
            icon = "📈" if pnl >= 0 else "📉"
            lines.append(f"{icon} <b>{p['symbol']}</b>: {pnl:+.4f} USDT")
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
        await update.message.reply_text(f"❌ خطأ: <code>{e}</code>",
                                        parse_mode=ParseMode.HTML)


async def cmd_orders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        orders = await binance_client.futures_get_open_orders()
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
        await update.message.reply_text(f"❌ خطأ: <code>{e}</code>",
                                        parse_mode=ParseMode.HTML)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    status = "🟢 متصل" if binance_client else "🔴 غير متصل"
    notif = "🔔 مفعلة" if notifications_enabled else "🔕 مكتومة"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    await update.message.reply_text(
        f"🤖 <b>حالة البوت</b>\n\n"
        f"Binance: {status}\n"
        f"الإشعارات: {notif}\n"
        f"الوقت: {ts}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_mute(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global notifications_enabled
    if not authorized(update):
        return
    notifications_enabled = False
    await update.message.reply_text(
        "🔕 تم إيقاف الإشعارات الفورية.\n"
        "الأوامر (/positions, /balance ...) لا تزال تعمل."
    )


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
    """تقرير كل ساعة. ينتظر 5 دقائق قبل أول تقرير لتجنب الضغط عند الإقلاع."""
    await asyncio.sleep(300)
    while True:
        try:
            positions, account = await get_open_positions()
            balance = float(account["totalWalletBalance"])
            unrealized = float(account["totalUnrealizedProfit"])
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            if not positions:
                msg = (
                    f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n"
                    f"لا توجد صفقات مفتوحة.\n"
                    f"💰 الرصيد: {balance:.2f} USDT"
                )
            else:
                body = "\n\n".join(filter(None, (fmt_position(p) for p in positions)))
                msg = (
                    f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n{body}\n\n"
                    f"──────────────\n"
                    f"💰 الرصيد: {balance:.2f} USDT\n"
                    f"📊 PnL: {unrealized:+.4f} USDT\n"
                    f"🔢 العدد: {len(positions)}"
                )
            await send(msg)
            log.info("✅ Hourly report sent")
        except Exception as e:
            log.exception(f"hourly_report: {e}")

        await asyncio.sleep(3600)


async def user_stream():
    """WebSocket: إشعارات فورية عند فتح/إغلاق/تعديل الصفقات."""
    bsm = BinanceSocketManager(binance_client)
    while True:
        try:
            async with bsm.futures_user_socket() as stream:
                log.info("🔌 WebSocket متصل بـ Binance")
                await send("🔌 تم الاتصال بـ Binance WebSocket")

                while True:
                    msg = await stream.recv()

                    # --- تحديث الحساب (رصيد، فتح صفقة) ---
                    if msg.get("e") == "ACCOUNT_UPDATE":
                        for pos in msg["a"]["P"]:
                            amt = float(pos["pa"])
                            # bc = balance change. إذا 0 → لم يُغلق شيء (فتح أو تعديل)
                            if amt != 0 and pos.get("bc", "0") == "0":
                                side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
                                await send(
                                    f"🚀 <b>صفقة مفتوحة</b>\n\n"
                                    f"الاتجاه: {side}\n"
                                    f"الرمز: <b>{pos['s']}</b>\n"
                                    f"الحجم: {abs(amt)}\n"
                                    f"الدخول: {pos['ep']}"
                                )

                    # --- تحديث الأوامر (تنفيذ/إغلاق) ---
                    elif msg.get("e") == "ORDER_TRADE_UPDATE":
                        o = msg["o"]
                        if o["X"] == "FILLED":
                            rp_val = float(o.get("rp", "0"))
                            pnl_txt = (
                                f"\n💰 PnL محقق: <b>{rp_val:+.4f} USDT</b>"
                                if rp_val != 0 else ""
                            )
                            await send(
                                f"⚡ <b>تنفيذ أمر</b>\n"
                                f"الرمز: <b>{o['s']}</b>\n"
                                f"الاتجاه: {o['S']}\n"
                                f"الكمية: {o['q']}\n"
                                f"متوسط السعر: {o.get('ap') or '0'}{pnl_txt}"
                            )
        except Exception as e:
            log.exception(f"user_stream error: {e}")
            await send("⚠️ انقطع WebSocket، إعادة المحاولة خلال 10 ثوان...")
            await asyncio.sleep(10)


# ============================================================
# نقطة البداية
# ============================================================
async def main():
    global binance_client, app

    # 1) عميل Binance
    binance_client = await AsyncClient.create(API_KEY, API_SECRET)

    # 2) تطبيق Telegram
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

    # تشغيل دورة حياة PTB يدوياً داخل asyncio الحالي
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    await send("🤖 <b>بدأ البوت</b>\nأرسل /help لعرض الأوامر.")

    try:
        # المهام الخلفية تعمل بالتوازي
        await asyncio.gather(
            user_stream(),
            hourly_report(),
        )
    finally:
        log.info("Shutting down...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await binance_client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
