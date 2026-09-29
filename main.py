import os
import sys
import asyncio
import logging
from datetime import datetime

from binance import AsyncClient, BinanceSocketManager
from telegram import Bot
from dotenv import load_dotenv

load_dotenv()

REQUIRED_ENV = ["BINANCE_API_KEY", "BINANCE_API_SECRET",
                "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]
missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    print(f"❌ متغيرات ناقصة: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

API_KEY = os.environ["BINANCE_API_KEY"]
API_SECRET = os.environ["BINANCE_API_SECRET"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)
bot = Bot(token=TELEGRAM_TOKEN)


async def send(text: str):
    try:
        await bot.send_message(
            chat_id=CHAT_ID, text=text,
            parse_mode="HTML", disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"Telegram error: {e}")


def fmt_position(p):
    amt = float(p["positionAmt"])
    if amt == 0:
        return None
    side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
    entry = float(p["entryPrice"])
    mark = float(p["markPrice"])
    pnl = float(p["unRealizedProfit"])
    lev = p.get("leverage", "?")
    pnl_icon = "📈" if pnl >= 0 else "📉"
    return (
        f"{side} | <b>{p['symbol']}</b>\n"
        f"  الحجم: {abs(amt)}\n"
        f"  الدخول: {entry}\n"
        f"  الحالي: {mark}\n"
        f"  الرافعة: x{lev}\n"
        f"  {pnl_icon} PnL: <b>{pnl:+.4f} USDT</b>"
    )


# ==========================================
# 1) تقرير كل ساعة (طلب واحد فقط في الساعة)
# ==========================================
async def hourly_report(client: AsyncClient):
    # انتظار 5 دقائق قبل أول تقرير لتجنب الضغط عند الإقلاع
    await asyncio.sleep(300)
    while True:
        try:
            account = await client.futures_account()
            positions = [p for p in account["positions"]
                         if float(p["positionAmt"]) != 0]
            balance = float(account["totalWalletBalance"])
            unrealized = float(account["totalUnrealizedProfit"])
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

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


# ==========================================
# 2) WebSocket stream — إشعارات فورية بدون طلبات
# ==========================================
async def user_stream(client: AsyncClient):
    """
    يستخدم userDataStream من Binance: يعطينا تحديثات الحساب والأوامر
    فورياً بدون أي طلبات HTTP إضافية.
    """
    bsm = BinanceSocketManager(client)

    while True:
        try:
            async with bsm.futures_user_socket() as stream:
                log.info("🔌 WebSocket متصل بـ Binance")
                await send("🔌 تم الاتصال بـ Binance WebSocket")

                while True:
                    msg = await stream.recv()

                    # ---- تحديث الحساب (رصيد، PnL) ----
                    if msg.get("e") == "ACCOUNT_UPDATE":
                        for pos in msg["a"]["P"]:
                            sym = pos["s"]
                            amt = float(pos["pa"])
                            entry = pos["ep"]

                            # فتح صفقة جديدة (من 0 إلى قيمة)
                            if amt != 0 and pos.get("bc", "0") == "0":
                                side = "🟢 LONG" if amt > 0 else "🔴 SHORT"
                                await send(
                                    f"🚀 <b>صفقة مفتوحة</b>\n\n"
                                    f"الاتجاه: {side}\n"
                                    f"الرمز: <b>{sym}</b>\n"
                                    f"الحجم: {abs(amt)}\n"
                                    f"الدخول: {entry}"
                                )

                    # ---- تحديث الأوامر (فتح/إغلاق/تنفيذ) ----
                    elif msg.get("e") == "ORDER_TRADE_UPDATE":
                        o = msg["o"]
                        status = o["X"]
                        sym = o["s"]
                        side = o["S"]
                        qty = o["q"]
                        avg = o.get("ap") or "0"
                        rp = o.get("rp", "0")  # realized pnl

                        if status == "FILLED":
                            rp_val = float(rp)
                            pnl_txt = f"\n💰 PnL محقق: <b>{rp_val:+.4f} USDT</b>" \
                                      if rp_val != 0 else ""
                            await send(
                                f"⚡ <b>تنفيذ أمر</b>\n"
                                f"الرمز: <b>{sym}</b>\n"
                                f"الاتجاه: {side}\n"
                                f"الكمية: {qty}\n"
                                f"متوسط السعر: {avg}{pnl_txt}"
                            )

        except Exception as e:
            log.exception(f"user_stream error: {e}")
            await send("⚠️ انقطع الاتصال بـ WebSocket، إعادة المحاولة...")
            await asyncio.sleep(10)  # تأخير قبل إعادة الاتصال


# ==========================================
# نقطة البداية
# ==========================================
async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET)

    await send("🤖 <b>بدأ البوت</b>\nسأتصل بـ WebSocket وأرسل تقرير كل ساعة.")

    try:
        await asyncio.gather(
            user_stream(client),
            hourly_report(client),
        )
    finally:
        await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
