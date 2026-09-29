import os
import asyncio
import logging
from datetime import datetime

from binance import AsyncClient, BinanceSocketManager
from telegram import Bot
from dotenv import load_dotenv

load_dotenv()

# ====== الإعدادات ======
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_TOKEN)

# تتبع الصفقات المفتوحة سابقاً
known_positions = {}


# ====== تنسيق الرسائل ======
def fmt_position(p):
    symbol = p["symbol"]
    side = "🟢 LONG" if float(p["positionAmt"]) > 0 else "🔴 SHORT"
    amt = abs(float(p["positionAmt"]))
    entry = float(p["entryPrice"])
    mark = float(p["markPrice"])
    pnl = float(p["unRealizedProfit"])
    lev = p.get("leverage", "?")
    pnl_icon = "📈" if pnl >= 0 else "📉"

    return (
        f"{side} | <b>{symbol}</b>\n"
        f"  الحجم: {amt}\n"
        f"  الدخول: {entry}\n"
        f"  السعر الحالي: {mark}\n"
        f"  الرافعة: x{lev}\n"
        f"  {pnl_icon} PnL: <b>{pnl:.4f} USDT</b>"
    )


async def send(text):
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        log.error(f"Telegram error: {e}")


# ====== جلب الصفقات المفتوحة ======
async def get_open_positions(client):
    account = await client.futures_account()
    positions = account.get("positions", [])
    open_pos = [p for p in positions if float(p["positionAmt"]) != 0]
    return open_pos, account


# ====== تقرير كل ساعة ======
async def hourly_report(client):
    while True:
        try:
            positions, account = await get_open_positions(client)
            balance = float(account["totalWalletBalance"])
            unrealized = float(account["totalUnrealizedProfit"])
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

            if not positions:
                msg = (
                    f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n"
                    f"لا توجد صفقات مفتوحة حالياً.\n"
                    f"💰 الرصيد: {balance:.2f} USDT"
                )
            else:
                body = "\n\n".join(fmt_position(p) for p in positions)
                msg = (
                    f"⏰ <b>تقرير كل ساعة</b> | {ts}\n\n"
                    f"{body}\n\n"
                    f"──────────────\n"
                    f"💰 الرصيد: {balance:.2f} USDT\n"
                    f"📊 إجمالي PnL: {unrealized:+.4f} USDT\n"
                    f"🔢 عدد الصفقات: {len(positions)}"
                )
            await send(msg)
            log.info("Hourly report sent.")
        except Exception as e:
            log.exception(f"hourly_report error: {e}")

        await asyncio.sleep(3600)


# ====== مراقبة فتح/إغلاق الصفقات ======
async def watch_positions(client):
    global known_positions

    # تهيئة الحالة الأولية
    positions, _ = await get_open_positions(client)
    for p in positions:
        known_positions[p["symbol"]] = float(p["positionAmt"])

    log.info(f"Initial positions tracked: {list(known_positions.keys())}")

    while True:
        try:
            positions, _ = await get_open_positions(client)
            current = {p["symbol"]: p for p in positions}
            current_syms = set(current.keys())
            known_syms = set(known_positions.keys())

            # ===== صفقات جديدة أو تعديل اتجاه =====
            for sym in current_syms:
                new_amt = float(current[sym]["positionAmt"])
                old_amt = known_positions.get(sym, 0.0)

                if sym not in known_positions or old_amt == 0:
                    side = "LONG 🟢" if new_amt > 0 else "SHORT 🔴"
                    p = current[sym]
                    await send(
                        f"🚀 <b>صفقة جديدة مفتوحة</b>\n\n"
                        f"الاتجاه: {side}\n"
                        f"الرمز: <b>{sym}</b>\n"
                        f"الحجم: {abs(new_amt)}\n"
                        f"سعر الدخول: {p['entryPrice']}\n"
                        f"الرافعة: x{p.get('leverage', '?')}"
                    )
                # تعديل كمية (زيادة/تقليل جزئي)
                elif abs(new_amt) != abs(old_amt):
                    action = "زيادة 📈" if abs(new_amt) > abs(old_amt) else "تقليل 📉"
                    await send(
                        f"✏️ <b>تعديل صفقة</b> ({action})\n"
                        f"الرمز: <b>{sym}</b>\n"
                        f"من {abs(old_amt)} → {abs(new_amt)}"
                    )

            # ===== صفقات مغلقة =====
            for sym in known_syms - current_syms:
                await send(
                    f"✅ <b>تم إغلاق صفقة</b>\n"
                    f"الرمز: <b>{sym}</b>"
                )

            # تحديث الحالة
            known_positions = {s: float(current[s]["positionAmt"]) for s in current_syms}

        except Exception as e:
            log.exception(f"watch_positions error: {e}")

        await asyncio.sleep(30)  # فحص كل 30 ثانية


# ====== نقطة البداية ======
async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET)

    await send("🤖 <b>بدأ بوت مراقبة Binance</b>\nسأرسل تقرير كل ساعة + إشعارات فورية.")

    try:
        await asyncio.gather(
            hourly_report(client),
            watch_positions(client),
        )
    finally:
        await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
