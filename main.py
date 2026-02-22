import os
import sqlite3
import asyncio
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

DB_PATH = os.getenv("DB_PATH", "bot.db")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

FEE_PCT = Decimal(os.getenv("FEE_PCT", "0.02"))
NETWORK_FEE = Decimal(os.getenv("NETWORK_FEE", "0.30"))

ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "6448246938")
NOTIFY_USER_IDS = os.getenv("NOTIFY_USER_IDS", "6448246938")
BANNER_URL = os.getenv("BANNER_URL", "").strip()

# 🔥 TEST MODE: 10 seconds auto-delete
NOTIFY_DELETE_SECONDS = 10

AWAITING_CUSTOM_AMOUNT = set()


def parse_ids(csv: str) -> set[int]:
    ids = set()
    for part in (csv or "").split(","):
        part = part.strip()
        if part:
            try:
                ids.add(int(part))
            except ValueError:
                pass
    return ids


ALLOWED_IDS = parse_ids(ALLOWED_USER_IDS)
NOTIFY_IDS = parse_ids(NOTIFY_USER_IDS) or set(ALLOWED_IDS)


def is_allowed(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in ALLOWED_IDS)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS state (
                chat_id INTEGER PRIMARY KEY,
                total_cents INTEGER NOT NULL DEFAULT 0,
                panel_message_id INTEGER,
                session_id INTEGER NOT NULL DEFAULT 1,
                panel_mode TEXT NOT NULL DEFAULT 'text'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                session_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                total_after_cents INTEGER NOT NULL,
                actor_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)


def money_to_cents(amount_str: str) -> int:
    amt = Decimal(amount_str.strip())
    cents = (amt * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    if cents < 0:
        raise ValueError
    return int(cents)


def cents_to_money_str(cents: int) -> str:
    return f"{Decimal(cents)/Decimal(100):.2f}"


def compute_fee_net(total_cents: int):
    total = Decimal(total_cents) / 100
    fee = (total * FEE_PCT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    network = NETWORK_FEE
    net = (total - fee - network).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    if net < 0:
        net = Decimal("0.00")

    return (
        int(fee * 100),
        int(network * 100),
        int(net * 100),
    )


def get_state(chat_id: int):
    with db() as conn:
        row = conn.execute("SELECT * FROM state WHERE chat_id=?", (chat_id,)).fetchone()
        if not row:
            conn.execute("INSERT INTO state(chat_id) VALUES (?)", (chat_id,))
            return {"total_cents": 0, "panel_message_id": None, "session_id": 1, "panel_mode": "text"}
        return dict(row)


def set_total(chat_id, total):
    with db() as conn:
        conn.execute("UPDATE state SET total_cents=? WHERE chat_id=?", (total, chat_id))


def set_panel_message_id(chat_id, mid):
    with db() as conn:
        conn.execute("UPDATE state SET panel_message_id=? WHERE chat_id=?", (mid, chat_id))


def set_session(chat_id, sid):
    with db() as conn:
        conn.execute("UPDATE state SET session_id=? WHERE chat_id=?", (sid, chat_id))


def set_panel_mode(chat_id, mode):
    with db() as conn:
        conn.execute("UPDATE state SET panel_mode=? WHERE chat_id=?", (mode, chat_id))


def now():
    return datetime.now(timezone.utc).isoformat()


async def auto_delete(context, chat_id, message_id):
    await asyncio.sleep(NOTIFY_DELETE_SECONDS)
    try:
        await context.bot.delete_message(chat_id, message_id)
    except:
        pass


async def notify(context: ContextTypes.DEFAULT_TYPE, text: str):
    for uid in NOTIFY_IDS:
        try:
            msg = await context.bot.send_message(uid, text, parse_mode=ParseMode.HTML)
            context.application.create_task(auto_delete(context, uid, msg.message_id))
        except:
            pass


def build_panel_text(total_cents: int):
    fee, network, net = compute_fee_net(total_cents)
    return (
        "<b>🔁 Yozu Tracker</b>\n\n"
        f"<b>Total</b>: <code>${cents_to_money_str(total_cents)}</code>\n"
        f"<b>Fee</b> ({(FEE_PCT*100):.0f}%): <code>${cents_to_money_str(fee)}</code>\n"
        f"<b>Network fee</b>: <code>${cents_to_money_str(network)}</code>\n"
        f"<b>Net</b>: <code>${cents_to_money_str(net)}</code>\n\n"
        "<i>Usa los botones. Custom se borra automáticamente.</i>"
    )


def build_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("+10", callback_data="add:10"),
            InlineKeyboardButton("+50", callback_data="add:50"),
            InlineKeyboardButton("+100", callback_data="add:100"),
        ],
        [
            InlineKeyboardButton("+200", callback_data="add:200"),
            InlineKeyboardButton("+500", callback_data="add:500"),
            InlineKeyboardButton("📝 Custom", callback_data="custom"),
        ],
        [
            InlineKeyboardButton("🚀 Release", callback_data="release"),
            InlineKeyboardButton("📜 History", callback_data="history"),
        ],
    ])


async def send_panel(update: Update, context):
    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    text = build_panel_text(st["total_cents"])

    if BANNER_URL:
        msg = await update.effective_chat.send_photo(
            photo=BANNER_URL,
            caption=text,
            reply_markup=build_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        set_panel_mode(chat_id, "banner")
    else:
        msg = await update.effective_chat.send_message(
            text=text,
            reply_markup=build_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        set_panel_mode(chat_id, "text")

    set_panel_message_id(chat_id, msg.message_id)


async def start(update: Update, context):
    if not is_allowed(update):
        return
    await send_panel(update, context)


async def on_button(update: Update, context):
    if not is_allowed(update):
        return

    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    total = st["total_cents"]
    session = st["session_id"]

    if query.data.startswith("add:"):
        amount = money_to_cents(query.data.split(":")[1])
        total += amount
        set_total(chat_id, total)

        await notify(context,
            f"<b>🔁 Yozu Tracker</b>\nSe agregó: <code>${cents_to_money_str(amount)}</code>\nTotal: <code>${cents_to_money_str(total)}</code>"
        )

    if query.data == "release":
        fee, network, net = compute_fee_net(total)

        await notify(context,
            (
                "<b>🚀 Release</b>\n"
                f"Total: <code>${cents_to_money_str(total)}</code>\n"
                f"Fee: <code>${cents_to_money_str(fee)}</code>\n"
                f"Network fee: <code>${cents_to_money_str(network)}</code>\n"
                f"Net: <code>${cents_to_money_str(net)}</code>"
            )
        )

        total = 0
        set_total(chat_id, 0)
        set_session(chat_id, session + 1)

    await query.message.edit_caption(
        caption=build_panel_text(total),
        reply_markup=build_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def on_message(update: Update, context):
    if not is_allowed(update):
        return

    if update.effective_user.id not in AWAITING_CUSTOM_AMOUNT:
        return

    chat_id = update.effective_chat.id
    try:
        amount = money_to_cents(update.message.text)
    except:
        return

    await context.bot.delete_message(chat_id, update.message.message_id)

    st = get_state(chat_id)
    total = st["total_cents"] + amount
    set_total(chat_id, total)

    await notify(context,
        f"<b>🔁 Yozu Tracker</b>\nSe agregó: <code>${cents_to_money_str(amount)}</code>\nTotal: <code>${cents_to_money_str(total)}</code>"
    )


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN env var")

    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    app.run_polling()


if __name__ == "__main__":
    main()
