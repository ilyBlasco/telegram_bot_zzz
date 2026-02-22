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
BANNER_URL = os.getenv("BANNER_URL", "").strip()

NOTIFY_DELETE_SECONDS = int(os.getenv("NOTIFY_DELETE_SECONDS", "10"))

AWAITING_CUSTOM_AMOUNT: set[int] = set()

MAX_USERS = 2


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
        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            session_id INTEGER NOT NULL,
            released_total_cents INTEGER NOT NULL,
            fee_cents INTEGER NOT NULL,
            network_fee_cents INTEGER NOT NULL,
            net_cents INTEGER NOT NULL,
            released_by INTEGER NOT NULL,
            released_at TEXT NOT NULL
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
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY
        )
        """)


def get_registered_users() -> set[int]:
    with db() as conn:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
        return {r["user_id"] for r in rows}


def register_user(user_id: int):
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO users(user_id) VALUES (?)", (user_id,))


def is_allowed(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    return user.id in get_registered_users()


def money_to_cents(amount_str: str) -> int:
    amt = Decimal(amount_str.strip())
    cents = (amt * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    if cents < 0:
        raise ValueError
    return int(cents)


def cents_to_money_str(cents: int) -> str:
    return f"{Decimal(cents) / Decimal(100):.2f}"


def compute_fee_net(total_cents: int):
    total = Decimal(total_cents) / 100
    fee = (total * FEE_PCT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    network_fee = NETWORK_FEE.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net = (total - fee - network_fee).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    fee_cents = int(fee * 100)
    network_fee_cents = int(network_fee * 100)
    net_cents = max(int(net * 100), 0)

    return fee_cents, network_fee_cents, net_cents


def get_state(chat_id: int):
    with db() as conn:
        row = conn.execute("SELECT * FROM state WHERE chat_id = ?", (chat_id,)).fetchone()
        if not row:
            conn.execute("INSERT INTO state(chat_id) VALUES (?)", (chat_id,))
            return get_state(chat_id)
        return dict(row)


def set_total(chat_id: int, total: int):
    with db() as conn:
        conn.execute("UPDATE state SET total_cents=? WHERE chat_id=?", (total, chat_id))


def set_session(chat_id: int, session: int):
    with db() as conn:
        conn.execute("UPDATE state SET session_id=? WHERE chat_id=?", (session, chat_id))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


async def notify(context, text):
    for uid in get_registered_users():
        try:
            msg = await context.bot.send_message(uid, text, parse_mode=ParseMode.HTML)
            context.application.create_task(delete_later(context, uid, msg.message_id))
        except:
            pass


async def delete_later(context, chat_id, msg_id):
    await asyncio.sleep(NOTIFY_DELETE_SECONDS)
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except:
        pass


def build_panel_text(total_cents: int):
    fee, net_fee, net = compute_fee_net(total_cents)
    return (
        "光 ═════════════ 光\n\n"
        f"💰 <b>TOTAL</b> :: <code>${cents_to_money_str(total_cents)}</code>\n"
        f"<b>費 Fee</b> ({(FEE_PCT*100):.0f}%) :: <code>${cents_to_money_str(fee)}</code>\n"
        f"<b>費 Network fee</b> :: <code>${cents_to_money_str(net_fee)}</code>\n"
        f"💵 <b>NET</b> :: <code>${cents_to_money_str(net)}</code>\n\n"
        "─── ( ZELLE CAPTURE ONLY ) ───\n\n"
        f"<i>⏳ Los mensajes desaparecen en {NOTIFY_DELETE_SECONDS}s</i>"
    )


def build_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("+10", callback_data="add:10"),
         InlineKeyboardButton("+50", callback_data="add:50"),
         InlineKeyboardButton("+100", callback_data="add:100")],
        [InlineKeyboardButton("✍ Custom", callback_data="custom")],
        [InlineKeyboardButton("📜 History", callback_data="history"),
         InlineKeyboardButton("⏪ Control + Z", callback_data="undo")],
        [InlineKeyboardButton("解Release除", callback_data="release")]
    ])


async def start(update: Update, context):
    user = update.effective_user
    users = get_registered_users()

    if user.id not in users:
        if len(users) >= MAX_USERS:
            await update.message.reply_text("Tracker already at max capacity.")
            return
        register_user(user.id)

    await send_panel(update, context)


async def send_panel(update, context):
    chat_id = update.effective_chat.id
    state = get_state(chat_id)
    text = build_panel_text(state["total_cents"])
    await update.effective_chat.send_message(text, reply_markup=build_keyboard(), parse_mode=ParseMode.HTML)


async def on_button(update: Update, context):
    if not is_allowed(update):
        return

    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    state = get_state(chat_id)
    total = state["total_cents"]
    session = state["session_id"]
    data = query.data

    if data.startswith("add:"):
        amount = int(data.split(":")[1]) * 100
        total += amount
        set_total(chat_id, total)

        await notify(context,
                     f"<b>Yozu Tracker</b>\nSe agregó: <code>${cents_to_money_str(amount)}</code>\nTotal: <code>${cents_to_money_str(total)}</code>")

        await query.message.edit_text(build_panel_text(total),
                                      reply_markup=build_keyboard(),
                                      parse_mode=ParseMode.HTML)

    elif data == "release":
        if total <= 0:
            await notify(context,
                         "La cantidad que intentas retirar es <b>$0.00</b>, tu IQ también es 0 coincidentemente.")
            return

        fee, net_fee, net = compute_fee_net(total)

        await notify(context,
                     f"<b>解Release除</b>\nTotal: <code>${cents_to_money_str(total)}</code>\n"
                     f"Fee: <code>${cents_to_money_str(fee)}</code>\n"
                     f"Network fee: <code>${cents_to_money_str(net_fee)}</code>\n"
                     f"Net: <code>${cents_to_money_str(net)}</code>")

        set_total(chat_id, 0)
        set_session(chat_id, session + 1)

        await query.message.edit_text(build_panel_text(0),
                                      reply_markup=build_keyboard(),
                                      parse_mode=ParseMode.HTML)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))

    app.run_polling()


if __name__ == "__main__":
    main()