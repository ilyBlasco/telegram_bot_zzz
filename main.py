import os
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
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
FEE_PCT = Decimal(os.getenv("FEE_PCT", "0.02"))  # 2% default
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "6448246938")  # comma-separated

# Per-user "waiting for custom amount" state (in-memory is fine for a single worker)
AWAITING_CUSTOM_AMOUNT = set()


def parse_allowed_ids() -> set[int]:
    ids = set()
    for part in ALLOWED_USER_IDS.split(","):
        part = part.strip()
        if part:
            try:
                ids.add(int(part))
            except ValueError:
                pass
    return ids


ALLOWED_IDS = parse_allowed_ids()


def is_allowed(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in ALLOWED_IDS)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                chat_id INTEGER PRIMARY KEY,
                total_cents INTEGER NOT NULL DEFAULT 0,
                panel_message_id INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS releases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                released_total_cents INTEGER NOT NULL,
                fee_cents INTEGER NOT NULL,
                net_cents INTEGER NOT NULL,
                released_by INTEGER NOT NULL,
                released_at TEXT NOT NULL
            )
            """
        )


def money_to_cents(amount_str: str) -> int:
    """
    Accepts "420", "420.5", "420.50" and returns cents as int.
    """
    amt = Decimal(amount_str.strip())
    cents = (amt * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    if cents < 0:
        raise ValueError("Negative amount not allowed")
    return int(cents)


def cents_to_money_str(cents: int) -> str:
    amt = Decimal(cents) / Decimal(100)
    return f"{amt:.2f}"


def compute_fee_net(total_cents: int) -> tuple[int, int]:
    total = Decimal(total_cents) / Decimal(100)
    fee = (total * FEE_PCT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net = (total - fee).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int((fee * 100).to_integral_value(rounding=ROUND_HALF_UP)), int(
        (net * 100).to_integral_value(rounding=ROUND_HALF_UP)
    )


def get_state(chat_id: int) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM state WHERE chat_id = ?", (chat_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO state(chat_id, total_cents, panel_message_id) VALUES (?, 0, NULL)",
                (chat_id,),
            )
            return {"chat_id": chat_id, "total_cents": 0, "panel_message_id": None}
        return dict(row)


def set_total(chat_id: int, total_cents: int):
    with db() as conn:
        conn.execute(
            "UPDATE state SET total_cents = ? WHERE chat_id = ?",
            (total_cents, chat_id),
        )


def set_panel_message_id(chat_id: int, message_id: int):
    with db() as conn:
        conn.execute(
            "UPDATE state SET panel_message_id = ? WHERE chat_id = ?",
            (message_id, chat_id),
        )


def build_panel_text(total_cents: int) -> str:
    fee_cents, net_cents = compute_fee_net(total_cents)
    return (
        f"<b>Running Total</b>: ${cents_to_money_str(total_cents)}\n"
        f"<b>Fee</b> ({(FEE_PCT * 100):.0f}%): ${cents_to_money_str(fee_cents)}\n"
        f"<b>Net (USDT)</b>: ${cents_to_money_str(net_cents)}\n\n"
        f"<i>Use buttons below to add amounts or release.</i>"
    )


def build_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("+10", callback_data="add:10"),
                InlineKeyboardButton("+50", callback_data="add:50"),
                InlineKeyboardButton("+100", callback_data="add:100"),
            ],
            [
                InlineKeyboardButton("+200", callback_data="add:200"),
                InlineKeyboardButton("+500", callback_data="add:500"),
                InlineKeyboardButton("Custom", callback_data="custom"),
            ],
            [
                InlineKeyboardButton("Release", callback_data="release"),
                InlineKeyboardButton("History", callback_data="history"),
            ],
        ]
    )


def build_history_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]])


async def send_or_update_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    total_cents = st["total_cents"]
    text = build_panel_text(total_cents)
    kb = build_panel_keyboard()

    # If we have a stored panel message, try to edit it; otherwise send a new one.
    panel_message_id = st.get("panel_message_id")

    if panel_message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=panel_message_id,
                text=text,
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception:
            # If editing fails (message deleted, etc.), fall back to sending a new panel.
            pass

    msg = await update.effective_chat.send_message(
        text=text,
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )
    set_panel_message_id(chat_id, msg.message_id)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await send_or_update_panel(update, context)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    total_cents = st["total_cents"]

    data = query.data or ""

    if data.startswith("add:"):
        add_amount = data.split(":", 1)[1]
        add_cents = money_to_cents(add_amount)
        total_cents += add_cents
        set_total(chat_id, total_cents)

        # Update panel in-place using the same message
        await query.edit_message_text(
            text=build_panel_text(total_cents),
            reply_markup=build_panel_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        set_panel_message_id(chat_id, query.message.message_id)
        return

    if data == "custom":
        AWAITING_CUSTOM_AMOUNT.add(update.effective_user.id)
        await query.edit_message_text(
            text=(
                "<b>Custom amount</b>\n"
                "Send a number like <code>420</code> or <code>420.50</code>.\n\n"
                "Then I will add it to the total."
            ),
            reply_markup=build_history_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "release":
        # Log release, reset to 0
        if total_cents <= 0:
            await query.edit_message_text(
                text=build_panel_text(0),
                reply_markup=build_panel_keyboard(),
                parse_mode=ParseMode.HTML,
            )
            set_total(chat_id, 0)
            set_panel_message_id(chat_id, query.message.message_id)
            return

        fee_cents, net_cents = compute_fee_net(total_cents)
        released_at = datetime.now(timezone.utc).isoformat()
        released_by = update.effective_user.id

        with db() as conn:
            conn.execute(
                """
                INSERT INTO releases(chat_id, released_total_cents, fee_cents, net_cents, released_by, released_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (chat_id, total_cents, fee_cents, net_cents, released_by, released_at),
            )

        set_total(chat_id, 0)

        await query.edit_message_text(
            text=(
                "<b>Released</b>\n"
                f"Total: ${cents_to_money_str(total_cents)}\n"
                f"Fee ({(FEE_PCT*100):.0f}%): ${cents_to_money_str(fee_cents)}\n"
                f"Net: ${cents_to_money_str(net_cents)}\n\n"
                "Total has been reset to <b>$0.00</b>."
            ),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to panel", callback_data="back")]]),
            parse_mode=ParseMode.HTML,
        )
        set_panel_message_id(chat_id, query.message.message_id)
        return

    if data == "history":
        with db() as conn:
            rows = conn.execute(
                """
                SELECT released_total_cents, fee_cents, net_cents, released_by, released_at
                FROM releases
                WHERE chat_id = ?
                ORDER BY id DESC
                LIMIT 10
                """,
                (chat_id,),
            ).fetchall()

        if not rows:
            hist_text = "<b>History</b>\nNo releases yet."
        else:
            lines = ["<b>History (last 10)</b>"]
            for r in rows:
                ts = r["released_at"].replace("T", " ").split(".")[0].replace("+00:00", " UTC")
                lines.append(
                    f"• ${cents_to_money_str(r['released_total_cents'])} "
                    f"(fee ${cents_to_money_str(r['fee_cents'])}, net ${cents_to_money_str(r['net_cents'])}) "
                    f"- {ts}"
                )
            hist_text = "\n".join(lines)

        await query.edit_message_text(
            text=hist_text,
            reply_markup=build_history_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    if data in ("back",):
        # Return to panel
        st = get_state(chat_id)
        total_cents = st["total_cents"]
        await query.edit_message_text(
            text=build_panel_text(total_cents),
            reply_markup=build_panel_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        set_panel_message_id(chat_id, query.message.message_id)
        return


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if user_id not in AWAITING_CUSTOM_AMOUNT:
        return

    text = (update.message.text or "").strip()
    try:
        add_cents = money_to_cents(text)
    except Exception:
        await update.message.reply_text("Invalid number. Send something like 420 or 420.50.")
        return

    AWAITING_CUSTOM_AMOUNT.discard(user_id)

    st = get_state(chat_id)
    total_cents = st["total_cents"] + add_cents
    set_total(chat_id, total_cents)

    # Show panel again
    await send_or_update_panel(update, context)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN env var")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
