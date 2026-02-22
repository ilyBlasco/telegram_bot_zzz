import os
import sqlite3
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

FEE_PCT = Decimal(os.getenv("FEE_PCT", "0.02"))  # 2% default
NETWORK_FEE = Decimal(os.getenv("NETWORK_FEE", "0.30"))  # $0.30 flat
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "6448246938")  # comma-separated
NOTIFY_USER_IDS = os.getenv("NOTIFY_USER_IDS", "6448246938")  # comma-separated
BANNER_URL = os.getenv("BANNER_URL", "").strip()  # optional public image URL

# Per-user "waiting for custom amount" state (in-memory OK for one worker)
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                chat_id INTEGER PRIMARY KEY,
                total_cents INTEGER NOT NULL DEFAULT 0,
                panel_message_id INTEGER,
                session_id INTEGER NOT NULL DEFAULT 1,
                panel_mode TEXT NOT NULL DEFAULT 'text' -- 'text' or 'banner'
            )
            """
        )
        conn.execute(
            """
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
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                session_id INTEGER NOT NULL,
                kind TEXT NOT NULL, -- 'add' or 'release'
                amount_cents INTEGER NOT NULL,
                total_after_cents INTEGER NOT NULL,
                actor_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


def money_to_cents(amount_str: str) -> int:
    amt = Decimal(amount_str.strip())
    cents = (amt * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    if cents < 0:
        raise ValueError("Negative amount not allowed")
    return int(cents)


def cents_to_money_str(cents: int) -> str:
    amt = Decimal(cents) / Decimal(100)
    return f"{amt:.2f}"


def compute_fee_net(total_cents: int) -> tuple[int, int, int]:
    """
    Returns (fee_cents, network_fee_cents, net_cents)
    net = total - fee - network_fee
    """
    total = Decimal(total_cents) / Decimal(100)
    fee = (total * FEE_PCT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    network_fee = NETWORK_FEE.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net = (total - fee - network_fee).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    fee_cents = int((fee * 100).to_integral_value(rounding=ROUND_HALF_UP))
    network_fee_cents = int((network_fee * 100).to_integral_value(rounding=ROUND_HALF_UP))
    net_cents = int((net * 100).to_integral_value(rounding=ROUND_HALF_UP))

    # Avoid showing negative net if total is very small.
    if net_cents < 0:
        net_cents = 0

    return fee_cents, network_fee_cents, net_cents


def get_state(chat_id: int) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM state WHERE chat_id = ?", (chat_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO state(chat_id, total_cents, panel_message_id, session_id, panel_mode) VALUES (?, 0, NULL, 1, 'text')",
                (chat_id,),
            )
            return {
                "chat_id": chat_id,
                "total_cents": 0,
                "panel_message_id": None,
                "session_id": 1,
                "panel_mode": "text",
            }
        return dict(row)


def set_total(chat_id: int, total_cents: int):
    with db() as conn:
        conn.execute("UPDATE state SET total_cents = ? WHERE chat_id = ?", (total_cents, chat_id))


def set_panel_message_id(chat_id: int, message_id: int):
    with db() as conn:
        conn.execute("UPDATE state SET panel_message_id = ? WHERE chat_id = ?", (message_id, chat_id))


def set_session_id(chat_id: int, session_id: int):
    with db() as conn:
        conn.execute("UPDATE state SET session_id = ? WHERE chat_id = ?", (session_id, chat_id))


def set_panel_mode(chat_id: int, mode: str):
    with db() as conn:
        conn.execute("UPDATE state SET panel_mode = ? WHERE chat_id = ?", (mode, chat_id))


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def notify(context: ContextTypes.DEFAULT_TYPE, text: str):
    # Best-effort notifications; never crash the bot if notification fails
    for uid in NOTIFY_IDS:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            pass


def build_panel_text(total_cents: int) -> str:
    fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)

    # Solo usamos 💱 en el título (y quitamos emojis por línea como pediste)
    return (
        "━━━━━━━━━━━━━━━━━━\n"
        "<b>💱 Yozu Tracker</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Total</b>: <code>${cents_to_money_str(total_cents)}</code>\n"
        f"<b>Fee</b> ({(FEE_PCT * 100):.0f}%): <code>${cents_to_money_str(fee_cents)}</code>\n"
        f"<b>Network fee</b>: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
        f"<b>Neto</b>: <code>${cents_to_money_str(net_cents)}</code>\n\n"
        "<i>Usa los botones. El monto 'Custom' se borra automáticamente.</i>"
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
                InlineKeyboardButton("Soltar", callback_data="release"),
                InlineKeyboardButton("Historial", callback_data="history"),
            ],
        ]
    )


def build_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Volver", callback_data="back")]])


def build_back_to_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Volver al panel", callback_data="back")]])


async def edit_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: InlineKeyboardMarkup):
    """
    Edita el panel en modo texto o en modo banner (foto + caption).
    """
    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    panel_message_id = st.get("panel_message_id")
    mode = st.get("panel_mode", "text")

    if not panel_message_id:
        # No panel yet; create one
        await send_or_update_panel(update, context)
        return

    if mode == "banner" and BANNER_URL:
        # Edit caption
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=panel_message_id,
                caption=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception:
            # fallback to text mode
            set_panel_mode(chat_id, "text")

    # Text mode edit
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=panel_message_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )


async def send_or_update_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    total_cents = st["total_cents"]
    text = build_panel_text(total_cents)
    kb = build_panel_keyboard()
    panel_message_id = st.get("panel_message_id")

    # If we have a stored panel message, try to edit it; otherwise create new one.
    if panel_message_id:
        try:
            await edit_panel(update, context, text=text, reply_markup=kb)
            return
        except Exception:
            pass

    # Create new panel: banner if configured, otherwise text
    if BANNER_URL:
        try:
            msg = await update.effective_chat.send_photo(
                photo=BANNER_URL,
                caption=text,
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
            )
            set_panel_mode(chat_id, "banner")
            set_panel_message_id(chat_id, msg.message_id)
            return
        except Exception:
            # fall back to text
            set_panel_mode(chat_id, "text")

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


def log_movement(chat_id: int, session_id: int, kind: str, amount_cents: int, total_after_cents: int, actor_id: int):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO movements(chat_id, session_id, kind, amount_cents, total_after_cents, actor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (chat_id, session_id, kind, amount_cents, total_after_cents, actor_id, now_utc_iso()),
        )


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    total_cents = st["total_cents"]
    session_id = st["session_id"]
    data = query.data or ""

    if data.startswith("add:"):
        add_amount = data.split(":", 1)[1]
        add_cents = money_to_cents(add_amount)
        total_cents += add_cents
        set_total(chat_id, total_cents)

        actor_id = update.effective_user.id
        log_movement(chat_id, session_id, "add", add_cents, total_cents, actor_id)

        await notify(
            context,
            f"<b>Yozu Tracker</b>\nSe agregó: <code>${cents_to_money_str(add_cents)}</code>\nTotal: <code>${cents_to_money_str(total_cents)}</code>",
        )

        await edit_panel(update, context, text=build_panel_text(total_cents), reply_markup=build_panel_keyboard())
        set_panel_message_id(chat_id, query.message.message_id)
        return

    if data == "custom":
        AWAITING_CUSTOM_AMOUNT.add(update.effective_user.id)
        set_panel_message_id(chat_id, query.message.message_id)

        await edit_panel(
            update,
            context,
            text=(
                "━━━━━━━━━━━━━━━━━━\n"
                "<b>Custom</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "Envía un número como <code>420</code> o <code>420.50</code>.\n"
                "<i>Tu mensaje se borrará automáticamente.</i>"
            ),
            reply_markup=build_back_keyboard(),
        )
        return

    if data == "release":
        actor_id = update.effective_user.id

        if total_cents <= 0:
            set_total(chat_id, 0)
            await edit_panel(update, context, text=build_panel_text(0), reply_markup=build_panel_keyboard())
            set_panel_message_id(chat_id, query.message.message_id)
            return

        fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)
        released_at = now_utc_iso()

        # Record release
        with db() as conn:
            conn.execute(
                """
                INSERT INTO releases(chat_id, session_id, released_total_cents, fee_cents, network_fee_cents, net_cents, released_by, released_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (chat_id, session_id, total_cents, fee_cents, network_fee_cents, net_cents, actor_id, released_at),
            )

        # Log movement release (amount = released_total)
        log_movement(chat_id, session_id, "release", total_cents, 0, actor_id)

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>Soltar</b>\n"
                f"Total: <code>${cents_to_money_str(total_cents)}</code>\n"
                f"Fee: <code>${cents_to_money_str(fee_cents)}</code>\n"
                f"Network fee: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
                f"Neto: <code>${cents_to_money_str(net_cents)}</code>"
            ),
        )

        # Reset total and advance session (so historial “se limpia” por release)
        set_total(chat_id, 0)
        set_session_id(chat_id, session_id + 1)
        AWAITING_CUSTOM_AMOUNT.discard(actor_id)

        await edit_panel(
            update,
            context,
            text=(
                "━━━━━━━━━━━━━━━━━━\n"
                "<b>Soltado</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                f"Total: <code>${cents_to_money_str(total_cents)}</code>\n"
                f"Fee ({(FEE_PCT*100):.0f}%): <code>${cents_to_money_str(fee_cents)}</code>\n"
                f"Network fee: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
                f"Neto: <code>${cents_to_money_str(net_cents)}</code>\n\n"
                "El total se reinició a <b>$0.00</b> y el historial de movimientos se reinició para la nueva sesión."
            ),
            reply_markup=build_back_to_panel_keyboard(),
        )
        set_panel_message_id(chat_id, query.message.message_id)
        return

    if data == "history":
        st = get_state(chat_id)
        session_id = st["session_id"]

        with db() as conn:
            rows = conn.execute(
                """
                SELECT kind, amount_cents, total_after_cents, actor_id, created_at
                FROM movements
                WHERE chat_id = ? AND session_id = ?
                ORDER BY id DESC
                LIMIT 20
                """,
                (chat_id, session_id),
            ).fetchall()

        if not rows:
            hist_text = (
                "━━━━━━━━━━━━━━━━━━\n"
                "<b>Historial</b>\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "No hay movimientos en esta sesión todavía."
            )
        else:
            lines = [
                "━━━━━━━━━━━━━━━━━━",
                "<b>Historial (sesión actual)</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
            ]
            for r in rows:
                ts = r["created_at"].replace("T", " ").split(".")[0].replace("+00:00", " UTC")
                kind = r["kind"]
                if kind == "add":
                    label = "Suma"
                elif kind == "release":
                    label = "Soltar"
                else:
                    label = kind

                lines.append(
                    f"• <b>{label}</b>: <code>${cents_to_money_str(r['amount_cents'])}</code> "
                    f"→ Total: <code>${cents_to_money_str(r['total_after_cents'])}</code>\n"
                    f"  <i>{ts}</i>"
                )
            hist_text = "\n".join(lines)

        await edit_panel(update, context, text=hist_text, reply_markup=build_back_keyboard())
        set_panel_message_id(chat_id, query.message.message_id)
        return

    if data == "back":
        st = get_state(chat_id)
        total_cents = st["total_cents"]
        await edit_panel(update, context, text=build_panel_text(total_cents), reply_markup=build_panel_keyboard())
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
        # Delete invalid input too
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
        except Exception:
            pass

        await update.effective_chat.send_message(
            "Número inválido. Envía algo como <code>420</code> o <code>420.50</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Remove waiting state
    AWAITING_CUSTOM_AMOUNT.discard(user_id)

    st = get_state(chat_id)
    total_cents = st["total_cents"] + add_cents
    set_total(chat_id, total_cents)

    # Log movement
    session_id = st["session_id"]
    log_movement(chat_id, session_id, "add", add_cents, total_cents, user_id)

    await notify(
        context,
        f"<b>Yozu Tracker</b>\nSe agregó: <code>${cents_to_money_str(add_cents)}</code>\nTotal: <code>${cents_to_money_str(total_cents)}</code>",
    )

    # Delete user's message to prevent spam
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception:
        pass

    # Update panel in place
    await edit_panel(update, context, text=build_panel_text(total_cents), reply_markup=build_panel_keyboard())


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
