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

FEE_PCT = Decimal(os.getenv("FEE_PCT", "0.02"))  # 2% default
NETWORK_FEE = Decimal(os.getenv("NETWORK_FEE", "0.30"))  # $0.30 flat
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "6448246938")  # comma-separated
NOTIFY_USER_IDS = os.getenv("NOTIFY_USER_IDS", "6448246938")  # comma-separated
BANNER_URL = os.getenv("BANNER_URL", "").strip()  # optional public image URL

# Delete notifications + small bot messages after N seconds (prod: 10800 for 3h)
NOTIFY_DELETE_SECONDS = int(os.getenv("NOTIFY_DELETE_SECONDS", "10"))

# Per-user "waiting for custom amount" state (in-memory OK for one worker)
AWAITING_CUSTOM_AMOUNT: set[int] = set()


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


def set_panel_message_id(chat_id: int, message_id: int | None):
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


async def delete_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, seconds: int):
    try:
        await asyncio.sleep(seconds)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def notify(context: ContextTypes.DEFAULT_TYPE, text: str):
    # Best-effort notifications; never crash the bot if notification fails
    for uid in NOTIFY_IDS:
        try:
            msg = await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            # Auto-delete notification after N seconds
            context.application.create_task(delete_later(context, uid, msg.message_id, NOTIFY_DELETE_SECONDS))
        except Exception:
            pass


def build_panel_text(total_cents: int) -> str:
    fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)
    return (
        "光 ═════════════ 光\n\n"
        f"💰 <b>TOTAL</b> :: <code>${cents_to_money_str(total_cents)}</code>\n"
        f"<b>費 Fee</b> ({(FEE_PCT * 100):.0f}%) :: <code>${cents_to_money_str(fee_cents)}</code>\n"
        f"<b>費 Network fee</b> :: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
        f"💵 <b>NET</b>   :: <code>${cents_to_money_str(net_cents)}</code>\n\n"
        f"<i>⏳ Los mensajes desaparecen en {NOTIFY_DELETE_SECONDS}s</i>"
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
                InlineKeyboardButton("✍ Custom", callback_data="custom"),
            ],
            [
                InlineKeyboardButton("📜 History", callback_data="history"),
                InlineKeyboardButton("⏪ Control + Z", callback_data="undo"),
            ],
            [
                InlineKeyboardButton("解Release除", callback_data="release"),
            ],
        ]
    )


def build_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Volver", callback_data="back")]])


def build_back_to_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Volver al panel", callback_data="back")]])


async def edit_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: InlineKeyboardMarkup):
    """
    Edita el panel en modo texto o banner (foto + caption).
    Se auto-recupera si el modo guardado no coincide con el tipo real de mensaje.
    """
    chat_id = update.effective_chat.id
    st = get_state(chat_id)
    panel_message_id = st.get("panel_message_id")

    if not panel_message_id:
        await send_or_update_panel(update, context)
        return

    # Try banner caption edit first if we believe it's banner
    mode = st.get("panel_mode", "text")
    if mode == "banner" and BANNER_URL:
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
            # maybe it's actually a text message; fall through
            set_panel_mode(chat_id, "text")

    # Try text edit
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=panel_message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        return
    except Exception:
        # maybe it's actually a banner message; try caption edit
        if BANNER_URL:
            try:
                await context.bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=panel_message_id,
                    caption=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML,
                )
                set_panel_mode(chat_id, "banner")
                return
            except Exception:
                pass

    # If we get here, editing failed; re-create panel fresh
    set_panel_message_id(chat_id, None)
    await send_or_update_panel(update, context)


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


def get_last_movement(chat_id: int) -> sqlite3.Row | None:
    with db() as conn:
        row = conn.execute(
            """
            SELECT id, session_id, kind, amount_cents, total_after_cents, actor_id, created_at
            FROM movements
            WHERE chat_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
        return row


def delete_movement(movement_id: int):
    with db() as conn:
        conn.execute("DELETE FROM movements WHERE id = ?", (movement_id,))


def delete_latest_release_for_session(chat_id: int, session_id: int):
    # Best effort: delete the latest release row for that session
    with db() as conn:
        row = conn.execute(
            """
            SELECT id FROM releases
            WHERE chat_id = ? AND session_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (chat_id, session_id),
        ).fetchone()
        if row:
            conn.execute("DELETE FROM releases WHERE id = ?", (row["id"],))


async def undo_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    st = get_state(chat_id)

    last = get_last_movement(chat_id)
    if not last:
        await notify(context, "<b>Yozu Tracker</b>\nNo hay nada que deshacer lol.")
        return

    last_kind = last["kind"]
    last_amount = int(last["amount_cents"])
    last_session = int(last["session_id"])

    # If last action was an ADD: subtract it
    if last_kind == "add":
        if st["session_id"] != last_session:
            set_session_id(chat_id, last_session)

        current_total = int(get_state(chat_id)["total_cents"])
        new_total = current_total - last_amount
        if new_total < 0:
            new_total = 0

        set_total(chat_id, new_total)
        delete_movement(int(last["id"]))

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                f"Se deshizo: <code>${cents_to_money_str(last_amount)}</code>\n"
                f"Total: <code>${cents_to_money_str(new_total)}</code>"
            ),
        )

        await edit_panel(update, context, text=build_panel_text(new_total), reply_markup=build_panel_keyboard())
        return

    # If last action was a RELEASE: restore that released total and restore session
    if last_kind == "release":
        restored_total = last_amount
        restored_session = last_session

        set_session_id(chat_id, restored_session)
        set_total(chat_id, restored_total)

        delete_latest_release_for_session(chat_id, restored_session)
        delete_movement(int(last["id"]))

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                "Se deshizo un <b>Release</b>.\n"
                f"Total restaurado: <code>${cents_to_money_str(restored_total)}</code>"
            ),
        )

        await edit_panel(update, context, text=build_panel_text(restored_total), reply_markup=build_panel_keyboard())
        return

    await notify(context, "<b>Yozu Tracker</b>\nNo se pudo deshacer (tipo desconocido).")


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
            (
                "<b>Yozu Tracker</b>\n"
                f"Se agregó: <code>${cents_to_money_str(add_cents)}</code>\n"
                f"Total: <code>${cents_to_money_str(total_cents)}</code>"
            ),
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
                "<b>✍ Custom</b>\n\n"
                "Envía un número como <code>420</code> o <code>420.50</code>, Sin letras ni símbolos.\n"
                f"<i>Tu mensaje se borrará automáticamente en {NOTIFY_DELETE_SECONDS}s.</i>"
            ),
            reply_markup=build_back_keyboard(),
        )
        return

    if data == "undo":
        await undo_last(update, context)
        return

    if data == "release":
        actor_id = update.effective_user.id

        # CHANGE #1: don't refresh panel / don't create new session if total is 0
        if total_cents <= 0:
            await notify(
                context,
                "La cantidad que intentas retirar es <b>$0.00</b>, Magistral.",
            )
            return

        fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)
        released_at = now_utc_iso()

        with db() as conn:
            conn.execute(
                """
                INSERT INTO releases(chat_id, session_id, released_total_cents, fee_cents, network_fee_cents, net_cents, released_by, released_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (chat_id, session_id, total_cents, fee_cents, network_fee_cents, net_cents, actor_id, released_at),
            )

        log_movement(chat_id, session_id, "release", total_cents, 0, actor_id)

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>解Release除</b>\n"
                f"Total: <code>${cents_to_money_str(total_cents)}</code>\n"
                f"Fee: <code>${cents_to_money_str(fee_cents)}</code>\n"
                f"Network fee: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
                f"Net: <code>${cents_to_money_str(net_cents)}</code>"
            ),
        )

        set_total(chat_id, 0)
        set_session_id(chat_id, session_id + 1)
        AWAITING_CUSTOM_AMOUNT.discard(actor_id)

        await edit_panel(
            update,
            context,
            text=(
                "<b>Released</b>\n\n"
                f"Total: <code>${cents_to_money_str(total_cents)}</code>\n"
                f"Fee ({(FEE_PCT*100):.0f}%): <code>${cents_to_money_str(fee_cents)}</code>\n"
                f"Network fee: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
                f"Net: <code>${cents_to_money_str(net_cents)}</code>\n\n"
                "El total se reinició a <b>$0.00</b>."
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
            hist_text = "<b>📜 History</b>\n\nNo hay movimientos en esta sesión todavía bro."
        else:
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            lines = ["<b>📜 History (sesión actual)</b>", ""]
            for r in rows:
                # CHANGE #2: show date only (no hour)
                dt_utc = datetime.fromisoformat(r["created_at"])
                ts = f"{dt_utc.day:02d} {months[dt_utc.month - 1]} {dt_utc.year}"

                kind = r["kind"]
                label = "Add" if kind == "add" else ("Release" if kind == "release" else kind)
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
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
        except Exception:
            pass

        msg = await update.effective_chat.send_message(
            "Número inválido. Envía algo como <code>420</code> o <code>420.50</code>, Sin letras ni símbolos.",
            parse_mode=ParseMode.HTML,
        )
        context.application.create_task(delete_later(context, chat_id, msg.message_id, NOTIFY_DELETE_SECONDS))
        return

    AWAITING_CUSTOM_AMOUNT.discard(user_id)

    st = get_state(chat_id)
    total_cents = st["total_cents"] + add_cents
    set_total(chat_id, total_cents)

    session_id = st["session_id"]
    log_movement(chat_id, session_id, "add", add_cents, total_cents, user_id)

    await notify(
        context,
        (
            "<b>Yozu Tracker</b>\n"
            f"Se agregó: <code>${cents_to_money_str(add_cents)}</code>\n"
            f"Total: <code>${cents_to_money_str(total_cents)}</code>"
        ),
    )

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception:
        pass

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