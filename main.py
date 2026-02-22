import os
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
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
BANNER_URL = os.getenv("BANNER_URL", "").strip()  # optional public image URL

# Delete notifications + small bot messages after N seconds (prod: 10800 for 3h)
NOTIFY_DELETE_SECONDS = int(os.getenv("NOTIFY_DELETE_SECONDS", "10"))

# Hard cap: only 2 participants for the whole bot
MAX_PARTICIPANTS = 2

# Confirmation window (fixed 24h)
CONFIRM_WINDOW_SECONDS = 24 * 60 * 60

# Per-user "waiting for custom amount" state (in-memory OK for one worker)
AWAITING_CUSTOM_AMOUNT: set[int] = set()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        # Per-chat panel state (each user has their own panel message)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_state (
                chat_id INTEGER PRIMARY KEY,
                panel_message_id INTEGER,
                panel_mode TEXT NOT NULL DEFAULT 'text' -- 'text' or 'banner'
            )
            """
        )

        # Global tracker state (shared across all chats/users)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS global_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_cents INTEGER NOT NULL DEFAULT 0,
                session_id INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO global_state(id, total_cents, session_id) VALUES (1, 0, 1)"
        )

        # Participants (global, hard cap 2). First row (oldest) is the CONFIRMER.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS participants (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                added_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS releases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                session_id INTEGER NOT NULL,
                kind TEXT NOT NULL, -- 'add' or 'release'
                amount_cents INTEGER NOT NULL,
                total_after_cents INTEGER NOT NULL,
                actor_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        # Confirmations for ADD movements (soft confirmation; total still updates immediately)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS confirmations (
                movement_id INTEGER PRIMARY KEY,
                actor_id INTEGER NOT NULL,
                amount_cents INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                is_confirmed INTEGER NOT NULL DEFAULT 0,
                confirmed_at TEXT,
                confirmed_by INTEGER,
                confirm_chat_id INTEGER,
                confirm_message_id INTEGER
            )
            """
        )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def iso_to_dt(s: str) -> datetime:
    # stored as ISO with timezone
    return datetime.fromisoformat(s)


def dt_to_iso(d: datetime) -> str:
    return d.astimezone(timezone.utc).isoformat()


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

    if net_cents < 0:
        net_cents = 0

    return fee_cents, network_fee_cents, net_cents


def get_global_state() -> dict:
    with db() as conn:
        row = conn.execute("SELECT total_cents, session_id FROM global_state WHERE id = 1").fetchone()
        return {"total_cents": int(row["total_cents"]), "session_id": int(row["session_id"])}


def set_global_total(total_cents: int):
    with db() as conn:
        conn.execute("UPDATE global_state SET total_cents = ? WHERE id = 1", (total_cents,))


def set_global_session(session_id: int):
    with db() as conn:
        conn.execute("UPDATE global_state SET session_id = ? WHERE id = 1", (session_id,))


def get_chat_state(chat_id: int) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM chat_state WHERE chat_id = ?", (chat_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO chat_state(chat_id, panel_message_id, panel_mode) VALUES (?, NULL, 'text')",
                (chat_id,),
            )
            return {"chat_id": chat_id, "panel_message_id": None, "panel_mode": "text"}
        return dict(row)


def set_panel_message_id(chat_id: int, message_id: int | None):
    with db() as conn:
        conn.execute("UPDATE chat_state SET panel_message_id = ? WHERE chat_id = ?", (message_id, chat_id))


def set_panel_mode(chat_id: int, mode: str):
    with db() as conn:
        conn.execute("UPDATE chat_state SET panel_mode = ? WHERE chat_id = ?", (mode, chat_id))


def participant_count() -> int:
    with db() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM participants").fetchone()
        return int(row["c"])


def is_participant(user_id: int) -> bool:
    with db() as conn:
        row = conn.execute("SELECT 1 FROM participants WHERE user_id = ?", (user_id,)).fetchone()
        return row is not None


def add_participant(user_id: int, first_name: str | None, username: str | None) -> bool:
    """
    Returns True if added or already exists. False if hard cap reached.
    """
    if is_participant(user_id):
        return True
    if participant_count() >= MAX_PARTICIPANTS:
        return False
    with db() as conn:
        conn.execute(
            """
            INSERT INTO participants(user_id, first_name, username, added_at)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, first_name or "", username or "", now_utc_iso()),
        )
    return True


def get_participants() -> list[int]:
    with db() as conn:
        rows = conn.execute("SELECT user_id FROM participants ORDER BY added_at ASC").fetchall()
        return [int(r["user_id"]) for r in rows]


def get_confirmer_id() -> int | None:
    with db() as conn:
        row = conn.execute(
            "SELECT user_id FROM participants ORDER BY added_at ASC LIMIT 1"
        ).fetchone()
        return int(row["user_id"]) if row else None


def pending_confirmations_count() -> int:
    now = now_utc_iso()
    with db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM confirmations
            WHERE is_confirmed = 0 AND expires_at > ?
            """,
            (now,),
        ).fetchone()
        return int(row["c"])


async def delete_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, seconds: int):
    try:
        await asyncio.sleep(seconds)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def notify(context: ContextTypes.DEFAULT_TYPE, text: str):
    # Best-effort notifications; never crash the bot if notification fails
    for uid in get_participants():
        try:
            msg = await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            context.application.create_task(delete_later(context, uid, msg.message_id, NOTIFY_DELETE_SECONDS))
        except Exception:
            pass


def build_panel_text(total_cents: int) -> str:
    fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)

    pending = pending_confirmations_count()
    pending_block = ""
    if pending > 0:
        pending_block = (
            f"\n危 {pending} movimientos no confirmados 危\n"
            "(se autoconfirman en 24h)\n"
        )

    return (
        "光 ═════════════ 光\n\n"
        f"💰 <b>TOTAL</b> :: <code>${cents_to_money_str(total_cents)}</code>\n"
        f"<b>費 Fee</b> ({(FEE_PCT * 100):.0f}%) :: <code>${cents_to_money_str(fee_cents)}</code>\n"
        f"<b>費 Network fee</b> :: <code>${cents_to_money_str(network_fee_cents)}</code>\n"
        f"💵 <b>NET</b>   :: <code>${cents_to_money_str(net_cents)}</code>\n"
        f"{pending_block}\n"
        "<b>(ZELLE CAPTURE ONLY)</b>\n\n"
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


def build_confirm_keyboard(movement_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{movement_id}")]]
    )


async def edit_panel(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: InlineKeyboardMarkup):
    """
    Edita el panel en modo texto o banner (foto + caption).
    Se auto-recupera si el modo guardado no coincide con el tipo real de mensaje.
    """
    st = get_chat_state(chat_id)
    panel_message_id = st.get("panel_message_id")

    if not panel_message_id:
        await send_or_update_panel(chat_id, context)
        return

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
            set_panel_mode(chat_id, "text")

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

    set_panel_message_id(chat_id, None)
    await send_or_update_panel(chat_id, context)


async def send_or_update_panel(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    chat_st = get_chat_state(chat_id)

    g = get_global_state()
    total_cents = g["total_cents"]

    text = build_panel_text(total_cents)
    kb = build_panel_keyboard()
    panel_message_id = chat_st.get("panel_message_id")

    if panel_message_id:
        try:
            await edit_panel(chat_id, context, text=text, reply_markup=kb)
            return
        except Exception:
            pass

    if BANNER_URL:
        try:
            msg = await context.bot.send_photo(
                chat_id=chat_id,
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

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )
    set_panel_message_id(chat_id, msg.message_id)


async def update_all_panels(context: ContextTypes.DEFAULT_TYPE):
    # Each participant has their own chat_id (private chat)
    for uid in get_participants():
        try:
            await send_or_update_panel(uid, context)
        except Exception:
            pass


def log_movement(kind: str, amount_cents: int, total_after_cents: int, actor_id: int) -> int:
    g = get_global_state()
    session_id = g["session_id"]
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO movements(session_id, kind, amount_cents, total_after_cents, actor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, kind, amount_cents, total_after_cents, actor_id, now_utc_iso()),
        )
        return int(cur.lastrowid)


def get_last_movement() -> sqlite3.Row | None:
    with db() as conn:
        row = conn.execute(
            """
            SELECT id, session_id, kind, amount_cents, total_after_cents, actor_id, created_at
            FROM movements
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        return row


def delete_movement(movement_id: int):
    with db() as conn:
        conn.execute("DELETE FROM movements WHERE id = ?", (movement_id,))


def delete_latest_release_for_session(session_id: int):
    with db() as conn:
        row = conn.execute(
            """
            SELECT id FROM releases
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if row:
            conn.execute("DELETE FROM releases WHERE id = ?", (row["id"],))


def create_confirmation_for_movement(movement_id: int, actor_id: int, amount_cents: int) -> None:
    created = now_utc()
    expires = created + timedelta(seconds=CONFIRM_WINDOW_SECONDS)
    with db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO confirmations(
                movement_id, actor_id, amount_cents, created_at, expires_at,
                is_confirmed, confirmed_at, confirmed_by, confirm_chat_id, confirm_message_id
            ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL)
            """,
            (movement_id, actor_id, amount_cents, dt_to_iso(created), dt_to_iso(expires)),
        )


def get_confirmation(movement_id: int) -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM confirmations WHERE movement_id = ?",
            (movement_id,),
        ).fetchone()


def mark_confirmed(movement_id: int, confirmed_by: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE confirmations
            SET is_confirmed = 1, confirmed_at = ?, confirmed_by = ?
            WHERE movement_id = ?
            """,
            (now_utc_iso(), confirmed_by, movement_id),
        )


def set_confirm_message_refs(movement_id: int, chat_id: int, message_id: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE confirmations
            SET confirm_chat_id = ?, confirm_message_id = ?
            WHERE movement_id = ?
            """,
            (chat_id, message_id, movement_id),
        )


def delete_confirmation(movement_id: int):
    with db() as conn:
        conn.execute("DELETE FROM confirmations WHERE movement_id = ?", (movement_id,))


async def try_delete_confirm_message(context: ContextTypes.DEFAULT_TYPE, movement_id: int):
    row = get_confirmation(movement_id)
    if not row:
        return
    chat_id = row["confirm_chat_id"]
    msg_id = row["confirm_message_id"]
    if chat_id and msg_id:
        try:
            await context.bot.delete_message(chat_id=int(chat_id), message_id=int(msg_id))
        except Exception:
            pass


async def cleanup_expired_confirmations(context: ContextTypes.DEFAULT_TYPE):
    """
    Auto-confirm expired items (24h). Also attempt to delete their confirm messages.
    Safe to call often.
    """
    now_iso = now_utc_iso()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT movement_id
            FROM confirmations
            WHERE is_confirmed = 0 AND expires_at <= ?
            """,
            (now_iso,),
        ).fetchall()

    for r in rows:
        mid = int(r["movement_id"])
        # Mark confirmed (auto)
        mark_confirmed(mid, confirmed_by=0)
        # Delete confirm message (best-effort)
        await try_delete_confirm_message(context, mid)


async def send_confirmation_request_to_confirmer(
    context: ContextTypes.DEFAULT_TYPE,
    movement_id: int,
    amount_cents: int,
    actor_id: int,
):
    confirmer_id = get_confirmer_id()
    if not confirmer_id:
        return

    # Only send to confirmer (you)
    try:
        msg = await context.bot.send_message(
            chat_id=confirmer_id,
            text=(
                "<b>Confirmación requerida</b>\n"
                f"Monto: <code>${cents_to_money_str(amount_cents)}</code>\n"
                f"Movimiento ID: <code>{movement_id}</code>\n\n"
                "<i>Se autoconfirma en 24h si no respondes.</i>"
            ),
            reply_markup=build_confirm_keyboard(movement_id),
            parse_mode=ParseMode.HTML,
        )
        # Store message refs so we can delete on confirm/expiry
        set_confirm_message_refs(movement_id, confirmer_id, msg.message_id)

        # Also schedule deletion at 24h best-effort (restart-safe because we also cleanup on interaction)
        context.application.create_task(delete_later(context, confirmer_id, msg.message_id, CONFIRM_WINDOW_SECONDS))
    except Exception:
        pass


async def undo_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cleanup_expired_confirmations(context)

    last = get_last_movement()
    if not last:
        await notify(context, "<b>Yozu Tracker</b>\nNo hay nada que deshacer lol.")
        return

    last_kind = last["kind"]
    last_amount = int(last["amount_cents"])
    last_session = int(last["session_id"])
    last_id = int(last["id"])

    g = get_global_state()
    current_total = g["total_cents"]
    current_session = g["session_id"]

    # If last action was ADD: subtract it + remove confirmation (and confirm message)
    if last_kind == "add":
        if current_session != last_session:
            set_global_session(last_session)
            current_session = last_session

        new_total = current_total - last_amount
        if new_total < 0:
            new_total = 0

        # Remove confirmation first (and its message)
        await try_delete_confirm_message(context, last_id)
        delete_confirmation(last_id)

        set_global_total(new_total)
        delete_movement(last_id)

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                f"Se deshizo: <code>${cents_to_money_str(last_amount)}</code>\n"
                f"Total: <code>${cents_to_money_str(new_total)}</code>"
            ),
        )

        await update_all_panels(context)
        return

    # If last action was RELEASE: restore that released total and restore session
    if last_kind == "release":
        restored_total = last_amount
        restored_session = last_session

        set_global_session(restored_session)
        set_global_total(restored_total)

        delete_latest_release_for_session(restored_session)
        delete_movement(last_id)

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                "Se deshizo un <b>Release</b>.\n"
                f"Total restaurado: <code>${cents_to_money_str(restored_total)}</code>"
            ),
        )

        await update_all_panels(context)
        return

    await notify(context, "<b>Yozu Tracker</b>\nNo se pudo deshacer (tipo desconocido).")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    await cleanup_expired_confirmations(context)

    ok = add_participant(user.id, user.first_name, user.username)
    if not ok:
        msg = await update.effective_chat.send_message("Este tracker ya está completo (máximo 2 usuarios).")
        context.application.create_task(delete_later(context, update.effective_chat.id, msg.message_id, 10))
        return

    # Make sure this user's panel exists/updates
    await send_or_update_panel(update.effective_chat.id, context)

    # Also refresh everybody (so both always see the same global totals)
    await update_all_panels(context)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_participant(user.id):
        return

    await cleanup_expired_confirmations(context)

    query = update.callback_query
    await query.answer()

    data = query.data or ""

    if data.startswith("add:"):
        add_amount = data.split(":", 1)[1]
        add_cents = money_to_cents(add_amount)

        g = get_global_state()
        total_cents = g["total_cents"] + add_cents
        set_global_total(total_cents)

        movement_id = log_movement("add", add_cents, total_cents, user.id)

        # Create confirmation (unconfirmed)
        create_confirmation_for_movement(movement_id, user.id, add_cents)

        # Notifications (auto-delete normal)
        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                f"Se agregó: <code>${cents_to_money_str(add_cents)}</code>\n"
                f"Total: <code>${cents_to_money_str(total_cents)}</code>"
            ),
        )

        # Confirmation request only to confirmer (24h)
        await send_confirmation_request_to_confirmer(
            context=context,
            movement_id=movement_id,
            amount_cents=add_cents,
            actor_id=user.id,
        )

        await update_all_panels(context)
        set_panel_message_id(update.effective_chat.id, query.message.message_id)
        return

    if data == "custom":
        AWAITING_CUSTOM_AMOUNT.add(user.id)
        set_panel_message_id(update.effective_chat.id, query.message.message_id)

        await edit_panel(
            update.effective_chat.id,
            context,
            text=(
                "<b>✍ Custom</b>\n\n"
                "Envía un número como <code>420</code> o <code>420.50</code>, Sin letras ni símbolos.\n\n"
                f"<i>Los mensajes desaparecen en {NOTIFY_DELETE_SECONDS}s.</i>"
            ),
            reply_markup=build_back_keyboard(),
        )
        return

    if data == "undo":
        await undo_last(update, context)
        return

    if data.startswith("confirm:"):
        # Only confirmer can confirm
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return

        try:
            movement_id = int(data.split(":", 1)[1])
        except Exception:
            return

        row = get_confirmation(movement_id)
        if not row:
            # already undone/deleted
            try:
                await query.edit_message_text("Confirmación ya no existe.", parse_mode=ParseMode.HTML)
            except Exception:
                pass
            return

        if int(row["is_confirmed"]) == 1:
            # already confirmed
            await try_delete_confirm_message(context, movement_id)
            return

        # Confirm it
        mark_confirmed(movement_id, confirmed_by=user.id)

        # Delete confirm message instantly
        await try_delete_confirm_message(context, movement_id)

        amount_cents = int(row["amount_cents"])
        actor_id = int(row["actor_id"])

        # Notify the actor (Yozu) that Blasco confirmed (auto-delete normal)
        if actor_id != user.id:
            try:
                msg = await context.bot.send_message(
                    chat_id=actor_id,
                    text=(
                        "<b>Confirmado</b>\n"
                        f"Blasco confirmó: <code>${cents_to_money_str(amount_cents)}</code>"
                    ),
                    parse_mode=ParseMode.HTML,
                )
                context.application.create_task(delete_later(context, actor_id, msg.message_id, NOTIFY_DELETE_SECONDS))
            except Exception:
                pass

        await update_all_panels(context)
        return

    if data == "release":
        g = get_global_state()
        total_cents = g["total_cents"]
        session_id = g["session_id"]

        if total_cents <= 0:
            await notify(context, "La cantidad que intentas retirar es <b>$0.00</b>, Magistral.")
            return

        fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)

        with db() as conn:
            conn.execute(
                """
                INSERT INTO releases(session_id, released_total_cents, fee_cents, network_fee_cents, net_cents, released_by, released_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, total_cents, fee_cents, network_fee_cents, net_cents, user.id, now_utc_iso()),
            )

        log_movement("release", total_cents, 0, user.id)

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

        set_global_total(0)
        set_global_session(session_id + 1)
        AWAITING_CUSTOM_AMOUNT.discard(user.id)

        await edit_panel(
            update.effective_chat.id,
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
        set_panel_message_id(update.effective_chat.id, query.message.message_id)

        # Refresh all panels too
        await update_all_panels(context)
        return

    if data == "history":
        g = get_global_state()
        session_id = g["session_id"]

        with db() as conn:
            rows = conn.execute(
                """
                SELECT kind, amount_cents, total_after_cents, actor_id, created_at
                FROM movements
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT 20
                """,
                (session_id,),
            ).fetchall()

        if not rows:
            hist_text = "<b>📜 History</b>\n\nNo hay movimientos en esta sesión todavía bro."
        else:
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            lines = ["<b>📜 History (sesión actual)</b>", ""]
            for r in rows:
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

        await edit_panel(update.effective_chat.id, context, text=hist_text, reply_markup=build_back_keyboard())
        set_panel_message_id(update.effective_chat.id, query.message.message_id)
        return

    if data == "back":
        g = get_global_state()
        total_cents = g["total_cents"]
        await edit_panel(update.effective_chat.id, context, text=build_panel_text(total_cents), reply_markup=build_panel_keyboard())
        set_panel_message_id(update.effective_chat.id, query.message.message_id)
        return


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_participant(user.id):
        return

    await cleanup_expired_confirmations(context)

    if user.id not in AWAITING_CUSTOM_AMOUNT:
        return

    chat_id = update.effective_chat.id
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

    AWAITING_CUSTOM_AMOUNT.discard(user.id)

    g = get_global_state()
    total_cents = g["total_cents"] + add_cents
    set_global_total(total_cents)

    movement_id = log_movement("add", add_cents, total_cents, user.id)

    # Create confirmation (unconfirmed)
    create_confirmation_for_movement(movement_id, user.id, add_cents)

    await notify(
        context,
        (
            "<b>Yozu Tracker</b>\n"
            f"Se agregó: <code>${cents_to_money_str(add_cents)}</code>\n"
            f"Total: <code>${cents_to_money_str(total_cents)}</code>"
        ),
    )

    await send_confirmation_request_to_confirmer(
        context=context,
        movement_id=movement_id,
        amount_cents=add_cents,
        actor_id=user.id,
    )

    # Delete user's message to prevent spam
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception:
        pass

    await update_all_panels(context)


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