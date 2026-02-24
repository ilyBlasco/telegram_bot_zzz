import os
import sqlite3
import asyncio
import logging
import json
import time
import base64
import hmac
import hashlib
import contextlib
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo

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

# =========================
# CONFIG
# =========================

DB_PATH = os.getenv("DB_PATH", "bot.db")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

FEE_PCT = Decimal(os.getenv("FEE_PCT", "0.02"))  # 2% default
NETWORK_FEE = Decimal(os.getenv("NETWORK_FEE", "0.30"))  # $0.30 flat
BANNER_URL = os.getenv("BANNER_URL", "").strip()  # optional public image URL
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY", "").strip()
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET", "").strip()
KRAKEN_REFRESH_SECONDS = max(5, int(os.getenv("KRAKEN_REFRESH_SECONDS", "60")))
KRAKEN_TIMEOUT_SECONDS = max(1, int(os.getenv("KRAKEN_TIMEOUT_SECONDS", "10")))
KRAKEN_ASSET = (os.getenv("KRAKEN_ASSET", "USDT").strip().upper() or "USDT")
KRAKEN_HOLD_DAYS = max(1, int(os.getenv("KRAKEN_HOLD_DAYS", "8")))
KRAKEN_LEDGER_MAX_PAGES = max(1, int(os.getenv("KRAKEN_LEDGER_MAX_PAGES", "10")))
_KRAKEN_DEPOSIT_ESTIMATOR_DEFAULT_MODE = "ui" if (KRAKEN_API_KEY and KRAKEN_API_SECRET) else "off"
KRAKEN_DEPOSIT_ESTIMATOR_MODE = (
    os.getenv("KRAKEN_DEPOSIT_ESTIMATOR_MODE", _KRAKEN_DEPOSIT_ESTIMATOR_DEFAULT_MODE).strip().lower()
    or _KRAKEN_DEPOSIT_ESTIMATOR_DEFAULT_MODE
)
if KRAKEN_DEPOSIT_ESTIMATOR_MODE not in {"off", "shadow", "ui"}:
    KRAKEN_DEPOSIT_ESTIMATOR_MODE = _KRAKEN_DEPOSIT_ESTIMATOR_DEFAULT_MODE
KRAKEN_DEPOSIT_STATUS_PAGE_LIMIT = max(1, int(os.getenv("KRAKEN_DEPOSIT_STATUS_PAGE_LIMIT", "50")))
KRAKEN_DEPOSIT_STATUS_MAX_PAGES = max(1, int(os.getenv("KRAKEN_DEPOSIT_STATUS_MAX_PAGES", "10")))
KRAKEN_DEPOSIT_STATUS_LOOKBACK_DAYS = max(1, int(os.getenv("KRAKEN_DEPOSIT_STATUS_LOOKBACK_DAYS", "14")))
_KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS_RAW = (os.getenv("KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS", "-23").strip() or "-23")
_KRAKEN_HOLD_ESTIMATE_OFFSET_INVALID = False
try:
    KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS = int(_KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS_RAW)
except Exception:
    KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS = -23
    _KRAKEN_HOLD_ESTIMATE_OFFSET_INVALID = True
if KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS < -48:
    KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS = -48
    _KRAKEN_HOLD_ESTIMATE_OFFSET_INVALID = True
elif KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS > 48:
    KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS = 48
    _KRAKEN_HOLD_ESTIMATE_OFFSET_INVALID = True
KRAKEN_DISPLAY_TZ = os.getenv("KRAKEN_DISPLAY_TZ", "America/Chicago").strip() or "America/Chicago"
_KRAKEN_DEPOSIT_TIME_ANCHOR_ALLOWED = {"auto", "processed", "completed", "accepted", "time", "request", "created"}
_KRAKEN_DEPOSIT_TIME_ANCHOR_RAW = (os.getenv("KRAKEN_DEPOSIT_TIME_ANCHOR", "auto").strip().lower() or "auto")
KRAKEN_DEPOSIT_TIME_ANCHOR = _KRAKEN_DEPOSIT_TIME_ANCHOR_RAW
_KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID = False
if KRAKEN_DEPOSIT_TIME_ANCHOR not in _KRAKEN_DEPOSIT_TIME_ANCHOR_ALLOWED:
    KRAKEN_DEPOSIT_TIME_ANCHOR = "auto"
    _KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID = True
KRAKEN_API_BASE = "https://api.kraken.com"
KRAKEN_BALANCE_EX_PATH = "/0/private/BalanceEx"
KRAKEN_LEDGERS_PATH = "/0/private/Ledgers"
KRAKEN_DEPOSIT_STATUS_PATH = "/0/private/DepositStatus"

# Delete notifications + small bot messages after N seconds (prod: 10800 for 3h)
NOTIFY_DELETE_SECONDS = int(os.getenv("NOTIFY_DELETE_SECONDS", "10"))

# Hard cap: only 2 participants for the whole bot
MAX_PARTICIPANTS = 2

# Confirmation window (fixed 24h)
CONFIRM_WINDOW_SECONDS = 24 * 60 * 60

# Per-user "waiting for custom amount" state (in-memory OK for one worker)
AWAITING_CUSTOM_AMOUNT: set[int] = set()

logger = logging.getLogger(__name__)

# Serialize state-changing DB operations inside this single process.
STATE_LOCK = asyncio.Lock()
KRAKEN_REFRESH_LOCK = asyncio.Lock()
KRAKEN_REFRESH_TASK: asyncio.Task | None = None
_KRAKEN_DISPLAY_TZINFO = None
_KRAKEN_DISPLAY_TZ_WARNED = False
_KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID_WARNED = False
_KRAKEN_HOLD_ESTIMATE_OFFSET_WARNED = False

KRAKEN_CACHE: dict = {
    "enabled": bool(KRAKEN_API_KEY and KRAKEN_API_SECRET),
    "balance_status": "loading" if (KRAKEN_API_KEY and KRAKEN_API_SECRET) else "disabled",
    "ledger_status": "disabled",
    "balance_usdt": None,  # Decimal | None
    "tradable_usdt": None,  # Display tradable (ledger-estimated when available)
    "locked_usdt": None,  # Display locked (ledger-estimated when available)
    "api_tradable_usdt": None,  # Raw BalanceEx available/derived tradable
    "api_locked_usdt": None,  # Raw BalanceEx locked/derived hold amount
    "unlock_rows": [],  # list[dict[str, Decimal|str]]
    "last_success_at_balance": None,
    "last_success_at_ledger": None,
    "last_attempt_at": None,
    "last_error_balance": None,
    "last_error_ledger": None,
    "deposit_estimator_status": "disabled",
    "deposit_hold_rows_usd": [],  # list[dict[str, Decimal|str]]
    "deposit_hold_total_usd": None,  # Decimal | None
    "last_success_at_deposit_status": None,
    "last_error_deposit_status": None,
    "countdown_refresh_bucket": None,
}


# =========================
# DB HELPERS
# =========================

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


# =========================
# TIME / MONEY
# =========================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def iso_to_dt(s: str) -> datetime:
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


# =========================
# GLOBAL STATE
# =========================

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


# =========================
# CHAT STATE (PANEL MESSAGE)
# =========================

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


# =========================
# PARTICIPANTS
# =========================

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


# =========================
# CONFIRMATIONS
# =========================

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


def confirm_movement_tx(movement_id: int, confirmer_id: int) -> dict:
    """
    Atomically checks and confirms a movement confirmation record.
    """
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM confirmations WHERE movement_id = ?",
            (movement_id,),
        ).fetchone()

        if not row:
            return {"status": "missing"}

        actor_id = int(row["actor_id"])
        amount_cents = int(row["amount_cents"])

        if int(row["is_confirmed"]) == 1:
            return {
                "status": "already_confirmed",
                "actor_id": actor_id,
                "amount_cents": amount_cents,
            }

        conn.execute(
            """
            UPDATE confirmations
            SET is_confirmed = 1, confirmed_at = ?, confirmed_by = ?
            WHERE movement_id = ?
            """,
            (now_utc_iso(), confirmer_id, movement_id),
        )

        return {
            "status": "confirmed",
            "actor_id": actor_id,
            "amount_cents": amount_cents,
        }


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
    async with STATE_LOCK:
        with db() as conn:
            rows = conn.execute(
                """
                SELECT movement_id
                FROM confirmations
                WHERE is_confirmed = 0 AND expires_at <= ?
                """,
                (now_iso,),
            ).fetchall()

        mids = [int(r["movement_id"]) for r in rows]
        for mid in mids:
            mark_confirmed(mid, confirmed_by=0)

    for mid in mids:
        await try_delete_confirm_message(context, mid)


def add_amount_with_confirmation(actor_id: int, add_cents: int) -> tuple[int, int]:
    """
    Atomically updates total, logs the movement, and creates the confirmation row.
    Returns (movement_id, new_total_cents).
    """
    created = now_utc()
    created_iso = dt_to_iso(created)
    expires_iso = dt_to_iso(created + timedelta(seconds=CONFIRM_WINDOW_SECONDS))

    with db() as conn:
        row = conn.execute(
            "SELECT total_cents, session_id FROM global_state WHERE id = 1"
        ).fetchone()
        total_cents = int(row["total_cents"]) + add_cents
        session_id = int(row["session_id"])

        conn.execute("UPDATE global_state SET total_cents = ? WHERE id = 1", (total_cents,))

        cur = conn.execute(
            """
            INSERT INTO movements(session_id, kind, amount_cents, total_after_cents, actor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, "add", add_cents, total_cents, actor_id, created_iso),
        )
        movement_id = int(cur.lastrowid)

        conn.execute(
            """
            INSERT OR REPLACE INTO confirmations(
                movement_id, actor_id, amount_cents, created_at, expires_at,
                is_confirmed, confirmed_at, confirmed_by, confirm_chat_id, confirm_message_id
            ) VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL)
            """,
            (movement_id, actor_id, add_cents, created_iso, expires_iso),
        )

    return movement_id, total_cents


def release_current_total(actor_id: int) -> dict | None:
    """
    Atomically records a release, logs it, and resets the running total/session.
    Returns release summary data or None if total <= 0.
    """
    with db() as conn:
        row = conn.execute(
            "SELECT total_cents, session_id FROM global_state WHERE id = 1"
        ).fetchone()
        total_cents = int(row["total_cents"])
        session_id = int(row["session_id"])

        if total_cents <= 0:
            return None

        fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)
        ts = now_utc_iso()

        conn.execute(
            """
            INSERT INTO releases(session_id, released_total_cents, fee_cents, network_fee_cents, net_cents, released_by, released_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (session_id, total_cents, fee_cents, network_fee_cents, net_cents, actor_id, ts),
        )

        conn.execute(
            """
            INSERT INTO movements(session_id, kind, amount_cents, total_after_cents, actor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, "release", total_cents, 0, actor_id, ts),
        )

        conn.execute(
            "UPDATE global_state SET total_cents = 0, session_id = ? WHERE id = 1",
            (session_id + 1,),
        )

    return {
        "session_id": session_id,
        "total_cents": total_cents,
        "fee_cents": fee_cents,
        "network_fee_cents": network_fee_cents,
        "net_cents": net_cents,
    }


def undo_last_movement_tx() -> dict | None:
    """
    Atomically undoes the latest movement and returns metadata for notifications/UI.
    """
    with db() as conn:
        last = conn.execute(
            """
            SELECT id, session_id, kind, amount_cents, total_after_cents, actor_id, created_at
            FROM movements
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if not last:
            return None

        last_kind = str(last["kind"])
        last_amount = int(last["amount_cents"])
        last_session = int(last["session_id"])
        last_id = int(last["id"])

        g = conn.execute(
            "SELECT total_cents, session_id FROM global_state WHERE id = 1"
        ).fetchone()
        current_total = int(g["total_cents"])
        current_session = int(g["session_id"])

        if last_kind == "add":
            if current_session != last_session:
                conn.execute("UPDATE global_state SET session_id = ? WHERE id = 1", (last_session,))

            new_total = current_total - last_amount
            if new_total < 0:
                new_total = 0

            conf = conn.execute(
                """
                SELECT confirm_chat_id, confirm_message_id
                FROM confirmations
                WHERE movement_id = ?
                """,
                (last_id,),
            ).fetchone()

            conn.execute("DELETE FROM confirmations WHERE movement_id = ?", (last_id,))
            conn.execute("UPDATE global_state SET total_cents = ? WHERE id = 1", (new_total,))
            conn.execute("DELETE FROM movements WHERE id = ?", (last_id,))

            return {
                "kind": "add",
                "amount_cents": last_amount,
                "new_total_cents": new_total,
                "confirm_chat_id": int(conf["confirm_chat_id"]) if conf and conf["confirm_chat_id"] else None,
                "confirm_message_id": int(conf["confirm_message_id"]) if conf and conf["confirm_message_id"] else None,
            }

        if last_kind == "release":
            conn.execute(
                "UPDATE global_state SET session_id = ?, total_cents = ? WHERE id = 1",
                (last_session, last_amount),
            )

            row = conn.execute(
                """
                SELECT id FROM releases
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (last_session,),
            ).fetchone()
            if row:
                conn.execute("DELETE FROM releases WHERE id = ?", (row["id"],))

            conn.execute("DELETE FROM movements WHERE id = ?", (last_id,))

            return {
                "kind": "release",
                "restored_total_cents": last_amount,
                "restored_session_id": last_session,
            }

        return {"kind": "unknown"}


# =========================
# MOVEMENTS / RELEASES
# =========================

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


# =========================
# KRAKEN BALANCE (CACHE + API)
# =========================

def _kraken_state_snapshot() -> dict:
    snap = dict(KRAKEN_CACHE)
    snap["unlock_rows"] = [dict(row) for row in (KRAKEN_CACHE.get("unlock_rows") or [])]
    snap["deposit_hold_rows_usd"] = [dict(row) for row in (KRAKEN_CACHE.get("deposit_hold_rows_usd") or [])]
    return snap


def _format_kraken_amount_4(value: Decimal) -> str:
    q = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return f"{q:.4f}"


def _kraken_decimal_or_none(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _format_kraken_amount_int_readable(value: Decimal) -> str:
    if value > 0 and value < 1:
        return "<1"
    if value < 0 and value > -1:
        return ">-1"
    return str(int(value))


def _format_usd_est_amount_int(value: Decimal | None) -> str:
    if value is None:
        return "--"
    if value > 0 and value < 1:
        return "<$1"
    if value < 0 and value > -1:
        return ">-$1"
    sign = "-" if value < 0 else ""
    return f"{sign}${int(abs(value))}"


def _format_usd_row_amount(value: Decimal | None) -> str:
    if value is None or value <= 0:
        return "+$0"
    if value < 1:
        return "+<$1"
    return f"+${int(value)}"


def _parse_iso_utc_or_none(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = iso_to_dt(str(s))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_utc_short(dt: datetime) -> str:
    dt_utc = dt.astimezone(timezone.utc)
    return f"{dt_utc.strftime('%b')} {dt_utc.day} {dt_utc.strftime('%H:%M')} UTC"


def _get_kraken_display_tzinfo():
    global _KRAKEN_DISPLAY_TZINFO, _KRAKEN_DISPLAY_TZ_WARNED
    if _KRAKEN_DISPLAY_TZINFO is not None:
        return _KRAKEN_DISPLAY_TZINFO
    try:
        _KRAKEN_DISPLAY_TZINFO = ZoneInfo(KRAKEN_DISPLAY_TZ)
    except Exception:
        _KRAKEN_DISPLAY_TZINFO = timezone.utc
        if not _KRAKEN_DISPLAY_TZ_WARNED:
            _KRAKEN_DISPLAY_TZ_WARNED = True
            logger.warning("Invalid KRAKEN_DISPLAY_TZ '%s'; falling back to UTC", KRAKEN_DISPLAY_TZ)
    return _KRAKEN_DISPLAY_TZINFO


def _format_kraken_display_time_short(dt: datetime) -> str:
    local_dt = dt.astimezone(_get_kraken_display_tzinfo())
    hour_12 = local_dt.hour % 12 or 12
    ampm = "AM" if local_dt.hour < 12 else "PM"
    tz_label = local_dt.tzname() or "UTC"
    return f"{local_dt.strftime('%b')} {local_dt.day} {hour_12}:{local_dt.strftime('%M')} {ampm} {tz_label}"


def _format_countdown_short(now_dt: datetime, target_dt: datetime) -> str:
    delta_seconds = int((target_dt - now_dt).total_seconds())
    if delta_seconds <= 0:
        return "NOW"

    total_minutes = max(1, (delta_seconds + 59) // 60)
    days, rem_minutes = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(rem_minutes, 60)

    if days > 0:
        return f"{days}d {hours:02d}h"
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _kraken_countdown_refresh_bucket(snapshot: dict, now_dt: datetime) -> str | None:
    deposit_status = str(snapshot.get("deposit_estimator_status") or "")
    if deposit_status not in {"ok", "stale"}:
        return None

    active_unlocks: list[datetime] = []
    for row in (snapshot.get("deposit_hold_rows_usd") or []):
        amount_usd = _kraken_decimal_or_none(row.get("amount_usd"))
        unlock_at = _parse_iso_utc_or_none(row.get("unlock_at_iso"))
        if amount_usd is None or amount_usd <= 0 or unlock_at is None or unlock_at <= now_dt:
            continue
        active_unlocks.append(unlock_at)

    if not active_unlocks:
        return None

    has_under_24h = any((unlock_at - now_dt).total_seconds() < 24 * 60 * 60 for unlock_at in active_unlocks)
    if has_under_24h:
        return now_dt.strftime("m:%Y%m%d%H%M")
    return now_dt.strftime("h:%Y%m%d%H")


def _kraken_asset_matches_target(asset_value: str | None) -> bool:
    asset_upper = str(asset_value or "").upper()
    return asset_upper == KRAKEN_ASSET or asset_upper.startswith(KRAKEN_ASSET + ".")


def _format_kraken_dashboard_block(snapshot: dict, render_now: datetime | None = None) -> str:
    if render_now is None:
        render_now = now_utc()

    balance_status = str(snapshot.get("balance_status") or "")
    balance = _kraken_decimal_or_none(snapshot.get("balance_usdt"))

    balance_str = _format_kraken_amount_4(balance) if balance is not None else "--"
    stale_suffix = " [STALE]" if (balance is not None and balance_status == "stale") else ""
    lines = [f"<b>KRAKEN BALANCE: {balance_str} (USDT){stale_suffix}</b>"]

    if KRAKEN_DEPOSIT_ESTIMATOR_MODE != "ui":
        return "\n".join(lines)

    deposit_status = str(snapshot.get("deposit_estimator_status") or "")
    if deposit_status not in {"ok", "stale"}:
        return "\n".join(lines)

    deposit_rows = snapshot.get("deposit_hold_rows_usd") or []
    row_lines: list[str] = []
    active_total = Decimal("0")
    for row in deposit_rows:
        amount_usd = _kraken_decimal_or_none(row.get("amount_usd"))
        unlock_at = _parse_iso_utc_or_none(row.get("unlock_at_iso"))
        if amount_usd is None or amount_usd <= 0 or unlock_at is None or unlock_at <= render_now:
            continue
        active_total += amount_usd
        row_lines.append(
            f"<i>{_format_usd_row_amount(amount_usd)} &#183; "
            f"{_format_countdown_short(render_now, unlock_at)} &#183; {_format_kraken_display_time_short(unlock_at)}</i>"
        )

    total_usd = active_total

    if balance is not None:
        est_tradable = balance - total_usd
        if est_tradable < 0:
            est_tradable = Decimal("0")
        lines.append(f"<b>KRAKEN TRADABLE [EST]: {_format_kraken_amount_4(est_tradable)} (USDT)</b>")

    if not row_lines:
        return "\n".join(lines)

    if deposit_status == "stale":
        lines.append("<i>&#9888; Kraken deposit hold estimate refresh failed, showing cached estimate</i>")

    lines.append(f"<i>KRAKEN HOLDS [EST USD]: {_format_usd_est_amount_int(total_usd)}</i>")
    lines.append("<i>UNLOCKS [EST USD]:</i>")
    lines.extend(row_lines)
    return "\n".join(lines)


def _kraken_sign(url_path: str, nonce: str, postdata: str, api_secret_b64: str) -> str:
    secret = base64.b64decode(api_secret_b64)
    sha256_digest = hashlib.sha256((nonce + postdata).encode("utf-8")).digest()
    message = url_path.encode("utf-8") + sha256_digest
    sig = hmac.new(secret, message, hashlib.sha512).digest()
    return base64.b64encode(sig).decode("utf-8")


def _kraken_private_post_sync(url_path: str, extra_form: dict | None = None) -> dict:
    if not KRAKEN_API_KEY or not KRAKEN_API_SECRET:
        raise RuntimeError("Kraken credentials not configured")

    nonce = str(int(time.time() * 1000))
    form = {"nonce": nonce}
    for key, value in (extra_form or {}).items():
        if value is None:
            continue
        form[str(key)] = str(value)
    postdata = urllib_parse.urlencode(form)
    api_sign = _kraken_sign(url_path, nonce, postdata, KRAKEN_API_SECRET)

    req = urllib_request.Request(
        url=f"{KRAKEN_API_BASE}{url_path}",
        data=postdata.encode("utf-8"),
        headers={
            "API-Key": KRAKEN_API_KEY,
            "API-Sign": api_sign,
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "telegram_bot_zzz/kraken-balance",
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(req, timeout=KRAKEN_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
    except urllib_error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        body_short = body[:200].replace("\n", " ").strip()
        raise RuntimeError(f"Kraken HTTP {e.code}: {body_short or e.reason}") from e
    except urllib_error.URLError as e:
        raise RuntimeError(f"Kraken network error: {e.reason}") from e

    try:
        payload = json.loads(body)
    except Exception as e:
        raise RuntimeError("Kraken returned invalid JSON") from e

    errors = payload.get("error") or []
    if errors:
        msg = "; ".join(str(x) for x in errors)
        if "otp" in msg.lower():
            raise RuntimeError("Kraken API key requires OTP/2FA (unsupported)")
        raise RuntimeError(f"Kraken API error: {msg}")

    if "result" not in payload:
        raise RuntimeError("Kraken response missing result")

    return payload


def _kraken_private_post_balance_ex_sync() -> dict:
    return _kraken_private_post_sync(KRAKEN_BALANCE_EX_PATH, {})


def _kraken_private_post_ledgers_sync(ofs: int = 0) -> dict:
    return _kraken_private_post_sync(KRAKEN_LEDGERS_PATH, {"asset": KRAKEN_ASSET, "ofs": ofs})


def _kraken_private_post_deposit_status_sync(cursor: str | None = None, limit: int | None = None) -> dict:
    form: dict[str, str | int] = {}
    if cursor:
        form["cursor"] = cursor
    if limit is not None:
        form["limit"] = int(limit)
    return _kraken_private_post_sync(KRAKEN_DEPOSIT_STATUS_PATH, form)


def _extract_balance_split_usdt(payload: dict) -> tuple[Decimal, Decimal, Decimal]:
    result = payload.get("result") or {}
    asset = result.get(KRAKEN_ASSET)
    if asset is None:
        return Decimal("0"), Decimal("0"), Decimal("0")

    if isinstance(asset, dict):
        try:
            balance = Decimal(str(asset.get("balance", "0")))
        except Exception as e:
            raise RuntimeError(f"Invalid {KRAKEN_ASSET} balance from Kraken") from e

        if asset.get("available") is not None:
            try:
                tradable = Decimal(str(asset.get("available")))
            except Exception as e:
                raise RuntimeError(f"Invalid {KRAKEN_ASSET} available from Kraken") from e
        elif asset.get("hold_trade") is not None:
            try:
                tradable = balance - Decimal(str(asset.get("hold_trade")))
            except Exception as e:
                raise RuntimeError(f"Invalid {KRAKEN_ASSET} hold_trade from Kraken") from e
        else:
            tradable = balance
    else:
        try:
            balance = Decimal(str(asset))
        except Exception as e:
            raise RuntimeError(f"Invalid {KRAKEN_ASSET} value from Kraken") from e
        tradable = balance

    if tradable < 0:
        tradable = Decimal("0")
    if balance >= 0 and tradable > balance:
        tradable = balance

    locked = balance - tradable
    if locked < 0:
        locked = Decimal("0")

    return balance, tradable, locked


def _kraken_parse_time_any(value) -> datetime | None:
    if value in (None, ""):
        return None

    # Numeric epoch timestamps (seconds or milliseconds).
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        text = str(value).strip()
        if text and text.replace(".", "", 1).isdigit():
            ts = float(text)
            if ts > 1e12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass

    # ISO-ish timestamps.
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _kraken_extract_result_items_and_cursor(payload: dict) -> tuple[list, str | None]:
    result = payload.get("result")

    if isinstance(result, list):
        next_cursor = payload.get("next_cursor") or payload.get("cursor")
        return result, str(next_cursor) if next_cursor else None

    if not isinstance(result, dict):
        raise RuntimeError("Kraken DepositStatus result has unsupported shape")

    items = None
    for key in ("deposits", "items", "data", "entries", "records"):
        candidate = result.get(key)
        if isinstance(candidate, list):
            items = candidate
            break

    if items is None:
        dict_values = list(result.values())
        if dict_values and all(isinstance(v, dict) for v in dict_values):
            # Legacy map-style shape keyed by id/ref.
            items = dict_values

    next_cursor = None
    for key in ("next_cursor", "nextCursor", "cursor", "next-cursor"):
        if result.get(key):
            next_cursor = result.get(key)
            break
    if next_cursor is None:
        for container_key in ("meta", "pagination", "page_info"):
            container = result.get(container_key)
            if not isinstance(container, dict):
                continue
            for key in ("next_cursor", "nextCursor", "cursor", "endCursor"):
                if container.get(key):
                    next_cursor = container.get(key)
                    break
            if next_cursor is not None:
                break

    if items is None:
        # Empty/metadata-only responses are valid.
        metadata_keys = {
            "count",
            "cursor",
            "next_cursor",
            "nextCursor",
            "meta",
            "pagination",
            "page_info",
        }
        if not result or set(result.keys()).issubset(metadata_keys):
            items = []
        else:
            raise RuntimeError(
                "Kraken DepositStatus result missing items list (keys: %s)"
                % ",".join(sorted(str(k) for k in result.keys()))
            )

    return items, str(next_cursor) if next_cursor else None


def _extract_usd_deposit_events(payload: dict) -> tuple[list[dict], str | None]:
    items, next_cursor = _kraken_extract_result_items_and_cursor(payload)
    events: list[dict] = []

    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue

        asset = str(item.get("asset") or item.get("aclass") or item.get("currency") or "").upper()
        if asset not in {"USD", "ZUSD"}:
            continue

        amount = _kraken_decimal_or_none(
            item.get("amount")
            if item.get("amount") is not None
            else item.get("amount_usd")
            if item.get("amount_usd") is not None
            else item.get("volume")
            if item.get("volume") is not None
            else item.get("vol")
        )
        if amount is None or amount <= 0:
            continue

        raw_status = str(
            item.get("status_name")
            or item.get("status_string")
            or item.get("status")
            or item.get("state")
            or ""
        ).strip()
        status_norm = raw_status.lower()
        if status_norm:
            if any(x in status_norm for x in ("fail", "error", "cancel", "reject", "denied", "pending", "initiated")):
                continue
            if not any(x in status_norm for x in ("success", "complete", "credited", "settled")):
                # Unknown status string; skip to avoid overstating held funds.
                continue

        time_key_groups = {
            "processed": ("processed_time", "processedAt", "processed_at"),
            "completed": ("completed_time", "completedAt", "completed_at"),
            "accepted": ("accepted_time", "acceptedAt", "accepted_at"),
            "time": ("time",),
            "request": ("request_time", "requestAt", "request_at"),
            "created": ("created_time", "createdAt", "created_at"),
        }
        candidates: dict[str, datetime] = {}
        for group_name, keys in time_key_groups.items():
            for key in keys:
                parsed = _kraken_parse_time_any(item.get(key))
                if parsed is not None:
                    candidates[group_name] = parsed
                    break

        processed_at = None
        processed_at_source = None
        if KRAKEN_DEPOSIT_TIME_ANCHOR != "auto":
            processed_at = candidates.get(KRAKEN_DEPOSIT_TIME_ANCHOR)
            processed_at_source = KRAKEN_DEPOSIT_TIME_ANCHOR if processed_at is not None else None
        if processed_at is None:
            for source_name in ("processed", "completed", "accepted", "time", "request", "created"):
                if source_name in candidates:
                    processed_at = candidates[source_name]
                    processed_at_source = source_name
                    break
        if processed_at is None:
            continue

        events.append(
            {
                "id": str(item.get("id") or item.get("refid") or item.get("txid") or idx),
                "asset": asset,
                "method": str(item.get("method") or item.get("method_name") or item.get("network") or ""),
                "status": raw_status or "unknown",
                "amount_usd": amount,
                "processed_at": processed_at,
                "processed_at_source": processed_at_source or "unknown",
            }
        )

    return events, next_cursor


def _fetch_usd_deposit_events_with_pagination(fetch_now: datetime) -> tuple[list[dict], bool]:
    cutoff = fetch_now - timedelta(days=KRAKEN_DEPOSIT_STATUS_LOOKBACK_DAYS)
    cursor: str | None = None
    all_events: list[dict] = []
    hit_cap = True

    for _ in range(KRAKEN_DEPOSIT_STATUS_MAX_PAGES):
        payload = _kraken_private_post_deposit_status_sync(
            cursor=cursor,
            limit=KRAKEN_DEPOSIT_STATUS_PAGE_LIMIT,
        )
        page_events, next_cursor = _extract_usd_deposit_events(payload)
        all_events.extend(page_events)

        oldest_page_time = None
        for ev in page_events:
            t = ev.get("processed_at")
            if isinstance(t, datetime) and (oldest_page_time is None or t < oldest_page_time):
                oldest_page_time = t

        if oldest_page_time is not None and oldest_page_time <= cutoff:
            hit_cap = False
            break
        if not next_cursor or next_cursor == cursor:
            hit_cap = False
            break

        cursor = next_cursor

    all_events.sort(key=lambda e: (e["processed_at"], e.get("id") or ""))
    return all_events, hit_cap


def _estimate_usd_hold_rows_from_deposits(events: list[dict], now_dt: datetime) -> list[dict]:
    hold_delta = timedelta(days=KRAKEN_HOLD_DAYS, hours=KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS)
    rows_by_minute: dict[str, dict] = {}

    for ev in events:
        processed_at = ev.get("processed_at")
        amount_usd = _kraken_decimal_or_none(ev.get("amount_usd"))
        if not isinstance(processed_at, datetime) or amount_usd is None or amount_usd <= 0:
            continue

        unlock_at = processed_at + hold_delta
        if unlock_at <= now_dt:
            continue

        minute_dt = unlock_at.astimezone(timezone.utc).replace(second=0, microsecond=0)
        minute_key = dt_to_iso(minute_dt)
        row = rows_by_minute.get(minute_key)
        if row is None:
            row = {"unlock_at": minute_dt, "amount_usd": Decimal("0")}
            rows_by_minute[minute_key] = row
        row["amount_usd"] += amount_usd

    rows = sorted(rows_by_minute.values(), key=lambda r: r["unlock_at"])
    return [
        {"unlock_at_iso": dt_to_iso(r["unlock_at"]), "amount_usd": r["amount_usd"]}
        for r in rows
        if r["amount_usd"] > 0
    ]


def _extract_usdt_ledger_events(payload: dict) -> list[dict]:
    result = payload.get("result") or {}
    ledger_map = result.get("ledger")
    if not isinstance(ledger_map, dict):
        raise RuntimeError("Kraken Ledgers response missing ledger object")

    events: list[dict] = []
    for ledger_id, item in ledger_map.items():
        if not isinstance(item, dict):
            continue

        asset_upper = str(item.get("asset") or "").upper()
        if not _kraken_asset_matches_target(asset_upper):
            continue

        try:
            amount = Decimal(str(item.get("amount")))
            ev_time = datetime.fromtimestamp(float(item.get("time")), tz=timezone.utc)
        except Exception:
            continue

        events.append(
            {
                "id": str(ledger_id),
                "refid": str(item.get("refid") or ""),
                "asset": asset_upper,
                "time": ev_time,
                "amount": amount,
            }
        )

    return events


def _fetch_usdt_ledger_events_with_pagination(fetch_now: datetime) -> tuple[list[dict], bool]:
    cutoff = fetch_now - timedelta(days=KRAKEN_HOLD_DAYS)
    ofs = 0
    all_events: list[dict] = []
    hit_cap = True

    for _ in range(KRAKEN_LEDGER_MAX_PAGES):
        payload = _kraken_private_post_ledgers_sync(ofs=ofs)
        result = payload.get("result") or {}
        ledger_map = result.get("ledger")
        if not isinstance(ledger_map, dict):
            raise RuntimeError("Kraken Ledgers response missing ledger object")
        if not ledger_map:
            hit_cap = False
            break

        page_events = _extract_usdt_ledger_events(payload)
        all_events.extend(page_events)

        page_len = len(ledger_map)
        ofs += page_len

        oldest_page_time = None
        for ev in page_events:
            t = ev["time"]
            if oldest_page_time is None or t < oldest_page_time:
                oldest_page_time = t

        try:
            count = int(result["count"]) if result.get("count") is not None else None
        except Exception:
            count = None

        if oldest_page_time is not None and oldest_page_time <= cutoff:
            hit_cap = False
            break
        if count is not None and ofs >= count:
            hit_cap = False
            break
        if page_len <= 0:
            hit_cap = False
            break

    all_events.sort(key=lambda e: (e["time"], e.get("id") or "", e.get("refid") or ""))
    return all_events, hit_cap


def _estimate_unlock_rows_fifo(events: list[dict], now_dt: datetime) -> list[dict]:
    hold_delta = timedelta(days=KRAKEN_HOLD_DAYS)
    lots: list[dict] = []

    for ev in events:
        ev_time = ev.get("time")
        amount = _kraken_decimal_or_none(ev.get("amount"))
        if not isinstance(ev_time, datetime) or amount is None:
            continue

        if amount > 0:
            lots.append(
                {
                    "unlock_at": ev_time + hold_delta,
                    "remaining": amount,
                }
            )
            continue

        if amount >= 0:
            continue

        to_consume = -amount
        for lot in lots:
            if to_consume <= 0:
                break
            rem = lot["remaining"]
            if rem <= 0:
                continue
            take = rem if rem <= to_consume else to_consume
            lot["remaining"] = rem - take
            to_consume -= take

    rows_by_minute: dict[str, dict] = {}
    for lot in lots:
        remaining = lot["remaining"]
        unlock_at = lot["unlock_at"]
        if remaining <= 0 or unlock_at <= now_dt:
            continue

        minute_dt = unlock_at.astimezone(timezone.utc).replace(second=0, microsecond=0)
        minute_key = dt_to_iso(minute_dt)
        row = rows_by_minute.get(minute_key)
        if row is None:
            row = {"unlock_at": minute_dt, "amount_usdt": Decimal("0")}
            rows_by_minute[minute_key] = row
        row["amount_usdt"] += remaining

    rows = sorted(rows_by_minute.values(), key=lambda r: r["unlock_at"])

    out_rows = [
        {"unlock_at_iso": dt_to_iso(r["unlock_at"]), "amount_usdt": r["amount_usdt"]}
        for r in rows
        if r["amount_usdt"] > 0
    ]
    return out_rows


async def refresh_kraken_cache_once(app: Application) -> None:
    global _KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID_WARNED, _KRAKEN_HOLD_ESTIMATE_OFFSET_WARNED
    if not KRAKEN_CACHE["enabled"]:
        return
    if _KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID and not _KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID_WARNED:
        _KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID_WARNED = True
        logger.warning(
            "Invalid KRAKEN_DEPOSIT_TIME_ANCHOR '%s'; falling back to 'auto'",
            _KRAKEN_DEPOSIT_TIME_ANCHOR_RAW,
        )
    if _KRAKEN_HOLD_ESTIMATE_OFFSET_INVALID and not _KRAKEN_HOLD_ESTIMATE_OFFSET_WARNED:
        _KRAKEN_HOLD_ESTIMATE_OFFSET_WARNED = True
        logger.warning(
            "Invalid/out-of-range KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS '%s'; using %s",
            _KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS_RAW,
            KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS,
        )

    should_refresh_panels = False
    force_countdown_refresh = False
    refresh_now = now_utc()

    async with KRAKEN_REFRESH_LOCK:
        before_block = _format_kraken_dashboard_block(_kraken_state_snapshot(), render_now=refresh_now)
        KRAKEN_CACHE["last_attempt_at"] = now_utc_iso()

        try:
            payload = await asyncio.to_thread(_kraken_private_post_balance_ex_sync)
            balance_usdt, tradable_usdt, locked_usdt = _extract_balance_split_usdt(payload)
        except Exception as e:
            had_success = KRAKEN_CACHE.get("last_success_at_balance") is not None
            KRAKEN_CACHE["balance_status"] = "stale" if had_success else "error"
            KRAKEN_CACHE["last_error_balance"] = str(e)[:200]
            logger.warning("Kraken balance refresh failed: %s", KRAKEN_CACHE["last_error_balance"])
        else:
            KRAKEN_CACHE["balance_status"] = "ok"
            KRAKEN_CACHE["balance_usdt"] = balance_usdt
            KRAKEN_CACHE["api_tradable_usdt"] = tradable_usdt
            KRAKEN_CACHE["api_locked_usdt"] = locked_usdt
            # Cold-start fallback until ledger-based estimate succeeds at least once.
            if KRAKEN_CACHE.get("last_success_at_ledger") is None:
                KRAKEN_CACHE["tradable_usdt"] = tradable_usdt
                KRAKEN_CACHE["locked_usdt"] = locked_usdt
            KRAKEN_CACHE["last_success_at_balance"] = now_utc_iso()
            KRAKEN_CACHE["last_error_balance"] = None
            logger.info(
                "Kraken %s raw API refreshed balance=%s tradable=%s locked=%s",
                KRAKEN_ASSET,
                _format_kraken_amount_4(balance_usdt),
                _format_kraken_amount_4(tradable_usdt),
                _format_kraken_amount_4(locked_usdt),
            )

        # Hotfix mode: disable unsupported hold/unlock estimation to avoid misleading values.
        KRAKEN_CACHE["ledger_status"] = "disabled"
        KRAKEN_CACHE["unlock_rows"] = []
        KRAKEN_CACHE["last_error_ledger"] = None

        if KRAKEN_DEPOSIT_ESTIMATOR_MODE == "off":
            KRAKEN_CACHE["deposit_estimator_status"] = "disabled"
            KRAKEN_CACHE["deposit_hold_rows_usd"] = []
            KRAKEN_CACHE["deposit_hold_total_usd"] = None
            KRAKEN_CACHE["last_error_deposit_status"] = None
        else:
            try:
                deposit_events, hit_cap = await asyncio.to_thread(
                    _fetch_usd_deposit_events_with_pagination,
                    refresh_now,
                )
                deposit_hold_rows_usd = _estimate_usd_hold_rows_from_deposits(deposit_events, refresh_now)
                deposit_hold_total_usd = sum(
                    (
                        _kraken_decimal_or_none(row.get("amount_usd")) or Decimal("0")
                        for row in deposit_hold_rows_usd
                    ),
                    Decimal("0"),
                )
            except Exception as e:
                had_success = KRAKEN_CACHE.get("last_success_at_deposit_status") is not None
                KRAKEN_CACHE["deposit_estimator_status"] = "stale" if had_success else "error"
                KRAKEN_CACHE["last_error_deposit_status"] = str(e)[:200]
                logger.warning(
                    "Kraken deposit hold estimate refresh failed: %s",
                    KRAKEN_CACHE["last_error_deposit_status"],
                )
                if not had_success:
                    KRAKEN_CACHE["deposit_hold_rows_usd"] = []
                    KRAKEN_CACHE["deposit_hold_total_usd"] = None
            else:
                KRAKEN_CACHE["deposit_estimator_status"] = "ok"
                KRAKEN_CACHE["deposit_hold_rows_usd"] = deposit_hold_rows_usd
                KRAKEN_CACHE["deposit_hold_total_usd"] = deposit_hold_total_usd
                KRAKEN_CACHE["last_success_at_deposit_status"] = now_utc_iso()
                KRAKEN_CACHE["last_error_deposit_status"] = None

                if hit_cap:
                    logger.warning(
                        "Kraken DepositStatus pagination cap hit (%s pages); hold estimate may be incomplete",
                        KRAKEN_DEPOSIT_STATUS_MAX_PAGES,
                    )

                if deposit_hold_rows_usd:
                    first = deposit_hold_rows_usd[0]
                    logger.info(
                        "Kraken deposit hold estimate refreshed: deposits=%s active_rows=%s hold_total=%s next=%s %s offset_hours=%s",
                        len(deposit_events),
                        len(deposit_hold_rows_usd),
                        _format_usd_est_amount_int(deposit_hold_total_usd),
                        first.get("unlock_at_iso"),
                        _format_usd_row_amount(_kraken_decimal_or_none(first.get("amount_usd"))),
                        KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS,
                    )
                else:
                    logger.info(
                        "Kraken deposit hold estimate refreshed: deposits=%s active_rows=0 hold_total=$0 offset_hours=%s",
                        len(deposit_events),
                        KRAKEN_HOLD_ESTIMATE_OFFSET_HOURS,
                    )

                source_counts: dict[str, int] = {}
                for ev in deposit_events:
                    src = str(ev.get("processed_at_source") or "unknown")
                    source_counts[src] = source_counts.get(src, 0) + 1
                if source_counts:
                    logger.info(
                        "Kraken deposit timestamp sources used (anchor=%s): %s",
                        KRAKEN_DEPOSIT_TIME_ANCHOR,
                        ", ".join(f"{k}={source_counts[k]}" for k in sorted(source_counts.keys())),
                    )

        after_snapshot = _kraken_state_snapshot()
        after_block = _format_kraken_dashboard_block(after_snapshot, render_now=refresh_now)
        should_refresh_panels = after_block != before_block
        if KRAKEN_DEPOSIT_ESTIMATOR_MODE == "ui":
            current_bucket = _kraken_countdown_refresh_bucket(after_snapshot, refresh_now)
            prev_bucket = KRAKEN_CACHE.get("countdown_refresh_bucket")
            force_countdown_refresh = current_bucket is not None and current_bucket != prev_bucket
            KRAKEN_CACHE["countdown_refresh_bucket"] = current_bucket

    if should_refresh_panels or force_countdown_refresh:
        try:
            await update_all_panels_for_app(app)
        except Exception:
            logger.warning("Kraken-triggered panel refresh failed", exc_info=True)


async def kraken_refresh_loop(app: Application) -> None:
    logger.info(
        "Kraken dashboard refresh loop started (asset=%s interval=%ss)",
        KRAKEN_ASSET,
        KRAKEN_REFRESH_SECONDS,
    )
    try:
        while True:
            try:
                await refresh_kraken_cache_once(app)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Unexpected Kraken refresh loop error", exc_info=True)

            await asyncio.sleep(KRAKEN_REFRESH_SECONDS)
    except asyncio.CancelledError:
        logger.info("Kraken dashboard refresh loop stopped")
        raise


# =========================
# UI BUILDERS
# =========================

def build_panel_text(total_cents: int) -> str:
    fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)
    kraken_block = _format_kraken_dashboard_block(_kraken_state_snapshot())

    pending = pending_confirmations_count()
    pending_block = ""

    if pending > 0:
        if pending == 1:
            pending_block = (
                "\n危 1 movimiento no confirmado 危\n"
                "(se autoconfirma en 24h)\n"
            )
        else:
            pending_block = (
                f"\n危 {pending} movimientos no confirmados 危\n"
                "(se autoconfirman en 24h)\n"
            )

    return (
        f"{kraken_block}\n\n"
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


# =========================
# DELETE HELPERS / NOTIFY
# =========================

async def delete_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, seconds: int):
    try:
        await asyncio.sleep(seconds)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        logger.debug(
            "delete_later failed for chat_id=%s message_id=%s",
            chat_id,
            message_id,
            exc_info=True,
        )


async def notify(context: ContextTypes.DEFAULT_TYPE, text: str):
    # Best-effort notifications; never crash the bot if notification fails
    for uid in get_participants():
        try:
            msg = await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            context.application.create_task(delete_later(context, uid, msg.message_id, NOTIFY_DELETE_SECONDS))
        except Exception:
            logger.warning("notify failed for participant user_id=%s", uid, exc_info=True)


# =========================
# PANEL RENDERING (FIXES DUPLICATION)
# =========================
# Core rule:
# - For a chat_id, we keep exactly ONE panel_message_id.
# - If edit fails, we attempt to delete the old message (best-effort) and create a new one, updating the stored id.
# This prevents "duplicate dashboards".

async def edit_panel_for_app(chat_id: int, app: Application, text: str, reply_markup: InlineKeyboardMarkup):
    st = get_chat_state(chat_id)
    panel_message_id = st.get("panel_message_id")

    if not panel_message_id:
        await send_or_update_panel_for_app(chat_id, app)
        return

    mode = st.get("panel_mode", "text")

    # Try banner caption edit if we believe it's banner
    if mode == "banner" and BANNER_URL:
        try:
            await app.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=panel_message_id,
                caption=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception:
            set_panel_mode(chat_id, "text")

    # Try text edit
    try:
        await app.bot.edit_message_text(
            chat_id=chat_id,
            message_id=panel_message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        return
    except Exception:
        # maybe it's actually a banner; try caption edit
        if BANNER_URL:
            try:
                await app.bot.edit_message_caption(
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

    # If we get here, editing failed. To prevent duplicates:
    # best-effort delete old panel message, then create a fresh one and store its id.
    try:
        await app.bot.delete_message(chat_id=chat_id, message_id=panel_message_id)
    except Exception:
        pass

    set_panel_message_id(chat_id, None)
    await send_or_update_panel_for_app(chat_id, app)


async def edit_panel(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: InlineKeyboardMarkup):
    await edit_panel_for_app(chat_id, context.application, text=text, reply_markup=reply_markup)


async def send_or_update_panel_for_app(chat_id: int, app: Application):
    st = get_chat_state(chat_id)
    g = get_global_state()
    total_cents = g["total_cents"]

    text = build_panel_text(total_cents)
    kb = build_panel_keyboard()

    panel_message_id = st.get("panel_message_id")
    if panel_message_id:
        # Try edit; if it fails, edit_panel will delete & recreate
        await edit_panel_for_app(chat_id, app, text=text, reply_markup=kb)
        return

    # Create new panel: banner if configured, otherwise text
    if BANNER_URL:
        try:
            msg = await app.bot.send_photo(
                chat_id=chat_id,
                photo=BANNER_URL,
                caption=text,
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
                disable_notification=True,
            )
            set_panel_mode(chat_id, "banner")
            set_panel_message_id(chat_id, msg.message_id)
            return
        except Exception:
            set_panel_mode(chat_id, "text")

    msg = await app.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
        disable_notification=True,
    )
    set_panel_message_id(chat_id, msg.message_id)


async def send_or_update_panel(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await send_or_update_panel_for_app(chat_id, context.application)


async def update_all_panels_for_app(app: Application):
    # Update or create exactly one panel per participant
    for uid in get_participants():
        try:
            await send_or_update_panel_for_app(uid, app)
        except Exception:
            logger.warning("panel update failed for user_id=%s", uid, exc_info=True)


async def update_all_panels(context: ContextTypes.DEFAULT_TYPE):
    await update_all_panels_for_app(context.application)


# =========================
# CONFIRMATION MESSAGES
# =========================

async def send_confirmation_request_to_confirmer(
    context: ContextTypes.DEFAULT_TYPE,
    movement_id: int,
    amount_cents: int,
    actor_id: int,
):
    confirmer_id = get_confirmer_id()
    if not confirmer_id:
        return

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
        set_confirm_message_refs(movement_id, confirmer_id, msg.message_id)

        # best-effort delete at 24h (also cleaned on interactions)
        context.application.create_task(delete_later(context, confirmer_id, msg.message_id, CONFIRM_WINDOW_SECONDS))
    except Exception:
        logger.warning(
            "failed to send confirmation request movement_id=%s confirmer_id=%s actor_id=%s",
            movement_id,
            confirmer_id,
            actor_id,
            exc_info=True,
        )


# =========================
# UNDO
# =========================

async def _undo_last_legacy(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


# Transactional undo implementation (overrides legacy helper above).
async def undo_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cleanup_expired_confirmations(context)

    async with STATE_LOCK:
        result = undo_last_movement_tx()

    if not result:
        await notify(context, "<b>Yozu Tracker</b>\nNo hay nada que deshacer lol.")
        return

    if result["kind"] == "add":
        chat_id = result.get("confirm_chat_id")
        msg_id = result.get("confirm_message_id")
        if chat_id and msg_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                logger.debug(
                    "failed to delete confirmation message chat_id=%s message_id=%s",
                    chat_id,
                    msg_id,
                    exc_info=True,
                )

        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                f"Se deshizo: <code>${cents_to_money_str(int(result['amount_cents']))}</code>\n"
                f"Total: <code>${cents_to_money_str(int(result['new_total_cents']))}</code>"
            ),
        )
        await update_all_panels(context)
        return

    if result["kind"] == "release":
        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                "Se deshizo un <b>Release</b>.\n"
                f"Total restaurado: <code>${cents_to_money_str(int(result['restored_total_cents']))}</code>"
            ),
        )
        await update_all_panels(context)
        return

    await notify(context, "<b>Yozu Tracker</b>\nNo se pudo deshacer (tipo desconocido).")


# =========================
# START
# =========================

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

    # IMPORTANT FIX: create/update only THIS user's panel first (no duplicates),
    # then update others (edit existing or create if missing).
    await send_or_update_panel(update.effective_chat.id, context)
    await update_all_panels(context)


# =========================
# BUTTON HANDLER
# =========================

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    # IMPORTANT FIX: Always answer query ASAP to avoid the "loading spinner forever",
    # even if we later reject due to permissions.
    try:
        await query.answer()
    except Exception:
        pass

    user = update.effective_user
    if not user or not is_participant(user.id):
        return

    await cleanup_expired_confirmations(context)

    data = query.data or ""

    if data.startswith("add:"):
        add_amount = data.split(":", 1)[1]
        add_cents = money_to_cents(add_amount)

        async with STATE_LOCK:
            movement_id, total_cents = add_amount_with_confirmation(user.id, add_cents)

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

        await update_all_panels(context)
        return

    if data == "custom":
        AWAITING_CUSTOM_AMOUNT.add(user.id)

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
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return

        try:
            movement_id = int(data.split(":", 1)[1])
        except Exception:
            return

        async with STATE_LOCK:
            confirm_result = confirm_movement_tx(movement_id, user.id)

        status = confirm_result["status"]
        if status == "missing":
            try:
                await query.edit_message_text("Confirmación ya no existe.", parse_mode=ParseMode.HTML)
            except Exception:
                pass
            await update_all_panels(context)
            return

        if status == "already_confirmed":
            await try_delete_confirm_message(context, movement_id)
            await update_all_panels(context)
            return

        await try_delete_confirm_message(context, movement_id)

        amount_cents = int(confirm_result["amount_cents"])
        actor_id = int(confirm_result["actor_id"])

        # Notify actor (auto-delete normal)
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
        async with STATE_LOCK:
            release_info = release_current_total(user.id)

        if not release_info:
            # Non-toxic, stable behavior: do nothing besides notifying
            await notify(
                context,
                "La cantidad que intentas retirar es <b>$0.00</b>, Magistral.",
            )
            await update_all_panels(context)
            return

        total_cents = int(release_info["total_cents"])
        fee_cents = int(release_info["fee_cents"])
        network_fee_cents = int(release_info["network_fee_cents"])
        net_cents = int(release_info["net_cents"])

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

        AWAITING_CUSTOM_AMOUNT.discard(user.id)

        # Show release summary in the current chat panel momentarily, then back
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
        return

    if data == "back":
        g = get_global_state()
        total_cents = g["total_cents"]
        await edit_panel(
            update.effective_chat.id,
            context,
            text=build_panel_text(total_cents),
            reply_markup=build_panel_keyboard(),
        )
        return


# =========================
# MESSAGE HANDLER (CUSTOM AMOUNT)
# =========================

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
            "Número inválido. Envía algo como <code>420</code> o <code>420.50</code>, sin letras ni símbolos.",
            parse_mode=ParseMode.HTML,
        )
        context.application.create_task(delete_later(context, chat_id, msg.message_id, NOTIFY_DELETE_SECONDS))
        return

    AWAITING_CUSTOM_AMOUNT.discard(user.id)

    async with STATE_LOCK:
        movement_id, total_cents = add_amount_with_confirmation(user.id, add_cents)

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


# =========================
# MAIN
# =========================

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if err is None:
        logger.error("Unhandled Telegram error (no exception object)")
        return

    logger.error(
        "Unhandled Telegram error: %s",
        err,
        exc_info=(type(err), err, err.__traceback__),
    )


async def on_app_init(app: Application):
    global KRAKEN_REFRESH_TASK

    if not KRAKEN_CACHE["enabled"]:
        logger.info("Kraken balance dashboard line enabled in placeholder mode (missing Kraken API creds)")
        return

    if KRAKEN_REFRESH_TASK and not KRAKEN_REFRESH_TASK.done():
        return

    KRAKEN_REFRESH_TASK = app.create_task(kraken_refresh_loop(app))


async def on_app_shutdown(app: Application):
    global KRAKEN_REFRESH_TASK

    task = KRAKEN_REFRESH_TASK
    KRAKEN_REFRESH_TASK = None
    if not task:
        return

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN env var")

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Avoid leaking the Telegram bot token via HTTP request URLs in INFO logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_app_init)
        .post_shutdown(on_app_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    app.add_error_handler(on_error)

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()


