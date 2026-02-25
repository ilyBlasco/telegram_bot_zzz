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
import re
import html as html_lib
import unicodedata
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from email.utils import parseaddr
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

GMAIL_ZELLE_ENABLED = (os.getenv("GMAIL_ZELLE_ENABLED", "0").strip() == "1")
_GMAIL_ZELLE_MODE_DEFAULT = "shadow"
GMAIL_ZELLE_MODE = (os.getenv("GMAIL_ZELLE_MODE", _GMAIL_ZELLE_MODE_DEFAULT).strip().lower() or _GMAIL_ZELLE_MODE_DEFAULT)
if GMAIL_ZELLE_MODE not in {"shadow", "live"}:
    GMAIL_ZELLE_MODE = _GMAIL_ZELLE_MODE_DEFAULT
GMAIL_ZELLE_POLL_SECONDS = max(15, int(os.getenv("GMAIL_ZELLE_POLL_SECONDS", "60")))
GMAIL_ZELLE_LABEL_NAME = (os.getenv("GMAIL_ZELLE_LABEL_NAME", "zelle-auto").strip() or "zelle-auto")
_GMAIL_ZELLE_ACTOR_USER_ID_RAW = (os.getenv("GMAIL_ZELLE_ACTOR_USER_ID", "").strip() or "")
_GMAIL_ZELLE_ACTOR_USER_ID_INVALID = False
try:
    GMAIL_ZELLE_ACTOR_USER_ID = int(_GMAIL_ZELLE_ACTOR_USER_ID_RAW) if _GMAIL_ZELLE_ACTOR_USER_ID_RAW else None
except Exception:
    GMAIL_ZELLE_ACTOR_USER_ID = None
    _GMAIL_ZELLE_ACTOR_USER_ID_INVALID = True
GMAIL_ZELLE_AUTO_PROMOTE_DAYS = max(1, int(os.getenv("GMAIL_ZELLE_AUTO_PROMOTE_DAYS", "14")))
GMAIL_ZELLE_NOTIFY_UNKNOWN_EVERY_MATCH = (os.getenv("GMAIL_ZELLE_NOTIFY_UNKNOWN_EVERY_MATCH", "1").strip() != "0")
GMAIL_ZELLE_CREDENTIALS_PATH = (os.getenv("GMAIL_ZELLE_CREDENTIALS_PATH", "secrets/gmail_client_secret.json").strip() or "secrets/gmail_client_secret.json")
GMAIL_ZELLE_TOKEN_PATH = (os.getenv("GMAIL_ZELLE_TOKEN_PATH", "secrets/gmail_token.json").strip() or "secrets/gmail_token.json")
GMAIL_ZELLE_QUERY_EXTRA = (os.getenv("GMAIL_ZELLE_QUERY_EXTRA", "").strip() or "")
GMAIL_ZELLE_SUBJECT_REGEX = (os.getenv("GMAIL_ZELLE_SUBJECT_REGEX", "").strip() or "")
GMAIL_ZELLE_AMOUNT_REGEX = (os.getenv("GMAIL_ZELLE_AMOUNT_REGEX", "").strip() or "")
GMAIL_ZELLE_SCOPES = ("https://www.googleapis.com/auth/gmail.readonly",)
GMAIL_ZELLE_NOTIFY_DELETE_SECONDS = max(60, int(os.getenv("GMAIL_ZELLE_NOTIFY_DELETE_SECONDS", "172800")))
TRACKING_MODE_DEFAULT = (os.getenv("TRACKING_MODE_DEFAULT", "auto").strip().lower() or "auto")
if TRACKING_MODE_DEFAULT not in {"auto", "manual"}:
    TRACKING_MODE_DEFAULT = "auto"
GMAIL_ZELLE_BASK_ONLY = (os.getenv("GMAIL_ZELLE_BASK_ONLY", "1").strip() != "0")
GMAIL_ZELLE_BASK_ALLOWED_SENDER_EMAILS = (
    os.getenv("GMAIL_ZELLE_BASK_ALLOWED_SENDER_EMAILS", "customersupport@baskbank.com").strip()
    or "customersupport@baskbank.com"
)
GMAIL_ZELLE_BASK_ALLOWED_SENDER_EMAILS_SET = {
    str(x).strip().lower()
    for x in GMAIL_ZELLE_BASK_ALLOWED_SENDER_EMAILS.replace(";", ",").split(",")
    if str(x).strip()
}
GMAIL_ZELLE_BASK_EXPECTED_TO_CONTAINS = (os.getenv("GMAIL_ZELLE_BASK_EXPECTED_TO_CONTAINS", "").strip() or "")
GMAIL_ZELLE_BASK_PARSER_STRICT = (os.getenv("GMAIL_ZELLE_BASK_PARSER_STRICT", "1").strip() != "0")

# Delete notifications + small bot messages after N seconds (prod: 10800 for 3h)
NOTIFY_DELETE_SECONDS = int(os.getenv("NOTIFY_DELETE_SECONDS", "10"))
TRACKING_MODE_NOTIFY_DELETE_SECONDS = 15
ADMIN_REVERSE_UI_LOOKBACK_HOURS = 24

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
GMAIL_ZELLE_TASK: asyncio.Task | None = None
_KRAKEN_DISPLAY_TZINFO = None
_KRAKEN_DISPLAY_TZ_WARNED = False
_KRAKEN_DEPOSIT_TIME_ANCHOR_INVALID_WARNED = False
_KRAKEN_HOLD_ESTIMATE_OFFSET_WARNED = False
_GMAIL_ZELLE_ACTOR_USER_ID_WARNED = False
_GMAIL_ZELLE_MODE_WARNED = False
_GMAIL_ZELLE_LABEL_ID_CACHE: str | None = None
_GMAIL_ZELLE_IMPORT_ERROR_WARNED = False

GMAIL_ZELLE_STATUS: dict = {
    "enabled": GMAIL_ZELLE_ENABLED,
    "mode": GMAIL_ZELLE_MODE,
    "tracking_mode": TRACKING_MODE_DEFAULT,
    "last_poll_started_at": None,
    "last_poll_success_at": None,
    "last_poll_error_at": None,
    "last_poll_error_text": None,
    "last_cycle_status": "idle" if GMAIL_ZELLE_ENABLED else "disabled",
}

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

GMAIL_SENDER_LIST_PAGE_SIZE = 10
ADMIN_REVERSE_PAGE_SIZE = 5


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

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gmail_processed_messages (
                gmail_message_id TEXT PRIMARY KEY,
                gmail_thread_id TEXT,
                sender_email TEXT,
                subject TEXT,
                internal_date_ms INTEGER,
                parsed_amount_cents INTEGER,
                parsed_sender_name TEXT,
                status TEXT NOT NULL,
                movement_id INTEGER,
                processed_at TEXT NOT NULL,
                raw_date_header TEXT,
                notes TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gmail_sender_trust (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_email TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                seen_count INTEGER NOT NULL DEFAULT 1,
                auto_promote_at TEXT,
                approved_at TEXT,
                approved_by INTEGER,
                blocked_at TEXT,
                blocked_by INTEGER,
                last_matched_amount_cents INTEGER,
                last_matched_message_id TEXT,
                display_name_hint TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                updated_by INTEGER
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gmail_reversals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gmail_message_id TEXT NOT NULL,
                original_movement_id INTEGER NOT NULL,
                reversal_movement_id INTEGER NOT NULL,
                payer_key TEXT,
                payer_display TEXT,
                amount_cents INTEGER NOT NULL,
                reason TEXT,
                reversed_by INTEGER NOT NULL,
                reversed_at TEXT NOT NULL
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


def _participant_display_name(first_name: str | None, username: str | None, user_id: int) -> str:
    first = str(first_name or "").strip()
    if first:
        return first
    uname = str(username or "").strip()
    if uname:
        return f"@{uname}"
    return str(user_id)


def get_participant_display_name_map() -> dict[int, str]:
    with db() as conn:
        rows = conn.execute(
            "SELECT user_id, first_name, username FROM participants"
        ).fetchall()
    out: dict[int, str] = {}
    for row in rows:
        uid = int(row["user_id"])
        out[uid] = _participant_display_name(row["first_name"], row["username"], uid)
    return out


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


def _normalize_sender_email(email_text: str | None) -> str:
    return str(email_text or "").strip().lower()


def _normalize_identity_key(value: str | None) -> str:
    return str(value or "").strip().lower()


def _normalize_payer_key(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def _json_dumps_compact(data: dict | None) -> str | None:
    if not data:
        return None
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return None


def _json_loads_object_or_none(text: str | None) -> dict | None:
    raw = str(text or "").strip()
    if not raw or not raw.startswith("{"):
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def get_app_setting(key: str) -> str | None:
    with db() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (str(key),)).fetchone()
        if not row:
            return None
        return str(row["value"])


def _normalize_tracking_mode(value: str | None) -> str:
    mode = str(value or "").strip().lower()
    if mode not in {"auto", "manual"}:
        return TRACKING_MODE_DEFAULT
    return mode


def get_tracking_mode() -> str:
    raw = get_app_setting("tracking_mode")
    if raw is None:
        return TRACKING_MODE_DEFAULT
    return _normalize_tracking_mode(raw)


def set_tracking_mode_tx(mode: str, updated_by: int | None) -> dict:
    normalized = _normalize_tracking_mode(mode)
    now_iso = now_utc_iso()
    with db() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = 'tracking_mode'").fetchone()
        previous = _normalize_tracking_mode(str(row["value"])) if row and row["value"] is not None else TRACKING_MODE_DEFAULT
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at, updated_by)
            VALUES('tracking_mode', ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by
            """,
            (normalized, now_iso, updated_by),
        )
    return {"mode": normalized, "previous_mode": previous, "changed": normalized != previous}


def _gmail_bask_metadata_from_parsed(parsed: dict) -> dict:
    return {
        "source_kind": "bask_zelle",
        "confirmation_number": str(parsed.get("confirmation_number") or ""),
        "payer_key": str(parsed.get("payer_key") or ""),
        "payer_display": str(parsed.get("payer_display") or ""),
        "bank_sender_email": str(parsed.get("bank_sender_email") or ""),
        "to_line": str(parsed.get("to_line") or ""),
        "parser": "bask_strict_v1",
    }


def _gmail_event_dt_from_parsed(parsed: dict) -> datetime:
    event_iso = _parse_iso_utc_or_none(str(parsed.get("event_time_iso") or ""))
    if event_iso:
        return event_iso
    internal_date_ms = parsed.get("internal_date_ms")
    try:
        if internal_date_ms is not None:
            return datetime.fromtimestamp(int(internal_date_ms) / 1000, tz=timezone.utc)
    except Exception:
        pass
    return now_utc()


def _format_gmail_event_time_display(parsed: dict) -> str:
    return _format_kraken_display_time_short(_gmail_event_dt_from_parsed(parsed))


def _find_gmail_processed_by_bask_confirmation_in_conn(
    conn: sqlite3.Connection, confirmation_number: str, amount_cents: int
) -> sqlite3.Row | None:
    conf = str(confirmation_number or "").strip()
    if not conf or amount_cents <= 0:
        return None
    rows = conn.execute(
        """
        SELECT gmail_message_id, parsed_amount_cents, notes
        FROM gmail_processed_messages
        WHERE parsed_amount_cents = ?
          AND notes IS NOT NULL
        """,
        (amount_cents,),
    ).fetchall()
    for row in rows:
        meta = _json_loads_object_or_none(row["notes"])
        if not meta:
            continue
        if str(meta.get("source_kind") or "") != "bask_zelle":
            continue
        if str(meta.get("confirmation_number") or "").strip() != conf:
            continue
        return row
    return None


def _insert_gmail_processed_message_in_conn(
    conn: sqlite3.Connection,
    *,
    parsed: dict,
    status: str,
    movement_id: int | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO gmail_processed_messages(
            gmail_message_id, gmail_thread_id, sender_email, subject, internal_date_ms,
            parsed_amount_cents, parsed_sender_name, status, movement_id, processed_at,
            raw_date_header, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(parsed.get("gmail_message_id") or ""),
            str(parsed.get("thread_id") or ""),
            _normalize_sender_email(parsed.get("sender_email")),
            str(parsed.get("subject") or ""),
            int(parsed.get("internal_date_ms") or 0) if parsed.get("internal_date_ms") is not None else None,
            int(parsed.get("amount_cents") or 0) if parsed.get("amount_cents") is not None else None,
            str(parsed.get("sender_display_name") or ""),
            status,
            movement_id,
            now_utc_iso(),
            str(parsed.get("date_header") or ""),
            str(notes or "") if notes else None,
        ),
    )


def add_amount_auto_confirmed(actor_id: int, add_cents: int) -> tuple[int, int]:
    """
    Atomically updates total and logs an ADD movement without creating a pending confirmation.
    Returns (movement_id, new_total_cents).
    """
    created_iso = now_utc_iso()
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
    return movement_id, total_cents


def get_gmail_sender_trust_by_id(sender_trust_id: int) -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM gmail_sender_trust WHERE id = ?",
            (sender_trust_id,),
        ).fetchone()


def sendertrust_action_tx(sender_trust_id: int, action: str, acting_user_id: int) -> dict:
    now_iso = now_utc_iso()
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM gmail_sender_trust WHERE id = ?",
            (sender_trust_id,),
        ).fetchone()
        if not row:
            return {"status": "missing"}

        sender_email = str(row["sender_email"] or "")
        current_state = str(row["state"] or "")

        if action == "approve":
            conn.execute(
                """
                UPDATE gmail_sender_trust
                SET state = 'approved',
                    approved_at = ?,
                    approved_by = ?,
                    blocked_at = NULL,
                    blocked_by = NULL,
                    auto_promote_at = NULL
                WHERE id = ?
                """,
                (now_iso, acting_user_id, sender_trust_id),
            )
            return {
                "status": "approved",
                "sender_email": sender_email,
                "previous_state": current_state,
            }

        if action == "block":
            conn.execute(
                """
                UPDATE gmail_sender_trust
                SET state = 'blocked',
                    blocked_at = ?,
                    blocked_by = ?,
                    auto_promote_at = NULL
                WHERE id = ?
                """,
                (now_iso, acting_user_id, sender_trust_id),
            )
            return {
                "status": "blocked",
                "sender_email": sender_email,
                "previous_state": current_state,
            }

        if action == "ignore":
            return {
                "status": "ignored",
                "sender_email": sender_email,
                "previous_state": current_state,
            }

        return {"status": "invalid_action"}


def _gmail_zelle_status_snapshot() -> dict:
    return {
        "enabled": bool(GMAIL_ZELLE_STATUS.get("enabled")),
        "mode": str(GMAIL_ZELLE_STATUS.get("mode") or GMAIL_ZELLE_MODE),
        "tracking_mode": str(GMAIL_ZELLE_STATUS.get("tracking_mode") or get_tracking_mode()),
        "last_poll_started_at": GMAIL_ZELLE_STATUS.get("last_poll_started_at"),
        "last_poll_success_at": GMAIL_ZELLE_STATUS.get("last_poll_success_at"),
        "last_poll_error_at": GMAIL_ZELLE_STATUS.get("last_poll_error_at"),
        "last_poll_error_text": str(GMAIL_ZELLE_STATUS.get("last_poll_error_text") or ""),
        "last_cycle_status": str(GMAIL_ZELLE_STATUS.get("last_cycle_status") or "idle"),
    }


def _format_elapsed_ago_short(dt: datetime | None, now_dt: datetime | None = None) -> str:
    if dt is None:
        return "never"
    now_dt = now_dt or now_utc()
    seconds = max(0, int((now_dt - dt).total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def get_gmail_sender_trust_counts() -> dict[str, int]:
    counts = {"approved": 0, "quarantine": 0, "blocked": 0}
    try:
        with db() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS c FROM gmail_sender_trust GROUP BY state"
            ).fetchall()
    except Exception:
        return counts

    for row in rows:
        state = str(row["state"] or "")
        if state in counts:
            counts[state] = int(row["c"] or 0)
    return counts


def _format_gmail_footer_status_block() -> str:
    snap = _gmail_zelle_status_snapshot()
    counts = get_gmail_sender_trust_counts()
    tracking_mode = _normalize_tracking_mode(str(snap.get("tracking_mode") or TRACKING_MODE_DEFAULT))
    mode_suffix = " (MANUAL)" if tracking_mode == "manual" else ""

    if not snap["enabled"] or not GMAIL_ZELLE_ENABLED:
        line1 = f"<i>Gmail Autovalidation</i>: <b>OFF</b>{mode_suffix}"
    else:
        cycle_status = str(snap.get("last_cycle_status") or "idle")
        state_label = "ERR" if cycle_status == "error" else "ON"
        line1 = f"<i>Gmail Autovalidation</i>: <b>{state_label}</b>{mode_suffix}"

    line2 = (
        f"<i>Senders</i>: ✅ {counts['approved']} "
        f"&#183; ☣️ {counts['quarantine']}"
    )
    return f"{line1}\n\n{line2}"


def _gmail_sender_state_rank(state: str) -> int:
    return {"approved": 0, "quarantine": 1, "blocked": 2}.get(state, 9)


def _format_sender_list_last_seen(last_seen_dt: datetime | None) -> str:
    if last_seen_dt is None:
        return "--"
    local_dt = last_seen_dt.astimezone(_get_kraken_display_tzinfo())
    hour_12 = local_dt.strftime("%I").lstrip("0") or "12"
    return f"{local_dt.strftime('%b')} {local_dt.day} {hour_12}:{local_dt.strftime('%M')} {local_dt.strftime('%p')}"


def _sender_state_badge(state: str) -> str:
    return {
        "approved": "✅",
        "quarantine": "☣️",
    }.get(state, "•")


def list_ranked_gmail_senders(page: int, page_size: int = GMAIL_SENDER_LIST_PAGE_SIZE) -> tuple[list[dict], bool, bool]:
    page = max(0, int(page))
    page_size = max(1, int(page_size))
    now_dt = now_utc()
    age_cutoff = now_dt - timedelta(days=14)
    matched_statuses = (
        "added",
        "shadow_approved_match",
        "quarantined_unknown_sender",
        "blocked_sender",
    )
    placeholders = ",".join(["?"] * len(matched_statuses))

    with db() as conn:
        trust_rows = conn.execute(
            """
            SELECT id, sender_email, state, first_seen_at, last_seen_at, seen_count, last_matched_amount_cents, display_name_hint
            FROM gmail_sender_trust
            """
        ).fetchall()

        avg_rows = conn.execute(
            f"""
            SELECT sender_email, AVG(parsed_amount_cents) AS avg_amount_cents
            FROM gmail_processed_messages
            WHERE status IN ({placeholders})
              AND parsed_amount_cents IS NOT NULL
              AND parsed_amount_cents > 0
            GROUP BY sender_email
            """,
            matched_statuses,
        ).fetchall()

    avg_by_sender: dict[str, int] = {}
    for row in avg_rows:
        sender_email = _normalize_sender_email(row["sender_email"])
        raw_avg = row["avg_amount_cents"]
        avg_cents = 0
        if raw_avg is not None:
            try:
                avg_cents = int(float(raw_avg))
            except Exception:
                avg_cents = 0
        avg_by_sender[sender_email] = max(0, avg_cents)

    ranked_rows: list[dict] = []
    for row in trust_rows:
        sender_email = _normalize_sender_email(row["sender_email"])
        state = str(row["state"] or "quarantine")
        if state == "blocked":
            continue
        seen_count = int(row["seen_count"] or 0)
        first_seen_dt = _parse_iso_utc_or_none(str(row["first_seen_at"] or ""))
        last_seen_dt = _parse_iso_utc_or_none(str(row["last_seen_at"] or ""))
        display_name = str(row["display_name_hint"] or "").strip() or sender_email
        avg_amount_cents = avg_by_sender.get(sender_email)
        if avg_amount_cents is None:
            avg_amount_cents = max(0, int(row["last_matched_amount_cents"] or 0))

        age_bonus_applied = bool(first_seen_dt and first_seen_dt <= age_cutoff)
        avg_amount_usd = max(0, avg_amount_cents // 100)
        score = (seen_count * 100) + avg_amount_usd + (1000 if age_bonus_applied else 0)
        last_seen_sort = int(last_seen_dt.timestamp()) if last_seen_dt else 0

        ranked_rows.append(
            {
                "sender_trust_id": int(row["id"]),
                "sender_email": sender_email,
                "display_name": display_name,
                "state": state,
                "seen_count": seen_count,
                "first_seen_at": first_seen_dt,
                "last_seen_at": last_seen_dt,
                "avg_amount_cents": avg_amount_cents,
                "score": score,
                "age_bonus_applied": age_bonus_applied,
                "_state_rank": _gmail_sender_state_rank(state),
                "_last_seen_sort": last_seen_sort,
            }
        )

    ranked_rows.sort(
        key=lambda r: (
            r["_state_rank"],
            -int(r["score"]),
            -int(r["_last_seen_sort"]),
            str(r["sender_email"]),
        )
    )

    if ranked_rows:
        max_page = (len(ranked_rows) - 1) // page_size
        page = min(page, max_page)
    else:
        page = 0

    start = page * page_size
    end = start + page_size
    page_rows = ranked_rows[start:end]
    has_prev = page > 0
    has_next = end < len(ranked_rows)
    return page_rows, has_prev, has_next


def build_senders_list_text(page: int, viewer_id: int) -> tuple[str, bool, bool]:
    _ = viewer_id  # Both participants can view; kept for future role-specific variants.
    page = max(0, int(page))
    rows, has_prev, has_next = list_ranked_gmail_senders(page)

    if not rows:
        return (
            "<b>📇 Zelle Senders (Top 10)</b>\n"
            f"<i>☣️ Nuevo / en observación · ✅ Establecido (auto-promueve tras {GMAIL_ZELLE_AUTO_PROMOTE_DAYS} días)</i>\n"
            "<i>Orden: estado → frecuencia → promedio → antigüedad</i>\n\n"
            "<i>No hay remitentes de Gmail/Zelle todavía.</i>",
            has_prev,
            has_next,
        )

    lines = [
        "<b>📇 Zelle Senders (Top 10)</b>",
        f"<i>☣️ Nuevo / en observación · ✅ Establecido (auto-promueve tras {GMAIL_ZELLE_AUTO_PROMOTE_DAYS} días)</i>",
        "<i>Orden: estado → frecuencia → promedio → antigüedad</i>",
        "",
    ]

    base_rank = (page * GMAIL_SENDER_LIST_PAGE_SIZE) + 1
    for idx, row in enumerate(rows):
        rank_num = base_rank + idx
        display_txt = _html_escape(str(row.get("display_name") or row.get("sender_email") or ""))
        state = str(row.get("state") or "")
        badge = _sender_state_badge(state)
        seen_count = int(row.get("seen_count") or 0)
        avg_cents = int(row.get("avg_amount_cents") or 0)
        avg_amount_txt = _format_usd_est_amount_int(Decimal(avg_cents) / Decimal(100))
        last_seen_txt = _format_sender_list_last_seen(row.get("last_seen_at"))

        lines.append(f"{rank_num}. {badge} <code>{display_txt}</code>")
        lines.append(
            f"   <i>freq</i>: {seen_count} &#183; <i>prom.</i>: {avg_amount_txt} &#183; <i>últ.</i>: {last_seen_txt}"
        )

    return "\n".join(lines), has_prev, has_next


def _gmail_auto_added_event_from_row(row: sqlite3.Row) -> dict:
    notes = _json_loads_object_or_none(row["notes"])
    payer_display = str(
        (notes or {}).get("payer_display")
        or (notes or {}).get("identity_display")
        or row["parsed_sender_name"]
        or row["sender_email"]
        or "Desconocido"
    ).strip()
    payer_key = str((notes or {}).get("payer_key") or (notes or {}).get("identity_key") or "").strip()
    confirmation_number = str((notes or {}).get("confirmation_number") or "").strip()

    event_dt = None
    try:
        internal_ms = row["internal_date_ms"]
        if internal_ms is not None:
            event_dt = datetime.fromtimestamp(int(internal_ms) / 1000, tz=timezone.utc)
    except Exception:
        event_dt = None
    if event_dt is None:
        event_dt = _parse_iso_utc_or_none(str(row["processed_at"] or "")) or now_utc()

    return {
        "gmail_message_id": str(row["gmail_message_id"] or ""),
        "movement_id": int(row["movement_id"]),
        "amount_cents": int(row["parsed_amount_cents"] or 0),
        "payer_display": payer_display,
        "payer_key": payer_key,
        "confirmation_number": confirmation_number,
        "event_dt": event_dt,
        "is_reversed": bool(row["reversal_id"] is not None),
        "reversed_at": _parse_iso_utc_or_none(str(row["reversed_at"] or "")) if row["reversed_at"] else None,
    }


def list_recent_gmail_auto_added_events(
    page: int,
    page_size: int = ADMIN_REVERSE_PAGE_SIZE,
) -> tuple[list[dict], bool, bool]:
    page = max(0, int(page))
    page_size = max(1, int(page_size))
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                g.gmail_message_id,
                g.movement_id,
                g.parsed_amount_cents,
                g.parsed_sender_name,
                g.sender_email,
                g.internal_date_ms,
                g.processed_at,
                g.notes,
                r.id AS reversal_id,
                r.reversed_at AS reversed_at
            FROM gmail_processed_messages g
            LEFT JOIN gmail_reversals r
              ON r.gmail_message_id = g.gmail_message_id
            WHERE g.status = 'added'
              AND g.movement_id IS NOT NULL
            ORDER BY COALESCE(g.internal_date_ms, 0) DESC, g.gmail_message_id DESC
            """
        ).fetchall()

    cutoff_dt = now_utc() - timedelta(hours=ADMIN_REVERSE_UI_LOOKBACK_HOURS)
    items = [
        item
        for item in (_gmail_auto_added_event_from_row(row) for row in rows)
        if (item.get("event_dt") or now_utc()) >= cutoff_dt
    ]
    if items:
        max_page = (len(items) - 1) // page_size
        page = min(page, max_page)
    else:
        page = 0
    start = page * page_size
    end = start + page_size
    page_items = items[start:end]
    return page_items, page > 0, end < len(items)


def get_recent_gmail_auto_added_event_by_message_id(gmail_message_id: str) -> dict | None:
    msg_id = str(gmail_message_id or "").strip()
    if not msg_id:
        return None
    with db() as conn:
        row = conn.execute(
            """
            SELECT
                g.gmail_message_id,
                g.movement_id,
                g.parsed_amount_cents,
                g.parsed_sender_name,
                g.sender_email,
                g.internal_date_ms,
                g.processed_at,
                g.notes,
                r.id AS reversal_id,
                r.reversed_at AS reversed_at
            FROM gmail_processed_messages g
            LEFT JOIN gmail_reversals r
              ON r.gmail_message_id = g.gmail_message_id
            WHERE g.gmail_message_id = ?
              AND g.status = 'added'
              AND g.movement_id IS NOT NULL
            LIMIT 1
            """,
            (msg_id,),
        ).fetchone()
    if not row:
        return None
    return _gmail_auto_added_event_from_row(row)


def build_admin_reverse_list_text(page: int, viewer_id: int) -> tuple[str, list[dict], bool, bool]:
    _ = viewer_id
    rows, has_prev, has_next = list_recent_gmail_auto_added_events(page=page, page_size=ADMIN_REVERSE_PAGE_SIZE)
    if not rows:
        return (
            (
                "<b>🛠 Admin Reverse</b>\n\n"
                f"<i>No hay transacciones auto-detectadas para revertir (últimas {ADMIN_REVERSE_UI_LOOKBACK_HOURS}h).</i>"
            ),
            [],
            has_prev,
            has_next,
        )

    lines = [
        "<b>🛠 Admin Reverse</b>",
        f"<i>Auto-ingest recientes (últimas {ADMIN_REVERSE_UI_LOOKBACK_HOURS}h)</i>",
        "",
    ]
    base_rank = (max(0, int(page)) * ADMIN_REVERSE_PAGE_SIZE) + 1
    for idx, row in enumerate(rows):
        rank_num = base_rank + idx
        payer_txt = _html_escape(str(row.get("payer_display") or "Desconocido"))
        amount_txt = cents_to_money_str(int(row.get("amount_cents") or 0))
        when_txt = _html_escape(_format_kraken_display_time_short(row["event_dt"]))
        conf = str(row.get("confirmation_number") or "")
        reversed_badge = " <i>[reverted]</i>" if row.get("is_reversed") else ""
        lines.append(f"{rank_num}. <code>{payer_txt}</code>{reversed_badge}")
        lines.append(f"   <i>${amount_txt} &#183; {when_txt}</i>")
        if conf:
            lines.append(f"   <i>Conf:</i> <code>{_html_escape(conf)}</code>")
    return "\n".join(lines), rows, has_prev, has_next


def build_admin_reverse_list_keyboard(page: int, rows: list[dict], has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    kb_rows: list[list[InlineKeyboardButton]] = []
    for row in rows:
        if row.get("is_reversed"):
            continue
        payer_name = str(row.get("payer_display") or "Desconocido")
        short_name = payer_name if len(payer_name) <= 18 else (payer_name[:15] + "...")
        amt = cents_to_money_str(int(row.get("amount_cents") or 0))
        kb_rows.append(
            [
                InlineKeyboardButton(
                    f"↩️ ${amt} · {short_name}",
                    callback_data=f"adminrev:select:{row['gmail_message_id']}",
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"adminrev:page:{max(0, page - 1)}"))
    if has_next:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"adminrev:page:{page + 1}"))
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("Volver", callback_data="back")])
    return InlineKeyboardMarkup(kb_rows)


def build_admin_reverse_confirm_text(event: dict) -> str:
    payer_txt = _html_escape(str(event.get("payer_display") or "Desconocido"))
    amt_txt = cents_to_money_str(int(event.get("amount_cents") or 0))
    when_txt = _html_escape(_format_kraken_display_time_short(event["event_dt"]))
    conf = str(event.get("confirmation_number") or "")
    reversed_line = "\n<i>Ya fue revertido.</i>" if event.get("is_reversed") else ""
    conf_line = f"\nConf: <code>{_html_escape(conf)}</code>" if conf else ""
    return (
        "<b>🛠 Admin Reverse</b>\n\n"
        f"Payer: <code>{payer_txt}</code>\n"
        f"Monto: <code>${amt_txt}</code>\n"
        f"Hora: <i>{when_txt}</i>"
        f"{conf_line}"
        f"{reversed_line}"
    )


def build_admin_reverse_confirm_keyboard(gmail_message_id: str, *, is_reversed: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if not is_reversed:
        rows.append(
            [
                InlineKeyboardButton("✅ Reverse", callback_data=f"adminrev:do:{gmail_message_id}"),
                InlineKeyboardButton("⛔ Reverse + block", callback_data=f"adminrev:block_and_do:{gmail_message_id}"),
            ]
        )
    rows.append([InlineKeyboardButton("⬅ Volver", callback_data="adminrev")])
    return InlineKeyboardMarkup(rows)


def admin_reverse_gmail_event_tx(gmail_message_id: str, acting_user_id: int, *, block_payer: bool = False) -> dict:
    msg_id = str(gmail_message_id or "").strip()
    if not msg_id:
        return {"status": "invalid"}

    now_iso = now_utc_iso()
    with db() as conn:
        row = conn.execute(
            """
            SELECT g.gmail_message_id, g.movement_id, g.parsed_amount_cents, g.notes
            FROM gmail_processed_messages g
            WHERE g.gmail_message_id = ?
              AND g.status = 'added'
              AND g.movement_id IS NOT NULL
            LIMIT 1
            """,
            (msg_id,),
        ).fetchone()
        if not row:
            return {"status": "missing"}

        existing_rev = conn.execute(
            "SELECT id FROM gmail_reversals WHERE gmail_message_id = ? LIMIT 1",
            (msg_id,),
        ).fetchone()
        if existing_rev:
            return {"status": "already_reversed"}

        original_movement_id = int(row["movement_id"])
        original_movement = conn.execute(
            "SELECT id FROM movements WHERE id = ? LIMIT 1",
            (original_movement_id,),
        ).fetchone()
        if not original_movement:
            return {"status": "missing_original_movement"}

        amount_cents = int(row["parsed_amount_cents"] or 0)
        if amount_cents <= 0:
            return {"status": "invalid_amount"}

        meta = _json_loads_object_or_none(row["notes"])
        payer_key = str((meta or {}).get("payer_key") or (meta or {}).get("identity_key") or "").strip()
        payer_display = str((meta or {}).get("payer_display") or (meta or {}).get("identity_display") or "").strip()

        g = conn.execute("SELECT total_cents, session_id FROM global_state WHERE id = 1").fetchone()
        current_total = int(g["total_cents"])
        current_session = int(g["session_id"])
        new_total = current_total - amount_cents
        if new_total < 0:
            new_total = 0

        conn.execute("UPDATE global_state SET total_cents = ? WHERE id = 1", (new_total,))
        cur = conn.execute(
            """
            INSERT INTO movements(session_id, kind, amount_cents, total_after_cents, actor_id, created_at)
            VALUES (?, 'reversal', ?, ?, ?, ?)
            """,
            (current_session, amount_cents, new_total, acting_user_id, now_iso),
        )
        reversal_movement_id = int(cur.lastrowid)

        conn.execute(
            """
            INSERT INTO gmail_reversals(
                gmail_message_id, original_movement_id, reversal_movement_id,
                payer_key, payer_display, amount_cents, reason, reversed_by, reversed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                msg_id,
                original_movement_id,
                reversal_movement_id,
                payer_key or None,
                payer_display or None,
                amount_cents,
                "admin_reverse",
                acting_user_id,
                now_iso,
            ),
        )

        blocked_applied = False
        if block_payer and payer_key:
            cur2 = conn.execute(
                """
                UPDATE gmail_sender_trust
                SET state = 'blocked',
                    blocked_at = ?,
                    blocked_by = ?,
                    auto_promote_at = NULL
                WHERE sender_email = ?
                """,
                (now_iso, acting_user_id, payer_key),
            )
            blocked_applied = cur2.rowcount > 0

        return {
            "status": "reversed",
            "gmail_message_id": msg_id,
            "amount_cents": amount_cents,
            "payer_key": payer_key,
            "payer_display": payer_display,
            "new_total_cents": new_total,
            "reversal_movement_id": reversal_movement_id,
            "blocked_applied": blocked_applied,
        }


def process_gmail_zelle_parsed_tx(parsed: dict, actor_id: int | None, mode: str) -> dict:
    """
    Dedupe + trust-policy + optional auto-add for a parsed Gmail Zelle candidate.
    Returns an action dict describing what happened.
    """
    now_dt = now_utc()
    now_iso = dt_to_iso(now_dt)
    sender_email = _normalize_sender_email(parsed.get("sender_email"))
    identity_key = _normalize_identity_key(parsed.get("identity_key")) or sender_email
    identity_display = str(
        parsed.get("identity_display")
        or parsed.get("payer_display")
        or parsed.get("sender_display_name")
        or sender_email
        or ""
    ).strip()
    source_kind = str(parsed.get("source_kind") or "")
    confirmation_number = str(parsed.get("confirmation_number") or "").strip()
    amount_cents = int(parsed.get("amount_cents") or 0)
    gmail_message_id = str(parsed.get("gmail_message_id") or "")
    sender_display_name = str(parsed.get("sender_display_name") or "")

    if not gmail_message_id:
        return {"status": "invalid_parsed", "reason": "missing_message_id"}
    if not sender_email:
        return {"status": "invalid_parsed", "reason": "missing_sender_email"}
    if not identity_key:
        return {"status": "invalid_parsed", "reason": "missing_identity_key"}
    if amount_cents <= 0:
        return {"status": "invalid_parsed", "reason": "invalid_amount"}

    meta: dict | None = None
    if source_kind == "bask_zelle":
        meta = _gmail_bask_metadata_from_parsed(parsed)
    base_notes = _json_dumps_compact(meta)

    with db() as conn:
        existing = conn.execute(
            "SELECT status, movement_id FROM gmail_processed_messages WHERE gmail_message_id = ?",
            (gmail_message_id,),
        ).fetchone()
        if existing:
            return {
                "status": "duplicate",
                "previous_status": str(existing["status"] or ""),
                "movement_id": int(existing["movement_id"]) if existing["movement_id"] is not None else None,
            }

        if source_kind == "bask_zelle" and confirmation_number:
            dup_conf = _find_gmail_processed_by_bask_confirmation_in_conn(conn, confirmation_number, amount_cents)
            if dup_conf:
                dup_meta = dict(meta or {})
                dup_meta["duplicate_reason"] = "bask_confirmation_number"
                dup_meta["duplicate_of_gmail_message_id"] = str(dup_conf["gmail_message_id"] or "")
                _insert_gmail_processed_message_in_conn(
                    conn,
                    parsed=parsed,
                    status="ignored_duplicate",
                    notes=_json_dumps_compact(dup_meta),
                )
                return {
                    "status": "duplicate",
                    "duplicate_reason": "bask_confirmation_number",
                    "duplicate_of_gmail_message_id": str(dup_conf["gmail_message_id"] or ""),
                }

        trust = conn.execute(
            "SELECT * FROM gmail_sender_trust WHERE sender_email = ?",
            (identity_key,),
        ).fetchone()

        is_new_sender = False
        if trust is None:
            is_new_sender = True
            auto_promote_at = dt_to_iso(now_dt + timedelta(days=GMAIL_ZELLE_AUTO_PROMOTE_DAYS))
            cur = conn.execute(
                """
                INSERT INTO gmail_sender_trust(
                    sender_email, state, first_seen_at, last_seen_at, seen_count,
                    auto_promote_at, approved_at, approved_by, blocked_at, blocked_by,
                    last_matched_amount_cents, last_matched_message_id, display_name_hint
                ) VALUES (?, 'quarantine', ?, ?, 1, ?, NULL, NULL, NULL, NULL, ?, ?, ?)
                """,
                (
                    identity_key,
                    now_iso,
                    now_iso,
                    auto_promote_at,
                    amount_cents,
                    gmail_message_id,
                    identity_display or sender_display_name or None,
                ),
            )
            sender_trust_id = int(cur.lastrowid)
            state = "quarantine"
            seen_count = 1
            auto_promoted = False
        else:
            sender_trust_id = int(trust["id"])
            state = str(trust["state"] or "quarantine")
            seen_count = int(trust["seen_count"] or 0) + 1

            conn.execute(
                """
                UPDATE gmail_sender_trust
                SET last_seen_at = ?,
                    seen_count = ?,
                    last_matched_amount_cents = ?,
                    last_matched_message_id = ?,
                    display_name_hint = COALESCE(?, display_name_hint)
                WHERE id = ?
                """,
                (
                    now_iso,
                    seen_count,
                    amount_cents,
                    gmail_message_id,
                    identity_display or sender_display_name or None,
                    sender_trust_id,
                ),
            )

            auto_promoted = False
            if state == "quarantine":
                auto_promote_at_raw = trust["auto_promote_at"]
                auto_promote_at = _parse_iso_utc_or_none(str(auto_promote_at_raw) if auto_promote_at_raw else None)
                if auto_promote_at is not None and auto_promote_at <= now_dt:
                    conn.execute(
                        """
                        UPDATE gmail_sender_trust
                        SET state = 'approved',
                            approved_at = ?,
                            approved_by = 0,
                            auto_promote_at = NULL
                        WHERE id = ?
                        """,
                        (now_iso, sender_trust_id),
                    )
                    state = "approved"
                    auto_promoted = True

        if state == "blocked":
            blocked_meta = dict(meta or {})
            blocked_meta["identity_key"] = identity_key
            blocked_meta["identity_display"] = identity_display
            _insert_gmail_processed_message_in_conn(
                conn,
                parsed=parsed,
                status="blocked_sender",
                notes=_json_dumps_compact(blocked_meta) or base_notes,
            )
            return {
                "status": "blocked_sender",
                "sender_trust_id": sender_trust_id,
                "sender_email": identity_key,
                "payer_display": identity_display,
            }

        # AUTO-accept path for valid Bask/gmail matches: quarantine is soft-trust (yellow), not a hard gate.
        if mode != "live" or actor_id is None:
            shadow_meta = dict(meta or {})
            shadow_meta["identity_key"] = identity_key
            shadow_meta["identity_display"] = identity_display
            shadow_meta["is_new_sender"] = bool(is_new_sender)
            shadow_meta["auto_promoted"] = bool(auto_promoted)
            _insert_gmail_processed_message_in_conn(
                conn,
                parsed=parsed,
                status="shadow_approved_match",
                notes=_json_dumps_compact(shadow_meta) or base_notes,
            )
            return {
                "status": "shadow_approved_match",
                "sender_trust_id": sender_trust_id,
                "sender_email": identity_key,
                "payer_display": identity_display,
                "auto_promoted": auto_promoted,
                "is_new_sender": is_new_sender,
                "state": state,
            }

        row = conn.execute(
            "SELECT total_cents, session_id FROM global_state WHERE id = 1"
        ).fetchone()
        total_cents = int(row["total_cents"]) + amount_cents
        session_id = int(row["session_id"])
        conn.execute("UPDATE global_state SET total_cents = ? WHERE id = 1", (total_cents,))
        cur = conn.execute(
            """
            INSERT INTO movements(session_id, kind, amount_cents, total_after_cents, actor_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, "add", amount_cents, total_cents, actor_id, now_iso),
        )
        movement_id = int(cur.lastrowid)

        _insert_gmail_processed_message_in_conn(
            conn,
            parsed=parsed,
            status="added",
            movement_id=movement_id,
            notes=_json_dumps_compact(
                {
                    **(meta or {}),
                    "identity_key": identity_key,
                    "identity_display": identity_display,
                    "is_new_sender": bool(is_new_sender),
                    "auto_promoted": bool(auto_promoted),
                }
            )
            or base_notes,
        )
        return {
            "status": "added",
            "movement_id": movement_id,
            "new_total_cents": total_cents,
            "sender_trust_id": sender_trust_id,
            "sender_email": identity_key,
            "payer_display": identity_display,
            "auto_promoted": auto_promoted,
            "is_new_sender": is_new_sender,
            "state": state,
            "actor_id": actor_id,
        }


def record_gmail_processed_message_tx(parsed: dict, status: str, notes: str | None = None) -> dict:
    gmail_message_id = str(parsed.get("gmail_message_id") or "")
    if not gmail_message_id:
        return {"status": "invalid_parsed", "reason": "missing_message_id"}

    with db() as conn:
        existing = conn.execute(
            "SELECT status, movement_id FROM gmail_processed_messages WHERE gmail_message_id = ?",
            (gmail_message_id,),
        ).fetchone()
        if existing:
            return {
                "status": "duplicate",
                "previous_status": str(existing["status"] or ""),
                "movement_id": int(existing["movement_id"]) if existing["movement_id"] is not None else None,
            }

        _insert_gmail_processed_message_in_conn(conn, parsed=parsed, status=status, notes=notes)
        return {"status": status}


def filter_unprocessed_gmail_message_ids(message_ids: list[str]) -> list[str]:
    ids = [str(mid) for mid in message_ids if str(mid or "").strip()]
    if not ids:
        return []

    placeholders = ",".join("?" for _ in ids)
    with db() as conn:
        rows = conn.execute(
            f"SELECT gmail_message_id FROM gmail_processed_messages WHERE gmail_message_id IN ({placeholders})",
            tuple(ids),
        ).fetchall()
    seen = {str(r["gmail_message_id"]) for r in rows}
    return [mid for mid in ids if mid not in seen]


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

        if last_kind == "reversal":
            new_total = current_total + last_amount
            conn.execute("UPDATE global_state SET total_cents = ? WHERE id = 1", (new_total,))
            rev = conn.execute(
                """
                SELECT id, gmail_message_id
                FROM gmail_reversals
                WHERE reversal_movement_id = ?
                LIMIT 1
                """,
                (last_id,),
            ).fetchone()
            if rev:
                conn.execute("DELETE FROM gmail_reversals WHERE id = ?", (int(rev["id"]),))
            conn.execute("DELETE FROM movements WHERE id = ?", (last_id,))
            return {
                "kind": "reversal",
                "undone_amount_cents": last_amount,
                "new_total_cents": new_total,
                "gmail_message_id": str(rev["gmail_message_id"] or "") if rev else None,
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

    lines.append("")
    if deposit_status == "stale":
        lines.append("<i>&#9888; Kraken deposit hold estimate refresh failed, showing cached estimate</i>")

    lines.append(f"<i>KRAKEN HOLDS [EST USD]: {_format_usd_est_amount_int(total_usd)}</i>")
    lines.append("")
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
# GMAIL ZELLE AUTO-INGEST
# =========================

def _html_escape(value: str | None) -> str:
    return html_lib.escape(str(value or ""), quote=False)


def _gmail_decode_b64url_text(data: str | None) -> str:
    if not data:
        return ""
    try:
        padded = str(data) + ("=" * (-len(str(data)) % 4))
        return base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _gmail_strip_html_to_text(html_text: str) -> str:
    text = html_text or ""
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s+", "\n", text)
    return text.strip()


def _gmail_collect_body_text_parts(payload: dict, plain_parts: list[str], html_parts: list[str]) -> None:
    if not isinstance(payload, dict):
        return

    mime_type = str(payload.get("mimeType") or "").lower()
    body = payload.get("body") or {}
    data = body.get("data") if isinstance(body, dict) else None
    if data:
        decoded = _gmail_decode_b64url_text(data)
        if decoded:
            if mime_type.startswith("text/plain"):
                plain_parts.append(decoded)
            elif mime_type.startswith("text/html"):
                html_parts.append(decoded)

    parts = payload.get("parts") or []
    if isinstance(parts, list):
        for part in parts:
            _gmail_collect_body_text_parts(part, plain_parts, html_parts)


def _gmail_extract_message_text(payload: dict, snippet: str | None = None) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []
    _gmail_collect_body_text_parts(payload, plain_parts, html_parts)
    if plain_parts:
        return "\n".join(x for x in plain_parts if x).strip()
    if html_parts:
        return _gmail_strip_html_to_text("\n".join(x for x in html_parts if x))
    return str(snippet or "").strip()


def _gmail_headers_map(payload: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    headers = payload.get("headers") if isinstance(payload, dict) else None
    if not isinstance(headers, list):
        return out
    for item in headers:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip().lower()
        if not name:
            continue
        out[name] = str(item.get("value") or "")
    return out


def _gmail_match_zelle_like_text(subject: str, body_text: str) -> bool:
    combined = f"{subject}\n{body_text}"
    default_pattern = r"(?i)\bzelle\b|sent you money|you received money|payment from"
    pattern = GMAIL_ZELLE_SUBJECT_REGEX or default_pattern
    try:
        return re.search(pattern, combined) is not None
    except re.error:
        return re.search(default_pattern, combined) is not None


def _gmail_extract_amount_cents(text: str) -> int | None:
    default_pattern = r"(?i)\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+\.[0-9]{2})"
    pattern = GMAIL_ZELLE_AMOUNT_REGEX or default_pattern
    try:
        regex = re.compile(pattern)
    except re.error:
        regex = re.compile(default_pattern)

    candidates_cents: list[int] = []
    for m in regex.finditer(text or ""):
        raw = None
        if m.groups():
            raw = m.group(1)
        else:
            raw = m.group(0)
        raw = str(raw or "").strip()
        if not raw:
            continue
        raw = raw.replace("$", "").replace(",", "").strip()
        if not raw:
            continue
        try:
            cents = money_to_cents(raw)
        except Exception:
            continue
        if cents > 0:
            candidates_cents.append(cents)

    if not candidates_cents:
        return None

    return max(candidates_cents)


def _gmail_extract_message_meta(msg: dict) -> dict:
    payload = msg.get("payload") or {}
    headers = _gmail_headers_map(payload)
    from_header = headers.get("from", "")
    sender_display_name, sender_email = parseaddr(from_header)
    internal_date_ms_raw = msg.get("internalDate")
    try:
        internal_date_ms = int(str(internal_date_ms_raw)) if internal_date_ms_raw is not None else None
    except Exception:
        internal_date_ms = None
    return {
        "gmail_message_id": str(msg.get("id") or ""),
        "thread_id": str(msg.get("threadId") or ""),
        "sender_email": _normalize_sender_email(sender_email),
        "sender_display_name": str(sender_display_name or "").strip(),
        "subject": str(headers.get("subject") or ""),
        "date_header": str(headers.get("date") or ""),
        "internal_date_ms": internal_date_ms,
    }


def _gmail_bask_sender_allowed(sender_email: str) -> bool:
    email_norm = _normalize_sender_email(sender_email)
    if not email_norm:
        return False
    if GMAIL_ZELLE_BASK_ALLOWED_SENDER_EMAILS_SET:
        return email_norm in GMAIL_ZELLE_BASK_ALLOWED_SENDER_EMAILS_SET
    return email_norm.endswith("@baskbank.com")


def _gmail_bask_section_text(body_text: str) -> str | None:
    text = str(body_text or "")
    m = re.search(r"(?is)\bPayment\s+Details\b(.*)", text)
    if not m:
        return None
    section = m.group(1)
    stop = re.search(
        r"(?is)\b(Check your account to see when the money will be available|Thank you for using Zelle|Sincerely,)\b",
        section,
    )
    if stop:
        section = section[: stop.start()]
    return section.strip() or None


def _gmail_bask_parse_fields_from_section(section_text: str) -> dict[str, str]:
    labels = ["Confirmation Number", "Amount", "From", "To", "Message"]
    lines = [re.sub(r"\s+", " ", str(line or "")).strip() for line in str(section_text or "").splitlines()]
    lines = [line for line in lines if line]
    out: dict[str, str] = {}
    i = 0
    while i < len(lines):
        line = lines[i]
        matched_label = None
        for label in labels:
            lab_low = label.lower()
            line_low = line.lower()
            if line_low == lab_low or line_low.startswith(lab_low + ":") or line_low.startswith(lab_low + " "):
                matched_label = label
                break
        if matched_label is None:
            i += 1
            continue

        remainder = line[len(matched_label):].lstrip(" :\t-").strip()
        if remainder:
            out[matched_label] = remainder
        elif i + 1 < len(lines):
            out[matched_label] = lines[i + 1].strip()
            i += 1
        i += 1
    return out


def _gmail_try_parse_bask_zelle(parsed: dict, body_text: str) -> tuple[dict, str]:
    sender_email = _normalize_sender_email(parsed.get("sender_email"))
    subject = str(parsed.get("subject") or "")
    body_text = str(body_text or "")

    # Strict Bask source gating.
    if not _gmail_bask_sender_allowed(sender_email):
        return parsed, "unmatched"

    subject_lower = subject.lower()
    body_lower = body_text.lower()
    if "payment details" not in body_lower:
        return parsed, "unmatched"
    if "zelle" not in body_lower and "zelle" not in subject_lower:
        return parsed, "unmatched"
    if "deposited" not in body_lower and "deposited" not in subject_lower:
        return parsed, "unmatched"

    section = _gmail_bask_section_text(body_text)
    if not section:
        return parsed, "unmatched"
    fields = _gmail_bask_parse_fields_from_section(section)

    conf_num = str(fields.get("Confirmation Number") or "").strip()
    amount_raw = str(fields.get("Amount") or "").strip()
    payer_display = re.sub(r"\s+", " ", str(fields.get("From") or "")).strip()
    to_line = re.sub(r"\s+", " ", str(fields.get("To") or "")).strip()

    if GMAIL_ZELLE_BASK_PARSER_STRICT:
        if not conf_num or not amount_raw or not payer_display or not to_line:
            return parsed, "unmatched"
        if not re.fullmatch(r"[A-Za-z0-9\-]+", conf_num):
            return parsed, "unmatched"
        if GMAIL_ZELLE_BASK_EXPECTED_TO_CONTAINS and GMAIL_ZELLE_BASK_EXPECTED_TO_CONTAINS.lower() not in to_line.lower():
            return parsed, "unmatched"

    amount_norm = amount_raw.replace("$", "").replace(",", "").strip()
    try:
        amount_cents = money_to_cents(amount_norm)
    except Exception:
        return parsed, "unmatched"
    if amount_cents <= 0:
        return parsed, "unmatched"

    parsed["amount_cents"] = amount_cents
    parsed["confirmation_number"] = conf_num
    parsed["payer_display"] = payer_display
    parsed["payer_key"] = _normalize_payer_key(payer_display)
    parsed["identity_key"] = parsed["payer_key"]
    parsed["identity_display"] = payer_display
    parsed["to_line"] = to_line
    parsed["bank_sender_email"] = sender_email
    parsed["source_kind"] = "bask_zelle"
    # Preserve the original bank sender in metadata, but make sender_display_name useful for UI/ranking fallbacks.
    parsed["bank_sender_display_name"] = str(parsed.get("sender_display_name") or "")
    parsed["sender_display_name"] = payer_display
    return parsed, "ok"


def parse_zelle_email_from_gmail_message(msg: dict) -> tuple[dict, str]:
    if not isinstance(msg, dict):
        raise RuntimeError("Gmail message has invalid shape")
    payload = msg.get("payload") or {}
    if not isinstance(payload, dict):
        raise RuntimeError("Gmail message missing payload")

    parsed = _gmail_extract_message_meta(msg)
    body_text = _gmail_extract_message_text(payload, snippet=msg.get("snippet"))

    if GMAIL_ZELLE_BASK_ONLY:
        return _gmail_try_parse_bask_zelle(parsed, body_text)

    if not _gmail_match_zelle_like_text(parsed.get("subject") or "", body_text):
        return parsed, "unmatched"

    amount_cents = _gmail_extract_amount_cents(body_text)
    if amount_cents is None:
        amount_cents = _gmail_extract_amount_cents(parsed.get("subject") or "")
    if amount_cents is None or amount_cents <= 0:
        return parsed, "unmatched"

    parsed["amount_cents"] = amount_cents
    return parsed, "ok"


def _gmail_get_service_sync():
    global _GMAIL_ZELLE_IMPORT_ERROR_WARNED
    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest
        from google.oauth2.credentials import Credentials as GoogleCredentials
        from googleapiclient.discovery import build as google_build
    except Exception as e:
        if not _GMAIL_ZELLE_IMPORT_ERROR_WARNED:
            _GMAIL_ZELLE_IMPORT_ERROR_WARNED = True
            logger.warning("Gmail API libraries not installed or import failed: %s", e)
        raise RuntimeError("Missing Gmail API client libraries") from e

    if not os.path.exists(GMAIL_ZELLE_TOKEN_PATH):
        if os.path.exists(GMAIL_ZELLE_CREDENTIALS_PATH):
            raise RuntimeError(f"Missing Gmail token file: {GMAIL_ZELLE_TOKEN_PATH} (generate OAuth token first)")
        raise RuntimeError(f"Missing Gmail token file: {GMAIL_ZELLE_TOKEN_PATH}")

    creds = GoogleCredentials.from_authorized_user_file(GMAIL_ZELLE_TOKEN_PATH, list(GMAIL_ZELLE_SCOPES))
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(GoogleAuthRequest())
            with open(GMAIL_ZELLE_TOKEN_PATH, "w", encoding="utf-8") as f:
                f.write(creds.to_json())
        else:
            raise RuntimeError("Gmail OAuth token is invalid and not refreshable")

    return google_build("gmail", "v1", credentials=creds, cache_discovery=False)


def _gmail_resolve_label_id_sync(service) -> str:
    global _GMAIL_ZELLE_LABEL_ID_CACHE
    if _GMAIL_ZELLE_LABEL_ID_CACHE:
        return _GMAIL_ZELLE_LABEL_ID_CACHE

    resp = service.users().labels().list(userId="me").execute()
    labels = resp.get("labels") or []
    for label in labels:
        if not isinstance(label, dict):
            continue
        if str(label.get("name") or "") == GMAIL_ZELLE_LABEL_NAME:
            _GMAIL_ZELLE_LABEL_ID_CACHE = str(label.get("id") or "")
            if _GMAIL_ZELLE_LABEL_ID_CACHE:
                return _GMAIL_ZELLE_LABEL_ID_CACHE
    raise RuntimeError(f"Gmail label not found: {GMAIL_ZELLE_LABEL_NAME}")


def _gmail_fetch_labeled_messages_sync() -> tuple[list[dict], int]:
    service = _gmail_get_service_sync()
    label_id = _gmail_resolve_label_id_sync(service)

    list_kwargs = {
        "userId": "me",
        "labelIds": [label_id],
        "maxResults": 50,
        "includeSpamTrash": False,
    }
    if GMAIL_ZELLE_QUERY_EXTRA:
        list_kwargs["q"] = GMAIL_ZELLE_QUERY_EXTRA

    resp = service.users().messages().list(**list_kwargs).execute()
    refs = resp.get("messages") or []
    if not isinstance(refs, list):
        refs = []
    listed_count = len(refs)
    if not refs:
        return [], listed_count

    ref_ids = [str(r.get("id") or "") for r in refs if isinstance(r, dict)]
    new_ids = set(filter_unprocessed_gmail_message_ids(ref_ids))
    if not new_ids:
        return [], listed_count

    full_messages: list[dict] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        msg_id = str(ref.get("id") or "")
        if msg_id not in new_ids:
            continue
        msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
        if isinstance(msg, dict):
            full_messages.append(msg)

    full_messages.sort(key=lambda m: int(str(m.get("internalDate") or "0")) if str(m.get("internalDate") or "").isdigit() else 0)
    return full_messages, listed_count


def _gmail_actor_id_ready() -> int | None:
    global _GMAIL_ZELLE_ACTOR_USER_ID_WARNED

    if _GMAIL_ZELLE_ACTOR_USER_ID_INVALID:
        if not _GMAIL_ZELLE_ACTOR_USER_ID_WARNED:
            _GMAIL_ZELLE_ACTOR_USER_ID_WARNED = True
            logger.warning(
                "Invalid GMAIL_ZELLE_ACTOR_USER_ID '%s'; Gmail approved-sender auto-add will run in shadow mode",
                _GMAIL_ZELLE_ACTOR_USER_ID_RAW,
            )
        return None

    if GMAIL_ZELLE_ACTOR_USER_ID is None:
        if not _GMAIL_ZELLE_ACTOR_USER_ID_WARNED:
            _GMAIL_ZELLE_ACTOR_USER_ID_WARNED = True
            logger.warning("Missing GMAIL_ZELLE_ACTOR_USER_ID; Gmail approved-sender auto-add will run in shadow mode")
        return None

    if not is_participant(GMAIL_ZELLE_ACTOR_USER_ID):
        if not _GMAIL_ZELLE_ACTOR_USER_ID_WARNED:
            _GMAIL_ZELLE_ACTOR_USER_ID_WARNED = True
            logger.warning(
                "GMAIL_ZELLE_ACTOR_USER_ID=%s is not a participant; Gmail approved-sender auto-add will run in shadow mode",
                GMAIL_ZELLE_ACTOR_USER_ID,
            )
        return None

    return GMAIL_ZELLE_ACTOR_USER_ID


async def send_gmail_unknown_sender_alert_for_app(app: Application, parsed: dict, sender_trust_id: int):
    confirmer_id = get_confirmer_id()
    if not confirmer_id:
        logger.warning("Gmail unknown sender matched but no confirmer exists to receive alert")
        return

    sender_email = _html_escape(parsed.get("sender_email"))
    sender_name = str(parsed.get("sender_display_name") or "").strip()
    sender_name_line = f"\nNombre: <code>{_html_escape(sender_name)}</code>" if sender_name else ""
    amount_cents = int(parsed.get("amount_cents") or 0)
    amount_str = cents_to_money_str(amount_cents) if amount_cents > 0 else "--"
    subject = _html_escape(parsed.get("subject") or "")

    trust_row = get_gmail_sender_trust_by_id(sender_trust_id)
    auto_promote_line = ""
    if trust_row and trust_row["auto_promote_at"]:
        ap_dt = _parse_iso_utc_or_none(str(trust_row["auto_promote_at"]))
        if ap_dt:
            auto_promote_line = f"\nAuto-trust en: <i>{_format_kraken_display_time_short(ap_dt)}</i>"

    try:
        msg = await app.bot.send_message(
            chat_id=confirmer_id,
            text=(
                "<b>Gmail Zelle: remitente nuevo</b>\n"
                f"Sender: <code>{sender_email}</code>{sender_name_line}\n"
                f"Monto detectado: <code>${amount_str}</code>\n"
                f"Asunto: <i>{subject[:180] or 'sin asunto'}</i>"
                f"{auto_promote_line}"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=build_sender_trust_keyboard(sender_trust_id),
        )
        app.create_task(delete_later_for_app(app, confirmer_id, msg.message_id, NOTIFY_DELETE_SECONDS))
    except Exception:
        logger.warning("Failed to send Gmail unknown-sender alert sender=%s", parsed.get("sender_email"), exc_info=True)


async def refresh_gmail_zelle_once(app: Application) -> None:
    if not GMAIL_ZELLE_ENABLED:
        GMAIL_ZELLE_STATUS.update(
            {
                "enabled": False,
                "tracking_mode": get_tracking_mode(),
                "last_cycle_status": "disabled",
            }
        )
        return

    actor_id = _gmail_actor_id_ready()
    tracking_mode = get_tracking_mode()
    effective_mode = GMAIL_ZELLE_MODE if actor_id is not None else "shadow"
    if tracking_mode == "manual":
        effective_mode = "shadow"
    GMAIL_ZELLE_STATUS.update(
        {
            "enabled": True,
            "mode": effective_mode,
            "tracking_mode": tracking_mode,
            "last_poll_started_at": now_utc(),
        }
    )

    try:
        full_messages, listed_count = await asyncio.to_thread(_gmail_fetch_labeled_messages_sync)
    except Exception as e:
        GMAIL_ZELLE_STATUS.update(
            {
                "enabled": True,
                "mode": effective_mode,
                "tracking_mode": tracking_mode,
                "last_cycle_status": "error",
                "last_poll_error_at": now_utc(),
                "last_poll_error_text": str(e)[:200],
            }
        )
        logger.warning("Gmail Zelle poll failed: %s", str(e)[:200])
        return

    counts = {
        "listed": listed_count,
        "fetched": len(full_messages),
        "added": 0,
        "quarantined": 0,
        "blocked": 0,
        "shadow": 0,
        "ignored_unmatched": 0,
        "parse_error": 0,
        "duplicate": 0,
    }
    panel_changed = False

    for raw_msg in full_messages:
        try:
            parsed, parse_status = parse_zelle_email_from_gmail_message(raw_msg)
        except Exception as e:
            parsed = _gmail_extract_message_meta(raw_msg if isinstance(raw_msg, dict) else {})
            record_result = record_gmail_processed_message_tx(parsed, "parse_error", notes=str(e)[:200])
            if record_result["status"] == "duplicate":
                counts["duplicate"] += 1
            else:
                counts["parse_error"] += 1
            continue

        if parse_status != "ok":
            record_result = record_gmail_processed_message_tx(parsed, "ignored_unmatched")
            if record_result["status"] == "duplicate":
                counts["duplicate"] += 1
            else:
                counts["ignored_unmatched"] += 1
            continue

        async with STATE_LOCK:
            result = process_gmail_zelle_parsed_tx(parsed, actor_id, effective_mode)

        status = str(result.get("status") or "")
        if status == "duplicate":
            counts["duplicate"] += 1
            continue

        if status == "quarantined_unknown_sender":
            counts["quarantined"] += 1
            panel_changed = True
            await notify_for_app(
                app,
                build_gmail_zelle_detected_notification_text(
                    parsed,
                    is_new_sender=True,
                    mode=effective_mode,
                ),
                delete_seconds=GMAIL_ZELLE_NOTIFY_DELETE_SECONDS,
            )
            continue

        if status == "blocked_sender":
            counts["blocked"] += 1
            panel_changed = True
            continue

        if status == "shadow_approved_match":
            counts["shadow"] += 1
            panel_changed = True
            await notify_for_app(
                app,
                build_gmail_zelle_detected_notification_text(
                    parsed,
                    is_new_sender=bool(result.get("is_new_sender")),
                    mode=effective_mode,
                ),
                delete_seconds=GMAIL_ZELLE_NOTIFY_DELETE_SECONDS,
            )
            continue

        if status == "added":
            counts["added"] += 1
            panel_changed = True
            await notify_for_app(
                app,
                build_gmail_zelle_detected_notification_text(
                    parsed,
                    is_new_sender=bool(result.get("is_new_sender")),
                    mode=effective_mode,
                ),
                delete_seconds=GMAIL_ZELLE_NOTIFY_DELETE_SECONDS,
            )
            continue

        logger.warning("Unhandled Gmail Zelle processing status=%s", status)

    if panel_changed:
        try:
            await update_all_panels_for_app(app)
        except Exception:
            logger.warning("Gmail-triggered panel refresh failed", exc_info=True)

    if (
        counts["fetched"]
        or counts["added"]
        or counts["quarantined"]
        or counts["blocked"]
        or counts["shadow"]
        or counts["ignored_unmatched"]
        or counts["parse_error"]
    ):
        logger.info(
            "Gmail Zelle poll summary: listed=%s fetched=%s added=%s quarantined=%s blocked=%s shadow=%s unmatched=%s parse_error=%s duplicate=%s mode=%s tracking_mode=%s",
            counts["listed"],
            counts["fetched"],
            counts["added"],
            counts["quarantined"],
            counts["blocked"],
            counts["shadow"],
            counts["ignored_unmatched"],
            counts["parse_error"],
            counts["duplicate"],
            effective_mode,
            tracking_mode,
        )

    GMAIL_ZELLE_STATUS.update(
        {
            "enabled": True,
            "mode": effective_mode,
            "tracking_mode": tracking_mode,
            "last_cycle_status": "ok",
            "last_poll_success_at": now_utc(),
            "last_poll_error_text": None,
        }
    )


async def gmail_zelle_poll_loop(app: Application) -> None:
    tracking_mode = get_tracking_mode()
    GMAIL_ZELLE_STATUS.update(
        {
            "enabled": GMAIL_ZELLE_ENABLED,
            "mode": GMAIL_ZELLE_MODE,
            "tracking_mode": tracking_mode,
            "last_cycle_status": "idle" if GMAIL_ZELLE_ENABLED else "disabled",
        }
    )
    logger.info(
        "Gmail Zelle poll loop started (label=%s interval=%ss mode=%s)",
        GMAIL_ZELLE_LABEL_NAME,
        GMAIL_ZELLE_POLL_SECONDS,
        GMAIL_ZELLE_MODE,
    )
    try:
        while True:
            try:
                await refresh_gmail_zelle_once(app)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                GMAIL_ZELLE_STATUS.update(
                    {
                        "enabled": GMAIL_ZELLE_ENABLED,
                        "mode": str(GMAIL_ZELLE_STATUS.get("mode") or GMAIL_ZELLE_MODE),
                        "tracking_mode": str(GMAIL_ZELLE_STATUS.get("tracking_mode") or get_tracking_mode()),
                        "last_cycle_status": "error",
                        "last_poll_error_at": now_utc(),
                        "last_poll_error_text": str(e)[:200],
                    }
                )
                logger.warning("Unexpected Gmail Zelle poll loop error", exc_info=True)

            await asyncio.sleep(GMAIL_ZELLE_POLL_SECONDS)
    except asyncio.CancelledError:
        GMAIL_ZELLE_STATUS["last_cycle_status"] = "idle" if GMAIL_ZELLE_ENABLED else "disabled"
        logger.info("Gmail Zelle poll loop stopped")
        raise


# =========================
# UI BUILDERS
# =========================

def build_panel_text(total_cents: int) -> str:
    fee_cents, network_fee_cents, net_cents = compute_fee_net(total_cents)
    kraken_block = _format_kraken_dashboard_block(_kraken_state_snapshot())
    gmail_footer_block = _format_gmail_footer_status_block()
    tracking_mode = get_tracking_mode()
    footer_lines = [gmail_footer_block]
    if tracking_mode == "manual":
        footer_lines.append(f"<i>⏳ Los mensajes desaparecen en {NOTIFY_DELETE_SECONDS}s</i>")
    gmail_footer_render = "\n\n".join(footer_lines)

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
        "光 ═════════════ 光\n"
        f"{gmail_footer_render}"
    )


def build_panel_keyboard(viewer_id: int | None = None) -> InlineKeyboardMarkup:
    tracking_mode = get_tracking_mode()
    confirmer_id = get_confirmer_id()
    is_admin_viewer = bool(confirmer_id and viewer_id == confirmer_id)
    rows: list[list[InlineKeyboardButton]] = []
    if is_admin_viewer:
        if tracking_mode == "manual":
            rows.append([InlineKeyboardButton("Switch to AUTO", callback_data="trackmode:auto")])
        else:
            rows.append([InlineKeyboardButton("Switch to MANUAL", callback_data="trackmode:manual")])

    rows.append([InlineKeyboardButton("📇 Senders", callback_data="senders")])

    if tracking_mode == "manual":
        rows.append([InlineKeyboardButton("✍ Custom", callback_data="custom")])

    history_row = [InlineKeyboardButton("📜 History", callback_data="history")]
    if tracking_mode == "manual":
        history_row.append(InlineKeyboardButton("⏪ Control + Z", callback_data="undo"))
    rows.append(history_row)

    if is_admin_viewer:
        rows.append([InlineKeyboardButton("🛠 Admin Reverse", callback_data="adminrev")])

    rows.append([InlineKeyboardButton("解Release除", callback_data="release")])
    return InlineKeyboardMarkup(rows)


def build_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Volver", callback_data="back")]])


def build_back_to_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Volver al panel", callback_data="back")]])


def build_confirm_keyboard(movement_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{movement_id}")]]
    )


# =========================
# GMAIL SENDER TRUST UI
# =========================

def build_sender_trust_keyboard(sender_trust_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Trust sender", callback_data=f"sendertrust:approve:{sender_trust_id}"),
                InlineKeyboardButton("⛔ Block sender", callback_data=f"sendertrust:block:{sender_trust_id}"),
            ],
            [InlineKeyboardButton("🙈 Ignore", callback_data=f"sendertrust:ignore:{sender_trust_id}")],
        ]
    )


def build_senders_list_keyboard(page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    nav: list[InlineKeyboardButton] = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"senders:page:{max(0, page - 1)}"))
    if has_next:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"senders:page:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("Volver", callback_data="back")])
    return InlineKeyboardMarkup(rows)


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


async def delete_later_for_app(app: Application, chat_id: int, message_id: int, seconds: int):
    try:
        await asyncio.sleep(seconds)
        await app.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        logger.debug(
            "delete_later_for_app failed for chat_id=%s message_id=%s",
            chat_id,
            message_id,
            exc_info=True,
        )


def build_gmail_zelle_detected_notification_text(parsed: dict, *, is_new_sender: bool, mode: str) -> str:
    payer_name = str(
        parsed.get("identity_display")
        or parsed.get("payer_display")
        or parsed.get("sender_display_name")
        or parsed.get("sender_email")
        or "Desconocido"
    ).strip()
    amount_cents = int(parsed.get("amount_cents") or 0)
    amount_label = "Monto detectado" if is_new_sender else "Monto"
    title_base = "Gmail Zelle: remitente nuevo" if is_new_sender else "Gmail Zelle detectado"
    title = f"{title_base} (shadow)" if mode != "live" else title_base
    return (
        f"<b>{_html_escape(title)}</b>\n"
        f"Nombre: <code>{_html_escape(payer_name)}</code>\n"
        f"{amount_label}: <code>${cents_to_money_str(amount_cents)}</code>\n"
        f"Hora: <i>{_html_escape(_format_gmail_event_time_display(parsed))}</i>"
    )


async def notify_for_app(app: Application, text: str, *, delete_seconds: int | None = None):
    ttl = NOTIFY_DELETE_SECONDS if delete_seconds is None else max(1, int(delete_seconds))
    for uid in get_participants():
        try:
            msg = await app.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            app.create_task(delete_later_for_app(app, uid, msg.message_id, ttl))
        except Exception:
            logger.warning("notify_for_app failed for participant user_id=%s", uid, exc_info=True)


async def notify(context: ContextTypes.DEFAULT_TYPE, text: str):
    # Best-effort notifications; never crash the bot if notification fails
    await notify_for_app(context.application, text)


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
    kb = build_panel_keyboard(chat_id)

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

    if result["kind"] == "reversal":
        await notify(
            context,
            (
                "<b>Yozu Tracker</b>\n"
                "<b>⏪ Control + Z</b>\n"
                "Se deshizo una <b>reversa</b>.\n"
                f"Total: <code>${cents_to_money_str(int(result['new_total_cents']))}</code>"
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
    tracking_mode = get_tracking_mode()

    if data.startswith("trackmode:"):
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return
        try:
            target_mode = _normalize_tracking_mode(data.split(":", 1)[1])
        except Exception:
            return

        async with STATE_LOCK:
            mode_result = set_tracking_mode_tx(target_mode, user.id)

        new_mode = str(mode_result.get("mode") or target_mode)
        GMAIL_ZELLE_STATUS["tracking_mode"] = new_mode
        if new_mode == "auto":
            AWAITING_CUSTOM_AMOUNT.clear()

        await update_all_panels(context)

        if bool(mode_result.get("changed")):
            await notify_for_app(
                context.application,
                (
                    "<b>Tracking mode</b>\n"
                    f"Modo activo: <b>{_html_escape(new_mode.upper())}</b>"
                ),
                delete_seconds=TRACKING_MODE_NOTIFY_DELETE_SECONDS,
            )
        return

    if data.startswith("add:"):
        if tracking_mode != "manual":
            return
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
        if tracking_mode != "manual":
            AWAITING_CUSTOM_AMOUNT.discard(user.id)
            return
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

    if data == "senders":
        page = 0
        list_text, has_prev, has_next = build_senders_list_text(page=page, viewer_id=user.id)
        await edit_panel(
            update.effective_chat.id,
            context,
            text=list_text,
            reply_markup=build_senders_list_keyboard(page, has_prev, has_next),
        )
        return

    if data.startswith("senders:page:"):
        try:
            page = max(0, int(data.split(":", 2)[2]))
        except Exception:
            page = 0
        list_text, has_prev, has_next = build_senders_list_text(page=page, viewer_id=user.id)
        await edit_panel(
            update.effective_chat.id,
            context,
            text=list_text,
            reply_markup=build_senders_list_keyboard(page, has_prev, has_next),
        )
        return

    if data == "adminrev":
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return
        page = 0
        list_text, rows, has_prev, has_next = build_admin_reverse_list_text(page=page, viewer_id=user.id)
        await edit_panel(
            update.effective_chat.id,
            context,
            text=list_text,
            reply_markup=build_admin_reverse_list_keyboard(page, rows, has_prev, has_next),
        )
        return

    if data.startswith("adminrev:page:"):
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return
        try:
            page = max(0, int(data.split(":", 2)[2]))
        except Exception:
            page = 0
        list_text, rows, has_prev, has_next = build_admin_reverse_list_text(page=page, viewer_id=user.id)
        await edit_panel(
            update.effective_chat.id,
            context,
            text=list_text,
            reply_markup=build_admin_reverse_list_keyboard(page, rows, has_prev, has_next),
        )
        return

    if data.startswith("adminrev:select:"):
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return
        gmail_message_id = data.split(":", 2)[2].strip()
        event = get_recent_gmail_auto_added_event_by_message_id(gmail_message_id)
        if not event:
            await edit_panel(
                update.effective_chat.id,
                context,
                text="<b>🛠 Admin Reverse</b>\n\n<i>La transacción ya no está disponible.</i>",
                reply_markup=build_back_keyboard(),
            )
            return
        await edit_panel(
            update.effective_chat.id,
            context,
            text=build_admin_reverse_confirm_text(event),
            reply_markup=build_admin_reverse_confirm_keyboard(
                gmail_message_id,
                is_reversed=bool(event.get("is_reversed")),
            ),
        )
        return

    if data.startswith("adminrev:do:") or data.startswith("adminrev:block_and_do:"):
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return
        try:
            _, action, gmail_message_id = data.split(":", 2)
        except Exception:
            return
        block_payer = action == "block_and_do"

        async with STATE_LOCK:
            reverse_result = admin_reverse_gmail_event_tx(gmail_message_id, user.id, block_payer=block_payer)

        status = str(reverse_result.get("status") or "")
        if status == "reversed":
            payer_display = _html_escape(reverse_result.get("payer_display") or "Desconocido")
            amount_cents = int(reverse_result.get("amount_cents") or 0)
            blocked_applied = bool(reverse_result.get("blocked_applied"))
            extra = "\n<i>Payer bloqueado.</i>" if blocked_applied else ""
            await notify(
                context,
                (
                    "<b>🛠 Admin Reverse</b>\n"
                    f"Payer: <code>{payer_display}</code>\n"
                    f"Reversa aplicada: <code>${cents_to_money_str(amount_cents)}</code>\n"
                    f"Total: <code>${cents_to_money_str(int(reverse_result.get('new_total_cents') or 0))}</code>"
                    f"{extra}"
                ),
            )
            await update_all_panels(context)
        elif status == "already_reversed":
            await notify(context, "<b>🛠 Admin Reverse</b>\nEsa transacción ya fue revertida.")
        elif status == "missing":
            await notify(context, "<b>🛠 Admin Reverse</b>\nNo encontré esa transacción.")
        else:
            await notify(context, "<b>🛠 Admin Reverse</b>\nNo se pudo aplicar la reversa.")

        event = get_recent_gmail_auto_added_event_by_message_id(gmail_message_id)
        if event:
            await edit_panel(
                update.effective_chat.id,
                context,
                text=build_admin_reverse_confirm_text(event),
                reply_markup=build_admin_reverse_confirm_keyboard(
                    gmail_message_id,
                    is_reversed=bool(event.get("is_reversed")),
                ),
            )
        else:
            page = 0
            list_text, rows, has_prev, has_next = build_admin_reverse_list_text(page=page, viewer_id=user.id)
            await edit_panel(
                update.effective_chat.id,
                context,
                text=list_text,
                reply_markup=build_admin_reverse_list_keyboard(page, rows, has_prev, has_next),
            )
        return

    if data == "undo":
        if tracking_mode != "manual":
            return
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
    if data.startswith("sendertrust:"):
        confirmer_id = get_confirmer_id()
        if not confirmer_id or user.id != confirmer_id:
            return

        try:
            _, action, sender_id_raw = data.split(":", 2)
            sender_trust_id = int(sender_id_raw)
        except Exception:
            return

        async with STATE_LOCK:
            trust_result = sendertrust_action_tx(sender_trust_id, action, user.id)

        status = str(trust_result.get("status") or "")
        if status == "missing":
            try:
                await query.edit_message_text("Ese remitente ya no existe en la tabla de confianza.")
            except Exception:
                pass
            return

        if status == "invalid_action":
            return

        sender_email = _html_escape(trust_result.get("sender_email"))
        if status == "approved":
            text = (
                "<b>Sender trusted</b>\n"
                f"<code>{sender_email}</code>\n"
                "<i>Los próximos correos de este remitente se auto-agregarán.</i>"
            )
        elif status == "blocked":
            text = (
                "<b>Sender blocked</b>\n"
                f"<code>{sender_email}</code>\n"
                "<i>Los correos futuros de este remitente no se agregarán.</i>"
            )
        else:
            text = (
                "<b>Sender ignored</b>\n"
                f"<code>{sender_email}</code>\n"
                "<i>Sigue en cuarentena.</i>"
            )

        try:
            await query.edit_message_text(text=text, parse_mode=ParseMode.HTML)
        except Exception:
            pass
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
                SELECT id, kind, amount_cents, total_after_cents, actor_id, created_at
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
            participant_names = get_participant_display_name_map()
            movement_ids = [int(r["id"]) for r in rows if r["id"] is not None]
            gmail_payer_by_movement: dict[int, str] = {}
            reversal_payer_by_movement: dict[int, str] = {}
            if movement_ids:
                placeholders = ",".join("?" for _ in movement_ids)
                with db() as conn:
                    gmail_rows = conn.execute(
                        f"""
                        SELECT movement_id, notes
                        FROM gmail_processed_messages
                        WHERE status = 'added'
                          AND movement_id IN ({placeholders})
                        """,
                        tuple(movement_ids),
                    ).fetchall()
                    rev_rows = conn.execute(
                        f"""
                        SELECT reversal_movement_id, payer_display
                        FROM gmail_reversals
                        WHERE reversal_movement_id IN ({placeholders})
                        """,
                        tuple(movement_ids),
                    ).fetchall()

                for gr in gmail_rows:
                    if gr["movement_id"] is None:
                        continue
                    meta = _json_loads_object_or_none(gr["notes"])
                    if not meta:
                        continue
                    payer = str(meta.get("payer_display") or meta.get("identity_display") or "").strip()
                    if payer:
                        gmail_payer_by_movement[int(gr["movement_id"])] = payer

                for rr in rev_rows:
                    if rr["reversal_movement_id"] is None:
                        continue
                    payer = str(rr["payer_display"] or "").strip()
                    if payer:
                        reversal_payer_by_movement[int(rr["reversal_movement_id"])] = payer

            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            lines = ["<b>📜 History (sesión actual)</b>", ""]
            for r in rows:
                dt_utc = datetime.fromisoformat(r["created_at"])
                ts = f"{dt_utc.day:02d} {months[dt_utc.month - 1]} {dt_utc.year}"

                movement_id = int(r["id"])
                kind = str(r["kind"] or "")
                amt_cents = int(r["amount_cents"] or 0)
                total_after_cents = int(r["total_after_cents"] or 0)
                actor_id = int(r["actor_id"]) if r["actor_id"] is not None else None
                actor_name = participant_names.get(actor_id or -1)
                display_name = None
                if kind == "add":
                    label = "Add"
                    amount_disp = f"${cents_to_money_str(amt_cents)}"
                    display_name = gmail_payer_by_movement.get(movement_id) or actor_name
                elif kind == "release":
                    label = "Release"
                    amount_disp = f"${cents_to_money_str(amt_cents)}"
                    display_name = actor_name
                elif kind == "reversal":
                    label = "Reversal"
                    amount_disp = f"-${cents_to_money_str(amt_cents)}"
                    display_name = reversal_payer_by_movement.get(movement_id) or actor_name
                else:
                    label = kind
                    amount_disp = f"${cents_to_money_str(amt_cents)}"
                    display_name = actor_name
                name_suffix = f" &#183; {_html_escape(display_name)}" if display_name else ""
                lines.append(
                    f"&#183; <b>{_html_escape(label)}{name_suffix}</b>: <code>{amount_disp}</code> "
                    f"&#8594; Total: <code>${cents_to_money_str(total_after_cents)}</code>\n"
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
            reply_markup=build_panel_keyboard(update.effective_chat.id),
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

    if get_tracking_mode() != "manual":
        AWAITING_CUSTOM_AMOUNT.discard(user.id)
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
    global KRAKEN_REFRESH_TASK, GMAIL_ZELLE_TASK

    if not KRAKEN_CACHE["enabled"]:
        logger.info("Kraken balance dashboard line enabled in placeholder mode (missing Kraken API creds)")
    elif not KRAKEN_REFRESH_TASK or KRAKEN_REFRESH_TASK.done():
        KRAKEN_REFRESH_TASK = app.create_task(kraken_refresh_loop(app))

    if not GMAIL_ZELLE_ENABLED:
        return

    if GMAIL_ZELLE_MODE not in {"shadow", "live"}:
        global _GMAIL_ZELLE_MODE_WARNED
        if not _GMAIL_ZELLE_MODE_WARNED:
            _GMAIL_ZELLE_MODE_WARNED = True
            logger.warning("Invalid GMAIL_ZELLE_MODE; falling back to shadow")

    if GMAIL_ZELLE_TASK and not GMAIL_ZELLE_TASK.done():
        return

    GMAIL_ZELLE_TASK = app.create_task(gmail_zelle_poll_loop(app))


async def on_app_shutdown(app: Application):
    global KRAKEN_REFRESH_TASK, GMAIL_ZELLE_TASK

    tasks = [t for t in (KRAKEN_REFRESH_TASK, GMAIL_ZELLE_TASK) if t]
    KRAKEN_REFRESH_TASK = None
    GMAIL_ZELLE_TASK = None
    for task in tasks:
        task.cancel()
    for task in tasks:
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


