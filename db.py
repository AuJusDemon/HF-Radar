"""
Database handler — SQLite, zero external dependencies.
DB file defaults to hfradar.db in the working directory.
Set "db_path" in config.json > "database" to override.

Tables are created on first run. Schema migrations (ADD COLUMN) run automatically.
SQLite DROP COLUMN requires 3.35+ — backward migrations are skipped since fresh
installs don't have dead columns.

Thread safety: each call opens and closes its own connection with WAL mode.
Safe for use from a thread-pool executor (bot.py's _db wrapper).
"""

import sqlite3
import json
import logging
import time
from contextlib import contextmanager

log = logging.getLogger("hfradar.db")


# ── Connection ─────────────────────────────────────────────────────────────────

def _db_path(cfg: dict) -> str:
    return cfg.get("db_path") or "hfradar.db"


def get_connection(cfg: dict) -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(cfg), check_same_thread=False, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_cursor(cfg: dict):
    conn = get_connection(cfg)
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Schema helpers ─────────────────────────────────────────────────────────────

def _add_column_if_missing(cur, table: str, column: str, definition: str):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        log.info(f"Added column {table}.{column}")
    except Exception:
        pass  # already exists


# ── init_db ────────────────────────────────────────────────────────────────────

def init_db(cfg: dict):
    conn = get_connection(cfg)
    cur  = conn.cursor()

    # ── users ──────────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id                         INTEGER PRIMARY KEY,
            hf_uid                          INTEGER,
            hf_username                     TEXT,
            access_token                    TEXT,
            linked_at                       INTEGER,
            token_expires                   INTEGER,
            paused                          INTEGER DEFAULT 0,
            active                          INTEGER DEFAULT 1,
            notifications                   TEXT    DEFAULT '{}',
            last_unread_pms                 INTEGER DEFAULT 0,
            welcome_sent                    INTEGER DEFAULT 0,
            tone                            TEXT    DEFAULT 'normal',
            buddy_list                      TEXT    DEFAULT '[]',
            tracked_fids                    TEXT    DEFAULT '[]',
            last_reputation                 INTEGER DEFAULT 0,
            known_tids                      TEXT    DEFAULT '[]',
            own_tids                        TEXT    DEFAULT '[]',
            participated_tids               TEXT    DEFAULT NULL,
            thread_meta                     TEXT    DEFAULT NULL,
            muted_tids                      TEXT    DEFAULT '[]',
            contract_states                 TEXT    DEFAULT '{}',
            dispute_states                  TEXT    DEFAULT '{}',
            last_bytes_balance              REAL    DEFAULT 0,
            buddy_usergroups                TEXT    DEFAULT '{}',
            uid_cache                       TEXT    DEFAULT NULL,
            own_tids_bootstrapped           INTEGER DEFAULT 0,
            participated_tids_bootstrapped  INTEGER DEFAULT 0,
            last_discovery_at               INTEGER DEFAULT 0,
            last_cold_poll_at               INTEGER DEFAULT 0,
            last_warm_poll_at               INTEGER DEFAULT 0,
            last_fid_poll_at                INTEGER DEFAULT 0,
            last_brating_check_at           INTEGER DEFAULT 0,
            last_discovery_postnum          INTEGER DEFAULT 0,
            last_postnum                    INTEGER DEFAULT 0,
            last_threadnum                  INTEGER DEFAULT 0,
            last_warningpoints              INTEGER DEFAULT 0,
            last_usergroup                  TEXT    DEFAULT '',
            rep_history                     TEXT    DEFAULT NULL,
            balance_history                 TEXT    DEFAULT NULL,
            postnum_history                 TEXT    DEFAULT NULL,
            threadnum_history               TEXT    DEFAULT NULL,
            bytes_log                       TEXT    DEFAULT NULL,
            last_balance_snap_at            INTEGER DEFAULT 0,
            last_digest_at                  INTEGER DEFAULT 0,
            features                        TEXT    DEFAULT NULL,
            banned                          INTEGER DEFAULT 0,
            joined_at                       INTEGER DEFAULT 0,
            thread_state                    TEXT    DEFAULT '{}'
        )
    """)

    _add_column_if_missing(cur, "users", "tone",                          "TEXT DEFAULT 'normal'")
    _add_column_if_missing(cur, "users", "buddy_list",                    "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "users", "tracked_fids",                  "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "users", "buddy_usergroups",              "TEXT DEFAULT '{}'")
    _add_column_if_missing(cur, "users", "banned",                        "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "joined_at",                     "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_reputation",               "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "known_tids",                    "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "users", "own_tids",                      "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "users", "thread_meta",                   "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "muted_tids",                    "TEXT DEFAULT '[]'")
    _add_column_if_missing(cur, "users", "contract_states",               "TEXT DEFAULT '{}'")
    _add_column_if_missing(cur, "users", "dispute_states",                "TEXT DEFAULT '{}'")
    _add_column_if_missing(cur, "users", "last_bytes_balance",            "REAL DEFAULT 0")
    _add_column_if_missing(cur, "users", "own_tids_bootstrapped",         "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_discovery_at",             "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_cold_poll_at",             "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "participated_tids",             "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "last_postnum",                  "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_threadnum",                "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_warningpoints",            "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_usergroup",                "TEXT DEFAULT ''")
    _add_column_if_missing(cur, "users", "last_warm_poll_at",             "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "rep_history",                   "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "features",                      "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "balance_history",               "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "last_balance_snap_at",          "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_digest_at",                "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "uid_cache",                     "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "last_brating_check_at",         "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_discovery_postnum",        "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "last_fid_poll_at",              "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "participated_tids_bootstrapped","INTEGER DEFAULT 0")
    _add_column_if_missing(cur, "users", "postnum_history",               "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "threadnum_history",             "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "bytes_log",                     "TEXT DEFAULT NULL")
    _add_column_if_missing(cur, "users", "thread_state",                  "TEXT DEFAULT '{}'")

    # ── loop_meta ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS loop_meta (
            loop_name   TEXT    PRIMARY KEY,
            last_ran_at INTEGER NOT NULL DEFAULT 0
        )
    """)

    # ── pending_auth ───────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_auth (
            chat_id    INTEGER PRIMARY KEY,
            auth_code  TEXT    DEFAULT NULL,
            created_at INTEGER
        )
    """)
    _add_column_if_missing(cur, "pending_auth", "auth_code", "TEXT DEFAULT NULL")

    # ── seen_events ────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_events (
            chat_id   INTEGER NOT NULL,
            namespace TEXT    NOT NULL,
            event_id  TEXT    NOT NULL,
            seen_at   INTEGER,
            UNIQUE (chat_id, namespace, event_id)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_seen_events_chat
        ON seen_events (chat_id, seen_at)
    """)

    conn.commit()
    conn.close()
    log.info("Database initialized.")


# ── Row parsing ────────────────────────────────────────────────────────────────

def _parse_user(row) -> dict | None:
    if not row:
        return None
    row = dict(row)

    for field in ("notifications",):
        if row.get(field):
            try:    row[field] = json.loads(row[field])
            except: row[field] = {}

    for field in ("buddy_list", "tracked_fids", "known_tids", "own_tids", "muted_tids"):
        if row.get(field):
            try:    row[field] = json.loads(row[field])
            except: row[field] = []

    for field in ("thread_state", "buddy_usergroups", "contract_states", "dispute_states",
                  "thread_meta", "features", "uid_cache"):
        if row.get(field):
            try:    row[field] = json.loads(row[field])
            except: row[field] = {}

    for field in ("participated_tids", "rep_history", "balance_history",
                  "postnum_history", "threadnum_history", "bytes_log"):
        if row.get(field):
            try:    row[field] = json.loads(row[field])
            except: row[field] = []

    return row


# ── User operations ────────────────────────────────────────────────────────────

def get_user(cfg: dict, chat_id: int) -> dict | None:
    with db_cursor(cfg) as cur:
        cur.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
        return _parse_user(cur.fetchone())


def get_user_by_hf_uid(cfg: dict, hf_uid: int) -> dict | None:
    with db_cursor(cfg) as cur:
        cur.execute("SELECT * FROM users WHERE hf_uid = ?", (hf_uid,))
        return _parse_user(cur.fetchone())


def get_all_active_users(cfg: dict) -> list:
    with db_cursor(cfg) as cur:
        cur.execute("SELECT * FROM users WHERE active = 1")
        return [_parse_user(row) for row in cur.fetchall()]


_LIST_JSON_FIELDS = frozenset((
    "buddy_list", "tracked_fids", "known_tids", "own_tids", "muted_tids",
))


def upsert_user(cfg: dict, chat_id: int, data: dict):
    for field, v in list(data.items()):
        if isinstance(v, (dict, list)):
            data[field] = json.dumps(v)
        elif v is None and field in _LIST_JSON_FIELDS:
            data[field] = "[]"

    cols   = ", ".join(data.keys())
    ph     = ", ".join(["?"] * len(data))
    update = ", ".join(f"{k}=excluded.{k}" for k in data)
    with db_cursor(cfg) as cur:
        cur.execute(
            f"INSERT INTO users (chat_id, {cols}) VALUES (?, {ph}) "
            f"ON CONFLICT(chat_id) DO UPDATE SET {update}",
            [chat_id] + list(data.values()),
        )


def ban_user(cfg: dict, chat_id: int):
    with db_cursor(cfg) as cur:
        cur.execute("UPDATE users SET banned=1, active=0 WHERE chat_id=?", (chat_id,))


def unban_user(cfg: dict, chat_id: int):
    with db_cursor(cfg) as cur:
        cur.execute("UPDATE users SET banned=0, active=1 WHERE chat_id=?", (chat_id,))


def get_recent_users(cfg: dict, limit: int = 10) -> list:
    with db_cursor(cfg) as cur:
        cur.execute(
            "SELECT chat_id, hf_uid, hf_username, joined_at, paused, banned "
            "FROM users ORDER BY joined_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(r) for r in cur.fetchall()]


# ── Pending auth ───────────────────────────────────────────────────────────────

def get_pending_auth(cfg: dict) -> list:
    cutoff = int(time.time()) - 600
    with db_cursor(cfg) as cur:
        cur.execute("SELECT * FROM pending_auth WHERE created_at > ?", (cutoff,))
        return [dict(r) for r in cur.fetchall()]


def add_pending_auth(cfg: dict, chat_id: int):
    now = int(time.time())
    with db_cursor(cfg) as cur:
        cur.execute(
            "INSERT INTO pending_auth (chat_id, created_at) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET created_at=excluded.created_at, auth_code=NULL",
            (chat_id, now)
        )


def remove_pending_auth(cfg: dict, chat_id: int):
    with db_cursor(cfg) as cur:
        cur.execute("DELETE FROM pending_auth WHERE chat_id=?", (chat_id,))


# ── Seen events ────────────────────────────────────────────────────────────────

def is_event_seen(cfg: dict, chat_id: int, namespace: str, event_id: str) -> bool:
    with db_cursor(cfg) as cur:
        cur.execute(
            "SELECT 1 FROM seen_events WHERE chat_id=? AND namespace=? AND event_id=?",
            (chat_id, namespace, str(event_id))
        )
        return cur.fetchone() is not None


def mark_event_seen(cfg: dict, chat_id: int, namespace: str, event_id: str):
    now = int(time.time())
    with db_cursor(cfg) as cur:
        cur.execute(
            "INSERT OR IGNORE INTO seen_events (chat_id, namespace, event_id, seen_at) "
            "VALUES (?, ?, ?, ?)",
            (chat_id, namespace, str(event_id), now)
        )


def prune_seen_events(cfg: dict) -> int:
    now     = int(time.time())
    deleted = 0
    with db_cursor(cfg) as cur:
        cur.execute(
            "DELETE FROM seen_events WHERE namespace IN ('reply','mention','bytes') "
            "AND seen_at < ?", (now - 7_776_000,)
        )
        deleted += cur.rowcount
        cur.execute(
            "DELETE FROM seen_events "
            "WHERE namespace NOT IN ('reply','mention','bytes','contract','dispute','brating','contract_expiry') "
            "AND seen_at < ?", (now - 15_552_000,)
        )
        deleted += cur.rowcount
        cur.execute(
            "DELETE FROM seen_events "
            "WHERE namespace IN ('contract','dispute','brating','contract_expiry') "
            "AND seen_at < ?", (now - 31_536_000,)
        )
        deleted += cur.rowcount
    return deleted


# ── Loop timing ────────────────────────────────────────────────────────────────

def get_loop_last_ran(cfg: dict, loop_name: str) -> int:
    with db_cursor(cfg) as cur:
        cur.execute("SELECT last_ran_at FROM loop_meta WHERE loop_name=?", (loop_name,))
        row = cur.fetchone()
        return int(row["last_ran_at"]) if row else 0


def set_loop_last_ran(cfg: dict, loop_name: str) -> None:
    now = int(time.time())
    with db_cursor(cfg) as cur:
        cur.execute(
            "INSERT INTO loop_meta (loop_name, last_ran_at) VALUES (?, ?) "
            "ON CONFLICT(loop_name) DO UPDATE SET last_ran_at=excluded.last_ran_at",
            (loop_name, now)
        )


# ── Stats ──────────────────────────────────────────────────────────────────────

def get_stats(cfg: dict) -> dict:
    now      = int(time.time())
    week_ago = now - 7 * 86400
    day_ago  = now - 86400
    with db_cursor(cfg) as cur:
        def _n(q, *a):
            cur.execute(q, a)
            return (cur.fetchone() or (0,))[0] or 0
        return {
            "total":    _n("SELECT COUNT(*) FROM users"),
            "active":   _n("SELECT COUNT(*) FROM users WHERE active=1 AND banned=0"),
            "paused":   _n("SELECT COUNT(*) FROM users WHERE paused=1"),
            "banned":   _n("SELECT COUNT(*) FROM users WHERE banned=1"),
            "new_week": _n("SELECT COUNT(*) FROM users WHERE joined_at > ?", week_ago),
            "new_day":  _n("SELECT COUNT(*) FROM users WHERE joined_at > ?", day_ago),
        }
