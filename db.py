"""
Database handler — dual backend.

  MySQL  : set db_host / db_user / db_password / db_name in config (or env vars).
           Uses thread-local connection caching, autocommit.
           This is what prod runs on.

  SQLite : automatic fallback when no MySQL config is present.
           Good for self-hosted single-instance setups.
           DB file path: config["database"]["db_path"] (default: hfradar.db)

All public functions have identical signatures regardless of backend.
Backend is selected once at init_db() time.
"""

import json
import logging
import threading as _threading
from contextlib import contextmanager

log = logging.getLogger("hfradar.db")

# ── Backend selection ──────────────────────────────────────────────────────────

_BACKEND: str = "sqlite"   # "mysql" or "sqlite" — set by init_db()


def _is_mysql(cfg: dict) -> bool:
    db = cfg if "db_host" in cfg else cfg.get("database", {})
    return bool(db.get("db_host"))


def _db_cfg(cfg: dict) -> dict:
    return cfg if "db_host" in cfg else cfg.get("database", cfg)


# ══════════════════════════════════════════════════════════════════════════════
#  MySQL backend
# ══════════════════════════════════════════════════════════════════════════════

_tls = _threading.local()


def _mysql_get_connection(cfg: dict):
    import pymysql
    conn = getattr(_tls, "conn", None)
    if conn is not None:
        try:
            conn.ping(reconnect=True)
            return conn
        except Exception:
            _tls.conn = None
    import time as _t
    db = _db_cfg(cfg)
    last_err = None
    for attempt in range(3):
        try:
            _tls.conn = pymysql.connect(
                host=db["db_host"],
                user=db["db_user"],
                password=db["db_password"],
                database=db["db_name"],
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                connect_timeout=8,
                read_timeout=10,
                write_timeout=10,
            )
            return _tls.conn
        except pymysql.err.OperationalError as e:
            last_err = e
            if attempt < 2:
                _t.sleep(1)
    raise last_err


@contextmanager
def _mysql_cursor(cfg: dict):
    conn = _mysql_get_connection(cfg)
    try:
        with conn.cursor() as cur:
            yield cur
    except Exception:
        _tls.conn = None
        raise


def _mysql_add_col(cur, table, col, defn):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        log.info(f"Added column {table}.{col}")
    except Exception:
        pass


def _mysql_drop_col(cur, table, col):
    try:
        cur.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
        log.info(f"Dropped column {table}.{col}")
    except Exception:
        pass


def _mysql_init(cfg: dict):
    conn = _mysql_get_connection(cfg)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id         BIGINT PRIMARY KEY,
                hf_uid          INT,
                hf_username     VARCHAR(100),
                access_token    TEXT,
                linked_at       INT,
                token_expires   INT,
                paused          TINYINT DEFAULT 0,
                active          TINYINT DEFAULT 1,
                notifications   TEXT DEFAULT '{}',
                last_unread_pms INT DEFAULT 0,
                welcome_sent    TINYINT DEFAULT 0,
                tone            VARCHAR(20) DEFAULT 'normal',
                buddy_list      TEXT DEFAULT '[]',
                tracked_fids    TEXT DEFAULT '[]',
                last_reputation INT DEFAULT 0,
                known_tids      TEXT DEFAULT '[]',
                own_tids        TEXT DEFAULT '[]',
                participated_tids        JSON DEFAULT NULL,
                thread_meta              JSON DEFAULT NULL,
                muted_tids               TEXT DEFAULT '[]',
                contract_states          TEXT DEFAULT '{}',
                dispute_states           TEXT DEFAULT '{}',
                last_bytes_balance       DECIMAL(12,2) DEFAULT 0,
                buddy_usergroups         TEXT DEFAULT '{}',
                uid_cache                JSON DEFAULT NULL,
                gambling_pending         JSON DEFAULT NULL,
                last_gambling_flush      INT DEFAULT 0,
                own_tids_bootstrapped    TINYINT DEFAULT 0,
                participated_tids_bootstrapped INT DEFAULT 0,
                last_discovery_at        INT DEFAULT 0,
                last_cold_poll_at        INT DEFAULT 0,
                last_warm_poll_at        INT DEFAULT 0,
                last_fid_poll_at         INT DEFAULT 0,
                last_brating_check_at    INT DEFAULT 0,
                last_buddy_check_at      INT DEFAULT 0,
                last_discovery_postnum   INT DEFAULT 0,
                last_postnum             INT DEFAULT 0,
                last_threadnum           INT DEFAULT 0,
                last_warningpoints       INT DEFAULT 0,
                last_usergroup           VARCHAR(20) DEFAULT '',
                rep_history              JSON DEFAULT NULL,
                balance_history          JSON DEFAULT NULL,
                postnum_history          JSON DEFAULT NULL,
                threadnum_history        JSON DEFAULT NULL,
                bytes_log                JSON DEFAULT NULL,
                last_balance_snap_at     INT DEFAULT 0,
                last_digest_at           INT DEFAULT 0,
                features                 JSON DEFAULT NULL,
                banned                   TINYINT DEFAULT 0,
                joined_at                INT DEFAULT 0,
                thread_state             TEXT DEFAULT '{}',
                last_dispute_check_at    INT DEFAULT 0,
                last_discovery_threadnum INT DEFAULT 0
            )
        """)
        for col, defn in [
            ("tone",                        "VARCHAR(20) DEFAULT 'normal'"),
            ("buddy_list",                  "TEXT DEFAULT '[]'"),
            ("tracked_fids",                "TEXT DEFAULT '[]'"),
            ("buddy_usergroups",            "TEXT DEFAULT '{}'"),
            ("banned",                      "TINYINT DEFAULT 0"),
            ("joined_at",                   "INT DEFAULT 0"),
            ("last_reputation",             "INT DEFAULT 0"),
            ("known_tids",                  "TEXT DEFAULT '[]'"),
            ("own_tids",                    "TEXT DEFAULT '[]'"),
            ("thread_meta",                 "JSON DEFAULT NULL"),
            ("muted_tids",                  "TEXT DEFAULT '[]'"),
            ("contract_states",             "TEXT DEFAULT '{}'"),
            ("dispute_states",              "TEXT DEFAULT '{}'"),
            ("last_bytes_balance",          "DECIMAL(12,2) DEFAULT 0"),
            ("own_tids_bootstrapped",       "TINYINT DEFAULT 0"),
            ("last_discovery_at",           "INT DEFAULT 0"),
            ("last_cold_poll_at",           "INT DEFAULT 0"),
            ("gambling_pending",            "JSON DEFAULT NULL"),
            ("last_gambling_flush",         "INT DEFAULT 0"),
            ("participated_tids",           "JSON DEFAULT NULL"),
            ("last_postnum",                "INT DEFAULT 0"),
            ("last_threadnum",              "INT DEFAULT 0"),
            ("last_warningpoints",          "INT DEFAULT 0"),
            ("last_usergroup",              "VARCHAR(20) DEFAULT ''"),
            ("last_warm_poll_at",           "INT DEFAULT 0"),
            ("rep_history",                 "JSON DEFAULT NULL"),
            ("features",                    "JSON DEFAULT NULL"),
            ("balance_history",             "JSON DEFAULT NULL"),
            ("last_balance_snap_at",        "INT DEFAULT 0"),
            ("last_digest_at",              "INT DEFAULT 0"),
            ("uid_cache",                   "JSON DEFAULT NULL"),
            ("last_brating_check_at",       "INT DEFAULT 0"),
            ("last_buddy_check_at",         "INT DEFAULT 0"),
            ("last_discovery_postnum",      "INT DEFAULT 0"),
            ("last_fid_poll_at",            "INT DEFAULT 0"),
            ("participated_tids_bootstrapped", "INT DEFAULT 0"),
            ("postnum_history",             "JSON DEFAULT NULL"),
            ("threadnum_history",           "JSON DEFAULT NULL"),
            ("bytes_log",                   "JSON DEFAULT NULL"),
            ("thread_state",                "TEXT DEFAULT '{}'"),
            ("last_dispute_check_at",       "INT DEFAULT 0"),
            ("last_discovery_threadnum",    "INT DEFAULT 0"),
        ]:
            _mysql_add_col(cur, "users", col, defn)
        try:
            cur.execute("ALTER TABLE users MODIFY COLUMN last_bytes_balance DECIMAL(12,2) DEFAULT 0")
        except Exception:
            pass
        for dead in ("buddy_tids", "buddy_thread_state", "thread_lpu", "thread_nr",
                     "expiry_warned", "paid_until", "trial_start", "last_expiry_alert",
                     "fid_tids", "contract_expiry_alerted", "is_donor", "total_donated"):
            _mysql_drop_col(cur, "users", dead)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS loop_meta (
                loop_name   VARCHAR(50) PRIMARY KEY,
                last_ran_at INT NOT NULL DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pending_auth (
                chat_id    BIGINT PRIMARY KEY,
                auth_code  VARCHAR(200) DEFAULT NULL,
                created_at INT
            )
        """)
        _mysql_add_col(cur, "pending_auth", "auth_code", "VARCHAR(200) DEFAULT NULL")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_events (
                chat_id    BIGINT      NOT NULL,
                namespace  VARCHAR(50) NOT NULL,
                event_id   VARCHAR(100) NOT NULL,
                seen_at    INT,
                UNIQUE KEY uq_event (chat_id, namespace, event_id)
            )
        """)
    log.info("MySQL database initialized.")


# ══════════════════════════════════════════════════════════════════════════════
#  SQLite backend
# ══════════════════════════════════════════════════════════════════════════════

import sqlite3


def _sqlite_path(cfg: dict) -> str:
    return _db_cfg(cfg).get("db_path") or "hfradar.db"


def _sqlite_conn(cfg: dict) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_path(cfg), check_same_thread=False, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def _sqlite_cursor(cfg: dict):
    conn = _sqlite_conn(cfg)
    try:
        with conn:
            yield conn.cursor()
    finally:
        conn.close()


def _sqlite_add_col(conn, table, col, defn):
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except Exception:
        pass


def _sqlite_init(cfg: dict):
    conn = _sqlite_conn(cfg)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id         INTEGER PRIMARY KEY,
                hf_uid          INTEGER,
                hf_username     TEXT,
                access_token    TEXT,
                linked_at       INTEGER,
                token_expires   INTEGER,
                paused          INTEGER DEFAULT 0,
                active          INTEGER DEFAULT 1,
                notifications   TEXT DEFAULT '{}',
                last_unread_pms INTEGER DEFAULT 0,
                welcome_sent    INTEGER DEFAULT 0,
                tone            TEXT DEFAULT 'normal',
                buddy_list      TEXT DEFAULT '[]',
                tracked_fids    TEXT DEFAULT '[]',
                last_reputation INTEGER DEFAULT 0,
                known_tids      TEXT DEFAULT '[]',
                own_tids        TEXT DEFAULT '[]',
                participated_tids        TEXT DEFAULT NULL,
                thread_meta              TEXT DEFAULT NULL,
                muted_tids               TEXT DEFAULT '[]',
                contract_states          TEXT DEFAULT '{}',
                dispute_states           TEXT DEFAULT '{}',
                last_bytes_balance       REAL DEFAULT 0,
                buddy_usergroups         TEXT DEFAULT '{}',
                uid_cache                TEXT DEFAULT NULL,
                gambling_pending         TEXT DEFAULT NULL,
                last_gambling_flush      INTEGER DEFAULT 0,
                own_tids_bootstrapped    INTEGER DEFAULT 0,
                participated_tids_bootstrapped INTEGER DEFAULT 0,
                last_discovery_at        INTEGER DEFAULT 0,
                last_cold_poll_at        INTEGER DEFAULT 0,
                last_warm_poll_at        INTEGER DEFAULT 0,
                last_fid_poll_at         INTEGER DEFAULT 0,
                last_brating_check_at    INTEGER DEFAULT 0,
                last_buddy_check_at      INTEGER DEFAULT 0,
                last_discovery_postnum   INTEGER DEFAULT 0,
                last_postnum             INTEGER DEFAULT 0,
                last_threadnum           INTEGER DEFAULT 0,
                last_warningpoints       INTEGER DEFAULT 0,
                last_usergroup           TEXT DEFAULT '',
                rep_history              TEXT DEFAULT NULL,
                balance_history          TEXT DEFAULT NULL,
                postnum_history          TEXT DEFAULT NULL,
                threadnum_history        TEXT DEFAULT NULL,
                bytes_log                TEXT DEFAULT NULL,
                last_balance_snap_at     INTEGER DEFAULT 0,
                last_digest_at           INTEGER DEFAULT 0,
                features                 TEXT DEFAULT NULL,
                banned                   INTEGER DEFAULT 0,
                joined_at                INTEGER DEFAULT 0,
                thread_state             TEXT DEFAULT '{}',
                last_dispute_check_at    INTEGER DEFAULT 0,
                last_discovery_threadnum INTEGER DEFAULT 0,
                gambling_pending_2       TEXT DEFAULT NULL,
                last_gambling_flush_2    INTEGER DEFAULT 0
            )
        """)
        for col, defn in [
            ("tone",                        "TEXT DEFAULT 'normal'"),
            ("buddy_list",                  "TEXT DEFAULT '[]'"),
            ("tracked_fids",                "TEXT DEFAULT '[]'"),
            ("buddy_usergroups",            "TEXT DEFAULT '{}'"),
            ("banned",                      "INTEGER DEFAULT 0"),
            ("joined_at",                   "INTEGER DEFAULT 0"),
            ("muted_tids",                  "TEXT DEFAULT '[]'"),
            ("contract_states",             "TEXT DEFAULT '{}'"),
            ("dispute_states",              "TEXT DEFAULT '{}'"),
            ("last_bytes_balance",          "REAL DEFAULT 0"),
            ("own_tids_bootstrapped",       "INTEGER DEFAULT 0"),
            ("participated_tids",           "TEXT DEFAULT NULL"),
            ("last_postnum",                "INTEGER DEFAULT 0"),
            ("last_threadnum",              "INTEGER DEFAULT 0"),
            ("last_warningpoints",          "INTEGER DEFAULT 0"),
            ("rep_history",                 "TEXT DEFAULT NULL"),
            ("balance_history",             "TEXT DEFAULT NULL"),
            ("postnum_history",             "TEXT DEFAULT NULL"),
            ("threadnum_history",           "TEXT DEFAULT NULL"),
            ("bytes_log",                   "TEXT DEFAULT NULL"),
            ("last_balance_snap_at",        "INTEGER DEFAULT 0"),
            ("last_digest_at",              "INTEGER DEFAULT 0"),
            ("uid_cache",                   "TEXT DEFAULT NULL"),
            ("last_brating_check_at",       "INTEGER DEFAULT 0"),
            ("last_buddy_check_at",         "INTEGER DEFAULT 0"),
            ("last_fid_poll_at",            "INTEGER DEFAULT 0"),
            ("participated_tids_bootstrapped", "INTEGER DEFAULT 0"),
            ("thread_state",                "TEXT DEFAULT '{}'"),
            ("last_dispute_check_at",       "INTEGER DEFAULT 0"),
            ("last_discovery_threadnum",    "INTEGER DEFAULT 0"),
            ("gambling_pending",            "TEXT DEFAULT NULL"),
            ("last_gambling_flush",         "INTEGER DEFAULT 0"),
            ("features",                    "TEXT DEFAULT NULL"),
        ]:
            _sqlite_add_col(conn, "users", col, defn)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS loop_meta (
                loop_name   TEXT PRIMARY KEY,
                last_ran_at INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_auth (
                chat_id    INTEGER PRIMARY KEY,
                auth_code  TEXT DEFAULT NULL,
                created_at INTEGER
            )
        """)
        _sqlite_add_col(conn, "pending_auth", "auth_code", "TEXT DEFAULT NULL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_events (
                chat_id   INTEGER NOT NULL,
                namespace TEXT    NOT NULL,
                event_id  TEXT    NOT NULL,
                seen_at   INTEGER,
                UNIQUE (chat_id, namespace, event_id)
            )
        """)
    conn.close()
    log.info(f"SQLite database initialized: {_sqlite_path(cfg)}")


# ══════════════════════════════════════════════════════════════════════════════
#  Shared helpers
# ══════════════════════════════════════════════════════════════════════════════

def _parse_user(row) -> dict | None:
    if not row:
        return None
    r = dict(row)
    for f in ("notifications", "buddy_usergroups", "contract_states",
              "dispute_states", "thread_state", "thread_meta", "buddy_last_pid"):
        if r.get(f):
            try:
                r[f] = json.loads(r[f])
            except Exception:
                r[f] = {}
    for f in ("buddy_list", "tracked_fids", "known_tids", "own_tids",
              "muted_tids", "participated_tids", "rep_history",
              "balance_history", "postnum_history", "threadnum_history",
              "bytes_log", "gambling_pending"):
        if r.get(f):
            try:
                r[f] = json.loads(r[f])
            except Exception:
                r[f] = []
    for f in ("features", "uid_cache"):
        if r.get(f):
            try:
                r[f] = json.loads(r[f])
            except Exception:
                r[f] = {}
    return r


_LIST_JSON_FIELDS = frozenset((
    "buddy_list", "tracked_fids", "known_tids", "own_tids", "muted_tids",
))


def _serialize(data: dict) -> dict:
    out = {}
    for k, v in data.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v)
        elif v is None and k in _LIST_JSON_FIELDS:
            out[k] = "[]"
        else:
            out[k] = v
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  Public API
# ══════════════════════════════════════════════════════════════════════════════

def init_db(cfg: dict):
    global _BACKEND
    if _is_mysql(cfg):
        _BACKEND = "mysql"
        _mysql_init(cfg)
    else:
        _BACKEND = "sqlite"
        _sqlite_init(cfg)
    log.info(f"DB backend: {_BACKEND.upper()}")


def get_user(cfg: dict, chat_id: int) -> dict | None:
    ph = "%s" if _BACKEND == "mysql" else "?"
    ctx = _mysql_cursor if _BACKEND == "mysql" else _sqlite_cursor
    with ctx(cfg) as cur:
        cur.execute(f"SELECT * FROM users WHERE chat_id = {ph}", (chat_id,))
        return _parse_user(cur.fetchone())


def upsert_user(cfg: dict, chat_id: int, data: dict):
    data = _serialize(data)
    if _BACKEND == "mysql":
        with _mysql_cursor(cfg) as cur:
            cols = "chat_id, " + ", ".join(data.keys())
            ph   = ", ".join(["%s"] * (len(data) + 1))
            upd  = ", ".join(f"{k} = VALUES({k})" for k in data)
            cur.execute(
                f"INSERT INTO users ({cols}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {upd}",
                [chat_id] + list(data.values()),
            )
    else:
        with _sqlite_cursor(cfg) as cur:
            cols = "chat_id, " + ", ".join(data.keys())
            ph   = ", ".join(["?"] * (len(data) + 1))
            upd  = ", ".join(f"{k} = excluded.{k}" for k in data)
            cur.execute(
                f"INSERT INTO users ({cols}) VALUES ({ph}) ON CONFLICT(chat_id) DO UPDATE SET {upd}",
                [chat_id] + list(data.values()),
            )


def get_all_active_users(cfg: dict) -> list:
    ctx = _mysql_cursor if _BACKEND == "mysql" else _sqlite_cursor
    with ctx(cfg) as cur:
        cur.execute("SELECT * FROM users WHERE active = 1")
        return [_parse_user(r) for r in cur.fetchall()]


def get_pending_auth(cfg: dict) -> list:
    import time as _t
    cutoff = int(_t.time()) - 600
    if _BACKEND == "mysql":
        with _mysql_cursor(cfg) as cur:
            cur.execute("SELECT * FROM pending_auth WHERE created_at > UNIX_TIMESTAMP() - 600")
            return list(cur.fetchall())
    else:
        with _sqlite_cursor(cfg) as cur:
            cur.execute("SELECT * FROM pending_auth WHERE created_at > ?", (cutoff,))
            return [dict(r) for r in cur.fetchall()]


def add_pending_auth(cfg: dict, chat_id: int):
    import time as _t
    now = int(_t.time())
    if _BACKEND == "mysql":
        with _mysql_cursor(cfg) as cur:
            cur.execute(
                "INSERT INTO pending_auth (chat_id, created_at) VALUES (%s, UNIX_TIMESTAMP()) "
                "ON DUPLICATE KEY UPDATE created_at = UNIX_TIMESTAMP(), auth_code = NULL",
                (chat_id,)
            )
    else:
        with _sqlite_cursor(cfg) as cur:
            cur.execute(
                "INSERT INTO pending_auth (chat_id, created_at) VALUES (?, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET created_at = excluded.created_at, auth_code = NULL",
                (chat_id, now)
            )


def remove_pending_auth(cfg: dict, chat_id: int):
    ph = "%s" if _BACKEND == "mysql" else "?"
    ctx = _mysql_cursor if _BACKEND == "mysql" else _sqlite_cursor
    with ctx(cfg) as cur:
        cur.execute(f"DELETE FROM pending_auth WHERE chat_id = {ph}", (chat_id,))


def is_event_seen(cfg: dict, chat_id: int, namespace: str, event_id: str) -> bool:
    ph = "%s" if _BACKEND == "mysql" else "?"
    ctx = _mysql_cursor if _BACKEND == "mysql" else _sqlite_cursor
    with ctx(cfg) as cur:
        cur.execute(
            f"SELECT 1 FROM seen_events WHERE chat_id={ph} AND namespace={ph} AND event_id={ph}",
            (chat_id, namespace, str(event_id))
        )
        return cur.fetchone() is not None


def mark_event_seen(cfg: dict, chat_id: int, namespace: str, event_id: str):
    import time as _t
    now = int(_t.time())
    if _BACKEND == "mysql":
        with _mysql_cursor(cfg) as cur:
            cur.execute(
                "INSERT IGNORE INTO seen_events (chat_id, namespace, event_id, seen_at) "
                "VALUES (%s, %s, %s, UNIX_TIMESTAMP())",
                (chat_id, namespace, str(event_id))
            )
    else:
        with _sqlite_cursor(cfg) as cur:
            cur.execute(
                "INSERT OR IGNORE INTO seen_events (chat_id, namespace, event_id, seen_at) "
                "VALUES (?, ?, ?, ?)",
                (chat_id, namespace, str(event_id), now)
            )


def prune_seen_events(cfg: dict) -> int:
    import time as _t
    now = int(_t.time())
    deleted = 0
    if _BACKEND == "mysql":
        with _mysql_cursor(cfg) as cur:
            cur.execute("DELETE FROM seen_events WHERE namespace IN ('reply','mention','bytes') AND seen_at < UNIX_TIMESTAMP() - 7776000")
            deleted += cur.rowcount
            cur.execute("DELETE FROM seen_events WHERE namespace NOT IN ('reply','mention','bytes','contract','dispute','brating','contract_expiry') AND seen_at < UNIX_TIMESTAMP() - 15552000")
            deleted += cur.rowcount
            cur.execute("DELETE FROM seen_events WHERE namespace IN ('contract','dispute','brating','contract_expiry') AND seen_at < UNIX_TIMESTAMP() - 31536000")
            deleted += cur.rowcount
    else:
        with _sqlite_cursor(cfg) as cur:
            cur.execute("DELETE FROM seen_events WHERE namespace IN ('reply','mention','bytes') AND seen_at < ?", (now - 7776000,))
            deleted += cur.rowcount
            cur.execute("DELETE FROM seen_events WHERE namespace NOT IN ('reply','mention','bytes','contract','dispute','brating','contract_expiry') AND seen_at < ?", (now - 15552000,))
            deleted += cur.rowcount
            cur.execute("DELETE FROM seen_events WHERE namespace IN ('contract','dispute','brating','contract_expiry') AND seen_at < ?", (now - 31536000,))
            deleted += cur.rowcount
    return deleted


def get_loop_last_ran(cfg: dict, loop_name: str) -> int:
    ph = "%s" if _BACKEND == "mysql" else "?"
    ctx = _mysql_cursor if _BACKEND == "mysql" else _sqlite_cursor
    with ctx(cfg) as cur:
        cur.execute(f"SELECT last_ran_at FROM loop_meta WHERE loop_name = {ph}", (loop_name,))
        row = cur.fetchone()
        return int(dict(row).get("last_ran_at", 0)) if row else 0


def set_loop_last_ran(cfg: dict, loop_name: str) -> None:
    import time as _t
    now = int(_t.time())
    if _BACKEND == "mysql":
        with _mysql_cursor(cfg) as cur:
            cur.execute(
                "INSERT INTO loop_meta (loop_name, last_ran_at) VALUES (%s, UNIX_TIMESTAMP()) "
                "ON DUPLICATE KEY UPDATE last_ran_at = UNIX_TIMESTAMP()",
                (loop_name,)
            )
    else:
        with _sqlite_cursor(cfg) as cur:
            cur.execute(
                "INSERT INTO loop_meta (loop_name, last_ran_at) VALUES (?, ?) "
                "ON CONFLICT(loop_name) DO UPDATE SET last_ran_at = excluded.last_ran_at",
                (loop_name, now)
            )
