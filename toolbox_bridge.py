"""
toolbox_bridge.py — Radar's read/write access to the Toolbox MySQL database.

Only used when TOOLBOX_DB_HOST is set in the environment.
Provides a separate PyMySQL connection pool (separate from Radar's own DB).

Key operations:
  get_pending_events_by_uid  — unsent alert_events for a given hf_uid
  get_pending_events_by_chat — resolve chat_id → hf_uid, return unsent events
  mark_event_delivered       — set telegram_sent=1
  get_toolbox_link           — look up telegram_links by hf_uid
  get_hf_uid_for_chat        — reverse lookup by chat_id
  create_toolbox_telegram_link — write to telegram_links and update mode
  consume_link_code          — validate + delete a link code, return hf_uid
  get_integration_mode       — read integration_accounts.mode
  set_integration_mode       — write integration_accounts.mode
"""

import os
import json
import time
import queue as _queue
import threading
import logging

log = logging.getLogger("hfradar.toolbox_bridge")

# ── Config ────────────────────────────────────────────────────────────────────

_CFG = {
    "host":     os.getenv("TOOLBOX_DB_HOST", ""),
    "port":     int(os.getenv("TOOLBOX_DB_PORT", "3306")),
    "user":     os.getenv("TOOLBOX_DB_USER", ""),
    "password": os.getenv("TOOLBOX_DB_PASSWORD", ""),
    "database": os.getenv("TOOLBOX_DB_NAME", ""),
    "charset":  "utf8mb4",
    "autocommit": False,
    "connect_timeout": 5,
    "read_timeout":    8,
    "write_timeout":   8,
}

ENABLED = bool(_CFG["host"] and _CFG["user"] and _CFG["database"])

LINK_CODE_TTL = 600  # must match integration_db.py


# ── Connection pool ────────────────────────────────────────────────────────────

MAX_CONNS = 4
_pool: _queue.Queue = _queue.Queue(maxsize=MAX_CONNS)
_pool_size_lock = threading.Lock()
_pool_size = 0


def _create_conn():
    import pymysql
    import pymysql.cursors
    cfg = dict(_CFG)
    cfg["cursorclass"] = pymysql.cursors.DictCursor
    return pymysql.connect(**cfg)


def _acquire():
    global _pool_size
    try:
        conn = _pool.get_nowait()
        try:
            conn.ping(reconnect=False)
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            with _pool_size_lock:
                _pool_size -= 1
    except _queue.Empty:
        pass

    with _pool_size_lock:
        if _pool_size < MAX_CONNS:
            _pool_size += 1
            create = True
        else:
            create = False

    if create:
        return _create_conn()

    try:
        conn = _pool.get(timeout=10)
        try:
            conn.ping(reconnect=False)
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            with _pool_size_lock:
                _pool_size -= 1
            return _create_conn()
    except _queue.Empty:
        with _pool_size_lock:
            _pool_size += 1
        return _create_conn()


def _release(conn):
    global _pool_size
    try:
        _pool.put_nowait(conn)
    except _queue.Full:
        try:
            conn.close()
        except Exception:
            pass
        with _pool_size_lock:
            _pool_size -= 1


from contextlib import contextmanager

@contextmanager
def _db():
    if not ENABLED:
        raise RuntimeError("Toolbox bridge not configured — set TOOLBOX_DB_* env vars")
    conn = _acquire()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _release(conn)


# ── Alert events ──────────────────────────────────────────────────────────────

def get_pending_events_by_uid(hf_uid: str, limit: int = 50) -> list[dict]:
    with _db() as cur:
        cur.execute(
            "SELECT id, hf_uid, type, dedupe_key, title, body, link, source, payload, created_at "
            "FROM alert_events "
            "WHERE hf_uid=%s AND telegram_sent=0 "
            "ORDER BY created_at ASC LIMIT %s",
            (str(hf_uid), limit)
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("payload"):
            try:
                d["payload"] = json.loads(d["payload"])
            except Exception:
                d["payload"] = None
        result.append(d)
    return result


def get_pending_events_by_chat(chat_id: int, limit: int = 50) -> list[dict]:
    hf_uid = get_hf_uid_for_chat(chat_id)
    if not hf_uid:
        return []
    return get_pending_events_by_uid(hf_uid, limit=limit)


def mark_event_delivered(event_id: int) -> None:
    with _db() as cur:
        cur.execute(
            "UPDATE alert_events SET telegram_sent=1 WHERE id=%s", (event_id,)
        )


# ── Telegram links ────────────────────────────────────────────────────────────

def get_toolbox_link(hf_uid: str) -> dict | None:
    """Returns {hf_uid, chat_id, linked_at} or None."""
    with _db() as cur:
        cur.execute(
            "SELECT hf_uid, chat_id, linked_at FROM telegram_links WHERE hf_uid=%s",
            (str(hf_uid),)
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_hf_uid_for_chat(chat_id: int) -> str | None:
    with _db() as cur:
        cur.execute(
            "SELECT hf_uid FROM telegram_links WHERE chat_id=%s", (int(chat_id),)
        )
        row = cur.fetchone()
    return str(row["hf_uid"]) if row else None


def create_toolbox_telegram_link(hf_uid: str, chat_id: int) -> None:
    """Write telegram_links row and set mode to toolbox_linked_relay."""
    now = int(time.time())
    with _db() as cur:
        # Remove any prior link for this chat_id
        cur.execute("DELETE FROM telegram_links WHERE chat_id=%s", (int(chat_id),))
        cur.execute(
            "INSERT INTO telegram_links (hf_uid, chat_id, linked_at) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE chat_id=VALUES(chat_id), linked_at=VALUES(linked_at)",
            (str(hf_uid), int(chat_id), now)
        )
        cur.execute(
            "INSERT INTO integration_accounts (hf_uid, mode, updated_at) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE mode=VALUES(mode), updated_at=VALUES(updated_at)",
            (str(hf_uid), "toolbox_linked_relay", now)
        )


# ── Link codes ────────────────────────────────────────────────────────────────

def consume_link_code(code: str) -> str | None:
    """
    Validate and consume a link code. Returns hf_uid if valid and not expired,
    None otherwise. Deletes the code regardless so it can't be reused.
    """
    now = int(time.time())
    with _db() as cur:
        cur.execute(
            "SELECT hf_uid, created_at FROM telegram_link_codes WHERE code=%s", (str(code),)
        )
        row = cur.fetchone()
        if not row:
            return None
        age = now - int(row["created_at"])
        cur.execute("DELETE FROM telegram_link_codes WHERE code=%s", (str(code),))
        if age > LINK_CODE_TTL:
            return None
        return str(row["hf_uid"])


# ── Integration mode ──────────────────────────────────────────────────────────

def get_integration_mode(hf_uid: str) -> str:
    with _db() as cur:
        cur.execute(
            "SELECT mode FROM integration_accounts WHERE hf_uid=%s", (str(hf_uid),)
        )
        row = cur.fetchone()
    return str(row["mode"]) if row else "toolbox_only"


def set_integration_mode(hf_uid: str, mode: str) -> None:
    now = int(time.time())
    with _db() as cur:
        cur.execute(
            "INSERT INTO integration_accounts (hf_uid, mode, updated_at) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE mode=VALUES(mode), updated_at=VALUES(updated_at)",
            (str(hf_uid), mode, now)
        )


# ── Delivery batch ───────────────────────────────────────────────────────────

def get_all_undelivered_events(limit: int = 100) -> list[dict]:
    """
    Return undelivered alert events for linked users, enriched with chat_id.
    Excludes both_linked users — their delivery is handled by Radar's native polling loops.
    """
    with _db() as cur:
        cur.execute(
            "SELECT ae.id, ae.hf_uid, ae.type, ae.dedupe_key, ae.title, ae.body, ae.link, "
            "       ae.source, ae.payload, ae.created_at, tl.chat_id "
            "FROM alert_events ae "
            "INNER JOIN telegram_links tl ON tl.hf_uid = ae.hf_uid "
            "LEFT JOIN integration_accounts ia ON ia.hf_uid = ae.hf_uid "
            "WHERE ae.telegram_sent = 0 "
            "  AND (ia.mode IS NULL OR ia.mode != 'both_linked') "
            "ORDER BY ae.created_at ASC LIMIT %s",
            (limit,)
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get("payload"):
            try:
                d["payload"] = json.loads(d["payload"])
            except Exception:
                d["payload"] = None
        result.append(d)
    return result


# ── Token access ─────────────────────────────────────────────────────────────

def get_user_access_token(hf_uid: str) -> str | None:
    try:
        with _db() as cur:
            cur.execute("SELECT token FROM users WHERE uid=%s", (str(hf_uid),))
            row = cur.fetchone()
        if not row or not row.get("token"):
            return None
        enc = row["token"]
        if not enc.startswith("e:"):
            return enc or None  # plaintext (no encryption)
        import base64
        from cryptography.fernet import Fernet, InvalidToken
        ciphertext = enc[2:].encode()
        key = os.getenv("TOKEN_ENCRYPT_KEY", "").strip()
        if key:
            try:
                return Fernet(key.encode()).decrypt(ciphertext).decode()
            except (InvalidToken, Exception):
                pass
        # SESSION_SECRET fallback (tokens encrypted before TOKEN_ENCRYPT_KEY was set)
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.primitives import hashes
        secret = os.getenv("SESSION_SECRET", "fallback-insecure-key")
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32,
                         salt=b"hftoolbox-token-enc", iterations=100_000)
        raw = kdf.derive(secret.encode())
        try:
            return Fernet(base64.urlsafe_b64encode(raw)).decrypt(ciphertext).decode()
        except Exception:
            return None
    except Exception as e:
        log.debug("get_user_access_token failed uid=%s: %s", hf_uid, e)
        return None


# ── User check ────────────────────────────────────────────────────────────────

def toolbox_user_exists(hf_uid: str) -> bool:
    try:
        with _db() as cur:
            cur.execute(
                "SELECT 1 FROM integration_accounts WHERE hf_uid=%s LIMIT 1", (str(hf_uid),)
            )
            return cur.fetchone() is not None
    except Exception as e:
        log.debug("toolbox_user_exists failed for uid=%s: %s", hf_uid, e)
        return False
