"""
toolbox_bridge.py - Radar's optional read access to the Toolbox MySQL database.

Only used when TOOLBOX_DB_HOST is set in the environment.
Provides a separate PyMySQL connection pool (separate from Radar's own DB).

Key operations:
  get_user_access_token - read and decrypt a user's HF access token from Toolbox
  toolbox_user_exists   - check if a uid has a Toolbox account (integration_accounts)
"""

import os
import time
import base64
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
        from cryptography.fernet import Fernet, InvalidToken
        ciphertext = enc[2:].encode()
        key = os.getenv("TOKEN_ENCRYPT_KEY", "").strip()
        if key:
            try:
                return Fernet(key.encode()).decrypt(ciphertext).decode()
            except (InvalidToken, Exception):
                pass
        # SESSION_SECRET fallback for tokens encrypted before TOKEN_ENCRYPT_KEY was set
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
