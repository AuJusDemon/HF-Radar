"""
HF Radar Bot - Main Entry Point
Run:  python bot.py
Test: python bot.py --test
"""

import asyncio
import concurrent.futures
import datetime
import json
import logging
import sys
import time
import io
from pathlib import Path

# Force UTF-8 on Windows console
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("hfradar.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("hfradar")

# DB thread pool — dedicated, never shares with HF or Telegram
_db_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix="db")

async def _db(fn, *args):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_db_executor, fn, *args),
            timeout=15,
        )
    except asyncio.TimeoutError:
        log.error(f"DB call timed out: {fn.__name__}")
        return None
    except Exception as e:
        log.error(f"DB call failed: {fn.__name__}: {e}")
        return None


from telegram import Update
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.error import TimedOut, NetworkError

from db           import init_db, get_all_active_users, get_pending_auth, remove_pending_auth, get_user, upsert_user, get_loop_last_ran, set_loop_last_ran, prune_seen_events
from hf_client    import HFClient, AuthExpired, get_rate_limit_remaining
from telegram_bot import TelegramBot, radar_keyboard, radar_text
from commands     import handle_start, handle_radar, handle_cancel, handle_help, handle_text, handle_callback, handle_test, build_auth_url, handle_whois, handle_balance
from detectors    import check_account_events, check_posts, check_fid_threads, check_buddy_threads, check_bratings, check_disputes

CONFIG_FILE = Path("config.json")


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        log.error("config.json not found.")
        raise SystemExit(1)
    with open(CONFIG_FILE) as f:
        return json.load(f)


def _get(context: ContextTypes.DEFAULT_TYPE):
    bd = context.application.bot_data
    return bd["tg"], bd["cfg"], bd["db_cfg"]


# ── PTB Command / Message Handlers ────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    from commands import _state as _cst
    _cst.pop(update.effective_chat.id, None)
    await handle_start(update.effective_chat.id, tg, cfg, db_cfg)

async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    await handle_radar(update.effective_chat.id, tg, cfg, db_cfg)

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    await handle_cancel(update.effective_chat.id, tg, cfg, db_cfg)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    await handle_help(update.effective_chat.id, tg)

async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    await handle_test(update.effective_chat.id, tg, cfg, db_cfg)

async def cmd_whois(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    uid_arg = " ".join(context.args).strip() if context.args else ""
    await handle_whois(update.effective_chat.id, uid_arg, tg, cfg, db_cfg)

async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    await handle_balance(update.effective_chat.id, tg, db_cfg)

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    chat_id = update.effective_chat.id
    text    = update.message.text or ""
    await handle_text(chat_id, text, tg, cfg, db_cfg)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg, cfg, db_cfg = _get(context)
    q          = update.callback_query
    chat_id    = q.message.chat.id
    message_id = q.message.message_id
    data       = q.data or ""
    cb_id      = q.id
    q_dict     = {
        "id":      cb_id,
        "data":    data,
        "message": {"chat": {"id": chat_id}, "message_id": message_id},
        "from":    {"id": chat_id},
    }
    await handle_callback(q_dict, tg, cfg, db_cfg)

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, (TimedOut, NetworkError)):
        log.warning(f"PTB network error (auto-retry): {context.error}")
    else:
        log.exception(f"PTB unhandled error: {context.error}")


# ── Buddy fail-streak tracking ─────────────────────────────────────────────────

_buddy_fail_streaks: dict[int, int] = {}
_buddy_skip_until:  dict[int, int] = {}
BUDDY_FAIL_LIMIT  = 3
BUDDY_SKIP_CYCLES = 10
_slow_loop_cycle  = 0

def _buddy_should_skip(uid: int) -> bool:
    skip_until = _buddy_skip_until.get(uid, 0)
    return bool(skip_until and _slow_loop_cycle < skip_until)

def record_buddy_failure(uid: int) -> None:
    _buddy_fail_streaks[uid] = _buddy_fail_streaks.get(uid, 0) + 1
    streak = _buddy_fail_streaks[uid]
    if streak >= BUDDY_FAIL_LIMIT:
        resume = _slow_loop_cycle + BUDDY_SKIP_CYCLES
        _buddy_skip_until[uid] = resume
        log.warning(f"Buddy uid={uid} failed {streak}x — skipping {BUDDY_SKIP_CYCLES} cycles (resumes {resume})")

def record_buddy_success(uid: int) -> None:
    _buddy_fail_streaks.pop(uid, None)
    _buddy_skip_until.pop(uid, None)


# ── Background loop helpers ────────────────────────────────────────────────────

async def _run_medium(user, hf, tg, cfg, db_cfg):
    await check_account_events(user, hf, tg, cfg, db_cfg)

async def _run_slow(user, hf, tg, cfg, db_cfg, other_users=None):
    chat_id = user.get("chat_id")
    
    async def _handle_auth_expired():
        """Nuke token and notify user on 401."""
        from db import upsert_user as _upsert
        _upsert(db_cfg, chat_id, {"active": 0, "paused": 1, "access_token": None})
        await tg.send(chat_id,
            "⚠️ <b>HF Radar : Authorization expired</b>\n\n"
            "Your HackForums access was revoked or expired.\n"
            "All your settings have been saved.\n\n"
            "Use /start to reconnect and pick up where you left off."
        )
    
    try:
        await asyncio.wait_for(check_posts(user, hf, tg, cfg, db_cfg, other_users=other_users), timeout=50)
    except AuthExpired:
        await _handle_auth_expired()
        return
    except asyncio.TimeoutError:
        log.warning(f"check_posts timed out chat_id={chat_id}")
    except Exception as e:
        log.warning(f"check_posts error chat_id={chat_id}: {e}")

    # FID forum watching runs independently — not inside check_posts — so it
    # always gets its own timeout budget regardless of how long thread polling ran.
    try:
        await asyncio.wait_for(check_fid_threads(user, hf, tg, cfg, db_cfg), timeout=20)
    except AuthExpired:
        await _handle_auth_expired()
        return
    except asyncio.TimeoutError:
        log.warning(f"check_fid_threads timed out chat_id={chat_id}")
    except Exception as e:
        log.warning(f"check_fid_threads error chat_id={chat_id}: {e}")

    # B-ratings moved from medium loop — rare events, 3-min cadence is fine
    try:
        await asyncio.wait_for(check_bratings(user, hf, tg, cfg, db_cfg), timeout=15)
    except AuthExpired:
        await _handle_auth_expired()
        return
    except asyncio.TimeoutError:
        log.warning(f"check_bratings timed out chat_id={chat_id}")
    except Exception as e:
        log.warning(f"check_bratings error chat_id={chat_id}: {e}")

    # Disputes safety net — catches disputes on old contracts that aged out of contract_states
    try:
        await asyncio.wait_for(check_disputes(user, hf, tg, cfg, db_cfg), timeout=15)
    except AuthExpired:
        await _handle_auth_expired()
        return
    except asyncio.TimeoutError:
        log.warning(f"check_disputes timed out chat_id={chat_id}")
    except Exception as e:
        log.warning(f"check_disputes error chat_id={chat_id}: {e}")


    buddies = user.get("buddy_list") or []
    active_buddies = []
    for b in buddies:
        uid = int(b.get("uid") or 0)
        if uid and not _buddy_should_skip(uid):
            active_buddies.append(b)
        elif uid:
            log.info(f"Skipping buddy uid={uid} (cooldown, resumes cycle {_buddy_skip_until.get(uid)})")

    if not active_buddies:
        return

    user_filtered = {**user, "buddy_list": active_buddies}
    buddy_timeout = 20 * len(active_buddies) + 10

    try:
        await asyncio.wait_for(
            check_buddy_threads(user_filtered, hf, tg, cfg, db_cfg),
            timeout=buddy_timeout,
        )
        for b in active_buddies:
            uid = int(b.get("uid") or 0)
            if uid:
                record_buddy_success(uid)
    except AuthExpired:
        await _handle_auth_expired()
        return
    except asyncio.TimeoutError:
        log.warning(f"check_buddy_threads timed out chat_id={chat_id}")
        for b in active_buddies:
            uid = int(b.get("uid") or 0)
            if uid:
                record_buddy_failure(uid)
    except Exception as e:
        log.warning(f"check_buddy_threads error chat_id={chat_id}: {e}")
        for b in active_buddies:
            uid = int(b.get("uid") or 0)
            if uid:
                record_buddy_failure(uid)


# ── Background Loops ──────────────────────────────────────────────────────────

async def auth_loop(tg, cfg, db_cfg):
    while True:
        try:
            pending = await _db(get_pending_auth, db_cfg) or []
            if pending:
                await check_pending_auths_list(pending, tg, cfg, db_cfg)
                await asyncio.sleep(3)   # fast poll while someone is in auth flow
            else:
                await asyncio.sleep(8)   # idle — back off to reduce DB load
        except Exception as e:
            log.exception(f"Auth loop error: {e}")
            await asyncio.sleep(10)

async def medium_loop(tg, cfg, db_cfg):
    INTERVAL  = 120
    LOOP_NAME = "medium"
    sem       = asyncio.Semaphore(5)

    async def _med_jitter_run(user, delay):
        if delay > 0:
            await asyncio.sleep(delay)
        async with sem:
            try:
                hf = HFClient(user["access_token"])
                await asyncio.wait_for(_run_medium(user, hf, tg, cfg, db_cfg), timeout=30)
            except asyncio.TimeoutError:
                log.warning(f"Medium loop timed out chat_id={user.get('chat_id')}")
            except Exception as e:
                log.exception(f"Medium loop error chat_id={user.get('chat_id')}: {e}")

    while True:
        now      = int(time.time())
        last_ran = await _db(get_loop_last_ran, db_cfg, LOOP_NAME)
        if last_ran:
            elapsed = now - last_ran
            wait    = max(0, INTERVAL - elapsed)
            if wait > 1:
                log.info(f"medium_loop: last ran {elapsed}s ago — waiting {wait}s before next cycle")
                await asyncio.sleep(wait)

        try:
            users  = await _db(get_all_active_users, db_cfg) or []
            active = [u for u in users if not u.get("paused") and u.get("access_token")]
            if active:
                jitter_step = min(INTERVAL / max(len(active), 1), 15)
                tasks = [
                    asyncio.create_task(_med_jitter_run(u, i * jitter_step))
                    for i, u in enumerate(active)
                ]
                await asyncio.gather(*tasks)
            for _u in active:
                log.info(f"Medium loop done — user={_u.get('hf_username','?')} — API calls remaining: {get_rate_limit_remaining(_u.get('access_token',''))}")
        except Exception as e:
            log.exception(f"Medium loop error: {e}")

        await _db(set_loop_last_ran, db_cfg, LOOP_NAME)

async def slow_loop(tg, cfg, db_cfg):
    global _slow_loop_cycle
    INTERVAL             = 180
    LOOP_NAME            = "slow"
    FRESH_INSTALL_OFFSET = 15
    sem                  = asyncio.Semaphore(5)

    async def _slow_jitter_run(user, delay, all_active):
        if delay > 0:
            await asyncio.sleep(delay)
        async with sem:
            try:
                hf    = HFClient(user["access_token"])
                other = [u for u in all_active if u.get("chat_id") != user.get("chat_id")]
                await asyncio.wait_for(_run_slow(user, hf, tg, cfg, db_cfg, other_users=other), timeout=130)
            except asyncio.TimeoutError:
                log.warning(f"Slow loop timed out chat_id={user.get('chat_id')}")
            except Exception as e:
                log.exception(f"Slow loop error chat_id={user.get('chat_id')}: {e}")

    while True:
        _slow_loop_cycle += 1

        now      = int(time.time())
        last_ran = await _db(get_loop_last_ran, db_cfg, LOOP_NAME)
        if last_ran:
            elapsed = now - last_ran
            wait    = max(0, INTERVAL - elapsed)
            if wait > 1:
                log.info(f"slow_loop: last ran {elapsed}s ago — waiting {wait}s before next cycle")
                await asyncio.sleep(wait)
        else:
            log.info(f"slow_loop: no prior run recorded — waiting {FRESH_INSTALL_OFFSET}s initial offset")
            await asyncio.sleep(FRESH_INSTALL_OFFSET)

        try:
            users  = await _db(get_all_active_users, db_cfg) or []
            active = [u for u in users if not u.get("paused") and u.get("access_token")]
            if active:
                jitter_step = min(INTERVAL / max(len(active), 1), 20)
                tasks = [
                    asyncio.create_task(_slow_jitter_run(u, i * jitter_step, active))
                    for i, u in enumerate(active)
                ]
                await asyncio.gather(*tasks)
            for _u in active:
                log.info(f"Slow loop cycle={_slow_loop_cycle} done — user={_u.get('hf_username','?')} — API calls remaining: {get_rate_limit_remaining(_u.get('access_token',''))}")
        except Exception as e:
            log.exception(f"Slow loop error: {e}")

        await _db(set_loop_last_ran, db_cfg, LOOP_NAME)

async def _restartable(name: str, coro_fn, *args):
    while True:
        try:
            await coro_fn(*args)
        except Exception as e:
            log.error(f"{name} crashed: {e} — restarting in 5s")
            await asyncio.sleep(5)


# ── Schedule Loop (every 5 min — balance snapshots + weekly digest) ───────────

async def schedule_loop(tg, cfg, db_cfg):
    """
    Decoupled from API polling — handles periodic non-API tasks:
      - Daily balance snapshot (written to balance_history for /balance command)
      - Weekly digest (pure DB aggregation, zero API calls, default Monday 9 AM)
    """
    INTERVAL = 300   # 5 minutes
    PRUNE_INTERVAL = 86400  # prune seen_events once a day
    while True:
        await asyncio.sleep(INTERVAL)
        try:
            users  = await _db(get_all_active_users, db_cfg) or []
            active = [u for u in users if not u.get("paused") and u.get("access_token")]
            now    = int(time.time())

            # ── Daily seen_events prune ──────────────────────────────────────
            last_prune = await _db(get_loop_last_ran, db_cfg, "seen_events_prune") or 0
            if (now - last_prune) >= PRUNE_INTERVAL:
                deleted = await _db(prune_seen_events, db_cfg)
                await _db(set_loop_last_ran, db_cfg, "seen_events_prune")
                log.info(f"seen_events prune: {deleted} rows deleted")

            for user in active:
                chat_id = user["chat_id"]

                # ── Daily snapshot: balance + postnum + threadnum ────────────
                last_snap = int(user.get("last_balance_snap_at") or 0)
                if (now - last_snap) >= 86400:
                    snap = {"last_balance_snap_at": now}

                    bal = user.get("last_bytes_balance")
                    if bal is not None:
                        bh = list(user.get("balance_history") or [])
                        bh.append({"ts": now, "val": float(bal)})
                        snap["balance_history"] = bh[-30:]

                    pn = user.get("last_postnum")
                    if pn is not None:
                        ph = list(user.get("postnum_history") or [])
                        ph.append({"ts": now, "val": int(pn)})
                        snap["postnum_history"] = ph[-30:]

                    tn = user.get("last_threadnum")
                    if tn is not None:
                        th = list(user.get("threadnum_history") or [])
                        th.append({"ts": now, "val": int(tn)})
                        snap["threadnum_history"] = th[-30:]

                    await _db(upsert_user, db_cfg, chat_id, snap)

                # ── Weekly digest ────────────────────────────────────────────
                # Default: Monday (weekday 0), 09:00 local — stored as
                # last_digest_at epoch. Fire if >= 7 days since last send.
                last_digest = int(user.get("last_digest_at") or 0)
                weekday = datetime.datetime.now(datetime.timezone.utc).weekday()   # 0=Mon
                hour    = datetime.datetime.now(datetime.timezone.utc).hour
                if (now - last_digest) >= 7 * 86400 and weekday == 0 and hour == 9:
                    await _send_weekly_digest(user, tg, db_cfg, now)

        except Exception as e:
            log.exception(f"schedule_loop error: {e}")


async def _send_weekly_digest(user: dict, tg, db_cfg: dict, now: int):
    chat_id     = user["chat_id"]
    username    = user.get("hf_username") or f"UID {user.get('hf_uid', '?')}"
    week_ago_ts = now - 7 * 86400
    prev_ago_ts = now - 14 * 86400

    def _delta(history: list, cutoff: int):
        """(current_val, delta_from_cutoff) or (None, None). Needs >= 2 entries."""
        if len(history) < 2:
            return None, None
        current = history[-1]["val"]
        past    = next((e for e in reversed(history) if e["ts"] <= cutoff), history[0])
        return current, current - past["val"]

    def _fmt_delta(n):
        sign = "+" if n >= 0 else ""
        return f"{sign}{int(n):,}"

    def _wow(hist, w_ts, pw_ts):
        """Week-over-week comparison string, empty if not enough data."""
        _, d_this = _delta(hist, w_ts)
        _, d_prev = _delta(hist, pw_ts)
        if d_this is None or d_prev is None:
            return ""
        diff  = d_this - d_prev
        arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "→")
        label = _fmt_delta(abs(diff)) if diff != 0 else "same"
        return f"  <i>({arrow} {label} vs last week)</i>"

    lines = [f"📊 <b>HF Radar Weekly Digest — {username}</b>\n"]

    # ── Balance ───────────────────────────────────────────────────────────────
    bal_hist               = list(user.get("balance_history") or [])
    current_bal, delta_bal = _delta(bal_hist, week_ago_ts)
    if current_bal is not None:
        bal_str = f"{current_bal:,.1f}" if current_bal != int(current_bal) else f"{int(current_bal):,}"
        wow     = _wow(bal_hist, week_ago_ts, prev_ago_ts)
        lines.append(f"💰 Balance: <b>{bal_str} bytes</b>  {_fmt_delta(delta_bal)} this week{wow}")

        # Biggest single tx this week
        bytes_log = list(user.get("bytes_log") or [])
        week_txs  = [t for t in bytes_log if int(t.get("ts") or 0) >= week_ago_ts]
        if week_txs:
            biggest    = max(week_txs, key=lambda t: abs(int(t.get("amount") or 0)))
            b_amt      = int(biggest.get("amount") or 0)
            b_reason   = biggest.get("reason") or ""
            b_from     = biggest.get("from_user") or ""
            from_str   = f" from <b>{b_from}</b>" if b_from else ""
            reason_str = f"  <i>{b_reason}</i>" if b_reason else ""
            lines.append(f"   ↳ biggest: <b>{b_amt:,} bytes</b>{from_str}{reason_str}")

    # ── Posts ─────────────────────────────────────────────────────────────────
    pn_hist              = list(user.get("postnum_history") or [])
    current_pn, delta_pn = _delta(pn_hist, week_ago_ts)
    if current_pn is not None and delta_pn is not None:
        wow = _wow(pn_hist, week_ago_ts, prev_ago_ts)
        lines.append(f"✏️ Posts: <b>{int(current_pn):,} total</b>  {_fmt_delta(delta_pn)} this week{wow}")

    # ── Threads ───────────────────────────────────────────────────────────────
    tn_hist              = list(user.get("threadnum_history") or [])
    current_tn, delta_tn = _delta(tn_hist, week_ago_ts)
    if current_tn is not None and delta_tn is not None:
        wow = _wow(tn_hist, week_ago_ts, prev_ago_ts)
        lines.append(f"🧵 Threads: <b>{int(current_tn):,} total</b>  {_fmt_delta(delta_tn)} this week{wow}")

    # ── Popularity ────────────────────────────────────────────────────────────
    rep_hist               = list(user.get("rep_history") or [])
    current_rep, delta_rep = _delta(rep_hist, week_ago_ts)
    if current_rep is not None:
        wow = _wow(rep_hist, week_ago_ts, prev_ago_ts)
        lines.append(f"⭐ Popularity: <b>{int(current_rep):,}</b>  {_fmt_delta(delta_rep)} this week{wow}")

    # ── Warning points ────────────────────────────────────────────────────────
    wp = int(user.get("last_warningpoints") or 0)
    if wp > 0:
        lines.append(f"⚠️ Warning points: <b>{wp}</b>")

    lines.append("\n🔗 hackforums.net/usercp.php")

    await tg.send(chat_id, "\n".join(lines))
    await _db(upsert_user, db_cfg, chat_id, {"last_digest_at": now})
    log.info(f"Weekly digest sent: chat_id={chat_id}")


# ── Auth Flow ──────────────────────────────────────────────────────────────────

async def check_pending_auths(tg, cfg, db_cfg):
    """Legacy entry point — fetches pending list then processes it."""
    pending = await _db(get_pending_auth, db_cfg) or []
    if pending:
        await check_pending_auths_list(pending, tg, cfg, db_cfg)


async def check_pending_auths_list(pending: list, tg, cfg, db_cfg):
    """Process a pre-fetched list of pending auth records."""
    for p in pending:
        chat_id   = p["chat_id"]
        auth_code = p.get("auth_code")
        if not auth_code:
            continue

        user = await _db(get_user, db_cfg, chat_id)
        # Only skip if user already has a live token — reconnecting users have
        # welcome_sent=1 but access_token=NULL, so we must let those through.
        if user and user.get("welcome_sent") and user.get("access_token"):
            await _db(remove_pending_auth, db_cfg, chat_id)
            continue

        log.info(f"Exchanging token for chat_id={chat_id}")
        from hf_client import exchange_code_for_token

        access_token, token_expires, _ = None, None, None
        for _attempt in range(3):
            access_token, token_expires, _ = await exchange_code_for_token(auth_code, cfg)
            if access_token:
                break
            if _attempt < 2:
                log.warning(f"Token exchange attempt {_attempt+1} failed for chat_id={chat_id}, retrying in 3s")
                await asyncio.sleep(3)

        if not access_token:
            # Check if the code is less than 9 minutes old — if so, leave it and retry next cycle
            code_age = int(time.time()) - int(p.get("created_at") or 0)
            if code_age < 540:  # 9 minutes
                log.warning(f"Token exchange failed for chat_id={chat_id} but code is only {code_age}s old — will retry")
                continue
            log.warning(f"Token exchange definitively failed for chat_id={chat_id} (code age {code_age}s)")
            await _db(remove_pending_auth, db_cfg, chat_id)
            await tg.send(chat_id,
                "couldn't connect — HackForums might be down or the link expired. tap below to try again.",
                reply_markup={"inline_keyboard": [[
                    {"text": "🔗 Connect HackForums", "url": build_auth_url(cfg, chat_id)}
                ]]}
            )
            continue

        token_expires = token_expires or int(time.time()) + 7776000
        hf      = HFClient(access_token)
        me_data = await hf.read({"me": {"uid": True, "username": True}})
        username, hf_uid = "", 0
        if me_data and "me" in me_data:
            me = me_data["me"]
            if isinstance(me, list): me = me[0]
            username = me.get("username", "")
            hf_uid   = int(me.get("uid") or 0)

        if not hf_uid:
            log.warning(f"Could not fetch hf_uid for chat_id={chat_id}")
            continue

        now        = int(time.time())
        is_reconnect = bool(user and user.get("welcome_sent"))
        upsert_data  = {
            "hf_uid":        hf_uid,
            "hf_username":   username,
            "access_token":  access_token,
            "linked_at":     now,
            "token_expires": token_expires,
            "welcome_sent":  1,
            "active":        1,
            "paused":        0,
        }
        # Don't reset join timestamp for reconnecting users
        if not is_reconnect:
            upsert_data["joined_at"] = now
        await _db(upsert_user, db_cfg, chat_id, upsert_data)

        await _db(remove_pending_auth, db_cfg, chat_id)
        user = await _db(get_user, db_cfg, chat_id)

        if is_reconnect:
            await tg.send(chat_id, f"✅ reconnected as <b>{username}</b> — you're back online.")
            log.info(f"Reconnected user: chat_id={chat_id} hf_uid={hf_uid} username={username}")
        else:
            await tg.send(chat_id,
                f"you're in, {username or f'UID {hf_uid}'}\n\n"
                f"everything is on by default. check the descriptions in the menu so you know what each thing actually tracks."
            )
            log.info(f"New user: chat_id={chat_id} hf_uid={hf_uid} username={username}")
        await tg.send(chat_id, radar_text(user), reply_markup=radar_keyboard(user, cfg))


# ── post_init: start loops AFTER app is running ────────────────────────────────

async def post_init(application: Application) -> None:
    tg     = application.bot_data["tg"]
    cfg    = application.bot_data["cfg"]
    db_cfg = application.bot_data["db_cfg"]

    application.create_task(_restartable("auth_loop",      auth_loop,      tg, cfg, db_cfg))
    application.create_task(_restartable("medium_loop",    medium_loop,    tg, cfg, db_cfg))
    application.create_task(_restartable("slow_loop",      slow_loop,      tg, cfg, db_cfg))
    application.create_task(_restartable("schedule_loop",  schedule_loop,  tg, cfg, db_cfg))
    log.info("Background loops started.")


# ── Test Mode ─────────────────────────────────────────────────────────────────

async def run_test(cfg):
    log.info("TEST MODE")
    tg      = TelegramBot(cfg["telegram_token"])
    chat_id = cfg["test_chat_id"]
    await tg.send(chat_id, "🧪 <b>HF Radar Test Mode</b>")
    await tg.send(chat_id, "💬 <b>New reply</b> in <i>My Thread</i>\n🔗 hackforums.net/showthread.php?tid=123&pid=456#pid456\n\nTest reply content here.")
    await tg.send(chat_id, "🔔 <b>You were mentioned</b> in <i>General Discussion</i>\n🔗 hackforums.net/showthread.php?tid=456&pid=789#pid789\n\nHey check this out")
    await tg.send(chat_id, "📨 <b>3 new private messages</b>\nTotal unread: 3\n🔗 hackforums.net/private.php")
    await tg.send(chat_id, "📝 <b>New Contract #12345</b>\nProduct: VPN Service\nPrice: 500 bytes\nStatus: pending\n🔗 hackforums.net/contract.php?cid=12345")
    await tg.send(chat_id, "👍 <b>New B-Rating</b> on Contract #12300\nFrom UID: 55555\nRating: +1\nMessage: Fast delivery!")
    await tg.send(chat_id, "💰 <b>250 bytes received!</b>\nReason: Test payment\n🔗 hackforums.net/myps.php?action=history")
    await tg.send(chat_id, "👥 <b>xVendor</b> posted a new thread\n<i>My New Service Thread</i>\n🔗 hackforums.net/showthread.php?tid=999")
    await tg.send(chat_id, "✅ <b>Test complete!</b>")
    log.info("Test done.")


# ── Main ──────────────────────────────────────────────────────────────────────

async def post_init_no_poll(application: Application) -> None:
    """post_init variant that skips medium + slow loops — for UI/command debugging."""
    tg     = application.bot_data["tg"]
    cfg    = application.bot_data["cfg"]
    db_cfg = application.bot_data["db_cfg"]
    application.create_task(_restartable("auth_loop",     auth_loop,     tg, cfg, db_cfg))
    application.create_task(_restartable("schedule_loop", schedule_loop, tg, cfg, db_cfg))
    log.info("Background loops started (--no-poll: medium + slow loops SKIPPED).")


def main():
    cfg     = load_config()
    db_cfg  = cfg["database"]

    from hf_client import configure as _configure_hf
    _configure_hf(cfg)
    no_poll = "--no-poll" in sys.argv

    # Optional startup delay — set startup_delay_seconds in config.json (default 0)
    startup_delay = int(cfg.get("startup_delay_seconds") or 0)
    if startup_delay > 0:
        log.info(f"Startup delay: {startup_delay}s")
        time.sleep(startup_delay)

    init_db(db_cfg)

    tg = TelegramBot(cfg["telegram_token"])

    app = (
        ApplicationBuilder()
        .token(cfg["telegram_token"])
        .post_init(post_init_no_poll if no_poll else post_init)
        .build()
    )

    app.bot_data["tg"]     = tg
    app.bot_data["cfg"]    = cfg
    app.bot_data["db_cfg"] = db_cfg

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("radar",   cmd_radar))
    app.add_handler(CommandHandler("cancel",  cmd_cancel))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("test",    cmd_test))
    app.add_handler(CommandHandler("whois",   cmd_whois))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    app.add_error_handler(on_error)

    if no_poll:
        log.info("HF Radar starting in --no-poll mode (API loops disabled)...")
    else:
        log.info("HF Radar starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    cfg = load_config()
    if "--test" in sys.argv:
        asyncio.run(run_test(cfg))
    else:
        main()