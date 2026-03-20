"""
Command handlers and callback routing.
Everything lives in /radar. /start is the entry command.
"""

import time
import logging

from db import get_user, upsert_user, add_pending_auth, remove_pending_auth
import asyncio

async def _db(fn, *args):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(loop.run_in_executor(None, fn, *args), timeout=15)
    except Exception as e:
        log.error("DB error in commands: " + fn.__name__ + ": " + str(e))
        return None
from telegram_bot import TelegramBot, radar_keyboard, radar_text, alerts_keyboard, forums_keyboard, DEFAULT_NOTIFICATIONS, MAX_FORUMS

log = logging.getLogger("hfradar.commands")

MAX_BUDDIES   = 5

# ── In-memory state machine ────────────────────────────────────────────────────
# {chat_id: {"step": str, ...extra data}}
_state: dict = {}


def _clear(chat_id: int):
    _state.pop(chat_id, None)


def build_auth_url(cfg: dict, chat_id: int) -> str:
    return (
        f"https://hackforums.net/api/v2/authorize"
        f"?response_type=code"
        f"&client_id={cfg['hf_client_id']}"
        f"&state={chat_id}"
    )


# ── /start ─────────────────────────────────────────────────────────────────────

async def handle_start(chat_id: int, tg: TelegramBot, cfg: dict, db_cfg: dict):
    _clear(chat_id)
    user = get_user(db_cfg, chat_id)

    if user:
        if user.get("access_token"):
            from telegram_bot import radar_keyboard, radar_text
            await tg.send(chat_id, radar_text(user), reply_markup=radar_keyboard(user, cfg))
        else:
            auth_url = build_auth_url(cfg, chat_id)
            add_pending_auth(db_cfg, chat_id)
            await _db(upsert_user, db_cfg, chat_id, {"active": 1})
            await tg.send(chat_id,
                "📡 <b>HF Radar</b>\n\n"
                "Your settings are still here, just reconnect and everything picks back up.",
                reply_markup={"inline_keyboard": [[
                    {"text": "🔗 Connect HackForums", "url": auth_url}
                ]]}
            )
        return

    auth_url = build_auth_url(cfg, chat_id)
    add_pending_auth(db_cfg, chat_id)

    await tg.send(chat_id,
        "📡 <b>HF Radar</b>\n\n"
        "Never miss a contract, reply, or byte again.\n\n"
        "Connect your HackForums account to get instant Telegram alerts for thread replies, mentions, contracts, bytes, PMs, and more.\n\n"
        "Takes 30 seconds.",
        reply_markup={"inline_keyboard": [[
            {"text": "🔗 Connect HackForums", "url": auth_url}
        ]]}
    )


# ── /radar ─────────────────────────────────────────────────────────────────────

async def handle_radar(chat_id: int, tg: TelegramBot, cfg: dict, db_cfg: dict):
    _clear(chat_id)
    user = get_user(db_cfg, chat_id)
    if not user:
        await tg.send(chat_id, "You're not connected yet. Use /start to link your account.")
        return
    await tg.send(chat_id, radar_text(user), reply_markup=radar_keyboard(user, cfg))


async def handle_cancel(chat_id: int, tg: TelegramBot, cfg: dict = None, db_cfg: dict = None):
    _clear(chat_id)
    if db_cfg:
        from db import get_user
        user = get_user(db_cfg, chat_id)
        if user:
            from telegram_bot import radar_keyboard, radar_text
            await tg.send(chat_id, radar_text(user), reply_markup=radar_keyboard(user, cfg))
            return
    await tg.send(chat_id, "cancelled")


# ── /help ──────────────────────────────────────────────────────────────────────

async def handle_help(chat_id: int, tg: TelegramBot):
    _clear(chat_id)
    await tg.send(chat_id,
        "📡 <b>HF Radar</b>\n\n"
        "/start   - link your HF account\n"
        "/radar   - open your dashboard\n"
        "/whois   - look up a HF user by UID\n"
        "/balance - your bytes balance history\n"
        "/cancel  - cancel whatever you're doing\n"
        "/help    - this"
    )


# ── /whois ─────────────────────────────────────────────────────────────────────

async def handle_whois(chat_id: int, uid_arg: str, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """
    User lookup — single API call to users endpoint only.
    contracts/disputes/bratings endpoints 503 for third-party UIDs (owner-only scope).
    """
    user = await _db(get_user, db_cfg, chat_id)
    if not user or not user.get("access_token"):
        await tg.send(chat_id, "connect first, use /start")
        return

    uid_str = uid_arg.strip()
    if not uid_str or not uid_str.isdigit():
        await tg.send(chat_id,
            "Usage: <code>/whois 12345</code>\n"
            "Provide a HackForums UID."
        )
        return

    uid = int(uid_str)
    from hf_client import HFClient, get_rate_limit_remaining
    hf = HFClient(user["access_token"])

    data = await hf.read({
        "users": {
            "_uid":         [uid],
            "uid":          True,
            "username":     True,
            "usergroup":    True,
            "displaygroup": True,
            "postnum":      True,
            "threadnum":    True,
            "reputation":   True,
            "myps":         True,
            "awards":       True,
            "usertitle":    True,
            "referrals":    True,
            "timeonline":   True,
            "website":      True,
        }
    })
    log.info(f"/whois uid={uid} — remaining: {get_rate_limit_remaining(hf.token)}")

    if not data or "users" not in data:
        await tg.send(chat_id, f"No user found for UID <code>{uid}</code>.")
        return
    u = data["users"]
    if isinstance(u, list): u = u[0] if u else None
    if not u:
        await tg.send(chat_id, f"No user found for UID <code>{uid}</code>.")
        return

    username     = u.get("username", f"UID {uid}")
    postcount    = int(u.get("postnum") or 0)
    threadnum    = int(u.get("threadnum") or 0)
    reputation   = int(float(u.get("reputation") or 0))
    myps_raw     = u.get("myps")
    usertitle    = (u.get("usertitle") or "").strip()
    awards_raw   = u.get("awards")
    referrals    = int(u.get("referrals") or 0)
    website      = (u.get("website") or "").strip()
    usergroup    = str(u.get("usergroup") or "")
    displaygroup = str(u.get("displaygroup") or "")

    # Account status
    UG_LABELS  = {"7": "⚓ Exiled", "38": "☠️ Banned"}
    status_tag = UG_LABELS.get(usergroup) or UG_LABELS.get(displaygroup) or ""

    # Bytes
    try:
        myps_f   = float(myps_raw)
        myps_str = f"{myps_f:,.1f}" if myps_f != int(myps_f) else f"{int(myps_f):,}"
    except (TypeError, ValueError):
        myps_str = str(myps_raw or "?")

    # Awards
    try:
        awards_count = len(awards_raw) if isinstance(awards_raw, list) else (len(str(awards_raw).split(",")) if awards_raw else 0)
    except Exception:
        awards_count = 0

    # timeonline — API returns raw seconds
    try:
        _secs = int(u.get("timeonline") or 0)
        _days = _secs // 86400
        _hrs  = (_secs % 86400) // 3600
        if _days >= 30:
            _mo = _days // 30; _d = _days % 30
            timeonline = f"{_mo}mo {_d}d" if _d else f"{_mo}mo"
        elif _days >= 1:
            timeonline = f"{_days}d {_hrs}h" if _hrs else f"{_days}d"
        elif _hrs >= 1:
            timeonline = f"{_hrs}h"
        else:
            timeonline = ""
    except (TypeError, ValueError):
        timeonline = ""

    # ── Compose output ────────────────────────────────────────────────────────
    lines = [f"🔍 <b>{username}</b>  (UID <code>{uid}</code>)"]
    if status_tag:
        lines.append(status_tag)
    if usertitle:
        lines.append(f"<i>{usertitle}</i>")
    lines.append("")
    lines.append(f"📝 {postcount:,} posts  ·  {threadnum:,} threads")
    lines.append(f"⭐ {reputation:,} popularity  ·  💰 {myps_str} bytes")
    if timeonline:
        lines.append(f"🕐 {timeonline} online")
    if referrals:
        lines.append(f"👥 {referrals:,} referrals")
    if awards_count:
        lines.append(f"🏆 {awards_count} award{'s' if awards_count != 1 else ''}")
    if website:
        lines.append(f"🌐 {website}")
    lines.append("")
    lines.append(f"🔗 hackforums.net/member.php?action=profile&uid={uid}")

    await tg.send(chat_id, "\n".join(lines))


# ── /balance ───────────────────────────────────────────────────────────────────

def _sparkline(values: list) -> str:
    """Return a 7-character text sparkline from a list of floats."""
    bars = "▁▂▃▄▅▆▇█"
    if not values or len(values) < 2:
        return "─"
    mn, mx = min(values), max(values)
    rng    = mx - mn or 1
    return "".join(bars[int((v - mn) / rng * (len(bars) - 1))] for v in values)


async def handle_balance(chat_id: int, tg: TelegramBot, db_cfg: dict):
    """
    Show bytes balance: current balance, weekly delta sparkline from snapshots,
    and the last 10 transactions pulled live from the API.
    """
    user = await _db(get_user, db_cfg, chat_id)
    if not user or not user.get("access_token"):
        await tg.send(chat_id, "connect first, use /start")
        return

    from hf_client import HFClient, get_rate_limit_remaining
    import time as _t

    my_uid  = user["hf_uid"]
    hf      = HFClient(user["access_token"])

    # Live API call — current balance + last 20 transactions
    data = await hf.read({
        "me": {
            "uid":   True,
            "bytes": True,
        },
        "bytes": {
            "_to":      [my_uid],
            "_perpage": 20,
            "id":       True,
            "amount":   True,
            "dateline": True,
            "reason":   True,
            "type":     True,
            "from":     True,
        },
    })
    log.info(f"/balance API call — remaining: {get_rate_limit_remaining(hf.token)}")

    now      = int(_t.time())
    week_ago = now - 7 * 86400

    # ── Current balance ───────────────────────────────────────────────────────
    current = None
    if data and "me" in data:
        me = data["me"]
        if isinstance(me, list): me = me[0]
        raw = me.get("bytes") if me.get("bytes") is not None else me.get("myps")
        if raw is not None:
            try:
                current = float(raw)
                # Persist latest balance so radar panel stays current
                await _db(upsert_user, db_cfg, chat_id, {"last_bytes_balance": current})
            except (ValueError, TypeError):
                pass

    if current is None:
        current = user.get("last_bytes_balance")

    cur_str = f"{float(current):,.1f}" if current is not None else "?"

    # ── Weekly delta from balance_history snapshots ───────────────────────────
    history   = list(user.get("balance_history") or [])
    delta_str = ""
    spark     = ""
    if history:
        recent = [e["val"] for e in history if e.get("ts", 0) >= week_ago]
        if current is not None:
            recent.append(float(current))
        if len(recent) >= 2:
            spark = f"<code>{_sparkline(recent)}</code>  "

        past = next((e["val"] for e in reversed(history) if e.get("ts", 0) <= week_ago), None)
        if past is not None and current is not None:
            delta  = float(current) - past
            sign   = "+" if delta >= 0 else ""
            delta_str = f"  ({sign}{delta:,.0f} this week)"

    # ── Resolve sender UIDs → usernames in one batch call ────────────────────
    txs = []
    if data and "bytes" in data:
        txs = data["bytes"]
        if isinstance(txs, dict): txs = [txs]
        txs = txs or []

    uids_to_resolve = set()
    for tx in txs:
        uid = tx.get("from")
        if uid and str(uid).isdigit() and int(uid) != my_uid:
            uids_to_resolve.add(int(uid))

    uid_names: dict = {}
    if uids_to_resolve:
        uid_data = await hf.read({
            "users": {"_uid": list(uids_to_resolve), "uid": True, "username": True}
        })
        if uid_data and "users" in uid_data:
            rows = uid_data["users"]
            if isinstance(rows, dict): rows = [rows]
            for u in (rows or []):
                try:
                    uid_names[int(u["uid"])] = u.get("username", "")
                except (KeyError, ValueError, TypeError):
                    pass

    def _resolve(uid) -> str:
        if not uid: return ""
        try: n = int(uid)
        except (ValueError, TypeError): return str(uid)
        return uid_names.get(n) or f"UID {uid}"

    # ── Format transaction list ───────────────────────────────────────────────
    TYPE_LABELS = {
        "1": "received",
        "2": "sent",
        "3": "admin",
        "4": "system",
    }
    tx_lines = []
    for tx in txs[:10]:
        amount   = int(float(tx.get("amount", 0) or 0))
        reason   = (tx.get("reason") or "").strip()
        from_uid = tx.get("from")
        tx_type  = str(tx.get("type", ""))
        dl       = int(tx.get("dateline") or 0)
        sender   = _resolve(from_uid) if from_uid and str(from_uid).isdigit() else ""

        # Date: "today HH:MM" or "Mar 01"
        if dl:
            import datetime as _dt
            dt = _dt.datetime.utcfromtimestamp(dl)
            if (now - dl) < 86400:
                date_str = dt.strftime("%H:%M")
            else:
                date_str = dt.strftime("%b %d")
        else:
            date_str = ""

        sign   = "+" if amount >= 0 else ""
        sender_str = f" ← {sender}" if sender else ""
        reason_str = f"  <i>{reason[:40]}</i>" if reason else ""
        date_col   = f"[{date_str}]  " if date_str else ""

        tx_lines.append(f"{date_col}<b>{sign}{amount:,}</b>{sender_str}{reason_str}")

    # ── Compose message ───────────────────────────────────────────────────────
    header = (
        f"💰 <b>Bytes Balance</b>\n\n"
        f"{spark}<b>{cur_str}</b>{delta_str}\n"
    )

    if tx_lines:
        body = "\n\n<b>Recent transactions</b>\n" + "\n".join(tx_lines)
    else:
        body = ""

    footer = "\n\n🔗 hackforums.net/myps.php?action=history"

    await tg.send(chat_id, header + body + footer)


# ── Free text input handler ────────────────────────────────────────────────────

async def handle_text(chat_id: int, text: str, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """Routes free-text input based on current state."""
    state = _state.get(chat_id)
    if not state:
        return

    step = state.get("step")

    # ── Forum watch: waiting for FID ───────────────────────────────────────
    if step == "fid_add":
        cleaned = text.strip()
        if not cleaned.isdigit():
            _clear(chat_id)
            await tg.send(chat_id, "that\'s not a valid FID, try again or /cancel")
            return
        fid = int(cleaned)
        _state[chat_id] = {"step": "fid_confirm_uid", "fid": fid}
        await _fid_lookup(chat_id, fid, tg, cfg, db_cfg)

    # ── Buddy list: waiting for UID ─────────────────────────────────────────
    elif step == "buddy_add_uid":
        cleaned = text.strip()
        if not cleaned.isdigit():
            _clear(chat_id)
            await tg.send(chat_id, "that's not a valid UID, try again")
            return
        uid = int(cleaned)
        _state[chat_id] = {"step": "buddy_confirm_uid", "uid": uid}
        # Look up the user to show their username
        await _buddy_lookup(chat_id, uid, tg, cfg, db_cfg)


async def _buddy_lookup(chat_id: int, uid: int, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """Look up a user by UID, confirm with the requester."""
    from hf_client import HFClient, get_rate_limit_remaining
    user = await _db(get_user, db_cfg, chat_id)
    if not user:
        _clear(chat_id)
        return

    hf   = HFClient(user["access_token"])
    data = await hf.read({
        "users": {
            "_uid":     [uid],
            "uid":      True,
            "username": True,
        }
    })
    log.info(f"_buddy_lookup API call done — remaining: {get_rate_limit_remaining(hf.token)}")

    if not data or "users" not in data:
        _clear(chat_id)
        await tg.send(chat_id, "couldn't find that UID, double check it and try again")
        return

    users = data["users"]
    if isinstance(users, dict): users = [users]
    if not users:
        _clear(chat_id)
        await tg.send(chat_id, "no user found for that UID")
        return

    found    = users[0]
    username = found.get("username", f"UID {uid}")
    _state[chat_id] = {"step": "buddy_confirm", "uid": uid, "username": username}

    await tg.send(chat_id,
        f"add <b>{username}</b> (UID {uid}) to your buddy list? you'll get pinged when they post a new thread.",
        reply_markup={"inline_keyboard": [[
            {"text": "✅ Add",       "callback_data": "buddy_confirm_add"},
            {"text": "🚫 Nevermind", "callback_data": "buddy_cancel"},
        ]]}
    )


async def _fid_lookup(chat_id: int, fid: int, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """Try to fetch threads from a FID to validate it."""
    from hf_client import HFClient, get_rate_limit_remaining
    user = await _db(get_user, db_cfg, chat_id)
    if not user:
        _clear(chat_id)
        return

    hf   = HFClient(user["access_token"])
    data = await hf.read({
        "threads": {
            "_fid":     [fid],
                "_page": 1,
                "_perpage": 1,
                "tid":      True,
                "subject":  True,
                "fid":      True,
            }
        })
    log.info(f"_fid_lookup threads call done — remaining: {get_rate_limit_remaining(hf.token)}")

    if not data or "threads" not in data:
        _clear(chat_id)
        await tg.send(chat_id,
            f"can't find FID {fid}, check the ID and try again"
        )
        return

    threads = data["threads"]
    if isinstance(threads, dict): threads = [threads]
    if not threads:
        _clear(chat_id)
        await tg.send(chat_id,
            f"FID {fid} looks empty, check the ID and try again"
        )
        return

    # Try to get forum name from forums endpoint
    forum_name = f"Forum #{fid}"
    hf        = HFClient(user["access_token"])
    frum_data = await hf.read({
            "forums": {
                "_fid": [fid],
                "fid":  True,
                "name": True,
            }
        })
    log.info(f"_fid_lookup forums call done — remaining: {get_rate_limit_remaining(hf.token)}")
    if frum_data and "forums" in frum_data:
        forums = frum_data["forums"]
        if isinstance(forums, dict): forums = [forums]
        if forums:
            forum_name = forums[0].get("name", forum_name)

    _state[chat_id] = {"step": "fid_confirm", "fid": fid, "name": forum_name}

    await tg.send(chat_id,
        f"track mentions in <b>{forum_name}</b> (FID {fid})? we'll alert you when someone tags you there.",
        reply_markup={"inline_keyboard": [[
            {"text": "✅ Add",       "callback_data": "fid_confirm_add"},
            {"text": "🚫 Nevermind", "callback_data": "fid_cancel"},
        ]]}
    )


# ── Callback handler ───────────────────────────────────────────────────────────


def _buddy_list_ui(user):
    buddies = user.get("buddy_list") or []
    buttons = []
    for b in buddies:
        uid  = b.get("uid")
        name = b.get("username", "UID " + str(uid))
        mode = b.get("mode", "threads")
        icon = "🧵" if mode == "threads" else "💬"
        buttons.append([{"text": icon + " " + name, "callback_data": "buddy_open_" + str(uid)}])
    if len(buddies) < MAX_BUDDIES:
        buttons.append([{"text": "➕ Add buddy", "callback_data": "add_buddy"}])
    buttons.append([{"text": "← Back", "callback_data": "back_to_radar"}])
    text = ("👥 <b>Buddy List</b> (" + str(len(buddies)) + "/" + str(MAX_BUDDIES) + ")\n\n"
            + "Tap a buddy to manage them.")
    return text, {"inline_keyboard": buttons}

async def _show_buddy_list(chat_id, message_id, tg, user):
    text, kb = _buddy_list_ui(user)
    await tg.edit(chat_id, message_id, text, kb)

def _buddy_detail_ui(buddy):
    uid  = buddy.get("uid")
    name = buddy.get("username", "UID " + str(uid))
    mode = buddy.get("mode", "threads")
    if mode == "threads":
        mode_label  = "🧵 threads only"
        toggle_text = "💬 Switch to threads + posts"
    else:
        mode_label  = "💬 threads + posts"
        toggle_text = "🧵 Switch to threads only"
    text = "👤 <b>" + name + "</b>\n\nTracking: <b>" + mode_label + "</b>"
    buttons = [
        [{"text": toggle_text, "callback_data": "buddy_mode_" + str(uid)}],
        [{"text": "🗑 Remove", "callback_data": "remove_buddy_" + str(uid)}],
        [{"text": "← Back", "callback_data": "open_buddies_back"}],
    ]
    return text, {"inline_keyboard": buttons}

async def handle_test(chat_id, tg, cfg, db_cfg):
    user = await _db(get_user, db_cfg, chat_id)
    if not user or not user.get("access_token"):
        await tg.send(chat_id, "connect first, use /start")
        return
    from hf_client import HFClient
    hf = HFClient(user["access_token"])
    my_uid = user["hf_uid"]
    lines = ["<b>HF Radar API Diagnostic</b>", ""]
    me_data = await hf.read({"me": {"uid": True, "username": True, "unreadpms": True, "reputation": True}})
    if me_data and "me" in me_data:
        me = me_data["me"]
        if isinstance(me, list): me = me[0]
        real_uid = int(me.get("uid") or 0)
        # Auto-repair hf_uid if it's 0 or wrong
        if real_uid and real_uid != my_uid:
            await _db(upsert_user, db_cfg, chat_id, {"hf_uid": real_uid})
            my_uid = real_uid
            lines.append("⚠️ Fixed stored UID: was " + str(user["hf_uid"]) + " → " + str(real_uid))
        lines.append("<b>Account:</b> " + str(me.get("username")) + " (UID " + str(me.get("uid")) + ")")
        lines.append("  Unread PMs: " + str(me.get("unreadpms", "?")))
        lines.append("  Reputation: " + str(me.get("reputation", "?")))
    else:
        lines.append("<b>Account:</b> FAILED - check token")
    lines.append("")
    c_data = await hf.read({"contracts": {"_uid": [my_uid], "_perpage": 30, "cid": True, "status": True}})
    if c_data and "contracts" in c_data:
        cs = c_data["contracts"]
        if isinstance(cs, dict): cs = [cs]
        lines.append("<b>Contracts:</b> " + str(len(cs or [])) + " returned")
        for c in (cs or [])[:3]:
            lines.append("  #" + str(c.get("cid")) + " status=" + str(c.get("status")))
    else:
        lines.append("<b>Contracts:</b> none or failed")
    lines.append("")
    known_tids = list(user.get("known_tids") or [])
    t_data = await hf.read({"threads": {"_uid": [my_uid], "_page": 1, "_perpage": 20, "tid": True, "subject": True}})
    if t_data and "threads" in t_data:
        ts = t_data["threads"]
        if isinstance(ts, dict): ts = [ts]
        lines.append("<b>Your Threads:</b> " + str(len(known_tids)) + " accumulated, API returned: " + str(len(ts or [])))
        for t in (ts or [])[:3]:
            lines.append("  tid=" + str(t.get("tid")) + " — " + str(t.get("subject",""))[:50])
    else:
        lines.append("<b>Your Threads:</b> none or failed")
    lines.append("")
    buddy_list = user.get("buddy_list") or []
    if buddy_list:
        lines.append("<b>Buddy Threads</b> (one call per buddy):")
        for buddy in buddy_list:
            buid = buddy.get("uid")
            bname = buddy.get("username", "UID " + str(buid))
            bt_data = await hf.read({"threads": {"_uid": [int(buid)], "_page": 1, "_perpage": 20, "tid": True, "uid": True, "subject": True, "dateline": True}})
            if bt_data and "threads" in bt_data:
                bts = bt_data["threads"]
                if isinstance(bts, dict): bts = [bts]
                lines.append("  " + bname + ": " + str(len(bts or [])) + " threads returned")
                for t in (bts or [])[:2]:
                    lines.append("    #" + str(t.get("tid")) + " " + str(t.get("subject","?"))[:40])
            else:
                lines.append("  " + bname + ": none or failed")
    else:
        lines.append("<b>Buddy Threads:</b> no buddies added")
    await tg.send(chat_id, "\n".join(lines))

async def handle_callback(query: dict, tg: TelegramBot, cfg: dict, db_cfg: dict):
    chat_id     = query["message"]["chat"]["id"]
    message_id  = query["message"]["message_id"]
    callback_id = query["id"]
    data        = query.get("data", "")

    user = get_user(db_cfg, chat_id)

    # ── Donate ────────────────────────────────────────────────────────────────
    if data.startswith("mute_thread_"):
        tid_to_mute = data.replace("mute_thread_", "")
        muted = list(user.get("muted_tids") or []) if user else []
        if tid_to_mute not in [str(t) for t in muted]:
            muted.append(int(tid_to_mute))
            await _db(upsert_user, db_cfg, chat_id, {"muted_tids": muted})
        await tg.answer_callback(callback_id, "🔕 Thread muted")
        return

    if data == "open_muted":
        await tg.answer_callback(callback_id)
        muted = list(user.get("muted_tids") or []) if user else []
        thread_meta = user.get("thread_meta") or {} if user else {}
        if not muted:
            text = "🔕 <b>Muted Threads</b>\n\nNo muted threads."
            buttons = [[{"text": "← Back", "callback_data": "back_to_radar"}]]
        else:
            text = "🔕 <b>Muted Threads</b>\n\nThese threads won't send you any more reply alerts.\nTap one to unmute it."
            buttons = []
            for tid in muted:
                subject = (thread_meta.get(str(tid)) or {}).get("sub") or f"Thread #{tid}"
                # Truncate long subjects to fit in button
                if len(subject) > 35:
                    subject = subject[:33] + "…"
                buttons.append([{"text": f"🔔 {subject}", "callback_data": f"unmute_thread_{tid}"}])
            buttons.append([{"text": "← Back", "callback_data": "back_to_radar"}])
        await tg.edit(chat_id, message_id, text, {"inline_keyboard": buttons})
        return

    if data.startswith("unmute_thread_"):
        tid_to_unmute = data.replace("unmute_thread_", "")
        muted = list(user.get("muted_tids") or []) if user else []
        muted = [t for t in muted if str(t) != tid_to_unmute]
        await _db(upsert_user, db_cfg, chat_id, {"muted_tids": muted})
        await tg.answer_callback(callback_id, "🔔 Thread unmuted")
        # Refresh muted list
        thread_meta = user.get("thread_meta") or {} if user else {}
        if not muted:
            text = "🔕 <b>Muted Threads</b>\n\nNo muted threads."
            buttons = [[{"text": "← Back", "callback_data": "back_to_radar"}]]
        else:
            text = "🔕 <b>Muted Threads</b>\n\nTap one to unmute it."
            buttons = []
            for tid in muted:
                subject = (thread_meta.get(str(tid)) or {}).get("sub") or f"Thread #{tid}"
                if len(subject) > 35:
                    subject = subject[:33] + "…"
                buttons.append([{"text": f"🔔 {subject}", "callback_data": f"unmute_thread_{tid}"}])
            buttons.append([{"text": "← Back", "callback_data": "back_to_radar"}])
        await tg.edit(chat_id, message_id, text, {"inline_keyboard": buttons})
        return

    # ── Buddy list ─────────────────────────────────────────────────────────────
    if data in ("open_buddies", "open_buddies_back"):
        await tg.answer_callback(callback_id)
        if not user:
            return
        await _show_buddy_list(chat_id, message_id, tg, user)
        return

    if data.startswith("buddy_open_"):
        uid_to_open = int(data.replace("buddy_open_", ""))
        if not user:
            return
        buddy = next((b for b in (user.get("buddy_list") or []) if b.get("uid") == uid_to_open), None)
        if not buddy:
            await tg.answer_callback(callback_id, "buddy not found")
            return
        await tg.answer_callback(callback_id)
        text, kb = _buddy_detail_ui(buddy)
        await tg.edit(chat_id, message_id, text, kb)
        return


    if data == "add_buddy":
        await tg.answer_callback(callback_id)
        if not user:
            return
        buddies = user.get("buddy_list") or []
        if len(buddies) >= MAX_BUDDIES:
            await tg.answer_callback(callback_id, f"Buddy list full ({MAX_BUDDIES} max)")
            return
        _state[chat_id] = {"step": "buddy_add_uid"}
        await tg.send(chat_id,
            "Enter the HF UID of the user you want to add.\n"
            "<i>Reply with their UID number, or /cancel to go back.</i>"
        )
        return

    if data.startswith("buddy_mode_"):
        uid_to_toggle = int(data.replace("buddy_mode_", ""))
        buddies = (user.get("buddy_list") or []) if user else []
        buddy = None
        for b in buddies:
            if int(b.get("uid", -1)) == uid_to_toggle:
                b["mode"] = "all" if b.get("mode", "threads") == "threads" else "threads"
                buddy = b
                break
        await _db(upsert_user, db_cfg, chat_id, {"buddy_list": buddies})
        if buddy:
            label = "threads + posts" if buddy["mode"] == "all" else "threads only"
            await tg.answer_callback(callback_id, "switched to " + label)
            text, kb = _buddy_detail_ui(buddy)
            await tg.edit(chat_id, message_id, text, kb)
        return

    if data.startswith("remove_buddy_"):
        uid_to_remove = int(data.replace("remove_buddy_", ""))
        if not user:
            return
        name = next((b.get("username", str(b.get("uid"))) for b in (user.get("buddy_list") or []) if b.get("uid") == uid_to_remove), "buddy")
        buddies = [b for b in (user.get("buddy_list") or []) if b.get("uid") != uid_to_remove]
        await _db(upsert_user, db_cfg, chat_id, {"buddy_list": buddies})
        user["buddy_list"] = buddies
        await tg.answer_callback(callback_id, "removed " + name)
        await _show_buddy_list(chat_id, message_id, tg, user)
        return




    if data == "buddy_confirm_add":
        state = _state.get(chat_id)
        if not state or state.get("step") != "buddy_confirm":
            await tg.answer_callback(callback_id, "session expired, try again")
            return
        uid      = state["uid"]
        username = state["username"]
        _clear(chat_id)
        if not user:
            return
        buddies = user.get("buddy_list") or []
        if any(b.get("uid") == uid for b in buddies):
            await tg.answer_callback(callback_id, "already in your list")
        elif len(buddies) >= MAX_BUDDIES:
            await tg.answer_callback(callback_id, "buddy list is full (5 max)")
        else:
            import time as _t
            buddies.append({"uid": uid, "username": username, "added_at": int(_t.time())})
            await _db(upsert_user, db_cfg, chat_id, {"buddy_list": buddies})
            await tg.answer_callback(callback_id, f"Added {username}")
        user["buddy_list"] = buddies
        await tg.edit(chat_id, message_id, radar_text(user), radar_keyboard(user, cfg))
        return

    if data == "buddy_cancel":
        _clear(chat_id)
        await tg.answer_callback(callback_id, "ok")
        if user:
            await tg.edit(chat_id, message_id, radar_text(user), radar_keyboard(user, cfg))
        return

    # ── FID tracking ──────────────────────────────────────────────────────────
    if data == "fid_confirm_add":
        state = _state.get(chat_id)
        if not state or state.get("step") != "fid_confirm":
            await tg.answer_callback(callback_id, "session expired, try again")
            return
        fid   = state["fid"]
        fname = state["name"]
        _clear(chat_id)
        if not user:
            return
        forums = list(user.get("tracked_fids") or [])
        if any(f.get("fid") == fid for f in forums):
            await tg.answer_callback(callback_id, "already watching that forum")
        elif len(forums) >= MAX_FORUMS:
            await tg.answer_callback(callback_id, f"max {MAX_FORUMS} forums — remove one first")
        else:
            import time as _t
            forums.append({"fid": fid, "name": fname, "added_at": int(_t.time())})
            await _db(upsert_user, db_cfg, chat_id, {"tracked_fids": forums})
            await tg.answer_callback(callback_id, f"now watching {fname}")
        user["tracked_fids"] = forums
        ftext, fkb = forums_keyboard(user)
        await tg.edit(chat_id, message_id, ftext, fkb)
        return

    if data == "fid_cancel":
        _clear(chat_id)
        await tg.answer_callback(callback_id, "ok")
        if user:
            ftext, fkb = forums_keyboard(user)
            await tg.edit(chat_id, message_id, ftext, fkb)
        return

    # ── Alerts drill-in ───────────────────────────────────────────────────────
    if data == "open_alerts":
        await tg.answer_callback(callback_id)
        if not user:
            return
        from telegram_bot import alerts_text
        await tg.edit(chat_id, message_id, alerts_text(user), alerts_keyboard(user))
        return

    # ── Forums drill-in ───────────────────────────────────────────────────────
    if data == "open_forums":
        await tg.answer_callback(callback_id)
        if not user:
            return
        ftext, fkb = forums_keyboard(user)
        await tg.edit(chat_id, message_id, ftext, fkb)
        return

    if data == "add_forum":
        await tg.answer_callback(callback_id)
        if not user:
            return
        forums = user.get("tracked_fids") or []
        if len(forums) >= MAX_FORUMS:
            await tg.answer_callback(callback_id, f"max {MAX_FORUMS} forums — remove one first")
            return
        _state[chat_id] = {"step": "fid_add"}
        await tg.send(chat_id,
            "Enter the FID (Forum ID) you want to watch.\n"
            "<i>Find it in the URL: hackforums.net/forumdisplay.php?fid=<b>25</b></i>\n\n"
            "Reply with the FID number, or /cancel to go back."
        )
        return

    if data.startswith("remove_forum_"):
        fid_to_remove = data.replace("remove_forum_", "")
        if not user:
            return
        forums = [f for f in (user.get("tracked_fids") or []) if str(f.get("fid")) != fid_to_remove]
        await _db(upsert_user, db_cfg, chat_id, {"tracked_fids": forums})
        user["tracked_fids"] = forums
        await tg.answer_callback(callback_id, "removed")
        ftext, fkb = forums_keyboard(user)
        await tg.edit(chat_id, message_id, ftext, fkb)
        return




    if data == "back_to_radar":
        await tg.answer_callback(callback_id)
        if user:
            await tg.edit(chat_id, message_id, radar_text(user), radar_keyboard(user, cfg))
        return

    # ── Notification toggles ───────────────────────────────────────────────────
    if data.startswith("toggle_"):
        if not user:
            await tg.answer_callback(callback_id, "Please /start first.")
            return
        key    = data.replace("toggle_", "")
        # Use DEFAULT_NOTIFICATIONS as authority — stored dict may be missing
        # newer keys (e.g. buddy_status, fid_threads) for older user accounts
        if key not in DEFAULT_NOTIFICATIONS:
            await tg.answer_callback(callback_id, "Unknown alert type.")
            return
        notifs  = dict(DEFAULT_NOTIFICATIONS)
        notifs.update(user.get("notifications") or {})
        notifs[key] = not notifs.get(key, True)
        await _db(upsert_user, db_cfg, chat_id, {"notifications": notifs})
        user["notifications"] = notifs
        status = "✅ On" if notifs[key] else "❌ Off"
        await tg.answer_callback(callback_id, status)
        from telegram_bot import alerts_text
        await tg.edit(chat_id, message_id, alerts_text(user), alerts_keyboard(user))
        return

    # ── Pause / Resume ─────────────────────────────────────────────────────────
    if data == "pause":
        if not user:
            return
        await _db(upsert_user, db_cfg, chat_id, {"paused": 1})
        user["paused"] = 1
        await tg.answer_callback(callback_id, "Paused")
        await tg.edit(chat_id, message_id, radar_text(user), radar_keyboard(user, cfg))
        return

    if data == "resume":
        if not user:
            return
        await _db(upsert_user, db_cfg, chat_id, {"paused": 0})
        user["paused"] = 0
        await tg.answer_callback(callback_id, "Resumed")
        await tg.edit(chat_id, message_id, radar_text(user), radar_keyboard(user, cfg))
        return