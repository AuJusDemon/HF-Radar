"""
Notification detectors.

Reply/mention detection:
  - posts _uid page 1, perpage 30  → discover thread IDs from recent posts (gated on postnum change)
  - threads _tid [known_tids]      → poll lastpost/lastposteruid every cycle
  - lastpost changed + poster != us → fetch posts _tid last page for snippet → alert
  - dispute_tid added to known_tids so dispute thread replies are caught automatically
  Deduplication via seen_events table only.

Buddy detection:
  - ALL buddy UIDs batched into ONE threads call (1 call vs N calls)
  - mode=all buddies also batched into ONE posts call (1 more call for all of them)
  - usergroup check folded into the same batch as 3rd endpoint (no separate call)
  - gate: max(270, 90 × buddy_count) seconds — fires ~13×/hr at 3 buddies

── API call budget (per hour, 1 user, 3 buddies, ~97 known threads) ────────────────────
  BASELINE (original code, no optimizations):
    check_account_events (medium):   3 calls × 30/hr  =  90 calls/hr
    per-buddy threads+posts:         3 calls × 20/hr  =  60 calls/hr
    standalone bratings:             2 calls/hr
    thread poll (hot):               1 call  × 20/hr  =  20 calls/hr
    discovery (gated):               ~0.5/hr
    fid threads:                     6/hr
    disputes:                        2/hr
    TOTAL ≈ 180/hr  ← was hitting the 240/hr wall

  AFTER all optimizations (this file):
    check_account_events (medium):   1 call × 20/hr   =  20 calls/hr   [interval 120→180]
    buddy batch (all UIDs, 1 call):  1 call × 13/hr   =  13 calls/hr   [was 60]
    bratings folded into medium:     0 dedicated calls [was 2-4/hr]
    thread poll (hot):               1 call × 20/hr   =  20 calls/hr   (unchanged)
    discovery (gated):               ~0.5/hr           (unchanged)
    fid threads:                     ~4/hr             [interval 600→900]
    disputes:                        2/hr               (unchanged)
    uid resolution (medium):         ~2/hr              (unchanged)
    TOTAL ≈ 62/hr  ← ~66% reduction, ~178 remaining at any point

  Change log with call impact:
    [-47/hr] buddy batching          all buddy UIDs in one _uid array, UG folded in
    [-10/hr] medium interval 120→180 20 cycles/hr instead of 30
    [ -4/hr] bratings folded         piggybacked as 4th endpoint in medium batch
    [ -2/hr] fid interval 600→900    new threads don't post faster than 15 min
    [-5/start] bootstrap fix         5-page sweep runs once ever, not every restart
    [  varies] rate limit guard      stops loops early if limit is low

  NOTE on buddy _perpage batching:
    When all buddy UIDs share one call, _perpage:30 is the TOTAL across all UIDs.
    At 3 buddies this is ~10 results/buddy. Thread alerts are forgiving (threads persist,
    missed ones show up next cycle). Posts on mode=all buddies are higher-risk — a very
    prolific buddy could crowd others out. Increase _perpage to 30 (already max) and
    rely on the 270s interval being short enough to not miss anything meaningful.
"""

import asyncio
import concurrent.futures
import json
import re
import time
import logging

from db import is_event_seen, mark_event_seen, upsert_user
from hf_client import HFClient, is_rate_limited, AuthExpired
from telegram_bot import TelegramBot
import alerts as tones

_dblog = logging.getLogger("hfradar.db")

# Dedicated thread pool for DB calls — prevents them from competing with
# other executor work (HF relay parsing, etc.) in the default pool.
_db_executor = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="det_db")


async def _db(fn, *args):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_db_executor, fn, *args),
            timeout=15,
        )
    except Exception as e:
        _dblog.error(f"DB call failed: {fn.__name__}: {e}")
        return None


log = logging.getLogger("hfradar.detectors")
SNIPPET_LEN = 200


def strip_bbcode(text: str) -> str:
    text = re.sub(r'\[/?[a-zA-Z][^\]]*\]', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def strip_quote_blocks(text: str) -> str:
    """Remove [quote=...] ... [/quote] blocks so the snippet shows the
    author's own words, not the text they were quoting."""
    cleaned = re.sub(r'\[quote[^\]]*\].*?\[/quote\]', '', text, flags=re.IGNORECASE | re.DOTALL)
    cleaned = cleaned.strip()
    # Fall back to full text if the reply was only the quote block (nothing left)
    return cleaned if cleaned else text


def find_mention(message: str, uid: str, username: str):
    """Detect @mention, [mention] tag, or [quote] of the user.
    Returns 'quote', 'mention', or False."""
    if username:
        _q = r"\[quote=['\"]?" + re.escape(username) + r"['\"]?(?:\s[^\]]*)?]"
        if re.search(_q, message, re.IGNORECASE):
            return "quote"
    if username and re.search(rf'\[mention\]{re.escape(username)}\[/mention\]', message, re.IGNORECASE):
        return "mention"
    if username and re.search(
        rf'(?<![a-zA-Z0-9_])@{re.escape(username)}(?![a-zA-Z0-9_])',
        message, re.IGNORECASE
    ):
        return "mention"
    return False


def _mute_button(tid) -> dict:
    """Inline keyboard with a Mute Thread button — attached to reply/mention alerts."""
    return {"inline_keyboard": [[{"text": "🔕 Mute thread", "callback_data": f"mute_thread_{tid}"}]]}


def rep_delta(history: list, window_seconds: int) -> int | None:
    """Return rep change over the last window_seconds. None if not enough history."""
    if not history or len(history) < 2:
        return None
    now_ts      = int(time.time())
    cutoff      = now_ts - window_seconds
    current_val = history[-1].get("val", 0)
    # Walk backwards to find the last entry at or before the cutoff
    past_entry = None
    for entry in reversed(history[:-1]):
        if entry.get("ts", 0) <= cutoff:
            past_entry = entry
            break
    if past_entry is None:
        past_entry = history[0]   # all entries within window — use oldest
    return current_val - past_entry.get("val", 0)


# ── Contract status map (module-level — shared by check_account_events + risk) ──
# Confirmed via live API polling — these differ from HF's public docs.
CONTRACT_STATUS = {
    "1": "awaiting approval",
    "2": "cancelled",
    "5": "active",
    "6": "complete",
    "8": "incomplete",
}


async def _counterparty_risk(hf: HFClient, their_uid: int) -> str:
    """
    Pull a quick risk brief on a contract counterparty.
    Fires 1 extra API call (3 endpoints) only when a NEW contract is detected.
    Returns a one-line summary string with risk flag, empty string on failure/rate-limit.

    Format examples:
      ⚠️ 12 posts · 3 contracts (2 bad) · b-ratings: +1/−2
         └ "never delivered, total scam"
      ✅ 4,201 posts · 18 contracts · b-ratings: +15/−0
    """
    if is_rate_limited(hf.token):
        return ""

    data = await hf.read({
        "users": {
            "_uid":       [their_uid],
            "uid":        True,
            "postnum":    True,
            "reputation": True,
        },
        "bratings": {
            "_to":      [their_uid],
            "_perpage": 30,
            "crid":     True,
            "amount":   True,
            "message":  True,
        },
        "contracts": {
            "_uid":     [their_uid],
            "_perpage": 30,
            "cid":      True,
            "status":   True,
            "inituid":  True,
            "otheruid": True,
        },
    })
    if not data:
        return ""

    # ── Users ─────────────────────────────────────────────────────────────────
    postnum = 0
    if "users" in data:
        u = data["users"]
        if isinstance(u, list): u = u[0] if u else {}
        postnum = int(u.get("postnum") or 0)

    # ── B-ratings ─────────────────────────────────────────────────────────────
    pos_ratings  = 0
    neg_ratings  = 0
    worst_note   = ""
    if "bratings" in data:
        brs = data["bratings"]
        if isinstance(brs, dict): brs = [brs]
        for b in (brs or []):
            amt = int(b.get("amount") or 0)
            if amt > 0:
                pos_ratings += 1
            elif amt < 0:
                neg_ratings += 1
                msg = (b.get("message") or "").strip()
                if msg and not worst_note:
                    worst_note = msg[:80]

    # ── Contracts ─────────────────────────────────────────────────────────────
    total_contracts = 0
    bad_contracts   = 0   # cancelled or incomplete
    if "contracts" in data:
        cts = data["contracts"]
        if isinstance(cts, dict): cts = [cts]
        for ct in (cts or []):
            ci = str(ct.get("inituid") or "")
            co = str(ct.get("otheruid") or "")
            if str(their_uid) not in (ci, co):
                continue
            total_contracts += 1
            mapped = CONTRACT_STATUS.get(str(ct.get("status") or ""), "")
            if mapped in ("cancelled", "incomplete"):
                bad_contracts += 1

    # ── Assemble ──────────────────────────────────────────────────────────────
    parts = [f"{postnum:,} posts"]

    if total_contracts > 0:
        ct_str = f"{total_contracts} contract{'s' if total_contracts != 1 else ''}"
        if bad_contracts > 0:
            ct_str += f" ({bad_contracts} bad)"
        parts.append(ct_str)
    else:
        parts.append("no contracts")

    if pos_ratings or neg_ratings:
        parts.append(f"b-ratings: +{pos_ratings}/−{neg_ratings}")
    else:
        parts.append("no b-ratings")

    risky  = (postnum < 50) or (neg_ratings > 0) or (bad_contracts > 0)
    prefix = "⚠️ " if risky else "✅ "
    result = prefix + " · ".join(parts)
    if worst_note:
        result += f'\n   └ "{worst_note}"'
    return result


# ── Account events ─────────────────────────────────────────────────────────────

async def check_account_events(user: dict, hf: HFClient, tg: TelegramBot, cfg: dict, db_cfg: dict):
    log.info(f"check_account_events chat_id={user.get('chat_id')} hf_uid={user.get('hf_uid')} user={user.get('hf_username','?')}")

    notifs  = user.get("notifications") or {}
    chat_id = user["chat_id"]
    my_uid  = user["hf_uid"]
    now     = int(time.time())

    # Single batched call: me + contracts (with embedded disputes) + bratings + bytes
    # idispute/odispute are sub-fields of contracts — they return dispute data inline,
    # so we never need a separate disputes API call. 4 endpoints is safe (HF 503s on 5+).
    try:
        data = await hf.read({
            "me": {
                "uid":           True,
                "username":      True,   # catch username changes
                "unreadpms":     True,
                "reputation":    True,
                "bytes":         True,
                "postnum":       True,   # gate posts discovery — skip if unchanged
                "threadnum":     True,   # gate own_tids sweep — skip if unchanged
                "warningpoints": True,   # alert if moderator warns the user
                "usergroup":     True,   # detect self-ban/exile
            },
            "contracts": {
                "_uid":         [my_uid],
                "_perpage":     30,
                "cid":          True,
                "dateline":     True,
                "status":       True,
                "type":         True,   # standard / middleman / exchange
                "muid":         True,   # middleman UID (if type=middleman)
                "inituid":      True,   # who opened the contract
                "otheruid":     True,   # other party
                "iprice":       True,
                "icurrency":    True,
                "iproduct":     True,
                "oprice":       True,   # other side (exchange contracts)
                "oproduct":     True,
                "ocurrency":    True,
                "timeout":      True,   # unix expiry timestamp
                "timeout_days": True,
                "tid":          True,   # linked thread if any
                "terms":        True,
                "istatus":      True,        # initiator's ready flag (0=not ready, 1=ready)
                "ostatus":      True,        # other party's ready flag
                "idispute":     ["cdid", "contractid", "claimantuid", "defendantuid",
                                 "dateline", "status", "dispute_tid", "claimantnotes"],
                "odispute":     ["cdid", "contractid", "claimantuid", "defendantuid",
                                 "dateline", "status", "dispute_tid", "claimantnotes"],
            },
            # bratings piggybacked here as 4th endpoint — zero extra API cost since the
            # call was already happening. Processing is still gated to 30 min (rare events)
            # but the data comes free every medium cycle. Eliminates standalone check_bratings call.
            "bratings": {
                "_to":        [my_uid],
                "_perpage":   10,
                "crid":       True,
                "contractid": True,
                "fromid":     True,
                "dateline":   True,
                "amount":     True,
                "message":    True,
            },
            "bytes": {
                "_to":      [my_uid],
                "_perpage": 10,   # medium loop is every 2min; 10 is plenty
                "id":       True,
                "amount":   True,
                "dateline": True,
                "reason":   True,
                "type":     True,
                "from":     True,
                "post":     True,   # post ID — lets us link directly to the post
            },
        })
    except AuthExpired:
        # 401 — token is actually dead, nuke it
        from db import upsert_user as _upsert
        _upsert(db_cfg, chat_id, {"active": 0, "paused": 1, "access_token": None})
        await tg.send(chat_id,
            "⚠️ <b>HF Radar : Authorization expired</b>\n\n"
            "Your HackForums access was revoked or expired.\n"
            "All your settings have been saved.\n\n"
            "Use /start to reconnect and pick up where you left off."
        )
        return

    if not data:
        # Transient failure (503, timeout, proxy hiccup) — skip this cycle, don't nuke token
        log.warning(f"check_account_events: transient failure for chat_id={chat_id}, skipping cycle")
        return

    # ── Batch-resolve UIDs → usernames (single extra call) ───────────────────
    # Collect every third-party UID that will appear in an alert message.
    # One users batch call costs 1 API call but eliminates "UID XXXXXX" from
    # every b-rating, bytes receipt, and contract notification permanently.
    uids_to_resolve: set = set()
    for blist in [data.get("bratings") or []]:
        if isinstance(blist, dict): blist = [blist]
        for b in blist:
            uid = b.get("fromid")
            if uid and str(uid).isdigit() and int(uid) != my_uid:
                uids_to_resolve.add(int(uid))
    for txlist in [data.get("bytes") or []]:
        if isinstance(txlist, dict): txlist = [txlist]
        for tx in txlist:
            # "from" is a list field — extract uid from from[0], same as detection code below
            _from_raw = tx.get("from") or []
            if isinstance(_from_raw, list) and _from_raw:
                _fi = _from_raw[0]
                uid_val = str(_fi.get("uid", "") if isinstance(_fi, dict) else _fi)
            elif isinstance(_from_raw, str):
                uid_val = _from_raw
            else:
                uid_val = ""
            if uid_val and uid_val.isdigit() and int(uid_val) != my_uid:
                uids_to_resolve.add(int(uid_val))
    for ctlist in [data.get("contracts") or []]:
        if isinstance(ctlist, dict): ctlist = [ctlist]
        for ct in ctlist:
            for field in ("inituid", "otheruid"):
                uid = ct.get(field)
                if uid and str(uid).isdigit() and int(uid) != my_uid:
                    uids_to_resolve.add(int(uid))

    # Load persistent UID cache — only resolve UIDs not already known
    uid_cache: dict = dict(user.get("uid_cache") or {})  # str(uid) → username
    uid_usernames: dict = {int(k): v for k, v in uid_cache.items() if k.isdigit()}
    unknown_uids = {u for u in uids_to_resolve if uid_cache.get(str(u)) is None}

    if unknown_uids and not is_rate_limited(hf.token):
        uid_data = await hf.read({
            "users": {
                "_uid":     list(unknown_uids),
                "uid":      True,
                "username": True,
            }
        })
        if uid_data and "users" in uid_data:
            rows = uid_data["users"]
            if isinstance(rows, dict): rows = [rows]
            for u in (rows or []):
                try:
                    n = int(u["uid"])
                    uid_usernames[n] = u.get("username", "")
                    uid_cache[str(n)] = u.get("username", "")
                except (KeyError, ValueError, TypeError):
                    pass
        # Persist updated cache — cap at 500 entries
        if len(uid_cache) > 500:
            uid_cache = dict(list(uid_cache.items())[-500:])
        await _db(upsert_user, db_cfg, chat_id, {"uid_cache": uid_cache})
        log.info(f"UID resolution: {len(unknown_uids)} new resolved, {len(uid_usernames)} total cached for chat_id={chat_id}")
    elif uids_to_resolve:
        log.info(f"UID resolution: {len(uids_to_resolve)} served from cache for chat_id={chat_id}")

    def _resolve_uid(uid) -> str:
        """Return username for uid, or 'UID XXXXX' as fallback."""
        if not uid:
            return ""
        try:
            n = int(uid)
        except (ValueError, TypeError):
            return str(uid)
        return uid_usernames.get(n) or f"UID {uid}"

    # ── PMs ───────────────────────────────────────────────────────────────────
    if notifs.get("unread_pms", True) and "me" in data:
        me = data["me"]
        if isinstance(me, list): me = me[0]
        raw_pms = me.get("unreadpms")
        log.info(f"PM check chat_id={chat_id}: raw_pms={repr(raw_pms)} last={repr(user.get('last_unread_pms'))}")
        if raw_pms is None:
            log.warning("unreadpms missing — check Advanced Info permission on HF OAuth app")
        else:
            current  = int(raw_pms)
            previous = int(user.get("last_unread_pms") or 0)
            if current > previous:
                await tg.send(chat_id, tones.fmt_pm(current - previous, current))
            await _db(upsert_user, db_cfg, chat_id, {"last_unread_pms": current})

    # ── Popularity ────────────────────────────────────────────────────────────
    if notifs.get("popularity", True) and "me" in data:
        me = data["me"]
        if isinstance(me, list): me = me[0]
        raw_rep = me.get("reputation")
        if raw_rep is not None:
            current_rep  = int(float(raw_rep))
            previous_rep = int(user.get("last_reputation") or 0)

            # Maintain rep_history for velocity calculation — append on every change
            rep_history = list(user.get("rep_history") or [])
            if current_rep != previous_rep or not rep_history:
                rep_history.append({"ts": now, "val": current_rep})
                rep_history = rep_history[-50:]   # cap at 50 entries
                await _db(upsert_user, db_cfg, chat_id, {"rep_history": rep_history})
                user["rep_history"] = rep_history

            if previous_rep != 0 and current_rep != previous_rep:
                diff      = current_rep - previous_rep
                delta_7d  = rep_delta(rep_history, 7 * 86400)
                await tg.send(chat_id, tones.fmt_popularity(diff, current_rep, my_uid, delta_7d=delta_7d))
            if current_rep != previous_rep or previous_rep == 0:
                await _db(upsert_user, db_cfg, chat_id, {"last_reputation": current_rep})

    # ── Bytes balance ─────────────────────────────────────────────────────────
    # Field is 'bytes' in me endpoint, 'myps' is the alias name in users endpoint.
    # If me doesn't return it, fall back to a users _uid call.
    if "me" in data:
        me = data["me"]
        if isinstance(me, list): me = me[0]
        raw_bal = me.get("bytes") if me.get("bytes") is not None else me.get("myps")
        log.info(f"Bytes balance raw from me: {repr(raw_bal)} (all me keys: {list(me.keys())}) chat_id={chat_id}")

        if raw_bal is None:
            # me endpoint didn't return bytes — fall back to users endpoint
            log.info(f"bytes missing from me, falling back to users _uid for chat_id={chat_id}")
            user_data = await hf.read({
                "users": {
                    "_uid": [my_uid],
                    "uid":  True,
                    "myps": True,
                }
            })
            if user_data and "users" in user_data:
                u = user_data["users"]
                if isinstance(u, list): u = u[0]
                raw_bal = u.get("myps")
                log.info(f"Bytes balance raw from users fallback: {repr(raw_bal)} chat_id={chat_id}")

        if raw_bal is not None:
            try:
                current_bal = float(raw_bal)
                # Just track the balance for the radar panel — no drop alert,
                # user knows when they're spending their own bytes.
                await _db(upsert_user, db_cfg, chat_id, {"last_bytes_balance": current_bal})
                user["last_bytes_balance"] = current_bal
            except (ValueError, TypeError) as e:
                log.warning(f"Could not parse bytes balance: {repr(raw_bal)}: {e}")

    # ── Warning points + self-ban/exile + username sync + postnum/threadnum cache ──
    if "me" in data:
        me = data["me"]
        if isinstance(me, list): me = me[0]

        # Username sync
        new_username = me.get("username", "")
        if new_username and new_username != user.get("hf_username", ""):
            await _db(upsert_user, db_cfg, chat_id, {"hf_username": new_username})
            user["hf_username"] = new_username
            log.info(f"Username synced for chat_id={chat_id}: {new_username}")

        # Warning points — alert when a moderator issues a warning
        raw_wp = me.get("warningpoints")
        if raw_wp is not None:
            try:
                current_wp = int(raw_wp)
                prev_wp    = int(user.get("last_warningpoints") or 0)
                if prev_wp != 0 and current_wp > prev_wp:
                    await tg.send(chat_id, tones.fmt_warning_points(prev_wp, current_wp))
                    log.info(f"Warning points alert: chat_id={chat_id} {prev_wp}→{current_wp}")
                if current_wp != prev_wp:
                    await _db(upsert_user, db_cfg, chat_id, {"last_warningpoints": current_wp})
                    user["last_warningpoints"] = current_wp
            except (ValueError, TypeError):
                pass

        # Self-ban / exile detection
        raw_ug = str(me.get("usergroup") or "")
        if raw_ug:
            prev_ug = str(user.get("last_usergroup") or "")
            if prev_ug and raw_ug != prev_ug:
                if raw_ug == "7":
                    await tg.send(chat_id, tones.fmt_self_exile())
                    log.info(f"Self-exile detected: chat_id={chat_id} ug={raw_ug}")
                elif raw_ug == "38":
                    await tg.send(chat_id, tones.fmt_self_ban())
                    log.info(f"Self-ban detected: chat_id={chat_id} ug={raw_ug}")
            if raw_ug != prev_ug:
                await _db(upsert_user, db_cfg, chat_id, {"last_usergroup": raw_ug})

        # Cache postnum + threadnum for slow-loop skip gates
        _pn_upd = {}
        for _field, _key in (("postnum", "last_postnum"), ("threadnum", "last_threadnum")):
            _raw = me.get(_field)
            if _raw is not None:
                try:
                    _pn_upd[_key] = int(_raw)
                except (ValueError, TypeError):
                    pass
        if _pn_upd:
            await _db(upsert_user, db_cfg, chat_id, _pn_upd)
            user.update(_pn_upd)

    # ── Contracts (new + status changes) ──────────────────────────────────────
    linked_at       = int(user.get("linked_at") or 0) or int(time.time())
    contract_states = dict(user.get("contract_states") or {})
    cs_changed      = False

    if "contracts" in data:
        contracts = data["contracts"]
        if isinstance(contracts, dict): contracts = [contracts]
        for ct in (contracts or []):
            cid            = str(ct.get("cid", ""))
            raw_status     = str(ct.get("status", ""))
            current_status = CONTRACT_STATUS.get(raw_status, f"status #{raw_status}")
            if not cid:
                continue

            dateline = int(ct.get("dateline") or 0)

            # Use batch-resolved usernames — falls back to "UID XXXXX" only if lookup failed
            init_username  = _resolve_uid(ct.get("inituid"))
            other_username = _resolve_uid(ct.get("otheruid"))
            istatus        = ct.get("istatus")
            ostatus        = ct.get("ostatus")

            # New contract alert
            _seen = await _db(is_event_seen, db_cfg, chat_id, "contract", cid)
            if _seen is None:
                log.warning(f"is_event_seen DB error for contract cid={cid}, skipping to avoid duplicate alert")
                continue
            if notifs.get("contracts", True) and not _seen:
                if dateline and dateline < linked_at:
                    await _db(mark_event_seen, db_cfg, chat_id, "contract", cid)
                else:
                    terms = strip_bbcode(ct.get("terms", ""))[:SNIPPET_LEN]
                    # Determine counterparty UID — whoever is not us
                    _init = str(ct.get("inituid") or "")
                    _other = str(ct.get("otheruid") or "")
                    _their_uid = int(_other) if _init == str(my_uid) and _other.isdigit() else (int(_init) if _init.isdigit() else 0)
                    risk_brief = ""
                    if _their_uid and _their_uid != my_uid:
                        risk_brief = await _counterparty_risk(hf, _their_uid)
                        log.info(f"counterparty risk for cid={cid} uid={_their_uid}: {risk_brief[:60]}")
                    await tg.send(chat_id, tones.fmt_contract(
                        cid,
                        contract_type  = ct.get("type", "standard"),
                        iproduct       = ct.get("iproduct") or "N/A",
                        iprice         = ct.get("iprice", "?"),
                        icurrency      = ct.get("icurrency", "bytes"),
                        oproduct       = ct.get("oproduct"),
                        oprice         = ct.get("oprice"),
                        ocurrency      = ct.get("ocurrency"),
                        inituid        = ct.get("inituid"),
                        otheruid       = ct.get("otheruid"),
                        init_username  = init_username,
                        other_username = other_username,
                        muid           = ct.get("muid"),
                        timeout_days   = ct.get("timeout_days"),
                        status         = current_status,
                        terms          = terms,
                        risk_brief     = risk_brief,
                    ))
                    await _db(mark_event_seen, db_cfg, chat_id, "contract", cid)

            # Status change alert — always on, disputes are time-sensitive
            prev_status_raw = contract_states.get(cid)
            # Translate stored value in case it was saved as a raw number before this fix
            prev_status = CONTRACT_STATUS.get(str(prev_status_raw), prev_status_raw) if prev_status_raw is not None else None
            if prev_status is None:
                contract_states[cid] = current_status
                cs_changed = True
            elif current_status != prev_status:
                contract_states[cid] = current_status
                cs_changed = True
                # Skip alert when contract moves to complete — both sides know when it's done
                if current_status != "complete":
                    await tg.send(chat_id, tones.fmt_contract_status(
                        cid,
                        prev_status    = prev_status,
                        new_status     = current_status,
                        iproduct       = ct.get("iproduct", ""),
                        iprice         = ct.get("iprice"),
                        icurrency      = ct.get("icurrency", "bytes"),
                        init_username  = init_username,
                        other_username = other_username,
                        inituid        = ct.get("inituid"),
                        my_uid         = str(my_uid),
                        istatus        = istatus,
                        ostatus        = ostatus,
                    ))

    if cs_changed:
        await _db(upsert_user, db_cfg, chat_id, {"contract_states": contract_states})

    # ── Contract expiry warnings ───────────────────────────────────────────────
    # timeout is a unix timestamp. Alert once when under 24h remaining.
    # Deduplication via seen_events namespace "contract_expiry" — no extra DB column needed.
    if notifs.get("contract_expiry", True) and "contracts" in data:
        now_ts = int(time.time())
        cts = data["contracts"]
        if isinstance(cts, dict): cts = [cts]
        for ct in (cts or []):
            cid     = str(ct.get("cid", ""))
            timeout = ct.get("timeout")
            status  = CONTRACT_STATUS.get(str(ct.get("status", "")), f"status #{ct.get('status', '')}")
            if not cid or not timeout:
                continue
            try:
                exp_ts = int(timeout)
            except (ValueError, TypeError):
                continue
            secs_left  = exp_ts - now_ts
            hours_left = int(secs_left // 3600)
            if 0 < secs_left < 86400 and status not in ("complete", "cancelled", "disputed"):
                if not await _db(is_event_seen, db_cfg, chat_id, "contract_expiry", cid):
                    product = ct.get("iproduct", "contract")
                    await tg.send(chat_id, tones.fmt_contract_expiry(cid, product, hours_left))
                    await _db(mark_event_seen, db_cfg, chat_id, "contract_expiry", cid)
                    log.info(f"contract expiry alert: cid={cid} hours_left={hours_left}")

    # ── Disputes — extracted from embedded idispute/odispute contract sub-fields ──
    # No extra API call needed — dispute data came embedded in contracts above.
    # Always alert regardless of notif toggles — disputes have HF time limits
    dispute_states = dict(user.get("dispute_states") or {})
    ds_changed     = False

    # Collect all unique disputes from idispute/odispute across all contracts
    seen_cdids: set = set()
    embedded_disputes: list = []
    contracts_for_disputes = data.get("contracts") or []
    if isinstance(contracts_for_disputes, dict):
        contracts_for_disputes = [contracts_for_disputes]
    for ct in contracts_for_disputes:
        for side_key in ("idispute", "odispute"):
            side_disputes = ct.get(side_key) or []
            if isinstance(side_disputes, dict):
                side_disputes = [side_disputes]
            for d in side_disputes:
                cdid = str(d.get("cdid", ""))
                if cdid and cdid not in seen_cdids:
                    seen_cdids.add(cdid)
                    embedded_disputes.append(d)
    if embedded_disputes:
        data["disputes"] = embedded_disputes

    if "disputes" in data:
        disputes = data["disputes"]
        if isinstance(disputes, dict): disputes = [disputes]
        known_tids       = list(user.get("known_tids") or [])
        dtids_changed    = False
        for d in (disputes or []):
            cdid        = str(d.get("cdid", ""))
            status      = str(d.get("status", ""))
            cid         = str(d.get("contractid", ""))
            dispute_tid = d.get("dispute_tid")
            if not cdid:
                continue
            # Add dispute thread to known_tids so reply/mention detection covers it
            if dispute_tid:
                dtid = int(dispute_tid)
                if dtid not in known_tids:
                    known_tids.append(dtid)
                    dtids_changed = True
            is_defendant = str(d.get("defendantuid", "")) == str(my_uid)
            prev_ds = dispute_states.get(cdid)
            if prev_ds is None:
                dispute_states[cdid] = status
                ds_changed = True
                notes = strip_bbcode(d.get("claimantnotes", ""))[:150]
                await tg.send(chat_id, tones.fmt_dispute_new(cdid, cid, is_defendant, notes))
            elif status != prev_ds:
                dispute_states[cdid] = status
                ds_changed = True
                await tg.send(chat_id, tones.fmt_dispute_update(cdid, cid, status))
        if dtids_changed:
            await _db(upsert_user, db_cfg, chat_id, {"known_tids": known_tids[-200:]})

    if ds_changed:
        await _db(upsert_user, db_cfg, chat_id, {"dispute_states": dispute_states})

    # ── B-Ratings — piggybacked into medium batch as 4th endpoint ────────────
    # Data arrives free every medium cycle; processing is still gated to 30 min
    # because b-ratings are rare. Eliminates the dedicated slow-loop call entirely.
    if notifs.get("bratings", True) and "bratings" in data:
        now_ts             = int(time.time())
        last_brating_check = int(user.get("last_brating_check_at") or 0)
        if (now_ts - last_brating_check) >= 1800:
            await _db(upsert_user, db_cfg, chat_id, {"last_brating_check_at": now_ts})
            brs = data["bratings"]
            if isinstance(brs, dict): brs = [brs]
            linked_at_br = int(user.get("linked_at") or 0) or now_ts
            # Resolve any unknown fromids using the uid_cache already populated above
            for b in (brs or []):
                crid = str(b.get("crid", ""))
                if not crid or await _db(is_event_seen, db_cfg, chat_id, "brating", crid):
                    continue
                dateline_br = int(b.get("dateline") or 0)
                if dateline_br and dateline_br < linked_at_br:
                    await _db(mark_event_seen, db_cfg, chat_id, "brating", crid)
                    continue
                from_uid = b.get("fromid", "?")
                await tg.send(chat_id, tones.fmt_brating(
                    b.get("contractid", "?"),
                    from_uid,
                    int(b.get("amount", 0)),
                    b.get("message", ""),
                    from_username=_resolve_uid(from_uid),
                ))
                await _db(mark_event_seen, db_cfg, chat_id, "brating", crid)
                log.info(f"brating alert (medium batch): chat_id={chat_id} crid={crid}")

    # ── Bytes received ────────────────────────────────────────────────────────
    if notifs.get("bytes", True) and "bytes" in data:
        txs = data["bytes"]
        if isinstance(txs, dict): txs = [txs]

        # Gambling wins accumulate in DB between cycles and flush every 30 min.
        # Regular (non-gambling) bytes still alert immediately.
        # Sanitize gambling_pending.
        # Root issue: MySQL JSON column comes back as a raw string if _parse_user missed it.
        # '[]' is truthy as a string → flush fires → iterates chars → int('[') → crash.
        # _parse_user now parses it, but keep this as belt-and-suspenders for stale DB rows.
        _gp_raw = user.get("gambling_pending") or []
        if isinstance(_gp_raw, str):
            try:
                _gp_raw = json.loads(_gp_raw)
            except Exception:
                _gp_raw = []
        if isinstance(_gp_raw, list) and _gp_raw and not isinstance(_gp_raw[0], (list, tuple)):
            _gp_raw = []
        gambling_pending = _gp_raw
        last_gambling_flush  = int(user.get("last_gambling_flush") or 0)
        GAMBLING_FLUSH_SECS  = 1800   # 30 minutes

        for tx in (txs or []):
            txid   = str(tx.get("id", ""))
            amount = int(float(tx.get("amount", 0) or 0))
            reason = tx.get("reason", "")
            if not txid or await _db(is_event_seen, db_cfg, chat_id, "bytes", txid):
                continue
            tx_date = int(tx.get("dateline") or 0)
            if tx_date and tx_date < linked_at:
                await _db(mark_event_seen, db_cfg, chat_id, "bytes", txid)
                continue
            # "from" and "post" are list fields in the bytes API response.
            # Extract uid from from[0] (may be dict with uid key, or raw uid string).
            _from_raw = tx.get("from")
            raw_from  = ""
            if isinstance(_from_raw, list) and _from_raw:
                _fi = _from_raw[0]
                raw_from = str(_fi.get("uid", "") if isinstance(_fi, dict) else _fi)
            elif isinstance(_from_raw, str) and _from_raw.isdigit():
                raw_from = _from_raw  # scalar uid string (shouldn't happen but handle it)
            from_user = _resolve_uid(raw_from) if raw_from.isdigit() else ""

            # post[0] may be a dict with pid/tid, or a raw pid string.
            _post_raw = tx.get("post") or []
            post_id   = ""
            post_tid  = ""
            if isinstance(_post_raw, list) and _post_raw:
                _post_item = _post_raw[0]
                if isinstance(_post_item, dict):
                    post_id  = str(_post_item.get("pid") or "")
                    post_tid = str(_post_item.get("tid") or "")
                else:
                    post_id = str(_post_item)
            await _db(mark_event_seen, db_cfg, chat_id, "bytes", txid)
            if tones.is_gambling_reason(reason):
                gambling_pending.append([amount, reason, from_user])
            else:
                await tg.send(chat_id, tones.fmt_bytes(amount, reason, from_user, post_id, post_tid))

        # Flush pending gambling wins if 30 min have passed (or we have a backlog)
        if gambling_pending and (now - last_gambling_flush) >= GAMBLING_FLUSH_SECS:
            await tg.send(chat_id, tones.fmt_bytes_bundle([tuple(t) for t in gambling_pending]))
            gambling_pending    = []
            last_gambling_flush = now

        await _db(upsert_user, db_cfg, chat_id, {
            "gambling_pending":    gambling_pending,
            "last_gambling_flush": last_gambling_flush,
        })



# ── B-Ratings (slow loop, every 3 min) ────────────────────────────────────────

async def check_bratings(user: dict, hf: HFClient, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """
    No-op shim — b-rating detection was moved into check_account_events (medium loop)
    where it piggybacks on the existing 4-endpoint batch at zero extra API cost.
    This function is kept so bot.py call sites don't need to change.
    """
    return



# ── Thread replies + mentions ──────────────────────────────────────────────────

async def check_posts(user: dict, hf: HFClient, tg: TelegramBot, cfg: dict, db_cfg: dict, other_users: list = None):
    log.info(f"check_posts chat_id={user.get('chat_id')} user={user.get('hf_username','?')}")

    notifs         = user.get("notifications") or {}
    chat_id        = user["chat_id"]
    my_uid         = str(user["hf_uid"])
    my_username    = user.get("hf_username", "")
    now            = int(time.time())
    check_replies  = notifs.get("thread_replies", True)
    check_mentions = notifs.get("mentions", True)
    muted_tids     = set(str(t) for t in (user.get("muted_tids") or []))

    # UIDs whose replies we should NOT alert on (e.g. bots like Stanley)
    # Comes from buddies with ignore_replies=True flag
    reply_ignore_uids = set(
        str(b["uid"])
        for b in (user.get("buddy_list") or [])
        if b.get("ignore_replies")
    )

    if not check_replies and not check_mentions:
        return

    known_tids       = list(user.get("known_tids") or [])
    own_tids         = set(str(t) for t in (user.get("own_tids") or []))        # threads the user CREATED
    participated_tids = set(str(t) for t in (user.get("participated_tids") or []))  # threads the user POSTED IN (not OP)
    tids_changed         = False
    own_changed          = False
    participated_changed = False

    # Step 1a: Discover new threads every cycle — 1 page only (1 API call).
    # Postnum gate: if me.postnum hasn't changed since last cycle, the user
    # hasn't posted anything new, so posts _uid discovery will find nothing.
    # Persisted in DB (last_discovery_postnum) so the gate survives restarts.
    # Previously used _last_seen_postnum on the in-memory user dict — that dict
    # is reloaded from DB every cycle, so the stamp was always None → gate never
    # fired → 3 discovery pages fetched every single cycle (fixed here).
    post_pages_to_fetch           = 1   # normal cadence — 1 page per cycle
    own_tids_bootstrapped         = bool(user.get("own_tids_bootstrapped"))
    participated_tids_bootstrapped = bool(user.get("participated_tids_bootstrapped"))
    current_postnum               = int(user.get("last_postnum") or 0)
    last_disc_postnum             = int(user.get("last_discovery_postnum") or 0)
    run_discovery = (
        not own_tids_bootstrapped           # always run until bootstrap completes
        or not participated_tids_bootstrapped  # backfill participated_tids on first run
        or current_postnum == 0             # me.postnum not yet fetched
        or current_postnum != last_disc_postnum   # we actually posted something new
    )
    # One-time bootstrap: scan 10 pages of posts _uid to retroactively populate
    # participated_tids with every thread the user has posted in.
    # After this runs once, normal 1-page discovery keeps it current.
    if not participated_tids_bootstrapped:
        post_pages_to_fetch = 10
        log.info(f"check_posts: participated_tids bootstrap — scanning up to 10 pages of posts for chat_id={chat_id}")

    # Baselines built from our own posts during discovery — used to skip posts
    # fetches for threads where our post is currently the latest (optimization 1),
    # and to break early in the inner post loop (optimization 4).
    discovery_datelines: dict[int, int] = {}   # {tid: our latest post dateline}
    own_pids_by_tid:     dict[int, int] = {}   # {tid: our latest post pid}
    bootstrap_new_tids:  set            = set()  # participated tids found during bootstrap
    new_participated_seeds: dict[str, int] = {}  # str(tid) → our post dateline, for thread_meta seeding

    if run_discovery:
        for post_page in range(1, post_pages_to_fetch + 1):
            if is_rate_limited(hf.token):
                log.warning(f"check_posts: rate limit hit, stopping posts discovery at page {post_page}")
                break
            disc = await hf.read({
                "posts": {
                    "_uid":     [user["hf_uid"]],
                    "_page":    post_page,
                    "_perpage": 30,
                    "pid":      True,
                    "tid":      True,
                    "dateline": True,   # free field — seed thread baseline, skip own-post fetches
                }
            })
            if not disc or "posts" not in disc:
                break
            rows = disc["posts"]
            if isinstance(rows, dict): rows = [rows]
            if not rows:
                break
            for p in rows:
                tid = int(p["tid"]) if "tid" in p else None
                pid = str(p.get("pid") or "")
                dl  = int(p.get("dateline") or 0)
                if tid and tid not in known_tids:
                    known_tids.append(tid)
                    tids_changed = True
                # Track as participated (replied-to) — used for reply alerts on non-OP threads
                if tid and str(tid) not in participated_tids and str(tid) not in own_tids:
                    participated_tids.add(str(tid))
                    participated_changed = True
                    if not participated_tids_bootstrapped:
                        bootstrap_new_tids.add(str(tid))
                    # Record our post dateline so thread_meta can be seeded after init.
                    # This baseline prevents the prev_lastpost==0 → 600s gate from
                    # silently dropping mentions posted after we replied but before
                    # the second poll cycle runs.
                    if dl:
                        new_participated_seeds[str(tid)] = dl
                # Seed thread baseline from our own posts — prevents false fetches when we
                # discover a thread this cycle where our post is currently the latest.
                if tid and dl:
                    existing = discovery_datelines.get(tid, 0)
                    if dl > existing:
                        discovery_datelines[tid] = dl
                        own_pids_by_tid[tid] = int(pid) if pid.isdigit() else 0
            if len(rows) < 30:
                break  # last page — fewer results than perpage means we hit the end

    # Step 1b: Discover threads you CREATED via threads _uid.
    # Only these get reply alerts. known_tids still gets them for mention polling.
    # If own_tids is very small (just started or column was freshly added), do
    # a deeper paginated sweep to catch all historical threads.
    # Bootstrap: sweep enough pages to cover all threads the user ever created.
    # threads _uid returns up to 20 per page. Run 10 pages (200 threads) on startup
    # if own_tids looks incomplete vs what we'd expect.
    # Threshold 150: re-bootstrap any time own_tids hasn't reached a comfortable ceiling,
    # ensuring necro'd threads and full history are always covered.
    # Bootstrap own_tids once per user — flag persists in DB so we never re-run
    # on every restart. Count-based threshold was wrong: users with < 150 threads
    # would bootstrap every single startup forever.
    # Gate own_tids sweep to every 30 min after bootstrap.
    # Threadnum gate: if me.threadnum hasn't changed, no new thread was created,
    # so we can skip the sweep entirely (saves 1 API call per 30-min window).
    last_own_tids_at     = int(user.get("last_discovery_at") or 0)
    current_threadnum    = int(user.get("last_threadnum") or 0)
    prev_threadnum_seen  = int(user.get("last_discovery_threadnum") or 0)
    threadnum_changed    = (current_threadnum != prev_threadnum_seen) or (current_threadnum == 0)
    run_own_tids = (not own_tids_bootstrapped) or (
        (now - last_own_tids_at) > 1800 and threadnum_changed
    ) or (not own_tids_bootstrapped)
    pages_to_fetch = 1 if own_tids_bootstrapped else 5
    log.info(f"check_posts: own_tids — have {len(own_tids)} threads, bootstrapped={own_tids_bootstrapped}, run={run_own_tids}, fetching up to {pages_to_fetch} pages")

    # thread_meta initialized here — before the sweep — so sweep can compare lastpost values.
    _raw_meta = user.get("thread_meta") or {}
    if not _raw_meta and user.get("thread_state"):
        _raw_meta = {tid: {"lp": int(lp or 0), "nr": 0, "lpu": ""}
                     for tid, lp in (user.get("thread_state") or {}).items()}
    thread_meta = dict(_raw_meta)

    # Seed thread_meta for threads we just started participating in, using our own
    # post's dateline as the baseline. Without this seed, the poll falls into the
    # prev_lastpost==0 path and the 600s freshness gate silently drops any mention/
    # reply that arrived > 10 min before that poll runs — permanently, since the gate
    # also writes thread_meta[tid]["lp"] to the quote's dateline, making future polls
    # see no delta and never fetch the posts.
    # Seeding here means the poll sees prev_lastpost = our_post_dateline (non-zero),
    # bypasses the 600s gate entirely, and catches any post newer than ours normally.
    for _tid_str, _disc_dl in new_participated_seeds.items():
        if _tid_str not in thread_meta:
            thread_meta[_tid_str] = {
                "lp": _disc_dl, "nr": 0, "lpu": str(my_uid),
                "sub": "", "bpid": "", "views": 0,
            }

    # Force-poll set: dormant own threads the sweep finds have new activity this cycle.
    own_tids_force_poll: set = set()

    if run_own_tids:
        for page in range(1, pages_to_fetch + 1):
            if is_rate_limited(hf.token):
                log.warning(f"check_posts: rate limit hit, stopping own_tids sweep at page {page}")
                break
            tdisc = await hf.read({
                "threads": {
                    "_uid":          [user["hf_uid"]],
                    "_page":         page,
                    "_perpage":      30,
                    "tid":           True,
                    "lastpost":      True,   # detect activity on cold own threads
                    "lastposteruid": True,   # skip if we were the last poster
                }
            })
            if not tdisc or "threads" not in tdisc:
                log.info(f"check_posts: threads _uid page={page} returned nothing, stopping")
                break
            rows = tdisc["threads"]
            if isinstance(rows, dict): rows = [rows]
            if not rows:
                log.info(f"check_posts: threads _uid page={page} empty, stopping")
                break
            log.info(f"check_posts: threads _uid page={page} returned {len(rows)} rows")
            for t in rows:
                tid = int(t["tid"]) if "tid" in t else None
                if tid:
                    if tid not in known_tids:
                        known_tids.append(tid)
                        tids_changed = True
                    if str(tid) not in own_tids:
                        own_tids.add(str(tid))
                        own_changed = True
                    # If lastpost moved since we stored it and it wasn't us posting,
                    # force-poll this thread this cycle even if it's cold.
                    sweep_lp  = int(t.get("lastpost") or 0)
                    sweep_lpu = str(t.get("lastposteruid") or "")
                    stored_lp = int((thread_meta.get(str(tid)) or {}).get("lp") or 0)
                    if sweep_lp and sweep_lp > stored_lp and sweep_lpu != my_uid:
                        own_tids_force_poll.add(tid)
                        log.info(f"check_posts: own thread tid={tid} revived (lp={sweep_lp} > stored={stored_lp}), force-polling")
            if len(rows) == 0:
                log.info(f"check_posts: threads _uid page={page} empty, stopping")
                break

    if tids_changed:
        known_tids = known_tids[-200:]
        await _db(upsert_user, db_cfg, chat_id, {"known_tids": known_tids})
        log.info(f"check_posts: now tracking {len(known_tids)} threads for chat_id={chat_id}")

    if own_changed:
        await _db(upsert_user, db_cfg, chat_id, {"own_tids": list(own_tids)[-200:]})

    if participated_changed:
        await _db(upsert_user, db_cfg, chat_id, {"participated_tids": list(participated_tids)[-300:]})

    # ── Retroactive scan for newly bootstrapped participated threads ──────────
    # We know exactly when we posted in each thread (discovery_datelines[btid]).
    # Fetch last-page posts and alert on anything newer than our post — no thread_meta
    # needed, no staleness gate, just our own post dateline as the baseline.
    if bootstrap_new_tids and (check_replies or check_mentions):
        log.info(f"check_posts: retroactive scan — {len(bootstrap_new_tids)} tids for chat_id={chat_id}")
        for btid_str in bootstrap_new_tids:
            btid        = int(btid_str)
            our_disc_dl = discovery_datelines.get(btid, 0)
            if not our_disc_dl:
                # Never saw our own post during discovery — can't set baseline, skip
                log.info(f"check_posts: retroactive skip tid={btid} — no discovery baseline")
                continue
            if is_rate_limited(hf.token):
                log.warning("check_posts: rate limit during retroactive scan, stopping")
                break
            # Get numreplies from thread_meta if available — determines last page
            _btm          = thread_meta.get(btid_str) or {}
            numreplies_bt = int(_btm.get("nr") or 0)
            last_page_bt  = max(1, (numreplies_bt + 1 + 9) // 10) if numreplies_bt else 1
            subject_bt    = _btm.get("sub") or f"Thread #{btid}"
            bt_data = await hf.read({
                "posts": {
                    "_tid":     [btid],
                    "_page":    last_page_bt,
                    "_perpage": 10,
                    "pid":      True,
                    "uid":      True,
                    "username": True,
                    "dateline": True,
                    "message":  True,
                    "subject":  True,
                }
            })
            if not bt_data or "posts" not in bt_data:
                continue
            bt_posts = bt_data["posts"]
            if isinstance(bt_posts, dict): bt_posts = [bt_posts]
            bt_posts.sort(key=lambda p: int(p.get("dateline") or 0), reverse=True)
            for post in bt_posts:
                pid      = str(post.get("pid", ""))
                uid      = str(post.get("uid", ""))
                username = post.get("username") or ""
                dateline = int(post.get("dateline") or 0)
                message  = post.get("message", "")
                subject_bt = post.get("subject") or subject_bt
                if dateline <= our_disc_dl:
                    break  # everything from here is older than our post
                if uid == my_uid or uid in reply_ignore_uids:
                    continue
                mention_type = check_mentions and find_mention(message, my_uid, my_username)
                if mention_type and not await _db(is_event_seen, db_cfg, chat_id, "mention", pid):
                    snippet = strip_bbcode(strip_quote_blocks(message))[:SNIPPET_LEN]
                    await tg.send(chat_id,
                        tones.fmt_mention(subject_bt, btid_str, pid, snippet, mention_type),
                        reply_markup=_mute_button(btid_str)
                    )
                    await _db(mark_event_seen, db_cfg, chat_id, "mention", pid)
                    await _db(mark_event_seen, db_cfg, chat_id, "reply", pid)
                    log.info(f"check_posts: retroactive mention — tid={btid} pid={pid}")
                elif check_replies and not await _db(is_event_seen, db_cfg, chat_id, "reply", pid):
                    snippet = strip_bbcode(message)[:SNIPPET_LEN]
                    await tg.send(chat_id,
                        tones.fmt_reply(subject_bt, btid_str, pid, snippet, replier=username),
                        reply_markup=_mute_button(btid_str)
                    )
                    await _db(mark_event_seen, db_cfg, chat_id, "reply", pid)
                    log.info(f"check_posts: retroactive reply — tid={btid} pid={pid} replier={username}")

    if run_discovery and current_postnum:
        # Persist the postnum we acted on — next cycle compares against this to gate discovery
        await _db(upsert_user, db_cfg, chat_id, {"last_discovery_postnum": current_postnum})

    if run_own_tids and current_threadnum:
        # Persist the threadnum we acted on — next cycle compares to gate own_tids sweep
        await _db(upsert_user, db_cfg, chat_id, {"last_discovery_threadnum": current_threadnum})

    if run_own_tids:
        # Stamp when own_tids sweep ran so the 30-min gate works
        await _db(upsert_user, db_cfg, chat_id, {"last_discovery_at": int(time.time())})

    if not own_tids_bootstrapped:
        # Save flag even if no new threads were found — otherwise it never sets
        # when all threads were already known and own_changed stays False
        await _db(upsert_user, db_cfg, chat_id, {"own_tids_bootstrapped": 1})

    if not participated_tids_bootstrapped and run_discovery:
        await _db(upsert_user, db_cfg, chat_id, {"participated_tids_bootstrapped": 1})
        log.info(f"check_posts: participated_tids bootstrap complete — {len(participated_tids)} threads for chat_id={chat_id}")

    if not known_tids:
        return

    # Step 2: Split known_tids into hot vs cold based on last activity.
    # Hot = active in last 30 days → poll every cycle (these matter).
    # Cold = older → poll every 30 min (dead threads, rarely change).
    # Uses a SEPARATE timer (last_cold_poll_at) — decoupled from last_discovery_at
    # so the per-cycle discovery stamp doesn't silently reset the cold poll window
    # and prevent cold threads from ever being polled.
    now            = int(time.time())
    last_cold_poll = int(user.get("last_cold_poll_at") or 0)
    last_warm_poll = int(user.get("last_warm_poll_at") or 0)
    # Seed timers on first run so threads don't all fire at once on restart
    if last_cold_poll == 0:
        last_cold_poll = now
        await _db(upsert_user, db_cfg, chat_id, {"last_cold_poll_at": now})
    if last_warm_poll == 0:
        last_warm_poll = now
        await _db(upsert_user, db_cfg, chat_id, {"last_warm_poll_at": now})

    run_cold = (now - last_cold_poll) > 1800   # cold threads every 30 min
    run_warm = (now - last_warm_poll) > 900    # warm threads every 15 min

    # 3-tier system:
    #   hot  = active in last 48h       → poll every cycle (3 min)
    #   warm = active in last 2-7 days  → poll every 15 min
    #   cold = older than 7 days        → poll every 30 min
    # High-reply threads get an extended hot window:
    #   200+  replies → hot for 14 days (instead of 48h)
    #   1000+ replies → hot for 30 days
    hot_cutoff  = now - (2 * 86400)    # 48h default
    warm_cutoff = now - (7 * 86400)    # 7 days

    hot_tids  = []
    warm_tids = []
    cold_tids = []
    for t in known_tids:
        _tm_t = thread_meta.get(str(t)) or {}
        last  = int(_tm_t.get("lp") or 0)
        nr    = int(_tm_t.get("nr") or 0)
        # Dynamic hot cutoff based on reply count
        if nr >= 1000:
            t_hot_cutoff = now - (30 * 86400)
        elif nr >= 200:
            t_hot_cutoff = now - (14 * 86400)
        else:
            t_hot_cutoff = hot_cutoff
        if last == 0:
            hot_tids.append(t)           # never polled → hot
        elif last >= t_hot_cutoff:
            hot_tids.append(t)           # active within dynamic window → hot
        elif last >= warm_cutoff:
            warm_tids.append(t)          # active 2-7 days → warm
        else:
            cold_tids.append(t)          # dormant > 7 days → cold

    # Sort each tier newest-first so recently-participated threads (appended last
    # to known_tids) are polled early in the cycle instead of being cut by the
    # timeout at the tail of a long queue.
    _lp_key = lambda t: int((thread_meta.get(str(t)) or {}).get("lp") or 0)
    hot_tids.sort(key=_lp_key, reverse=True)
    warm_tids.sort(key=_lp_key, reverse=True)

    # Force-poll own threads where the sweep detected new lastpost this cycle.
    already_polling = set(hot_tids)
    force_poll_list = [t for t in own_tids_force_poll if t not in already_polling]
    warm_to_poll    = warm_tids if run_warm else []
    cold_to_poll    = cold_tids[:30] if run_cold else []
    tids_to_poll    = hot_tids + force_poll_list + warm_to_poll + cold_to_poll
    log.info(
        f"check_posts: polling {len(hot_tids)} hot + {len(force_poll_list)} force "
        f"+ {len(warm_to_poll)}/{len(warm_tids)} warm "
        f"+ {len(cold_to_poll)}/{len(cold_tids)} cold "
        f"for chat_id={chat_id}"
    )

    if run_warm and warm_tids:
        await _db(upsert_user, db_cfg, chat_id, {"last_warm_poll_at": now})
    if run_cold and cold_tids:
        await _db(upsert_user, db_cfg, chat_id, {"last_cold_poll_at": now})

    # ── Step 2: threads _tid poll (hot + warm + cold + force) ───────────────────
    # posts _tid with _page:1 returns oldest-first — useless for detecting new replies
    # on multi-page threads. Keep the original threads _tid detect → last-page posts
    # fetch approach. Call savings come from hot/cold tiering and the discovery gate fix,
    # not from collapsing the two steps.
    # True if we seeded thread_meta for new participated threads above — must persist.
    meta_changed = bool(new_participated_seeds)

    # ── Step 3: Poll tids_to_poll via threads _tid ────────────────────────────
    # These run less frequently (warm=15min, cold=30min) so per-thread posts fetches
    # on activity are fine — low call rate. Also handles bestpid/views/closed detection
    # for all tiers (hot threads inherit those checks here on their warm cycle).
    threads  = []
    bad_tids = set()

    async def poll_chunk(chunk):
        if not chunk:
            return []
        if is_rate_limited(hf.token):
            return []  # Don't bisect while rate limited — would nuke all known_tids
        state_data = await hf.read({
            "threads": {
                "_tid":          chunk,
                "tid":           True,
                "subject":       True,
                "lastpost":      True,
                "lastposteruid": True,
                "lastposter":    True,   # username string — free, no extra lookup needed
                "numreplies":    True,
                "fid":           True,   # forum id — useful for muted list display
                "uid":           True,   # thread creator UID
                "username":      True,   # thread creator username
                "views":         True,   # track view count for spike detection
                "bestpid":       True,   # detect best answer being marked
                "closed":        True,   # detect thread being closed
            }
        })
        if state_data and "threads" in state_data:
            rows = state_data["threads"]
            if isinstance(rows, dict): rows = [rows]
            return rows
        if state_data is None:
            # None = network error, timeout, or rate limit — NOT a missing thread
            # Do not bisect or prune, just skip this chunk silently
            return []
        if len(chunk) == 1:
            # Got a real API response but thread wasn't in it — actually inaccessible
            log.info(f"Removing inaccessible tid={chunk[0]} from known_tids")
            bad_tids.add(chunk[0])
            return []
        # Bisect — split and retry each half
        mid = len(chunk) // 2
        return (await poll_chunk(chunk[:mid])) + (await poll_chunk(chunk[mid:]))

    for i in range(0, len(tids_to_poll), 30):
        chunk = tids_to_poll[i:i+30]
        threads.extend(await poll_chunk(chunk))

    if bad_tids:
        known_tids = [t for t in known_tids if t not in bad_tids]
        await _db(upsert_user, db_cfg, chat_id, {"known_tids": known_tids})
        log.info(f"Pruned {len(bad_tids)} inaccessible tids from known_tids")

    for t in (threads or []):
        tid           = str(t.get("tid", ""))
        subject       = t.get("subject", "Thread")
        lastpost      = int(t.get("lastpost") or 0)
        lastposteruid = str(t.get("lastposteruid") or "")
        lastposter    = t.get("lastposter") or ""
        numreplies    = int(t.get("numreplies") or 0)
        last_page     = max(1, (numreplies + 1 + 9) // 10)

        if not tid or not lastpost:
            continue

        if tid in muted_tids:
            continue

        _tm           = thread_meta.get(tid) or {}
        stored_lp     = int(_tm.get("lp") or 0)
        disc_lp       = discovery_datelines.get(int(tid), 0)
        prev_lastpost = stored_lp if stored_lp else disc_lp
        prev_nr       = int(_tm.get("nr") or 0)
        prev_lpu      = _tm.get("lpu") or ""

        # ── Thread state change detection (free from the poll we already do) ───
        bestpid = str(t.get("bestpid") or "")
        views   = int(t.get("views") or 0)
        closed  = bool(t.get("closed"))

        _tm_full     = thread_meta.get(tid) or {}
        prev_bestpid = str(_tm_full.get("bpid") or "")
        prev_views   = int(_tm_full.get("views") or 0)

        # Best answer alert — someone marked a post as best in your thread
        if bestpid and bestpid != prev_bestpid and tid in own_tids and bestpid != "0":
            await tg.send(chat_id, tones.fmt_best_answer(subject, tid, bestpid))
            log.info(f"Best answer: tid={tid} bestpid={bestpid} chat_id={chat_id}")

        # View spike alert — 500+ views in one polling cycle (thread going viral or mod review)
        VIEW_SPIKE = 500
        if prev_views and views > 0 and (views - prev_views) >= VIEW_SPIKE and tid in own_tids:
            spike = views - prev_views
            await tg.send(chat_id, tones.fmt_view_spike(subject, tid, spike, views))
            log.info(f"View spike: tid={tid} +{spike} views chat_id={chat_id}")

        if prev_lastpost == 0:
            thread_meta[tid] = {"lp": lastpost, "nr": numreplies, "lpu": lastposteruid, "sub": subject, "bpid": bestpid, "views": views}
            meta_changed = True
            if lastposteruid == my_uid or lastposteruid in reply_ignore_uids:
                continue
            # Only process (mentions or replies) if the activity is fresh
            if time.time() - lastpost > 600:
                continue
            # Fall through to fetch posts and check for mention/reply

        if lastpost <= prev_lastpost:
            continue

        thread_meta[tid] = {"lp": lastpost, "nr": numreplies, "lpu": lastposteruid, "sub": subject, "bpid": bestpid, "views": views}
        meta_changed = True

        if lastposteruid == my_uid:
            continue

        # Skip reply alerts from buddies flagged as ignore_replies (e.g. HF bots like Stanley)
        if lastposteruid in reply_ignore_uids:
            log.info(f"check_posts: skipping reply in tid={tid} from ignored uid={lastposteruid}")
            continue

        # Optimization: numreplies unchanged → someone edited, not a new reply. Skip fetch.
        if prev_nr > 0 and numreplies == prev_nr:
            log.info(f"skip posts fetch tid={tid}: numreplies unchanged ({numreplies}) — edit")
            thread_meta[tid] = {"lp": lastpost, "nr": numreplies, "lpu": lastposteruid, "sub": subject, "bpid": bestpid, "views": views}
            meta_changed = True
            continue

        # Step 3: Fetch newest posts using last page so we actually get the new reply
        post_data = await hf.read({
            "posts": {
                "_tid":     [int(tid)],
                "_page":    last_page,
                "_perpage": 10,
                "pid":      True,
                "uid":      True,
                "username": True,   # inline — no extra users lookup needed
                "dateline": True,
                "message":  True,
                "subject":  True,
            }
        })

        if not post_data or "posts" not in post_data:
            if check_replies and tid in own_tids:
                await tg.send(chat_id,
                    f"💬 new reply in <i>{subject}</i>\n"
                    f"🔗 hackforums.net/showthread.php?tid={tid}",
                    reply_markup=_mute_button(tid)
                )
            continue

        posts = post_data["posts"]
        if isinstance(posts, dict): posts = [posts]
        posts.sort(key=lambda p: int(p.get("dateline") or 0), reverse=True)

        for post in posts:
            pid      = str(post.get("pid", ""))
            uid      = str(post.get("uid", ""))
            username = post.get("username") or lastposter or ""  # inline username from API
            dateline = int(post.get("dateline") or 0)
            message  = post.get("message", "")

            is_new   = dateline > prev_lastpost
            is_by_me = uid == my_uid

            # ── Cross-user piggyback mention scan ────────────────────────────
            # When we fetch posts for this user's thread scan, we already have
            # the raw post content. Scan for other bot users' mentions at zero
            # extra API cost — the collective thread pool of all users is searched
            # on behalf of everyone, so you don't have to post in a thread to be
            # alerted if another bot user's scan happens to pull it.
            if other_users and is_new and not is_by_me:
                for other in other_users:
                    o_chat_id  = other.get("chat_id")
                    o_uid      = str(other.get("hf_uid", ""))
                    o_username = other.get("hf_username", "")
                    o_notifs   = other.get("notifications") or {}
                    if not o_chat_id or not o_uid or o_uid == my_uid:
                        continue
                    if not o_notifs.get("mentions", True):
                        continue  # user has mentions disabled
                    if uid == o_uid:
                        continue  # their own post — don't self-alert
                    o_muted = set(str(t) for t in (other.get("muted_tids") or []))
                    if tid in o_muted:
                        continue  # thread is muted by this user
                    o_mention = find_mention(message, o_uid, o_username)
                    if o_mention and not await _db(is_event_seen, db_cfg, o_chat_id, "mention", pid):
                        snippet = strip_bbcode(strip_quote_blocks(message))[:SNIPPET_LEN]
                        await tg.send(o_chat_id,
                            tones.fmt_mention(subject, tid, pid, snippet, o_mention),
                            reply_markup=_mute_button(tid)
                        )
                        await _db(mark_event_seen, db_cfg, o_chat_id, "mention", pid)
                        log.info(f"cross-user mention: tid={tid} pid={pid} → {o_username} (chat={o_chat_id}) type={o_mention}")

            if is_by_me:
                continue
            if not is_new:
                continue
            # Optimization: pid-based early exit. PIDs are sequential — if this
            # pid is <= our own latest pid in this thread, we've seen it before.
            own_pid = own_pids_by_tid.get(int(tid), 0)
            if own_pid and int(pid) <= own_pid:
                break  # sorted newest-first; everything after is older than our post
            # Don't alert on posts from buddies flagged as ignore_replies
            if uid in reply_ignore_uids:
                continue

            mention_type = check_mentions and find_mention(message, my_uid, my_username)

            if mention_type and not await _db(is_event_seen, db_cfg, chat_id, "mention", pid):
                snippet = strip_bbcode(strip_quote_blocks(message))[:SNIPPET_LEN]
                await tg.send(chat_id,
                    tones.fmt_mention(subject, tid, pid, snippet, mention_type),
                    reply_markup=_mute_button(tid)
                )
                await _db(mark_event_seen, db_cfg, chat_id, "mention", pid)
                await _db(mark_event_seen, db_cfg, chat_id, "reply", pid)

            elif check_replies and (tid in own_tids or tid in participated_tids) and not await _db(is_event_seen, db_cfg, chat_id, "reply", pid):
                # Alert on replies in threads YOU created OR posted in
                snippet = strip_bbcode(message)[:SNIPPET_LEN]
                await tg.send(chat_id,
                    tones.fmt_reply(subject, tid, pid, snippet, replier=username),
                    reply_markup=_mute_button(tid)
                )
                await _db(mark_event_seen, db_cfg, chat_id, "reply", pid)

    if meta_changed:
        known_tid_strs = set(str(t) for t in known_tids)
        thread_meta    = {k: v for k, v in thread_meta.items() if k in known_tid_strs}
        await _db(upsert_user, db_cfg, chat_id, {"thread_meta": thread_meta})

    # ── Step 4: FID forum watching — extracted to check_fid_threads() ────────────
    # Moved out of check_posts so it isn't cancelled by the check_posts timeout.
    # bot.py _run_slow calls check_fid_threads() separately after check_posts.




async def check_fid_threads(user: dict, hf: HFClient, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """
    Forum-ID thread watcher — alert on new threads in watched forums.
    Runs every 10 minutes per user.

    Extracted from check_posts so it has its own timeout budget in _run_slow
    and is NOT cancelled when the thread-poll phase runs long.  New users with
    no post history (empty known_tids) are also covered — check_posts would
    have returned early before ever reaching this code.
    """
    notifs      = user.get("notifications") or {}
    chat_id     = user["chat_id"]
    now         = int(time.time())

    tracked_fids  = list(user.get("tracked_fids") or [])
    last_fid_poll = int(user.get("last_fid_poll_at") or 0)
    FID_INTERVAL  = 900   # 15 minutes — was 10min (6/hr). New threads don't post faster than this.

    if not tracked_fids:
        return
    if not notifs.get("fid_threads", True):
        return
    if (now - last_fid_poll) < FID_INTERVAL:
        return
    if is_rate_limited(hf.token):
        log.info(f"check_fid_threads skipped: rate limited chat_id={chat_id}")
        return

    alert_cutoff = now - (6 * 3600)

    fid_meta = {fw.get("fid"): fw for fw in tracked_fids if fw.get("fid")}
    all_fids = [int(fid) for fid in fid_meta.keys()]
    if not all_fids:
        return

    perpage  = 30   # API hard max for _perpage — applies globally, not per-fid
    fid_data = await hf.read({
        "threads": {
            "_fid":      all_fids,
            "_page":     1,
            "_perpage":  perpage,
            "tid":       True,
            "fid":       True,
            "subject":   True,
            "dateline":  True,
            "lastpost":  True,
            "username":  True,
            "firstpost": True,
        }
    })
    if fid_data is None:
        # Transient relay failure — don't burn the cooldown, retry next cycle
        log.warning(f"check_fid_threads: relay returned None for fids={all_fids} chat_id={chat_id} — will retry next cycle")
        return
    if "threads" not in fid_data:
        # API responded but no threads key — forums may be empty or restricted
        log.info(f"check_fid_threads: no threads key in response for fids={all_fids} chat_id={chat_id}")
        await _db(upsert_user, db_cfg, chat_id, {"last_fid_poll_at": now})
        return

    # Success — stamp the cooldown now
    await _db(upsert_user, db_cfg, chat_id, {"last_fid_poll_at": now})

    frows = fid_data["threads"]
    if isinstance(frows, dict): frows = [frows]
    all_frows = frows or []
    log.info(f"FID batch: {len(all_frows)} threads for {len(all_fids)} forums chat_id={chat_id}")

    for ft in all_frows:
        ftid      = str(ft.get("tid", ""))
        fid       = ft.get("fid")
        fsubject  = ft.get("subject", "")
        fdateline = int(ft.get("dateline") or 0)
        fusername = ft.get("username", "")
        if not ftid or not fid:
            continue
        fw       = fid_meta.get(int(fid)) or fid_meta.get(str(fid)) or {}
        fname    = fw.get("name", f"Forum #{fid}")
        added_at = int(fw.get("added_at") or 0)

        ns_fid = f"fid_{fid}"
        if await _db(is_event_seen, db_cfg, chat_id, ns_fid, ftid):
            continue
        await _db(mark_event_seen, db_cfg, chat_id, ns_fid, ftid)

        too_old = (added_at and fdateline < added_at) or (fdateline < alert_cutoff)
        if too_old:
            continue

        fp = ft.get("firstpost") or {}
        if isinstance(fp, list) and fp: fp = fp[0]
        snippet = strip_bbcode(fp.get("message", "") if isinstance(fp, dict) else "")[:SNIPPET_LEN]
        log.info(f"FID alert: fid={fid} fname={fname} tid={ftid} chat_id={chat_id}")
        await tg.send(chat_id, tones.fmt_fid_thread(
            fname, fid, fsubject, ftid, fusername, snippet
        ))


# ── Buddy tracking ─────────────────────────────────────────────────────────────

async def check_buddy_activity(user: dict, hf: HFClient, tg: TelegramBot, cfg: dict, db_cfg: dict):
    log.info(f"check_buddy_threads chat_id={user.get('chat_id')} user={user.get('hf_username','?')} buddies={len(user.get('buddy_list') or [])}")

    chat_id    = user["chat_id"]
    buddy_list = user.get("buddy_list") or []
    notifs     = user.get("notifications") or {}

    if not buddy_list:
        return

    # ── Dynamic gate: scale with buddy count ──────────────────────────────────
    # 90s × buddy count gives comfortable headroom. At 3 buddies = 270s, we fire
    # ~13x/hr instead of ~20x/hr. Combined with batching (1 call vs N), that's
    # 13 calls/hr for buddies instead of 60 — a 78% reduction.
    BUDDY_INTERVAL = max(270, 90 * len(buddy_list))
    now_ts         = int(time.time())
    last_buddy     = int(user.get("last_buddy_check_at") or 0)
    if (now_ts - last_buddy) < BUDDY_INTERVAL:
        log.info(f"check_buddy_activity: skipping ({now_ts - last_buddy}s < {BUDDY_INTERVAL}s gate) chat_id={chat_id}")
        return
    await _db(upsert_user, db_cfg, chat_id, {"last_buddy_check_at": now_ts})

    now          = int(time.time())
    alert_cutoff = now - (6 * 3600)
    buddy_ugs    = dict(user.get("buddy_usergroups") or {})
    EXILED_UG    = {"7"}
    BANNED_UG    = {"38"}

    buddy_by_uid = {str(b["uid"]): b for b in buddy_list if b.get("uid")}

    # ── Single batched call: threads + posts for ALL buddies at once ──────────
    # Pass all UIDs to _uid — API returns results from all, separated by the uid
    # field in each row. 3 buddies = 1 call instead of 3. Slots used:
    #   slot 1: threads for all buddy UIDs
    #   slot 2: posts for mode=all buddy UIDs (if any)
    #   slot 3: users for usergroup-due UIDs (if any, fits when < 4 slots used)
    # _perpage:30 is shared across all buddies — at 180s cadence, 30 total results
    # is plenty; no buddy posts faster than 10 times in 3 minutes.
    if is_rate_limited(hf.token):
        log.warning(f"check_buddy_activity: rate limited, skipping chat_id={chat_id}")
        return

    all_thread_uids = [int(b["uid"]) for b in buddy_list if b.get("uid")]
    all_post_uids   = [int(b["uid"]) for b in buddy_list
                       if b.get("uid") and b.get("mode", "threads") == "all"]

    # Determine which buddies need a usergroup check (every 4 hours)
    ug_due_uids = []
    if notifs.get("buddy_status", True):
        for b in buddy_list:
            uid        = str(b.get("uid", ""))
            ug_entry   = buddy_ugs.get(uid)
            last_check = int((ug_entry or {}).get("checked_at", 0)) if isinstance(ug_entry, dict) else 0
            if (now - last_check) >= 14400:
                ug_due_uids.append(int(uid))

    # Build the batch (max 4 endpoints)
    asks: dict = {
        "threads": {
            "_uid":     all_thread_uids,
            "_page":    1,
            "_perpage": 30,
            "tid":      True,
            "uid":      True,
            "subject":  True,
            "dateline": True,
        }
    }
    if all_post_uids:
        asks["posts"] = {
            "_uid":     all_post_uids,
            "_page":    1,
            "_perpage": 30,
            "pid":      True,
            "uid":      True,
            "tid":      True,
            "dateline": True,
            "message":  True,
            "subject":  True,
        }
    if ug_due_uids and len(asks) < 4:
        asks["users"] = {
            "_uid":      ug_due_uids,
            "uid":       True,
            "usergroup": True,
        }

    log.info(
        f"check_buddy_activity: batch call — {len(all_thread_uids)} thread UIDs, "
        f"{len(all_post_uids)} post UIDs, {len(ug_due_uids)} UG UIDs — "
        f"{len(asks)} endpoints — chat_id={chat_id}"
    )
    data = await hf.read(asks)
    if not data:
        log.warning(f"check_buddy_activity: 503/timeout chat_id={chat_id}")
        return

    # Index thread + post results by uid for O(1) per-buddy lookup
    from collections import defaultdict
    threads_by_uid: dict = defaultdict(list)
    posts_by_uid:   dict = defaultdict(list)

    raw_threads = data.get("threads") or []
    if isinstance(raw_threads, dict): raw_threads = [raw_threads]
    for t in raw_threads:
        uid_key = str(t.get("uid", ""))
        if uid_key:
            threads_by_uid[uid_key].append(t)

    raw_posts = data.get("posts") or []
    if isinstance(raw_posts, dict): raw_posts = [raw_posts]
    for p in raw_posts:
        uid_key = str(p.get("uid", ""))
        if uid_key:
            posts_by_uid[uid_key].append(p)

    # ── Process threads + posts per buddy from indexed results ─────────────────
    for buddy in buddy_list:
        if not buddy.get("uid"):
            continue
        uid      = str(buddy["uid"])
        username = buddy.get("username", f"UID {uid}")
        added_at = int(buddy.get("added_at") or 0)
        mode     = buddy.get("mode", "threads")

        buddy_threads = threads_by_uid.get(uid, [])
        buddy_posts   = posts_by_uid.get(uid, []) if mode == "all" else []

        log.info(f"buddy threads: uid={uid} username={username} returned {len(buddy_threads)} threads")

        # ── Threads ──────────────────────────────────────────────────────────
        ns_thread = f"buddy_thread_{uid}"
        for t in buddy_threads:
            tid      = str(t.get("tid", ""))
            subject  = t.get("subject", "")
            dateline = int(t.get("dateline") or 0)
            if not tid:
                continue
            too_old  = (added_at and dateline < added_at) or dateline < alert_cutoff
            tid_seen = await _db(is_event_seen, db_cfg, chat_id, ns_thread, tid)
            if tid_seen:
                continue
            await _db(mark_event_seen, db_cfg, chat_id, ns_thread, tid)
            if too_old:
                continue
            log.info(f"buddy thread alert: uid={uid} username={username} tid={tid}")
            await tg.send(chat_id, tones.fmt_buddy_thread(username, subject, tid))

        # ── Posts (mode=all only) ─────────────────────────────────────────────
        if mode == "all" and buddy_posts:
            ns_post   = f"buddy_post_{uid}"
            buddy_posts.sort(key=lambda p: int(p.get("dateline") or 0), reverse=True)
            log.info(f"buddy posts: uid={uid} username={username} returned {len(buddy_posts)} posts")
            for p in buddy_posts:
                pid      = str(p.get("pid", ""))
                tid      = str(p.get("tid", ""))
                subject  = p.get("subject", "")
                dateline = int(p.get("dateline") or 0)
                if not pid or not tid:
                    continue
                too_old  = (added_at and dateline < added_at) or dateline < alert_cutoff
                pid_seen = await _db(is_event_seen, db_cfg, chat_id, ns_post, pid)
                if pid_seen:
                    break  # sorted newest-first — everything after is older
                await _db(mark_event_seen, db_cfg, chat_id, ns_post, pid)
                if too_old:
                    continue
                snippet = strip_bbcode(p.get("message", ""))[:SNIPPET_LEN]
                log.info(f"buddy post alert: uid={uid} username={username} pid={pid} tid={tid}")
                await tg.send(chat_id, tones.fmt_buddy_post(username, subject, tid, pid, snippet))

    # ── Usergroup checks — results already in the batch ───────────────────────
    if ug_due_uids and "users" in data and notifs.get("buddy_status", True):
        u_rows = data["users"]
        if isinstance(u_rows, dict): u_rows = [u_rows]
        for u in (u_rows or []):
            uid        = str(u.get("uid", ""))
            buddy      = buddy_by_uid.get(uid)
            if not buddy:
                continue
            username   = buddy.get("username", f"UID {uid}")
            current_ug = str(u.get("usergroup", ""))
            ug_entry   = buddy_ugs.get(uid)
            prev_ug    = str((ug_entry or {}).get("ug", "")) if isinstance(ug_entry, dict) else str(ug_entry or "")
            if prev_ug and current_ug != prev_ug:
                if current_ug in EXILED_UG:
                    await tg.send(chat_id, tones.fmt_buddy_status(username, uid, "exiled"))
                    log.info(f"buddy exiled: uid={uid} username={username} ug={current_ug}")
                elif current_ug in BANNED_UG:
                    await tg.send(chat_id, tones.fmt_buddy_status(username, uid, "banned"))
                    log.info(f"buddy banned: uid={uid} username={username} ug={current_ug}")
            buddy_ugs[uid] = {"ug": current_ug, "checked_at": now}
    elif ug_due_uids and "users" not in data and notifs.get("buddy_status", True):
        # UG check couldn't fit in the batch (e.g. slot limit) — fire a separate call
        ug_data = await hf.read({"users": {"_uid": ug_due_uids, "uid": True, "usergroup": True}})
        if ug_data and "users" in ug_data:
            u_rows = ug_data["users"]
            if isinstance(u_rows, dict): u_rows = [u_rows]
            for u in (u_rows or []):
                uid        = str(u.get("uid", ""))
                buddy      = buddy_by_uid.get(uid)
                if not buddy:
                    continue
                username   = buddy.get("username", f"UID {uid}")
                current_ug = str(u.get("usergroup", ""))
                ug_entry   = buddy_ugs.get(uid)
                prev_ug    = str((ug_entry or {}).get("ug", "")) if isinstance(ug_entry, dict) else str(ug_entry or "")
                if prev_ug and current_ug != prev_ug:
                    if current_ug in EXILED_UG:
                        await tg.send(chat_id, tones.fmt_buddy_status(username, uid, "exiled"))
                    elif current_ug in BANNED_UG:
                        await tg.send(chat_id, tones.fmt_buddy_status(username, uid, "banned"))
                buddy_ugs[uid] = {"ug": current_ug, "checked_at": now}

    if ug_due_uids:
        await _db(upsert_user, db_cfg, chat_id, {"buddy_usergroups": buddy_ugs})
        user["buddy_usergroups"] = buddy_ugs





async def check_expiry(user: dict, tg: TelegramBot, cfg: dict, db_cfg: dict):
    pass  # Trial system removed — HF Radar is free with no trial period


# Aliases so existing imports keep working
check_buddy_threads = check_buddy_activity
check_token_expiry  = check_expiry
# check_bratings is already defined as a standalone function above


# ── Dispute safety net (slow loop) ────────────────────────────────────────────

async def check_disputes(user: dict, hf: HFClient, tg: TelegramBot, cfg: dict, db_cfg: dict):
    """
    Explicit disputes _uid call — catches disputes on contracts that aged out
    of contract_states (those are only embedded in active/recent contracts).

    Gate: only runs if user has ever had contracts.
    Deduplication via seen_events namespace "dispute".
    """
    chat_id = user["chat_id"]
    my_uid  = user["hf_uid"]

    # Only run for users who have ever opened a contract
    if not user.get("contract_states"):
        log.info(f"check_disputes skipped: no contract_states chat_id={chat_id}")
        return

    # Gate: embedded idispute/odispute in check_account_events handles real-time detection.
    # This function is a safety net for old contracts only — 30-min cadence is plenty.
    now_ts = int(time.time())
    last_dc = int(user.get("last_dispute_check_at") or 0)
    if (now_ts - last_dc) < 1800:
        log.info(f"check_disputes skipped: {now_ts - last_dc}s since last check chat_id={chat_id}")
        return
    await _db(upsert_user, db_cfg, chat_id, {"last_dispute_check_at": now_ts})

    if is_rate_limited(hf.token):
        log.info(f"check_disputes skipped: rate limited chat_id={chat_id}")
        return

    log.info(f"check_disputes chat_id={chat_id} hf_uid={my_uid}")
    linked_at = int(user.get("linked_at") or 0) or int(time.time())

    data = await hf.read({
        "disputes": {
            "_uid":          [my_uid],
            "_perpage":      20,
            "cdid":          True,
            "contractid":    True,
            "claimantuid":   True,
            "defendantuid":  True,
            "dateline":      True,
            "status":        True,
            "dispute_tid":   True,
            "claimantnotes": True,
        }
    })
    if not data or "disputes" not in data:
        log.info(f"check_disputes: no data returned chat_id={chat_id}")
        return

    disputes = data["disputes"]
    if isinstance(disputes, dict): disputes = [disputes]
    log.info(f"check_disputes: {len(disputes)} dispute(s) found chat_id={chat_id}")

    dispute_states = dict(user.get("dispute_states") or {})
    ds_changed     = False
    known_tids     = list(user.get("known_tids") or [])
    dtids_changed  = False

    for d in (disputes or []):
        cdid        = str(d.get("cdid", ""))
        status      = str(d.get("status", ""))
        cid         = str(d.get("contractid", ""))
        dispute_tid = d.get("dispute_tid")
        dateline    = int(d.get("dateline") or 0)
        if not cdid:
            continue
        if dateline and dateline < linked_at:
            continue

        # Add dispute thread to known_tids so reply detection covers it
        if dispute_tid:
            dtid = int(dispute_tid)
            if dtid not in known_tids:
                known_tids.append(dtid)
                dtids_changed = True

        is_defendant = str(d.get("defendantuid", "")) == str(my_uid)
        prev_ds      = dispute_states.get(cdid)

        if prev_ds is None:
            dispute_states[cdid] = status
            ds_changed = True
            if not await _db(is_event_seen, db_cfg, chat_id, "dispute", cdid):
                notes = strip_bbcode(d.get("claimantnotes", ""))[:150]
                await tg.send(chat_id, tones.fmt_dispute_new(cdid, cid, is_defendant, notes))
                await _db(mark_event_seen, db_cfg, chat_id, "dispute", cdid)
                log.info(f"dispute safety net: cdid={cdid} cid={cid} chat_id={chat_id}")
        elif status != prev_ds:
            dispute_states[cdid] = status
            ds_changed = True
            await tg.send(chat_id, tones.fmt_dispute_update(cdid, cid, status))

    if dtids_changed:
        await _db(upsert_user, db_cfg, chat_id, {"known_tids": known_tids[-200:]})
    if ds_changed:
        await _db(upsert_user, db_cfg, chat_id, {"dispute_states": dispute_states})
