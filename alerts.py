"""
Alert message formatting.
"""


def fmt_reply(tone: str, subject: str, tid, pid, snippet: str, replier: str = "") -> str:
    url = f"hackforums.net/showthread.php?tid={tid}&pid={pid}#pid{pid}"
    who = replier if replier else "someone"
    return f"💬 {who} replied to <i>{subject}</i>\n🔗 {url}\n\n{snippet}"


def fmt_pm(tone: str, count: int, total: int) -> str:
    s = "s" if count > 1 else ""
    return (
        f"📨 {count} new PM{s} (unread: {total})\n"
        f"🔗 hackforums.net/private.php"
    )


def fmt_contract(
    tone: str, cid,
    contract_type: str = "standard",
    iproduct: str = "N/A", iprice = "?", icurrency: str = "bytes",
    oproduct: str = None, oprice = None, ocurrency: str = None,
    inituid = None, otheruid = None, muid = None,
    init_username: str = "", other_username: str = "",
    timeout_days = None, status: str = "", terms: str = "",
) -> str:
    url        = f"🔗 hackforums.net/contracts.php?action=view&cid={cid}"
    terms_line = f"Terms: {terms}\n" if terms else ""

    ct = str(contract_type or "standard").lower()
    if ct == "middleman":
        type_label = "🛡 Middleman Contract"
    elif ct == "exchange":
        type_label = "🔄 Exchange Contract"
    else:
        type_label = "📝 Standard Contract"

    if ct == "exchange" and oproduct:
        deal_line  = f"{iproduct} ↔ {oproduct}\n"
        price_line = ""
    else:
        deal_line  = f"{iproduct}\n"
        price_line = f"Price: {iprice} {icurrency}\n"

    mm_line      = f"Middleman: UID {muid}\n" if muid else ""
    init_label   = init_username  or (f"UID {inituid}"  if inituid  else None)
    other_label  = other_username or (f"UID {otheruid}" if otheruid else None)
    parties_line = f"Parties: {init_label} ↔ {other_label}\n" if (init_label and other_label) else ""
    timeout_line = f"Expires: {timeout_days} day(s)\n" if timeout_days else ""

    body = f"{deal_line}{price_line}{mm_line}{parties_line}{timeout_line}{terms_line}"
    return f"📝 new {type_label.lower()} #{cid}\n{body}{url}"


def fmt_contract_status(
    tone: str, cid,
    prev_status: str, new_status: str,
    iproduct: str = "", iprice = None, icurrency: str = "bytes",
    init_username: str = "", other_username: str = "",
    inituid = None, my_uid: str = "",
    istatus = None, ostatus = None,
) -> str:
    url = f"🔗 hackforums.net/contracts.php?action=view&cid={cid}"

    STATUS_ICONS = {
        "disputed":          "⚠️",
        "complete":          "✅",
        "cancelled":         "❌",
        "active":            "✅",
        "awaiting approval": "⏳",
        "incomplete":        "❌",
    }
    icon = STATUS_ICONS.get(new_status, "📝")

    product_line = f"{iproduct} · {iprice} {icurrency}\n" if iproduct and iprice else (f"{iproduct}\n" if iproduct else "")
    init_label   = init_username  or (f"UID {inituid}" if inituid else "?")
    other_label  = other_username or "counterparty"
    parties_line = f"{init_label} ↔ {other_label}\n"

    am_initiator = str(my_uid) == str(inituid) if my_uid and inituid else None
    my_name      = init_label  if am_initiator else other_label
    their_name   = other_label if am_initiator else init_label
    my_status    = (istatus if am_initiator else ostatus) if am_initiator is not None else None
    their_status = (ostatus if am_initiator else istatus) if am_initiator is not None else None

    ready_line = ""
    if new_status == "awaiting approval" and my_status is not None and their_status is not None:
        i_ready    = str(my_status)    in ("1", "True", "true")
        they_ready = str(their_status) in ("1", "True", "true")
        if i_ready and not they_ready:
            ready_line = f"You've marked ready — waiting on {their_name}\n"
        elif they_ready and not i_ready:
            ready_line = f"{their_name} marked ready — waiting on you\n"
        elif i_ready and they_ready:
            ready_line = "Both parties marked ready\n"
    elif new_status == "active":
        ready_line = "Both parties approved — contract is live\n"

    body = f"{product_line}{parties_line}{ready_line}"
    return f"{icon} contract #{cid} — {new_status}\n{body}{url}"


def fmt_dispute_new(tone: str, cdid, cid, is_defendant: bool, notes: str) -> str:
    url        = f"🔗 hackforums.net/contracts.php?action=view&cid={cid}"
    role       = "defendant" if is_defendant else "claimant"
    notes_line = f'"{notes}"\n' if notes else ""
    warn       = " ⚠️ respond ASAP" if is_defendant else ""
    return (
        f"🚨 dispute opened — you're the {role}{warn}\n"
        f"contract #{cid}\n{notes_line}{url}"
    )


def fmt_dispute_update(tone: str, cdid, cid, status: str) -> str:
    url = f"🔗 hackforums.net/contracts.php?action=view&cid={cid}"
    return f"📋 dispute #{cdid} — {status}\n{url}"


def fmt_brating(tone: str, cid, fromid, amount: int, message: str, from_username: str = "") -> str:
    sign      = "👍" if amount > 0 else ("😐" if amount == 0 else "👎")
    plus      = "+" if amount > 0 else ""
    msg_line  = f'"{message}"\n' if message else ""
    url       = f"🔗 hackforums.net/contracts.php?action=view&cid={cid}"
    from_disp = from_username if from_username else f"UID {fromid}"
    return (
        f"{sign} b-rating on contract #{cid} from <b>{from_disp}</b>: {plus}{amount}\n"
        f"{msg_line}{url}"
    )


def fmt_bytes(tone: str, amount: int, reason: str, from_user: str = "", post_id: str = "", post_tid: str = "") -> str:
    reason_line = f"{reason}\n" if reason else ""
    if post_id and post_tid:
        base = f"🔗 hackforums.net/showthread.php?tid={post_tid}&pid={post_id}#pid{post_id}"
    elif post_id:
        base = f"🔗 hackforums.net/showthread.php?pid={post_id}#pid{post_id}"
    else:
        base = "🔗 hackforums.net/myps.php?action=history"
    if from_user:
        return f"💰 {amount:,} bytes from <b>{from_user}</b>\n{reason_line}{base}"
    return f"💰 {amount:,} bytes\n{reason_line}{base}"


_GAMBLING_KEYWORDS = {
    "slots winner",
    "blackjack winner",
    "sports wager winner",
    "crypto game coin sell",
    "flips winner",
}

def is_gambling_reason(reason: str) -> bool:
    return any(k in reason.lower() for k in _GAMBLING_KEYWORDS)


def fmt_bytes_bundle(tone: str, txs: list) -> str:
    """txs: list of (amount, reason, from_user) tuples — bundled gambling summary."""
    total      = sum(int(t[0]) for t in txs)
    count      = len(txs)
    base       = "🔗 hackforums.net/myps.php?action=history"
    win_str    = "win" if count == 1 else "wins"
    sorted_txs = sorted(txs, key=lambda t: int(t[0]), reverse=True)
    lines      = "\n".join(f"  <code>+{int(amt):<7,}</code> {reason}" for amt, reason, _ in sorted_txs)
    return f"🎰 <b>{count} gambling {win_str} — {total:,} bytes</b>\n{lines}\n{base}"


def fmt_buddy_thread(tone: str, username: str, subject: str, tid) -> str:
    url = f"hackforums.net/showthread.php?tid={tid}"
    return (
        f"👥 {username} posted a new thread\n"
        f"<i>{subject}</i>\n"
        f"🔗 {url}"
    )


def fmt_mention(tone: str, subject: str, tid, pid, snippet: str, mention_type: str = "mention") -> str:
    url      = f"hackforums.net/showthread.php?tid={tid}&pid={pid}#pid{pid}"
    is_quote = mention_type == "quote"
    icon     = "💬" if is_quote else "🔔"
    action   = "quoted you" if is_quote else "tagged you"
    return f"{icon} someone {action} in <i>{subject}</i>\n🔗 {url}\n\n{snippet}"


def fmt_buddy_post(tone: str, username: str, subject: str, tid, pid, snippet: str) -> str:
    url          = f"hackforums.net/showthread.php?tid={tid}&pid={pid}#pid{pid}" if pid else f"hackforums.net/showthread.php?tid={tid}"
    snippet_line = f"\n\n{snippet}" if snippet else ""
    return f"👥 {username} posted\n<i>{subject}</i>\n🔗 {url}{snippet_line}"


def fmt_popularity(tone: str, diff: int, current: int, uid, delta_7d: int = None) -> str:
    sign     = ("+" + str(diff)) if diff > 0 else str(diff)
    icon     = "📈" if diff > 0 else "📉"
    url      = f"🔗 hackforums.net/reputation.php?uid={uid}"
    week_str = ""
    if delta_7d is not None:
        w_sign   = ("+" + str(delta_7d)) if delta_7d >= 0 else str(delta_7d)
        week_str = f", {w_sign} this week"
    return f"{icon} popularity {sign} (total: {current:,}{week_str})\n{url}"


def fmt_contract_expiry(
    tone: str, cid, product: str, hours_left: int,
    iprice=None, icurrency: str = "bytes",
    init_username: str = "", other_username: str = "",
    my_uid: str = "", inituid=None,
) -> str:
    url          = f"🔗 hackforums.net/contracts.php?action=view&cid={cid}"
    time_str     = f"{hours_left}h" if hours_left > 0 else "under 1h"
    product      = product or "contract"
    price_line   = f"{iprice} {icurrency}  ·  " if iprice else ""
    am_initiator = str(my_uid) == str(inituid) if my_uid and inituid else None
    if init_username and other_username:
        if am_initiator is True:
            party_line = f"with {other_username}"
        elif am_initiator is False:
            party_line = f"with {init_username}"
        else:
            party_line = f"{init_username} ↔ {other_username}"
    else:
        party_line = ""
    detail      = f"{price_line}{party_line}".strip(" ·").strip()
    detail_line = f"\n{product}" + (f"  ·  {detail}" if detail else "")
    return f"⏳ contract #{cid} expires in {time_str}{detail_line}\n{url}"


def fmt_buddy_status(tone: str, username: str, uid, event: str) -> str:
    url = f"🔗 hackforums.net/member.php?action=profile&uid={uid}"
    if event == "exiled":
        return f"⚓ {username} was exiled — could be voluntary or not\n{url}"
    return f"☠️ {username} got banned\n{url}"


def fmt_fid_thread(tone: str, forum_name: str, fid, subject: str, tid, username: str, snippet: str = "") -> str:
    url          = f"hackforums.net/showthread.php?tid={tid}"
    by_line      = f"by {username}\n" if username else ""
    snippet_line = f"\n{snippet}" if snippet else ""
    return f"📋 new thread in {forum_name}\n<i>{subject}</i>\n{by_line}🔗 {url}{snippet_line}"


def fmt_warning_points(tone: str, prev: int, current: int) -> str:
    added = current - prev
    url   = "🔗 hackforums.net/usercp.php"
    return f"⚠️ <b>Warning points increased</b>\n{prev} → {current} (+{added})\n{url}"


def fmt_self_exile(tone: str) -> str:
    url = "🔗 hackforums.net/usercp.php"
    return f"😶 <b>Your HackForums account has been exiled</b>\nCheck your inbox for a reason.\n{url}"


def fmt_self_ban(tone: str) -> str:
    url = "🔗 hackforums.net/usercp.php"
    return f"☠️ <b>Your HackForums account has been banned</b>\nCheck your email for details.\n{url}"


def fmt_best_answer(tone: str, subject: str, tid, bestpid) -> str:
    url = f"🔗 hackforums.net/showthread.php?tid={tid}&pid={bestpid}#pid{bestpid}"
    return f"✅ <b>Best Answer marked</b> in your thread\n<i>{subject}</i>\n{url}"


def fmt_view_spike(tone: str, subject: str, tid, spike: int, total: int) -> str:
    url = f"🔗 hackforums.net/showthread.php?tid={tid}"
    return f"📈 <b>View spike!</b> +{spike:,} views\n<i>{subject}</i>\nTotal: {total:,}\n{url}"
