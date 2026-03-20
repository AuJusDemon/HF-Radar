"""
Telegram helpers: TelegramBot shim + keyboard/text builders.

TelegramBot wraps PTB's telegram.Bot so all existing call sites
(handle_start, detectors, etc.) stay completely unchanged.
"""

import logging
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton

log = logging.getLogger("hfradar.telegram")


def _to_markup(reply_markup) -> InlineKeyboardMarkup | None:
    """Convert our raw dict format to a PTB InlineKeyboardMarkup."""
    if reply_markup is None:
        return None
    if isinstance(reply_markup, InlineKeyboardMarkup):
        return reply_markup
    rows = []
    for row in reply_markup.get("inline_keyboard", []):
        buttons = []
        for btn in row:
            if "url" in btn:
                buttons.append(InlineKeyboardButton(btn["text"], url=btn["url"]))
            else:
                buttons.append(InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"]))
        rows.append(buttons)
    return InlineKeyboardMarkup(rows)


class TelegramBot:
    """
    Thin async shim over PTB's Bot.
    Accepts either a token string (creates its own Bot) or an existing Bot object.
    """
    def __init__(self, token_or_bot):
        from telegram import Bot as _Bot
        if isinstance(token_or_bot, str):
            self._bot = _Bot(token_or_bot)
        else:
            self._bot = token_or_bot

    def set_bot(self, bot):
        """Replace internal bot with the live PTB application bot (shares connection pool)."""
        self._bot = bot

    async def send(self, chat_id: int, text: str, reply_markup: dict = None):
        log.info(f"TG >> sendMessage chat={chat_id} | {text[:60].strip()}")
        try:
            await self._bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_to_markup(reply_markup),
            )
        except Exception as e:
            log.warning(f"TG sendMessage failed chat={chat_id}: {e}")

    async def edit(self, chat_id: int, message_id: int, text: str, reply_markup: dict = None):
        log.info(f"TG >> editMessageText chat={chat_id} msg={message_id}")
        try:
            await self._bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_to_markup(reply_markup),
            )
        except Exception as e:
            log.warning(f"TG editMessageText failed chat={chat_id}: {e}")

    async def answer_callback(self, callback_query_id: str, text: str = ""):
        try:
            await self._bot.answer_callback_query(callback_query_id, text=text)
        except Exception as e:
            log.warning(f"TG answerCallbackQuery failed: {e}")


# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_NOTIFICATIONS = {
    "thread_replies":   True,
    "contracts":        True,
    "contract_expiry":  True,
    "bytes":            True,
    "unread_pms":       True,
    "mentions":         True,
    "popularity":       True,
    "bratings":         True,
    "fid_threads":      True,
    "buddy_status":     True,
}

NOTIF_INFO = {
    "thread_replies":  ("Replies",        "new posts in threads you started"),
    "mentions":        ("Mentions",        "someone tagged or quoted you"),
    "contracts":       ("Contracts",       "someone opened a contract with you"),
    "contract_expiry": ("Contract Expiry", "alert 24h before a contract times out"),
    "bytes":           ("Bytes In",        "bytes land in your account"),
    "unread_pms":      ("PMs",             "your unread PM count goes up"),
    "popularity":      ("Popularity",      "someone rates your popularity"),
    "bratings":        ("B-Ratings",       "someone leaves a b-rating on your contracts"),
    "fid_threads":     ("Forum Threads",   "new thread posted in a watched forum"),
    "buddy_status":    ("Buddy Status",    "a buddy gets banned or exiled"),
}


# ── Keyboard builders ──────────────────────────────────────────────────────────

MAX_FORUMS = 3

def radar_keyboard(user: dict, cfg: dict = None) -> dict:
    notifs       = user.get("notifications") or DEFAULT_NOTIFICATIONS
    paused       = user.get("paused", 0)
    buddies      = user.get("buddy_list") or []
    forums       = user.get("tracked_fids") or []
    muted_count  = len(user.get("muted_tids") or [])

    # Count how many alert types are currently off
    off_count    = sum(1 for k in DEFAULT_NOTIFICATIONS if not notifs.get(k, True))
    alert_label  = f"🔔 Alerts ({off_count} off)" if off_count else "🔔 Alerts"

    buttons = [
        [{
            "text": alert_label,
            "callback_data": "open_alerts"
        }],
        [
            {"text": f"👥 Buddies ({len(buddies)}/5)",       "callback_data": "open_buddies"},
            {"text": f"📋 Forums ({len(forums)}/{MAX_FORUMS})", "callback_data": "open_forums"},
        ],
        [
            {"text": f"🔕 Muted ({muted_count})", "callback_data": "open_muted"},
        ],
    ]

    pause_btn = (
        {"text": "▶️ Resume", "callback_data": "resume"}
        if paused else
        {"text": "⏸ Pause",  "callback_data": "pause"}
    )
    buttons.append([pause_btn])

    return {"inline_keyboard": buttons}


def alerts_text(user: dict) -> str:
    """Header text for the alerts panel — shows every type with on/off state and description."""
    notifs = dict(DEFAULT_NOTIFICATIONS)
    notifs.update(user.get("notifications") or {})
    lines = ["🔔 <b>Alert Settings</b>\n"]
    for key, (label, desc) in NOTIF_INFO.items():
        on   = notifs.get(key, True)
        icon = "✅" if on else "❌"
        lines.append(f"{icon} <b>{label}</b>  <i>{desc}</i>")
    lines.append("\nTap a button below to toggle.")
    return "\n".join(lines)


def alerts_keyboard(user: dict) -> dict:
    """Toggle buttons for every alert type — 2 per row."""
    notifs = dict(DEFAULT_NOTIFICATIONS)
    notifs.update(user.get("notifications") or {})

    items   = list(NOTIF_INFO.keys())
    buttons = []
    for i in range(0, len(items), 2):
        row = []
        for key in items[i:i+2]:
            label = NOTIF_INFO[key][0]
            on    = notifs.get(key, True)
            icon  = "✅" if on else "❌"
            row.append({"text": f"{icon} {label}", "callback_data": f"toggle_{key}"})
        buttons.append(row)

    buttons.append([{"text": "← Back", "callback_data": "back_to_radar"}])
    return {"inline_keyboard": buttons}


def forums_keyboard(user: dict) -> tuple:
    """Returns (text, keyboard) for the forums drill-in screen."""
    forums  = user.get("tracked_fids") or []
    buttons = []
    for fw in forums:
        fid   = fw.get("fid")
        fname = fw.get("name", f"Forum #{fid}")
        buttons.append([{"text": f"🗑 {fname}", "callback_data": f"remove_forum_{fid}"}])
    if len(forums) < MAX_FORUMS:
        buttons.append([{"text": "➕ Add Forum", "callback_data": "add_forum"}])
    buttons.append([{"text": "← Back", "callback_data": "back_to_radar"}])
    count = len(forums)
    text  = f"📋 <b>Watched Forums</b> ({count}/{MAX_FORUMS})\n\nGet alerted when a new thread is posted in any of these.\nTap a forum to remove it."
    return text, {"inline_keyboard": buttons}

def radar_text(user: dict) -> str:
    hf_uid   = user.get("hf_uid") or "?"
    username = user.get("hf_username") or f"UID {hf_uid}"
    paused   = user.get("paused", 0)
    buddies  = user.get("buddy_list") or []
    notifs   = user.get("notifications") or DEFAULT_NOTIFICATIONS

    status    = "⏸ Paused" if paused else "🟢 Active"

    off_keys   = [key for key in DEFAULT_NOTIFICATIONS if not notifs.get(key, True)]
    if off_keys:
        off_labels = [NOTIF_INFO[k][0] for k in off_keys if k in NOTIF_INFO]
        notif_line = f"muted: {', '.join(off_labels).lower()}"
    else:
        notif_line = "all on"

    lines = []
    bal = user.get("last_bytes_balance")
    if bal is not None:
        bal_f   = float(bal)
        bal_str = f"{bal_f:,.1f}" if bal_f != int(bal_f) else f"{int(bal_f):,}"
        lines.append(f"💰 {bal_str} bytes")
    rep = user.get("last_reputation")
    if rep:
        lines.append(f"⭐ {rep:,} popularity")
    pms = user.get("last_unread_pms")
    if pms and int(pms) > 0:
        lines.append(f"📨 {pms} unread PM{'s' if int(pms) != 1 else ''}")

    stats_line = "   ".join(lines) if lines else ""
    buddy_line = ""
    if buddies:
        names      = ", ".join(b.get("username", f"UID {b.get('uid')}") for b in buddies)
        buddy_line = f"\n👥 {names}"

    forums      = user.get("tracked_fids") or []
    forum_line  = ""
    if forums:
        fnames     = ", ".join(fw.get("name", f"FID {fw.get('fid')}") for fw in forums)
        forum_line = f"\n📋 {fnames}"

    text = (
        f"📡 <b>HF Radar</b>  {username}\n"
        f"{status}  ·  {notif_line}"
    )
    if stats_line:
        text += f"\n{stats_line}"
    text += buddy_line
    text += forum_line
    return text