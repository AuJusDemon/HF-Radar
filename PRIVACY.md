# What HF Radar reads from your account

HF Radar uses the HackForums OAuth2 API. When you authorize it, you grant it
read access to your account. This document lists exactly what is polled and
when. Every line here corresponds directly to code in `detectors.py`.

---

## Every ~2 minutes (medium loop)

One batched API call containing:

| Endpoint | Fields read | Why |
|---|---|---|
| `me` | uid, username, unread PMs, reputation, byte balance, post count, thread count, warning points, usergroup | Detect PM count changes, rep changes, byte arrivals, warning point increases, account status changes (ban/exile) |
| `contracts` | All contracts where you're a party, including embedded dispute data | Detect new contracts, status changes, expiry warnings |
| `bytes` | Last 10 incoming transactions | Detect bytes received |

---

## Every ~15–30 minutes (slow loop)

| Endpoint | Fields read | Why |
|---|---|---|
| `posts` (your UID, page 1) | pid, tid, dateline | Discover threads you've participated in — used to build the known-thread list for reply detection |
| `threads` (known TIDs) | lastpost, lastposteruid, numreplies, views | Detect new replies in threads you've posted in |
| `bratings` (your UID) | crid, contractid, fromid, amount, message | Detect new b-ratings on your contracts |
| `disputes` (your UID) | cdid, contractid, claimant/defendant, status, notes | Catch disputes on older contracts (safety net — active ones are covered inline via `contracts`) |
| `users` (buddy UIDs only) | uid, usergroup | Detect if a buddy gets banned or exiled |
| `threads` (watched forum FIDs) | tid, subject, uid, dateline | Detect new threads in forums you've chosen to watch |

---

## What we do NOT do

- **Never read PM content.** We detect the unread count changing, not what's in your inbox.
- **Never write anything.** No posts, threads, bytes, or contracts are ever created on your behalf.
- **Never store post content.** Message text is used briefly to extract a snippet for your alert, then discarded. Only the post ID is stored (for deduplication).
- **Never read threads you're not already in** (except watched forums you explicitly added).
- **No third-party data.** We only read your account. Buddy usergroup checks only fetch the `usergroup` field, nothing else.

---

## What is stored in the database

Per user:

- HackForums UID and username
- OAuth access token (used to make API calls on your behalf)
- Notification preferences
- List of known thread IDs (for reply polling)
- List of buddies (UIDs + usernames you added)
- List of watched forum IDs
- Last known byte balance, reputation, unread PM count
- Muted thread IDs
- Contract/dispute state (to detect changes)
- Seen-event IDs (for deduplication — prevents duplicate alerts)

Nothing else. Source: `db.py`, `users` table schema.
