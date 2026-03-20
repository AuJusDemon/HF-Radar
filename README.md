# 📡 HF Radar

Telegram bot that monitors your [HackForums](https://hackforums.net) account and sends instant alerts for:

- Thread replies and mentions/quotes
- New contracts, status changes, and expiry warnings
- Incoming bytes
- New private messages (count)
- B-ratings on contracts
- Contract disputes
- Buddy ban/exile status
- New threads in watched forums
- Warning points, account ban/exile

---

## How it works

HF Radar polls the HackForums API v2 using your OAuth token. Every cycle it makes
a small number of batched API calls and fires Telegram messages for anything new.

**Medium loop (~2 min):** Account events — PMs, bytes, contracts, rep, warning points.  
**Slow loop (~15–30 min):** Reply detection, b-ratings, disputes, buddy status, forum threads.

The polling logic and every API call made is documented in [`detectors.py`](detectors.py)
and [`PRIVACY.md`](PRIVACY.md). Nothing is hidden.

---

## Requirements

- Python 3.11+
- A residential proxy (HackForums uses Cloudflare — datacenter IPs are blocked)
- A Telegram bot token ([@BotFather](https://t.me/BotFather))
- A HackForums API application (OAuth client ID + secret)

---

## Setup

### 1. Register a HackForums API app

Go to your HF account settings → API → create an application.  
Set the redirect URI to wherever your callback lands (or use the Telegram state flow — see `commands.py`).  
You'll get a `client_id` and `client_secret`.

### 2. Clone and install

```bash
git clone https://github.com/youruser/hf-radar.git
cd hf-radar
pip install -r requirements.txt
```

### 3. Configure

```bash
cp config.example.json config.json
```

Edit `config.json`:

```json
{
  "telegram_token": "your-telegram-bot-token",
  "hf_client_id": "hf_clientid_...",
  "hf_client_secret": "hf_secret_...",
  "hf_proxy_url": "socks5://user:pass@your-residential-proxy:port",
  "database": {
    "db_path": "hfradar.db"
  }
}
```

`config.json` is in `.gitignore`. Never commit it.

### 4. Add `configure()` call to bot.py

In `bot.py`, add this near the top of `main()` after `cfg = load_config()`:

```python
from hf_client import configure as configure_hf
configure_hf(cfg)
```

This wires the proxy/relay settings into the transport layer.

### 5. Database

No setup needed. SQLite creates `hfradar.db` in the working directory on first run.
To use a different path, set `"db_path"` in `config.json > "database"`.

### 6. Run

```bash
python bot.py
```

Test mode (sends sample alerts to `test_chat_id`, no polling):

```bash
python bot.py --test
```

No-poll mode (UI/commands only, loops disabled):

```bash
python bot.py --no-poll
```

---

## Transport options

### Direct (recommended for self-hosters)

Set `hf_proxy_url` in config. The bot calls the HF API directly through your residential proxy.

### Relay

If you want to run a relay to handle proxy rotation centrally (e.g. multiple bots sharing one proxy pool), set `vps_relay` and `proxy_secret` in config. The relay must implement `POST /read`, `POST /write`, `POST /token` and forward `X-Rate-Limit-Remaining` in responses.

---

## File overview

| File | What it does |
|---|---|
| `bot.py` | Entry point, PTB setup, polling loops |
| `detectors.py` | All HF API polling logic — every alert is fired from here |
| `commands.py` | Telegram command handlers and callback routing |
| `telegram_bot.py` | TelegramBot shim, keyboard builders, UI text |
| `alerts.py` | Alert message formatting |
| `hf_client.py` | HF API transport — handles auth, rate limiting, retries |
| `db.py` | SQLite schema + all DB operations |

---

## API call budget

Per user per hour with the default polling schedule:

- ~66 API calls/hour at steady state (active user with threads to poll)
- HF's rate limit is ~240 calls/hour per token
- Hot/cold thread split: active threads polled every cycle, stale threads every 30 min

See the budget breakdown comment at the top of `detectors.py` for full detail.

---

## Privacy

See [PRIVACY.md](PRIVACY.md) for a complete breakdown of what is polled and what is stored.

**Short version:** HF Radar reads your account data to send you alerts. It never writes anything, never reads PM content, and never stores post message text. All polling logic is in `detectors.py` — readable, auditable, no surprises.

---

## License

MIT
