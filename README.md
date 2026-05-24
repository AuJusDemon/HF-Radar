# 📡 HF Radar

Telegram bot that monitors your [HackForums](https://hackforums.net) account and sends instant alerts for:

- Thread replies and mentions/quotes
- New contracts, status changes, and expiry warnings
- Incoming bytes transactions
- New private messages (count)
- B-ratings on contracts
- Contract disputes
- Buddy ban/exile status changes
- New threads in watched forums
- Warning points, account ban/exile

---

## How it works

HF Radar polls the HackForums API v2 using your OAuth token. Every cycle it makes
a small number of batched API calls and fires Telegram messages for anything new.

**Medium loop (~2 min):** Account events — PMs, bytes, contracts, rep, warning points.  
**Slow loop (~3 min):** Reply detection, b-ratings, disputes, buddy status, forum threads.

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
Set the redirect URI to any URL — HF Radar uses the `state` parameter flow, so the exact redirect doesn't matter as long as it matches what you registered.  
You'll get a `client_id` and `client_secret`.

### 2. Clone and install

```bash
git clone https://github.com/youruser/hf-radar.git
cd hf-radar
pip install -r requirements.txt
```

### 3. Configure via .env

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
TELEGRAM_TOKEN=your-bot-token
HF_CLIENT_ID=hf_clientid_...
HF_CLIENT_SECRET=hf_secret_...
HF_PROXY_URL=socks5://user:pass@your-residential-proxy:port
DB_PATH=hfradar.db
```

`.env` is in `.gitignore` — never commit it.

> **Legacy format:** `config.json` is still supported. Copy `config.example.json` → `config.json` if you prefer JSON. `.env` takes priority when `TELEGRAM_TOKEN` is set.

### 4. Run

```bash
python bot.py
```

The bot creates `hfradar.db` on first run and sets up all tables automatically.

**Debug mode** (auth flow only, no polling loops):

```bash
python bot.py --no-poll
```

**Test mode** (sends sample alerts to `TEST_CHAT_ID`):

```bash
python bot.py --test
```

---

## Running as a service (Linux)

Create `/etc/systemd/system/hfradar.service`:

```ini
[Unit]
Description=HF Radar Telegram Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/hf-radar
ExecStart=/usr/bin/python3 bot.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now hfradar
sudo journalctl -u hfradar -f
```

---

## Proxy notes

HackForums is behind Cloudflare, which blocks traffic from datacenters and VPS providers.
**You must use a residential proxy.** Any provider that offers residential SOCKS5 or HTTP
proxies will work — set `HF_PROXY_URL` in your `.env`.

If you're running multiple bot instances or want to centralize proxy management, you can
run a relay server instead — set `VPS_RELAY` and `PROXY_SECRET` and leave `HF_PROXY_URL` empty.

---

## Configuration reference

| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_TOKEN` | ✅ | Bot token from @BotFather |
| `HF_CLIENT_ID` | ✅ | HackForums OAuth client ID |
| `HF_CLIENT_SECRET` | ✅ | HackForums OAuth client secret |
| `HF_PROXY_URL` | ✅* | Residential proxy URL (`socks5://` or `http://`) |
| `VPS_RELAY` | ✅* | Relay server URL (alternative to direct proxy) |
| `PROXY_SECRET` | if relay | Shared secret for relay auth |
| `DB_PATH` | ❌ | SQLite file path (default: `hfradar.db`) |
| `TEST_CHAT_ID` | ❌ | Chat ID for `--test` mode |
| `STARTUP_DELAY_SECONDS` | ❌ | Delay startup N seconds (default: 0) |

*Either `HF_PROXY_URL` or `VPS_RELAY` is required — not both.

---

## Privacy

See [PRIVACY.md](PRIVACY.md) for exactly what data the bot stores and why.

---

## License

[MIT](LICENSE)
