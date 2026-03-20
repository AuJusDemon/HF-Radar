"""
hf_client.py — HF API transport layer.

Two supported modes (set in config.json):

  1. Direct + proxy (recommended for self-hosters)
     Set `hf_proxy_url` to a residential proxy URL.
     Requests go: Bot → ResidentialProxy → HackForums
     HackForums uses Cloudflare which blocks datacenter IPs.
     Example: "socks5://user:pass@proxy.example.com:1080"

  2. VPS relay (optional — for multi-instance setups)
     Set `vps_relay` + `proxy_secret` in config.
     Requests go: Bot → YourRelay → ResidentialProxy → HackForums
     Relay must implement /read, /write, /token endpoints
     and forward x-rate-limit-remaining back in the response header.

Rate limit tracking: tracked per-token, exposed via is_rate_limited() /
get_rate_limit_remaining(). HF's limit is ~240 calls/hour per token.
"""
import asyncio
import logging

import aiohttp

log = logging.getLogger("hfradar.api")

HF_API_BASE  = "https://hackforums.net/api/v2"

# ── Runtime configuration ──────────────────────────────────────────────────────
# Call configure() from bot.py at startup before any HFClient is created.
# Alternatively, set these directly if you're not using bot.py.
_VPS_RELAY    = ""      # e.g. "http://your-relay-host:5765"  — leave blank for direct
_PROXY_SECRET = ""      # shared secret for relay auth
_HF_PROXY_URL = ""      # e.g. "socks5://user:pass@host:port" — residential proxy for direct mode


def configure(cfg: dict):
    """
    Called once at startup with the loaded config dict.
    Pulls relay/proxy settings so all HFClient instances use them.
    """
    global _VPS_RELAY, _PROXY_SECRET, _HF_PROXY_URL
    _VPS_RELAY    = cfg.get("vps_relay")    or ""
    _PROXY_SECRET = cfg.get("proxy_secret") or ""
    _HF_PROXY_URL = cfg.get("hf_proxy_url") or ""


# ── Auth error ─────────────────────────────────────────────────────────────────

class AuthExpired(Exception):
    """Raised when HF returns 401 — token is dead."""
    pass


# ── Rate limit tracking ────────────────────────────────────────────────────────
_rate_limits: dict[str, int] = {}  # token → remaining calls


def is_rate_limited(token: str) -> bool:
    """True if token has < 20 calls remaining."""
    return _rate_limits.get(token, 9999) < 20


def get_rate_limit_remaining(token: str) -> int:
    return _rate_limits.get(token, 9999)


def _update_rate_limit(token: str, remaining: int) -> None:
    _rate_limits[token] = remaining


# ── Shared aiohttp session ─────────────────────────────────────────────────────
_sessions: dict[int, aiohttp.ClientSession] = {}


def _get_session() -> aiohttp.ClientSession:
    loop_id = id(asyncio.get_running_loop())
    if loop_id not in _sessions or _sessions[loop_id].closed:
        _sessions[loop_id] = aiohttp.ClientSession()
    return _sessions[loop_id]


# ── Core request helper ────────────────────────────────────────────────────────

async def _request(token: str, route: str, body: dict, retry: bool = True) -> dict | None:
    """
    POST to either the VPS relay or directly to the HF API.
    Re-raises AuthExpired. Returns None on transient failures.
    Retries once on 503 after a 3s sleep.
    """
    if _VPS_RELAY:
        url = f"{_VPS_RELAY}/{route}"
        headers = {
            "Authorization":  f"Bearer {token}",
            "X-Proxy-Secret": _PROXY_SECRET,
        }
        proxy = None
    else:
        # Direct mode — call HF API with residential proxy
        url     = f"{HF_API_BASE}/{route}"
        headers = {"Authorization": f"Bearer {token}"}
        proxy   = _HF_PROXY_URL or None

    timeout = aiohttp.ClientTimeout(total=30)

    try:
        session = _get_session()
        async with session.post(url, json=body, headers=headers,
                                timeout=timeout, proxy=proxy) as resp:
            rl = resp.headers.get("X-Rate-Limit-Remaining")
            if rl and rl.isdigit():
                _update_rate_limit(token, int(rl))

            if resp.status == 200:
                return await resp.json()
            if resp.status == 401:
                raise AuthExpired()
            if resp.status == 403:
                log.warning("HF 403 — proxy may be blocked")
                return None
            if resp.status == 503:
                if retry:
                    log.warning(f"503 on /{route} — retrying in 3s")
                    await asyncio.sleep(3)
                    return await _request(token, route, body, retry=False)
                log.warning(f"503 on /{route} — skipping cycle")
                return None

            log.warning(f"/{route} returned HTTP {resp.status}")
            return None

    except AuthExpired:
        raise
    except Exception as e:
        log.error(f"/{route} error: {e}")
        return None


# ── HFClient ───────────────────────────────────────────────────────────────────

class HFClient:
    """
    Async HF API client. All call sites use HFClient(token).
    Transport (relay vs direct) is controlled by configure() at startup.
    """
    def __init__(self, token: str, **kwargs):
        self.token = token

    async def read(self, asks: dict) -> dict | None:
        return await _request(self.token, "read", asks)

    async def write(self, asks: dict) -> dict | None:
        return await _request(self.token, "write", asks)

    async def ping(self) -> bool:
        try:
            result = await self.read({"me": {"uid": True}})
            return result is not None and "me" in result
        except AuthExpired:
            return False
        except Exception:
            return False


# ── OAuth token exchange ───────────────────────────────────────────────────────

async def exchange_code_for_token(code: str, cfg: dict):
    """
    Exchange an OAuth auth code for an access token.
    Returns (access_token, expires_in, refresh_token) or (None, None, None).
    """
    try:
        session = _get_session()

        if _VPS_RELAY:
            # Route through relay if configured
            async with session.post(
                f"{_VPS_RELAY}/token",
                data={
                    "grant_type":    "authorization_code",
                    "code":          code,
                    "client_id":     cfg["hf_client_id"],
                    "client_secret": cfg.get("hf_client_secret") or cfg.get("hf_secret"),
                },
                headers={"X-Proxy-Secret": _PROXY_SECRET},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("access_token"), data.get("expires_in"), data.get("refresh_token")
                log.error(f"Token exchange failed via relay: HTTP {resp.status}")
                return None, None, None
        else:
            # Direct to HF token endpoint
            async with session.post(
                f"{HF_API_BASE}/token",
                data={
                    "grant_type":    "authorization_code",
                    "code":          code,
                    "client_id":     cfg["hf_client_id"],
                    "client_secret": cfg.get("hf_client_secret") or cfg.get("hf_secret"),
                },
                proxy=_HF_PROXY_URL or None,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("access_token"), data.get("expires_in"), data.get("refresh_token")
                log.error(f"Token exchange failed: HTTP {resp.status}")
                return None, None, None

    except Exception as e:
        log.error(f"Token exchange error: {e}")
        return None, None, None
