# tempog_bot_v26.py
# Python 3.10+ | aiogram 3.x | aiohttp | aiosqlite
# pip install aiogram aiohttp aiosqlite

import asyncio
import logging
import random
import string
import re
import time
import aiosqlite
import aiohttp
from datetime import datetime
from email.utils import parsedate_to_datetime
from html import escape
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, BufferedInputFile, InlineKeyboardMarkup,
    InlineKeyboardButton, CallbackQuery
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

# ══════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════
import os
from dotenv import load_dotenv

load_dotenv()  # loads .env file when running locally

BOT_TOKEN  = os.environ["BOT_TOKEN"]          # crashes loudly if missing
ADMIN_ID   = int(os.environ["ADMIN_ID"])
DB_PATH    = os.environ.get("DB_PATH", "/data/tempog_v26.db")
MAX_EMAILS_PER_USER  = 5      # 0 = unlimited
VAULT_RETENTION_DAYS = 30
POLL_INTERVAL        = 30     # slower sweep; provider requests are paced globally below
API_MAX_RETRIES      = 3

# Regex: valid custom prefix (3–30 alphanumeric / dot / hyphen / underscore)
PREFIX_RE = re.compile(r'^[a-z0-9._\-]{3,30}$')
# Regex: looks like an email address
EMAIL_RE  = re.compile(r'^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("TempOGv26")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher(storage=MemoryStorage())

# ══════════════════════════════════════════
# FSM STATES
# ══════════════════════════════════════════
class AdminSearch(StatesGroup):
    waiting = State()

class AdminNote(StatesGroup):
    waiting = State()

# ══════════════════════════════════════════
# GLOBALS
# ══════════════════════════════════════════
GLOBAL_SESSION  = None
CACHED_DOMAINS  = []
# A semaphore limits in-flight work, but does not limit request rate.  mail.tm
# applies a shared per-IP quota, so hosted deployments must pace all requests.
API_SEMAPHORE   = asyncio.Semaphore(4)
POLL_SEMAPHORE  = asyncio.Semaphore(2)
API_RATE_LOCK   = asyncio.Lock()
API_NEXT_REQUEST_AT = {}
API_COOLDOWN_UNTIL = {}
API_RATE_LIMIT_STREAK = {}
API_MIN_INTERVAL = 1.0       # cap aggregate traffic at 1 request/sec per provider
ACCOUNT_CREATE_MIN_INTERVAL = 2.0  # be extra conservative for account creation
DEFAULT_RATE_LIMIT_COOLDOWN = 60.0
DOMAIN_REFRESH_LOCK = asyncio.Lock()
DOMAIN_LAST_FAILURE_UNTIL = 0.0

# ══════════════════════════════════════════
# HTTP SESSION
# ══════════════════════════════════════════
async def get_session() -> aiohttp.ClientSession:
    global GLOBAL_SESSION
    if GLOBAL_SESSION is None or GLOBAL_SESSION.closed:
        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        GLOBAL_SESSION = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
    return GLOBAL_SESSION

async def _pace_api(api_url: str, *, account_creation: bool = False, skip_during_cooldown: bool = False):
    """Pace request starts and make queued work observe provider-wide cooldowns."""
    interval = ACCOUNT_CREATE_MIN_INTERVAL if account_creation else API_MIN_INTERVAL
    while True:
        async with API_RATE_LOCK:
            now = time.monotonic()
            if skip_during_cooldown and API_COOLDOWN_UNTIL.get(api_url, 0.0) > now:
                return False
            allowed_at = max(
                API_NEXT_REQUEST_AT.get(api_url, now),
                API_COOLDOWN_UNTIL.get(api_url, now),
            )
            if now >= allowed_at:
                API_NEXT_REQUEST_AT[api_url] = now + interval
                return True
            wait_for = allowed_at - now
        await asyncio.sleep(wait_for)

def _retry_after(response: aiohttp.ClientResponse) -> float | None:
    value = response.headers.get("Retry-After")
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        if value:
            try:
                return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                pass
        return None

async def _apply_rate_limit(api_url: str, response: aiohttp.ClientResponse) -> int:
    """Pause provider calls, backing off longer when repeated cooldowns don't help."""
    retry_after = _retry_after(response)
    now = time.monotonic()
    async with API_RATE_LOCK:
        # Several requests may already be in flight when the first 429 arrives.
        # Count that as one throttling episode, not several consecutive episodes.
        if API_COOLDOWN_UNTIL.get(api_url, 0.0) <= now:
            API_RATE_LIMIT_STREAK[api_url] = API_RATE_LIMIT_STREAK.get(api_url, 0) + 1
        streak = API_RATE_LIMIT_STREAK.get(api_url, 1)
        # Repeating the same 60-second retry interval can create an endless 429 loop.
        # Increase the quiet period up to 15 minutes until a request succeeds.
        backoff = min(900.0, DEFAULT_RATE_LIMIT_COOLDOWN * (2 ** min(streak - 1, 4)))
        delay = max(1.0, retry_after if retry_after is not None else backoff)
        until = max(API_COOLDOWN_UNTIL.get(api_url, now), now + delay)
        API_COOLDOWN_UNTIL[api_url] = until
        API_NEXT_REQUEST_AT[api_url] = max(API_NEXT_REQUEST_AT.get(api_url, now), until)
    return max(1, int(delay + 0.999))

async def _clear_rate_limit(api_url: str):
    """Reset escalating backoff after the provider accepts a request."""
    async with API_RATE_LOCK:
        API_RATE_LIMIT_STREAK.pop(api_url, None)

def _rate_limit_remaining(api_url: str) -> int:
    return max(0, int(API_COOLDOWN_UNTIL.get(api_url, 0.0) - time.monotonic() + 0.999))

async def _backoff(attempt: int, retry_after: float | None = None):
    # Honor the provider's delay when present; otherwise use bounded exponential backoff.
    delay = retry_after if retry_after is not None else min(8.0, 1.0 * (2 ** attempt))
    await asyncio.sleep(delay + random.uniform(0.0, 0.25))

async def safe_send(chat_id: int, text: str, reply_markup=None) -> types.Message | None:
    try:
        return await bot.send_message(chat_id, text, reply_markup=reply_markup)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return await bot.send_message(chat_id, text, reply_markup=reply_markup)
    except TelegramForbiddenError:
        pass
    except Exception as e:
        logger.error(f"safe_send {chat_id}: {e}")
    return None

# ══════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA cache_size=-32000;")  # 32MB page cache
        await db.execute("PRAGMA temp_store=MEMORY;")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id  INTEGER PRIMARY KEY,
                username     TEXT,
                first_name   TEXT,
                last_name    TEXT,
                is_banned    INTEGER DEFAULT 0,
                note         TEXT    DEFAULT '',
                joined_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_seen    DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                email        TEXT PRIMARY KEY,
                telegram_id  INTEGER,
                password     TEXT,
                token        TEXT,
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS inbox (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id       TEXT UNIQUE,
                to_email     TEXT,
                from_email   TEXT,
                subject      TEXT,
                body         TEXT,
                html_body    TEXT,
                seen         INTEGER  DEFAULT 0,
                timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS vault_emails (
                email        TEXT PRIMARY KEY,
                telegram_id  INTEGER,
                password     TEXT,
                token        TEXT,
                deleted_by   TEXT    DEFAULT 'user',
                deleted_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS vault_inbox (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id       TEXT,
                to_email     TEXT,
                from_email   TEXT,
                subject      TEXT,
                body         TEXT,
                html_body    TEXT,
                original_ts  DATETIME,
                archived_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS admin_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                action       TEXT,
                target_id    INTEGER DEFAULT 0,
                detail       TEXT    DEFAULT '',
                timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes for speed
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_emails_tid     ON emails(telegram_id)",
            "CREATE INDEX IF NOT EXISTS idx_inbox_to       ON inbox(to_email)",
            "CREATE INDEX IF NOT EXISTS idx_inbox_ts       ON inbox(timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_vault_email    ON vault_emails(email)",
            "CREATE INDEX IF NOT EXISTS idx_vault_inbox_to ON vault_inbox(to_email)",
        ]:
            await db.execute(idx)

        # Migrations
        for col in [
            "ALTER TABLE users ADD COLUMN first_name TEXT",
            "ALTER TABLE users ADD COLUMN last_name  TEXT",
            "ALTER TABLE users ADD COLUMN note       TEXT DEFAULT ''",
            "ALTER TABLE users ADD COLUMN last_seen  DATETIME DEFAULT CURRENT_TIMESTAMP",
            "ALTER TABLE inbox ADD COLUMN seen        INTEGER DEFAULT 0",
            "ALTER TABLE inbox ADD COLUMN html_body   TEXT",
        ]:
            try:
                await db.execute(col)
            except aiosqlite.OperationalError:
                pass

        await db.commit()

async def upsert_user(user: types.User):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (telegram_id, username, first_name, last_name, last_seen)
            VALUES (?,?,?,?,CURRENT_TIMESTAMP)
            ON CONFLICT(telegram_id) DO UPDATE SET
                username   = excluded.username,
                first_name = excluded.first_name,
                last_name  = excluded.last_name,
                last_seen  = CURRENT_TIMESTAMP
        """, (user.id, user.username or "", user.first_name or "", user.last_name or ""))
        await db.commit()

async def is_user_banned(uid: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT is_banned FROM users WHERE telegram_id=?", (uid,)) as c:
            row = await c.fetchone()
            return bool(row and row[0])

async def log_admin(action: str, target: int = 0, detail: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO admin_log (action, target_id, detail) VALUES (?,?,?)",
            (action, target, detail)
        )
        await db.commit()

# ══════════════════════════════════════════
# GHOST VAULT
# ══════════════════════════════════════════
async def archive_to_vault(email: str, deleted_by: str = "user"):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT email, telegram_id, password, token FROM emails WHERE email=?", (email,)
        ) as c:
            row = await c.fetchone()
        if not row:
            return

        await db.execute("""
            INSERT INTO vault_emails (email, telegram_id, password, token, deleted_by)
            VALUES (?,?,?,?,?)
            ON CONFLICT(email) DO UPDATE SET
                deleted_at = CURRENT_TIMESTAMP,
                deleted_by = excluded.deleted_by
        """, row + (deleted_by,))

        async with db.execute(
            "SELECT msg_id, to_email, from_email, subject, body, html_body, timestamp FROM inbox WHERE to_email=?",
            (email,)
        ) as c:
            mails = await c.fetchall()

        for m in mails:
            await db.execute("""
                INSERT OR IGNORE INTO vault_inbox
                    (msg_id, to_email, from_email, subject, body, html_body, original_ts)
                VALUES (?,?,?,?,?,?,?)
            """, m)

        await db.commit()

async def restore_from_vault(email: str, user_id: int) -> tuple[bool, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        # Already active?
        async with db.execute("SELECT telegram_id FROM emails WHERE email=?", (email,)) as c:
            owner = await c.fetchone()
        if owner:
            if owner[0] == user_id:
                return False, "already_mine"
            return False, "taken"

        async with db.execute(
            "SELECT email, password, token FROM vault_emails WHERE email=?", (email,)
        ) as c:
            row = await c.fetchone()

        if not row:
            return False, "not_found"

        v_email, v_pass, v_token = row

        await db.execute("""
            INSERT INTO emails (email, telegram_id, password, token)
            VALUES (?,?,?,?)
            ON CONFLICT(email) DO UPDATE SET telegram_id = excluded.telegram_id
        """, (v_email, user_id, v_pass, v_token))

        async with db.execute(
            "SELECT msg_id, to_email, from_email, subject, body, html_body, original_ts FROM vault_inbox WHERE to_email=?",
            (v_email,)
        ) as c:
            vault_mails = await c.fetchall()

        for vm in vault_mails:
            await db.execute("""
                INSERT OR IGNORE INTO inbox
                    (msg_id, to_email, from_email, subject, body, html_body, timestamp)
                VALUES (?,?,?,?,?,?,?)
            """, vm)

        await db.commit()

    return True, v_email

async def purge_old_vault():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"DELETE FROM vault_inbox  WHERE archived_at < datetime('now','-{VAULT_RETENTION_DAYS} days')"
        )
        await db.execute(
            f"DELETE FROM vault_emails WHERE deleted_at  < datetime('now','-{VAULT_RETENTION_DAYS} days')"
        )
        await db.commit()

# ══════════════════════════════════════════
# OWNERSHIP CHECK HELPER
# ══════════════════════════════════════════
async def check_email_ownership(prefix: str, domains: list) -> tuple[str | None, int | None]:
    """
    Returns (email, owner_telegram_id) if the prefix@anydomain is already
    active in the emails table, else (None, None).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        for d in domains:
            candidate = f"{prefix}@{d['domain']}"
            async with db.execute(
                "SELECT email, telegram_id FROM emails WHERE email=?", (candidate,)
            ) as c:
                row = await c.fetchone()
            if row:
                return row[0], row[1]
    return None, None

# ══════════════════════════════════════════
# MOBILE HTML RENDERER
# ══════════════════════════════════════════
def render_html_mobile(raw_html: str, subject: str, from_addr: str, ts: str) -> str:
    s_subj = escape(subject)
    s_from = escape(from_addr)
    s_ts   = escape(ts)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=5,viewport-fit=cover">
<meta name="color-scheme" content="light dark">
<title>{s_subj}</title>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#f4f4f8;--surface:#fff;--border:#e2e2e8;
  --text:#1a1a2e;--muted:#6b6b80;--accent:#5b5fc7;--accent-bg:#ededfa;
  --r:14px;--sh:0 2px 12px rgba(0,0,0,.08);
  --font:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
}}
@media(prefers-color-scheme:dark){{
  :root{{
    --bg:#0f0f17;--surface:#1a1a28;--border:#2e2e42;
    --text:#e8e8f0;--muted:#9090a8;--accent:#7b7fe8;--accent-bg:#1e1e38;
    --sh:0 2px 12px rgba(0,0,0,.35);
  }}
}}
body{{background:var(--bg);color:var(--text);font-family:var(--font);
  font-size:15px;line-height:1.6;padding:12px;-webkit-text-size-adjust:100%}}
.hd{{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  box-shadow:var(--sh);padding:16px 18px;margin-bottom:12px}}
.badge{{display:inline-block;background:var(--accent-bg);color:var(--accent);
  font-size:11px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;
  padding:3px 9px;border-radius:20px;margin-bottom:10px}}
.subj{{font-size:17px;font-weight:700;line-height:1.3;word-break:break-word;margin-bottom:10px}}
.meta{{font-size:13px;color:var(--muted)}}
.meta span{{color:var(--text);font-weight:500}}
.body{{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
  box-shadow:var(--sh);padding:18px;overflow-x:auto;word-break:break-word;overflow-wrap:break-word}}
.body table{{width:100%!important;max-width:100%!important;table-layout:fixed!important;border-collapse:collapse}}
.body td,.body th{{word-break:break-word!important;overflow-wrap:break-word!important;padding:4px 6px}}
.body img{{max-width:100%!important;height:auto!important;display:block;border-radius:8px;margin:6px 0}}
.body a{{color:var(--accent);word-break:break-all}}
.body p{{margin-bottom:.75em}}
.body h1,.body h2,.body h3{{font-size:1.1em!important;margin:.8em 0 .4em}}
.body [style*="width:"]{{width:100%!important;max-width:100%!important}}
.body [style*="font-size: 0"]{{font-size:inherit!important}}
.body center{{text-align:left!important}}
.body [align]{{text-align:left!important}}
.body font{{font-size:inherit!important}}
.body pre,.body code{{background:var(--accent-bg);border-radius:6px;font-size:13px;
  padding:2px 5px;overflow-x:auto;white-space:pre-wrap;word-break:break-all}}
.ft{{text-align:center;color:var(--muted);font-size:11px;margin-top:16px;padding-bottom:8px}}
</style>
</head>
<body>
<div class="hd">
  <div class="badge">📩 New Mail</div>
  <div class="subj">{s_subj}</div>
  <div class="meta">From &nbsp;<span>{s_from}</span></div>
  <div class="meta" style="margin-top:4px">📅 &nbsp;{s_ts}</div>
</div>
<div class="body">{raw_html}</div>
<div class="ft">TempMail Bot · Mobile View</div>
</body>
</html>"""

# ══════════════════════════════════════════
# DOMAIN / API HELPERS
# ══════════════════════════════════════════
async def fetch_all_active_domains():
    global CACHED_DOMAINS, DOMAIN_LAST_FAILURE_UNTIL
    async with DOMAIN_REFRESH_LOCK:
        if time.monotonic() < DOMAIN_LAST_FAILURE_UNTIL:
            return CACHED_DOMAINS
        session = await get_session()
        try:
            await _pace_api("https://api.mail.tm")
            async with session.get("https://api.mail.tm/domains") as resp:
                if resp.status == 429:
                    delay = await _apply_rate_limit("https://api.mail.tm", resp)
                    DOMAIN_LAST_FAILURE_UNTIL = time.monotonic() + delay
                    logger.warning("Mail provider domain list rate-limited; cooling down for %s seconds", delay)
                    return CACHED_DOMAINS
                if resp.status == 200:
                    data = await resp.json()
                    members = data if isinstance(data, list) else data.get("hydra:member", [])
                    domains = [
                        {"domain": d["domain"], "api": "https://api.mail.tm"}
                        for d in members if d.get("isActive", True)
                    ]
                    if domains:
                        CACHED_DOMAINS = domains
                        DOMAIN_LAST_FAILURE_UNTIL = 0.0
                        return CACHED_DOMAINS
                    logger.warning("Mail provider returned no active domains")
                else:
                    logger.warning("Mail provider domain list returned HTTP %s", resp.status)
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logger.warning("Mail provider domain list failed (%s)", type(e).__name__)
        DOMAIN_LAST_FAILURE_UNTIL = time.monotonic() + DEFAULT_RATE_LIMIT_COOLDOWN
        return CACHED_DOMAINS

async def get_domains():
    if not CACHED_DOMAINS:
        await fetch_all_active_domains()
    return CACHED_DOMAINS

async def api_create_account(api_url, address, password):
    session = await get_session()
    for attempt in range(API_MAX_RETRIES + 1):
        async with API_SEMAPHORE:
            await _pace_api(api_url, account_creation=True)
            try:
                async with session.post(f"{api_url}/accounts", json={"address": address, "password": password}) as resp:
                    if resp.status == 201:
                        await _clear_rate_limit(api_url)
                        return True, "OK"
                    if resp.status == 422: return False, "taken"
                    if resp.status == 429:
                        delay = await _apply_rate_limit(api_url, resp)
                        logger.warning("Mail provider account request rate-limited; shared cooldown is %s seconds", delay)
                        return False, f"rate_limited:{delay}"
                    if 500 <= resp.status < 600:
                        logger.warning("Mail provider account request returned HTTP %s", resp.status)
                        if attempt < API_MAX_RETRIES:
                            await _backoff(attempt)
                            continue
                        return False, "provider_unavailable"
                    try:
                        err = await resp.json()
                        logger.warning("Mail provider account request returned HTTP %s", resp.status)
                        return False, err.get("detail", f"error_{resp.status}")
                    except (aiohttp.ContentTypeError, ValueError):
                        logger.warning("Mail provider account request returned HTTP %s", resp.status)
                        return False, f"error_{resp.status}"
            except asyncio.TimeoutError:
                logger.warning("Mail provider account request timed out; not replaying ambiguous POST")
                return False, "provider_unavailable"
            except aiohttp.ClientError as e:
                if attempt < API_MAX_RETRIES:
                    await _backoff(attempt)
                    continue
                logger.warning("Mail provider account request failed after retries (%s)", type(e).__name__)
                return False, "provider_unavailable"
    return False, "rate_limited"

async def api_get_token(api_url, address, password):
    session = await get_session()
    for attempt in range(API_MAX_RETRIES + 1):
        async with API_SEMAPHORE:
            if not await _pace_api(api_url, skip_during_cooldown=True):
                return None, f"rate_limited:{_rate_limit_remaining(api_url)}"
            try:
                async with session.post(f"{api_url}/token", json={"address": address, "password": password}) as resp:
                    if resp.status == 200:
                        await _clear_rate_limit(api_url)
                        token = (await resp.json()).get("token")
                        return (token, None) if token else (None, "provider_error")
                    if resp.status == 429:
                        delay = await _apply_rate_limit(api_url, resp)
                        logger.warning("Mail provider token request rate-limited; shared cooldown is %s seconds", delay)
                        return None, f"rate_limited:{delay}"
                    if 500 <= resp.status < 600:
                        logger.warning("Mail provider token request returned HTTP %s", resp.status)
                        if attempt < API_MAX_RETRIES:
                            await _backoff(attempt)
                            continue
                        return None, "provider_unavailable"
                    logger.warning("Mail provider token request returned HTTP %s", resp.status)
                    return None, "provider_error"
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                if attempt < API_MAX_RETRIES:
                    await _backoff(attempt)
                    continue
                logger.warning("Mail provider token request failed after retries (%s)", type(e).__name__)
                return None, "provider_unavailable"
    return None, "provider_unavailable"

async def api_fetch_messages(api_url, token):
    session = await get_session()
    for attempt in range(API_MAX_RETRIES + 1):
        async with API_SEMAPHORE:
            if not await _pace_api(api_url, skip_during_cooldown=True):
                return []
            try:
                async with session.get(f"{api_url}/messages", headers={"Authorization": f"Bearer {token}"}) as resp:
                    if resp.status == 200:
                        await _clear_rate_limit(api_url)
                        data = await resp.json()
                        return data if isinstance(data, list) else data.get("hydra:member", [])
                    if resp.status == 429:
                        delay = await _apply_rate_limit(api_url, resp)
                        logger.warning("Mail provider inbox request rate-limited; shared cooldown is %s seconds", delay)
                        return []
                    if 500 <= resp.status < 600:
                        if attempt < API_MAX_RETRIES:
                            await _backoff(attempt)
                            continue
            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < API_MAX_RETRIES:
                    await _backoff(attempt)
    return []

async def api_fetch_body(api_url, token, msg_id):
    session = await get_session()
    for attempt in range(API_MAX_RETRIES + 1):
        async with API_SEMAPHORE:
            if not await _pace_api(api_url, skip_during_cooldown=True):
                return None
            try:
                async with session.get(
                    f"{api_url}/messages/{msg_id}",
                    headers={"Authorization": f"Bearer {token}"}
                ) as resp:
                    if resp.status == 200:
                        await _clear_rate_limit(api_url)
                        return await resp.json()
                    if resp.status == 429:
                        delay = await _apply_rate_limit(api_url, resp)
                        logger.warning("Mail provider message request rate-limited; shared cooldown is %s seconds", delay)
                        return None
                    if 500 <= resp.status < 600:
                        if attempt < API_MAX_RETRIES:
                            await _backoff(attempt)
                            continue
            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < API_MAX_RETRIES:
                    await _backoff(attempt)
    return None

# ══════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════
def kb_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Generate Temp Mail", callback_data="menu_generate")],
        [
            InlineKeyboardButton(text="📥 Inbox",           callback_data="menu_inbox"),
            InlineKeyboardButton(text="📋 My Addresses",    callback_data="menu_id")
        ],
        [InlineKeyboardButton(text="ℹ️ Help",               callback_data="menu_help")]
    ])

def kb_inbox():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Refresh", callback_data="inbox_refresh"),
        InlineKeyboardButton(text="⬅️ Back",    callback_data="menu_back")
    ]])

def kb_addresses(emails):
    kb = [[InlineKeyboardButton(text=f"📧 {e}", callback_data=f"addr_{e}")] for e in emails]
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="menu_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_single_addr(email):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📥 View Mail",  callback_data=f"view_{email}"),
            InlineKeyboardButton(text="🔄 Transfer",   callback_data=f"trans_{email}")
        ],
        [InlineKeyboardButton(text="🗑️ Delete",        callback_data=f"del_{email}")],
        [InlineKeyboardButton(text="⬅️ Back",           callback_data="menu_id")]
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Live Stats",        callback_data="admin_stats")],
        [
            InlineKeyboardButton(text="👥 Users",          callback_data="admin_users_0"),
            InlineKeyboardButton(text="📧 Mail Log",       callback_data="admin_mails_0")
        ],
        [
            InlineKeyboardButton(text="🔍 Search",         callback_data="admin_search"),
            InlineKeyboardButton(text="📋 Audit Log",      callback_data="admin_audit_0")
        ],
        [
            InlineKeyboardButton(text="🗄️ Ghost Vault",    callback_data="admin_vault_0"),
            InlineKeyboardButton(text="🗑️ Purge Old Mail", callback_data="admin_purge_confirm")
        ],
        [InlineKeyboardButton(text="📢 Broadcast",         callback_data="admin_broadcast")]
    ])

def kb_admin_user(uid, banned):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Unban" if banned else "🚫 Ban", callback_data=f"adm_ban_{uid}"),
            InlineKeyboardButton(text="📧 Emails",    callback_data=f"adm_emails_{uid}_0")
        ],
        [
            InlineKeyboardButton(text="📩 Inbox",     callback_data=f"adm_inbox_{uid}_0"),
            InlineKeyboardButton(text="🔑 Credentials", callback_data=f"adm_creds_{uid}")
        ],
        [
            InlineKeyboardButton(text="📝 Note",      callback_data=f"adm_note_{uid}"),
            InlineKeyboardButton(text="🗑️ Nuke Emails", callback_data=f"adm_nuke_{uid}")
        ],
        [InlineKeyboardButton(text="⬅️ Back",          callback_data="admin_users_0")]
    ])

# ══════════════════════════════════════════
# CORE — GENERATE EMAIL
# ══════════════════════════════════════════
async def generate_email(user_id: int, prefix: str = None) -> tuple[bool, str]:
    """
    Returns (success, result).
    result is the new email address on success, or an error code string on failure:
      "banned" | "limit" | "taken:<email>" | "no_domains" | "exhausted"
    """
    if await is_user_banned(user_id):
        return False, "banned"

    if MAX_EMAILS_PER_USER > 0:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM emails WHERE telegram_id=?", (user_id,)
            ) as c:
                if (await c.fetchone())[0] >= MAX_EMAILS_PER_USER:
                    return False, "limit"

    domains = await get_domains()
    if not domains:
        cooldown = _rate_limit_remaining("https://api.mail.tm")
        if cooldown:
            return False, f"rate_limited:{cooldown}"
        return False, "no_domains"

    if prefix:
        prefix = prefix.strip().lower()
        # Validate prefix
        if not PREFIX_RE.match(prefix):
            return False, "invalid_prefix"

        # ── OWNERSHIP CHECK ──────────────────────────────────────
        owned_email, owner_id = await check_email_ownership(prefix, domains)
        if owned_email:
            if owner_id == user_id:
                return False, f"already_mine:{owned_email}"
            return False, f"taken:{owned_email}"
        # ─────────────────────────────────────────────────────────
    else:
        prefix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))

    cooldown = _rate_limit_remaining("https://api.mail.tm")
    if cooldown:
        return False, f"rate_limited:{cooldown}"

    for target in domains:
        address  = f"{prefix}@{target['domain']}"
        password = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

        ok, msg = await api_create_account(target['api'], address, password)
        if not ok:
            if msg.startswith("rate_limited") or msg == "provider_unavailable":
                return False, msg
            if msg == "taken":        continue
            continue

        token, token_error = await api_get_token(target['api'], address, password)
        if not token:
            # The provider has already created this mailbox. Keep it locally even if
            # token issuance is throttled, so retries don't create orphan accounts.
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO emails (email, telegram_id, password, token) VALUES (?,?,?,NULL)",
                    (address, user_id, password)
                )
                await db.commit()
            logger.warning("Mailbox created but token is pending for %s (%s)", address, token_error)
            return True, address

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO emails (email, telegram_id, password, token) VALUES (?,?,?,?)",
                (address, user_id, password, token)
            )
            await db.commit()
        return True, address

    return False, "exhausted"

def format_gen_error(code: str) -> str:
    if code == "banned":          return "🚫 You are banned."
    if code == "limit":           return f"❌ Limit reached ({MAX_EMAILS_PER_USER} max). Delete one first."
    if code == "no_domains":      return "❌ No mail domains available right now."
    if code.startswith("rate_limited"):
        try:
            wait_seconds = max(1, int(code.split(":", 1)[1]))
            return f"⚠️ Mail provider rate limit reached. Please wait about {wait_seconds} seconds, then try again."
        except (IndexError, ValueError):
            return "⚠️ Mail provider rate limit reached. Please wait a little, then try again."
    if code == "provider_unavailable": return "⚠️ Mail provider is temporarily unavailable. Please try again shortly."
    if code == "exhausted":       return "❌ All domains exhausted. Try a different name."
    if code == "invalid_prefix":  return "❌ Name must be 3–30 chars: letters, numbers, dots, hyphens, underscores only."
    if code.startswith("already_mine:"):
        email = code.split(":", 1)[1]
        return f"📬 You already own <code>{escape(email)}</code>!"
    if code.startswith("taken:"):
        email = code.split(":", 1)[1]
        return f"🚫 <b>Address Taken</b>\n\n<code>{escape(email)}</code> is already registered to another user.\n\nTry a different name."
    return f"❌ {code}"

# ══════════════════════════════════════════
# USER — COMMAND HANDLERS
# ══════════════════════════════════════════
@dp.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    if await is_user_banned(user.id):
        return await message.answer("🚫 <b>You are banned.</b>")
    await upsert_user(user)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM emails WHERE telegram_id=?", (user.id,)
        ) as c:
            count = (await c.fetchone())[0]

    if count > 0:
        await _show_addresses(user.id, message)
    else:
        await message.answer(
            "👋 <b>Welcome to TempMail Bot!</b>\n\n"
            "Get instant disposable email addresses — no sign-up.\n"
            "All mail is delivered here directly.\n\n"
            "💡 <b>Tip:</b> Just type any name to create a custom address instantly!\n\n"
            "<i>Use the buttons below to get started.</i>",
            reply_markup=kb_main()
        )

@dp.message(Command("set"))
async def cmd_set(message: Message):
    if await is_user_banned(message.from_user.id): return
    await upsert_user(message.from_user)
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("Usage: <code>/set yourname</code>")
    msg = await message.answer("⏳ Creating custom address...")
    ok, result = await generate_email(message.from_user.id, prefix=args[1])
    if ok:
        await msg.edit_text(
            f"✅ <b>Created!</b>\n\n<code>{result}</code>",
            reply_markup=kb_inbox()
        )
    else:
        await msg.edit_text(format_gen_error(result), reply_markup=kb_main())

@dp.message(Command("transfer"))
async def cmd_transfer(message: Message):
    if await is_user_banned(message.from_user.id): return
    args = message.text.split()
    if len(args) != 3:
        return await message.answer("Usage: <code>/transfer email@domain.com @username</code>")

    email_addr      = args[1].strip().lower()
    target_username = args[2].replace("@", "").lower()
    sender_id       = message.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT email FROM emails WHERE telegram_id=? AND email=?", (sender_id, email_addr)
        ) as c:
            if not await c.fetchone():
                return await message.answer("❌ You don't own this email.")
        async with db.execute(
            "SELECT telegram_id FROM users WHERE username=?", (target_username,)
        ) as c:
            row = await c.fetchone()
            if not row:
                return await message.answer(f"❌ @{target_username} not found.")
            target_id = row[0]
        await db.execute(
            "UPDATE emails SET telegram_id=? WHERE email=?", (target_id, email_addr)
        )
        await db.commit()

    await message.answer(f"✅ Transferred <code>{email_addr}</code> to @{target_username}.")
    await safe_send(target_id, f"🎁 You received <code>{email_addr}</code>!", reply_markup=kb_main())

# ══════════════════════════════════════════
# SMART PLAIN-TEXT HANDLER
# Handles three cases in one place:
#   1. Looks like an email  → vault restore attempt
#   2. Looks like a name    → auto-generate with that prefix
#   3. Everything else      → silently ignored
# ══════════════════════════════════════════
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_plain_text(message: Message):
    user = message.from_user
    if await is_user_banned(user.id): return
    await upsert_user(user)

    text = message.text.strip().lower()

    # ── CASE 1: Looks like an email → vault restore ──────────────
    if EMAIL_RE.match(text):
        # Check if it's already active under this user
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT email FROM emails WHERE email=? AND telegram_id=?",
                (text, user.id)
            ) as c:
                already_mine = await c.fetchone()

        if already_mine:
            return await _show_inbox(user.id, await message.answer("📥 Loading..."), target_email=text)

        ok, result = await restore_from_vault(text, user.id)

        if ok:
            return await message.answer(
                f"✅ <b>Address Restored!</b>\n\n"
                f"<code>{result}</code>\n\n"
                f"<i>Your mail history has been recovered.</i>",
                reply_markup=kb_inbox()
            )
        if result == "taken":
            return await message.answer(
                f"🚫 <b>Address Taken</b>\n\n"
                f"<code>{escape(text)}</code> is currently registered to another user."
            )
        if result == "already_mine":
            return await _show_inbox(user.id, await message.answer("📥 Loading..."), target_email=text)
        # "not_found" → silent, don't reveal vault mechanics

        return  # fall through silently if not in vault

    # ── CASE 2: Looks like a valid custom name → auto-generate ───
    candidate = text.replace(" ", "").lower()
    if PREFIX_RE.match(candidate):
        msg = await message.answer(f"⚡ <b>Auto-generating</b> <code>{escape(candidate)}@…</code>…")
        ok, result = await generate_email(user.id, prefix=candidate)

        if ok:
            await msg.edit_text(
                f"✅ <b>Auto-created!</b>\n\n"
                f"<code>{result}</code>\n\n"
                f"<i>Listening for incoming mail.</i>",
                reply_markup=kb_inbox()
            )
        else:
            await msg.edit_text(format_gen_error(result), reply_markup=kb_main())

        return

    # ── CASE 3: Anything else → ignore silently ──────────────────

# ══════════════════════════════════════════
# USER — CALLBACK HANDLERS
# ══════════════════════════════════════════
@dp.callback_query(F.data.startswith("menu_"))
async def handle_menu(call: CallbackQuery):
    if await is_user_banned(call.from_user.id):
        return await call.answer("🚫 Banned.", show_alert=True)
    await upsert_user(call.from_user)
    action = call.data.split("_", 1)[1]

    if action == "back":
        await call.answer()
        await call.message.edit_text("👋 <b>TempMail Bot</b>", reply_markup=kb_main())

    elif action == "help":
        await call.answer()
        await call.message.edit_text(
            "📖 <b>How to use</b>\n\n"
            "• <b>Type any name</b> to auto-create that address instantly\n"
            "• <b>Paste an email</b> you owned before to recover it\n"
            "• /set <i>name</i> — same as typing a name\n"
            "• /transfer <i>email</i> <i>@user</i> — hand off an address\n\n"
            f"Max per account: <b>{MAX_EMAILS_PER_USER or '∞'}</b> addresses",
            reply_markup=kb_main()
        )

    elif action == "generate":
        await call.answer()
        msg = await call.message.edit_text("⏳ Generating random address…")
        ok, result = await generate_email(call.from_user.id)
        if ok:
            await msg.edit_text(
                f"✅ <b>Temp Mail ready!</b>\n\n<code>{result}</code>\n\n"
                f"<i>Listening for incoming mail.</i>",
                reply_markup=kb_inbox()
            )
        else:
            await msg.edit_text(format_gen_error(result), reply_markup=kb_main())

    elif action == "inbox":
        await call.answer()
        await _show_inbox(call.from_user.id, call.message)

    elif action == "id":
        await call.answer()
        await _show_addresses(call.from_user.id, call.message)

@dp.callback_query(F.data == "inbox_refresh")
async def cb_refresh(call: CallbackQuery):
    if await is_user_banned(call.from_user.id): return
    await call.answer("Refreshing…")
    await _show_inbox(call.from_user.id, call.message)

@dp.callback_query(F.data.startswith("addr_"))
async def cb_single_addr(call: CallbackQuery):
    if await is_user_banned(call.from_user.id): return
    email = call.data.split("_", 1)[1]
    await call.answer()
    await call.message.edit_text(
        f"📧 <b>Managing:</b> <code>{email}</code>",
        reply_markup=kb_single_addr(email)
    )

@dp.callback_query(F.data.startswith("del_"))
async def cb_delete(call: CallbackQuery):
    if await is_user_banned(call.from_user.id): return
    email = call.data.split("_", 1)[1]
    await archive_to_vault(email, deleted_by="user")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM emails WHERE email=? AND telegram_id=?", (email, call.from_user.id)
        )
        await db.commit()
    await call.answer("🗑️ Deleted.", show_alert=True)
    await _show_addresses(call.from_user.id, call.message)

@dp.callback_query(F.data.startswith("trans_"))
async def cb_transfer_btn(call: CallbackQuery):
    if await is_user_banned(call.from_user.id): return
    email = call.data.split("_", 1)[1]
    await call.answer()
    await call.message.edit_text(
        f"🔄 <b>Transfer</b> <code>{email}</code>\n\n"
        f"<code>/transfer {email} @username</code>",
        reply_markup=kb_single_addr(email)
    )

@dp.callback_query(F.data.startswith("view_"))
async def cb_view(call: CallbackQuery):
    if await is_user_banned(call.from_user.id): return
    email = call.data.split("_", 1)[1]
    await call.answer()
    await _show_inbox(call.from_user.id, call.message, target_email=email)

# ══════════════════════════════════════════
# DISPLAY HELPERS
# ══════════════════════════════════════════
async def _show_addresses(user_id: int, msg_or_call):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT email FROM emails WHERE telegram_id=? ORDER BY created_at DESC", (user_id,)
        ) as c:
            emails = [r[0] for r in await c.fetchall()]

    if not emails:
        text = "❌ No addresses. Generate one or type a name!"
        if isinstance(msg_or_call, CallbackQuery):
            return await msg_or_call.message.edit_text(text, reply_markup=kb_main())
        return await msg_or_call.answer(text, reply_markup=kb_main())

    text = f"📋 <b>Your Addresses</b> ({len(emails)}/{MAX_EMAILS_PER_USER or '∞'})"
    if isinstance(msg_or_call, CallbackQuery):
        await msg_or_call.message.edit_text(text, reply_markup=kb_addresses(emails))
    else:
        await msg_or_call.answer(text, reply_markup=kb_addresses(emails))

async def _show_inbox(user_id: int, message_to_edit, target_email: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        if target_email:
            emails = [target_email]
        else:
            async with db.execute(
                "SELECT email FROM emails WHERE telegram_id=?", (user_id,)
            ) as c:
                emails = [r[0] for r in await c.fetchall()]

        if not emails:
            return await message_to_edit.edit_text("❌ No addresses.", reply_markup=kb_main())

        ph = ','.join('?' * len(emails))
        async with db.execute(
            f"SELECT id, from_email, subject, body, html_body, timestamp "
            f"FROM inbox WHERE to_email IN ({ph}) ORDER BY timestamp DESC LIMIT 10",
            emails
        ) as c:
            mails = await c.fetchall()

    if not mails:
        return await message_to_edit.edit_text("📭 Inbox is empty.", reply_markup=kb_inbox())

    for mail_id, sender, subj, body, html_body, ts in mails:
        await safe_send(
            user_id,
            f"📩 <b>From:</b> {escape(sender)}\n"
            f"<b>Subject:</b> {escape(subj)}\n"
            f"<b>Time:</b> {ts}\n\n"
            f"{escape(body[:3000])}"
        )
        if html_body and isinstance(html_body, str) and html_body.strip():
            try:
                html_out = render_html_mobile(html_body, subj, sender, ts)
                await bot.send_document(
                    user_id,
                    document=BufferedInputFile(html_out.encode("utf-8"), filename=f"mail_{mail_id}.html"),
                    caption=f"📱 {escape(subj[:60])}"
                )
            except Exception as e:
                logger.error(f"HTML inbox: {e}")

    await message_to_edit.edit_text("📥 <b>End of inbox.</b>", reply_markup=kb_inbox())

# ══════════════════════════════════════════
# ADMIN — COMMANDS
# ══════════════════════════════════════════
@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID: return
    await message.answer("👑 <b>Admin Panel v26</b>", reply_markup=kb_admin())

@dp.message(Command("find"))
async def cmd_find(message: Message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("Usage: <code>/find @user or ID</code>")
    await _admin_find_respond(message, args[1].strip())

@dp.message(Command("note"))
async def cmd_note(message: Message):
    if message.from_user.id != ADMIN_ID: return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3: return await message.answer("Usage: <code>/note ID text</code>")
    try:
        uid = int(parts[1])
    except ValueError:
        return await message.answer("❌ Invalid ID.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET note=? WHERE telegram_id=?", (parts[2], uid))
        await db.commit()
    await message.answer(f"✅ Note saved for <code>{uid}</code>.")
    await log_admin("note", uid, parts[2])

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("Usage: <code>/broadcast msg</code>")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT telegram_id FROM users WHERE is_banned=0") as c:
            users = [r[0] for r in await c.fetchall()]
    status = await message.answer(f"⏳ Sending to {len(users)} users…")
    success = 0
    for uid in users:
        try:
            await safe_send(uid, f"📢 <b>Announcement</b>\n\n{args[1]}")
            success += 1
            await asyncio.sleep(0.04)
        except: pass
    await status.edit_text(f"✅ Delivered to <b>{success}/{len(users)}</b> users.")
    await log_admin("broadcast", detail=f"{success}/{len(users)}")

# ══════════════════════════════════════════
# ADMIN — CALLBACKS
# ══════════════════════════════════════════
@dp.callback_query(F.data.startswith("admin_"))
async def handle_admin_cb(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer("⛔ Unauthorized", show_alert=True)

    parts  = call.data.split("_")
    action = parts[1]

    # ── STATS ────────────────────────────────────────────────────
    if action == "stats":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as c:                 u  = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE is_banned=1") as c: bn = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM emails") as c:                e  = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM inbox") as c:                 m  = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM inbox WHERE seen=0") as c:    un = (await c.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM vault_emails") as c:          v  = (await c.fetchone())[0]
            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE last_seen >= datetime('now','-1 day')"
            ) as c: a24 = (await c.fetchone())[0]

        await call.message.edit_text(
            f"📊 <b>Live Stats</b>\n\n"
            f"👥 Total Users:    <b>{u}</b>\n"
            f"🟢 Active (24h):   <b>{a24}</b>\n"
            f"🚫 Banned:         <b>{bn}</b>\n"
            f"📧 Emails:         <b>{e}</b>\n"
            f"📩 Total Mail:     <b>{m}</b>\n"
            f"🔵 Unread:         <b>{un}</b>\n"
            f"🗄️ Vault Entries:  <b>{v}</b>",
            reply_markup=kb_admin()
        )

    # ── USERS LIST ───────────────────────────────────────────────
    elif action == "users":
        page = int(parts[2]) if len(parts) > 2 else 0
        await _admin_users_page(call.message, page)

    # ── MAIL LOG ─────────────────────────────────────────────────
    elif action == "mails":
        page = int(parts[2]) if len(parts) > 2 else 0
        await _admin_mails_page(call.message, page)

    # ── GHOST VAULT ──────────────────────────────────────────────
    elif action == "vault":
        page = int(parts[2]) if len(parts) > 2 else 0
        await _admin_vault_page(call.message, page)

    # ── AUDIT LOG ────────────────────────────────────────────────
    elif action == "audit":
        page = int(parts[2]) if len(parts) > 2 else 0
        await _admin_audit_page(call.message, page)

    # ── SEARCH ───────────────────────────────────────────────────
    elif action == "search":
        await call.answer()
        await state.set_state(AdminSearch.waiting)
        await call.message.edit_text(
            "🔍 <b>Search</b>\n\nSend username or Telegram ID:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Cancel", callback_data="admin_cancel")
            ]])
        )

    elif action == "cancel":
        await state.clear()
        await call.message.edit_text("👑 <b>Admin Panel</b>", reply_markup=kb_admin())

    # ── BROADCAST ────────────────────────────────────────────────
    elif action == "broadcast":
        await call.message.edit_text(
            "📢 Use: <code>/broadcast Your message here</code>",
            reply_markup=kb_admin()
        )

    # ── PURGE ────────────────────────────────────────────────────
    elif action == "purge":
        sub = parts[2] if len(parts) > 2 else ""
        if sub == "confirm":
            await call.message.edit_text(
                "⚠️ <b>Purge Inbox</b>\n\nDelete all inbox mail older than 7 days. Confirm?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Yes", callback_data="admin_purge_do"),
                    InlineKeyboardButton(text="❌ No",  callback_data="admin_stats")
                ]])
            )
        elif sub == "do":
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM inbox WHERE timestamp < datetime('now','-7 days')")
                await db.commit()
            await call.answer("🗑️ Purged.", show_alert=True)
            await log_admin("purge_inbox")
            await call.message.edit_text("👑 <b>Admin Panel</b>", reply_markup=kb_admin())

@dp.message(AdminSearch.waiting)
async def admin_search_input(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.clear()
    await _admin_find_respond(message, message.text.strip())

async def _admin_find_respond(message: Message, query: str):
    q = query.lstrip("@")
    async with aiosqlite.connect(DB_PATH) as db:
        if q.isdigit():
            async with db.execute(
                "SELECT telegram_id,username,first_name,is_banned,note,joined_at FROM users WHERE telegram_id=?",
                (int(q),)
            ) as c: row = await c.fetchone()
        else:
            async with db.execute(
                "SELECT telegram_id,username,first_name,is_banned,note,joined_at FROM users WHERE username=?",
                (q.lower(),)
            ) as c: row = await c.fetchone()

    if not row:
        return await message.answer(f"❌ <b>{escape(query)}</b> not found.")

    uid, uname, fname, banned, note, joined = row
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM emails WHERE telegram_id=?", (uid,)) as c:
            ec = (await c.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM inbox WHERE to_email IN (SELECT email FROM emails WHERE telegram_id=?)", (uid,)
        ) as c:
            mc = (await c.fetchone())[0]

    await message.answer(
        f"👤 <b>User Profile</b>\n\n"
        f"🆔 ID:       <code>{uid}</code>\n"
        f"📛 Username: @{uname or 'N/A'}\n"
        f"👤 Name:     {escape(fname or 'N/A')}\n"
        f"📅 Joined:   {joined}\n"
        f"📧 Emails:   {ec}\n"
        f"📩 Mail:     {mc}\n"
        f"🛡️ Status:  <b>{'🚫 Banned' if banned else '✅ Active'}</b>\n"
        f"📝 Note:     {escape(note or '—')}",
        reply_markup=kb_admin_user(uid, banned)
    )

# ── USER PROFILE ──────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_user_"))
async def adm_user_profile(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    uid = int(call.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT username,first_name,is_banned,note,joined_at FROM users WHERE telegram_id=?", (uid,)
        ) as c: row = await c.fetchone()
        if not row: return await call.answer("Not found.", show_alert=True)
        uname, fname, banned, note, joined = row
        async with db.execute("SELECT COUNT(*) FROM emails WHERE telegram_id=?", (uid,)) as c:
            ec = (await c.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM inbox WHERE to_email IN (SELECT email FROM emails WHERE telegram_id=?)", (uid,)
        ) as c:
            mc = (await c.fetchone())[0]

    await call.message.edit_text(
        f"👤 <b>User Profile</b>\n\n"
        f"🆔 ID:       <code>{uid}</code>\n"
        f"📛 Username: @{uname or 'N/A'}\n"
        f"👤 Name:     {escape(fname or 'N/A')}\n"
        f"📅 Joined:   {joined}\n"
        f"📧 Emails:   {ec}\n"
        f"📩 Mail:     {mc}\n"
        f"🛡️ Status:  <b>{'🚫 Banned' if banned else '✅ Active'}</b>\n"
        f"📝 Note:     {escape(note or '—')}",
        reply_markup=kb_admin_user(uid, banned)
    )

# ── BAN TOGGLE ────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_ban_"))
async def adm_ban(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    uid = int(call.data.split("_")[2])
    if uid == ADMIN_ID: return await call.answer("Can't ban yourself.", show_alert=True)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT is_banned FROM users WHERE telegram_id=?", (uid,)) as c:
            cur = (await c.fetchone())[0]
        new = 0 if cur else 1
        await db.execute("UPDATE users SET is_banned=? WHERE telegram_id=?", (new, uid))
        await db.commit()
    await call.answer(f"{'Banned' if new else 'Unbanned'}!", show_alert=True)
    await log_admin("ban" if new else "unban", uid)
    call.data = f"adm_user_{uid}"
    await adm_user_profile(call)

# ── USER EMAILS ───────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_emails_"))
async def adm_user_emails(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    parts = call.data.split("_")
    uid, page = int(parts[2]), int(parts[3]) if len(parts) > 3 else 0
    offset = page * 6
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM emails WHERE telegram_id=?", (uid,)) as c:
            total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT email, created_at FROM emails WHERE telegram_id=? ORDER BY created_at DESC LIMIT 6 OFFSET ?",
            (uid, offset)
        ) as c: rows = await c.fetchall()
    if not rows: return await call.answer("No emails.", show_alert=True)
    kb = [[InlineKeyboardButton(text=f"📧 {r[0]}", callback_data=f"adm_inbox_{uid}_0")] for r in rows]
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"adm_emails_{uid}_{page-1}"))
    if offset+6 < total:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"adm_emails_{uid}_{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data=f"adm_user_{uid}")])
    await call.message.edit_text(
        f"📧 <b>Emails of <code>{uid}</code></b> · {total} total",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# ── USER INBOX (ADMIN) ────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_inbox_"))
async def adm_user_inbox(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    parts = call.data.split("_")
    uid, page = int(parts[2]), int(parts[3]) if len(parts) > 3 else 0
    offset = page * 6
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM inbox WHERE to_email IN (SELECT email FROM emails WHERE telegram_id=?)", (uid,)
        ) as c: total = (await c.fetchone())[0]
        async with db.execute(
            """SELECT id, to_email, from_email, subject, timestamp FROM inbox
               WHERE to_email IN (SELECT email FROM emails WHERE telegram_id=?)
               ORDER BY timestamp DESC LIMIT 6 OFFSET ?""", (uid, offset)
        ) as c: rows = await c.fetchall()
    if not rows: return await call.answer("Inbox empty.", show_alert=True)
    kb = [[InlineKeyboardButton(
        text=f"📩 {r[3][:22]} | {r[2][:14]}",
        callback_data=f"adm_mail_{r[0]}"
    )] for r in rows]
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"adm_inbox_{uid}_{page-1}"))
    if offset+6 < total:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"adm_inbox_{uid}_{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data=f"adm_user_{uid}")])
    await call.message.edit_text(
        f"📩 <b>Inbox of <code>{uid}</code></b> (Page {page+1}) · {total} total",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# ── SINGLE MAIL ───────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_mail_"))
async def adm_view_mail(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    mail_id = int(call.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT to_email, from_email, subject, body, html_body, timestamp FROM inbox WHERE id=?", (mail_id,)
        ) as c: row = await c.fetchone()
        await db.execute("UPDATE inbox SET seen=1 WHERE id=?", (mail_id,))
        await db.commit()
    if not row: return await call.answer("Not found.", show_alert=True)
    to_e, from_e, subj, body, html_body, ts = row
    await call.message.edit_text(
        f"📧 <b>Mail</b>\n\n"
        f"<b>To:</b>      <code>{escape(to_e)}</code>\n"
        f"<b>From:</b>    {escape(from_e)}\n"
        f"<b>Subject:</b> {escape(subj)}\n"
        f"<b>Time:</b>    {ts}\n\n"
        f"{escape(body[:4000])}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_mails_0")
        ]])
    )
    if html_body and isinstance(html_body, str) and html_body.strip():
        try:
            await bot.send_document(
                call.from_user.id,
                document=BufferedInputFile(
                    render_html_mobile(html_body, subj, from_e, ts).encode("utf-8"),
                    filename=f"mail_{mail_id}.html"
                ),
                caption=f"📱 {escape(subj[:60])}"
            )
        except Exception as e:
            logger.error(f"Admin HTML: {e}")

# ── CREDENTIALS ───────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_creds_"))
async def adm_creds(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    uid = int(call.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT email, password, token, created_at FROM emails WHERE telegram_id=? ORDER BY created_at DESC", (uid,)
        ) as c: rows = await c.fetchall()
    if not rows: return await call.answer("No credentials.", show_alert=True)
    lines = [f"🔑 <b>Credentials — <code>{uid}</code></b>\n"]
    for email, pw, token, created in rows:
        lines.append(
            f"📧 <code>{escape(email)}</code>\n"
            f"🔐 <code>{escape(pw)}</code>\n"
            f"🎫 <code>{escape(token[:30])}…</code>\n"
            f"🕐 {created}\n"
        )
    await call.message.edit_text(
        "\n".join(lines)[:4096],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⬅️ Back", callback_data=f"adm_user_{uid}")
        ]])
    )
    await log_admin("view_creds", uid)

# ── NOTE FSM ──────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_note_"))
async def adm_note_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    uid = int(call.data.split("_")[2])
    await state.set_state(AdminNote.waiting)
    await state.update_data(uid=uid)
    await call.message.edit_text(
        f"📝 <b>Add note for <code>{uid}</code></b>\n\nSend your note:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"adm_user_{uid}")
        ]])
    )

@dp.message(AdminNote.waiting)
async def adm_note_save(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    data = await state.get_data()
    uid  = data["uid"]
    note = message.text.strip()
    await state.clear()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET note=? WHERE telegram_id=?", (note, uid))
        await db.commit()
    await message.answer(f"✅ Note saved for <code>{uid}</code>.")
    await log_admin("note", uid, note)

# ── NUKE USER EMAILS ──────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("adm_nuke_"))
async def adm_nuke(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    uid = int(call.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT email FROM emails WHERE telegram_id=?", (uid,)) as c:
            emails = [r[0] for r in await c.fetchall()]
    for email in emails:
        await archive_to_vault(email, deleted_by=f"admin:{ADMIN_ID}")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM inbox WHERE to_email IN (SELECT email FROM emails WHERE telegram_id=?)", (uid,)
        )
        await db.execute("DELETE FROM emails WHERE telegram_id=?", (uid,))
        await db.commit()
    await call.answer(f"🗑️ Nuked {len(emails)} emails.", show_alert=True)
    await log_admin("nuke_emails", uid, f"{len(emails)} vaulted")
    call.data = f"adm_user_{uid}"
    await adm_user_profile(call)

# ══════════════════════════════════════════
# ADMIN — PAGE HELPERS
# ══════════════════════════════════════════
async def _admin_users_page(message, page: int):
    offset = page * 8
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT telegram_id, username, first_name, is_banned FROM users ORDER BY joined_at DESC LIMIT 8 OFFSET ?",
            (offset,)
        ) as c: rows = await c.fetchall()
    if not rows: return await message.edit_text("No users.", reply_markup=kb_admin())
    kb = [[InlineKeyboardButton(
        text=f"{'🚫' if b else '✅'} @{u or uid} ({fn or ''})"[:58],
        callback_data=f"adm_user_{uid}"
    )] for uid, u, fn, b in rows]
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_users_{page-1}"))
    if offset+8 < total:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_users_{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_stats")])
    await message.edit_text(
        f"👥 <b>Users</b> · {total} total (Page {page+1})",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

async def _admin_mails_page(message, page: int):
    offset = page * 8
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM inbox") as c: total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT id, to_email, from_email, subject, timestamp FROM inbox ORDER BY timestamp DESC LIMIT 8 OFFSET ?",
            (offset,)
        ) as c: rows = await c.fetchall()
    if not rows: return await message.edit_text("No mail.", reply_markup=kb_admin())
    kb = [[InlineKeyboardButton(
        text=f"📩 {r[3][:20]} ← {r[2][:14]}",
        callback_data=f"adm_mail_{r[0]}"
    )] for r in rows]
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_mails_{page-1}"))
    if offset+8 < total:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_mails_{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_stats")])
    await message.edit_text(
        f"📧 <b>Mail Log</b> · {total} total (Page {page+1})",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

async def _admin_vault_page(message, page: int):
    offset = page * 8
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM vault_emails") as c: total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT email, telegram_id, deleted_by, deleted_at FROM vault_emails ORDER BY deleted_at DESC LIMIT 8 OFFSET ?",
            (offset,)
        ) as c: rows = await c.fetchall()
    if not rows: return await message.edit_text("🗄️ Vault is empty.", reply_markup=kb_admin())
    lines = [f"🗄️ <b>Ghost Vault</b> · {total} entries (Page {page+1})\n"]
    for email, uid, by, at in rows:
        lines.append(f"• <code>{escape(email)}</code>\n  👤 {uid} | 🗑️ {by} | 📅 {at[:16]}")
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_vault_{page-1}"))
    if offset+8 < total:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_vault_{page+1}"))
    kb = [nav] if nav else []
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_stats")])
    await message.edit_text(
        "\n".join(lines)[:4096],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

async def _admin_audit_page(message, page: int):
    offset = page * 8
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM admin_log") as c: total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT action, target_id, detail, timestamp FROM admin_log ORDER BY timestamp DESC LIMIT 8 OFFSET ?",
            (offset,)
        ) as c: rows = await c.fetchall()
    if not rows: return await message.edit_text("No log.", reply_markup=kb_admin())
    lines = [f"📋 <b>Audit Log</b> · {total} entries\n"]
    for action, tid, detail, ts in rows:
        lines.append(f"• <b>{action}</b> → <code>{tid}</code> {detail[:25]} <i>{ts[:16]}</i>")
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_audit_{page-1}"))
    if offset+8 < total:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_audit_{page+1}"))
    kb = [nav] if nav else []
    kb.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_stats")])
    await message.edit_text(
        "\n".join(lines)[:4096],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

# ══════════════════════════════════════════
# BACKGROUND POLLER
# ══════════════════════════════════════════
async def _poll_account(user_id: int, user_email: str, token: str):
    if not token:
        # A mailbox may have been created just before mail.tm throttled token
        # issuance. Retry token setup during normal polling instead of losing it.
        if _rate_limit_remaining("https://api.mail.tm"):
            return
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT password FROM emails WHERE email=? AND telegram_id=?",
                (user_email, user_id)
            ) as c:
                row = await c.fetchone()
        if not row:
            return
        token, error = await api_get_token("https://api.mail.tm", user_email, row[0])
        if not token:
            return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE emails SET token=? WHERE email=?", (token, user_email))
            await db.commit()

    messages = await api_fetch_messages("https://api.mail.tm", token)
    if not messages: return

    async with aiosqlite.connect(DB_PATH) as db:
        for msg in messages:
            msg_id = str(msg["id"])
            async with db.execute("SELECT id FROM inbox WHERE msg_id=?", (msg_id,)) as c:
                if await c.fetchone(): continue

            full = await api_fetch_body("https://api.mail.tm", token, msg_id)
            if not full: continue

            body     = full.get("text") or "No body"
            html_raw = full.get("html")
            html_str = None
            if html_raw:
                html_str = html_raw[0] if isinstance(html_raw, list) and html_raw else html_raw
                if not isinstance(html_str, str): html_str = None

            await db.execute(
                "INSERT INTO inbox (msg_id, to_email, from_email, subject, body, html_body) VALUES (?,?,?,?,?,?)",
                (msg_id, user_email, msg["from"]["address"], msg["subject"], body, html_str)
            )
            await db.commit()

            ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            asyncio.create_task(_deliver(
                user_id,
                f"📩 <b>New Mail!</b>\n\n"
                f"<b>To:</b>      <code>{escape(user_email)}</code>\n"
                f"<b>From:</b>    {escape(msg['from']['address'])}\n"
                f"<b>Subject:</b> {escape(msg['subject'])}\n\n"
                f"{escape(body[:3000])}",
                html_str, msg_id, msg["subject"],
                msg["from"]["address"], ts_str
            ))

async def _deliver(user_id, text, html_body, msg_id, subject, from_addr, ts):
    await safe_send(user_id, text, reply_markup=kb_inbox())
    if html_body and isinstance(html_body, str) and html_body.strip():
        try:
            await bot.send_document(
                user_id,
                document=BufferedInputFile(
                    render_html_mobile(html_body, subject, from_addr, ts).encode("utf-8"),
                    filename=f"mail_{msg_id}.html"
                ),
                caption=f"📱 {escape(subject[:60])}"
            )
        except Exception as e:
            logger.error(f"Deliver HTML: {e}")

async def _poll_account_bounded(user_id: int, user_email: str, token: str):
    async with POLL_SEMAPHORE:
        await _poll_account(user_id, user_email, token)

async def mail_poller():
    logger.info("Poller started.")
    await fetch_all_active_domains()
    cycle = 0
    while True:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT telegram_id, email, token FROM emails") as c:
                    accounts = await c.fetchall()
            if accounts:
                await asyncio.gather(*[_poll_account_bounded(u, e, t) for u, e, t in accounts])
            # Refresh domains every 100 cycles (~6 min), purge vault every 1000 cycles (~1hr)
            cycle += 1
            if cycle % 100  == 0: await fetch_all_active_domains()
            if cycle % 1000 == 0: await purge_old_vault()
        except Exception as e:
            logger.error(f"Poller: {e}")
        await asyncio.sleep(POLL_INTERVAL)

# ══════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════
async def health_check(request):
    return web.Response(text="OK")

async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", "10000"))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info("Health server listening on 0.0.0.0:%s", port)
    return runner

async def main():
    poller_task = None
    health_runner = None
    await init_db()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        health_runner = await start_health_server()
        poller_task = asyncio.create_task(mail_poller())
        logger.info("TempOG v26 live.")
        await dp.start_polling(bot)
    finally:
        if poller_task:
            poller_task.cancel()
            await asyncio.gather(poller_task, return_exceptions=True)
        if health_runner:
            await health_runner.cleanup()
        if GLOBAL_SESSION and not GLOBAL_SESSION.closed:
            await GLOBAL_SESSION.close()

if __name__ == "__main__":
    asyncio.run(main())
