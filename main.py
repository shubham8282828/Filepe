"""
Telegram File Sharing + Streaming Bot
Production-ready with MongoDB, Pixeldrain, Flask streaming, UPI payments
"""

import os
import io
import re
import uuid
import logging
import asyncio
import hashlib
import threading
import time
import requests
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, request, jsonify, Response, render_template_string, abort, make_response
from functools import lru_cache
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    Bot, InputMediaPhoto
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import urllib.parse

# ─────────────────────────────────────────────
#  LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
BOT_TOKEN          = os.environ.get("BOT_TOKEN", "")
MONGO_URI          = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
PIXELDRAIN_API_KEY = os.environ.get("PIXELDRAIN_API_KEY", "")
ADMIN_IDS          = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
BOT_USERNAME       = os.environ.get("BOT_USERNAME", "YourBot")          # without @
SHORTENER_API_KEY  = os.environ.get("SHORTENER_API_KEY", "")
SHORTENER_DOMAIN   = os.environ.get("SHORTENER_DOMAIN", "api.shrtco.de") # default free shortener
FLASK_SECRET       = os.environ.get("FLASK_SECRET", uuid.uuid4().hex)
BASE_URL           = os.environ.get("BASE_URL", "http://localhost:5000")  # your public URL
UPI_ID             = os.environ.get("UPI_ID", "yourname@upi")
UPI_QR_URL         = os.environ.get("UPI_QR_URL", "")                    # optional hosted QR image URL
TOKEN_VALIDITY_HOURS = int(os.environ.get("TOKEN_VALIDITY_HOURS", "24"))
PORT               = int(os.environ.get("PORT", "5000"))

# Premium plans (days: price in INR)
PREMIUM_PLANS = {
    "1month":  {"days": 30,  "price": 49,  "label": "1 Month"},
    "3months": {"days": 90,  "price": 129, "label": "3 Months"},
    "1year":   {"days": 365, "price": 399, "label": "1 Year"},
    "lifetime":{"days": 36500,"price": 799, "label": "Lifetime"},
}

# Referral rewards
REFERRAL_REWARD_HOURS   = int(os.environ.get("REFERRAL_REWARD_HOURS", "6"))
REFERRAL_PREMIUM_COUNT  = int(os.environ.get("REFERRAL_PREMIUM_COUNT", "10"))

# Pre-computed at startup for zero per-request overhead
import base64 as _b64
PD_AUTH_HEADER = f"Basic {_b64.b64encode(f':{PIXELDRAIN_API_KEY}'.encode()).decode()}" if PIXELDRAIN_API_KEY else ""
PD_SESSION = None  # Initialized in main() for connection reuse

# ─────────────────────────────────────────────
#  MULTI-BOT LOAD BALANCING CONFIG
# ─────────────────────────────────────────────
# Each bot instance gets a SERVER_ID (1,2,3...)
SERVER_ID      = int(os.environ.get("SERVER_ID", "1"))
MAX_USERS      = int(os.environ.get("MAX_USERS", "500"))   # Max registered users per bot

# Chain of bots: bot1 → bot2 → bot3 → bot4 → bot5 → bot1
# Format: "BotUsername|https://t.me/BotUsername"
NEXT_BOT_RAW   = os.environ.get("NEXT_BOT", "")           # e.g. "filepe2_bot|https://t.me/filepe2_bot"
NEXT_BOT_NAME  = NEXT_BOT_RAW.split("|")[0] if "|" in NEXT_BOT_RAW else ""
NEXT_BOT_LINK  = NEXT_BOT_RAW.split("|")[1] if "|" in NEXT_BOT_RAW else ""

# All bots list for file link generation (comma separated "username|base_url")
# e.g. "filepe_bot|https://filepe.onrender.com,filepe2_bot|https://filepe2.onrender.com"
ALL_BOTS_RAW   = os.environ.get("ALL_BOTS", "")
ALL_BOTS       = []
for b in ALL_BOTS_RAW.split(","):
    b = b.strip()
    if "|" in b:
        parts = b.split("|")
        ALL_BOTS.append({"username": parts[0], "base_url": parts[1]})

# Concurrent user tracking
_active_users: dict = {}   # {user_id: last_active_timestamp}
_active_lock = threading.Lock()

def track_active_user(user_id: int):
    """Track user activity for concurrent user counting."""
    with _active_lock:
        _active_users[user_id] = time.time()
        # Clean users inactive for > 5 minutes
        cutoff = time.time() - 300
        inactive = [uid for uid, t in _active_users.items() if t < cutoff]
        for uid in inactive:
            _active_users.pop(uid, None)

def get_concurrent_users() -> int:
    """Get number of users active in last 5 minutes."""
    with _active_lock:
        cutoff = time.time() - 300
        return sum(1 for t in _active_users.values() if t >= cutoff)

def is_bot_full() -> bool:
    """Check if this bot has reached max registered users."""
    total = users_col.count_documents({})
    return total >= MAX_USERS

# ─────────────────────────────────────────────
#  MONGODB SETUP
# ─────────────────────────────────────────────
mongo_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=50,          # Max 50 concurrent MongoDB connections
    minPoolSize=5,           # Keep 5 connections warm
    maxIdleTimeMS=30000,     # Close idle connections after 30s
    connectTimeoutMS=10000,
    socketTimeoutMS=30000,
    retryWrites=True,
    retryReads=True,
)
db = mongo_client["filebot"]

# Collections
# users_bot{ID} = per-bot registration + count only
# access = SHARED (token, premium — verify once, works everywhere)
# files, payments, referrals = SHARED

users_col    = db[f"users_bot{SERVER_ID}"]  # Per-bot: registration & count
access_col   = db["user_access"]            # SHARED: token_expiry, premium_expiry
files_col    = db["files"]                  # Shared: same files on all bots
payments_col = db["payments"]               # Shared: admin sees all payments
referrals_col= db["referrals"]              # Shared: referrals work across bots

def setup_indexes():
    """Create indexes for fast queries."""
    users_col.create_index("user_id", unique=True)
    # access_col is SHARED — token & premium indexed
    access_col.create_index("user_id", unique=True)
    access_col.create_index("token_expiry")
    access_col.create_index("premium_expiry")
    files_col.create_index("file_unique_id", unique=True)
    files_col.create_index("created_at")
    payments_col.create_index("utr")
    payments_col.create_index([("user_id", ASCENDING), ("status", ASCENDING)])
    referrals_col.create_index([("referrer_id", ASCENDING), ("referred_user_id", ASCENDING)], unique=True)
    logger.info(f"✅ MongoDB indexes created. Bot collection: users_bot{SERVER_ID} | Access: shared")

setup_indexes()

# ─────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────
def get_user(user_id: int) -> dict:
    """Fetch or create user in per-bot collection (registration only)."""
    user = users_col.find_one({"user_id": user_id})
    if not user:
        user = {
            "user_id":        user_id,
            "referrals_count": 0,
            "referred_by":    None,
            "joined_at":      datetime.utcnow(),
        }
        users_col.insert_one(user)
    return user

def get_access(user_id: int) -> dict:
    """Fetch or create SHARED access document (token + premium)."""
    access = access_col.find_one({"user_id": user_id})
    if not access:
        access = {
            "user_id":        user_id,
            "token_expiry":   None,
            "premium_expiry": None,
        }
        access_col.insert_one(access)
    return access

def is_premium(user_id: int) -> bool:
    """Return True if user has active premium — checks SHARED access collection."""
    access = get_access(user_id)
    exp = access.get("premium_expiry")
    return exp is not None and exp > datetime.utcnow()

# Simple in-memory cache: {user_id: (result, cache_until)}
_token_cache: dict = {}
_token_cache_lock = threading.Lock()

def has_valid_token(user_id: int) -> bool:
    """Return True if user has valid token OR premium. Cached for 60s."""
    now = datetime.utcnow()

    with _token_cache_lock:
        cached = _token_cache.get(user_id)
        if cached:
            result, cache_until = cached
            if now < cache_until:
                return result

    # Cache miss — check DB
    result = _check_token_db(user_id)

    with _token_cache_lock:
        # Cache for 60 seconds
        _token_cache[user_id] = (result, now + timedelta(seconds=60))
        # Clean old entries if cache too big
        if len(_token_cache) > 10000:
            cutoff = now - timedelta(minutes=5)
            expired = [k for k, v in _token_cache.items() if v[1] < cutoff]
            for k in expired:
                _token_cache.pop(k, None)

    return result

def _check_token_db(user_id: int) -> bool:
    """Internal: check SHARED access collection for token/premium validity."""
    if is_premium(user_id):
        return True
    access = get_access(user_id)
    exp = access.get("token_expiry")
    return exp is not None and exp > datetime.utcnow()

def invalidate_token_cache(user_id: int):
    """Call this when token/premium is granted or revoked."""
    with _token_cache_lock:
        _token_cache.pop(user_id, None)

def grant_token(user_id: int, hours: int = TOKEN_VALIDITY_HOURS):
    """Give user a token — saved in SHARED access collection."""
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"token_expiry": datetime.utcnow() + timedelta(hours=hours)}},
        upsert=True
    )
    invalidate_token_cache(user_id)

def grant_premium(user_id: int, days: int):
    """Give user premium — saved in SHARED access collection (stacks on existing)."""
    access = get_access(user_id)
    base = access.get("premium_expiry") or datetime.utcnow()
    if base < datetime.utcnow():
        base = datetime.utcnow()
    new_expiry = base + timedelta(days=days)
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": new_expiry}},
        upsert=True
    )
    invalidate_token_cache(user_id)
    return new_expiry

def revoke_premium(user_id: int):
    """Revoke premium from SHARED access collection."""
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": None}},
        upsert=True
    )
    invalidate_token_cache(user_id)

def save_file(file_unique_id: str, telegram_file_id: str, pixeldrain_id: str, file_type: str = "video", file_name: str = ""):
    """Save or update file record."""
    files_col.update_one(
        {"file_unique_id": file_unique_id},
        {"$set": {
            "telegram_file_id": telegram_file_id,
            "pixeldrain_id":    pixeldrain_id,
            "file_type":        file_type,
            "file_name":        file_name,
            "created_at":       datetime.utcnow(),
            "last_checked":     datetime.utcnow(),
        }},
        upsert=True
    )

def get_file(file_unique_id: str) -> dict | None:
    return files_col.find_one({"file_unique_id": file_unique_id})

def record_referral(referrer_id: int, referred_id: int) -> bool:
    """Record referral; returns True if new referral."""
    if referrer_id == referred_id:
        return False
    try:
        referrals_col.insert_one({
            "referrer_id":      referrer_id,
            "referred_user_id": referred_id,
            "timestamp":        datetime.utcnow()
        })
        # Increment referral count
        users_col.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referrals_count": 1}}
        )
        return True
    except DuplicateKeyError:
        return False

def check_referral_rewards(referrer_id: int):
    """Grant rewards based on referral milestones. Rewards go to SHARED access."""
    user = get_user(referrer_id)
    count = user.get("referrals_count", 0)
    # Every 5 referrals → bonus token hours
    if count % 5 == 0:
        grant_token(referrer_id, hours=REFERRAL_REWARD_HOURS * (count // 5))
    # 10 referrals → 7 days premium (shared — works on all bots)
    if count >= REFERRAL_PREMIUM_COUNT and not is_premium(referrer_id):
        grant_premium(referrer_id, 7)
        return "premium"
    return "token"

# ─────────────────────────────────────────────
#  PIXELDRAIN API
# ─────────────────────────────────────────────
PD_UPLOAD_URL   = "https://pixeldrain.com/api/file"
PD_DOWNLOAD_URL = "https://pixeldrain.com/api/file/{id}"
PD_CHECK_URL    = "https://pixeldrain.com/api/file/{id}/info"

def upload_to_pixeldrain(file_bytes: bytes, filename: str, max_retries: int = 3) -> str | None:
    """Upload file bytes to Pixeldrain. Returns file ID or None on failure."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Pixeldrain upload attempt {attempt}/{max_retries}: {filename} ({len(file_bytes)/1024/1024:.1f} MB)")
            resp = requests.post(
                PD_UPLOAD_URL,
                files={"file": (filename, io.BytesIO(file_bytes), "application/octet-stream")},
                auth=("", PIXELDRAIN_API_KEY),
                timeout=300  # 5 min for large files
            )
            logger.info(f"Pixeldrain response [{resp.status_code}]: {resp.text[:200]}")

            if resp.status_code == 201:
                pd_id = resp.json().get("id")
                if not pd_id:
                    logger.error("Pixeldrain returned 201 but no ID!")
                    continue

                # ── Verify file actually exists ──────────────
                verify = requests.get(
                    f"https://pixeldrain.com/api/file/{pd_id}/info",
                    auth=("", PIXELDRAIN_API_KEY),
                    timeout=15
                )
                if verify.status_code == 200:
                    info = verify.json()
                    logger.info(f"✅ Pixeldrain verified: {pd_id} | size: {info.get('size')} | name: {info.get('name')}")
                    return pd_id
                else:
                    logger.error(f"❌ Pixeldrain verify failed [{verify.status_code}]: {verify.text}")
                    continue
            else:
                logger.warning(f"Pixeldrain upload failed [{resp.status_code}]: {resp.text}")
        except requests.exceptions.Timeout:
            logger.error(f"Pixeldrain upload timeout on attempt {attempt}")
        except Exception as e:
            logger.error(f"Pixeldrain upload error: {e}")
        time.sleep(3 * attempt)
    return None

def check_pixeldrain_file(pd_id: str) -> bool:
    """Returns True if the file still exists on Pixeldrain."""
    try:
        resp = requests.get(
            PD_CHECK_URL.format(id=pd_id),
            auth=("", PIXELDRAIN_API_KEY),
            timeout=15
        )
        return resp.status_code == 200
    except Exception:
        return False

def stream_from_pixeldrain(pd_id: str, range_header: str | None = None):
    """Generator: proxy-stream Pixeldrain file bytes. Hides real URL."""
    headers = {}
    if range_header:
        headers["Range"] = range_header
    url = PD_DOWNLOAD_URL.format(id=pd_id)
    with requests.get(
        url,
        headers=headers,
        auth=("", PIXELDRAIN_API_KEY),
        stream=True,
        timeout=60
    ) as r:
        for chunk in r.iter_content(chunk_size=1024 * 512):  # 512 KB chunks
            if chunk:
                yield chunk

# ─────────────────────────────────────────────
#  URL SHORTENER
# ─────────────────────────────────────────────
def shorten_url(long_url: str) -> str:
    """Shorten URL via configured API. Falls back to original URL on error."""
    if not SHORTENER_API_KEY:
        # Use free shrtco.de as default
        try:
            resp = requests.get(
                f"https://api.shrtco.de/v2/shorten?url={urllib.parse.quote(long_url)}",
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json()["result"]["full_short_link"]
        except Exception as e:
            logger.warning(f"shrtco.de shortener failed: {e}")
        return long_url

    # mdisk / other custom shortener
    try:
        resp = requests.get(
            f"https://{SHORTENER_DOMAIN}/api?api={SHORTENER_API_KEY}&url={urllib.parse.quote(long_url)}",
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("shortenedUrl", data.get("short_link", long_url))
    except Exception as e:
        logger.warning(f"Shortener failed: {e}")
    return long_url

# ─────────────────────────────────────────────
#  DECORATORS
# ─────────────────────────────────────────────
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only command.")
            return
        return await func(update, ctx, *args, **kwargs)
    return wrapper

# ─────────────────────────────────────────────
#  TELEGRAM BOT HANDLERS
# ─────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start with optional deep link payload."""
    user = update.effective_user
    args = ctx.args

    # Track active user
    track_active_user(user.id)

    payload = args[0] if args else ""

    # ── Referral handling ────────────────────
    if payload.startswith("ref_"):
        try:
            referrer_id = int(payload[4:])
            # Check if bot is full BEFORE registering new user
            is_new_user = users_col.find_one({"user_id": user.id}) is None
            if is_new_user and is_bot_full() and NEXT_BOT_LINK:
                await send_redirect_message(update, ctx)
                return
            get_user(user.id)
            if record_referral(referrer_id, user.id):
                reward_type = check_referral_rewards(referrer_id)
                logger.info(f"Referral: {referrer_id} → {user.id} | reward: {reward_type}")
                try:
                    msg = (
                        f"🎉 <b>New Referral!</b>\n"
                        f"User <code>{user.id}</code> joined via your link!\n"
                        f"{'🏆 You earned 7-day Premium!' if reward_type == 'premium' else '⏰ You earned bonus token hours!'}"
                    )
                    await ctx.bot.send_message(referrer_id, msg, parse_mode="HTML")
                except Exception:
                    pass
        except (ValueError, IndexError):
            pass
        await send_welcome(update, ctx)
        return

    # ── File request handling ─────────────────
    if payload.startswith("file_"):
        file_unique_id = payload[5:]
        # If NEW user and bot is FULL → redirect WITH file_id preserved
        # So user can access same file on next bot seamlessly
        is_new_user = users_col.find_one({"user_id": user.id}) is None
        if is_new_user and is_bot_full() and NEXT_BOT_LINK:
            await send_redirect_message(update, ctx, file_id=file_unique_id)
            return
        # Existing users always served here — no redirect
        get_user(user.id)
        track_active_user(user.id)
        await handle_file_request(update, ctx, file_unique_id)
        return

    # ── Token verify callback ─────────────────
    if payload.startswith("verify_"):
        uid = int(payload[7:])
        if uid == user.id:
            grant_token(user.id)
            await update.message.reply_text(
                "✅ <b>Verification Successful!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🎉 You now have access for <b>24 hours!</b>\n\n"
                "📌 <b>What to do next:</b>\n"
                "┗ Click your file link again to access it\n\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💎 Get Premium to skip verification forever!",
                parse_mode="HTML"
            )
            return

    # ── New user check — redirect if bot full ──
    is_new_user = users_col.find_one({"user_id": user.id}) is None
    if is_new_user and is_bot_full() and NEXT_BOT_LINK:
        await send_redirect_message(update, ctx, file_id="")
        return

    # Ensure user exists
    get_user(user.id)
    await send_welcome(update, ctx)


async def send_redirect_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_id: str = ""):
    """Redirect new user to next bot — with optional file_id to preserve access."""
    user = update.effective_user
    next_name = f"@{NEXT_BOT_NAME}" if NEXT_BOT_NAME else "our other bot"

    if file_id and NEXT_BOT_LINK:
        next_bot_username = NEXT_BOT_NAME
        deep_link = f"https://t.me/{next_bot_username}?start=file_{file_id}"
        text = (
            f"👋 <b>Hello {user.first_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ This bot is currently full!\n\n"
            f"✅ Your file is available on our sister bot.\n"
            f"Click below — it will open automatically!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Sister bot: <b>{next_name}</b>"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Get My File Now", url=deep_link)
        ]])
    else:
        text = (
            f"👋 <b>Hello {user.first_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ This bot is currently full!\n"
            f"👤 Capacity: {MAX_USERS} users reached\n\n"
            f"✅ Join our sister bot:\n"
            f"┗ Same files & features!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <b>{next_name}</b>"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➡️ Join Sister Bot", url=NEXT_BOT_LINK)
        ]])

    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
    logger.info(f"Redirected user {user.id} to {NEXT_BOT_NAME} | file_id: {file_id or 'none'}")


async def send_welcome(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    total_users = users_col.count_documents({})
    concurrent   = get_concurrent_users()
    slots_left   = max(0, MAX_USERS - total_users)

    text = (
        f"👋 <b>Welcome, {user.first_name}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎬 <b>File Sharing Bot</b> — Server {SERVER_ID}\n\n"
        f"📌 <b>Commands:</b>\n"
        f"┣ 💎 /premium — Get premium access\n"
        f"┣ 👥 /referral — Your referral link\n"
        f"┗ 📊 /mystatus — Token & premium status\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"🎁 Refer friends & earn free premium!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Server {SERVER_ID}  •  👤 {total_users}/{MAX_USERS} users  •  ⚡ {concurrent} active"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu"),
        InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}")
    ]])
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


async def handle_file_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_unique_id: str):
    """Core: validate access, then send file link or stream link."""
    user = update.effective_user
    file_doc = get_file(file_unique_id)

    if not file_doc:
        await update.message.reply_text("❌ File not found or has been removed.")
        return

    # ── Access check ─────────────────────────
    if not has_valid_token(user.id):
        await send_verification_prompt(update, ctx, file_unique_id)
        return

    # ── Validate Pixeldrain link (auto re-upload if broken) ──
    msg = await update.message.reply_text("⏳ Checking file availability...")
    pd_id = file_doc.get("pixeldrain_id")

    # Only re-check if last_checked > 1 hour ago to avoid excessive API calls
    last_checked = file_doc.get("last_checked")
    need_check = (
        not pd_id or
        not last_checked or
        (datetime.utcnow() - last_checked).total_seconds() > 3600
    )

    if need_check and pd_id:
        if not check_pixeldrain_file(pd_id):
            await msg.edit_text("🔄 Re-uploading file, please wait (30-60s)...")
            pd_id = await re_upload_file(file_doc)
            if not pd_id:
                await msg.edit_text("❌ File unavailable. Please contact admin.")
                return
    elif not pd_id:
        await msg.edit_text("🔄 Processing file, please wait...")
        pd_id = await re_upload_file(file_doc)
        if not pd_id:
            await msg.edit_text("❌ File unavailable. Please contact admin.")
            return

    # Update last_checked
    files_col.update_one(
        {"file_unique_id": file_unique_id},
        {"$set": {"last_checked": datetime.utcnow(), "pixeldrain_id": pd_id}}
    )

    file_type = file_doc.get("file_type", "video")

    if file_type == "video":
        watch_url = make_watch_url(user.id, file_unique_id)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Watch Now", url=watch_url)
        ]])
        await msg.edit_text(
            f"✅ <b>{file_doc.get('file_name', 'Video')}</b>\n\n"
            f"🎬 Click below to stream your video.",
            parse_mode="HTML",
            reply_markup=kb
        )
    else:
        # For non-video: send file directly via Telegram
        try:
            await ctx.bot.send_document(
                chat_id=user.id,
                document=file_doc["telegram_file_id"],
                caption=f"📁 <b>{file_doc.get('file_name', 'File')}</b>",
                parse_mode="HTML"
            )
            await msg.delete()
        except Exception as e:
            logger.error(f"Send document error: {e}")
            await msg.edit_text("❌ Failed to send file. Try again later.")


async def re_upload_file(file_doc: dict) -> str | None:
    """Download from Telegram using file_id and re-upload to Pixeldrain."""
    try:
        bot = Bot(token=BOT_TOKEN)
        tg_file = await bot.get_file(file_doc["telegram_file_id"])
        buf = await tg_file.download_as_bytearray()
        file_bytes = bytes(buf)
        filename = file_doc.get("file_name", "file")
        pd_id = upload_to_pixeldrain(file_bytes, filename)
        if pd_id:
            files_col.update_one(
                {"file_unique_id": file_doc["file_unique_id"]},
                {"$set": {"pixeldrain_id": pd_id, "last_checked": datetime.utcnow()}}
            )
        return pd_id
    except Exception as e:
        logger.error(f"Re-upload failed: {e}")
        return None


async def send_verification_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_unique_id: str):
    """Send shortened verification link when user has no valid token."""
    user = update.effective_user
    verify_url = f"https://t.me/{BOT_USERNAME}?start=verify_{user.id}"
    short_url = shorten_url(verify_url)

    text = (
        "🔐 <b>Verification Required</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ You need to verify to access this file.\n\n"
        "📋 <b>Simple Steps:</b>\n"
        "┣ 1️⃣ Click <b>Verify Now</b> below\n"
        "┣ 2️⃣ Complete the short page\n"
        "┗ 3️⃣ Come back & click file link again\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⏳ Token valid for <b>24 hours</b> after verification\n"
        "💎 Get Premium to skip verification forever!"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Verify Now", url=short_url),
        InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu")
    ]])
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


# ─────────────────────────────────────────────
#  FILE UPLOAD (Admin only)
# ─────────────────────────────────────────────
@admin_only
async def handle_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin uploads a file → bot processes and returns deep link."""
    msg = update.message
    user = update.effective_user

    tg_file = None
    file_type = "document"
    file_name = "file"
    file_size = 0

    if msg.video:
        tg_file   = msg.video
        file_type = "video"
        file_name = msg.video.file_name or f"video_{int(time.time())}.mp4"
        file_size = msg.video.file_size or 0
    elif msg.document:
        tg_file   = msg.document
        file_type = "document"
        file_name = msg.document.file_name or f"document_{int(time.time())}"
        file_size = msg.document.file_size or 0
    elif msg.audio:
        tg_file   = msg.audio
        file_type = "audio"
        file_name = msg.audio.file_name or f"audio_{int(time.time())}.mp3"
        file_size = msg.audio.file_size or 0
    else:
        return

    # Telegram Bot API limit = 20MB download
    MAX_TG_SIZE = 20 * 1024 * 1024  # 20 MB
    if file_size > MAX_TG_SIZE:
        size_mb = file_size / 1024 / 1024
        text = (
            "⚠️ <b>File too big for direct upload!</b>\n\n"
            f"📦 File size: <b>{size_mb:.1f} MB</b>\n"
            "📏 Telegram limit: <b>20 MB</b>\n\n"
            "<b>Use this instead:</b>\n"
            "1. Upload file to pixeldrain.com manually\n"
            "2. Copy the file ID from URL\n"
            "   Example: pixeldrain.com/u/<b>AbCdEfGh</b>\n"
            "3. Send command:\n"
            f"   <code>/addfile AbCdEfGh {file_name}</code>"
        )
        await msg.reply_text(text, parse_mode="HTML")
        return

    processing_msg = await msg.reply_text("⏳ Processing file, please wait...")

    try:
        # Get Telegram file object
        file_obj = await ctx.bot.get_file(tg_file.file_id)
        file_bytes = bytes(await file_obj.download_as_bytearray())

        await processing_msg.edit_text("☁️ Uploading to cloud storage...")

        pd_id = upload_to_pixeldrain(file_bytes, file_name)
        if not pd_id:
            await processing_msg.edit_text("❌ Cloud upload failed. Try again.")
            return

        file_unique_id = tg_file.file_unique_id
        save_file(file_unique_id, tg_file.file_id, pd_id, file_type, file_name)

        # ONE universal link — always points to main bot (BOT_USERNAME)
        # If that bot is full, it auto-redirects user to next bot WITH file_id
        universal_link = f"https://t.me/{BOT_USERNAME}?start=file_{file_unique_id}"

        await processing_msg.edit_text(
            f"✅ <b>File Uploaded Successfully!</b>\n\n"
            f"📁 <b>Name:</b> <code>{file_name}</code>\n\n"
            f"🔗 <b>Universal Share Link:</b>\n"
            f"<code>{universal_link}</code>\n\n"
            f"✅ <b>This ONE link works on ALL bots!</b>\n"
            f"Users are auto-redirected if bot is full.\n\n"
            f"📌 Share only this link — nothing else needed.",
            parse_mode="HTML"
        )
        logger.info(f"Admin {user.id} uploaded: {file_name} → PD:{pd_id}")

    except Exception as e:
        logger.error(f"Upload error: {e}")
        await processing_msg.edit_text(f"❌ Error: {str(e)}")


# ─────────────────────────────────────────────
#  STATUS & REFERRAL COMMANDS
# ─────────────────────────────────────────────
async def mystatus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = get_user(user.id)
    access   = get_access(user.id)  # Shared: token + premium

    token_exp = access.get("token_expiry")
    prem_exp  = access.get("premium_expiry")
    refs      = db_user.get("referrals_count", 0)

    token_str = "❌ No active token"
    if token_exp and token_exp > datetime.utcnow():
        remaining = token_exp - datetime.utcnow()
        h, rem = divmod(int(remaining.total_seconds()), 3600)
        m = rem // 60
        token_str = f"✅ Valid  •  ⏳ {h}h {m}m left"

    prem_str = "❌ Not Active"
    if prem_exp and prem_exp > datetime.utcnow():
        prem_str = f"✅ Active until {prem_exp.strftime('%d %b %Y')}"

    joined = db_user.get("joined_at", "N/A")
    joined_str = joined.strftime("%d %b %Y") if hasattr(joined, "strftime") else str(joined)[:10]

    text = (
        f"📊 <b>Your Account Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔐 <b>Token Access</b>\n"
        f"┗ {token_str}\n\n"
        f"💎 <b>Premium</b>\n"
        f"┗ {prem_str}\n\n"
        f"👥 <b>Referrals</b>\n"
        f"┗ {refs} friends referred\n\n"
        f"📅 <b>Joined</b>\n"
        f"┗ {joined_str}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Server {SERVER_ID}"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def referral_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = get_user(user.id)
    refs = db_user.get("referrals_count", 0)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"

    next_milestone = REFERRAL_PREMIUM_COUNT - refs if refs < REFERRAL_PREMIUM_COUNT else 0
    text = (
        f"👥 <b>Referral Program</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"📊 <b>Your Stats:</b>\n"
        f"┣ 👤 Total Referred: <b>{refs}</b>\n"
        f"┗ 🎯 Next Reward: <b>{next_milestone} more</b> for 7-day Premium\n\n"
        f"🎁 <b>Rewards:</b>\n"
        f"┣ ⏰ Every 5 referrals → Bonus token hours\n"
        f"┗ 🏆 {REFERRAL_PREMIUM_COUNT} referrals → 7-day Premium FREE!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Share now and start earning! 🚀"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📤 Share Now", url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}&text=Join+this+amazing+bot!")
    ]])
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


# ─────────────────────────────────────────────
#  PREMIUM / PAYMENT COMMANDS
# ─────────────────────────────────────────────
async def premium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_premium_menu(update.message, update.effective_user)


async def show_premium_menu(msg_or_query, user):
    plan_lines = "\n".join([
        f"┣ 💎 <b>{v['label']}</b>  —  ₹{v['price']}" for v in list(PREMIUM_PLANS.values())[:-1]
    ] + [
        f"┗ 👑 <b>{list(PREMIUM_PLANS.values())[-1]['label']}</b>  —  ₹{list(PREMIUM_PLANS.values())[-1]['price']}"
    ])
    text = (
        f"💎 <b>Premium Access</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>Available Plans:</b>\n"
        f"{plan_lines}\n\n"
        f"✅ <b>Premium Benefits:</b>\n"
        f"┣ ⚡ Skip daily verification\n"
        f"┣ ♾️ Unlimited file access\n"
        f"┣ 🚀 Faster streaming\n"
        f"┗ 🎯 Priority support\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👇 Select a plan to proceed:"
    )
    buttons = [
        [InlineKeyboardButton(f"{v['label']} – ₹{v['price']}", callback_data=f"buy_{k}")]
        for k, v in PREMIUM_PLANS.items()
    ]
    kb = InlineKeyboardMarkup(buttons)
    if hasattr(msg_or_query, "reply_text"):
        await msg_or_query.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)


async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "premium_menu":
        await show_premium_menu(q, q.from_user)
        return

    if data.startswith("buy_"):
        plan_key = data[4:]
        plan = PREMIUM_PLANS.get(plan_key)
        if not plan:
            return
        upi_text = (
            f"💳 <b>Complete Your Payment</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>Plan:</b> {plan['label']}\n"
            f"💰 <b>Amount:</b> ₹{plan['price']}\n\n"
            f"📲 <b>Pay to UPI ID:</b>\n"
            f"<code>{UPI_ID}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>After Payment:</b>\n"
            f"Send your UTR/Transaction ID:\n"
            f"<code>/utr YOUR_UTR_NUMBER</code>\n\n"
            f"📌 Example: <code>/utr 123456789012</code>\n\n"
            f"⚠️ <b>Important:</b> Do NOT close this chat until approved!"
        )
        buttons = []
        if UPI_QR_URL:
            buttons.append([InlineKeyboardButton("📷 View QR Code", url=UPI_QR_URL)])
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="premium_menu")])

        ctx.user_data["pending_plan"] = plan_key
        await q.edit_message_text(upi_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))


async def utr_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User submits UTR for payment verification."""
    user = update.effective_user
    args = ctx.args

    if not args:
        await update.message.reply_text("❗ Usage: /utr <your_UTR_number>")
        return

    utr = args[0].strip()
    if not re.match(r'^[A-Za-z0-9]{10,22}$', utr):
        await update.message.reply_text("❌ Invalid UTR format. Please check and try again.")
        return

    # Check for duplicate UTR
    existing = payments_col.find_one({"utr": utr})
    if existing:
        await update.message.reply_text("❌ This UTR has already been submitted.")
        return

    plan_key = ctx.user_data.get("pending_plan", "1month")
    plan = PREMIUM_PLANS.get(plan_key, PREMIUM_PLANS["1month"])

    payments_col.insert_one({
        "user_id":    user.id,
        "username":   user.username or "N/A",
        "utr":        utr,
        "plan":       plan_key,
        "price":      plan["price"],
        "status":     "pending",
        "created_at": datetime.utcnow()
    })

    await update.message.reply_text(
        f"✅ <b>Payment Submitted!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔢 <b>UTR Number:</b>\n"
        f"<code>{utr}</code>\n\n"
        f"📦 <b>Plan:</b> {plan['label']}\n"
        f"💰 <b>Amount:</b> ₹{plan['price']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Admin will verify within a few hours\n"
        f"🔔 You will be notified once approved\n\n"
        f"📞 Contact admin if not approved in 24h",
        parse_mode="HTML"
    )

    # Notify admins
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                admin_id,
                f"🔔 <b>New Payment Request!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>User:</b> <code>{user.id}</code>\n"
                f"📛 <b>Username:</b> @{user.username or 'N/A'}\n"
                f"📦 <b>Plan:</b> {plan['label']}\n"
                f"💰 <b>Amount:</b> ₹{plan['price']}\n"
                f"🔢 <b>UTR:</b> <code>{utr}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ Approve: <code>/approve {user.id}</code>\n"
                f"❌ Reject:  <code>/reject {user.id}</code>",
                parse_mode="HTML"
            )
        except Exception:
            pass


# ─────────────────────────────────────────────
#  ADMIN COMMANDS
# ─────────────────────────────────────────────
@admin_only
async def approve_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin approves payment: /approve user_id"""
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /approve <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return

    payment = payments_col.find_one({"user_id": uid, "status": "pending"}, sort=[("created_at", -1)])
    if not payment:
        await update.message.reply_text(f"No pending payment found for {uid}.")
        return

    plan = PREMIUM_PLANS.get(payment.get("plan", "1month"), PREMIUM_PLANS["1month"])
    expiry = grant_premium(uid, plan["days"])

    payments_col.update_one({"_id": payment["_id"]}, {"$set": {"status": "approved"}})

    await update.message.reply_text(f"✅ Approved! User {uid} now has premium until {expiry.strftime('%d %b %Y')}.")
    try:
        await ctx.bot.send_message(
            uid,
            f"🎉 <b>Payment Approved!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ <b>{plan['label']} Premium</b> activated!\n\n"
            f"📅 <b>Expires:</b> {expiry.strftime('%d %b %Y')}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ You can now access all files without verification!\n"
            f"🚀 Enjoy unlimited access!",
            parse_mode="HTML"
        )
    except Exception:
        pass


@admin_only
async def reject_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin rejects payment: /reject user_id"""
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /reject <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return

    payment = payments_col.find_one({"user_id": uid, "status": "pending"}, sort=[("created_at", -1)])
    if not payment:
        await update.message.reply_text(f"No pending payment found for {uid}.")
        return

    payments_col.update_one({"_id": payment["_id"]}, {"$set": {"status": "rejected"}})

    await update.message.reply_text(f"❌ Rejected payment for user {uid}.")
    try:
        await ctx.bot.send_message(
            uid,
            "❌ <b>Payment Rejected</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ Your payment could not be verified.\n\n"
            "📋 <b>Possible reasons:</b>\n"
            "┣ Invalid UTR number\n"
            "┣ Payment not received\n"
            "┗ Wrong UPI ID used\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💬 Contact admin for help\n"
            "🔄 Or try again with correct UTR",
            parse_mode="HTML"
        )
    except Exception:
        pass


@admin_only
async def addpremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin manually adds premium: /addpremium user_id days"""
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /addpremium <user_id> <days>")
        return
    try:
        uid  = int(args[0])
        days = int(args[1])
    except ValueError:
        await update.message.reply_text("Invalid arguments.")
        return
    expiry = grant_premium(uid, days)
    await update.message.reply_text(f"✅ Added {days}-day premium to {uid}. Expires: {expiry.strftime('%d %b %Y')}")
    try:
        await ctx.bot.send_message(uid, f"🎁 Admin gifted you {days}-day Premium! Enjoy! 🚀")
    except Exception:
        pass


@admin_only
async def removepremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin removes premium: /removepremium user_id"""
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /removepremium <user_id>")
        return
    uid = int(args[0])
    revoke_premium(uid)
    await update.message.reply_text(f"✅ Premium removed from {uid}.")


@admin_only
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    total_users    = users_col.count_documents({})
    premium_users  = users_col.count_documents({"premium_expiry": {"$gt": datetime.utcnow()}})
    total_files    = files_col.count_documents({})
    pending_pay    = payments_col.count_documents({"status": "pending"})
    total_referrals= referrals_col.count_documents({})
    concurrent     = get_concurrent_users()
    slots_left     = max(0, MAX_USERS - total_users)
    load_pct       = int((total_users / MAX_USERS) * 100) if MAX_USERS > 0 else 0

    # Load bar
    filled = int(load_pct / 10)
    bar = "█" * filled + "░" * (10 - filled)

    await update.message.reply_text(
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🤖 <b>Server {SERVER_ID}</b>\n\n"
        f"👥 <b>Users</b>\n"
        f"┣ 👤 Total: <b>{total_users} / {MAX_USERS}</b>\n"
        f"┣ 💎 Premium: <b>{premium_users}</b>\n"
        f"┣ ⚡ Active Now: <b>{concurrent}</b>\n"
        f"┗ 🆓 Slots Left: <b>{slots_left}</b>\n\n"
        f"📈 <b>Load</b>\n"
        f"┗ [{bar}] {load_pct}%\n\n"
        f"📁 <b>Content</b>\n"
        f"┣ 🎬 Files: <b>{total_files}</b>\n"
        f"┣ 💳 Pending Payments: <b>{pending_pay}</b>\n"
        f"┗ 👥 Total Referrals: <b>{total_referrals}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Next Bot: {NEXT_BOT_NAME or '❌ Not set'}",
        parse_mode="HTML"
    )


@admin_only
async def files_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    files = list(files_col.find({}, {"file_name": 1, "file_unique_id": 1, "file_type": 1, "created_at": 1}).sort("created_at", -1).limit(10))
    if not files:
        await update.message.reply_text("No files uploaded yet.")
        return
    lines = [f"📁 <b>Last 10 Files:</b>\n"]
    for f in files:
        link = f"https://t.me/{BOT_USERNAME}?start=file_{f['file_unique_id']}"
        lines.append(f"▪️ {f.get('file_name','?')} [{f.get('file_type','?')}]\n   <code>{link}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


@admin_only
async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all users: reply to a message with /broadcast"""
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a message with /broadcast to send it to all users.")
        return

    reply_msg = update.message.reply_to_message
    all_users = users_col.find({}, {"user_id": 1})
    user_ids  = [u["user_id"] for u in all_users]

    status_msg = await update.message.reply_text(f"📡 Broadcasting to {len(user_ids)} users...")
    success, failed = 0, 0

    for uid in user_ids:
        try:
            await reply_msg.copy(chat_id=uid)
            success += 1
            await asyncio.sleep(0.05)  # Rate limit: 20 msg/sec
        except Exception:
            failed += 1

    await status_msg.edit_text(
        f"✅ Broadcast complete!\n✔️ Delivered: {success}\n❌ Failed: {failed}"
    )


# ─────────────────────────────────────────────
#  FLASK WEB APP (Streaming)
# ─────────────────────────────────────────────
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET

WATCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{ file_name }}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg:        #0a0a0f;
      --surface:   #12121a;
      --card:      #1a1a27;
      --border:    #ffffff12;
      --accent:    #6c63ff;
      --accent2:   #ff6584;
      --text:      #e8e8f0;
      --muted:     #6b6b80;
      --glow:      rgba(108,99,255,0.35);
    }

    html, body {
      height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', sans-serif;
      overflow-x: hidden;
    }

    /* Animated background */
    body::before {
      content: '';
      position: fixed;
      inset: 0;
      background:
        radial-gradient(ellipse 80% 50% at 20% 10%, rgba(108,99,255,0.12) 0%, transparent 60%),
        radial-gradient(ellipse 60% 40% at 80% 90%, rgba(255,101,132,0.08) 0%, transparent 60%);
      pointer-events: none;
      z-index: 0;
    }

    .page {
      position: relative;
      z-index: 1;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 24px 16px 40px;
    }

    /* Top bar */
    .topbar {
      width: 100%;
      max-width: 820px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 28px;
    }

    .logo {
      display: flex;
      align-items: center;
      gap: 10px;
      text-decoration: none;
    }

    .logo-icon {
      width: 36px; height: 36px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      border-radius: 10px;
      display: flex; align-items: center; justify-content: center;
      font-size: 18px;
      box-shadow: 0 0 16px var(--glow);
    }

    .logo-text {
      font-size: 1rem;
      font-weight: 700;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      letter-spacing: -0.3px;
    }

    .badge {
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      color: #fff;
      font-size: 0.7rem;
      font-weight: 600;
      padding: 4px 10px;
      border-radius: 20px;
      letter-spacing: 0.5px;
      text-transform: uppercase;
    }

    /* Player card */
    .player-card {
      width: 100%;
      max-width: 820px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 20px;
      overflow: hidden;
      box-shadow:
        0 0 0 1px var(--border),
        0 20px 60px rgba(0,0,0,0.6),
        0 0 80px rgba(108,99,255,0.08);
    }

    /* Video container */
    .video-wrap {
      position: relative;
      width: 100%;
      background: #000;
      border-radius: 0;
    }

    video {
      width: 100%;
      display: block;
      max-height: 480px;
      background: #000;
    }

    /* Gradient overlay at bottom of video */
    .video-wrap::after {
      content: '';
      position: absolute;
      bottom: 0; left: 0; right: 0;
      height: 4px;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
    }

    /* Info section */
    .info {
      padding: 20px 24px 16px;
      border-bottom: 1px solid var(--border);
    }

    .file-title {
      font-size: 1.05rem;
      font-weight: 600;
      color: var(--text);
      letter-spacing: -0.2px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .file-meta {
      margin-top: 6px;
      font-size: 0.78rem;
      color: var(--muted);
      display: flex;
      gap: 16px;
    }

    .file-meta span {
      display: flex;
      align-items: center;
      gap: 5px;
    }

    /* Actions */
    .actions {
      padding: 16px 24px 20px;
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 20px;
      border-radius: 10px;
      font-size: 0.85rem;
      font-weight: 600;
      text-decoration: none;
      cursor: pointer;
      border: none;
      transition: all 0.2s ease;
    }

    .btn-primary {
      background: linear-gradient(135deg, var(--accent), #8b7cf8);
      color: #fff;
      box-shadow: 0 4px 16px rgba(108,99,255,0.35);
    }

    .btn-primary:hover {
      transform: translateY(-1px);
      box-shadow: 0 6px 24px rgba(108,99,255,0.5);
    }

    .btn-ghost {
      background: var(--surface);
      color: var(--text);
      border: 1px solid var(--border);
    }

    .btn-ghost:hover {
      background: var(--card);
      border-color: var(--accent);
      color: var(--accent);
    }

    /* Stats bar */
    .stats {
      margin: 0 24px 20px;
      padding: 14px 18px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      display: flex;
      gap: 24px;
      flex-wrap: wrap;
    }

    .stat {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    .stat-label {
      font-size: 0.68rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.8px;
      font-weight: 500;
    }

    .stat-value {
      font-size: 0.88rem;
      font-weight: 600;
      color: var(--text);
    }

    .stat-value.accent {
      color: var(--accent);
    }

    /* Loading state */
    .loader {
      display: none;
      position: absolute;
      inset: 0;
      background: rgba(10,10,15,0.85);
      align-items: center;
      justify-content: center;
      flex-direction: column;
      gap: 16px;
      z-index: 10;
    }

    .spinner {
      width: 44px; height: 44px;
      border: 3px solid var(--border);
      border-top-color: var(--accent);
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }

    @keyframes spin { to { transform: rotate(360deg); } }

    .loader-text {
      font-size: 0.85rem;
      color: var(--muted);
    }

    /* Footer */
    .footer {
      margin-top: 28px;
      text-align: center;
      font-size: 0.75rem;
      color: var(--muted);
      line-height: 1.7;
    }

    .footer a {
      color: var(--accent);
      text-decoration: none;
    }

    /* Progress glow effect */
    video::-webkit-media-controls-timeline {
      background: linear-gradient(90deg, var(--accent), var(--accent2));
    }

    /* Mobile */
    @media (max-width: 600px) {
      .page { padding: 16px 12px 32px; }
      .topbar { margin-bottom: 16px; }
      .info { padding: 16px 16px 12px; }
      .actions { padding: 12px 16px 16px; gap: 8px; }
      .stats { margin: 0 16px 16px; gap: 16px; }
      video { max-height: 240px; }
      .file-title { font-size: 0.95rem; }
    }
  </style>
</head>
<body>
  <div class="page">

    <!-- Top bar -->
    <div class="topbar">
      <a class="logo" href="#">
        <div class="logo-icon">🎬</div>
        <span class="logo-text">FileBot</span>
      </a>
      <span class="badge">Premium Stream</span>
    </div>

    <!-- Player Card -->
    <div class="player-card">

      <!-- Video -->
      <div class="video-wrap">
        <div class="loader" id="loader">
          <div class="spinner"></div>
          <span class="loader-text">Loading stream...</span>
        </div>
        <video
          id="player"
          controls
          autoplay
          preload="metadata"
          playsinline
        >
          <source src="{{ stream_url }}" type="video/mp4">
          <source src="{{ stream_url }}" type="video/webm">
          Your browser does not support HTML5 video.
        </video>
      </div>

      <!-- File info -->
      <div class="info">
        <div class="file-title">{{ file_name }}</div>
        <div class="file-meta">
          <span>🔒 Secured Stream</span>
          <span>📡 Proxy Protected</span>
          <span id="quality-label">🎥 HD Ready</span>
        </div>
      </div>

      <!-- Stats -->
      <div class="stats">
        <div class="stat">
          <span class="stat-label">Status</span>
          <span class="stat-value accent" id="stat-status">Connecting...</span>
        </div>
        <div class="stat">
          <span class="stat-label">Duration</span>
          <span class="stat-value" id="stat-duration">--:--</span>
        </div>
        <div class="stat">
          <span class="stat-label">Resolution</span>
          <span class="stat-value" id="stat-res">--</span>
        </div>
        <div class="stat">
          <span class="stat-label">Buffered</span>
          <span class="stat-value" id="stat-buf">0%</span>
        </div>
      </div>

      <!-- Actions -->
      <div class="actions">
        <a class="btn btn-primary" href="{{ stream_url }}" download="{{ file_name }}">
          ⬇️ Download
        </a>
        <button class="btn btn-ghost" onclick="toggleFullscreen()">
          ⛶ Fullscreen
        </button>
        <button class="btn btn-ghost" onclick="copyLink()">
          🔗 Copy Link
        </button>
      </div>

    </div>

    <!-- Footer -->
    <div class="footer">
      Powered by <strong>FileBot</strong> · Secure Streaming<br>
      🔒 Your access is protected &amp; encrypted
    </div>

  </div>

  <script>
    const video = document.getElementById('player');
    const loader = document.getElementById('loader');

    // Show loader while buffering
    video.addEventListener('waiting', () => {
      loader.style.display = 'flex';
    });
    video.addEventListener('playing', () => {
      loader.style.display = 'none';
      document.getElementById('stat-status').textContent = '▶ Playing';
      document.getElementById('stat-status').style.color = '#4ade80';
    });
    video.addEventListener('pause', () => {
      document.getElementById('stat-status').textContent = '⏸ Paused';
      document.getElementById('stat-status').style.color = '#facc15';
    });
    video.addEventListener('error', () => {
      loader.style.display = 'none';
      document.getElementById('stat-status').textContent = '✗ Error';
      document.getElementById('stat-status').style.color = '#f87171';
    });

    // Duration
    video.addEventListener('loadedmetadata', () => {
      const d = video.duration;
      if (isFinite(d)) {
        const m = Math.floor(d / 60);
        const s = Math.floor(d % 60).toString().padStart(2, '0');
        document.getElementById('stat-duration').textContent = m + ':' + s;
      }
      // Resolution
      if (video.videoWidth) {
        document.getElementById('stat-res').textContent = video.videoWidth + '×' + video.videoHeight;
      }
      loader.style.display = 'none';
    });

    // Buffer progress
    setInterval(() => {
      if (video.buffered.length > 0 && video.duration) {
        const pct = Math.round((video.buffered.end(video.buffered.length - 1) / video.duration) * 100);
        document.getElementById('stat-buf').textContent = pct + '%';
      }
    }, 1000);

    // Fullscreen
    function toggleFullscreen() {
      if (document.fullscreenElement) {
        document.exitFullscreen();
      } else {
        video.requestFullscreen().catch(() => {
          video.webkitEnterFullscreen && video.webkitEnterFullscreen();
        });
      }
    }

    // Copy link
    function copyLink() {
      navigator.clipboard.writeText(window.location.href).then(() => {
        const btn = event.target;
        btn.textContent = '✅ Copied!';
        setTimeout(() => btn.textContent = '🔗 Copy Link', 2000);
      });
    }

    // Keyboard shortcuts
    document.addEventListener('keydown', e => {
      if (e.code === 'Space') { e.preventDefault(); video.paused ? video.play() : video.pause(); }
      if (e.code === 'ArrowRight') video.currentTime += 10;
      if (e.code === 'ArrowLeft') video.currentTime -= 10;
      if (e.code === 'ArrowUp') video.volume = Math.min(1, video.volume + 0.1);
      if (e.code === 'ArrowDown') video.volume = Math.max(0, video.volume - 0.1);
      if (e.code === 'KeyF') toggleFullscreen();
    });
  </script>
</body>
</html>"""


# File doc cache — avoids repeated MongoDB lookups for same file
_file_cache: dict = {}
_file_cache_lock = threading.Lock()

def get_file_cached(file_unique_id: str) -> dict | None:
    """Get file doc with 5-min cache to reduce MongoDB hits."""
    now = time.time()
    with _file_cache_lock:
        cached = _file_cache.get(file_unique_id)
        if cached and now - cached[1] < 300:  # 5 min TTL
            return cached[0]
    doc = get_file(file_unique_id)
    if doc:
        with _file_cache_lock:
            _file_cache[file_unique_id] = (doc, now)
            # Prune cache if too large
            if len(_file_cache) > 500:
                oldest = sorted(_file_cache.items(), key=lambda x: x[1][1])[:100]
                for k, _ in oldest:
                    _file_cache.pop(k, None)
    return doc

def verify_stream_access(file_unique_id: str) -> tuple[dict | None, str | None]:
    """Verify streaming access — optimized with caching."""
    token       = request.args.get("t", "")
    user_id_raw = request.args.get("u", "")

    if not user_id_raw:
        return None, "Missing auth"

    # HMAC signature check (pure CPU — very fast)
    expected = hashlib.sha256(
        f"{user_id_raw}:{file_unique_id}:{FLASK_SECRET}".encode()
    ).hexdigest()[:16]
    if token != expected:
        return None, "Invalid token"

    try:
        uid = int(user_id_raw)
    except ValueError:
        return None, "Bad user ID"

    # Token check (uses in-memory cache — no DB hit if cached)
    if not has_valid_token(uid):
        return None, "No valid token"

    # File doc (cached — no DB hit on repeat requests)
    file_doc = get_file_cached(file_unique_id)
    if not file_doc:
        return None, "File not found"

    return file_doc, None


@flask_app.route("/watch")
def watch():
    file_unique_id = request.args.get("id", "")
    if not file_unique_id:
        abort(400)

    file_doc, err = verify_stream_access(file_unique_id)
    if err:
        return f"""
        <html><body style="background:#0a0a0f;color:#e8e8f0;font-family:Inter,sans-serif;
        display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px">
        <div style="font-size:48px">🔒</div>
        <h2 style="color:#f87171">Access Denied</h2>
        <p style="color:#6b6b80">{err}</p>
        <p style="color:#6b6b80;font-size:0.85rem">Open the file link via Telegram bot</p>
        </body></html>
        """, 403

    u = request.args.get("u", "")
    t = request.args.get("t", "")
    stream_url = f"/stream?id={file_unique_id}&u={u}&t={t}"

    resp = make_response(render_template_string(
        WATCH_HTML,
        file_name=file_doc.get("file_name", "Video"),
        stream_url=stream_url
    ))
    # Cache HTML page for 5 min (static per user)
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp


# ── Pixeldrain MIME map (pre-built, zero runtime cost) ─────────────
_MIME = {
    ".mp4": "video/mp4", ".m4v": "video/mp4",
    ".mkv": "video/x-matroska", ".webm": "video/webm",
    ".avi": "video/x-msvideo", ".mov": "video/quicktime",
    ".ts":  "video/mp2t",      ".flv": "video/x-flv",
    ".wmv": "video/x-ms-wmv",  ".3gp": "video/3gpp",
}

# Persistent requests.Session — reuses TCP connection to Pixeldrain
_pd_session = requests.Session()
_pd_session.headers.update({
    "Authorization": PD_AUTH_HEADER,
    "User-Agent":    "FileBot/1.0",
})
# Connection pool: 20 keep-alive connections
adapter = requests.adapters.HTTPAdapter(
    pool_connections=20,
    pool_maxsize=20,
    max_retries=1
)
_pd_session.mount("https://", adapter)

@flask_app.route("/stream")
def stream():
    file_unique_id = request.args.get("id", "")
    if not file_unique_id:
        abort(400)

    # Auth check
    file_doc, err = verify_stream_access(file_unique_id)
    if err:
        abort(403)

    pd_id = file_doc.get("pixeldrain_id")
    if not pd_id:
        abort(404)

    # Range header (default full file)
    range_header = request.headers.get("Range", "bytes=0-")
    pd_url = f"https://pixeldrain.com/api/file/{pd_id}"

    try:
        # Single GET — persistent session (TCP reuse = faster)
        pd_resp = _pd_session.get(
            pd_url,
            headers={"Range": range_header},
            stream=True,
            timeout=(3, 60)   # (connect_timeout, read_timeout)
        )

        # MIME type — from extension first (fastest), then Content-Type header
        fname = file_doc.get("file_name", "").lower()
        ext   = "." + fname.rsplit(".", 1)[-1] if "." in fname else ""
        ct    = _MIME.get(ext) or pd_resp.headers.get("Content-Type", "video/mp4")
        if not ct.startswith("video/"):
            ct = "video/mp4"

        # Build response headers
        resp_h = {
            "Accept-Ranges":  "bytes",
            "Content-Type":   ct,
            "Cache-Control":  "no-cache",
            "X-Accel-Buffering": "no",   # Disable nginx buffering
        }
        cl = pd_resp.headers.get("Content-Length")
        cr = pd_resp.headers.get("Content-Range")
        if cl: resp_h["Content-Length"] = cl
        if cr: resp_h["Content-Range"]  = cr

        status = pd_resp.status_code  # 206 partial or 200 full

        # Streaming generator — 512KB chunks for fast throughput
        def generate():
            try:
                for chunk in pd_resp.iter_content(chunk_size=524288):
                    if chunk:
                        yield chunk
            except Exception as ex:
                logger.debug(f"Stream closed: {ex}")
            finally:
                pd_resp.close()

        return Response(
            generate(),
            status=status,
            headers=resp_h,
            content_type=ct,
            direct_passthrough=True
        )

    except requests.exceptions.Timeout:
        logger.warning(f"Pixeldrain timeout: {pd_id}")
        abort(504)
    except Exception as e:
        logger.error(f"Stream error [{pd_id}]: {e}")
        abort(502)


@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@flask_app.route("/test-stream/<pd_id>")
def test_stream(pd_id):
    """Debug route — test Pixeldrain file info + streaming."""
    try:
        # Check file info first
        info_resp = requests.get(
            f"https://pixeldrain.com/api/file/{pd_id}/info",
            auth=("", PIXELDRAIN_API_KEY),
            timeout=15
        )
        info = info_resp.json() if info_resp.status_code == 200 else {}

        # Try streaming
        stream_resp = _pd_session.get(
            f"https://pixeldrain.com/api/file/{pd_id}",
            headers={"Range": "bytes=0-1024"},
            stream=True,
            timeout=10
        )
        stream_resp.close()

        return jsonify({
            "pd_id":          pd_id,
            "info_status":    info_resp.status_code,
            "file_name":      info.get("name"),
            "file_size":      info.get("size"),
            "file_type":      info.get("mime_type"),
            "stream_status":  stream_resp.status_code,
            "stream_ct":      stream_resp.headers.get("Content-Type"),
            "stream_cl":      stream_resp.headers.get("Content-Length"),
            "auth_header_set": bool(PD_AUTH_HEADER),
            "ok":             info_resp.status_code == 200 and stream_resp.status_code in [200, 206]
        })
    except Exception as e:
        return jsonify({"error": str(e), "pd_id": pd_id}), 500


@flask_app.route("/")
def index():
    return f"<h2>📁 FileBot is running!</h2><p>Use the Telegram bot @{BOT_USERNAME}</p>"


# ─────────────────────────────────────────────
#  GENERATE SIGNED WATCH URL (for bot)
# ─────────────────────────────────────────────
def make_watch_url(user_id: int, file_unique_id: str) -> str:
    """Generate a signed watch URL for a user."""
    sig = hashlib.sha256(f"{user_id}:{file_unique_id}:{FLASK_SECRET}".encode()).hexdigest()[:16]
    return f"{BASE_URL}/watch?id={file_unique_id}&u={user_id}&t={sig}"


# ─────────────────────────────────────────────
#  PATCH handle_file_request to use signed URL
# ─────────────────────────────────────────────
# We override the stream URL creation with a signed URL
_original_handle_file_request = handle_file_request

async def _patched_handle_file_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_unique_id: str):
    user = update.effective_user
    file_doc = get_file(file_unique_id)

    if not file_doc:
        await update.message.reply_text("❌ File not found or has been removed.")
        return

    if not has_valid_token(user.id):
        await send_verification_prompt(update, ctx, file_unique_id)
        return

    msg = await update.message.reply_text("⏳ Checking file availability...")
    pd_id = file_doc.get("pixeldrain_id")
    last_checked = file_doc.get("last_checked")
    need_check = (
        not pd_id or
        not last_checked or
        (datetime.utcnow() - last_checked).total_seconds() > 3600
    )

    if need_check and pd_id:
        if not check_pixeldrain_file(pd_id):
            await msg.edit_text("🔄 Re-uploading file, please wait (30-60s)...")
            pd_id = await re_upload_file(file_doc)
            if not pd_id:
                await msg.edit_text("❌ File unavailable. Please contact admin.")
                return
    elif not pd_id:
        await msg.edit_text("🔄 Processing file, please wait...")
        pd_id = await re_upload_file(file_doc)
        if not pd_id:
            await msg.edit_text("❌ File unavailable. Please contact admin.")
            return

    files_col.update_one(
        {"file_unique_id": file_unique_id},
        {"$set": {"last_checked": datetime.utcnow(), "pixeldrain_id": pd_id}}
    )

    file_type = file_doc.get("file_type", "video")

    if file_type == "video":
        watch_url = make_watch_url(user.id, file_unique_id)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Watch Now", url=watch_url)
        ]])
        await msg.edit_text(
            f"🎬 <b>{file_doc.get('file_name', 'Video')}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ File ready to stream!\n\n"
            f"🔒 Personal secure link\n"
            f"⏳ Expires with your token\n"
            f"🤖 Served by Bot {SERVER_ID}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 Click to watch now:",
            parse_mode="HTML",
            reply_markup=kb
        )
    else:
        try:
            await ctx.bot.send_document(
                chat_id=user.id,
                document=file_doc["telegram_file_id"],
                caption=f"📁 <b>{file_doc.get('file_name', 'File')}</b>",
                parse_mode="HTML"
            )
            await msg.delete()
        except Exception as e:
            logger.error(f"Send document error: {e}")
            await msg.edit_text("❌ Failed to send file. Try again later.")

# Replace with patched version
handle_file_request = _patched_handle_file_request


# ─────────────────────────────────────────────
#  BOT APPLICATION SETUP
# ─────────────────────────────────────────────
@admin_only
async def addfile_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Add large file via Pixeldrain ID (bypasses Telegram 20MB limit).
    Usage: /addfile <pixeldrain_id> <filename>
    Example: /addfile AbCdEfGh lecture_video.mp4
    """
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "📋 <b>Usage:</b>\n"
            "<code>/addfile PIXELDRAIN_ID filename.mp4</code>\n\n"
            "<b>How to get Pixeldrain ID:</b>\n"
            "1. Go to pixeldrain.com\n"
            "2. Upload your file\n"
            "3. Copy ID from URL:\n"
            "   pixeldrain.com/u/<b>AbCdEfGh</b>\n\n"
            "<b>Example:</b>\n"
            "<code>/addfile AbCdEfGh my_video.mp4</code>",
            parse_mode="HTML"
        )
        return

    pd_id     = args[0].strip()
    file_name = " ".join(args[1:]).strip() if len(args) > 1 else f"file_{pd_id}"
    file_type = "video" if any(file_name.lower().endswith(e) for e in [".mp4",".mkv",".webm",".avi",".mov"]) else "document"

    msg = await update.message.reply_text("🔍 Verifying Pixeldrain file...")

    # Verify file exists on Pixeldrain
    try:
        verify = requests.get(
            f"https://pixeldrain.com/api/file/{pd_id}/info",
            auth=("", PIXELDRAIN_API_KEY),
            timeout=15
        )
        if verify.status_code != 200:
            await msg.edit_text(
                f"❌ <b>Pixeldrain file not found!</b>\n\n"
                f"ID: <code>{pd_id}</code>\n"
                f"Status: {verify.status_code}\n\n"
                "Make sure the file is public on Pixeldrain.",
                parse_mode="HTML"
            )
            return

        info      = verify.json()
        real_name = info.get("name", file_name)
        size_mb   = info.get("size", 0) / 1024 / 1024

        # Use real name if filename not provided
        if len(args) == 1:
            file_name = real_name
            file_type = "video" if any(file_name.lower().endswith(e) for e in [".mp4",".mkv",".webm",".avi",".mov"]) else "document"

    except Exception as e:
        await msg.edit_text(f"❌ Verification error: {e}")
        return

    # Generate unique ID for this file
    import uuid as _uuid
    file_unique_id = _uuid.uuid4().hex[:20]

    # Save to DB (no telegram_file_id since uploaded directly)
    files_col.update_one(
        {"file_unique_id": file_unique_id},
        {"$set": {
            "file_unique_id":    file_unique_id,
            "telegram_file_id":  "",           # Empty — direct PD upload
            "pixeldrain_id":     pd_id,
            "file_type":         file_type,
            "file_name":         file_name,
            "created_at":        datetime.utcnow(),
            "last_checked":      datetime.utcnow(),
            "size_mb":           round(size_mb, 2),
        }},
        upsert=True
    )

    universal_link = f"https://t.me/{BOT_USERNAME}?start=file_{file_unique_id}"

    await msg.edit_text(
        f"✅ <b>File Added Successfully!</b>\n\n"
        f"📁 <b>Name:</b> <code>{file_name}</code>\n"
        f"📦 <b>Size:</b> {size_mb:.1f} MB\n"
        f"🆔 <b>Pixeldrain ID:</b> <code>{pd_id}</code>\n\n"
        f"🔗 <b>Universal Share Link:</b>\n"
        f"<code>{universal_link}</code>\n\n"
        "✅ This ONE link works on ALL bots!\n"
        "📌 Share only this link — nothing else needed.",
        parse_mode="HTML"
    )
    logger.info(f"Admin {update.effective_user.id} added file via PD: {pd_id} → {file_name}")


def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Public commands
    app.add_handler(CommandHandler("start",     start))
    app.add_handler(CommandHandler("mystatus",  mystatus))
    app.add_handler(CommandHandler("referral",  referral_cmd))
    app.add_handler(CommandHandler("premium",   premium_cmd))
    app.add_handler(CommandHandler("utr",       utr_cmd))

    # Admin commands
    app.add_handler(CommandHandler("stats",         stats_cmd))
    app.add_handler(CommandHandler("files",         files_cmd))
    app.add_handler(CommandHandler("broadcast",     broadcast_cmd))
    app.add_handler(CommandHandler("approve",       approve_cmd))
    app.add_handler(CommandHandler("reject",        reject_cmd))
    app.add_handler(CommandHandler("addpremium",    addpremium_cmd))
    app.add_handler(CommandHandler("removepremium", removepremium_cmd))
    app.add_handler(CommandHandler("addfile",       addfile_cmd))

    # File upload handler (admin)
    app.add_handler(MessageHandler(
        filters.User(ADMIN_IDS) & (filters.VIDEO | filters.Document.ALL | filters.AUDIO),
        handle_upload
    ))

    # Inline buttons
    app.add_handler(CallbackQueryHandler(callback_handler))

    return app



# ─────────────────────────────────────────────
#  MAIN ENTRY POINT
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
#  WEBHOOK MODE — No Conflict on Render
# ─────────────────────────────────────────────

# Global state for webhook mode
_bot_application = None
_bot_loop = None


async def init_application():
    """Initialize bot application for webhook mode."""
    global _bot_application
    _bot_application = build_application()
    await _bot_application.initialize()
    await _bot_application.start()
    logger.info("✅ Bot application initialized for webhook mode.")


def process_update_sync(data: dict):
    """Process a Telegram update using the shared persistent event loop."""
    global _bot_application, _bot_loop
    from telegram import Update as TGUpdate
    try:
        update = TGUpdate.de_json(data, _bot_application.bot)
        future = asyncio.run_coroutine_threadsafe(
            _bot_application.process_update(update),
            _bot_loop
        )
        future.result(timeout=60)  # Wait up to 60s for processing
    except Exception as e:
        logger.error(f"Update processing error: {e}")


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    """Receive updates from Telegram via webhook."""
    global _bot_application
    if _bot_application is None:
        logger.error("Bot application not initialized")
        abort(503)

    data = request.get_json(force=True, silent=True)
    if not data:
        abort(400)

    # Process in background thread to return 200 quickly to Telegram
    t = threading.Thread(target=process_update_sync, args=(data,), daemon=True)
    t.start()

    return "OK", 200


def run_event_loop(loop: asyncio.AbstractEventLoop):
    """Run the asyncio event loop in a dedicated background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


def main():
    global _bot_loop

    logger.info("🚀 Starting Telegram FileBot + Flask Streaming Server (Webhook Mode)")

    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set!")
        return
    if not ADMIN_IDS:
        logger.warning("⚠️  No ADMIN_IDS set. Admin commands won't work.")
    if not BASE_URL:
        logger.error("❌ BASE_URL not set! Required for webhook.")
        return

    # Create a persistent event loop that runs in its own thread
    _bot_loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_event_loop, args=(_bot_loop,), daemon=True)
    loop_thread.start()
    logger.info("✅ Async event loop started in background thread.")

    # Initialize bot application in the persistent loop
    future = asyncio.run_coroutine_threadsafe(init_application(), _bot_loop)
    future.result(timeout=30)

    # Set webhook with Telegram
    webhook_url = f"{BASE_URL}/webhook/{BOT_TOKEN}"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json={
                "url": webhook_url,
                "drop_pending_updates": True,
                "allowed_updates": [
                    "message", "callback_query", "inline_query",
                    "chosen_inline_result", "channel_post"
                ]
            },
            timeout=15
        )
        result = resp.json()
        if result.get("ok"):
            logger.info(f"✅ Webhook set: {webhook_url}")
        else:
            logger.error(f"❌ Webhook setup failed: {result}")
    except Exception as e:
        logger.error(f"❌ Could not set webhook: {e}")

    logger.info(f"✅ Starting Flask on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
