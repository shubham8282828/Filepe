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

from flask import Flask, request, jsonify, Response, render_template_string, abort
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
REFERRAL_REWARD_HOURS   = int(os.environ.get("REFERRAL_REWARD_HOURS", "6"))   # token hours per referral
REFERRAL_PREMIUM_COUNT  = int(os.environ.get("REFERRAL_PREMIUM_COUNT", "10")) # referrals for 7-day premium

# ─────────────────────────────────────────────
#  MONGODB SETUP
# ─────────────────────────────────────────────
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo_client["filebot"]

# Collections
users_col    = db["users"]
files_col    = db["files"]
payments_col = db["payments"]
referrals_col= db["referrals"]

def setup_indexes():
    """Create indexes for fast queries."""
    users_col.create_index("user_id", unique=True)
    users_col.create_index("token_expiry")
    files_col.create_index("file_unique_id", unique=True)
    files_col.create_index("created_at")
    payments_col.create_index("utr")
    payments_col.create_index([("user_id", ASCENDING), ("status", ASCENDING)])
    referrals_col.create_index([("referrer_id", ASCENDING), ("referred_user_id", ASCENDING)], unique=True)
    logger.info("✅ MongoDB indexes created.")

setup_indexes()

# ─────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────
def get_user(user_id: int) -> dict:
    """Fetch or create a user document."""
    user = users_col.find_one({"user_id": user_id})
    if not user:
        user = {
            "user_id":        user_id,
            "token_expiry":   None,
            "premium_expiry": None,
            "referrals_count": 0,
            "referred_by":    None,
            "joined_at":      datetime.utcnow(),
        }
        users_col.insert_one(user)
    return user

def is_premium(user_id: int) -> bool:
    """Return True if user has active premium."""
    user = get_user(user_id)
    exp = user.get("premium_expiry")
    return exp is not None and exp > datetime.utcnow()

def has_valid_token(user_id: int) -> bool:
    """Return True if user has valid token OR premium."""
    if is_premium(user_id):
        return True
    user = get_user(user_id)
    exp = user.get("token_expiry")
    return exp is not None and exp > datetime.utcnow()

def grant_token(user_id: int, hours: int = TOKEN_VALIDITY_HOURS):
    """Give user a verification token valid for `hours`."""
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"token_expiry": datetime.utcnow() + timedelta(hours=hours)}}
    )

def grant_premium(user_id: int, days: int):
    """Give user premium for `days` days (stacks on existing)."""
    user = get_user(user_id)
    base = user.get("premium_expiry") or datetime.utcnow()
    if base < datetime.utcnow():
        base = datetime.utcnow()
    new_expiry = base + timedelta(days=days)
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": new_expiry}}
    )
    return new_expiry

def revoke_premium(user_id: int):
    users_col.update_one({"user_id": user_id}, {"$set": {"premium_expiry": None}})

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
    """Grant rewards based on referral count milestones."""
    user = get_user(referrer_id)
    count = user.get("referrals_count", 0)
    # Every 5 referrals → 6 hours token
    if count % 5 == 0:
        grant_token(referrer_id, hours=REFERRAL_REWARD_HOURS * (count // 5))
    # 10 referrals → 7 days premium
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
            logger.info(f"Pixeldrain upload attempt {attempt}/{max_retries}: {filename}")
            resp = requests.post(
                PD_UPLOAD_URL,
                files={"file": (filename, io.BytesIO(file_bytes), "application/octet-stream")},
                auth=("", PIXELDRAIN_API_KEY),
                timeout=120
            )
            if resp.status_code == 201:
                pd_id = resp.json().get("id")
                logger.info(f"✅ Uploaded to Pixeldrain: {pd_id}")
                return pd_id
            else:
                logger.warning(f"Pixeldrain upload failed [{resp.status_code}]: {resp.text}")
        except requests.exceptions.Timeout:
            logger.error(f"Pixeldrain upload timeout on attempt {attempt}")
        except Exception as e:
            logger.error(f"Pixeldrain upload error: {e}")
        time.sleep(2 * attempt)
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
    args = ctx.args  # list of arguments after /start

    # Ensure user exists in DB
    get_user(user.id)

    payload = args[0] if args else ""

    # ── Referral handling ────────────────────
    if payload.startswith("ref_"):
        try:
            referrer_id = int(payload[4:])
            if record_referral(referrer_id, user.id):
                reward_type = check_referral_rewards(referrer_id)
                logger.info(f"Referral: {referrer_id} → {user.id} | reward: {reward_type}")
                # Notify referrer
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
        await handle_file_request(update, ctx, file_unique_id)
        return

    # ── Token verify callback ─────────────────
    if payload.startswith("verify_"):
        uid = int(payload[7:])
        if uid == user.id:
            grant_token(user.id)
            await update.message.reply_text(
                "✅ <b>Token Verified!</b>\n\n"
                "You now have access for the next 24 hours.\n"
                "Click your file link again to download.",
                parse_mode="HTML"
            )
            return

    await send_welcome(update, ctx)


async def send_welcome(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    text = (
        f"👋 <b>Welcome, {user.first_name}!</b>\n\n"
        f"🤖 I'm a File Sharing Bot with streaming support.\n\n"
        f"<b>Commands:</b>\n"
        f"▪️ /premium - Get premium access\n"
        f"▪️ /referral - Your referral link\n"
        f"▪️ /mystatus - Your token & premium status\n\n"
        f"🔗 <b>Your Referral Link:</b>\n<code>{ref_link}</code>\n"
        f"Earn rewards for every friend you refer!"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu"),
        InlineKeyboardButton("📤 Share Referral", url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}")
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
        stream_url = f"{BASE_URL}/watch?id={file_unique_id}"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Watch Now", url=stream_url)
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
        "🔐 <b>Verification Required</b>\n\n"
        "To access this file, you need to verify yourself.\n\n"
        "📋 <b>Steps:</b>\n"
        "1. Click the button below\n"
        "2. Complete the short page\n"
        "3. Come back and click your file link again\n\n"
        "⏳ Token valid for 24 hours after verification."
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

    if msg.video:
        tg_file = msg.video
        file_type = "video"
        file_name = msg.video.file_name or f"video_{int(time.time())}.mp4"
    elif msg.document:
        tg_file = msg.document
        file_type = "document"
        file_name = msg.document.file_name or f"document_{int(time.time())}"
    elif msg.audio:
        tg_file = msg.audio
        file_type = "audio"
        file_name = msg.audio.file_name or f"audio_{int(time.time())}.mp3"
    else:
        return  # Not a supported file type

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

        deep_link = f"https://t.me/{BOT_USERNAME}?start=file_{file_unique_id}"

        await processing_msg.edit_text(
            f"✅ <b>File Uploaded Successfully!</b>\n\n"
            f"📁 Name: <code>{file_name}</code>\n"
            f"🔗 Share Link:\n<code>{deep_link}</code>\n\n"
            f"Copy this link and share it with users.",
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

    token_exp = db_user.get("token_expiry")
    prem_exp  = db_user.get("premium_expiry")
    refs      = db_user.get("referrals_count", 0)

    token_str = "❌ No active token"
    if token_exp and token_exp > datetime.utcnow():
        remaining = token_exp - datetime.utcnow()
        h, rem = divmod(int(remaining.total_seconds()), 3600)
        m = rem // 60
        token_str = f"✅ Valid ({h}h {m}m remaining)"

    prem_str = "❌ Not premium"
    if prem_exp and prem_exp > datetime.utcnow():
        remaining = prem_exp - datetime.utcnow()
        prem_str = f"💎 Active until {prem_exp.strftime('%d %b %Y')}"

    text = (
        f"📊 <b>Your Status</b>\n\n"
        f"🔐 Token: {token_str}\n"
        f"👑 Premium: {prem_str}\n"
        f"👥 Referrals: {refs}\n"
        f"📅 Joined: {db_user.get('joined_at', 'N/A')}"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def referral_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = get_user(user.id)
    refs = db_user.get("referrals_count", 0)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"

    text = (
        f"👥 <b>Your Referral Program</b>\n\n"
        f"🔗 Your Link:\n<code>{ref_link}</code>\n\n"
        f"📊 Total Referrals: <b>{refs}</b>\n\n"
        f"🎁 <b>Rewards:</b>\n"
        f"▪️ Every 5 referrals → Bonus token hours\n"
        f"▪️ {REFERRAL_PREMIUM_COUNT} referrals → 7-day Premium FREE!\n\n"
        f"Share your link to earn rewards! 🚀"
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
        f"▪️ <b>{v['label']}</b> – ₹{v['price']}" for v in PREMIUM_PLANS.values()
    ])
    text = (
        f"💎 <b>Premium Plans</b>\n\n"
        f"{plan_lines}\n\n"
        f"✅ Benefits:\n"
        f"  • Skip daily verification\n"
        f"  • Unlimited file access\n"
        f"  • Priority support\n\n"
        f"Select a plan to proceed:"
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
            f"💳 <b>UPI Payment</b>\n\n"
            f"Plan: <b>{plan['label']}</b> – ₹{plan['price']}\n\n"
            f"📲 Pay to UPI ID:\n<code>{UPI_ID}</code>\n\n"
            f"After payment, send your UTR number:\n"
            f"/utr &lt;your UTR&gt;\n\n"
            f"Example: <code>/utr 123456789012</code>\n\n"
            f"⚠️ Do NOT close this chat until approved."
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
        f"✅ <b>Payment Submitted!</b>\n\n"
        f"UTR: <code>{utr}</code>\n"
        f"Plan: <b>{plan['label']}</b>\n\n"
        f"⏳ Admin will verify within a few hours.\n"
        f"You'll be notified once approved.",
        parse_mode="HTML"
    )

    # Notify admins
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                admin_id,
                f"💳 <b>New Payment Request</b>\n\n"
                f"User ID: <code>{user.id}</code>\n"
                f"Username: @{user.username or 'N/A'}\n"
                f"UTR: <code>{utr}</code>\n"
                f"Plan: {plan['label']} (₹{plan['price']})\n\n"
                f"Approve: /approve {user.id}\n"
                f"Reject: /reject {user.id}",
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
            f"🎉 <b>Payment Approved!</b>\n\n"
            f"You now have <b>{plan['label']}</b> Premium.\n"
            f"Access expires: {expiry.strftime('%d %b %Y %H:%M UTC')}\n\n"
            f"Enjoy unlimited access! 🚀",
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
            "❌ <b>Payment Rejected</b>\n\nYour payment could not be verified.\n"
            "Please contact support or try again with a valid UTR.",
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

    await update.message.reply_text(
        f"📊 <b>Bot Statistics</b>\n\n"
        f"👤 Total Users: <b>{total_users}</b>\n"
        f"💎 Premium Users: <b>{premium_users}</b>\n"
        f"📁 Total Files: <b>{total_files}</b>\n"
        f"💳 Pending Payments: <b>{pending_pay}</b>\n"
        f"👥 Total Referrals: <b>{total_referrals}</b>",
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
  <title>{{ file_name }} – Watch</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0d0d0d; color: #eee;
      font-family: 'Segoe UI', sans-serif;
      display: flex; flex-direction: column;
      align-items: center; justify-content: center;
      min-height: 100vh; padding: 20px;
    }
    h1 { font-size: 1.2rem; margin-bottom: 16px; color: #aaa; max-width: 720px; text-align: center; }
    video {
      width: 100%; max-width: 720px;
      border-radius: 10px;
      background: #000;
      box-shadow: 0 0 30px rgba(0,0,0,0.8);
    }
    .powered { margin-top: 12px; font-size: 0.75rem; color: #555; }
  </style>
</head>
<body>
  <h1>▶ {{ file_name }}</h1>
  <video controls autoplay preload="metadata">
    <source src="{{ stream_url }}" type="video/mp4">
    Your browser does not support the video tag.
  </video>
  <p class="powered">Powered by FileBot</p>
</body>
</html>"""


def verify_stream_access(file_unique_id: str) -> tuple[dict | None, str | None]:
    """Verify streaming access using Telegram user_id cookie-style token."""
    # We pass user_id as a query param (signed) for web access validation
    # In a full production app, use JWT; here we use a signed token
    token = request.args.get("t", "")
    user_id_raw = request.args.get("u", "")

    if not user_id_raw:
        return None, "Missing auth"

    # Validate HMAC signature
    expected = hashlib.sha256(f"{user_id_raw}:{file_unique_id}:{FLASK_SECRET}".encode()).hexdigest()[:16]
    if token != expected:
        return None, "Invalid token"

    try:
        uid = int(user_id_raw)
    except ValueError:
        return None, "Bad user ID"

    if not has_valid_token(uid):
        return None, "No valid token"

    file_doc = get_file(file_unique_id)
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
        return f"<h2>Access Denied: {err}</h2><p>Open the file link via Telegram bot.</p>", 403

    stream_url = f"/stream?id={file_unique_id}&u={request.args.get('u','')}&t={request.args.get('t','')}"
    return render_template_string(
        WATCH_HTML,
        file_name=file_doc.get("file_name", "Video"),
        stream_url=stream_url
    )


@flask_app.route("/stream")
def stream():
    file_unique_id = request.args.get("id", "")
    if not file_unique_id:
        abort(400)

    file_doc, err = verify_stream_access(file_unique_id)
    if err:
        abort(403)

    pd_id = file_doc.get("pixeldrain_id")
    if not pd_id:
        abort(404)

    range_header = request.headers.get("Range")

    # Get content info from Pixeldrain
    try:
        head_resp = requests.head(
            PD_DOWNLOAD_URL.format(id=pd_id),
            auth=("", PIXELDRAIN_API_KEY),
            timeout=10
        )
        content_length = head_resp.headers.get("Content-Length", "0")
        content_type   = head_resp.headers.get("Content-Type", "video/mp4")
    except Exception:
        content_length = "0"
        content_type   = "video/mp4"

    if range_header:
        # Partial content for video seeking
        resp_headers = {
            "Content-Type":   content_type,
            "Accept-Ranges":  "bytes",
            "Content-Range":  head_resp.headers.get("Content-Range", f"bytes 0-/{content_length}"),
            "Content-Length": head_resp.headers.get("Content-Length", content_length),
        }
        return Response(
            stream_from_pixeldrain(pd_id, range_header),
            status=206,
            headers=resp_headers,
            content_type=content_type,
            direct_passthrough=True
        )

    resp_headers = {
        "Content-Type":  content_type,
        "Accept-Ranges": "bytes",
        "Content-Length": content_length,
    }
    return Response(
        stream_from_pixeldrain(pd_id),
        status=200,
        headers=resp_headers,
        content_type=content_type,
        direct_passthrough=True
    )


@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


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
            f"✅ <b>{file_doc.get('file_name', 'Video')}</b>\n\n"
            f"🎬 Click below to stream your video.\n"
            f"🔒 Link is personal & expires with your token.",
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
def run_flask():
    """Run Flask in a separate thread."""
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)


def main():
    logger.info("🚀 Starting Telegram FileBot + Flask Streaming Server")

    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set!")
        return
    if not ADMIN_IDS:
        logger.warning("⚠️  No ADMIN_IDS set. Admin commands won't work.")

    # Start Flask in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"✅ Flask streaming server started on port {PORT}")

    # Start Telegram bot
    application = build_application()
    logger.info("✅ Bot started. Polling for updates...")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
