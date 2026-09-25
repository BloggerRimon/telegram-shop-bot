import os
import asyncio
import hashlib
import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import random
import string
import requests
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from datetime import datetime

from telegram import (
    Update,
    BotCommand,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    MessageEntity,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
import payment as paymod
import crypto_verify as cv
import wallet_checker as wc

try:
    import cryptomus as cm
except Exception as e:
    cm = None
    print("⚠️ Cryptomus module not available:", e)

try:
    import database as db
except Exception as e:
    db = None
    print("⚠️ Database module not available:", e)


# =========================
# BASIC SETTINGS
# =========================

# 🔐 TOKEN (GitHub-এ দিবা না)
BOT_TOKEN = os.getenv("BOT_TOKEN")

BOT_USERNAME = "SupremeLeaderShopBot"   # @ ছাড়া
SUPPORT_USERNAME = "@serpstacking"
SUPPORT_URL = "https://t.me/serpstacking"
PUBLIC_SITE_URL = "https://telegram-shop-bot-production-48fa.up.railway.app"
TERMS_URL = f"{PUBLIC_SITE_URL}/terms"
PRIVACY_URL = f"{PUBLIC_SITE_URL}/privacy"
LEGAL_URL = f"{PUBLIC_SITE_URL}/legal"

ADMIN_IDS = {6795246172}

REQUIRED_CHANNEL_ID = os.getenv("REQUIRED_CHANNEL_ID", "").strip()
REQUIRED_CHANNEL_URL = os.getenv("REQUIRED_CHANNEL_URL", "").strip()
REQUIRED_CHANNEL_ENABLED = os.getenv("REQUIRED_CHANNEL_ENABLED", "false").strip()

BUYER_API_ENABLED = os.getenv("BUYER_API_ENABLED", "false").strip()
BUYER_API_URL = os.getenv("BUYER_API_URL", "").strip()
BUYER_API_KEY = os.getenv("BUYER_API_KEY", "").strip()

BINANCE_ID = "828543482"
BYBIT_ID = "199582741"

# 🔐 API KEYS (Railway Variables / Environment থেকে নাও)
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "").strip()
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()

# 🔐 NOWPayments settings (set these in Railway Variables)
NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY", "").strip()
NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
NOWPAYMENTS_WEBHOOK_PATH = os.getenv("NOWPAYMENTS_WEBHOOK_PATH", "/nowpayments/webhook").strip() or "/nowpayments/webhook"
NOWPAYMENTS_API_BASE = "https://api.nowpayments.io/v1"

# 🔐 Payment gateway switch: set PAYMENT_GATEWAY=cryptomus in Railway to use Cryptomus.
PAYMENT_GATEWAY = os.getenv("PAYMENT_GATEWAY", "nowpayments").strip().lower()
CRYPTOMUS_MERCHANT_ID = os.getenv("CRYPTOMUS_MERCHANT_ID", "").strip()
CRYPTOMUS_PAYMENT_API_KEY = os.getenv("CRYPTOMUS_PAYMENT_API_KEY", "").strip()
CRYPTOMUS_WEBHOOK_PATH = os.getenv("CRYPTOMUS_WEBHOOK_PATH", "/cryptomus/webhook").strip() or "/cryptomus/webhook"
CRYPTOMUS_COURSE_SOURCE = os.getenv("CRYPTOMUS_COURSE_SOURCE", "Binance").strip() or "Binance"
CRYPTOMUS_ACCURACY_PERCENT = os.getenv("CRYPTOMUS_ACCURACY_PERCENT", "1").strip() or "1"
# 0 = merchant pays Cryptomus commission; 100 = client pays Cryptomus commission.
CRYPTOMUS_SUBTRACT_PERCENT = int(os.getenv("CRYPTOMUS_SUBTRACT_PERCENT", "0") or "0")
# Extra percent added to Cryptomus invoice amount to cover merchant processing cost.
# Example: user selects $10 deposit, invoice amount becomes $10.10, wallet credit remains $10.00.
CRYPTOMUS_CLIENT_MARKUP_PERCENT = Decimal(os.getenv("CRYPTOMUS_CLIENT_MARKUP_PERCENT", "1.0") or "1.0")

# Fee/margin in USD payment amount before creating NOWPayments invoice.
# If you already use NOWPayments dashboard markup, set this Railway variable to 0.
NOWPAYMENTS_FEE_MARGIN_PERCENT = Decimal(os.getenv("NOWPAYMENTS_FEE_MARGIN_PERCENT", "1.0"))
NOWPAYMENTS_UNIQUE_SUFFIX_MAX_CENTS = int(os.getenv("NOWPAYMENTS_UNIQUE_SUFFIX_MAX_CENTS", "20"))


# =========================
# CHECK TOKEN (optional but good)
# =========================
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set. Please set it in Railway.")

if not TRONGRID_API_KEY:
    print("⚠️ TRONGRID_API_KEY not set")
if not ETHERSCAN_API_KEY:
    print("⚠️ ETHERSCAN_API_KEY not set")
if not HELIUS_API_KEY:
    print("⚠️ HELIUS_API_KEY not set")
if not NOWPAYMENTS_API_KEY:
    print("⚠️ NOWPAYMENTS_API_KEY not set")
if not NOWPAYMENTS_IPN_SECRET:
    print("⚠️ NOWPAYMENTS_IPN_SECRET not set")
if PAYMENT_GATEWAY == "cryptomus":
    if not CRYPTOMUS_MERCHANT_ID:
        print("⚠️ CRYPTOMUS_MERCHANT_ID not set")
    if not CRYPTOMUS_PAYMENT_API_KEY:
        print("⚠️ CRYPTOMUS_PAYMENT_API_KEY not set")
if not PUBLIC_BASE_URL:
    print("⚠️ PUBLIC_BASE_URL not set")

TRONGRID_BASE = "https://api.trongrid.io"
ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
BTC_API_BASE = "https://mempool.space/api"
LTC_API_BASE = "https://litecoinspace.org/api"

COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"


def build_verify_config():
    return {
        "TRONGRID_BASE": TRONGRID_BASE,
        "TRONGRID_API_KEY": TRONGRID_API_KEY,
        "ETHERSCAN_V2_URL": ETHERSCAN_V2_URL,
        "ETHERSCAN_API_KEY": ETHERSCAN_API_KEY,
        "HELIUS_RPC_URL": HELIUS_RPC_URL,
        "BTC_API_BASE": BTC_API_BASE,
        "LTC_API_BASE": LTC_API_BASE,
        "USDT_TRC20_CONTRACT": USDT_TRC20_CONTRACT,
        "USDT_ERC20_CONTRACT": USDT_ERC20_CONTRACT,
        "USDT_BEP20_CONTRACT": USDT_BEP20_CONTRACT,
        "ETH_CHAIN_ID": ETH_CHAIN_ID,
        "BSC_CHAIN_ID": BSC_CHAIN_ID,
        "ERC20_TRANSFER_TOPIC": ERC20_TRANSFER_TOPIC,
    }


def fetch_live_rates_usd():
    ids = "bitcoin,litecoin,ethereum,binancecoin,solana,tron,tether"
    try:
        res = requests.get(
            COINGECKO_SIMPLE_PRICE_URL,
            params={"ids": ids, "vs_currencies": "usd"},
            timeout=20,
        )
        data = res.json() if res.ok else {}
    except Exception:
        data = {}

    return {
        "USDT (TRC20)": Decimal(str(data.get("tether", {}).get("usd", 1))),
        "USDT (ERC20)": Decimal(str(data.get("tether", {}).get("usd", 1))),
        "USDT (BEP20)": Decimal(str(data.get("tether", {}).get("usd", 1))),
        "BTC": Decimal(str(data.get("bitcoin", {}).get("usd", 70000))),
        "LTC": Decimal(str(data.get("litecoin", {}).get("usd", 80))),
        "ETH (ERC20)": Decimal(str(data.get("ethereum", {}).get("usd", 3000))),
        "BNB (BEP20)": Decimal(str(data.get("binancecoin", {}).get("usd", 600))),
        "SOL": Decimal(str(data.get("solana", {}).get("usd", 150))),
        "TRX (TRC20)": Decimal(str(data.get("tron", {}).get("usd", 0.12))),
    }


def build_unique_crypto_amount(usd_amount: float, network: str, user_id: int, ref: str = ""):
    rates = fetch_live_rates_usd()
    rate = rates.get(network, Decimal("1"))
    if rate <= 0:
        rate = Decimal("1")
    return paymod.calculate_exact_crypto_amount_from_rate(
        usd_amount=usd_amount,
        network=network,
        usd_rate=rate,
        user_id=user_id,
        ref=ref,
    )

RECHECK_INTERVAL_SECONDS = 20
VERIFY_RETRY_SECONDS = 10
VERIFY_MAX_SECONDS_FAST = 300
VERIFY_MAX_SECONDS_SLOW = 600
VERIFY_UI_REFRESH_SECONDS = 1
MAX_RECHECK_ATTEMPTS = 12

# =========================
# WALLET / TOKEN ADDRESSES
# =========================
USDT_TRC20_RECEIVE_ADDRESS = "TFWMEL6o5Kxnh1h25XMuWG6b6HaeF7vNf1"
USDT_ERC20_RECEIVE_ADDRESS = "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331"
USDT_BEP20_RECEIVE_ADDRESS = "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331"
LTC_RECEIVE_ADDRESS = "LQcmsEwAHuyWyY3Heu2XMYShfirxomCVtk"
BTC_RECEIVE_ADDRESS = "15ykQZeq9jQTjJEzY2faG4LpirS2bYcb8L"
BNB_BEP20_RECEIVE_ADDRESS = "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331"
SOL_RECEIVE_ADDRESS = "23MdGndZ85eJR58JWHiHNFmrQDMU1Leipzhnx4wtgnWE"
TRX_RECEIVE_ADDRESS = "TFWMEL6o5Kxnh1h25XMuWG6b6HaeF7vNf1"
ETH_ERC20_RECEIVE_ADDRESS = "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331"

# =========================
# CONTRACTS / CHAIN CONFIG
# =========================
USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_ERC20_CONTRACT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDT_BEP20_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"

ETH_CHAIN_ID = "1"
BSC_CHAIN_ID = "56"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# =========================
# PAYMENT ADDRESSES
# =========================
CRYPTO_ADDRESSES = {
    "USDT (TRC20)": USDT_TRC20_RECEIVE_ADDRESS,
    "USDT (ERC20)": USDT_ERC20_RECEIVE_ADDRESS,
    "USDT (BEP20)": USDT_BEP20_RECEIVE_ADDRESS,
    "LTC": LTC_RECEIVE_ADDRESS,
    "BTC": BTC_RECEIVE_ADDRESS,
    "BNB (BEP20)": BNB_BEP20_RECEIVE_ADDRESS,
    "SOL": SOL_RECEIVE_ADDRESS,
    "TRX (TRC20)": TRX_RECEIVE_ADDRESS,
    "ETH (ERC20)": ETH_ERC20_RECEIVE_ADDRESS,
}

# =========================
# PRODUCTS + ORDER
# =========================
PRODUCTS = {
    "p1": {
        "name": "ChatGPT Plus",
        "icon": "🧠",
        "month": "1",
        "price": 5.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [],
        "display_stock": 0,
    },
    "p2": {
        "name": "SEMrush Guru",
        "icon": "📈",
        "month": "14 Days",
        "price": 2.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [{"email": "semrush1@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_1", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush2@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_2", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush3@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_3", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush4@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_4", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush5@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_5", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush6@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_6", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush7@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_7", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush8@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_8", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush9@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_9", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "semrush10@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_10", "note": "Replace this placeholder account from Admin Panel"}],
        "display_stock": 10,
    },
    "p3": {
        "name": "CapCut Pro",
        "icon": "🎬",
        "month": "1",
        "price": 2.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [{"email": "capcut1@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_1", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "capcut2@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_2", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "capcut3@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_3", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "capcut4@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_4", "note": "Replace this placeholder account from Admin Panel"}],
        "display_stock": 4,
    },
    "p4": {
        "name": "SuperGrok",
        "icon": "🌕",
        "month": "1",
        "price": 8.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [{"email": "supergrok1@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_1", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "supergrok2@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_2", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "supergrok3@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_3", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "supergrok4@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_4", "note": "Replace this placeholder account from Admin Panel"}],
        "display_stock": 4,
    },
    "p5": {
        "name": "Perplexity AI Pro",
        "icon": "🌟",
        "month": "1 Year",
        "price": 13.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [{"email": "perplexity1@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_1", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "perplexity2@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_2", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "perplexity3@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_3", "note": "Replace this placeholder account from Admin Panel"}],
        "display_stock": 3,
    },
    "p6": {
        "name": "Gemini Pro",
        "icon": "🤖",
        "month": "4",
        "price": 7.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [{"email": "gemini1@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_1", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini2@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_2", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini3@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_3", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini4@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_4", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini5@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_5", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini6@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_6", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini7@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_7", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini8@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_8", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "gemini9@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_9", "note": "Replace this placeholder account from Admin Panel"}],
        "display_stock": 9,
    },
    "p7": {
        "name": "Microsoft Office 365 Pro Plus",
        "icon": "📝",
        "month": "0",
        "price": 2.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [{"email": "office3651@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_1", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3652@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_2", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3653@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_3", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3654@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_4", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3655@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_5", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3656@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_6", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3657@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_7", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3658@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_8", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office3659@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_9", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36510@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_10", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36511@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_11", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36512@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_12", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36513@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_13", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36514@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_14", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36515@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_15", "note": "Replace this placeholder account from Admin Panel"},
            {"email": "office36516@replace-stock.local", "password": "CHANGE_THIS_PASSWORD_16", "note": "Replace this placeholder account from Admin Panel"}],
        "display_stock": 16,
    },
    "p8": {
        "name": "Jasper AI",
        "icon": "✨",
        "month": "7 Days",
        "price": 2.5,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [],
        "display_stock": 0,
    },
    "p9": {
        "name": "SpyHero",
        "icon": "✨",
        "month": "7 Days",
        "price": 3.0,
        "details": ["✅ Auto Delivery", "✅ Account Details Delivery", "✅ Replace stock from Admin Panel"],
        "accounts": [],
        "display_stock": 0,
    },
}
product_order = ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"]

DEFAULT_CATEGORY_ID = "cat_default"
DEFAULT_CATEGORY_NAME = "Uncategorized"
CATEGORIES = {
    DEFAULT_CATEGORY_ID: {
        "id": DEFAULT_CATEGORY_ID,
        "name": DEFAULT_CATEGORY_NAME,
        "icon": "🗂",
        "order": 0,
    }
}
category_order = [DEFAULT_CATEGORY_ID]
next_category_number = 1
shop_order = []

DASHBOARD_EMOJI_KEYS = (
    "shop",
    "orders",
    "wallet",
    "topup",
    "promo",
    "refer",
    "profile",
    "transactions",
    "support",
)
dashboard_custom_emoji_ids = {key: "" for key in DASHBOARD_EMOJI_KEYS}

DASHBOARD_HEADER_EMOJIS = {
    "welcome": {"label": "Welcome", "emoji": "👋"},
    "premium": {"label": "Premium", "emoji": "💼"},
    "delivery": {"label": "Delivery", "emoji": "⚡"},
    "secure": {"label": "Secure", "emoji": "🔐"},
    "support": {"label": "Support", "emoji": "🎧"},
    "wallet": {"label": "Wallet", "emoji": "💰"},
    "choose": {"label": "Choose", "emoji": "👇"},
}
dashboard_header_custom_emoji_ids = {key: "" for key in DASHBOARD_HEADER_EMOJIS}

DEFAULT_DELIVERY_GUIDE = """📌 Account Login Guide
Please follow these simple steps to use your new account:

🛠 Step 1: Preparation
🧹 Clear your browser cookies before you start.
🔄 If you are already logged into another account, clear your cookies or use a new browser / Incognito / Private window.

🔐 Step 2: Security
🔑 This account may not have 2FA or a password lock set yet.
⚙️ Please change the password immediately after buying to make it secure.

🚀 Step 3: Login Process
🌐 Go to the product login website or the link provided in the note.
📋 Copy the account information carefully and log in.
🟢 If OTP/login code is required, follow the note/instruction included with the account.

🕐 Warranty Policy
⚠️ This account comes with a 24-hour warranty."""


def get_delivery_guide(product_id: str) -> str:
    product = PRODUCTS.get(product_id, {})
    # IMPORTANT: Do not auto-add the fixed/default login guide to every delivery.
    # If admin added a product-specific delivery guide, show only that guide.
    # If no guide is set, deliver only the actual stock/account line admin entered.
    return str(product.get("delivery_guide", "") or "").strip()

# =========================
# PROMO CODES
# =========================
PROMO_CODES = {
    "FREE5": {
        "amount": 5.0,
        "enabled": True,
        "one_time": True,
        "created_at": datetime.now(),
        "created_by": "system",
        "used_by": None,
        "used_at": None,
    },
    "BONUS10": {
        "amount": 10.0,
        "enabled": True,
        "one_time": True,
        "created_at": datetime.now(),
        "created_by": "system",
        "used_by": None,
        "used_at": None,
    },
}

# =========================
# IN-MEMORY STORAGE
# =========================
user_wallet = {}
user_orders = {}
user_transactions = {}
used_promo_codes = {}
user_state = {}
user_mode = {}
notify_waitlist = {product_id: set() for product_id in PRODUCTS}
gold_vip_users = set()
active_flash_deal = {}
pending_crypto_deposits = {}
pending_crypto_orders = {}
used_txids = set()
admin_temp = {}
next_product_number = len(PRODUCTS) + 1

global_order_id = 1
global_tx_id = 1

all_orders = []
all_transactions = []
all_users = set()
user_profiles = {}
app_instance = None

# =========================
# PERSISTENT STORAGE
# =========================
BOT_STATE_FILE = os.getenv("BOT_STATE_FILE", "bot_state.json")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_DATABASE = bool(DATABASE_URL)


def _json_safe(value):
    if isinstance(value, datetime):
        return {"__datetime__": value.isoformat()}
    if isinstance(value, set):
        return list(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _restore_datetime(value):
    if isinstance(value, dict) and "__datetime__" in value:
        try:
            return datetime.fromisoformat(value["__datetime__"])
        except Exception:
            return value.get("__datetime__")
    if isinstance(value, dict):
        return {k: _restore_datetime(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_restore_datetime(v) for v in value]
    return value


def build_state_snapshot():
    """Collect all important bot data in one snapshot for persistent storage."""
    # Clean old/bad emoji icon data before saving.
    # This does NOT delete products/orders/users; it only converts icon dicts into safe fields.
    if "normalize_all_product_icons" in globals():
        normalize_all_product_icons()
    if "normalize_all_category_icons" in globals():
        normalize_all_category_icons()
    if "normalize_categories" in globals():
        normalize_categories()
    if "normalize_shop_order" in globals():
        normalize_shop_order()

    state = {
        "user_wallet": user_wallet,
        "user_orders": user_orders,
        "user_transactions": user_transactions,
        "used_promo_codes": used_promo_codes,
        "all_users": list(all_users),
        "user_profiles": user_profiles,
        "all_orders": all_orders,
        "all_transactions": all_transactions,
        "PRODUCTS": PRODUCTS,
        "product_order": product_order,
        "CATEGORIES": CATEGORIES,
        "category_order": category_order,
        "shop_order": shop_order,
        "dashboard_custom_emoji_ids": dashboard_custom_emoji_ids,
        "dashboard_header_custom_emoji_ids": dashboard_header_custom_emoji_ids,
        "PROMO_CODES": PROMO_CODES,
        "notify_waitlist": {k: list(v) for k, v in notify_waitlist.items()},
        "gold_vip_users": list(gold_vip_users),
        "active_flash_deal": active_flash_deal,
        "global_order_id": global_order_id,
        "global_tx_id": global_tx_id,
        "next_product_number": next_product_number,
        "next_category_number": next_category_number,
    }

    # These globals are defined later in the file. If they already exist, keep them too.
    if "NOWPAYMENTS_PAYMENTS" in globals():
        state["NOWPAYMENTS_PAYMENTS"] = globals().get("NOWPAYMENTS_PAYMENTS", {})
    if "NOWPAYMENTS_PROCESSED" in globals():
        state["NOWPAYMENTS_PROCESSED"] = list(globals().get("NOWPAYMENTS_PROCESSED", set()) or [])
    if "CRYPTOMUS_PAYMENTS" in globals():
        state["CRYPTOMUS_PAYMENTS"] = globals().get("CRYPTOMUS_PAYMENTS", {})
    if "CRYPTOMUS_PROCESSED" in globals():
        state["CRYPTOMUS_PROCESSED"] = list(globals().get("CRYPTOMUS_PROCESSED", set()) or [])

    return state


def apply_loaded_state(data: dict):
    """Apply a loaded state snapshot back into the in-memory bot structures."""
    global PRODUCTS, product_order, CATEGORIES, category_order, shop_order, PROMO_CODES
    global gold_vip_users, active_flash_deal
    global global_order_id, global_tx_id, next_product_number, next_category_number
    global NOWPAYMENTS_PAYMENTS, NOWPAYMENTS_PROCESSED
    global CRYPTOMUS_PAYMENTS, CRYPTOMUS_PROCESSED

    data = _restore_datetime(data or {})

    loaded_products = data.get("PRODUCTS")
    if loaded_products:
        PRODUCTS.clear()
        PRODUCTS.update(loaded_products)

    product_order.clear()
    product_order.extend([pid for pid in data.get("product_order", []) if pid in PRODUCTS])
    for pid in PRODUCTS:
        if pid not in product_order:
            product_order.append(pid)

    loaded_categories = data.get("CATEGORIES")
    if isinstance(loaded_categories, dict) and loaded_categories:
        CATEGORIES.clear()
        CATEGORIES.update(loaded_categories)

    loaded_category_order = data.get("category_order")
    if isinstance(loaded_category_order, list):
        category_order.clear()
        category_order.extend(loaded_category_order)

    next_category_number = int(data.get("next_category_number", next_category_number) or next_category_number)
    if "normalize_categories" in globals():
        normalize_categories()

    loaded_shop_order = data.get("shop_order")
    shop_order.clear()
    if isinstance(loaded_shop_order, list):
        shop_order.extend(loaded_shop_order)
    if "normalize_shop_order" in globals():
        normalize_shop_order()

    loaded_dashboard_emojis = data.get("dashboard_custom_emoji_ids", {})
    if isinstance(loaded_dashboard_emojis, dict):
        for key in DASHBOARD_EMOJI_KEYS:
            dashboard_custom_emoji_ids[key] = str(loaded_dashboard_emojis.get(key) or "").strip()

    loaded_dashboard_header_emojis = data.get("dashboard_header_custom_emoji_ids", {})
    if isinstance(loaded_dashboard_header_emojis, dict):
        for key in DASHBOARD_HEADER_EMOJIS:
            dashboard_header_custom_emoji_ids[key] = str(loaded_dashboard_header_emojis.get(key) or "").strip()

    # Migrate any old/bad saved custom emoji data.
    # Some previous code may have saved the whole Telegram entity dict inside product["icon"],
    # which makes buttons show {'type': 'custom_emoji', ...}. This converts it safely.
    if "normalize_all_product_icons" in globals():
        normalize_all_product_icons()
    if "normalize_all_category_icons" in globals():
        normalize_all_category_icons()

    loaded_promos = data.get("PROMO_CODES")
    if loaded_promos:
        PROMO_CODES.clear()
        PROMO_CODES.update(loaded_promos)

    user_wallet.clear()
    user_wallet.update({int(k): float(v) for k, v in (data.get("user_wallet", {}) or {}).items()})

    user_orders.clear()
    user_orders.update({int(k): v for k, v in (data.get("user_orders", {}) or {}).items()})

    user_transactions.clear()
    user_transactions.update({int(k): v for k, v in (data.get("user_transactions", {}) or {}).items()})

    used_promo_codes.clear()
    used_promo_codes.update({int(k): set(v or []) for k, v in (data.get("used_promo_codes", {}) or {}).items()})

    all_users.clear()
    all_users.update(int(x) for x in (data.get("all_users", []) or []))

    user_profiles.clear()
    user_profiles.update({int(k): v for k, v in (data.get("user_profiles", {}) or {}).items()})

    all_orders.clear()
    all_orders.extend(data.get("all_orders", []) or [])

    all_transactions.clear()
    all_transactions.extend(data.get("all_transactions", []) or [])

    notify_waitlist.clear()
    notify_waitlist.update({pid: set(users or []) for pid, users in (data.get("notify_waitlist", {}) or {}).items()})
    for pid in PRODUCTS:
        notify_waitlist.setdefault(pid, set())

    gold_vip_users.clear()
    gold_vip_users.update(int(x) for x in (data.get("gold_vip_users", []) or []))

    active_flash_deal.clear()
    loaded_flash_deal = data.get("active_flash_deal", {}) or {}
    if isinstance(loaded_flash_deal, dict):
        active_flash_deal.update(loaded_flash_deal)

    global_order_id = int(data.get("global_order_id", global_order_id) or global_order_id)
    global_tx_id = int(data.get("global_tx_id", global_tx_id) or global_tx_id)
    next_product_number = int(data.get("next_product_number", next_product_number) or next_product_number)

    if "NOWPAYMENTS_PAYMENTS" in globals() and "NOWPAYMENTS_PAYMENTS" in data:
        NOWPAYMENTS_PAYMENTS = data.get("NOWPAYMENTS_PAYMENTS", {}) or {}
    if "NOWPAYMENTS_PROCESSED" in globals() and "NOWPAYMENTS_PROCESSED" in data:
        NOWPAYMENTS_PROCESSED = set(data.get("NOWPAYMENTS_PROCESSED", []) or [])
    if "CRYPTOMUS_PAYMENTS" in globals() and "CRYPTOMUS_PAYMENTS" in data:
        CRYPTOMUS_PAYMENTS = data.get("CRYPTOMUS_PAYMENTS", {}) or {}
    if "CRYPTOMUS_PROCESSED" in globals() and "CRYPTOMUS_PROCESSED" in data:
        CRYPTOMUS_PROCESSED = set(data.get("CRYPTOMUS_PROCESSED", []) or [])


# Fast persistence settings.
# Old code saved the full database state after EVERY button click/message.
# That can make the Telegram bot wait 30-60+ seconds when the state grows.
# This version schedules saving in the background, so buttons reply instantly.
SAVE_DEBOUNCE_SECONDS = float(os.getenv("SAVE_DEBOUNCE_SECONDS", "2"))
_save_lock = threading.Lock()
_save_timer = None


def _save_bot_state_now():
    """Actually write current bot state to storage. Runs in a background thread."""
    try:
        state = build_state_snapshot()

        if USE_DATABASE:
            if db is None:
                raise RuntimeError("DATABASE_URL is set but database.py could not be imported")
            # Save the whole bot state once as JSONB.
            # user_profiles is already inside this state, so we do NOT upsert every user
            # into the users table on every click. That loop was the main speed problem.
            db.save_state(_json_safe(state))
            return

        tmp_file = BOT_STATE_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(_json_safe(state), f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, BOT_STATE_FILE)
    except Exception as e:
        print("⚠️ Failed to save bot state:", e)


def _save_bot_state_worker():
    global _save_timer
    try:
        _save_bot_state_now()
    finally:
        with _save_lock:
            _save_timer = None


def save_bot_state():
    """Request a background save without blocking Telegram replies."""
    global _save_timer
    try:
        with _save_lock:
            if _save_timer is not None and _save_timer.is_alive():
                return
            _save_timer = threading.Timer(SAVE_DEBOUNCE_SECONDS, _save_bot_state_worker)
            _save_timer.daemon = True
            _save_timer.start()
    except Exception as e:
        print("⚠️ Failed to schedule bot state save:", e)


def force_save_bot_state():
    """Use only for rare critical shutdown/manual cases where immediate save is needed."""
    _save_bot_state_now()


def load_bot_state():
    try:
        if USE_DATABASE:
            if db is None:
                raise RuntimeError("DATABASE_URL is set but database.py could not be imported")
            data = db.load_state()
            if data is None:
                print("ℹ️ Database is empty; using code defaults for first run.")
                save_bot_state()
                return
            apply_loaded_state(data)
            print(f"✅ Loaded bot state from database: {len(all_users)} users, {len(PRODUCTS)} products")
            return

        if not os.path.exists(BOT_STATE_FILE):
            print("ℹ️ No bot_state.json found; using code defaults for first run.")
            return
        with open(BOT_STATE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        apply_loaded_state(raw)
        print(f"✅ Loaded bot state from file: {len(all_users)} users, {len(PRODUCTS)} products")
    except Exception as e:
        if USE_DATABASE:
            print("FATAL: Failed to load bot state from database; stopping startup to protect existing data:", e)
            raise
        print("⚠️ Failed to load bot state; using code defaults:", e)


# =========================
# TIME HELPERS
# =========================
def now_dt():
    return datetime.now()


def format_dt(dt_obj):
    if not dt_obj:
        return "N/A"
    if isinstance(dt_obj, str):
        return dt_obj
    return dt_obj.strftime("%Y-%m-%d %I:%M:%S %p")


# =========================
# BASIC HELPERS
# =========================
def ensure_user(user_id: int, tg_user=None):
    all_users.add(user_id)

    if user_id not in user_wallet:
        user_wallet[user_id] = 0.0
    if user_id not in user_orders:
        user_orders[user_id] = []
    if user_id not in user_transactions:
        user_transactions[user_id] = []
    if user_id not in used_promo_codes:
        used_promo_codes[user_id] = set()
    if user_id not in user_state:
        user_state[user_id] = {"step": "main"}
    if user_id not in user_mode:
        user_mode[user_id] = "client"
    if user_id not in admin_temp:
        admin_temp[user_id] = {}

    update_user_profile(user_id, tg_user)


def update_user_profile(user_id: int, tg_user=None):
    now = now_dt()
    profile = user_profiles.get(user_id, {})
    if not profile.get("first_seen_at"):
        profile["first_seen_at"] = now
    profile["last_seen_at"] = now
    profile["user_id"] = user_id
    profile["wallet_balance"] = float(user_wallet.get(user_id, 0.0))

    if tg_user is not None:
        profile["username"] = getattr(tg_user, "username", None) or profile.get("username")
        profile["first_name"] = getattr(tg_user, "first_name", None) or profile.get("first_name")
        profile["last_name"] = getattr(tg_user, "last_name", None) or profile.get("last_name")
        profile["is_bot"] = bool(getattr(tg_user, "is_bot", False))

    user_profiles[user_id] = profile
    return profile


def get_user_profile(user_id: int) -> dict:
    ensure_user(user_id)
    profile = user_profiles.get(user_id, {})
    profile["wallet_balance"] = float(user_wallet.get(user_id, 0.0))
    return profile


def format_user_link(user_id: int) -> str:
    profile = get_user_profile(user_id)
    username = str(profile.get("username") or "").strip().lstrip("@")
    first = str(profile.get("first_name") or "").strip()
    last = str(profile.get("last_name") or "").strip()
    full_name = (first + " " + last).strip()

    if username:
        return f'<a href="https://t.me/{escape_html(username)}">@{escape_html(username)}</a>'
    if full_name:
        return escape_html(full_name)
    return "No username"


async def notify_admin_order(bot, user_id: int, product_id: str, qty: int, total: float, payment_type: str):
    product = PRODUCTS.get(product_id, {})
    real_stock = get_product_stock(product_id) if product_id in PRODUCTS else 0
    display_stock = get_display_stock(product_id) if product_id in PRODUCTS else 0

    stock_lines = [
        f"<b>Remaining Real Stock:</b> {real_stock} pcs",
        f"<b>Display Stock:</b> {display_stock} pcs",
    ]

    stock_alert = ""
    if real_stock <= 0 or display_stock <= 0:
        stock_alert = (
            "\n\n🚨 <b>STOCK OUT REMINDER</b>\n"
            f"<b>{escape_html(product.get('name', product_id))}</b> is now stock out or display stock is 0.\n"
            "Please update stock from Admin Panel."
        )

    text = (
        "🛒 <b>NEW PURCHASE NOTIFICATION</b>\n\n"
        f"<b>User:</b> {format_user_link(user_id)}\n"
        f"<b>User ID:</b> <code>{user_id}</code>\n"
        f"<b>Product:</b> {escape_html(product.get('name', product_id))}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total:</b> {format_money(total)}\n"
        f"<b>Payment:</b> {escape_html(payment_type)}\n"
        + "\n".join(stock_lines)
        + stock_alert
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as e:
            print(f"⚠️ Failed to notify admin {admin_id}:", e)


def format_money(value: float) -> str:
    return f"${float(value):.2f}"


def get_product_stock(product_id: str) -> int:
    return len(PRODUCTS[product_id]["accounts"])


def get_display_stock(product_id: str) -> int:
    return int(PRODUCTS[product_id].get("display_stock", len(PRODUCTS[product_id]["accounts"])))


def is_gold_vip_user(user_id: int) -> bool:
    try:
        return int(user_id) in gold_vip_users
    except Exception:
        return False


def get_product_base_price(product_id: str) -> float:
    return float(PRODUCTS[product_id].get("price", 0.0))


def get_product_vip_price(product_id: str):
    product = PRODUCTS.get(product_id, {})
    value = product.get("gold_vip_price")
    if value in (None, ""):
        return None
    try:
        value = float(value)
        return value if value > 0 else None
    except Exception:
        return None


def get_product_price(product_id: str, user_id: int = None) -> float:
    flash_price = get_flash_deal_price(product_id) if "get_flash_deal_price" in globals() else None
    if flash_price is not None:
        return float(flash_price)
    vip_price = get_product_vip_price(product_id)
    if user_id is not None and is_gold_vip_user(user_id) and vip_price is not None:
        return float(vip_price)
    return get_product_base_price(product_id)


def get_bulk_pricing_tiers(product) -> list:
    if isinstance(product, str):
        product = PRODUCTS.get(product, {})
    if not isinstance(product, dict):
        return []

    tiers_by_quantity = {}
    for tier in product.get("bulk_pricing") or []:
        if not isinstance(tier, dict):
            continue
        min_qty = safe_decimal(tier.get("min_qty"))
        unit_price = safe_decimal(tier.get("unit_price"))
        if (
            min_qty is None
            or unit_price is None
            or not min_qty.is_finite()
            or not unit_price.is_finite()
            or min_qty != min_qty.to_integral_value()
            or min_qty < 2
            or unit_price <= 0
        ):
            continue
        quantity = int(min_qty)
        tiers_by_quantity[quantity] = {
            "min_qty": quantity,
            "unit_price": float(unit_price),
        }
    return [tiers_by_quantity[quantity] for quantity in sorted(tiers_by_quantity)]


def get_effective_unit_price(product, quantity, user_id: int = None) -> float:
    product_id = product if isinstance(product, str) else None
    product_data = PRODUCTS.get(product_id, {}) if product_id else product
    if not isinstance(product_data, dict):
        raise ValueError("Product was not found.")
    if product_id is None:
        product_id = next((pid for pid, item in PRODUCTS.items() if item is product_data), None)

    quantity_value = safe_decimal(quantity)
    if (
        quantity_value is None
        or not quantity_value.is_finite()
        or quantity_value != quantity_value.to_integral_value()
        or quantity_value < 1
    ):
        raise ValueError("Quantity must be a positive integer.")
    quantity_int = int(quantity_value)

    flash_price = get_flash_deal_price(product_id) if product_id and "get_flash_deal_price" in globals() else None
    if flash_price is not None:
        return float(flash_price)

    if product_id:
        current_price = get_product_price(product_id, user_id)
    else:
        current_price = float(product_data.get("price", 0))
        vip_price = product_data.get("gold_vip_price")
        if user_id is not None and is_gold_vip_user(user_id) and vip_price not in (None, ""):
            try:
                vip_value = float(vip_price)
                if vip_value > 0:
                    current_price = vip_value
            except (TypeError, ValueError):
                pass

    applicable_tiers = [
        tier for tier in get_bulk_pricing_tiers(product_data)
        if quantity_int >= tier["min_qty"]
    ]
    bulk_price = float(applicable_tiers[-1]["unit_price"]) if applicable_tiers else None
    return min(float(current_price), bulk_price) if bulk_price is not None else float(current_price)


def calculate_order_total(product_id: str, quantity: int, user_id: int = None):
    unit_price = float(get_effective_unit_price(product_id, quantity, user_id))
    total = unit_price * int(quantity)
    unit_decimal = safe_decimal(unit_price)
    total_decimal = safe_decimal(total)
    if (
        unit_decimal is None
        or total_decimal is None
        or not unit_decimal.is_finite()
        or not total_decimal.is_finite()
        or unit_decimal <= 0
        or total_decimal <= 0
    ):
        raise ValueError("Calculated order price must be greater than zero.")
    return unit_price, total


def format_product_price_for_user(product_id: str, user_id: int = None) -> str:
    flash_price = get_flash_deal_price(product_id) if "get_flash_deal_price" in globals() else None
    if flash_price is not None:
        return f"{format_money(flash_price)} ⚡"
    vip_price = get_product_vip_price(product_id)
    if user_id is not None and is_gold_vip_user(user_id) and vip_price is not None:
        return f"{format_money(vip_price)} 👑"
    return format_money(get_product_base_price(product_id))


def format_duration_text(value) -> str:
    text = str(value or "").strip()
    if not text or text == "0":
        return ""
    if text.isdigit():
        return f"{text} Month"
    return text


def safe_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def is_valid_txid_format(txid: str) -> bool:
    txid = txid.strip()
    if len(txid) < 20:
        return False
    hex_allowed = "0123456789abcdefABCDEF"
    if all(ch in hex_allowed for ch in txid):
        return True
    base58_allowed = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    if all(ch in base58_allowed for ch in txid):
        return True
    return False


def trongrid_headers():
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "TRON-PRO-API-KEY": TRONGRID_API_KEY,
    }


def get_wallet_balance_text(user_id: int) -> str:
    return f"💰 <b>New wallet balance:</b> {format_money(user_wallet[user_id])}"


def normalize_evm_address(addr: str) -> str:
    return str(addr or "").strip().lower()


def to_evm_topic_address(addr: str) -> str:
    return "0x" + normalize_evm_address(addr).replace("0x", "").rjust(64, "0")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


class BuyerAPIError(Exception):
    pass


def is_buyer_api_enabled() -> bool:
    return BUYER_API_ENABLED.lower() == "true"


def mask_buyer_api_key() -> str:
    if not BUYER_API_KEY:
        return "Not configured"
    if len(BUYER_API_KEY) <= 8:
        return "*" * len(BUYER_API_KEY)
    return f"{BUYER_API_KEY[:4]}{'*' * (len(BUYER_API_KEY) - 8)}{BUYER_API_KEY[-4:]}"


def normalize_buyer_api_url() -> str:
    base_url = BUYER_API_URL.strip().rstrip("/")
    api_suffix = "/api/telegram-buyer"
    if base_url.lower().endswith(api_suffix):
        base_url = base_url[:-len(api_suffix)].rstrip("/")
    return base_url


def buyer_api_url_warning() -> str:
    configured_url = BUYER_API_URL.strip().rstrip("/")
    if configured_url.lower().endswith("/api/telegram-buyer"):
        return "BUYER_API_URL should be base URL only: http://54.255.147.200:3002"
    return ""


def buyer_api_endpoint(path: str) -> str:
    return f"{normalize_buyer_api_url()}{path}"


def buyer_api_endpoint_preview(path: str, params: dict = None) -> str:
    query_parts = [f"key={mask_buyer_api_key()}"]
    for key, value in (params or {}).items():
        query_parts.append(f"{key}={value}")
    return f"GET {buyer_api_endpoint(path)}?{'&'.join(query_parts)}"


def _validate_buyer_api_config():
    if not is_buyer_api_enabled():
        raise BuyerAPIError("Seller API is disabled. Set BUYER_API_ENABLED=true to enable tests.")
    if not normalize_buyer_api_url():
        raise BuyerAPIError("BUYER_API_URL is not configured.")
    if not BUYER_API_KEY:
        raise BuyerAPIError("BUYER_API_KEY is not configured.")


def _buyer_api_get(path: str, params: dict = None):
    _validate_buyer_api_config()
    request_params = dict(params or {})
    request_params["key"] = BUYER_API_KEY
    try:
        response = requests.get(
            buyer_api_endpoint(path),
            params=request_params,
            timeout=20,
        )
    except requests.Timeout as exc:
        raise BuyerAPIError("Seller API request timed out.") from exc
    except requests.ConnectionError as exc:
        raise BuyerAPIError("Could not connect to the Seller API.") from exc
    except requests.RequestException as exc:
        raise BuyerAPIError("Seller API request failed.") from exc

    if response.status_code == 401:
        response_note = _safe_buyer_api_error_body(response)
        message = (
            "HTTP 401 Unauthorized\n\n"
            "Possible reasons:\n"
            "- API key is invalid/expired\n"
            "- API key was regenerated but Railway still has the old key\n"
            "- Seller API access is not enabled for this account\n"
            "- Seller server rejected this key"
        )
        if response_note:
            message += f"\n\nSeller response: {response_note}"
        raise BuyerAPIError(message)
    if not response.ok:
        raise BuyerAPIError(f"Seller API returned HTTP {response.status_code}.")
    try:
        payload = response.json()
    except ValueError as exc:
        raise BuyerAPIError("Seller API returned invalid JSON.") from exc
    if not isinstance(payload, (dict, list)):
        raise BuyerAPIError("Seller API returned an unexpected response format.")
    if isinstance(payload, dict) and payload.get("success") is False:
        raise BuyerAPIError("Seller API reported that the request failed.")
    return payload


def _safe_buyer_api_error_body(response) -> str:
    try:
        payload = response.json()
    except (ValueError, TypeError):
        payload = None

    candidate = ""
    if isinstance(payload, dict):
        for key in ("message", "error", "detail"):
            value = payload.get(key)
            if isinstance(value, (str, int, float, bool)):
                candidate = str(value)
                break
    if not candidate:
        try:
            candidate = str(response.text or "")
        except Exception:
            candidate = ""

    candidate = " ".join(candidate.split()).strip()
    if not candidate:
        return ""
    if BUYER_API_KEY and BUYER_API_KEY.lower() in candidate.lower():
        return ""
    lowered = candidate.lower()
    sensitive_markers = (
        '"password"', 'password=', '"secret"', 'secret=',
        '"token"', 'token=', '"key"', 'key=',
    )
    if any(marker in lowered for marker in sensitive_markers):
        return ""
    return candidate[:160]


def _buyer_api_data(payload):
    if isinstance(payload, dict):
        for key in ("data", "result"):
            if isinstance(payload.get(key), (dict, list)):
                return payload[key]
    return payload


def fetch_buyer_api_balance() -> dict:
    payload = _buyer_api_data(_buyer_api_get("/api/telegram-buyer/balance"))
    if not isinstance(payload, dict):
        raise BuyerAPIError("Seller API balance response is missing balance data.")
    return payload


def fetch_buyer_api_products():
    payload = _buyer_api_get("/api/telegram-buyer/products", {"lang": "en"})
    data = _buyer_api_data(payload)
    if isinstance(data, list):
        total = payload.get("total", payload.get("count", len(data))) if isinstance(payload, dict) else len(data)
        try:
            total = int(total)
        except (TypeError, ValueError):
            total = len(data)
        return data, total
    if not isinstance(data, dict):
        raise BuyerAPIError("Seller API product response is missing the product list.")
    products = data.get("products")
    if not isinstance(products, list):
        products = data.get("items")
    if not isinstance(products, list):
        raise BuyerAPIError("Seller API product response is missing the product list.")
    pagination = data.get("pagination") if isinstance(data.get("pagination"), dict) else {}
    total = data.get("total", data.get("count", pagination.get("total", len(products))))
    if total == len(products) and isinstance(payload, dict) and payload is not data:
        payload_pagination = payload.get("pagination") if isinstance(payload.get("pagination"), dict) else {}
        total = payload.get("total", payload.get("count", payload_pagination.get("total", total)))
    try:
        total = int(total)
    except (TypeError, ValueError):
        total = len(products)
    return products, total


def is_channel_gate_enabled() -> bool:
    return REQUIRED_CHANNEL_ENABLED.lower() == "true"


def _required_channel_chat_id():
    channel_id = REQUIRED_CHANNEL_ID
    if channel_id.lstrip("-").isdigit():
        return int(channel_id)
    return channel_id


def _normalize_chat_member_status(raw_status) -> str:
    status = str(getattr(raw_status, "value", raw_status) or "").strip().lower()
    if "." in status:
        status = status.rsplit(".", 1)[-1]
    return status


async def check_required_channel_membership(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    if not is_channel_gate_enabled():
        print(f"ℹ️ Channel membership check skipped because gate is disabled. user_id={user_id}")
        return True
    if is_admin(user_id):
        print(f"ℹ️ Channel membership check bypassed for admin user_id={user_id}")
        return True
    if not REQUIRED_CHANNEL_ID:
        print("⚠️ REQUIRED_CHANNEL_ENABLED is true but REQUIRED_CHANNEL_ID is missing")
        return False

    channel_id = _required_channel_chat_id()
    print(f"ℹ️ Checking channel membership: channel_id={channel_id!r}, user_id={user_id}")

    try:
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        except Exception as direct_error:
            if not isinstance(channel_id, str) or not channel_id.startswith("@"):
                raise
            print(
                f"⚠️ Direct channel membership check failed for channel_id={channel_id!r}, "
                f"user_id={user_id}. Trying resolved numeric channel ID. "
                f"Error: {type(direct_error).__name__}: {direct_error}"
            )
            channel = await context.bot.get_chat(chat_id=channel_id)
            resolved_channel_id = int(channel.id)
            print(f"ℹ️ Resolved REQUIRED_CHANNEL_ID to numeric chat_id={resolved_channel_id}")
            member = await context.bot.get_chat_member(chat_id=resolved_channel_id, user_id=user_id)

        raw_status = getattr(member, "status", "")
        status = _normalize_chat_member_status(raw_status)
        is_member = getattr(member, "is_member", None)
        print(
            f"ℹ️ Channel membership result: channel_id={channel_id!r}, user_id={user_id}, "
            f"raw_status={raw_status!r}, normalized_status={status!r}, is_member={is_member!r}"
        )
        if status in {"member", "administrator", "creator"}:
            return True
        return status == "restricted" and bool(is_member)
    except Exception as e:
        print(
            f"⚠️ Channel membership check failed for channel_id={channel_id!r}, user_id={user_id}. "
            f"Make sure bot is admin in REQUIRED_CHANNEL_ID. Error: {type(e).__name__}: {e}"
        )
        return False


def required_channel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel", url=REQUIRED_CHANNEL_URL)],
        [InlineKeyboardButton("✅ I’ve Joined", callback_data="required_channel_check")],
    ])


async def ensure_channel_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if await check_required_channel_membership(context, user_id):
        return True
    await update.effective_message.reply_text(
        "📢 Join our official channel first.",
        reply_markup=required_channel_keyboard(),
    )
    return False


def reset_admin_temp(user_id: int):
    admin_temp[user_id] = {}


def generate_new_product_id() -> str:
    global next_product_number
    while True:
        product_id = f"p{next_product_number}"
        next_product_number += 1
        if product_id not in PRODUCTS:
            return product_id


def normalize_categories() -> bool:
    """Add category metadata without removing or replacing any existing product."""
    global next_category_number
    changed = False

    if DEFAULT_CATEGORY_ID not in CATEGORIES:
        CATEGORIES[DEFAULT_CATEGORY_ID] = {
            "id": DEFAULT_CATEGORY_ID,
            "name": DEFAULT_CATEGORY_NAME,
            "icon": "🗂",
            "order": 0,
        }
        changed = True

    for category_id, category in list(CATEGORIES.items()):
        if not isinstance(category, dict):
            CATEGORIES[category_id] = {
                "id": category_id,
                "name": str(category or category_id),
                "icon": "📁",
            }
            changed = True
            continue
        if category.get("id") != category_id:
            category["id"] = category_id
            changed = True
        if not str(category.get("name") or "").strip():
            category["name"] = DEFAULT_CATEGORY_NAME if category_id == DEFAULT_CATEGORY_ID else category_id
            changed = True
        fallback_icon = "🗂" if category_id == DEFAULT_CATEGORY_ID else "📁"
        old_icon = category.get("icon")
        old_custom_id = category.get("icon_custom_emoji_id")
        if "_normalize_category_icon_fields" in globals():
            _normalize_category_icon_fields(category, fallback=fallback_icon)
        elif not _looks_like_unicode_emoji(str(category.get("icon") or "")):
            category["icon"] = fallback_icon
        if category.get("icon") != old_icon or category.get("icon_custom_emoji_id") != old_custom_id:
            changed = True

    cleaned_order = []
    for category_id in category_order:
        if category_id in CATEGORIES and category_id not in cleaned_order:
            cleaned_order.append(category_id)
    for category_id in CATEGORIES:
        if category_id not in cleaned_order:
            cleaned_order.append(category_id)
    if cleaned_order != category_order:
        category_order.clear()
        category_order.extend(cleaned_order)
        changed = True

    for index, category_id in enumerate(category_order):
        if CATEGORIES[category_id].get("order") != index:
            CATEGORIES[category_id]["order"] = index
            changed = True

    for product in PRODUCTS.values():
        category_id = product.get("category_id")
        if category_id not in CATEGORIES:
            product["category_id"] = DEFAULT_CATEGORY_ID
            changed = True

    used_numbers = []
    for category_id in CATEGORIES:
        if category_id.startswith("cat") and category_id[3:].isdigit():
            used_numbers.append(int(category_id[3:]))
    safe_next = max([next_category_number, *[number + 1 for number in used_numbers]])
    if safe_next != next_category_number:
        next_category_number = safe_next
        changed = True
    return changed


def generate_new_category_id() -> str:
    global next_category_number
    while True:
        category_id = f"cat{next_category_number}"
        next_category_number += 1
        if category_id not in CATEGORIES:
            return category_id


def get_category_product_ids(category_id: str) -> list:
    normalize_categories()
    return [
        product_id for product_id in product_order
        if product_id in PRODUCTS and PRODUCTS[product_id].get("category_id") == category_id
    ]


def normalize_shop_order() -> bool:
    """Keep a safe combined shop order without deleting products or categories."""
    normalize_categories()
    changed = False

    valid_product_ids = [product_id for product_id in product_order if product_id in PRODUCTS]
    for product_id in PRODUCTS:
        if product_id not in valid_product_ids:
            valid_product_ids.append(product_id)

    valid_category_ids = [
        category_id for category_id in category_order
        if category_id in CATEGORIES and category_id != DEFAULT_CATEGORY_ID
    ]
    for category_id in CATEGORIES:
        if category_id != DEFAULT_CATEGORY_ID and category_id not in valid_category_ids:
            valid_category_ids.append(category_id)

    valid_items = {
        *[f"product:{product_id}" for product_id in valid_product_ids],
        *[f"category:{category_id}" for category_id in valid_category_ids],
    }
    cleaned_order = []
    for item in shop_order:
        item = str(item)
        if item in valid_items and item not in cleaned_order:
            cleaned_order.append(item)

    if not cleaned_order:
        # Match the shop layout that existed before combined ordering was introduced.
        cleaned_order.extend(f"category:{category_id}" for category_id in valid_category_ids)
        cleaned_order.extend(f"product:{product_id}" for product_id in valid_product_ids)
    else:
        for product_id in valid_product_ids:
            item = f"product:{product_id}"
            if item not in cleaned_order:
                cleaned_order.append(item)
        for category_id in valid_category_ids:
            item = f"category:{category_id}"
            if item not in cleaned_order:
                cleaned_order.append(item)

    if cleaned_order != shop_order:
        shop_order.clear()
        shop_order.extend(cleaned_order)
        changed = True

    ordered_products = [item.split(":", 1)[1] for item in shop_order if item.startswith("product:")]
    if ordered_products != product_order:
        product_order.clear()
        product_order.extend(ordered_products)
        changed = True

    ordered_categories = [DEFAULT_CATEGORY_ID]
    ordered_categories.extend(item.split(":", 1)[1] for item in shop_order if item.startswith("category:"))
    if ordered_categories != category_order:
        category_order.clear()
        category_order.extend(ordered_categories)
        changed = True

    for index, category_id in enumerate(category_order):
        if CATEGORIES[category_id].get("order") != index:
            CATEGORIES[category_id]["order"] = index
            changed = True
    return changed


def is_main_shop_order_item(item: str) -> bool:
    if item.startswith("category:"):
        return item.split(":", 1)[1] in CATEGORIES
    if item.startswith("product:"):
        product = PRODUCTS.get(item.split(":", 1)[1])
        return bool(product and product.get("category_id") == DEFAULT_CATEGORY_ID)
    return False


def shop_order_move_peers(item: str) -> list:
    normalize_shop_order()
    if is_main_shop_order_item(item):
        return [candidate for candidate in shop_order if is_main_shop_order_item(candidate)]
    if item.startswith("product:"):
        return [candidate for candidate in shop_order if candidate.startswith("product:")]
    return list(shop_order)


def can_move_shop_order_item(item: str, direction: int) -> bool:
    peers = shop_order_move_peers(item)
    if item not in peers:
        return False
    target_index = peers.index(item) + direction
    return 0 <= target_index < len(peers)


def move_shop_order_item(item: str, direction: int) -> bool:
    normalize_shop_order()
    peers = shop_order_move_peers(item)
    if item not in peers:
        return False
    peer_index = peers.index(item)
    target_peer_index = peer_index + direction
    if target_peer_index < 0 or target_peer_index >= len(peers):
        return False
    target_item = peers[target_peer_index]
    index = shop_order.index(item)
    target_index = shop_order.index(target_item)
    shop_order[index], shop_order[target_index] = shop_order[target_index], shop_order[index]
    normalize_shop_order()
    return True


def enter_client_mode(user_id: int):
    user_mode[user_id] = "client"
    user_state[user_id] = {"step": "main"}
    reset_admin_temp(user_id)


def enter_admin_mode(user_id: int):
    user_mode[user_id] = "admin"
    user_state[user_id] = {"step": "admin_main"}
    reset_admin_temp(user_id)


def parse_account_line(line: str):
    """
    Flexible stock parser.

    Supported:
    - email@gmail.com|password|note  (old format)
    - https://example.com/product-link
    - license key / coupon code / any single text line

    Delivery already uses raw_line/raw_fields first, so existing old stock keeps working
    and new link/code stock is delivered exactly as admin entered it.
    """
    raw_line = str(line or "").strip()
    if not raw_line:
        return None

    # Old format: email | password | note | extra...
    if "|" in raw_line:
        parts = [x.strip() for x in raw_line.split("|")]
        parts = [x for x in parts if x != ""]
        if len(parts) >= 2:
            email = parts[0]
            password = parts[1]
            note = " | ".join(parts[2:]) if len(parts) >= 3 else ""
            if email and password:
                return {
                    "email": email,
                    "password": password,
                    "note": note,
                    "raw_fields": parts,
                    "raw_line": " | ".join(parts),
                    "stock_type": "account",
                }

    # New flexible format: link/code/any text as one stock item.
    return {
        "email": raw_line,
        "password": "",
        "note": "",
        "raw_fields": [raw_line],
        "raw_line": raw_line,
        "stock_type": "raw",
    }


def format_stock_item_for_admin(acc: dict, max_len: int = 70) -> str:
    """Short safe preview for stock buttons/lists. Works for old accounts and raw link/code stock."""
    if not isinstance(acc, dict):
        text = str(acc or "").strip()
    else:
        text = str(acc.get("raw_line") or "").strip()
        if not text:
            raw_fields = acc.get("raw_fields")
            if isinstance(raw_fields, list) and raw_fields:
                text = " | ".join(str(x).strip() for x in raw_fields if str(x).strip())
        if not text:
            fields = [
                str(acc.get("email", "") or "").strip(),
                str(acc.get("password", "") or "").strip(),
                str(acc.get("note", "") or "").strip(),
            ]
            text = " | ".join(x for x in fields if x)

    text = text or "Stock Item"
    text = " ".join(text.split())
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def format_stock_item_full(acc: dict) -> str:
    """Full stock text for admin view and delivery."""
    if not isinstance(acc, dict):
        return str(acc or "").strip()
    raw_fields = acc.get("raw_fields")
    if isinstance(raw_fields, list) and raw_fields:
        return " | ".join(str(x).strip() for x in raw_fields if str(x).strip())
    raw_line = str(acc.get("raw_line", "") or "").strip()
    if raw_line:
        return raw_line
    fields = [
        str(acc.get("email", "") or "").strip(),
        str(acc.get("password", "") or "").strip(),
        str(acc.get("note", "") or "").strip(),
    ]
    return " | ".join(x for x in fields if x)


def get_next_order_id():
    global global_order_id
    oid = global_order_id
    global_order_id += 1
    return oid


def get_next_tx_id():
    global global_tx_id
    tid = global_tx_id
    global_tx_id += 1
    return tid


def snapshot_delivered_items(delivered_items) -> list:
    return [format_stock_item_full(item) for item in (delivered_items or [])]


def add_order_record(
    user_id: int,
    product_id: str,
    qty: int,
    total: float,
    status: str,
    payment_type: str,
    delivered_items=None,
):
    order = {
        "id": get_next_order_id(),
        "user_id": user_id,
        "product_id": product_id,
        "product": PRODUCTS[product_id]["name"],
        "qty": qty,
        "total": total,
        "status": status,
        "payment_type": payment_type,
        "created_at": now_dt(),
        "updated_at": now_dt(),
    }
    if delivered_items is not None:
        order["delivered_items"] = snapshot_delivered_items(delivered_items)
    user_orders[user_id].append(order)
    all_orders.append(order)
    return order


def add_transaction_record(user_id: int, tx_type: str, amount: float, status: str, meta=None):
    tx = {
        "id": get_next_tx_id(),
        "user_id": user_id,
        "type": tx_type,
        "amount": amount,
        "status": status,
        "meta": meta or {},
        "created_at": now_dt(),
        "updated_at": now_dt(),
    }
    user_transactions[user_id].append(tx)
    all_transactions.append(tx)
    return tx


def set_order_status(order_obj: dict, new_status: str):
    order_obj["status"] = new_status
    order_obj["updated_at"] = now_dt()


def set_tx_status(tx_obj: dict, new_status: str):
    tx_obj["status"] = new_status
    tx_obj["updated_at"] = now_dt()


def find_order_by_id(order_id: int):
    for order in all_orders:
        if order["id"] == order_id:
            return order
    return None


def find_tx_by_id(tx_id: int):
    for tx in all_transactions:
        if tx["id"] == tx_id:
            return tx
    return None


def escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# =========================
# EMOJI / CUSTOM EMOJI HELPERS
# =========================
# Telegram has two different emoji systems:
# 1) Normal Unicode emoji: stored directly in product["icon"].
# 2) Telegram custom/animated emoji: stored in product["icon_custom_emoji_id"].
#
# IMPORTANT:
# A previous emoji build could save the whole Telegram entity dict into product["icon"].
# Then buttons show text like {'type': 'custom_emoji', 'emoji_id': ...}.
# The helpers below clean that automatically and keep only:
#   product["icon"] = fallback emoji text
#   product["icon_custom_emoji_id"] = Telegram custom emoji ID

def _is_custom_emoji_entity(entity) -> bool:
    entity_type = str(getattr(entity, "type", ""))
    return entity_type == "custom_emoji" or entity_type == "MessageEntityType.CUSTOM_EMOJI"


def _looks_like_unicode_emoji(text: str) -> bool:
    text = str(text or "").strip()
    if not text or len(text) > 16:
        return False
    if any(ch.isspace() for ch in text):
        return False

    has_emoji_codepoint = False
    for ch in text:
        code = ord(ch)
        if ch in ("\ufe0f", "\ufe0e", "\u200d"):
            continue
        if 0x1F000 <= code <= 0x1FAFF:
            has_emoji_codepoint = True
            continue
        if 0x2600 <= code <= 0x27BF:
            has_emoji_codepoint = True
            continue
        if 0x1F1E6 <= code <= 0x1F1FF:
            has_emoji_codepoint = True
            continue
        if 0xE0020 <= code <= 0xE007F:
            continue
        if 0xFE00 <= code <= 0xFE0F:
            continue
        return False
    return has_emoji_codepoint


def _entity_text_by_utf16(text: str, entity) -> str:
    """Telegram MessageEntity offsets are UTF-16 based, not Python indexes."""
    try:
        offset = int(getattr(entity, "offset", 0) or 0)
        length = int(getattr(entity, "length", 0) or 0)
        raw = str(text or "").encode("utf-16-le")
        part = raw[offset * 2:(offset + length) * 2]
        return part.decode("utf-16-le").strip()
    except Exception:
        return ""


def _clean_icon_text(value, fallback: str = "🔹") -> str:
    """Return a safe short emoji text. Never returns a dict string for button labels."""
    if isinstance(value, dict):
        for key in ("emoji", "text", "fallback", "icon"):
            candidate = value.get(key)
            if candidate and not isinstance(candidate, (dict, list, tuple, set)):
                candidate = str(candidate).strip()
                if _looks_like_unicode_emoji(candidate):
                    return candidate
        return fallback

    text = str(value or "").strip()
    if not text or text.startswith("{") or text.startswith("["):
        return fallback
    if _looks_like_unicode_emoji(text):
        return text
    # Keep old simple icons if they exist, but avoid huge/bad text.
    if len(text) <= 4 and not any(ch.isspace() for ch in text):
        return text
    return fallback


def _extract_custom_emoji_id_from_icon_value(value) -> str:
    """Read a custom emoji ID from old/new saved icon structures."""
    if isinstance(value, dict):
        for key in ("icon_custom_emoji_id", "custom_emoji_id", "emoji_id", "emoji-id", "id"):
            candidate = str(value.get(key) or "").strip()
            if candidate:
                return candidate
    return ""


def _product_custom_emoji_id(product_or_temp) -> str:
    product_or_temp = product_or_temp or {}
    custom_id = str(product_or_temp.get("icon_custom_emoji_id") or "").strip()
    if custom_id:
        return custom_id
    return _extract_custom_emoji_id_from_icon_value(product_or_temp.get("icon"))


def _normal_icon_text(product_or_temp, fallback: str = "📦") -> str:
    product_or_temp = product_or_temp or {}
    icon_value = product_or_temp.get("icon", fallback)
    return _clean_icon_text(icon_value, fallback=("🔹" if _product_custom_emoji_id(product_or_temp) else fallback))


def _normalize_product_icon_fields(product: dict):
    """Migrate icon dict/string problems into clean fields. Does not touch any product data except icon fields."""
    if not isinstance(product, dict):
        return product
    custom_id = _product_custom_emoji_id(product)
    icon_text = _normal_icon_text(product, fallback="📦")
    product["icon"] = icon_text
    if custom_id:
        product["icon_custom_emoji_id"] = custom_id
    else:
        product.pop("icon_custom_emoji_id", None)
    return product


def normalize_all_product_icons():
    try:
        for product in PRODUCTS.values():
            _normalize_product_icon_fields(product)
    except Exception as e:
        print("⚠️ Failed to normalize product icons:", e)


def _extract_supported_icon_from_message(message):
    """
    Return (icon_text, custom_emoji_id, error).

    Normal emoji => icon_text is the emoji, custom_emoji_id is None.
    Telegram custom/animated emoji => icon_text is a safe fallback emoji, custom_emoji_id is saved.
    """
    sticker = getattr(message, "sticker", None)
    if sticker is not None:
        custom_id = str(getattr(sticker, "custom_emoji_id", "") or "").strip()
        if not custom_id:
            return None, None, "❌ <b>This sticker is not a Telegram custom emoji.</b> Please send a custom emoji or normal emoji."
        fallback = _clean_icon_text(getattr(sticker, "emoji", None), fallback="🔹")
        return fallback, custom_id, None

    text = str(getattr(message, "text", None) or getattr(message, "caption", None) or "").strip()
    entities = list(getattr(message, "entities", None) or getattr(message, "caption_entities", None) or [])
    custom_entities = [e for e in entities if _is_custom_emoji_entity(e)]

    if custom_entities:
        if len(custom_entities) != 1:
            return None, None, "❌ <b>Please send only one emoji icon.</b>"
        entity = custom_entities[0]
        custom_id = str(getattr(entity, "custom_emoji_id", "") or "").strip()
        if not custom_id:
            return None, None, "❌ <b>Could not read this custom emoji ID.</b> Please send another emoji."
        fallback = _entity_text_by_utf16(text, entity) or text or "🔹"
        cleaned_without_entity = text.replace(fallback, "", 1).strip() if fallback else text.strip()
        if cleaned_without_entity:
            return None, None, "❌ <b>Please send only the emoji, without extra text.</b>"
        fallback = _clean_icon_text(fallback, fallback="🔹")
        return fallback, custom_id, None

    if not text:
        return None, None, "❌ <b>Icon cannot be empty.</b> Please send an emoji."
    if not _looks_like_unicode_emoji(text):
        return None, None, "❌ <b>This emoji is not supported.</b> Please send another emoji."
    return text, None, None


def _icon_html(icon_text: str = "📦", custom_emoji_id: str = None) -> str:
    icon_text = _clean_icon_text(icon_text, fallback="🔹")
    custom_emoji_id = str(custom_emoji_id or "").strip()
    if custom_emoji_id:
        return f'<tg-emoji emoji-id="{escape_html(custom_emoji_id)}">{escape_html(icon_text)}</tg-emoji>'
    return escape_html(icon_text)


def product_icon_html(product_or_temp) -> str:
    return _icon_html(_normal_icon_text(product_or_temp), _product_custom_emoji_id(product_or_temp))


def product_label_prefix(product_or_temp) -> str:
    # If custom emoji ID exists, do NOT put product["icon"] in the text.
    # The icon should be sent via icon_custom_emoji_id, otherwise Telegram may show fallback text/dict.
    if _product_custom_emoji_id(product_or_temp):
        return ""
    return _normal_icon_text(product_or_temp, fallback="📦")


def product_label_text(product_or_temp, core_text: str) -> str:
    prefix = product_label_prefix(product_or_temp)
    return f"{prefix} {core_text}".strip() if prefix else str(core_text)


def make_inline_button_with_optional_icon(
    text: str,
    callback_data: str,
    custom_emoji_id: str = None,
    style: str = None,
):
    return make_styled_inline_button(
        str(text),
        callback_data=callback_data,
        style=style,
        icon_custom_emoji_id=custom_emoji_id,
    )


def make_product_inline_button(product_or_temp, core_text: str, callback_data: str, style: str = None):
    label = product_label_text(product_or_temp, core_text)
    try:
        label = _short_button_text(label)
    except Exception:
        pass
    return make_inline_button_with_optional_icon(
        label,
        callback_data,
        _product_custom_emoji_id(product_or_temp),
        style=style,
    )


def _category_custom_emoji_id(category) -> str:
    category = category or {}
    custom_id = str(category.get("icon_custom_emoji_id") or "").strip()
    if custom_id:
        return custom_id
    return _extract_custom_emoji_id_from_icon_value(category.get("icon"))


def _category_normal_icon_text(category, fallback: str = "📁") -> str:
    category = category or {}
    icon_value = category.get("icon", fallback)
    safe_fallback = "🔹" if _category_custom_emoji_id(category) else fallback
    return _clean_icon_text(icon_value, fallback=safe_fallback)


def _normalize_category_icon_fields(category: dict, fallback: str = "📁"):
    """Clean only category icon fields; never changes category IDs, order, or products."""
    if not isinstance(category, dict):
        return category
    custom_id = _category_custom_emoji_id(category)
    category["icon"] = _category_normal_icon_text(category, fallback=fallback)
    if custom_id:
        category["icon_custom_emoji_id"] = custom_id
    else:
        category.pop("icon_custom_emoji_id", None)
    return category


def normalize_all_category_icons():
    try:
        for category_id, category in CATEGORIES.items():
            fallback = "🗂" if category_id == DEFAULT_CATEGORY_ID else "📁"
            _normalize_category_icon_fields(category, fallback=fallback)
    except Exception as e:
        print(f"Failed to normalize category icons: {type(e).__name__}")


def category_icon_html(category) -> str:
    return _icon_html(_category_normal_icon_text(category), _category_custom_emoji_id(category))


def category_label_prefix(category) -> str:
    if _category_custom_emoji_id(category):
        return ""
    return _category_normal_icon_text(category)


def category_label_text(category, core_text: str) -> str:
    prefix = category_label_prefix(category)
    return f"{prefix} {core_text}".strip() if prefix else str(core_text)


def make_category_inline_button(category, core_text: str, callback_data: str, style: str = None):
    label = _short_button_text(category_label_text(category, core_text))
    return make_inline_button_with_optional_icon(
        label,
        callback_data,
        _category_custom_emoji_id(category),
        style=style,
    )

# =========================
# ADVANCED USER / PROMO HELPERS
# =========================
def generate_unique_promo_code(length: int = 10):
    alphabet = string.ascii_uppercase + string.digits
    while True:
        code = "".join(random.choice(alphabet) for _ in range(length))
        if code not in PROMO_CODES:
            return code


def get_user_total_deposit(user_id: int) -> float:
    total = 0.0
    for tx in user_transactions.get(user_id, []):
        if tx["type"] == "Deposit" and tx["status"] == "Completed":
            total += float(tx["amount"])
    return total


def get_user_total_order_spent(user_id: int) -> float:
    total = 0.0
    for order in user_orders.get(user_id, []):
        if order["status"] == "Completed":
            total += float(order["total"])
    return total


def get_user_completed_orders_count(user_id: int) -> int:
    count = 0
    for order in user_orders.get(user_id, []):
        if order["status"] == "Completed":
            count += 1
    return count


def get_user_pending_orders_count(user_id: int) -> int:
    count = 0
    for order in user_orders.get(user_id, []):
        if order["status"] == "Waiting Manual Confirmation":
            count += 1
    return count


def get_user_completed_deposit_count(user_id: int) -> int:
    count = 0
    for tx in user_transactions.get(user_id, []):
        if tx["type"] == "Deposit" and tx["status"] == "Completed":
            count += 1
    return count


def get_user_pending_deposit_count(user_id: int) -> int:
    count = 0
    for tx in user_transactions.get(user_id, []):
        if tx["type"] == "Deposit" and tx["status"] == "Waiting Manual Confirmation":
            count += 1
    return count


def get_user_search_summary_text(user_id: int) -> str:
    ensure_user(user_id)

    wallet_balance = user_wallet.get(user_id, 0.0)
    total_deposit = get_user_total_deposit(user_id)
    total_spent = get_user_total_order_spent(user_id)
    completed_orders = get_user_completed_orders_count(user_id)
    pending_orders = get_user_pending_orders_count(user_id)
    completed_deposits = get_user_completed_deposit_count(user_id)
    pending_deposits = get_user_pending_deposit_count(user_id)

    lines = [
        "🆔 <b>USER SEARCH RESULT</b>",
        "",
        f"<b>User ID:</b> <code>{user_id}</code>",
        f"<b>Current Wallet:</b> {format_money(wallet_balance)}",
        f"<b>Total Deposit:</b> {format_money(total_deposit)}",
        f"<b>Total Spent:</b> {format_money(total_spent)}",
        f"<b>Completed Deposits:</b> {completed_deposits}",
        f"<b>Pending Deposits:</b> {pending_deposits}",
        f"<b>Completed Orders:</b> {completed_orders}",
        f"<b>Pending Orders:</b> {pending_orders}",
        "",
        "━━━━━━━━━━━━━━",
        "",
        "<b>Recent Transactions:</b>",
    ]

    txs = user_transactions.get(user_id, [])
    if not txs:
        lines.append("No transactions found.")
    else:
        for tx in reversed(txs[-12:]):
            lines.append(
                f"\nTX#{tx['id']} | {tx['type']}\n"
                f"Amount: {format_money(tx['amount'])}\n"
                f"Status: {tx['status']}\n"
                f"Date: {format_dt(tx.get('created_at'))}"
            )

    lines.extend(["", "━━━━━━━━━━━━━━", "", "<b>Recent Orders:</b>"])

    orders = user_orders.get(user_id, [])
    if not orders:
        lines.append("No orders found.")
    else:
        for order in reversed(orders[-12:]):
            lines.append(
                f"\nOrder#{order['id']} | {order['product']}\n"
                f"Qty: {order['qty']}\n"
                f"Total: {format_money(order['total'])}\n"
                f"Payment: {order.get('payment_type', 'Unknown')}\n"
                f"Status: {order['status']}\n"
                f"Date: {format_dt(order.get('created_at'))}"
            )

    return "\n".join(lines)


# =========================
# ADMIN BALANCE HELPERS
# =========================
def render_admin_balance_panel() -> str:
    return (
        "💰 <b>USER BALANCE ADMIN</b>\n\n"
        "Add, minus, or check any user wallet balance.\n\n"
        "Choose an action below."
    )


def render_balance_check_text(target_user_id: int) -> str:
    ensure_user(int(target_user_id))
    balance = float(user_wallet.get(int(target_user_id), 0.0))
    return (
        "🔎 <b>USER BALANCE</b>\n\n"
        f"<b>User ID:</b> <code>{int(target_user_id)}</code>\n"
        f"<b>Wallet Balance:</b> {format_money(balance)}"
    )


def render_balance_adjust_preview(admin_id: int) -> str:
    temp = admin_temp.get(admin_id, {})
    action = temp.get("balance_action", "add")
    target_user_id = int(temp.get("target_user_id"))
    amount = float(temp.get("balance_amount", 0))
    reason = str(temp.get("balance_reason") or "Admin adjustment").strip()
    current_balance = float(user_wallet.get(target_user_id, 0.0))
    if action == "add":
        new_balance = current_balance + amount
        action_text = "➕ Add Balance"
        change_text = f"+{format_money(amount)}"
    else:
        new_balance = current_balance - amount
        action_text = "➖ Minus Balance"
        change_text = f"-{format_money(amount)}"

    return (
        "💰 <b>CONFIRM BALANCE UPDATE</b>\n\n"
        f"<b>Action:</b> {action_text}\n"
        f"<b>User ID:</b> <code>{target_user_id}</code>\n"
        f"<b>Current Balance:</b> {format_money(current_balance)}\n"
        f"<b>Change:</b> {change_text}\n"
        f"<b>New Balance:</b> {format_money(new_balance)}\n"
        f"<b>Reason:</b> {escape_html(reason)}\n\n"
        "Confirm this wallet update?"
    )


def balance_adjust_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm Update", callback_data="balance_confirm_update")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")],
    ])


def apply_admin_balance_adjustment(admin_id: int):
    temp = admin_temp.get(admin_id, {})
    action = temp.get("balance_action")
    target_user_id = int(temp.get("target_user_id"))
    amount = float(temp.get("balance_amount", 0))
    reason = str(temp.get("balance_reason") or "Admin adjustment").strip()

    ensure_user(target_user_id)
    old_balance = float(user_wallet.get(target_user_id, 0.0))

    if amount <= 0:
        return False, "Invalid amount.", old_balance, old_balance

    if action == "minus" and old_balance < amount:
        return False, "Insufficient wallet balance. Balance cannot go below $0.00.", old_balance, old_balance

    if action == "add":
        new_balance = old_balance + amount
        tx_type = "Admin Credit"
        tx_amount = amount
    else:
        new_balance = old_balance - amount
        tx_type = "Admin Deduct"
        tx_amount = -amount

    user_wallet[target_user_id] = round(new_balance, 2)
    add_transaction_record(
        target_user_id,
        tx_type,
        tx_amount,
        "Completed",
        {
            "admin_id": admin_id,
            "reason": reason,
            "old_balance": old_balance,
            "new_balance": user_wallet[target_user_id],
        },
    )

    update_user_profile(target_user_id)
    return True, "Balance updated successfully.", old_balance, user_wallet[target_user_id]


# =========================
# GOLD VIP HELPERS
# =========================
def render_gold_vip_panel() -> str:
    vip_price_count = len([pid for pid in PRODUCTS if get_product_vip_price(pid) is not None])
    return (
        "👑 <b>GOLD VIP ADMIN</b>\n\n"
        f"<b>Total VIP Users:</b> {len(gold_vip_users)}\n"
        f"<b>Products With VIP Price:</b> {vip_price_count}\n\n"
        "Manage reseller/VIP users and product VIP prices."
    )


def render_gold_vip_user_list() -> str:
    lines = ["👑 <b>GOLD VIP USERS</b>", ""]
    if not gold_vip_users:
        lines.append("No Gold VIP users added yet.")
        return "\n".join(lines)
    for idx, uid in enumerate(sorted(gold_vip_users), start=1):
        lines.append(f"{idx}. {format_user_link(uid)} | ID: <code>{uid}</code>")
    return "\n".join(lines)


def render_gold_vip_price_list() -> str:
    lines = ["👑 <b>GOLD VIP PRICE LIST</b>", ""]
    found = False
    for product_id in product_order:
        if product_id not in PRODUCTS:
            continue
        vip_price = get_product_vip_price(product_id)
        if vip_price is None:
            continue
        found = True
        product = PRODUCTS[product_id]
        lines.append(
            f"{product_icon_html(product)} <b>{escape_html(product.get('name', product_id))}</b> "
            f"(<code>{product_id}</code>)\n"
            f"Normal: {format_money(get_product_base_price(product_id))} | Gold VIP: <b>{format_money(vip_price)}</b>"
        )
    if not found:
        lines.append("No VIP price set yet.")
    return "\n\n".join(lines)


def render_gold_vip_price_preview(admin_id: int) -> str:
    temp = admin_temp.get(admin_id, {})
    product_id = temp.get("selected_product_id")
    product = PRODUCTS.get(product_id, {})
    new_price = float(temp.get("gold_vip_price", 0))
    old_vip = get_product_vip_price(product_id)
    old_vip_text = format_money(old_vip) if old_vip is not None else "Not set"
    return (
        "👑 <b>CONFIRM GOLD VIP PRICE</b>\n\n"
        f"<b>Product:</b> {product_icon_html(product)} {escape_html(product.get('name', product_id))}\n"
        f"<b>Normal Price:</b> {format_money(get_product_base_price(product_id))}\n"
        f"<b>Old VIP Price:</b> {old_vip_text}\n"
        f"<b>New VIP Price:</b> {format_money(new_price)}\n\n"
        "Only Gold VIP users will see/pay this price."
    )


def gold_vip_price_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm VIP Price", callback_data="vip_confirm_price")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")],
    ])


async def send_gold_vip_welcome(bot, target_user_id: int):
    text = (
        "👑 <b>Congratulations!</b>\n\n"
        "You are now a <b>Gold VIP</b> member.\n"
        "You can now see special reseller/VIP prices on selected products.\n\n"
        "🛍 Open the shop and enjoy your VIP deals."
    )
    try:
        await bot.send_message(chat_id=target_user_id, text=text, parse_mode="HTML")
    except Exception as e:
        print(f"⚠️ Failed to send Gold VIP welcome to {target_user_id}:", e)


# =========================
# FLASH DEAL HELPERS
# =========================
def _flash_deal_end_at():
    end_at = active_flash_deal.get("end_at") if isinstance(active_flash_deal, dict) else None
    if isinstance(end_at, str):
        try:
            end_at = datetime.fromisoformat(end_at)
            active_flash_deal["end_at"] = end_at
        except Exception:
            return None
    return end_at


def _flash_seconds_left() -> int:
    end_at = _flash_deal_end_at()
    if not end_at:
        return 0
    return max(0, int((end_at - now_dt()).total_seconds()))


def format_countdown_seconds(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def is_flash_deal_active() -> bool:
    if not isinstance(active_flash_deal, dict) or not active_flash_deal:
        return False
    product_id = active_flash_deal.get("product_id")
    if product_id not in PRODUCTS:
        active_flash_deal.clear()
        return False
    try:
        if float(active_flash_deal.get("deal_price", 0)) <= 0:
            active_flash_deal.clear()
            return False
    except Exception:
        active_flash_deal.clear()
        return False
    if _flash_seconds_left() <= 0:
        active_flash_deal.clear()
        return False
    return True


def get_flash_deal_price(product_id: str):
    if is_flash_deal_active() and active_flash_deal.get("product_id") == product_id:
        try:
            return float(active_flash_deal.get("deal_price"))
        except Exception:
            return None
    return None


def render_flash_deal_banner() -> str:
    if not is_flash_deal_active():
        return ""
    product_id = active_flash_deal.get("product_id")
    product = PRODUCTS.get(product_id, {})
    deal_price = float(active_flash_deal.get("deal_price", 0))
    old_price = get_product_base_price(product_id)
    time_left = format_countdown_seconds(_flash_seconds_left())
    return (
        "╭━━━━━━━ ⚡ <b>FLASH DEAL</b> ━━━━━━━╮\n\n"
        f"{product_icon_html(product)} <b>{escape_html(product.get('name', product_id))}</b>\n\n"
        f"💸 Regular: <s>{format_money(old_price)}</s>\n"
        f"🔥 Deal: <b>{format_money(deal_price)}</b>\n\n"
        f"⏳ Ends in: <b>{time_left}</b>\n\n"
        "╰━━ 🛒 Grab it before it’s gone ━━╯"
    )


def render_flash_deal_panel() -> str:
    if not is_flash_deal_active():
        return "⚡ <b>FLASH DEAL ADMIN</b>\n\nNo active flash deal right now."
    product_id = active_flash_deal.get("product_id")
    product = PRODUCTS.get(product_id, {})
    return (
        "⚡ <b>FLASH DEAL ADMIN</b>\n\n"
        "<b>Status:</b> Active\n"
        f"<b>Product:</b> {product_icon_html(product)} {escape_html(product.get('name', product_id))}\n"
        f"<b>Regular Price:</b> {format_money(get_product_base_price(product_id))}\n"
        f"<b>Deal Price:</b> {format_money(float(active_flash_deal.get('deal_price', 0)))}\n"
        f"<b>Time Left:</b> {format_countdown_seconds(_flash_seconds_left())}"
    )


def flash_deal_admin_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("⚡ Start Flash Deal", callback_data="flash_start_menu")]]
    if is_flash_deal_active():
        rows.append([InlineKeyboardButton("🛑 Cancel Active Deal", callback_data="flash_cancel_active")])
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="flash_refresh")])
    rows.append([InlineKeyboardButton("⬅️ Close", callback_data="flash_close")])
    return InlineKeyboardMarkup(rows)


def flash_deal_product_select_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for product_id in product_order:
        if product_id not in PRODUCTS:
            continue
        product = PRODUCTS[product_id]
        core_text = f"{product.get('name', product_id)} | {format_money(get_product_base_price(product_id))}"
        rows.append([make_product_inline_button(product, core_text, f"flash_pick_product_{product_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="flash_back")])
    return InlineKeyboardMarkup(rows)


def flash_deal_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Start Flash Deal", callback_data="flash_confirm_start")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")],
    ])


def render_flash_deal_confirm(admin_id: int) -> str:
    temp = admin_temp.get(admin_id, {})
    product_id = temp.get("selected_product_id")
    product = PRODUCTS.get(product_id, {})
    deal_price = float(temp.get("flash_deal_price", 0))
    minutes = int(temp.get("flash_deal_minutes", 0))
    return (
        "⚡ <b>CONFIRM FLASH DEAL</b>\n\n"
        f"<b>Product:</b> {product_icon_html(product)} {escape_html(product.get('name', product_id))}\n"
        f"<b>Regular Price:</b> {format_money(get_product_base_price(product_id))}\n"
        f"<b>Deal Price:</b> {format_money(deal_price)}\n"
        f"<b>Duration:</b> {minutes} minutes\n\n"
        "After start, users will see the deal price in Shop."
    )


async def _send_flash_deal_broadcast_job(bot, product_id: str, deal_price: float, minutes: int, admin_id: int = None):
    product = PRODUCTS.get(product_id, {})
    text = (
        "⚡ <b>FLASH DEAL LIVE!</b>\n\n"
        f"{product_icon_html(product)} <b>{escape_html(product.get('name', product_id))}</b>\n"
        f"💸 Regular: <s>{format_money(get_product_base_price(product_id))}</s>\n"
        f"🔥 Deal: <b>{format_money(deal_price)}</b>\n"
        f"⏳ Duration: <b>{minutes} minutes</b>\n\n"
        "🛒 Open shop and grab it before it ends."
    )
    fallback_text = text.replace(product_icon_html(product), escape_html(_normal_icon_text(product)), 1)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Open Shop", callback_data="back_shop_cards")]])
    targets = sorted(int(uid) for uid in list(all_users))
    sent = 0
    failed = 0
    for uid in targets:
        try:
            await bot.send_message(chat_id=int(uid), text=text, reply_markup=keyboard, parse_mode="HTML")
            sent += 1
        except Exception:
            fallback_sent = False
            if fallback_text != text:
                try:
                    await bot.send_message(chat_id=int(uid), text=fallback_text, reply_markup=keyboard, parse_mode="HTML")
                    sent += 1
                    fallback_sent = True
                except Exception:
                    pass
            if not fallback_sent:
                failed += 1
        # Yield to the event loop so normal user buttons keep responding during broadcast.
        await asyncio.sleep(0.03)

    if admin_id:
        try:
            await bot.send_message(
                chat_id=int(admin_id),
                text=(
                    "✅ <b>Flash deal broadcast completed</b>\n\n"
                    f"<b>Sent:</b> {sent}\n"
                    f"<b>Failed/Blocked:</b> {failed}\n"
                    f"<b>Total targets:</b> {len(targets)}"
                ),
                parse_mode="HTML",
            )
        except Exception:
            pass


def start_flash_deal_broadcast_background(bot, product_id: str, deal_price: float, minutes: int, admin_id: int = None):
    asyncio.create_task(_send_flash_deal_broadcast_job(bot, product_id, deal_price, minutes, admin_id))


def serialize_message_entities(entities) -> list:
    serialized = []
    for entity in entities or []:
        try:
            entity_data = entity.to_dict() if hasattr(entity, "to_dict") else dict(entity)
            json.dumps(entity_data)
            serialized.append(entity_data)
        except Exception as e:
            print(f"Broadcast entity serialization failed: {type(e).__name__}")
            return []
    return serialized


def deserialize_message_entities(entity_data: list, bot=None) -> list:
    entities = []
    for item in entity_data or []:
        try:
            if isinstance(item, MessageEntity):
                entities.append(item)
            elif isinstance(item, dict):
                entities.append(MessageEntity.de_json(dict(item), bot))
        except Exception as e:
            print(f"Broadcast entity deserialization failed: {type(e).__name__}")
            return []
    return entities


def render_serialized_entities_html(text: str, entity_data: list) -> str:
    from telegram import Chat, Message

    entities = deserialize_message_entities(entity_data)
    if not entities:
        return ""
    try:
        message = Message(message_id=0, date=datetime.now(), chat=Chat(id=0, type="private"), text=text, entities=entities)
        return message.text_html
    except Exception as e:
        print(f"Rich product details rendering failed; using plain text: {type(e).__name__}")
        return ""


def normalize_broadcast_chat_id(chat_id):
    value = str(chat_id or "").strip()
    if value.lstrip("-").isdigit():
        return int(value)
    return value


def get_broadcast_entity_types(entity_data: list, entities: list = None) -> list:
    entity_types = []
    for item in entity_data or []:
        value = item.get("type") if isinstance(item, dict) else getattr(item, "type", "")
        value = getattr(value, "value", value)
        if value:
            entity_types.append(str(value))
    if not entity_types:
        for entity in entities or []:
            value = getattr(entity, "type", "")
            value = getattr(value, "value", value)
            if value:
                entity_types.append(str(value))
    return entity_types


async def send_rich_broadcast_message(
    bot,
    chat_id,
    message: str,
    entity_data: list = None,
    reply_markup=None,
    report_delivery_mode: bool = False,
    preserve_custom_emoji: bool = False,
):
    target_chat_id = normalize_broadcast_chat_id(chat_id)
    entities = deserialize_message_entities(entity_data, bot)
    entity_types = get_broadcast_entity_types(entity_data, entities)
    has_custom_emoji = "custom_emoji" in entity_types
    if preserve_custom_emoji and has_custom_emoji and not entities:
        raise ValueError("Custom emoji entities could not be deserialized.")
    if entities:
        try:
            result = await bot.send_message(
                chat_id=target_chat_id,
                text=message,
                entities=entities,
                reply_markup=reply_markup,
            )
            return (result, "rich") if report_delivery_mode else result
        except Exception as e:
            type_summary = ",".join(sorted(set(entity_types))) or "unknown"
            if preserve_custom_emoji and has_custom_emoji:
                print(
                    "Rich channel broadcast failed; plain fallback skipped "
                    f"to preserve custom emoji (entity_count={len(entities)}, types={type_summary}): "
                    f"{type(e).__name__}"
                )
                raise
            print(
                "Rich broadcast send failed; using plain text "
                f"(entity_count={len(entities)}, types={type_summary}): {type(e).__name__}"
            )
            result = await bot.send_message(chat_id=target_chat_id, text=message, reply_markup=reply_markup)
            return (result, "plain_fallback") if report_delivery_mode else result
    result = await bot.send_message(chat_id=target_chat_id, text=message, reply_markup=reply_markup)
    return (result, "plain") if report_delivery_mode else result


def format_channel_broadcast_error(error) -> str:
    if isinstance(error, str):
        return error
    message = str(error or "").strip()
    lowered = message.lower()
    if "chat not found" in lowered:
        return "Chat not found."
    if "forbidden" in lowered:
        return "Forbidden. Make sure the bot is an admin in the configured channel."
    if "administrator" in lowered or "not enough rights" in lowered or "not an admin" in lowered:
        return "Bot is not admin in the configured channel."
    if "custom emoji" in lowered:
        return "Telegram rejected the custom emoji entity."
    return f"Telegram error ({type(error).__name__})."


async def _send_admin_broadcast_job(
    bot,
    admin_id: int,
    message: str,
    entity_data: list,
    targets: list,
    destination: str = "users",
    channel_id=None,
):
    sent = 0
    failed = 0
    channel_result = "Not selected"

    if destination in {"channel", "both"}:
        if not str(channel_id or "").strip():
            channel_result = "Failed — Channel ID is not configured."
        else:
            try:
                _, delivery_mode = await send_rich_broadcast_message(
                    bot,
                    channel_id,
                    message,
                    entity_data,
                    report_delivery_mode=True,
                    preserve_custom_emoji=True,
                )
                if delivery_mode == "rich":
                    channel_result = "Sent with rich text"
                else:
                    channel_result = "Sent without rich text fallback"
            except Exception as e:
                channel_result = f"Failed — {format_channel_broadcast_error(e)}"

    if destination in {"users", "both"}:
        for target_id in targets:
            try:
                await send_rich_broadcast_message(bot, int(target_id), message, entity_data)
                sent += 1
            except Exception:
                failed += 1
            # Yield to the event loop so normal user buttons keep responding during broadcast.
            await asyncio.sleep(0.03)

    try:
        await bot.send_message(
            chat_id=int(admin_id),
            text=(
                "✅ <b>Broadcast completed</b>\n\n"
                f"<b>Users sent:</b> {sent}\n"
                f"<b>Users failed:</b> {failed}\n"
                f"<b>Channel:</b> {escape_html(channel_result)}"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


def start_admin_broadcast_background(
    bot,
    admin_id: int,
    message: str,
    entity_data: list,
    targets: list,
    destination: str = "users",
    channel_id=None,
):
    asyncio.create_task(
        _send_admin_broadcast_job(
            bot,
            admin_id,
            message,
            entity_data,
            targets,
            destination=destination,
            channel_id=channel_id,
        )
    )


# =========================
# ADMIN NOTIFY REQUEST HELPERS
# =========================
def _normalize_notify_user_ids(users) -> set:
    cleaned = set()
    for uid in (users or []):
        try:
            cleaned.add(int(uid))
        except Exception:
            continue
    return cleaned


def normalize_notify_waitlist():
    """Keep notify waitlist clean and product-id based. Admin counts use this only."""
    try:
        # Merge any legacy keys saved by product name back into the correct product_id.
        name_to_pid = {str(p.get("name", "")).strip(): pid for pid, p in PRODUCTS.items()}
        for key in list(notify_waitlist.keys()):
            if key in PRODUCTS:
                notify_waitlist[key] = _normalize_notify_user_ids(notify_waitlist.get(key, set()))
                continue
            pid = name_to_pid.get(str(key).strip())
            if pid:
                notify_waitlist.setdefault(pid, set()).update(_normalize_notify_user_ids(notify_waitlist.get(key, set())))
            notify_waitlist.pop(key, None)

        for pid in PRODUCTS:
            notify_waitlist.setdefault(pid, set())
            notify_waitlist[pid] = _normalize_notify_user_ids(notify_waitlist.get(pid, set()))
    except Exception as e:
        print("⚠️ Failed to normalize notify waitlist:", e)


def sync_notify_waitlist_from_database():
    """Admin-only lightweight sync, so notify counts show latest saved data without touching other bot data."""
    if not USE_DATABASE or db is None:
        normalize_notify_waitlist()
        return
    try:
        data = db.load_state() or {}
        saved_waitlist = data.get("notify_waitlist", {}) or {}
        if isinstance(saved_waitlist, dict):
            for pid, users in saved_waitlist.items():
                notify_waitlist.setdefault(pid, set()).update(_normalize_notify_user_ids(users))
        normalize_notify_waitlist()
    except Exception as e:
        print("⚠️ Failed to sync notify waitlist from database:", e)
        normalize_notify_waitlist()


def _notify_waiters(product_id: str) -> set:
    normalize_notify_waitlist()
    return _normalize_notify_user_ids(notify_waitlist.get(product_id, set()))


def _notify_count(product_id: str) -> int:
    return len(_notify_waiters(product_id))


def render_admin_notify_panel() -> str:
    sync_notify_waitlist_from_database()
    total_waiting = sum(_notify_count(pid) for pid in PRODUCTS)
    active_products = len([pid for pid in PRODUCTS if _notify_count(pid) > 0])
    return (
        "🔔 <b>NOTIFY REQUESTS</b>\n\n"
        f"<b>Total Waiting Users:</b> {total_waiting}\n"
        f"<b>Products With Demand:</b> {active_products}\n\n"
        "Choose an option below."
    )


def render_notify_requests_summary() -> str:
    sync_notify_waitlist_from_database()
    lines = [
        "🔔 <b>NOTIFY REQUESTS SUMMARY</b>",
        "",
    ]

    found = False
    for product_id in product_order:
        product = PRODUCTS.get(product_id, {})
        count = _notify_count(product_id)
        if count <= 0:
            continue
        found = True
        lines.append(
            f"{product_icon_html(product)} <b>{escape_html(product.get('name', product_id))}</b> "
            f"(<code>{product_id}</code>) — <b>{count}</b> waiting"
        )

    if not found:
        lines.append("No notify requests found yet.")

    return "\n".join(lines)


def render_notify_product_details(product_id: str) -> str:
    sync_notify_waitlist_from_database()
    product = PRODUCTS.get(product_id, {})
    waiters = sorted(_notify_waiters(product_id))
    lines = [
        "🔔 <b>PRODUCT NOTIFY DETAILS</b>",
        "",
        f"<b>Product:</b> {product_icon_html(product)} {escape_html(product.get('name', product_id))}",
        f"<b>Product ID:</b> <code>{product_id}</code>",
        f"<b>Waiting Users:</b> {len(waiters)}",
        "",
    ]

    if not waiters:
        lines.append("No users are waiting for this product.")
        return "\n".join(lines)

    lines.append("<b>User List:</b>")
    for idx, uid in enumerate(waiters[:60], start=1):
        lines.append(f"{idx}. {format_user_link(uid)} | ID: <code>{uid}</code>")

    if len(waiters) > 60:
        lines.append(f"\nAnd {len(waiters) - 60} more users...")

    return "\n".join(lines)


def admin_notify_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📊 Summary", callback_data="admin_notify_summary")],
        [InlineKeyboardButton("📦 Product Details", callback_data="admin_notify_products")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="admin_notify_refresh")],
        [InlineKeyboardButton("⬅️ Close", callback_data="admin_notify_close")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_notify_product_select_keyboard() -> InlineKeyboardMarkup:
    sync_notify_waitlist_from_database()
    rows = []
    for product_id in product_order:
        product = PRODUCTS.get(product_id, {})
        count = _notify_count(product_id)
        core_text = f"{product.get('name', product_id)} ({count} waiting)"
        rows.append([make_product_inline_button(product, core_text, f"admin_notify_product_{product_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_notify_back")])
    return InlineKeyboardMarkup(rows)


def admin_notify_product_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Product List", callback_data="admin_notify_products")],
        [InlineKeyboardButton("📊 Summary", callback_data="admin_notify_summary")],
        [InlineKeyboardButton("⬅️ Close", callback_data="admin_notify_close")],
    ])


# =========================
# MENUS
# =========================
def main_menu() -> ReplyKeyboardRemove:
    return ReplyKeyboardRemove()


def make_user_dashboard_button(text: str, callback_data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=callback_data)


DASHBOARD_BUTTONS = {
    "shop": {"text": "Shop", "emoji": "🛍", "callback": "user_dashboard_shop", "style": "primary"},
    "orders": {"text": "My Orders", "emoji": "📦", "callback": "user_dashboard_orders", "style": "primary"},
    "wallet": {"text": "Wallet", "emoji": "💰", "callback": "user_dashboard_wallet", "style": "success"},
    "topup": {"text": "Top Up", "emoji": "💳", "callback": "user_dashboard_topup", "style": "success"},
    "promo": {"text": "Promo", "emoji": "🎁", "callback": "user_dashboard_promo", "style": "success"},
    "refer": {"text": "Refer & Earn", "emoji": "👥", "callback": "user_dashboard_refer", "style": "success"},
    "profile": {"text": "Profile", "emoji": "🆔", "callback": "user_dashboard_profile", "style": "primary"},
    "transactions": {"text": "Transactions", "emoji": "🧾", "callback": "user_dashboard_transactions", "style": "primary"},
    "support": {"text": "Support", "emoji": "🎧", "callback": "user_dashboard_support", "style": "danger"},
}
DASHBOARD_BUTTON_LAYOUT = (
    ("shop", "orders"),
    ("wallet", "topup"),
    ("promo", "refer"),
    ("profile", "transactions"),
    ("support",),
)


def make_styled_inline_button(
    text: str,
    callback_data=None,
    url=None,
    style=None,
    icon_custom_emoji_id=None,
) -> InlineKeyboardButton:
    button_kwargs = {}
    if callback_data is not None:
        button_kwargs["callback_data"] = callback_data
    if url is not None:
        button_kwargs["url"] = url
    api_fields = {}
    custom_emoji_id = str(icon_custom_emoji_id or "").strip()
    if custom_emoji_id:
        api_fields["icon_custom_emoji_id"] = custom_emoji_id
    # Telegram predefined styles only: primary, success, danger, or the neutral default.
    if style in {"primary", "success", "danger"}:
        api_fields["style"] = style
    if api_fields:
        try:
            return InlineKeyboardButton(text, **api_fields, **button_kwargs)
        except (TypeError, ValueError):
            try:
                return InlineKeyboardButton(text, api_kwargs=api_fields, **button_kwargs)
            except (TypeError, ValueError):
                pass
    return InlineKeyboardButton(text, **button_kwargs)


def user_dashboard_keyboard(styled: bool = True, custom_icons: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for row_keys in DASHBOARD_BUTTON_LAYOUT:
        row = []
        for key in row_keys:
            config = DASHBOARD_BUTTONS[key]
            custom_emoji_id = dashboard_custom_emoji_ids.get(key, "") if custom_icons else ""
            button_text = config["text"] if custom_emoji_id else f"{config['emoji']} {config['text']}"
            row.append(make_styled_inline_button(
                button_text,
                callback_data=config["callback"],
                style=config["style"] if styled else None,
                icon_custom_emoji_id=custom_emoji_id,
            ))
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def user_dashboard_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_user_dashboard_button("⬅️ Back to Menu", "user_dashboard")],
    ])


def user_back_to_menu_keyboard(styled: bool = True) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        make_styled_inline_button(
            "🏠 Back to Menu",
            callback_data="user_back_to_dashboard",
            style="danger" if styled else None,
        )
    ]])

def admin_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📦 Products", "📥 Stock"],
        ["🗂 Categories"],
        ["🎟 Promo Admin", "📦 Orders Admin"],
        ["💳 Deposits Admin", "👤 Users Admin"],
        ["💰 User Balance", "🔔 Notify Requests"],
        ["👑 Gold VIP", "⚡ Flash Deal"],
        ["👥 User Details", "📊 Analytics"],
        ["🎨 Dashboard Emojis"],
        ["🔌 Seller API"],
        ["📢 Broadcast"],
        ["🚪 Exit Admin"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def seller_api_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧪 Test Connection", callback_data="seller_api_test")],
        [InlineKeyboardButton("💰 Check API Balance", callback_data="seller_api_balance")],
        [InlineKeyboardButton("📦 Fetch API Products", callback_data="seller_api_products")],
        [InlineKeyboardButton("🔎 Debug Config", callback_data="seller_api_debug")],
        [InlineKeyboardButton("⬅️ Back", callback_data="seller_api_back")],
    ])


def render_seller_api_panel() -> str:
    enabled = "✅ Enabled" if is_buyer_api_enabled() else "❌ Disabled"
    api_url = BUYER_API_URL or "Not configured"
    prefix_status = "✅ Yes" if BUYER_API_KEY.startswith("api_") else "❌ No"
    warning = buyer_api_url_warning()
    warning_text = f"\n\n⚠️ {escape_html(warning)}" if warning else ""
    return (
        "🔌 <b>SELLER API</b>\n\n"
        f"<b>Status:</b> {enabled}\n"
        f"<b>API URL:</b> <code>{escape_html(api_url)}</code>\n"
        f"<b>API key:</b> <code>{escape_html(mask_buyer_api_key())}</code>\n"
        f"<b>Key length:</b> {len(BUYER_API_KEY)}\n"
        f"<b>Starts with api_:</b> {prefix_status}\n"
        f"<b>Balance request:</b> <code>{escape_html(buyer_api_endpoint_preview('/api/telegram-buyer/balance'))}</code>"
        f"{warning_text}\n\n"
        "Read-only connection tests only. No products or orders are saved."
    )


def render_seller_api_debug_config() -> str:
    enabled = "true" if is_buyer_api_enabled() else "false"
    api_url = BUYER_API_URL or "Not configured"
    prefix_status = "Yes" if BUYER_API_KEY.startswith("api_") else "No"
    warning = buyer_api_url_warning()
    lines = [
        "🔎 <b>SELLER API DEBUG CONFIG</b>",
        "",
        f"<b>Enabled:</b> {enabled}",
        f"<b>Base URL:</b> <code>{escape_html(api_url)}</code>",
        f"<b>Masked key:</b> <code>{escape_html(mask_buyer_api_key())}</code>",
        f"<b>Key length:</b> {len(BUYER_API_KEY)}",
        f"<b>Starts with api_:</b> {prefix_status}",
        "",
        "<b>Balance endpoint:</b>",
        f"<code>{escape_html(buyer_api_endpoint_preview('/api/telegram-buyer/balance'))}</code>",
        "",
        "<b>Products endpoint:</b>",
        f"<code>{escape_html(buyer_api_endpoint_preview('/api/telegram-buyer/products', {'lang': 'en'}))}</code>",
    ]
    if warning:
        lines.extend(["", f"⚠️ <b>Warning:</b> {escape_html(warning)}"])
    return "\n".join(lines)


def _format_buyer_api_value(value, limit: int = 32) -> str:
    if value is None or value == "":
        return "N/A"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    else:
        text = str(value)
    if len(text) > limit:
        text = text[:limit - 1] + "…"
    return escape_html(text)


def render_buyer_api_balance(balance_data: dict, heading: str = "API BALANCE") -> str:
    return (
        f"💰 <b>{escape_html(heading)}</b>\n\n"
        f"<b>Balance:</b> {_format_buyer_api_value(balance_data.get('balance'))}\n"
        f"<b>Balance text:</b> {_format_buyer_api_value(balance_data.get('balanceText'))}\n"
        f"<b>Wallet currency:</b> {_format_buyer_api_value(balance_data.get('walletCurrency'))}"
    )


def render_buyer_api_products(products: list, total: int) -> str:
    lines = [
        "📦 <b>API PRODUCTS</b>",
        "",
        f"<b>Total products:</b> {total}",
        f"<b>Showing:</b> {min(10, len(products))}",
    ]
    for index, product in enumerate(products[:10], start=1):
        if not isinstance(product, dict):
            lines.extend(["", f"<b>{index}.</b> Invalid product data"])
            continue
        product_id = product.get("id", product.get("productId", product.get("_id")))
        product_name = product.get("name", product.get("productName", product.get("title")))
        seller_price = product.get("sellerPrice", product.get("seller_price", product.get("price")))
        available_stock = product.get(
            "availableStock",
            product.get("available_stock", product.get("stock", product.get("quantity"))),
        )
        lines.extend([
            "",
            f"<b>{index}. {_format_buyer_api_value(product_name)}</b>",
            f"ID: <code>{_format_buyer_api_value(product_id)}</code>",
            f"Seller price: {_format_buyer_api_value(seller_price)}",
            f"Wallet pricing: {_format_buyer_api_value(product.get('walletPricing'))}",
            f"USD pricing: {_format_buyer_api_value(product.get('usdPricing'))}",
            f"Available stock: {_format_buyer_api_value(available_stock)}",
            f"Requires customer email: {_format_buyer_api_value(product.get('requiresCustomerEmail'))}",
            f"Slot product: {_format_buyer_api_value(product.get('isSlotProduct'))}",
        ])
    return "\n".join(lines)


def render_dashboard_emoji_admin_text() -> str:
    lines = ["🎨 <b>DASHBOARD EMOJIS</b>", "", "<b>Button Icons:</b>"]
    for key in DASHBOARD_EMOJI_KEYS:
        config = DASHBOARD_BUTTONS[key]
        status = "✅ Set" if dashboard_custom_emoji_ids.get(key) else "❌ Not set"
        lines.append(f"{config['emoji']} <b>{escape_html(config['text'])}:</b> {status}")
    lines.extend(["", "<b>Header Icons:</b>"])
    for key, config in DASHBOARD_HEADER_EMOJIS.items():
        status = "✅ Set" if dashboard_header_custom_emoji_ids.get(key) else "❌ Not set"
        lines.append(f"{config['emoji']} <b>{escape_html(config['label'])}:</b> {status}")
    return "\n".join(lines)


def dashboard_emoji_admin_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for key in DASHBOARD_EMOJI_KEYS:
        config = DASHBOARD_BUTTONS[key]
        status = "✅ Set" if dashboard_custom_emoji_ids.get(key) else "❌ Not set"
        rows.append([
            InlineKeyboardButton(
                f"{config['emoji']} {config['text']} — {status}",
                callback_data=f"dashboard_emoji_pick_{key}",
            )
        ])
    rows.append([InlineKeyboardButton("🧹 Clear Button Emoji", callback_data="dashboard_emoji_clear")])
    rows.append([InlineKeyboardButton("──── Header Emojis ────", callback_data="noop")])
    for key, config in DASHBOARD_HEADER_EMOJIS.items():
        status = "✅ Set" if dashboard_header_custom_emoji_ids.get(key) else "❌ Not set"
        rows.append([
            InlineKeyboardButton(
                f"{config['emoji']} {config['label']} Icon — {status}",
                callback_data=f"dashboard_header_emoji_pick_{key}",
            )
        ])
    rows.append([InlineKeyboardButton("🧹 Clear Header Emoji", callback_data="dashboard_header_emoji_clear")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="dashboard_emoji_back")])
    return InlineKeyboardMarkup(rows)


def dashboard_emoji_clear_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for key in DASHBOARD_EMOJI_KEYS:
        config = DASHBOARD_BUTTONS[key]
        status = "✅ Set" if dashboard_custom_emoji_ids.get(key) else "❌ Not set"
        rows.append([
            InlineKeyboardButton(
                f"🧹 {config['text']} — {status}",
                callback_data=f"dashboard_emoji_clear_{key}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="dashboard_emoji_panel")])
    return InlineKeyboardMarkup(rows)


def dashboard_header_emoji_clear_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for key, config in DASHBOARD_HEADER_EMOJIS.items():
        status = "✅ Set" if dashboard_header_custom_emoji_ids.get(key) else "❌ Not set"
        rows.append([
            InlineKeyboardButton(
                f"🧹 {config['label']} — {status}",
                callback_data=f"dashboard_header_emoji_clear_{key}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="dashboard_emoji_panel")])
    return InlineKeyboardMarkup(rows)


def dashboard_emoji_input_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back", callback_data="dashboard_emoji_panel")],
    ])


def deposit_amount_keyboard(back_callback: str = "user_back_to_dashboard", styled: bool = True) -> InlineKeyboardMarkup:
    amount_style = "primary" if styled else None
    rows = [
        [
            make_styled_inline_button("$5", callback_data="dep_amt_5", style=amount_style),
            make_styled_inline_button("$10", callback_data="dep_amt_10", style=amount_style),
        ],
        [
            make_styled_inline_button("$15", callback_data="dep_amt_15", style=amount_style),
            make_styled_inline_button("$20", callback_data="dep_amt_20", style=amount_style),
        ],
        [make_styled_inline_button("✏️ Custom Amount", callback_data="dep_custom", style="success" if styled else None)],
        [make_styled_inline_button(
            "🏠 Back to Menu" if back_callback in {"user_dashboard", "user_back_to_dashboard"} else "⬅️ Back",
            callback_data=back_callback,
            style=("danger" if back_callback in {"user_dashboard", "user_back_to_dashboard"} else "primary") if styled else None,
        )],
    ]
    return InlineKeyboardMarkup(rows)


def payment_method_keyboard(prefix: str, styled: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            make_styled_inline_button("🏦 Binance ID", callback_data=f"{prefix}_method_binance", style="success" if styled else None),
            make_styled_inline_button("🏦 Bybit ID", callback_data=f"{prefix}_method_bybit", style="success" if styled else None),
        ],
        [make_styled_inline_button("💸 Crypto Address", callback_data=f"{prefix}_method_crypto", style="primary" if styled else None)],
        [make_styled_inline_button("⬅️ Back", callback_data=f"{prefix}_back", style="primary" if styled else None)],
    ]
    if prefix == "dep":
        rows.append([make_styled_inline_button(
            "🏠 Back to Menu",
            callback_data="user_back_to_dashboard",
            style="danger" if styled else None,
        )])
    return InlineKeyboardMarkup(rows)


def network_keyboard(prefix: str, styled: bool = False) -> InlineKeyboardMarkup:
    network_style = "primary" if styled else None
    rows = [
        [
            make_styled_inline_button("USDT (TRC20)", callback_data=f"{prefix}_net_USDT_TRC20", style=network_style),
            make_styled_inline_button("USDT (ERC20)", callback_data=f"{prefix}_net_USDT_ERC20", style=network_style),
        ],
        [
            make_styled_inline_button("USDT (BEP20)", callback_data=f"{prefix}_net_USDT_BEP20", style=network_style),
            make_styled_inline_button("TRX (TRC20)", callback_data=f"{prefix}_net_TRX_TRC20", style=network_style),
        ],
        [
            make_styled_inline_button("BTC", callback_data=f"{prefix}_net_BTC", style=network_style),
            make_styled_inline_button("LTC", callback_data=f"{prefix}_net_LTC", style=network_style),
        ],
        [
            make_styled_inline_button("ETH (ERC20)", callback_data=f"{prefix}_net_ETH_ERC20", style=network_style),
            make_styled_inline_button("BNB (BEP20)", callback_data=f"{prefix}_net_BNB_BEP20", style=network_style),
        ],
        [make_styled_inline_button("SOL", callback_data=f"{prefix}_net_SOL", style=network_style)],
        [make_styled_inline_button("⬅️ Back", callback_data=f"{prefix}_back_method", style=network_style)],
    ]
    if prefix == "dep":
        rows.append([make_styled_inline_button(
            "🏠 Back to Menu",
            callback_data="user_back_to_dashboard",
            style="danger" if styled else None,
        )])
    return InlineKeyboardMarkup(rows)


def deposit_custom_amount_keyboard(styled: bool = True) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_styled_inline_button("⬅️ Back", callback_data="dep_back", style="primary" if styled else None)],
        [make_styled_inline_button("🏠 Back to Menu", callback_data="user_back_to_dashboard", style="danger" if styled else None)],
    ])


def deposit_manual_keyboard(styled: bool = True) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_styled_inline_button("✅ Submitted", callback_data="depmanual_submitted", style="success" if styled else None)],
        [make_styled_inline_button("❌ Cancel", callback_data="depmanual_cancel", style="danger" if styled else None)],
        [make_styled_inline_button("⬅️ Back", callback_data="dep_back_method", style="primary" if styled else None)],
        [make_styled_inline_button("🏠 Back to Menu", callback_data="user_back_to_dashboard", style="danger" if styled else None)],
    ])


def deposit_payment_request_keyboard(styled: bool = True) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_styled_inline_button("✅ I Have Paid (Verify)", callback_data="deppay_verify", style="success" if styled else None)],
        [make_styled_inline_button("🔁 Change Network", callback_data="deppay_change_network", style="primary" if styled else None)],
        [make_styled_inline_button("⬅️ Back", callback_data="deppay_back_network", style="primary" if styled else None)],
        [make_styled_inline_button("🏠 Back to Menu", callback_data="user_back_to_dashboard", style="danger" if styled else None)],
    ])


def buy_qty_keyboard(product_id: str, styled: bool = True) -> InlineKeyboardMarkup:
    action_style = "primary" if styled else None
    back_style = "primary" if styled else None
    menu_style = "danger" if styled else None
    tiers = get_bulk_pricing_tiers(PRODUCTS.get(product_id, {}))
    if tiers:
        stock = min(get_display_stock(product_id), get_product_stock(product_id))
        quick_quantities = [1] if stock >= 1 else []
        for tier in tiers:
            if tier["min_qty"] <= stock and tier["min_qty"] not in quick_quantities:
                quick_quantities.append(tier["min_qty"])
        quick_buttons = [
            make_styled_inline_button(
                f"🛒 Buy {quantity}x",
                callback_data=f"buy_qty_{product_id}_{quantity}",
                style=action_style,
            )
            for quantity in quick_quantities
        ]
        rows = [quick_buttons[index:index + 2] for index in range(0, len(quick_buttons), 2)]
        rows.append([make_styled_inline_button("✏️ Custom Qty", callback_data=f"buy_custom_{product_id}", style=action_style)])
    else:
        rows = [
            [
                make_styled_inline_button("🛒 Buy 1x", callback_data=f"buy_qty_{product_id}_1", style=action_style),
                make_styled_inline_button("🛒 Buy 5x", callback_data=f"buy_qty_{product_id}_5", style=action_style),
            ],
            [
                make_styled_inline_button("🛒 Buy 10x", callback_data=f"buy_qty_{product_id}_10", style=action_style),
                make_styled_inline_button("✏️ Custom Qty", callback_data=f"buy_custom_{product_id}", style=action_style),
            ],
        ]
    rows.extend([
        [make_styled_inline_button("⬅️ Back to Shop", callback_data="back_shop_cards", style=back_style)],
        [make_styled_inline_button("🏠 Back to Menu", callback_data="user_back_to_dashboard", style=menu_style)],
    ])
    return InlineKeyboardMarkup(rows)


def shop_return_keyboard(styled: bool = True) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_styled_inline_button(
            "⬅️ Back to Shop",
            callback_data="back_shop_cards",
            style="primary" if styled else None,
        )],
        [make_styled_inline_button(
            "🏠 Back to Menu",
            callback_data="user_back_to_dashboard",
            style="danger" if styled else None,
        )],
    ])


def out_of_stock_product_keyboard(product_id: str, styled: bool = True) -> InlineKeyboardMarkup:
    rows = [[make_styled_inline_button(
        "🔔 Notify Me",
        callback_data=f"shop_notify_{product_id}",
        style="danger" if styled else None,
    )]]
    rows.extend(shop_return_keyboard(styled=styled).inline_keyboard)
    return InlineKeyboardMarkup(rows)


def final_manual_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✅ Submitted", callback_data=f"{prefix}_submitted")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"{prefix}_cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def close_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Close", callback_data="close_inline")]])


def promo_generator_amount_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("$1", callback_data="promo_gen_amt_1"),
            InlineKeyboardButton("$5", callback_data="promo_gen_amt_5"),
        ],
        [
            InlineKeyboardButton("$10", callback_data="promo_gen_amt_10"),
            InlineKeyboardButton("✏️ Custom Amount", callback_data="promo_gen_custom"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="promo_back")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_products_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("➕ Add Product", callback_data="admin_add_product")],
        [InlineKeyboardButton("📋 View Products", callback_data="admin_view_products")],
        [InlineKeyboardButton("✏️ Edit Name", callback_data="admin_edit_name_menu")],
        [InlineKeyboardButton("💲 Edit Price", callback_data="admin_edit_price_menu")],
        [InlineKeyboardButton("📅 Edit Month", callback_data="admin_edit_month_menu")],
        [InlineKeyboardButton("📝 Edit Details", callback_data="admin_edit_details_menu")],
        [InlineKeyboardButton("📌 Edit Delivery Guide", callback_data="admin_edit_delivery_guide_menu")],
        [InlineKeyboardButton("😀 Edit Icon", callback_data="admin_edit_icon_menu")],
        [InlineKeyboardButton("📦 Edit Display Stock", callback_data="admin_edit_display_stock_menu")],
        [InlineKeyboardButton("💸 Bulk Pricing", callback_data="admin_bulk_pricing_menu")],
        [InlineKeyboardButton("↕️ Reorder in Shop", callback_data="admin_reorder_menu")],
        [InlineKeyboardButton("🗑 Delete Product", callback_data="admin_delete_product_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="admin_products_close")],
    ]
    return InlineKeyboardMarkup(rows)


def bulk_pricing_admin_keyboard(product_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View Tiers", callback_data="admin_bulk_view")],
        [InlineKeyboardButton("➕ Add Tier", callback_data="admin_bulk_add")],
        [InlineKeyboardButton("🗑 Remove Tier", callback_data="admin_bulk_remove_menu")],
        [InlineKeyboardButton("🧹 Clear All Tiers", callback_data="admin_bulk_clear")],
        [InlineKeyboardButton("⬅️ Back", callback_data="admin_bulk_back")],
    ])


def bulk_pricing_remove_keyboard(product_id: str) -> InlineKeyboardMarkup:
    rows = []
    for tier in get_bulk_pricing_tiers(PRODUCTS.get(product_id, {})):
        rows.append([InlineKeyboardButton(
            f"{tier['min_qty']}+ = {format_money(tier['unit_price'])}",
            callback_data=f"admin_bulk_remove_{tier['min_qty']}",
        )])
    if not rows:
        rows.append([InlineKeyboardButton("No tiers configured", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_bulk_view")])
    return InlineKeyboardMarkup(rows)


def bulk_pricing_clear_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Clear All Tiers", callback_data="admin_bulk_clear_confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_bulk_view")],
    ])


def admin_categories_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Category", callback_data="category_add")],
        [InlineKeyboardButton("📋 View Categories", callback_data="category_view")],
        [InlineKeyboardButton("✏️ Rename Category", callback_data="category_rename_menu")],
        [InlineKeyboardButton("😀 Edit Category Icon", callback_data="category_icon_menu")],
        [InlineKeyboardButton("↕️ Reorder in Shop", callback_data="category_reorder_menu")],
        [InlineKeyboardButton("🗃 Move Product to Category", callback_data="category_move_product_menu")],
        [InlineKeyboardButton("🗑 Delete Category", callback_data="category_delete_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="category_close")],
    ])


def category_select_keyboard(action_prefix: str, include_default: bool = True) -> InlineKeyboardMarkup:
    normalize_categories()
    rows = []
    for category_id in category_order:
        if category_id not in CATEGORIES or (not include_default and category_id == DEFAULT_CATEGORY_ID):
            continue
        category = CATEGORIES[category_id]
        rows.append([
            make_category_inline_button(
                category,
                category.get("name", category_id),
                f"{action_prefix}_{category_id}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="category_back")])
    return InlineKeyboardMarkup(rows)


def category_product_select_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for product_id in product_order:
        if product_id not in PRODUCTS:
            continue
        product = PRODUCTS[product_id]
        rows.append([make_product_inline_button(product, product.get("name", product_id), f"category_move_product_{product_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="category_back")])
    return InlineKeyboardMarkup(rows)


def category_reorder_selected_keyboard(category_id: str) -> InlineKeyboardMarkup:
    normalize_shop_order()
    item = f"category:{category_id}"
    if item not in shop_order:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Category List", callback_data="category_reorder_menu")]])
    rows = []
    if can_move_shop_order_item(item, -1):
        rows.append([InlineKeyboardButton("⬆️ Move Up", callback_data=f"category_move_up_{category_id}")])
    if can_move_shop_order_item(item, 1):
        rows.append([InlineKeyboardButton("⬇️ Move Down", callback_data=f"category_move_down_{category_id}")])
    rows.append([InlineKeyboardButton("⬅️ Category List", callback_data="category_reorder_menu")])
    return InlineKeyboardMarkup(rows)


def admin_stock_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📋 View Stock", callback_data="stock_view")],
        [InlineKeyboardButton("➕ Add Single Account", callback_data="stock_add_single_menu")],
        [InlineKeyboardButton("📥 Add Bulk Accounts", callback_data="stock_add_bulk_menu")],
        [InlineKeyboardButton("👀 View Account List", callback_data="stock_view_accounts_menu")],
        [InlineKeyboardButton("✏️ Edit Account", callback_data="stock_edit_account_menu")],
        [InlineKeyboardButton("🗑 Delete Account", callback_data="stock_delete_account_menu")],
        [InlineKeyboardButton("🔢 Set Display Stock", callback_data="stock_set_display_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="stock_close")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_promo_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🎲 Promo Generator", callback_data="promo_generator")],
        [InlineKeyboardButton("📋 View Promos", callback_data="promo_view")],
        [InlineKeyboardButton("🔁 Enable / Disable", callback_data="promo_toggle_menu")],
        [InlineKeyboardButton("🗑 Delete Promo", callback_data="promo_delete_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="promo_close")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_orders_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📋 View All Orders", callback_data="orders_view_all")],
        [InlineKeyboardButton("⏳ Pending Manual Orders", callback_data="orders_view_pending_manual")],
        [InlineKeyboardButton("✅ Completed Orders", callback_data="orders_view_completed")],
        [InlineKeyboardButton("🔎 User ID Search", callback_data="orders_user_search")],
        [InlineKeyboardButton("☑️ Confirm / Reject Manual", callback_data="orders_manual_pick_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="orders_close")],
    ]
    return InlineKeyboardMarkup(rows)


def deposits_admin_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⏳ Pending Manual Deposits", callback_data="deposits_pending_manual")],
        [InlineKeyboardButton("📋 All Deposits", callback_data="deposits_all")],
        [InlineKeyboardButton("🔎 User ID Search", callback_data="deposits_user_search")],
        [InlineKeyboardButton("☑️ Confirm / Reject Deposit", callback_data="deposits_pick_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="deposits_close")],
    ]
    return InlineKeyboardMarkup(rows)


def users_admin_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📊 View Summary", callback_data="users_summary")],
        [InlineKeyboardButton("👥 User Details", callback_data="users_details_0")],
        [InlineKeyboardButton("🔎 Search User ID", callback_data="users_search")],
        [InlineKeyboardButton("⬅️ Close", callback_data="users_close")],
    ]
    return InlineKeyboardMarkup(rows)


def user_balance_admin_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("➕ Add Balance", callback_data="balance_add"),
            InlineKeyboardButton("➖ Minus Balance", callback_data="balance_minus"),
        ],
        [InlineKeyboardButton("🔎 Check User Balance", callback_data="balance_check")],
        [InlineKeyboardButton("⬅️ Close", callback_data="balance_close")],
    ]
    return InlineKeyboardMarkup(rows)


def gold_vip_admin_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("➕ Add VIP User", callback_data="vip_add_user"),
            InlineKeyboardButton("➖ Remove VIP User", callback_data="vip_remove_user"),
        ],
        [InlineKeyboardButton("📋 VIP User List", callback_data="vip_user_list")],
        [
            InlineKeyboardButton("💲 Set VIP Price", callback_data="vip_set_price_menu"),
            InlineKeyboardButton("🗑 Remove VIP Price", callback_data="vip_remove_price_menu"),
        ],
        [InlineKeyboardButton("📦 VIP Price List", callback_data="vip_price_list")],
        [InlineKeyboardButton("⬅️ Close", callback_data="vip_close")],
    ]
    return InlineKeyboardMarkup(rows)


def gold_vip_product_select_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for product_id in product_order:
        if product_id not in PRODUCTS:
            continue
        product = PRODUCTS[product_id]
        vip_price = get_product_vip_price(product_id)
        vip_text = format_money(vip_price) if vip_price is not None else "Not set"
        core_text = f"{product.get('name', product_id)} | VIP: {vip_text}"
        rows.append([make_product_inline_button(product, core_text, f"{prefix}_{product_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="vip_back")])
    return InlineKeyboardMarkup(rows)


def users_details_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"users_details_{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"users_details_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="users_summary")])
    return InlineKeyboardMarkup(rows)


def admin_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")]])


def admin_confirm_add_product_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✅ Confirm Add", callback_data="admin_confirm_add_product")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_product_select_keyboard(action_prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for product_id in product_order:
        product = PRODUCTS[product_id]
        rows.append([
            make_product_inline_button(
                product,
                f"{product['name']} ({product_id})",
                f"{action_prefix}_{product_id}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_products_back")])
    return InlineKeyboardMarkup(rows)


def stock_product_select_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for product_id in product_order:
        product = PRODUCTS[product_id]
        rows.append([
            make_product_inline_button(
                product,
                f"{product['name']} ({product_id})",
                f"{prefix}_{product_id}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="stock_back")])
    return InlineKeyboardMarkup(rows)


def admin_confirm_keyboard(confirm_callback: str, confirm_text: str = "✅ Confirm") -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(confirm_text, callback_data=confirm_callback)],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")],
    ]
    return InlineKeyboardMarkup(rows)


def broadcast_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Send to Users + Channel", callback_data="admin_broadcast_send_both")],
        [InlineKeyboardButton("👥 Users Only", callback_data="admin_broadcast_send_users")],
        [InlineKeyboardButton("📣 Channel Only", callback_data="admin_broadcast_send_channel")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_flow")],
    ])


def admin_reorder_selected_keyboard(product_id: str) -> InlineKeyboardMarkup:
    normalize_shop_order()
    item = f"product:{product_id}"
    if item not in shop_order:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_reorder_menu")]])
    rows = []
    if can_move_shop_order_item(item, -1):
        rows.append([InlineKeyboardButton("⬆️ Move Up", callback_data=f"admin_move_up_{product_id}")])
    if can_move_shop_order_item(item, 1):
        rows.append([InlineKeyboardButton("⬇️ Move Down", callback_data=f"admin_move_down_{product_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_reorder_menu")])
    return InlineKeyboardMarkup(rows)
def promo_select_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = []
    if not PROMO_CODES:
        rows.append([InlineKeyboardButton("No promos", callback_data="noop")])
    else:
        for code, info in PROMO_CODES.items():
            status = "ON" if info.get("enabled", True) else "OFF"
            rows.append([
                InlineKeyboardButton(
                    f"{code} ({format_money(float(info['amount']))}) [{status}]",
                    callback_data=f"{prefix}_{code}",
                )
            ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="promo_back")])
    return InlineKeyboardMarkup(rows)


def pending_manual_orders_keyboard() -> InlineKeyboardMarkup:
    rows = []
    found = False
    for order in all_orders:
        if order["status"] == "Waiting Manual Confirmation":
            found = True
            rows.append([
                InlineKeyboardButton(
                    f"#{order['id']} {order['product']} x{order['qty']} ({format_money(order['total'])})",
                    callback_data=f"orders_pick_manual_{order['id']}",
                )
            ])
    if not found:
        rows.append([InlineKeyboardButton("No pending manual orders", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="orders_back")])
    return InlineKeyboardMarkup(rows)


def pending_manual_deposits_keyboard() -> InlineKeyboardMarkup:
    rows = []
    found = False
    for tx in all_transactions:
        if tx["type"] == "Deposit" and tx["status"] == "Waiting Manual Confirmation":
            found = True
            rows.append([
                InlineKeyboardButton(
                    f"TX#{tx['id']} User {tx['user_id']} {format_money(tx['amount'])}",
                    callback_data=f"deposits_pick_{tx['id']}",
                )
            ])
    if not found:
        rows.append([InlineKeyboardButton("No pending deposits", callback_data="noop")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="deposits_back")])
    return InlineKeyboardMarkup(rows)


def manual_order_action_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm", callback_data=f"orders_confirm_{order_id}")],
        [InlineKeyboardButton("❌ Reject", callback_data=f"orders_reject_{order_id}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="orders_manual_pick_menu")],
    ])


def manual_deposit_action_keyboard(tx_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm", callback_data=f"deposits_confirm_{tx_id}")],
        [InlineKeyboardButton("❌ Reject", callback_data=f"deposits_reject_{tx_id}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="deposits_pick_menu")],
    ])


def account_serial_keyboard(product_id: str, prefix: str, page: int = 0, page_size: int = 15) -> InlineKeyboardMarkup:
    accounts = PRODUCTS[product_id]["accounts"]
    rows = []
    start = page * page_size
    end = min(start + page_size, len(accounts))

    for idx in range(start, end):
        label = f"#{idx + 1} {format_stock_item_for_admin(accounts[idx], 45)}"
        rows.append([InlineKeyboardButton(label, callback_data=f"{prefix}_{product_id}_{idx}")])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{prefix}_page_{product_id}_{page - 1}"))
    if end < len(accounts):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_page_{product_id}_{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="stock_back")])
    return InlineKeyboardMarkup(rows)


def map_network_callback_to_label(network_callback_tail: str) -> str:
    network = network_callback_tail.replace("_", " ")
    network_map = {
        "USDT TRC20": "USDT (TRC20)",
        "USDT ERC20": "USDT (ERC20)",
        "USDT BEP20": "USDT (BEP20)",
        "TRX TRC20": "TRX (TRC20)",
        "BTC": "BTC",
        "LTC": "LTC",
        "ETH ERC20": "ETH (ERC20)",
        "BNB BEP20": "BNB (BEP20)",
        "SOL": "SOL",
    }
    return network_map[network]


async def send_client_main_text(update: Update, text: str):
    await update.message.reply_text(text, reply_markup=main_menu(), parse_mode="HTML")


async def send_user_dashboard(message, menu_notice: str = None):
    # Step A: ReplyKeyboardRemove serializes remove_keyboard=True and clears any legacy user/admin keyboard.
    await message.reply_text(
        menu_notice or "Opening menu...",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML",
    )
    # Step B: send a separate message so the dashboard is a real InlineKeyboardMarkup.
    user_id = getattr(getattr(message, "chat", None), "id", 0)
    variants = [(True, True, True)]
    if any(dashboard_header_custom_emoji_ids.values()):
        variants.append((False, True, True))
    variants.extend([(False, False, True), (False, False, False)])
    last_error = None
    for header_icons, button_icons, styled in variants:
        try:
            await message.reply_text(
                render_home_text(user_id, custom_icons=header_icons),
                reply_markup=user_dashboard_keyboard(styled=styled, custom_icons=button_icons),
                parse_mode="HTML",
            )
            return
        except Exception as error:
            last_error = error
            print(
                "Dashboard render rejected; trying safe fallback: "
                f"{type(error).__name__}: {error}"
            )
    if last_error:
        raise last_error


async def edit_user_dashboard_panel(
    query,
    text: str,
    keyboard: InlineKeyboardMarkup,
    fallback_keyboards=None,
    fallback_text: str = None,
):
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return
        print(f"User dashboard edit failed; sending fallback: {type(e).__name__}: {e}")
        if fallback_keyboards is None:
            await query.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")
            return
        for fallback_keyboard in fallback_keyboards:
            try:
                await query.message.reply_text(
                    fallback_text or text,
                    reply_markup=fallback_keyboard,
                    parse_mode="HTML",
                )
                return
            except Exception as fallback_error:
                print(
                    "User dashboard fallback failed; trying next fallback: "
                    f"{type(fallback_error).__name__}: {fallback_error}"
                )


async def send_admin_main_text(update: Update, text: str):
    await update.message.reply_text(text, reply_markup=admin_menu(), parse_mode="HTML")


async def send_inline_from_text(update: Update, text: str, keyboard: InlineKeyboardMarkup):
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def send_inline_from_callback(query, text: str, keyboard=None):
    if keyboard is None:
        await query.message.reply_text(text, parse_mode="HTML")
    else:
        await query.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def send_shop_inline_with_style_fallback(query, text: str, keyboard, fallback_keyboard):
    try:
        await send_inline_from_callback(query, text, keyboard)
    except Exception as e:
        print(f"Styled shop buttons were rejected; retrying normal buttons: {type(e).__name__}: {e}")
        await send_inline_from_callback(query, text, fallback_keyboard)


async def send_user_inline_with_style_fallback(query, text: str, keyboard, fallback_keyboard):
    try:
        await send_inline_from_callback(query, text, keyboard)
    except Exception as e:
        print(f"Styled user buttons were rejected; retrying normal buttons: {type(e).__name__}: {e}")
        await send_inline_from_callback(query, text, fallback_keyboard)


async def send_user_inline_from_text_with_style_fallback(update: Update, text: str, keyboard, fallback_keyboard):
    try:
        await send_inline_from_text(update, text, keyboard)
    except Exception as e:
        print(f"Styled user buttons were rejected; retrying normal buttons: {type(e).__name__}: {e}")
        await send_inline_from_text(update, text, fallback_keyboard)


# =========================
# BASE58 / TRON HELPERS
# =========================
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(data: bytes) -> str:
    num = int.from_bytes(data, "big")
    encoded = ""
    while num > 0:
        num, rem = divmod(num, 58)
        encoded = B58_ALPHABET[rem] + encoded
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return "1" * pad + (encoded or "1")


def tron_hex_to_base58(hex_addr: str) -> str:
    hex_addr = hex_addr.lower().replace("0x", "").strip()
    if len(hex_addr) == 40:
        hex_addr = "41" + hex_addr
    raw = bytes.fromhex(hex_addr)
    checksum = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    return b58encode(raw + checksum)


# =========================
# RENDER TEXTS
# =========================
def dashboard_header_icon_html(key: str, custom_icons: bool = True) -> str:
    config = DASHBOARD_HEADER_EMOJIS[key]
    custom_emoji_id = dashboard_header_custom_emoji_ids.get(key, "") if custom_icons else ""
    return _icon_html(config["emoji"], custom_emoji_id)


def render_home_text(user_id: int, custom_icons: bool = True) -> str:
    wallet_balance = float(user_wallet.get(user_id, 0.0))
    return (
        f"{dashboard_header_icon_html('welcome', custom_icons)} <b>Welcome back, Supreme Leader</b>\n\n"
        f"{dashboard_header_icon_html('premium', custom_icons)} Premium digital subscriptions\n"
        f"{dashboard_header_icon_html('delivery', custom_icons)} Instant delivery • "
        f"{dashboard_header_icon_html('secure', custom_icons)} Secure orders • "
        f"{dashboard_header_icon_html('support', custom_icons)} Support\n\n"
        f"{dashboard_header_icon_html('wallet', custom_icons)} <b>Wallet:</b> {wallet_balance:.2f} USDT\n\n"
        f"Choose an option below {dashboard_header_icon_html('choose', custom_icons)}"
    )


def render_wallet_text(user_id: int) -> str:
    return (
        "💰 <b>WALLET</b>\n\n"
        f"<b>Current Balance:</b> {format_money(user_wallet[user_id])}"
    )


def render_user_id_text(user_id: int) -> str:
    return (
        "🆔 <b>YOUR USER ID</b>\n\n"
        f"<code>{user_id}</code>\n\n"
        "Send this User ID to admin when needed."
    )


def render_orders_text(user_id: int) -> str:
    orders = user_orders[user_id]
    if not orders:
        return "📦 <b>ORDERS</b>\n\nNo orders found."

    lines = ["📦 <b>ORDERS</b>\n"]
    for order in reversed(orders[-25:]):
        lines.append(
            f"#{order['id']} <b>{order['product']}</b>\n"
            f"   Quantity: {order['qty']}\n"
            f"   Total: {format_money(order['total'])}\n"
            f"   Payment: {order.get('payment_type', 'Unknown')}\n"
            f"   Status: <b>{order['status']}</b>\n"
            f"   Date: {format_dt(order.get('created_at'))}\n"
        )
    return "\n".join(lines)


USER_ORDERS_PAGE_SIZE = 6


def _latest_user_orders(user_id: int) -> list:
    orders = user_orders.get(user_id, [])
    return [order for order in reversed(orders) if isinstance(order, dict)]


def find_user_order_by_id(user_id: int, order_id):
    wanted_id = str(order_id)
    for order in user_orders.get(user_id, []):
        if isinstance(order, dict) and str(order.get("id")) == wanted_id:
            return order
    return None


def format_order_status(status) -> str:
    raw_status = str(status or "").strip()
    if not raw_status:
        return "N/A"
    normalized = raw_status.lower().replace("-", "_").replace(" ", "_")
    labels = {
        "completed": "Completed",
        "pending": "Pending",
        "pending_manual": "Pending Manual",
        "pending_auto": "Pending Auto",
        "processing": "Processing",
        "rejected": "Rejected",
        "cancelled": "Cancelled",
        "canceled": "Cancelled",
        "failed": "Failed",
    }
    return labels.get(normalized, raw_status.replace("_", " ").title())


def _order_product_name(order: dict) -> str:
    return str(order.get("product") or order.get("product_name") or "N/A")


def _order_total_text(order: dict) -> str:
    try:
        return format_money(order.get("total"))
    except (TypeError, ValueError):
        return "N/A"


def _order_date_text(order: dict) -> str:
    try:
        return format_dt(order.get("created_at"))
    except (AttributeError, TypeError, ValueError):
        return "N/A"


def _order_price_type(order: dict) -> str:
    raw_value = order.get("price_type") or order.get("applied_price_type")
    if raw_value:
        normalized = str(raw_value).strip().lower().replace("-", "_").replace(" ", "_")
        labels = {
            "normal": "Normal price",
            "normal_price": "Normal price",
            "vip": "Gold VIP price",
            "vip_price": "Gold VIP price",
            "gold_vip": "Gold VIP price",
            "gold_vip_price": "Gold VIP price",
            "flash": "Flash deal price",
            "flash_deal": "Flash deal price",
            "flash_deal_price": "Flash deal price",
        }
        return labels.get(normalized, str(raw_value))
    if order.get("is_flash_deal") is True:
        return "Flash deal price"
    if order.get("is_vip") is True or order.get("is_gold_vip") is True:
        return "Gold VIP price"
    return "N/A"


def _order_payment_reference(order: dict) -> str:
    reference_keys = (
        "payment_reference",
        "transaction_reference",
        "transaction_id",
        "txid",
        "payment_id",
        "provider_payment_id",
    )
    for key in reference_keys:
        value = order.get(key)
        if value not in (None, ""):
            return str(value)
    meta = order.get("meta")
    if isinstance(meta, dict):
        for key in reference_keys:
            value = meta.get(key)
            if value not in (None, ""):
                return str(value)
    return "N/A"


def render_user_orders_page(user_id: int, page: int = 0):
    orders = _latest_user_orders(user_id)
    total_orders = len(orders)
    total_pages = max(1, (total_orders + USER_ORDERS_PAGE_SIZE - 1) // USER_ORDERS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    if not orders:
        return "📦 <b>MY ORDERS</b>\n\nNo orders found.", page, total_pages
    start = page * USER_ORDERS_PAGE_SIZE
    end = min(start + USER_ORDERS_PAGE_SIZE, total_orders)
    text = (
        "📦 <b>MY ORDERS</b>\n\n"
        "Tap an order below to view its full details.\n\n"
        f"Showing <b>{start + 1}-{end}</b> of <b>{total_orders}</b>\n"
        f"Page <b>{page + 1}</b> of <b>{total_pages}</b>"
    )
    return text, page, total_pages


def user_orders_keyboard(user_id: int, page: int = 0) -> InlineKeyboardMarkup:
    orders = _latest_user_orders(user_id)
    total_pages = max(1, (len(orders) + USER_ORDERS_PAGE_SIZE - 1) // USER_ORDERS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * USER_ORDERS_PAGE_SIZE
    page_orders = orders[start:start + USER_ORDERS_PAGE_SIZE]
    rows = []
    for order in page_orders:
        order_id = order.get("id")
        product_name = _order_product_name(order).replace("\n", " ").strip()
        if len(product_name) > 18:
            product_name = product_name[:17] + "…"
        label = (
            f"Order #{order_id if order_id not in (None, '') else 'N/A'} • "
            f"{product_name} • {_order_total_text(order)} • {format_order_status(order.get('status'))}"
        )
        if len(label) > 64:
            label = label[:63] + "…"
        order_id_text = str(order_id) if order_id not in (None, "") else ""
        callback_data = (
            f"user_order_view_{order_id_text}_{page}"
            if order_id_text.isdigit()
            else "user_order_unavailable"
        )
        rows.append([InlineKeyboardButton(label, callback_data=callback_data)])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"user_orders_page_{page - 1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"user_orders_page_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🏠 Back to Menu", callback_data="user_back_to_dashboard")])
    return InlineKeyboardMarkup(rows)


def render_user_order_details(user_id: int, order_id) -> str:
    order = find_user_order_by_id(user_id, order_id)
    if not order:
        return "📦 <b>ORDER DETAILS</b>\n\nOrder not found."
    value = lambda item: escape_html(item if item not in (None, "") else "N/A")
    product_id = order.get("product_id")
    product = PRODUCTS.get(product_id) if isinstance(product_id, str) else None
    product_details = ""
    if isinstance(product, dict):
        plain_details = product.get("details")
        if isinstance(plain_details, list):
            product_details = "\n".join(
                escape_html(detail)
                for detail in plain_details
                if detail not in (None, "") and str(detail).strip()
            )
        rich_details = product.get("details_rich")
        if isinstance(rich_details, dict):
            rich_text = rich_details.get("text")
            if isinstance(rich_text, str) and rich_text.strip():
                product_details = render_serialized_entities_html(
                    rich_text, rich_details.get("entities")
                ) or product_details
    details_section = (
        f"📋 <b>Product Details:</b>\n{product_details}"
        if product_details
        else "📋 <b>Product Details:</b> N/A"
    )
    delivered_items = order.get("delivered_items")
    delivered_lines = []
    if isinstance(delivered_items, list):
        delivered_lines = [
            f"<code>{escape_html(item)}</code>"
            for item in delivered_items
            if item not in (None, "") and str(item).strip()
        ]
    delivered_section = (
        "🔐 <b>Delivered Account Details:</b>\n" + "\n".join(delivered_lines)
        if delivered_lines
        else "🔐 <b>Delivered Account Details:</b> N/A"
    )
    return (
        "📦 <b>ORDER DETAILS</b>\n\n"
        f"<b>Order ID:</b> #{value(order.get('id'))}\n"
        f"<b>Product:</b> {value(_order_product_name(order))}\n"
        f"<b>Product ID:</b> <code>{value(order.get('product_id'))}</code>\n"
        f"<b>Quantity:</b> {value(order.get('qty'))}\n"
        f"<b>Total Price:</b> {_order_total_text(order)}\n"
        f"<b>Payment Type:</b> {value(order.get('payment_type'))}\n"
        f"<b>Price Type:</b> {value(_order_price_type(order))}\n"
        f"<b>Status:</b> {value(format_order_status(order.get('status')))}\n"
        f"<b>Date/Time:</b> {value(_order_date_text(order))}\n"
        f"<b>Payment Reference:</b> <code>{value(_order_payment_reference(order))}</code>\n\n"
        f"{delivered_section}\n\n"
        f"{details_section}"
    )


def user_order_details_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    page = max(0, page)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Contact Support", url=SUPPORT_URL)],
        [InlineKeyboardButton("⬅️ Back to Orders", callback_data=f"user_orders_page_{page}")],
        [InlineKeyboardButton("🏠 Back to Menu", callback_data="user_back_to_dashboard")],
    ])


async def edit_user_orders_message(query, text: str, keyboard=None):
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return
        print(f"⚠️ Failed to edit user order navigation: {type(e).__name__}: {e}")
        await query.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


def render_transactions_text(user_id: int) -> str:
    txs = user_transactions[user_id]
    if not txs:
        return "🧾 <b>TRANSACTIONS</b>\n\nNo transaction history found."

    lines = ["🧾 <b>TRANSACTIONS</b>\n"]
    for tx in reversed(txs[-25:]):
        lines.append(
            f"TX#{tx['id']} <b>{tx['type']}</b>\n"
            f"   Amount: {format_money(tx['amount'])}\n"
            f"   Status: <b>{tx['status']}</b>\n"
            f"   Date: {format_dt(tx.get('created_at'))}\n"
        )
    return "\n".join(lines)


def render_refer_text(user_id: int) -> str:
    ref_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    return (
        "👥 <b>REFER & EARN</b>\n\n"
        "Invite friends and get rewarded.\n\n"
        f"🔗 <b>Your Link:</b>\n{ref_link}\n\n"
        "📊 <b>Total Invited:</b> 0\n"
        "💵 <b>Rewards Earned:</b> $0"
    )


def render_support_text() -> str:
    return (
        "💬 <b>SUPPORT</b>\n\n"
        f"Contact admin: {SUPPORT_USERNAME}\n"
        f"Support link: {SUPPORT_URL}\n\n"
        "For payment, order, warranty, or delivery issues, contact support with your User ID and order details."
    )


def render_terms_text() -> str:
    return (
        "📜 <b>TERMS OF USE</b>\n\n"
        "By using this bot, you agree to the shop terms. All products are digital goods delivered automatically or manually after payment confirmation. "
        "Customers must check product details before purchase. Warranty, replacement, and refund rules depend on the product description and admin decision.\n\n"
        f"Full Terms: {TERMS_URL}"
    )


def render_privacy_text() -> str:
    return (
        "🔐 <b>PRIVACY POLICY</b>\n\n"
        "The bot stores Telegram user ID, username, wallet balance, orders, transactions, and support-related data needed to operate the shop. "
        "We do not sell user data. Payment data may be processed by third-party payment providers.\n\n"
        f"Full Privacy Policy: {PRIVACY_URL}"
    )


def render_legal_text() -> str:
    return (
        "⚖️ <b>LEGAL INFORMATION</b>\n\n"
        "This bot sells digital products and subscription access items. Users are responsible for following the rules of any third-party services they use. "
        "For legal/support requests, contact the shop admin.\n\n"
        f"Legal Page: {LEGAL_URL}\n"
        f"Support: {SUPPORT_URL}"
    )


def render_product_card(product_id: str, user_id: int = None) -> str:
    product = PRODUCTS[product_id]
    stock = get_display_stock(product_id)
    stock_text = f"{stock} pcs" if stock > 0 else "Stock Out"
    icon = product_icon_html(product)
    duration = format_duration_text(product.get("month", ""))
    duration_line = f"<b>Duration:</b> {duration}\n" if duration else ""
    return (
        f"{icon} <b>{product['name']}</b>\n"
        f"{duration_line}"
        f"<b>Price:</b> {format_product_price_for_user(product_id, user_id)}\n"
        f"<b>Stock:</b> {stock_text}"
    )


def render_bulk_pricing_offers(product) -> str:
    tiers = get_bulk_pricing_tiers(product)
    if not tiers:
        return ""
    lines = ["💸 <b>Bulk Discount Offers</b>"]
    for tier in tiers:
        lines.append(
            f"✅ Buy {tier['min_qty']}+ → {float(tier['unit_price']):.2f} USDT each"
        )
    return "\n".join(lines)


def render_product_details(product_id: str, user_id: int = None) -> str:
    product = PRODUCTS[product_id]
    detail_lines = "\n".join(product["details"])
    rich_details = product.get("details_rich")
    if isinstance(rich_details, dict):
        detail_lines = render_serialized_entities_html(
            rich_details.get("text", detail_lines), rich_details.get("entities")
        ) or detail_lines
    stock = get_display_stock(product_id)
    icon = product_icon_html(product)
    duration = format_duration_text(product.get("month", ""))
    duration_line = f"<b>Duration:</b> {duration}\n" if duration else ""
    bulk_offers = render_bulk_pricing_offers(product)
    bulk_section = f"{bulk_offers}\n\n" if bulk_offers else ""
    return (
        "📦 <b>PRODUCT DETAILS</b>\n\n"
        f"<b>Icon:</b> {icon}\n"
        f"<b>Name:</b> {product['name']}\n"
        f"{duration_line}"
        f"<b>Price:</b> {format_product_price_for_user(product_id, user_id)}\n"
        f"<b>Stock:</b> {stock} pcs\n\n"
        f"{detail_lines}\n\n"
        f"{bulk_section}"
        "<b>Select quantity below:</b>"
    )

def render_buy_summary(product_id: str, qty: int, wallet_balance: float, user_id: int = None) -> str:
    product = PRODUCTS[product_id]
    unit_price, total = calculate_order_total(product_id, qty, user_id)
    remaining = wallet_balance - total
    if wallet_balance >= total:
        return (
            "🛒 <b>ORDER SUMMARY</b>\n\n"
            f"<b>Product:</b> {product['name']}\n"
            f"<b>Unit Price:</b> {format_money(unit_price)}\n"
            f"<b>Quantity:</b> {qty}\n"
            f"<b>Total Price:</b> {format_money(total)}\n"
            f"<b>Wallet Balance:</b> {format_money(wallet_balance)}\n"
            f"<b>Remaining After Purchase:</b> {format_money(remaining)}\n\n"
            "✅ <b>You have enough wallet balance.</b>\n"
            "This order will be completed directly from your wallet."
        )

    shortage = total - wallet_balance
    return (
        "🛒 <b>ORDER SUMMARY</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Unit Price:</b> {format_money(unit_price)}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total Price:</b> {format_money(total)}\n"
        f"<b>Wallet Balance:</b> {format_money(wallet_balance)}\n"
        f"<b>Shortage:</b> {format_money(shortage)}\n\n"
        "❌ <b>Wallet balance is not enough.</b>\n"
        "<b>Please select a payment method:</b>"
    )


def render_deposit_text() -> str:
    return "💳 <b>CRYPTO DEPOSIT</b>\n\n<b>Please select an amount below:</b>"


def render_deposit_method_text(amount: float) -> str:
    return (
        "💳 <b>SELECT PAYMENT METHOD</b>\n\n"
        f"<b>Amount to deposit:</b> {format_money(amount)}\n\n"
        "<b>Choose a payment method below:</b>"
    )


def render_manual_payment_text(amount: float, method: str, details: str) -> str:
    return (
        "🏦 <b>Exchange Payment</b>\n\n"
        f"<b>Amount:</b> {format_money(amount)}\n"
        f"<b>Method:</b> {method}\n\n"
        f"{escape_html(details)}\n\n"
        "<b>Send payment screenshot to Live Support for confirmation.</b>"
    )


def render_crypto_payment_text(amount: float, network: str, address: str) -> str:
    return (
        "✅ <b>DEPOSIT PAYMENT DETAILS</b>\n\n"
        f"<b>Amount:</b> {format_money(amount)}\n"
        f"<b>Method:</b> Crypto Address\n"
        f"<b>Network:</b> {network}\n\n"
        f"{escape_html(address)}\n\n"
        "<b>After payment, send your TXID in chat.</b>"
    )


def render_buy_crypto_payment_text(product_id: str, qty: int, total: float, network: str, address: str) -> str:
    product = PRODUCTS[product_id]
    return (
        "✅ <b>ORDER PAYMENT DETAILS</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total:</b> {format_money(total)}\n"
        f"<b>Method:</b> Crypto Address\n"
        f"<b>Network:</b> {network}\n\n"
        f"{escape_html(address)}\n\n"
        "<b>After payment, send your TXID in chat.</b>"
    )


def render_buy_manual_payment_text(product_id: str, qty: int, total: float, method: str, details: str) -> str:
    product = PRODUCTS[product_id]
    return (
        "🏦 <b>ORDER PAYMENT DETAILS</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total:</b> {format_money(total)}\n"
        f"<b>Method:</b> {method}\n\n"
        f"{escape_html(details)}\n\n"
        "<b>Send payment screenshot to Live Support for confirmation.</b>"
    )


def render_admin_products_text() -> str:
    return "🛠 <b>PRODUCTS MANAGEMENT</b>\n\nChoose what you want to do."


def render_admin_products_list() -> str:
    lines = ["📋 <b>PRODUCT LIST</b>\n"]
    for idx, product_id in enumerate(product_order, start=1):
        product = PRODUCTS[product_id]
        lines.append(
            f"\n<b>{idx}.</b> {product_icon_html(product)} <b>{product['name']}</b> ({product_id})\n"
            f"Month: {product['month']}\n"
            f"Price: {format_money(product['price'])}\n"
            f"Display Stock: {get_display_stock(product_id)} pcs\n"
            f"Real Stock: {get_product_stock(product_id)} pcs"
        )
    return "\n".join(lines)


def render_bulk_pricing_admin(product_id: str) -> str:
    product = PRODUCTS.get(product_id, {})
    tiers = get_bulk_pricing_tiers(product)
    lines = [
        "💸 <b>BULK PRICING</b>",
        "",
        f"<b>Product:</b> {escape_html(product.get('name', product_id))}",
        f"<b>Normal Price:</b> {format_money(product.get('price', 0))}",
        "",
        "<b>Configured Tiers:</b>",
    ]
    if tiers:
        for tier in tiers:
            lines.append(
                f"✅ {tier['min_qty']}+ pcs = {format_money(tier['unit_price'])} each"
            )
    else:
        lines.append("No bulk pricing tiers configured.")
    return "\n".join(lines)


def render_admin_categories_text() -> str:
    normalize_categories()
    return (
        "🗂 <b>CATEGORIES</b>\n\n"
        f"<b>Total Categories:</b> {len(CATEGORIES)}\n"
        f"<b>Total Products:</b> {len(PRODUCTS)}\n\n"
        "Choose a category action below."
    )


def render_admin_categories_list() -> str:
    normalize_categories()
    lines = ["📋 <b>CATEGORY LIST</b>", ""]
    for index, category_id in enumerate(category_order, start=1):
        category = CATEGORIES.get(category_id, {})
        default_text = " (Default)" if category_id == DEFAULT_CATEGORY_ID else ""
        lines.append(
            f"{index}. {category_icon_html(category)} "
            f"<b>{escape_html(category.get('name', category_id))}</b>{default_text}\n"
            f"   ID: <code>{escape_html(category_id)}</code> | "
            f"Products: {len(get_category_product_ids(category_id))}"
        )
    return "\n".join(lines)


def render_admin_add_product_preview(user_id: int) -> str:
    temp = admin_temp[user_id]
    details_text = "\n".join(temp.get("details", []))
    return (
        "🆕 <b>CONFIRM NEW PRODUCT</b>\n\n"
        f"<b>Icon:</b> {product_icon_html(temp)}\n"
        f"<b>Name:</b> {temp.get('name', '')}\n"
        f"<b>Month:</b> {temp.get('month', '')}\n"
        f"<b>Price:</b> {format_money(float(temp.get('price', 0)))}\n"
        f"<b>Display Stock:</b> {int(temp.get('display_stock', 0))}\n\n"
        f"<b>Details:</b>\n{details_text}\n\n"
        "Confirm add product?"
    )
def render_admin_edit_name_preview(product_id: str, new_name: str) -> str:
    product = PRODUCTS[product_id]
    return (
        "✏️ <b>CONFIRM NAME UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>New Name:</b> {new_name}\n\n"
        "Confirm update?"
    )


def render_admin_edit_price_preview(product_id: str, new_price: float) -> str:
    product = PRODUCTS[product_id]
    return (
        "💲 <b>CONFIRM PRICE UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Old Price:</b> {format_money(product['price'])}\n"
        f"<b>New Price:</b> {format_money(new_price)}\n\n"
        "Confirm update?"
    )


def render_admin_edit_month_preview(product_id: str, new_month: str) -> str:
    product = PRODUCTS[product_id]
    return (
        "📅 <b>CONFIRM MONTH UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Old Month:</b> {product['month']}\n"
        f"<b>New Month:</b> {new_month}\n\n"
        "Confirm update?"
    )


def render_admin_edit_details_preview(product_id: str, new_details: list, new_details_rich: dict = None) -> str:
    product = PRODUCTS[product_id]
    details_text = "\n".join(new_details)
    if isinstance(new_details_rich, dict):
        details_text = render_serialized_entities_html(
            new_details_rich.get("text", details_text), new_details_rich.get("entities")
        ) or details_text
    return (
        "📝 <b>CONFIRM DETAILS UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n\n"
        f"<b>New Details:</b>\n{details_text}\n\nConfirm update?"
    )


def render_admin_edit_delivery_guide_preview(product_id: str, new_guide: str) -> str:
    product = PRODUCTS[product_id]
    preview = new_guide.strip()
    if len(preview) > 2500:
        preview = preview[:2500] + "\n..."
    return (
        "📌 <b>CONFIRM DELIVERY GUIDE UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n\n"
        f"<b>New Guide:</b>\n{escape_html(preview)}\n\n"
        "Confirm update?"
    )


def render_admin_edit_icon_preview(product_id: str, new_icon: str, new_icon_custom_emoji_id: str = None) -> str:
    product = PRODUCTS[product_id]
    temp_icon = {"icon": new_icon, "icon_custom_emoji_id": new_icon_custom_emoji_id}
    return (
        "😀 <b>CONFIRM ICON UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Old Icon:</b> {product_icon_html(product)}\n"
        f"<b>New Icon:</b> {product_icon_html(temp_icon)}\n\n"
        "Confirm update?"
    )


def render_admin_edit_display_stock_preview(product_id: str, new_display_stock: int) -> str:
    product = PRODUCTS[product_id]
    return (
        "📦 <b>CONFIRM DISPLAY STOCK UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Old Display Stock:</b> {get_display_stock(product_id)}\n"
        f"<b>New Display Stock:</b> {new_display_stock}\n\n"
        "Confirm update?"
    )


def render_admin_delete_preview(product_id: str) -> str:
    product = PRODUCTS[product_id]
    return (
        "🗑 <b>DELETE PRODUCT</b>\n\n"
        f"<b>Product:</b> {product_icon_html(product)} {product['name']}\n"
        f"<b>ID:</b> {product_id}\n"
        f"<b>Display Stock:</b> {get_display_stock(product_id)} pcs\n"
        f"<b>Real Stock:</b> {get_product_stock(product_id)} pcs\n\n"
        "⚠️ <b>This action cannot be undone.</b>\n"
        "Confirm delete?"
    )


def render_admin_stock_list() -> str:
    lines = ["📦 <b>STOCK LIST</b>\n"]
    for idx, product_id in enumerate(product_order, start=1):
        product = PRODUCTS[product_id]
        lines.append(
            f"\n<b>{idx}.</b> {product_icon_html(product)} <b>{product['name']}</b> ({product_id})\n"
            f"Display Stock: {get_display_stock(product_id)} pcs\n"
            f"Real Stock: {get_product_stock(product_id)} pcs"
        )
    return "\n".join(lines)


def render_account_list_text(product_id: str, page: int = 0, page_size: int = 15) -> str:
    product = PRODUCTS[product_id]
    accounts = product["accounts"]
    if not accounts:
        return f"📭 <b>{product['name']}</b>\n\nNo accounts found."

    start = page * page_size
    end = min(start + page_size, len(accounts))
    lines = [
        f"👀 <b>ACCOUNT LIST</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Total Accounts:</b> {len(accounts)}\n"
        f"<b>Showing:</b> {start + 1}-{end}\n"
    ]
    for i in range(start, end):
        acc = accounts[i]
        stock_text = format_stock_item_full(acc)
        lines.append(
            f"\n<b>#{i + 1}</b>\n"
            f"<code>{escape_html(stock_text)}</code>"
        )
    return "\n".join(lines)


def render_selected_account(product_id: str, index: int) -> str:
    product = PRODUCTS[product_id]
    acc = product["accounts"][index]
    stock_text = format_stock_item_full(acc)
    return (
        f"🔐 <b>STOCK ITEM DETAILS</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Serial:</b> #{index + 1}\n\n"
        f"<code>{escape_html(stock_text)}</code>"
    )


def render_promo_list() -> str:
    if not PROMO_CODES:
        return "🎟 <b>PROMO LIST</b>\n\nNo promo codes found."

    lines = ["🎟 <b>PROMO LIST</b>\n"]
    for code, info in PROMO_CODES.items():
        lines.append(
            f"\n<b>{code}</b>\n"
            f"Amount: {format_money(float(info['amount']))}\n"
            f"Status: {'Enabled' if info.get('enabled', True) else 'Disabled'}\n"
            f"One Time: {'Yes' if info.get('one_time', False) else 'No'}\n"
            f"Created: {format_dt(info.get('created_at'))}\n"
            f"Created By: {info.get('created_by', 'system')}"
        )
    return "\n".join(lines)


def render_generated_promo_text(code: str) -> str:
    info = PROMO_CODES[code]
    return (
        "🎉 <b>PROMO GENERATED</b>\n\n"
        f"<b>Code:</b> <code>{code}</code>\n"
        f"<b>Amount:</b> {format_money(info['amount'])}\n"
        f"<b>One Time:</b> Yes\n"
        f"<b>Created:</b> {format_dt(info.get('created_at'))}"
    )


def render_order_list(mode: str) -> str:
    if mode == "all":
        orders = all_orders
        title = "📦 <b>ALL ORDERS</b>"
    elif mode == "pending_manual":
        orders = [o for o in all_orders if o["status"] == "Waiting Manual Confirmation"]
        title = "⏳ <b>PENDING MANUAL ORDERS</b>"
    else:
        orders = [o for o in all_orders if o["status"] == "Completed"]
        title = "✅ <b>COMPLETED ORDERS</b>"

    if not orders:
        return f"{title}\n\nNo records found."

    lines = [title + "\n"]
    for order in reversed(orders[-50:]):
        lines.append(
            f"\n<b>#{order['id']}</b>\n"
            f"User: {order['user_id']}\n"
            f"Product: {order['product']}\n"
            f"Qty: {order['qty']}\n"
            f"Total: {format_money(order['total'])}\n"
            f"Payment: {order.get('payment_type', 'Unknown')}\n"
            f"Status: <b>{order['status']}</b>\n"
            f"Date: {format_dt(order.get('created_at'))}"
        )
    return "\n".join(lines)


def render_all_deposits_text() -> str:
    deposits = [tx for tx in all_transactions if tx["type"] == "Deposit"]
    if not deposits:
        return "💳 <b>ALL DEPOSITS</b>\n\nNo deposits found."

    lines = ["💳 <b>ALL DEPOSITS</b>\n"]
    for tx in reversed(deposits[-50:]):
        lines.append(
            f"\n<b>TX#{tx['id']}</b>\n"
            f"User: {tx['user_id']}\n"
            f"Amount: {format_money(tx['amount'])}\n"
            f"Status: <b>{tx['status']}</b>\n"
            f"Date: {format_dt(tx.get('created_at'))}"
        )
    return "\n".join(lines)


def render_pending_manual_deposits() -> str:
    deposits = [tx for tx in all_transactions if tx["type"] == "Deposit" and tx["status"] == "Waiting Manual Confirmation"]
    if not deposits:
        return "💳 <b>PENDING MANUAL DEPOSITS</b>\n\nNo pending deposits found."

    lines = ["💳 <b>PENDING MANUAL DEPOSITS</b>\n"]
    for tx in reversed(deposits[-50:]):
        lines.append(
            f"\n<b>TX#{tx['id']}</b>\n"
            f"User: {tx['user_id']}\n"
            f"Amount: {format_money(tx['amount'])}\n"
            f"Status: <b>{tx['status']}</b>\n"
            f"Date: {format_dt(tx.get('created_at'))}"
        )
    return "\n".join(lines)


def render_users_admin() -> str:
    total_users = len(all_users)
    total_wallet = sum(user_wallet.values()) if user_wallet else 0.0
    return (
        "👤 <b>USERS ADMIN</b>\n\n"
        f"<b>Total Users:</b> {total_users}\n"
        f"<b>Total Wallet Balance:</b> {format_money(total_wallet)}\n"
        f"<b>Total Orders:</b> {len(all_orders)}\n"
        f"<b>Total Transactions:</b> {len(all_transactions)}\n\n"
        "Tap <b>User Details</b> to see usernames and Telegram user IDs."
    )


def render_user_details_page(page: int = 0, per_page: int = 25):
    users = sorted(list(all_users))
    total = len(users)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    end = start + per_page

    lines = [
        "👥 <b>USER DETAILS</b>",
        f"<b>Total Users:</b> {total}",
        f"<b>Page:</b> {page + 1}/{total_pages}",
        "",
    ]

    if not users:
        lines.append("No users found yet.")
        return "\n".join(lines), page, total_pages

    for idx, uid in enumerate(users[start:end], start=start + 1):
        profile = get_user_profile(uid)
        username = str(profile.get("username") or "").strip().lstrip("@")
        name = ((str(profile.get("first_name") or "").strip() + " " + str(profile.get("last_name") or "").strip()).strip())
        wallet = float(user_wallet.get(uid, 0.0))
        if username:
            user_text = f'<a href="https://t.me/{escape_html(username)}">@{escape_html(username)}</a>'
        elif name:
            user_text = escape_html(name)
        else:
            user_text = "No username"
        lines.append(f"{idx}. {user_text} | ID: <code>{uid}</code> | Wallet: {format_money(wallet)}")

    return "\n".join(lines), page, total_pages


def render_analytics() -> str:
    total_users = len(all_users)
    total_wallet_balance = sum(user_wallet.values()) if user_wallet else 0.0

    completed_orders = [o for o in all_orders if o["status"] == "Completed"]
    pending_orders = [o for o in all_orders if o["status"] == "Waiting Manual Confirmation"]

    completed_deposits = [t for t in all_transactions if t["type"] == "Deposit" and t["status"] == "Completed"]
    pending_deposits = [t for t in all_transactions if t["type"] == "Deposit" and t["status"] == "Waiting Manual Confirmation"]

    total_sales = sum(o["total"] for o in completed_orders)
    total_deposit_amount = sum(t["amount"] for t in completed_deposits)

    promo_total = len(PROMO_CODES)
    enabled_promos = len([p for p in PROMO_CODES.values() if p.get("enabled", True)])

    product_sales_map = {}
    for order in completed_orders:
        product_sales_map[order["product"]] = product_sales_map.get(order["product"], 0) + order["qty"]

    top_product_text = "N/A"
    if product_sales_map:
        top_product_text = max(product_sales_map.items(), key=lambda x: x[1])[0]

    lines = [
        "📊 <b>ANALYTICS</b>\n",
        f"Users: {total_users}",
        f"Completed Orders: {len(completed_orders)}",
        f"Pending Orders: {len(pending_orders)}",
        f"Completed Deposits: {len(completed_deposits)}",
        f"Pending Deposits: {len(pending_deposits)}",
        f"Total Sales: {format_money(total_sales)}",
        f"Total Deposit Amount: {format_money(total_deposit_amount)}",
        f"Total User Wallet Balance: {format_money(total_wallet_balance)}",
        f"Total Promos: {promo_total}",
        f"Enabled Promos: {enabled_promos}",
        f"Top Product: {top_product_text}",
    ]
    return "\n".join(lines)


# =========================
# REQUEST HELPERS
# =========================
def http_get_json(url: str, params=None, headers=None, timeout=25):
    try:
        res = requests.get(url, params=params, headers=headers, timeout=timeout)
        return {"ok": res.ok, "status_code": res.status_code, "data": res.json() if res.content else {}}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def http_post_json(url: str, payload=None, headers=None, timeout=25):
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        return {"ok": res.ok, "status_code": res.status_code, "data": res.json() if res.content else {}}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def verify_result(ok: bool, status: str, reason: str):
    return {"ok": ok, "status": status, "reason": reason}
def amount_within_tolerance(actual_amount, expected_amount, tolerance=0.10):
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)

    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False

    return abs(actual_dec - expected_dec) <= tolerance_dec


def verify_usdt_trc20_txid(txid: str, expected_amount: float, expected_to_address: str):
    info_res = http_post_json(
        f"{TRONGRID_BASE}/walletsolidity/gettransactioninfobyid",
        payload={"value": txid},
        headers=trongrid_headers(),
        timeout=20,
    )
    if not info_res["ok"]:
        return verify_result(False, "pending", f"tron info http {info_res['status_code']}")

    info_data = info_res["data"]
    if not info_data:
        return verify_result(False, "pending", "transaction not confirmed yet")

    receipt = info_data.get("receipt", {}) or {}
    receipt_result = str(receipt.get("result", "")).upper()
    if receipt_result and receipt_result != "SUCCESS":
        return verify_result(False, "rejected", f"receipt result = {receipt_result}")

    ev_res = http_get_json(
        f"{TRONGRID_BASE}/v1/transactions/{txid}/events",
        params={"only_confirmed": "true"},
        headers=trongrid_headers(),
        timeout=20,
    )
    if not ev_res["ok"]:
        return verify_result(False, "pending", f"tron event http {ev_res['status_code']}")

    events = ev_res["data"].get("data", [])
    if not events:
        return verify_result(False, "pending", "no confirmed events found")

    for ev in events:
        if str(ev.get("event_name", "")).lower() != "transfer":
            continue

        contract_address = str(ev.get("contract_address", "")).strip()
        if contract_address != USDT_TRC20_CONTRACT:
            continue

        result = ev.get("result", {}) or {}
        to_addr = result.get("to", "") or result.get("_to", "")
        value_raw = result.get("value", "") or result.get("_value", "")
        if not to_addr or value_raw == "":
            continue

        try:
            value_int = int(str(value_raw))
        except Exception:
            continue

        actual_amount = Decimal(value_int) / Decimal("1000000")
        if (
            str(to_addr).strip() == str(expected_to_address).strip()
            and amount_within_tolerance(actual_amount, expected_amount, 0.10)
        ):
            return verify_result(True, "confirmed", "verified")

    return verify_result(False, "rejected", "no matching USDT TRC20 transfer found")


def verify_trx_transfer(txid: str, expected_amount: float, expected_to_address: str):
    tx_res = http_post_json(
        f"{TRONGRID_BASE}/wallet/gettransactionbyid",
        payload={"value": txid},
        headers=trongrid_headers(),
        timeout=20,
    )
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"tron tx http {tx_res['status_code']}")

    tx_data = tx_res["data"]
    if not tx_data:
        return verify_result(False, "pending", "transaction not found yet")

    info_res = http_post_json(
        f"{TRONGRID_BASE}/walletsolidity/gettransactioninfobyid",
        payload={"value": txid},
        headers=trongrid_headers(),
        timeout=20,
    )
    if not info_res["ok"]:
        return verify_result(False, "pending", f"tron info http {info_res['status_code']}")

    info_data = info_res["data"]
    if not info_data:
        return verify_result(False, "pending", "transaction not confirmed yet")

    receipt = info_data.get("receipt", {}) or {}
    receipt_result = str(receipt.get("result", "")).upper()
    if receipt_result and receipt_result != "SUCCESS":
        return verify_result(False, "rejected", f"receipt result = {receipt_result}")

    contracts = (((tx_data.get("raw_data") or {}).get("contract")) or [])
    if not contracts:
        return verify_result(False, "rejected", "no TRX transfer contract found")

    contract = contracts[0] or {}
    param_value = (((contract.get("parameter") or {}).get("value")) or {})
    amount_sun = int(param_value.get("amount", 0))
    to_address_hex = str(param_value.get("to_address", "")).strip()
    if not to_address_hex:
        return verify_result(False, "rejected", "no destination found")

    actual_to = tron_hex_to_base58(to_address_hex)

    if actual_to != expected_to_address:
        return verify_result(False, "rejected", "destination address mismatch")

    actual_amount = Decimal(amount_sun) / Decimal("1000000")
    if not amount_within_tolerance(actual_amount, expected_amount, 0.10):
        return verify_result(False, "rejected", "amount mismatch")

    return verify_result(True, "confirmed", "verified")


def verify_evm_native_transfer(txid: str, expected_amount: float, expected_to_address: str, chainid: str, symbol: str):
    tx_res = get_evm_tx_by_hash(chainid, txid)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"{symbol} tx http {tx_res['status_code']}")

    tx_data = tx_res["data"].get("result")
    if not tx_data:
        return verify_result(False, "pending", "transaction not found yet")

    receipt_res = get_evm_tx_receipt(chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{symbol} receipt http {receipt_res['status_code']}")

    receipt = receipt_res["data"].get("result")
    if not receipt:
        return verify_result(False, "pending", "transaction not confirmed yet")

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "transaction failed")

    actual_to = normalize_evm_address(tx_data.get("to"))
    expected_to = normalize_evm_address(expected_to_address)
    if actual_to != expected_to:
        return verify_result(False, "rejected", "destination address mismatch")

    try:
        value_wei = int(str(tx_data.get("value", "0")), 16)
    except Exception:
        return verify_result(False, "rejected", "invalid value")

    actual_amount = Decimal(value_wei) / Decimal("1000000000000000000")
    if not amount_within_tolerance(actual_amount, expected_amount, 0.10):
        return verify_result(False, "rejected", "amount mismatch")

    return verify_result(True, "confirmed", "verified")


def verify_evm_token_transfer(
    txid: str,
    expected_amount: float,
    expected_to_address: str,
    chainid: str,
    token_contract: str,
    decimals: int,
    symbol: str,
):
    receipt_res = get_evm_tx_receipt(chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{symbol} receipt http {receipt_res['status_code']}")

    receipt = receipt_res["data"].get("result")
    if not receipt:
        return verify_result(False, "pending", "transaction not confirmed yet")

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "transaction failed")

    logs = receipt.get("logs", []) or []
    expected_contract = normalize_evm_address(token_contract)
    expected_to_topic = to_evm_topic_address(expected_to_address).lower()
    unit = Decimal(10) ** Decimal(decimals)

    for log in logs:
        log_address = normalize_evm_address(log.get("address"))
        if log_address != expected_contract:
            continue

        topics = log.get("topics", []) or []
        if len(topics) < 3:
            continue

        if str(topics[0]).lower() != ERC20_TRANSFER_TOPIC:
            continue

        if str(topics[2]).lower() != expected_to_topic:
            continue

        data_hex = str(log.get("data", "0x0"))
        try:
            value_raw = int(data_hex, 16)
        except Exception:
            continue

        actual_amount = Decimal(value_raw) / unit
        if amount_within_tolerance(actual_amount, expected_amount, 0.10):
            return verify_result(True, "confirmed", "verified")

    return verify_result(False, "rejected", "no matching token transfer found")


def verify_btc_transfer(txid: str, expected_amount: float, expected_to_address: str):
    tx_res = http_get_json(f"{BTC_API_BASE}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"btc tx http {tx_res['status_code']}")

    tx = tx_res["data"]
    status = tx.get("status", {}) or {}
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")

    if not status.get("confirmed"):
        return verify_result(False, "pending", "transaction not confirmed yet")

    for vout in tx.get("vout", []) or []:
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if (
            vout.get("scriptpubkey_address") == expected_to_address
            and amount_within_tolerance(actual_amount, expected_amount, 0.10)
        ):
            return verify_result(True, "confirmed", "verified")

    return verify_result(False, "rejected", "no matching BTC output found")


def verify_ltc_transfer(txid: str, expected_amount: float, expected_to_address: str):
    tx_res = http_get_json(f"{LTC_API_BASE}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"ltc tx http {tx_res['status_code']}")

    tx = tx_res["data"]
    status = tx.get("status", {}) or {}
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")

    if not status.get("confirmed"):
        return verify_result(False, "pending", "transaction not confirmed yet")

    for vout in tx.get("vout", []) or []:
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if (
            vout.get("scriptpubkey_address") == expected_to_address
            and amount_within_tolerance(actual_amount, expected_amount, 0.10)
        ):
            return verify_result(True, "confirmed", "verified")

    return verify_result(False, "rejected", "no matching LTC output found")


def helius_rpc(method: str, params):
    payload = {"jsonrpc": "2.0", "id": "1", "method": method, "params": params}
    return http_post_json(
        HELIUS_RPC_URL,
        payload=payload,
        headers={"content-type": "application/json"},
        timeout=25,
    )


def verify_sol_transfer(txid: str, expected_amount: float, expected_to_address: str):
    res = helius_rpc(
        "getTransaction",
        [
            txid,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed",
            },
        ],
    )
    if not res["ok"]:
        return verify_result(False, "pending", f"sol rpc http {res['status_code']}")

    tx = res["data"].get("result")
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")

    meta = tx.get("meta", {}) or {}
    if meta.get("err") is not None:
        return verify_result(False, "rejected", "solana transaction failed")

    instructions = []
    message = (tx.get("transaction", {}) or {}).get("message", {}) or {}
    instructions.extend(message.get("instructions", []) or [])

    for inner in meta.get("innerInstructions", []) or []:
        instructions.extend(inner.get("instructions", []) or [])

    for ins in instructions:
        parsed = ins.get("parsed")
        if not parsed:
            continue

        info = parsed.get("info", {}) or {}
        if parsed.get("type") == "transfer":
            destination = info.get("destination")
            lamports = info.get("lamports")

            actual_amount = Decimal(int(lamports)) / Decimal("1000000000")
            if (
                destination == expected_to_address
                and amount_within_tolerance(actual_amount, expected_amount, 0.10)
            ):
                return verify_result(True, "confirmed", "verified")

    return verify_result(False, "rejected", "no matching SOL transfer found")

# =========================
# ACTION HELPERS
# =========================
async def notify_waiters_for_product(context: ContextTypes.DEFAULT_TYPE, product_id: str):
    waiters = list(notify_waitlist.get(product_id, set()))
    if not waiters:
        return

    product = PRODUCTS[product_id]
    text = (
        f"🔔 {product_icon_html(product)} <b>{escape_html(product['name'])}</b> is back in stock!\n\n"
        f"<b>Month:</b> {product['month']}\n"
        f"<b>Price:</b> {format_money(product['price'])}\n"
        f"<b>Available now:</b> {get_display_stock(product_id)} pcs"
    )
    fallback_text = text.replace(product_icon_html(product), escape_html(_normal_icon_text(product)), 1)
    for waiter_id in waiters:
        try:
            await context.bot.send_message(waiter_id, text, parse_mode="HTML")
        except Exception:
            if fallback_text != text:
                try:
                    await context.bot.send_message(waiter_id, fallback_text, parse_mode="HTML")
                except Exception:
                    pass
    notify_waitlist[product_id].clear()


def _short_button_text(text: str, max_len: int = 60) -> str:
    text = str(text)
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


def render_shop_menu_text() -> str:
    base = (
        "🛒🛒 <b>STORE MENU</b>\n\n"
        "──── ⚡ <b>AUTO DELIVERY</b> ────\n"
        "Tap any product below to continue."
    )
    flash_banner = render_flash_deal_banner() if "render_flash_deal_banner" in globals() else ""
    return f"{flash_banner}\n\n{base}" if flash_banner else base


def shop_product_rows(product_ids: list, user_id: int = None, styled: bool = True) -> list:
    rows = []
    for product_id in product_ids:
        if product_id not in PRODUCTS:
            continue
        product = PRODUCTS[product_id]
        stock = get_display_stock(product_id)
        month = format_duration_text(product.get("month", ""))
        month_part = f" {month}" if month else ""
        price_text = format_product_price_for_user(product_id, user_id)
        if stock <= 0:
            rows.append([
                make_product_inline_button(
                    product,
                    f"{product['name']}{month_part} - {price_text} | 🔔 Notify",
                    f"shop_notify_{product_id}",
                    style="danger" if styled else None,
                )
            ])
            continue
        core_label = f"{product['name']}{month_part} - {price_text} | 📦 {stock} Pcs"
        rows.append([
            make_product_inline_button(
                product,
                core_label,
                f"shop_buy_{product_id}",
                style="primary" if styled else None,
            )
        ])
    return rows


def shop_categories_keyboard(user_id: int = None, styled: bool = True) -> InlineKeyboardMarkup:
    normalize_shop_order()
    rows = [[InlineKeyboardButton("──── ⚡ AUTO DELIVERY ────", callback_data="noop")]]
    for item in shop_order:
        item_type, item_id = item.split(":", 1)
        if item_type == "product":
            product = PRODUCTS.get(item_id)
            if product and product.get("category_id") == DEFAULT_CATEGORY_ID:
                rows.extend(shop_product_rows([item_id], user_id, styled=styled))
            continue
        category = CATEGORIES.get(item_id, {})
        if not category or item_id == DEFAULT_CATEGORY_ID:
            continue
        label = f"{category.get('name', item_id)}  ▶"
        rows.append([
            make_category_inline_button(
                category,
                label,
                f"shop_category_{item_id}",
                style="primary" if styled else None,
            )
        ])
    rows.append([
        make_styled_inline_button(
            "🏠 Back to Menu",
            callback_data="user_back_to_dashboard",
            style="danger" if styled else None,
        )
    ])
    return InlineKeyboardMarkup(rows)


def shop_menu_keyboard(user_id: int = None, category_id: str = None, styled: bool = True) -> InlineKeyboardMarkup:
    normalize_categories()
    rows = [[InlineKeyboardButton("──── ⚡ AUTO DELIVERY ────", callback_data="noop")]]
    rows.extend(shop_product_rows(get_category_product_ids(category_id), user_id, styled=styled))
    if category_id and category_id != DEFAULT_CATEGORY_ID:
        rows.append([
            make_styled_inline_button(
                "⬅️ Back to Categories",
                callback_data="back_shop_cards",
                style="primary" if styled else None,
            )
        ])
    rows.append([
        make_styled_inline_button(
            "🏠 Back to Menu",
            callback_data="user_back_to_dashboard",
            style="danger" if styled else None,
        )
    ])
    return InlineKeyboardMarkup(rows)


async def send_shop_cards_message(source, from_callback: bool = False, category_id: str = None):
    # Kept same function name so existing callbacks keep working.
    normalize_categories()
    viewer_id = getattr(getattr(source, "from_user", None), "id", None)
    if category_id is None or category_id == DEFAULT_CATEGORY_ID:
        text = render_shop_menu_text()
        keyboard = shop_categories_keyboard(viewer_id)
        fallback_keyboard = shop_categories_keyboard(viewer_id, styled=False)
    else:
        category = CATEGORIES.get(category_id, CATEGORIES[DEFAULT_CATEGORY_ID])
        text = (
            f"{category_icon_html(category)} "
            f"<b>{escape_html(category.get('name', category_id))}</b>\n\n"
            f"{render_shop_menu_text()}"
        )
        keyboard = shop_menu_keyboard(viewer_id, category_id)
        fallback_keyboard = shop_menu_keyboard(viewer_id, category_id, styled=False)
    if from_callback:
        try:
            await source.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
        except Exception as e:
            if "message is not modified" in str(e).lower():
                return
            print(f"Shop navigation edit failed; retrying normal buttons: {type(e).__name__}: {e}")
            try:
                await source.edit_message_text(text=text, reply_markup=fallback_keyboard, parse_mode="HTML")
            except Exception as fallback_error:
                if "message is not modified" in str(fallback_error).lower():
                    return
                print(f"Shop navigation fallback edit failed; sending new message: {type(fallback_error).__name__}: {fallback_error}")
                await source.message.reply_text(text, reply_markup=fallback_keyboard, parse_mode="HTML")
    else:
        try:
            await source.reply_text(text, reply_markup=keyboard, parse_mode="HTML")
        except Exception as e:
            print(f"Shop buttons were rejected; retrying normal buttons: {type(e).__name__}: {e}")
            await source.reply_text(text, reply_markup=fallback_keyboard, parse_mode="HTML")



async def send_html_lines(bot, chat_id: int, lines: list, max_len: int = 3800):
    """Send long HTML-safe messages without breaking Telegram's 4096 character limit."""
    chunk = []
    chunk_len = 0
    for line in lines:
        add_len = len(line) + 1
        if chunk and chunk_len + add_len > max_len:
            await bot.send_message(chat_id=chat_id, text="\n".join(chunk), parse_mode="HTML", disable_web_page_preview=True)
            chunk = []
            chunk_len = 0
        chunk.append(line)
        chunk_len += add_len
    if chunk:
        await bot.send_message(chat_id=chat_id, text="\n".join(chunk), parse_mode="HTML", disable_web_page_preview=True)


async def deliver_accounts_to_user(bot, user_id: int, product_id: str, qty: int):
    product = PRODUCTS[product_id]
    available = product["accounts"]
    if len(available) < qty:
        await bot.send_message(
            chat_id=user_id,
            text="❌ <b>Not enough real account inventory available right now.</b>\n\nPlease contact support.",
            parse_mode="HTML",
        )
        return False, []

    delivered = available[:qty]
    del available[:qty]

    current_display = get_display_stock(product_id)
    product["display_stock"] = max(0, current_display - qty)

    lines = [
        f"✅ <b>Order Completed:</b> {escape_html(product['name'])}",
        f"<b>Quantity:</b> {qty}",
        "",
        "🔐 <b>Your Account Details:</b>",
        "",
    ]
    for acc in delivered:
        raw_fields = acc.get("raw_fields")
        if isinstance(raw_fields, list) and raw_fields:
            account_line = " | ".join(str(x).strip() for x in raw_fields if str(x).strip())
        else:
            raw_line = str(acc.get("raw_line", "") or "").strip()
            if raw_line:
                account_line = raw_line
            else:
                fields = [
                    str(acc.get("email", "") or "").strip(),
                    str(acc.get("password", "") or "").strip(),
                    str(acc.get("note", "") or "").strip(),
                ]
                account_line = " | ".join(x for x in fields if x)
        lines.append(f"<code>{escape_html(account_line)}</code>")

    guide = get_delivery_guide(product_id)
    rich_guide = product.get("delivery_guide_rich")
    if not isinstance(rich_guide, dict):
        rich_guide = None
    if guide and not rich_guide:
        lines.append("")
        lines.append(escape_html(guide))

    await send_html_lines(bot, user_id, lines)
    if guide and rich_guide:
        entities = deserialize_message_entities(rich_guide.get("entities"), bot)
        if entities:
            try:
                await bot.send_message(chat_id=user_id, text=rich_guide.get("text", guide), entities=entities)
                return True, delivered
            except Exception as e:
                print(f"Rich delivery guide send failed for product_id={product_id}; using plain text: {type(e).__name__}")
        await bot.send_message(chat_id=user_id, text=guide)
    return True, delivered


async def process_wallet_purchase(update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int, product_id: str, qty: int, total: float):
    if user_wallet[user_id] < total:
        return False

    ok, delivered = await deliver_accounts_to_user(context.bot, user_id, product_id, qty)
    if not ok:
        return False

    user_wallet[user_id] -= total
    add_order_record(user_id, product_id, qty, total, "Completed", "Wallet", delivered_items=delivered)
    add_transaction_record(user_id, "Wallet Purchase", total, "Completed", {"product_id": product_id, "qty": qty})
    await notify_admin_order(context.bot, user_id, product_id, qty, total, "Wallet")

    msg = (
        f"✅ <b>Order completed successfully.</b>\n\n"
        f"<b>{format_money(total)}</b> deducted from your wallet.\n"
        f"{get_wallet_balance_text(user_id)}"
    )

    if hasattr(update_or_query, "message"):
        await update_or_query.message.reply_text(
            msg,
            reply_markup=main_menu() if user_mode.get(user_id) == "client" else admin_menu(),
            parse_mode="HTML",
        )
    else:
        await update_or_query.reply_text(
            msg,
            reply_markup=main_menu() if user_mode.get(user_id) == "client" else admin_menu(),
            parse_mode="HTML",
        )
    return True


async def finalize_verified_deposit(bot, user_id: int, amount: float, txid: str):
    used_txids.add(txid)
    user_wallet[user_id] = user_wallet.get(user_id, 0) + amount

    for tx in reversed(user_transactions.get(user_id, [])):
        if tx["type"] == "Deposit" and tx["status"] == "Checking TXID" and tx["amount"] == amount:
            set_tx_status(tx, "Completed")
            tx["meta"]["txid"] = txid
            break

    for tx in reversed(all_transactions):
        if tx["user_id"] == user_id and tx["type"] == "Deposit" and tx["status"] == "Checking TXID" and tx["amount"] == amount:
            set_tx_status(tx, "Completed")
            tx["meta"]["txid"] = txid
            break

    pending_crypto_deposits.pop(user_id, None)

    await bot.send_message(
        chat_id=user_id,
        text=(
            f"✅ <b>Payment confirmed.</b>\n\n"
            f"<b>{format_money(amount)}</b> added to your wallet.\n"
            f"{get_wallet_balance_text(user_id)}"
        ),
        parse_mode="HTML",
    )


async def finalize_verified_order(bot, user_id: int, product_id: str, qty: int, total: float, txid: str):
    ok, delivered = await deliver_accounts_to_user(bot, user_id, product_id, qty)
    if not ok:
        pending_crypto_orders.pop(user_id, None)
        return False

    used_txids.add(txid)
    add_order_record(user_id, product_id, qty, total, "Completed", "Crypto", delivered_items=delivered)
    await notify_admin_order(bot, user_id, product_id, qty, total, "Crypto")

    for tx in reversed(user_transactions.get(user_id, [])):
        if tx["type"] == "Order Payment" and tx["status"] == "Checking TXID" and tx["amount"] == total:
            set_tx_status(tx, "Completed")
            tx["meta"]["txid"] = txid
            break

    for tx in reversed(all_transactions):
        if tx["user_id"] == user_id and tx["type"] == "Order Payment" and tx["status"] == "Checking TXID" and tx["amount"] == total:
            set_tx_status(tx, "Completed")
            tx["meta"]["txid"] = txid
            break

    pending_crypto_orders.pop(user_id, None)

    await bot.send_message(
        chat_id=user_id,
        text=(
            f"✅ <b>Payment confirmed.</b>\n\n"
            f"<b>Order completed</b> for {PRODUCTS[product_id]['name']}.\n"
            f"<b>Quantity:</b> {qty}\n"
            f"<b>Total:</b> {format_money(total)}"
        ),
        parse_mode="HTML",
    )
    return True


async def confirm_manual_order(context: ContextTypes.DEFAULT_TYPE, order_id: int):
    order = find_order_by_id(order_id)
    if not order:
        return False, "Order not found."
    if order["status"] != "Waiting Manual Confirmation":
        return False, "Order is no longer pending."

    ok, delivered = await deliver_accounts_to_user(context.bot, order["user_id"], order["product_id"], order["qty"])
    if not ok:
        return False, "Not enough real stock to deliver."

    delivered_snapshot = snapshot_delivered_items(delivered)
    order["delivered_items"] = delivered_snapshot
    set_order_status(order, "Completed")
    await notify_admin_order(context.bot, order["user_id"], order["product_id"], order["qty"], order["total"], order.get("payment_type", "Manual"))
    for user_order in user_orders.get(order["user_id"], []):
        if user_order["id"] == order_id:
            user_order["delivered_items"] = list(delivered_snapshot)
            set_order_status(user_order, "Completed")
            break

    for tx in reversed(all_transactions):
        if tx["user_id"] == order["user_id"] and tx["type"] == "Order Payment" and tx["amount"] == order["total"] and tx["status"] == "Waiting Manual Confirmation":
            set_tx_status(tx, "Completed")
            break

    for tx in reversed(user_transactions.get(order["user_id"], [])):
        if tx["type"] == "Order Payment" and tx["amount"] == order["total"] and tx["status"] == "Waiting Manual Confirmation":
            set_tx_status(tx, "Completed")
            break

    await context.bot.send_message(
        chat_id=order["user_id"],
        text=(
            f"✅ <b>Your manual payment has been confirmed.</b>\n\n"
            f"<b>Product:</b> {order['product']}\n"
            f"<b>Quantity:</b> {order['qty']}\n"
            f"<b>Total:</b> {format_money(order['total'])}"
        ),
        parse_mode="HTML",
    )
    return True, "Manual order confirmed."


async def reject_manual_order(context: ContextTypes.DEFAULT_TYPE, order_id: int):
    order = find_order_by_id(order_id)
    if not order:
        return False, "Order not found."
    if order["status"] != "Waiting Manual Confirmation":
        return False, "Order is no longer pending."

    set_order_status(order, "Rejected")
    for user_order in user_orders.get(order["user_id"], []):
        if user_order["id"] == order_id:
            set_order_status(user_order, "Rejected")
            break

    for tx in reversed(all_transactions):
        if tx["user_id"] == order["user_id"] and tx["type"] == "Order Payment" and tx["amount"] == order["total"] and tx["status"] == "Waiting Manual Confirmation":
            set_tx_status(tx, "Rejected")
            break

    for tx in reversed(user_transactions.get(order["user_id"], [])):
        if tx["type"] == "Order Payment" and tx["amount"] == order["total"] and tx["status"] == "Waiting Manual Confirmation":
            set_tx_status(tx, "Rejected")
            break

    await context.bot.send_message(
        chat_id=order["user_id"],
        text=(
            f"❌ <b>Your manual order payment was rejected.</b>\n\n"
            f"<b>Product:</b> {order['product']}\n"
            f"<b>Total:</b> {format_money(order['total'])}\n\n"
            f"Please contact support."
        ),
        parse_mode="HTML",
    )
    return True, "Manual order rejected."


async def confirm_manual_deposit(context: ContextTypes.DEFAULT_TYPE, tx_id: int):
    tx = find_tx_by_id(tx_id)
    if not tx:
        return False, "Deposit record not found."
    if tx["type"] != "Deposit" or tx["status"] != "Waiting Manual Confirmation":
        return False, "Deposit is no longer pending."

    set_tx_status(tx, "Completed")
    for user_tx in user_transactions.get(tx["user_id"], []):
        if user_tx["id"] == tx_id:
            set_tx_status(user_tx, "Completed")
            break

    user_wallet[tx["user_id"]] = user_wallet.get(tx["user_id"], 0) + tx["amount"]

    await context.bot.send_message(
        chat_id=tx["user_id"],
        text=(
            f"✅ <b>Your manual deposit has been confirmed.</b>\n\n"
            f"<b>Amount:</b> {format_money(tx['amount'])}\n"
            f"{get_wallet_balance_text(tx['user_id'])}"
        ),
        parse_mode="HTML",
    )
    return True, "Manual deposit confirmed."


async def reject_manual_deposit(context: ContextTypes.DEFAULT_TYPE, tx_id: int):
    tx = find_tx_by_id(tx_id)
    if not tx:
        return False, "Deposit record not found."
    if tx["type"] != "Deposit" or tx["status"] != "Waiting Manual Confirmation":
        return False, "Deposit is no longer pending."

    set_tx_status(tx, "Rejected")
    for user_tx in user_transactions.get(tx["user_id"], []):
        if user_tx["id"] == tx_id:
            set_tx_status(user_tx, "Rejected")
            break

    await context.bot.send_message(
        chat_id=tx["user_id"],
        text=(
            f"❌ <b>Your manual deposit was rejected.</b>\n\n"
            f"<b>Amount:</b> {format_money(tx['amount'])}\n\n"
            f"Please contact support."
        ),
        parse_mode="HTML",
    )
    return True, "Manual deposit rejected."


async def finalize_auto_deposit_record(record: dict):
    user_id = record["user_id"]
    amount = float(record["usd_amount"])
    wc.complete_deposit_record(
        record,
        user_wallet,
        add_transaction_record=add_transaction_record,
        set_tx_status=set_tx_status,
        user_transactions=user_transactions,
        all_transactions=all_transactions,
    )
    await app_instance.bot.send_message(
        chat_id=user_id,
        text=(
            f"✅ <b>Payment confirmed automatically.</b>\n\n"
            f"<b>{format_money(amount)}</b> added to your wallet.\n"
            f"{get_wallet_balance_text(user_id)}"
        ),
        parse_mode="HTML",
    )


async def finalize_auto_order_record(record: dict):
    user_id = record["user_id"]
    result = await wc.complete_order_record_async(
        record,
        deliver_accounts_to_user,
        app_instance.bot,
        add_order_record,
        add_transaction_record=add_transaction_record,
        set_tx_status=set_tx_status,
        user_transactions=user_transactions,
        all_transactions=all_transactions,
    )
    if result["ok"]:
        await app_instance.bot.send_message(
            chat_id=user_id,
            text=(
                f"✅ <b>Payment confirmed automatically.</b>\n\n"
                f"<b>Order completed</b> for {PRODUCTS[record['product_id']]['name']}.\n"
                f"<b>Quantity:</b> {record['qty']}\n"
                f"<b>Total:</b> {format_money(record['usd_amount'])}"
            ),
            parse_mode="HTML",
        )


async def send_auto_pending_message(user_id: int, record: dict, result: dict):
    return


async def send_auto_rejected_message(user_id: int, record: dict, result: dict):
    await app_instance.bot.send_message(
        chat_id=user_id,
        text=f"❌ <b>Payment rejected.</b>\n\n{escape_html(result.get('message') or result.get('reason') or 'Rejected')}",
        parse_mode="HTML",
    )


async def send_auto_completed_message(user_id: int, record: dict, result: dict):
    return


def auto_scan_callable_from_record(record: dict):
    return cv.auto_verify_by_record(record, build_verify_config())


async def background_crypto_recheck(context: ContextTypes.DEFAULT_TYPE):
    await wc.background_auto_recheck(
        context,
        auto_scan_callable_from_record,
        deposit_complete_async=finalize_auto_deposit_record,
        order_complete_async=finalize_auto_order_record,
        send_pending_message_async=send_auto_pending_message,
        send_rejected_message_async=send_auto_rejected_message,
        send_completed_message_async=send_auto_completed_message,
    )


async def background_job(context: ContextTypes.DEFAULT_TYPE):
    await background_crypto_recheck(context)


# =========================
# CRYPTOMUS INTEGRATION
# =========================
CRYPTOMUS_PAYMENTS = {}
CRYPTOMUS_PROCESSED = set()

CRYPTOMUS_COIN_MAP = {
    "USDT (TRC20)": {"to_currency": "USDT", "network": "tron"},
    "USDT (ERC20)": {"to_currency": "USDT", "network": "eth"},
    "USDT (BEP20)": {"to_currency": "USDT", "network": "bsc"},
    "TRX (TRC20)": {"to_currency": "TRX", "network": "tron"},
    "BTC": {"to_currency": "BTC"},
    "LTC": {"to_currency": "LTC"},
    "ETH (ERC20)": {"to_currency": "ETH", "network": "eth"},
    "BNB (BEP20)": {"to_currency": "BNB", "network": "bsc"},
    "SOL": {"to_currency": "SOL", "network": "sol"},
}


def cryptomus_callback_url() -> str:
    if not PUBLIC_BASE_URL:
        return ""
    return f"{PUBLIC_BASE_URL}{CRYPTOMUS_WEBHOOK_PATH}"


def save_cryptomus_pending():
    try:
        save_bot_state()
    except Exception as e:
        print("⚠️ Failed to save Cryptomus pending:", e)


def create_cryptomus_payment(user_id: int, kind: str, usd_amount: float, network_label: str, ref: str, product_id=None, qty=None) -> dict:
    if cm is None:
        raise RuntimeError("cryptomus.py is missing or could not be imported.")
    if not CRYPTOMUS_MERCHANT_ID:
        raise RuntimeError("CRYPTOMUS_MERCHANT_ID is not set in Railway Variables.")
    if not CRYPTOMUS_PAYMENT_API_KEY:
        raise RuntimeError("CRYPTOMUS_PAYMENT_API_KEY is not set in Railway Variables.")
    if network_label not in CRYPTOMUS_COIN_MAP:
        raise RuntimeError(f"Unsupported Cryptomus network: {network_label}")

    # Cryptomus order_id allows alpha, numbers, underscores, dashes only.
    order_id = f"{kind}_{user_id}_{int(datetime.utcnow().timestamp())}_{random.randint(1000, 9999)}"
    base_amount = Decimal(str(usd_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    invoice_amount = (base_amount * (Decimal("100") + CRYPTOMUS_CLIENT_MARKUP_PERCENT) / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_UP)
    amount_text = f"{invoice_amount:.2f}"
    coin_cfg = CRYPTOMUS_COIN_MAP[network_label]
    payload = {
        "amount": amount_text,
        "currency": "USD",
        "order_id": order_id,
        "url_callback": cryptomus_callback_url(),
        "is_payment_multiple": False,
        "lifetime": 3600,
        "accuracy_payment_percent": str(CRYPTOMUS_ACCURACY_PERCENT),
        "course_source": CRYPTOMUS_COURSE_SOURCE,
        "subtract": CRYPTOMUS_SUBTRACT_PERCENT,
        "additional_data": f"{kind}:{user_id}:{ref}"[:255],
    }
    payload.update(coin_cfg)

    data = cm.create_invoice(payload, CRYPTOMUS_MERCHANT_ID, CRYPTOMUS_PAYMENT_API_KEY)
    result = data.get("result") or data
    uuid = str(result.get("uuid") or "")
    if not uuid:
        raise RuntimeError(f"Cryptomus did not return uuid: {data}")

    record = {
        "provider": "Cryptomus",
        "uuid": uuid,
        "payment_id": uuid,
        "order_id": order_id,
        "kind": kind,
        "user_id": user_id,
        "usd_amount": float(usd_amount),
        "invoice_amount": float(invoice_amount),
        "client_markup_percent": float(CRYPTOMUS_CLIENT_MARKUP_PERCENT),
        "network": network_label,
        "to_currency": coin_cfg.get("to_currency"),
        "network_code": coin_cfg.get("network"),
        "pay_currency": result.get("payer_currency") or coin_cfg.get("to_currency"),
        "pay_amount": result.get("payer_amount") or result.get("payment_amount"),
        "pay_address": result.get("address"),
        "payment_url": result.get("url"),
        "payment_status": result.get("payment_status") or result.get("status") or "check",
        "product_id": product_id,
        "qty": qty,
        "created_at": result.get("created_at") or datetime.utcnow().isoformat(),
        "raw": data,
    }
    CRYPTOMUS_PAYMENTS[uuid] = record
    save_cryptomus_pending()
    return record


def render_cryptomus_payment_text(record: dict) -> str:
    network = record.get("network", "")
    pay_amount = record.get("pay_amount")
    pay_currency = str(record.get("pay_currency") or record.get("to_currency") or "").upper()
    pay_address = record.get("pay_address") or "Open payment link below"
    payment_url = record.get("payment_url") or ""
    usd_amount = float(record.get("usd_amount", 0))

    amount_line = "Open payment link to see exact amount"
    if pay_amount:
        amount_line = f"<code>{escape_html(str(pay_amount))} {escape_html(pay_currency)}</code>"

    text = (
        "✅ <b>PAYMENT REQUEST GENERATED!</b>\n\n"
        f"💵 <b>Amount:</b>\n${usd_amount:.2f}\n\n"
        f"🪙 <b>Amount to send:</b>\n{amount_line}\n\n"
        f"🌐 <b>Network:</b>\n{escape_html(network)}\n\n"
        f"🏦 <b>Payment Address:</b>\n<code>{escape_html(str(pay_address))}</code>\n\n"
    )
    # Keep the payment processor private from customers: do not show the hosted payment link.
    # Users can pay directly to the generated address and exact amount shown above.
    text += (
        "⚠️ <b>IMPORTANT:</b>\n"
        "Send <b>EXACTLY</b> the amount shown above. Do not round the amount.\n"
        "If you send a different amount, the bot may not verify your payment automatically.\n"
        "After payment, tap <b>I Have Paid (Verify)</b>."
    )
    return text


def get_cryptomus_status(record: dict) -> dict:
    if cm is None:
        raise RuntimeError("cryptomus.py is missing or could not be imported.")
    return cm.payment_info(record.get("uuid"), record.get("order_id"), CRYPTOMUS_MERCHANT_ID, CRYPTOMUS_PAYMENT_API_KEY)


def is_cryptomus_success_status(status: str) -> bool:
    return str(status or "").lower() in {"paid", "paid_over"}


def is_cryptomus_failed_status(status: str) -> bool:
    return str(status or "").lower() in {"fail", "wrong_amount", "cancel", "system_fail", "refund_fail", "refund_paid"}


def find_latest_cryptomus_record(user_id: int, kind: str = None):
    records = []
    for rec in CRYPTOMUS_PAYMENTS.values():
        if int(rec.get("user_id", 0)) != int(user_id):
            continue
        if kind and rec.get("kind") != kind:
            continue
        if str(rec.get("uuid")) in CRYPTOMUS_PROCESSED:
            continue
        records.append(rec)
    if not records:
        return None
    return records[-1]


async def finalize_cryptomus_record(record: dict, payload: dict = None):
    uuid = str(record.get("uuid") or record.get("payment_id"))
    if uuid in CRYPTOMUS_PROCESSED:
        return

    user_id = int(record["user_id"])
    amount = float(record["usd_amount"])
    kind = record.get("kind")
    txid = ""
    if payload:
        txid = str(payload.get("txid") or payload.get("uuid") or uuid)
    else:
        txid = uuid

    if kind == "deposit":
        user_wallet[user_id] = user_wallet.get(user_id, 0.0) + amount
        add_transaction_record(
            user_id,
            "Deposit",
            amount,
            "Completed",
            {"provider": "Cryptomus", "payment_id": uuid, "txid": txid},
        )
        await app_instance.bot.send_message(
            chat_id=user_id,
            text=(
                "🎉 <b>PAYMENT VERIFIED!</b>\n\n"
                f"<b>{format_money(amount)}</b> added to your wallet.\n"
                f"{get_wallet_balance_text(user_id)}"
            ),
            parse_mode="HTML",
        )

    elif kind == "order":
        product_id = record.get("product_id")
        qty = int(record.get("qty") or 1)
        ok, delivered = await deliver_accounts_to_user(app_instance.bot, user_id, product_id, qty)
        if ok:
            add_order_record(
                user_id, product_id, qty, amount, "Completed", "Cryptomus", delivered_items=delivered
            )
            await notify_admin_order(app_instance.bot, user_id, product_id, qty, amount, "Cryptomus")
            add_transaction_record(
                user_id,
                "Order Payment",
                amount,
                "Completed",
                {"provider": "Cryptomus", "payment_id": uuid, "product_id": product_id, "qty": qty, "txid": txid},
            )
            await app_instance.bot.send_message(
                chat_id=user_id,
                text=(
                    "🎉 <b>PAYMENT VERIFIED!</b>\n\n"
                    f"<b>Order completed</b> for {PRODUCTS[product_id]['name']}.\n"
                    f"<b>Quantity:</b> {qty}\n"
                    f"<b>Total:</b> {format_money(amount)}"
                ),
                parse_mode="HTML",
            )
        else:
            await app_instance.bot.send_message(
                chat_id=user_id,
                text="✅ Payment received, but stock delivery failed. Please contact live support.",
                parse_mode="HTML",
            )

    CRYPTOMUS_PROCESSED.add(uuid)
    record["payment_status"] = "completed"
    record["completed_at"] = datetime.utcnow().isoformat()
    save_cryptomus_pending()
    save_bot_state()


async def handle_cryptomus_webhook(payload: dict):
    uuid = str(payload.get("uuid") or "")
    order_id = str(payload.get("order_id") or "")
    status = str(payload.get("status") or payload.get("payment_status") or "").lower()

    record = CRYPTOMUS_PAYMENTS.get(uuid)
    if not record and order_id:
        for rec in CRYPTOMUS_PAYMENTS.values():
            if str(rec.get("order_id")) == order_id:
                record = rec
                break

    if not record:
        print("⚠️ Cryptomus webhook received for unknown payment:", payload)
        return

    record["payment_status"] = status
    record["last_webhook"] = payload
    save_cryptomus_pending()

    if is_cryptomus_success_status(status):
        await finalize_cryptomus_record(record, payload)
    elif is_cryptomus_failed_status(status):
        user_id = int(record.get("user_id", 0))
        await app_instance.bot.send_message(
            chat_id=user_id,
            text=f"❌ <b>Payment failed/cancelled.</b>\n\nStatus: <code>{escape_html(status)}</code>",
            parse_mode="HTML",
        )


async def run_cryptomus_manual_verify(query, user_id: int, kind: str):
    state = user_state.setdefault(user_id, {})
    if state.get("verify_in_progress"):
        await query.answer("⏳ Verification already in progress. Please wait.", show_alert=False)
        return

    record = find_latest_cryptomus_record(user_id, kind)
    if not record:
        await query.message.reply_text("❌ No pending payment found.")
        return

    state["verify_in_progress"] = True
    try:
        await query.message.reply_text("⏳ Checking payment status...")
        status_payload = get_cryptomus_status(record)
        result = status_payload.get("result") or status_payload
        status = str(result.get("payment_status") or result.get("status") or "").lower()
        record["payment_status"] = status
        record["last_status_check"] = status_payload
        save_cryptomus_pending()

        if is_cryptomus_success_status(status):
            await finalize_cryptomus_record(record, result)
        elif is_cryptomus_failed_status(status):
            await query.message.reply_text(
                f"❌ Payment status: {status}\n\nPlease create a new payment request or contact live support: {SUPPORT_USERNAME}"
            )
        else:
            await query.message.reply_text(
                "⌛ Payment not confirmed yet.\n\n"
                "Please wait 1–2 minutes and tap Verify again.\n"
                f"If it still fails after 10–15 minutes, contact live support: {SUPPORT_USERNAME}"
            )
    except Exception as e:
        print("Payment manual verify error:", e)
        await query.message.reply_text(
            f"❌ Could not check payment right now.\n\nPlease contact live support: {SUPPORT_USERNAME}"
        )
    finally:
        state["verify_in_progress"] = False


def create_gateway_payment(user_id: int, kind: str, usd_amount: float, network_label: str, ref: str, product_id=None, qty=None) -> dict:
    if PAYMENT_GATEWAY == "cryptomus":
        return create_cryptomus_payment(user_id, kind, usd_amount, network_label, ref, product_id=product_id, qty=qty)
    return create_nowpayments_payment(user_id, kind, usd_amount, network_label, ref, product_id=product_id, qty=qty)


def render_gateway_payment_text(record: dict) -> str:
    if str(record.get("provider") or "").lower() == "cryptomus":
        return render_cryptomus_payment_text(record)
    return render_nowpayments_payment_text(record)


async def run_gateway_manual_verify(query, user_id: int, kind: str):
    if PAYMENT_GATEWAY == "cryptomus":
        await run_cryptomus_manual_verify(query, user_id, kind)
    else:
        await run_nowpayments_manual_verify(query, user_id, kind)


# NOWPAYMENTS INTEGRATION
# =========================
app_loop = None
webhook_server_started = False
NOWPAYMENTS_PENDING_FILE = "nowpayments_pending.json"
NOWPAYMENTS_PAYMENTS = {}
NOWPAYMENTS_PROCESSED = set()

NOWPAYMENTS_CURRENCY_MAP = {
    "USDT (TRC20)": "usdttrc20",
    "USDT (ERC20)": "usdterc20",
    "USDT (BEP20)": "usdtbsc",
    "TRX (TRC20)": "trx",
    "BTC": "btc",
    "LTC": "ltc",
    "ETH (ERC20)": "eth",
    "BNB (BEP20)": "bnbbsc",
    "SOL": "sol",
}


def nowpayments_callback_url() -> str:
    if not PUBLIC_BASE_URL:
        return ""
    return f"{PUBLIC_BASE_URL}{NOWPAYMENTS_WEBHOOK_PATH}"


def nowpayments_headers() -> dict:
    return {
        "x-api-key": NOWPAYMENTS_API_KEY,
        "Content-Type": "application/json",
    }


def stable_unique_usd_amount(base_amount: float, user_id: int, ref: str = "") -> Decimal:
    base = Decimal(str(base_amount)).quantize(Decimal("0.01"))
    fee_multiplier = Decimal("1") + (NOWPAYMENTS_FEE_MARGIN_PERCENT / Decimal("100"))
    with_fee = (base * fee_multiplier).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    max_cents = max(1, min(99, NOWPAYMENTS_UNIQUE_SUFFIX_MAX_CENTS))
    seed = f"{user_id}:{ref}".encode("utf-8")
    cents = (int(hashlib.sha256(seed).hexdigest(), 16) % max_cents) + 1
    return (with_fee + (Decimal(cents) / Decimal("100"))).quantize(Decimal("0.01"))


def save_nowpayments_pending():
    try:
        if USE_DATABASE:
            save_bot_state()
            return
        data = {
            "payments": NOWPAYMENTS_PAYMENTS,
            "processed": list(NOWPAYMENTS_PROCESSED),
        }
        with open(NOWPAYMENTS_PENDING_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("⚠️ Failed to save NOWPayments pending:", e)


def load_nowpayments_pending():
    global NOWPAYMENTS_PAYMENTS, NOWPAYMENTS_PROCESSED
    try:
        if USE_DATABASE:
            # NOWPayments pending records are loaded together with the main database state.
            return
        if not os.path.exists(NOWPAYMENTS_PENDING_FILE):
            return
        with open(NOWPAYMENTS_PENDING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        NOWPAYMENTS_PAYMENTS = data.get("payments", {}) or {}
        NOWPAYMENTS_PROCESSED = set(data.get("processed", []) or [])
        print(f"✅ Loaded NOWPayments pending: {len(NOWPAYMENTS_PAYMENTS)} payments")
    except Exception as e:
        print("⚠️ Failed to load NOWPayments pending:", e)


def sort_object_for_ipn(obj):
    if isinstance(obj, dict):
        return {k: sort_object_for_ipn(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [sort_object_for_ipn(x) for x in obj]
    return obj


def verify_nowpayments_signature(payload: dict, signature: str) -> bool:
    if not NOWPAYMENTS_IPN_SECRET:
        print("⚠️ NOWPAYMENTS_IPN_SECRET missing; rejecting IPN")
        return False
    if not signature:
        return False
    sorted_payload = sort_object_for_ipn(payload)
    body = json.dumps(sorted_payload, separators=(",", ":"), ensure_ascii=False)
    expected = hmac.new(
        NOWPAYMENTS_IPN_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def create_nowpayments_payment(user_id: int, kind: str, usd_amount: float, network_label: str, ref: str, product_id=None, qty=None) -> dict:
    if not NOWPAYMENTS_API_KEY:
        raise RuntimeError("NOWPAYMENTS_API_KEY is not set in Railway Variables.")
    if network_label not in NOWPAYMENTS_CURRENCY_MAP:
        raise RuntimeError(f"Unsupported NOWPayments network: {network_label}")

    price_amount = stable_unique_usd_amount(usd_amount, user_id, ref)
    order_id = f"{kind}:{user_id}:{int(datetime.utcnow().timestamp())}:{random.randint(1000, 9999)}"
    payload = {
        "price_amount": float(price_amount),
        "price_currency": "usd",
        "pay_currency": NOWPAYMENTS_CURRENCY_MAP[network_label],
        "order_id": order_id,
        "order_description": f"Supreme Leader Shop {kind} for user {user_id}",
    }
    callback_url = nowpayments_callback_url()
    if callback_url:
        payload["ipn_callback_url"] = callback_url

    res = requests.post(
        f"{NOWPAYMENTS_API_BASE}/payment",
        headers=nowpayments_headers(),
        json=payload,
        timeout=30,
    )

    try:
        data = res.json()
    except Exception:
        data = {"raw": res.text[:1000]}

    if not res.ok:
        raise RuntimeError(f"NOWPayments create payment failed: {data}")

    payment_id = str(data.get("payment_id") or "")
    if not payment_id:
        raise RuntimeError(f"NOWPayments did not return payment_id: {data}")

    record = {
        "provider": "NOWPayments",
        "payment_id": payment_id,
        "order_id": order_id,
        "kind": kind,
        "user_id": user_id,
        "usd_amount": float(usd_amount),
        "price_amount": float(price_amount),
        "network": network_label,
        "pay_currency": data.get("pay_currency") or payload["pay_currency"],
        "pay_amount": data.get("pay_amount"),
        "pay_address": data.get("pay_address"),
        "payment_status": data.get("payment_status", "waiting"),
        "product_id": product_id,
        "qty": qty,
        "created_at": data.get("created_at") or datetime.utcnow().isoformat(),
        "raw": data,
    }
    NOWPAYMENTS_PAYMENTS[payment_id] = record
    save_nowpayments_pending()
    return record


def format_nowpayments_display_amount(value, pay_currency: str = "") -> str:
    """
    User-facing display only.
    Backend keeps NOWPayments original pay_amount/payment_id for verification.
    """
    try:
        from decimal import Decimal, ROUND_UP

        amount = Decimal(str(value))
        cur = str(pay_currency or "").upper()

        if cur.startswith("USDT") or cur in {"USDC", "BUSD", "DAI"}:
            decimals = 2
        elif cur == "BTC":
            decimals = 8
        elif cur == "LTC":
            decimals = 6
        elif cur in {"SOL", "TRX", "ETH", "BNB"} or cur.startswith("BNB"):
            decimals = 6
        else:
            decimals = 6

        q = Decimal("1." + ("0" * decimals))
        formatted = amount.quantize(q, rounding=ROUND_UP)

        if decimals > 2:
            return format(formatted.normalize(), "f")

        return f"{formatted:.2f}"
    except Exception:
        return str(value)

def render_nowpayments_payment_text(record: dict) -> str:
    network = record.get("network", "")
    pay_amount = record.get("pay_amount")
    pay_currency = str(record.get("pay_currency") or "").upper()
    pay_amount_display = format_nowpayments_display_amount(pay_amount, pay_currency)
    pay_address = record.get("pay_address") or "Not generated"
    usd_amount = float(record.get("usd_amount", 0))
    price_amount = float(record.get("price_amount", usd_amount))

    return (
        "✅ <b>PAYMENT REQUEST GENERATED!</b>\n\n"
        f"💵 <b>Amount:</b>\n${usd_amount:.2f}\n\n"
        f"🪙 <b>Amount to send:</b>\n<code>{pay_amount_display} {pay_currency}</code>\n\n"
        f"🌐 <b>Network:</b>\n{network}\n\n"
        f"🏦 <b>Payment Address:</b>\n<code>{escape_html(str(pay_address))}</code>\n\n"
        "⚠️ <b>IMPORTANT:</b>\n"
        "Send <b>EXACTLY</b> the amount shown above. Do not round the amount.\n"
        "If you send a different amount, the bot may not verify your payment automatically.\n\n"
        "After payment, tap <b>I Have Paid (Verify)</b>."
    )


def get_nowpayments_status(payment_id: str) -> dict:
    res = requests.get(
        f"{NOWPAYMENTS_API_BASE}/payment/{payment_id}",
        headers=nowpayments_headers(),
        timeout=30,
    )
    try:
        return res.json()
    except Exception:
        return {"error": res.text[:1000], "status_code": res.status_code}


def is_nowpayments_success_status(status: str) -> bool:
    return str(status or "").lower() in {"confirmed", "finished", "sending"}


def is_nowpayments_failed_status(status: str) -> bool:
    return str(status or "").lower() in {"failed", "refunded", "expired"}


def find_latest_nowpayments_record(user_id: int, kind: str = None):
    records = []
    for rec in NOWPAYMENTS_PAYMENTS.values():
        if int(rec.get("user_id", 0)) != int(user_id):
            continue
        if kind and rec.get("kind") != kind:
            continue
        if str(rec.get("payment_id")) in NOWPAYMENTS_PROCESSED:
            continue
        records.append(rec)
    if not records:
        return None
    return records[-1]


async def finalize_nowpayments_record(record: dict, payload: dict = None):
    payment_id = str(record.get("payment_id"))
    if payment_id in NOWPAYMENTS_PROCESSED:
        return

    user_id = int(record["user_id"])
    amount = float(record["usd_amount"])
    kind = record.get("kind")
    txid = ""
    if payload:
        txid = str(payload.get("purchase_id") or payload.get("outcome_hash") or payload.get("payin_hash") or payload.get("payment_id") or payment_id)
    else:
        txid = payment_id

    if kind == "deposit":
        user_wallet[user_id] = user_wallet.get(user_id, 0.0) + amount
        add_transaction_record(
            user_id,
            "Deposit",
            amount,
            "Completed",
            {"provider": "NOWPayments", "payment_id": payment_id, "txid": txid},
        )
        await app_instance.bot.send_message(
            chat_id=user_id,
            text=(
                "🎉 <b>PAYMENT VERIFIED!</b>\n\n"
                f"<b>{format_money(amount)}</b> added to your wallet.\n"
                f"{get_wallet_balance_text(user_id)}"
            ),
            parse_mode="HTML",
        )

    elif kind == "order":
        product_id = record.get("product_id")
        qty = int(record.get("qty") or 1)
        ok, delivered = await deliver_accounts_to_user(app_instance.bot, user_id, product_id, qty)
        if ok:
            add_order_record(
                user_id, product_id, qty, amount, "Completed", "NOWPayments", delivered_items=delivered
            )
            await notify_admin_order(app_instance.bot, user_id, product_id, qty, amount, "NOWPayments")
            add_transaction_record(
                user_id,
                "Order Payment",
                amount,
                "Completed",
                {"provider": "NOWPayments", "payment_id": payment_id, "product_id": product_id, "qty": qty, "txid": txid},
            )
            await app_instance.bot.send_message(
                chat_id=user_id,
                text=(
                    "🎉 <b>PAYMENT VERIFIED!</b>\n\n"
                    f"<b>Order completed</b> for {PRODUCTS[product_id]['name']}.\n"
                    f"<b>Quantity:</b> {qty}\n"
                    f"<b>Total:</b> {format_money(amount)}"
                ),
                parse_mode="HTML",
            )
        else:
            await app_instance.bot.send_message(
                chat_id=user_id,
                text="✅ Payment received, but stock delivery failed. Please contact live support.",
                parse_mode="HTML",
            )

    NOWPAYMENTS_PROCESSED.add(payment_id)
    record["payment_status"] = "completed"
    record["completed_at"] = datetime.utcnow().isoformat()
    save_nowpayments_pending()
    save_bot_state()


async def handle_nowpayments_ipn(payload: dict):
    payment_id = str(payload.get("payment_id") or "")
    order_id = str(payload.get("order_id") or "")
    status = str(payload.get("payment_status") or "").lower()

    record = NOWPAYMENTS_PAYMENTS.get(payment_id)
    if not record and order_id:
        for rec in NOWPAYMENTS_PAYMENTS.values():
            if str(rec.get("order_id")) == order_id:
                record = rec
                break

    if not record:
        print("⚠️ NOWPayments IPN received for unknown payment:", payload)
        return

    record["payment_status"] = status
    record["last_ipn"] = payload
    save_nowpayments_pending()

    if is_nowpayments_success_status(status):
        await finalize_nowpayments_record(record, payload)
    elif is_nowpayments_failed_status(status):
        user_id = int(record.get("user_id", 0))
        await app_instance.bot.send_message(
            chat_id=user_id,
            text=f"❌ <b>Payment failed or expired.</b>\n\nStatus: <code>{escape_html(status)}</code>",
            parse_mode="HTML",
        )


async def run_nowpayments_manual_verify(query, user_id: int, kind: str):
    state = user_state.setdefault(user_id, {})
    if state.get("verify_in_progress"):
        await query.answer("⏳ Verification already in progress. Please wait.", show_alert=False)
        return

    record = find_latest_nowpayments_record(user_id, kind)
    if not record:
        await query.message.reply_text("❌ No pending NOWPayments payment found.")
        return

    state["verify_in_progress"] = True
    try:
        await query.message.reply_text("⏳ Checking NOWPayments payment status...")
        status_payload = get_nowpayments_status(str(record["payment_id"]))
        status = str(status_payload.get("payment_status") or "").lower()
        record["payment_status"] = status
        record["last_status_check"] = status_payload
        save_nowpayments_pending()

        if is_nowpayments_success_status(status):
            await finalize_nowpayments_record(record, status_payload)
        elif is_nowpayments_failed_status(status):
            await query.message.reply_text(
                f"❌ Payment status: {status}\n\nPlease create a new payment request or contact live support: {SUPPORT_USERNAME}"
            )
        else:
            await query.message.reply_text(
                "⌛ Payment not confirmed yet.\n\n"
                "Please wait 1–2 minutes and tap Verify again.\n"
                f"If it still fails after 10–15 minutes, contact live support: {SUPPORT_USERNAME}"
            )
    except Exception as e:
        print("NOWPayments manual verify error:", e)
        await query.message.reply_text(
            f"❌ Could not check payment right now.\n\nPlease contact live support: {SUPPORT_USERNAME}"
        )
    finally:
        state["verify_in_progress"] = False



def _public_page_html(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 860px; margin: 40px auto; padding: 0 18px; line-height: 1.6; color: #111; }}
    h1 {{ margin-bottom: 8px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 14px; padding: 20px; background: #fff; }}
    a {{ color: #0b65c2; }}
    .muted {{ color: #555; }}
  </style>
</head>
<body>
  <div class="card">
    {body}
  </div>
</body>
</html>"""


def public_home_html() -> str:
    return _public_page_html(
        "Supreme Leader Shop",
        f"""
        <h1>Supreme Leader Shop</h1>
        <p class="muted">Digital products and subscription shop operated through Telegram.</p>
        <p><b>Open Telegram Bot:</b> <a href="https://t.me/{BOT_USERNAME}">https://t.me/{BOT_USERNAME}</a></p>
        <p><b>Support:</b> <a href="{SUPPORT_URL}">{SUPPORT_USERNAME}</a></p>
        <p><a href="/terms">Terms of Use</a> | <a href="/privacy">Privacy Policy</a> | <a href="/legal">Legal Information</a></p>
        """
    )


def public_support_html() -> str:
    return _public_page_html(
        "Support - Supreme Leader Shop",
        f"""
        <h1>Support</h1>
        <p>For payment, order, delivery, warranty, or account issues, contact our Telegram support.</p>
        <p><b>Support:</b> <a href="{SUPPORT_URL}">{SUPPORT_USERNAME}</a></p>
        <p>Please include your Telegram User ID, order details, and payment information when contacting support.</p>
        <p><a href="/">Home</a></p>
        """
    )


def public_terms_html() -> str:
    return _public_page_html(
        "Terms of Use - Supreme Leader Shop",
        f"""
        <h1>Terms of Use</h1>
        <p>Supreme Leader Shop provides digital products and subscription access items through a Telegram bot.</p>
        <p>By using the bot, users agree to check product details, price, stock, delivery method, and warranty conditions before purchase.</p>
        <p>All digital products are delivered through the bot after payment confirmation. Because products are digital, refunds or replacements are handled according to the product warranty and support review.</p>
        <p>Users are responsible for using purchased digital products in accordance with applicable laws and any third-party service rules.</p>
        <p>Abuse, fraud, chargeback attempts, duplicate payment claims, or misuse of the bot may result in account restrictions.</p>
        <p><b>Support:</b> <a href="{SUPPORT_URL}">{SUPPORT_USERNAME}</a></p>
        <p><a href="/">Home</a> | <a href="/privacy">Privacy Policy</a> | <a href="/legal">Legal Information</a></p>
        """
    )


def public_privacy_html() -> str:
    return _public_page_html(
        "Privacy Policy - Supreme Leader Shop",
        f"""
        <h1>Privacy Policy</h1>
        <p>The bot stores information needed to operate the shop, including Telegram user ID, username, wallet balance, orders, transactions, product deliveries, and support-related data.</p>
        <p>Payment-related information may be processed by third-party payment providers. We do not sell user personal data.</p>
        <p>Data is used to provide order delivery, wallet balance tracking, fraud prevention, support, and transaction history.</p>
        <p>Users can contact support for questions about their data or account.</p>
        <p><b>Support:</b> <a href="{SUPPORT_URL}">{SUPPORT_USERNAME}</a></p>
        <p><a href="/">Home</a> | <a href="/terms">Terms of Use</a> | <a href="/legal">Legal Information</a></p>
        """
    )


def public_legal_html() -> str:
    return _public_page_html(
        "Legal Information - Supreme Leader Shop",
        f"""
        <h1>Legal Information</h1>
        <p>Supreme Leader Shop is a Telegram-based digital products shop. Customers purchase digital products and subscription access items through the bot.</p>
        <p>Customers are responsible for ensuring their use of purchased products follows applicable laws and third-party service terms.</p>
        <p>For legal, compliance, or support inquiries, contact the shop support account below.</p>
        <p><b>Support:</b> <a href="{SUPPORT_URL}">{SUPPORT_USERNAME}</a></p>
        <p><a href="/">Home</a> | <a href="/terms">Terms of Use</a> | <a href="/privacy">Privacy Policy</a></p>
        """
    )


class NowPaymentsWebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _send_json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, code: int, body_text: str, content_type: str = "text/plain; charset=utf-8"):
        body = body_text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        clean_path = self.path.split("?")[0]
        if clean_path == "/cryptomus_d7a0b5fb.html":
            self._send_text(200, "cryptomus=d7a0b5fb", "text/html; charset=utf-8")
        elif clean_path == "/":
            self._send_text(200, public_home_html(), "text/html; charset=utf-8")
        elif clean_path == "/support":
            self._send_text(200, public_support_html(), "text/html; charset=utf-8")
        elif clean_path == "/terms":
            self._send_text(200, public_terms_html(), "text/html; charset=utf-8")
        elif clean_path == "/privacy":
            self._send_text(200, public_privacy_html(), "text/html; charset=utf-8")
        elif clean_path == "/legal":
            self._send_text(200, public_legal_html(), "text/html; charset=utf-8")
        elif clean_path == NOWPAYMENTS_WEBHOOK_PATH:
            self._send_json(200, {"ok": True, "service": "telegram-shop-bot"})
        else:
            self._send_json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        clean_path = self.path.split("?")[0]
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0") or "0"))
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            self._send_json(400, {"ok": False, "error": "invalid json"})
            return

        if clean_path == CRYPTOMUS_WEBHOOK_PATH:
            if cm is None or not cm.verify_webhook_signature(payload, CRYPTOMUS_PAYMENT_API_KEY):
                self._send_json(401, {"ok": False, "error": "invalid cryptomus signature"})
                return
            if app_loop is None:
                self._send_json(503, {"ok": False, "error": "bot loop not ready"})
                return
            try:
                asyncio.run_coroutine_threadsafe(handle_cryptomus_webhook(payload), app_loop)
                self._send_json(200, {"ok": True})
            except Exception as e:
                print("Cryptomus webhook schedule error:", e)
                self._send_json(500, {"ok": False, "error": "internal error"})
            return

        if clean_path == NOWPAYMENTS_WEBHOOK_PATH:
            signature = self.headers.get("x-nowpayments-sig", "")
            if not verify_nowpayments_signature(payload, signature):
                self._send_json(401, {"ok": False, "error": "invalid signature"})
                return
            if app_loop is None:
                self._send_json(503, {"ok": False, "error": "bot loop not ready"})
                return
            try:
                asyncio.run_coroutine_threadsafe(handle_nowpayments_ipn(payload), app_loop)
                self._send_json(200, {"ok": True})
            except Exception as e:
                print("NOWPayments webhook schedule error:", e)
                self._send_json(500, {"ok": False, "error": "internal error"})
            return

        self._send_json(404, {"ok": False, "error": "not found"})


def start_nowpayments_webhook_server():
    global webhook_server_started
    if webhook_server_started:
        return
    webhook_server_started = True
    port = int(os.getenv("PORT", "8080"))

    def run_server():
        server = HTTPServer(("0.0.0.0", port), NowPaymentsWebhookHandler)
        print(f"✅ Payment webhook server listening on port {port}, NOWPayments {NOWPAYMENTS_WEBHOOK_PATH}, Cryptomus {CRYPTOMUS_WEBHOOK_PATH}")
        server.serve_forever()

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()


async def post_init(application):
    global app_loop
    app_loop = asyncio.get_running_loop()
    load_bot_state()
    await application.bot.set_my_commands([
        BotCommand("start", "Start bot"),
        BotCommand("shop", "Open shop"),
        BotCommand("wallet", "Check wallet"),
        BotCommand("topup", "Add balance"),
        BotCommand("orders", "My orders"),
        BotCommand("support", "Contact support"),
        BotCommand("myid", "Show user ID"),
    ])
    load_nowpayments_pending()
    start_nowpayments_webhook_server()


# =========================
# COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await ensure_channel_access(update, context):
        return
    ensure_user(user_id, update.effective_user)
    enter_client_mode(user_id)
    await send_user_dashboard(update.message)


async def client_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    command = update.message.text.split()[0].split("@")[0].lower()

    if command in {"/shop", "/wallet", "/topup"} and not await ensure_channel_access(update, context):
        return

    ensure_user(user_id, update.effective_user)
    enter_client_mode(user_id)
    if command == "/shop":
        user_state[user_id] = {"step": "shop"}
        await send_shop_cards_message(update.message, from_callback=False)
    elif command == "/wallet":
        await send_client_main_text(update, render_wallet_text(user_id))
    elif command == "/topup":
        user_state[user_id] = {"step": "deposit_amount"}
        await send_user_inline_from_text_with_style_fallback(
            update,
            render_deposit_text(),
            deposit_amount_keyboard(),
            deposit_amount_keyboard(styled=False),
        )
    elif command == "/orders":
        text, page, _ = render_user_orders_page(user_id)
        await send_inline_from_text(update, text, user_orders_keyboard(user_id, page))
    elif command == "/support":
        await send_client_main_text(update, render_support_text())


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id, update.effective_user)
    if not is_admin(user_id):
        await update.message.reply_text("❌ <b>You are not allowed to open admin panel.</b>", parse_mode="HTML")
        return
    enter_admin_mode(user_id)
    await send_admin_main_text(update, "🛠 <b>ADMIN MODE ON</b>\n\nBottom menu now switched to admin menu.")


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Your Telegram ID: {update.effective_user.id}")


async def checkchannel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not allowed to use this command.")
        return

    channel_id = _required_channel_chat_id()
    lines = [
        "🔎 <b>CHANNEL CONFIG CHECK</b>",
        "",
        f"<b>REQUIRED_CHANNEL_ENABLED:</b> <code>{escape_html(REQUIRED_CHANNEL_ENABLED)}</code>",
        f"<b>REQUIRED_CHANNEL_ID:</b> <code>{escape_html(REQUIRED_CHANNEL_ID)}</code>",
        f"<b>REQUIRED_CHANNEL_URL:</b> <code>{escape_html(REQUIRED_CHANNEL_URL)}</code>",
    ]
    try:
        chat = await context.bot.get_chat(chat_id=channel_id)
        lines.extend([
            "",
            "<b>get_chat result:</b> ✅ Success",
            f"<b>Channel title:</b> {escape_html(getattr(chat, 'title', '') or 'N/A')}",
            f"<b>Numeric chat ID:</b> <code>{getattr(chat, 'id', 'N/A')}</code>",
        ])
    except Exception as e:
        lines.extend([
            "",
            "<b>get_chat result:</b> ❌ Failed",
            f"<b>Error:</b> <code>{escape_html(type(e).__name__)}: {escape_html(str(e))}</code>",
        ])
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)


async def checkmember_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    if not is_admin(admin_id):
        await update.message.reply_text("❌ You are not allowed to use this command.")
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /checkmember USER_ID")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ USER_ID must be numeric.")
        return

    channel_id = _required_channel_chat_id()
    try:
        checked_channel_id = channel_id
        try:
            member = await context.bot.get_chat_member(chat_id=checked_channel_id, user_id=target_user_id)
        except Exception:
            if not isinstance(channel_id, str) or not channel_id.startswith("@"):
                raise
            channel = await context.bot.get_chat(chat_id=channel_id)
            checked_channel_id = int(channel.id)
            member = await context.bot.get_chat_member(chat_id=checked_channel_id, user_id=target_user_id)
        raw_status = getattr(member, "status", "")
        normalized_status = _normalize_chat_member_status(raw_status)
        is_member = getattr(member, "is_member", None)
        passed = normalized_status in {"member", "administrator", "creator"} or (
            normalized_status == "restricted" and bool(is_member)
        )
        await update.message.reply_text(
            "🔎 <b>CHANNEL MEMBER CHECK</b>\n\n"
            f"<b>Configured channel:</b> <code>{escape_html(str(channel_id))}</code>\n"
            f"<b>Checked channel ID:</b> <code>{escape_html(str(checked_channel_id))}</code>\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n"
            f"<b>Raw status:</b> <code>{escape_html(repr(raw_status))}</code>\n"
            f"<b>Normalized status:</b> <code>{escape_html(normalized_status)}</code>\n"
            f"<b>is_member:</b> <code>{escape_html(repr(is_member))}</code>\n"
            f"<b>Final result:</b> {'✅ PASS' if passed else '❌ FAIL'}",
            parse_mode="HTML",
        )
    except Exception as e:
        await update.message.reply_text(
            "❌ <b>CHANNEL MEMBER CHECK FAILED</b>\n\n"
            f"<b>Channel:</b> <code>{escape_html(str(channel_id))}</code>\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n"
            f"<b>Error:</b> <code>{escape_html(type(e).__name__)}: {escape_html(str(e))}</code>\n\n"
            "Make sure the bot is an admin in REQUIRED_CHANNEL_ID.",
            parse_mode="HTML",
        )


async def addstock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id, update.effective_user)
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not allowed to use this command.")
        return
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /addstock p3 5")
        return

    product_id = context.args[0]
    qty_text = context.args[1]

    if product_id not in PRODUCTS:
        await update.message.reply_text("❌ Invalid product id. Example: p1, p2, p3")
        return
    try:
        qty = int(qty_text)
    except ValueError:
        await update.message.reply_text("❌ Quantity must be a number.")
        return
    if qty <= 0:
        await update.message.reply_text("❌ Quantity must be greater than 0.")
        return

    for _ in range(qty):
        index = len(PRODUCTS[product_id]["accounts"]) + 1
        PRODUCTS[product_id]["accounts"].append(
            {"email": f"{product_id}_auto_{index}@example.com", "password": "Pass1234", "note": "Added by admin"}
        )

    PRODUCTS[product_id]["display_stock"] = max(get_display_stock(product_id), get_product_stock(product_id))
    product = PRODUCTS[product_id]

    await update.message.reply_text(
        f"✅ Stock added.\n\nProduct: {product['name']}\nAdded: {qty}\nCurrent Real Stock: {get_product_stock(product_id)} pcs\nDisplay Stock: {get_display_stock(product_id)} pcs"
    )
    await notify_waiters_for_product(context, product_id)


async def handle_category_icon_message(update: Update, user_id: int, step: str) -> bool:
    message = update.effective_message
    category_icon, category_custom_emoji_id, icon_error = _extract_supported_icon_from_message(message)
    if icon_error:
        await message.reply_text(icon_error, parse_mode="HTML")
        return True

    if step == "category_add_icon":
        category_name = admin_temp.get(user_id, {}).get("category_name")
        if not category_name:
            await message.reply_text("❌ <b>Category setup expired.</b> Please start Add Category again.", parse_mode="HTML")
            return True
        category_id = generate_new_category_id()
        CATEGORIES[category_id] = {
            "id": category_id,
            "name": category_name,
            "icon": category_icon,
            "icon_custom_emoji_id": category_custom_emoji_id,
            "order": len(category_order),
        }
        _normalize_category_icon_fields(CATEGORIES[category_id])
        category_order.append(category_id)
        shop_order.append(f"category:{category_id}")
        confirmation_prefix = "✅ <b>Category added.</b>\n\n"
        confirmation_suffix = f"\nID: <code>{escape_html(category_id)}</code>"
    else:
        category_id = admin_temp.get(user_id, {}).get("selected_category_id")
        if category_id not in CATEGORIES:
            reset_admin_temp(user_id)
            await message.reply_text("❌ <b>Category was not found.</b>", reply_markup=admin_categories_keyboard(), parse_mode="HTML")
            return True
        CATEGORIES[category_id]["icon"] = category_icon
        CATEGORIES[category_id]["icon_custom_emoji_id"] = category_custom_emoji_id
        _normalize_category_icon_fields(CATEGORIES[category_id])
        confirmation_prefix = "✅ "
        confirmation_suffix = ""

    custom_captured = bool(_category_custom_emoji_id(CATEGORIES[category_id]))
    icon_result = "Category custom emoji icon updated." if custom_captured else "Category normal emoji icon updated."
    print(f"Category icon update: category_id={category_id}, custom_emoji_captured={custom_captured}")
    reset_admin_temp(user_id)
    user_state[user_id] = {"step": "admin_categories"}
    await message.reply_text(
        f"{confirmation_prefix}<b>{icon_result}</b>{confirmation_suffix}",
        reply_markup=admin_categories_keyboard(),
        parse_mode="HTML",
    )
    return True


async def handle_dashboard_emoji_message(update: Update, user_id: int) -> bool:
    if not is_admin(user_id):
        return False

    message = update.effective_message
    dashboard_key = admin_temp.get(user_id, {}).get("selected_dashboard_emoji_key")
    header_key = admin_temp.get(user_id, {}).get("selected_dashboard_header_emoji_key")
    if dashboard_key in DASHBOARD_BUTTONS:
        target_ids = dashboard_custom_emoji_ids
        target_label = DASHBOARD_BUTTONS[dashboard_key]["text"]
        target_kind = "button"
    elif header_key in DASHBOARD_HEADER_EMOJIS:
        dashboard_key = header_key
        target_ids = dashboard_header_custom_emoji_ids
        target_label = DASHBOARD_HEADER_EMOJIS[header_key]["label"]
        target_kind = "header"
    else:
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "dashboard_emoji_admin"}
        await message.reply_text(
            "❌ <b>Dashboard emoji setup expired.</b> Please select an icon again.",
            reply_markup=dashboard_emoji_admin_keyboard(),
            parse_mode="HTML",
        )
        return True

    _, custom_emoji_id, _ = _extract_supported_icon_from_message(message)
    custom_emoji_id = str(custom_emoji_id or "").strip()
    if not custom_emoji_id:
        await message.reply_text(
            "Please send a Telegram custom/animated emoji, not a normal emoji.",
            reply_markup=dashboard_emoji_input_keyboard(),
        )
        return True

    target_ids[dashboard_key] = custom_emoji_id
    print(f"Dashboard emoji update: type={target_kind}, key={dashboard_key}, custom_emoji_captured=True")
    reset_admin_temp(user_id)
    user_state[user_id] = {"step": "dashboard_emoji_admin"}
    await message.reply_text(
        f"✅ <b>{escape_html(target_label)} custom emoji updated.</b>\n\n"
        f"{render_dashboard_emoji_admin_text()}",
        reply_markup=dashboard_emoji_admin_keyboard(),
        parse_mode="HTML",
    )
    return True


# =========================
# TEXT HANDLER
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id, update.effective_user)
    text = update.message.text.strip()
    state = user_state[user_id]
    step = state.get("step", "main")

    if user_mode[user_id] != "admin" and text == "Menu":
        enter_client_mode(user_id)
        await send_user_dashboard(update.message)
        return

    # ========= ADMIN MAIN MENUS =========
    if user_mode[user_id] == "admin":
        if text == "🚪 Exit Admin":
            enter_client_mode(user_id)
            await send_user_dashboard(update.message, "✅ <b>Admin mode off.</b>")
            return

        if text == "📦 Products":
            user_state[user_id] = {"step": "admin_products"}
            await update.message.reply_text(render_admin_products_text(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Choose a products action below.", reply_markup=admin_products_keyboard(), parse_mode="HTML")
            return

        if text == "🗂 Categories":
            user_state[user_id] = {"step": "admin_categories"}
            reset_admin_temp(user_id)
            await update.message.reply_text(render_admin_categories_text(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Category actions:", reply_markup=admin_categories_keyboard(), parse_mode="HTML")
            return

        if text == "📥 Stock":
            user_state[user_id] = {"step": "admin_stock"}
            await update.message.reply_text("📥 <b>STOCK MANAGEMENT</b>\n\nChoose an option below.", reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Stock actions:", reply_markup=admin_stock_keyboard(), parse_mode="HTML")
            return

        if text == "🎟 Promo Admin":
            user_state[user_id] = {"step": "promo_admin"}
            await update.message.reply_text("🎟 <b>PROMO ADMIN</b>\n\nChoose an option below.", reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Promo actions:", reply_markup=admin_promo_keyboard(), parse_mode="HTML")
            return

        if text == "📦 Orders Admin":
            user_state[user_id] = {"step": "orders_admin"}
            await update.message.reply_text("📦 <b>ORDERS ADMIN</b>\n\nChoose an option below.", reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Orders actions:", reply_markup=admin_orders_keyboard(), parse_mode="HTML")
            return

        if text == "💳 Deposits Admin":
            user_state[user_id] = {"step": "deposits_admin"}
            await update.message.reply_text("💳 <b>DEPOSITS ADMIN</b>\n\nChoose an option below.", reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Deposits actions:", reply_markup=deposits_admin_keyboard(), parse_mode="HTML")
            return

        if text == "👤 Users Admin":
            user_state[user_id] = {"step": "users_admin"}
            await update.message.reply_text(render_users_admin(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Users actions:", reply_markup=users_admin_keyboard(), parse_mode="HTML")
            return

        if text == "💰 User Balance":
            user_state[user_id] = {"step": "balance_admin"}
            reset_admin_temp(user_id)
            await update.message.reply_text(render_admin_balance_panel(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Balance actions:", reply_markup=user_balance_admin_keyboard(), parse_mode="HTML")
            return

        if text == "🔔 Notify Requests":
            user_state[user_id] = {"step": "notify_requests_admin"}
            reset_admin_temp(user_id)
            await update.message.reply_text(render_admin_notify_panel(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Notify actions:", reply_markup=admin_notify_keyboard(), parse_mode="HTML")
            return

        if text == "👑 Gold VIP":
            user_state[user_id] = {"step": "gold_vip_admin"}
            reset_admin_temp(user_id)
            await update.message.reply_text(render_gold_vip_panel(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Gold VIP actions:", reply_markup=gold_vip_admin_keyboard(), parse_mode="HTML")
            return

        if text == "⚡ Flash Deal":
            user_state[user_id] = {"step": "flash_deal_admin"}
            reset_admin_temp(user_id)
            await update.message.reply_text(render_flash_deal_panel(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Flash Deal actions:", reply_markup=flash_deal_admin_keyboard(), parse_mode="HTML")
            return

        if text == "👥 User Details":
            user_state[user_id] = {"step": "users_admin"}
            details_text, page, total_pages = render_user_details_page(0)
            await update.message.reply_text(details_text, reply_markup=users_details_keyboard(page, total_pages), parse_mode="HTML", disable_web_page_preview=True)
            return

        if text == "📊 Analytics":
            await update.message.reply_text(render_analytics(), reply_markup=admin_menu(), parse_mode="HTML")
            return

        if text == "🎨 Dashboard Emojis":
            reset_admin_temp(user_id)
            user_state[user_id] = {"step": "dashboard_emoji_admin"}
            await update.message.reply_text(
                "🎨 <b>DASHBOARD EMOJI SETTINGS</b>",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            await update.message.reply_text(
                render_dashboard_emoji_admin_text(),
                reply_markup=dashboard_emoji_admin_keyboard(),
                parse_mode="HTML",
            )
            return

        if text == "🔌 Seller API":
            user_state[user_id] = {"step": "seller_api_admin"}
            await update.message.reply_text(
                "🔌 <b>SELLER API TEST PANEL</b>",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            await update.message.reply_text(
                render_seller_api_panel(),
                reply_markup=seller_api_keyboard(),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return

        if text == "📢 Broadcast":
            user_state[user_id] = {"step": "admin_broadcast_input"}
            await update.message.reply_text(
                "📢 <b>BROADCAST MESSAGE</b>\n\n"
                f"Saved users: <b>{len(all_users)}</b>\n\n"
                "Send the message you want to broadcast.\n"
                "It will be previewed before sending.",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

    if step in {"dashboard_emoji_input", "dashboard_header_emoji_input"}:
        await handle_dashboard_emoji_message(update, user_id)
        return

    if step == "admin_broadcast_input":
        broadcast_text = update.message.text or ""
        if len(broadcast_text) > 4096:
            await update.message.reply_text("❌ Broadcast message is too long. Keep it under 4096 characters.", reply_markup=admin_menu())
            return
        if not broadcast_text.strip():
            await update.message.reply_text("❌ Broadcast message cannot be empty.", reply_markup=admin_menu())
            return
        broadcast_entities = serialize_message_entities(update.message.entities)
        admin_temp[user_id]["broadcast_text"] = broadcast_text
        admin_temp[user_id]["broadcast_entities"] = broadcast_entities
        user_state[user_id] = {"step": "admin_broadcast_confirm"}
        await update.message.reply_text(
            "📢 <b>Broadcast Preview</b>\n\n"
            f"<b>Users:</b> {max(0, len(all_users) - 1)}\n\n"
            "<b>Destination:</b> Users + Channel\n\n"
            "The message below will be sent with its Telegram formatting and emoji entities.",
            parse_mode="HTML",
        )
        await send_rich_broadcast_message(
            context.bot,
            update.effective_chat.id,
            broadcast_text,
            broadcast_entities,
            reply_markup=broadcast_confirm_keyboard(),
        )
        return

    # ========= CATEGORY MANAGEMENT =========
    if step == "category_add_name":
        category_name = text.strip()
        if not category_name:
            await update.message.reply_text("❌ <b>Category name cannot be empty.</b>", parse_mode="HTML")
            return
        if len(category_name) > 60:
            await update.message.reply_text("❌ <b>Category name is too long.</b> Keep it under 60 characters.", parse_mode="HTML")
            return
        admin_temp[user_id]["category_name"] = category_name
        user_state[user_id] = {"step": "category_add_icon"}
        await update.message.reply_text(
            "😀 <b>Add Category</b>\n\nSend one normal or Telegram custom emoji for this category.\nExample: 📁",
            reply_markup=admin_cancel_keyboard(),
            parse_mode="HTML",
        )
        return

    if step == "category_add_icon":
        await handle_category_icon_message(update, user_id, step)
        return

    if step == "category_rename_input":
        category_name = text.strip()
        category_id = admin_temp.get(user_id, {}).get("selected_category_id")
        if category_id not in CATEGORIES:
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ <b>Category was not found.</b>", reply_markup=admin_categories_keyboard(), parse_mode="HTML")
            return
        if not category_name or len(category_name) > 60:
            await update.message.reply_text("❌ <b>Send a category name between 1 and 60 characters.</b>", parse_mode="HTML")
            return
        CATEGORIES[category_id]["name"] = category_name
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_categories"}
        await update.message.reply_text("✅ <b>Category renamed.</b>", reply_markup=admin_categories_keyboard(), parse_mode="HTML")
        return

    if step == "category_icon_input":
        await handle_category_icon_message(update, user_id, step)
        return

    # ========= PRODUCT ADD =========
    if step == "admin_add_product_icon":
        icon, icon_custom_emoji_id, icon_error = _extract_supported_icon_from_message(update.message)
        if icon_error:
            await update.message.reply_text(icon_error, parse_mode="HTML")
            return
        admin_temp[user_id]["icon"] = icon
        admin_temp[user_id]["icon_custom_emoji_id"] = icon_custom_emoji_id
        user_state[user_id] = {"step": "admin_add_product_name"}
        await update.message.reply_text("🆕 <b>Add Product</b>\n\nNow send product name.", reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "admin_add_product_name":
        admin_temp[user_id]["name"] = text
        user_state[user_id] = {"step": "admin_add_product_month"}
        await update.message.reply_text("🆕 <b>Add Product</b>\n\nNow send product month.", reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "admin_add_product_month":
        admin_temp[user_id]["month"] = text
        user_state[user_id] = {"step": "admin_add_product_price"}
        await update.message.reply_text("🆕 <b>Add Product</b>\n\nNow send product price.", reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "admin_add_product_price":
        try:
            price = float(text)
            if price <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid price.</b> Send a valid number.", parse_mode="HTML")
            return
        admin_temp[user_id]["price"] = price
        user_state[user_id] = {"step": "admin_add_product_display_stock"}
        await update.message.reply_text("🆕 <b>Add Product</b>\n\nNow send display stock number.", reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "admin_add_product_display_stock":
        try:
            display_stock = int(text)
            if display_stock < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid display stock.</b> Send 0 or more.", parse_mode="HTML")
            return
        admin_temp[user_id]["display_stock"] = display_stock
        user_state[user_id] = {"step": "admin_add_product_details"}
        await update.message.reply_text(
            "🆕 <b>Add Product</b>\n\nNow send product details line by line.\n\nExample:\n✅ Private Account\n✅ Auto Delivery",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "admin_add_product_details":
        details = [line.strip() for line in text.splitlines() if line.strip()]
        if not details:
            await update.message.reply_text("❌ <b>Please send at least one detail line.</b>", parse_mode="HTML")
            return
        admin_temp[user_id]["details"] = details
        user_state[user_id] = {"step": "admin_add_product_confirm"}
        await update.message.reply_text(render_admin_add_product_preview(user_id), reply_markup=admin_confirm_add_product_keyboard(), parse_mode="HTML")
        return

    # ========= PRODUCT EDIT =========
    if step == "admin_edit_name_input":
        new_name = text.strip()
        if not new_name:
            await update.message.reply_text("❌ <b>Name cannot be empty.</b>", parse_mode="HTML")
            return
        admin_temp[user_id]["new_name"] = new_name
        user_state[user_id] = {"step": "admin_edit_name_confirm"}
        await update.message.reply_text(
            render_admin_edit_name_preview(admin_temp[user_id]["selected_product_id"], new_name),
            reply_markup=admin_confirm_keyboard("admin_confirm_name_update", "✅ Confirm Name"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_price_input":
        try:
            new_price = float(text)
            if new_price <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid price.</b> Send a valid number.", parse_mode="HTML")
            return
        admin_temp[user_id]["new_price"] = new_price
        user_state[user_id] = {"step": "admin_edit_price_confirm"}
        await update.message.reply_text(
            render_admin_edit_price_preview(admin_temp[user_id]["selected_product_id"], new_price),
            reply_markup=admin_confirm_keyboard("admin_confirm_price_update", "✅ Confirm Price"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_month_input":
        new_month = text.strip()
        if not new_month:
            await update.message.reply_text("❌ <b>Month cannot be empty.</b>", parse_mode="HTML")
            return
        admin_temp[user_id]["new_month"] = new_month
        user_state[user_id] = {"step": "admin_edit_month_confirm"}
        await update.message.reply_text(
            render_admin_edit_month_preview(admin_temp[user_id]["selected_product_id"], new_month),
            reply_markup=admin_confirm_keyboard("admin_confirm_month_update", "✅ Confirm Month"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_details_input":
        details = [line.strip() for line in text.splitlines() if line.strip()]
        if not details:
            await update.message.reply_text("❌ <b>Please send at least one detail line.</b>", parse_mode="HTML")
            return
        admin_temp[user_id]["new_details"] = details
        details_entities = serialize_message_entities(update.message.entities)
        admin_temp[user_id]["new_details_rich"] = (
            {"text": update.message.text or "", "entities": details_entities} if details_entities else None
        )
        user_state[user_id] = {"step": "admin_edit_details_confirm"}
        await update.message.reply_text(
            render_admin_edit_details_preview(
                admin_temp[user_id]["selected_product_id"], details, admin_temp[user_id]["new_details_rich"]
            ),
            reply_markup=admin_confirm_keyboard("admin_confirm_details_update", "✅ Confirm Details"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_delivery_guide_input":
        new_guide_text = update.message.text or ""
        new_guide = new_guide_text.strip()
        if not new_guide:
            await update.message.reply_text("❌ <b>Delivery guide cannot be empty.</b>", parse_mode="HTML")
            return
        admin_temp[user_id]["new_delivery_guide"] = new_guide
        guide_entities = serialize_message_entities(update.message.entities)
        admin_temp[user_id]["new_delivery_guide_rich"] = (
            {"text": new_guide_text, "entities": guide_entities} if guide_entities else None
        )
        user_state[user_id] = {"step": "admin_edit_delivery_guide_confirm"}
        await update.message.reply_text(
            render_admin_edit_delivery_guide_preview(admin_temp[user_id]["selected_product_id"], new_guide),
            reply_markup=admin_confirm_keyboard("admin_confirm_delivery_guide_update", "✅ Confirm Delivery Guide"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_icon_input":
        new_icon, new_icon_custom_emoji_id, icon_error = _extract_supported_icon_from_message(update.message)
        if icon_error:
            await update.message.reply_text(icon_error, parse_mode="HTML")
            return
        admin_temp[user_id]["new_icon"] = new_icon
        admin_temp[user_id]["new_icon_custom_emoji_id"] = new_icon_custom_emoji_id
        user_state[user_id] = {"step": "admin_edit_icon_confirm"}
        await update.message.reply_text(
            render_admin_edit_icon_preview(admin_temp[user_id]["selected_product_id"], new_icon, new_icon_custom_emoji_id),
            reply_markup=admin_confirm_keyboard("admin_confirm_icon_update", "✅ Confirm Icon"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_display_stock_input":
        try:
            new_display_stock = int(text)
            if new_display_stock < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid display stock.</b> Send 0 or more.", parse_mode="HTML")
            return
        admin_temp[user_id]["new_display_stock"] = new_display_stock
        user_state[user_id] = {"step": "admin_edit_display_stock_confirm"}
        await update.message.reply_text(
            render_admin_edit_display_stock_preview(admin_temp[user_id]["selected_product_id"], new_display_stock),
            reply_markup=admin_confirm_keyboard("admin_confirm_display_stock_update", "✅ Confirm Display Stock"),
            parse_mode="HTML",
        )
        return

    if step == "admin_bulk_min_qty_input":
        min_qty = safe_decimal(text.strip())
        if (
            min_qty is None
            or not min_qty.is_finite()
            or min_qty != min_qty.to_integral_value()
            or min_qty < 2
        ):
            await update.message.reply_text(
                "❌ <b>Invalid minimum quantity.</b> Send an integer of 2 or more.",
                parse_mode="HTML",
            )
            return
        admin_temp[user_id]["bulk_min_qty"] = int(min_qty)
        user_state[user_id] = {"step": "admin_bulk_unit_price_input"}
        await update.message.reply_text(
            f"💸 Minimum quantity: <b>{int(min_qty)}+</b>\n\nNow send the unit price.",
            parse_mode="HTML",
        )
        return

    if step == "admin_bulk_unit_price_input":
        unit_price = safe_decimal(text.strip())
        if unit_price is None or not unit_price.is_finite() or unit_price <= 0:
            await update.message.reply_text(
                "❌ <b>Invalid unit price.</b> Send a number greater than 0.",
                parse_mode="HTML",
            )
            return
        product_id = admin_temp[user_id].get("selected_product_id")
        min_qty = admin_temp[user_id].get("bulk_min_qty")
        if product_id not in PRODUCTS or not isinstance(min_qty, int) or min_qty < 2:
            reset_admin_temp(user_id)
            user_state[user_id] = {"step": "admin_products"}
            await update.message.reply_text(
                "❌ <b>Bulk pricing setup expired.</b> Please start again.",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return
        tiers_by_quantity = {
            tier["min_qty"]: tier for tier in get_bulk_pricing_tiers(PRODUCTS[product_id])
        }
        tiers_by_quantity[min_qty] = {
            "min_qty": min_qty,
            "unit_price": float(unit_price),
        }
        PRODUCTS[product_id]["bulk_pricing"] = [
            tiers_by_quantity[quantity] for quantity in sorted(tiers_by_quantity)
        ]
        admin_temp[user_id].pop("bulk_min_qty", None)
        user_state[user_id] = {"step": "admin_bulk_pricing"}
        await update.message.reply_text(
            "✅ <b>Bulk pricing tier saved.</b>\n\n" + render_bulk_pricing_admin(product_id),
            reply_markup=bulk_pricing_admin_keyboard(product_id),
            parse_mode="HTML",
        )
        return

    # ========= STOCK INPUTS =========
    if step == "stock_add_single_input":
        product_id = admin_temp[user_id].get("selected_product_id")
        if not product_id or product_id not in PRODUCTS:
            user_state[user_id] = {"step": "admin_stock"}
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ Product not found.", reply_markup=admin_menu())
            return
        account = parse_account_line(text)
        if not account:
            await update.message.reply_text("❌ <b>Invalid stock.</b>\n\nSend one link/code/text, or use old format:\n<code>email@gmail.com|password123|Private Account</code>", parse_mode="HTML")
            return
        PRODUCTS[product_id]["accounts"].append(account)
        PRODUCTS[product_id]["display_stock"] = max(get_display_stock(product_id), get_product_stock(product_id))
        user_state[user_id] = {"step": "admin_stock"}
        reset_admin_temp(user_id)
        await update.message.reply_text(
            f"✅ <b>Single account added successfully.</b>\n\n<b>Product:</b> {PRODUCTS[product_id]['name']}\n<b>Real Stock:</b> {get_product_stock(product_id)} pcs\n<b>Display Stock:</b> {get_display_stock(product_id)} pcs",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        await notify_waiters_for_product(context, product_id)
        return

    if step == "stock_add_bulk_input":
        product_id = admin_temp[user_id].get("selected_product_id")
        if not product_id or product_id not in PRODUCTS:
            user_state[user_id] = {"step": "admin_stock"}
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ Product not found.", reply_markup=admin_menu())
            return
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        added = 0
        for line in lines:
            account = parse_account_line(line)
            if account:
                PRODUCTS[product_id]["accounts"].append(account)
                added += 1
        if added == 0:
            await update.message.reply_text("❌ <b>No valid stock line found.</b>\n\nSend links/codes/text line by line, or use old format:\n<code>email@gmail.com|password123|Private Account</code>", parse_mode="HTML")
            return
        PRODUCTS[product_id]["display_stock"] = max(get_display_stock(product_id), get_product_stock(product_id))
        user_state[user_id] = {"step": "admin_stock"}
        reset_admin_temp(user_id)
        await update.message.reply_text(
            f"✅ <b>Bulk accounts added successfully.</b>\n\n<b>Product:</b> {PRODUCTS[product_id]['name']}\n<b>Added:</b> {added}\n<b>Real Stock:</b> {get_product_stock(product_id)} pcs\n<b>Display Stock:</b> {get_display_stock(product_id)} pcs",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        await notify_waiters_for_product(context, product_id)
        return

    if step == "stock_edit_account_input":
        product_id = admin_temp[user_id].get("selected_product_id")
        account_index = admin_temp[user_id].get("selected_account_index")
        if product_id not in PRODUCTS or account_index is None:
            user_state[user_id] = {"step": "admin_stock"}
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ Product/account not found.", reply_markup=admin_menu())
            return
        account = parse_account_line(text)
        if not account:
            await update.message.reply_text("❌ <b>Invalid stock.</b>\n\nSend one link/code/text, or use old format:\n<code>email@gmail.com|password123|Private Account</code>", parse_mode="HTML")
            return
        PRODUCTS[product_id]["accounts"][account_index] = account
        user_state[user_id] = {"step": "admin_stock"}
        reset_admin_temp(user_id)
        await update.message.reply_text("✅ <b>Account updated successfully.</b>", reply_markup=admin_menu(), parse_mode="HTML")
        return

    if step == "stock_set_display_input":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            user_state[user_id] = {"step": "admin_stock"}
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ Product not found.", reply_markup=admin_menu())
            return
        try:
            new_display_stock = int(text)
            if new_display_stock < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid number.</b> Send 0 or more.", parse_mode="HTML")
            return
        PRODUCTS[product_id]["display_stock"] = new_display_stock
        user_state[user_id] = {"step": "admin_stock"}
        reset_admin_temp(user_id)
        await update.message.reply_text(
            f"✅ <b>Display stock updated.</b>\n\n<b>Product:</b> {PRODUCTS[product_id]['name']}\n<b>Display Stock:</b> {get_display_stock(product_id)} pcs\n<b>Real Stock:</b> {get_product_stock(product_id)} pcs",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        return

    # ========= PROMO INPUTS =========
    if step == "promo_generate_custom_amount":
        try:
            amount = float(text)
            if amount <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid amount.</b>", parse_mode="HTML")
            return
        code = generate_unique_promo_code()
        PROMO_CODES[code] = {
            "amount": amount,
            "enabled": True,
            "one_time": True,
            "created_at": now_dt(),
            "created_by": str(user_id),
            "used_by": None,
            "used_at": None,
        }
        user_state[user_id] = {"step": "promo_admin"}
        reset_admin_temp(user_id)
        await update.message.reply_text(render_generated_promo_text(code), reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Promo actions:", reply_markup=admin_promo_keyboard(), parse_mode="HTML")
        return

    if step == "awaiting_promo":
        promo = text.upper().strip()

        if promo in used_promo_codes[user_id]:
            user_state[user_id] = {"step": "main"}
            await send_client_main_text(update, "❌ <b>This promo code has already been used.</b>")
            return

        if promo in PROMO_CODES:
            info = PROMO_CODES[promo]

            if not info.get("enabled", True):
                user_state[user_id] = {"step": "main"}
                await send_client_main_text(update, "❌ <b>This promo code is disabled.</b>")
                return

            amount = float(info["amount"])
            user_wallet[user_id] += amount
            used_promo_codes[user_id].add(promo)
            add_transaction_record(user_id, "Promo Bonus", amount, "Completed", {"promo_code": promo})

            info["used_by"] = user_id
            info["used_at"] = now_dt()

            if info.get("one_time", False):
                PROMO_CODES.pop(promo, None)

            user_state[user_id] = {"step": "main"}
            await send_client_main_text(update, f"✅ <b>Promo applied successfully.</b>\n\n{format_money(amount)} added to your wallet.\n{get_wallet_balance_text(user_id)}")
            return

        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, "❌ <b>Invalid promo code.</b>")
        return

    # ========= ADMIN SEARCH INPUTS =========
    if step == "orders_user_search_input":
        try:
            target_user_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid User ID.</b>", parse_mode="HTML")
            return
        user_state[user_id] = {"step": "orders_admin"}
        await update.message.reply_text(get_user_search_summary_text(target_user_id), reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Orders actions:", reply_markup=admin_orders_keyboard(), parse_mode="HTML")
        return

    if step == "deposits_user_search_input":
        try:
            target_user_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid User ID.</b>", parse_mode="HTML")
            return
        user_state[user_id] = {"step": "deposits_admin"}
        await update.message.reply_text(get_user_search_summary_text(target_user_id), reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Deposits actions:", reply_markup=deposits_admin_keyboard(), parse_mode="HTML")
        return

    if step == "balance_user_id_input":
        try:
            target_user_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid User ID.</b> Send numeric Telegram user ID.", parse_mode="HTML")
            return

        admin_temp[user_id]["target_user_id"] = target_user_id
        action = admin_temp[user_id].get("balance_action")

        if action == "check":
            user_state[user_id] = {"step": "balance_admin"}
            await update.message.reply_text(render_balance_check_text(target_user_id), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Balance actions:", reply_markup=user_balance_admin_keyboard(), parse_mode="HTML")
            return

        user_state[user_id] = {"step": "balance_amount_input"}
        current_balance = float(user_wallet.get(target_user_id, 0.0))
        await update.message.reply_text(
            "💰 <b>User Balance Update</b>\n\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n"
            f"<b>Current Balance:</b> {format_money(current_balance)}\n\n"
            "Now send amount. Example: <code>5</code>",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "balance_amount_input":
        amount = safe_decimal(text)
        if amount is None or amount <= 0:
            await update.message.reply_text("❌ <b>Invalid amount.</b> Send a valid number like <code>5</code> or <code>2.50</code>.", parse_mode="HTML")
            return

        target_user_id = int(admin_temp[user_id]["target_user_id"])
        action = admin_temp[user_id].get("balance_action")
        amount_float = float(amount)

        if action == "minus" and float(user_wallet.get(target_user_id, 0.0)) < amount_float:
            await update.message.reply_text(
                "❌ <b>Insufficient balance.</b>\n\n"
                f"Current balance: {format_money(user_wallet.get(target_user_id, 0.0))}\n"
                f"Minus amount: {format_money(amount_float)}",
                parse_mode="HTML",
            )
            return

        admin_temp[user_id]["balance_amount"] = amount_float
        user_state[user_id] = {"step": "balance_reason_input"}
        await update.message.reply_text(
            "📝 <b>Reason / Note</b>\n\n"
            "Send a short reason for this wallet update.\n"
            "Example: <code>Manual service delivered</code>",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        await update.message.reply_text("Cancel if needed.", reply_markup=admin_cancel_keyboard(), parse_mode="HTML")
        return

    if step == "balance_reason_input":
        reason = text.strip()
        if not reason:
            await update.message.reply_text("❌ <b>Reason cannot be empty.</b>", parse_mode="HTML")
            return
        if len(reason) > 300:
            await update.message.reply_text("❌ <b>Reason too long.</b> Keep it under 300 characters.", parse_mode="HTML")
            return

        admin_temp[user_id]["balance_reason"] = reason
        user_state[user_id] = {"step": "balance_confirm"}
        await update.message.reply_text(
            render_balance_adjust_preview(user_id),
            reply_markup=balance_adjust_confirm_keyboard(),
            parse_mode="HTML",
        )
        return

    if step == "vip_user_id_input":
        try:
            target_user_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid User ID.</b> Send numeric Telegram user ID.", parse_mode="HTML")
            return
        action = admin_temp[user_id].get("vip_action")
        ensure_user(target_user_id)
        if action == "add":
            gold_vip_users.add(target_user_id)
            msg = "✅ <b>User added to Gold VIP.</b>"
            await send_gold_vip_welcome(context.bot, target_user_id)
        else:
            gold_vip_users.discard(target_user_id)
            msg = "✅ <b>User removed from Gold VIP.</b>"
        user_state[user_id] = {"step": "gold_vip_admin"}
        reset_admin_temp(user_id)
        await update.message.reply_text(
            f"{msg}\n\n<b>User ID:</b> <code>{target_user_id}</code>",
            reply_markup=admin_menu(),
            parse_mode="HTML",
        )
        await update.message.reply_text("Gold VIP actions:", reply_markup=gold_vip_admin_keyboard(), parse_mode="HTML")
        return

    if step == "flash_price_input":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            user_state[user_id] = {"step": "flash_deal_admin"}
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ Product not found.", reply_markup=admin_menu())
            return
        try:
            amount = float(text)
            if amount <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid deal price.</b> Send a valid number like <code>4</code> or <code>3.50</code>.", parse_mode="HTML")
            return
        admin_temp[user_id]["flash_deal_price"] = float(amount)
        user_state[user_id] = {"step": "flash_duration_input"}
        await update.message.reply_text("⏳ <b>Send deal duration in minutes.</b>\n\nExample: <code>10</code>", reply_markup=admin_menu(), parse_mode="HTML")
        return

    if step == "flash_duration_input":
        try:
            minutes = int(text)
            if minutes <= 0 or minutes > 1440:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid duration.</b> Send minutes between 1 and 1440.", parse_mode="HTML")
            return
        admin_temp[user_id]["flash_deal_minutes"] = minutes
        user_state[user_id] = {"step": "flash_confirm"}
        await update.message.reply_text(render_flash_deal_confirm(user_id), reply_markup=flash_deal_confirm_keyboard(), parse_mode="HTML")
        return

    if step == "vip_price_input":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            user_state[user_id] = {"step": "gold_vip_admin"}
            reset_admin_temp(user_id)
            await update.message.reply_text("❌ Product not found.", reply_markup=admin_menu())
            return
        amount = safe_decimal(text)
        if amount is None or amount <= 0:
            await update.message.reply_text("❌ <b>Invalid VIP price.</b> Send a valid number like <code>4</code> or <code>3.50</code>.", parse_mode="HTML")
            return
        admin_temp[user_id]["gold_vip_price"] = float(amount)
        user_state[user_id] = {"step": "vip_price_confirm"}
        await update.message.reply_text(
            render_gold_vip_price_preview(user_id),
            reply_markup=gold_vip_price_confirm_keyboard(),
            parse_mode="HTML",
        )
        return

    if step == "users_search_input":
        try:
            target_user_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid User ID.</b>", parse_mode="HTML")
            return
        user_state[user_id] = {"step": "users_admin"}
        await update.message.reply_text(get_user_search_summary_text(target_user_id), reply_markup=admin_menu(), parse_mode="HTML")
        await update.message.reply_text("Users actions:", reply_markup=users_admin_keyboard(), parse_mode="HTML")
        return

    # ========= MAIN MENU OVERRIDE =========
    client_main_menu_texts = {
        "🛍 Shop",
        "💰 Wallet",
        "💳 Top Up",
        "🎟 Promo",
        "📦 Orders",
        "🆔 User ID",
        "🧾 Transactions",
        "👥 Refer & Earn",
        "💬 Support",
    }

    if user_mode[user_id] != "admin" and text in client_main_menu_texts:
        user_state[user_id] = {"step": "main"}
        reset_admin_temp(user_id)
        state = user_state[user_id]
        step = "main"

    # ========= CLIENT BUY =========
    if step == "buy_custom_qty":
        product_id = state["product_id"]
        stock = get_display_stock(product_id)
        try:
            qty = int(text)
            if qty <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid quantity.</b> Please send a valid number.", parse_mode="HTML")
            return
        if qty > stock:
            await update.message.reply_text(f"❌ <b>Only {stock} pcs available.</b>", parse_mode="HTML")
            return

        try:
            _, total = calculate_order_total(product_id, qty, user_id)
        except ValueError:
            await update.message.reply_text(
                "❌ <b>This product has an invalid price.</b> Please contact support.",
                parse_mode="HTML",
            )
            return
        if user_wallet[user_id] >= total:
            await process_wallet_purchase(update, context, user_id, product_id, qty, total)
            user_state[user_id] = {"step": "main"}
            return

        user_state[user_id] = {"step": "buy_payment_method", "product_id": product_id, "qty": qty, "total": total}
        await update.message.reply_text(render_buy_summary(product_id, qty, user_wallet[user_id], user_id), reply_markup=payment_method_keyboard("buy"), parse_mode="HTML")
        return

    if step == "deposit_custom_amount":
        try:
            amount = float(text)
            if amount <= 0:
                raise ValueError
        except ValueError:
            await send_user_inline_from_text_with_style_fallback(
                update,
                "❌ <b>Invalid amount.</b> Please send a valid number.",
                deposit_custom_amount_keyboard(),
                deposit_custom_amount_keyboard(styled=False),
            )
            return
        user_state[user_id] = {"step": "deposit_payment_method", "amount": amount}
        await send_user_inline_from_text_with_style_fallback(
            update,
            render_deposit_method_text(amount),
            payment_method_keyboard("dep", styled=True),
            payment_method_keyboard("dep", styled=False),
        )
        return

    if step == "awaiting_crypto_txid_deposit":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, "ℹ️ <b>TXID is no longer required.</b>\n\nPlease use the <b>I Have Paid (Verify)</b> button on the payment request.")
        return

    if step == "awaiting_crypto_txid_buy":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, "ℹ️ <b>TXID is no longer required.</b>\n\nPlease use the <b>I Have Paid (Verify)</b> button on the payment request.")
        return

    # ========= NORMAL CLIENT MENUS =========
    if text == "🛍 Shop":
        if not await ensure_channel_access(update, context):
            return
        user_state[user_id] = {"step": "shop"}
        await send_shop_cards_message(update.message, from_callback=False)
        return

    if text == "💰 Wallet":
        if not await ensure_channel_access(update, context):
            return
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_wallet_text(user_id))
        return

    if text == "🆔 User ID":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_user_id_text(user_id))
        return

    if text == "💳 Top Up":
        if not await ensure_channel_access(update, context):
            return
        user_state[user_id] = {"step": "deposit_amount"}
        await send_user_inline_from_text_with_style_fallback(
            update,
            render_deposit_text(),
            deposit_amount_keyboard(),
            deposit_amount_keyboard(styled=False),
        )
        return

    if text == "📦 Orders":
        user_state[user_id] = {"step": "main"}
        orders_text, page, _ = render_user_orders_page(user_id)
        await send_inline_from_text(update, orders_text, user_orders_keyboard(user_id, page))
        return

    if text == "🎟 Promo":
        user_state[user_id] = {"step": "awaiting_promo"}
        await send_client_main_text(update, "🎟 <b>PROMO</b>\n\nPlease send your promo code.")
        return

    if text == "👥 Refer & Earn":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_refer_text(user_id))
        return

    if text == "🧾 Transactions":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_transactions_text(user_id))
        return

    if text == "💬 Support":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_support_text())
        return

    if text == "📜 Terms":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_terms_text())
        return

    if text == "🔐 Privacy":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_privacy_text())
        return

    if text == "⚖️ Legal":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_legal_text())
        return

    await update.message.reply_text(
        "Please use the Menu button below.",
        reply_markup=admin_menu() if user_mode[user_id] == "admin" else main_menu()
    )
# =========================
# CALLBACK HANDLER
# =========================

def estimate_verify_timeout(network: str) -> int:
    if network in {"BTC", "LTC"}:
        return VERIFY_MAX_SECONDS_SLOW
    return VERIFY_MAX_SECONDS_FAST


def build_verify_status_text(network: str, seconds_left: int) -> str:
    mins = max(seconds_left, 0) // 60
    secs = max(seconds_left, 0) % 60
    return (
        "⏳ <b>Payment verification in progress</b>\n\n"
        f"<b>Network:</b> {network}\n"
        "Please wait while the bot checks your payment.\n\n"
        f"⏱ <b>Time left:</b> {mins:02d}:{secs:02d}"
    )



async def run_simple_verify_flow(query, context, user_id: int, record_kind: str):
    state = user_state.setdefault(user_id, {})
    if state.get("verify_in_progress"):
        await query.answer("⏳ Verification already in progress. Please wait.", show_alert=False)
        return

    state["verify_in_progress"] = True
    try:
        await query.message.reply_text("⏳ Checking blockchain network records...")
    except Exception:
        pass

    try:
        result = wc.on_verify_clicked(user_id, auto_scan_callable_from_record)

        if result.get("status") == "confirmed":
            if record_kind == "order":
                record = wc.get_user_pending_order(user_id)
                if record:
                    await finalize_auto_order_record(record)
                total = user_state.get(user_id, {}).get("total", 0)
                await query.message.reply_text(f"🎉 PAYMENT VERIFIED! Amount added: ${float(total):.2f}")
            else:
                record = wc.get_user_pending_deposit(user_id)
                if record:
                    await finalize_auto_deposit_record(record)
                amount = user_state.get(user_id, {}).get("amount", 0)
                await query.message.reply_text(f"🎉 PAYMENT VERIFIED! Amount added: ${float(amount):.2f}")
        else:
            await query.message.reply_text(
                "⌛ Transaction not found on the network yet. Please wait 1–2 minutes and click Verify again.\n\n"
                "If it still fails after 10–15 minutes, please contact live support: @serpstacking"
            )
    finally:
        state["verify_in_progress"] = False


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    data = query.data

    if data == "required_channel_check":
        if await check_required_channel_membership(context, user_id):
            ensure_user(user_id, query.from_user)
            enter_client_mode(user_id)
            await send_user_dashboard(query.message)
        else:
            await query.message.reply_text(
                "📢 Please join our official channel first.",
                reply_markup=required_channel_keyboard(),
            )
        return

    ensure_user(user_id, query.from_user)

    if data == "noop":
        return

    # ========= SELLER API ADMIN (READ-ONLY) =========
    if data.startswith("seller_api_") and not is_admin(user_id):
        await send_inline_from_callback(query, "❌ <b>You are not allowed.</b>", close_keyboard())
        return

    if data == "seller_api_back":
        user_state[user_id] = {"step": "admin_main"}
        await send_inline_from_callback(query, "⬅️ Back to admin menu.", close_keyboard())
        return

    if data == "seller_api_debug":
        await send_inline_from_callback(query, render_seller_api_debug_config(), seller_api_keyboard())
        return

    if data == "seller_api_test":
        try:
            balance_data = await asyncio.to_thread(fetch_buyer_api_balance)
            text = (
                "✅ <b>SELLER API CONNECTION SUCCESSFUL</b>\n\n"
                f"<b>API key:</b> <code>{escape_html(mask_buyer_api_key())}</code>\n\n"
                + render_buyer_api_balance(balance_data, "BALANCE ENDPOINT RESPONSE")
            )
        except BuyerAPIError as error:
            text = f"❌ <b>SELLER API CONNECTION FAILED</b>\n\n{escape_html(str(error))}"
        except Exception as error:
            print(f"Seller API connection test failed: {type(error).__name__}")
            text = "❌ <b>SELLER API CONNECTION FAILED</b>\n\nUnexpected Seller API error."
        await send_inline_from_callback(query, text, seller_api_keyboard())
        return

    if data == "seller_api_balance":
        try:
            balance_data = await asyncio.to_thread(fetch_buyer_api_balance)
            text = render_buyer_api_balance(balance_data)
        except BuyerAPIError as error:
            text = f"❌ <b>API BALANCE CHECK FAILED</b>\n\n{escape_html(str(error))}"
        except Exception as error:
            print(f"Seller API balance check failed: {type(error).__name__}")
            text = "❌ <b>API BALANCE CHECK FAILED</b>\n\nUnexpected Seller API error."
        await send_inline_from_callback(query, text, seller_api_keyboard())
        return

    if data == "seller_api_products":
        try:
            products, total = await asyncio.to_thread(fetch_buyer_api_products)
            text = render_buyer_api_products(products, total)
        except BuyerAPIError as error:
            text = f"❌ <b>API PRODUCT FETCH FAILED</b>\n\n{escape_html(str(error))}"
        except Exception as error:
            print(f"Seller API product fetch failed: {type(error).__name__}")
            text = "❌ <b>API PRODUCT FETCH FAILED</b>\n\nUnexpected Seller API error."
        await send_inline_from_callback(query, text, seller_api_keyboard())
        return

    # ========= DASHBOARD EMOJI ADMIN =========
    if (
        data.startswith("dashboard_emoji_") or data.startswith("dashboard_header_emoji_")
    ) and not is_admin(user_id):
        await send_inline_from_callback(query, "❌ <b>You are not allowed.</b>", close_keyboard())
        return

    if data == "dashboard_emoji_back":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_main"}
        await send_inline_from_callback(query, "⬅️ Back to admin menu.", close_keyboard())
        return

    if data == "dashboard_emoji_panel":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "dashboard_emoji_admin"}
        await send_inline_from_callback(
            query,
            render_dashboard_emoji_admin_text(),
            dashboard_emoji_admin_keyboard(),
        )
        return

    if data == "dashboard_emoji_clear":
        user_state[user_id] = {"step": "dashboard_emoji_admin"}
        await send_inline_from_callback(
            query,
            "🧹 <b>Clear Dashboard Emoji</b>\n\nSelect the dashboard button whose custom emoji you want to clear.",
            dashboard_emoji_clear_keyboard(),
        )
        return

    if data.startswith("dashboard_emoji_clear_"):
        dashboard_key = data.replace("dashboard_emoji_clear_", "", 1)
        if dashboard_key not in DASHBOARD_BUTTONS:
            await send_inline_from_callback(
                query,
                "❌ <b>Dashboard button was not found.</b>",
                dashboard_emoji_admin_keyboard(),
            )
            return
        dashboard_custom_emoji_ids[dashboard_key] = ""
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "dashboard_emoji_admin"}
        print(f"Dashboard emoji update: key={dashboard_key}, custom_emoji_captured=False")
        await send_inline_from_callback(
            query,
            f"✅ <b>{escape_html(DASHBOARD_BUTTONS[dashboard_key]['text'])} custom emoji cleared.</b>\n\n"
            f"{render_dashboard_emoji_admin_text()}",
            dashboard_emoji_admin_keyboard(),
        )
        return

    if data.startswith("dashboard_emoji_pick_"):
        dashboard_key = data.replace("dashboard_emoji_pick_", "", 1)
        if dashboard_key not in DASHBOARD_BUTTONS:
            await send_inline_from_callback(
                query,
                "❌ <b>Dashboard button was not found.</b>",
                dashboard_emoji_admin_keyboard(),
            )
            return
        reset_admin_temp(user_id)
        admin_temp.setdefault(user_id, {})["selected_dashboard_emoji_key"] = dashboard_key
        user_state[user_id] = {"step": "dashboard_emoji_input"}
        await send_inline_from_callback(
            query,
            "Send the custom/animated emoji you want to use for this dashboard button.",
            dashboard_emoji_input_keyboard(),
        )
        return

    if data == "dashboard_header_emoji_clear":
        user_state[user_id] = {"step": "dashboard_emoji_admin"}
        await send_inline_from_callback(
            query,
            "🧹 <b>Clear Header Emoji</b>\n\nSelect the dashboard header icon whose custom emoji you want to clear.",
            dashboard_header_emoji_clear_keyboard(),
        )
        return

    if data.startswith("dashboard_header_emoji_clear_"):
        header_key = data.replace("dashboard_header_emoji_clear_", "", 1)
        if header_key not in DASHBOARD_HEADER_EMOJIS:
            await send_inline_from_callback(
                query,
                "❌ <b>Dashboard header icon was not found.</b>",
                dashboard_emoji_admin_keyboard(),
            )
            return
        dashboard_header_custom_emoji_ids[header_key] = ""
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "dashboard_emoji_admin"}
        print(f"Dashboard emoji update: type=header, key={header_key}, custom_emoji_captured=False")
        await send_inline_from_callback(
            query,
            f"✅ <b>{escape_html(DASHBOARD_HEADER_EMOJIS[header_key]['label'])} custom emoji cleared.</b>\n\n"
            f"{render_dashboard_emoji_admin_text()}",
            dashboard_emoji_admin_keyboard(),
        )
        return

    if data.startswith("dashboard_header_emoji_pick_"):
        header_key = data.replace("dashboard_header_emoji_pick_", "", 1)
        if header_key not in DASHBOARD_HEADER_EMOJIS:
            await send_inline_from_callback(
                query,
                "❌ <b>Dashboard header icon was not found.</b>",
                dashboard_emoji_admin_keyboard(),
            )
            return
        reset_admin_temp(user_id)
        admin_temp.setdefault(user_id, {})["selected_dashboard_header_emoji_key"] = header_key
        user_state[user_id] = {"step": "dashboard_header_emoji_input"}
        await send_inline_from_callback(
            query,
            "Send the custom/animated emoji you want to use for this dashboard header icon.",
            dashboard_emoji_input_keyboard(),
        )
        return

    # ========= CATEGORY ADMIN =========
    if data.startswith("category_") and not is_admin(user_id):
        await send_inline_from_callback(query, "❌ <b>You are not allowed.</b>", close_keyboard())
        return

    if data == "category_close":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_main"}
        await send_inline_from_callback(query, "Closed categories panel.", close_keyboard())
        return

    if data == "category_back":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_categories"}
        await send_inline_from_callback(query, render_admin_categories_text(), admin_categories_keyboard())
        return

    if data == "category_add":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "category_add_name"}
        await send_inline_from_callback(query, "➕ <b>Add Category</b>\n\nSend the category name.", admin_cancel_keyboard())
        return

    if data == "category_view":
        user_state[user_id] = {"step": "admin_categories"}
        await send_inline_from_callback(query, render_admin_categories_list(), admin_categories_keyboard())
        return

    if data == "category_rename_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "category_rename_pick"}
        await send_inline_from_callback(
            query,
            f"✏️ <b>Rename Category</b>\n\nSelect a category. The default {DEFAULT_CATEGORY_NAME} category is kept unchanged.",
            category_select_keyboard("category_rename_pick", include_default=False),
        )
        return

    if data == "category_icon_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "category_icon_pick"}
        await send_inline_from_callback(query, "😀 <b>Edit Category Icon</b>\n\nSelect a category.", category_select_keyboard("category_icon_pick"))
        return

    if data == "category_reorder_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "category_reorder_pick"}
        await send_inline_from_callback(
            query,
            "↕️ <b>Reorder in Shop</b>\n\nSelect a category, then move it above or below products and other folders.",
            category_select_keyboard("category_reorder_pick"),
        )
        return

    if data == "category_move_product_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "category_move_product_pick"}
        await send_inline_from_callback(query, "🗃 <b>Move Product to Category</b>\n\nSelect a product.", category_product_select_keyboard())
        return

    if data == "category_delete_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "category_delete_pick"}
        await send_inline_from_callback(
            query,
            "🗑 <b>Delete Category</b>\n\nSelect a category. Products will be moved to the default category.",
            category_select_keyboard("category_delete_pick", include_default=False),
        )
        return

    if data.startswith("category_rename_pick_"):
        category_id = data.replace("category_rename_pick_", "", 1)
        if category_id == DEFAULT_CATEGORY_ID or category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>The default category cannot be renamed.</b>", admin_categories_keyboard())
            return
        admin_temp[user_id]["selected_category_id"] = category_id
        user_state[user_id] = {"step": "category_rename_input"}
        await send_inline_from_callback(
            query,
            f"✏️ <b>Rename Category</b>\n\nCurrent: <b>{escape_html(CATEGORIES[category_id]['name'])}</b>\n\nSend the new name.",
            admin_cancel_keyboard(),
        )
        return

    if data.startswith("category_icon_pick_"):
        category_id = data.replace("category_icon_pick_", "", 1)
        if category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>Category was not found.</b>", admin_categories_keyboard())
            return
        admin_temp[user_id]["selected_category_id"] = category_id
        user_state[user_id] = {"step": "category_icon_input"}
        await send_inline_from_callback(
            query,
            f"😀 <b>Edit Category Icon</b>\n\n"
            f"Category: {category_icon_html(CATEGORIES[category_id])} <b>{escape_html(CATEGORIES[category_id]['name'])}</b>\n\n"
            "Send one normal or Telegram custom emoji.",
            admin_cancel_keyboard(),
        )
        return

    if data.startswith("category_reorder_pick_"):
        category_id = data.replace("category_reorder_pick_", "", 1)
        if category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>Category was not found.</b>", admin_categories_keyboard())
            return
        await send_inline_from_callback(
            query,
            f"↕️ <b>Reorder Category</b>\n\nSelected: <b>{escape_html(CATEGORIES[category_id]['name'])}</b>",
            category_reorder_selected_keyboard(category_id),
        )
        return

    if data.startswith("category_move_up_"):
        category_id = data.replace("category_move_up_", "", 1)
        if category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>Category was not found.</b>", admin_categories_keyboard())
            return
        move_shop_order_item(f"category:{category_id}", -1)
        await send_inline_from_callback(query, "✅ <b>Category moved up.</b>", category_reorder_selected_keyboard(category_id))
        return

    if data.startswith("category_move_down_"):
        category_id = data.replace("category_move_down_", "", 1)
        if category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>Category was not found.</b>", admin_categories_keyboard())
            return
        move_shop_order_item(f"category:{category_id}", 1)
        await send_inline_from_callback(query, "✅ <b>Category moved down.</b>", category_reorder_selected_keyboard(category_id))
        return

    if data.startswith("category_move_product_"):
        product_id = data.replace("category_move_product_", "", 1)
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product was not found.</b>", admin_categories_keyboard())
            return
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "category_move_target_pick"}
        await send_inline_from_callback(
            query,
            f"🗃 <b>Move Product</b>\n\nProduct: <b>{escape_html(PRODUCTS[product_id]['name'])}</b>\n\nSelect the destination category.",
            category_select_keyboard("category_move_target"),
        )
        return

    if data.startswith("category_move_target_"):
        category_id = data.replace("category_move_target_", "", 1)
        product_id = admin_temp.get(user_id, {}).get("selected_product_id")
        if category_id not in CATEGORIES or product_id not in PRODUCTS:
            reset_admin_temp(user_id)
            await send_inline_from_callback(query, "❌ <b>Product or category was not found.</b>", admin_categories_keyboard())
            return
        PRODUCTS[product_id]["category_id"] = category_id
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_categories"}
        await send_inline_from_callback(query, "✅ <b>Product moved to category.</b>", admin_categories_keyboard())
        return

    if data.startswith("category_delete_pick_"):
        category_id = data.replace("category_delete_pick_", "", 1)
        if category_id == DEFAULT_CATEGORY_ID or category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>The default category cannot be deleted.</b>", admin_categories_keyboard())
            return
        product_count = len(get_category_product_ids(category_id))
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Delete", callback_data=f"category_confirm_delete_{category_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="category_back")],
        ])
        await send_inline_from_callback(
            query,
            f"🗑 <b>Delete Category?</b>\n\n"
            f"Category: <b>{escape_html(CATEGORIES[category_id]['name'])}</b>\n"
            f"Products to move to {DEFAULT_CATEGORY_NAME}: <b>{product_count}</b>\n\n"
            "No products will be deleted.",
            keyboard,
        )
        return

    if data.startswith("category_confirm_delete_"):
        category_id = data.replace("category_confirm_delete_", "", 1)
        if category_id == DEFAULT_CATEGORY_ID or category_id not in CATEGORIES:
            await send_inline_from_callback(query, "❌ <b>The default category cannot be deleted.</b>", admin_categories_keyboard())
            return
        moved_count = 0
        for product in PRODUCTS.values():
            if product.get("category_id") == category_id:
                product["category_id"] = DEFAULT_CATEGORY_ID
                moved_count += 1
        CATEGORIES.pop(category_id, None)
        if category_id in category_order:
            category_order.remove(category_id)
        category_item = f"category:{category_id}"
        if category_item in shop_order:
            shop_order.remove(category_item)
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_categories"}
        await send_inline_from_callback(
            query,
            f"✅ <b>Category deleted.</b>\n\nProducts moved to {DEFAULT_CATEGORY_NAME}: <b>{moved_count}</b>",
            admin_categories_keyboard(),
        )
        return

    # ========= PRODUCT ADMIN =========
    if data.startswith("admin_bulk_") and not is_admin(user_id):
        await send_inline_from_callback(query, "❌ <b>You are not allowed.</b>", close_keyboard())
        return

    if data == "admin_products_close":
        await send_inline_from_callback(query, "Closed products panel.", close_keyboard())
        return

    if data == "admin_products_back":
        user_state[user_id] = {"step": "admin_products"}
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, render_admin_products_text(), admin_products_keyboard())
        return

    if data == "admin_view_products":
        user_state[user_id] = {"step": "admin_products"}
        await send_inline_from_callback(query, render_admin_products_list(), admin_products_keyboard())
        return

    if data == "admin_add_product":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_add_product_icon"}
        await send_inline_from_callback(
            query,
            "🆕 <b>Add Product</b>\n\nFirst send product icon/emoji.\nExample: 📊",
            admin_cancel_keyboard(),
        )
        return

    if data == "admin_edit_name_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_name_pick"}
        await send_inline_from_callback(query, "✏️ <b>Edit Name</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_name"))
        return

    if data == "admin_edit_price_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_price_pick"}
        await send_inline_from_callback(query, "💲 <b>Edit Price</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_price"))
        return

    if data == "admin_edit_month_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_month_pick"}
        await send_inline_from_callback(query, "📅 <b>Edit Month</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_month"))
        return

    if data == "admin_edit_details_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_details_pick"}
        await send_inline_from_callback(query, "📝 <b>Edit Details</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_details"))
        return

    if data == "admin_edit_delivery_guide_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_delivery_guide_pick"}
        await send_inline_from_callback(query, "📌 <b>Edit Delivery Guide</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_delivery_guide"))
        return

    if data == "admin_edit_icon_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_icon_pick"}
        await send_inline_from_callback(query, "😀 <b>Edit Icon</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_icon"))
        return

    if data == "admin_edit_display_stock_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_edit_display_stock_pick"}
        await send_inline_from_callback(query, "📦 <b>Edit Display Stock</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_display_stock"))
        return

    if data == "admin_bulk_pricing_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_bulk_product_pick"}
        await send_inline_from_callback(
            query,
            "💸 <b>Bulk Pricing</b>\n\nSelect a product below.",
            admin_product_select_keyboard("admin_bulk_pick"),
        )
        return

    if data.startswith("admin_bulk_pick_"):
        product_id = data.replace("admin_bulk_pick_", "", 1)
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_products_keyboard())
            return
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_bulk_pricing"}
        await send_inline_from_callback(
            query,
            render_bulk_pricing_admin(product_id),
            bulk_pricing_admin_keyboard(product_id),
        )
        return

    if data == "admin_bulk_view":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_products_keyboard())
            return
        user_state[user_id] = {"step": "admin_bulk_pricing"}
        await send_inline_from_callback(
            query,
            render_bulk_pricing_admin(product_id),
            bulk_pricing_admin_keyboard(product_id),
        )
        return

    if data == "admin_bulk_add":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_products_keyboard())
            return
        user_state[user_id] = {"step": "admin_bulk_min_qty_input"}
        await send_inline_from_callback(
            query,
            "➕ <b>Add Bulk Pricing Tier</b>\n\nSend the minimum quantity.\nExample: <code>5</code>",
            admin_cancel_keyboard(),
        )
        return

    if data == "admin_bulk_remove_menu":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_products_keyboard())
            return
        await send_inline_from_callback(
            query,
            "🗑 <b>Remove Bulk Pricing Tier</b>\n\nSelect a tier below.",
            bulk_pricing_remove_keyboard(product_id),
        )
        return

    if data.startswith("admin_bulk_remove_"):
        product_id = admin_temp[user_id].get("selected_product_id")
        try:
            min_qty = int(data.replace("admin_bulk_remove_", "", 1))
        except ValueError:
            min_qty = 0
        if product_id not in PRODUCTS or min_qty < 2:
            await send_inline_from_callback(query, "❌ <b>Bulk pricing tier not found.</b>", admin_products_keyboard())
            return
        remaining_tiers = [
            tier for tier in get_bulk_pricing_tiers(PRODUCTS[product_id])
            if tier["min_qty"] != min_qty
        ]
        if remaining_tiers:
            PRODUCTS[product_id]["bulk_pricing"] = remaining_tiers
        else:
            PRODUCTS[product_id].pop("bulk_pricing", None)
        user_state[user_id] = {"step": "admin_bulk_pricing"}
        await send_inline_from_callback(
            query,
            "✅ <b>Bulk pricing tier removed.</b>\n\n" + render_bulk_pricing_admin(product_id),
            bulk_pricing_admin_keyboard(product_id),
        )
        return

    if data == "admin_bulk_clear":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_products_keyboard())
            return
        await send_inline_from_callback(
            query,
            "🧹 <b>Clear all bulk pricing tiers?</b>\n\n"
            f"Product: <b>{escape_html(PRODUCTS[product_id]['name'])}</b>\n\n"
            "This does not change the normal product price.",
            bulk_pricing_clear_confirm_keyboard(),
        )
        return

    if data == "admin_bulk_clear_confirm":
        product_id = admin_temp[user_id].get("selected_product_id")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_products_keyboard())
            return
        PRODUCTS[product_id].pop("bulk_pricing", None)
        user_state[user_id] = {"step": "admin_bulk_pricing"}
        await send_inline_from_callback(
            query,
            "✅ <b>All bulk pricing tiers cleared.</b>\n\n" + render_bulk_pricing_admin(product_id),
            bulk_pricing_admin_keyboard(product_id),
        )
        return

    if data == "admin_bulk_back":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_products"}
        await send_inline_from_callback(query, render_admin_products_text(), admin_products_keyboard())
        return

    if data == "admin_delete_product_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_delete_product_pick"}
        await send_inline_from_callback(query, "🗑 <b>Delete Product</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_delete"))
        return

    if data == "admin_reorder_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_reorder_pick"}
        await send_inline_from_callback(query, "↕️ <b>Reorder in Shop</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_reorder"))
        return

    if data in {
        "admin_broadcast_send",
        "admin_broadcast_send_both",
        "admin_broadcast_send_users",
        "admin_broadcast_send_channel",
    }:
        if not is_admin(user_id):
            await send_inline_from_callback(query, "❌ <b>You are not allowed.</b>", close_keyboard())
            return
        message = admin_temp.get(user_id, {}).get("broadcast_text")
        if not message:
            await send_inline_from_callback(query, "❌ <b>No broadcast message found.</b>", close_keyboard())
            return
        entity_data = list(admin_temp.get(user_id, {}).get("broadcast_entities") or [])

        destination = {
            "admin_broadcast_send_users": "users",
            "admin_broadcast_send_channel": "channel",
        }.get(data, "both")
        targets = sorted(int(uid) for uid in all_users if int(uid) != int(user_id))
        destination_label = {
            "users": "Users Only",
            "channel": "Official Channel Only",
            "both": "Users + Official Channel",
        }[destination]
        if destination in {"channel", "both"} and not REQUIRED_CHANNEL_ID:
            channel_note = "\n\n⚠️ Channel ID is not configured."
        else:
            channel_note = ""
        await send_inline_from_callback(
            query,
            "📢 <b>Broadcast started in background...</b>\n\n"
            f"<b>Destination:</b> {destination_label}\n"
            f"<b>User targets:</b> {len(targets) if destination in {'users', 'both'} else 0}"
            f"{channel_note}",
        )
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_main"}
        start_admin_broadcast_background(
            context.bot,
            user_id,
            message,
            entity_data,
            targets,
            destination=destination,
            channel_id=REQUIRED_CHANNEL_ID,
        )
        return

    if data == "admin_cancel_flow":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "main"}
        await send_inline_from_callback(query, "❌ <b>Cancelled.</b>", close_keyboard())
        return

    # ========= USER BALANCE ADMIN =========
    if data == "balance_close":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_main"}
        await send_inline_from_callback(query, "Closed balance panel.", close_keyboard())
        return

    if data == "balance_add":
        reset_admin_temp(user_id)
        admin_temp[user_id]["balance_action"] = "add"
        user_state[user_id] = {"step": "balance_user_id_input"}
        await send_inline_from_callback(query, "➕ <b>Add Balance</b>\n\nSend user ID now.", admin_cancel_keyboard())
        return

    if data == "balance_minus":
        reset_admin_temp(user_id)
        admin_temp[user_id]["balance_action"] = "minus"
        user_state[user_id] = {"step": "balance_user_id_input"}
        await send_inline_from_callback(query, "➖ <b>Minus Balance</b>\n\nSend user ID now.", admin_cancel_keyboard())
        return

    if data == "balance_check":
        reset_admin_temp(user_id)
        admin_temp[user_id]["balance_action"] = "check"
        user_state[user_id] = {"step": "balance_user_id_input"}
        await send_inline_from_callback(query, "🔎 <b>Check User Balance</b>\n\nSend user ID now.", admin_cancel_keyboard())
        return

    if data == "balance_confirm_update":
        ok, msg, old_balance, new_balance = apply_admin_balance_adjustment(user_id)
        if ok:
            save_bot_state()
            target_user_id = int(admin_temp.get(user_id, {}).get("target_user_id", 0) or 0)
            reset_admin_temp(user_id)
            user_state[user_id] = {"step": "balance_admin"}
            await send_inline_from_callback(
                query,
                "✅ <b>Balance updated.</b>\n\n"
                f"<b>User ID:</b> <code>{target_user_id}</code>\n"
                f"<b>Old Balance:</b> {format_money(old_balance)}\n"
                f"<b>New Balance:</b> {format_money(new_balance)}",
                user_balance_admin_keyboard(),
            )
        else:
            await send_inline_from_callback(query, f"❌ <b>{escape_html(msg)}</b>", user_balance_admin_keyboard())
        return

    # ========= NOTIFY REQUESTS ADMIN =========
    if data == "admin_notify_close":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_main"}
        await send_inline_from_callback(query, "Closed notify requests panel.", close_keyboard())
        return

    if data in ("admin_notify_refresh", "admin_notify_back"):
        user_state[user_id] = {"step": "notify_requests_admin"}
        await send_inline_from_callback(query, render_admin_notify_panel(), admin_notify_keyboard())
        return

    if data == "admin_notify_summary":
        user_state[user_id] = {"step": "notify_requests_admin"}
        await send_inline_from_callback(query, render_notify_requests_summary(), admin_notify_keyboard())
        return

    if data == "admin_notify_products":
        user_state[user_id] = {"step": "notify_requests_admin"}
        await send_inline_from_callback(
            query,
            "🔔 <b>SELECT PRODUCT</b>\n\nChoose a product to see waiting users.",
            admin_notify_product_select_keyboard(),
        )
        return

    if data.startswith("admin_notify_product_"):
        product_id = data.replace("admin_notify_product_", "")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", admin_notify_keyboard())
            return
        user_state[user_id] = {"step": "notify_requests_admin"}
        await send_inline_from_callback(query, render_notify_product_details(product_id), admin_notify_product_back_keyboard())
        return

    # ========= FLASH DEAL ADMIN =========
    if data == "flash_close":
        user_state[user_id] = {"step": "admin_main"}
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "Closed Flash Deal panel.", close_keyboard())
        return

    if data in ("flash_back", "flash_refresh"):
        user_state[user_id] = {"step": "flash_deal_admin"}
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, render_flash_deal_panel(), flash_deal_admin_keyboard())
        return

    if data == "flash_start_menu":
        user_state[user_id] = {"step": "flash_product_pick"}
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "⚡ <b>Start Flash Deal</b>\n\nSelect product below.", flash_deal_product_select_keyboard())
        return

    if data.startswith("flash_pick_product_"):
        product_id = data.replace("flash_pick_product_", "")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", flash_deal_admin_keyboard())
            return
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "flash_price_input"}
        product = PRODUCTS[product_id]
        await send_inline_from_callback(
            query,
            f"⚡ <b>Set Deal Price</b>\n\n<b>Product:</b> {product_icon_html(product)} {escape_html(product.get('name', product_id))}\n<b>Regular Price:</b> {format_money(get_product_base_price(product_id))}\n\nNow send deal price.",
            admin_cancel_keyboard(),
        )
        return

    if data == "flash_confirm_start":
        product_id = admin_temp[user_id].get("selected_product_id")
        deal_price = float(admin_temp[user_id].get("flash_deal_price", 0))
        minutes = int(admin_temp[user_id].get("flash_deal_minutes", 0))
        if product_id not in PRODUCTS or deal_price <= 0 or minutes <= 0:
            await send_inline_from_callback(query, "❌ <b>Flash deal data missing.</b>", flash_deal_admin_keyboard())
            return
        active_flash_deal.clear()
        active_flash_deal.update({
            "product_id": product_id,
            "deal_price": float(deal_price),
            "duration_minutes": int(minutes),
            "started_at": now_dt(),
            "end_at": datetime.fromtimestamp(now_dt().timestamp() + minutes * 60),
            "created_by": int(user_id),
        })
        user_state[user_id] = {"step": "flash_deal_admin"}
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Flash Deal started.</b>\n\n📢 User notification is sending in background.", flash_deal_admin_keyboard())
        start_flash_deal_broadcast_background(context.bot, product_id, deal_price, minutes, user_id)
        return

    if data == "flash_cancel_active":
        active_flash_deal.clear()
        user_state[user_id] = {"step": "flash_deal_admin"}
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "🛑 <b>Active Flash Deal cancelled.</b>", flash_deal_admin_keyboard())
        return

    # ========= GOLD VIP ADMIN =========
    if data == "vip_close":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_main"}
        await send_inline_from_callback(query, "Closed Gold VIP panel.", close_keyboard())
        return

    if data == "vip_back":
        user_state[user_id] = {"step": "gold_vip_admin"}
        await send_inline_from_callback(query, render_gold_vip_panel(), gold_vip_admin_keyboard())
        return

    if data == "vip_add_user":
        reset_admin_temp(user_id)
        admin_temp[user_id]["vip_action"] = "add"
        user_state[user_id] = {"step": "vip_user_id_input"}
        await send_inline_from_callback(query, "👑 <b>Add Gold VIP User</b>\n\nSend user ID now.", admin_cancel_keyboard())
        return

    if data == "vip_remove_user":
        reset_admin_temp(user_id)
        admin_temp[user_id]["vip_action"] = "remove"
        user_state[user_id] = {"step": "vip_user_id_input"}
        await send_inline_from_callback(query, "👑 <b>Remove Gold VIP User</b>\n\nSend user ID now.", admin_cancel_keyboard())
        return

    if data == "vip_user_list":
        user_state[user_id] = {"step": "gold_vip_admin"}
        await send_inline_from_callback(query, render_gold_vip_user_list(), gold_vip_admin_keyboard())
        return

    if data == "vip_price_list":
        user_state[user_id] = {"step": "gold_vip_admin"}
        await send_inline_from_callback(query, render_gold_vip_price_list(), gold_vip_admin_keyboard())
        return

    if data == "vip_set_price_menu":
        user_state[user_id] = {"step": "vip_price_pick"}
        await send_inline_from_callback(query, "👑 <b>Set Gold VIP Price</b>\n\nSelect product below.", gold_vip_product_select_keyboard("vip_pick_price"))
        return

    if data == "vip_remove_price_menu":
        user_state[user_id] = {"step": "vip_remove_price_pick"}
        await send_inline_from_callback(query, "🗑 <b>Remove Gold VIP Price</b>\n\nSelect product below.", gold_vip_product_select_keyboard("vip_remove_price"))
        return

    if data.startswith("vip_pick_price_"):
        product_id = data.replace("vip_pick_price_", "")
        if product_id not in PRODUCTS:
            await send_inline_from_callback(query, "❌ <b>Product not found.</b>", gold_vip_admin_keyboard())
            return
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "vip_price_input"}
        current_vip = get_product_vip_price(product_id)
        current_vip_text = format_money(current_vip) if current_vip is not None else "Not set"
        await send_inline_from_callback(
            query,
            f"👑 <b>Set Gold VIP Price</b>\n\n<b>Product:</b> {escape_html(PRODUCTS[product_id]['name'])}\n<b>Normal Price:</b> {format_money(get_product_base_price(product_id))}\n<b>Current VIP Price:</b> {current_vip_text}\n\nNow send VIP price.",
            admin_cancel_keyboard(),
        )
        return

    if data.startswith("vip_remove_price_"):
        product_id = data.replace("vip_remove_price_", "")
        if product_id in PRODUCTS:
            PRODUCTS[product_id].pop("gold_vip_price", None)
        user_state[user_id] = {"step": "gold_vip_admin"}
        await send_inline_from_callback(query, "✅ <b>VIP price removed.</b>", gold_vip_admin_keyboard())
        return

    if data == "vip_confirm_price":
        product_id = admin_temp[user_id].get("selected_product_id")
        vip_price = float(admin_temp[user_id].get("gold_vip_price", 0))
        if product_id in PRODUCTS and vip_price > 0:
            PRODUCTS[product_id]["gold_vip_price"] = vip_price
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "gold_vip_admin"}
        await send_inline_from_callback(query, "✅ <b>Gold VIP price updated.</b>", gold_vip_admin_keyboard())
        return

    if data.startswith("admin_pick_name_"):
        product_id = data.replace("admin_pick_name_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_name_input"}
        await send_inline_from_callback(query, f"✏️ <b>Edit Name</b>\n\nCurrent: <b>{PRODUCTS[product_id]['name']}</b>\n\nNow send new name.", admin_cancel_keyboard())
        return

    if data.startswith("admin_pick_price_"):
        product_id = data.replace("admin_pick_price_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_price_input"}
        await send_inline_from_callback(query, f"💲 <b>Edit Price</b>\n\nCurrent: <b>{format_money(PRODUCTS[product_id]['price'])}</b>\n\nNow send new price.", admin_cancel_keyboard())
        return

    if data.startswith("admin_pick_month_"):
        product_id = data.replace("admin_pick_month_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_month_input"}
        await send_inline_from_callback(query, f"📅 <b>Edit Month</b>\n\nCurrent: <b>{PRODUCTS[product_id]['month']}</b>\n\nNow send new month.", admin_cancel_keyboard())
        return

    if data.startswith("admin_pick_details_"):
        product_id = data.replace("admin_pick_details_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_details_input"}
        await send_inline_from_callback(query, f"📝 <b>Edit Details</b>\n\nSend new details line by line.", admin_cancel_keyboard())
        return

    if data.startswith("admin_pick_delivery_guide_"):
        product_id = data.replace("admin_pick_delivery_guide_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_delivery_guide_input"}
        current = get_delivery_guide(product_id)
        if len(current) > 2500:
            current = current[:2500] + "\n..."
        await send_inline_from_callback(
            query,
            "📌 <b>Edit Delivery Guide</b>\n\n"
            f"<b>Product:</b> {PRODUCTS[product_id]['name']}\n\n"
            f"<b>Current Guide:</b>\n{escape_html(current)}\n\n"
            "Now send the new full delivery guide/instruction. It will be sent once after all account details.",
            admin_cancel_keyboard(),
        )
        return

    if data.startswith("admin_pick_icon_"):
        product_id = data.replace("admin_pick_icon_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_icon_input"}
        await send_inline_from_callback(query, f"😀 <b>Edit Icon</b>\n\nCurrent: {product_icon_html(PRODUCTS[product_id])}\n\nNow send new icon.", admin_cancel_keyboard())
        return

    if data.startswith("admin_pick_display_stock_"):
        product_id = data.replace("admin_pick_display_stock_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_display_stock_input"}
        await send_inline_from_callback(query, f"📦 <b>Edit Display Stock</b>\n\nCurrent: {get_display_stock(product_id)}\n\nNow send new display stock.", admin_cancel_keyboard())
        return

    if data.startswith("admin_pick_delete_"):
        product_id = data.replace("admin_pick_delete_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_delete_confirm"}
        await send_inline_from_callback(query, render_admin_delete_preview(product_id), admin_confirm_keyboard("admin_confirm_delete_product", "🗑 Confirm Delete"))
        return

    if data.startswith("admin_pick_reorder_"):
        product_id = data.replace("admin_pick_reorder_", "")
        user_state[user_id] = {"step": "admin_reorder_selected"}
        await send_inline_from_callback(query, f"↕️ <b>Reorder Product</b>\n\nSelected: {PRODUCTS[product_id]['name']}", admin_reorder_selected_keyboard(product_id))
        return

    if data.startswith("admin_move_up_"):
        product_id = data.replace("admin_move_up_", "")
        move_shop_order_item(f"product:{product_id}", -1)
        await send_inline_from_callback(query, "✅ <b>Moved up.</b>", admin_reorder_selected_keyboard(product_id))
        return

    if data.startswith("admin_move_down_"):
        product_id = data.replace("admin_move_down_", "")
        move_shop_order_item(f"product:{product_id}", 1)
        await send_inline_from_callback(query, "✅ <b>Moved down.</b>", admin_reorder_selected_keyboard(product_id))
        return

    if data == "admin_confirm_add_product":
        temp = admin_temp[user_id]
        product_id = generate_new_product_id()
        PRODUCTS[product_id] = {
            "name": temp["name"],
            "icon": temp.get("icon", "📦"),
            "icon_custom_emoji_id": temp.get("icon_custom_emoji_id"),
            "category_id": DEFAULT_CATEGORY_ID,
            "month": temp["month"],
            "price": float(temp["price"]),
            "details": list(temp["details"]),
            "accounts": [],
            "display_stock": int(temp.get("display_stock", 0)),
        }
        _normalize_product_icon_fields(PRODUCTS[product_id])
        notify_waitlist[product_id] = set()
        product_order.append(product_id)
        shop_order.append(f"product:{product_id}")
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_products"}
        await send_inline_from_callback(query, f"✅ <b>Product added.</b>\n\nID: {product_id}\nName: {PRODUCTS[product_id]['name']}", admin_products_keyboard())
        return

    if data == "admin_confirm_name_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["name"] = admin_temp[user_id]["new_name"]
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Name updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_price_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["price"] = float(admin_temp[user_id]["new_price"])
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Price updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_month_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["month"] = admin_temp[user_id]["new_month"]
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Month updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_details_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["details"] = list(admin_temp[user_id]["new_details"])
        rich_details = admin_temp[user_id].get("new_details_rich")
        if rich_details:
            PRODUCTS[product_id]["details_rich"] = rich_details
        else:
            PRODUCTS[product_id].pop("details_rich", None)
        save_bot_state()
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Details updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_delivery_guide_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["delivery_guide"] = admin_temp[user_id]["new_delivery_guide"]
        rich_guide = admin_temp[user_id].get("new_delivery_guide_rich")
        if rich_guide:
            PRODUCTS[product_id]["delivery_guide_rich"] = rich_guide
        else:
            PRODUCTS[product_id].pop("delivery_guide_rich", None)
        save_bot_state()
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Delivery guide updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_icon_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["icon"] = admin_temp[user_id]["new_icon"]
        PRODUCTS[product_id]["icon_custom_emoji_id"] = admin_temp[user_id].get("new_icon_custom_emoji_id")
        _normalize_product_icon_fields(PRODUCTS[product_id])
        save_bot_state()
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Icon updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_display_stock_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["display_stock"] = int(admin_temp[user_id]["new_display_stock"])
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Display stock updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_delete_product":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS.pop(product_id, None)
        notify_waitlist.pop(product_id, None)
        if product_id in product_order:
            product_order.remove(product_id)
        product_item = f"product:{product_id}"
        if product_item in shop_order:
            shop_order.remove(product_item)
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Product deleted.</b>", admin_products_keyboard())
        return

    # ========= STOCK ADMIN =========
    if data == "stock_view":
        await send_inline_from_callback(query, render_admin_stock_list(), admin_stock_keyboard())
        return

    if data == "stock_add_single_menu":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "➕ <b>Add Single Account</b>\n\nSelect a product below.", stock_product_select_keyboard("stock_pick_single"))
        return

    if data == "stock_add_bulk_menu":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "📥 <b>Add Bulk Accounts</b>\n\nSelect a product below.", stock_product_select_keyboard("stock_pick_bulk"))
        return

    if data == "stock_view_accounts_menu":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "👀 <b>View Account List</b>\n\nSelect a product below.", stock_product_select_keyboard("stock_view_accounts"))
        return

    if data == "stock_edit_account_menu":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✏️ <b>Edit Account</b>\n\nSelect a product below.", stock_product_select_keyboard("stock_edit_pick_product"))
        return

    if data == "stock_delete_account_menu":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "🗑 <b>Delete Account</b>\n\nSelect a product below.", stock_product_select_keyboard("stock_delete_pick_product"))
        return

    if data == "stock_set_display_menu":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "🔢 <b>Set Display Stock</b>\n\nSelect a product below.", stock_product_select_keyboard("stock_set_display_pick_product"))
        return

    if data == "stock_back":
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "📥 <b>STOCK MANAGEMENT</b>", admin_stock_keyboard())
        return

    if data == "stock_close":
        await send_inline_from_callback(query, "Closed stock panel.", close_keyboard())
        return

    if data.startswith("stock_pick_single_"):
        product_id = data.replace("stock_pick_single_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "stock_add_single_input"}
        await send_inline_from_callback(query, "📥 <b>Send stock item</b>\n\nYou can send:\n<code>email@gmail.com|password|note</code>\nOR\n<code>https://your-link.com</code>\nOR any license/code/text.", admin_cancel_keyboard())
        return

    if data.startswith("stock_pick_bulk_"):
        product_id = data.replace("stock_pick_bulk_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "stock_add_bulk_input"}
        await send_inline_from_callback(query, "📥 <b>Send bulk stock</b>\n\nSend one item per line, or upload a .txt file.\n\nExample:\n<code>link1\nlink2\nemail@gmail.com|password|note</code>", admin_cancel_keyboard())
        return

    if data.startswith("stock_view_accounts_"):
        product_id = data.replace("stock_view_accounts_", "")
        await send_inline_from_callback(query, render_account_list_text(product_id, 0), account_serial_keyboard(product_id, "stock_view_acc", 0))
        return

    if data.startswith("stock_view_acc_page_"):
        rest = data.replace("stock_view_acc_page_", "")
        product_id, page_str = rest.rsplit("_", 1)
        page = int(page_str)
        await send_inline_from_callback(query, render_account_list_text(product_id, page), account_serial_keyboard(product_id, "stock_view_acc", page))
        return

    if data.startswith("stock_view_acc_") and not data.startswith("stock_view_acc_page_"):
        rest = data.replace("stock_view_acc_", "")
        product_id, idx_str = rest.rsplit("_", 1)
        idx = int(idx_str)
        await send_inline_from_callback(query, render_selected_account(product_id, idx), InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"stock_view_accounts_{product_id}")]]))
        return

    if data.startswith("stock_edit_pick_product_"):
        product_id = data.replace("stock_edit_pick_product_", "")
        await send_inline_from_callback(query, render_account_list_text(product_id, 0), account_serial_keyboard(product_id, "stock_edit_acc", 0))
        return

    if data.startswith("stock_edit_acc_page_"):
        rest = data.replace("stock_edit_acc_page_", "")
        product_id, page_str = rest.rsplit("_", 1)
        page = int(page_str)
        await send_inline_from_callback(query, render_account_list_text(product_id, page), account_serial_keyboard(product_id, "stock_edit_acc", page))
        return

    if data.startswith("stock_edit_acc_") and not data.startswith("stock_edit_acc_page_"):
        rest = data.replace("stock_edit_acc_", "")
        product_id, idx_str = rest.rsplit("_", 1)
        idx = int(idx_str)
        admin_temp[user_id]["selected_product_id"] = product_id
        admin_temp[user_id]["selected_account_index"] = idx
        user_state[user_id] = {"step": "stock_edit_account_input"}
        await send_inline_from_callback(query, "✏️ <b>Send new stock item</b>\n\nYou can send link/code/text or old format:\n<code>email@gmail.com|password|note</code>", admin_cancel_keyboard())
        return

    if data.startswith("stock_delete_pick_product_"):
        product_id = data.replace("stock_delete_pick_product_", "")
        await send_inline_from_callback(query, render_account_list_text(product_id, 0), account_serial_keyboard(product_id, "stock_delete_acc", 0))
        return

    if data.startswith("stock_delete_acc_page_"):
        rest = data.replace("stock_delete_acc_page_", "")
        product_id, page_str = rest.rsplit("_", 1)
        page = int(page_str)
        await send_inline_from_callback(query, render_account_list_text(product_id, page), account_serial_keyboard(product_id, "stock_delete_acc", page))
        return

    if data.startswith("stock_delete_acc_") and not data.startswith("stock_delete_acc_page_"):
        rest = data.replace("stock_delete_acc_", "")
        product_id, idx_str = rest.rsplit("_", 1)
        idx = int(idx_str)
        if 0 <= idx < len(PRODUCTS[product_id]["accounts"]):
            PRODUCTS[product_id]["accounts"].pop(idx)
        await send_inline_from_callback(query, "✅ <b>Account deleted.</b>", admin_stock_keyboard())
        return

    if data.startswith("stock_set_display_pick_product_"):
        product_id = data.replace("stock_set_display_pick_product_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "stock_set_display_input"}
        await send_inline_from_callback(query, f"Send new display stock for <b>{PRODUCTS[product_id]['name']}</b>:", admin_cancel_keyboard())
        return

    # ========= PROMO ADMIN =========
    if data == "promo_close":
        await send_inline_from_callback(query, "Closed promo panel.", close_keyboard())
        return

    if data == "promo_back":
        user_state[user_id] = {"step": "promo_admin"}
        await send_inline_from_callback(query, "🎟 <b>PROMO ADMIN</b>\n\nChoose an option below.", admin_promo_keyboard())
        return

    if data == "promo_generator":
        await send_inline_from_callback(query, "🎲 <b>PROMO GENERATOR</b>\n\nChoose amount below.", promo_generator_amount_keyboard())
        return

    if data == "promo_gen_amt_1":
        code = generate_unique_promo_code()
        PROMO_CODES[code] = {"amount": 1.0, "enabled": True, "one_time": True, "created_at": now_dt(), "created_by": str(user_id), "used_by": None, "used_at": None}
        await send_inline_from_callback(query, render_generated_promo_text(code), admin_promo_keyboard())
        return

    if data == "promo_gen_amt_5":
        code = generate_unique_promo_code()
        PROMO_CODES[code] = {"amount": 5.0, "enabled": True, "one_time": True, "created_at": now_dt(), "created_by": str(user_id), "used_by": None, "used_at": None}
        await send_inline_from_callback(query, render_generated_promo_text(code), admin_promo_keyboard())
        return

    if data == "promo_gen_amt_10":
        code = generate_unique_promo_code()
        PROMO_CODES[code] = {"amount": 10.0, "enabled": True, "one_time": True, "created_at": now_dt(), "created_by": str(user_id), "used_by": None, "used_at": None}
        await send_inline_from_callback(query, render_generated_promo_text(code), admin_promo_keyboard())
        return

    if data == "promo_gen_custom":
        user_state[user_id] = {"step": "promo_generate_custom_amount"}
        await send_inline_from_callback(query, "💲 Send custom promo amount now.", admin_cancel_keyboard())
        return

    if data == "promo_view":
        await send_inline_from_callback(query, render_promo_list(), admin_promo_keyboard())
        return

    if data == "promo_toggle_menu":
        await send_inline_from_callback(query, "🔁 <b>Enable / Disable Promo</b>\n\nSelect a promo below.", promo_select_keyboard("promo_toggle"))
        return

    if data == "promo_delete_menu":
        await send_inline_from_callback(query, "🗑 <b>Delete Promo</b>\n\nSelect a promo below.", promo_select_keyboard("promo_delete"))
        return

    if data.startswith("promo_toggle_"):
        code = data.replace("promo_toggle_", "")
        if code in PROMO_CODES:
            PROMO_CODES[code]["enabled"] = not PROMO_CODES[code].get("enabled", True)
            status = "Enabled" if PROMO_CODES[code]["enabled"] else "Disabled"
            await send_inline_from_callback(query, f"✅ <b>Promo updated.</b>\n\nCode: {code}\nStatus: {status}", promo_select_keyboard("promo_toggle"))
        return

    if data.startswith("promo_delete_"):
        code = data.replace("promo_delete_", "")
        if code in PROMO_CODES:
            PROMO_CODES.pop(code, None)
            await send_inline_from_callback(query, f"✅ <b>Promo deleted.</b>\n\nCode: {code}", admin_promo_keyboard())
        return

    # ========= USER ORDERS =========
    if data == "user_orders_close":
        enter_client_mode(user_id)
        await edit_user_dashboard_panel(
            query,
            render_home_text(user_id),
            user_dashboard_keyboard(),
            fallback_keyboards=[
                user_dashboard_keyboard(),
                user_dashboard_keyboard(custom_icons=False),
                user_dashboard_keyboard(styled=False, custom_icons=False),
            ],
            fallback_text=render_home_text(user_id, custom_icons=False),
        )
        return

    if data == "user_order_unavailable":
        await edit_user_orders_message(
            query,
            "📦 <b>ORDER DETAILS</b>\n\nThis order has no usable order ID.",
            user_order_details_keyboard(0),
        )
        return

    if data.startswith("user_orders_page_"):
        try:
            requested_page = int(data.rsplit("_", 1)[1])
        except (TypeError, ValueError):
            requested_page = 0
        text, page, _ = render_user_orders_page(user_id, requested_page)
        await edit_user_orders_message(query, text, user_orders_keyboard(user_id, page))
        return

    if data.startswith("user_order_view_"):
        try:
            order_id, page_text = data[len("user_order_view_"):].rsplit("_", 1)
            page = max(0, int(page_text))
        except (TypeError, ValueError):
            order_id, page = "", 0
        order = find_user_order_by_id(user_id, order_id)
        if not order:
            await edit_user_orders_message(
                query,
                "📦 <b>ORDER DETAILS</b>\n\nOrder not found.",
                user_order_details_keyboard(page),
            )
            return
        await edit_user_orders_message(
            query,
            render_user_order_details(user_id, order_id),
            user_order_details_keyboard(page),
        )
        return

    # ========= ORDERS ADMIN =========
    if data == "orders_close":
        await send_inline_from_callback(query, "Closed orders panel.", close_keyboard())
        return

    if data == "orders_back":
        await send_inline_from_callback(query, "📦 <b>ORDERS ADMIN</b>", admin_orders_keyboard())
        return

    if data == "orders_view_all":
        await send_inline_from_callback(query, render_order_list("all"), admin_orders_keyboard())
        return

    if data == "orders_view_pending_manual":
        await send_inline_from_callback(query, render_order_list("pending_manual"), admin_orders_keyboard())
        return

    if data == "orders_view_completed":
        await send_inline_from_callback(query, render_order_list("completed"), admin_orders_keyboard())
        return

    if data == "orders_user_search":
        user_state[user_id] = {"step": "orders_user_search_input"}
        await send_inline_from_callback(query, "🔎 Send User ID to search order/user history.", admin_cancel_keyboard())
        return

    if data == "orders_manual_pick_menu":
        await send_inline_from_callback(query, "☑️ <b>Confirm / Reject Manual Orders</b>\n\nSelect pending order below.", pending_manual_orders_keyboard())
        return

    if data.startswith("orders_pick_manual_"):
        order_id = int(data.replace("orders_pick_manual_", ""))
        order = find_order_by_id(order_id)
        if not order:
            await send_inline_from_callback(query, "❌ Order not found.", admin_orders_keyboard())
            return
        await send_inline_from_callback(
            query,
            f"⏳ <b>MANUAL ORDER</b>\n\nOrder ID: #{order['id']}\nUser: {order['user_id']}\nProduct: {order['product']}\nQty: {order['qty']}\nTotal: {format_money(order['total'])}\nStatus: {order['status']}",
            manual_order_action_keyboard(order_id),
        )
        return

    if data.startswith("orders_confirm_"):
        order_id = int(data.replace("orders_confirm_", ""))
        ok, msg = await confirm_manual_order(context, order_id)
        await send_inline_from_callback(query, ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>", admin_orders_keyboard())
        return

    if data.startswith("orders_reject_"):
        order_id = int(data.replace("orders_reject_", ""))
        ok, msg = await reject_manual_order(context, order_id)
        await send_inline_from_callback(query, ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>", admin_orders_keyboard())
        return

    # ========= DEPOSITS ADMIN =========
    if data == "deposits_close":
        await send_inline_from_callback(query, "Closed deposits panel.", close_keyboard())
        return

    if data == "deposits_back":
        await send_inline_from_callback(query, "💳 <b>DEPOSITS ADMIN</b>", deposits_admin_keyboard())
        return

    if data == "deposits_pending_manual":
        await send_inline_from_callback(query, render_pending_manual_deposits(), deposits_admin_keyboard())
        return

    if data == "deposits_all":
        await send_inline_from_callback(query, render_all_deposits_text(), deposits_admin_keyboard())
        return

    if data == "deposits_user_search":
        user_state[user_id] = {"step": "deposits_user_search_input"}
        await send_inline_from_callback(query, "🔎 Send User ID to search deposit/user history.", admin_cancel_keyboard())
        return

    if data == "deposits_pick_menu":
        await send_inline_from_callback(query, "☑️ <b>Confirm / Reject Manual Deposits</b>\n\nSelect pending deposit below.", pending_manual_deposits_keyboard())
        return

    if data.startswith("deposits_pick_"):
        tx_id = int(data.replace("deposits_pick_", ""))
        tx = find_tx_by_id(tx_id)
        if not tx:
            await send_inline_from_callback(query, "❌ Deposit not found.", deposits_admin_keyboard())
            return
        await send_inline_from_callback(query, f"💳 <b>MANUAL DEPOSIT</b>\n\nTX ID: #{tx['id']}\nUser: {tx['user_id']}\nAmount: {format_money(tx['amount'])}\nStatus: {tx['status']}", manual_deposit_action_keyboard(tx_id))
        return

    if data.startswith("deposits_confirm_"):
        tx_id = int(data.replace("deposits_confirm_", ""))
        ok, msg = await confirm_manual_deposit(context, tx_id)
        await send_inline_from_callback(query, ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>", deposits_admin_keyboard())
        return

    if data.startswith("deposits_reject_"):
        tx_id = int(data.replace("deposits_reject_", ""))
        ok, msg = await reject_manual_deposit(context, tx_id)
        await send_inline_from_callback(query, ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>", deposits_admin_keyboard())
        return

    # ========= USERS ADMIN =========
    if data == "users_close":
        await send_inline_from_callback(query, "Closed users panel.", close_keyboard())
        return

    if data == "users_summary":
        await send_inline_from_callback(query, render_users_admin(), users_admin_keyboard())
        return

    if data == "users_search":
        user_state[user_id] = {"step": "users_search_input"}
        await send_inline_from_callback(query, "🔎 Send User ID now.", admin_cancel_keyboard())
        return

    if data.startswith("users_details_"):
        try:
            page = int(data.replace("users_details_", ""))
        except Exception:
            page = 0
        details_text, page, total_pages = render_user_details_page(page)
        await send_inline_from_callback(query, details_text, users_details_keyboard(page, total_pages))
        return

    # ========= CLIENT FLOWS =========
    if data == "user_back_to_dashboard":
        enter_client_mode(user_id)
        await edit_user_dashboard_panel(
            query,
            render_home_text(user_id),
            user_dashboard_keyboard(),
            fallback_keyboards=[
                user_dashboard_keyboard(),
                user_dashboard_keyboard(custom_icons=False),
                user_dashboard_keyboard(styled=False, custom_icons=False),
            ],
            fallback_text=render_home_text(user_id, custom_icons=False),
        )
        return

    if data.startswith("user_dashboard"):
        enter_client_mode(user_id)

        if data == "user_dashboard":
            await edit_user_dashboard_panel(
                query,
                render_home_text(user_id),
                user_dashboard_keyboard(),
                fallback_keyboards=[
                    user_dashboard_keyboard(),
                    user_dashboard_keyboard(custom_icons=False),
                    user_dashboard_keyboard(styled=False, custom_icons=False),
                ],
                fallback_text=render_home_text(user_id, custom_icons=False),
            )
            return

        if data == "user_dashboard_shop":
            if not await ensure_channel_access(update, context):
                return
            user_state[user_id] = {"step": "shop"}
            await send_shop_cards_message(query, from_callback=True)
            return

        if data == "user_dashboard_wallet":
            if not await ensure_channel_access(update, context):
                return
            await edit_user_dashboard_panel(
                query, render_wallet_text(user_id), user_dashboard_back_keyboard()
            )
            return

        if data == "user_dashboard_topup":
            if not await ensure_channel_access(update, context):
                return
            user_state[user_id] = {"step": "deposit_amount"}
            await edit_user_dashboard_panel(
                query,
                render_deposit_text(),
                deposit_amount_keyboard(),
                fallback_keyboards=[deposit_amount_keyboard(styled=False)],
            )
            return

        if data == "user_dashboard_orders":
            orders_text, page, _ = render_user_orders_page(user_id)
            await edit_user_orders_message(query, orders_text, user_orders_keyboard(user_id, page))
            return

        if data == "user_dashboard_transactions":
            await edit_user_dashboard_panel(
                query, render_transactions_text(user_id), user_dashboard_back_keyboard()
            )
            return

        if data == "user_dashboard_profile":
            await edit_user_dashboard_panel(
                query, render_user_id_text(user_id), user_dashboard_back_keyboard()
            )
            return

        if data == "user_dashboard_promo":
            user_state[user_id] = {"step": "awaiting_promo"}
            await edit_user_dashboard_panel(
                query,
                "🎟 <b>PROMO</b>\n\nPlease send your promo code.",
                user_dashboard_back_keyboard(),
            )
            return

        if data == "user_dashboard_refer":
            await edit_user_dashboard_panel(
                query, render_refer_text(user_id), user_dashboard_back_keyboard()
            )
            return

        if data == "user_dashboard_support":
            await edit_user_dashboard_panel(
                query, render_support_text(), user_dashboard_back_keyboard()
            )
            return

    if data == "close_inline":
        if user_mode.get(user_id) != "admin":
            enter_client_mode(user_id)
            await edit_user_dashboard_panel(
                query,
                render_home_text(user_id),
                user_dashboard_keyboard(),
                fallback_keyboards=[
                    user_dashboard_keyboard(),
                    user_dashboard_keyboard(custom_icons=False),
                    user_dashboard_keyboard(styled=False, custom_icons=False),
                ],
                fallback_text=render_home_text(user_id, custom_icons=False),
            )
            return
        await send_inline_from_callback(query, "Closed.", close_keyboard())
        return

    if data == "back_shop_cards":
        user_state[user_id] = {"step": "shop"}
        await send_shop_cards_message(query, from_callback=True)
        return

    if data.startswith("shop_category_"):
        category_id = data.replace("shop_category_", "", 1)
        if category_id == DEFAULT_CATEGORY_ID or category_id not in CATEGORIES:
            await send_shop_cards_message(query, from_callback=True)
            return
        user_state[user_id] = {"step": "shop", "category_id": category_id}
        await send_shop_cards_message(query, from_callback=True, category_id=category_id)
        return

    if data.startswith("shop_buy_"):
        product_id = data.replace("shop_buy_", "")
        if get_display_stock(product_id) <= 0:
            await send_shop_inline_with_style_fallback(
                query,
                render_product_details(product_id, user_id),
                out_of_stock_product_keyboard(product_id),
                out_of_stock_product_keyboard(product_id, styled=False),
            )
            return
        user_state[user_id] = {"step": "buy_qty_select", "product_id": product_id}
        await send_shop_inline_with_style_fallback(
            query,
            render_product_details(product_id, user_id),
            buy_qty_keyboard(product_id),
            buy_qty_keyboard(product_id, styled=False),
        )
        return

    if data.startswith("shop_notify_"):
        product_id = data.replace("shop_notify_", "")
        normalize_notify_waitlist()
        notify_waitlist.setdefault(product_id, set())
        before_count = len(_notify_waiters(product_id))
        notify_waitlist[product_id].add(int(user_id))
        notify_waitlist[product_id] = _normalize_notify_user_ids(notify_waitlist.get(product_id, set()))
        after_count = len(_notify_waiters(product_id))
        if after_count != before_count:
            save_bot_state()
        product = PRODUCTS[product_id]
        await send_shop_inline_with_style_fallback(
            query,
            f"🔔 You will be notified when <b>{product['name']}</b> is back in stock.",
            shop_return_keyboard(),
            shop_return_keyboard(styled=False),
        )
        return

    if data.startswith("buy_qty_"):
        _, _, product_id, qty_str = data.split("_")
        qty = int(qty_str)
        stock = get_display_stock(product_id)
        if qty > stock:
            await send_shop_inline_with_style_fallback(
                query,
                f"❌ <b>Only {stock} pcs available.</b>",
                buy_qty_keyboard(product_id),
                buy_qty_keyboard(product_id, styled=False),
            )
            return
        try:
            _, total = calculate_order_total(product_id, qty, user_id)
        except ValueError:
            await send_shop_inline_with_style_fallback(
                query,
                "❌ <b>This product has an invalid price.</b> Please contact support.",
                buy_qty_keyboard(product_id),
                buy_qty_keyboard(product_id, styled=False),
            )
            return
        if user_wallet[user_id] >= total:
            await process_wallet_purchase(query, context, user_id, product_id, qty, total)
            user_state[user_id] = {"step": "main"}
            return
        user_state[user_id] = {"step": "buy_payment_method", "product_id": product_id, "qty": qty, "total": total}
        await send_inline_from_callback(query, render_buy_summary(product_id, qty, user_wallet[user_id], user_id), payment_method_keyboard("buy"))
        return

    if data.startswith("buy_custom_"):
        product_id = data.replace("buy_custom_", "")
        user_state[user_id] = {"step": "buy_custom_qty", "product_id": product_id}
        await query.message.reply_text("✏️ Send custom quantity as a number.\nExample: 2")
        return

    if data == "buy_method_binance":
        state = user_state[user_id]
        await send_inline_from_callback(query, render_buy_manual_payment_text(state["product_id"], state["qty"], state["total"], "Binance ID", BINANCE_ID), final_manual_keyboard("buymanual"))
        return

    if data == "buy_method_bybit":
        state = user_state[user_id]
        await send_inline_from_callback(query, render_buy_manual_payment_text(state["product_id"], state["qty"], state["total"], "Bybit ID", BYBIT_ID), final_manual_keyboard("buymanual"))
        return

    if data == "buy_method_crypto":
        user_state[user_id]["step"] = "buy_network"
        await send_inline_from_callback(query, "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:", network_keyboard("buy"))
        return

    if data == "buy_back_method":
        state = user_state[user_id]
        await send_inline_from_callback(query, render_buy_summary(state["product_id"], state["qty"], user_wallet[user_id], user_id), payment_method_keyboard("buy"))
        return

    if data.startswith("buy_net_"):
        network_label = paymod.map_network_callback_to_label(data.replace("buy_net_", ""))
        state = user_state[user_id]
        ref = f"order:{user_id}:{state['product_id']}:{state['qty']}:{state['total']}"
        try:
            np_record = create_gateway_payment(
                user_id=user_id,
                kind="order",
                usd_amount=state["total"],
                network_label=network_label,
                ref=ref,
                product_id=state["product_id"],
                qty=state["qty"],
            )
        except Exception as e:
            print("Crypto order create error:", e)
            await send_inline_from_callback(
                query,
                f"❌ <b>Could not create crypto payment.</b>\n\nPlease try again or contact live support: {SUPPORT_USERNAME}",
                paymod.network_keyboard("buy"),
            )
            return

        user_state[user_id] = {
            "step": "buy_payment_ready",
            "product_id": state["product_id"],
            "qty": state["qty"],
            "total": state["total"],
            "network": network_label,
            "crypto_payment_id": np_record.get("payment_id") or np_record.get("uuid"),
            "payment_provider": np_record.get("provider"),
        }
        await send_inline_from_callback(
            query,
            render_gateway_payment_text(np_record),
            paymod.payment_request_keyboard("buypay"),
        )
        return

    if data == "buypay_change_network":
        user_state[user_id]["step"] = "buy_network"
        await send_inline_from_callback(query, "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:", paymod.network_keyboard("buy"))
        return

    if data == "buypay_verify":
        context.application.create_task(run_gateway_manual_verify(query, user_id, "order"))
        return

    if data == "buymanual_submitted":
        state = user_state[user_id]
        product_id = state.get("product_id")
        qty = state.get("qty")
        total = state.get("total")
        if product_id and qty and total:
            add_order_record(user_id, product_id, qty, total, "Waiting Manual Confirmation", "Manual")
            add_transaction_record(user_id, "Order Payment", total, "Waiting Manual Confirmation", {"product_id": product_id, "qty": qty})
        user_state[user_id] = {"step": "main"}
        await send_user_inline_with_style_fallback(
            query,
            "✅ <b>Submitted.</b>\n\nSend payment screenshot to Live Support for confirmation.",
            user_back_to_menu_keyboard(),
            user_back_to_menu_keyboard(styled=False),
        )
        return

    if data == "buymanual_cancel":
        user_state[user_id] = {"step": "main"}
        await send_user_inline_with_style_fallback(
            query,
            "❌ <b>Order cancelled.</b>",
            user_back_to_menu_keyboard(),
            user_back_to_menu_keyboard(styled=False),
        )
        return

    if data.startswith("dep_amt_"):
        amount = float(data.replace("dep_amt_", ""))
        user_state[user_id] = {"step": "deposit_payment_method", "amount": amount}
        await send_user_inline_with_style_fallback(
            query,
            render_deposit_method_text(amount),
            payment_method_keyboard("dep", styled=True),
            payment_method_keyboard("dep", styled=False),
        )
        return

    if data == "dep_custom":
        user_state[user_id] = {"step": "deposit_custom_amount"}
        await send_user_inline_with_style_fallback(
            query,
            "✏️ Send custom deposit amount.\nExample: 25",
            deposit_custom_amount_keyboard(),
            deposit_custom_amount_keyboard(styled=False),
        )
        return

    if data == "dep_back":
        user_state[user_id] = {"step": "deposit_amount"}
        await send_user_inline_with_style_fallback(
            query,
            render_deposit_text(),
            deposit_amount_keyboard(),
            deposit_amount_keyboard(styled=False),
        )
        return

    if data == "dep_method_binance":
        amount = user_state.get(user_id, {}).get("amount")
        if amount is None:
            await query.answer("Please start a new deposit request.", show_alert=True)
            return
        await send_user_inline_with_style_fallback(
            query,
            render_manual_payment_text(amount, "Binance ID", BINANCE_ID),
            deposit_manual_keyboard(),
            deposit_manual_keyboard(styled=False),
        )
        return

    if data == "dep_method_bybit":
        amount = user_state.get(user_id, {}).get("amount")
        if amount is None:
            await query.answer("Please start a new deposit request.", show_alert=True)
            return
        await send_user_inline_with_style_fallback(
            query,
            render_manual_payment_text(amount, "Bybit ID", BYBIT_ID),
            deposit_manual_keyboard(),
            deposit_manual_keyboard(styled=False),
        )
        return

    if data == "dep_method_crypto":
        user_state[user_id]["step"] = "deposit_network"
        await send_user_inline_with_style_fallback(
            query,
            "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:",
            network_keyboard("dep", styled=True),
            network_keyboard("dep", styled=False),
        )
        return

    if data == "dep_back_method":
        amount = user_state.get(user_id, {}).get("amount")
        if amount is None:
            await query.answer("Please start a new deposit request.", show_alert=True)
            return
        user_state[user_id]["step"] = "deposit_payment_method"
        await send_user_inline_with_style_fallback(
            query,
            render_deposit_method_text(amount),
            payment_method_keyboard("dep", styled=True),
            payment_method_keyboard("dep", styled=False),
        )
        return

    if data.startswith("dep_net_"):
        network_label = paymod.map_network_callback_to_label(data.replace("dep_net_", ""))
        amount = user_state.get(user_id, {}).get("amount")
        if amount is None:
            await query.answer("Please start a new deposit request.", show_alert=True)
            return
        ref = f"deposit:{user_id}:{amount}"
        try:
            np_record = create_gateway_payment(
                user_id=user_id,
                kind="deposit",
                usd_amount=amount,
                network_label=network_label,
                ref=ref,
            )
        except Exception as e:
            print("Crypto deposit create error:", e)
            await send_user_inline_with_style_fallback(
                query,
                f"❌ <b>Could not create crypto payment.</b>\n\nPlease try again or contact live support: {SUPPORT_USERNAME}",
                network_keyboard("dep", styled=True),
                network_keyboard("dep", styled=False),
            )
            return

        user_state[user_id] = {
            "step": "deposit_payment_ready",
            "amount": amount,
            "network": network_label,
            "crypto_payment_id": np_record.get("payment_id") or np_record.get("uuid"),
            "payment_provider": np_record.get("provider"),
        }
        await send_user_inline_with_style_fallback(
            query,
            render_gateway_payment_text(np_record),
            deposit_payment_request_keyboard(),
            deposit_payment_request_keyboard(styled=False),
        )
        return

    if data == "retry_verify_after_timeout":
        record = wc.get_user_pending_any(user_id)
        if record and record.get("kind") == "order":
            context.application.create_task(run_gateway_manual_verify(query, user_id, "order"))
        else:
            context.application.create_task(run_gateway_manual_verify(query, user_id, "deposit"))
        return

    if data == "deppay_change_network":
        user_state[user_id]["step"] = "deposit_network"
        await send_user_inline_with_style_fallback(
            query,
            "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:",
            network_keyboard("dep", styled=True),
            network_keyboard("dep", styled=False),
        )
        return

    if data == "deppay_back_network":
        user_state[user_id]["step"] = "deposit_network"
        await send_user_inline_with_style_fallback(
            query,
            "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:",
            network_keyboard("dep", styled=True),
            network_keyboard("dep", styled=False),
        )
        return

    if data == "deppay_verify":
        context.application.create_task(run_gateway_manual_verify(query, user_id, "deposit"))
        return

    if data == "depmanual_submitted":
        amount = user_state[user_id].get("amount", 0)
        add_transaction_record(user_id, "Deposit", amount, "Waiting Manual Confirmation")
        user_state[user_id] = {"step": "main"}
        await send_user_inline_with_style_fallback(
            query,
            "✅ <b>Submitted.</b>\n\nSend payment screenshot to Live Support for confirmation.",
            user_back_to_menu_keyboard(),
            user_back_to_menu_keyboard(styled=False),
        )
        return

    if data == "depmanual_cancel":
        user_state[user_id] = {"step": "main"}
        await send_user_inline_with_style_fallback(
            query,
            "❌ <b>Deposit cancelled.</b>",
            user_back_to_menu_keyboard(),
            user_back_to_menu_keyboard(styled=False),
        )
        return


# =========================
# CATEGORY ICON STICKER HANDLER
# =========================
async def handle_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id, update.effective_user)
    if user_mode.get(user_id) != "admin":
        return False
    step = user_state.get(user_id, {}).get("step")
    if step in {"dashboard_emoji_input", "dashboard_header_emoji_input"}:
        return await handle_dashboard_emoji_message(update, user_id)
    if step not in {"category_add_icon", "category_icon_input"}:
        return False
    return await handle_category_icon_message(update, user_id, step)


# =========================
# TXT STOCK UPLOAD HANDLER
# =========================
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id, update.effective_user)

    if user_id not in ADMIN_IDS:
        return

    state = user_state.get(user_id, {})
    step = state.get("step")
    if step in {"dashboard_emoji_input", "dashboard_header_emoji_input"}:
        await handle_dashboard_emoji_message(update, user_id)
        return
    if step not in ("stock_add_single_input", "stock_add_bulk_input"):
        await update.message.reply_text("❌ Please open Stock > Add Bulk Accounts first, then upload the .txt file.", parse_mode="HTML")
        return

    product_id = admin_temp.get(user_id, {}).get("selected_product_id")
    if not product_id or product_id not in PRODUCTS:
        user_state[user_id] = {"step": "admin_stock"}
        reset_admin_temp(user_id)
        await update.message.reply_text("❌ Product not found.", reply_markup=admin_menu())
        return

    doc = update.message.document
    file_name = str(getattr(doc, "file_name", "") or "").lower()
    mime_type = str(getattr(doc, "mime_type", "") or "").lower()

    if not (file_name.endswith(".txt") or mime_type.startswith("text/")):
        await update.message.reply_text("❌ Please upload a .txt file only.", parse_mode="HTML")
        return

    try:
        tg_file = await context.bot.get_file(doc.file_id)
        data = await tg_file.download_as_bytearray()
        try:
            content = bytes(data).decode("utf-8")
        except UnicodeDecodeError:
            content = bytes(data).decode("latin-1")
    except Exception as e:
        print("⚠️ Failed to read txt stock file:", e)
        await update.message.reply_text("❌ Could not read this .txt file. Please try again.", parse_mode="HTML")
        return

    lines = [line.strip() for line in content.splitlines() if line.strip()]
    added = 0
    for line in lines:
        account = parse_account_line(line)
        if account:
            PRODUCTS[product_id]["accounts"].append(account)
            added += 1

    if added == 0:
        await update.message.reply_text("❌ <b>No valid stock line found in the file.</b>", parse_mode="HTML")
        return

    PRODUCTS[product_id]["display_stock"] = max(get_display_stock(product_id), get_product_stock(product_id))
    user_state[user_id] = {"step": "admin_stock"}
    reset_admin_temp(user_id)

    await update.message.reply_text(
        f"✅ <b>TXT stock imported successfully.</b>\n\n"
        f"<b>Product:</b> {PRODUCTS[product_id]['name']}\n"
        f"<b>Added:</b> {added}\n"
        f"<b>Real Stock:</b> {get_product_stock(product_id)} pcs\n"
        f"<b>Display Stock:</b> {get_display_stock(product_id)} pcs",
        reply_markup=admin_menu(),
        parse_mode="HTML",
    )
    await notify_waiters_for_product(context, product_id)


async def handle_dashboard_emoji_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id, update.effective_user)
    if not is_admin(user_id):
        return False
    if user_state.get(user_id, {}).get("step") not in {"dashboard_emoji_input", "dashboard_header_emoji_input"}:
        return False
    return await handle_dashboard_emoji_message(update, user_id)


# =========================
# PERSISTENCE WRAPPERS
# =========================
async def start_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await start(update, context)
    finally:
        save_bot_state()


async def client_menu_command_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await client_menu_command(update, context)
    finally:
        save_bot_state()


async def admin_command_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await admin_command(update, context)
    finally:
        save_bot_state()


async def addstock_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await addstock(update, context)
    finally:
        save_bot_state()


async def handle_text_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await handle_text(update, context)
    finally:
        save_bot_state()


async def handle_sticker_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await handle_sticker(update, context):
        save_bot_state()


async def handle_document_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await handle_document(update, context)
    finally:
        save_bot_state()


async def handle_dashboard_emoji_media_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await handle_dashboard_emoji_media(update, context):
        save_bot_state()


async def handle_callback_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await handle_callback(update, context)
    finally:
        save_bot_state()


# =========================
# MAIN APP
# =========================
def main():
    global app_instance
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app_instance = app

    app.add_handler(CommandHandler("start", start_persistent))
    app.add_handler(CommandHandler(["shop", "wallet", "topup", "orders", "support"], client_menu_command_persistent))
    app.add_handler(CommandHandler("admin", admin_command_persistent))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("checkchannel", checkchannel_command))
    app.add_handler(CommandHandler("checkmember", checkmember_command))
    app.add_handler(CommandHandler("addstock", addstock_persistent))

    app.add_handler(CallbackQueryHandler(handle_callback_persistent))
    app.add_handler(MessageHandler(filters.Sticker.ALL, handle_sticker_persistent))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document_persistent))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION, handle_dashboard_emoji_media_persistent))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_persistent))

    # Old custom blockchain scanner disabled. NOWPayments webhook is the main verifier.
    # app.job_queue.run_repeating(background_job, interval=60, first=30)

    print("✅ Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
