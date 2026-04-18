# =========================
# PART 1 OF 10 - START
# Paste this at the TOP of your bot file
# =========================

import os
import re
import json
import time
import math
import uuid
import hashlib
import random
import sqlite3
import string
import requests

from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime, timedelta, timezone

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    CopyTextButton,
)

from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =========================
# BASIC SETTINGS
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

BOT_USERNAME = os.getenv("BOT_USERNAME", "SupremeLeaderShopBot").replace("@", "").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@serpstacking").strip()

# Comma separated admin ids in Railway Variable: ADMIN_IDS=123,456
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "6795246172").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in ADMIN_IDS_RAW.split(",")
    if x.strip().isdigit()
}

BINANCE_ID = os.getenv("BINANCE_ID", "828543482").strip()
BYBIT_ID = os.getenv("BYBIT_ID", "199582741").strip()

TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "").strip()
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()

# Optional live pricing API
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

TRONGRID_BASE = "https://api.trongrid.io"
ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
BTC_API_BASE = "https://mempool.space/api"
LTC_API_BASE = "https://litecoinspace.org/api"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing. Please set BOT_TOKEN in Railway Variables.")

# =========================
# GLOBAL APP RULES
# =========================

APP_TIMEZONE = timezone.utc
TIME_DISPLAY_FORMAT = "%Y-%m-%d %I:%M:%S %p UTC"

# payment request validity
PAYMENT_REQUEST_EXPIRE_MINUTES = 30

# verify button background recheck support
RECHECK_INTERVAL_SECONDS = 20
MAX_RECHECK_ATTEMPTS = 12

# unique extra charge rule requested by you
MIN_UNIQUE_EXTRA = Decimal("0.10")
MAX_UNIQUE_EXTRA = Decimal("0.30")

# small safety tolerance
USD_TOLERANCE = Decimal("0.03")

# network confirmation display text
NETWORK_CONFIRMATION_GUIDE = {
    "USDT (TRC20)": "Usually 1-5 minutes",
    "USDT (ERC20)": "Usually 1-10 minutes",
    "USDT (BEP20)": "Usually 1-5 minutes",
    "TRX (TRC20)": "Usually 1-5 minutes",
    "BTC": "Usually 10-60 minutes",
    "LTC": "Usually 5-30 minutes",
    "ETH (ERC20)": "Usually 1-10 minutes",
    "BNB (BEP20)": "Usually 1-5 minutes",
    "SOL": "Usually 1-5 minutes",
}

# =========================
# DATABASE
# =========================

DB_PATH = os.getenv("DB_PATH", "shopbot.sqlite3").strip()

# =========================
# WALLET / RECEIVING ADDRESSES
# =========================

USDT_TRC20_RECEIVE_ADDRESS = os.getenv("USDT_TRC20_RECEIVE_ADDRESS", "TFWMEL6o5Kxnh1h25XMuWG6b6HaeF7vNf1").strip()
USDT_ERC20_RECEIVE_ADDRESS = os.getenv("USDT_ERC20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()
USDT_BEP20_RECEIVE_ADDRESS = os.getenv("USDT_BEP20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()
LTC_RECEIVE_ADDRESS = os.getenv("LTC_RECEIVE_ADDRESS", "LQcmsEwAHuyWyY3Heu2XMYShfirxomCVtk").strip()
BTC_RECEIVE_ADDRESS = os.getenv("BTC_RECEIVE_ADDRESS", "15ykQZeq9jQTjJEzY2faG4LpirS2bYcb8L").strip()
BNB_BEP20_RECEIVE_ADDRESS = os.getenv("BNB_BEP20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()
SOL_RECEIVE_ADDRESS = os.getenv("SOL_RECEIVE_ADDRESS", "23MdGndZ85eJR58JWHiHNFmrQDMU1Leipzhnx4wtgnWE").strip()
TRX_RECEIVE_ADDRESS = os.getenv("TRX_RECEIVE_ADDRESS", "TFWMEL6o5Kxnh1h25XMuWG6b6HaeF7vNf1").strip()
ETH_ERC20_RECEIVE_ADDRESS = os.getenv("ETH_ERC20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()

# =========================
# CONTRACTS / CHAIN CONFIG
# =========================

USDT_TRC20_CONTRACT = os.getenv("USDT_TRC20_CONTRACT", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").strip()
USDT_ERC20_CONTRACT = os.getenv("USDT_ERC20_CONTRACT", "0xdAC17F958D2ee523a2206206994597C13D831ec7").strip()
USDT_BEP20_CONTRACT = os.getenv("USDT_BEP20_CONTRACT", "0x55d398326f99059fF775485246999027B3197955").strip()

ETH_CHAIN_ID = "1"
BSC_CHAIN_ID = "56"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

CRYPTO_ADDRESSES = {
    "USDT (TRC20)": USDT_TRC20_RECEIVE_ADDRESS,
    "USDT (ERC20)": USDT_ERC20_RECEIVE_ADDRESS,
    "USDT (BEP20)": USDT_BEP20_RECEIVE_ADDRESS,
    "TRX (TRC20)": TRX_RECEIVE_ADDRESS,
    "BTC": BTC_RECEIVE_ADDRESS,
    "LTC": LTC_RECEIVE_ADDRESS,
    "ETH (ERC20)": ETH_ERC20_RECEIVE_ADDRESS,
    "BNB (BEP20)": BNB_BEP20_RECEIVE_ADDRESS,
    "SOL": SOL_RECEIVE_ADDRESS,
}

NETWORK_SYMBOL_MAP = {
    "USDT (TRC20)": "USDT",
    "USDT (ERC20)": "USDT",
    "USDT (BEP20)": "USDT",
    "TRX (TRC20)": "TRX",
    "BTC": "BTC",
    "LTC": "LTC",
    "ETH (ERC20)": "ETH",
    "BNB (BEP20)": "BNB",
    "SOL": "SOL",
}

NETWORK_PRICE_ID_MAP = {
    "USDT (TRC20)": "tether",
    "USDT (ERC20)": "tether",
    "USDT (BEP20)": "tether",
    "TRX (TRC20)": "tron",
    "BTC": "bitcoin",
    "LTC": "litecoin",
    "ETH (ERC20)": "ethereum",
    "BNB (BEP20)": "binancecoin",
    "SOL": "solana",
}

NETWORK_DECIMALS = {
    "USDT (TRC20)": 6,
    "USDT (ERC20)": 6,
    "USDT (BEP20)": 18,
    "TRX (TRC20)": 6,
    "BTC": 8,
    "LTC": 8,
    "ETH (ERC20)": 18,
    "BNB (BEP20)": 18,
    "SOL": 9,
}

# =========================
# PRODUCT SEED DATA
# =========================

DEFAULT_PRODUCTS = {
    "p1": {
        "name": "Netflix Premium Account",
        "icon": "🎬",
        "month": "1",
        "price": 5.0,
        "details": [
            "✅ Private account access",
            "✅ Automatic delivery",
            "✅ Email and password delivery",
        ],
        "accounts": [
            {"email": "netflix1@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix2@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix3@example.com", "password": "Pass1234", "note": "Private Account"},
        ],
        "display_stock": 25,
    },
    "p2": {
        "name": "Spotify Premium Account",
        "icon": "🎵",
        "month": "1",
        "price": 3.0,
        "details": [
            "✅ Private account access",
            "✅ Automatic delivery",
            "✅ Email and password delivery",
        ],
        "accounts": [
            {"email": "spotify1@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "spotify2@example.com", "password": "Pass1234", "note": "Private Account"},
        ],
        "display_stock": 18,
    },
    "p3": {
        "name": "YouTube Premium Account",
        "icon": "▶️",
        "month": "1",
        "price": 4.0,
        "details": [
            "✅ Private account access",
            "✅ Automatic delivery",
            "✅ Email and password delivery",
        ],
        "accounts": [],
        "display_stock": 0,
    },
}

DEFAULT_PRODUCT_ORDER = ["p1", "p2", "p3"]

DEFAULT_PROMOS = {
    "FREE5": {
        "amount": 5.0,
        "enabled": True,
        "one_time": True,
        "created_by": "system",
    },
    "BONUS10": {
        "amount": 10.0,
        "enabled": True,
        "one_time": True,
        "created_by": "system",
    },
}

# =========================
# IN-MEMORY STATE
# =========================

user_state = {}
user_mode = {}
admin_temp = {}
notify_waitlist = {}
rate_cache = {
    "prices": {},
    "updated_at": None,
}

# =========================
# DB CORE
# =========================

def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc():
    return datetime.now(APP_TIMEZONE)


def now_iso():
    return now_utc().isoformat()


def parse_iso(dt_text):
    if not dt_text:
        return None
    try:
        return datetime.fromisoformat(dt_text)
    except Exception:
        return None


def format_dt(dt_value):
    if not dt_value:
        return "N/A"

    if isinstance(dt_value, str):
        parsed = parse_iso(dt_value)
        if parsed is None:
            return dt_value
        return parsed.astimezone(APP_TIMEZONE).strftime(TIME_DISPLAY_FORMAT)

    if isinstance(dt_value, datetime):
        return dt_value.astimezone(APP_TIMEZONE).strftime(TIME_DISPLAY_FORMAT)

    return str(dt_value)


def init_db():
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            wallet_balance TEXT NOT NULL DEFAULT '0',
            referred_by TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_meta (
            user_id INTEGER PRIMARY KEY,
            mode TEXT NOT NULL DEFAULT 'client',
            state_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            icon TEXT NOT NULL,
            month TEXT NOT NULL,
            price TEXT NOT NULL,
            details_json TEXT NOT NULL,
            display_stock INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            email TEXT NOT NULL,
            password TEXT NOT NULL,
            note TEXT,
            is_delivered INTEGER NOT NULL DEFAULT 0,
            delivered_to_user_id INTEGER,
            delivered_order_id INTEGER,
            delivered_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            one_time INTEGER NOT NULL DEFAULT 1,
            created_by TEXT,
            used_by TEXT,
            used_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_usages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            amount TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id TEXT NOT NULL,
            product_name TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_price TEXT NOT NULL,
            base_total_usd TEXT NOT NULL,
            payable_total_usd TEXT NOT NULL,
            payment_type TEXT NOT NULL,
            status TEXT NOT NULL,
            invoice_id TEXT,
            created_at TEXT NOT NULL,
            paid_at TEXT,
            delivered_at TEXT,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tx_type TEXT NOT NULL,
            amount_usd TEXT NOT NULL,
            network TEXT,
            coin_symbol TEXT,
            coin_amount TEXT,
            status TEXT NOT NULL,
            invoice_id TEXT,
            blockchain_txid TEXT,
            note TEXT,
            created_at TEXT NOT NULL,
            confirmed_at TEXT,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            invoice_type TEXT NOT NULL,
            related_order_id INTEGER,
            amount_base_usd TEXT NOT NULL,
            amount_extra_usd TEXT NOT NULL,
            amount_payable_usd TEXT NOT NULL,
            network TEXT NOT NULL,
            coin_symbol TEXT NOT NULL,
            coin_amount TEXT NOT NULL,
            deposit_address TEXT NOT NULL,
            status TEXT NOT NULL,
            blockchain_txid TEXT,
            blockchain_status TEXT,
            problem_reason TEXT,
            verify_attempts INTEGER NOT NULL DEFAULT 0,
            used_tx_unique_key TEXT,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            paid_at TEXT,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS used_chain_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            network TEXT NOT NULL,
            unique_key TEXT NOT NULL UNIQUE,
            txid TEXT,
            invoice_id TEXT NOT NULL,
            used_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def seed_default_data():
    conn = db_conn()
    cur = conn.cursor()

    # products
    existing_count = cur.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
    if existing_count == 0:
        for idx, product_id in enumerate(DEFAULT_PRODUCT_ORDER, start=1):
            p = DEFAULT_PRODUCTS[product_id]
            created_at = now_iso()

            cur.execute("""
                INSERT INTO products (
                    product_id, name, icon, month, price, details_json,
                    display_stock, sort_order, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                product_id,
                p["name"],
                p["icon"],
                p["month"],
                str(p["price"]),
                json.dumps(p["details"]),
                int(p["display_stock"]),
                idx,
                created_at,
                created_at,
            ))

            for acc in p["accounts"]:
                cur.execute("""
                    INSERT INTO stock_accounts (
                        product_id, email, password, note,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    product_id,
                    acc["email"],
                    acc["password"],
                    acc.get("note", ""),
                    created_at,
                    created_at,
                ))

    # promo codes
    promo_count = cur.execute("SELECT COUNT(*) AS c FROM promo_codes").fetchone()["c"]
    if promo_count == 0:
        for code, data in DEFAULT_PROMOS.items():
            created_at = now_iso()
            cur.execute("""
                INSERT INTO promo_codes (
                    code, amount, enabled, one_time, created_by,
                    used_by, used_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
                str(data["amount"]),
                1 if data["enabled"] else 0,
                1 if data["one_time"] else 0,
                data["created_by"],
                None,
                None,
                created_at,
                created_at,
            ))

    conn.commit()
    conn.close()

# =========================
# GENERIC HELPERS
# =========================

def safe_decimal(value, default=None):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def decimal_to_str(value, places=8):
    if not isinstance(value, Decimal):
        value = safe_decimal(value, Decimal("0"))
    q = Decimal("1") if places == 0 else Decimal(f"1.{'0' * places}")
    return str(value.quantize(q, rounding=ROUND_DOWN))


def money_2(value):
    dec = safe_decimal(value, Decimal("0"))
    return dec.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def format_money(value):
    dec = money_2(value)
    return f"${dec}"


def escape_html(text):
    return (
        str(text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def normalize_evm_address(addr):
    return str(addr or "").strip().lower()


def to_evm_topic_address(addr):
    return "0x" + normalize_evm_address(addr).replace("0x", "").rjust(64, "0")


def is_admin(user_id):
    return int(user_id) in ADMIN_IDS


def random_invoice_id():
    return "INV-" + uuid.uuid4().hex[:14].upper()


def random_unique_extra():
    # 0.10 to 0.30
    candidates = []
    value = MIN_UNIQUE_EXTRA
    while value <= MAX_UNIQUE_EXTRA:
        candidates.append(value.quantize(Decimal("0.01")))
        value += Decimal("0.01")
    return random.choice(candidates)


def amount_within_tolerance(actual_amount, expected_amount, tolerance=USD_TOLERANCE):
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)

    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False

    return abs(actual_dec - expected_dec) <= tolerance_dec


def future_iso(minutes):
    return (now_utc() + timedelta(minutes=minutes)).isoformat()


def dt_is_expired(dt_text):
    parsed = parse_iso(dt_text)
    if not parsed:
        return True
    return now_utc() > parsed


def http_get_json(url, params=None, headers=None, timeout=25):
    try:
        res = requests.get(url, params=params, headers=headers, timeout=timeout)
        data = {}
        if res.content:
            try:
                data = res.json()
            except Exception:
                data = {"raw_text": res.text}
        return {"ok": res.ok, "status_code": res.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def http_post_json(url, payload=None, headers=None, timeout=25):
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        data = {}
        if res.content:
            try:
                data = res.json()
            except Exception:
                data = {"raw_text": res.text}
        return {"ok": res.ok, "status_code": res.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def verify_result(ok, status, reason, extra=None):
    return {
        "ok": ok,
        "status": status,
        "reason": reason,
        "extra": extra or {},
    }

# =========================
# USER / STATE HELPERS
# =========================

def ensure_user(user_id):
    user_id = int(user_id)
    conn = db_conn()
    cur = conn.cursor()

    existing = cur.execute(
        "SELECT user_id FROM users WHERE user_id = ?",
        (user_id,)
    ).fetchone()

    if not existing:
        now_text = now_iso()
        cur.execute("""
            INSERT INTO users (user_id, wallet_balance, referred_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            user_id,
            "0",
            None,
            now_text,
            now_text,
        ))

    existing_meta = cur.execute(
        "SELECT user_id FROM user_meta WHERE user_id = ?",
        (user_id,)
    ).fetchone()

    if not existing_meta:
        now_text = now_iso()
        cur.execute("""
            INSERT INTO user_meta (user_id, mode, state_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            user_id,
            "client",
            json.dumps({"step": "main"}),
            now_text,
            now_text,
        ))

    conn.commit()
    conn.close()


def get_user_mode(user_id):
    conn = db_conn()
    row = conn.execute(
        "SELECT mode FROM user_meta WHERE user_id = ?",
        (int(user_id),)
    ).fetchone()
    conn.close()
    if not row:
        return "client"
    return row["mode"] or "client"


def set_user_mode(user_id, mode):
    conn = db_conn()
    conn.execute("""
        UPDATE user_meta
        SET mode = ?, updated_at = ?
        WHERE user_id = ?
    """, (
        mode,
        now_iso(),
        int(user_id),
    ))
    conn.commit()
    conn.close()


def get_user_state(user_id):
    conn = db_conn()
    row = conn.execute(
        "SELECT state_json FROM user_meta WHERE user_id = ?",
        (int(user_id),)
    ).fetchone()
    conn.close()

    if not row:
        return {"step": "main"}

    try:
        data = json.loads(row["state_json"] or "{}")
        if not isinstance(data, dict):
            return {"step": "main"}
        return data
    except Exception:
        return {"step": "main"}


def set_user_state(user_id, state_dict):
    conn = db_conn()
    conn.execute("""
        UPDATE user_meta
        SET state_json = ?, updated_at = ?
        WHERE user_id = ?
    """, (
        json.dumps(state_dict or {}),
        now_iso(),
        int(user_id),
    ))
    conn.commit()
    conn.close()


def reset_admin_temp(user_id):
    admin_temp[int(user_id)] = {}


def enter_client_mode(user_id):
    set_user_mode(user_id, "client")
    set_user_state(user_id, {"step": "main"})
    reset_admin_temp(user_id)


def enter_admin_mode(user_id):
    set_user_mode(user_id, "admin")
    set_user_state(user_id, {"step": "admin_main"})
    reset_admin_temp(user_id)


def get_wallet_balance(user_id):
    conn = db_conn()
    row = conn.execute(
        "SELECT wallet_balance FROM users WHERE user_id = ?",
        (int(user_id),)
    ).fetchone()
    conn.close()

    if not row:
        return Decimal("0")
    return safe_decimal(row["wallet_balance"], Decimal("0"))


def set_wallet_balance(user_id, amount):
    dec = safe_decimal(amount, Decimal("0"))
    conn = db_conn()
    conn.execute("""
        UPDATE users
        SET wallet_balance = ?, updated_at = ?
        WHERE user_id = ?
    """, (
        str(dec),
        now_iso(),
        int(user_id),
    ))
    conn.commit()
    conn.close()


def add_wallet_balance(user_id, amount):
    current = get_wallet_balance(user_id)
    new_balance = current + safe_decimal(amount, Decimal("0"))
    set_wallet_balance(user_id, new_balance)
    return new_balance


def subtract_wallet_balance(user_id, amount):
    current = get_wallet_balance(user_id)
    dec = safe_decimal(amount, Decimal("0"))
    if current < dec:
        return False, current
    new_balance = current - dec
    set_wallet_balance(user_id, new_balance)
    return True, new_balance

# =========================
# PRODUCT / STOCK HELPERS
# =========================

def get_product_order():
    conn = db_conn()
    rows = conn.execute("""
        SELECT product_id
        FROM products
        ORDER BY sort_order ASC, product_id ASC
    """).fetchall()
    conn.close()
    return [row["product_id"] for row in rows]


def get_product(product_id):
    conn = db_conn()
    row = conn.execute("""
        SELECT *
        FROM products
        WHERE product_id = ?
    """, (product_id,)).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "product_id": row["product_id"],
        "name": row["name"],
        "icon": row["icon"],
        "month": row["month"],
        "price": float(row["price"]),
        "details": json.loads(row["details_json"] or "[]"),
        "display_stock": int(row["display_stock"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def get_all_products():
    items = []
    for pid in get_product_order():
        p = get_product(pid)
        if p:
            items.append(p)
    return items


def get_real_stock_count(product_id):
    conn = db_conn()
    row = conn.execute("""
        SELECT COUNT(*) AS c
        FROM stock_accounts
        WHERE product_id = ? AND is_delivered = 0
    """, (product_id,)).fetchone()
    conn.close()
    return int(row["c"] if row else 0)


def get_display_stock(product_id):
    p = get_product(product_id)
    if not p:
        return 0
    return int(p["display_stock"])


def generate_new_product_id():
    conn = db_conn()
    rows = conn.execute("SELECT product_id FROM products").fetchall()
    conn.close()
    existing = {row["product_id"] for row in rows}

    n = 1
    while True:
        candidate = f"p{n}"
        if candidate not in existing:
            return candidate
        n += 1


def parse_account_line(line):
    parts = [x.strip() for x in str(line).split("|")]
    if len(parts) < 2:
        return None

    email = parts[0]
    password = parts[1]
    note = parts[2] if len(parts) >= 3 else ""

    if not email or not password:
        return None

    return {
        "email": email,
        "password": password,
        "note": note,
    }

# =========================
# PROMO HELPERS
# =========================

def generate_unique_promo_code(length=10):
    alphabet = string.ascii_uppercase + string.digits
    conn = db_conn()
    existing = {
        row["code"]
        for row in conn.execute("SELECT code FROM promo_codes").fetchall()
    }
    conn.close()

    while True:
        code = "".join(random.choice(alphabet) for _ in range(length))
        if code not in existing:
            return code


def has_user_used_promo(user_id, code):
    conn = db_conn()
    row = conn.execute("""
        SELECT id
        FROM promo_usages
        WHERE user_id = ? AND code = ?
        LIMIT 1
    """, (int(user_id), str(code).upper())).fetchone()
    conn.close()
    return row is not None

# =========================
# MENU LAYER
# =========================

def main_menu():
    keyboard = [
        ["🛍 Shop", "💰 Wallet"],
        ["💳 Top Up", "🎟 Promo"],
        ["📦 Orders", "🧾 Transactions"],
        ["🛠 Payment Checker", "📘 Bot Guideline"],
        ["🆔 User ID", "👥 Refer & Earn"],
        ["💬 Support"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def admin_menu():
    keyboard = [
        ["📦 Products", "📥 Stock"],
        ["🎟 Promo Admin", "📦 Orders Admin"],
        ["💳 Deposits Admin", "👤 Users Admin"],
        ["📊 Analytics", "🚪 Exit Admin"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def deposit_amount_keyboard():
    rows = [
        [
            InlineKeyboardButton("$5", callback_data="dep_amt_5"),
            InlineKeyboardButton("$10", callback_data="dep_amt_10"),
        ],
        [
            InlineKeyboardButton("$15", callback_data="dep_amt_15"),
            InlineKeyboardButton("$20", callback_data="dep_amt_20"),
        ],
        [InlineKeyboardButton("✏️ Custom Amount", callback_data="dep_custom")],
        [InlineKeyboardButton("⬅️ Close", callback_data="close_inline")],
    ]
    return InlineKeyboardMarkup(rows)


def payment_method_keyboard(prefix):
    rows = [
        [
            InlineKeyboardButton("🏦 Binance ID", callback_data=f"{prefix}_method_binance"),
            InlineKeyboardButton("🏦 Bybit ID", callback_data=f"{prefix}_method_bybit"),
        ],
        [InlineKeyboardButton("💸 Crypto Address", callback_data=f"{prefix}_method_crypto")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"{prefix}_back")],
    ]
    return InlineKeyboardMarkup(rows)


def network_keyboard(prefix):
    rows = [
        [
            InlineKeyboardButton("USDT (TRC20)", callback_data=f"{prefix}_net_USDT_TRC20"),
            InlineKeyboardButton("USDT (ERC20)", callback_data=f"{prefix}_net_USDT_ERC20"),
        ],
        [
            InlineKeyboardButton("USDT (BEP20)", callback_data=f"{prefix}_net_USDT_BEP20"),
            InlineKeyboardButton("TRX (TRC20)", callback_data=f"{prefix}_net_TRX_TRC20"),
        ],
        [
            InlineKeyboardButton("BTC", callback_data=f"{prefix}_net_BTC"),
            InlineKeyboardButton("LTC", callback_data=f"{prefix}_net_LTC"),
        ],
        [
            InlineKeyboardButton("ETH (ERC20)", callback_data=f"{prefix}_net_ETH_ERC20"),
            InlineKeyboardButton("BNB (BEP20)", callback_data=f"{prefix}_net_BNB_BEP20"),
        ],
        [InlineKeyboardButton("SOL", callback_data=f"{prefix}_net_SOL")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"{prefix}_back_method")],
    ]
    return InlineKeyboardMarkup(rows)


def buy_qty_keyboard(product_id):
    rows = [
        [
            InlineKeyboardButton("🛒 Buy 1x", callback_data=f"buy_qty_{product_id}_1"),
            InlineKeyboardButton("🛒 Buy 5x", callback_data=f"buy_qty_{product_id}_5"),
        ],
        [
            InlineKeyboardButton("🛒 Buy 10x", callback_data=f"buy_qty_{product_id}_10"),
            InlineKeyboardButton("✏️ Custom Qty", callback_data=f"buy_custom_{product_id}"),
        ],
        [InlineKeyboardButton("⬅️ Back to Shop", callback_data="back_shop_cards")],
    ]
    return InlineKeyboardMarkup(rows)


def close_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Close", callback_data="close_inline")]
    ])


def payment_invoice_keyboard(invoice_id, address_text):
    rows = [
        [InlineKeyboardButton("✅ I Have Paid (Verify)", callback_data=f"verify_invoice_{invoice_id}")],
        [InlineKeyboardButton("🔄 Refresh Status", callback_data=f"refresh_invoice_{invoice_id}")],
        [InlineKeyboardButton("❌ Cancel Request", callback_data=f"cancel_invoice_{invoice_id}")],
    ]

    # Try copy button if available in user's Telegram client.
    try:
        rows.insert(0, [InlineKeyboardButton("📋 Copy Payment Address", copy_text=CopyTextButton(address_text))])
    except Exception:
        # Fallback button if copy API not supported
        rows.insert(0, [InlineKeyboardButton("📋 Copy Payment Address", callback_data=f"copy_address_hint_{invoice_id}")])

    return InlineKeyboardMarkup(rows)


def guideline_keyboard():
    rows = [
        [InlineKeyboardButton("🛍 How To Buy Product", callback_data="guide_buy")],
        [InlineKeyboardButton("💳 How To Top Up", callback_data="guide_topup")],
        [InlineKeyboardButton("🎟 How To Use Promo", callback_data="guide_promo")],
        [InlineKeyboardButton("🛠 Payment Help", callback_data="guide_payment")],
        [InlineKeyboardButton("📦 Order Delivery Info", callback_data="guide_delivery")],
        [InlineKeyboardButton("⬅️ Close", callback_data="close_inline")],
    ]
    return InlineKeyboardMarkup(rows)


def payment_checker_keyboard():
    rows = [
        [InlineKeyboardButton("🔎 Check By TXID", callback_data="payment_checker_txid")],
        [InlineKeyboardButton("📄 View My Open Requests", callback_data="payment_checker_requests")],
        [InlineKeyboardButton("⬅️ Close", callback_data="close_inline")],
    ]
    return InlineKeyboardMarkup(rows)

# =========================
# PART 1 OF 10 - END
# =========================

# =========================
# PART 2 OF 10 - START
# Paste this directly BELOW Part 1
# =========================

# =========================
# RENDER / UI TEXT HELPERS
# =========================

def render_home_text():
    return (
        "👑 <b>SupremeLeader Premium Shop</b>\n\n"
        "Welcome to your premium digital marketplace.\n"
        "Use the menu below to browse products, top up your wallet, "
        "track payments, and manage your orders."
    )


def render_wallet_text(user_id):
    balance = get_wallet_balance(user_id)
    return (
        "💰 <b>WALLET OVERVIEW</b>\n\n"
        f"<b>Available Balance:</b> {format_money(balance)}\n"
        f"<b>Checked At:</b> {format_dt(now_iso())}"
    )


def render_user_id_text(user_id):
    return (
        "🆔 <b>YOUR USER ID</b>\n\n"
        f"<code>{user_id}</code>\n\n"
        "Keep this ID safe. You may need it for support or admin confirmation."
    )


def render_support_text():
    return (
        "💬 <b>SUPPORT</b>\n\n"
        f"If you need help, contact: {escape_html(SUPPORT_USERNAME)}\n\n"
        f"<b>Checked At:</b> {format_dt(now_iso())}"
    )


def render_refer_text(user_id):
    ref_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    return (
        "👥 <b>REFER & EARN</b>\n\n"
        "Invite your friends using your personal referral link.\n\n"
        f"<b>Your Link:</b>\n{escape_html(ref_link)}\n\n"
        "Referral rewards can be expanded later.\n"
        f"<b>Generated At:</b> {format_dt(now_iso())}"
    )


def render_product_card(product_id):
    product = get_product(product_id)
    if not product:
        return "❌ Product not found."

    display_stock = get_display_stock(product_id)
    stock_text = f"{display_stock} pcs" if display_stock > 0 else "Out of stock"

    return (
        f"{escape_html(product['icon'])} <b>{escape_html(product['name'])}</b>\n"
        f"<b>Plan:</b> {escape_html(product['month'])} month\n"
        f"<b>Price:</b> {format_money(product['price'])}\n"
        f"<b>Visible Stock:</b> {stock_text}"
    )


def render_product_details(product_id):
    product = get_product(product_id)
    if not product:
        return "❌ Product not found."

    detail_lines = "\n".join(product["details"]) if product["details"] else "No details available."
    display_stock = get_display_stock(product_id)
    real_stock = get_real_stock_count(product_id)

    return (
        "📦 <b>PRODUCT DETAILS</b>\n\n"
        f"<b>Icon:</b> {escape_html(product['icon'])}\n"
        f"<b>Name:</b> {escape_html(product['name'])}\n"
        f"<b>Plan:</b> {escape_html(product['month'])} month\n"
        f"<b>Unit Price:</b> {format_money(product['price'])}\n"
        f"<b>Display Stock:</b> {display_stock} pcs\n"
        f"<b>Real Stock:</b> {real_stock} pcs\n\n"
        f"{detail_lines}\n\n"
        "<b>Select a quantity below:</b>"
    )


def render_buy_summary(product_id, qty, wallet_balance):
    product = get_product(product_id)
    if not product:
        return "❌ Product not found."

    total = money_2(Decimal(str(product["price"])) * Decimal(str(qty)))
    wallet_dec = safe_decimal(wallet_balance, Decimal("0"))

    if wallet_dec >= total:
        remaining = wallet_dec - total
        return (
            "🛒 <b>ORDER SUMMARY</b>\n\n"
            f"<b>Product:</b> {escape_html(product['name'])}\n"
            f"<b>Unit Price:</b> {format_money(product['price'])}\n"
            f"<b>Quantity:</b> {qty}\n"
            f"<b>Total:</b> {format_money(total)}\n"
            f"<b>Wallet Balance:</b> {format_money(wallet_dec)}\n"
            f"<b>Balance After Purchase:</b> {format_money(remaining)}\n\n"
            "✅ Your wallet balance is enough for this purchase.\n"
            "The order can be completed directly from your wallet."
        )

    shortage = total - wallet_dec
    return (
        "🛒 <b>ORDER SUMMARY</b>\n\n"
        f"<b>Product:</b> {escape_html(product['name'])}\n"
        f"<b>Unit Price:</b> {format_money(product['price'])}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total:</b> {format_money(total)}\n"
        f"<b>Wallet Balance:</b> {format_money(wallet_dec)}\n"
        f"<b>Still Needed:</b> {format_money(shortage)}\n\n"
        "Choose a payment method below to continue."
    )


def render_deposit_text():
    return (
        "💳 <b>TOP UP YOUR WALLET</b>\n\n"
        "Choose a deposit amount below or enter a custom amount."
    )


def render_deposit_method_text(amount):
    return (
        "💳 <b>SELECT PAYMENT METHOD</b>\n\n"
        f"<b>Requested Top-Up:</b> {format_money(amount)}\n\n"
        "Choose how you want to complete this payment."
    )


def render_manual_payment_text(amount, method, details):
    return (
        "🏦 <b>MANUAL PAYMENT INSTRUCTION</b>\n\n"
        f"<b>Amount:</b> {format_money(amount)}\n"
        f"<b>Method:</b> {escape_html(method)}\n\n"
        f"{escape_html(details)}\n\n"
        "After sending payment, contact support with proof of payment."
    )


def render_buy_manual_payment_text(product_id, qty, total, method, details):
    product = get_product(product_id)
    name = product["name"] if product else "Unknown Product"

    return (
        "🏦 <b>ORDER PAYMENT DETAILS</b>\n\n"
        f"<b>Product:</b> {escape_html(name)}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total:</b> {format_money(total)}\n"
        f"<b>Method:</b> {escape_html(method)}\n\n"
        f"{escape_html(details)}\n\n"
        "After payment, contact support with your payment proof for manual review."
    )


def render_invoice_payment_text(invoice_row):
    invoice_type = invoice_row["invoice_type"]
    header = "✅ <b>PAYMENT REQUEST CREATED</b>"

    if invoice_type == "deposit":
        purpose_text = "Wallet Top-Up"
    else:
        purpose_text = "Product Order"

    eta_text = NETWORK_CONFIRMATION_GUIDE.get(invoice_row["network"], "Usually a few minutes")

    return (
        f"{header}\n\n"
        f"<b>Purpose:</b> {escape_html(purpose_text)}\n"
        f"<b>Base Amount:</b> {format_money(invoice_row['amount_base_usd'])}\n"
        f"<b>Service Buffer:</b> {format_money(invoice_row['amount_extra_usd'])}\n"
        f"<b>Final Payable:</b> {format_money(invoice_row['amount_payable_usd'])}\n\n"
        f"<b>Send This Coin Amount:</b>\n"
        f"<code>{escape_html(invoice_row['coin_amount'])} {escape_html(invoice_row['coin_symbol'])}</code>\n\n"
        f"<b>Network:</b> {escape_html(invoice_row['network'])}\n"
        f"<b>Deposit Address:</b>\n"
        f"<code>{escape_html(invoice_row['deposit_address'])}</code>\n\n"
        f"⚠️ <b>Important:</b>\n"
        f"• Please send the exact coin amount shown above.\n"
        f"• Exchange withdrawal fees are separate and must be covered by the sender.\n"
        f"• A different amount may fail automatic detection.\n"
        f"• This payment request expires at: {format_dt(invoice_row['expires_at'])}\n\n"
        f"<b>Estimated Confirmation Time:</b> {escape_html(eta_text)}\n"
        f"<b>Created At:</b> {format_dt(invoice_row['created_at'])}"
    )


def render_orders_text(user_id):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM orders
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 25
    """, (int(user_id),)).fetchall()
    conn.close()

    if not rows:
        return "📦 <b>YOUR ORDERS</b>\n\nNo orders found."

    lines = ["📦 <b>YOUR ORDERS</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>Order #{row['id']}</b>\n"
            f"Product: {escape_html(row['product_name'])}\n"
            f"Qty: {row['qty']}\n"
            f"Base Total: {format_money(row['base_total_usd'])}\n"
            f"Paid Amount: {format_money(row['payable_total_usd'])}\n"
            f"Payment Type: {escape_html(row['payment_type'])}\n"
            f"Status: <b>{escape_html(row['status'])}</b>\n"
            f"Created: {format_dt(row['created_at'])}\n"
            f"Paid: {format_dt(row['paid_at'])}\n"
            f"Delivered: {format_dt(row['delivered_at'])}\n"
            f"Updated: {format_dt(row['updated_at'])}"
        )

    return "\n".join(lines)


def render_transactions_text(user_id):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM transactions
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 30
    """, (int(user_id),)).fetchall()
    conn.close()

    if not rows:
        return "🧾 <b>YOUR TRANSACTIONS</b>\n\nNo transactions found."

    lines = ["🧾 <b>YOUR TRANSACTIONS</b>\n"]
    for row in rows:
        coin_part = ""
        if row["coin_amount"] and row["coin_symbol"]:
            coin_part = f"\nCoin Amount: {escape_html(row['coin_amount'])} {escape_html(row['coin_symbol'])}"

        txid_part = ""
        if row["blockchain_txid"]:
            txid_part = f"\nTXID: <code>{escape_html(row['blockchain_txid'])}</code>"

        lines.append(
            f"\n<b>TX #{row['id']}</b>\n"
            f"Type: {escape_html(row['tx_type'])}\n"
            f"USD Amount: {format_money(row['amount_usd'])}\n"
            f"Network: {escape_html(row['network'] or 'N/A')}"
            f"{coin_part}"
            f"\nStatus: <b>{escape_html(row['status'])}</b>"
            f"{txid_part}"
            f"\nCreated: {format_dt(row['created_at'])}\n"
            f"Confirmed: {format_dt(row['confirmed_at'])}\n"
            f"Updated: {format_dt(row['updated_at'])}"
        )

    return "\n".join(lines)


def render_open_invoices_text(user_id):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM invoices
        WHERE user_id = ? AND status IN ('pending', 'checking', 'awaiting_payment')
        ORDER BY created_at DESC
        LIMIT 15
    """, (int(user_id),)).fetchall()
    conn.close()

    if not rows:
        return "📄 <b>OPEN PAYMENT REQUESTS</b>\n\nYou do not have any active payment requests."

    lines = ["📄 <b>OPEN PAYMENT REQUESTS</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>{escape_html(row['invoice_id'])}</b>\n"
            f"Type: {escape_html(row['invoice_type'])}\n"
            f"Network: {escape_html(row['network'])}\n"
            f"Final Payable: {format_money(row['amount_payable_usd'])}\n"
            f"Coin Amount: {escape_html(row['coin_amount'])} {escape_html(row['coin_symbol'])}\n"
            f"Status: <b>{escape_html(row['status'])}</b>\n"
            f"Created: {format_dt(row['created_at'])}\n"
            f"Expires: {format_dt(row['expires_at'])}"
        )

    return "\n".join(lines)


def render_payment_checker_intro():
    return (
        "🛠 <b>PAYMENT CHECKER</b>\n\n"
        "Use this section if:\n"
        "• you paid but left the chat,\n"
        "• your payment is still pending,\n"
        "• you want to check a TXID manually,\n"
        "• you want to view your open payment requests.\n\n"
        "Choose an option below."
    )


def render_guideline_home():
    return (
        "📘 <b>BOT GUIDELINE</b>\n\n"
        "If you are new, this guide will help you understand how the bot works.\n\n"
        "Choose a topic below."
    )


def render_guideline_text(topic):
    mapping = {
        "guide_buy": (
            "🛍 <b>HOW TO BUY A PRODUCT</b>\n\n"
            "1. Open <b>Shop</b>\n"
            "2. Select a product\n"
            "3. Choose quantity\n"
            "4. If wallet balance is enough, the order completes instantly\n"
            "5. If not, choose a payment method\n"
            "6. For crypto, complete the payment request and press <b>I Have Paid (Verify)</b>\n"
            "7. After confirmation, your product details will be delivered automatically"
        ),
        "guide_topup": (
            "💳 <b>HOW TO TOP UP</b>\n\n"
            "1. Open <b>Top Up</b>\n"
            "2. Select or enter the amount\n"
            "3. Choose crypto or manual payment\n"
            "4. For crypto, send the exact displayed amount\n"
            "5. Press <b>I Have Paid (Verify)</b>\n"
            "6. Once confirmed, the balance will be added to your wallet"
        ),
        "guide_promo": (
            "🎟 <b>HOW TO USE A PROMO CODE</b>\n\n"
            "1. Open <b>Promo</b>\n"
            "2. Send your promo code in chat\n"
            "3. If valid, the bonus will be added to your wallet\n"
            "4. Some promo codes may be one-time only"
        ),
        "guide_payment": (
            "🛠 <b>PAYMENT HELP</b>\n\n"
            "• Always send the exact displayed amount\n"
            "• The sender must cover exchange withdrawal fees\n"
            "• Use the correct network only\n"
            "• If verification is delayed, use <b>Payment Checker</b>\n"
            "• If a payment is already on chain but still pending, wait for blockchain confirmations"
        ),
        "guide_delivery": (
            "📦 <b>ORDER DELIVERY INFO</b>\n\n"
            "• Wallet-paid orders are completed instantly if stock is available\n"
            "• Verified crypto orders are delivered automatically after confirmation\n"
            "• If stock is unavailable, contact support\n"
            "• Delivery timestamps are shown in your order history"
        ),
    }
    return mapping.get(topic, "Guide not found.")

# =========================
# DATABASE RECORD HELPERS
# =========================

def add_transaction_record(
    user_id,
    tx_type,
    amount_usd,
    status,
    network=None,
    coin_symbol=None,
    coin_amount=None,
    invoice_id=None,
    blockchain_txid=None,
    note=None,
):
    conn = db_conn()
    cur = conn.cursor()
    now_text = now_iso()

    cur.execute("""
        INSERT INTO transactions (
            user_id, tx_type, amount_usd, network, coin_symbol, coin_amount,
            status, invoice_id, blockchain_txid, note,
            created_at, confirmed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        int(user_id),
        tx_type,
        str(money_2(amount_usd)),
        network,
        coin_symbol,
        None if coin_amount is None else str(coin_amount),
        status,
        invoice_id,
        blockchain_txid,
        note,
        now_text,
        None,
        now_text,
    ))

    tx_id = cur.lastrowid
    conn.commit()
    conn.close()
    return tx_id


def update_transaction_status(tx_id, status, confirmed=False, blockchain_txid=None, note=None):
    conn = db_conn()
    cur = conn.cursor()

    current = cur.execute(
        "SELECT * FROM transactions WHERE id = ?",
        (int(tx_id),)
    ).fetchone()

    if not current:
        conn.close()
        return False

    confirmed_at = current["confirmed_at"]
    if confirmed and not confirmed_at:
        confirmed_at = now_iso()

    new_note = current["note"]
    if note is not None:
        new_note = note

    new_txid = current["blockchain_txid"]
    if blockchain_txid is not None:
        new_txid = blockchain_txid

    cur.execute("""
        UPDATE transactions
        SET status = ?, confirmed_at = ?, blockchain_txid = ?, note = ?, updated_at = ?
        WHERE id = ?
    """, (
        status,
        confirmed_at,
        new_txid,
        new_note,
        now_iso(),
        int(tx_id),
    ))

    conn.commit()
    conn.close()
    return True


def add_order_record(
    user_id,
    product_id,
    product_name,
    qty,
    unit_price,
    base_total_usd,
    payable_total_usd,
    payment_type,
    status,
    invoice_id=None,
):
    conn = db_conn()
    cur = conn.cursor()
    now_text = now_iso()

    cur.execute("""
        INSERT INTO orders (
            user_id, product_id, product_name, qty, unit_price,
            base_total_usd, payable_total_usd,
            payment_type, status, invoice_id,
            created_at, paid_at, delivered_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        int(user_id),
        product_id,
        product_name,
        int(qty),
        str(money_2(unit_price)),
        str(money_2(base_total_usd)),
        str(money_2(payable_total_usd)),
        payment_type,
        status,
        invoice_id,
        now_text,
        None,
        None,
        now_text,
    ))

    order_id = cur.lastrowid
    conn.commit()
    conn.close()
    return order_id


def update_order_status(order_id, status, mark_paid=False, mark_delivered=False):
    conn = db_conn()
    cur = conn.cursor()

    row = cur.execute("SELECT * FROM orders WHERE id = ?", (int(order_id),)).fetchone()
    if not row:
        conn.close()
        return False

    paid_at = row["paid_at"]
    delivered_at = row["delivered_at"]

    if mark_paid and not paid_at:
        paid_at = now_iso()

    if mark_delivered and not delivered_at:
        delivered_at = now_iso()

    cur.execute("""
        UPDATE orders
        SET status = ?, paid_at = ?, delivered_at = ?, updated_at = ?
        WHERE id = ?
    """, (
        status,
        paid_at,
        delivered_at,
        now_iso(),
        int(order_id),
    ))

    conn.commit()
    conn.close()
    return True


def create_invoice(
    user_id,
    invoice_type,
    amount_base_usd,
    amount_extra_usd,
    amount_payable_usd,
    network,
    coin_symbol,
    coin_amount,
    deposit_address,
    related_order_id=None,
):
    conn = db_conn()
    cur = conn.cursor()

    invoice_id = random_invoice_id()
    created_at = now_iso()
    expires_at = future_iso(PAYMENT_REQUEST_EXPIRE_MINUTES)

    cur.execute("""
        INSERT INTO invoices (
            invoice_id, user_id, invoice_type, related_order_id,
            amount_base_usd, amount_extra_usd, amount_payable_usd,
            network, coin_symbol, coin_amount, deposit_address,
            status, blockchain_txid, blockchain_status, problem_reason,
            verify_attempts, used_tx_unique_key,
            expires_at, created_at, paid_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        invoice_id,
        int(user_id),
        invoice_type,
        related_order_id,
        str(money_2(amount_base_usd)),
        str(money_2(amount_extra_usd)),
        str(money_2(amount_payable_usd)),
        network,
        coin_symbol,
        str(coin_amount),
        deposit_address,
        "awaiting_payment",
        None,
        None,
        None,
        0,
        None,
        expires_at,
        created_at,
        None,
        created_at,
    ))

    conn.commit()
    conn.close()
    return invoice_id


def get_invoice(invoice_id):
    conn = db_conn()
    row = conn.execute(
        "SELECT * FROM invoices WHERE invoice_id = ?",
        (invoice_id,)
    ).fetchone()
    conn.close()
    return row


def update_invoice_status(
    invoice_id,
    status,
    blockchain_txid=None,
    blockchain_status=None,
    problem_reason=None,
    paid=False,
    used_tx_unique_key=None,
    increase_attempt=False,
):
    conn = db_conn()
    cur = conn.cursor()

    row = cur.execute(
        "SELECT * FROM invoices WHERE invoice_id = ?",
        (invoice_id,)
    ).fetchone()

    if not row:
        conn.close()
        return False

    attempts = int(row["verify_attempts"] or 0)
    if increase_attempt:
        attempts += 1

    paid_at = row["paid_at"]
    if paid and not paid_at:
        paid_at = now_iso()

    new_blockchain_txid = row["blockchain_txid"]
    if blockchain_txid is not None:
        new_blockchain_txid = blockchain_txid

    new_blockchain_status = row["blockchain_status"]
    if blockchain_status is not None:
        new_blockchain_status = blockchain_status

    new_problem_reason = row["problem_reason"]
    if problem_reason is not None:
        new_problem_reason = problem_reason

    new_unique_key = row["used_tx_unique_key"]
    if used_tx_unique_key is not None:
        new_unique_key = used_tx_unique_key

    cur.execute("""
        UPDATE invoices
        SET status = ?, blockchain_txid = ?, blockchain_status = ?, problem_reason = ?,
            verify_attempts = ?, used_tx_unique_key = ?, paid_at = ?, updated_at = ?
        WHERE invoice_id = ?
    """, (
        status,
        new_blockchain_txid,
        new_blockchain_status,
        new_problem_reason,
        attempts,
        new_unique_key,
        paid_at,
        now_iso(),
        invoice_id,
    ))

    conn.commit()
    conn.close()
    return True


def cancel_expired_invoices():
    conn = db_conn()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT invoice_id, expires_at, status
        FROM invoices
        WHERE status IN ('awaiting_payment', 'pending', 'checking')
    """).fetchall()

    for row in rows:
        if dt_is_expired(row["expires_at"]):
            cur.execute("""
                UPDATE invoices
                SET status = ?, updated_at = ?
                WHERE invoice_id = ?
            """, (
                "expired",
                now_iso(),
                row["invoice_id"],
            ))

    conn.commit()
    conn.close()


def has_chain_payment_been_used(unique_key):
    conn = db_conn()
    row = conn.execute("""
        SELECT id
        FROM used_chain_payments
        WHERE unique_key = ?
        LIMIT 1
    """, (unique_key,)).fetchone()
    conn.close()
    return row is not None


def mark_chain_payment_used(network, unique_key, txid, invoice_id):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO used_chain_payments (
            network, unique_key, txid, invoice_id, used_at
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        network,
        unique_key,
        txid,
        invoice_id,
        now_iso(),
    ))
    conn.commit()
    conn.close()


def get_user_search_summary_text(user_id):
    ensure_user(user_id)

    conn = db_conn()
    cur = conn.cursor()

    wallet_row = cur.execute(
        "SELECT wallet_balance, created_at, updated_at FROM users WHERE user_id = ?",
        (int(user_id),)
    ).fetchone()

    total_deposit_row = cur.execute("""
        SELECT COALESCE(SUM(CAST(amount_usd AS REAL)), 0) AS total
        FROM transactions
        WHERE user_id = ? AND tx_type = 'Deposit' AND status = 'Completed'
    """, (int(user_id),)).fetchone()

    total_spent_row = cur.execute("""
        SELECT COALESCE(SUM(CAST(payable_total_usd AS REAL)), 0) AS total
        FROM orders
        WHERE user_id = ? AND status = 'Completed'
    """, (int(user_id),)).fetchone()

    completed_orders = cur.execute("""
        SELECT COUNT(*) AS c FROM orders
        WHERE user_id = ? AND status = 'Completed'
    """, (int(user_id),)).fetchone()["c"]

    pending_orders = cur.execute("""
        SELECT COUNT(*) AS c FROM orders
        WHERE user_id = ? AND status IN ('Awaiting Manual Confirmation', 'Pending Payment', 'Checking Payment')
    """, (int(user_id),)).fetchone()["c"]

    completed_deposits = cur.execute("""
        SELECT COUNT(*) AS c FROM transactions
        WHERE user_id = ? AND tx_type = 'Deposit' AND status = 'Completed'
    """, (int(user_id),)).fetchone()["c"]

    pending_deposits = cur.execute("""
        SELECT COUNT(*) AS c FROM transactions
        WHERE user_id = ? AND tx_type = 'Deposit'
        AND status IN ('Pending Payment', 'Checking Payment', 'Awaiting Manual Confirmation')
    """, (int(user_id),)).fetchone()["c"]

    tx_rows = cur.execute("""
        SELECT *
        FROM transactions
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 12
    """, (int(user_id),)).fetchall()

    order_rows = cur.execute("""
        SELECT *
        FROM orders
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 12
    """, (int(user_id),)).fetchall()

    conn.close()

    wallet_balance = safe_decimal(wallet_row["wallet_balance"], Decimal("0")) if wallet_row else Decimal("0")
    total_deposit = safe_decimal(total_deposit_row["total"], Decimal("0"))
    total_spent = safe_decimal(total_spent_row["total"], Decimal("0"))

    lines = [
        "🆔 <b>USER SEARCH RESULT</b>",
        "",
        f"<b>User ID:</b> <code>{int(user_id)}</code>",
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

    if not tx_rows:
        lines.append("No transactions found.")
    else:
        for tx in tx_rows:
            lines.append(
                f"\nTX#{tx['id']} | {escape_html(tx['tx_type'])}\n"
                f"Amount: {format_money(tx['amount_usd'])}\n"
                f"Status: {escape_html(tx['status'])}\n"
                f"Created: {format_dt(tx['created_at'])}\n"
                f"Confirmed: {format_dt(tx['confirmed_at'])}"
            )

    lines.extend(["", "━━━━━━━━━━━━━━", "", "<b>Recent Orders:</b>"])

    if not order_rows:
        lines.append("No orders found.")
    else:
        for order in order_rows:
            lines.append(
                f"\nOrder#{order['id']} | {escape_html(order['product_name'])}\n"
                f"Qty: {order['qty']}\n"
                f"Payable: {format_money(order['payable_total_usd'])}\n"
                f"Payment: {escape_html(order['payment_type'])}\n"
                f"Status: {escape_html(order['status'])}\n"
                f"Created: {format_dt(order['created_at'])}\n"
                f"Paid: {format_dt(order['paid_at'])}\n"
                f"Delivered: {format_dt(order['delivered_at'])}"
            )

    return "\n".join(lines)

# =========================
# LIVE PRICE / CONVERSION HELPERS
# =========================

def get_cached_rate(network):
    updated_at = rate_cache.get("updated_at")
    prices = rate_cache.get("prices", {})

    if updated_at and (time.time() - updated_at) <= 60 and network in prices:
        return prices[network]

    return None


def set_cached_rate(network, usd_price):
    if "prices" not in rate_cache:
        rate_cache["prices"] = {}
    rate_cache["prices"][network] = safe_decimal(usd_price, Decimal("0"))
    rate_cache["updated_at"] = time.time()


def fetch_live_usd_rate(network):
    cached = get_cached_rate(network)
    if cached is not None:
        return cached

    coin_id = NETWORK_PRICE_ID_MAP.get(network)
    if not coin_id:
        return None

    # USDT stays 1
    if coin_id == "tether":
        set_cached_rate(network, Decimal("1"))
        return Decimal("1")

    res = http_get_json(
        f"{COINGECKO_BASE}/simple/price",
        params={
            "ids": coin_id,
            "vs_currencies": "usd",
        },
        timeout=20,
    )

    if not res["ok"]:
        return None

    data = res["data"] or {}
    usd_price = (((data.get(coin_id) or {}).get("usd")))
    usd_dec = safe_decimal(usd_price)
    if usd_dec is None or usd_dec <= 0:
        return None

    set_cached_rate(network, usd_dec)
    return usd_dec


def quantize_coin_amount(amount_decimal, network):
    amount_decimal = safe_decimal(amount_decimal, Decimal("0"))

    if network == "BTC":
        return amount_decimal.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    if network == "LTC":
        return amount_decimal.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    if network == "ETH (ERC20)":
        return amount_decimal.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    if network == "BNB (BEP20)":
        return amount_decimal.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    if network == "SOL":
        return amount_decimal.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

    if network == "TRX (TRC20)":
        return amount_decimal.quantize(Decimal("0.001"), rounding=ROUND_DOWN)

    if network.startswith("USDT"):
        return amount_decimal.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    return amount_decimal.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)


def usd_to_coin_amount(usd_amount, network):
    usd_dec = safe_decimal(usd_amount)
    if usd_dec is None or usd_dec <= 0:
        return None, None

    usd_rate = fetch_live_usd_rate(network)
    if usd_rate is None or usd_rate <= 0:
        return None, None

    # Example: if BTC price is 65000 USD, then coin = usd / 65000
    coin_amount = usd_dec / usd_rate
    coin_amount = quantize_coin_amount(coin_amount, network)

    return coin_amount, usd_rate


def build_unique_payable_amount(base_amount_usd):
    base_dec = money_2(base_amount_usd)

    # avoid collision with currently active invoice amounts as much as possible
    conn = db_conn()
    rows = conn.execute("""
        SELECT amount_payable_usd
        FROM invoices
        WHERE status IN ('awaiting_payment', 'pending', 'checking')
    """).fetchall()
    conn.close()

    active_amounts = {money_2(row["amount_payable_usd"]) for row in rows}

    for _ in range(100):
        extra = random_unique_extra()
        payable = money_2(base_dec + extra)
        if payable not in active_amounts:
            return extra, payable

    # fallback
    extra = Decimal("0.17")
    return extra, money_2(base_dec + extra)


def build_payment_request(user_id, invoice_type, base_amount_usd, network, related_order_id=None):
    base_dec = money_2(base_amount_usd)
    extra_dec, payable_dec = build_unique_payable_amount(base_dec)

    coin_symbol = NETWORK_SYMBOL_MAP[network]
    coin_amount, usd_rate = usd_to_coin_amount(payable_dec, network)

    if coin_amount is None:
        return {
            "ok": False,
            "reason": "Live coin rate could not be loaded right now. Please try again."
        }

    deposit_address = CRYPTO_ADDRESSES[network]

    invoice_id = create_invoice(
        user_id=user_id,
        invoice_type=invoice_type,
        amount_base_usd=base_dec,
        amount_extra_usd=extra_dec,
        amount_payable_usd=payable_dec,
        network=network,
        coin_symbol=coin_symbol,
        coin_amount=coin_amount,
        deposit_address=deposit_address,
        related_order_id=related_order_id,
    )

    invoice = get_invoice(invoice_id)

    return {
        "ok": True,
        "invoice_id": invoice_id,
        "invoice": invoice,
        "usd_rate": usd_rate,
    }


def get_evm_tx_by_hash(chainid, txhash):
    params = {
        "chainid": chainid,
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": txhash,
        "apikey": ETHERSCAN_API_KEY,
    }
    return http_get_json(ETHERSCAN_V2_URL, params=params, timeout=25)


def get_evm_tx_receipt(chainid, txhash):
    params = {
        "chainid": chainid,
        "module": "proxy",
        "action": "eth_getTransactionReceipt",
        "txhash": txhash,
        "apikey": ETHERSCAN_API_KEY,
    }
    return http_get_json(ETHERSCAN_V2_URL, params=params, timeout=25)


def trongrid_headers():
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "TRON-PRO-API-KEY": TRONGRID_API_KEY,
    }


def is_valid_txid_format(txid):
    txid = str(txid or "").strip()

    if txid.startswith("0x") and len(txid) == 66:
        hex_part = txid[2:]
        return all(c in "0123456789abcdefABCDEF" for c in hex_part)

    if len(txid) == 64:
        return all(c in "0123456789abcdefABCDEF" for c in txid)

    base58_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    if len(txid) >= 20:
        return all(c in base58_chars for c in txid)

    return False

# =========================
# PART 2 OF 10 - END
# =========================

# =========================
# PART 3 OF 10 - START
# Paste this directly BELOW Part 2
# =========================

# =========================
# TRON / BASE58 HELPERS
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
    hex_addr = str(hex_addr or "").lower().replace("0x", "").strip()
    if len(hex_addr) == 40:
        hex_addr = "41" + hex_addr

    raw = bytes.fromhex(hex_addr)
    checksum = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    return b58encode(raw + checksum)


# =========================
# TXID CHECKER / VERIFICATION ENGINE
# These are used by:
# 1) manual Payment Checker menu
# 2) backup/manual recovery flow
# =========================

def verify_usdt_trc20_txid(txid: str, expected_amount, expected_to_address: str):
    info_res = http_post_json(
        f"{TRONGRID_BASE}/walletsolidity/gettransactioninfobyid",
        payload={"value": txid},
        headers=trongrid_headers(),
        timeout=20,
    )

    if not info_res["ok"]:
        return verify_result(False, "pending", f"TRON info API returned status {info_res['status_code']}")

    info_data = info_res["data"]
    if not info_data:
        return verify_result(False, "pending", "Transaction not confirmed on blockchain yet.")

    receipt = info_data.get("receipt", {}) or {}
    receipt_result = str(receipt.get("result", "")).upper()

    if receipt_result and receipt_result != "SUCCESS":
        return verify_result(False, "rejected", f"Transaction failed with receipt result: {receipt_result}")

    ev_res = http_get_json(
        f"{TRONGRID_BASE}/v1/transactions/{txid}/events",
        params={"only_confirmed": "true"},
        headers=trongrid_headers(),
        timeout=20,
    )

    if not ev_res["ok"]:
        return verify_result(False, "pending", f"TRON event API returned status {ev_res['status_code']}")

    events = (ev_res["data"] or {}).get("data", []) or []
    if not events:
        return verify_result(False, "pending", "No confirmed TRC20 transfer event found yet.")

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

        if str(to_addr).strip() != str(expected_to_address).strip():
            continue

        if amount_within_tolerance(actual_amount, expected_amount, USD_TOLERANCE):
            unique_key = f"TRC20-USDT::{txid}::{expected_to_address}::{actual_amount}"
            return verify_result(
                True,
                "confirmed",
                "Payment matched successfully.",
                extra={
                    "txid": txid,
                    "network": "USDT (TRC20)",
                    "unique_key": unique_key,
                    "amount": str(actual_amount),
                    "to_address": to_addr,
                },
            )

        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} USDT instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": "USDT (TRC20)",
                "amount": str(actual_amount),
                "to_address": to_addr,
            },
        )

    return verify_result(False, "rejected", "No matching USDT TRC20 transfer was found for this address.")


def verify_trx_transfer(txid: str, expected_amount, expected_to_address: str):
    tx_res = http_post_json(
        f"{TRONGRID_BASE}/wallet/gettransactionbyid",
        payload={"value": txid},
        headers=trongrid_headers(),
        timeout=20,
    )

    if not tx_res["ok"]:
        return verify_result(False, "pending", f"TRON tx API returned status {tx_res['status_code']}")

    tx_data = tx_res["data"]
    if not tx_data:
        return verify_result(False, "pending", "Transaction not found on TRON yet.")

    info_res = http_post_json(
        f"{TRONGRID_BASE}/walletsolidity/gettransactioninfobyid",
        payload={"value": txid},
        headers=trongrid_headers(),
        timeout=20,
    )

    if not info_res["ok"]:
        return verify_result(False, "pending", f"TRON confirmation API returned status {info_res['status_code']}")

    info_data = info_res["data"]
    if not info_data:
        return verify_result(False, "pending", "Transaction not confirmed on blockchain yet.")

    receipt = info_data.get("receipt", {}) or {}
    receipt_result = str(receipt.get("result", "")).upper()

    if receipt_result and receipt_result != "SUCCESS":
        return verify_result(False, "rejected", f"Transaction failed with receipt result: {receipt_result}")

    contracts = (((tx_data.get("raw_data") or {}).get("contract")) or [])
    if not contracts:
        return verify_result(False, "rejected", "No TRX transfer data found in this transaction.")

    contract = contracts[0] or {}
    param_value = (((contract.get("parameter") or {}).get("value")) or {})

    amount_sun = int(param_value.get("amount", 0))
    to_address_hex = str(param_value.get("to_address", "")).strip()

    if not to_address_hex:
        return verify_result(False, "rejected", "Destination address not found in transaction.")

    actual_to = tron_hex_to_base58(to_address_hex)
    actual_amount = Decimal(amount_sun) / Decimal("1000000")

    if actual_to != expected_to_address:
        return verify_result(False, "rejected", "Destination address does not match expected address.")

    if not amount_within_tolerance(actual_amount, expected_amount, USD_TOLERANCE):
        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} TRX instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": "TRX (TRC20)",
                "amount": str(actual_amount),
                "to_address": actual_to,
            },
        )

    unique_key = f"TRX::{txid}::{actual_to}::{actual_amount}"
    return verify_result(
        True,
        "confirmed",
        "Payment matched successfully.",
        extra={
            "txid": txid,
            "network": "TRX (TRC20)",
            "unique_key": unique_key,
            "amount": str(actual_amount),
            "to_address": actual_to,
        },
    )


def verify_evm_native_transfer(txid: str, expected_amount, expected_to_address: str, chainid: str, symbol: str, network_label: str):
    tx_res = get_evm_tx_by_hash(chainid, txid)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"{symbol} transaction API returned status {tx_res['status_code']}")

    tx_data = (tx_res["data"] or {}).get("result")
    if not tx_data:
        return verify_result(False, "pending", "Transaction not found on chain yet.")

    receipt_res = get_evm_tx_receipt(chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{symbol} receipt API returned status {receipt_res['status_code']}")

    receipt = (receipt_res["data"] or {}).get("result")
    if not receipt:
        return verify_result(False, "pending", "Transaction not confirmed on blockchain yet.")

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "Blockchain reports this transaction as failed.")

    actual_to = normalize_evm_address(tx_data.get("to"))
    expected_to = normalize_evm_address(expected_to_address)

    if actual_to != expected_to:
        return verify_result(False, "rejected", "Destination address does not match expected address.")

    try:
        value_wei = int(str(tx_data.get("value", "0")), 16)
    except Exception:
        return verify_result(False, "rejected", "Could not read transaction value.")

    actual_amount = Decimal(value_wei) / Decimal("1000000000000000000")

    if not amount_within_tolerance(actual_amount, expected_amount, Decimal("0.00000001")):
        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} {symbol} instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": network_label,
                "amount": str(actual_amount),
                "to_address": actual_to,
            },
        )

    unique_key = f"{network_label}::{txid}::{actual_to}::{actual_amount}"
    return verify_result(
        True,
        "confirmed",
        "Payment matched successfully.",
        extra={
            "txid": txid,
            "network": network_label,
            "unique_key": unique_key,
            "amount": str(actual_amount),
            "to_address": actual_to,
        },
    )


def verify_evm_token_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    chainid: str,
    token_contract: str,
    decimals: int,
    symbol: str,
    network_label: str,
):
    receipt_res = get_evm_tx_receipt(chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{symbol} receipt API returned status {receipt_res['status_code']}")

    receipt = (receipt_res["data"] or {}).get("result")
    if not receipt:
        return verify_result(False, "pending", "Transaction not confirmed on blockchain yet.")

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "Blockchain reports this transaction as failed.")

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

        if amount_within_tolerance(actual_amount, expected_amount, USD_TOLERANCE):
            unique_key = f"{network_label}::{txid}::{expected_to_address.lower()}::{actual_amount}"
            return verify_result(
                True,
                "confirmed",
                "Payment matched successfully.",
                extra={
                    "txid": txid,
                    "network": network_label,
                    "unique_key": unique_key,
                    "amount": str(actual_amount),
                    "to_address": expected_to_address,
                },
            )

        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} {symbol} instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": network_label,
                "amount": str(actual_amount),
                "to_address": expected_to_address,
            },
        )

    return verify_result(False, "rejected", "No matching token transfer was found for the expected address.")


def verify_btc_transfer(txid: str, expected_amount, expected_to_address: str):
    tx_res = http_get_json(f"{BTC_API_BASE}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"BTC transaction API returned status {tx_res['status_code']}")

    tx = tx_res["data"]
    if not tx:
        return verify_result(False, "pending", "Transaction not found on Bitcoin yet.")

    status = tx.get("status", {}) or {}
    if not status.get("confirmed"):
        return verify_result(False, "pending", "Transaction found but not confirmed yet.")

    for vout in tx.get("vout", []) or []:
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        output_address = vout.get("scriptpubkey_address")

        if output_address != expected_to_address:
            continue

        if amount_within_tolerance(actual_amount, expected_amount, Decimal("0.00000001")):
            unique_key = f"BTC::{txid}::{output_address}::{actual_amount}"
            return verify_result(
                True,
                "confirmed",
                "Payment matched successfully.",
                extra={
                    "txid": txid,
                    "network": "BTC",
                    "unique_key": unique_key,
                    "amount": str(actual_amount),
                    "to_address": output_address,
                },
            )

        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} BTC instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": "BTC",
                "amount": str(actual_amount),
                "to_address": output_address,
            },
        )

    return verify_result(False, "rejected", "No matching BTC output was found for the expected address.")


def verify_ltc_transfer(txid: str, expected_amount, expected_to_address: str):
    tx_res = http_get_json(f"{LTC_API_BASE}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"LTC transaction API returned status {tx_res['status_code']}")

    tx = tx_res["data"]
    if not tx:
        return verify_result(False, "pending", "Transaction not found on Litecoin yet.")

    status = tx.get("status", {}) or {}
    if not status.get("confirmed"):
        return verify_result(False, "pending", "Transaction found but not confirmed yet.")

    for vout in tx.get("vout", []) or []:
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        output_address = vout.get("scriptpubkey_address")

        if output_address != expected_to_address:
            continue

        if amount_within_tolerance(actual_amount, expected_amount, Decimal("0.00000001")):
            unique_key = f"LTC::{txid}::{output_address}::{actual_amount}"
            return verify_result(
                True,
                "confirmed",
                "Payment matched successfully.",
                extra={
                    "txid": txid,
                    "network": "LTC",
                    "unique_key": unique_key,
                    "amount": str(actual_amount),
                    "to_address": output_address,
                },
            )

        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} LTC instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": "LTC",
                "amount": str(actual_amount),
                "to_address": output_address,
            },
        )

    return verify_result(False, "rejected", "No matching LTC output was found for the expected address.")


def helius_rpc(method: str, params):
    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": method,
        "params": params,
    }
    return http_post_json(
        HELIUS_RPC_URL,
        payload=payload,
        headers={"content-type": "application/json"},
        timeout=25,
    )


def verify_sol_transfer(txid: str, expected_amount, expected_to_address: str):
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
        return verify_result(False, "pending", f"SOL transaction API returned status {res['status_code']}")

    tx = (res["data"] or {}).get("result")
    if not tx:
        return verify_result(False, "pending", "Transaction not found on Solana yet.")

    meta = tx.get("meta", {}) or {}
    if meta.get("err") is not None:
        return verify_result(False, "rejected", "Blockchain reports this Solana transaction as failed.")

    instructions = []
    message = (tx.get("transaction", {}) or {}).get("message", {}) or {}
    instructions.extend(message.get("instructions", []) or [])

    for inner in meta.get("innerInstructions", []) or []:
        instructions.extend(inner.get("instructions", []) or [])

    for ins in instructions:
        parsed = ins.get("parsed")
        if not parsed:
            continue

        if parsed.get("type") != "transfer":
            continue

        info = parsed.get("info", {}) or {}
        destination = info.get("destination")
        lamports = info.get("lamports")

        if destination != expected_to_address or lamports is None:
            continue

        actual_amount = Decimal(int(lamports)) / Decimal("1000000000")

        if amount_within_tolerance(actual_amount, expected_amount, Decimal("0.000001")):
            unique_key = f"SOL::{txid}::{destination}::{actual_amount}"
            return verify_result(
                True,
                "confirmed",
                "Payment matched successfully.",
                extra={
                    "txid": txid,
                    "network": "SOL",
                    "unique_key": unique_key,
                    "amount": str(actual_amount),
                    "to_address": destination,
                },
            )

        return verify_result(
            False,
            "amount_mismatch",
            f"Transaction found, but received amount was {actual_amount} SOL instead of expected {expected_amount}.",
            extra={
                "txid": txid,
                "network": "SOL",
                "amount": str(actual_amount),
                "to_address": destination,
            },
        )

    return verify_result(False, "rejected", "No matching SOL transfer was found for the expected address.")


def verify_crypto_payment_by_txid(network: str, txid: str, expected_coin_amount, expected_to_address: str):
    if network == "USDT (TRC20)":
        return verify_usdt_trc20_txid(txid, expected_coin_amount, expected_to_address)

    if network == "TRX (TRC20)":
        return verify_trx_transfer(txid, expected_coin_amount, expected_to_address)

    if network == "USDT (ERC20)":
        return verify_evm_token_transfer(
            txid=txid,
            expected_amount=expected_coin_amount,
            expected_to_address=expected_to_address,
            chainid=ETH_CHAIN_ID,
            token_contract=USDT_ERC20_CONTRACT,
            decimals=6,
            symbol="USDT",
            network_label="USDT (ERC20)",
        )

    if network == "USDT (BEP20)":
        return verify_evm_token_transfer(
            txid=txid,
            expected_amount=expected_coin_amount,
            expected_to_address=expected_to_address,
            chainid=BSC_CHAIN_ID,
            token_contract=USDT_BEP20_CONTRACT,
            decimals=18,
            symbol="USDT",
            network_label="USDT (BEP20)",
        )

    if network == "ETH (ERC20)":
        return verify_evm_native_transfer(
            txid=txid,
            expected_amount=expected_coin_amount,
            expected_to_address=expected_to_address,
            chainid=ETH_CHAIN_ID,
            symbol="ETH",
            network_label="ETH (ERC20)",
        )

    if network == "BNB (BEP20)":
        return verify_evm_native_transfer(
            txid=txid,
            expected_amount=expected_coin_amount,
            expected_to_address=expected_to_address,
            chainid=BSC_CHAIN_ID,
            symbol="BNB",
            network_label="BNB (BEP20)",
        )

    if network == "BTC":
        return verify_btc_transfer(txid, expected_coin_amount, expected_to_address)

    if network == "LTC":
        return verify_ltc_transfer(txid, expected_coin_amount, expected_to_address)

    if network == "SOL":
        return verify_sol_transfer(txid, expected_coin_amount, expected_to_address)

    return verify_result(False, "rejected", f"Unsupported network: {network}")


# =========================
# SHARED-ADDRESS AUTO-DETECT HELPERS
# These are used by the main "I Have Paid (Verify)" flow
# without requiring TXID from the client.
# =========================

def invoice_recent_enough(invoice_row, minutes=240):
    created_dt = parse_iso(invoice_row["created_at"])
    if not created_dt:
        return False
    return now_utc() <= (created_dt + timedelta(minutes=minutes))


def make_used_unique_key(network, txid, to_address, amount):
    return f"{network}::{txid}::{str(to_address).lower()}::{str(amount)}"


def scan_recent_btc_matches(expected_address, expected_amount, created_after=None):
    # mempool.space does not offer a clean "address incoming tx list with parsed filters"
    # in the same easy way for this bot, so for button verify we ask for TXID fallback for BTC
    return verify_result(
        False,
        "manual_check_required",
        "Automatic scan is limited for BTC on shared-address mode. Please use Payment Checker with TXID if needed."
    )


def scan_recent_ltc_matches(expected_address, expected_amount, created_after=None):
    return verify_result(
        False,
        "manual_check_required",
        "Automatic scan is limited for LTC on shared-address mode. Please use Payment Checker with TXID if needed."
    )


def scan_recent_sol_matches(expected_address, expected_amount, created_after=None):
    return verify_result(
        False,
        "manual_check_required",
        "Automatic scan is limited for SOL on shared-address mode. Please use Payment Checker with TXID if needed."
    )


def scan_recent_trx_matches(expected_address, expected_amount, created_after=None):
    # TRON native scan without TXID is less reliable here with current public API usage.
    return verify_result(
        False,
        "manual_check_required",
        "Automatic scan is limited for TRX on shared-address mode. Please use Payment Checker with TXID if needed."
    )


def scan_recent_trc20_usdt_matches(expected_address, expected_amount, created_after=None):
    # Public TRONGRID address event scan is possible, but response formats can vary.
    # We keep this stable with a graceful fallback.
    return verify_result(
        False,
        "manual_check_required",
        "Automatic scan is limited for USDT TRC20 on shared-address mode right now. Please use Payment Checker with TXID if needed."
    )


def scan_recent_evm_native_matches(network_label, chainid, expected_address, expected_amount, created_after=None):
    # For shared-address automatic button verification, a stable address-level explorer lookup
    # is not guaranteed in this bot version. TXID checker remains available as backup.
    return verify_result(
        False,
        "manual_check_required",
        f"Automatic scan is limited for {network_label} on shared-address mode. Please use Payment Checker with TXID if needed."
    )


def scan_recent_evm_token_matches(network_label, chainid, token_contract, expected_address, expected_amount, created_after=None):
    # Same logic: stable address event indexing varies by provider plan and endpoint.
    return verify_result(
        False,
        "manual_check_required",
        f"Automatic scan is limited for {network_label} on shared-address mode. Please use Payment Checker with TXID if needed."
    )


def auto_detect_payment_for_invoice(invoice_row):
    if not invoice_row:
        return verify_result(False, "rejected", "Invoice not found.")

    if invoice_row["status"] in {"cancelled", "expired", "completed"}:
        return verify_result(False, "rejected", f"This payment request is already {invoice_row['status']}.")

    if dt_is_expired(invoice_row["expires_at"]):
        update_invoice_status(invoice_row["invoice_id"], "expired", problem_reason="Payment request expired.")
        return verify_result(False, "expired", "This payment request has expired.")

    network = invoice_row["network"]
    expected_address = invoice_row["deposit_address"]
    expected_amount = safe_decimal(invoice_row["coin_amount"], Decimal("0"))
    created_after = invoice_row["created_at"]

    if network == "USDT (TRC20)":
        return scan_recent_trc20_usdt_matches(expected_address, expected_amount, created_after)

    if network == "TRX (TRC20)":
        return scan_recent_trx_matches(expected_address, expected_amount, created_after)

    if network == "USDT (ERC20)":
        return scan_recent_evm_token_matches(
            network_label="USDT (ERC20)",
            chainid=ETH_CHAIN_ID,
            token_contract=USDT_ERC20_CONTRACT,
            expected_address=expected_address,
            expected_amount=expected_amount,
            created_after=created_after,
        )

    if network == "USDT (BEP20)":
        return scan_recent_evm_token_matches(
            network_label="USDT (BEP20)",
            chainid=BSC_CHAIN_ID,
            token_contract=USDT_BEP20_CONTRACT,
            expected_address=expected_address,
            expected_amount=expected_amount,
            created_after=created_after,
        )

    if network == "ETH (ERC20)":
        return scan_recent_evm_native_matches(
            network_label="ETH (ERC20)",
            chainid=ETH_CHAIN_ID,
            expected_address=expected_address,
            expected_amount=expected_amount,
            created_after=created_after,
        )

    if network == "BNB (BEP20)":
        return scan_recent_evm_native_matches(
            network_label="BNB (BEP20)",
            chainid=BSC_CHAIN_ID,
            expected_address=expected_address,
            expected_amount=expected_amount,
            created_after=created_after,
        )

    if network == "BTC":
        return scan_recent_btc_matches(expected_address, expected_amount, created_after)

    if network == "LTC":
        return scan_recent_ltc_matches(expected_address, expected_amount, created_after)

    if network == "SOL":
        return scan_recent_sol_matches(expected_address, expected_amount, created_after)

    return verify_result(False, "rejected", f"Unsupported invoice network: {network}")


# =========================
# PAYMENT CHECKER TEXT HELPERS
# =========================

def render_txid_check_result(network, txid, result):
    status = result.get("status", "unknown")
    reason = result.get("reason", "No message available.")
    extra = result.get("extra", {}) or {}

    lines = [
        "🛠 <b>PAYMENT CHECK RESULT</b>",
        "",
        f"<b>Network:</b> {escape_html(network)}",
        f"<b>TXID:</b> <code>{escape_html(txid)}</code>",
        f"<b>Status:</b> <b>{escape_html(status.replace('_', ' ').title())}</b>",
        f"<b>Message:</b> {escape_html(reason)}",
    ]

    if extra.get("amount"):
        lines.append(f"<b>Detected Amount:</b> {escape_html(extra['amount'])}")

    if extra.get("to_address"):
        lines.append(f"<b>Receiver:</b> <code>{escape_html(extra['to_address'])}</code>")

    eta = NETWORK_CONFIRMATION_GUIDE.get(network)
    if status == "pending" and eta:
        lines.append(f"<b>Estimated Time:</b> {escape_html(eta)}")

    lines.append(f"<b>Checked At:</b> {format_dt(now_iso())}")

    if status in {"amount_mismatch", "rejected"}:
        lines.append("")
        lines.append("If you believe this is a mistake, contact support with your TXID and payment details.")

    return "\n".join(lines)


def render_invoice_verify_result(invoice_row, result):
    status = result.get("status", "unknown")
    reason = result.get("reason", "No message available.")

    lines = [
        "🔎 <b>PAYMENT STATUS</b>",
        "",
        f"<b>Invoice:</b> <code>{escape_html(invoice_row['invoice_id'])}</code>",
        f"<b>Type:</b> {escape_html(invoice_row['invoice_type'])}",
        f"<b>Network:</b> {escape_html(invoice_row['network'])}",
        f"<b>Expected Amount:</b> {escape_html(invoice_row['coin_amount'])} {escape_html(invoice_row['coin_symbol'])}",
        f"<b>Status:</b> <b>{escape_html(status.replace('_', ' ').title())}</b>",
        f"<b>Message:</b> {escape_html(reason)}",
        f"<b>Checked At:</b> {format_dt(now_iso())}",
    ]

    if status == "pending":
        eta = NETWORK_CONFIRMATION_GUIDE.get(invoice_row["network"])
        if eta:
            lines.append(f"<b>Estimated Confirmation Time:</b> {escape_html(eta)}")

    if status == "manual_check_required":
        lines.append("")
        lines.append("Please use <b>Payment Checker</b> with your TXID for manual blockchain lookup.")

    if status == "expired":
        lines.append("")
        lines.append("This request is no longer active. Please create a new payment request.")

    return "\n".join(lines)

# =========================
# PART 3 OF 10 - END
# =========================

# =========================
# PART 4 OF 10 - START
# Paste this directly BELOW Part 3
# =========================

# =========================
# BASIC HANDLERS (START / MENU)
# =========================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id

    ensure_user(user_id)

    # referral tracking
    if context.args:
        try:
            ref_id = int(context.args[0])
            if ref_id != user_id:
                conn = db_conn()
                conn.execute("""
                    UPDATE users
                    SET referred_by = ?, updated_at = ?
                    WHERE user_id = ? AND referred_by IS NULL
                """, (str(ref_id), now_iso(), user_id))
                conn.commit()
                conn.close()
        except Exception:
            pass

    enter_client_mode(user_id)

    await update.message.reply_text(
        render_home_text(),
        reply_markup=main_menu(),
        parse_mode="HTML",
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    ensure_user(user_id)

    mode = get_user_mode(user_id)
    state = get_user_state(user_id)

    # =========================
    # CLIENT MODE
    # =========================
    if mode == "client":

        if text == "🛍 Shop":
            return await handle_shop(update, context)

        if text == "💰 Wallet":
            return await handle_wallet(update, context)

        if text == "💳 Top Up":
            return await handle_topup(update, context)

        if text == "📦 Orders":
            return await handle_orders(update, context)

        if text == "🧾 Transactions":
            return await handle_transactions(update, context)

        if text == "💬 Support":
            return await handle_support(update, context)

        if text == "🆔 User ID":
            return await handle_user_id(update, context)

        if text == "👥 Refer & Earn":
            return await handle_refer(update, context)

        if text == "🛠 Payment Checker":
            return await handle_payment_checker(update, context)

        if text == "📘 Bot Guideline":
            return await handle_guideline(update, context)

        if text == "🎟 Promo":
            return await handle_promo_input(update, context)

        # fallback
        await update.message.reply_text(
            "Please choose an option from the menu.",
            reply_markup=main_menu(),
        )
        return

    # =========================
    # ADMIN MODE (basic exit)
    # =========================
    if mode == "admin":
        if text == "🚪 Exit Admin":
            enter_client_mode(user_id)
            await update.message.reply_text(
                "Exited admin mode.",
                reply_markup=main_menu(),
            )
            return

        await update.message.reply_text(
            "Admin panel is active. Use admin menu.",
            reply_markup=admin_menu(),
        )


# =========================
# CLIENT MENU HANDLERS
# =========================

async def handle_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        render_wallet_text(user_id),
        parse_mode="HTML",
    )


async def handle_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        render_user_id_text(user_id),
        parse_mode="HTML",
    )


async def handle_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        render_support_text(),
        parse_mode="HTML",
    )


async def handle_refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        render_refer_text(user_id),
        parse_mode="HTML",
    )


async def handle_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        render_orders_text(user_id),
        parse_mode="HTML",
    )


async def handle_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        render_transactions_text(user_id),
        parse_mode="HTML",
    )


# =========================
# SHOP HANDLERS
# =========================

async def handle_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    products = get_all_products()

    if not products:
        await update.message.reply_text("No products available right now.")
        return

    for p in products:
        await update.message.reply_text(
            render_product_card(p["product_id"]),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("View Details", callback_data=f"view_product_{p['product_id']}")]
            ]),
        )


async def handle_product_view(query, product_id):
    text = render_product_details(product_id)

    await query.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=buy_qty_keyboard(product_id),
    )


# =========================
# TOPUP HANDLERS
# =========================

async def handle_topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        render_deposit_text(),
        parse_mode="HTML",
        reply_markup=deposit_amount_keyboard(),
    )


# =========================
# PROMO HANDLER
# =========================

async def handle_promo_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    code = (update.message.text or "").strip().upper()

    conn = db_conn()
    row = conn.execute(
        "SELECT * FROM promo_codes WHERE code = ?",
        (code,)
    ).fetchone()

    if not row:
        conn.close()
        await update.message.reply_text("❌ Invalid promo code.")
        return

    if not row["enabled"]:
        conn.close()
        await update.message.reply_text("❌ This promo code is disabled.")
        return

    if row["one_time"] and has_user_used_promo(user_id, code):
        conn.close()
        await update.message.reply_text("❌ You already used this promo code.")
        return

    amount = safe_decimal(row["amount"], Decimal("0"))
    add_wallet_balance(user_id, amount)

    now_text = now_iso()

    conn.execute("""
        INSERT INTO promo_usages (code, user_id, amount, created_at)
        VALUES (?, ?, ?, ?)
    """, (code, int(user_id), str(amount), now_text))

    if row["one_time"]:
        conn.execute("""
            UPDATE promo_codes
            SET enabled = 0, used_by = ?, used_at = ?, updated_at = ?
            WHERE code = ?
        """, (str(user_id), now_text, now_text, code))

    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"✅ Promo applied!\nYou received {format_money(amount)}",
        parse_mode="HTML",
    )


# =========================
# GUIDELINE HANDLER
# =========================

async def handle_guideline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        render_guideline_home(),
        parse_mode="HTML",
        reply_markup=guideline_keyboard(),
    )


# =========================
# PAYMENT CHECKER HANDLER (UI LEVEL)
# =========================

async def handle_payment_checker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        render_payment_checker_intro(),
        parse_mode="HTML",
        reply_markup=payment_checker_keyboard(),
    )


# =========================
# CALLBACK HANDLER (PARTIAL)
# =========================

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    user_id = query.from_user.id

    # =========================
    # PRODUCT VIEW
    # =========================
    if data.startswith("view_product_"):
        product_id = data.replace("view_product_", "")
        return await handle_product_view(query, product_id)

    # =========================
    # CLOSE BUTTON
    # =========================
    if data == "close_inline":
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    # =========================
    # GUIDELINE CLICK
    # =========================
    if data.startswith("guide_"):
        text = render_guideline_text(data)
        await query.message.reply_text(text, parse_mode="HTML")
        return

    # =========================
    # PAYMENT CHECKER MENU
    # =========================
    if data == "payment_checker_txid":
        state = get_user_state(user_id)
        state["step"] = "awaiting_txid_input"
        set_user_state(user_id, state)

        await query.message.reply_text(
            "🔎 Send your TXID to check payment status.",
            parse_mode="HTML",
        )
        return

    if data == "payment_checker_requests":
        await query.message.reply_text(
            render_open_invoices_text(user_id),
            parse_mode="HTML",
        )
        return


# =========================
# PART 4 OF 10 - END
# =========================

# =========================
# PART 5 OF 10 - START
# Paste this directly BELOW Part 4
# =========================

# =========================
# SHOP BUY FLOW
# =========================

async def continue_text_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    ensure_user(user_id)
    state = get_user_state(user_id)
    step = state.get("step", "")

    # -------------------------
    # custom quantity input
    # -------------------------
    if step == "awaiting_custom_qty":
        product_id = state.get("product_id")
        product = get_product(product_id)

        if not product:
            set_user_state(user_id, {"step": "main"})
            await update.message.reply_text("❌ Product not found.")
            return True

        try:
            qty = int(text)
            if qty <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text("Please send a valid quantity number, like 1 or 2.")
            return True

        display_stock = get_display_stock(product_id)
        if qty > display_stock:
            await update.message.reply_text(f"Only {display_stock} pcs are currently available.")
            return True

        wallet_balance = get_wallet_balance(user_id)
        total = money_2(Decimal(str(product["price"])) * Decimal(str(qty)))

        set_user_state(user_id, {
            "step": "buy_payment_method",
            "product_id": product_id,
            "qty": qty,
            "total_usd": str(total),
        })

        await update.message.reply_text(
            render_buy_summary(product_id, qty, wallet_balance),
            parse_mode="HTML",
            reply_markup=payment_method_keyboard("buy"),
        )
        return True

    # -------------------------
    # custom deposit amount input
    # -------------------------
    if step == "awaiting_custom_deposit_amount":
        try:
            amount = safe_decimal(text)
            if amount is None or amount <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text("Please send a valid deposit amount, like 5 or 10.")
            return True

        amount = money_2(amount)

        set_user_state(user_id, {
            "step": "deposit_payment_method",
            "amount_usd": str(amount),
        })

        await update.message.reply_text(
            render_deposit_method_text(amount),
            parse_mode="HTML",
            reply_markup=payment_method_keyboard("dep"),
        )
        return True

    # -------------------------
    # TXID checker manual input
    # -------------------------
    if step == "awaiting_txid_input":
        txid = text.strip()

        if not is_valid_txid_format(txid):
            await update.message.reply_text("❌ Invalid TXID format. Please send a valid blockchain TXID.")
            return True

        set_user_state(user_id, {
            "step": "awaiting_txid_network"
        })

        # temporarily store txid in admin_temp-like memory
        if user_id not in admin_temp:
            admin_temp[user_id] = {}
        admin_temp[user_id]["checker_txid"] = txid

        await update.message.reply_text(
            "Now send the network name exactly like one of these:\n\n"
            "USDT (TRC20)\n"
            "USDT (ERC20)\n"
            "USDT (BEP20)\n"
            "TRX (TRC20)\n"
            "BTC\n"
            "LTC\n"
            "ETH (ERC20)\n"
            "BNB (BEP20)\n"
            "SOL"
        )
        return True

    if step == "awaiting_txid_network":
        network = text.strip()
        txid = admin_temp.get(user_id, {}).get("checker_txid")

        if not txid:
            set_user_state(user_id, {"step": "main"})
            await update.message.reply_text("TXID session expired. Please open Payment Checker again.")
            return True

        if network not in CRYPTO_ADDRESSES:
            await update.message.reply_text("Unsupported network. Please send one of the listed network names exactly.")
            return True

        # manual checker cannot know expected amount automatically
        # so we show a guided status message
        eta = NETWORK_CONFIRMATION_GUIDE.get(network, "Usually a few minutes")
        set_user_state(user_id, {"step": "main"})
        admin_temp[user_id]["checker_network"] = network

        await update.message.reply_text(
            "🔎 <b>BASIC TXID CHECK MODE</b>\n\n"
            f"<b>TXID:</b> <code>{escape_html(txid)}</code>\n"
            f"<b>Network:</b> {escape_html(network)}\n"
            f"<b>Estimated Confirmation Time:</b> {escape_html(eta)}\n\n"
            "To fully validate amount and receiver, the bot needs an active invoice or known expected payment details.\n"
            "If your payment belongs to an active invoice, use <b>View My Open Requests</b> and press Verify there.\n"
            "If not, contact support with your TXID.",
            parse_mode="HTML",
        )
        return True

    return False


# =========================
# BUY FLOW CALLBACKS
# =========================

async def handle_buy_qty_callback(query, user_id, product_id, qty):
    product = get_product(product_id)
    if not product:
        await query.message.reply_text("❌ Product not found.")
        return

    display_stock = get_display_stock(product_id)
    if qty > display_stock:
        await query.message.reply_text(f"Only {display_stock} pcs are currently available.")
        return

    wallet_balance = get_wallet_balance(user_id)
    total = money_2(Decimal(str(product["price"])) * Decimal(str(qty)))

    set_user_state(user_id, {
        "step": "buy_payment_method",
        "product_id": product_id,
        "qty": qty,
        "total_usd": str(total),
    })

    await query.message.reply_text(
        render_buy_summary(product_id, qty, wallet_balance),
        parse_mode="HTML",
        reply_markup=payment_method_keyboard("buy"),
    )


async def handle_buy_custom_qty_callback(query, user_id, product_id):
    set_user_state(user_id, {
        "step": "awaiting_custom_qty",
        "product_id": product_id,
    })

    await query.message.reply_text(
        "Send the quantity you want to buy.\nExample: 2"
    )


async def handle_buy_payment_method_callback(query, user_id, method):
    state = get_user_state(user_id)

    product_id = state.get("product_id")
    qty = int(state.get("qty", 0))
    total_usd = safe_decimal(state.get("total_usd"), Decimal("0"))

    if not product_id or qty <= 0 or total_usd <= 0:
        await query.message.reply_text("Buy session expired. Please start again from Shop.")
        set_user_state(user_id, {"step": "main"})
        return

    if method == "binance":
        await query.message.reply_text(
            render_buy_manual_payment_text(
                product_id=product_id,
                qty=qty,
                total=total_usd,
                method="Binance ID",
                details=BINANCE_ID,
            ),
            parse_mode="HTML",
        )
        return

    if method == "bybit":
        await query.message.reply_text(
            render_buy_manual_payment_text(
                product_id=product_id,
                qty=qty,
                total=total_usd,
                method="Bybit ID",
                details=BYBIT_ID,
            ),
            parse_mode="HTML",
        )
        return

    if method == "crypto":
        set_user_state(user_id, {
            "step": "buy_network_select",
            "product_id": product_id,
            "qty": qty,
            "total_usd": str(total_usd),
        })

        await query.message.reply_text(
            "Choose the payment network below:",
            reply_markup=network_keyboard("buy"),
        )
        return


async def handle_buy_network_callback(query, user_id, network):
    state = get_user_state(user_id)

    product_id = state.get("product_id")
    qty = int(state.get("qty", 0))
    total_usd = safe_decimal(state.get("total_usd"), Decimal("0"))

    product = get_product(product_id)
    if not product or qty <= 0 or total_usd <= 0:
        await query.message.reply_text("Buy session expired. Please start again.")
        set_user_state(user_id, {"step": "main"})
        return

    # create pending order first
    order_id = add_order_record(
        user_id=user_id,
        product_id=product_id,
        product_name=product["name"],
        qty=qty,
        unit_price=product["price"],
        base_total_usd=total_usd,
        payable_total_usd=total_usd,
        payment_type="Crypto",
        status="Pending Payment",
        invoice_id=None,
    )

    build_result = build_payment_request(
        user_id=user_id,
        invoice_type="order",
        base_amount_usd=total_usd,
        network=network,
        related_order_id=order_id,
    )

    if not build_result["ok"]:
        await query.message.reply_text(build_result["reason"])
        return

    invoice = build_result["invoice"]
    invoice_id = build_result["invoice_id"]

    # update order with invoice id and payable amount
    conn = db_conn()
    conn.execute("""
        UPDATE orders
        SET invoice_id = ?, payable_total_usd = ?, updated_at = ?
        WHERE id = ?
    """, (
        invoice_id,
        str(invoice["amount_payable_usd"]),
        now_iso(),
        int(order_id),
    ))
    conn.commit()
    conn.close()

    add_transaction_record(
        user_id=user_id,
        tx_type="Order Payment",
        amount_usd=invoice["amount_payable_usd"],
        network=invoice["network"],
        coin_symbol=invoice["coin_symbol"],
        coin_amount=invoice["coin_amount"],
        status="Pending Payment",
        invoice_id=invoice_id,
        blockchain_txid=None,
        note=f"Created for Order #{order_id}",
    )

    set_user_state(user_id, {"step": "main"})

    await query.message.reply_text(
        render_invoice_payment_text(invoice),
        parse_mode="HTML",
        reply_markup=payment_invoice_keyboard(invoice_id, invoice["deposit_address"]),
    )


# =========================
# DEPOSIT FLOW CALLBACKS
# =========================

async def handle_deposit_amount_callback(query, user_id, amount):
    amount = money_2(amount)

    set_user_state(user_id, {
        "step": "deposit_payment_method",
        "amount_usd": str(amount),
    })

    await query.message.reply_text(
        render_deposit_method_text(amount),
        parse_mode="HTML",
        reply_markup=payment_method_keyboard("dep"),
    )


async def handle_deposit_custom_amount_callback(query, user_id):
    set_user_state(user_id, {
        "step": "awaiting_custom_deposit_amount",
    })

    await query.message.reply_text("Send the deposit amount you want to add.\nExample: 10")


async def handle_deposit_payment_method_callback(query, user_id, method):
    state = get_user_state(user_id)
    amount_usd = safe_decimal(state.get("amount_usd"), Decimal("0"))

    if amount_usd <= 0:
        await query.message.reply_text("Deposit session expired. Please start again.")
        set_user_state(user_id, {"step": "main"})
        return

    if method == "binance":
        await query.message.reply_text(
            render_manual_payment_text(amount_usd, "Binance ID", BINANCE_ID),
            parse_mode="HTML",
        )
        return

    if method == "bybit":
        await query.message.reply_text(
            render_manual_payment_text(amount_usd, "Bybit ID", BYBIT_ID),
            parse_mode="HTML",
        )
        return

    if method == "crypto":
        set_user_state(user_id, {
            "step": "deposit_network_select",
            "amount_usd": str(amount_usd),
        })

        await query.message.reply_text(
            "Choose the network for your wallet top-up:",
            reply_markup=network_keyboard("dep"),
        )
        return


async def handle_deposit_network_callback(query, user_id, network):
    state = get_user_state(user_id)
    amount_usd = safe_decimal(state.get("amount_usd"), Decimal("0"))

    if amount_usd <= 0:
        await query.message.reply_text("Deposit session expired. Please start again.")
        set_user_state(user_id, {"step": "main"})
        return

    build_result = build_payment_request(
        user_id=user_id,
        invoice_type="deposit",
        base_amount_usd=amount_usd,
        network=network,
        related_order_id=None,
    )

    if not build_result["ok"]:
        await query.message.reply_text(build_result["reason"])
        return

    invoice = build_result["invoice"]
    invoice_id = build_result["invoice_id"]

    add_transaction_record(
        user_id=user_id,
        tx_type="Deposit",
        amount_usd=invoice["amount_payable_usd"],
        network=invoice["network"],
        coin_symbol=invoice["coin_symbol"],
        coin_amount=invoice["coin_amount"],
        status="Pending Payment",
        invoice_id=invoice_id,
        blockchain_txid=None,
        note="Wallet top-up request created",
    )

    set_user_state(user_id, {"step": "main"})

    await query.message.reply_text(
        render_invoice_payment_text(invoice),
        parse_mode="HTML",
        reply_markup=payment_invoice_keyboard(invoice_id, invoice["deposit_address"]),
    )


# =========================
# INVOICE ACTION CALLBACKS
# =========================

async def handle_invoice_verify_callback(query, user_id, invoice_id):
    invoice = get_invoice(invoice_id)

    if not invoice:
        await query.message.reply_text("❌ Payment request not found.")
        return

    if int(invoice["user_id"]) != int(user_id) and not is_admin(user_id):
        await query.message.reply_text("❌ You are not allowed to verify this payment request.")
        return

    result = auto_detect_payment_for_invoice(invoice)

    update_invoice_status(
        invoice_id=invoice_id,
        status="checking" if result["status"] in {"pending", "manual_check_required"} else invoice["status"],
        blockchain_status=result["status"],
        problem_reason=result["reason"],
        increase_attempt=True,
    )

    await query.message.reply_text(
        render_invoice_verify_result(invoice, result),
        parse_mode="HTML",
    )


async def handle_invoice_refresh_callback(query, user_id, invoice_id):
    invoice = get_invoice(invoice_id)

    if not invoice:
        await query.message.reply_text("❌ Payment request not found.")
        return

    if int(invoice["user_id"]) != int(user_id) and not is_admin(user_id):
        await query.message.reply_text("❌ You are not allowed to refresh this request.")
        return

    # reload current row
    invoice = get_invoice(invoice_id)

    await query.message.reply_text(
        render_invoice_payment_text(invoice),
        parse_mode="HTML",
        reply_markup=payment_invoice_keyboard(invoice_id, invoice["deposit_address"]),
    )


async def handle_invoice_cancel_callback(query, user_id, invoice_id):
    invoice = get_invoice(invoice_id)

    if not invoice:
        await query.message.reply_text("❌ Payment request not found.")
        return

    if int(invoice["user_id"]) != int(user_id) and not is_admin(user_id):
        await query.message.reply_text("❌ You are not allowed to cancel this request.")
        return

    if invoice["status"] in {"completed", "cancelled"}:
        await query.message.reply_text(f"This request is already {invoice['status']}.")
        return

    update_invoice_status(
        invoice_id=invoice_id,
        status="cancelled",
        problem_reason="Cancelled by user",
    )

    # if related to order, mark order as cancelled if still unpaid
    if invoice["invoice_type"] == "order" and invoice["related_order_id"]:
        update_order_status(invoice["related_order_id"], "Cancelled")

    await query.message.reply_text("❌ Payment request cancelled.")


async def handle_copy_address_hint_callback(query, invoice_id):
    invoice = get_invoice(invoice_id)
    if not invoice:
        await query.message.reply_text("Payment request not found.")
        return

    await query.message.reply_text(
        "📋 <b>COPY ADDRESS</b>\n\n"
        "If your Telegram app does not support the instant copy button, copy the address below manually:\n\n"
        f"<code>{escape_html(invoice['deposit_address'])}</code>",
        parse_mode="HTML",
    )


# =========================
# CALLBACK ROUTER EXTENSION
# This function continues callback routing from Part 4.
# =========================

async def callback_handler_part_2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""
    user_id = query.from_user.id

    # -------------------------
    # buy qty fixed buttons
    # -------------------------
    if data.startswith("buy_qty_"):
        parts = data.split("_")
        product_id = parts[2]
        qty = int(parts[3])
        return await handle_buy_qty_callback(query, user_id, product_id, qty)

    if data.startswith("buy_custom_"):
        product_id = data.replace("buy_custom_", "")
        return await handle_buy_custom_qty_callback(query, user_id, product_id)

    # -------------------------
    # buy payment method
    # -------------------------
    if data == "buy_method_binance":
        return await handle_buy_payment_method_callback(query, user_id, "binance")

    if data == "buy_method_bybit":
        return await handle_buy_payment_method_callback(query, user_id, "bybit")

    if data == "buy_method_crypto":
        return await handle_buy_payment_method_callback(query, user_id, "crypto")

    if data == "buy_back":
        await query.message.reply_text("Please re-open the product from Shop.")
        return

    # -------------------------
    # buy network
    # -------------------------
    if data.startswith("buy_net_"):
        net_key = data.replace("buy_net_", "")
        mapping = {
            "USDT_TRC20": "USDT (TRC20)",
            "USDT_ERC20": "USDT (ERC20)",
            "USDT_BEP20": "USDT (BEP20)",
            "TRX_TRC20": "TRX (TRC20)",
            "BTC": "BTC",
            "LTC": "LTC",
            "ETH_ERC20": "ETH (ERC20)",
            "BNB_BEP20": "BNB (BEP20)",
            "SOL": "SOL",
        }
        network = mapping.get(net_key)
        if not network:
            await query.message.reply_text("Unsupported network.")
            return
        return await handle_buy_network_callback(query, user_id, network)

    # -------------------------
    # deposit amount buttons
    # -------------------------
    if data == "dep_amt_5":
        return await handle_deposit_amount_callback(query, user_id, Decimal("5"))

    if data == "dep_amt_10":
        return await handle_deposit_amount_callback(query, user_id, Decimal("10"))

    if data == "dep_amt_15":
        return await handle_deposit_amount_callback(query, user_id, Decimal("15"))

    if data == "dep_amt_20":
        return await handle_deposit_amount_callback(query, user_id, Decimal("20"))

    if data == "dep_custom":
        return await handle_deposit_custom_amount_callback(query, user_id)

    # -------------------------
    # deposit payment method
    # -------------------------
    if data == "dep_method_binance":
        return await handle_deposit_payment_method_callback(query, user_id, "binance")

    if data == "dep_method_bybit":
        return await handle_deposit_payment_method_callback(query, user_id, "bybit")

    if data == "dep_method_crypto":
        return await handle_deposit_payment_method_callback(query, user_id, "crypto")

    if data == "dep_back":
        await query.message.reply_text(
            render_deposit_text(),
            parse_mode="HTML",
            reply_markup=deposit_amount_keyboard(),
        )
        return

    # -------------------------
    # deposit network
    # -------------------------
    if data.startswith("dep_net_"):
        net_key = data.replace("dep_net_", "")
        mapping = {
            "USDT_TRC20": "USDT (TRC20)",
            "USDT_ERC20": "USDT (ERC20)",
            "USDT_BEP20": "USDT (BEP20)",
            "TRX_TRC20": "TRX (TRC20)",
            "BTC": "BTC",
            "LTC": "LTC",
            "ETH_ERC20": "ETH (ERC20)",
            "BNB_BEP20": "BNB (BEP20)",
            "SOL": "SOL",
        }
        network = mapping.get(net_key)
        if not network:
            await query.message.reply_text("Unsupported network.")
            return
        return await handle_deposit_network_callback(query, user_id, network)

    # -------------------------
    # invoice actions
    # -------------------------
    if data.startswith("verify_invoice_"):
        invoice_id = data.replace("verify_invoice_", "")
        return await handle_invoice_verify_callback(query, user_id, invoice_id)

    if data.startswith("refresh_invoice_"):
        invoice_id = data.replace("refresh_invoice_", "")
        return await handle_invoice_refresh_callback(query, user_id, invoice_id)

    if data.startswith("cancel_invoice_"):
        invoice_id = data.replace("cancel_invoice_", "")
        return await handle_invoice_cancel_callback(query, user_id, invoice_id)

    if data.startswith("copy_address_hint_"):
        invoice_id = data.replace("copy_address_hint_", "")
        return await handle_copy_address_hint_callback(query, invoice_id)


# =========================
# PART 5 OF 10 - END
# =========================

# =========================
# PART 6 OF 10 - START
# Paste this directly BELOW Part 5
# =========================

# =========================
# STOCK DELIVERY / ORDER COMPLETION HELPERS
# =========================

def get_available_stock_accounts(product_id, qty):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM stock_accounts
        WHERE product_id = ? AND is_delivered = 0
        ORDER BY id ASC
        LIMIT ?
    """, (product_id, int(qty))).fetchall()
    conn.close()
    return rows


def mark_stock_accounts_delivered(product_id, qty, user_id, order_id):
    conn = db_conn()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT id
        FROM stock_accounts
        WHERE product_id = ? AND is_delivered = 0
        ORDER BY id ASC
        LIMIT ?
    """, (product_id, int(qty))).fetchall()

    if len(rows) < int(qty):
        conn.close()
        return False, []

    ids = [row["id"] for row in rows]
    delivered_at = now_iso()

    for stock_id in ids:
        cur.execute("""
            UPDATE stock_accounts
            SET is_delivered = 1,
                delivered_to_user_id = ?,
                delivered_order_id = ?,
                delivered_at = ?,
                updated_at = ?
            WHERE id = ?
        """, (
            int(user_id),
            int(order_id),
            delivered_at,
            delivered_at,
            int(stock_id),
        ))

    delivered_rows = cur.execute(f"""
        SELECT *
        FROM stock_accounts
        WHERE id IN ({",".join(["?"] * len(ids))})
        ORDER BY id ASC
    """, ids).fetchall()

    conn.commit()
    conn.close()
    return True, delivered_rows


async def deliver_accounts_to_user(bot, user_id, order_id, product_id, qty):
    product = get_product(product_id)
    if not product:
        await bot.send_message(
            chat_id=user_id,
            text="❌ Product not found during delivery.",
        )
        return False, []

    ok, delivered_rows = mark_stock_accounts_delivered(product_id, qty, user_id, order_id)
    if not ok:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "❌ <b>Not enough stock is available right now.</b>\n\n"
                "Your order is recorded, but automatic delivery could not complete.\n"
                "Please contact support."
            ),
            parse_mode="HTML",
        )
        return False, []

    lines = [
        f"✅ <b>ORDER COMPLETED</b>",
        "",
        f"<b>Product:</b> {escape_html(product['name'])}",
        f"<b>Quantity:</b> {qty}",
        f"<b>Delivered At:</b> {format_dt(now_iso())}",
        "",
        "🔐 <b>Your Account Details:</b>",
        "",
    ]

    for idx, row in enumerate(delivered_rows, start=1):
        lines.append(f"{idx}. <b>Email/Username:</b> <code>{escape_html(row['email'])}</code>")
        lines.append(f"   <b>Password:</b> <code>{escape_html(row['password'])}</code>")
        if row["note"]:
            lines.append(f"   <b>Note:</b> {escape_html(row['note'])}")
        lines.append("")

    await bot.send_message(
        chat_id=user_id,
        text="\n".join(lines),
        parse_mode="HTML",
    )
    return True, delivered_rows


# =========================
# WALLET PURCHASE FLOW
# =========================

async def complete_wallet_purchase(bot, user_id, product_id, qty):
    product = get_product(product_id)
    if not product:
        await bot.send_message(chat_id=user_id, text="❌ Product not found.")
        return False

    total = money_2(Decimal(str(product["price"])) * Decimal(str(qty)))
    wallet_balance = get_wallet_balance(user_id)

    if wallet_balance < total:
        await bot.send_message(
            chat_id=user_id,
            text="❌ Wallet balance is not enough for this purchase.",
        )
        return False

    real_stock = get_real_stock_count(product_id)
    if real_stock < qty:
        await bot.send_message(
            chat_id=user_id,
            text="❌ Not enough stock is available right now.",
        )
        return False

    ok, new_balance = subtract_wallet_balance(user_id, total)
    if not ok:
        await bot.send_message(chat_id=user_id, text="❌ Failed to deduct wallet balance.")
        return False

    order_id = add_order_record(
        user_id=user_id,
        product_id=product_id,
        product_name=product["name"],
        qty=qty,
        unit_price=product["price"],
        base_total_usd=total,
        payable_total_usd=total,
        payment_type="Wallet",
        status="Completed",
        invoice_id=None,
    )

    update_order_status(order_id, "Completed", mark_paid=True, mark_delivered=True)

    add_transaction_record(
        user_id=user_id,
        tx_type="Wallet Purchase",
        amount_usd=total,
        network=None,
        coin_symbol=None,
        coin_amount=None,
        status="Completed",
        invoice_id=None,
        blockchain_txid=None,
        note=f"Wallet payment for Order #{order_id}",
    )

    await deliver_accounts_to_user(bot, user_id, order_id, product_id, qty)

    await bot.send_message(
        chat_id=user_id,
        text=(
            "✅ <b>Wallet payment completed successfully.</b>\n\n"
            f"<b>Deducted:</b> {format_money(total)}\n"
            f"<b>New Wallet Balance:</b> {format_money(new_balance)}\n"
            f"<b>Processed At:</b> {format_dt(now_iso())}"
        ),
        parse_mode="HTML",
    )
    return True


# =========================
# INVOICE / PAYMENT FINALIZATION
# =========================

def get_transaction_by_invoice(invoice_id):
    conn = db_conn()
    row = conn.execute("""
        SELECT *
        FROM transactions
        WHERE invoice_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (invoice_id,)).fetchone()
    conn.close()
    return row


def get_order_by_invoice(invoice_id):
    conn = db_conn()
    row = conn.execute("""
        SELECT *
        FROM orders
        WHERE invoice_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (invoice_id,)).fetchone()
    conn.close()
    return row


def set_transaction_completed_for_invoice(invoice_id, blockchain_txid=None, note=None):
    tx = get_transaction_by_invoice(invoice_id)
    if not tx:
        return None

    update_transaction_status(
        tx_id=tx["id"],
        status="Completed",
        confirmed=True,
        blockchain_txid=blockchain_txid,
        note=note,
    )
    return tx["id"]


async def finalize_verified_deposit(bot, invoice_row, verify_result_obj):
    user_id = int(invoice_row["user_id"])
    invoice_id = invoice_row["invoice_id"]
    payable_usd = safe_decimal(invoice_row["amount_payable_usd"], Decimal("0"))

    extra = verify_result_obj.get("extra", {}) or {}
    txid = extra.get("txid")
    unique_key = extra.get("unique_key")

    if unique_key and has_chain_payment_been_used(unique_key):
        await bot.send_message(
            chat_id=user_id,
            text="⚠️ This blockchain payment was already used before.",
        )
        return False

    add_wallet_balance(user_id, payable_usd)

    update_invoice_status(
        invoice_id=invoice_id,
        status="completed",
        blockchain_txid=txid,
        blockchain_status="confirmed",
        problem_reason=None,
        paid=True,
        used_tx_unique_key=unique_key,
    )

    if unique_key:
        mark_chain_payment_used(invoice_row["network"], unique_key, txid, invoice_id)

    set_transaction_completed_for_invoice(
        invoice_id=invoice_id,
        blockchain_txid=txid,
        note="Deposit verified successfully",
    )

    new_balance = get_wallet_balance(user_id)

    await bot.send_message(
        chat_id=user_id,
        text=(
            "✅ <b>DEPOSIT CONFIRMED</b>\n\n"
            f"<b>Added To Wallet:</b> {format_money(payable_usd)}\n"
            f"<b>New Wallet Balance:</b> {format_money(new_balance)}\n"
            f"<b>Network:</b> {escape_html(invoice_row['network'])}\n"
            f"<b>Confirmed At:</b> {format_dt(now_iso())}"
        ),
        parse_mode="HTML",
    )
    return True


async def finalize_verified_order(bot, invoice_row, verify_result_obj):
    user_id = int(invoice_row["user_id"])
    invoice_id = invoice_row["invoice_id"]

    order = get_order_by_invoice(invoice_id)
    if not order:
        await bot.send_message(
            chat_id=user_id,
            text="❌ Order record not found for this payment.",
        )
        return False

    extra = verify_result_obj.get("extra", {}) or {}
    txid = extra.get("txid")
    unique_key = extra.get("unique_key")

    if unique_key and has_chain_payment_been_used(unique_key):
        await bot.send_message(
            chat_id=user_id,
            text="⚠️ This blockchain payment was already used before.",
        )
        return False

    # stock check before completing
    real_stock = get_real_stock_count(order["product_id"])
    if real_stock < int(order["qty"]):
        update_invoice_status(
            invoice_id=invoice_id,
            status="checking",
            blockchain_txid=txid,
            blockchain_status="confirmed",
            problem_reason="Payment found but stock is not enough for auto delivery.",
            paid=True,
            used_tx_unique_key=unique_key,
        )

        await bot.send_message(
            chat_id=user_id,
            text=(
                "⚠️ <b>Payment detected, but auto delivery could not finish.</b>\n\n"
                "Reason: stock is currently not enough.\n"
                "Please contact support for manual assistance."
            ),
            parse_mode="HTML",
        )
        return False

    update_invoice_status(
        invoice_id=invoice_id,
        status="completed",
        blockchain_txid=txid,
        blockchain_status="confirmed",
        problem_reason=None,
        paid=True,
        used_tx_unique_key=unique_key,
    )

    if unique_key:
        mark_chain_payment_used(invoice_row["network"], unique_key, txid, invoice_id)

    set_transaction_completed_for_invoice(
        invoice_id=invoice_id,
        blockchain_txid=txid,
        note=f"Order payment verified for Order #{order['id']}",
    )

    update_order_status(
        order_id=order["id"],
        status="Completed",
        mark_paid=True,
        mark_delivered=True,
    )

    await deliver_accounts_to_user(
        bot=bot,
        user_id=user_id,
        order_id=order["id"],
        product_id=order["product_id"],
        qty=int(order["qty"]),
    )

    await bot.send_message(
        chat_id=user_id,
        text=(
            "✅ <b>ORDER PAYMENT CONFIRMED</b>\n\n"
            f"<b>Order ID:</b> #{order['id']}\n"
            f"<b>Product:</b> {escape_html(order['product_name'])}\n"
            f"<b>Quantity:</b> {order['qty']}\n"
            f"<b>Paid:</b> {format_money(order['payable_total_usd'])}\n"
            f"<b>Confirmed At:</b> {format_dt(now_iso())}"
        ),
        parse_mode="HTML",
    )
    return True


async def finalize_invoice_if_verified(bot, invoice_row, verify_result_obj):
    if not verify_result_obj.get("ok"):
        return False

    if verify_result_obj.get("status") != "confirmed":
        return False

    if invoice_row["invoice_type"] == "deposit":
        return await finalize_verified_deposit(bot, invoice_row, verify_result_obj)

    if invoice_row["invoice_type"] == "order":
        return await finalize_verified_order(bot, invoice_row, verify_result_obj)

    return False


# =========================
# MANUAL PAYMENT / MANUAL ADMIN HELPERS
# =========================

def get_pending_manual_orders():
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM orders
        WHERE status = 'Awaiting Manual Confirmation'
        ORDER BY id DESC
    """).fetchall()
    conn.close()
    return rows


def get_pending_manual_deposits():
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM transactions
        WHERE tx_type = 'Deposit' AND status = 'Awaiting Manual Confirmation'
        ORDER BY id DESC
    """).fetchall()
    conn.close()
    return rows


async def confirm_manual_order(bot, order_id):
    conn = db_conn()
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (int(order_id),)).fetchone()
    conn.close()

    if not row:
        return False, "Order not found."

    if row["status"] != "Awaiting Manual Confirmation":
        return False, "Order is no longer pending manual confirmation."

    real_stock = get_real_stock_count(row["product_id"])
    if real_stock < int(row["qty"]):
        return False, "Not enough stock available to complete this order."

    update_order_status(row["id"], "Completed", mark_paid=True, mark_delivered=True)

    tx = get_transaction_by_invoice(row["invoice_id"]) if row["invoice_id"] else None
    if tx:
        update_transaction_status(
            tx["id"],
            "Completed",
            confirmed=True,
            note=f"Manually approved for Order #{row['id']}",
        )

    await deliver_accounts_to_user(
        bot=bot,
        user_id=int(row["user_id"]),
        order_id=int(row["id"]),
        product_id=row["product_id"],
        qty=int(row["qty"]),
    )

    await bot.send_message(
        chat_id=int(row["user_id"]),
        text=(
            "✅ <b>Your manual order payment has been approved.</b>\n\n"
            f"<b>Order ID:</b> #{row['id']}\n"
            f"<b>Product:</b> {escape_html(row['product_name'])}\n"
            f"<b>Approved At:</b> {format_dt(now_iso())}"
        ),
        parse_mode="HTML",
    )
    return True, "Manual order confirmed."


async def reject_manual_order(bot, order_id):
    conn = db_conn()
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (int(order_id),)).fetchone()
    conn.close()

    if not row:
        return False, "Order not found."

    if row["status"] != "Awaiting Manual Confirmation":
        return False, "Order is no longer pending manual confirmation."

    update_order_status(row["id"], "Rejected")

    tx = get_transaction_by_invoice(row["invoice_id"]) if row["invoice_id"] else None
    if tx:
        update_transaction_status(
            tx["id"],
            "Rejected",
            confirmed=False,
            note=f"Manually rejected for Order #{row['id']}",
        )

    await bot.send_message(
        chat_id=int(row["user_id"]),
        text=(
            "❌ <b>Your manual order payment was rejected.</b>\n\n"
            f"<b>Order ID:</b> #{row['id']}\n"
            f"<b>Product:</b> {escape_html(row['product_name'])}\n"
            f"<b>Updated At:</b> {format_dt(now_iso())}\n\n"
            "Please contact support if you think this is a mistake."
        ),
        parse_mode="HTML",
    )
    return True, "Manual order rejected."


async def confirm_manual_deposit(bot, tx_id):
    conn = db_conn()
    tx = conn.execute("SELECT * FROM transactions WHERE id = ?", (int(tx_id),)).fetchone()
    conn.close()

    if not tx:
        return False, "Deposit record not found."

    if tx["tx_type"] != "Deposit" or tx["status"] != "Awaiting Manual Confirmation":
        return False, "Deposit is no longer pending manual confirmation."

    payable_usd = safe_decimal(tx["amount_usd"], Decimal("0"))
    add_wallet_balance(int(tx["user_id"]), payable_usd)

    update_transaction_status(
        tx["id"],
        "Completed",
        confirmed=True,
        note="Manual deposit approved",
    )

    await bot.send_message(
        chat_id=int(tx["user_id"]),
        text=(
            "✅ <b>Your manual deposit has been approved.</b>\n\n"
            f"<b>Amount Added:</b> {format_money(payable_usd)}\n"
            f"<b>Approved At:</b> {format_dt(now_iso())}"
        ),
        parse_mode="HTML",
    )
    return True, "Manual deposit confirmed."


async def reject_manual_deposit(bot, tx_id):
    conn = db_conn()
    tx = conn.execute("SELECT * FROM transactions WHERE id = ?", (int(tx_id),)).fetchone()
    conn.close()

    if not tx:
        return False, "Deposit record not found."

    if tx["tx_type"] != "Deposit" or tx["status"] != "Awaiting Manual Confirmation":
        return False, "Deposit is no longer pending manual confirmation."

    update_transaction_status(
        tx["id"],
        "Rejected",
        confirmed=False,
        note="Manual deposit rejected",
    )

    await bot.send_message(
        chat_id=int(tx["user_id"]),
        text=(
            "❌ <b>Your manual deposit was rejected.</b>\n\n"
            f"<b>Amount:</b> {format_money(tx['amount_usd'])}\n"
            f"<b>Updated At:</b> {format_dt(now_iso())}\n\n"
            "Please contact support if you need help."
        ),
        parse_mode="HTML",
    )
    return True, "Manual deposit rejected."


# =========================
# INVOICE STATUS / HOUSEKEEPING
# =========================

def get_open_invoices():
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM invoices
        WHERE status IN ('awaiting_payment', 'pending', 'checking')
        ORDER BY created_at ASC
    """).fetchall()
    conn.close()
    return rows


def expire_and_mark_stale_invoices():
    cancel_expired_invoices()

    conn = db_conn()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT *
        FROM invoices
        WHERE status = 'awaiting_payment'
    """).fetchall()

    for row in rows:
        if dt_is_expired(row["expires_at"]):
            cur.execute("""
                UPDATE invoices
                SET status = 'expired',
                    blockchain_status = 'expired',
                    problem_reason = 'Payment request expired.',
                    updated_at = ?
                WHERE invoice_id = ?
            """, (now_iso(), row["invoice_id"]))

    conn.commit()
    conn.close()


# =========================
# BACKGROUND RECHECK
# =========================

async def background_recheck_invoices(context: ContextTypes.DEFAULT_TYPE):
    expire_and_mark_stale_invoices()

    invoices = get_open_invoices()
    if not invoices:
        return

    for invoice in invoices:
        # only recheck invoices that already had at least one verify attempt
        verify_attempts = int(invoice["verify_attempts"] or 0)
        if verify_attempts <= 0:
            continue

        if invoice["status"] == "completed":
            continue

        result = auto_detect_payment_for_invoice(invoice)

        update_invoice_status(
            invoice_id=invoice["invoice_id"],
            status="checking" if result["status"] in {"pending", "manual_check_required"} else invoice["status"],
            blockchain_status=result["status"],
            problem_reason=result["reason"],
            increase_attempt=True,
        )

        await finalize_invoice_if_verified(context.bot, invoice, result)

        if result["status"] == "pending":
            # silent background state update only
            continue

        if result["status"] == "expired":
            try:
                await context.bot.send_message(
                    chat_id=int(invoice["user_id"]),
                    text=(
                        "⌛ <b>Payment request expired.</b>\n\n"
                        f"<b>Invoice:</b> <code>{escape_html(invoice['invoice_id'])}</code>\n"
                        "Please create a new payment request if you still want to continue."
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass


async def background_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        await background_recheck_invoices(context)
    except Exception as e:
        # safe fallback so Railway bot does not crash due to job error
        print(f"[background_job_error] {e}")


# =========================
# PART 6 OF 10 - END
# =========================

# =========================
# PART 7 OF 10 - START
# Paste this directly BELOW Part 6
# =========================

# =========================
# ADMIN MODE ENTER
# =========================

async def admin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not authorized.")
        return

    enter_admin_mode(user_id)

    await update.message.reply_text(
        "🔐 <b>ADMIN PANEL</b>\n\nChoose an option:",
        parse_mode="HTML",
        reply_markup=admin_menu(),
    )


# =========================
# ADMIN TEXT HANDLER
# =========================

async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    if not is_admin(user_id):
        return

    # -------------------------
    # dashboard
    # -------------------------
    if text == "📊 Dashboard":
        await update.message.reply_text(get_admin_dashboard_text(), parse_mode="HTML")
        return

    # -------------------------
    # search user
    # -------------------------
    if text == "🔍 Search User":
        admin_temp[user_id] = {"step": "search_user"}
        await update.message.reply_text("Send user ID to search:")
        return

    if admin_temp.get(user_id, {}).get("step") == "search_user":
        try:
            uid = int(text)
        except:
            await update.message.reply_text("Invalid user ID.")
            return

        await update.message.reply_text(
            get_user_search_summary_text(uid),
            parse_mode="HTML"
        )
        admin_temp[user_id] = {}
        return

    # -------------------------
    # stock menu
    # -------------------------
    if text == "📦 Stock":
        await update.message.reply_text(
            "Choose product to manage stock:",
            reply_markup=admin_product_keyboard()
        )
        return

    # -------------------------
    # add product
    # -------------------------
    if text == "➕ Add Product":
        admin_temp[user_id] = {"step": "add_product_name"}
        await update.message.reply_text("Send product name:")
        return

    step = admin_temp.get(user_id, {}).get("step")

    if step == "add_product_name":
        admin_temp[user_id]["name"] = text
        admin_temp[user_id]["step"] = "add_product_price"
        await update.message.reply_text("Send product price (USD):")
        return

    if step == "add_product_price":
        try:
            price = float(text)
        except:
            await update.message.reply_text("Invalid price.")
            return

        admin_temp[user_id]["price"] = price
        admin_temp[user_id]["step"] = "add_product_month"
        await update.message.reply_text("Send plan duration (month):")
        return

    if step == "add_product_month":
        admin_temp[user_id]["month"] = text
        admin_temp[user_id]["step"] = "add_product_icon"
        await update.message.reply_text("Send icon (emoji):")
        return

    if step == "add_product_icon":
        data = admin_temp[user_id]

        product_id = f"prod_{random.randint(1000,9999)}"

        conn = db_conn()
        conn.execute("""
            INSERT INTO products (product_id, name, price, month, icon, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            data["name"],
            str(data["price"]),
            data["month"],
            text,
            now_iso(),
            now_iso()
        ))
        conn.commit()
        conn.close()

        admin_temp[user_id] = {}

        await update.message.reply_text(f"✅ Product added: {data['name']}")
        return


# =========================
# ADMIN CALLBACK HANDLER
# =========================

async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""
    user_id = query.from_user.id

    if not is_admin(user_id):
        return

    # -------------------------
    # view product stock
    # -------------------------
    if data.startswith("admin_prod_"):
        product_id = data.replace("admin_prod_", "")

        conn = db_conn()
        rows = conn.execute("""
            SELECT * FROM stock_accounts
            WHERE product_id = ?
        """, (product_id,)).fetchall()
        conn.close()

        text = f"📦 <b>Stock for {product_id}</b>\n\nTotal: {len(rows)}"

        await query.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Stock", callback_data=f"add_stock_{product_id}")]
            ])
        )
        return

    # -------------------------
    # add stock
    # -------------------------
    if data.startswith("add_stock_"):
        product_id = data.replace("add_stock_", "")
        admin_temp[user_id] = {
            "step": "add_stock_input",
            "product_id": product_id
        }

        await query.message.reply_text(
            "Send stock in format:\nemail:password"
        )
        return

    if admin_temp.get(user_id, {}).get("step") == "add_stock_input":
        product_id = admin_temp[user_id]["product_id"]

        try:
            email, password = (update.message.text or "").split(":")
        except:
            await update.message.reply_text("Invalid format. Use email:password")
            return

        conn = db_conn()
        conn.execute("""
            INSERT INTO stock_accounts (product_id, email, password, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            product_id,
            email.strip(),
            password.strip(),
            now_iso(),
            now_iso()
        ))
        conn.commit()
        conn.close()

        admin_temp[user_id] = {}

        await update.message.reply_text("✅ Stock added.")
        return

    # -------------------------
    # manual order confirm/reject
    # -------------------------
    if data.startswith("approve_order_"):
        order_id = int(data.replace("approve_order_", ""))
        ok, msg = await confirm_manual_order(context.bot, order_id)
        await query.message.reply_text(msg)
        return

    if data.startswith("reject_order_"):
        order_id = int(data.replace("reject_order_", ""))
        ok, msg = await reject_manual_order(context.bot, order_id)
        await query.message.reply_text(msg)
        return

    # -------------------------
    # manual deposit confirm/reject
    # -------------------------
    if data.startswith("approve_dep_"):
        tx_id = int(data.replace("approve_dep_", ""))
        ok, msg = await confirm_manual_deposit(context.bot, tx_id)
        await query.message.reply_text(msg)
        return

    if data.startswith("reject_dep_"):
        tx_id = int(data.replace("reject_dep_", ""))
        ok, msg = await reject_manual_deposit(context.bot, tx_id)
        await query.message.reply_text(msg)
        return


# =========================
# ADMIN DASHBOARD
# =========================

def get_admin_dashboard_text():
    conn = db_conn()

    users = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    orders = conn.execute("SELECT COUNT(*) as c FROM orders").fetchone()["c"]
    txs = conn.execute("SELECT COUNT(*) as c FROM transactions").fetchone()["c"]

    revenue = conn.execute("""
        SELECT COALESCE(SUM(CAST(payable_total_usd AS REAL)),0) as r
        FROM orders WHERE status='Completed'
    """).fetchone()["r"]

    conn.close()

    return (
        "📊 <b>DASHBOARD</b>\n\n"
        f"Users: {users}\n"
        f"Orders: {orders}\n"
        f"Transactions: {txs}\n"
        f"Revenue: ${round(revenue,2)}"
    )


# =========================
# ADMIN PRODUCT KEYBOARD
# =========================

def admin_product_keyboard():
    products = get_all_products()

    buttons = []
    for p in products:
        buttons.append([InlineKeyboardButton(
            p["name"],
            callback_data=f"admin_prod_{p['product_id']}"
        )])

    return InlineKeyboardMarkup(buttons)


# =========================
# PART 7 OF 10 - END
# =========================

# =========================
# PART 8 OF 10 - START
# Paste this directly BELOW Part 7
# =========================

# =========================
# ADMIN RENDER HELPERS
# =========================

def render_admin_products_list():
    products = get_all_products()
    if not products:
        return "📦 <b>PRODUCT LIST</b>\n\nNo products found."

    lines = ["📦 <b>PRODUCT LIST</b>\n"]
    for p in products:
        real_stock = get_real_stock_count(p["product_id"])
        lines.append(
            f"\n<b>{escape_html(p['name'])}</b>\n"
            f"ID: <code>{escape_html(p['product_id'])}</code>\n"
            f"Price: {format_money(p['price'])}\n"
            f"Plan: {escape_html(p['month'])} month\n"
            f"Display Stock: {p['display_stock']}\n"
            f"Real Stock: {real_stock}\n"
            f"Created: {format_dt(p['created_at'])}\n"
            f"Updated: {format_dt(p['updated_at'])}"
        )
    return "\n".join(lines)


def render_admin_stock_list():
    products = get_all_products()
    if not products:
        return "📥 <b>STOCK LIST</b>\n\nNo products found."

    lines = ["📥 <b>STOCK LIST</b>\n"]
    for p in products:
        real_stock = get_real_stock_count(p["product_id"])
        lines.append(
            f"\n<b>{escape_html(p['name'])}</b>\n"
            f"ID: <code>{escape_html(p['product_id'])}</code>\n"
            f"Visible Stock: {p['display_stock']}\n"
            f"Real Stock: {real_stock}"
        )
    return "\n".join(lines)


def render_pending_manual_orders():
    rows = get_pending_manual_orders()
    if not rows:
        return "📦 <b>PENDING MANUAL ORDERS</b>\n\nNo pending manual orders."

    lines = ["📦 <b>PENDING MANUAL ORDERS</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>Order #{row['id']}</b>\n"
            f"User: <code>{row['user_id']}</code>\n"
            f"Product: {escape_html(row['product_name'])}\n"
            f"Qty: {row['qty']}\n"
            f"Payable: {format_money(row['payable_total_usd'])}\n"
            f"Status: {escape_html(row['status'])}\n"
            f"Created: {format_dt(row['created_at'])}"
        )
    return "\n".join(lines)


def render_pending_manual_deposits():
    rows = get_pending_manual_deposits()
    if not rows:
        return "💳 <b>PENDING MANUAL DEPOSITS</b>\n\nNo pending manual deposits."

    lines = ["💳 <b>PENDING MANUAL DEPOSITS</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>TX #{row['id']}</b>\n"
            f"User: <code>{row['user_id']}</code>\n"
            f"Amount: {format_money(row['amount_usd'])}\n"
            f"Status: {escape_html(row['status'])}\n"
            f"Created: {format_dt(row['created_at'])}"
        )
    return "\n".join(lines)


def render_all_orders_admin(limit=50):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM orders
        ORDER BY id DESC
        LIMIT ?
    """, (int(limit),)).fetchall()
    conn.close()

    if not rows:
        return "📦 <b>ALL ORDERS</b>\n\nNo orders found."

    lines = ["📦 <b>ALL ORDERS</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>Order #{row['id']}</b>\n"
            f"User: <code>{row['user_id']}</code>\n"
            f"Product: {escape_html(row['product_name'])}\n"
            f"Qty: {row['qty']}\n"
            f"Base Total: {format_money(row['base_total_usd'])}\n"
            f"Payable: {format_money(row['payable_total_usd'])}\n"
            f"Payment Type: {escape_html(row['payment_type'])}\n"
            f"Status: <b>{escape_html(row['status'])}</b>\n"
            f"Created: {format_dt(row['created_at'])}\n"
            f"Paid: {format_dt(row['paid_at'])}\n"
            f"Delivered: {format_dt(row['delivered_at'])}"
        )
    return "\n".join(lines)


def render_all_deposits_admin(limit=50):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM transactions
        WHERE tx_type = 'Deposit'
        ORDER BY id DESC
        LIMIT ?
    """, (int(limit),)).fetchall()
    conn.close()

    if not rows:
        return "💳 <b>ALL DEPOSITS</b>\n\nNo deposits found."

    lines = ["💳 <b>ALL DEPOSITS</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>TX #{row['id']}</b>\n"
            f"User: <code>{row['user_id']}</code>\n"
            f"USD Amount: {format_money(row['amount_usd'])}\n"
            f"Network: {escape_html(row['network'] or 'N/A')}\n"
            f"Coin Amount: {escape_html(str(row['coin_amount'] or 'N/A'))} {escape_html(str(row['coin_symbol'] or ''))}\n"
            f"Status: <b>{escape_html(row['status'])}</b>\n"
            f"Created: {format_dt(row['created_at'])}\n"
            f"Confirmed: {format_dt(row['confirmed_at'])}"
        )
    return "\n".join(lines)


def admin_orders_keyboard():
    rows = []
    for row in get_pending_manual_orders():
        rows.append([
            InlineKeyboardButton(
                f"Approve Order #{row['id']}",
                callback_data=f"approve_order_{row['id']}"
            ),
            InlineKeyboardButton(
                f"Reject #{row['id']}",
                callback_data=f"reject_order_{row['id']}"
            ),
        ])

    if not rows:
        rows.append([InlineKeyboardButton("No pending manual orders", callback_data="noop_admin")])

    return InlineKeyboardMarkup(rows)


def admin_deposits_keyboard():
    rows = []
    for row in get_pending_manual_deposits():
        rows.append([
            InlineKeyboardButton(
                f"Approve Deposit #{row['id']}",
                callback_data=f"approve_dep_{row['id']}"
            ),
            InlineKeyboardButton(
                f"Reject #{row['id']}",
                callback_data=f"reject_dep_{row['id']}"
            ),
        ])

    if not rows:
        rows.append([InlineKeyboardButton("No pending manual deposits", callback_data="noop_admin")])

    return InlineKeyboardMarkup(rows)


# =========================
# ADMIN MENU ACTIONS
# =========================

async def admin_menu_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    if not is_admin(user_id):
        return False

    if text == "📦 Products":
        await update.message.reply_text(
            render_admin_products_list(),
            parse_mode="HTML",
        )
        return True

    if text == "📥 Stock":
        await update.message.reply_text(
            render_admin_stock_list(),
            parse_mode="HTML",
            reply_markup=admin_product_keyboard(),
        )
        return True

    if text == "🎟 Promo Admin":
        await update.message.reply_text(
            "Promo admin can be expanded later.\nUse current promo flow from client side for now."
        )
        return True

    if text == "📦 Orders Admin":
        await update.message.reply_text(
            render_all_orders_admin(),
            parse_mode="HTML",
        )
        await update.message.reply_text(
            render_pending_manual_orders(),
            parse_mode="HTML",
            reply_markup=admin_orders_keyboard(),
        )
        return True

    if text == "💳 Deposits Admin":
        await update.message.reply_text(
            render_all_deposits_admin(),
            parse_mode="HTML",
        )
        await update.message.reply_text(
            render_pending_manual_deposits(),
            parse_mode="HTML",
            reply_markup=admin_deposits_keyboard(),
        )
        return True

    if text == "👤 Users Admin":
        await update.message.reply_text(
            get_admin_users_summary(),
            parse_mode="HTML",
        )
        return True

    if text == "📊 Analytics":
        await update.message.reply_text(
            get_admin_analytics_text(),
            parse_mode="HTML",
        )
        return True

    return False


def get_admin_users_summary():
    conn = db_conn()

    total_users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    wallet_total = conn.execute("""
        SELECT COALESCE(SUM(CAST(wallet_balance AS REAL)), 0) AS total
        FROM users
    """).fetchone()["total"]

    conn.close()

    return (
        "👤 <b>USERS SUMMARY</b>\n\n"
        f"Total Users: {total_users}\n"
        f"Total Wallet Balance: ${round(wallet_total, 2)}\n"
        f"Checked At: {format_dt(now_iso())}"
    )


def get_admin_analytics_text():
    conn = db_conn()

    total_users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    total_orders = conn.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"]
    completed_orders = conn.execute("SELECT COUNT(*) AS c FROM orders WHERE status = 'Completed'").fetchone()["c"]
    pending_orders = conn.execute("""
        SELECT COUNT(*) AS c FROM orders
        WHERE status IN ('Pending Payment', 'Checking Payment', 'Awaiting Manual Confirmation')
    """).fetchone()["c"]

    total_deposits = conn.execute("""
        SELECT COUNT(*) AS c FROM transactions
        WHERE tx_type = 'Deposit'
    """).fetchone()["c"]

    completed_deposits = conn.execute("""
        SELECT COUNT(*) AS c FROM transactions
        WHERE tx_type = 'Deposit' AND status = 'Completed'
    """).fetchone()["c"]

    revenue = conn.execute("""
        SELECT COALESCE(SUM(CAST(payable_total_usd AS REAL)), 0) AS total
        FROM orders
        WHERE status = 'Completed'
    """).fetchone()["total"]

    conn.close()

    return (
        "📊 <b>ANALYTICS</b>\n\n"
        f"Users: {total_users}\n"
        f"Orders: {total_orders}\n"
        f"Completed Orders: {completed_orders}\n"
        f"Pending Orders: {pending_orders}\n"
        f"Deposit Records: {total_deposits}\n"
        f"Completed Deposits: {completed_deposits}\n"
        f"Revenue: ${round(revenue, 2)}\n"
        f"Generated At: {format_dt(now_iso())}"
    )


# =========================
# MASTER TEXT ROUTER
# This wraps client flow + admin flow together.
# =========================

async def master_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    # 1) continuation text flows first
    handled = await continue_text_flow(update, context)
    if handled:
        return

    mode = get_user_mode(user_id)

    # 2) admin menu actions
    if mode == "admin":
        admin_menu_handled = await admin_menu_action_handler(update, context)
        if admin_menu_handled:
            return

        await admin_text_handler(update, context)
        return

    # 3) normal client main text handler
    await text_handler(update, context)


# =========================
# MASTER CALLBACK ROUTER
# This combines all callback parts.
# =========================

async def master_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""

    if data == "noop_admin":
        await query.answer("Nothing pending right now.")
        return

    # run part 1 router
    await callback_handler(update, context)

    # run part 2 router
    await callback_handler_part_2(update, context)

    # run admin router
    await admin_callback_handler(update, context)


# =========================
# SAFE PROMO BUTTON HELPER (optional future use)
# =========================

def promo_admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Promo feature can be expanded later", callback_data="noop_admin")]
    ])


# =========================
# PART 8 OF 10 - END
# =========================

# =========================
# PART 9 OF 10 - START
# Paste this directly BELOW Part 8
# =========================

# =========================
# TXID CHECKER FULL FLOW
# =========================

def get_latest_active_invoice_for_user_and_network(user_id, network):
    conn = db_conn()
    row = conn.execute("""
        SELECT *
        FROM invoices
        WHERE user_id = ?
          AND network = ?
          AND status IN ('awaiting_payment', 'pending', 'checking')
        ORDER BY created_at DESC
        LIMIT 1
    """, (int(user_id), network)).fetchone()
    conn.close()
    return row


def get_invoice_by_txid(txid):
    conn = db_conn()
    row = conn.execute("""
        SELECT *
        FROM invoices
        WHERE blockchain_txid = ?
        ORDER BY updated_at DESC
        LIMIT 1
    """, (txid,)).fetchone()
    conn.close()
    return row


async def handle_checker_txid_with_network(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, txid, network):
    invoice = get_latest_active_invoice_for_user_and_network(user_id, network)

    if invoice:
        expected_amount = safe_decimal(invoice["coin_amount"], Decimal("0"))
        expected_to = invoice["deposit_address"]

        result = verify_crypto_payment_by_txid(
            network=network,
            txid=txid,
            expected_coin_amount=expected_amount,
            expected_to_address=expected_to,
        )

        update_invoice_status(
            invoice_id=invoice["invoice_id"],
            status="checking" if result["status"] in {"pending", "amount_mismatch", "manual_check_required"} else invoice["status"],
            blockchain_txid=txid,
            blockchain_status=result["status"],
            problem_reason=result["reason"],
            increase_attempt=True,
        )

        await finalize_invoice_if_verified(context.bot, invoice, result)

        await update.message.reply_text(
            render_txid_check_result(network, txid, result),
            parse_mode="HTML",
        )
        return

    # fallback: known txid already linked before
    existing_invoice = get_invoice_by_txid(txid)
    if existing_invoice:
        await update.message.reply_text(
            "ℹ️ This TXID is already linked with a payment request.\n\n"
            f"Invoice: <code>{escape_html(existing_invoice['invoice_id'])}</code>\n"
            f"Status: <b>{escape_html(existing_invoice['status'])}</b>\n"
            f"Updated: {format_dt(existing_invoice['updated_at'])}",
            parse_mode="HTML",
        )
        return

    # fallback: no active invoice
    eta = NETWORK_CONFIRMATION_GUIDE.get(network, "Usually a few minutes")
    await update.message.reply_text(
        "🛠 <b>TXID CHECK RESULT</b>\n\n"
        f"<b>Network:</b> {escape_html(network)}\n"
        f"<b>TXID:</b> <code>{escape_html(txid)}</code>\n"
        f"<b>Status:</b> <b>Reference Missing</b>\n"
        "The bot could not find an active invoice on your account for this network.\n\n"
        "That means one of these is true:\n"
        "• the payment request already expired\n"
        "• the payment belongs to an older request\n"
        "• the payment was sent without creating a request first\n"
        "• the payment belongs to another account\n\n"
        f"<b>Estimated Confirmation Time:</b> {escape_html(eta)}\n"
        f"<b>Checked At:</b> {format_dt(now_iso())}\n\n"
        "Please contact support with your TXID if you already paid.",
        parse_mode="HTML",
    )


# =========================
# CONTINUE TEXT FLOW EXTENSION
# This extends the earlier continue_text_flow logic.
# =========================

async def continue_text_flow_part_2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    ensure_user(user_id)
    state = get_user_state(user_id)
    step = state.get("step", "")

    if step == "awaiting_txid_network":
        network = text.strip()

        txid = admin_temp.get(user_id, {}).get("checker_txid")
        if not txid:
            set_user_state(user_id, {"step": "main"})
            await update.message.reply_text("TXID session expired. Please open Payment Checker again.")
            return True

        if network not in CRYPTO_ADDRESSES:
            await update.message.reply_text(
                "Unsupported network.\nPlease send one of the exact supported network names."
            )
            return True

        set_user_state(user_id, {"step": "main"})
        await handle_checker_txid_with_network(update, context, user_id, txid, network)

        if user_id in admin_temp:
            admin_temp[user_id].pop("checker_txid", None)
            admin_temp[user_id].pop("checker_network", None)

        return True

    return False


# =========================
# MANUAL STATUS / PAYMENT PROBLEM HELPERS
# =========================

def mark_invoice_problem(invoice_id, reason, blockchain_status="problem"):
    update_invoice_status(
        invoice_id=invoice_id,
        status="checking",
        blockchain_status=blockchain_status,
        problem_reason=reason,
        increase_attempt=False,
    )


def set_related_records_rejected(invoice_id, note=None):
    tx = get_transaction_by_invoice(invoice_id)
    if tx:
        update_transaction_status(
            tx_id=tx["id"],
            status="Rejected",
            confirmed=False,
            note=note or "Rejected by system check",
        )

    order = get_order_by_invoice(invoice_id)
    if order and order["status"] not in {"Completed", "Cancelled"}:
        update_order_status(order["id"], "Rejected")


def set_related_records_pending(invoice_id, note=None):
    tx = get_transaction_by_invoice(invoice_id)
    if tx and tx["status"] not in {"Completed"}:
        update_transaction_status(
            tx_id=tx["id"],
            status="Checking Payment",
            confirmed=False,
            note=note or "Pending blockchain confirmation",
        )

    order = get_order_by_invoice(invoice_id)
    if order and order["status"] not in {"Completed", "Cancelled"}:
        update_order_status(order["id"], "Checking Payment")


def set_related_records_awaiting_manual(invoice_id, note=None):
    tx = get_transaction_by_invoice(invoice_id)
    if tx and tx["status"] != "Completed":
        update_transaction_status(
            tx_id=tx["id"],
            status="Awaiting Manual Confirmation",
            confirmed=False,
            note=note or "Manual review required",
        )

    order = get_order_by_invoice(invoice_id)
    if order and order["status"] not in {"Completed", "Cancelled"}:
        update_order_status(order["id"], "Awaiting Manual Confirmation")


# =========================
# BETTER INVOICE VERIFY CALLBACK
# Upgrades the earlier verify callback by routing problem statuses.
# =========================

async def handle_invoice_verify_callback_v2(query, user_id, invoice_id):
    invoice = get_invoice(invoice_id)

    if not invoice:
        await query.message.reply_text("❌ Payment request not found.")
        return

    if int(invoice["user_id"]) != int(user_id) and not is_admin(user_id):
        await query.message.reply_text("❌ You are not allowed to verify this request.")
        return

    if invoice["status"] == "completed":
        await query.message.reply_text(
            "✅ This payment request is already completed.\n\n"
            f"Invoice: <code>{escape_html(invoice['invoice_id'])}</code>\n"
            f"Paid At: {format_dt(invoice['paid_at'])}",
            parse_mode="HTML",
        )
        return

    if invoice["status"] == "cancelled":
        await query.message.reply_text("❌ This payment request was cancelled.")
        return

    if dt_is_expired(invoice["expires_at"]):
        update_invoice_status(
            invoice_id=invoice_id,
            status="expired",
            blockchain_status="expired",
            problem_reason="Payment request expired.",
        )
        await query.message.reply_text(
            "⌛ This payment request has expired.\nPlease create a new payment request."
        )
        return

    result = auto_detect_payment_for_invoice(invoice)

    if result["status"] == "pending":
        set_related_records_pending(invoice_id, note=result["reason"])

    elif result["status"] == "manual_check_required":
        mark_invoice_problem(invoice_id, result["reason"], blockchain_status="manual_check_required")

    elif result["status"] == "amount_mismatch":
        set_related_records_awaiting_manual(invoice_id, note=result["reason"])
        mark_invoice_problem(invoice_id, result["reason"], blockchain_status="amount_mismatch")

    elif result["status"] == "rejected":
        set_related_records_rejected(invoice_id, note=result["reason"])
        update_invoice_status(
            invoice_id=invoice_id,
            status="rejected",
            blockchain_status="rejected",
            problem_reason=result["reason"],
            increase_attempt=True,
        )

    else:
        update_invoice_status(
            invoice_id=invoice_id,
            status="checking" if result["status"] in {"pending", "manual_check_required", "amount_mismatch"} else invoice["status"],
            blockchain_status=result["status"],
            problem_reason=result["reason"],
            increase_attempt=True,
        )

    await finalize_invoice_if_verified(context=query._bot if hasattr(query, "_bot") else None, invoice_row=invoice, verify_result_obj=result)

    await query.message.reply_text(
        render_invoice_verify_result(invoice, result),
        parse_mode="HTML",
    )


# =========================
# HISTORY / DETAIL HELPERS
# =========================

def render_single_invoice_details(invoice_id):
    invoice = get_invoice(invoice_id)
    if not invoice:
        return "Invoice not found."

    return (
        "📄 <b>PAYMENT REQUEST DETAILS</b>\n\n"
        f"<b>Invoice ID:</b> <code>{escape_html(invoice['invoice_id'])}</code>\n"
        f"<b>Type:</b> {escape_html(invoice['invoice_type'])}\n"
        f"<b>Network:</b> {escape_html(invoice['network'])}\n"
        f"<b>Coin Amount:</b> {escape_html(invoice['coin_amount'])} {escape_html(invoice['coin_symbol'])}\n"
        f"<b>Base Amount:</b> {format_money(invoice['amount_base_usd'])}\n"
        f"<b>Extra Charge:</b> {format_money(invoice['amount_extra_usd'])}\n"
        f"<b>Final Payable:</b> {format_money(invoice['amount_payable_usd'])}\n"
        f"<b>Status:</b> <b>{escape_html(invoice['status'])}</b>\n"
        f"<b>Blockchain Status:</b> {escape_html(invoice['blockchain_status'] or 'N/A')}\n"
        f"<b>Problem:</b> {escape_html(invoice['problem_reason'] or 'None')}\n"
        f"<b>Created:</b> {format_dt(invoice['created_at'])}\n"
        f"<b>Paid:</b> {format_dt(invoice['paid_at'])}\n"
        f"<b>Expires:</b> {format_dt(invoice['expires_at'])}\n"
        f"<b>Updated:</b> {format_dt(invoice['updated_at'])}"
    )


def get_recent_invoices_for_user(user_id, limit=10):
    conn = db_conn()
    rows = conn.execute("""
        SELECT *
        FROM invoices
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (int(user_id), int(limit))).fetchall()
    conn.close()
    return rows


def render_recent_invoice_history(user_id):
    rows = get_recent_invoices_for_user(user_id, limit=15)
    if not rows:
        return "📄 <b>PAYMENT HISTORY</b>\n\nNo payment requests found."

    lines = ["📄 <b>PAYMENT HISTORY</b>\n"]
    for row in rows:
        lines.append(
            f"\n<b>{escape_html(row['invoice_id'])}</b>\n"
            f"Type: {escape_html(row['invoice_type'])}\n"
            f"Network: {escape_html(row['network'])}\n"
            f"Final Payable: {format_money(row['amount_payable_usd'])}\n"
            f"Status: <b>{escape_html(row['status'])}</b>\n"
            f"Created: {format_dt(row['created_at'])}\n"
            f"Paid: {format_dt(row['paid_at'])}\n"
            f"Updated: {format_dt(row['updated_at'])}"
        )
    return "\n".join(lines)


# =========================
# STARTUP / BOOTSTRAP HELPERS
# =========================

def bootstrap_system():
    init_db()
    seed_default_data()
    expire_and_mark_stale_invoices()


def get_bot_runtime_summary():
    return (
        "✅ Bot bootstrap completed.\n"
        f"Database: {escape_html(DB_PATH)}\n"
        f"Started At: {format_dt(now_iso())}"
    )


# =========================
# MASTER TEXT ROUTER OVERRIDE
# This extends Part 8 master router.
# =========================

async def master_text_handler_v2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    handled = await continue_text_flow(update, context)
    if handled:
        return

    handled2 = await continue_text_flow_part_2(update, context)
    if handled2:
        return

    mode = get_user_mode(user_id)

    if mode == "admin":
        admin_menu_handled = await admin_menu_action_handler(update, context)
        if admin_menu_handled:
            return

        await admin_text_handler(update, context)
        return

    await text_handler(update, context)


# =========================
# CALLBACK ROUTER OVERRIDE
# This extends earlier callback routing with upgraded invoice verify.
# =========================

async def master_callback_handler_v2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""

    if data == "noop_admin":
        await query.answer("Nothing pending right now.")
        return

    # upgraded verify action
    if data.startswith("verify_invoice_"):
        invoice_id = data.replace("verify_invoice_", "")
        return await handle_invoice_verify_callback(query, query.from_user.id, invoice_id)

    # old part routers
    await callback_handler(update, context)
    await callback_handler_part_2(update, context)
    await admin_callback_handler(update, context)


# =========================
# PART 9 OF 10 - END
# =========================

# =========================
# PART 10 OF 10 - START
# Paste this directly BELOW Part 9
# =========================

# =========================
# EXTRA COMMANDS
# =========================

async def myid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Your Telegram ID: <code>{update.effective_user.id}</code>",
        parse_mode="HTML",
    )


async def recent_payments_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    await update.message.reply_text(
        render_recent_invoice_history(user_id),
        parse_mode="HTML",
    )


async def invoice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    if not context.args:
        await update.message.reply_text(
            "Usage:\n/invoice YOUR_INVOICE_ID\n\nExample:\n/invoice INV-ABC1234567890"
        )
        return

    invoice_id = context.args[0].strip()
    invoice = get_invoice(invoice_id)

    if not invoice:
        await update.message.reply_text("Invoice not found.")
        return

    if int(invoice["user_id"]) != int(user_id) and not is_admin(user_id):
        await update.message.reply_text("You are not allowed to view this invoice.")
        return

    await update.message.reply_text(
        render_single_invoice_details(invoice_id),
        parse_mode="HTML",
        reply_markup=payment_invoice_keyboard(invoice_id, invoice["deposit_address"])
        if invoice["status"] in {"awaiting_payment", "pending", "checking"}
        else None,
    )


# =========================
# OPTIONAL ADMIN QUICK STOCK COMMAND
# =========================

async def addstock_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not allowed to use this command.")
        return

    if len(context.args) < 3:
        await update.message.reply_text(
            "Usage:\n/addstock PRODUCT_ID email password\n\nExample:\n/addstock p1 test@example.com Pass1234"
        )
        return

    product_id = context.args[0].strip()
    email = context.args[1].strip()
    password = context.args[2].strip()

    product = get_product(product_id)
    if not product:
        await update.message.reply_text("❌ Invalid product ID.")
        return

    conn = db_conn()
    conn.execute("""
        INSERT INTO stock_accounts (
            product_id, email, password, note, is_delivered,
            delivered_to_user_id, delivered_order_id, delivered_at,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, 0, NULL, NULL, NULL, ?, ?)
    """, (
        product_id,
        email,
        password,
        "Added by admin command",
        now_iso(),
        now_iso(),
    ))
    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"✅ Stock added to {product['name']}\n"
        f"Real stock now: {get_real_stock_count(product_id)}"
    )


# =========================
# APP FACTORY
# =========================

def build_application():
    bootstrap_system()

    app = Application.builder().token(BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("admin", admin_handler))
    app.add_handler(CommandHandler("myid", myid_handler))
    app.add_handler(CommandHandler("invoice", invoice_handler))
    app.add_handler(CommandHandler("payments", recent_payments_handler))
    app.add_handler(CommandHandler("addstock", addstock_handler))

    # callbacks
    app.add_handler(CallbackQueryHandler(master_callback_handler_v2))

    # text messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, master_text_handler_v2))

    # background job
    if app.job_queue:
        app.job_queue.run_repeating(
            background_job,
            interval=RECHECK_INTERVAL_SECONDS,
            first=RECHECK_INTERVAL_SECONDS,
        )

    return app


# =========================
# MAIN ENTRY
# =========================

def main():
    app = build_application()

    print(get_bot_runtime_summary())
    print("✅ Bot is running...")

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

# =========================
# PART 10 OF 10 - END
# =========================
