# ============
# Part 1 Start From here
# ===========

import os
import sqlite3
import hashlib
import random
import string
import requests

from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Tuple

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =========================================================
# BASIC SETTINGS
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "SupremeLeaderShopBot").replace("@", "").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@serpstacking").strip()

# comma separated admin ids in Railway variable, example: 12345,67890
ADMIN_IDS_ENV = os.getenv("ADMIN_IDS", "6795246172").strip()

BINANCE_ID = os.getenv("BINANCE_ID", "828543482").strip()
BYBIT_ID = os.getenv("BYBIT_ID", "199582741").strip()

TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "").strip()
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()

DATABASE_PATH = os.getenv("DATABASE_PATH", "shopbot.db").strip()

# crypto price api
COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"

# blockchain api bases
TRONGRID_BASE = "https://api.trongrid.io"
ETHERSCAN_V2_URL = "https://api.etherscan.io/v2/api"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
BTC_API_BASE = "https://mempool.space/api"
LTC_API_BASE = "https://litecoinspace.org/api"

# app behavior
RECHECK_INTERVAL_SECONDS = 20
MAX_RECHECK_ATTEMPTS = 18
REQUEST_TIMEOUT_SECONDS = 25

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set. Please set it in Railway Variables.")

try:
    ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_ENV.split(",") if x.strip()}
except ValueError:
    raise ValueError("ADMIN_IDS must be comma separated integer Telegram IDs.")

# =========================================================
# RECEIVE ADDRESSES
# =========================================================

USDT_TRC20_RECEIVE_ADDRESS = os.getenv("USDT_TRC20_RECEIVE_ADDRESS", "TFWMEL6o5Kxnh1h25XMuWG6b6HaeF7vNf1").strip()
USDT_ERC20_RECEIVE_ADDRESS = os.getenv("USDT_ERC20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()
USDT_BEP20_RECEIVE_ADDRESS = os.getenv("USDT_BEP20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()
LTC_RECEIVE_ADDRESS = os.getenv("LTC_RECEIVE_ADDRESS", "LQcmsEwAHuyWyY3Heu2XMYShfirxomCVtk").strip()
BTC_RECEIVE_ADDRESS = os.getenv("BTC_RECEIVE_ADDRESS", "15ykQZeq9jQTjJEzY2faG4LpirS2bYcb8L").strip()
BNB_BEP20_RECEIVE_ADDRESS = os.getenv("BNB_BEP20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()
SOL_RECEIVE_ADDRESS = os.getenv("SOL_RECEIVE_ADDRESS", "23MdGndZ85eJR58JWHiHNFmrQDMU1Leipzhnx4wtgnWE").strip()
TRX_RECEIVE_ADDRESS = os.getenv("TRX_RECEIVE_ADDRESS", "TFWMEL6o5Kxnh1h25XMuWG6b6HaeF7vNf1").strip()
ETH_ERC20_RECEIVE_ADDRESS = os.getenv("ETH_ERC20_RECEIVE_ADDRESS", "0x0bf8d98f93f31b879cb72005a01f0a0f5f3f4331").strip()

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

# =========================================================
# CONTRACTS / CHAIN CONFIG
# =========================================================

USDT_TRC20_CONTRACT = os.getenv("USDT_TRC20_CONTRACT", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").strip()
USDT_ERC20_CONTRACT = os.getenv("USDT_ERC20_CONTRACT", "0xdAC17F958D2ee523a2206206994597C13D831ec7").strip()
USDT_BEP20_CONTRACT = os.getenv("USDT_BEP20_CONTRACT", "0x55d398326f99059fF775485246999027B3197955").strip()

ETH_CHAIN_ID = "1"
BSC_CHAIN_ID = "56"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

NETWORK_CONFIG = {
    "USDT (TRC20)": {
        "symbol": "USDT",
        "coingecko_id": None,
        "decimals": 6,
        "price_decimals": 2,
        "tolerance": Decimal("0.05"),
        "address": USDT_TRC20_RECEIVE_ADDRESS,
    },
    "USDT (ERC20)": {
        "symbol": "USDT",
        "coingecko_id": None,
        "decimals": 6,
        "price_decimals": 2,
        "tolerance": Decimal("0.05"),
        "address": USDT_ERC20_RECEIVE_ADDRESS,
    },
    "USDT (BEP20)": {
        "symbol": "USDT",
        "coingecko_id": None,
        "decimals": 6,
        "price_decimals": 2,
        "tolerance": Decimal("0.05"),
        "address": USDT_BEP20_RECEIVE_ADDRESS,
    },
    "TRX (TRC20)": {
        "symbol": "TRX",
        "coingecko_id": "tron",
        "decimals": 3,
        "price_decimals": 3,
        "tolerance": Decimal("0.01"),
        "address": TRX_RECEIVE_ADDRESS,
    },
    "BTC": {
        "symbol": "BTC",
        "coingecko_id": "bitcoin",
        "decimals": 8,
        "price_decimals": 8,
        "tolerance": Decimal("0.00000100"),
        "address": BTC_RECEIVE_ADDRESS,
    },
    "LTC": {
        "symbol": "LTC",
        "coingecko_id": "litecoin",
        "decimals": 8,
        "price_decimals": 8,
        "tolerance": Decimal("0.00001000"),
        "address": LTC_RECEIVE_ADDRESS,
    },
    "ETH (ERC20)": {
        "symbol": "ETH",
        "coingecko_id": "ethereum",
        "decimals": 8,
        "price_decimals": 8,
        "tolerance": Decimal("0.00001000"),
        "address": ETH_ERC20_RECEIVE_ADDRESS,
    },
    "BNB (BEP20)": {
        "symbol": "BNB",
        "coingecko_id": "binancecoin",
        "decimals": 8,
        "price_decimals": 8,
        "tolerance": Decimal("0.00001000"),
        "address": BNB_BEP20_RECEIVE_ADDRESS,
    },
    "SOL": {
        "symbol": "SOL",
        "coingecko_id": "solana",
        "decimals": 6,
        "price_decimals": 6,
        "tolerance": Decimal("0.000100"),
        "address": SOL_RECEIVE_ADDRESS,
    },
}

# =========================================================
# DEFAULT PRODUCTS
# =========================================================

DEFAULT_PRODUCTS = {
    "p1": {
        "name": "Netflix Premium Account",
        "icon": "🎬",
        "month": "1",
        "price_usd": "5.00",
        "display_stock": 25,
        "details": [
            "✅ Private Account",
            "✅ Auto Delivery",
            "✅ Email:Password Delivery",
        ],
        "accounts": [
            {"email": "netflix1@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix2@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix3@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix4@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix5@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "netflix6@example.com", "password": "Pass1234", "note": "Private Account"},
        ],
    },
    "p2": {
        "name": "Spotify Premium Account",
        "icon": "🎵",
        "month": "1",
        "price_usd": "3.00",
        "display_stock": 18,
        "details": [
            "✅ Private Account",
            "✅ Auto Delivery",
            "✅ Email:Password Delivery",
        ],
        "accounts": [
            {"email": "spotify1@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "spotify2@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "spotify3@example.com", "password": "Pass1234", "note": "Private Account"},
            {"email": "spotify4@example.com", "password": "Pass1234", "note": "Private Account"},
        ],
    },
    "p3": {
        "name": "YouTube Premium Account",
        "icon": "▶️",
        "month": "1",
        "price_usd": "4.00",
        "display_stock": 0,
        "details": [
            "✅ Private Account",
            "✅ Auto Delivery",
            "✅ Email:Password Delivery",
        ],
        "accounts": [],
    },
}

DEFAULT_PROMOS = {
    "FREE5": {
        "amount": "5.00",
        "enabled": 1,
        "one_time": 1,
        "created_by": "system",
    },
    "BONUS10": {
        "amount": "10.00",
        "enabled": 1,
        "one_time": 1,
        "created_by": "system",
    },
}

# =========================================================
# RUNTIME MEMORY ONLY
# =========================================================

# runtime only; persistent data will stay in sqlite
notify_waitlist: Dict[str, set] = {}
price_cache: Dict[str, Dict[str, Any]] = {}

# =========================================================
# GENERAL HELPERS
# =========================================================

def now_dt() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_dt().isoformat()


def format_dt(dt_value: Optional[str]) -> str:
    if not dt_value:
        return "N/A"
    try:
        return datetime.fromisoformat(dt_value).astimezone().strftime("%Y-%m-%d %I:%M:%S %p")
    except Exception:
        return str(dt_value)


def safe_decimal(value: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def qdec(value: Any, places: str = "0.01") -> Decimal:
    dec = safe_decimal(value)
    if dec is None:
        return Decimal("0")
    return dec.quantize(Decimal(places), rounding=ROUND_DOWN)


def format_money(value: Any) -> str:
    dec = safe_decimal(value) or Decimal("0")
    return f"${dec.quantize(Decimal('0.01'))}"


def escape_html(text: Any) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_evm_address(addr: str) -> str:
    return str(addr or "").strip().lower()


def to_evm_topic_address(addr: str) -> str:
    return "0x" + normalize_evm_address(addr).replace("0x", "").rjust(64, "0")


def get_network_symbol(network_label: str) -> str:
    return NETWORK_CONFIG[network_label]["symbol"]


def amount_within_tolerance(actual_amount: Any, expected_amount: Any, tolerance: Any) -> bool:
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)
    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False
    return abs(actual_dec - expected_dec) <= tolerance_dec


def generate_unique_code(length: int = 10) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


def is_valid_txid_format(txid: str) -> bool:
    txid = txid.strip()

    # EVM
    if txid.startswith("0x") and len(txid) == 66:
        hex_body = txid[2:]
        return all(c in "0123456789abcdefABCDEF" for c in hex_body)

    # BTC/LTC style 64 hex
    if len(txid) == 64 and all(c in "0123456789abcdefABCDEF" for c in txid):
        return True

    # TRON / SOL / other base58-like
    base58_allowed = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    if 20 <= len(txid) <= 100 and all(ch in base58_allowed for ch in txid):
        return True

    return False


# =========================================================
# HTTP HELPERS
# =========================================================

def http_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = REQUEST_TIMEOUT_SECONDS) -> dict:
    try:
        res = requests.get(url, params=params, headers=headers, timeout=timeout)
        content_type = res.headers.get("content-type", "")
        data = res.json() if "json" in content_type.lower() else {}
        return {"ok": res.ok, "status_code": res.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def http_post_json(url: str, payload: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = REQUEST_TIMEOUT_SECONDS) -> dict:
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        content_type = res.headers.get("content-type", "")
        data = res.json() if "json" in content_type.lower() else {}
        return {"ok": res.ok, "status_code": res.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def verify_result(ok: bool, status: str, reason: str, actual_amount: Optional[str] = None) -> dict:
    return {
        "ok": ok,
        "status": status,   # confirmed / pending / rejected
        "reason": reason,
        "actual_amount": actual_amount,
    }


def trongrid_headers() -> dict:
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }
    if TRONGRID_API_KEY:
        headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY
    return headers


# =========================================================
# BASE58 / TRON HELPERS
# =========================================================

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


# =========================================================
# SQLITE
# =========================================================

@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def db_execute(query: str, params: Tuple = ()) -> None:
    with get_db() as conn:
        conn.execute(query, params)


def db_fetchone(query: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(query, params).fetchone()


def db_fetchall(query: str, params: Tuple = ()) -> List[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(query, params).fetchall()


def init_db() -> None:
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                wallet_balance TEXT NOT NULL DEFAULT '0',
                mode TEXT NOT NULL DEFAULT 'client',
                state_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                icon TEXT NOT NULL DEFAULT '📦',
                month_label TEXT NOT NULL,
                price_usd TEXT NOT NULL,
                display_stock INTEGER NOT NULL DEFAULT 0,
                details_text TEXT NOT NULL DEFAULT '',
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                email TEXT NOT NULL,
                password TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                is_delivered INTEGER NOT NULL DEFAULT 0,
                delivered_to INTEGER,
                delivered_order_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                product_id TEXT NOT NULL,
                product_name TEXT NOT NULL,
                qty INTEGER NOT NULL,
                total_usd TEXT NOT NULL,
                payment_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payment_request_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tx_type TEXT NOT NULL,
                amount_usd TEXT NOT NULL,
                status TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS promos (
                code TEXT PRIMARY KEY,
                amount_usd TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                one_time INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                used_by INTEGER,
                created_at TEXT NOT NULL,
                used_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_promos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                used_at TEXT NOT NULL,
                UNIQUE(user_id, code)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS payment_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                request_type TEXT NOT NULL,           -- deposit / order
                product_id TEXT,
                qty INTEGER,
                usd_amount TEXT NOT NULL,
                network TEXT NOT NULL,
                coin_symbol TEXT NOT NULL,
                coin_amount TEXT NOT NULL,
                tolerance_amount TEXT NOT NULL,
                receive_address TEXT NOT NULL,
                txid TEXT,
                tx_status TEXT NOT NULL DEFAULT 'awaiting_payment',
                verify_attempts INTEGER NOT NULL DEFAULT 0,
                linked_order_id INTEGER,
                linked_transaction_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS used_txids (
                txid TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                network TEXT NOT NULL,
                payment_request_id INTEGER,
                created_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_accounts_product_id ON product_accounts(product_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payment_requests_user_id ON payment_requests(user_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payment_requests_status ON payment_requests(tx_status)
        """)

    seed_defaults()


def seed_defaults() -> None:
    now = now_iso()

    for idx, (product_id, product) in enumerate(DEFAULT_PRODUCTS.items(), start=1):
        row = db_fetchone("SELECT product_id FROM products WHERE product_id = ?", (product_id,))
        if row is None:
            db_execute("""
                INSERT INTO products (
                    product_id, name, icon, month_label, price_usd, display_stock,
                    details_text, sort_order, is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, (
                product_id,
                product["name"],
                product["icon"],
                product["month"],
                product["price_usd"],
                int(product["display_stock"]),
                "\n".join(product["details"]),
                idx,
                now,
                now,
            ))

            for acc in product["accounts"]:
                db_execute("""
                    INSERT INTO product_accounts (
                        product_id, email, password, note, is_delivered,
                        delivered_to, delivered_order_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 0, NULL, NULL, ?, ?)
                """, (
                    product_id,
                    acc["email"],
                    acc["password"],
                    acc.get("note", ""),
                    now,
                    now,
                ))

    for code, info in DEFAULT_PROMOS.items():
        row = db_fetchone("SELECT code FROM promos WHERE code = ?", (code,))
        if row is None:
            db_execute("""
                INSERT INTO promos (
                    code, amount_usd, enabled, one_time, created_by, used_by, created_at, used_at
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL)
            """, (
                code,
                info["amount"],
                int(info["enabled"]),
                int(info["one_time"]),
                info["created_by"],
                now,
            ))


# =========================================================
# DB HELPERS
# =========================================================

def ensure_user(user_id: int) -> None:
    row = db_fetchone("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if row is None:
        now = now_iso()
        db_execute("""
            INSERT INTO users (user_id, wallet_balance, mode, state_json, created_at, updated_at)
            VALUES (?, '0', 'client', '{}', ?, ?)
        """, (user_id, now, now))


def get_user_mode(user_id: int) -> str:
    ensure_user(user_id)
    row = db_fetchone("SELECT mode FROM users WHERE user_id = ?", (user_id,))
    return row["mode"] if row else "client"


def set_user_mode(user_id: int, mode: str) -> None:
    ensure_user(user_id)
    db_execute("""
        UPDATE users SET mode = ?, updated_at = ? WHERE user_id = ?
    """, (mode, now_iso(), user_id))


def get_user_wallet(user_id: int) -> Decimal:
    ensure_user(user_id)
    row = db_fetchone("SELECT wallet_balance FROM users WHERE user_id = ?", (user_id,))
    return safe_decimal(row["wallet_balance"] if row else "0") or Decimal("0")


def set_user_wallet(user_id: int, amount: Decimal) -> None:
    ensure_user(user_id)
    db_execute("""
        UPDATE users SET wallet_balance = ?, updated_at = ? WHERE user_id = ?
    """, (str(amount), now_iso(), user_id))


def add_user_wallet(user_id: int, amount: Decimal) -> Decimal:
    current = get_user_wallet(user_id)
    new_balance = current + amount
    set_user_wallet(user_id, new_balance)
    return new_balance


def get_user_state(user_id: int) -> Dict[str, Any]:
    ensure_user(user_id)
    row = db_fetchone("SELECT state_json FROM users WHERE user_id = ?", (user_id,))
    raw = row["state_json"] if row else "{}"
    try:
        import json
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"step": "main"}


def set_user_state(user_id: int, state: Dict[str, Any]) -> None:
    ensure_user(user_id)
    import json
    db_execute("""
        UPDATE users SET state_json = ?, updated_at = ? WHERE user_id = ?
    """, (json.dumps(state, ensure_ascii=False), now_iso(), user_id))


def reset_user_state(user_id: int) -> None:
    set_user_state(user_id, {"step": "main"})


def enter_client_mode(user_id: int) -> None:
    set_user_mode(user_id, "client")
    reset_user_state(user_id)


def enter_admin_mode(user_id: int) -> None:
    set_user_mode(user_id, "admin")
    set_user_state(user_id, {"step": "admin_main"})


def get_product_rows() -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM products
        WHERE is_active = 1
        ORDER BY sort_order ASC, product_id ASC
    """)


def get_product_row(product_id: str) -> Optional[sqlite3.Row]:
    return db_fetchone("""
        SELECT * FROM products
        WHERE product_id = ? AND is_active = 1
    """, (product_id,))

# =============
# Part 1 End here
# =============

# ============
# Part 2A Start From here
# ============

# =========================================================
# PRODUCT / STOCK HELPERS
# =========================================================

def get_product_accounts(product_id: str) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM product_accounts
        WHERE product_id = ? AND is_delivered = 0
        ORDER BY id ASC
    """, (product_id,))


def get_product_available_stock(product_id: str) -> int:
    row = db_fetchone("""
        SELECT COUNT(*) as total FROM product_accounts
        WHERE product_id = ? AND is_delivered = 0
    """, (product_id,))
    return int(row["total"]) if row else 0


def get_product_display_stock(product_row: sqlite3.Row) -> int:
    return int(product_row["display_stock"])


def deliver_accounts(product_id: str, qty: int, user_id: int, order_id: int) -> List[sqlite3.Row]:
    accounts = get_product_accounts(product_id)
    selected = accounts[:qty]

    delivered = []
    now = now_iso()

    for acc in selected:
        db_execute("""
            UPDATE product_accounts
            SET is_delivered = 1,
                delivered_to = ?,
                delivered_order_id = ?,
                updated_at = ?
            WHERE id = ?
        """, (user_id, order_id, now, acc["id"]))
        delivered.append(acc)

    return delivered


# =========================================================
# PROMO HELPERS
# =========================================================

def get_promo(code: str) -> Optional[sqlite3.Row]:
    return db_fetchone("SELECT * FROM promos WHERE code = ?", (code.upper(),))


def mark_promo_used(user_id: int, code: str) -> None:
    now = now_iso()

    db_execute("""
        INSERT OR IGNORE INTO user_promos (user_id, code, used_at)
        VALUES (?, ?, ?)
    """, (user_id, code.upper(), now))

    db_execute("""
        UPDATE promos
        SET used_by = ?, used_at = ?
        WHERE code = ?
    """, (user_id, now, code.upper()))


def has_user_used_promo(user_id: int, code: str) -> bool:
    row = db_fetchone("""
        SELECT id FROM user_promos
        WHERE user_id = ? AND code = ?
    """, (user_id, code.upper()))
    return row is not None


# =========================================================
# ORDER / TRANSACTION HELPERS
# =========================================================

def create_order(user_id: int, product_row: sqlite3.Row, qty: int, total_usd: Decimal, payment_type: str) -> int:
    now = now_iso()
    db_execute("""
        INSERT INTO orders (
            user_id, product_id, product_name, qty,
            total_usd, payment_type, status,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
    """, (
        user_id,
        product_row["product_id"],
        product_row["name"],
        qty,
        str(total_usd),
        payment_type,
        now,
        now,
    ))

    row = db_fetchone("SELECT last_insert_rowid() as id")
    return int(row["id"])


def update_order_status(order_id: int, status: str) -> None:
    db_execute("""
        UPDATE orders
        SET status = ?, updated_at = ?
        WHERE id = ?
    """, (status, now_iso(), order_id))


def create_transaction(user_id: int, tx_type: str, amount_usd: Decimal, status: str, meta_json: str = "{}") -> int:
    now = now_iso()
    db_execute("""
        INSERT INTO transactions (
            user_id, tx_type, amount_usd, status,
            meta_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id,
        tx_type,
        str(amount_usd),
        status,
        meta_json,
        now,
        now,
    ))

    row = db_fetchone("SELECT last_insert_rowid() as id")
    return int(row["id"])


def update_transaction_status(tx_id: int, status: str) -> None:
    db_execute("""
        UPDATE transactions
        SET status = ?, updated_at = ?
        WHERE id = ?
    """, (status, now_iso(), tx_id))


# =========================================================
# PAYMENT REQUEST (CORE NEW SYSTEM)
# =========================================================

def create_payment_request(
    user_id: int,
    request_type: str,
    usd_amount: Decimal,
    network: str,
    product_id: Optional[str] = None,
    qty: Optional[int] = None,
) -> int:

    config = NETWORK_CONFIG[network]
    symbol = config["symbol"]
    tolerance = config["tolerance"]
    address = config["address"]

    coin_amount = calculate_crypto_amount(usd_amount, network)

    now = now_iso()

    db_execute("""
        INSERT INTO payment_requests (
            user_id, request_type, product_id, qty,
            usd_amount, network, coin_symbol, coin_amount,
            tolerance_amount, receive_address,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id,
        request_type,
        product_id,
        qty,
        str(usd_amount),
        network,
        symbol,
        str(coin_amount),
        str(tolerance),
        address,
        now,
        now,
    ))

    row = db_fetchone("SELECT last_insert_rowid() as id")
    return int(row["id"])


def get_payment_request(request_id: int) -> Optional[sqlite3.Row]:
    return db_fetchone("""
        SELECT * FROM payment_requests WHERE id = ?
    """, (request_id,))


def set_payment_txid(request_id: int, txid: str) -> None:
    db_execute("""
        UPDATE payment_requests
        SET txid = ?, tx_status = 'checking', updated_at = ?
        WHERE id = ?
    """, (txid, now_iso(), request_id))


def update_payment_status(request_id: int, status: str) -> None:
    db_execute("""
        UPDATE payment_requests
        SET tx_status = ?, updated_at = ?
        WHERE id = ?
    """, (status, now_iso(), request_id))


def increment_payment_attempt(request_id: int) -> None:
    db_execute("""
        UPDATE payment_requests
        SET verify_attempts = verify_attempts + 1,
            updated_at = ?
        WHERE id = ?
    """, (now_iso(), request_id))


# =========================================================
# TXID SECURITY (FIXED ISSUE)
# =========================================================

def is_txid_used(txid: str) -> bool:
    row = db_fetchone("""
        SELECT txid FROM used_txids WHERE txid = ?
    """, (txid,))
    return row is not None


def mark_txid_used(txid: str, user_id: int, network: str, request_id: int) -> None:
    db_execute("""
        INSERT OR IGNORE INTO used_txids (
            txid, user_id, network, payment_request_id, created_at
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        txid,
        user_id,
        network,
        request_id,
        now_iso(),
    ))

# ============
# Part 2A end here
# ============

# ============
# Part 2B Start From here
# ============

# =========================================================
# CRYPTO PRICE / EXACT AMOUNT HELPERS
# =========================================================

def get_cached_price(coingecko_id: str) -> Optional[Decimal]:
    cached = price_cache.get(coingecko_id)
    if not cached:
        return None

    ts = cached.get("ts")
    price = cached.get("price")
    if not ts or price is None:
        return None

    age = (now_dt() - ts).total_seconds()
    if age > 120:
        return None

    return safe_decimal(price)


def set_cached_price(coingecko_id: str, price: Decimal) -> None:
    price_cache[coingecko_id] = {
        "price": str(price),
        "ts": now_dt(),
    }


def get_usd_price_for_network(network: str) -> Optional[Decimal]:
    config = NETWORK_CONFIG.get(network)
    if not config:
        return None

    coin_id = config.get("coingecko_id")
    if not coin_id:
        # USDT-like fixed assumption
        return Decimal("1")

    cached = get_cached_price(coin_id)
    if cached is not None and cached > 0:
        return cached

    res = http_get_json(
        COINGECKO_SIMPLE_PRICE_URL,
        params={"ids": coin_id, "vs_currencies": "usd"},
        timeout=20,
    )
    if not res["ok"]:
        return None

    data = res["data"] or {}
    usd_price = safe_decimal(((data.get(coin_id) or {}).get("usd")))
    if usd_price is None or usd_price <= 0:
        return None

    set_cached_price(coin_id, usd_price)
    return usd_price


def quantize_coin_amount(amount: Decimal, network: str) -> Decimal:
    config = NETWORK_CONFIG[network]
    decimals = int(config["decimals"])
    quant = Decimal("1") / (Decimal("10") ** decimals)
    return amount.quantize(quant, rounding=ROUND_DOWN)


def calculate_crypto_amount(usd_amount: Decimal, network: str) -> Decimal:
    config = NETWORK_CONFIG[network]
    symbol = config["symbol"]

    if symbol == "USDT":
        return quantize_coin_amount(usd_amount, network)

    price_usd = get_usd_price_for_network(network)
    if price_usd is None or price_usd <= 0:
        raise ValueError(f"Could not fetch live {symbol} price right now.")

    raw_coin_amount = usd_amount / price_usd
    return quantize_coin_amount(raw_coin_amount, network)


def format_coin_amount(amount: Any, network: str) -> str:
    dec = safe_decimal(amount) or Decimal("0")
    config = NETWORK_CONFIG[network]
    decimals = int(config["decimals"])
    return f"{dec:.{decimals}f}".rstrip("0").rstrip(".") if "." in f"{dec:.{decimals}f}" else f"{dec:.{decimals}f}"


def get_request_display_amount(request_row: sqlite3.Row) -> str:
    return f"{format_coin_amount(request_row['coin_amount'], request_row['network'])} {request_row['coin_symbol']}"


# =========================================================
# MENU BUTTON TEXTS (STATE LOCK FIX)
# =========================================================

CLIENT_MENU_BUTTONS = {
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

ADMIN_MENU_BUTTONS = {
    "📦 Products",
    "📥 Stock",
    "🎟 Promo Admin",
    "📦 Orders Admin",
    "💳 Deposits Admin",
    "👤 Users Admin",
    "📊 Analytics",
    "🚪 Exit Admin",
}

INTERRUPTIBLE_STEPS = {
    "deposit_custom_amount",
    "buy_custom_qty",
    "awaiting_promo",
    "awaiting_crypto_txid_deposit",
    "awaiting_crypto_txid_buy",
    "awaiting_txid_for_request",
    "admin_add_product_icon",
    "admin_add_product_name",
    "admin_add_product_month",
    "admin_add_product_price",
    "admin_add_product_display_stock",
    "admin_add_product_details",
    "admin_edit_name_input",
    "admin_edit_price_input",
    "admin_edit_month_input",
    "admin_edit_details_input",
    "admin_edit_icon_input",
    "admin_edit_display_stock_input",
    "stock_add_single_input",
    "stock_add_bulk_input",
    "stock_edit_account_input",
    "stock_set_display_input",
    "promo_generate_custom_amount",
    "orders_user_search_input",
    "deposits_user_search_input",
    "users_search_input",
}


def should_cancel_previous_state_on_menu_click(step: str, text: str, mode: str) -> bool:
    if step not in INTERRUPTIBLE_STEPS:
        return False

    if mode == "admin" and text in ADMIN_MENU_BUTTONS:
        return True

    if text in CLIENT_MENU_BUTTONS:
        return True

    return False


# =========================================================
# KEYBOARDS
# =========================================================

def main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["🛍 Shop", "💰 Wallet"],
        ["💳 Top Up", "🎟 Promo"],
        ["📦 Orders", "🆔 User ID"],
        ["🧾 Transactions", "👥 Refer & Earn"],
        ["💬 Support"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def admin_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📦 Products", "📥 Stock"],
        ["🎟 Promo Admin", "📦 Orders Admin"],
        ["💳 Deposits Admin", "👤 Users Admin"],
        ["📊 Analytics", "🚪 Exit Admin"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def close_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Close", callback_data="close_inline")]
    ])


def deposit_amount_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
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
    ])


def payment_method_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏦 Binance ID", callback_data=f"{prefix}_method_binance"),
            InlineKeyboardButton("🏦 Bybit ID", callback_data=f"{prefix}_method_bybit"),
        ],
        [InlineKeyboardButton("💸 Crypto Address", callback_data=f"{prefix}_method_crypto")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"{prefix}_back")],
    ])


def network_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
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
    ])


def buy_qty_keyboard(product_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🛒 Buy 1x", callback_data=f"buy_qty_{product_id}_1"),
            InlineKeyboardButton("🛒 Buy 5x", callback_data=f"buy_qty_{product_id}_5"),
        ],
        [
            InlineKeyboardButton("🛒 Buy 10x", callback_data=f"buy_qty_{product_id}_10"),
            InlineKeyboardButton("✏️ Custom Qty", callback_data=f"buy_custom_{product_id}"),
        ],
        [InlineKeyboardButton("⬅️ Back to Shop", callback_data="back_shop_cards")],
    ])


def final_manual_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Submitted", callback_data=f"{prefix}_submitted")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"{prefix}_cancel")],
    ])


def copy_verify_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Copy Address", callback_data=f"copy_address_{request_id}")],
        [InlineKeyboardButton("✅ I Have Paid (Verify)", callback_data=f"verify_request_{request_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_request_{request_id}")],
    ])


# =========================================================
# TEXT RENDER HELPERS
# =========================================================

def render_home_text() -> str:
    return (
        "👑 <b>SupremeLeader Premium Shop</b>\n\n"
        "Welcome to your premium digital marketplace.\n"
        "<b>Please select an option below:</b>"
    )


def render_wallet_text(user_id: int) -> str:
    balance = get_user_wallet(user_id)
    return (
        "💰 <b>WALLET</b>\n\n"
        f"<b>Current Balance:</b> {format_money(balance)}"
    )


def render_user_id_text(user_id: int) -> str:
    return (
        "🆔 <b>YOUR USER ID</b>\n\n"
        f"<code>{user_id}</code>\n\n"
        "Send this User ID to admin when needed."
    )


def render_support_text() -> str:
    return f"💬 <b>SUPPORT</b>\n\nContact admin: {escape_html(SUPPORT_USERNAME)}"


def render_refer_text(user_id: int) -> str:
    ref_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    return (
        "👥 <b>REFER & EARN</b>\n\n"
        "Invite friends and get rewarded.\n\n"
        f"🔗 <b>Your Link:</b>\n{escape_html(ref_link)}\n\n"
        "📊 <b>Total Invited:</b> 0\n"
        "💵 <b>Rewards Earned:</b> $0"
    )


def render_deposit_text() -> str:
    return "💳 <b>CRYPTO DEPOSIT</b>\n\n<b>Please select an amount below:</b>"


def render_deposit_method_text(amount: Decimal) -> str:
    return (
        "💳 <b>SELECT PAYMENT METHOD</b>\n\n"
        f"<b>Amount to deposit:</b> {format_money(amount)}\n\n"
        "<b>Choose a payment method below:</b>"
    )


def render_manual_payment_text(amount: Decimal, method: str, details: str) -> str:
    return (
        "🏦 <b>EXCHANGE PAYMENT</b>\n\n"
        f"<b>Amount:</b> {format_money(amount)}\n"
        f"<b>Method:</b> {escape_html(method)}\n\n"
        f"<code>{escape_html(details)}</code>\n\n"
        "<b>Send payment screenshot to Live Support for confirmation.</b>"
    )


def render_product_card(product_row: sqlite3.Row) -> str:
    stock = get_product_display_stock(product_row)
    stock_text = f"{stock} pcs" if stock > 0 else "Stock Out"

    return (
        f"{escape_html(product_row['icon'])} <b>{escape_html(product_row['name'])}</b>\n"
        f"<b>Month:</b> {escape_html(product_row['month_label'])}\n"
        f"<b>Price:</b> {format_money(product_row['price_usd'])}\n"
        f"<b>Stock:</b> {stock_text}"
    )


def render_product_details(product_row: sqlite3.Row) -> str:
    display_stock = get_product_display_stock(product_row)
    real_stock = get_product_available_stock(product_row["product_id"])
    detail_lines = escape_html(product_row["details_text"]).replace("\n", "\n")

    return (
        "📦 <b>PRODUCT DETAILS</b>\n\n"
        f"<b>Icon:</b> {escape_html(product_row['icon'])}\n"
        f"<b>Name:</b> {escape_html(product_row['name'])}\n"
        f"<b>Month:</b> {escape_html(product_row['month_label'])}\n"
        f"<b>Price:</b> {format_money(product_row['price_usd'])}\n"
        f"<b>Stock:</b> {display_stock} pcs\n"
        f"<b>Real Stock:</b> {real_stock} pcs\n\n"
        f"{detail_lines}\n\n"
        "<b>Select quantity below:</b>"
    )


def render_buy_summary(product_row: sqlite3.Row, qty: int, wallet_balance: Decimal) -> str:
    total = safe_decimal(product_row["price_usd"]) * Decimal(qty)
    remaining = wallet_balance - total

    if wallet_balance >= total:
        return (
            "🛒 <b>ORDER SUMMARY</b>\n\n"
            f"<b>Product:</b> {escape_html(product_row['name'])}\n"
            f"<b>Unit Price:</b> {format_money(product_row['price_usd'])}\n"
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
        f"<b>Product:</b> {escape_html(product_row['name'])}\n"
        f"<b>Unit Price:</b> {format_money(product_row['price_usd'])}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total Price:</b> {format_money(total)}\n"
        f"<b>Wallet Balance:</b> {format_money(wallet_balance)}\n"
        f"<b>Shortage:</b> {format_money(shortage)}\n\n"
        "❌ <b>Wallet balance is not enough.</b>\n"
        "<b>Please select a payment method:</b>"
    )


def render_exact_crypto_payment_text(request_row: sqlite3.Row, title: str, subtitle_lines: List[str]) -> str:
    amount_line = get_request_display_amount(request_row)
    address = request_row["receive_address"]

    subtitle = "\n".join(subtitle_lines)

    return (
        f"✅ <b>{escape_html(title)}</b>\n\n"
        f"{subtitle}\n\n"
        f"🪙 <b>Amount to send:</b>\n"
        f"<code>{escape_html(amount_line)}</code>\n\n"
        f"🏦 <b>Deposit Address:</b>\n"
        f"<code>{escape_html(address)}</code>\n\n"
        "⚠️ <b>IMPORTANT:</b> Send <b>exactly</b> this amount.\n"
        "Do not round it.\n"
        "Your exchange withdrawal fee must be added separately.\n\n"
        "If you send a different amount, automatic verification may fail."
    )


def render_copy_address_text(request_row: sqlite3.Row) -> str:
    return (
        "📋 <b>COPY ADDRESS</b>\n\n"
        f"<code>{escape_html(request_row['receive_address'])}</code>"
    )

# ============
# Part 2B end here
# ============

# ============
# Part 3A Start From here
# ============

# =========================================================
# BLOCKCHAIN VERIFY FUNCTIONS (CORE FIX)
# =========================================================

# ---------- TRC20 (USDT TRON) ----------
def verify_trc20_usdt(txid: str, request_row: sqlite3.Row) -> dict:
    url = f"{TRONGRID_BASE}/v1/transactions/{txid}"
    res = http_get_json(url, headers=trongrid_headers())

    if not res["ok"]:
        return verify_result(False, "pending", "Tron API error")

    data = res["data"].get("data", [])
    if not data:
        return verify_result(False, "pending", "Transaction not found")

    tx = data[0]

    if tx.get("ret", [{}])[0].get("contractRet") != "SUCCESS":
        return verify_result(False, "rejected", "Transaction failed")

    # check transfer logs
    url2 = f"{TRONGRID_BASE}/v1/accounts/{TRX_RECEIVE_ADDRESS}/transactions/trc20"
    res2 = http_get_json(url2, headers=trongrid_headers())

    if not res2["ok"]:
        return verify_result(False, "pending", "TRC20 log fetch failed")

    for item in res2["data"].get("data", []):
        if item.get("transaction_id") != txid:
            continue

        if item.get("to") != request_row["receive_address"]:
            continue

        value = safe_decimal(item.get("value")) / Decimal(10**6)

        if amount_within_tolerance(
            value,
            request_row["coin_amount"],
            request_row["tolerance_amount"],
        ):
            return verify_result(True, "confirmed", "TRC20 matched", str(value))

    return verify_result(False, "pending", "No matching transfer found")


# ---------- TRX ----------
def verify_trx(txid: str, request_row: sqlite3.Row) -> dict:
    url = f"{TRONGRID_BASE}/v1/transactions/{txid}"
    res = http_get_json(url, headers=trongrid_headers())

    if not res["ok"]:
        return verify_result(False, "pending", "TRX API error")

    data = res["data"].get("data", [])
    if not data:
        return verify_result(False, "pending", "Transaction not found")

    tx = data[0]

    if tx.get("ret", [{}])[0].get("contractRet") != "SUCCESS":
        return verify_result(False, "rejected", "Transaction failed")

    contract = tx.get("raw_data", {}).get("contract", [{}])[0]
    value = contract.get("parameter", {}).get("value", {})

    to_addr = tron_hex_to_base58(value.get("to_address", ""))
    amount = safe_decimal(value.get("amount")) / Decimal(10**6)

    if to_addr != request_row["receive_address"]:
        return verify_result(False, "rejected", "Wrong address")

    if amount_within_tolerance(
        amount,
        request_row["coin_amount"],
        request_row["tolerance_amount"],
    ):
        return verify_result(True, "confirmed", "TRX matched", str(amount))

    return verify_result(False, "pending", "Amount mismatch")


# ---------- BTC ----------
def verify_btc(txid: str, request_row: sqlite3.Row) -> dict:
    url = f"{BTC_API_BASE}/tx/{txid}"
    res = http_get_json(url)

    if not res["ok"]:
        return verify_result(False, "pending", "BTC API error")

    tx = res["data"]

    outputs = tx.get("vout", [])
    for out in outputs:
        addr = out.get("scriptpubkey_address")
        value = safe_decimal(out.get("value")) / Decimal(10**8)

        if addr != request_row["receive_address"]:
            continue

        if amount_within_tolerance(
            value,
            request_row["coin_amount"],
            request_row["tolerance_amount"],
        ):
            return verify_result(True, "confirmed", "BTC matched", str(value))

    return verify_result(False, "pending", "No matching output")


# ---------- LTC ----------
def verify_ltc(txid: str, request_row: sqlite3.Row) -> dict:
    url = f"{LTC_API_BASE}/tx/{txid}"
    res = http_get_json(url)

    if not res["ok"]:
        return verify_result(False, "pending", "LTC API error")

    tx = res["data"]

    outputs = tx.get("vout", [])
    for out in outputs:
        addr = out.get("scriptpubkey_address")
        value = safe_decimal(out.get("value")) / Decimal(10**8)

        if addr != request_row["receive_address"]:
            continue

        if amount_within_tolerance(
            value,
            request_row["coin_amount"],
            request_row["tolerance_amount"],
        ):
            return verify_result(True, "confirmed", "LTC matched", str(value))

    return verify_result(False, "pending", "No matching output")

# ============
# Part 3A end here
# ============

# ============
# Part 3B Start From here
# ============

# ---------- EVM HELPERS ----------
def get_evm_tx_by_hash(chainid: str, txid: str) -> dict:
    return http_get_json(
        ETHERSCAN_V2_URL,
        params={
            "chainid": chainid,
            "module": "proxy",
            "action": "eth_getTransactionByHash",
            "txhash": txid,
            "apikey": ETHERSCAN_API_KEY,
        },
    )


def get_evm_tx_receipt(chainid: str, txid: str) -> dict:
    return http_get_json(
        ETHERSCAN_V2_URL,
        params={
            "chainid": chainid,
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": txid,
            "apikey": ETHERSCAN_API_KEY,
        },
    )


# ---------- ETH / BNB NATIVE ----------
def verify_evm_native(txid: str, request_row: sqlite3.Row, chainid: str, symbol: str) -> dict:
    tx_res = get_evm_tx_by_hash(chainid, txid)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"{symbol} tx lookup failed")

    tx_data = (tx_res["data"] or {}).get("result")
    if not tx_data:
        return verify_result(False, "pending", f"{symbol} transaction not found")

    receipt_res = get_evm_tx_receipt(chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{symbol} receipt lookup failed")

    receipt = (receipt_res["data"] or {}).get("result")
    if not receipt:
        return verify_result(False, "pending", f"{symbol} transaction not confirmed yet")

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", f"{symbol} transaction failed")

    actual_to = normalize_evm_address(tx_data.get("to"))
    expected_to = normalize_evm_address(request_row["receive_address"])

    if actual_to != expected_to:
        return verify_result(False, "rejected", "Wrong address")

    try:
        value_wei = int(str(tx_data.get("value", "0")), 16)
    except Exception:
        return verify_result(False, "rejected", "Invalid value")

    actual_amount = Decimal(value_wei) / Decimal("1000000000000000000")

    if amount_within_tolerance(
        actual_amount,
        request_row["coin_amount"],
        request_row["tolerance_amount"],
    ):
        return verify_result(True, "confirmed", f"{symbol} matched", str(actual_amount))

    return verify_result(False, "pending", "Amount mismatch")


# ---------- ERC20 / BEP20 USDT ----------
def verify_evm_token(txid: str, request_row: sqlite3.Row, chainid: str, token_contract: str, decimals: int, symbol: str) -> dict:
    receipt_res = get_evm_tx_receipt(chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{symbol} receipt lookup failed")

    receipt = (receipt_res["data"] or {}).get("result")
    if not receipt:
        return verify_result(False, "pending", f"{symbol} transaction not confirmed yet")

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", f"{symbol} transaction failed")

    expected_contract = normalize_evm_address(token_contract)
    expected_to_topic = to_evm_topic_address(request_row["receive_address"]).lower()
    divisor = Decimal(10) ** Decimal(decimals)

    for log in receipt.get("logs", []) or []:
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

        try:
            value_raw = int(str(log.get("data", "0x0")), 16)
        except Exception:
            continue

        actual_amount = Decimal(value_raw) / divisor

        if amount_within_tolerance(
            actual_amount,
            request_row["coin_amount"],
            request_row["tolerance_amount"],
        ):
            return verify_result(True, "confirmed", f"{symbol} matched", str(actual_amount))

    return verify_result(False, "pending", "No matching token transfer")


# ---------- SOL ----------
def helius_rpc(method: str, params: list) -> dict:
    if not HELIUS_RPC_URL:
        return {"ok": False, "status_code": 0, "data": {"error": "HELIUS_API_KEY missing"}}

    return http_post_json(
        HELIUS_RPC_URL,
        payload={
            "jsonrpc": "2.0",
            "id": "1",
            "method": method,
            "params": params,
        },
        headers={"content-type": "application/json"},
        timeout=25,
    )


def verify_sol(txid: str, request_row: sqlite3.Row) -> dict:
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
        return verify_result(False, "pending", "SOL RPC error")

    tx = (res["data"] or {}).get("result")
    if not tx:
        return verify_result(False, "pending", "SOL transaction not found")

    meta = tx.get("meta", {}) or {}
    if meta.get("err") is not None:
        return verify_result(False, "rejected", "SOL transaction failed")

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
        if destination != request_row["receive_address"] or lamports is None:
            continue

        actual_amount = Decimal(int(lamports)) / Decimal("1000000000")

        if amount_within_tolerance(
            actual_amount,
            request_row["coin_amount"],
            request_row["tolerance_amount"],
        ):
            return verify_result(True, "confirmed", "SOL matched", str(actual_amount))

    return verify_result(False, "pending", "No matching SOL transfer")


# =========================================================
# MAIN VERIFY ROUTER (MISSING FUNCTION FIXED)
# =========================================================

def verify_crypto_payment(txid: str, request_row: sqlite3.Row) -> dict:
    network = request_row["network"]

    if network == "USDT (TRC20)":
        return verify_trc20_usdt(txid, request_row)

    if network == "TRX (TRC20)":
        return verify_trx(txid, request_row)

    if network == "BTC":
        return verify_btc(txid, request_row)

    if network == "LTC":
        return verify_ltc(txid, request_row)

    if network == "ETH (ERC20)":
        return verify_evm_native(txid, request_row, ETH_CHAIN_ID, "ETH")

    if network == "BNB (BEP20)":
        return verify_evm_native(txid, request_row, BSC_CHAIN_ID, "BNB")

    if network == "USDT (ERC20)":
        return verify_evm_token(txid, request_row, ETH_CHAIN_ID, USDT_ERC20_CONTRACT, 6, "USDT ERC20")

    if network == "USDT (BEP20)":
        return verify_evm_token(txid, request_row, BSC_CHAIN_ID, USDT_BEP20_CONTRACT, 18, "USDT BEP20")

    if network == "SOL":
        return verify_sol(txid, request_row)

    return verify_result(False, "rejected", f"Unsupported network: {network}")


# =========================================================
# PAYMENT FINALIZATION / RECHECK HELPERS
# =========================================================

def get_pending_payment_requests() -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM payment_requests
        WHERE tx_status = 'checking'
        ORDER BY id ASC
    """)


def get_user_open_payment_request(user_id: int, request_type: str) -> Optional[sqlite3.Row]:
    return db_fetchone("""
        SELECT * FROM payment_requests
        WHERE user_id = ?
          AND request_type = ?
          AND tx_status IN ('awaiting_payment', 'awaiting_txid', 'checking')
        ORDER BY id DESC
        LIMIT 1
    """, (user_id, request_type))


async def finalize_deposit_payment(bot, request_row: sqlite3.Row, txid: str) -> None:
    user_id = int(request_row["user_id"])
    usd_amount = safe_decimal(request_row["usd_amount"]) or Decimal("0")

    update_payment_status(request_row["id"], "confirmed")
    mark_txid_used(txid, user_id, request_row["network"], request_row["id"])

    new_balance = add_user_wallet(user_id, usd_amount)

    tx_id = request_row["linked_transaction_id"]
    if tx_id:
        update_transaction_status(int(tx_id), "Completed")

    reset_user_state(user_id)

    await bot.send_message(
        chat_id=user_id,
        text=(
            "✅ <b>Deposit confirmed.</b>\n\n"
            f"<b>Amount Added:</b> {format_money(usd_amount)}\n"
            f"<b>New Wallet Balance:</b> {format_money(new_balance)}"
        ),
        parse_mode="HTML",
        reply_markup=main_menu(),
    )


async def finalize_order_payment(bot, request_row: sqlite3.Row, txid: str) -> None:
    user_id = int(request_row["user_id"])
    qty = int(request_row["qty"] or 0)
    product_id = request_row["product_id"]

    product_row = get_product_row(product_id)
    if not product_row:
        update_payment_status(request_row["id"], "rejected")
        await bot.send_message(
            chat_id=user_id,
            text="❌ <b>Product not found anymore.</b>",
            parse_mode="HTML",
            reply_markup=main_menu(),
        )
        return

    order_id = request_row["linked_order_id"]
    if not order_id:
        update_payment_status(request_row["id"], "rejected")
        await bot.send_message(
            chat_id=user_id,
            text="❌ <b>Linked order not found.</b>",
            parse_mode="HTML",
            reply_markup=main_menu(),
        )
        return

    available_stock = get_product_available_stock(product_id)
    if available_stock < qty:
        update_payment_status(request_row["id"], "rejected")
        update_order_status(int(order_id), "Rejected")
        await bot.send_message(
            chat_id=user_id,
            text="❌ <b>Not enough stock available right now.</b>\n\nPlease contact support.",
            parse_mode="HTML",
            reply_markup=main_menu(),
        )
        return

    delivered = deliver_accounts(product_id, qty, user_id, int(order_id))

    update_payment_status(request_row["id"], "confirmed")
    update_order_status(int(order_id), "Completed")
    mark_txid_used(txid, user_id, request_row["network"], request_row["id"])

    tx_id = request_row["linked_transaction_id"]
    if tx_id:
        update_transaction_status(int(tx_id), "Completed")

    lines = [
        "✅ <b>Order payment confirmed.</b>",
        "",
        f"<b>Product:</b> {escape_html(product_row['name'])}",
        f"<b>Quantity:</b> {qty}",
        "",
        "🔐 <b>Your Account Details:</b>",
        "",
    ]

    for idx, acc in enumerate(delivered, start=1):
        lines.append(f"{idx}. <b>Email/Username:</b> <code>{escape_html(acc['email'])}</code>")
        lines.append(f"   <b>Password:</b> <code>{escape_html(acc['password'])}</code>")
        if acc["note"]:
            lines.append(f"   <b>Note:</b> {escape_html(acc['note'])}")
        lines.append("")

    reset_user_state(user_id)

    await bot.send_message(
        chat_id=user_id,
        text="\n".join(lines),
        parse_mode="HTML",
        reply_markup=main_menu(),
    )


async def handle_payment_verification_result(bot, request_row: sqlite3.Row, txid: str, result: dict) -> None:
    status = result.get("status")

    if status == "confirmed":
        if request_row["request_type"] == "deposit":
            await finalize_deposit_payment(bot, request_row, txid)
        else:
            await finalize_order_payment(bot, request_row, txid)
        return

    if status == "rejected":
        update_payment_status(request_row["id"], "rejected")

        if request_row["linked_order_id"]:
            update_order_status(int(request_row["linked_order_id"]), "Rejected")

        if request_row["linked_transaction_id"]:
            update_transaction_status(int(request_row["linked_transaction_id"]), "Rejected")

        await bot.send_message(
            chat_id=int(request_row["user_id"]),
            text=f"❌ <b>Payment rejected.</b>\n\n{escape_html(result.get('reason', 'Unknown reason'))}",
            parse_mode="HTML",
            reply_markup=main_menu(),
        )
        return

    # pending
    attempts = int(request_row["verify_attempts"] or 0)
    if attempts >= MAX_RECHECK_ATTEMPTS:
        update_payment_status(request_row["id"], "awaiting_txid")
        reset_user_state(int(request_row["user_id"]))

        await bot.send_message(
            chat_id=int(request_row["user_id"]),
            text=(
                "⏳ <b>Payment still not confirmed.</b>\n\n"
                "Please send the same TXID again later or contact support."
            ),
            parse_mode="HTML",
            reply_markup=main_menu(),
        )


async def background_crypto_recheck(context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = get_pending_payment_requests()

    for row in rows:
        txid = (row["txid"] or "").strip()
        if not txid:
            continue

        increment_payment_attempt(int(row["id"]))

        fresh_row = get_payment_request(int(row["id"]))
        if not fresh_row:
            continue

        result = verify_crypto_payment(txid, fresh_row)
        await handle_payment_verification_result(context.bot, fresh_row, txid, result)

# ============
# Part 3B end here
# ============

# ============
# Part 4A Start From here
# ============

# =========================================================
# USER HISTORY / SUMMARY RENDER HELPERS
# =========================================================

def get_user_orders(user_id: int, limit: int = 25) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM orders
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (user_id, limit))


def get_user_transactions(user_id: int, limit: int = 25) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM transactions
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (user_id, limit))


def render_orders_text(user_id: int) -> str:
    orders = get_user_orders(user_id)
    if not orders:
        return "📦 <b>ORDERS</b>\n\nNo orders found."

    lines = ["📦 <b>ORDERS</b>\n"]
    for order in orders:
        lines.append(
            f"#{order['id']} <b>{escape_html(order['product_name'])}</b>\n"
            f"   Quantity: {order['qty']}\n"
            f"   Total: {format_money(order['total_usd'])}\n"
            f"   Payment: {escape_html(order['payment_type'])}\n"
            f"   Status: <b>{escape_html(order['status'])}</b>\n"
            f"   Date: {format_dt(order['created_at'])}\n"
        )
    return "\n".join(lines)


def render_transactions_text(user_id: int) -> str:
    txs = get_user_transactions(user_id)
    if not txs:
        return "🧾 <b>TRANSACTIONS</b>\n\nNo transaction history found."

    lines = ["🧾 <b>TRANSACTIONS</b>\n"]
    for tx in txs:
        lines.append(
            f"TX#{tx['id']} <b>{escape_html(tx['tx_type'])}</b>\n"
            f"   Amount: {format_money(tx['amount_usd'])}\n"
            f"   Status: <b>{escape_html(tx['status'])}</b>\n"
            f"   Date: {format_dt(tx['created_at'])}\n"
        )
    return "\n".join(lines)


def get_total_users_count() -> int:
    row = db_fetchone("SELECT COUNT(*) as total FROM users")
    return int(row["total"]) if row else 0


def get_total_wallet_balance() -> Decimal:
    rows = db_fetchall("SELECT wallet_balance FROM users")
    total = Decimal("0")
    for row in rows:
        total += safe_decimal(row["wallet_balance"]) or Decimal("0")
    return total


def render_users_admin() -> str:
    return (
        "👤 <b>USERS ADMIN</b>\n\n"
        f"<b>Total Users:</b> {get_total_users_count()}\n"
        f"<b>Total Wallet Balance:</b> {format_money(get_total_wallet_balance())}"
    )


def render_analytics() -> str:
    total_users = get_total_users_count()

    completed_orders_row = db_fetchone("SELECT COUNT(*) as total FROM orders WHERE status = 'Completed'")
    pending_orders_row = db_fetchone("SELECT COUNT(*) as total FROM orders WHERE status = 'Pending Manual Review'")
    completed_deposits_row = db_fetchone("SELECT COUNT(*) as total FROM transactions WHERE tx_type = 'Deposit' AND status = 'Completed'")
    pending_deposits_row = db_fetchone("SELECT COUNT(*) as total FROM transactions WHERE tx_type = 'Deposit' AND status = 'Pending Manual Review'")

    sales_rows = db_fetchall("SELECT total_usd FROM orders WHERE status = 'Completed'")
    deposit_rows = db_fetchall("SELECT amount_usd FROM transactions WHERE tx_type = 'Deposit' AND status = 'Completed'")

    total_sales = Decimal("0")
    for row in sales_rows:
        total_sales += safe_decimal(row["total_usd"]) or Decimal("0")

    total_deposit = Decimal("0")
    for row in deposit_rows:
        total_deposit += safe_decimal(row["amount_usd"]) or Decimal("0")

    return (
        "📊 <b>ANALYTICS</b>\n\n"
        f"<b>Users:</b> {total_users}\n"
        f"<b>Completed Orders:</b> {int(completed_orders_row['total']) if completed_orders_row else 0}\n"
        f"<b>Pending Orders:</b> {int(pending_orders_row['total']) if pending_orders_row else 0}\n"
        f"<b>Completed Deposits:</b> {int(completed_deposits_row['total']) if completed_deposits_row else 0}\n"
        f"<b>Pending Deposits:</b> {int(pending_deposits_row['total']) if pending_deposits_row else 0}\n"
        f"<b>Total Sales:</b> {format_money(total_sales)}\n"
        f"<b>Total Deposit Amount:</b> {format_money(total_deposit)}\n"
        f"<b>Total User Wallet Balance:</b> {format_money(get_total_wallet_balance())}"
    )


# =========================================================
# ADMIN DATA HELPERS
# =========================================================

def get_all_orders(limit: int = 50) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM orders
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))


def get_pending_manual_orders(limit: int = 50) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM orders
        WHERE status = 'Pending Manual Review'
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))


def get_completed_orders(limit: int = 50) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM orders
        WHERE status = 'Completed'
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))


def get_all_deposits(limit: int = 50) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM transactions
        WHERE tx_type = 'Deposit'
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))


def get_pending_manual_deposits(limit: int = 50) -> List[sqlite3.Row]:
    return db_fetchall("""
        SELECT * FROM transactions
        WHERE tx_type = 'Deposit' AND status = 'Pending Manual Review'
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))


def find_order_by_id(order_id: int) -> Optional[sqlite3.Row]:
    return db_fetchone("SELECT * FROM orders WHERE id = ?", (order_id,))


def find_transaction_by_id(tx_id: int) -> Optional[sqlite3.Row]:
    return db_fetchone("SELECT * FROM transactions WHERE id = ?", (tx_id,))


def render_order_list(mode: str) -> str:
    if mode == "all":
        rows = get_all_orders()
        title = "📦 <b>ALL ORDERS</b>"
    elif mode == "pending_manual":
        rows = get_pending_manual_orders()
        title = "⏳ <b>PENDING MANUAL ORDERS</b>"
    else:
        rows = get_completed_orders()
        title = "✅ <b>COMPLETED ORDERS</b>"

    if not rows:
        return f"{title}\n\nNo records found."

    lines = [title, ""]
    for order in rows:
        lines.append(
            f"<b>#{order['id']}</b>\n"
            f"User: {order['user_id']}\n"
            f"Product: {escape_html(order['product_name'])}\n"
            f"Qty: {order['qty']}\n"
            f"Total: {format_money(order['total_usd'])}\n"
            f"Payment: {escape_html(order['payment_type'])}\n"
            f"Status: <b>{escape_html(order['status'])}</b>\n"
            f"Date: {format_dt(order['created_at'])}\n"
        )
    return "\n".join(lines)


def render_all_deposits_text() -> str:
    rows = get_all_deposits()
    if not rows:
        return "💳 <b>ALL DEPOSITS</b>\n\nNo deposits found."

    lines = ["💳 <b>ALL DEPOSITS</b>", ""]
    for tx in rows:
        lines.append(
            f"<b>TX#{tx['id']}</b>\n"
            f"User: {tx['user_id']}\n"
            f"Amount: {format_money(tx['amount_usd'])}\n"
            f"Status: <b>{escape_html(tx['status'])}</b>\n"
            f"Date: {format_dt(tx['created_at'])}\n"
        )
    return "\n".join(lines)


def render_pending_manual_deposits() -> str:
    rows = get_pending_manual_deposits()
    if not rows:
        return "💳 <b>PENDING MANUAL DEPOSITS</b>\n\nNo pending deposits found."

    lines = ["💳 <b>PENDING MANUAL DEPOSITS</b>", ""]
    for tx in rows:
        lines.append(
            f"<b>TX#{tx['id']}</b>\n"
            f"User: {tx['user_id']}\n"
            f"Amount: {format_money(tx['amount_usd'])}\n"
            f"Status: <b>{escape_html(tx['status'])}</b>\n"
            f"Date: {format_dt(tx['created_at'])}\n"
        )
    return "\n".join(lines)


def get_user_search_summary_text(user_id: int) -> str:
    ensure_user(user_id)

    wallet_balance = get_user_wallet(user_id)
    orders = get_user_orders(user_id, 12)
    txs = get_user_transactions(user_id, 12)

    total_deposit = Decimal("0")
    total_spent = Decimal("0")
    completed_orders = 0
    pending_orders = 0
    completed_deposits = 0
    pending_deposits = 0

    for tx in db_fetchall("SELECT * FROM transactions WHERE user_id = ?", (user_id,)):
        amount = safe_decimal(tx["amount_usd"]) or Decimal("0")
        if tx["tx_type"] == "Deposit" and tx["status"] == "Completed":
            total_deposit += amount
            completed_deposits += 1
        elif tx["tx_type"] == "Deposit" and tx["status"] == "Pending Manual Review":
            pending_deposits += 1

    for order in db_fetchall("SELECT * FROM orders WHERE user_id = ?", (user_id,)):
        amount = safe_decimal(order["total_usd"]) or Decimal("0")
        if order["status"] == "Completed":
            total_spent += amount
            completed_orders += 1
        elif order["status"] == "Pending Manual Review":
            pending_orders += 1

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

    if not txs:
        lines.append("No transactions found.")
    else:
        for tx in txs:
            lines.append(
                f"\nTX#{tx['id']} | {escape_html(tx['tx_type'])}\n"
                f"Amount: {format_money(tx['amount_usd'])}\n"
                f"Status: {escape_html(tx['status'])}\n"
                f"Date: {format_dt(tx['created_at'])}"
            )

    lines.extend(["", "━━━━━━━━━━━━━━", "", "<b>Recent Orders:</b>"])

    if not orders:
        lines.append("No orders found.")
    else:
        for order in orders:
            lines.append(
                f"\nOrder#{order['id']} | {escape_html(order['product_name'])}\n"
                f"Qty: {order['qty']}\n"
                f"Total: {format_money(order['total_usd'])}\n"
                f"Payment: {escape_html(order['payment_type'])}\n"
                f"Status: {escape_html(order['status'])}\n"
                f"Date: {format_dt(order['created_at'])}"
            )

    return "\n".join(lines)


# =========================================================
# SIMPLE SEND HELPERS
# =========================================================

async def send_client_main_text(update: Update, text: str):
    await update.message.reply_text(
        text,
        reply_markup=main_menu(),
        parse_mode="HTML",
    )


async def send_admin_main_text(update: Update, text: str):
    await update.message.reply_text(
        text,
        reply_markup=admin_menu(),
        parse_mode="HTML",
    )


async def send_inline_from_text(update: Update, text: str, keyboard: InlineKeyboardMarkup):
    await update.message.reply_text(
        text,
        reply_markup=keyboard,
        parse_mode="HTML",
    )


async def send_inline_from_callback(query, text: str, keyboard: Optional[InlineKeyboardMarkup] = None):
    if keyboard is None:
        await query.message.reply_text(text, parse_mode="HTML")
    else:
        await query.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


# =========================================================
# SHOP / DELIVERY HELPERS
# =========================================================

async def send_shop_cards_message(source, from_callback: bool = False):
    rows = get_product_rows()

    for product_row in rows:
        stock = get_product_display_stock(product_row)

        if stock > 0:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🛒 Buy Now", callback_data=f"shop_buy_{product_row['product_id']}")]
            ])
        else:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔔 Notify Me", callback_data=f"shop_notify_{product_row['product_id']}")]
            ])

        if from_callback:
            await source.message.reply_text(
                render_product_card(product_row),
                reply_markup=keyboard,
                parse_mode="HTML",
            )
        else:
            await source.reply_text(
                render_product_card(product_row),
                reply_markup=keyboard,
                parse_mode="HTML",
            )

    if from_callback:
        await source.message.reply_text("Tap a product option above.", reply_markup=close_keyboard())
    else:
        await source.reply_text("Tap a product option above.", reply_markup=close_keyboard())


async def notify_waiters_for_product(context: ContextTypes.DEFAULT_TYPE, product_id: str):
    waiters = list(notify_waitlist.get(product_id, set()))
    if not waiters:
        return

    product_row = get_product_row(product_id)
    if not product_row:
        return

    text = (
        f"🔔 <b>{escape_html(product_row['name'])}</b> is back in stock!\n\n"
        f"<b>Month:</b> {escape_html(product_row['month_label'])}\n"
        f"<b>Price:</b> {format_money(product_row['price_usd'])}\n"
        f"<b>Available now:</b> {get_product_display_stock(product_row)} pcs"
    )

    for waiter_id in waiters:
        try:
            await context.bot.send_message(waiter_id, text, parse_mode="HTML")
        except Exception:
            pass

    notify_waitlist[product_id].clear()


async def process_wallet_purchase(update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int, product_row: sqlite3.Row, qty: int) -> bool:
    price = safe_decimal(product_row["price_usd"]) or Decimal("0")
    total = price * Decimal(qty)
    wallet = get_user_wallet(user_id)

    if wallet < total:
        return False

    available_stock = get_product_available_stock(product_row["product_id"])
    if available_stock < qty:
        target = update_or_query.message if hasattr(update_or_query, "message") else update_or_query
        await target.reply_text(
            "❌ <b>Not enough real stock available right now.</b>\n\nPlease contact support.",
            parse_mode="HTML",
        )
        return False

    order_id = create_order(user_id, product_row, qty, total, "Wallet")
    update_order_status(order_id, "Completed")

    delivered = deliver_accounts(product_row["product_id"], qty, user_id, order_id)
    add_user_wallet(user_id, -total)
    create_transaction(user_id, "Wallet Purchase", total, "Completed")

    lines = [
        "✅ <b>Order completed successfully.</b>",
        "",
        f"<b>{format_money(total)}</b> deducted from your wallet.",
        f"<b>New Wallet Balance:</b> {format_money(get_user_wallet(user_id))}",
        "",
        "🔐 <b>Your Account Details:</b>",
        "",
    ]

    for idx, acc in enumerate(delivered, start=1):
        lines.append(f"{idx}. <b>Email/Username:</b> <code>{escape_html(acc['email'])}</code>")
        lines.append(f"   <b>Password:</b> <code>{escape_html(acc['password'])}</code>")
        if acc["note"]:
            lines.append(f"   <b>Note:</b> {escape_html(acc['note'])}")
        lines.append("")

    target = update_or_query.message if hasattr(update_or_query, "message") else update_or_query
    await target.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=main_menu() if get_user_mode(user_id) == "client" else admin_menu(),
    )

    return True

# ============
# Part 4A end here
# ============

# ============
# Part 4B Start From here
# ============

# =========================================================
# COMMAND HANDLERS
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)
    enter_client_mode(user_id)
    await send_client_main_text(update, render_home_text())


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    if not is_admin(user_id):
        await update.message.reply_text(
            "❌ <b>You are not allowed to open admin panel.</b>",
            parse_mode="HTML",
        )
        return

    enter_admin_mode(user_id)
    await send_admin_main_text(
        update,
        "🛠 <b>ADMIN MODE ON</b>\n\nBottom menu now switched to admin menu."
    )


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Your Telegram ID: {update.effective_user.id}")


async def addstock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    if not is_admin(user_id):
        await update.message.reply_text("❌ You are not allowed to use this command.")
        return

    if len(context.args) != 2:
        await update.message.reply_text("Usage: /addstock p3 5")
        return

    product_id = context.args[0].strip()
    qty_text = context.args[1].strip()

    product_row = get_product_row(product_id)
    if not product_row:
        await update.message.reply_text("❌ Invalid product id. Example: p1, p2, p3")
        return

    try:
        qty = int(qty_text)
        if qty <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Quantity must be a positive number.")
        return

    now = now_iso()
    for i in range(qty):
        db_execute("""
            INSERT INTO product_accounts (
                product_id, email, password, note, is_delivered,
                delivered_to, delivered_order_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 0, NULL, NULL, ?, ?)
        """, (
            product_id,
            f"{product_id}_auto_{random.randint(100000, 999999)}@example.com",
            "Pass1234",
            "Added by admin",
            now,
            now,
        ))

    real_stock = get_product_available_stock(product_id)
    current_display = get_product_display_stock(product_row)
    new_display = max(current_display, real_stock)

    db_execute("""
        UPDATE products
        SET display_stock = ?, updated_at = ?
        WHERE product_id = ?
    """, (new_display, now_iso(), product_id))

    await update.message.reply_text(
        f"✅ Stock added.\n\n"
        f"Product: {product_row['name']}\n"
        f"Added: {qty}\n"
        f"Current Real Stock: {real_stock} pcs\n"
        f"Display Stock: {new_display} pcs"
    )

    await notify_waiters_for_product(context, product_id)


# =========================================================
# TEXT HANDLER
# =========================================================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)

    text = update.message.text.strip()
    mode = get_user_mode(user_id)
    state = get_user_state(user_id)
    step = state.get("step", "main")

    # -----------------------------------------------------
    # GLOBAL MENU OVERRIDE FIX
    # -----------------------------------------------------
    if should_cancel_previous_state_on_menu_click(step, text, mode):
        reset_user_state(user_id)
        state = get_user_state(user_id)
        step = state.get("step", "main")

    # -----------------------------------------------------
    # ADMIN MAIN MENU TEXTS
    # -----------------------------------------------------
    if mode == "admin":
        if text == "🚪 Exit Admin":
            enter_client_mode(user_id)
            await send_client_main_text(update, "✅ <b>Admin mode off.</b>\n\nBack to client menu.")
            return

        if text == "📦 Products":
            set_user_state(user_id, {"step": "admin_products"})
            await update.message.reply_text(
                "🛠 <b>PRODUCTS MANAGEMENT</b>\n\nUse the admin buttons below.",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

        if text == "📥 Stock":
            set_user_state(user_id, {"step": "admin_stock"})
            await update.message.reply_text(
                "📥 <b>STOCK MANAGEMENT</b>\n\nUse the admin buttons below.",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

        if text == "🎟 Promo Admin":
            set_user_state(user_id, {"step": "promo_admin"})
            await update.message.reply_text(
                "🎟 <b>PROMO ADMIN</b>\n\nUse the admin buttons below.",
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

        if text == "📦 Orders Admin":
            set_user_state(user_id, {"step": "orders_admin"})
            await update.message.reply_text(
                render_order_list("all"),
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

        if text == "💳 Deposits Admin":
            set_user_state(user_id, {"step": "deposits_admin"})
            await update.message.reply_text(
                render_all_deposits_text(),
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

        if text == "👤 Users Admin":
            set_user_state(user_id, {"step": "users_admin"})
            await update.message.reply_text(
                render_users_admin(),
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

        if text == "📊 Analytics":
            await update.message.reply_text(
                render_analytics(),
                reply_markup=admin_menu(),
                parse_mode="HTML",
            )
            return

    # -----------------------------------------------------
    # CLIENT INPUT STATES
    # -----------------------------------------------------
    if step == "buy_custom_qty":
        product_id = state.get("product_id")
        product_row = get_product_row(product_id) if product_id else None

        if not product_row:
            reset_user_state(user_id)
            await send_client_main_text(update, "❌ <b>Product not found.</b>")
            return

        try:
            qty = int(text)
            if qty <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ <b>Invalid quantity.</b> Please send a valid number.",
                parse_mode="HTML",
            )
            return

        display_stock = get_product_display_stock(product_row)
        if qty > display_stock:
            await update.message.reply_text(
                f"❌ <b>Only {display_stock} pcs available.</b>",
                parse_mode="HTML",
            )
            return

        wallet_balance = get_user_wallet(user_id)
        total = (safe_decimal(product_row["price_usd"]) or Decimal("0")) * Decimal(qty)

        if wallet_balance >= total:
            ok = await process_wallet_purchase(update, context, user_id, product_row, qty)
            if ok:
                reset_user_state(user_id)
            return

        set_user_state(user_id, {
            "step": "buy_payment_method",
            "product_id": product_id,
            "qty": qty,
            "total_usd": str(total),
        })
        await update.message.reply_text(
            render_buy_summary(product_row, qty, wallet_balance),
            reply_markup=payment_method_keyboard("buy"),
            parse_mode="HTML",
        )
        return

    if step == "deposit_custom_amount":
        try:
            amount = safe_decimal(text)
            if amount is None or amount <= 0:
                raise ValueError
        except Exception:
            await update.message.reply_text(
                "❌ <b>Invalid amount.</b> Please send a valid number.",
                parse_mode="HTML",
            )
            return

        set_user_state(user_id, {
            "step": "deposit_payment_method",
            "amount_usd": str(amount),
        })
        await update.message.reply_text(
            render_deposit_method_text(amount),
            reply_markup=payment_method_keyboard("dep"),
            parse_mode="HTML",
        )
        return

    if step == "awaiting_promo":
        promo_code = text.upper().strip()
        promo_row = get_promo(promo_code)

        if has_user_used_promo(user_id, promo_code):
            reset_user_state(user_id)
            await send_client_main_text(update, "❌ <b>This promo code has already been used.</b>")
            return

        if not promo_row:
            reset_user_state(user_id)
            await send_client_main_text(update, "❌ <b>Invalid promo code.</b>")
            return

        if int(promo_row["enabled"]) != 1:
            reset_user_state(user_id)
            await send_client_main_text(update, "❌ <b>This promo code is disabled.</b>")
            return

        amount = safe_decimal(promo_row["amount_usd"]) or Decimal("0")
        new_balance = add_user_wallet(user_id, amount)
        create_transaction(user_id, "Promo Bonus", amount, "Completed")
        mark_promo_used(user_id, promo_code)

        if int(promo_row["one_time"]) == 1:
            db_execute("DELETE FROM promos WHERE code = ?", (promo_code,))

        reset_user_state(user_id)
        await send_client_main_text(
            update,
            f"✅ <b>Promo applied successfully.</b>\n\n"
            f"{format_money(amount)} added to your wallet.\n"
            f"<b>New Wallet Balance:</b> {format_money(new_balance)}"
        )
        return

    if step == "awaiting_txid_for_request":
        txid = text.strip()

        if not is_valid_txid_format(txid):
            await update.message.reply_text(
                "❌ <b>Invalid TXID format.</b>\n\nPlease send a valid TXID.",
                parse_mode="HTML",
            )
            return

        if is_txid_used(txid):
            reset_user_state(user_id)
            await send_client_main_text(update, "❌ <b>This TXID has already been used.</b>")
            return

        request_id = state.get("request_id")
        request_row = get_payment_request(int(request_id)) if request_id else None

        if not request_row:
            reset_user_state(user_id)
            await send_client_main_text(update, "❌ <b>No pending payment request found.</b>")
            return

        set_payment_txid(int(request_row["id"]), txid)

        fresh_row = get_payment_request(int(request_row["id"]))
        result = verify_crypto_payment(txid, fresh_row)

        await update.message.reply_text(
            "⏳ <b>TXID received.</b>\n\nYour payment is being checked automatically.",
            parse_mode="HTML",
            reply_markup=main_menu(),
        )

        await handle_payment_verification_result(context.bot, fresh_row, txid, result)

        if result.get("status") != "pending":
            reset_user_state(user_id)

        return

    # -----------------------------------------------------
    # CLIENT MAIN MENUS
    # -----------------------------------------------------
    if text == "🛍 Shop":
        set_user_state(user_id, {"step": "shop"})
        await update.message.reply_text("🛍 <b>SHOP MENU</b>", parse_mode="HTML")
        await send_shop_cards_message(update.message, from_callback=False)
        return

    if text == "💰 Wallet":
        reset_user_state(user_id)
        await send_client_main_text(update, render_wallet_text(user_id))
        return

    if text == "🆔 User ID":
        reset_user_state(user_id)
        await send_client_main_text(update, render_user_id_text(user_id))
        return

    if text == "💳 Top Up":
        set_user_state(user_id, {"step": "deposit_amount"})
        await send_inline_from_text(update, render_deposit_text(), deposit_amount_keyboard())
        return

    if text == "📦 Orders":
        reset_user_state(user_id)
        await send_client_main_text(update, render_orders_text(user_id))
        return

    if text == "🎟 Promo":
        set_user_state(user_id, {"step": "awaiting_promo"})
        await send_client_main_text(update, "🎟 <b>PROMO</b>\n\nPlease send your promo code.")
        return

    if text == "👥 Refer & Earn":
        reset_user_state(user_id)
        await send_client_main_text(update, render_refer_text(user_id))
        return

    if text == "🧾 Transactions":
        reset_user_state(user_id)
        await send_client_main_text(update, render_transactions_text(user_id))
        return

    if text == "💬 Support":
        reset_user_state(user_id)
        await send_client_main_text(update, render_support_text())
        return

    # -----------------------------------------------------
    # FALLBACK
    # -----------------------------------------------------
    await update.message.reply_text(
        "Please use the fixed menu below.",
        reply_markup=admin_menu() if mode == "admin" else main_menu(),
    )

# ============
# Part 4B end here
# ============

# ============
# Part 5A Start From here
# ============

# =========================================================
# ADMIN INLINE KEYBOARDS
# =========================================================

def admin_products_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View Products", callback_data="admin_view_products")],
        [InlineKeyboardButton("⬅️ Close", callback_data="admin_products_close")],
    ])


def admin_stock_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View Stock", callback_data="stock_view")],
        [InlineKeyboardButton("⬅️ Close", callback_data="stock_close")],
    ])


def admin_promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View Promos", callback_data="promo_view")],
        [InlineKeyboardButton("⬅️ Close", callback_data="promo_close")],
    ])


def admin_orders_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View All Orders", callback_data="orders_view_all")],
        [InlineKeyboardButton("⏳ Pending Manual Orders", callback_data="orders_view_pending_manual")],
        [InlineKeyboardButton("✅ Completed Orders", callback_data="orders_view_completed")],
        [InlineKeyboardButton("⬅️ Close", callback_data="orders_close")],
    ])


def deposits_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Pending Manual Deposits", callback_data="deposits_pending_manual")],
        [InlineKeyboardButton("📋 All Deposits", callback_data="deposits_all")],
        [InlineKeyboardButton("⬅️ Close", callback_data="deposits_close")],
    ])


def users_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 View Summary", callback_data="users_summary")],
        [InlineKeyboardButton("⬅️ Close", callback_data="users_close")],
    ])


def pending_manual_orders_keyboard() -> InlineKeyboardMarkup:
    rows = []
    orders = get_pending_manual_orders(30)

    if not orders:
        rows.append([InlineKeyboardButton("No pending manual orders", callback_data="noop")])
    else:
        for order in orders:
            rows.append([
                InlineKeyboardButton(
                    f"#{order['id']} {order['product_name']} x{order['qty']} ({format_money(order['total_usd'])})",
                    callback_data=f"orders_pick_manual_{order['id']}",
                )
            ])

    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="orders_back")])
    return InlineKeyboardMarkup(rows)


def pending_manual_deposits_keyboard() -> InlineKeyboardMarkup:
    rows = []
    txs = get_pending_manual_deposits(30)

    if not txs:
        rows.append([InlineKeyboardButton("No pending deposits", callback_data="noop")])
    else:
        for tx in txs:
            rows.append([
                InlineKeyboardButton(
                    f"TX#{tx['id']} User {tx['user_id']} {format_money(tx['amount_usd'])}",
                    callback_data=f"deposits_pick_{tx['id']}",
                )
            ])

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


# =========================================================
# MANUAL PAYMENT FINALIZATION HELPERS
# =========================================================

async def confirm_manual_order(context: ContextTypes.DEFAULT_TYPE, order_id: int) -> Tuple[bool, str]:
    order = find_order_by_id(order_id)
    if not order:
        return False, "Order not found."

    if order["status"] != "Pending Manual Review":
        return False, "Order is no longer pending."

    product_row = get_product_row(order["product_id"])
    if not product_row:
        update_order_status(order_id, "Rejected")
        return False, "Product not found."

    qty = int(order["qty"])
    available_stock = get_product_available_stock(order["product_id"])
    if available_stock < qty:
        return False, "Not enough real stock to deliver."

    delivered = deliver_accounts(order["product_id"], qty, int(order["user_id"]), order_id)
    update_order_status(order_id, "Completed")

    tx_row = db_fetchone("""
        SELECT * FROM transactions
        WHERE user_id = ? AND tx_type = 'Order Payment' AND status = 'Pending Manual Review'
        ORDER BY id DESC
        LIMIT 1
    """, (order["user_id"],))
    if tx_row:
        update_transaction_status(int(tx_row["id"]), "Completed")

    lines = [
        "✅ <b>Your manual order payment has been confirmed.</b>",
        "",
        f"<b>Product:</b> {escape_html(order['product_name'])}",
        f"<b>Quantity:</b> {qty}",
        f"<b>Total:</b> {format_money(order['total_usd'])}",
        "",
        "🔐 <b>Your Account Details:</b>",
        "",
    ]

    for idx, acc in enumerate(delivered, start=1):
        lines.append(f"{idx}. <b>Email/Username:</b> <code>{escape_html(acc['email'])}</code>")
        lines.append(f"   <b>Password:</b> <code>{escape_html(acc['password'])}</code>")
        if acc["note"]:
            lines.append(f"   <b>Note:</b> {escape_html(acc['note'])}")
        lines.append("")

    await context.bot.send_message(
        chat_id=int(order["user_id"]),
        text="\n".join(lines),
        parse_mode="HTML",
        reply_markup=main_menu(),
    )
    return True, "Manual order confirmed."


async def reject_manual_order(context: ContextTypes.DEFAULT_TYPE, order_id: int) -> Tuple[bool, str]:
    order = find_order_by_id(order_id)
    if not order:
        return False, "Order not found."

    if order["status"] != "Pending Manual Review":
        return False, "Order is no longer pending."

    update_order_status(order_id, "Rejected")

    tx_row = db_fetchone("""
        SELECT * FROM transactions
        WHERE user_id = ? AND tx_type = 'Order Payment' AND status = 'Pending Manual Review'
        ORDER BY id DESC
        LIMIT 1
    """, (order["user_id"],))
    if tx_row:
        update_transaction_status(int(tx_row["id"]), "Rejected")

    await context.bot.send_message(
        chat_id=int(order["user_id"]),
        text=(
            "❌ <b>Your manual order payment was rejected.</b>\n\n"
            f"<b>Product:</b> {escape_html(order['product_name'])}\n"
            f"<b>Total:</b> {format_money(order['total_usd'])}\n\n"
            "Please contact support."
        ),
        parse_mode="HTML",
        reply_markup=main_menu(),
    )
    return True, "Manual order rejected."


async def confirm_manual_deposit(context: ContextTypes.DEFAULT_TYPE, tx_id: int) -> Tuple[bool, str]:
    tx = find_transaction_by_id(tx_id)
    if not tx:
        return False, "Deposit record not found."

    if tx["tx_type"] != "Deposit" or tx["status"] != "Pending Manual Review":
        return False, "Deposit is no longer pending."

    update_transaction_status(tx_id, "Completed")

    amount = safe_decimal(tx["amount_usd"]) or Decimal("0")
    new_balance = add_user_wallet(int(tx["user_id"]), amount)

    await context.bot.send_message(
        chat_id=int(tx["user_id"]),
        text=(
            "✅ <b>Your manual deposit has been confirmed.</b>\n\n"
            f"<b>Amount:</b> {format_money(amount)}\n"
            f"<b>New Wallet Balance:</b> {format_money(new_balance)}"
        ),
        parse_mode="HTML",
        reply_markup=main_menu(),
    )
    return True, "Manual deposit confirmed."


async def reject_manual_deposit(context: ContextTypes.DEFAULT_TYPE, tx_id: int) -> Tuple[bool, str]:
    tx = find_transaction_by_id(tx_id)
    if not tx:
        return False, "Deposit record not found."

    if tx["tx_type"] != "Deposit" or tx["status"] != "Pending Manual Review":
        return False, "Deposit is no longer pending."

    update_transaction_status(tx_id, "Rejected")

    await context.bot.send_message(
        chat_id=int(tx["user_id"]),
        text=(
            "❌ <b>Your manual deposit was rejected.</b>\n\n"
            f"<b>Amount:</b> {format_money(tx['amount_usd'])}\n\n"
            "Please contact support."
        ),
        parse_mode="HTML",
        reply_markup=main_menu(),
    )
    return True, "Manual deposit rejected."


# =========================================================
# CALLBACK HANDLER (PART 1)
# =========================================================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    ensure_user(user_id)

    await query.answer()
    data = query.data
    mode = get_user_mode(user_id)
    state = get_user_state(user_id)

    if data == "noop":
        return

    if data == "close_inline":
        await send_inline_from_callback(query, "Closed.", close_keyboard())
        return

    # -----------------------------------------------------
    # SIMPLE ADMIN PANELS
    # -----------------------------------------------------
    if data == "admin_products_close":
        await send_inline_from_callback(query, "Closed products panel.", close_keyboard())
        return

    if data == "stock_close":
        await send_inline_from_callback(query, "Closed stock panel.", close_keyboard())
        return

    if data == "promo_close":
        await send_inline_from_callback(query, "Closed promo panel.", close_keyboard())
        return

    if data == "orders_close":
        await send_inline_from_callback(query, "Closed orders panel.", close_keyboard())
        return

    if data == "deposits_close":
        await send_inline_from_callback(query, "Closed deposits panel.", close_keyboard())
        return

    if data == "users_close":
        await send_inline_from_callback(query, "Closed users panel.", close_keyboard())
        return

    if data == "admin_view_products":
        rows = get_product_rows()
        if not rows:
            await send_inline_from_callback(query, "No active products found.", admin_products_keyboard())
            return

        lines = ["📋 <b>PRODUCT LIST</b>", ""]
        for i, product in enumerate(rows, start=1):
            lines.append(
                f"<b>{i}.</b> {escape_html(product['icon'])} <b>{escape_html(product['name'])}</b> ({product['product_id']})\n"
                f"Month: {escape_html(product['month_label'])}\n"
                f"Price: {format_money(product['price_usd'])}\n"
                f"Display Stock: {get_product_display_stock(product)} pcs\n"
                f"Real Stock: {get_product_available_stock(product['product_id'])} pcs\n"
            )

        await send_inline_from_callback(query, "\n".join(lines), admin_products_keyboard())
        return

    if data == "stock_view":
        rows = get_product_rows()
        if not rows:
            await send_inline_from_callback(query, "No active products found.", admin_stock_keyboard())
            return

        lines = ["📦 <b>STOCK LIST</b>", ""]
        for i, product in enumerate(rows, start=1):
            lines.append(
                f"<b>{i}.</b> {escape_html(product['icon'])} <b>{escape_html(product['name'])}</b> ({product['product_id']})\n"
                f"Display Stock: {get_product_display_stock(product)} pcs\n"
                f"Real Stock: {get_product_available_stock(product['product_id'])} pcs\n"
            )

        await send_inline_from_callback(query, "\n".join(lines), admin_stock_keyboard())
        return

    if data == "promo_view":
        rows = db_fetchall("SELECT * FROM promos ORDER BY code ASC")
        if not rows:
            await send_inline_from_callback(query, "🎟 <b>PROMO LIST</b>\n\nNo promo codes found.", admin_promo_keyboard())
            return

        lines = ["🎟 <b>PROMO LIST</b>", ""]
        for promo in rows:
            lines.append(
                f"<b>{escape_html(promo['code'])}</b>\n"
                f"Amount: {format_money(promo['amount_usd'])}\n"
                f"Status: {'Enabled' if int(promo['enabled']) == 1 else 'Disabled'}\n"
                f"One Time: {'Yes' if int(promo['one_time']) == 1 else 'No'}\n"
                f"Created: {format_dt(promo['created_at'])}\n"
            )

        await send_inline_from_callback(query, "\n".join(lines), admin_promo_keyboard())
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

    if data == "deposits_all":
        await send_inline_from_callback(query, render_all_deposits_text(), deposits_admin_keyboard())
        return

    if data == "deposits_pending_manual":
        await send_inline_from_callback(query, render_pending_manual_deposits(), deposits_admin_keyboard())
        return

    if data == "users_summary":
        await send_inline_from_callback(query, render_users_admin(), users_admin_keyboard())
        return

    if data == "orders_back":
        await send_inline_from_callback(query, "📦 <b>ORDERS ADMIN</b>", admin_orders_keyboard())
        return

    if data == "deposits_back":
        await send_inline_from_callback(query, "💳 <b>DEPOSITS ADMIN</b>", deposits_admin_keyboard())
        return

    if data == "orders_manual_pick_menu":
        await send_inline_from_callback(
            query,
            "☑️ <b>Confirm / Reject Manual Orders</b>\n\nSelect pending order below.",
            pending_manual_orders_keyboard(),
        )
        return

    if data == "deposits_pick_menu":
        await send_inline_from_callback(
            query,
            "☑️ <b>Confirm / Reject Manual Deposits</b>\n\nSelect pending deposit below.",
            pending_manual_deposits_keyboard(),
        )
        return

    if data.startswith("orders_pick_manual_"):
        order_id = int(data.replace("orders_pick_manual_", ""))
        order = find_order_by_id(order_id)
        if not order:
            await send_inline_from_callback(query, "❌ Order not found.", admin_orders_keyboard())
            return

        await send_inline_from_callback(
            query,
            f"⏳ <b>MANUAL ORDER</b>\n\n"
            f"Order ID: #{order['id']}\n"
            f"User: {order['user_id']}\n"
            f"Product: {escape_html(order['product_name'])}\n"
            f"Qty: {order['qty']}\n"
            f"Total: {format_money(order['total_usd'])}\n"
            f"Status: {escape_html(order['status'])}",
            manual_order_action_keyboard(order_id),
        )
        return

    if data.startswith("deposits_pick_"):
        tx_id = int(data.replace("deposits_pick_", ""))
        tx = find_transaction_by_id(tx_id)
        if not tx:
            await send_inline_from_callback(query, "❌ Deposit not found.", deposits_admin_keyboard())
            return

        await send_inline_from_callback(
            query,
            f"💳 <b>MANUAL DEPOSIT</b>\n\n"
            f"TX ID: #{tx['id']}\n"
            f"User: {tx['user_id']}\n"
            f"Amount: {format_money(tx['amount_usd'])}\n"
            f"Status: {escape_html(tx['status'])}",
            manual_deposit_action_keyboard(tx_id),
        )
        return

# ============
# Part 5A end here
# ============

# ============
# Part 5B Start From here
# ============

    # -----------------------------------------------------
    # MANUAL CONFIRM / REJECT ACTIONS
    # -----------------------------------------------------
    if data.startswith("orders_confirm_"):
        order_id = int(data.replace("orders_confirm_", ""))
        ok, msg = await confirm_manual_order(context, order_id)
        await send_inline_from_callback(
            query,
            ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>",
            admin_orders_keyboard(),
        )
        return

    if data.startswith("orders_reject_"):
        order_id = int(data.replace("orders_reject_", ""))
        ok, msg = await reject_manual_order(context, order_id)
        await send_inline_from_callback(
            query,
            ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>",
            admin_orders_keyboard(),
        )
        return

    if data.startswith("deposits_confirm_"):
        tx_id = int(data.replace("deposits_confirm_", ""))
        ok, msg = await confirm_manual_deposit(context, tx_id)
        await send_inline_from_callback(
            query,
            ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>",
            deposits_admin_keyboard(),
        )
        return

    if data.startswith("deposits_reject_"):
        tx_id = int(data.replace("deposits_reject_", ""))
        ok, msg = await reject_manual_deposit(context, tx_id)
        await send_inline_from_callback(
            query,
            ("✅ " if ok else "❌ ") + f"<b>{escape_html(msg)}</b>",
            deposits_admin_keyboard(),
        )
        return

    # -----------------------------------------------------
    # CLIENT SHOP FLOW
    # -----------------------------------------------------
    if data == "back_shop_cards":
        set_user_state(user_id, {"step": "shop"})
        await send_inline_from_callback(query, "🛍 <b>SHOP MENU</b>")
        await send_shop_cards_message(query, from_callback=True)
        return

    if data.startswith("shop_notify_"):
        product_id = data.replace("shop_notify_", "")
        if product_id not in notify_waitlist:
            notify_waitlist[product_id] = set()
        notify_waitlist[product_id].add(user_id)

        product_row = get_product_row(product_id)
        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        await send_inline_from_callback(
            query,
            f"🔔 You will be notified when <b>{escape_html(product_row['name'])}</b> is back in stock.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Shop", callback_data="back_shop_cards")]
            ]),
        )
        return

    if data.startswith("shop_buy_"):
        product_id = data.replace("shop_buy_", "")
        product_row = get_product_row(product_id)

        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        if get_product_display_stock(product_row) <= 0:
            await send_inline_from_callback(query, "❌ <b>This product is currently out of stock.</b>", close_keyboard())
            return

        set_user_state(user_id, {"step": "buy_qty_select", "product_id": product_id})
        await send_inline_from_callback(
            query,
            render_product_details(product_row),
            buy_qty_keyboard(product_id),
        )
        return

    if data.startswith("buy_qty_"):
        _, _, product_id, qty_str = data.split("_")
        product_row = get_product_row(product_id)

        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        qty = int(qty_str)
        display_stock = get_product_display_stock(product_row)
        if qty > display_stock:
            await send_inline_from_callback(
                query,
                f"❌ <b>Only {display_stock} pcs available.</b>",
                buy_qty_keyboard(product_id),
            )
            return

        wallet_balance = get_user_wallet(user_id)
        total = (safe_decimal(product_row["price_usd"]) or Decimal("0")) * Decimal(qty)

        if wallet_balance >= total:
            ok = await process_wallet_purchase(query, context, user_id, product_row, qty)
            if ok:
                reset_user_state(user_id)
            return

        set_user_state(user_id, {
            "step": "buy_payment_method",
            "product_id": product_id,
            "qty": qty,
            "total_usd": str(total),
        })
        await send_inline_from_callback(
            query,
            render_buy_summary(product_row, qty, wallet_balance),
            payment_method_keyboard("buy"),
        )
        return

    if data.startswith("buy_custom_"):
        product_id = data.replace("buy_custom_", "")
        set_user_state(user_id, {"step": "buy_custom_qty", "product_id": product_id})
        await query.message.reply_text(
            "✏️ Send custom quantity as a number.\nExample: 2",
            parse_mode="HTML",
        )
        return

    # -----------------------------------------------------
    # BUY PAYMENT METHOD FLOW
    # -----------------------------------------------------
    if data == "buy_back":
        set_user_state(user_id, {"step": "shop"})
        await send_inline_from_callback(query, "🛍 <b>SHOP MENU</b>")
        await send_shop_cards_message(query, from_callback=True)
        return

    if data == "buy_method_binance":
        state = get_user_state(user_id)
        product_row = get_product_row(state.get("product_id"))
        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        total = safe_decimal(state.get("total_usd")) or Decimal("0")
        qty = int(state.get("qty", 0))

        order_id = create_order(user_id, product_row, qty, total, "Manual")
        update_order_status(order_id, "Pending Manual Review")
        create_transaction(user_id, "Order Payment", total, "Pending Manual Review")

        reset_user_state(user_id)
        await send_inline_from_callback(
            query,
            render_manual_payment_text(total, "Binance ID", BINANCE_ID),
            final_manual_keyboard("buymanual"),
        )
        return

    if data == "buy_method_bybit":
        state = get_user_state(user_id)
        product_row = get_product_row(state.get("product_id"))
        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        total = safe_decimal(state.get("total_usd")) or Decimal("0")
        qty = int(state.get("qty", 0))

        order_id = create_order(user_id, product_row, qty, total, "Manual")
        update_order_status(order_id, "Pending Manual Review")
        create_transaction(user_id, "Order Payment", total, "Pending Manual Review")

        reset_user_state(user_id)
        await send_inline_from_callback(
            query,
            render_manual_payment_text(total, "Bybit ID", BYBIT_ID),
            final_manual_keyboard("buymanual"),
        )
        return

    if data == "buy_method_crypto":
        state = get_user_state(user_id)
        state["step"] = "buy_network"
        set_user_state(user_id, state)
        await send_inline_from_callback(
            query,
            "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:",
            network_keyboard("buy"),
        )
        return

    if data == "buy_back_method":
        state = get_user_state(user_id)
        product_row = get_product_row(state.get("product_id"))
        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        qty = int(state.get("qty", 0))
        wallet_balance = get_user_wallet(user_id)
        await send_inline_from_callback(
            query,
            render_buy_summary(product_row, qty, wallet_balance),
            payment_method_keyboard("buy"),
        )
        return

    if data.startswith("buy_net_"):
        network_label = data.replace("buy_net_", "").replace("_", " ")
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
        network = network_map.get(network_label)

        if not network:
            await send_inline_from_callback(query, "❌ Unsupported network.", close_keyboard())
            return

        state = get_user_state(user_id)
        product_row = get_product_row(state.get("product_id"))
        if not product_row:
            await send_inline_from_callback(query, "❌ Product not found.", close_keyboard())
            return

        qty = int(state.get("qty", 0))
        total = safe_decimal(state.get("total_usd")) or Decimal("0")

        order_id = create_order(user_id, product_row, qty, total, "Crypto")
        update_order_status(order_id, "Awaiting Crypto Payment")
        tx_id = create_transaction(user_id, "Order Payment", total, "Awaiting TXID")

        request_id = create_payment_request(
            user_id=user_id,
            request_type="order",
            usd_amount=total,
            network=network,
            product_id=product_row["product_id"],
            qty=qty,
        )

        db_execute("""
            UPDATE payment_requests
            SET linked_order_id = ?, linked_transaction_id = ?, updated_at = ?
            WHERE id = ?
        """, (order_id, tx_id, now_iso(), request_id))

        request_row = get_payment_request(request_id)
        set_user_state(user_id, {
            "step": "awaiting_txid_for_request",
            "request_id": request_id,
        })

        await send_inline_from_callback(
            query,
            render_exact_crypto_payment_text(
                request_row,
                "ORDER PAYMENT REQUEST GENERATED",
                [
                    f"<b>Product:</b> {escape_html(product_row['name'])}",
                    f"<b>Quantity:</b> {qty}",
                    f"<b>Total:</b> {format_money(total)}",
                    f"<b>Network:</b> {escape_html(network)}",
                ],
            ),
            copy_verify_keyboard(request_id),
        )
        return

    if data == "buymanual_submitted":
        await send_inline_from_callback(
            query,
            "✅ <b>Submitted.</b>\n\nSend payment screenshot to Live Support for confirmation.",
            close_keyboard(),
        )
        return

    if data == "buymanual_cancel":
        await send_inline_from_callback(query, "❌ <b>Order cancelled.</b>", close_keyboard())
        return

    # -----------------------------------------------------
    # DEPOSIT FLOW
    # -----------------------------------------------------
    if data.startswith("dep_amt_"):
        amount = safe_decimal(data.replace("dep_amt_", "")) or Decimal("0")
        set_user_state(user_id, {
            "step": "deposit_payment_method",
            "amount_usd": str(amount),
        })
        await send_inline_from_callback(
            query,
            render_deposit_method_text(amount),
            payment_method_keyboard("dep"),
        )
        return

    if data == "dep_custom":
        set_user_state(user_id, {"step": "deposit_custom_amount"})
        await query.message.reply_text(
            "✏️ Send custom deposit amount.\nExample: 25",
            parse_mode="HTML",
        )
        return

    if data == "dep_back":
        await send_inline_from_callback(query, render_deposit_text(), deposit_amount_keyboard())
        return

# ============
# Part 5B end here
# ============

# ============
# Part 6A Start From here
# ============

    # -----------------------------------------------------
    # DEPOSIT PAYMENT METHOD FLOW
    # -----------------------------------------------------
    if data == "dep_method_binance":
        state = get_user_state(user_id)
        amount = safe_decimal(state.get("amount_usd")) or Decimal("0")

        create_transaction(user_id, "Deposit", amount, "Pending Manual Review")
        reset_user_state(user_id)

        await send_inline_from_callback(
            query,
            render_manual_payment_text(amount, "Binance ID", BINANCE_ID),
            final_manual_keyboard("depmanual"),
        )
        return

    if data == "dep_method_bybit":
        state = get_user_state(user_id)
        amount = safe_decimal(state.get("amount_usd")) or Decimal("0")

        create_transaction(user_id, "Deposit", amount, "Pending Manual Review")
        reset_user_state(user_id)

        await send_inline_from_callback(
            query,
            render_manual_payment_text(amount, "Bybit ID", BYBIT_ID),
            final_manual_keyboard("depmanual"),
        )
        return

    if data == "dep_method_crypto":
        state = get_user_state(user_id)
        state["step"] = "deposit_network"
        set_user_state(user_id, state)

        await send_inline_from_callback(
            query,
            "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:",
            network_keyboard("dep"),
        )
        return

    if data == "dep_back_method":
        state = get_user_state(user_id)
        amount = safe_decimal(state.get("amount_usd")) or Decimal("0")

        await send_inline_from_callback(
            query,
            render_deposit_method_text(amount),
            payment_method_keyboard("dep"),
        )
        return

    if data.startswith("dep_net_"):
        network_label = data.replace("dep_net_", "").replace("_", " ")
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
        network = network_map.get(network_label)

        if not network:
            await send_inline_from_callback(query, "❌ Unsupported network.", close_keyboard())
            return

        state = get_user_state(user_id)
        amount = safe_decimal(state.get("amount_usd")) or Decimal("0")

        tx_id = create_transaction(user_id, "Deposit", amount, "Awaiting TXID")

        request_id = create_payment_request(
            user_id=user_id,
            request_type="deposit",
            usd_amount=amount,
            network=network,
        )

        db_execute("""
            UPDATE payment_requests
            SET linked_transaction_id = ?, updated_at = ?
            WHERE id = ?
        """, (tx_id, now_iso(), request_id))

        request_row = get_payment_request(request_id)

        set_user_state(user_id, {
            "step": "awaiting_txid_for_request",
            "request_id": request_id,
        })

        await send_inline_from_callback(
            query,
            render_exact_crypto_payment_text(
                request_row,
                "DEPOSIT PAYMENT REQUEST GENERATED",
                [
                    f"<b>USD Value:</b> {format_money(amount)}",
                    f"<b>Network:</b> {escape_html(network)}",
                ],
            ),
            copy_verify_keyboard(request_id),
        )
        return

    if data == "depmanual_submitted":
        await send_inline_from_callback(
            query,
            "✅ <b>Submitted.</b>\n\nSend payment screenshot to Live Support for confirmation.",
            close_keyboard(),
        )
        return

    if data == "depmanual_cancel":
        await send_inline_from_callback(query, "❌ <b>Deposit cancelled.</b>", close_keyboard())
        return

    # -----------------------------------------------------
    # COPY ADDRESS / VERIFY / CANCEL REQUEST FLOW
    # -----------------------------------------------------
    if data.startswith("copy_address_"):
        request_id = int(data.replace("copy_address_", ""))
        request_row = get_payment_request(request_id)

        if not request_row or int(request_row["user_id"]) != user_id:
            await send_inline_from_callback(query, "❌ Payment request not found.", close_keyboard())
            return

        await send_inline_from_callback(
            query,
            render_copy_address_text(request_row),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ I Have Paid (Verify)", callback_data=f"verify_request_{request_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_request_{request_id}")],
            ]),
        )
        return

    if data.startswith("verify_request_"):
        request_id = int(data.replace("verify_request_", ""))
        request_row = get_payment_request(request_id)

        if not request_row or int(request_row["user_id"]) != user_id:
            await send_inline_from_callback(query, "❌ Payment request not found.", close_keyboard())
            return

        update_payment_status(request_id, "awaiting_txid")
        set_user_state(user_id, {
            "step": "awaiting_txid_for_request",
            "request_id": request_id,
        })

        await send_inline_from_callback(
            query,
            "🧾 <b>Send your TXID now.</b>\n\n"
            "After you send the TXID, the bot will automatically verify your payment.",
            close_keyboard(),
        )
        return

    if data.startswith("cancel_request_"):
        request_id = int(data.replace("cancel_request_", ""))
        request_row = get_payment_request(request_id)

        if request_row and int(request_row["user_id"]) == user_id:
            update_payment_status(request_id, "cancelled")

            if request_row["linked_order_id"]:
                update_order_status(int(request_row["linked_order_id"]), "Cancelled")

            if request_row["linked_transaction_id"]:
                update_transaction_status(int(request_row["linked_transaction_id"]), "Cancelled")

        reset_user_state(user_id)
        await send_inline_from_callback(
            query,
            "❌ <b>Payment request cancelled.</b>",
            close_keyboard(),
        )
        return

    # -----------------------------------------------------
    # FALLBACK CALLBACK
    # -----------------------------------------------------
    await send_inline_from_callback(query, "Unknown action.", close_keyboard())


# =========================================================
# BACKGROUND JOB WRAPPER
# =========================================================

async def background_job(context: ContextTypes.DEFAULT_TYPE):
    await background_crypto_recheck(context)

# ============
# Part 6A end here
# ============

# ============
# Part 6B Start From here
# ============

# =========================================================
# APP STARTUP
# =========================================================

def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("addstock", addstock))

    # callbacks first
    app.add_handler(CallbackQueryHandler(handle_callback))

    # text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # background job
    if app.job_queue:
        app.job_queue.run_repeating(
            background_job,
            interval=RECHECK_INTERVAL_SECONDS,
            first=RECHECK_INTERVAL_SECONDS,
        )

    print("✅ Bot started...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

# ============
# Part 6B end here
# ============
