import os
import hashlib
import random
import string
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime

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
import payment as paymod
import crypto_verify as cv
import wallet_checker as wc


# =========================
# BASIC SETTINGS
# =========================

# 🔐 TOKEN (GitHub-এ দিবা না)
BOT_TOKEN = os.getenv("BOT_TOKEN")

BOT_USERNAME = "SupremeLeaderShopBot"   # @ ছাড়া
SUPPORT_USERNAME = "@serpstacking"

ADMIN_IDS = {6795246172}

BINANCE_ID = "828543482"
BYBIT_ID = "199582741"

# 🔐 API KEYS (empty রাখো এখন)
TRONGRID_API_KEY = ""
ETHERSCAN_API_KEY = ""
HELIUS_API_KEY = ""

# =========================
# CHECK TOKEN (optional but good)
# =========================
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set. Please set it in Railway.")

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


def build_unique_crypto_amount(usd_amount: float, network: str, user_id: int):
    rates = fetch_live_rates_usd()
    rate = rates.get(network, Decimal("1"))
    if rate <= 0:
        rate = Decimal("1")
    base_amount = Decimal(str(usd_amount)) / rate
    buffered = paymod.calculate_buffered_amount(base_amount, network)
    decimals = paymod.NETWORK_DECIMALS.get(network, 8)

    if decimals >= 6:
        suffix_step = Decimal("1") / (Decimal(10) ** decimals)
        suffix = suffix_step * Decimal((user_id % 97) + 1)
        return buffered + suffix
    if decimals == 2:
        suffix_step = Decimal("0.01")
        suffix = suffix_step * Decimal((user_id % 3))
        return buffered + suffix
    return buffered

RECHECK_INTERVAL_SECONDS = 20
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
        "name": "Netflix Premium Account",
        "icon": "🎬",
        "month": "1",
        "price": 5.0,
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
        "display_stock": 25,
    },
    "p2": {
        "name": "Spotify Premium Account",
        "icon": "🎵",
        "month": "1",
        "price": 3.0,
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
        "display_stock": 18,
    },
    "p3": {
        "name": "YouTube Premium Account",
        "icon": "▶️",
        "month": "1",
        "price": 4.0,
        "details": [
            "✅ Private Account",
            "✅ Auto Delivery",
            "✅ Email:Password Delivery",
        ],
        "accounts": [],
        "display_stock": 0,
    },
}
product_order = ["p1", "p2", "p3"]

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
app_instance = None

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
def ensure_user(user_id: int):
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


def format_money(value: float) -> str:
    return f"${float(value):.2f}"


def get_product_stock(product_id: str) -> int:
    return len(PRODUCTS[product_id]["accounts"])


def get_display_stock(product_id: str) -> int:
    return int(PRODUCTS[product_id].get("display_stock", len(PRODUCTS[product_id]["accounts"])))


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


def reset_admin_temp(user_id: int):
    admin_temp[user_id] = {}


def generate_new_product_id() -> str:
    global next_product_number
    while True:
        product_id = f"p{next_product_number}"
        next_product_number += 1
        if product_id not in PRODUCTS:
            return product_id


def enter_client_mode(user_id: int):
    user_mode[user_id] = "client"
    user_state[user_id] = {"step": "main"}
    reset_admin_temp(user_id)


def enter_admin_mode(user_id: int):
    user_mode[user_id] = "admin"
    user_state[user_id] = {"step": "admin_main"}
    reset_admin_temp(user_id)


def parse_account_line(line: str):
    parts = [x.strip() for x in line.split("|")]
    if len(parts) < 2:
        return None
    email = parts[0]
    password = parts[1]
    note = parts[2] if len(parts) >= 3 else ""
    if not email or not password:
        return None
    return {"email": email, "password": password, "note": note}


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


def add_order_record(user_id: int, product_id: str, qty: int, total: float, status: str, payment_type: str):
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
# MENUS
# =========================
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


def deposit_amount_keyboard() -> InlineKeyboardMarkup:
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


def payment_method_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🏦 Binance ID", callback_data=f"{prefix}_method_binance"),
            InlineKeyboardButton("🏦 Bybit ID", callback_data=f"{prefix}_method_bybit"),
        ],
        [InlineKeyboardButton("💸 Crypto Address", callback_data=f"{prefix}_method_crypto")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"{prefix}_back")],
    ]
    return InlineKeyboardMarkup(rows)


def network_keyboard(prefix: str) -> InlineKeyboardMarkup:
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


def buy_qty_keyboard(product_id: str) -> InlineKeyboardMarkup:
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
        [InlineKeyboardButton("😀 Edit Icon", callback_data="admin_edit_icon_menu")],
        [InlineKeyboardButton("📦 Edit Display Stock", callback_data="admin_edit_display_stock_menu")],
        [InlineKeyboardButton("↕️ Reorder Products", callback_data="admin_reorder_menu")],
        [InlineKeyboardButton("🗑 Delete Product", callback_data="admin_delete_product_menu")],
        [InlineKeyboardButton("⬅️ Close", callback_data="admin_products_close")],
    ]
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
        [InlineKeyboardButton("🔎 Search User ID", callback_data="users_search")],
        [InlineKeyboardButton("⬅️ Close", callback_data="users_close")],
    ]
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
            InlineKeyboardButton(
                f"{product.get('icon', '📦')} {product['name']} ({product_id})",
                callback_data=f"{action_prefix}_{product_id}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="admin_products_back")])
    return InlineKeyboardMarkup(rows)


def stock_product_select_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for product_id in product_order:
        product = PRODUCTS[product_id]
        rows.append([
            InlineKeyboardButton(
                f"{product.get('icon', '📦')} {product['name']} ({product_id})",
                callback_data=f"{prefix}_{product_id}",
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


def admin_reorder_selected_keyboard(product_id: str) -> InlineKeyboardMarkup:
    idx = product_order.index(product_id)
    rows = []
    if idx > 0:
        rows.append([InlineKeyboardButton("⬆️ Move Up", callback_data=f"admin_move_up_{product_id}")])
    if idx < len(product_order) - 1:
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
        label = f"#{idx + 1} {accounts[idx]['email']}"
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


async def send_admin_main_text(update: Update, text: str):
    await update.message.reply_text(text, reply_markup=admin_menu(), parse_mode="HTML")


async def send_inline_from_text(update: Update, text: str, keyboard: InlineKeyboardMarkup):
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def send_inline_from_callback(query, text: str, keyboard=None):
    if keyboard is None:
        await query.message.reply_text(text, parse_mode="HTML")
    else:
        await query.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


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
def render_home_text() -> str:
    return (
        "👑 <b>SupremeLeader Premium Shop</b>\n\n"
        "Welcome to your premium digital marketplace.\n"
        "<b>Please select an option below:</b>"
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
    return f"💬 <b>SUPPORT</b>\n\nContact admin: {SUPPORT_USERNAME}"


def render_product_card(product_id: str) -> str:
    product = PRODUCTS[product_id]
    stock = get_display_stock(product_id)
    stock_text = f"{stock} pcs" if stock > 0 else "Stock Out"
    icon = product.get("icon", "📦")
    return (
        f"{icon} <b>{product['name']}</b>\n"
        f"<b>Month:</b> {product['month']}\n"
        f"<b>Price:</b> {format_money(product['price'])}\n"
        f"<b>Stock:</b> {stock_text}"
    )


def render_product_details(product_id: str) -> str:
    product = PRODUCTS[product_id]
    detail_lines = "\n".join(product["details"])
    stock = get_display_stock(product_id)
    real_stock = get_product_stock(product_id)
    icon = product.get("icon", "📦")
    return (
        "📦 <b>PRODUCT DETAILS</b>\n\n"
        f"<b>Icon:</b> {icon}\n"
        f"<b>Name:</b> {product['name']}\n"
        f"<b>Month:</b> {product['month']}\n"
        f"<b>Price:</b> {format_money(product['price'])}\n"
        f"<b>Stock:</b> {stock} pcs\n"
        f"<b>Real Stock:</b> {real_stock} pcs\n\n"
        f"{detail_lines}\n\n"
        "<b>Select quantity below:</b>"
    )


def render_buy_summary(product_id: str, qty: int, wallet_balance: float) -> str:
    product = PRODUCTS[product_id]
    total = product["price"] * qty
    remaining = wallet_balance - total
    if wallet_balance >= total:
        return (
            "🛒 <b>ORDER SUMMARY</b>\n\n"
            f"<b>Product:</b> {product['name']}\n"
            f"<b>Unit Price:</b> {format_money(product['price'])}\n"
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
        f"<b>Unit Price:</b> {format_money(product['price'])}\n"
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
            f"\n<b>{idx}.</b> {product.get('icon', '📦')} <b>{product['name']}</b> ({product_id})\n"
            f"Month: {product['month']}\n"
            f"Price: {format_money(product['price'])}\n"
            f"Display Stock: {get_display_stock(product_id)} pcs\n"
            f"Real Stock: {get_product_stock(product_id)} pcs"
        )
    return "\n".join(lines)


def render_admin_add_product_preview(user_id: int) -> str:
    temp = admin_temp[user_id]
    details_text = "\n".join(temp.get("details", []))
    return (
        "🆕 <b>CONFIRM NEW PRODUCT</b>\n\n"
        f"<b>Icon:</b> {temp.get('icon', '📦')}\n"
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


def render_admin_edit_details_preview(product_id: str, new_details: list) -> str:
    product = PRODUCTS[product_id]
    return (
        "📝 <b>CONFIRM DETAILS UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n\n"
        f"<b>New Details:</b>\n" + "\n".join(new_details) + "\n\nConfirm update?"
    )


def render_admin_edit_icon_preview(product_id: str, new_icon: str) -> str:
    product = PRODUCTS[product_id]
    return (
        "😀 <b>CONFIRM ICON UPDATE</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Old Icon:</b> {product.get('icon', '📦')}\n"
        f"<b>New Icon:</b> {new_icon}\n\n"
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
        f"<b>Product:</b> {product.get('icon', '📦')} {product['name']}\n"
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
            f"\n<b>{idx}.</b> {product.get('icon', '📦')} <b>{product['name']}</b> ({product_id})\n"
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
        lines.append(
            f"\n<b>#{i + 1}</b>\n"
            f"Email: <code>{escape_html(acc['email'])}</code>\n"
            f"Password: <code>{escape_html(acc['password'])}</code>\n"
            f"Note: {escape_html(acc.get('note', ''))}"
        )
    return "\n".join(lines)


def render_selected_account(product_id: str, index: int) -> str:
    product = PRODUCTS[product_id]
    acc = product["accounts"][index]
    return (
        f"🔐 <b>ACCOUNT DETAILS</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Serial:</b> #{index + 1}\n\n"
        f"<b>Email:</b> <code>{escape_html(acc['email'])}</code>\n"
        f"<b>Password:</b> <code>{escape_html(acc['password'])}</code>\n"
        f"<b>Note:</b> {escape_html(acc.get('note', ''))}"
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
        f"<b>Total Transactions:</b> {len(all_transactions)}"
    )


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
        f"🔔 <b>{product['name']}</b> is back in stock!\n\n"
        f"<b>Month:</b> {product['month']}\n"
        f"<b>Price:</b> {format_money(product['price'])}\n"
        f"<b>Available now:</b> {get_display_stock(product_id)} pcs"
    )
    for waiter_id in waiters:
        try:
            await context.bot.send_message(waiter_id, text, parse_mode="HTML")
        except Exception:
            pass
    notify_waitlist[product_id].clear()


async def send_shop_cards_message(source, from_callback: bool = False):
    for product_id in product_order:
        if product_id not in PRODUCTS:
            continue
        stock = get_display_stock(product_id)
        if stock > 0:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Buy Now", callback_data=f"shop_buy_{product_id}")]])
        else:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔔 Notify Me", callback_data=f"shop_notify_{product_id}")]])

        if from_callback:
            await source.message.reply_text(render_product_card(product_id), reply_markup=keyboard, parse_mode="HTML")
        else:
            await source.reply_text(render_product_card(product_id), reply_markup=keyboard, parse_mode="HTML")

    if from_callback:
        await source.message.reply_text("Tap a product option above.", reply_markup=close_keyboard())
    else:
        await source.reply_text("Tap a product option above.", reply_markup=close_keyboard())


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
        f"✅ <b>Order Completed:</b> {product['name']}",
        f"<b>Quantity:</b> {qty}",
        "",
        "🔐 <b>Your Account Details:</b>",
        "",
    ]
    for idx, acc in enumerate(delivered, start=1):
        lines.append(f"{idx}. <b>Email/Username:</b> {escape_html(acc['email'])}")
        lines.append(f"   <b>Password:</b> {escape_html(acc['password'])}")
        if acc.get("note"):
            lines.append(f"   <b>Note:</b> {escape_html(acc['note'])}")
        lines.append("")

    await bot.send_message(chat_id=user_id, text="\n".join(lines), parse_mode="HTML")
    return True, delivered


async def process_wallet_purchase(update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int, product_id: str, qty: int, total: float):
    if user_wallet[user_id] < total:
        return False

    ok, _ = await deliver_accounts_to_user(context.bot, user_id, product_id, qty)
    if not ok:
        return False

    user_wallet[user_id] -= total
    add_order_record(user_id, product_id, qty, total, "Completed", "Wallet")
    add_transaction_record(user_id, "Wallet Purchase", total, "Completed", {"product_id": product_id, "qty": qty})

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
    ok, _ = await deliver_accounts_to_user(bot, user_id, product_id, qty)
    if not ok:
        pending_crypto_orders.pop(user_id, None)
        return False

    used_txids.add(txid)
    add_order_record(user_id, product_id, qty, total, "Completed", "Crypto")

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

    ok, _ = await deliver_accounts_to_user(context.bot, order["user_id"], order["product_id"], order["qty"])
    if not ok:
        return False, "Not enough real stock to deliver."

    set_order_status(order, "Completed")
    for user_order in user_orders.get(order["user_id"], []):
        if user_order["id"] == order_id:
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
# COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)
    enter_client_mode(user_id)
    await send_client_main_text(update, render_home_text())


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)
    if not is_admin(user_id):
        await update.message.reply_text("❌ <b>You are not allowed to open admin panel.</b>", parse_mode="HTML")
        return
    enter_admin_mode(user_id)
    await send_admin_main_text(update, "🛠 <b>ADMIN MODE ON</b>\n\nBottom menu now switched to admin menu.")


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Your Telegram ID: {update.effective_user.id}")


async def addstock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)
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
# =========================
# TEXT HANDLER
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ensure_user(user_id)
    text = update.message.text.strip()
    state = user_state[user_id]
    step = state.get("step", "main")

    # ========= ADMIN MAIN MENUS =========
    if user_mode[user_id] == "admin":
        if text == "🚪 Exit Admin":
            enter_client_mode(user_id)
            await send_client_main_text(update, "✅ <b>Admin mode off.</b>\n\nBack to client menu.")
            return

        if text == "📦 Products":
            user_state[user_id] = {"step": "admin_products"}
            await update.message.reply_text(render_admin_products_text(), reply_markup=admin_menu(), parse_mode="HTML")
            await update.message.reply_text("Choose a products action below.", reply_markup=admin_products_keyboard(), parse_mode="HTML")
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

        if text == "📊 Analytics":
            await update.message.reply_text(render_analytics(), reply_markup=admin_menu(), parse_mode="HTML")
            return

    # ========= PRODUCT ADD =========
    if step == "admin_add_product_icon":
        admin_temp[user_id]["icon"] = text
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
        user_state[user_id] = {"step": "admin_edit_details_confirm"}
        await update.message.reply_text(
            render_admin_edit_details_preview(admin_temp[user_id]["selected_product_id"], details),
            reply_markup=admin_confirm_keyboard("admin_confirm_details_update", "✅ Confirm Details"),
            parse_mode="HTML",
        )
        return

    if step == "admin_edit_icon_input":
        new_icon = text.strip()
        if not new_icon:
            await update.message.reply_text("❌ <b>Icon cannot be empty.</b>", parse_mode="HTML")
            return
        admin_temp[user_id]["new_icon"] = new_icon
        user_state[user_id] = {"step": "admin_edit_icon_confirm"}
        await update.message.reply_text(
            render_admin_edit_icon_preview(admin_temp[user_id]["selected_product_id"], new_icon),
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
            await update.message.reply_text("❌ <b>Invalid format.</b>\n\nUse:\n<code>email@gmail.com|password123|Private Account</code>", parse_mode="HTML")
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
            await update.message.reply_text("❌ <b>No valid account line found.</b>\n\nUse:\n<code>email@gmail.com|password123|Private Account</code>", parse_mode="HTML")
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
            await update.message.reply_text("❌ <b>Invalid format.</b>\n\nUse:\n<code>email@gmail.com|password123|Private Account</code>", parse_mode="HTML")
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

        total = PRODUCTS[product_id]["price"] * qty
        if user_wallet[user_id] >= total:
            await process_wallet_purchase(update, context, user_id, product_id, qty, total)
            user_state[user_id] = {"step": "main"}
            return

        user_state[user_id] = {"step": "buy_payment_method", "product_id": product_id, "qty": qty, "total": total}
        await update.message.reply_text(render_buy_summary(product_id, qty, user_wallet[user_id]), reply_markup=payment_method_keyboard("buy"), parse_mode="HTML")
        return

    if step == "deposit_custom_amount":
        try:
            amount = float(text)
            if amount <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ <b>Invalid amount.</b> Please send a valid number.", parse_mode="HTML")
            return
        user_state[user_id] = {"step": "deposit_payment_method", "amount": amount}
        await update.message.reply_text(render_deposit_method_text(amount), reply_markup=payment_method_keyboard("dep"), parse_mode="HTML")
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
        user_state[user_id] = {"step": "shop"}
        await update.message.reply_text("🛍 <b>SHOP MENU</b>\n", parse_mode="HTML")
        await send_shop_cards_message(update.message, from_callback=False)
        return

    if text == "💰 Wallet":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_wallet_text(user_id))
        return

    if text == "🆔 User ID":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_user_id_text(user_id))
        return

    if text == "💳 Top Up":
        user_state[user_id] = {"step": "deposit_amount"}
        await send_inline_from_text(update, render_deposit_text(), deposit_amount_keyboard())
        return

    if text == "📦 Orders":
        user_state[user_id] = {"step": "main"}
        await send_client_main_text(update, render_orders_text(user_id))
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

    await update.message.reply_text(
        "Please use the fixed menu below.",
        reply_markup=admin_menu() if user_mode[user_id] == "admin" else main_menu()
    )
# =========================
# CALLBACK HANDLER
# =========================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    ensure_user(user_id)
    await query.answer()
    data = query.data

    if data == "noop":
        return

    # ========= PRODUCT ADMIN =========
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

    if data == "admin_delete_product_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_delete_product_pick"}
        await send_inline_from_callback(query, "🗑 <b>Delete Product</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_delete"))
        return

    if data == "admin_reorder_menu":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "admin_reorder_pick"}
        await send_inline_from_callback(query, "↕️ <b>Reorder Products</b>\n\nSelect a product below.", admin_product_select_keyboard("admin_pick_reorder"))
        return

    if data == "admin_cancel_flow":
        reset_admin_temp(user_id)
        user_state[user_id] = {"step": "main"}
        await send_inline_from_callback(query, "❌ <b>Cancelled.</b>", close_keyboard())
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

    if data.startswith("admin_pick_icon_"):
        product_id = data.replace("admin_pick_icon_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "admin_edit_icon_input"}
        await send_inline_from_callback(query, f"😀 <b>Edit Icon</b>\n\nCurrent: {PRODUCTS[product_id].get('icon', '📦')}\n\nNow send new icon.", admin_cancel_keyboard())
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
        idx = product_order.index(product_id)
        if idx > 0:
            product_order[idx], product_order[idx - 1] = product_order[idx - 1], product_order[idx]
        await send_inline_from_callback(query, "✅ <b>Moved up.</b>", admin_reorder_selected_keyboard(product_id))
        return

    if data.startswith("admin_move_down_"):
        product_id = data.replace("admin_move_down_", "")
        idx = product_order.index(product_id)
        if idx < len(product_order) - 1:
            product_order[idx], product_order[idx + 1] = product_order[idx + 1], product_order[idx]
        await send_inline_from_callback(query, "✅ <b>Moved down.</b>", admin_reorder_selected_keyboard(product_id))
        return

    if data == "admin_confirm_add_product":
        temp = admin_temp[user_id]
        product_id = generate_new_product_id()
        PRODUCTS[product_id] = {
            "name": temp["name"],
            "icon": temp.get("icon", "📦"),
            "month": temp["month"],
            "price": float(temp["price"]),
            "details": list(temp["details"]),
            "accounts": [],
            "display_stock": int(temp.get("display_stock", 0)),
        }
        notify_waitlist[product_id] = set()
        product_order.append(product_id)
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
        reset_admin_temp(user_id)
        await send_inline_from_callback(query, "✅ <b>Details updated.</b>", admin_products_keyboard())
        return

    if data == "admin_confirm_icon_update":
        product_id = admin_temp[user_id]["selected_product_id"]
        PRODUCTS[product_id]["icon"] = admin_temp[user_id]["new_icon"]
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
        await send_inline_from_callback(query, "Send account in this format:\n<code>email@gmail.com|password|note</code>", admin_cancel_keyboard())
        return

    if data.startswith("stock_pick_bulk_"):
        product_id = data.replace("stock_pick_bulk_", "")
        admin_temp[user_id]["selected_product_id"] = product_id
        user_state[user_id] = {"step": "stock_add_bulk_input"}
        await send_inline_from_callback(query, "Send multiple accounts line by line:\n<code>email@gmail.com|password|note</code>", admin_cancel_keyboard())
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
        await send_inline_from_callback(query, "Send new account in this format:\n<code>email@gmail.com|password|note</code>", admin_cancel_keyboard())
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

    # ========= CLIENT FLOWS =========
    if data == "close_inline":
        await send_inline_from_callback(query, "Closed.", close_keyboard())
        return

    if data == "back_shop_cards":
        user_state[user_id] = {"step": "shop"}
        await send_inline_from_callback(query, "🛍 <b>SHOP MENU</b>\n")
        await send_shop_cards_message(query, from_callback=True)
        return

    if data.startswith("shop_buy_"):
        product_id = data.replace("shop_buy_", "")
        if get_display_stock(product_id) <= 0:
            await send_inline_from_callback(query, "❌ <b>This product is currently out of stock.</b>", close_keyboard())
            return
        user_state[user_id] = {"step": "buy_qty_select", "product_id": product_id}
        await send_inline_from_callback(query, render_product_details(product_id), buy_qty_keyboard(product_id))
        return

    if data.startswith("shop_notify_"):
        product_id = data.replace("shop_notify_", "")
        notify_waitlist[product_id].add(user_id)
        product = PRODUCTS[product_id]
        await send_inline_from_callback(query, f"🔔 You will be notified when <b>{product['name']}</b> is back in stock.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Shop", callback_data="back_shop_cards")]]))
        return

    if data.startswith("buy_qty_"):
        _, _, product_id, qty_str = data.split("_")
        qty = int(qty_str)
        stock = get_display_stock(product_id)
        if qty > stock:
            await send_inline_from_callback(query, f"❌ <b>Only {stock} pcs available.</b>", buy_qty_keyboard(product_id))
            return
        total = PRODUCTS[product_id]["price"] * qty
        if user_wallet[user_id] >= total:
            await process_wallet_purchase(query, context, user_id, product_id, qty, total)
            user_state[user_id] = {"step": "main"}
            return
        user_state[user_id] = {"step": "buy_payment_method", "product_id": product_id, "qty": qty, "total": total}
        await send_inline_from_callback(query, render_buy_summary(product_id, qty, user_wallet[user_id]), payment_method_keyboard("buy"))
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
        await send_inline_from_callback(query, render_buy_summary(state["product_id"], state["qty"], user_wallet[user_id]), payment_method_keyboard("buy"))
        return

    if data.startswith("buy_net_"):
        network_label = paymod.map_network_callback_to_label(data.replace("buy_net_", ""))
        address = CRYPTO_ADDRESSES[network_label]
        state = user_state[user_id]
        crypto_amount = build_unique_crypto_amount(state["total"], network_label, user_id)
        wc.create_or_update_pending_order(
            user_id,
            state["product_id"],
            state["qty"],
            state["total"],
            float(crypto_amount),
            network_label,
            address,
        )
        user_state[user_id] = {"step": "buy_payment_ready", "product_id": state["product_id"], "qty": state["qty"], "total": state["total"], "network": network_label}
        await send_inline_from_callback(
            query,
            paymod.render_buy_crypto_payment_text(
                state["product_id"],
                state["qty"],
                state["total"],
                crypto_amount,
                network_label,
                address,
                PRODUCTS,
            ),
            paymod.payment_request_keyboard("buypay"),
        )
        return

    if data == "buypay_change_network":
        user_state[user_id]["step"] = "buy_network"
        await send_inline_from_callback(query, "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:", paymod.network_keyboard("buy"))
        return

    if data == "buypay_verify":
        result = wc.on_verify_clicked(user_id, auto_scan_callable_from_record)
        if result["status"] == "confirmed":
            record = wc.get_user_pending_order(user_id)
            if record:
                await finalize_auto_order_record(record)
            await send_inline_from_callback(query, paymod.render_auto_verify_success_text(user_state[user_id].get("total", 0)), close_keyboard())
        else:
            await send_inline_from_callback(query, paymod.render_auto_verify_pending_text(), paymod.payment_request_keyboard("buypay"))
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
        await send_inline_from_callback(query, "✅ <b>Submitted.</b>\n\nSend payment screenshot to Live Support for confirmation.", close_keyboard())
        return

    if data == "buymanual_cancel":
        user_state[user_id] = {"step": "main"}
        await send_inline_from_callback(query, "❌ <b>Order cancelled.</b>", close_keyboard())
        return

    if data.startswith("dep_amt_"):
        amount = float(data.replace("dep_amt_", ""))
        user_state[user_id] = {"step": "deposit_payment_method", "amount": amount}
        await send_inline_from_callback(query, render_deposit_method_text(amount), payment_method_keyboard("dep"))
        return

    if data == "dep_custom":
        user_state[user_id] = {"step": "deposit_custom_amount"}
        await query.message.reply_text("✏️ Send custom deposit amount.\nExample: 25")
        return

    if data == "dep_back":
        await send_inline_from_callback(query, render_deposit_text(), deposit_amount_keyboard())
        return

    if data == "dep_method_binance":
        amount = user_state[user_id]["amount"]
        await send_inline_from_callback(query, render_manual_payment_text(amount, "Binance ID", BINANCE_ID), final_manual_keyboard("depmanual"))
        return

    if data == "dep_method_bybit":
        amount = user_state[user_id]["amount"]
        await send_inline_from_callback(query, render_manual_payment_text(amount, "Bybit ID", BYBIT_ID), final_manual_keyboard("depmanual"))
        return

    if data == "dep_method_crypto":
        user_state[user_id]["step"] = "deposit_network"
        await send_inline_from_callback(query, "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:", network_keyboard("dep"))
        return

    if data == "dep_back_method":
        amount = user_state[user_id]["amount"]
        await send_inline_from_callback(query, render_deposit_method_text(amount), payment_method_keyboard("dep"))
        return

    if data.startswith("dep_net_"):
        network_label = paymod.map_network_callback_to_label(data.replace("dep_net_", ""))
        address = CRYPTO_ADDRESSES[network_label]
        amount = user_state[user_id]["amount"]
        crypto_amount = build_unique_crypto_amount(amount, network_label, user_id)
        wc.create_or_update_pending_deposit(
            user_id,
            amount,
            float(crypto_amount),
            network_label,
            address,
        )
        user_state[user_id] = {"step": "deposit_payment_ready", "amount": amount, "network": network_label}
        await send_inline_from_callback(
            query,
            paymod.render_crypto_payment_text(amount, crypto_amount, network_label, address),
            paymod.payment_request_keyboard("deppay"),
        )
        return

    if data == "deppay_change_network":
        user_state[user_id]["step"] = "deposit_network"
        await send_inline_from_callback(query, "🌐 <b>SELECT NETWORK</b>\n\nChoose a cryptocurrency below:", paymod.network_keyboard("dep"))
        return

    if data == "deppay_verify":
        result = wc.on_verify_clicked(user_id, auto_scan_callable_from_record)
        if result["status"] == "confirmed":
            record = wc.get_user_pending_deposit(user_id)
            if record:
                await finalize_auto_deposit_record(record)
            await send_inline_from_callback(query, paymod.render_auto_verify_success_text(user_state[user_id].get("amount", 0)), close_keyboard())
        else:
            await send_inline_from_callback(query, paymod.render_auto_verify_pending_text(), paymod.payment_request_keyboard("deppay"))
        return

    if data == "depmanual_submitted":
        amount = user_state[user_id].get("amount", 0)
        add_transaction_record(user_id, "Deposit", amount, "Waiting Manual Confirmation")
        user_state[user_id] = {"step": "main"}
        await send_inline_from_callback(query, "✅ <b>Submitted.</b>\n\nSend payment screenshot to Live Support for confirmation.", close_keyboard())
        return

    if data == "depmanual_cancel":
        user_state[user_id] = {"step": "main"}
        await send_inline_from_callback(query, "❌ <b>Deposit cancelled.</b>", close_keyboard())
        return


# =========================
# MAIN APP
# =========================
def main():
    global app_instance
    app = Application.builder().token(BOT_TOKEN).build()
    app_instance = app

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("addstock", addstock))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.job_queue.run_repeating(background_job, interval=20, first=20)

    print("✅ Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
