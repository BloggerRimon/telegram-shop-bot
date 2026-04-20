from decimal import Decimal, InvalidOperation, ROUND_UP
from telegram import InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# BASIC HELPERS
# =========================
def escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def safe_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def format_money(value: float) -> str:
    return f"${float(value):.2f}"


def quantize_up(value, decimals: int) -> Decimal:
    dec = safe_decimal(value)
    if dec is None:
        return Decimal("0")
    q = Decimal("1." + ("0" * decimals))
    return dec.quantize(q, rounding=ROUND_UP)


# =========================
# ORIGINAL PAYMENT HELPERS + REQUIRED UPGRADE HELPERS
# =========================
NETWORK_DECIMALS = {
    "USDT (TRC20)": 2,
    "USDT (ERC20)": 2,
    "USDT (BEP20)": 2,
    "BTC": 8,
    "LTC": 8,
    "ETH (ERC20)": 8,
    "BNB (BEP20)": 8,
    "SOL": 8,
    "TRX (TRC20)": 8,
}

NETWORK_LABELS = {
    "USDT (TRC20)": "USDT (TRC20)",
    "USDT (ERC20)": "USDT (ERC20)",
    "USDT (BEP20)": "USDT (BEP20)",
    "BTC": "BTC (Bitcoin)",
    "LTC": "LTC (Litecoin)",
    "ETH (ERC20)": "ETH (ERC20)",
    "BNB (BEP20)": "BNB (BEP20)",
    "SOL": "SOL (Solana)",
    "TRX (TRC20)": "TRX (TRC20)",
}


def format_network_amount(crypto_amount, network: str) -> str:
    dec = safe_decimal(crypto_amount)
    if dec is None:
        dec = Decimal("0")
    decimals = NETWORK_DECIMALS.get(network, 8)
    fmt = f"{{0:.{decimals}f}}"
    return f"{fmt.format(dec)} {NETWORK_LABELS.get(network, network)}"


def calculate_buffered_amount(crypto_amount, network: str):
    amount_dec = safe_decimal(crypto_amount)
    if amount_dec is None:
        return Decimal("0")

    if network.startswith("USDT"):
        buffered = amount_dec + Decimal("0.10")
    else:
        buffered = amount_dec * Decimal("1.01")

    return quantize_up(buffered, NETWORK_DECIMALS.get(network, 8))


def amount_within_tolerance(actual_amount, expected_amount, tolerance=0.10):
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)

    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False

    return abs(actual_dec - expected_dec) <= tolerance_dec


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


# =========================
# ORIGINAL KEYBOARDS (RETAINED)
# =========================
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


# =========================
# REQUIRED NEW KEYBOARD FOR YOUR EXACT FLOW
# =========================
def payment_request_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✅ I Have Paid (Verify)", callback_data=f"{prefix}_verify")],
        [InlineKeyboardButton("🔁 Change Network", callback_data=f"{prefix}_change_network")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"{prefix}_back_method")],
    ]
    return InlineKeyboardMarkup(rows)


# =========================
# ORIGINAL TEXT RENDERERS (UPGRADED WHERE REQUIRED)
# =========================
def render_buy_summary(product_id: str, qty: int, wallet_balance: float, products: dict) -> str:
    product = products[product_id]
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


def render_crypto_payment_text(usd_amount: float, crypto_amount, network: str, address: str) -> str:
    return (
        "✅ <b>PAYMENT REQUEST GENERATED!</b>\n\n"
        "💵 <b>Amount:</b>\n"
        f"<code>{format_money(usd_amount)}</code>\n\n"
        "🪙 <b>Amount to send:</b>\n"
        f"<code>{escape_html(format_network_amount(crypto_amount, network))}</code>\n\n"
        "🏦 <b>Deposit Address:</b>\n"
        f"<code>{escape_html(address)}</code>\n\n"
        "⚠️ <b>CRITICAL:</b> Send <b>EXACTLY</b> the amount below. Do not round!\n"
        "Account for your exchange's withdrawal fee.\n\n"
        "If you send a different amount, the system will NOT automatically recognize it."
    )


def render_buy_crypto_payment_text(product_id: str, qty: int, usd_amount: float, crypto_amount, network: str, address: str, products: dict) -> str:
    product = products[product_id]
    return (
        "✅ <b>PAYMENT REQUEST GENERATED!</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Quantity:</b> {qty}\n\n"
        "💵 <b>Amount:</b>\n"
        f"<code>{format_money(usd_amount)}</code>\n\n"
        "🪙 <b>Amount to send:</b>\n"
        f"<code>{escape_html(format_network_amount(crypto_amount, network))}</code>\n\n"
        "🏦 <b>Deposit Address:</b>\n"
        f"<code>{escape_html(address)}</code>\n\n"
        "⚠️ <b>CRITICAL:</b> Send <b>EXACTLY</b> the amount below. Do not round!\n"
        "Account for your exchange's withdrawal fee.\n\n"
        "If you send a different amount, the system will NOT automatically recognize it."
    )


def render_buy_manual_payment_text(product_id: str, qty: int, total: float, method: str, details: str, products: dict) -> str:
    product = products[product_id]
    return (
        "🏦 <b>ORDER PAYMENT DETAILS</b>\n\n"
        f"<b>Product:</b> {product['name']}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Total:</b> {format_money(total)}\n"
        f"<b>Method:</b> {method}\n\n"
        f"{escape_html(details)}\n\n"
        "<b>Send payment screenshot to Live Support for confirmation.</b>"
    )


# =========================
# FULLY AUTOMATIC UI MESSAGES
# =========================
def render_auto_verify_wait_text() -> str:
    return (
        "⏳ <b>Checking payment automatically...</b>\n\n"
        "The bot is now checking the blockchain.\n"
        "No TXID is required."
    )


def render_auto_verify_pending_text() -> str:
    return (
        "⏳ <b>No matching payment found yet.</b>\n\n"
        "If you already paid, wait a little and tap verify again."
    )


def render_auto_verify_success_text(usd_amount: float) -> str:
    return (
        "✅ <b>Payment confirmed automatically.</b>\n\n"
        f"<b>Amount:</b> {format_money(usd_amount)}"
    )
