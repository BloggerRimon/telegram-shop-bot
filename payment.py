from decimal import Decimal, InvalidOperation, ROUND_UP, ROUND_DOWN
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
import random


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


def quantize_down(value, decimals: int) -> Decimal:
    dec = safe_decimal(value)
    if dec is None:
        return Decimal("0")
    q = Decimal("1." + ("0" * decimals))
    return dec.quantize(q, rounding=ROUND_DOWN)


# =========================
# NETWORK / DISPLAY CONFIG
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

NETWORK_TO_COIN = {
    "USDT (TRC20)": "USDT",
    "USDT (ERC20)": "USDT",
    "USDT (BEP20)": "USDT",
    "BTC": "BTC",
    "LTC": "LTC",
    "ETH (ERC20)": "ETH",
    "BNB (BEP20)": "BNB",
    "SOL": "SOL",
    "TRX (TRC20)": "TRX",
}

COIN_KEY_TO_NETWORK = {
    "BTC": "BTC",
    "LTC": "LTC",
    "ETH": "ETH (ERC20)",
    "BNB": "BNB (BEP20)",
    "SOL": "SOL",
    "TRX": "TRX (TRC20)",
    "USDT_TRC20": "USDT (TRC20)",
    "USDT_ERC20": "USDT (ERC20)",
    "USDT_BEP20": "USDT (BEP20)",
}


def format_network_amount(crypto_amount, network: str) -> str:
    dec = safe_decimal(crypto_amount)
    if dec is None:
        dec = Decimal("0")
    decimals = NETWORK_DECIMALS.get(network, 8)
    fmt = f"{{0:.{decimals}f}}"
    return f"{fmt.format(dec)} {NETWORK_LABELS.get(network, network)}"


def amount_within_tolerance(actual_amount, expected_amount, tolerance=0.10):
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)

    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False

    return abs(actual_dec - expected_dec) <= tolerance_dec


# =========================
# PAYMENT AMOUNT GENERATION
# =========================
def normalize_rate_value(rate_value):
    rate_dec = safe_decimal(rate_value)
    if rate_dec is None or rate_dec <= 0:
        raise ValueError("Invalid rate value")
    return rate_dec


def generate_unique_amount(base_amount: Decimal, network: str) -> Decimal:
    """
    Unique amount for reliable auto-detection.
    Stablecoins get 2 decimals.
    Volatile coins keep more decimals.
    """
    decimals = NETWORK_DECIMALS.get(network, 8)

    if network.startswith("USDT"):
        # keep stablecoin display human-friendly but still unique
        extra = Decimal(str(random.choice([0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09])))
    elif network in {"BTC", "LTC", "ETH (ERC20)", "BNB (BEP20)", "SOL", "TRX (TRC20)"}:
        extra = Decimal(str(random.uniform(0.000001, 0.000099)))
    else:
        extra = Decimal("0")

    final_amount = safe_decimal(base_amount) or Decimal("0")
    final_amount += extra
    return quantize_down(final_amount, decimals)


def calculate_exact_crypto_amount_from_rate(usd_amount, network: str, usd_rate):
    """
    This is the IMPORTANT function:
    displayed amount == backend expected amount
    """
    usd_dec = safe_decimal(usd_amount)
    rate_dec = normalize_rate_value(usd_rate)

    if usd_dec is None or usd_dec <= 0:
        raise ValueError("Invalid USD amount")

    base_amount = usd_dec / rate_dec
    return generate_unique_amount(base_amount, network)


def calculate_buffered_amount(crypto_amount, network: str):
    """
    Kept for compatibility. Do NOT show this separately to users.
    Prefer calculate_exact_crypto_amount_from_rate() for real payment requests.
    """
    amount_dec = safe_decimal(crypto_amount)
    if amount_dec is None:
        return Decimal("0")

    if network.startswith("USDT"):
        buffered = amount_dec + Decimal("0.10")
    else:
        buffered = amount_dec * Decimal("1.01")

    return quantize_up(buffered, NETWORK_DECIMALS.get(network, 8))


def create_payment_request(user_id: int, usd_amount: float, network: str, address: str, usd_rate):
    crypto_amount = calculate_exact_crypto_amount_from_rate(usd_amount, network, usd_rate)
    return {
        "user_id": user_id,
        "usd_amount": float(usd_amount),
        "crypto_amount": float(crypto_amount),
        "network": network,
        "address": address,
    }


def create_payment_request_from_key(user_id: int, usd_amount: float, network_key: str, wallet_addresses: dict, usd_rates: dict):
    """
    Convenience wrapper if bot stores addresses/rates by short keys.
    """
    if network_key not in COIN_KEY_TO_NETWORK:
        raise ValueError("Invalid network key")

    network = COIN_KEY_TO_NETWORK[network_key]
    address = wallet_addresses.get(network_key) or wallet_addresses.get(network)
    if not address:
        raise ValueError("Wallet address not found")

    coin = NETWORK_TO_COIN[network]
    usd_rate = usd_rates[coin]
    return create_payment_request(user_id, usd_amount, network, address, usd_rate)


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


def payment_request_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✅ I Have Paid (Verify)", callback_data=f"{prefix}_verify")],
        [InlineKeyboardButton("🔁 Change Network", callback_data=f"{prefix}_change_network")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"{prefix}_back_method")],
    ]
    return InlineKeyboardMarkup(rows)


# =========================
# FLOW HELPERS
# =========================
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
# ORIGINAL TEXT RENDERERS (RETAINED + FIXED)
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
        "Use the correct network only.\n\n"
        "If you send a different amount or wrong network, the system may not automatically recognize it."
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
        "Use the correct network only.\n\n"
        "If you send a different amount or wrong network, the system may not automatically recognize it."
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
# AUTOMATIC VERIFY UI MESSAGES
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
