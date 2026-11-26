"""Static network / asset configuration for self-custody payment monitoring.

Receiving addresses are PUBLIC addresses supplied by the shop owner. They are
hardcoded on purpose: changing a receiving destination must be a reviewed code
change, never an environment edit or an admin click.

Ethereum and BNB Smart Chain deliberately share the same address text. They are
kept as separate networks everywhere (keys, invoices, transfer ids, ledgers).
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from typing import Dict, Optional

# ---------------------------------------------------------------------------
# Public receiving addresses (exact strings supplied by the owner).
# ---------------------------------------------------------------------------
TRON_ADDRESS = "TGvd1iCDKswiejr14gsDxDvEm5d5atCwuw"
ETHEREUM_ADDRESS = "0x51a2aF4Bbcfb6d605018afE57CBbffB3de3d2022"
BSC_ADDRESS = "0x51a2aF4Bbcfb6d605018afE57CBbffB3de3d2022"
BITCOIN_ADDRESS = "bc1qv52jn5v4hg5j0fwfawv02v32e8g7wqhx39jsgw"
LITECOIN_ADDRESS = "ltc1qjjh6vu4qhg0f4smg5xwgms4jn29hesglxha363"
SOLANA_ADDRESS = "FHiqkiX5AmVMEZnt1w8oC6vh2fXNXueZHgNCXUzFrtrq"

ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


@dataclass(frozen=True)
class Network:
    key: str
    label: str
    address: str
    family: str  # evm | tron | utxo | solana
    chain_id: Optional[int]
    alchemy_networks: tuple  # accepted values of event.network in webhook payloads
    alchemy_rpc_host: str
    min_confirmations: int
    finality_note: str


@dataclass(frozen=True)
class Asset:
    id: str
    label: str
    network: str
    kind: str  # native | token
    contract: Optional[str]  # lowercase hex for EVM, base58 for TRON
    decimals: int
    display_decimals: int  # unique-amount quantum = 10**(decimals-display_decimals)
    coingecko_id: str
    stablecoin: bool
    issuer_note: str


NETWORKS: Dict[str, Network] = {
    "tron": Network("tron", "TRON Mainnet", TRON_ADDRESS, "tron", None, ("TRON_MAINNET",),
                    "tron-mainnet.g.alchemy.com", 19, "solidified block (walletsolidity)"),
    "ethereum": Network("ethereum", "Ethereum Mainnet", ETHEREUM_ADDRESS, "evm", 1, ("ETH_MAINNET",),
                        "eth-mainnet.g.alchemy.com", 12, "12 confirmations"),
    "bsc": Network("bsc", "BNB Smart Chain Mainnet (56)", BSC_ADDRESS, "evm", 56, ("BNB_MAINNET", "BSC_MAINNET"),
                   "bnb-mainnet.g.alchemy.com", 15, "15 confirmations"),
    "bitcoin": Network("bitcoin", "Bitcoin Mainnet", BITCOIN_ADDRESS, "utxo", None, ("BITCOIN_MAINNET",),
                       "bitcoin-mainnet.g.alchemy.com", 2, "2 confirmations"),
    "litecoin": Network("litecoin", "Litecoin Mainnet", LITECOIN_ADDRESS, "utxo", None, ("LITECOIN_MAINNET",),
                        "litecoin-mainnet.g.alchemy.com", 6, "6 confirmations"),
    "solana": Network("solana", "Solana Mainnet", SOLANA_ADDRESS, "solana", None, ("SOLANA_MAINNET",),
                      "solana-mainnet.g.alchemy.com", 1, "finalized commitment"),
}

# Allowlisted token contracts. Never match a token by name/symbol.
USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"  # Tether-issued (tether.to supported protocols)
USDT_ERC20_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"  # Tether-issued (tether.to supported protocols)
USDT_BEP20_CONTRACT = "0x55d398326f99059ff775485246999027b3197955"  # Binance-Peg BSC-USD, issued/custodied by Binance

ASSETS: Dict[str, Asset] = {
    "usdt_trc20": Asset("usdt_trc20", "USDT TRC20", "tron", "token", USDT_TRC20_CONTRACT, 6, 4, "tether", True,
                        "Tether-issued USDT on TRON"),
    "usdt_erc20": Asset("usdt_erc20", "USDT ERC20", "ethereum", "token", USDT_ERC20_CONTRACT, 6, 4, "tether", True,
                        "Tether-issued USDT on Ethereum"),
    "usdt_bep20": Asset("usdt_bep20", "USDT BEP20", "bsc", "token", USDT_BEP20_CONTRACT, 18, 4, "tether", True,
                        "Binance-Peg BSC-USD (Binance-issued bridged USDT, not issued by Tether directly)"),
    "trx": Asset("trx", "TRX on TRON", "tron", "native", None, 6, 4, "tron", False, "native"),
    "btc": Asset("btc", "BTC on Bitcoin", "bitcoin", "native", None, 8, 8, "bitcoin", False, "native"),
    "ltc": Asset("ltc", "LTC on Litecoin", "litecoin", "native", None, 8, 8, "litecoin", False, "native"),
    "eth": Asset("eth", "ETH on Ethereum", "ethereum", "native", None, 18, 6, "ethereum", False, "native"),
    "bnb": Asset("bnb", "BNB on BNB Smart Chain", "bsc", "native", None, 18, 6, "binancecoin", False, "native"),
    "sol": Asset("sol", "SOL on Solana", "solana", "native", None, 9, 6, "solana", False, "native"),
}

ASSET_ORDER = ["usdt_trc20", "usdt_erc20", "usdt_bep20", "trx", "btc", "ltc", "eth", "bnb", "sol"]


def assets_for_network(network_key: str):
    return [a for a in (ASSETS[i] for i in ASSET_ORDER) if a.network == network_key]


def token_asset_for_contract(network_key: str, contract: str) -> Optional[Asset]:
    c = (contract or "").strip()
    for a in assets_for_network(network_key):
        if a.kind != "token":
            continue
        if NETWORKS[network_key].family == "evm":
            if a.contract == c.lower():
                return a
        elif a.contract == c:
            return a
    return None


def native_asset(network_key: str) -> Asset:
    for a in assets_for_network(network_key):
        if a.kind == "native":
            return a
    raise KeyError(network_key)


# ---------------------------------------------------------------------------
# Address format validation (format only; this cannot prove ownership).
# ---------------------------------------------------------------------------
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s: str) -> bytes:
    n = 0
    for ch in s:
        idx = _B58.find(ch)
        if idx < 0:
            raise ValueError("invalid base58")
        n = n * 58 + idx
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + raw


def b58encode(b: bytes) -> str:
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    pad = len(b) - len(b.lstrip(b"\x00"))
    return "1" * pad + out


def tron_to_hex(addr: str) -> str:
    """Base58check TRON address -> 21-byte hex starting with 41."""
    raw = b58decode(addr)
    if len(raw) != 25:
        raise ValueError("bad tron length")
    body, chk = raw[:21], raw[21:]
    if hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4] != chk:
        raise ValueError("bad tron checksum")
    if body[0] != 0x41:
        raise ValueError("bad tron prefix")
    return body.hex()


def tron_hex_to_base58(hex_addr: str) -> str:
    h = hex_addr.lower().removeprefix("0x")
    if len(h) == 40:
        h = "41" + h
    body = bytes.fromhex(h)
    chk = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    return b58encode(body + chk)


_BECH32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def _bech32_polymod(values):
    gen = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        top = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i in range(5):
            chk ^= gen[i] if ((top >> i) & 1) else 0
    return chk


def bech32_valid(addr: str, hrp: str) -> bool:
    if addr.lower() != addr and addr.upper() != addr:
        return False
    addr = addr.lower()
    pos = addr.rfind("1")
    if addr[:pos] != hrp or pos + 7 > len(addr):
        return False
    data = []
    for ch in addr[pos + 1:]:
        if ch not in _BECH32:
            return False
        data.append(_BECH32.index(ch))
    hrp_exp = [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]
    const = _bech32_polymod(hrp_exp + data)
    witver = data[0]
    # witness v0 uses bech32 (const 1); v1+ uses bech32m
    return (witver == 0 and const == 1) or (witver > 0 and const == 0x2BC830A3)


def validate_address(network_key: str, address: str) -> bool:
    try:
        if network_key == "tron":
            tron_to_hex(address)
            return True
        if network_key in ("ethereum", "bsc"):
            return bool(re.fullmatch(r"0x[0-9a-fA-F]{40}", address))
        if network_key == "bitcoin":
            return bech32_valid(address, "bc")
        if network_key == "litecoin":
            return bech32_valid(address, "ltc")
        if network_key == "solana":
            return len(b58decode(address)) == 32
    except Exception:
        return False
    return False


def same_address(network_key: str, a: str, b: str) -> bool:
    if not a or not b:
        return False
    if NETWORKS[network_key].family == "evm":
        return a.lower() == b.lower()
    if network_key == "tron":
        try:
            ha = tron_to_hex(a) if a.startswith("T") else ("41" + a[-40:] if len(a) in (40, 42) else a)
            hb = tron_to_hex(b) if b.startswith("T") else ("41" + b[-40:] if len(b) in (40, 42) else b)
            return ha.lower()[-40:] == hb.lower()[-40:]
        except Exception:
            return False
    return a == b


# ---------------------------------------------------------------------------
# Environment (names only are ever shown to admins; values are never logged).
# ---------------------------------------------------------------------------
ENV_ALCHEMY_API_KEY = "ALCHEMY_API_KEY"
ENV_TRONGRID_API_KEY = "TRONGRID_API_KEY"
ENV_EMERGENCY = "ONCHAIN_EMERGENCY_DISABLE"


def signing_key_env(network_key: str) -> str:
    return f"ALCHEMY_SIGNING_KEY_{network_key.upper()}"


def webhook_id_env(network_key: str) -> str:
    return f"ALCHEMY_WEBHOOK_ID_{network_key.upper()}"


def env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def emergency_disabled() -> bool:
    return env(ENV_EMERGENCY).lower() in {"1", "true", "yes", "on"}


def alchemy_url(network_key: str) -> str:
    override = env(f"ONCHAIN_RPC_URL_{network_key.upper()}")
    if override:
        return override
    key = env(ENV_ALCHEMY_API_KEY)
    if not key:
        return ""
    return f"https://{NETWORKS[network_key].alchemy_rpc_host}/v2/{key}"


def mask(value: str) -> str:
    v = value or ""
    if not v:
        return "not set"
    return "set"
