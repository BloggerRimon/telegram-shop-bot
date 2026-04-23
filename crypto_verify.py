"""
Production-oriented crypto verification module.

Public entrypoints:
- auto_verify_by_record(record: dict, config: dict) -> dict
- verify_crypto_payment(network, txid, expected_amount, expected_to_address, config) -> dict
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

import requests


def safe_decimal(value: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def verify_result(ok: bool, status: str, reason: str, meta: Optional[dict] = None) -> dict:
    return {"ok": ok, "status": status, "reason": reason, "meta": meta or {}}


def debug_meta(**kwargs) -> dict:
    return {k: v for k, v in kwargs.items() if v is not None}


def normalize_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    text = str(value).strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        ts = float(text)
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def tx_is_within_request_window(tx_time: Any, request_created_at: Any, backward_minutes: int = 15, forward_hours: int = 24) -> bool:
    tx_dt = normalize_dt(tx_time)
    req_dt = normalize_dt(request_created_at)
    if tx_dt is None or req_dt is None:
        return False
    return (req_dt - timedelta(minutes=backward_minutes)) <= tx_dt <= (req_dt + timedelta(hours=forward_hours))


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_evm_address(addr: Any) -> str:
    return normalize_text(addr).lower()


def amount_within_tolerance(actual_amount: Any, expected_amount: Any, tolerance: Any) -> bool:
    a = safe_decimal(actual_amount)
    e = safe_decimal(expected_amount)
    t = safe_decimal(tolerance)
    if a is None or e is None or t is None:
        return False
    return abs(a - e) <= t


def get_network_tolerance(network: str) -> Decimal:
    return {
        "USDT (TRC20)": Decimal("0.03"),
        "USDT (ERC20)": Decimal("0.03"),
        "USDT (BEP20)": Decimal("0.03"),
        "TRX (TRC20)": Decimal("0.20"),
        "ETH (ERC20)": Decimal("0.0002"),
        "BNB (BEP20)": Decimal("0.0002"),
        "BTC": Decimal("0.00001000"),
        "LTC": Decimal("0.00010000"),
        "SOL": Decimal("0.0005"),
    }.get(network, Decimal("0.10"))


def http_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 25) -> dict:
    try:
        res = requests.get(url, params=params, headers=headers, timeout=timeout)
        try:
            data = res.json() if res.content else {}
        except Exception:
            data = {"raw_text": getattr(res, "text", "")[:1000]}
        return {"ok": res.ok, "status_code": res.status_code, "data": data, "text": getattr(res, "text", "")[:1000]}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}, "text": str(e)}


def http_post_json(url: str, payload: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 25) -> dict:
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        try:
            data = res.json() if res.content else {}
        except Exception:
            data = {"raw_text": getattr(res, "text", "")[:1000]}
        return {"ok": res.ok, "status_code": res.status_code, "data": data, "text": getattr(res, "text", "")[:1000]}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}, "text": str(e)}


# =========================
# EVM helpers
# =========================
def normalize_etherscan_v2_url(raw_url: str = "") -> str:
    raw = str(raw_url or "").strip()
    if not raw:
        return "https://api.etherscan.io/v2/api"
    lower = raw.lower()
    if "/v2/api" in lower:
        return raw
    if lower.endswith("/api"):
        return raw[:-4] + "/v2/api"
    if lower.endswith("/"):
        return raw + "v2/api"
    return raw + "/v2/api"


def get_evm_tx_by_hash(v2_url: str, api_key: str, chainid: str, txid: str) -> dict:
    return http_get_json(
        normalize_etherscan_v2_url(v2_url),
        params={"chainid": chainid, "module": "proxy", "action": "eth_getTransactionByHash", "txhash": txid, "apikey": api_key},
        timeout=20,
    )


def get_evm_tx_receipt(v2_url: str, api_key: str, chainid: str, txid: str) -> dict:
    return http_get_json(
        normalize_etherscan_v2_url(v2_url),
        params={"chainid": chainid, "module": "proxy", "action": "eth_getTransactionReceipt", "txhash": txid, "apikey": api_key},
        timeout=20,
    )


def get_evm_token_transfers_to_address(v2_url: str, api_key: str, chainid: str, address: str, contract_address: str, page: int = 1, offset: int = 100) -> dict:
    return http_get_json(
        normalize_etherscan_v2_url(v2_url),
        params={
            "chainid": chainid,
            "module": "account",
            "action": "tokentx",
            "address": address,
            "contractaddress": contract_address,
            "page": page,
            "offset": offset,
            "sort": "desc",
            "apikey": api_key,
        },
        timeout=20,
    )


def get_evm_native_transactions_to_address(v2_url: str, api_key: str, chainid: str, address: str, page: int = 1, offset: int = 100) -> dict:
    return http_get_json(
        normalize_etherscan_v2_url(v2_url),
        params={
            "chainid": chainid,
            "module": "account",
            "action": "txlist",
            "address": address,
            "page": page,
            "offset": offset,
            "sort": "desc",
            "apikey": api_key,
        },
        timeout=20,
    )


# =========================
# Tron helpers
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
    hex_addr = normalize_text(hex_addr).lower().replace("0x", "")
    if len(hex_addr) == 40:
        hex_addr = "41" + hex_addr
    raw = bytes.fromhex(hex_addr)
    checksum = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    return b58encode(raw + checksum)


def trongrid_headers(api_key: str) -> dict:
    return {"accept": "application/json", "content-type": "application/json", "TRON-PRO-API-KEY": api_key}


# =========================
# Solana helpers
# =========================
def helius_rpc(helius_rpc_url: str, method: str, params: list) -> dict:
    payload = {"jsonrpc": "2.0", "id": "1", "method": method, "params": params}
    return http_post_json(helius_rpc_url, payload=payload, headers={"content-type": "application/json"}, timeout=25)


def get_sol_signatures_for_address(helius_rpc_url: str, address: str, limit: int = 50) -> dict:
    return helius_rpc(helius_rpc_url, "getSignaturesForAddress", [address, {"limit": limit}])


# =========================
# Verify by txid
# =========================
def verify_evm_native_transfer(txid: str, expected_amount: Any, expected_to_address: str, chainid: str, network_label: str, v2_url: str, api_key: str) -> dict:
    tolerance = get_network_tolerance(network_label)
    tx_res = get_evm_tx_by_hash(v2_url, api_key, chainid, txid)
    tx = (tx_res.get("data") or {}).get("result")
    if not tx:
        return verify_result(False, "pending", f"{network_label} tx not found yet", debug_meta(raw=tx_res.get("text")))

    receipt_res = get_evm_tx_receipt(v2_url, api_key, chainid, txid)
    receipt = (receipt_res.get("data") or {}).get("result")
    if not receipt:
        return verify_result(False, "pending", f"{network_label} receipt not available yet", debug_meta(raw=receipt_res.get("text")))

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "Transaction failed")

    actual_to = normalize_evm_address(tx.get("to"))
    expected_to = normalize_evm_address(expected_to_address)
    if actual_to != expected_to:
        return verify_result(False, "rejected", "Destination address mismatch", debug_meta(actual_to=actual_to, expected_to=expected_to))

    try:
        value_wei = int(str(tx.get("value", "0")), 16)
    except Exception:
        return verify_result(False, "rejected", "Invalid native transfer value")

    actual_amount = Decimal(value_wei) / Decimal("1000000000000000000")
    if not amount_within_tolerance(actual_amount, expected_amount, tolerance):
        return verify_result(False, "rejected", "Amount mismatch", debug_meta(actual_amount=str(actual_amount), expected_amount=str(expected_amount)))

    return verify_result(True, "confirmed", "verified", debug_meta(network=network_label, txid=txid, actual_amount=str(actual_amount)))


def verify_evm_token_transfer(txid: str, expected_amount: Any, expected_to_address: str, chainid: str, token_contract: str, decimals: int, network_label: str, v2_url: str, api_key: str) -> dict:
    tolerance = get_network_tolerance(network_label)
    receipt_res = get_evm_tx_receipt(v2_url, api_key, chainid, txid)
    receipt = (receipt_res.get("data") or {}).get("result")
    if not receipt:
        return verify_result(False, "pending", f"{network_label} receipt not available yet", debug_meta(raw=receipt_res.get("text")))

    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "Transaction failed")

    expected_contract = normalize_evm_address(token_contract)
    expected_to = normalize_evm_address(expected_to_address)
    unit = Decimal(10) ** Decimal(decimals)

    for log in receipt.get("logs", []) or []:
        if normalize_evm_address(log.get("address")) != expected_contract:
            continue
        topics = log.get("topics", []) or []
        if len(topics) < 3:
            continue
        actual_to = "0x" + str(topics[2]).lower()[-40:]
        if normalize_evm_address(actual_to) != expected_to:
            continue
        try:
            value_raw = int(str(log.get("data", "0x0")), 16)
        except Exception:
            continue
        actual_amount = Decimal(value_raw) / unit
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "verified", debug_meta(network=network_label, txid=txid, actual_amount=str(actual_amount), contract=expected_contract))

    return verify_result(False, "rejected", "No matching token transfer found", debug_meta(expected_to=expected_to, expected_contract=expected_contract))


def verify_usdt_trc20_txid(txid: str, expected_amount: Any, expected_to_address: str, trongrid_base: str, api_key: str, usdt_trc20_contract: str) -> dict:
    tolerance = get_network_tolerance("USDT (TRC20)")
    info_res = http_post_json(f"{trongrid_base}/walletsolidity/gettransactioninfobyid", payload={"value": txid}, headers=trongrid_headers(api_key), timeout=20)
    if not info_res.get("ok"):
        return verify_result(False, "pending", "TRON tx info request failed", debug_meta(raw=info_res.get("text")))
    if not info_res.get("data"):
        return verify_result(False, "pending", "TRON transaction not confirmed yet")

    ev_res = http_get_json(f"{trongrid_base}/v1/transactions/{txid}/events", params={"only_confirmed": "true"}, headers=trongrid_headers(api_key), timeout=20)
    if not ev_res.get("ok"):
        return verify_result(False, "pending", "TRON event request failed", debug_meta(raw=ev_res.get("text")))

    expected_contract = normalize_text(usdt_trc20_contract).lower()
    for ev in (ev_res.get("data") or {}).get("data", []):
        if normalize_text(ev.get("event_name")).lower() != "transfer":
            continue
        if normalize_text(ev.get("contract_address")).lower() != expected_contract:
            continue
        result = ev.get("result", {}) or {}
        to_addr = result.get("to") or result.get("_to")
        value_raw = result.get("value") or result.get("_value")
        if not to_addr or value_raw in (None, ""):
            continue
        try:
            actual_amount = Decimal(int(str(value_raw))) / Decimal("1000000")
        except Exception:
            continue
        if normalize_text(to_addr) == normalize_text(expected_to_address) and amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "verified", debug_meta(network="USDT (TRC20)", txid=txid, actual_amount=str(actual_amount)))
    return verify_result(False, "rejected", "No matching USDT TRC20 transfer found")


def verify_trx_transfer(txid: str, expected_amount: Any, expected_to_address: str, trongrid_base: str, api_key: str) -> dict:
    tolerance = get_network_tolerance("TRX (TRC20)")
    tx_res = http_post_json(f"{trongrid_base}/wallet/gettransactionbyid", payload={"value": txid}, headers=trongrid_headers(api_key), timeout=20)
    if not tx_res.get("ok"):
        return verify_result(False, "pending", "TRX tx request failed", debug_meta(raw=tx_res.get("text")))
    tx_data = tx_res.get("data") or {}
    if not tx_data:
        return verify_result(False, "pending", "TRX transaction not found yet")
    contracts = (((tx_data.get("raw_data") or {}).get("contract")) or [])
    if not contracts:
        return verify_result(False, "rejected", "No TRX transfer contract found")
    param_value = (((contracts[0].get("parameter") or {}).get("value")) or {})
    to_address_hex = normalize_text(param_value.get("to_address"))
    if not to_address_hex:
        return verify_result(False, "rejected", "No TRX destination found")
    actual_to = tron_hex_to_base58(to_address_hex)
    actual_amount = Decimal(int(param_value.get("amount", 0))) / Decimal("1000000")
    if actual_to != expected_to_address:
        return verify_result(False, "rejected", "Destination address mismatch", debug_meta(actual_to=actual_to, expected_to=expected_to_address))
    if not amount_within_tolerance(actual_amount, expected_amount, tolerance):
        return verify_result(False, "rejected", "Amount mismatch", debug_meta(actual_amount=str(actual_amount), expected_amount=str(expected_amount)))
    return verify_result(True, "confirmed", "verified", debug_meta(network="TRX (TRC20)", txid=txid, actual_amount=str(actual_amount)))


def verify_btc_like_txid(txid: str, expected_amount: Any, expected_to_address: str, api_base: str, network_label: str) -> dict:
    tolerance = get_network_tolerance(network_label)
    tx_res = http_get_json(f"{api_base}/tx/{txid}", timeout=20)
    if not tx_res.get("ok"):
        return verify_result(False, "pending", f"{network_label} tx request failed", debug_meta(raw=tx_res.get("text")))
    tx = tx_res.get("data") or {}
    if not tx:
        return verify_result(False, "pending", f"{network_label} transaction not found yet")
    if not ((tx.get("status") or {}).get("confirmed")):
        return verify_result(False, "pending", f"{network_label} transaction not confirmed yet")
    for vout in tx.get("vout", []) or []:
        if normalize_text(vout.get("scriptpubkey_address")) != normalize_text(expected_to_address):
            continue
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "verified", debug_meta(network=network_label, txid=txid, actual_amount=str(actual_amount)))
    return verify_result(False, "rejected", f"No matching {network_label} output found")


def verify_sol_transfer(txid: str, expected_amount: Any, expected_to_address: str, helius_rpc_url: str) -> dict:
    tolerance = get_network_tolerance("SOL")
    res = helius_rpc(helius_rpc_url, "getTransaction", [txid, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}])
    if not res.get("ok"):
        return verify_result(False, "pending", "SOL tx request failed", debug_meta(raw=res.get("text")))
    tx = (res.get("data") or {}).get("result")
    if not tx:
        return verify_result(False, "pending", "SOL transaction not found yet")
    meta = tx.get("meta", {}) or {}
    if meta.get("err") is not None:
        return verify_result(False, "rejected", "SOL transaction failed")
    instructions = []
    message = ((tx.get("transaction") or {}).get("message")) or {}
    instructions.extend(message.get("instructions", []) or [])
    for inner in meta.get("innerInstructions", []) or []:
        instructions.extend(inner.get("instructions", []) or [])
    for ins in instructions:
        parsed = ins.get("parsed")
        if not parsed:
            continue
        info = parsed.get("info", {}) or {}
        if parsed.get("type") == "transfer" and normalize_text(info.get("destination")) == normalize_text(expected_to_address):
            actual_amount = Decimal(int(info.get("lamports"))) / Decimal("1000000000")
            if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                return verify_result(True, "confirmed", "verified", debug_meta(network="SOL", txid=txid, actual_amount=str(actual_amount)))
    return verify_result(False, "rejected", "No matching SOL transfer found")


# =========================
# Auto scan by address
# =========================
def auto_scan_evm_token_by_address(expected_amount: Any, expected_to_address: str, request_created_at: Any, chainid: str, token_contract: str, decimals: int, network_label: str, v2_url: str, api_key: str) -> dict:
    tolerance = get_network_tolerance(network_label)
    res = get_evm_token_transfers_to_address(v2_url, api_key, chainid, expected_to_address, token_contract, page=1, offset=100)
    if not res.get("ok"):
        return verify_result(False, "pending", f"{network_label} token scan failed", debug_meta(http=res.get("status_code"), raw=res.get("text")))
    rows = (res.get("data") or {}).get("result", [])
    if not isinstance(rows, list):
        return verify_result(False, "pending", f"{network_label} token scan returned invalid data", debug_meta(raw=str(res.get("data"))[:1000]))

    expected_to = normalize_evm_address(expected_to_address)
    expected_contract = normalize_evm_address(token_contract)
    unit = Decimal(10) ** Decimal(decimals)

    scanned_preview = []
    for row in rows[:10]:
        scanned_preview.append({
            "hash": row.get("hash"),
            "to": normalize_evm_address(row.get("to")),
            "contract": normalize_evm_address(row.get("contractAddress")),
            "value": row.get("value"),
            "timeStamp": row.get("timeStamp"),
        })

    for row in rows:
        tx_time = row.get("timeStamp")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        if normalize_evm_address(row.get("to")) != expected_to:
            continue
        if normalize_evm_address(row.get("contractAddress")) != expected_contract:
            continue
        try:
            value_raw = int(str(row.get("value", "0")))
        except Exception:
            continue
        actual_amount = Decimal(value_raw) / unit
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "matched by address scan", debug_meta(network=network_label, txid=row.get("hash"), actual_amount=str(actual_amount), expected_amount=str(expected_amount), tx_time=str(normalize_dt(tx_time)), scanned_preview=scanned_preview))

    return verify_result(False, "pending", f"No matching {network_label} payment found yet", debug_meta(expected_amount=str(expected_amount), expected_to=expected_to, expected_contract=expected_contract, scanned_preview=scanned_preview))


def auto_scan_evm_native_by_address(expected_amount: Any, expected_to_address: str, request_created_at: Any, chainid: str, network_label: str, v2_url: str, api_key: str) -> dict:
    tolerance = get_network_tolerance(network_label)
    res = get_evm_native_transactions_to_address(v2_url, api_key, chainid, expected_to_address, page=1, offset=100)
    if not res.get("ok"):
        return verify_result(False, "pending", f"{network_label} address scan failed", debug_meta(http=res.get("status_code"), raw=res.get("text")))
    rows = (res.get("data") or {}).get("result", [])
    if not isinstance(rows, list):
        return verify_result(False, "pending", f"{network_label} native scan returned invalid data", debug_meta(raw=str(res.get("data"))[:1000]))

    expected_to = normalize_evm_address(expected_to_address)
    for row in rows:
        tx_time = row.get("timeStamp")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        if normalize_evm_address(row.get("to")) != expected_to:
            continue
        if str(row.get("isError", "0")) not in {"0", ""}:
            continue
        try:
            value_raw = int(str(row.get("value", "0")))
        except Exception:
            continue
        actual_amount = Decimal(value_raw) / Decimal("1000000000000000000")
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "matched by address scan", debug_meta(network=network_label, txid=row.get("hash"), actual_amount=str(actual_amount), expected_amount=str(expected_amount), tx_time=str(normalize_dt(tx_time))))
    return verify_result(False, "pending", f"No matching {network_label} payment found yet", debug_meta(expected_amount=str(expected_amount), expected_to=expected_to))


def auto_scan_trx_by_address(expected_amount: Any, expected_to_address: str, request_created_at: Any, trongrid_base: str, api_key: str) -> dict:
    tolerance = get_network_tolerance("TRX (TRC20)")
    res = http_get_json(f"{trongrid_base}/v1/accounts/{expected_to_address}/transactions", params={"only_to": "true", "limit": 50, "order_by": "block_timestamp,desc"}, headers=trongrid_headers(api_key), timeout=20)
    if not res.get("ok"):
        return verify_result(False, "pending", "TRX address scan failed", debug_meta(raw=res.get("text")))
    rows = ((res.get("data") or {}).get("data")) or []
    for tx in rows:
        tx_time = tx.get("block_timestamp")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        contracts = (((tx.get("raw_data") or {}).get("contract")) or [])
        if not contracts:
            continue
        param_value = (((contracts[0].get("parameter") or {}).get("value")) or {})
        to_address_hex = normalize_text(param_value.get("to_address"))
        if not to_address_hex:
            continue
        actual_to = tron_hex_to_base58(to_address_hex)
        if actual_to != expected_to_address:
            continue
        actual_amount = Decimal(int(param_value.get("amount", 0))) / Decimal("1000000")
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "matched by address scan", debug_meta(network="TRX (TRC20)", txid=tx.get("txID"), actual_amount=str(actual_amount), tx_time=str(normalize_dt(tx_time))))
    return verify_result(False, "pending", "No matching TRX payment found yet")


def auto_scan_usdt_trc20_by_address(expected_amount: Any, expected_to_address: str, request_created_at: Any, trongrid_base: str, api_key: str, usdt_trc20_contract: str) -> dict:
    tolerance = get_network_tolerance("USDT (TRC20)")
    res = http_get_json(f"{trongrid_base}/v1/accounts/{expected_to_address}/transactions/trc20", params={"limit": 100, "only_confirmed": "true"}, headers=trongrid_headers(api_key), timeout=20)
    if not res.get("ok"):
        return verify_result(False, "pending", "USDT TRC20 address scan failed", debug_meta(raw=res.get("text")))
    rows = ((res.get("data") or {}).get("data")) or []
    expected_contract = normalize_text(usdt_trc20_contract).lower()
    for tx in rows:
        tx_time = tx.get("block_ts") or tx.get("block_timestamp") or tx.get("timestamp")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        token_info = tx.get("token_info", {}) or {}
        if normalize_text(token_info.get("address")).lower() != expected_contract:
            continue
        if normalize_text(tx.get("to")) != normalize_text(expected_to_address):
            continue
        value_raw = tx.get("value")
        if value_raw is None:
            continue
        token_decimals = int(str(token_info.get("decimals", "6")))
        actual_amount = Decimal(str(value_raw)) / (Decimal(10) ** Decimal(token_decimals))
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "matched by address scan", debug_meta(network="USDT (TRC20)", txid=tx.get("transaction_id"), actual_amount=str(actual_amount), tx_time=str(normalize_dt(tx_time))))
    return verify_result(False, "pending", "No matching USDT TRC20 payment found yet")


def auto_scan_btc_like_by_address(expected_amount: Any, expected_to_address: str, request_created_at: Any, api_base: str, network_label: str) -> dict:
    tolerance = get_network_tolerance(network_label)
    res = http_get_json(f"{api_base}/address/{expected_to_address}/txs", timeout=20)
    if not res.get("ok"):
        return verify_result(False, "pending", f"{network_label} address scan failed", debug_meta(raw=res.get("text")))
    rows = res.get("data")
    if not isinstance(rows, list) or not rows:
        return verify_result(False, "pending", f"No incoming {network_label} tx found yet")
    for tx in rows:
        tx_time = ((tx.get("status") or {}).get("block_time")) or tx.get("block_time")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        if not ((tx.get("status") or {}).get("confirmed")):
            continue
        for vout in tx.get("vout", []) or []:
            if normalize_text(vout.get("scriptpubkey_address")) != normalize_text(expected_to_address):
                continue
            actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
            if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                return verify_result(True, "confirmed", "matched by address scan", debug_meta(network=network_label, txid=tx.get("txid"), actual_amount=str(actual_amount), tx_time=str(normalize_dt(tx_time))))
    return verify_result(False, "pending", f"No matching {network_label} payment found yet")


def auto_scan_sol_by_address(expected_amount: Any, expected_to_address: str, request_created_at: Any, helius_rpc_url: str) -> dict:
    tolerance = get_network_tolerance("SOL")
    sig_res = get_sol_signatures_for_address(helius_rpc_url, expected_to_address, limit=50)
    if not sig_res.get("ok"):
        return verify_result(False, "pending", "SOL address scan failed", debug_meta(raw=sig_res.get("text")))
    signatures = ((sig_res.get("data") or {}).get("result")) or []
    if not signatures:
        return verify_result(False, "pending", "No incoming SOL signatures found yet")
    for sig in signatures:
        tx_time = sig.get("blockTime")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        txid = sig.get("signature")
        if not txid:
            continue
        res = helius_rpc(helius_rpc_url, "getTransaction", [txid, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}])
        tx = ((res.get("data") or {}).get("result")) or {}
        if not tx:
            continue
        meta = tx.get("meta", {}) or {}
        if meta.get("err") is not None:
            continue
        instructions = []
        message = ((tx.get("transaction") or {}).get("message")) or {}
        instructions.extend(message.get("instructions", []) or [])
        for inner in meta.get("innerInstructions", []) or []:
            instructions.extend(inner.get("instructions", []) or [])
        for ins in instructions:
            parsed = ins.get("parsed")
            if not parsed:
                continue
            info = parsed.get("info", {}) or {}
            if parsed.get("type") == "transfer" and normalize_text(info.get("destination")) == normalize_text(expected_to_address):
                actual_amount = Decimal(int(info.get("lamports"))) / Decimal("1000000000")
                if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                    return verify_result(True, "confirmed", "matched by address scan", debug_meta(network="SOL", txid=txid, actual_amount=str(actual_amount), tx_time=str(normalize_dt(tx_time))))
    return verify_result(False, "pending", "No matching SOL payment found yet")


# =========================
# Public routers
# =========================
def verify_crypto_payment(network: str, txid: str, expected_amount: Any, expected_to_address: str, config: Dict[str, Any]) -> dict:
    if network == "USDT (TRC20)":
        return verify_usdt_trc20_txid(txid, expected_amount, expected_to_address, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"], config["USDT_TRC20_CONTRACT"])
    if network == "TRX (TRC20)":
        return verify_trx_transfer(txid, expected_amount, expected_to_address, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"])
    if network == "ETH (ERC20)":
        return verify_evm_native_transfer(txid, expected_amount, expected_to_address, str(config["ETH_CHAIN_ID"]), "ETH (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BNB (BEP20)":
        return verify_evm_native_transfer(txid, expected_amount, expected_to_address, str(config["BSC_CHAIN_ID"]), "BNB (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (ERC20)":
        return verify_evm_token_transfer(txid, expected_amount, expected_to_address, str(config["ETH_CHAIN_ID"]), config["USDT_ERC20_CONTRACT"], 6, "USDT (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (BEP20)":
        return verify_evm_token_transfer(txid, expected_amount, expected_to_address, str(config["BSC_CHAIN_ID"]), config["USDT_BEP20_CONTRACT"], 18, "USDT (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BTC":
        return verify_btc_like_txid(txid, expected_amount, expected_to_address, config["BTC_API_BASE"], "BTC")
    if network == "LTC":
        return verify_btc_like_txid(txid, expected_amount, expected_to_address, config["LTC_API_BASE"], "LTC")
    if network == "SOL":
        return verify_sol_transfer(txid, expected_amount, expected_to_address, config["HELIUS_RPC_URL"])
    return verify_result(False, "rejected", f"Unsupported network: {network}")


def auto_verify_by_record(record: Dict[str, Any], config: Dict[str, Any]) -> dict:
    network = record["network"]
    expected_amount = record["crypto_amount"]
    expected_to_address = record["address"]
    request_created_at = record.get("created_at")

    if network == "USDT (TRC20)":
        return auto_scan_usdt_trc20_by_address(expected_amount, expected_to_address, request_created_at, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"], config["USDT_TRC20_CONTRACT"])
    if network == "TRX (TRC20)":
        return auto_scan_trx_by_address(expected_amount, expected_to_address, request_created_at, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"])
    if network == "ETH (ERC20)":
        return auto_scan_evm_native_by_address(expected_amount, expected_to_address, request_created_at, str(config["ETH_CHAIN_ID"]), "ETH (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BNB (BEP20)":
        return auto_scan_evm_native_by_address(expected_amount, expected_to_address, request_created_at, str(config["BSC_CHAIN_ID"]), "BNB (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (ERC20)":
        return auto_scan_evm_token_by_address(expected_amount, expected_to_address, request_created_at, str(config["ETH_CHAIN_ID"]), config["USDT_ERC20_CONTRACT"], 6, "USDT (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (BEP20)":
        return auto_scan_evm_token_by_address(expected_amount, expected_to_address, request_created_at, str(config["BSC_CHAIN_ID"]), config["USDT_BEP20_CONTRACT"], 18, "USDT (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BTC":
        return auto_scan_btc_like_by_address(expected_amount, expected_to_address, request_created_at, config["BTC_API_BASE"], "BTC")
    if network == "LTC":
        return auto_scan_btc_like_by_address(expected_amount, expected_to_address, request_created_at, config["LTC_API_BASE"], "LTC")
    if network == "SOL":
        return auto_scan_sol_by_address(expected_amount, expected_to_address, request_created_at, config["HELIUS_RPC_URL"])
    return verify_result(False, "rejected", f"Unsupported network: {network}", debug_meta(network=network))
