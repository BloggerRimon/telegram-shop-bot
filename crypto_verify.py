import hashlib
import requests
from decimal import Decimal, InvalidOperation


# =========================
# BASIC HELPERS
# =========================
def safe_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def verify_result(ok: bool, status: str, reason: str, meta=None):
    return {
        "ok": ok,
        "status": status,
        "reason": reason,
        "meta": meta or {},
    }


def amount_within_tolerance(actual_amount, expected_amount, tolerance=0.10):
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)

    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False

    return abs(actual_dec - expected_dec) <= tolerance_dec


def is_valid_txid_format(txid: str) -> bool:
    txid = txid.strip()

    if len(txid) < 20:
        return False

    if txid.startswith("0x") and len(txid) == 66:
        return all(ch in "0123456789abcdefABCDEF" for ch in txid[2:])

    if len(txid) == 64 and all(ch in "0123456789abcdefABCDEF" for ch in txid):
        return True

    base58_allowed = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    if all(ch in base58_allowed for ch in txid):
        return True

    return False


# =========================
# REQUEST HELPERS
# =========================
def http_get_json(url: str, params=None, headers=None, timeout=25):
    try:
        res = requests.get(url, params=params, headers=headers, timeout=timeout)
        return {
            "ok": res.ok,
            "status_code": res.status_code,
            "data": res.json() if res.content else {},
        }
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


def http_post_json(url: str, payload=None, headers=None, timeout=25):
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        return {
            "ok": res.ok,
            "status_code": res.status_code,
            "data": res.json() if res.content else {},
        }
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}}


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


def trongrid_headers(trongrid_api_key: str):
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "TRON-PRO-API-KEY": trongrid_api_key,
    }


# =========================
# EVM HELPERS
# =========================
def normalize_evm_address(addr: str) -> str:
    return str(addr or "").strip().lower()


def to_evm_topic_address(addr: str) -> str:
    return "0x" + normalize_evm_address(addr).replace("0x", "").rjust(64, "0")


def get_evm_tx_by_hash(etherscan_v2_url: str, api_key: str, chainid: str, txid: str):
    return http_get_json(
        etherscan_v2_url,
        params={
            "chainid": chainid,
            "module": "proxy",
            "action": "eth_getTransactionByHash",
            "txhash": txid,
            "apikey": api_key,
        },
        timeout=20,
    )


def get_evm_tx_receipt(etherscan_v2_url: str, api_key: str, chainid: str, txid: str):
    return http_get_json(
        etherscan_v2_url,
        params={
            "chainid": chainid,
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": txid,
            "apikey": api_key,
        },
        timeout=20,
    )


def get_evm_token_transfers_to_address(
    etherscan_v2_url: str,
    api_key: str,
    chainid: str,
    address: str,
    contract_address: str,
    page: int = 1,
    offset: int = 20,
):
    return http_get_json(
        etherscan_v2_url,
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


def get_evm_native_transactions_to_address(
    etherscan_v2_url: str,
    api_key: str,
    chainid: str,
    address: str,
    page: int = 1,
    offset: int = 20,
):
    return http_get_json(
        etherscan_v2_url,
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
# SOLANA HELPERS
# =========================
def helius_rpc(helius_rpc_url: str, method: str, params):
    payload = {"jsonrpc": "2.0", "id": "1", "method": method, "params": params}
    return http_post_json(
        helius_rpc_url,
        payload=payload,
        headers={"content-type": "application/json"},
        timeout=25,
    )


def get_sol_signatures_for_address(helius_rpc_url: str, address: str, limit: int = 20):
    return helius_rpc(
        helius_rpc_url,
        "getSignaturesForAddress",
        [address, {"limit": limit}],
    )


# =========================
# DIRECT TXID VERIFICATION
# =========================
def verify_usdt_trc20_txid(
    txid: str,
    expected_amount,
    expected_to_address: str,
    trongrid_base: str,
    trongrid_api_key: str,
    usdt_trc20_contract: str,
):
    info_res = http_post_json(
        f"{trongrid_base}/walletsolidity/gettransactioninfobyid",
        payload={"value": txid},
        headers=trongrid_headers(trongrid_api_key),
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
        f"{trongrid_base}/v1/transactions/{txid}/events",
        params={"only_confirmed": "true"},
        headers=trongrid_headers(trongrid_api_key),
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
        if contract_address != usdt_trc20_contract:
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
            return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "rejected", "no matching USDT TRC20 transfer found")


def verify_trx_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    trongrid_base: str,
    trongrid_api_key: str,
):
    tx_res = http_post_json(
        f"{trongrid_base}/wallet/gettransactionbyid",
        payload={"value": txid},
        headers=trongrid_headers(trongrid_api_key),
        timeout=20,
    )
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"tron tx http {tx_res['status_code']}")

    tx_data = tx_res["data"]
    if not tx_data:
        return verify_result(False, "pending", "transaction not found yet")

    info_res = http_post_json(
        f"{trongrid_base}/walletsolidity/gettransactioninfobyid",
        payload={"value": txid},
        headers=trongrid_headers(trongrid_api_key),
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

    return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})


def verify_evm_native_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    chainid: str,
    symbol: str,
    etherscan_v2_url: str,
    api_key: str,
):
    tx_res = get_evm_tx_by_hash(etherscan_v2_url, api_key, chainid, txid)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"{symbol} tx http {tx_res['status_code']}")

    tx_data = tx_res["data"].get("result")
    if not tx_data:
        return verify_result(False, "pending", "transaction not found yet")

    receipt_res = get_evm_tx_receipt(etherscan_v2_url, api_key, chainid, txid)
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

    return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})


def verify_evm_token_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    chainid: str,
    token_contract: str,
    decimals: int,
    symbol: str,
    etherscan_v2_url: str,
    api_key: str,
    erc20_transfer_topic: str,
):
    receipt_res = get_evm_tx_receipt(etherscan_v2_url, api_key, chainid, txid)
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

        if str(topics[0]).lower() != erc20_transfer_topic:
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
            return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "rejected", "no matching token transfer found")


def verify_btc_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    btc_api_base: str,
):
    tx_res = http_get_json(f"{btc_api_base}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"btc tx http {tx_res['status_code']}")

    tx = tx_res["data"]
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")

    status = tx.get("status", {}) or {}
    if not status.get("confirmed"):
        return verify_result(False, "pending", "transaction not confirmed yet")

    for vout in tx.get("vout", []) or []:
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if (
            vout.get("scriptpubkey_address") == expected_to_address
            and amount_within_tolerance(actual_amount, expected_amount, 0.10)
        ):
            return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "rejected", "no matching BTC output found")


def verify_ltc_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    ltc_api_base: str,
):
    tx_res = http_get_json(f"{ltc_api_base}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"ltc tx http {tx_res['status_code']}")

    tx = tx_res["data"]
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")

    status = tx.get("status", {}) or {}
    if not status.get("confirmed"):
        return verify_result(False, "pending", "transaction not confirmed yet")

    for vout in tx.get("vout", []) or []:
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if (
            vout.get("scriptpubkey_address") == expected_to_address
            and amount_within_tolerance(actual_amount, expected_amount, 0.10)
        ):
            return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "rejected", "no matching LTC output found")


def verify_sol_transfer(
    txid: str,
    expected_amount,
    expected_to_address: str,
    helius_rpc_url: str,
):
    res = helius_rpc(
        helius_rpc_url,
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
                return verify_result(True, "confirmed", "verified", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "rejected", "no matching SOL transfer found")


# =========================
# ADDRESS-BASED AUTO SCAN
# =========================
def auto_scan_trx_by_address(
    expected_amount,
    expected_to_address: str,
    trongrid_base: str,
    trongrid_api_key: str,
):
    res = http_get_json(
        f"{trongrid_base}/v1/accounts/{expected_to_address}/transactions",
        params={"only_to": "true", "limit": 20, "order_by": "block_timestamp,desc"},
        headers=trongrid_headers(trongrid_api_key),
        timeout=20,
    )
    if not res["ok"]:
        return verify_result(False, "pending", f"tron account tx http {res['status_code']}")

    txs = res["data"].get("data", [])
    if not txs:
        return verify_result(False, "pending", "no incoming TRX transactions found yet")

    for tx in txs:
        ret_list = tx.get("ret", []) or []
        if ret_list and str(ret_list[0].get("contractRet", "")).upper() not in {"SUCCESS", ""}:
            continue

        contracts = (((tx.get("raw_data") or {}).get("contract")) or [])
        if not contracts:
            continue

        contract = contracts[0] or {}
        param_value = (((contract.get("parameter") or {}).get("value")) or {})
        amount_sun = int(param_value.get("amount", 0))
        to_address_hex = str(param_value.get("to_address", "")).strip()
        if not to_address_hex:
            continue

        actual_to = tron_hex_to_base58(to_address_hex)
        if actual_to != expected_to_address:
            continue

        actual_amount = Decimal(amount_sun) / Decimal("1000000")
        if amount_within_tolerance(actual_amount, expected_amount, 0.10):
            txid = tx.get("txID")
            return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", "no matching TRX transfer found yet")


def auto_scan_usdt_trc20_by_address(
    expected_amount,
    expected_to_address: str,
    trongrid_base: str,
    trongrid_api_key: str,
    usdt_trc20_contract: str,
):
    res = http_get_json(
        f"{trongrid_base}/v1/accounts/{expected_to_address}/transactions/trc20",
        params={"limit": 20, "only_confirmed": "true"},
        headers=trongrid_headers(trongrid_api_key),
        timeout=20,
    )
    if not res["ok"]:
        return verify_result(False, "pending", f"tron trc20 scan http {res['status_code']}")

    txs = res["data"].get("data", [])
    if not txs:
        return verify_result(False, "pending", "no incoming TRC20 transactions found yet")

    expected_contract = str(usdt_trc20_contract).strip().lower()

    for tx in txs:
        token_info = tx.get("token_info", {}) or {}
        token_address = str(token_info.get("address", "")).strip().lower()
        if token_address != expected_contract:
            continue

        to_addr = tx.get("to")
        if str(to_addr).strip() != str(expected_to_address).strip():
            continue

        value_raw = tx.get("value")
        if value_raw is None:
            continue

        token_decimal = int(str(token_info.get("decimals", "6")))
        actual_amount = Decimal(str(value_raw)) / (Decimal(10) ** Decimal(token_decimal))
        if amount_within_tolerance(actual_amount, expected_amount, 0.10):
            txid = tx.get("transaction_id")
            return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", "no matching USDT TRC20 transfer found yet")


def auto_scan_evm_native_by_address(
    expected_amount,
    expected_to_address: str,
    chainid: str,
    symbol: str,
    etherscan_v2_url: str,
    api_key: str,
):
    res = get_evm_native_transactions_to_address(
        etherscan_v2_url,
        api_key,
        chainid,
        expected_to_address,
        page=1,
        offset=20,
    )
    if not res["ok"]:
        return verify_result(False, "pending", f"{symbol} address scan http {res['status_code']}")

    data = res["data"]
    rows = data.get("result", [])
    if not isinstance(rows, list):
        return verify_result(False, "pending", f"{symbol} no incoming tx list yet")

    expected_to = normalize_evm_address(expected_to_address)

    for row in rows:
        if normalize_evm_address(row.get("to")) != expected_to:
            continue
        if str(row.get("isError", "0")) not in {"0", ""}:
            continue

        try:
            value_wei = int(str(row.get("value", "0")))
        except Exception:
            continue

        actual_amount = Decimal(value_wei) / Decimal("1000000000000000000")
        if amount_within_tolerance(actual_amount, expected_amount, 0.10):
            txid = row.get("hash")
            return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", f"no matching {symbol} transfer found yet")


def auto_scan_evm_token_by_address(
    expected_amount,
    expected_to_address: str,
    chainid: str,
    token_contract: str,
    decimals: int,
    symbol: str,
    etherscan_v2_url: str,
    api_key: str,
):
    res = get_evm_token_transfers_to_address(
        etherscan_v2_url,
        api_key,
        chainid,
        expected_to_address,
        token_contract,
        page=1,
        offset=20,
    )
    if not res["ok"]:
        return verify_result(False, "pending", f"{symbol} token scan http {res['status_code']}")

    data = res["data"]
    rows = data.get("result", [])
    if not isinstance(rows, list):
        return verify_result(False, "pending", f"{symbol} no token transfer list yet")

    expected_to = normalize_evm_address(expected_to_address)
    expected_contract = normalize_evm_address(token_contract)
    unit = Decimal(10) ** Decimal(decimals)

    for row in rows:
        if normalize_evm_address(row.get("to")) != expected_to:
            continue
        if normalize_evm_address(row.get("contractAddress")) != expected_contract:
            continue

        try:
            value_raw = int(str(row.get("value", "0")))
        except Exception:
            continue

        actual_amount = Decimal(value_raw) / unit
        if amount_within_tolerance(actual_amount, expected_amount, 0.10):
            txid = row.get("hash")
            return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", f"no matching {symbol} token transfer found yet")


def auto_scan_btc_by_address(
    expected_amount,
    expected_to_address: str,
    btc_api_base: str,
):
    res = http_get_json(f"{btc_api_base}/address/{expected_to_address}/txs", timeout=20)
    if not res["ok"]:
        return verify_result(False, "pending", f"btc address scan http {res['status_code']}")

    rows = res["data"]
    if not isinstance(rows, list) or not rows:
        return verify_result(False, "pending", "no incoming BTC transactions found yet")

    for tx in rows:
        status = tx.get("status", {}) or {}
        if not status.get("confirmed"):
            continue

        for vout in tx.get("vout", []) or []:
            if vout.get("scriptpubkey_address") != expected_to_address:
                continue

            actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
            if amount_within_tolerance(actual_amount, expected_amount, 0.10):
                txid = tx.get("txid")
                return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", "no matching BTC output found yet")


def auto_scan_ltc_by_address(
    expected_amount,
    expected_to_address: str,
    ltc_api_base: str,
):
    res = http_get_json(f"{ltc_api_base}/address/{expected_to_address}/txs", timeout=20)
    if not res["ok"]:
        return verify_result(False, "pending", f"ltc address scan http {res['status_code']}")

    rows = res["data"]
    if not isinstance(rows, list) or not rows:
        return verify_result(False, "pending", "no incoming LTC transactions found yet")

    for tx in rows:
        status = tx.get("status", {}) or {}
        if not status.get("confirmed"):
            continue

        for vout in tx.get("vout", []) or []:
            if vout.get("scriptpubkey_address") != expected_to_address:
                continue

            actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
            if amount_within_tolerance(actual_amount, expected_amount, 0.10):
                txid = tx.get("txid")
                return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", "no matching LTC output found yet")


def auto_scan_sol_by_address(
    expected_amount,
    expected_to_address: str,
    helius_rpc_url: str,
):
    sig_res = get_sol_signatures_for_address(helius_rpc_url, expected_to_address, limit=20)
    if not sig_res["ok"]:
        return verify_result(False, "pending", f"sol address scan http {sig_res['status_code']}")

    signatures = sig_res["data"].get("result", [])
    if not signatures:
        return verify_result(False, "pending", "no incoming SOL signatures found yet")

    for sig_item in signatures:
        txid = sig_item.get("signature")
        if not txid:
            continue

        res = helius_rpc(
            helius_rpc_url,
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
            continue

        tx = res["data"].get("result")
        if not tx:
            continue

        meta = tx.get("meta", {}) or {}
        if meta.get("err") is not None:
            continue

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
                if destination != expected_to_address:
                    continue

                actual_amount = Decimal(int(lamports)) / Decimal("1000000000")
                if amount_within_tolerance(actual_amount, expected_amount, 0.10):
                    return verify_result(True, "confirmed", "matched by address scan", {"actual_amount": str(actual_amount), "txid": txid})

    return verify_result(False, "pending", "no matching SOL transfer found yet")


# =========================
# MAIN TXID ROUTER
# =========================
def verify_crypto_payment(
    network: str,
    txid: str,
    expected_amount,
    expected_to_address: str,
    config: dict,
):
    if network == "USDT (TRC20)":
        return verify_usdt_trc20_txid(
            txid,
            expected_amount,
            expected_to_address,
            config["TRONGRID_BASE"],
            config["TRONGRID_API_KEY"],
            config["USDT_TRC20_CONTRACT"],
        )

    if network == "TRX (TRC20)":
        return verify_trx_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["TRONGRID_BASE"],
            config["TRONGRID_API_KEY"],
        )

    if network == "ETH (ERC20)":
        return verify_evm_native_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["ETH_CHAIN_ID"],
            "ETH",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
        )

    if network == "BNB (BEP20)":
        return verify_evm_native_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["BSC_CHAIN_ID"],
            "BNB",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
        )

    if network == "USDT (ERC20)":
        return verify_evm_token_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["ETH_CHAIN_ID"],
            config["USDT_ERC20_CONTRACT"],
            6,
            "USDT ERC20",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
            config["ERC20_TRANSFER_TOPIC"],
        )

    if network == "USDT (BEP20)":
        return verify_evm_token_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["BSC_CHAIN_ID"],
            config["USDT_BEP20_CONTRACT"],
            18,
            "USDT BEP20",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
            config["ERC20_TRANSFER_TOPIC"],
        )

    if network == "BTC":
        return verify_btc_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["BTC_API_BASE"],
        )

    if network == "LTC":
        return verify_ltc_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["LTC_API_BASE"],
        )

    if network == "SOL":
        return verify_sol_transfer(
            txid,
            expected_amount,
            expected_to_address,
            config["HELIUS_RPC_URL"],
        )

    return verify_result(False, "rejected", f"unsupported network: {network}")


# =========================
# MAIN AUTO-SCAN ROUTER
# =========================
def auto_verify_by_record(record: dict, config: dict):
    network = record["network"]
    expected_amount = record["crypto_amount"]
    expected_to_address = record["address"]

    if network == "USDT (TRC20)":
        return auto_scan_usdt_trc20_by_address(
            expected_amount,
            expected_to_address,
            config["TRONGRID_BASE"],
            config["TRONGRID_API_KEY"],
            config["USDT_TRC20_CONTRACT"],
        )

    if network == "TRX (TRC20)":
        return auto_scan_trx_by_address(
            expected_amount,
            expected_to_address,
            config["TRONGRID_BASE"],
            config["TRONGRID_API_KEY"],
        )

    if network == "ETH (ERC20)":
        return auto_scan_evm_native_by_address(
            expected_amount,
            expected_to_address,
            config["ETH_CHAIN_ID"],
            "ETH",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
        )

    if network == "BNB (BEP20)":
        return auto_scan_evm_native_by_address(
            expected_amount,
            expected_to_address,
            config["BSC_CHAIN_ID"],
            "BNB",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
        )

    if network == "USDT (ERC20)":
        return auto_scan_evm_token_by_address(
            expected_amount,
            expected_to_address,
            config["ETH_CHAIN_ID"],
            config["USDT_ERC20_CONTRACT"],
            6,
            "USDT ERC20",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
        )

    if network == "USDT (BEP20)":
        return auto_scan_evm_token_by_address(
            expected_amount,
            expected_to_address,
            config["BSC_CHAIN_ID"],
            config["USDT_BEP20_CONTRACT"],
            18,
            "USDT BEP20",
            config["ETHERSCAN_V2_URL"],
            config["ETHERSCAN_API_KEY"],
        )

    if network == "BTC":
        return auto_scan_btc_by_address(
            expected_amount,
            expected_to_address,
            config["BTC_API_BASE"],
        )

    if network == "LTC":
        return auto_scan_ltc_by_address(
            expected_amount,
            expected_to_address,
            config["LTC_API_BASE"],
        )

    if network == "SOL":
        return auto_scan_sol_by_address(
            expected_amount,
            expected_to_address,
            config["HELIUS_RPC_URL"],
        )

    return verify_result(False, "rejected", f"unsupported network: {network}")
