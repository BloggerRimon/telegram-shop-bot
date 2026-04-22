
import hashlib
import requests
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta

def safe_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None

def verify_result(ok: bool, status: str, reason: str, meta=None):
    return {"ok": ok, "status": status, "reason": reason, "meta": meta or {}}

def normalize_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
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
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        ts = float(text)
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None

def tx_is_within_request_window(tx_time, request_created_at, backward_minutes: int = 15, forward_hours: int = 24) -> bool:
    tx_dt = normalize_dt(tx_time)
    req_dt = normalize_dt(request_created_at)
    if tx_dt is None or req_dt is None:
        return False
    return (req_dt - timedelta(minutes=backward_minutes)) <= tx_dt <= (req_dt + timedelta(hours=forward_hours))

def get_network_tolerance(network: str):
    mapping = {
        "USDT (TRC20)": Decimal("0.03"),
        "USDT (ERC20)": Decimal("0.03"),
        "USDT (BEP20)": Decimal("0.03"),
        "TRX (TRC20)": Decimal("0.20"),
        "ETH (ERC20)": Decimal("0.0002"),
        "BNB (BEP20)": Decimal("0.0002"),
        "BTC": Decimal("0.00001000"),
        "LTC": Decimal("0.00010000"),
        "SOL": Decimal("0.0005"),
    }
    return mapping.get(network, Decimal("0.10"))

def amount_within_tolerance(actual_amount, expected_amount, tolerance):
    actual_dec = safe_decimal(actual_amount)
    expected_dec = safe_decimal(expected_amount)
    tolerance_dec = safe_decimal(tolerance)
    if actual_dec is None or expected_dec is None or tolerance_dec is None:
        return False
    return abs(actual_dec - expected_dec) <= tolerance_dec

def http_get_json(url: str, params=None, headers=None, timeout=25):
    try:
        res = requests.get(url, params=params, headers=headers, timeout=timeout)
        return {"ok": res.ok, "status_code": res.status_code, "data": res.json() if res.content else {}, "text": getattr(res, "text", "")[:500]}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}, "text": str(e)}

def http_post_json(url: str, payload=None, headers=None, timeout=25):
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        return {"ok": res.ok, "status_code": res.status_code, "data": res.json() if res.content else {}, "text": getattr(res, "text", "")[:500]}
    except Exception as e:
        return {"ok": False, "status_code": 0, "data": {"error": str(e)}, "text": str(e)}

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
    return {"accept": "application/json", "content-type": "application/json", "TRON-PRO-API-KEY": trongrid_api_key}

def normalize_evm_address(addr: str) -> str:
    return str(addr or "").strip().lower()

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

def get_evm_tx_by_hash(etherscan_v2_url: str, api_key: str, chainid: str, txid: str):
    return http_get_json(normalize_etherscan_v2_url(etherscan_v2_url), params={"chainid": chainid, "module": "proxy", "action": "eth_getTransactionByHash", "txhash": txid, "apikey": api_key}, timeout=20)

def get_evm_tx_receipt(etherscan_v2_url: str, api_key: str, chainid: str, txid: str):
    return http_get_json(normalize_etherscan_v2_url(etherscan_v2_url), params={"chainid": chainid, "module": "proxy", "action": "eth_getTransactionReceipt", "txhash": txid, "apikey": api_key}, timeout=20)

def get_evm_token_transfers_to_address(etherscan_v2_url: str, api_key: str, chainid: str, address: str, contract_address: str, page: int = 1, offset: int = 100):
    return http_get_json(normalize_etherscan_v2_url(etherscan_v2_url), params={"chainid": chainid, "module": "account", "action": "tokentx", "address": address, "contractaddress": contract_address, "page": page, "offset": offset, "sort": "desc", "apikey": api_key}, timeout=20)

def get_evm_native_transactions_to_address(etherscan_v2_url: str, api_key: str, chainid: str, address: str, page: int = 1, offset: int = 50):
    return http_get_json(normalize_etherscan_v2_url(etherscan_v2_url), params={"chainid": chainid, "module": "account", "action": "txlist", "address": address, "page": page, "offset": offset, "sort": "desc", "apikey": api_key}, timeout=20)

def helius_rpc(helius_rpc_url: str, method: str, params):
    payload = {"jsonrpc": "2.0", "id": "1", "method": method, "params": params}
    return http_post_json(helius_rpc_url, payload=payload, headers={"content-type": "application/json"}, timeout=25)

def get_sol_signatures_for_address(helius_rpc_url: str, address: str, limit: int = 50):
    return helius_rpc(helius_rpc_url, "getSignaturesForAddress", [address, {"limit": limit}])

def verify_evm_token_transfer(txid, expected_amount, expected_to_address, chainid, token_contract, decimals, network_label, etherscan_v2_url, api_key):
    tolerance = get_network_tolerance(network_label)
    receipt_res = get_evm_tx_receipt(etherscan_v2_url, api_key, chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{network_label} receipt request failed", {"http": receipt_res["status_code"], "raw": receipt_res.get("text", "")})
    receipt = receipt_res["data"].get("result")
    if not receipt:
        return verify_result(False, "pending", "transaction not confirmed yet", {"raw": str(receipt_res["data"])[:300]})
    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "transaction failed")
    expected_contract = normalize_evm_address(token_contract)
    expected_to = normalize_evm_address(expected_to_address)
    unit = Decimal(10) ** Decimal(decimals)
    for log in receipt.get("logs", []) or []:
        log_contract = normalize_evm_address(log.get("address"))
        if log_contract != expected_contract:
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
            return verify_result(True, "confirmed", "verified", {"network": network_label, "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "to_address": expected_to, "contract": expected_contract})
    return verify_result(False, "rejected", "No matching token transfer found", {"network": network_label, "expected_amount": str(expected_amount), "expected_to": expected_to, "expected_contract": expected_contract})

def verify_evm_native_transfer(txid, expected_amount, expected_to_address, chainid, network_label, etherscan_v2_url, api_key):
    tolerance = get_network_tolerance(network_label)
    tx_res = get_evm_tx_by_hash(etherscan_v2_url, api_key, chainid, txid)
    if not tx_res["ok"]:
        return verify_result(False, "pending", f"{network_label} tx request failed", {"http": tx_res["status_code"], "raw": tx_res.get("text", "")})
    tx_data = tx_res["data"].get("result")
    if not tx_data:
        return verify_result(False, "pending", "transaction not found yet", {"raw": str(tx_res["data"])[:300]})
    receipt_res = get_evm_tx_receipt(etherscan_v2_url, api_key, chainid, txid)
    if not receipt_res["ok"]:
        return verify_result(False, "pending", f"{network_label} receipt request failed", {"http": receipt_res["status_code"], "raw": receipt_res.get("text", "")})
    receipt = receipt_res["data"].get("result")
    if not receipt:
        return verify_result(False, "pending", "transaction not confirmed yet", {"raw": str(receipt_res["data"])[:300]})
    if str(receipt.get("status", "")).lower() not in {"0x1", "1"}:
        return verify_result(False, "rejected", "transaction failed")
    actual_to = normalize_evm_address(tx_data.get("to"))
    expected_to = normalize_evm_address(expected_to_address)
    if actual_to != expected_to:
        return verify_result(False, "rejected", "Destination address mismatch", {"actual_to": actual_to, "expected_to": expected_to})
    try:
        value_wei = int(str(tx_data.get("value", "0")), 16)
    except Exception:
        return verify_result(False, "rejected", "Invalid value in tx")
    actual_amount = Decimal(value_wei) / Decimal("1000000000000000000")
    if not amount_within_tolerance(actual_amount, expected_amount, tolerance):
        return verify_result(False, "rejected", "Amount mismatch", {"network": network_label, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})
    return verify_result(True, "confirmed", "verified", {"network": network_label, "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "to_address": actual_to})

def verify_usdt_trc20_txid(txid, expected_amount, expected_to_address, trongrid_base, trongrid_api_key, usdt_trc20_contract):
    tolerance = get_network_tolerance("USDT (TRC20)")
    info_res = http_post_json(f"{trongrid_base}/walletsolidity/gettransactioninfobyid", payload={"value": txid}, headers=trongrid_headers(trongrid_api_key), timeout=20)
    if not info_res["ok"]:
        return verify_result(False, "pending", "TRON info request failed", {"http": info_res["status_code"]})
    if not info_res["data"]:
        return verify_result(False, "pending", "transaction not confirmed yet")
    ev_res = http_get_json(f"{trongrid_base}/v1/transactions/{txid}/events", params={"only_confirmed": "true"}, headers=trongrid_headers(trongrid_api_key), timeout=20)
    if not ev_res["ok"]:
        return verify_result(False, "pending", "TRON event request failed", {"http": ev_res["status_code"]})
    for ev in ev_res["data"].get("data", []):
        if str(ev.get("event_name", "")).lower() != "transfer":
            continue
        if str(ev.get("contract_address", "")).strip().lower() != str(usdt_trc20_contract).strip().lower():
            continue
        result = ev.get("result", {}) or {}
        to_addr = result.get("to", "") or result.get("_to", "")
        value_raw = result.get("value", "") or result.get("_value", "")
        if not to_addr or value_raw == "":
            continue
        try:
            actual_amount = Decimal(int(str(value_raw))) / Decimal("1000000")
        except Exception:
            continue
        if str(to_addr).strip() == str(expected_to_address).strip() and amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "verified", {"network": "USDT (TRC20)", "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})
    return verify_result(False, "rejected", "No matching USDT TRC20 transfer found")

def verify_trx_transfer(txid, expected_amount, expected_to_address, trongrid_base, trongrid_api_key):
    tolerance = get_network_tolerance("TRX (TRC20)")
    tx_res = http_post_json(f"{trongrid_base}/wallet/gettransactionbyid", payload={"value": txid}, headers=trongrid_headers(trongrid_api_key), timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", "TRX tx request failed", {"http": tx_res["status_code"]})
    tx_data = tx_res["data"]
    if not tx_data:
        return verify_result(False, "pending", "transaction not found yet")
    contracts = (((tx_data.get("raw_data") or {}).get("contract")) or [])
    if not contracts:
        return verify_result(False, "rejected", "No TRX transfer contract found")
    param_value = (((contracts[0].get("parameter") or {}).get("value")) or {})
    to_address_hex = str(param_value.get("to_address", "")).strip()
    if not to_address_hex:
        return verify_result(False, "rejected", "No destination found")
    actual_to = tron_hex_to_base58(to_address_hex)
    actual_amount = Decimal(int(param_value.get("amount", 0))) / Decimal("1000000")
    if actual_to != expected_to_address:
        return verify_result(False, "rejected", "Destination address mismatch", {"actual_to": actual_to})
    if not amount_within_tolerance(actual_amount, expected_amount, tolerance):
        return verify_result(False, "rejected", "Amount mismatch", {"actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})
    return verify_result(True, "confirmed", "verified", {"network": "TRX (TRC20)", "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})

def verify_btc_transfer(txid, expected_amount, expected_to_address, api_base):
    tolerance = get_network_tolerance("BTC")
    tx_res = http_get_json(f"{api_base}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", "BTC tx request failed", {"http": tx_res["status_code"]})
    tx = tx_res["data"]
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")
    if not (tx.get("status", {}) or {}).get("confirmed"):
        return verify_result(False, "pending", "transaction not confirmed yet")
    for vout in tx.get("vout", []) or []:
        if vout.get("scriptpubkey_address") != expected_to_address:
            continue
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "verified", {"network": "BTC", "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})
    return verify_result(False, "rejected", "No matching BTC output found")

def verify_ltc_transfer(txid, expected_amount, expected_to_address, api_base):
    tolerance = get_network_tolerance("LTC")
    tx_res = http_get_json(f"{api_base}/tx/{txid}", timeout=20)
    if not tx_res["ok"]:
        return verify_result(False, "pending", "LTC tx request failed", {"http": tx_res["status_code"]})
    tx = tx_res["data"]
    if not tx:
        return verify_result(False, "pending", "transaction not found yet")
    if not (tx.get("status", {}) or {}).get("confirmed"):
        return verify_result(False, "pending", "transaction not confirmed yet")
    for vout in tx.get("vout", []) or []:
        if vout.get("scriptpubkey_address") != expected_to_address:
            continue
        actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "verified", {"network": "LTC", "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})
    return verify_result(False, "rejected", "No matching LTC output found")

def verify_sol_transfer(txid, expected_amount, expected_to_address, helius_rpc_url):
    tolerance = get_network_tolerance("SOL")
    res = helius_rpc(helius_rpc_url, "getTransaction", [txid, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}])
    if not res["ok"]:
        return verify_result(False, "pending", "SOL tx request failed", {"http": res["status_code"]})
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
        if parsed.get("type") == "transfer" and info.get("destination") == expected_to_address:
            actual_amount = Decimal(int(info.get("lamports"))) / Decimal("1000000000")
            if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                return verify_result(True, "confirmed", "verified", {"network": "SOL", "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount)})
    return verify_result(False, "rejected", "No matching SOL transfer found")

def auto_scan_evm_token_by_address(expected_amount, expected_to_address, request_created_at, chainid, token_contract, decimals, network_label, etherscan_v2_url, api_key):
    tolerance = get_network_tolerance(network_label)
    res = get_evm_token_transfers_to_address(etherscan_v2_url, api_key, chainid, expected_to_address, token_contract, page=1, offset=100)
    if not res["ok"]:
        return verify_result(False, "pending", f"{network_label} token scan failed", {"http": res["status_code"], "raw": res.get("text", "")})
    rows = res["data"].get("result", [])
    if not isinstance(rows, list):
        return verify_result(False, "pending", f"{network_label} no token list yet", {"raw": str(res["data"])[:400]})
    expected_to = normalize_evm_address(expected_to_address)
    expected_contract = normalize_evm_address(token_contract)
    unit = Decimal(10) ** Decimal(decimals)
    for row in rows:
        if not tx_is_within_request_window(row.get("timeStamp"), request_created_at):
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
            return verify_result(True, "confirmed", "matched by address scan", {"network": network_label, "txid": row.get("hash"), "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(row.get("timeStamp"))), "contract": expected_contract, "from_address": normalize_evm_address(row.get("from"))})
    return verify_result(False, "pending", f"No matching {network_label} payment found yet", {"expected_amount": str(expected_amount), "expected_to": expected_to, "expected_contract": expected_contract, "hint": "Check API key, chainid, contract, and amount"})

def auto_scan_evm_native_by_address(expected_amount, expected_to_address, request_created_at, chainid, network_label, etherscan_v2_url, api_key):
    tolerance = get_network_tolerance(network_label)
    res = get_evm_native_transactions_to_address(etherscan_v2_url, api_key, chainid, expected_to_address, page=1, offset=50)
    if not res["ok"]:
        return verify_result(False, "pending", f"{network_label} address scan failed", {"http": res["status_code"], "raw": res.get("text", "")})
    rows = res["data"].get("result", [])
    if not isinstance(rows, list):
        return verify_result(False, "pending", f"{network_label} no tx list yet", {"raw": str(res["data"])[:300]})
    expected_to = normalize_evm_address(expected_to_address)
    for row in rows:
        if not tx_is_within_request_window(row.get("timeStamp"), request_created_at):
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
            return verify_result(True, "confirmed", "matched by address scan", {"network": network_label, "txid": row.get("hash"), "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(row.get("timeStamp")))})
    return verify_result(False, "pending", f"No matching {network_label} payment found yet")

def auto_scan_trx_by_address(expected_amount, expected_to_address, request_created_at, trongrid_base, trongrid_api_key):
    tolerance = get_network_tolerance("TRX (TRC20)")
    res = http_get_json(f"{trongrid_base}/v1/accounts/{expected_to_address}/transactions", params={"only_to": "true", "limit": 50, "order_by": "block_timestamp,desc"}, headers=trongrid_headers(trongrid_api_key), timeout=20)
    if not res["ok"]:
        return verify_result(False, "pending", "TRX address scan failed", {"http": res["status_code"]})
    for tx in res["data"].get("data", []):
        tx_time = tx.get("block_timestamp")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        contracts = (((tx.get("raw_data") or {}).get("contract")) or [])
        if not contracts:
            continue
        param_value = (((contracts[0].get("parameter") or {}).get("value")) or {})
        to_address_hex = str(param_value.get("to_address", "")).strip()
        if not to_address_hex:
            continue
        actual_to = tron_hex_to_base58(to_address_hex)
        if actual_to != expected_to_address:
            continue
        actual_amount = Decimal(int(param_value.get("amount", 0))) / Decimal("1000000")
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "matched by address scan", {"network": "TRX (TRC20)", "txid": tx.get("txID"), "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(tx_time))})
    return verify_result(False, "pending", "No matching TRX payment found yet")

def auto_scan_usdt_trc20_by_address(expected_amount, expected_to_address, request_created_at, trongrid_base, trongrid_api_key, usdt_trc20_contract):
    tolerance = get_network_tolerance("USDT (TRC20)")
    res = http_get_json(f"{trongrid_base}/v1/accounts/{expected_to_address}/transactions/trc20", params={"limit": 50, "only_confirmed": "true"}, headers=trongrid_headers(trongrid_api_key), timeout=20)
    if not res["ok"]:
        return verify_result(False, "pending", "USDT TRC20 address scan failed", {"http": res["status_code"]})
    expected_contract = str(usdt_trc20_contract).strip().lower()
    for tx in res["data"].get("data", []):
        tx_time = tx.get("block_ts") or tx.get("block_timestamp") or tx.get("timestamp")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        token_info = tx.get("token_info", {}) or {}
        if str(token_info.get("address", "")).strip().lower() != expected_contract:
            continue
        if str(tx.get("to", "")).strip() != str(expected_to_address).strip():
            continue
        value_raw = tx.get("value")
        if value_raw is None:
            continue
        token_decimal = int(str(token_info.get("decimals", "6")))
        actual_amount = Decimal(str(value_raw)) / (Decimal(10) ** Decimal(token_decimal))
        if amount_within_tolerance(actual_amount, expected_amount, tolerance):
            return verify_result(True, "confirmed", "matched by address scan", {"network": "USDT (TRC20)", "txid": tx.get("transaction_id"), "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(tx_time))})
    return verify_result(False, "pending", "No matching USDT TRC20 payment found yet")

def auto_scan_btc_by_address(expected_amount, expected_to_address, request_created_at, api_base):
    tolerance = get_network_tolerance("BTC")
    res = http_get_json(f"{api_base}/address/{expected_to_address}/txs", timeout=20)
    if not res["ok"]:
        return verify_result(False, "pending", "BTC address scan failed", {"http": res["status_code"]})
    rows = res["data"]
    if not isinstance(rows, list) or not rows:
        return verify_result(False, "pending", "No incoming BTC tx found yet")
    for tx in rows:
        tx_time = ((tx.get("status") or {}).get("block_time")) or tx.get("block_time")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        if not (tx.get("status", {}) or {}).get("confirmed"):
            continue
        for vout in tx.get("vout", []) or []:
            if vout.get("scriptpubkey_address") != expected_to_address:
                continue
            actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
            if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                return verify_result(True, "confirmed", "matched by address scan", {"network": "BTC", "txid": tx.get("txid"), "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(tx_time))})
    return verify_result(False, "pending", "No matching BTC payment found yet")

def auto_scan_ltc_by_address(expected_amount, expected_to_address, request_created_at, api_base):
    tolerance = get_network_tolerance("LTC")
    res = http_get_json(f"{api_base}/address/{expected_to_address}/txs", timeout=20)
    if not res["ok"]:
        return verify_result(False, "pending", "LTC address scan failed", {"http": res["status_code"]})
    rows = res["data"]
    if not isinstance(rows, list) or not rows:
        return verify_result(False, "pending", "No incoming LTC tx found yet")
    for tx in rows:
        tx_time = ((tx.get("status") or {}).get("block_time")) or tx.get("block_time")
        if not tx_is_within_request_window(tx_time, request_created_at):
            continue
        if not (tx.get("status", {}) or {}).get("confirmed"):
            continue
        for vout in tx.get("vout", []) or []:
            if vout.get("scriptpubkey_address") != expected_to_address:
                continue
            actual_amount = Decimal(vout.get("value", 0)) / Decimal("100000000")
            if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                return verify_result(True, "confirmed", "matched by address scan", {"network": "LTC", "txid": tx.get("txid"), "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(tx_time))})
    return verify_result(False, "pending", "No matching LTC payment found yet")

def auto_scan_sol_by_address(expected_amount, expected_to_address, request_created_at, helius_rpc_url):
    tolerance = get_network_tolerance("SOL")
    sig_res = get_sol_signatures_for_address(helius_rpc_url, expected_to_address, limit=50)
    if not sig_res["ok"]:
        return verify_result(False, "pending", "SOL address scan failed", {"http": sig_res["status_code"]})
    signatures = sig_res["data"].get("result", [])
    if not signatures:
        return verify_result(False, "pending", "No incoming SOL signatures found yet")
    for sig_item in signatures:
        if not tx_is_within_request_window(sig_item.get("blockTime"), request_created_at):
            continue
        txid = sig_item.get("signature")
        if not txid:
            continue
        res = helius_rpc(helius_rpc_url, "getTransaction", [txid, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}])
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
            if parsed.get("type") == "transfer" and info.get("destination") == expected_to_address:
                actual_amount = Decimal(int(info.get("lamports"))) / Decimal("1000000000")
                if amount_within_tolerance(actual_amount, expected_amount, tolerance):
                    return verify_result(True, "confirmed", "matched by address scan", {"network": "SOL", "txid": txid, "actual_amount": str(actual_amount), "expected_amount": str(expected_amount), "tx_time": str(normalize_dt(sig_item.get("blockTime")))})
    return verify_result(False, "pending", "No matching SOL payment found yet")

def verify_crypto_payment(network: str, txid: str, expected_amount, expected_to_address: str, config: dict):
    if network == "USDT (TRC20)":
        return verify_usdt_trc20_txid(txid, expected_amount, expected_to_address, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"], config["USDT_TRC20_CONTRACT"])
    if network == "TRX (TRC20)":
        return verify_trx_transfer(txid, expected_amount, expected_to_address, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"])
    if network == "ETH (ERC20)":
        return verify_evm_native_transfer(txid, expected_amount, expected_to_address, config["ETH_CHAIN_ID"], "ETH (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BNB (BEP20)":
        return verify_evm_native_transfer(txid, expected_amount, expected_to_address, config["BSC_CHAIN_ID"], "BNB (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (ERC20)":
        return verify_evm_token_transfer(txid, expected_amount, expected_to_address, config["ETH_CHAIN_ID"], config["USDT_ERC20_CONTRACT"], 6, "USDT (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (BEP20)":
        return verify_evm_token_transfer(txid, expected_amount, expected_to_address, config["BSC_CHAIN_ID"], config["USDT_BEP20_CONTRACT"], 18, "USDT (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BTC":
        return verify_btc_transfer(txid, expected_amount, expected_to_address, config["BTC_API_BASE"])
    if network == "LTC":
        return verify_ltc_transfer(txid, expected_amount, expected_to_address, config["LTC_API_BASE"])
    if network == "SOL":
        return verify_sol_transfer(txid, expected_amount, expected_to_address, config["HELIUS_RPC_URL"])
    return verify_result(False, "rejected", f"Unsupported network: {network}")

def auto_verify_by_record(record: dict, config: dict):
    network = record["network"]
    expected_amount = record["crypto_amount"]
    expected_to_address = record["address"]
    request_created_at = record.get("created_at")
    if network == "USDT (TRC20)":
        return auto_scan_usdt_trc20_by_address(expected_amount, expected_to_address, request_created_at, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"], config["USDT_TRC20_CONTRACT"])
    if network == "TRX (TRC20)":
        return auto_scan_trx_by_address(expected_amount, expected_to_address, request_created_at, config["TRONGRID_BASE"], config["TRONGRID_API_KEY"])
    if network == "ETH (ERC20)":
        return auto_scan_evm_native_by_address(expected_amount, expected_to_address, request_created_at, config["ETH_CHAIN_ID"], "ETH (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BNB (BEP20)":
        return auto_scan_evm_native_by_address(expected_amount, expected_to_address, request_created_at, config["BSC_CHAIN_ID"], "BNB (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (ERC20)":
        return auto_scan_evm_token_by_address(expected_amount, expected_to_address, request_created_at, config["ETH_CHAIN_ID"], config["USDT_ERC20_CONTRACT"], 6, "USDT (ERC20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "USDT (BEP20)":
        return auto_scan_evm_token_by_address(expected_amount, expected_to_address, request_created_at, config["BSC_CHAIN_ID"], config["USDT_BEP20_CONTRACT"], 18, "USDT (BEP20)", config.get("ETHERSCAN_V2_URL", ""), config["ETHERSCAN_API_KEY"])
    if network == "BTC":
        return auto_scan_btc_by_address(expected_amount, expected_to_address, request_created_at, config["BTC_API_BASE"])
    if network == "LTC":
        return auto_scan_ltc_by_address(expected_amount, expected_to_address, request_created_at, config["LTC_API_BASE"])
    if network == "SOL":
        return auto_scan_sol_by_address(expected_amount, expected_to_address, request_created_at, config["HELIUS_RPC_URL"])
    return verify_result(False, "rejected", f"Unsupported network: {network}", {"network": network})
