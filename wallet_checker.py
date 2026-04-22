from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Callable, Dict, Optional


# =========================
# INTERNAL STORAGE
# =========================
pending_crypto_deposits: Dict[int, dict] = {}
pending_crypto_orders: Dict[int, dict] = {}

# In-memory used tx cache
# For production, database storage is better.
used_txids = set()


# =========================
# BASIC HELPERS
# =========================
def now_dt():
    return datetime.now()


def safe_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def checker_result(ok: bool, status: str, message: str, data=None):
    return {
        "ok": ok,
        "status": status,
        "message": message,
        "data": data or {},
    }


# =========================
# TXID / DUPLICATE HELPERS
# =========================
def is_txid_already_used(txid: str) -> bool:
    return bool(txid) and txid in used_txids


def mark_txid_used(txid: str):
    if txid:
        used_txids.add(txid)


def unmark_txid_used(txid: str):
    if txid and txid in used_txids:
        used_txids.remove(txid)


def clear_user_pending_payment(user_id: int):
    pending_crypto_deposits.pop(user_id, None)
    pending_crypto_orders.pop(user_id, None)


# =========================
# CREATE / UPDATE PENDING RECORDS
# =========================
def _base_pending_record(user_id: int, usd_amount, crypto_amount, network: str, address: str, kind: str):
    return {
        "user_id": user_id,
        "kind": kind,
        "usd_amount": float(usd_amount),
        "crypto_amount": float(crypto_amount),
        "network": network,
        "address": address,
        "status": "awaiting_payment",
        "created_at": now_dt(),
        "updated_at": now_dt(),
        "verify_clicks": 0,
        "auto_check_attempts": 0,
        "matched_txid": None,
        "last_result": None,
        "last_reason": None,
        "last_scan_meta": {},
        "auto_verified": False,
    }


def create_or_update_pending_deposit(
    user_id: int,
    usd_amount,
    crypto_amount,
    network: str,
    address: str,
):
    pending_crypto_orders.pop(user_id, None)

    record = _base_pending_record(user_id, usd_amount, crypto_amount, network, address, "deposit")
    pending_crypto_deposits[user_id] = record
    return record


def create_or_update_pending_order(
    user_id: int,
    product_id: str,
    qty: int,
    usd_amount,
    crypto_amount,
    network: str,
    address: str,
):
    pending_crypto_deposits.pop(user_id, None)

    record = _base_pending_record(user_id, usd_amount, crypto_amount, network, address, "order")
    record["product_id"] = product_id
    record["qty"] = int(qty)
    pending_crypto_orders[user_id] = record
    return record


# =========================
# GETTERS
# =========================
def get_user_pending_deposit(user_id: int):
    return pending_crypto_deposits.get(user_id)


def get_user_pending_order(user_id: int):
    return pending_crypto_orders.get(user_id)


def get_user_pending_any(user_id: int):
    if user_id in pending_crypto_deposits:
        return pending_crypto_deposits[user_id]
    if user_id in pending_crypto_orders:
        return pending_crypto_orders[user_id]
    return None


def get_all_pending_deposits():
    return list(pending_crypto_deposits.values())


def get_all_pending_orders():
    return list(pending_crypto_orders.values())


def get_all_pending_records():
    return get_all_pending_deposits() + get_all_pending_orders()


# =========================
# STATE HELPERS
# =========================
def set_pending_status(record: dict, status: str, reason: Optional[str] = None):
    record["status"] = status
    if reason is not None:
        record["last_reason"] = reason
    record["updated_at"] = now_dt()


def touch_verify_click(record: dict):
    record["verify_clicks"] = int(record.get("verify_clicks", 0)) + 1
    record["updated_at"] = now_dt()


def touch_auto_attempt(record: dict):
    record["auto_check_attempts"] = int(record.get("auto_check_attempts", 0)) + 1
    record["updated_at"] = now_dt()


def attach_detected_txid(record: dict, txid: Optional[str]):
    if txid:
        record["matched_txid"] = txid
        mark_txid_used(txid)
    record["updated_at"] = now_dt()


def attach_scan_result(record: dict, result: dict):
    record["last_result"] = result
    record["updated_at"] = now_dt()

    if isinstance(result, dict):
        record["last_reason"] = result.get("reason")
        meta = result.get("meta", {}) or {}
        record["last_scan_meta"] = meta


# =========================
# COMPLETE DEPOSIT
# =========================
def complete_deposit_record(
    record: dict,
    user_wallet: dict,
    add_transaction_record: Optional[Callable] = None,
    set_tx_status: Optional[Callable] = None,
    user_transactions: Optional[dict] = None,
    all_transactions: Optional[list] = None,
):
    user_id = record["user_id"]
    usd_amount = float(record["usd_amount"])

    user_wallet[user_id] = user_wallet.get(user_id, 0) + usd_amount
    record["auto_verified"] = True
    set_pending_status(record, "completed", "Deposit completed automatically.")

    if add_transaction_record:
        add_transaction_record(
            user_id,
            "Deposit",
            usd_amount,
            "Completed",
            {
                "network": record["network"],
                "crypto_amount": record["crypto_amount"],
                "txid": record.get("matched_txid"),
                "auto_verified": True,
                "scan_meta": record.get("last_scan_meta", {}),
            },
        )

    if set_tx_status and user_transactions is not None:
        for tx in reversed(user_transactions.get(user_id, [])):
            if tx["type"] == "Deposit" and tx["status"] in {"Checking TXID", "Pending Auto Check", "Awaiting Blockchain Match"}:
                set_tx_status(tx, "Completed")
                tx.setdefault("meta", {})
                tx["meta"]["auto_verified"] = True
                tx["meta"]["txid"] = record.get("matched_txid")
                tx["meta"]["scan_meta"] = record.get("last_scan_meta", {})
                break

    if set_tx_status and all_transactions is not None:
        for tx in reversed(all_transactions):
            if tx["user_id"] == user_id and tx["type"] == "Deposit" and tx["status"] in {"Checking TXID", "Pending Auto Check", "Awaiting Blockchain Match"}:
                set_tx_status(tx, "Completed")
                tx.setdefault("meta", {})
                tx["meta"]["auto_verified"] = True
                tx["meta"]["txid"] = record.get("matched_txid")
                tx["meta"]["scan_meta"] = record.get("last_scan_meta", {})
                break

    pending_crypto_deposits.pop(user_id, None)
    return usd_amount


# =========================
# COMPLETE ORDER
# =========================
async def complete_order_record_async(
    record: dict,
    deliver_accounts_to_user_async: Callable,
    bot,
    add_order_record: Callable,
    add_transaction_record: Optional[Callable] = None,
    set_tx_status: Optional[Callable] = None,
    user_transactions: Optional[dict] = None,
    all_transactions: Optional[list] = None,
):
    user_id = record["user_id"]
    product_id = record["product_id"]
    qty = int(record["qty"])
    usd_amount = float(record["usd_amount"])

    ok, delivered = await deliver_accounts_to_user_async(bot, user_id, product_id, qty)
    if not ok:
        set_pending_status(record, "delivery_failed", "Not enough real stock to deliver.")
        return checker_result(False, "failed", "Not enough real stock to deliver.")

    add_order_record(user_id, product_id, qty, usd_amount, "Completed", "Crypto Auto")

    if add_transaction_record:
        add_transaction_record(
            user_id,
            "Order Payment",
            usd_amount,
            "Completed",
            {
                "network": record["network"],
                "crypto_amount": record["crypto_amount"],
                "txid": record.get("matched_txid"),
                "auto_verified": True,
                "product_id": product_id,
                "qty": qty,
                "scan_meta": record.get("last_scan_meta", {}),
            },
        )

    if set_tx_status and user_transactions is not None:
        for tx in reversed(user_transactions.get(user_id, [])):
            if tx["type"] == "Order Payment" and tx["status"] in {"Checking TXID", "Pending Auto Check", "Awaiting Blockchain Match"}:
                set_tx_status(tx, "Completed")
                tx.setdefault("meta", {})
                tx["meta"]["auto_verified"] = True
                tx["meta"]["txid"] = record.get("matched_txid")
                tx["meta"]["scan_meta"] = record.get("last_scan_meta", {})
                break

    if set_tx_status and all_transactions is not None:
        for tx in reversed(all_transactions):
            if tx["user_id"] == user_id and tx["type"] == "Order Payment" and tx["status"] in {"Checking TXID", "Pending Auto Check", "Awaiting Blockchain Match"}:
                set_tx_status(tx, "Completed")
                tx.setdefault("meta", {})
                tx["meta"]["auto_verified"] = True
                tx["meta"]["txid"] = record.get("matched_txid")
                tx["meta"]["scan_meta"] = record.get("last_scan_meta", {})
                break

    record["auto_verified"] = True
    set_pending_status(record, "completed", "Order completed automatically.")
    pending_crypto_orders.pop(user_id, None)

    return checker_result(True, "completed", "Order completed automatically.", {"delivered": delivered})


# =========================
# AUTO VERIFY CORE
# =========================
def try_auto_verify_record(record: dict, auto_scan_callable: Callable) -> dict:
    touch_auto_attempt(record)

    result = auto_scan_callable(record)
    attach_scan_result(record, result)

    if not isinstance(result, dict):
        set_pending_status(record, "error", "Auto scan callable returned invalid result.")
        return checker_result(False, "error", "Auto scan callable returned invalid result.")

    status = result.get("status", "pending")
    reason = result.get("reason", "")
    meta = result.get("meta", {}) or {}
    txid = meta.get("txid") or result.get("txid")

    if txid and is_txid_already_used(txid):
        # If the same record already holds this txid, allow it
        if record.get("matched_txid") != txid:
            set_pending_status(record, "awaiting_blockchain_match", "Matched transaction already used before.")
            return checker_result(False, "pending", "Matched transaction was already used before.", result)

    if status == "confirmed":
        attach_detected_txid(record, txid)
        set_pending_status(record, "matched", reason or "Payment matched automatically.")
        return checker_result(True, "confirmed", reason or "Payment matched automatically.", result)

    if status == "rejected":
        set_pending_status(record, "rejected", reason or "Payment rejected.")
        return checker_result(False, "rejected", reason or "Payment rejected.", result)

    set_pending_status(record, "awaiting_blockchain_match", reason or "No matching payment found yet.")
    return checker_result(False, "pending", reason or "No matching payment found yet.", result)


def on_verify_clicked(user_id: int, auto_scan_callable: Callable) -> dict:
    record = get_user_pending_any(user_id)
    if not record:
        return checker_result(False, "not_found", "No pending payment request found.")

    touch_verify_click(record)
    set_pending_status(record, "checking", "User clicked verify.")
    return try_auto_verify_record(record, auto_scan_callable)


# =========================
# BACKGROUND AUTO RECHECK
# =========================
async def background_auto_recheck(
    context,
    auto_scan_callable: Callable,
    deposit_complete_async: Optional[Callable] = None,
    order_complete_async: Optional[Callable] = None,
    send_pending_message_async: Optional[Callable] = None,
    send_rejected_message_async: Optional[Callable] = None,
    send_completed_message_async: Optional[Callable] = None,
):
    deposit_items = list(pending_crypto_deposits.items())
    order_items = list(pending_crypto_orders.items())

    for user_id, record in deposit_items + order_items:
        if record.get("status") in {"completed", "rejected", "delivery_failed"}:
            continue

        result = try_auto_verify_record(record, auto_scan_callable)

        if result["status"] == "confirmed":
            if record["kind"] == "deposit" and deposit_complete_async:
                await deposit_complete_async(record)
            elif record["kind"] == "order" and order_complete_async:
                await order_complete_async(record)

            if send_completed_message_async:
                await send_completed_message_async(user_id, record, result)
            continue

        if result["status"] == "rejected":
            if send_rejected_message_async:
                await send_rejected_message_async(user_id, record, result)
            continue

        if send_pending_message_async:
            await send_pending_message_async(user_id, record, result)


# =========================
# OPTIONAL TXID FALLBACK
# =========================
def verify_manual_txid_for_record(
    user_id: int,
    txid: str,
    txid_verify_callable: Callable,
) -> dict:
    if is_txid_already_used(txid):
        return checker_result(False, "used", "This TXID has already been used.")

    record = get_user_pending_any(user_id)
    if not record:
        return checker_result(False, "not_found", "No pending payment request found.")

    result = txid_verify_callable(
        record["network"],
        txid,
        record["crypto_amount"],
        record["address"],
    )

    attach_scan_result(record, result)

    if not isinstance(result, dict):
        set_pending_status(record, "error", "TXID verify callable returned invalid result.")
        return checker_result(False, "error", "TXID verify callable returned invalid result.")

    if result.get("status") == "confirmed":
        attach_detected_txid(record, txid)
        set_pending_status(record, "matched", result.get("reason", "TXID verified successfully."))
        return checker_result(True, "confirmed", result.get("reason", "TXID verified successfully."), result)

    if result.get("status") == "rejected":
        set_pending_status(record, "rejected", result.get("reason", "TXID rejected."))
        return checker_result(False, "rejected", result.get("reason", "TXID rejected."), result)

    set_pending_status(record, "awaiting_blockchain_match", result.get("reason", "TXID still pending."))
    return checker_result(False, "pending", result.get("reason", "TXID still pending."), result)


# =========================
# MAINTENANCE / DEBUG
# =========================
def cleanup_completed_or_rejected():
    remove_dep = [uid for uid, rec in pending_crypto_deposits.items() if rec.get("status") in {"completed", "rejected", "delivery_failed"}]
    remove_ord = [uid for uid, rec in pending_crypto_orders.items() if rec.get("status") in {"completed", "rejected", "delivery_failed"}]

    for uid in remove_dep:
        pending_crypto_deposits.pop(uid, None)

    for uid in remove_ord:
        pending_crypto_orders.pop(uid, None)


def reset_all_pending():
    pending_crypto_deposits.clear()
    pending_crypto_orders.clear()


def format_pending_record_text(record: dict) -> str:
    lines = [
        f"Kind: {record.get('kind', 'unknown')}",
        f"Status: {record.get('status', 'unknown')}",
        f"Reason: {record.get('last_reason')}",
        f"USD Amount: {record.get('usd_amount')}",
        f"Crypto Amount: {record.get('crypto_amount')}",
        f"Network: {record.get('network')}",
        f"Address: {record.get('address')}",
        f"Verify Clicks: {record.get('verify_clicks', 0)}",
        f"Auto Check Attempts: {record.get('auto_check_attempts', 0)}",
        f"Matched TXID: {record.get('matched_txid')}",
        f"Created: {record.get('created_at')}",
        f"Updated: {record.get('updated_at')}",
    ]

    meta = record.get("last_scan_meta", {}) or {}
    if meta:
        lines.append(f"Last Scan Meta: {meta}")

    if record.get("kind") == "order":
        lines.insert(1, f"Product ID: {record.get('product_id')}")
        lines.insert(2, f"Qty: {record.get('qty')}")

    return "\n".join(lines)
