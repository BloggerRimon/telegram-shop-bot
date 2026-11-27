"""Read-only solid-block discovery independent of public address-history indexers.

Only transaction hashes become hints; the existing chain verifier and invoice
matcher still decide whether a transfer is payable. No balance writes occur here.
"""
import re

from . import chains, networks as nw, store


def post(client, method, payload):
    return client.post(f"{chains.tron_base()}/walletsolidity/{method}", payload,
                       chains._tron_headers(), retries=0)


def solid_head(client):
    block = post(client, "getnowblock", {})
    try:
        return int(block["block_header"]["raw_data"]["number"])
    except (TypeError, KeyError, ValueError):
        raise chains.ChainError("tron_invalid_solid_head") from None


def block_at_time(client, head, timestamp_ms):
    low, high = 0, head
    while low < high:
        middle = (low + high) // 2
        block = post(client, "getblockbynum", {"num": middle})
        raw = (block.get("block_header") or {}).get("raw_data") or {}
        if raw.get("number", -1) != middle or not raw.get("timestamp"):
            raise chains.ChainError("tron_invalid_block_time")
        if int(raw["timestamp"]) < timestamp_ms:
            low = middle + 1
        else:
            high = middle
    return max(0, low - 1)


def discover_blocks(client, start, end):
    if start > end or end - start >= 40:
        raise chains.ChainError("tron_block_range_invalid")
    response = post(client, "getblockbylimitnext", {"startNum": start, "endNum": end + 1})
    blocks = response.get("block") if isinstance(response, dict) else None
    if not isinstance(blocks, list):
        raise chains.ChainError("tron_block_range_missing")
    by_height = {(b.get("block_header") or {}).get("raw_data", {}).get("number"): b for b in blocks}
    if any(height not in by_height for height in range(start, end + 1)):
        raise chains.ChainError("tron_block_range_incomplete")
    our_hex = nw.tron_to_hex(nw.TRON_ADDRESS)[-40:]
    contract_hex = nw.tron_to_hex(nw.USDT_TRC20_CONTRACT)[-40:]
    txids = []
    for height in range(start, end + 1):
        has_contract_calls = False
        for tx in by_height[height].get("transactions") or []:
            for contract in (tx.get("raw_data") or {}).get("contract") or []:
                if contract.get("type") == "TransferContract":
                    value = (contract.get("parameter") or {}).get("value") or {}
                    if nw.same_address("tron", value.get("to_address", ""), nw.TRON_ADDRESS):
                        txids.append(tx.get("txID", ""))
                elif contract.get("type") == "TriggerSmartContract":
                    has_contract_calls = True
        if not has_contract_calls:
            continue
        receipts = post(client, "gettransactioninfobyblocknum", {"num": height})
        if not isinstance(receipts, list) or not receipts:
            raise chains.ChainError("tron_block_receipts_missing")
        for receipt in receipts:
            for log in receipt.get("log") or []:
                topics = [str(t).lower().removeprefix("0x") for t in log.get("topics") or []]
                address = str(log.get("address") or "").lower().removeprefix("0x")
                if (address[-40:] == contract_hex and len(topics) == 3
                        and "0x" + topics[0] == nw.ERC20_TRANSFER_TOPIC and topics[2][-40:] == our_hex):
                    txids.append(receipt.get("id", ""))
    return list(dict.fromkeys(t.lower() for t in txids if re.fullmatch(r"[0-9a-fA-F]{64}", t)))


def poll(client):
    oldest = store.earliest_open_invoice_time("tron")
    if oldest is None:
        return  # no whole-chain polling cost without an open invoice
    head = solid_head(client)
    cursor = store.get_checkpoint("tron_solid_block")
    stamp = oldest.isoformat()
    if cursor is None or store.get_checkpoint("tron_scan_invoice_since") != stamp:
        cursor = block_at_time(client, head, int(oldest.timestamp() * 1000)) - 1
        store.set_checkpoint("tron_scan_invoice_since", stamp)
        store.set_checkpoint("tron_solid_block", cursor)
    start = int(cursor) + 1
    if start > head:
        return
    end = min(head, start + 39)
    for txid in discover_blocks(client, start, end):
        store.add_hint("tron", txid, "solid_block_scan")
    store.set_checkpoint("tron_solid_block", end)
