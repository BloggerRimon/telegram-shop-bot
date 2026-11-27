"""Indexer-independent read-only recovery for EVM and Bitcoin-family payments."""
from datetime import datetime, timezone

from . import chains, networks as nw, store, tron_scan


def head(client, network):
    method = "eth_blockNumber" if nw.NETWORKS[network].family == "evm" else "getblockcount"
    value = client.rpc(nw.alchemy_url(network), method, [])
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


def block(client, network, height, full=False):
    url = nw.alchemy_url(network)
    if nw.NETWORKS[network].family == "evm":
        result = client.rpc(url, "eth_getBlockByNumber", [hex(height), full])
        if not isinstance(result, dict) or int(result.get("number", "-1"), 16) != height or not result.get("hash"):
            raise chains.ChainError("recovery_block_missing")
        return result, int(result["timestamp"], 16)
    digest = client.rpc(url, "getblockhash", [height])
    result = client.rpc(url, "getblock" if full else "getblockheader", [digest, 2] if full else [digest])
    if (not isinstance(result, dict) or result.get("height") != height
            or result.get("hash") != digest or (full and not isinstance(result.get("tx"), list))):
        raise chains.ChainError("recovery_block_missing")
    return result, int(result["time"])


def first_block(client, network, latest, timestamp):
    low, high = 0, latest
    while low < high:
        middle = (low + high) // 2
        _, stamp = block(client, network, middle)
        if stamp < timestamp:
            low = middle + 1
        else:
            high = middle
    return max(0, low - 1)


def evm_hashes(client, network, start, end, assets):
    net = nw.NETWORKS[network]
    url, hashes = nw.alchemy_url(network), []
    if nw.native_asset(network).id in assets:
        for height in range(start, end + 1):
            data, _ = block(client, network, height, full=True)
            if not all(isinstance(tx, dict) for tx in data.get("transactions", [])):
                raise chains.ChainError("recovery_full_transactions_missing")
            hashes.extend(tx["hash"] for tx in data.get("transactions", [])
                          if (tx.get("to") or "").lower() == net.address.lower()
                          and int(tx.get("value", "0x0"), 16) > 0)
    tokens = [nw.ASSETS[aid].contract for aid in assets if nw.ASSETS[aid].kind == "token"]
    if tokens:
        # Alchemy Free limits eth_getLogs to ten blocks on ETH and BSC.
        # Complete every chunk before the caller advances its durable cursor.
        for offset in range(start, end + 1, 10):
            logs = client.rpc(url, "eth_getLogs", [{"fromBlock": hex(offset), "toBlock": hex(min(end, offset + 9)),
                "address": tokens, "topics": [nw.ERC20_TRANSFER_TOPIC, None, "0x" + "0" * 24 + net.address[2:].lower()]}])
            if not isinstance(logs, list):
                raise chains.ChainError("recovery_logs_missing")
            hashes.extend(log["transactionHash"] for log in logs if log.get("transactionHash") and not log.get("removed"))
    return list(dict.fromkeys(hashes))


def utxo_hashes(client, network, start, end):
    hashes = []
    for height in range(start, end + 1):
        data, _ = block(client, network, height, full=True)
        for tx in data["tx"]:
            if not isinstance(tx, dict):
                raise chains.ChainError("recovery_full_transactions_missing")
            if any(chains._out_address(out) == nw.NETWORKS[network].address for out in tx.get("vout") or []):
                hashes.append(tx["txid"])
    return list(dict.fromkeys(hashes))


def poll(client, network):
    window = store.recovery_invoice_window(network)
    if not window or window["started"] is None:
        return
    key = "rpc_recovery:" + network
    state = store.get_checkpoint(key, {}) or {}
    since, ended = window["started"].isoformat(), window["ended"].isoformat()
    if state.get("since") == since and state.get("covered_end") == ended:
        return  # completed invoice windows are not rescanned indefinitely
    latest = head(client, network)
    if state.get("since") != since or state.get("next") is None or state.get("covered_end"):
        start_at = window.get("active_started") or window["started"]
        start = first_block(client, network, latest, int(start_at.timestamp()) - 30)
    else:
        start = int(state["next"])
    if start > latest:
        return
    end = min(latest, start + (199 if nw.NETWORKS[network].family == "evm" else 7))
    if nw.NETWORKS[network].family == "evm":
        if int(client.rpc(nw.alchemy_url(network), "eth_chainId", []), 16) != nw.NETWORKS[network].chain_id:
            raise chains.ChainError("wrong_chain_id")
        hashes = evm_hashes(client, network, start, end, window["assets"] or [])
    else:
        hashes = utxo_hashes(client, network, start, end)
    _, through = block(client, network, end)
    for txid in hashes:
        store.add_hint(network, txid, "rpc_block_recovery")
    store.set_checkpoint(key, {"since": since, "next": end + 1, "through": through,
        "covered_end": ended if through >= int(window["ended"].timestamp()) else None})


def probe_all():
    """Read-only startup canary; no invoices, transactions, signatures or balance edits."""
    client = chains.RpcClient(timeout=8, retries=0)
    for network, net in nw.NETWORKS.items():
        result = {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": False}
        try:
            if net.family == "evm":
                url = nw.alchemy_url(network)
                if int(client.rpc(url, "eth_chainId", []), 16) != net.chain_id:
                    raise chains.ChainError("wrong_chain_id")
                latest = head(client, network)
                block(client, network, latest, full=True)
                evm_hashes(client, network, latest, latest, [a.id for a in nw.assets_for_network(network) if a.kind == "token"])
            elif net.family == "utxo":
                latest = head(client, network)
                block(client, network, latest)
                block(client, network, latest, full=True)
            elif net.family == "solana":
                latest = int(client.rpc(nw.alchemy_url(network), "getSlot", [{"commitment": "finalized"}]))
                rows = client.rpc(nw.alchemy_url(network), "getSignaturesForAddress", [net.address, {"limit": 1}])
                if not isinstance(rows, list):
                    raise chains.ChainError("solana_history_missing")
            else:
                latest = tron_scan.solid_head(client)
                tron_scan.discover_blocks(client, latest, latest)
            result.update(ok=True, height=latest)
        except Exception as exc:
            result["error"] = chains.safe_error(exc)
        store.set_checkpoint("rpc_probe:" + network, result)
        print(f"[onchain] RPC probe {network}: {'OK' if result['ok'] else result.get('error', 'failed')}")
