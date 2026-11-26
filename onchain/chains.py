"""Read-only chain inspection. Truth always comes from the chain, never from a
webhook payload or a user-supplied TXID: a webhook is only a *hint* that a
transaction is worth looking at.

inspect_tx(network, txid) returns:
  {"status": "found" | "not_found" | "failed" | "error",
   "final": bool, "confirmations": int, "block_number": int|None, "block_time": datetime|None,
   "transfers": [ {asset_id, locator, to_address, from_address, amount_units} ... ],   # incoming only
   "error": str|None}
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

import requests

from . import networks as nw

TIMEOUT = 20


class ChainError(Exception):
    pass


class RpcClient:
    """Small JSON-RPC / HTTP helper with bounded retries and backoff. Never logs URLs (they contain keys)."""

    def __init__(self, session: Optional[requests.Session] = None):
        self.s = session or requests.Session()

    def rpc(self, url: str, method: str, params: list, retries: int = 2):
        if not url:
            raise ChainError("rpc_not_configured")
        delay = 1.0
        for attempt in range(retries + 1):
            try:
                r = self.s.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                timeout=TIMEOUT)
                if r.status_code == 429 or r.status_code >= 500:
                    raise ChainError(f"http_{r.status_code}")
                data = r.json()
                if data.get("error"):
                    raise ChainError(f"rpc_error:{str(data['error'].get('message', ''))[:120]}")
                return data.get("result")
            except (requests.RequestException, ValueError, ChainError) as e:
                if attempt >= retries or (isinstance(e, ChainError) and str(e).startswith("rpc_error")):
                    raise ChainError(str(e)[:160]) from None
                time.sleep(delay)
                delay *= 2

    def post(self, url: str, payload: dict, headers: Optional[dict] = None, retries: int = 2):
        if not url:
            raise ChainError("http_not_configured")
        delay = 1.0
        for attempt in range(retries + 1):
            try:
                r = self.s.post(url, json=payload, headers=headers or {}, timeout=TIMEOUT)
                if r.status_code == 429 or r.status_code >= 500:
                    raise ChainError(f"http_{r.status_code}")
                return r.json()
            except (requests.RequestException, ValueError, ChainError) as e:
                if attempt >= retries:
                    raise ChainError(str(e)[:160]) from None
                time.sleep(delay)
                delay *= 2

    def get(self, url: str, params: Optional[dict] = None, headers: Optional[dict] = None, retries: int = 2):
        delay = 1.0
        for attempt in range(retries + 1):
            try:
                r = self.s.get(url, params=params or {}, headers=headers or {}, timeout=TIMEOUT)
                if r.status_code == 429 or r.status_code >= 500:
                    raise ChainError(f"http_{r.status_code}")
                return r.json()
            except (requests.RequestException, ValueError, ChainError) as e:
                if attempt >= retries:
                    raise ChainError(str(e)[:160]) from None
                time.sleep(delay)
                delay *= 2


def _result(status, **kw):
    out = {"status": status, "final": False, "confirmations": 0, "block_number": None, "block_time": None,
           "transfers": [], "error": None}
    out.update(kw)
    return out


def _ts(seconds) -> Optional[datetime]:
    if seconds is None:
        return None
    s = float(seconds)
    if s > 1e12:
        s /= 1000.0
    return datetime.fromtimestamp(s, tz=timezone.utc)


# ------------------------------------------------------------------ EVM
def inspect_evm(network_key: str, txid: str, client: RpcClient) -> dict:
    net = nw.NETWORKS[network_key]
    url = nw.alchemy_url(network_key)
    chain_id = client.rpc(url, "eth_chainId", [])
    if int(chain_id, 16) != net.chain_id:
        return _result("error", error="wrong_chain_id")
    tx = client.rpc(url, "eth_getTransactionByHash", [txid])
    if not tx:
        return _result("not_found")
    rcpt = client.rpc(url, "eth_getTransactionReceipt", [txid])
    if not rcpt or not rcpt.get("blockNumber"):
        return _result("found", final=False)  # pending
    if int(rcpt.get("status", "0x0"), 16) != 1:
        return _result("failed", error="execution_failed")
    block_number = int(rcpt["blockNumber"], 16)
    head = int(client.rpc(url, "eth_blockNumber", []), 16)
    block = client.rpc(url, "eth_getBlockByNumber", [rcpt["blockNumber"], False]) or {}
    if block.get("hash") and rcpt.get("blockHash") and block["hash"].lower() != rcpt["blockHash"].lower():
        return _result("not_found", error="reorg_block_hash_mismatch")
    confs = head - block_number + 1
    ours = net.address.lower()
    transfers = []
    if (tx.get("to") or "").lower() == ours and int(tx.get("value", "0x0"), 16) > 0:
        transfers.append({"asset_id": nw.native_asset(network_key).id, "locator": "native", "to_address": net.address,
                          "from_address": tx.get("from"), "amount_units": int(tx["value"], 16)})
    our_topic = "0x" + "0" * 24 + ours[2:]
    for log in rcpt.get("logs") or []:
        topics = [t.lower() for t in (log.get("topics") or [])]
        if len(topics) != 3 or topics[0] != nw.ERC20_TRANSFER_TOPIC or topics[2] != our_topic:
            continue
        asset = nw.token_asset_for_contract(network_key, log.get("address", ""))
        if not asset:
            continue  # not allowlisted (fake "USDT" tokens land here)
        if log.get("removed"):
            continue
        amount = int(log.get("data") or "0x0", 16)
        if amount <= 0:
            continue
        transfers.append({"asset_id": asset.id, "locator": f"log:{int(log['logIndex'], 16)}",
                          "to_address": net.address, "from_address": "0x" + topics[1][-40:],
                          "amount_units": amount})
    return _result("found", final=confs >= net.min_confirmations, confirmations=confs, block_number=block_number,
                   block_time=_ts(int(block["timestamp"], 16)) if block.get("timestamp") else None,
                   transfers=transfers)


# ------------------------------------------------------------------ TRON
def tron_base() -> str:
    override = nw.env("ONCHAIN_RPC_URL_TRON")
    if override:
        return override.rstrip("/")
    key = nw.env(nw.ENV_ALCHEMY_API_KEY)
    if key:
        return f"https://tron-mainnet.g.alchemy.com/v2/{key}"
    return "https://api.trongrid.io"


def _tron_headers():
    k = nw.env(nw.ENV_TRONGRID_API_KEY)
    base = tron_base()
    return {"TRON-PRO-API-KEY": k} if (k and "trongrid" in base) else {}


def inspect_tron(txid: str, client: RpcClient) -> dict:
    net = nw.NETWORKS["tron"]
    base = tron_base()
    h = _tron_headers()
    tx = client.post(f"{base}/wallet/gettransactionbyid", {"value": txid}, h)
    if not tx or not tx.get("txID"):
        return _result("not_found")
    solid = client.post(f"{base}/walletsolidity/gettransactioninfobyid", {"value": txid}, h) or {}
    info = solid if solid.get("id") else (client.post(f"{base}/wallet/gettransactioninfobyid", {"value": txid}, h) or {})
    if not info.get("id"):
        return _result("found", final=False)
    final = bool(solid.get("id"))
    ret = (tx.get("ret") or [{}])[0].get("contractRet")
    our_hex = nw.tron_to_hex(net.address)
    transfers = []
    contract = ((tx.get("raw_data") or {}).get("contract") or [{}])[0]
    ctype = contract.get("type")
    if ctype == "TransferContract":
        if ret != "SUCCESS":
            return _result("failed", error=f"contractRet:{ret}")
        v = (contract.get("parameter") or {}).get("value") or {}
        if (v.get("to_address") or "").lower() == our_hex and int(v.get("amount", 0)) > 0:
            transfers.append({"asset_id": "trx", "locator": "native", "to_address": net.address,
                              "from_address": nw.tron_hex_to_base58(v.get("owner_address", "41" + "0" * 40)),
                              "amount_units": int(v["amount"])})
    elif ctype == "TriggerSmartContract":
        if (info.get("receipt") or {}).get("result") != "SUCCESS" or ret != "SUCCESS":
            return _result("failed", error="trigger_failed")
        for idx, log in enumerate(info.get("log") or []):
            topics = [t.lower().removeprefix("0x") for t in (log.get("topics") or [])]
            if len(topics) != 3 or "0x" + topics[0] != nw.ERC20_TRANSFER_TOPIC:
                continue
            if topics[2][-40:] != our_hex[-40:]:
                continue
            asset = nw.token_asset_for_contract("tron", nw.tron_hex_to_base58(log.get("address", "")))
            if not asset:
                continue
            amount = int(log.get("data") or "0", 16)
            if amount > 0:
                transfers.append({"asset_id": asset.id, "locator": f"log:{idx}", "to_address": net.address,
                                  "from_address": nw.tron_hex_to_base58(topics[1][-40:]), "amount_units": amount})
    block_number = info.get("blockNumber")
    return _result("found", final=final, confirmations=net.min_confirmations if final else 1,
                   block_number=block_number, block_time=_ts(info.get("blockTimeStamp")), transfers=transfers)


# ------------------------------------------------------------------ UTXO (BTC / LTC)
def _units8(value) -> int:
    return int((Decimal(str(value)) * Decimal(10 ** 8)).to_integral_exact())


def _out_address(vout: dict) -> str:
    spk = vout.get("scriptPubKey") or {}
    return spk.get("address") or ((spk.get("addresses") or [None])[0]) or ""


def inspect_utxo(network_key: str, txid: str, client: RpcClient, max_inputs: int = 25) -> dict:
    net = nw.NETWORKS[network_key]
    url = nw.alchemy_url(network_key)
    tx = client.rpc(url, "getrawtransaction", [txid, True])
    if not tx:
        return _result("not_found")
    # Change detection: if any input spends from our address, this is our own outgoing tx;
    # its outputs back to us are change, never a new customer deposit.
    vins = tx.get("vin") or []
    if len(vins) > max_inputs:
        return _result("error", error="too_many_inputs_for_change_check")
    for vin in vins:
        if "coinbase" in vin:
            continue
        prev_addr = None
        if vin.get("prevout"):
            prev_addr = _out_address(vin["prevout"])
        else:
            prev = client.rpc(url, "getrawtransaction", [vin["txid"], True])
            prev_out = (prev.get("vout") or [])[vin["vout"]] if prev else {}
            prev_addr = _out_address(prev_out)
        if prev_addr == net.address:
            return _result("found", final=False, transfers=[], error="outgoing_tx_change_ignored")
    confs = int(tx.get("confirmations") or 0)
    transfers = []
    for out in tx.get("vout") or []:
        if _out_address(out) == net.address:
            units = _units8(out.get("value", 0))
            if units > 0:
                transfers.append({"asset_id": nw.native_asset(network_key).id, "locator": f"vout:{out['n']}",
                                  "to_address": net.address, "from_address": None, "amount_units": units})
    return _result("found", final=confs >= net.min_confirmations, confirmations=confs,
                   block_number=None, block_time=_ts(tx.get("blocktime")) if tx.get("blocktime") else None,
                   transfers=transfers)


# ------------------------------------------------------------------ Solana
def inspect_solana(signature: str, client: RpcClient) -> dict:
    net = nw.NETWORKS["solana"]
    url = nw.alchemy_url("solana")
    opts = {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
    tx = client.rpc(url, "getTransaction", [signature, dict(opts, commitment="finalized")])
    final = tx is not None
    if tx is None:
        tx = client.rpc(url, "getTransaction", [signature, dict(opts, commitment="confirmed")])
    if tx is None:
        return _result("not_found")
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return _result("failed", error="tx_err")
    keys = [(k.get("pubkey") if isinstance(k, dict) else k) for k in
            (((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or [])]
    transfers = []
    if net.address in keys:
        i = keys.index(net.address)
        delta = int(meta["postBalances"][i]) - int(meta["preBalances"][i])
        if delta > 0 and i != 0:  # index 0 is the fee payer (= our own outgoing tx)
            transfers.append({"asset_id": "sol", "locator": "native", "to_address": net.address,
                              "from_address": keys[0] if keys else None, "amount_units": delta})
    return _result("found", final=final, confirmations=1 if final else 0, block_number=tx.get("slot"),
                   block_time=_ts(tx.get("blockTime")), transfers=transfers)


def inspect_tx(network_key: str, txid: str, client: Optional[RpcClient] = None) -> dict:
    client = client or RpcClient()
    try:
        fam = nw.NETWORKS[network_key].family
        if fam == "evm":
            return inspect_evm(network_key, txid, client)
        if fam == "tron":
            return inspect_tron(txid, client)
        if fam == "utxo":
            return inspect_utxo(network_key, txid, client)
        if fam == "solana":
            return inspect_solana(txid, client)
        return _result("error", error="unknown_network")
    except ChainError as e:
        return _result("error", error=str(e))
    except Exception as e:  # defensive: never crash the worker on a malformed response
        return _result("error", error=f"{type(e).__name__}")


# ------------------------------------------------------------------ discovery (reconciliation pollers)
def discover_solana(client: RpcClient, until_sig: Optional[str], limit: int = 50) -> List[str]:
    url = nw.alchemy_url("solana")
    params = {"limit": limit, "commitment": "confirmed"}
    if until_sig:
        params["until"] = until_sig
    rows = client.rpc(url, "getSignaturesForAddress", [nw.SOLANA_ADDRESS, params]) or []
    return [r["signature"] for r in rows if r.get("err") is None]


def discover_tron(client: RpcClient, min_timestamp_ms: int, limit: int = 50) -> List[dict]:
    """TronGrid v1 account history (incoming only). Returns [{txid, ts_ms}]."""
    base = nw.env("ONCHAIN_TRONGRID_BASE") or "https://api.trongrid.io"
    h = {"TRON-PRO-API-KEY": nw.env(nw.ENV_TRONGRID_API_KEY)} if nw.env(nw.ENV_TRONGRID_API_KEY) else {}
    out = []
    p = {"only_to": "true", "limit": limit, "min_timestamp": int(min_timestamp_ms), "order_by": "block_timestamp,asc"}
    trc20 = client.get(f"{base}/v1/accounts/{nw.TRON_ADDRESS}/transactions/trc20",
                       dict(p, contract_address=nw.USDT_TRC20_CONTRACT), h)
    for r in (trc20 or {}).get("data") or []:
        out.append({"txid": r.get("transaction_id"), "ts_ms": int(r.get("block_timestamp") or 0)})
    native = client.get(f"{base}/v1/accounts/{nw.TRON_ADDRESS}/transactions", p, h)
    for r in (native or {}).get("data") or []:
        out.append({"txid": r.get("txID"), "ts_ms": int(r.get("block_timestamp") or 0)})
    return [x for x in out if x["txid"]]


def evm_head(network_key: str, client: RpcClient) -> int:
    return int(client.rpc(nw.alchemy_url(network_key), "eth_blockNumber", []), 16)


def discover_evm(network_key: str, client: RpcClient, from_block: int, to_block: int) -> List[str]:
    """alchemy_getAssetTransfers toAddress=ours over a bounded block window."""
    net = nw.NETWORKS[network_key]
    tokens = [a.contract for a in nw.assets_for_network(network_key) if a.kind == "token"]
    hashes = []
    for cats, extra in ((["external"], {}), (["erc20"], {"contractAddresses": tokens})):
        params = {"fromBlock": hex(from_block), "toBlock": hex(to_block), "toAddress": net.address,
                  "category": cats, "withMetadata": False, "excludeZeroValue": True, "maxCount": "0x64"}
        params.update(extra)
        res = client.rpc(nw.alchemy_url(network_key), "alchemy_getAssetTransfers", [params]) or {}
        hashes.extend(t.get("hash") for t in res.get("transfers") or [] if t.get("hash"))
    return list(dict.fromkeys(hashes))
