"""Tests for the admin-only self-custody payment module.

Requires a disposable PostgreSQL database in ONCHAIN_TEST_DATABASE_URL; DB tests
are skipped otherwise. Never point this at production.
"""

import asyncio
import hashlib
import hmac
import json
import os
import threading
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http.server import ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("BOT_TOKEN", "1:TEST")
TEST_DB = os.getenv("ONCHAIN_TEST_DATABASE_URL", "")

from onchain import chains, engine, store, webhook  # noqa: E402
from onchain import networks as nw  # noqa: E402

ADMIN = 6795246172
USER = 111


def is_admin(uid):
    return uid == ADMIN


def now():
    return datetime.now(timezone.utc)


class AddressTests(unittest.TestCase):
    def test_all_six_owner_addresses_valid_and_exact(self):
        self.assertEqual(nw.NETWORKS["tron"].address, "TGvd1iCDKswiejr14gsDxDvEm5d5atCwuw")
        self.assertEqual(nw.NETWORKS["ethereum"].address, "0x51a2aF4Bbcfb6d605018afE57CBbffB3de3d2022")
        self.assertEqual(nw.NETWORKS["bsc"].address, "0x51a2aF4Bbcfb6d605018afE57CBbffB3de3d2022")
        self.assertEqual(nw.NETWORKS["bitcoin"].address, "bc1qv52jn5v4hg5j0fwfawv02v32e8g7wqhx39jsgw")
        self.assertEqual(nw.NETWORKS["litecoin"].address, "ltc1qjjh6vu4qhg0f4smg5xwgms4jn29hesglxha363")
        self.assertEqual(nw.NETWORKS["solana"].address, "FHiqkiX5AmVMEZnt1w8oC6vh2fXNXueZHgNCXUzFrtrq")
        for k, n in nw.NETWORKS.items():
            self.assertTrue(nw.validate_address(k, n.address), k)

    def test_corrupted_addresses_rejected(self):
        self.assertFalse(nw.validate_address("tron", "TGvd1iCDKswiejr14gsDxDvEm5d5atCwuv"))
        self.assertFalse(nw.validate_address("bitcoin", "bc1qv52jn5v4hg5j0fwfawv02v32e8g7wqhx39jsgq"))
        self.assertFalse(nw.validate_address("litecoin", nw.BITCOIN_ADDRESS))

    def test_nine_assets_six_networks(self):
        self.assertEqual(len(nw.ASSET_ORDER), 9)
        self.assertEqual(len(nw.NETWORKS), 6)
        self.assertNotIn("usdt_spl", nw.ASSETS)  # Solana option is SOL only

    def test_token_allowlist_by_contract_not_symbol(self):
        self.assertIsNone(nw.token_asset_for_contract("ethereum", "0x" + "1" * 40))
        self.assertEqual(nw.token_asset_for_contract("ethereum", nw.USDT_ERC20_CONTRACT.upper().replace("0X", "0x")).id,
                         "usdt_erc20")
        # BSC USDT contract must not be accepted on Ethereum (same address text, different chain)
        self.assertIsNone(nw.token_asset_for_contract("ethereum", nw.USDT_BEP20_CONTRACT))


class SignatureTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"ALCHEMY_SIGNING_KEY_ETHEREUM": "whsec_test_key"}, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def body(self, network="ETH_MAINNET", wid="wh_1"):
        return json.dumps({"webhookId": wid, "id": "whevt_1", "type": "ADDRESS_ACTIVITY",
                           "event": {"network": network, "activity": []}}).encode()

    def sig(self, raw, key="whsec_test_key"):
        return hmac.new(key.encode(), raw, hashlib.sha256).hexdigest()

    def test_missing_and_invalid_signature_rejected(self):
        raw = self.body()
        self.assertEqual(webhook.handle("/alchemy/webhook/ethereum", {}, raw)[0], 401)
        self.assertEqual(webhook.handle("/alchemy/webhook/ethereum", {"X-Alchemy-Signature": "00" * 32}, raw)[0], 401)
        self.assertEqual(webhook.handle("/alchemy/webhook/ethereum",
                                        {"X-Alchemy-Signature": self.sig(raw, "other")}, raw)[0], 401)

    def test_signature_over_raw_body_not_reserialized(self):
        raw = b'{"webhookId":"wh_1","id":"e2","event":{"network":"ETH_MAINNET"}  }'
        self.assertTrue(webhook.verify_signature(raw, self.sig(raw), "whsec_test_key"))
        self.assertFalse(webhook.verify_signature(json.dumps(json.loads(raw)).encode(), self.sig(raw), "whsec_test_key"))

    def test_unconfigured_network_and_unknown_path(self):
        raw = self.body("BNB_MAINNET")
        self.assertEqual(webhook.handle("/alchemy/webhook/bsc", {"X-Alchemy-Signature": self.sig(raw)}, raw)[0], 503)
        self.assertEqual(webhook.handle("/alchemy/webhook/litecoin", {}, raw)[0], 404)

    def test_wrong_network_and_wrong_webhook_identity(self):
        raw = self.body("BNB_MAINNET")  # a BSC payload signed with the ETH key, sent to the ETH path
        self.assertEqual(webhook.handle("/alchemy/webhook/ethereum", {"X-Alchemy-Signature": self.sig(raw)}, raw)[0], 400)
        with patch.dict(os.environ, {"ALCHEMY_WEBHOOK_ID_ETHEREUM": "wh_expected"}):
            raw = self.body(wid="wh_other")
            self.assertEqual(webhook.handle("/alchemy/webhook/ethereum",
                                            {"X-Alchemy-Signature": self.sig(raw)}, raw)[0], 403)

    def test_oversize(self):
        raw = b"x" * (webhook.MAX_BODY_BYTES + 1)
        self.assertEqual(webhook.handle("/alchemy/webhook/ethereum", {"X-Alchemy-Signature": self.sig(raw)}, raw)[0], 413)

    def test_extract_txids(self):
        h = "0x" + "ab" * 32
        p = {"event": {"activity": [{"hash": h.upper().replace("0X", "0x")}, {"hash": "0x12"}]}}
        self.assertEqual(webhook.extract_txids("ethereum", p), [h])
        self.assertEqual(webhook.extract_txids("bitcoin", {"event": {"activity": [{"txid": "cd" * 32}]}}), ["cd" * 32])


class FakeRpc(chains.RpcClient):
    def __init__(self, table):
        self.table = table
        self.s = None

    def rpc(self, url, method, params, retries=2):
        key = (method, json.dumps(params, sort_keys=True))
        if key in self.table:
            return self.table[key]
        if method in self.table:
            v = self.table[method]
            return v(params) if callable(v) else v
        raise chains.ChainError(f"no fake for {method}")

    def post(self, url, payload, headers=None, retries=2):
        return self.rpc(url, url.rsplit("/", 2)[-2] + "/" + url.rsplit("/", 1)[-1], [payload])

    def get(self, url, params=None, headers=None, retries=2):
        return self.rpc(url, "GET", [url])


class ChainParsingTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"ALCHEMY_API_KEY": "k"})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def evm_tables(self, logs, value="0x0", to="0x0000000000000000000000000000000000000001", chain="0x1"):
        return {"eth_chainId": chain, "eth_getTransactionByHash": {"to": to, "value": value, "from": "0xabc"},
                "eth_getTransactionReceipt": {"blockNumber": "0x64", "blockHash": "0xbb", "status": "0x1", "logs": logs},
                "eth_blockNumber": "0x6f", "eth_getBlockByNumber": {"hash": "0xbb", "timestamp": hex(1_800_000_000)}}

    def log(self, contract, to=nw.ETHEREUM_ADDRESS, amount=1_000_000, idx=3):
        return {"address": contract, "logIndex": hex(idx), "data": hex(amount),
                "topics": [nw.ERC20_TRANSFER_TOPIC, "0x" + "0" * 24 + "ab" * 20, "0x" + "0" * 24 + to.lower()[2:]]}

    def test_evm_allowlisted_token_only(self):
        fake = self.log("0x" + "99" * 20)
        good = self.log(nw.USDT_ERC20_CONTRACT, idx=7)
        res = chains.inspect_evm("ethereum", "0xh", FakeRpc(self.evm_tables([fake, good])))
        self.assertEqual([(t["asset_id"], t["locator"], t["amount_units"]) for t in res["transfers"]],
                         [("usdt_erc20", "log:7", 1_000_000)])
        self.assertEqual(res["confirmations"], 12)
        self.assertTrue(res["final"])

    def test_evm_wrong_chain_and_failed_tx(self):
        res = chains.inspect_evm("bsc", "0xh", FakeRpc(self.evm_tables([], chain="0x1")))
        self.assertEqual(res["error"], "wrong_chain_id")
        t = self.evm_tables([])
        t["eth_getTransactionReceipt"]["status"] = "0x0"
        self.assertEqual(chains.inspect_evm("ethereum", "0xh", FakeRpc(t))["status"], "failed")

    def test_evm_native_and_wrong_destination(self):
        t = self.evm_tables([], value=hex(10 ** 15), to=nw.ETHEREUM_ADDRESS.lower())
        res = chains.inspect_evm("ethereum", "0xh", FakeRpc(t))
        self.assertEqual(res["transfers"][0]["asset_id"], "eth")
        t = self.evm_tables([self.log(nw.USDT_ERC20_CONTRACT, to="0x" + "12" * 20)])
        self.assertEqual(chains.inspect_evm("ethereum", "0xh", FakeRpc(t))["transfers"], [])

    def test_evm_insufficient_confirmations(self):
        t = self.evm_tables([self.log(nw.USDT_ERC20_CONTRACT)])
        t["eth_blockNumber"] = "0x66"
        res = chains.inspect_evm("ethereum", "0xh", FakeRpc(t))
        self.assertFalse(res["final"])

    def test_utxo_outputs_and_change(self):
        tx = {"confirmations": 3, "blocktime": 1_800_000_000,
              "vin": [{"txid": "aa" * 32, "vout": 0, "prevout": {"scriptPubKey": {"address": "bc1qother"}}}],
              "vout": [{"n": 0, "value": 0.0001, "scriptPubKey": {"address": nw.BITCOIN_ADDRESS}},
                       {"n": 1, "value": 0.5, "scriptPubKey": {"address": "bc1qsomeoneelse"}},
                       {"n": 2, "value": 0.00002, "scriptPubKey": {"address": nw.BITCOIN_ADDRESS}}]}
        res = chains.inspect_utxo("bitcoin", "t", FakeRpc({"getrawtransaction": tx}))
        self.assertEqual([(t["locator"], t["amount_units"]) for t in res["transfers"]], [("vout:0", 10000), ("vout:2", 2000)])
        tx["vin"][0]["prevout"]["scriptPubKey"]["address"] = nw.BITCOIN_ADDRESS  # our own spend -> change
        self.assertEqual(chains.inspect_utxo("bitcoin", "t", FakeRpc({"getrawtransaction": tx}))["transfers"], [])

    def test_litecoin_addresses_array_format(self):
        # Alchemy's Litecoin node returns scriptPubKey.addresses (list), and no prevout in verbosity 2.
        prev = {"vout": [{"n": 0, "value": 1, "scriptPubKey": {"addresses": ["ltc1qsender"]}}]}
        tx = {"confirmations": 6, "blocktime": 1_800_000_000, "vin": [{"txid": "pp", "vout": 0}],
              "vout": [{"n": 1, "value": "0.01", "scriptPubKey": {"addresses": [nw.LITECOIN_ADDRESS]}}]}
        fake = FakeRpc({"getrawtransaction": lambda p: prev if p[0] == "pp" else tx})
        res = chains.inspect_utxo("litecoin", "t", fake)
        self.assertEqual((res["transfers"][0]["amount_units"], res["final"]), (1_000_000, True))

    def test_tron_trc20_and_fake_token(self):
        our = nw.tron_to_hex(nw.TRON_ADDRESS)[2:]
        usdt = nw.tron_to_hex(nw.USDT_TRC20_CONTRACT)[2:]
        def info(contract):
            return {"id": "x", "blockNumber": 5, "blockTimeStamp": 1_800_000_000_000, "receipt": {"result": "SUCCESS"},
                    "log": [{"address": contract, "data": hex(2_500_000)[2:],
                             "topics": [nw.ERC20_TRANSFER_TOPIC[2:], "0" * 24 + "11" * 20, "0" * 24 + our]}]}
        tx = {"txID": "x", "ret": [{"contractRet": "SUCCESS"}], "raw_data": {"contract": [{"type": "TriggerSmartContract"}]}}
        for contract, expected in ((usdt, 1), ("22" * 20, 0)):
            fake = FakeRpc({"wallet/gettransactionbyid": tx, "walletsolidity/gettransactioninfobyid": info(contract)})
            res = chains.inspect_tron("x", fake)
            self.assertEqual(len(res["transfers"]), expected)
            self.assertTrue(res["final"])

    def test_solana_delta_and_fee_payer(self):
        tx = {"slot": 9, "blockTime": 1_800_000_000, "meta": {"err": None, "preBalances": [5, 100], "postBalances": [1, 1100]},
              "transaction": {"message": {"accountKeys": [{"pubkey": "Sender"}, {"pubkey": nw.SOLANA_ADDRESS}]}}}
        res = chains.inspect_solana("sig", FakeRpc({"getTransaction": tx}))
        self.assertEqual(res["transfers"][0]["amount_units"], 1000)
        tx["transaction"]["message"]["accountKeys"] = [{"pubkey": nw.SOLANA_ADDRESS}, {"pubkey": "X"}]
        self.assertEqual(chains.inspect_solana("sig", FakeRpc({"getTransaction": tx}))["transfers"], [])


@unittest.skipUnless(TEST_DB, "ONCHAIN_TEST_DATABASE_URL not set")
class DbTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"ONCHAIN_DATABASE_URL": TEST_DB, "ALCHEMY_API_KEY": "k"})
        self.env.start()
        store._initialized = False
        with store.conn() as c, c.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS onchain_customer_settlements, onchain_settings, onchain_audit, onchain_invoices, onchain_raw_events, "
                        "onchain_tx_hints, onchain_transfers, onchain_test_ledger, onchain_review, onchain_checkpoints")
        store.init_schema(force=True)
        # monitoring started 1h ago so "now" transfers are eligible
        store.set_checkpoint("monitoring_started_at:ethereum", (now() - timedelta(hours=1)).isoformat())
        store.set_checkpoint("monitoring_started_at:tron", (now() - timedelta(hours=1)).isoformat())
        self.notes = []
        self.eng = engine.Engine(notify=self.notes.append)

    def tearDown(self):
        self.env.stop()

    def inv(self, asset="usdt_erc20", usd="1"):
        return engine.create_admin_test_invoice(ADMIN, asset, Decimal(usd), is_admin, rate_fn=lambda _: Decimal("2000"))

    def fake_found(self, inv, final=True, block_time=None, amount=None, asset=None, locator="log:1", confs=12):
        return {"status": "found", "final": final, "confirmations": confs, "block_number": 100,
                "block_time": block_time or now(), "error": None,
                "transfers": [{"asset_id": asset or inv["asset_id"], "locator": locator,
                               "to_address": inv["destination"], "from_address": "0xsender",
                               "amount_units": int(amount if amount is not None else inv["expected_units"])}]}

    def run_hint(self, network, txid, result):
        store.add_hint(network, txid, "test")
        with patch.object(chains, "inspect_tx", return_value=result):
            self.eng.process_hint(network, txid, 0)

    def ledger_count(self):
        with store.conn() as c, c.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM onchain_test_ledger")
            return cur.fetchone()["n"]

    # --- flags & gates
    def test_defaults_off_and_customer_gate_closed(self):
        self.assertIs(store.get_setting("customer_new_payments_enabled"), False)
        for k in nw.NETWORKS:
            self.assertIs(store.get_setting(f"network_enabled:{k}"), False)
        for a in nw.ASSET_ORDER:
            self.assertEqual(engine.customer_gate(a), (False, "master_off"))
        with self.assertRaises(engine.GateClosed):
            engine.create_customer_invoice(USER, "usdt_trc20", Decimal("5"))

    def test_customer_release_is_required_even_when_flags_on(self):
        inv = self.inv()
        self.run_hint("ethereum", "0x" + "a1" * 32, self.fake_found(inv))
        store.set_setting("network_enabled:ethereum", True, ADMIN)
        store.set_setting("customer_new_payments_enabled", True, ADMIN)
        self.assertEqual(engine.customer_gate("usdt_erc20"), (False, "checkout_off"))
        with self.assertRaises(engine.GateClosed):
            engine.create_customer_invoice(USER, "usdt_erc20", Decimal("5"))
        with patch.dict(os.environ, {"ONCHAIN_CUSTOMER_CHECKOUT_ENABLED": "true"}):
            self.assertEqual(engine.customer_gate("usdt_erc20"), (True, "ok"))
            self.assertEqual(engine.customer_gate("eth")[1], "asset_not_ready")
            inv = engine.create_customer_invoice(USER, "usdt_erc20", Decimal("5"))
            self.assertEqual(inv["purpose"], "customer")
        with patch.dict(os.environ, {"ONCHAIN_EMERGENCY_DISABLE": "true"}):
            self.assertEqual(engine.customer_gate("usdt_erc20"), (False, "emergency_disabled"))

    def test_non_admin_cannot_create_test_invoice_or_hint(self):
        with self.assertRaises(engine.GateClosed):
            engine.create_admin_test_invoice(USER, "usdt_erc20", Decimal("1"), is_admin)
        self.assertFalse(self.eng.add_admin_hint(USER, "ethereum", "0x" + "1" * 64, is_admin))

    def test_network_enable_requires_successful_test(self):
        self.assertFalse(engine.can_enable_network("ethereum")[0])

    # --- attribution & crediting
    def test_unique_amounts_concurrent(self):
        with ThreadPoolExecutor(8) as ex:
            invs = list(ex.map(lambda _: self.inv(), range(24)))
        self.assertEqual(len({i["expected_units"] for i in invs}), 24)
        q = 10 ** (6 - 4)
        self.assertTrue(all(int(i["expected_units"]) % q == 0 for i in invs))

    def test_happy_path_credits_test_ledger_only_and_replay_is_idempotent(self):
        inv = self.inv()
        tx = "0x" + "b2" * 32
        self.run_hint("ethereum", tx, self.fake_found(inv))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")
        self.assertEqual(self.ledger_count(), 1)
        for _ in range(3):  # duplicate webhook / reconcile replays
            with patch.object(chains, "inspect_tx", return_value=self.fake_found(inv)):
                self.eng.process_hint("ethereum", tx, 0)
        self.assertEqual(self.ledger_count(), 1)
        with store.conn() as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('public.users') AS t")
            self.assertIsNone(cur.fetchone()["t"])  # module never created/touched customer tables

    def test_insufficient_finality_then_final(self):
        inv = self.inv()
        tx = "0x" + "c3" * 32
        self.run_hint("ethereum", tx, self.fake_found(inv, final=False, confs=3))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "detected")
        self.assertEqual(self.ledger_count(), 0)
        with patch.object(chains, "inspect_tx", return_value=self.fake_found(inv)):
            self.eng.process_hint("ethereum", tx, 1)
        self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")

    def test_reorg_releases_invoice(self):
        inv = self.inv()
        tx = "0x" + "d4" * 32
        self.run_hint("ethereum", tx, self.fake_found(inv, final=False, confs=2))
        with patch.object(chains, "inspect_tx", return_value={"status": "not_found", "transfers": [], "final": False}):
            self.eng.process_hint("ethereum", tx, 1)
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")
        self.assertEqual(store.get_transfer(f"ethereum:{tx}:log:1")["status"], "dropped")
        self.assertEqual(self.ledger_count(), 0)

    def test_failed_tx_never_credits(self):
        inv = self.inv()
        self.run_hint("ethereum", "0x" + "e5" * 32, {"status": "failed", "transfers": [], "final": False,
                                                      "error": "execution_failed"})
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")

    def test_historical_transfer_never_credited(self):
        inv = self.inv()
        self.run_hint("ethereum", "0x" + "f6" * 32, self.fake_found(inv, block_time=now() - timedelta(days=30)))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")
        self.assertEqual(self.ledger_count(), 0)

    def test_public_txid_from_before_invoice_cannot_be_claimed(self):
        # Someone sees an earlier public tx with amount X and then asks for an invoice of amount X.
        inv = self.inv()
        self.run_hint("ethereum", "0x" + "17" * 32,
                      self.fake_found(inv, block_time=inv["created_at"] - timedelta(minutes=5)))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")
        self.assertEqual(store.list_review()[0]["reason"], "transfer_before_invoice")

    def test_amount_mismatch_wrong_asset_and_unmatched_go_to_review(self):
        inv = self.inv()
        self.run_hint("ethereum", "0x" + "28" * 32, self.fake_found(inv, amount=int(inv["expected_units"]) - 1))
        self.run_hint("ethereum", "0x" + "29" * 32, self.fake_found(inv, asset="eth", locator="native"))
        reasons = sorted(r["reason"] for r in store.list_review())
        self.assertEqual(reasons, ["amount_mismatch", "unmatched"])
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")

    def test_same_address_text_eth_vs_bsc_isolated(self):
        inv = self.inv("usdt_erc20")
        # A BSC USDT transfer with the identical unit count must not pay the Ethereum invoice.
        res = self.fake_found(inv, asset="usdt_bep20")
        store.set_checkpoint("monitoring_started_at:bsc", (now() - timedelta(hours=1)).isoformat())
        self.run_hint("bsc", "0x" + "3a" * 32, res)
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")

    def test_late_payment_after_expiry_goes_to_review(self):
        inv = self.inv()
        with store.conn() as c, c.cursor() as cur:
            cur.execute("UPDATE onchain_invoices SET expires_at = NOW() - interval '1 minute' WHERE id=%s", (inv["id"],))
        store.expire_invoices()
        self.run_hint("ethereum", "0x" + "4b" * 32, self.fake_found(inv))
        self.assertEqual(store.list_review()[0]["reason"], "late_payment")

    def test_sample_webhook_event_never_credits(self):
        # Dashboard "test webhook" deliveries carry made-up hashes that the chain does not know.
        inv = self.inv()
        store.store_raw_event("alchemy", "ethereum", "wh", "sample-1",
                              {"event": {"network": "ETH_MAINNET", "activity": [{"hash": "0x" + "5c" * 32}]}})
        self.eng.ingest_raw_events()
        with patch.object(chains, "inspect_tx", return_value={"status": "not_found", "transfers": [], "final": False}):
            self.eng.process_hints()
        self.assertEqual(store.get_invoice(inv["id"])["status"], "awaiting")
        self.assertEqual(self.ledger_count(), 0)

    def test_duplicate_raw_event_stored_once(self):
        self.assertTrue(store.store_raw_event("alchemy", "ethereum", "wh", "dup", {"event": {}}))
        self.assertFalse(store.store_raw_event("alchemy", "ethereum", "wh", "dup", {"event": {}}))

    def test_concurrent_credit_single_ledger_row(self):
        inv = self.inv()
        tx = "0x" + "6d" * 32
        self.run_hint("ethereum", tx, self.fake_found(inv, final=False))
        uid = f"ethereum:{tx}:log:1"
        barrier = threading.Barrier(6)

        def go(_):
            barrier.wait()
            return store.credit_final_transfer(uid, 12)["result"]
        with ThreadPoolExecutor(6) as ex:
            results = list(ex.map(go, range(6)))
        self.assertEqual(results.count("credited"), 1)
        self.assertEqual(self.ledger_count(), 1)

    def test_rollback_does_not_stop_tracking_issued_invoices(self):
        inv = self.inv()
        store.set_setting("new_invoices_paused", True, ADMIN)
        store.set_setting("customer_new_payments_enabled", False, ADMIN)
        self.run_hint("ethereum", "0x" + "7e" * 32, self.fake_found(inv))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")

    def test_settings_changes_are_audited(self):
        store.set_setting("network_enabled:tron", True, ADMIN)
        a = store.recent_audit(1)[0]
        self.assertEqual((a["actor"], a["action"]), (ADMIN, "setting_changed"))

    def test_quota_errors_retry_with_backoff(self):
        store.add_hint("ethereum", "0xq", "test")
        with patch.object(chains, "inspect_tx", return_value={"status": "error", "error": "http_429", "transfers": []}):
            self.eng.process_hint("ethereum", "0xq", 0)
        with store.conn() as c, c.cursor() as cur:
            cur.execute("SELECT status, next_check_at > NOW() AS later FROM onchain_tx_hints WHERE txid='0xq'")
            r = cur.fetchone()
        self.assertEqual((r["status"], r["later"]), ("retry", True))

    def test_evm_poller_starts_at_head_no_backfill(self):
        fake = FakeRpc({"eth_blockNumber": "0x100", "alchemy_getAssetTransfers": {"transfers": [{"hash": "0xold"}]}})
        self.eng.discovery_client = fake
        self.eng.poll_evm("ethereum")
        self.assertEqual(store.get_checkpoint("evm_block:ethereum"), 256)
        self.assertEqual(store.recent_hints(), [])

    # --- admin UI authorization
    def test_admin_callbacks_refuse_normal_users(self):
        from onchain import admin_ui
        sent = []

        class Q:
            from_user = SimpleNamespace(id=USER)

            async def edit_message_text(self, text, **kw):
                sent.append(text)
        for data in ("oc:master:confirm", "oc:new:usdt_erc20:5", "oc:nets:ethereum:on", "oc:pause"):
            asyncio.run(admin_ui.callback(Q(), data, is_admin))
        self.assertTrue(all("not allowed" in s for s in sent))
        self.assertIs(store.get_setting("customer_new_payments_enabled"), False)
        self.assertEqual(store.list_invoices(), [])

    def test_admin_master_requires_successful_test(self):
        from onchain import admin_ui
        sent = []

        class Q:
            from_user = SimpleNamespace(id=ADMIN)

            async def edit_message_text(self, text, **kw):
                sent.append(text)
        asyncio.run(admin_ui.callback(Q(), "oc:master:confirm", is_admin))
        self.assertIn("Refused", sent[-1])
        self.assertIs(store.get_setting("customer_new_payments_enabled"), False)
        asyncio.run(admin_ui.callback(Q(), "oc:home", is_admin))
        self.assertIn("Cryptomus", sent[-1])


class BotIntegrationTests(unittest.TestCase):
    """The existing bot keeps its behaviour; customers see nothing new."""

    @classmethod
    def setUpClass(cls):
        import bot
        cls.bot = bot

    def test_customer_payment_keyboards_have_no_onchain_entries(self):
        for prefix in ("deposit", "buy"):
            kb = self.bot.payment_method_keyboard(prefix, user_id=USER)
            data = [b.callback_data or "" for row in kb.inline_keyboard for b in row]
            self.assertFalse(any(d.startswith("oc:") for d in data))
            text = " ".join(b.text for row in kb.inline_keyboard for b in row)
            self.assertNotIn(nw.ETHEREUM_ADDRESS, text)

    def test_http_routes(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), self.bot.NowPaymentsWebhookHandler)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            def post(path, body=b"{}", headers=None):
                req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, method="POST",
                                             headers=headers or {"Content-Type": "application/json"})
                try:
                    return urllib.request.urlopen(req, timeout=5).status
                except urllib.error.HTTPError as e:
                    return e.code
            # Cryptomus route still handled exactly as before (unsigned -> 401)
            self.assertEqual(post(self.bot.CRYPTOMUS_WEBHOOK_PATH, b'{"sign":"x"}'), 401)
            with patch.dict(os.environ, {"ALCHEMY_SIGNING_KEY_ETHEREUM": "k"}):
                self.assertEqual(post("/alchemy/webhook/ethereum", b'{"event":{}}'), 401)
            self.assertEqual(post("/alchemy/webhook/bsc", b"{}"), 503)
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/onchain/health", timeout=5) as r:
                self.assertEqual(json.loads(r.read())["onchain_module"], True)
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
