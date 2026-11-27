"""All-network recovery tests; supplier/blockchain/Telegram calls are mocked."""
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from onchain import chains, engine, networks as nw, recovery, store, tron_scan
from test_onchain import TEST_DB, now
import test_onchain as onchain_cases


class NetworkRecoveryTests(unittest.TestCase):
    def test_evm_drains_all_transfer_pages_and_keeps_categories_separate(self):
        client = Mock()
        client.rpc.side_effect = [{"transfers": [{"hash": "a"}], "pageKey": "next"},
                                  {"transfers": [{"hash": "b"}]}, {"transfers": [{"hash": "c"}]}]
        self.assertEqual(chains.discover_evm("bsc", client, 1, 2), ["a", "b", "c"])
        calls = client.rpc.call_args_list
        self.assertEqual(calls[1].args[2][0]["pageKey"], "next")
        self.assertNotIn("pageKey", calls[2].args[2][0])

    def test_evm_stalled_pagination_is_an_error_instead_of_skipping_transfers(self):
        client = Mock()
        client.rpc.return_value = {"transfers": [], "pageKey": "same"}
        with self.assertRaisesRegex(chains.ChainError, "pagination_stalled"):
            chains.discover_evm("ethereum", client, 1, 2)

    def test_raw_evm_finds_native_and_token_hashes_without_transfer_indexer(self):
        client = Mock()
        def rpc(url, method, params):
            if method == "eth_getBlockByNumber":
                return {"number": params[0], "timestamp": "0x64", "hash": "block", "transactions": [
                    {"hash": "native", "to": nw.BSC_ADDRESS, "value": "0x1"},
                    {"hash": "wrong", "to": "0xother", "value": "0x1"}]}
            self.assertEqual(method, "eth_getLogs")
            return [{"transactionHash": "token"}]
        client.rpc.side_effect = rpc
        self.assertEqual(recovery.evm_hashes(client, "bsc", 1, 2, ["bnb", "usdt_bep20"]), ["native", "token"])

    def test_btc_and_ltc_raw_outputs_are_verified_before_becoming_hints(self):
        for network in ("bitcoin", "litecoin"):
            client = Mock()
            client.rpc.side_effect = lambda url, method, params: (
                "block" if method == "getblockhash" else {"hash": "block", "height": 10, "time": 100,
                    "tx": [{"txid": "incoming", "vout": [{"scriptPubKey": {"address": nw.NETWORKS[network].address}}]}]})
            self.assertEqual(recovery.utxo_hashes(client, network, 10, 10), ["incoming"])
            client.rpc.side_effect = None
            client.rpc.return_value = None
            with self.assertRaises(chains.ChainError):
                recovery.utxo_hashes(client, network, 10, 10)

    def test_recovery_does_not_read_rpc_without_invoice_window(self):
        client = Mock()
        with patch.object(store, "recovery_invoice_window", return_value={"started": None}):
            recovery.poll(client, "litecoin")
        client.rpc.assert_not_called()

    def test_failed_recovery_range_keeps_existing_cursor(self):
        start = now() - timedelta(minutes=10)
        window = {"started": start, "ended": now(), "assets": ["ltc"], "active_started": start}
        with patch.object(store, "recovery_invoice_window", return_value=window), \
                patch.object(store, "get_checkpoint", return_value={"since": start.isoformat(), "next": 10}), \
                patch.object(store, "set_checkpoint") as save, patch.object(recovery, "head", return_value=20), \
                patch.object(recovery, "utxo_hashes", side_effect=chains.ChainError("unavailable")):
            with self.assertRaises(chains.ChainError):
                recovery.poll(Mock(), "litecoin")
        save.assert_not_called()

    def test_completed_window_does_not_rescan_forever(self):
        start, end = now() - timedelta(hours=2), now() - timedelta(hours=1)
        with patch.object(store, "recovery_invoice_window", return_value={"started": start, "ended": end}), \
                patch.object(store, "get_checkpoint", return_value={"since": start.isoformat(), "covered_end": end.isoformat()}):
            client = Mock()
            recovery.poll(client, "ethereum")
        client.rpc.assert_not_called()

    def test_error_redaction_and_short_discovery_timeout(self):
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "long-secret-test-key"}):
            self.assertNotIn("long-secret-test-key", chains.safe_error("Error https://eth.example/v2/long-secret-test-key"))
        session = Mock()
        session.post.return_value = Mock(status_code=429)
        client = chains.RpcClient(session, timeout=8, retries=0)
        with self.assertRaises(chains.ChainError):
            client.rpc("https://rpc.test", "method", [])
        session.post.assert_called_once()
        self.assertEqual(session.post.call_args.kwargs["timeout"], 8)

    def test_repeated_poller_error_alert_is_throttled_and_recovery_is_reported(self):
        notes, checkpoints = [], {}
        eng = engine.Engine(notify=notes.append)
        eng.poll_tron = Mock(side_effect=chains.ChainError("http_429"))
        eng.poll_solana, eng.poll_utxo, eng.poll_evm = Mock(), Mock(), Mock()
        with patch.object(eng, "_due", return_value=True), patch.object(tron_scan, "poll"), \
                patch.object(recovery, "poll"), \
                patch.object(store, "get_checkpoint", side_effect=lambda k, default=None: checkpoints.get(k, default)), \
                patch.object(store, "set_checkpoint", side_effect=lambda k, v: checkpoints.update({k: v})):
            for timestamp in (10000, 10150, 10500):
                with patch.object(engine.time, "time", return_value=timestamp):
                    eng.run_pollers()
            self.assertEqual(len(notes), 1)
            eng.poll_tron = Mock()
            with patch.object(engine.time, "time", return_value=11200):
                eng.run_pollers()
            self.assertEqual(len(notes), 2)
            self.assertIn("recovered", notes[-1])

    def test_readonly_canary_records_each_network_and_isolates_one_failure(self):
        results = {}
        client = Mock()
        def rpc(url, method, params):
            if method == "eth_chainId":
                return "0x38" if "bnb-mainnet" in url else "0x1"
            if method == "getSlot":
                return 100
            if method == "getSignaturesForAddress":
                return []
            raise AssertionError(method)
        client.rpc.side_effect = rpc
        def read_block(client, network, height, full=False):
            if network == "litecoin":
                raise chains.ChainError("http_429")
            return {}, 100
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "canary-test-key"}), \
                patch.object(chains, "RpcClient", return_value=client), \
                patch.object(recovery, "head", return_value=100), patch.object(recovery, "block", side_effect=read_block), \
                patch.object(recovery, "evm_hashes", return_value=[]), \
                patch.object(tron_scan, "solid_head", return_value=100), patch.object(tron_scan, "discover_blocks", return_value=[]), \
                patch.object(store, "set_checkpoint", side_effect=lambda k, v: results.update({k: v})):
            recovery.probe_all()
        self.assertEqual(len(results), 6)
        self.assertFalse(results["rpc_probe:litecoin"]["ok"])
        self.assertTrue(results["rpc_probe:solana"]["ok"])
        self.assertTrue(all(result["ok"] for key, result in results.items() if "litecoin" not in key))


@unittest.skipUnless(TEST_DB, "disposable PostgreSQL required")
class PaymentRecoveryDbTests(unittest.TestCase):
    setUp, tearDown = onchain_cases.DbTests.setUp, onchain_cases.DbTests.tearDown
    inv, fake_found, run_hint, ledger_count = (onchain_cases.DbTests.inv, onchain_cases.DbTests.fake_found,
                                             onchain_cases.DbTests.run_hint, onchain_cases.DbTests.ledger_count)

    def test_on_time_transfer_discovered_after_invoice_expiry_is_paid_once(self):
        inv = self.inv()
        with store.conn() as c:
            c.execute("UPDATE onchain_invoices SET created_at=NOW()-interval '10 minutes', "
                      "expires_at=NOW()-interval '5 minutes' WHERE id=%s", (inv["id"],))
        store.expire_invoices()
        result = self.fake_found(inv, block_time=now() - timedelta(minutes=7))
        self.run_hint("ethereum", "0x" + "a1" * 32, result)
        self.run_hint("ethereum", "0x" + "a1" * 32, result)
        self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")
        self.assertEqual(self.ledger_count(), 1)

    def test_cancelled_invoice_stays_unpayable(self):
        inv = self.inv()
        store.cancel_invoice(inv["id"], 1)
        self.run_hint("ethereum", "0x" + "b2" * 32, self.fake_found(inv))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "cancelled")
        self.assertEqual(self.ledger_count(), 0)

    def test_transfer_reappearing_after_reorg_can_finish_normally(self):
        inv, tx = self.inv(), "0x" + "c3" * 32
        self.run_hint("ethereum", tx, self.fake_found(inv, final=False))
        with patch.object(chains, "inspect_tx", return_value={"status": "not_found", "transfers": [], "final": False}):
            self.eng.process_hint("ethereum", tx, 1)
        self.run_hint("ethereum", tx, self.fake_found(inv))
        self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")
        self.assertEqual(self.ledger_count(), 1)

    def test_fresh_discovery_can_retry_exhausted_hint_without_resetting_completed_hint(self):
        tx = "0x" + "d4" * 32
        store.add_hint("ethereum", tx, "test")
        store.update_hint("ethereum", tx, "gave_up", "http_429")
        with store.conn() as c:
            c.execute("UPDATE onchain_tx_hints SET updated_at=NOW()-interval '11 minutes' WHERE txid=%s", (tx,))
        self.assertTrue(store.add_hint("ethereum", tx, "rpc_block_recovery"))
        self.assertEqual(store.recent_hints(1)[0]["status"], "new")
        store.update_hint("ethereum", tx, "done")
        self.assertFalse(store.add_hint("ethereum", tx, "replay"))

    def test_periodic_recovery_retries_stopped_hints_but_preserves_done(self):
        for index, status in enumerate(("not_found", "gave_up", "done", "failed")):
            tx = "0x" + str(index) * 64
            store.add_hint("ethereum", tx, "test")
            store.update_hint("ethereum", tx, status)
        with store.conn() as c:
            c.execute("UPDATE onchain_tx_hints SET updated_at=NOW()-interval '11 minutes'")
        self.assertEqual(store.retry_stale_hints(), 2)
        self.assertEqual(sorted(h["status"] for h in store.recent_hints()), ["done", "failed", "new", "new"])


if __name__ == "__main__":
    unittest.main()
