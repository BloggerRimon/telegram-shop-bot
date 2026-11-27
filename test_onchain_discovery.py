"""Discovery regression tests: first deposits, pagination, outages and independent checks."""
import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from onchain import chains, engine, networks as nw, store


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"ALCHEMY_API_KEY": "test"})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def test_empty_solana_wallet_first_deposit_is_queued(self):
        checkpoints = {"solana_last_sig": ""}
        client = Mock()
        client.rpc.return_value = [{"signature": "first", "err": None}]
        eng = engine.Engine(client=client)
        with patch.object(store, "get_checkpoint", side_effect=lambda k: checkpoints.get(k)), \
                patch.object(store, "set_checkpoint", side_effect=lambda k, v: checkpoints.update({k: v})), \
                patch.object(store, "add_hint") as add:
            eng.poll_solana()
        add.assert_called_once_with("solana", "first", "poller")
        self.assertEqual(checkpoints["solana_last_sig"], "first")

    def test_solana_pages_all_signatures_before_advancing(self):
        client = Mock()
        client.rpc.side_effect = [
            [{"signature": "new", "err": None}, {"signature": "middle", "err": None}],
            [{"signature": "old", "err": None}]]
        self.assertEqual(chains.discover_solana(client, "previous", limit=2), ["new", "middle", "old"])
        self.assertEqual(client.rpc.call_args_list[1].args[2][1]["before"], "middle")

    def test_utxo_index_is_paginated_and_only_valid_hashes_are_hints(self):
        client = Mock()
        client.get.side_effect = [
            {"address": nw.LITECOIN_ADDRESS, "hasMore": True,
             "txs": [{"hash": "a" * 64, "block_height": 110}, {"hash": "invalid", "block_height": 109}]},
            {"address": nw.LITECOIN_ADDRESS,
             "txs": [{"hash": "b" * 64, "block_height": 108}, {"hash": "c" * 64, "block_height": -1}]}]
        found = chains.discover_utxo("litecoin", client, 100)
        self.assertEqual(found, {"txids": ["a" * 64, "b" * 64, "c" * 64], "height": 110})
        self.assertEqual(client.get.call_args_list[1].args[1]["before"], 109)

    def test_utxo_wrong_address_rejected_and_checkpoint_unchanged_on_failure(self):
        client = Mock()
        client.get.return_value = {"address": nw.BITCOIN_ADDRESS, "txs": []}
        with self.assertRaises(chains.ChainError):
            chains.discover_utxo("litecoin", client, 100)
        eng = engine.Engine(client=client)
        with patch.object(store, "get_checkpoint", return_value=100), \
                patch.object(store, "set_checkpoint") as checkpoint:
            with self.assertRaises(chains.ChainError):
                eng.poll_utxo("litecoin")
        checkpoint.assert_not_called()

    def test_tron_pages_tokens_and_native_independently(self):
        client = Mock()
        client.get.side_effect = [
            {"success": True, "data": [{"transaction_id": "t1", "block_timestamp": 101}],
             "meta": {"fingerprint": "token-next"}},
            {"success": True, "data": [{"transaction_id": "t2", "block_timestamp": 102}]},
            {"success": True, "data": [{"txID": "n1", "block_timestamp": 100}],
             "meta": {"fingerprint": "native-next"}},
            {"success": True, "data": [{"txID": "n2", "block_timestamp": 103}]}]
        found = chains.discover_tron(client, 99)
        self.assertEqual([r["txid"] for r in found], ["t1", "t2", "n1", "n2"])
        calls = client.get.call_args_list
        self.assertEqual(calls[1].args[1]["fingerprint"], "token-next")
        self.assertNotIn("fingerprint", calls[2].args[1])
        self.assertEqual(calls[3].args[1]["fingerprint"], "native-next")

    def test_tron_error_response_does_not_advance_cursor(self):
        client = Mock()
        client.get.return_value = {"success": False, "error": "rate limited"}
        eng = engine.Engine(client=client)
        with patch.object(store, "get_checkpoint", return_value=100), \
                patch.object(store, "monitoring_started_at", return_value=datetime(1970, 1, 1, tzinfo=timezone.utc)), \
                patch.object(store, "set_checkpoint") as checkpoint:
            with self.assertRaises(chains.ChainError):
                eng.poll_tron()
        checkpoint.assert_not_called()

    def test_one_rate_limited_poller_does_not_stop_other_networks(self):
        eng = engine.Engine()
        eng.poll_tron = Mock(side_effect=chains.ChainError("http_429"))
        eng.poll_solana = Mock()
        eng.poll_utxo = Mock()
        eng.poll_evm = Mock()
        with patch.object(eng, "_due", return_value=True), patch.object(engine.time, "time", return_value=1000):
            eng.run_pollers()
            eng.run_pollers()
        self.assertEqual(eng.poll_tron.call_count, 1)
        self.assertEqual(eng.poll_evm.call_count, 4)
        self.assertEqual(eng.poller_errors["tron"], "http_429")

    def test_http_error_json_is_not_treated_as_empty_success(self):
        session = Mock()
        session.get.return_value = SimpleNamespace(status_code=403, json=lambda: {"error": "denied"})
        with self.assertRaisesRegex(chains.ChainError, "http_403"):
            chains.RpcClient(session).get("https://example.test", retries=0)

    def test_litecoin_ready_still_requires_paid_admin_test(self):
        self.assertFalse(engine.asset_ready("ltc", paid_assets=set()))
        self.assertTrue(engine.asset_ready("ltc", paid_assets={"ltc"}))


if __name__ == "__main__":
    unittest.main()
