"""Independent TRON discovery during indexer outages; no real payments."""
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from onchain import chains, networks as nw, store, tron_scan


class TronScanTests(unittest.TestCase):
    def block(self, height, transactions=None):
        return {"block_header": {"raw_data": {"number": height, "timestamp": 1000 + height * 3000}},
                "transactions": transactions or []}

    def test_native_and_usdt_receipts_supply_hash_hints_only(self):
        tx = {"txID": "a" * 64, "raw_data": {"contract": [{"type": "TransferContract",
               "parameter": {"value": {"to_address": nw.tron_to_hex(nw.TRON_ADDRESS), "amount": 19686200}}}]}}
        call = {"txID": "b" * 64, "raw_data": {"contract": [{"type": "TriggerSmartContract"}]}}
        log = {"address": nw.tron_to_hex(nw.USDT_TRC20_CONTRACT),
               "topics": [nw.ERC20_TRANSFER_TOPIC[2:], "0" * 64,
                          "0" * 24 + nw.tron_to_hex(nw.TRON_ADDRESS)[-40:]], "data": "01"}
        client = Mock()
        client.post.side_effect = [{"block": [self.block(10, [tx, call])]}, [{"id": "b" * 64, "log": [log]}]]
        self.assertEqual(tron_scan.discover_blocks(client, 10, 10), ["a" * 64, "b" * 64])
        self.assertTrue(all(c.kwargs["retries"] == 0 for c in client.post.call_args_list))

    def test_incomplete_ranges_and_missing_receipts_fail_closed(self):
        client = Mock()
        client.post.return_value = {"block": [self.block(10)]}
        with self.assertRaisesRegex(chains.ChainError, "incomplete"):
            tron_scan.discover_blocks(client, 10, 11)
        client.post.side_effect = [{"block": [self.block(10, [{"raw_data": {
            "contract": [{"type": "TriggerSmartContract"}]}}])]}, []]
        with self.assertRaisesRegex(chains.ChainError, "receipts_missing"):
            tron_scan.discover_blocks(client, 10, 10)

    def test_wrong_token_or_recipient_is_not_discovered(self):
        client = Mock()
        call = {"raw_data": {"contract": [{"type": "TriggerSmartContract"}]}}
        client.post.side_effect = [{"block": [self.block(10, [call])]}, [{"id": "a" * 64, "log": [
            {"address": "41" + "ab" * 20, "topics": [nw.ERC20_TRANSFER_TOPIC[2:], "0" * 64,
                "0" * 24 + nw.tron_to_hex(nw.TRON_ADDRESS)[-40:]]}]}]]
        self.assertEqual(tron_scan.discover_blocks(client, 10, 10), [])

    def test_block_timestamp_search_covers_invoice_boundary(self):
        client = Mock()
        client.post.side_effect = lambda url, payload, *args, **kwargs: self.block(payload["num"])
        self.assertEqual(tron_scan.block_at_time(client, 100, 301000), 99)
        with self.assertRaises(chains.ChainError):
            tron_scan.discover_blocks(client, 0, 40)

    def test_idle_wallet_does_not_scan_the_entire_chain(self):
        client = Mock()
        with patch.object(store, "earliest_open_invoice_time", return_value=None):
            tron_scan.poll(client)
        client.post.assert_not_called()

    def test_failed_range_does_not_advance_checkpoint(self):
        oldest = datetime.now(timezone.utc)
        with patch.object(store, "earliest_open_invoice_time", return_value=oldest), \
                patch.object(store, "get_checkpoint", side_effect=lambda k: 10 if k == "tron_solid_block" else oldest.isoformat()), \
                patch.object(store, "set_checkpoint") as checkpoint, \
                patch.object(tron_scan, "solid_head", return_value=20), \
                patch.object(tron_scan, "discover_blocks", side_effect=chains.ChainError("http_429")):
            with self.assertRaises(chains.ChainError):
                tron_scan.poll(Mock())
        checkpoint.assert_not_called()

    def test_restart_reuses_cursor_and_queues_each_transfer_normally(self):
        oldest = datetime.now(timezone.utc)
        with patch.object(store, "earliest_open_invoice_time", return_value=oldest), \
                patch.object(store, "get_checkpoint", side_effect=lambda k: 10 if k == "tron_solid_block" else oldest.isoformat()), \
                patch.object(store, "set_checkpoint") as checkpoint, \
                patch.object(store, "add_hint") as hint, \
                patch.object(tron_scan, "solid_head", return_value=20), \
                patch.object(tron_scan, "block_at_time") as binary, \
                patch.object(tron_scan, "discover_blocks", return_value=["a" * 64]):
            tron_scan.poll(Mock())
        binary.assert_not_called()
        hint.assert_called_once_with("tron", "a" * 64, "solid_block_scan")
        checkpoint.assert_called_once_with("tron_solid_block", 20)


if __name__ == "__main__":
    unittest.main()
