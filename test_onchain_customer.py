"""Customer gateway integration: real disposable PG, mocked chain/Telegram only."""
import asyncio
import copy
import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BOT_TOKEN", "1:TEST")
import bot
from onchain import customer, engine, store, chains, networks as nw

TEST_DB = os.getenv("ONCHAIN_TEST_DATABASE_URL", "")


@unittest.skipUnless(TEST_DB, "disposable PostgreSQL required")
class CustomerGatewayTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {
            "ONCHAIN_DATABASE_URL": TEST_DB, "ALCHEMY_API_KEY": "test",
            "ALCHEMY_SIGNING_KEY_BSC": "test", "ONCHAIN_CUSTOMER_CHECKOUT_ENABLED": "true",
            "ONCHAIN_OWNER_APPROVED_ASSETS": "",
            "ONCHAIN_CUSTOMER_LAUNCH": "", "ONCHAIN_EMERGENCY_DISABLE": "false"})
        self.env.start()
        with store.conn() as c:
            c.execute("DROP TABLE IF EXISTS onchain_payment_reports, onchain_delay_alerts, onchain_manual_receipts, onchain_manual_drafts, onchain_customer_settlements, onchain_settings, onchain_audit, "
                      "onchain_invoices, onchain_raw_events, onchain_tx_hints, onchain_transfers, "
                      "onchain_test_ledger, onchain_review, onchain_checkpoints")
        store._initialized = False
        store.init_schema()
        store.set_checkpoint("monitoring_started_at:bsc", (datetime.now(timezone.utc)-timedelta(hours=1)).isoformat())
        self.eng = engine.Engine()
        self.patches = []
        for name, value in {"user_wallet": {}, "user_transactions": {}, "user_orders": {},
                            "all_transactions": [], "all_orders": [], "credited_topups": {},
                            "PRODUCTS": {}, "global_order_id": 1, "global_tx_id": 1,
                            "user_profiles": {}}.items():
            self.patch(name, value)
        self.patch("force_save_bot_state", lambda: None)
        self.patch("schedule_admin_topup_credit_alert", lambda r: None)
        self.patch("save_bot_state_strict", lambda: None)
        self.sender = AsyncMock()
        self.patch("app_instance", SimpleNamespace(bot=SimpleNamespace(send_message=self.sender)))
        self.patch("send_html_lines", AsyncMock())
        self.patch("get_delivery_guide", lambda pid: "")
        customer._settlement_lock = None

    def patch(self, name, value):
        p = patch.object(bot, name, value)
        p.start()
        self.patches.append(p)

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        customer._settlement_lock = None
        self.env.stop()

    def found(self, inv):
        return {"status": "found", "final": True, "confirmations": 25, "block_number": 100,
                "block_time": datetime.now(timezone.utc), "error": None,
                "transfers": [{"asset_id": inv["asset_id"], "locator": "log:1",
                               "to_address": inv["destination"], "from_address": "0xsender",
                               "amount_units": int(inv["expected_units"])}]}

    def confirm(self, inv, txid):
        store.add_hint("bsc", txid, "test")
        with patch.object(chains, "inspect_tx", return_value=self.found(inv)):
            self.eng.process_hint("bsc", txid, 0)

    def ready(self):
        inv = engine.create_admin_test_invoice(1, "usdt_bep20", Decimal("1"), lambda u: u == 1)
        self.confirm(inv, "0x" + "a"*64)
        customer.switch_gateway("trust_wallet", 1)

    def paid_customer(self, kind="deposit"):
        self.ready()
        inv = engine.create_customer_invoice(42, "usdt_bep20", Decimal("5"), kind=kind,
                                             product_id="p" if kind == "order" else None,
                                             qty=1 if kind == "order" else None)
        self.confirm(inv, "0x" + "b"*64)
        return inv

    def test_switch_requires_real_admin_test_and_release(self):
        with self.assertRaises(engine.GateClosed):
            customer.switch_gateway("trust_wallet", 1)
        self.ready()
        self.assertEqual(customer.gateway(), "trust_wallet")
        self.assertTrue(engine.customer_gate("usdt_bep20")[0])
        self.assertFalse(engine.customer_gate("bnb")[0])
        self.assertFalse(engine.customer_gate("usdt_erc20")[0])
        with patch.dict(os.environ, {"ONCHAIN_CUSTOMER_CHECKOUT_ENABLED": "false"}):
            with self.assertRaises(engine.GateClosed):
                customer.switch_gateway("trust_wallet", 1)

    def test_bootstrap_does_not_override_later_rollback(self):
        self.ready()
        with patch.dict(os.environ, {"ONCHAIN_CUSTOMER_LAUNCH": "trust_wallet_bsc_v1"}):
            customer.bootstrap()
            customer.switch_gateway("cryptomus", 1)
            customer.bootstrap()
        self.assertEqual(customer.gateway(), "cryptomus")

    def test_deposit_credits_once_and_survives_outbox_retry(self):
        inv = self.paid_customer()
        self.assertEqual(bot.user_wallet, {})
        with patch.object(bot, "save_bot_state_strict", side_effect=RuntimeError("db unavailable")):
            asyncio.run(customer.settle_pending(bot))
        self.assertEqual(len(store.pending_customer_settlements()), 1)
        self.assertEqual(bot.user_wallet[42], 5)
        self.sender.assert_not_called()
        asyncio.run(customer.settle_pending(bot))
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(len(bot.credited_topups), 1)
        self.assertEqual(len(bot.all_transactions), 1)
        self.assertEqual(store.pending_customer_settlements(), [])
        self.assertEqual(len(store.test_ledger()), 1)  # the previous admin test only
        self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")

    def test_restart_before_credit_save_replays_safely(self):
        self.paid_customer()
        with patch.object(bot, "save_bot_state_strict", side_effect=RuntimeError("db unavailable")):
            asyncio.run(customer.settle_pending(bot))
        bot.user_wallet.clear()
        bot.credited_topups.clear()
        bot.all_transactions.clear()
        bot.user_transactions.clear()
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(len(bot.all_transactions), 1)

    def test_customer_credit_continues_after_switch_and_pause(self):
        inv = self.paid_customer()
        customer.switch_gateway("cryptomus", 1)
        store.set_setting("customer_new_payments_enabled", False, 1)
        store.set_setting("new_invoices_paused", True, 1)
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        with self.assertRaises(engine.GateClosed):
            engine.create_customer_invoice(42, "usdt_bep20", Decimal("5"))

    def test_admin_invoice_never_credits_customer_balance(self):
        self.ready()
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet, {})
        self.assertEqual(store.pending_customer_settlements(), [])

    def test_verify_refuses_other_user_and_admin_test(self):
        inv = self.paid_customer()
        query = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
        with patch.object(customer, "settle_pending", new_callable=AsyncMock) as settle:
            asyncio.run(customer.verify(bot, query, 7, inv["id"]))
            asyncio.run(customer.verify(bot, query, 1, 1))
            settle.assert_not_called()
        self.assertEqual(bot.user_wallet, {})

    def test_verify_reports_pending_settlement_until_credit_is_durable(self):
        inv = self.paid_customer()
        q = SimpleNamespace(message=SimpleNamespace(reply_text=AsyncMock()))
        with patch.object(bot, "save_bot_state_strict", side_effect=RuntimeError("db unavailable")):
            asyncio.run(customer.verify(bot, q, 42, inv["id"]))
        self.assertIn("being updated", q.message.reply_text.call_args.args[0])
        asyncio.run(customer.verify(bot, q, 42, inv["id"]))
        self.assertIn("already confirmed", q.message.reply_text.call_args.args[0])
        self.assertEqual(bot.user_wallet[42], 5)

    def test_webhook_replay_and_six_workers_make_one_customer_receipt(self):
        inv = self.paid_customer()
        def replay(_):
            with patch.object(chains, "inspect_tx", return_value=self.found(inv)):
                self.eng.process_hint("bsc", "0x"+"b"*64, 1)
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(replay, range(6)))
        self.assertEqual(len(store.pending_customer_settlements()), 1)
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)

    def test_order_delivery_failure_reuses_reserved_items(self):
        bot.PRODUCTS["p"] = {"name": "Product", "price": 5, "accounts": [{"raw_line": "mail|pass"}],
                             "display_stock": 1, "delivery_type": "automatic"}
        self.paid_customer("order")
        with patch.object(bot, "send_html_lines", new_callable=AsyncMock,
                          side_effect=[RuntimeError("Telegram unavailable"), None]) as send:
            asyncio.run(customer.settle_pending(bot))
            asyncio.run(customer.settle_pending(bot))
            self.assertEqual(send.call_count, 2)
            self.assertEqual(send.call_args_list[0], send.call_args_list[1])
        self.assertEqual(bot.user_wallet[42], 0)
        self.assertEqual(len(bot.all_orders), 1)
        self.assertEqual(bot.PRODUCTS["p"]["accounts"], [])
        self.assertEqual(bot.PRODUCTS["p"]["display_stock"], 0)
        self.assertEqual(store.pending_customer_settlements(), [])

    def test_out_of_stock_keeps_paid_funds_in_wallet(self):
        self.paid_customer("order")
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(bot.all_orders, [])
        self.assertIn("product is unavailable", self.sender.call_args.args[1])

    def test_customer_amount_is_unique_and_renderer_keeps_payment_design(self):
        self.ready()
        a = customer.create_payment(42, "deposit", 5, "USDT (BEP20)")
        b = customer.create_payment(43, "deposit", 5, "USDT (BEP20)")
        self.assertNotEqual(a["pay_amount"], b["pay_amount"])
        text = bot.render_gateway_payment_text(a)
        for label in ("PAYMENT REQUEST GENERATED!", "Amount to send:", "Network:", "Payment Address:",
                      "I Have Paid (Verify)", a["pay_address"], a["pay_amount"]):
            self.assertIn(label, text)
        kb = bot.deposit_payment_request_keyboard(user_id=42)
        bound = bot.gateway_payment_request_keyboard(a, kb)
        self.assertEqual([[b.text for b in row] for row in kb.inline_keyboard],
                         [[b.text for b in row] for row in bound.inline_keyboard])
        self.assertEqual(bound.inline_keyboard[0][0].callback_data, f"ocpay:verify:{a['invoice_id']}")
        self.assertEqual(bound.inline_keyboard[1][0].callback_data, "deppay_change_network")

    def test_existing_cryptomus_verify_ignores_new_gateway_switch(self):
        self.ready()
        with patch.object(bot, "PAYMENT_GATEWAY", "cryptomus"), \
             patch.object(bot, "run_cryptomus_manual_verify", new_callable=AsyncMock) as cm, \
             patch.object(bot, "run_nowpayments_manual_verify", new_callable=AsyncMock) as np:
            asyncio.run(bot.run_gateway_manual_verify(SimpleNamespace(), 42, "deposit"))
            cm.assert_awaited_once()
            np.assert_not_called()

    def test_normal_user_cannot_switch_gateway(self):
        from onchain import admin_ui
        q = SimpleNamespace(from_user=SimpleNamespace(id=42), edit_message_text=AsyncMock())
        asyncio.run(admin_ui.callback(q, "oc:gateway:trust_wallet", lambda u: u == 1))
        self.assertEqual(customer.gateway(), "cryptomus")

    def confirm_asset(self, inv, digit):
        net = nw.NETWORKS[inv["network"]]
        txid = digit * (88 if net.family == "solana" else 64)
        if net.family == "evm":
            txid = "0x" + txid
        result = self.found(inv)
        result["confirmations"] = net.min_confirmations
        result["transfers"][0]["locator"] = ("log:1" if nw.ASSETS[inv["asset_id"]].kind == "token"
                                                 else "vout:0" if net.family == "utxo" else "native")
        store.add_hint(net.key, txid, "test")
        with patch.object(chains, "inspect_tx", return_value=result):
            self.eng.process_hint(net.key, txid, 0)
        return txid, result

    def test_all_nine_assets_only_release_after_own_test_and_settle_once(self):
        store.set_setting("customer_payment_gateway", "trust_wallet", 1)
        store.set_setting("customer_new_payments_enabled", True, 1)
        with patch.dict(os.environ, {"ONCHAIN_AUTO_ENABLE_AFTER_TEST": "true"}):
            for index, aid in enumerate(nw.ASSET_ORDER):
                with self.subTest(asset=aid):
                    network = nw.ASSETS[aid].network
                    store.set_checkpoint("monitoring_started_at:" + network,
                                         (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())
                    self.assertFalse(engine.customer_gate(aid)[0])
                    admin = engine.create_admin_test_invoice(1, aid, Decimal("1"), lambda u: u == 1,
                                                              rate_fn=lambda _: Decimal("100"))
                    self.confirm_asset(admin, "a")
                    self.assertTrue(engine.customer_gate(aid)[0])
                    customer_invoice = engine.create_customer_invoice(42, aid, Decimal("5"),
                                                                       rate_fn=lambda _: Decimal("100"))
                    txid, result = self.confirm_asset(customer_invoice, "b")
                    # Replayed delivery cannot make a second settlement.
                    with patch.object(chains, "inspect_tx", return_value=result):
                        self.eng.process_hint(network, txid, 0)
                    asyncio.run(customer.settle_pending(bot))
            self.assertEqual(bot.user_wallet[42], 45)
            self.assertEqual(len(bot.credited_topups), 9)
            with store.conn() as c:
                self.assertEqual(c.execute("SELECT count(*) AS n FROM onchain_test_ledger").fetchone()["n"], 9)

    def test_auto_enable_does_not_override_pause_or_cryptomus_rollback(self):
        store.set_setting("customer_new_payments_enabled", True, 1)
        with patch.dict(os.environ, {"ONCHAIN_AUTO_ENABLE_AFTER_TEST": "true"}):
            for aid, paused, gateway in (("eth", True, "trust_wallet"), ("sol", False, "cryptomus")):
                network = nw.ASSETS[aid].network
                store.set_checkpoint("monitoring_started_at:" + network,
                                     (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())
                store.set_setting("new_invoices_paused", paused, 1)
                store.set_setting("customer_payment_gateway", gateway, 1)
                inv = engine.create_admin_test_invoice(1, aid, Decimal("1"), lambda u: u == 1,
                                                       rate_fn=lambda _: Decimal("100"))
                self.confirm_asset(inv, "a")
                self.assertEqual(store.get_invoice(inv["id"])["status"], "paid")
                self.assertFalse(engine.network_enabled(network))

    def test_batched_transaction_separate_outputs_credit_separate_customers(self):
        network, aid = "litecoin", "ltc"
        store.set_checkpoint("monitoring_started_at:" + network,
                             (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())
        admin = engine.create_admin_test_invoice(1, aid, Decimal("1"), lambda u: u == 1,
                                                 rate_fn=lambda _: Decimal("100"))
        self.confirm_asset(admin, "a")
        customer.switch_gateway("trust_wallet", 1)
        for index, user_id in enumerate((42, 43)):
            inv = engine.create_customer_invoice(user_id, aid, Decimal("5"),
                                                 rate_fn=lambda _: Decimal("100"))
            found = self.found(inv)
            found["transfers"][0]["locator"] = f"vout:{index}"
            txid = "b" * 64
            store.add_hint(network, txid, "test")
            with patch.object(chains, "inspect_tx", return_value=found):
                self.eng.process_hint(network, txid, 0)
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(bot.user_wallet[43], 5)
        self.assertEqual(len(bot.credited_topups), 2)

    def test_owner_approved_all_asset_launch_does_not_fake_payment_tests(self):
        approved = ",".join(nw.ASSET_ORDER)
        with patch.dict(os.environ, {"ONCHAIN_OWNER_APPROVED_ASSETS": approved,
                                     "ONCHAIN_CUSTOMER_LAUNCH": "trust_wallet_all_v1"}):
            customer.bootstrap()
            for aid in nw.ASSET_ORDER:
                with self.subTest(asset=aid):
                    self.assertFalse(engine.asset_ready(aid))
                    self.assertTrue(engine.asset_available(aid))
                    self.assertTrue(engine.customer_gate(aid)[0])
                    inv = engine.create_customer_invoice(42, aid, Decimal("5"),
                                                         rate_fn=lambda _: Decimal("100"))
                    self.assertEqual(inv["destination"], nw.NETWORKS[inv["network"]].address)
            self.assertEqual(store.paid_test_assets(), set())
            self.assertEqual(store.test_ledger(), [])
            audit = [a for a in store.recent_audit(30) if a["action"] == "customer_launch"]
            self.assertEqual(len(audit), 1)
            self.assertEqual(set(audit[0]["detail"]["owner_approved_assets"]), set(nw.ASSET_ORDER))
            from onchain import admin_ui
            text = admin_ui.home_text()
            self.assertEqual(text.count("real test pending"), 9)
            self.assertNotIn("real test passed", text)

    def test_owner_approval_preserves_all_other_payment_gates(self):
        with patch.dict(os.environ, {"ONCHAIN_OWNER_APPROVED_ASSETS": ",".join(nw.ASSET_ORDER),
                                     "ONCHAIN_CUSTOMER_LAUNCH": "trust_wallet_all_v1"}):
            customer.bootstrap()
            store.set_setting("new_invoices_paused", True, 1)
            self.assertEqual(engine.customer_gate("ltc"), (False, "new_invoices_paused"))
            store.set_setting("new_invoices_paused", False, 1)
            with patch.dict(os.environ, {"ONCHAIN_EMERGENCY_DISABLE": "true"}):
                self.assertEqual(engine.customer_gate("ltc"), (False, "emergency_disabled"))
            with patch.dict(os.environ, {"ONCHAIN_CUSTOMER_CHECKOUT_ENABLED": "false"}):
                self.assertEqual(engine.customer_gate("ltc"), (False, "checkout_off"))
            with patch.dict(os.environ, {"ONCHAIN_OWNER_APPROVED_ASSETS": ""}):
                self.assertEqual(engine.customer_gate("ltc"), (False, "asset_not_ready"))
            store.set_setting("network_enabled:litecoin", False, 1)
            self.assertEqual(engine.customer_gate("ltc"), (False, "network_off"))

    def test_owner_launch_does_not_repeat_after_admin_rollback(self):
        with patch.dict(os.environ, {"ONCHAIN_OWNER_APPROVED_ASSETS": ",".join(nw.ASSET_ORDER),
                                     "ONCHAIN_CUSTOMER_LAUNCH": "trust_wallet_all_v1"}):
            customer.bootstrap()
            customer.switch_gateway("cryptomus", 1)
            store.set_setting("network_enabled:ethereum", False, 1)
            customer.bootstrap()
            self.assertEqual(customer.gateway(), "cryptomus")
            self.assertFalse(engine.network_enabled("ethereum"))

    def test_owner_allowlist_rejects_wildcards_unknown_assets_and_missing_rpc(self):
        with patch.dict(os.environ, {"ONCHAIN_OWNER_APPROVED_ASSETS": "*,fake,ltc"}):
            self.assertEqual(engine.owner_approved_assets(), {"ltc"})
            self.assertFalse(engine.asset_available("eth"))
            self.assertFalse(engine.asset_available("fake"))
            with patch.dict(os.environ, {"ALCHEMY_API_KEY": "", "ONCHAIN_RPC_URL_LITECOIN": ""}):
                self.assertFalse(engine.asset_available("ltc"))


if __name__ == "__main__":
    unittest.main()
