import asyncio
import concurrent.futures
import copy
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot


class Phase14StateMixin:
    def setUp(self):
        self.saved = {
            "PRODUCTS": copy.deepcopy(bot.PRODUCTS),
            "product_order": list(bot.product_order),
            "shop_order": list(bot.shop_order),
            "wallet": copy.deepcopy(bot.user_wallet),
            "orders": copy.deepcopy(bot.user_orders),
            "transactions": copy.deepcopy(bot.user_transactions),
            "all_orders": copy.deepcopy(bot.all_orders),
            "all_transactions": copy.deepcopy(bot.all_transactions),
            "profiles": copy.deepcopy(bot.user_profiles),
            "credited_topups": copy.deepcopy(bot.credited_topups),
            "api_product_mappings": copy.deepcopy(bot.api_product_mappings),
            "global_order_id": bot.global_order_id,
            "global_tx_id": bot.global_tx_id,
        }
        bot.PRODUCTS.clear()
        bot.PRODUCTS.update({
            "auto": {
                "name": "Automatic Account",
                "price": 2.0,
                "month": "1",
                "details": ["Instant"],
                "accounts": [{"raw_line": "user|pass"}],
                "display_stock": 1,
                "category_id": bot.DEFAULT_CATEGORY_ID,
            },
            "manual": {
                "name": "Google Voice Service",
                "price": 5.0,
                "month": "N/A",
                "details": ["Made to order"],
                "accounts": [],
                "display_stock": 10,
                "category_id": bot.DEFAULT_CATEGORY_ID,
                "delivery_type": bot.MANUAL_DELIVERY,
                "support_instructions": "Send the requested region to Support.",
            },
        })
        bot.product_order[:] = ["auto", "manual"]
        bot.shop_order[:] = ["product:auto", "product:manual"]
        bot.user_wallet.clear()
        bot.user_orders.clear()
        bot.user_transactions.clear()
        bot.all_orders.clear()
        bot.all_transactions.clear()
        bot.user_profiles.clear()
        bot.credited_topups.clear()
        bot.api_product_mappings.clear()
        bot.api_purchase_in_progress.clear()
        bot.global_order_id = 1
        bot.global_tx_id = 1
        self.user_id = 101
        self.admin_id = next(iter(bot.ADMIN_IDS))
        bot.user_wallet[self.user_id] = 10.0
        bot.user_orders[self.user_id] = []
        bot.user_transactions[self.user_id] = []

    def tearDown(self):
        bot.PRODUCTS.clear()
        bot.PRODUCTS.update(self.saved["PRODUCTS"])
        bot.product_order[:] = self.saved["product_order"]
        bot.shop_order[:] = self.saved["shop_order"]
        bot.user_wallet.clear()
        bot.user_wallet.update(self.saved["wallet"])
        bot.user_orders.clear()
        bot.user_orders.update(self.saved["orders"])
        bot.user_transactions.clear()
        bot.user_transactions.update(self.saved["transactions"])
        bot.all_orders[:] = self.saved["all_orders"]
        bot.all_transactions[:] = self.saved["all_transactions"]
        bot.user_profiles.clear()
        bot.user_profiles.update(self.saved["profiles"])
        bot.credited_topups.clear()
        bot.credited_topups.update(self.saved["credited_topups"])
        bot.api_product_mappings.clear()
        bot.api_product_mappings.update(self.saved["api_product_mappings"])
        bot.api_purchase_in_progress.clear()
        bot.global_order_id = self.saved["global_order_id"]
        bot.global_tx_id = self.saved["global_tx_id"]

    def buy_manual(self, reference="test-purchase"):
        with patch.object(bot, "save_bot_state_strict"):
            return bot.create_paid_manual_order(self.user_id, "manual", 1, reference)

    @staticmethod
    def simulate_restart():
        snapshot = copy.deepcopy(bot._json_safe(bot.build_state_snapshot()))
        bot.apply_loaded_state(snapshot)


class Phase14CoreTests(Phase14StateMixin, unittest.TestCase):
    def test_existing_product_defaults_to_automatic_and_seller_api_stays_available(self):
        self.assertEqual(bot.product_delivery_type(bot.PRODUCTS["auto"]), bot.AUTOMATIC_DELIVERY)
        item = bot.seller_api_product_item("local", "auto", bot.PRODUCTS["auto"], "en")
        self.assertIsNotNone(item)
        self.assertEqual(item["stats"]["available"], 1)

    def test_manual_product_is_labeled_and_excluded_from_seller_api(self):
        page = bot.render_product_details("manual", self.user_id)
        self.assertIn("Manual Delivery", page)
        self.assertIn("delivered manually after payment", page)
        self.assertIsNone(bot.seller_api_product_item("local", "manual", bot.PRODUCTS["manual"], "en"))

    def test_my_order_support_link_contains_owned_order_context(self):
        result = self.buy_manual("support-details")
        order = result["order"]

        keyboard = bot.user_order_details_keyboard(0, self.user_id, order)
        support_url = keyboard.inline_keyboard[0][0].url

        self.assertIn(f"Order%20%23{order['id']}", support_url)
        self.assertIn(bot.quote(order["product"]), support_url)
        self.assertIn(bot.quote(bot._order_payment_reference(order)), support_url)
        public_id = bot.seller_api_public_local_product_id("manual")
        self.assertEqual(bot.seller_api_resolve_product(public_id), (None, None, None))
        order_context = {"id": 77, "product": "Google Voice Service", "purchase_reference": "manual-wallet:101:abc"}
        support_url = bot.manual_purchase_success_keyboard(self.user_id, order_context).inline_keyboard[0][0].url
        self.assertIn("77", support_url)
        self.assertIn("Google%20Voice%20Service", support_url)
        self.assertIn("manual-wallet%3A101%3Aabc", support_url)

    def test_manual_bulk_pricing_uses_configured_availability_not_credential_stock(self):
        bot.PRODUCTS["manual"]["bulk_pricing"] = [{"min_qty": 5, "unit_price": 4.5}]
        keyboard = bot.buy_qty_keyboard("manual", styled=False, user_id=self.user_id)
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertIn("🛒 Buy 1x", labels)
        self.assertIn("🛒 Buy 5x", labels)

    def test_purchase_deducts_once_creates_paid_pending_order_and_transaction(self):
        first = self.buy_manual("same-click")
        second = self.buy_manual("same-click")
        self.assertTrue(first["ok"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(bot.user_wallet[self.user_id], 5.0)
        self.assertEqual(len(bot.all_orders), 1)
        self.assertEqual(len(bot.all_transactions), 1)
        order = bot.all_orders[0]
        self.assertEqual(order["status"], bot.MANUAL_AWAITING_STATUS)
        self.assertEqual(order["payment_status"], "Paid")
        self.assertNotEqual(order["status"], "Completed")
        self.assertEqual(order["original_amount_paid"], 5.0)
        self.assertEqual(order["original_wallet_user_id"], self.user_id)

    def test_insufficient_balance_creates_nothing(self):
        bot.user_wallet[self.user_id] = 4.99
        result = self.buy_manual("no-funds")
        self.assertFalse(result["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], 4.99)
        self.assertFalse(bot.all_orders)
        self.assertFalse(bot.all_transactions)

    def test_changed_price_requires_customer_to_review_again(self):
        with patch.object(bot, "save_bot_state_strict"):
            result = bot.create_paid_manual_order(
                self.user_id, "manual", 1, "price-changed", expected_total=4.0
            )
        self.assertFalse(result["ok"])
        self.assertIn("price changed", result["error"])
        self.assertEqual(bot.user_wallet[self.user_id], 10.0)
        self.assertFalse(bot.all_orders)

    def test_pending_order_visible_to_customer_and_admin(self):
        order = self.buy_manual()["order"]
        self.assertIn("Awaiting Manual Delivery", bot.render_user_order_details(self.user_id, order["id"]))
        self.assertIn(order, bot.manual_delivery_orders("pending"))
        self.assertIn(f"#{order['id']}", bot.render_manual_order_admin_detail(order))

    def test_processing_then_delivered_and_delivered_cannot_refund(self):
        order = self.buy_manual()["order"]
        with patch.object(bot, "save_bot_state_strict"):
            self.assertTrue(bot.update_manual_order_status(order["id"], bot.MANUAL_PROCESSING_STATUS, self.admin_id)[0])
            self.assertTrue(bot.update_manual_order_status(order["id"], bot.MANUAL_DELIVERED_STATUS, self.admin_id)[0])
            result = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertFalse(result["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], 5.0)
        self.assertFalse([tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"])

    def test_refund_uses_original_amount_and_is_exactly_once(self):
        order = self.buy_manual()["order"]
        bot.PRODUCTS["manual"]["price"] = 99.0
        with patch.object(bot, "save_bot_state_strict"):
            first = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
            second = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertTrue(first["ok"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(bot.user_wallet[self.user_id], 10.0)
        refunds = [tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"]
        self.assertEqual(len(refunds), 1)
        self.assertEqual(refunds[0]["amount"], 5.0)
        self.assertEqual(order["status"], bot.MANUAL_REFUNDED_STATUS)
        self.assertEqual(order["refunded_by_admin_id"], self.admin_id)
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 10)
        self.assertTrue(order["stock_restored"])

    def test_concurrent_admin_refund_only_credits_once(self):
        order = self.buy_manual()["order"]
        with patch.object(bot, "save_bot_state_strict"), patch.object(bot, "ADMIN_IDS", {900, 901}):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda admin: bot.cancel_manual_order_with_refund(order["id"], admin), (900, 901)))
        self.assertTrue(all(result["ok"] for result in results))
        self.assertEqual(bot.user_wallet[self.user_id], 10.0)
        self.assertEqual(len([tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"]), 1)

    def test_failed_refund_persistence_never_reports_completed(self):
        order = self.buy_manual()["order"]
        with patch.object(bot, "save_bot_state_strict", side_effect=RuntimeError("disk unavailable")):
            result = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertFalse(result["ok"])
        self.assertEqual(order["status"], bot.MANUAL_REFUND_ERROR_STATUS)
        self.assertEqual(order["payment_status"], "Refund Persistence Unconfirmed")
        self.assertTrue(order.get("refund_transaction_reference"))
        self.assertEqual(len([tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"]), 1)

    def test_failed_purchase_persistence_is_auditable_and_not_charged_twice(self):
        with patch.object(bot, "save_bot_state_strict", side_effect=RuntimeError("disk unavailable")):
            first = bot.create_paid_manual_order(self.user_id, "manual", 1, "persist-failure")
            second = bot.create_paid_manual_order(self.user_id, "manual", 1, "persist-failure")
        self.assertFalse(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], 5.0)
        self.assertEqual(len(bot.all_orders), 1)
        self.assertEqual(len(bot.all_transactions), 1)
        self.assertEqual(bot.all_orders[0]["status"], "Payment Persistence Error")
        self.assertEqual(bot.all_transactions[0]["status"], "Persistence Unconfirmed")

    def test_purchase_survives_restart_and_duplicate_reference_does_not_recharge(self):
        order = self.buy_manual("restart-purchase")["order"]
        order_id = order["id"]
        self.simulate_restart()
        with patch.object(bot, "save_bot_state_strict"):
            duplicate = bot.create_paid_manual_order(self.user_id, "manual", 1, "restart-purchase")
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["order"]["id"], order_id)
        self.assertEqual(bot.user_wallet[self.user_id], 5.0)
        self.assertEqual(len(bot.all_orders), 1)
        self.assertEqual(len(bot.all_transactions), 1)
        self.assertEqual(bot.user_orders[self.user_id][0]["status"], bot.MANUAL_AWAITING_STATUS)

    def test_successful_refund_survives_restart_and_repairs_customer_views(self):
        order = self.buy_manual("restart-refund")["order"]
        with patch.object(bot, "save_bot_state_strict"):
            self.assertTrue(bot.cancel_manual_order_with_refund(order["id"], self.admin_id)["ok"])
        self.simulate_restart()
        with patch.object(bot, "save_bot_state_strict"):
            duplicate = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(bot.user_wallet[self.user_id], 10.0)
        self.assertEqual(bot.user_orders[self.user_id][0]["status"], bot.MANUAL_REFUNDED_STATUS)
        self.assertEqual(bot.user_transactions[self.user_id][-1]["type"], "Manual Order Refund")
        self.assertEqual(len([tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"]), 1)
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 10)

    def test_concurrent_refund_and_manual_purchase_preserve_wallet_and_stock(self):
        first = self.buy_manual("first-order")["order"]
        with patch.object(bot, "save_bot_state_strict"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                refund_future = pool.submit(bot.cancel_manual_order_with_refund, first["id"], self.admin_id)
                purchase_future = pool.submit(bot.create_paid_manual_order, self.user_id, "manual", 1, "second-order")
                refund_result = refund_future.result()
                purchase_result = purchase_future.result()
        self.assertTrue(refund_result["ok"])
        self.assertTrue(purchase_result["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], 5.0)
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 9)
        self.assertEqual(len(bot.all_orders), 2)

    def test_concurrent_refund_and_topup_do_not_lose_wallet_updates(self):
        order = self.buy_manual("refund-with-topup")["order"]
        with (
            patch.object(bot, "save_bot_state_strict"),
            patch.object(bot, "force_save_bot_state"),
            patch.object(bot, "schedule_admin_topup_credit_alert"),
        ):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                refund_future = pool.submit(bot.cancel_manual_order_with_refund, order["id"], self.admin_id)
                topup_future = pool.submit(
                    bot.apply_confirmed_topup_credit,
                    self.user_id,
                    3,
                    "NOWPayments",
                    "mock-concurrent",
                    payment_id="concurrent-payment",
                )
                refund_result = refund_future.result()
                topup_result = topup_future.result()
        self.assertTrue(refund_result["ok"])
        self.assertTrue(topup_result["credited"])
        self.assertEqual(bot.user_wallet[self.user_id], 13.0)

    def test_concurrent_limited_stock_purchases_cannot_oversell(self):
        second_user = 202
        bot.user_wallet[second_user] = 10.0
        bot.user_orders[second_user] = []
        bot.user_transactions[second_user] = []
        bot.PRODUCTS["manual"]["display_stock"] = 1
        with patch.object(bot, "save_bot_state_strict"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(bot.create_paid_manual_order, uid, "manual", 1, f"limited-{uid}")
                    for uid in (self.user_id, second_user)
                ]
                results = [future.result() for future in futures]
        self.assertEqual(sum(bool(result["ok"]) for result in results), 1)
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 0)
        self.assertEqual(len(bot.all_orders), 1)
        self.assertEqual(sum(bot.user_wallet[uid] for uid in (self.user_id, second_user)), 15.0)

    def test_stock_is_restored_once_only(self):
        order = self.buy_manual("stock-refund")["order"]
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 9)
        with patch.object(bot, "save_bot_state_strict"):
            bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
            bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 10)
        self.assertEqual(order["stock_restored_qty"], 1)

    def test_invalid_and_unauthorized_transitions_are_blocked(self):
        order = self.buy_manual("transitions")["order"]
        with patch.object(bot, "save_bot_state_strict"):
            self.assertFalse(bot.update_manual_order_status(order["id"], bot.MANUAL_PROCESSING_STATUS, 999999)[0])
            self.assertFalse(bot.cancel_manual_order_with_refund(order["id"], 999999)["ok"])
            self.assertTrue(bot.cancel_manual_order_with_refund(order["id"], self.admin_id)["ok"])
            self.assertFalse(bot.update_manual_order_status(order["id"], bot.MANUAL_DELIVERED_STATUS, self.admin_id)[0])
        self.assertEqual(order["status"], bot.MANUAL_REFUNDED_STATUS)

    def test_orphaned_refund_reference_blocks_automatic_credit(self):
        order = self.buy_manual("orphan-ref")["order"]
        order["refund_transaction_reference"] = f"MRF-{order['id']}-orphan"
        balance_before = bot.user_wallet[self.user_id]
        with patch.object(bot, "save_bot_state_strict"):
            result = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertFalse(result["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], balance_before)
        self.assertEqual(order["status"], bot.MANUAL_REFUND_ERROR_STATUS)
        self.assertFalse([tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"])

    def test_ambiguous_refund_persistence_recovers_without_second_credit(self):
        order = self.buy_manual("ambiguous-refund")["order"]
        with patch.object(bot, "save_bot_state_strict", side_effect=[RuntimeError("commit response lost"), None]):
            first = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertFalse(first["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], 10.0)
        self.simulate_restart()
        with patch.object(bot, "save_bot_state_strict"):
            recovery = bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        self.assertTrue(recovery["ok"])
        self.assertEqual(bot.user_wallet[self.user_id], 10.0)
        self.assertEqual(len([tx for tx in bot.all_transactions if tx["type"] == "Manual Order Refund"]), 1)
        self.assertEqual(bot.PRODUCTS["manual"]["display_stock"], 10)

    def test_refunded_order_is_not_counted_as_sales(self):
        order = self.buy_manual()["order"]
        with patch.object(bot, "save_bot_state_strict"):
            bot.cancel_manual_order_with_refund(order["id"], self.admin_id)
        day = order["created_at"].date()
        report = bot.aggregate_sales_report(bot.all_orders, day, day)
        self.assertEqual(report["totals"]["total_sales"], 0)
        self.assertEqual(report["totals"]["total_orders"], 0)

    def test_existing_topup_duplicate_credit_and_alert_guards_remain(self):
        with (
            patch.object(bot, "force_save_bot_state"),
            patch.object(bot, "schedule_admin_topup_credit_alert") as alert,
        ):
            first = bot.apply_confirmed_topup_credit(
                self.user_id, 3, "NOWPayments", "mock-test", payment_id="payment-1"
            )
            second = bot.apply_confirmed_topup_credit(
                self.user_id, 3, "NOWPayments", "mock-test", payment_id="payment-1"
            )
        self.assertTrue(first["credited"])
        self.assertTrue(second["already_credited"])
        self.assertEqual(bot.user_wallet[self.user_id], 13.0)
        self.assertEqual(alert.call_count, 1)


class Phase14AsyncTests(Phase14StateMixin, unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def admin_user():
        admin_id = next(iter(bot.ADMIN_IDS))
        return SimpleNamespace(id=admin_id, username="admin", first_name="Admin", last_name="", is_bot=False)

    async def test_existing_automatic_wallet_purchase_still_delivers_and_completes(self):
        context = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        query = SimpleNamespace(reply_text=AsyncMock())
        with patch.object(bot, "notify_admin_order", new=AsyncMock()):
            ok = await bot.process_wallet_purchase(query, context, self.user_id, "auto", 1, 2.0)
        self.assertTrue(ok)
        self.assertEqual(bot.user_wallet[self.user_id], 8.0)
        self.assertEqual(bot.all_orders[0]["status"], "Completed")
        self.assertEqual(len(bot.PRODUCTS["auto"]["accounts"]), 0)

    async def test_existing_mapped_provider_purchase_still_completes(self):
        mapping = {
            "api_product_id": "provider-product-1",
            "provider_id": "default",
            "enabled": True,
            "category_id": bot.DEFAULT_CATEGORY_ID,
            "selling_price": 2.0,
            "last_stock": 5,
            "display_name": "Mapped Account",
            "name": "Mapped Account",
        }
        bot.api_product_mappings["provider-product-1"] = mapping
        token = bot.api_shop_callback_token("provider-product-1")
        telegram_bot = SimpleNamespace(send_message=AsyncMock())
        application = SimpleNamespace(create_task=lambda coroutine: coroutine.close())
        context = SimpleNamespace(bot=telegram_bot, application=application)
        with (
            patch.object(bot, "get_seller_api_provider", return_value={"enabled": True, "configured": True}),
            patch.object(bot, "purchase_buyer_api_product", return_value={
                "accounts": ["mapped-user|mapped-pass"],
                "order_code": "API-ORDER-1",
                "remaining_stock": 4,
            }),
            patch.object(bot, "notify_admin_api_purchase_success", new=AsyncMock()),
        ):
            ok = await bot.process_api_shop_purchase(context, self.user_id, token, 1)
        self.assertTrue(ok)
        self.assertEqual(bot.user_wallet[self.user_id], 8.0)
        self.assertEqual(bot.all_orders[0]["product_type"], "api")
        self.assertEqual(bot.all_orders[0]["status"], "Completed")
        self.assertEqual(bot.all_orders[0]["payment_reference"], "API-ORDER-1")

    async def test_duplicate_manual_confirmation_notifies_admin_once(self):
        nonce = "one-confirmation"
        bot.user_state[self.user_id] = {
            "step": "manual_purchase_confirm",
            "product_id": "manual",
            "qty": 1,
            "manual_purchase_nonce": nonce,
        }
        query = SimpleNamespace()
        context = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
        with (
            patch.object(bot, "save_bot_state_strict"),
            patch.object(bot, "send_inline_from_callback", new=AsyncMock()),
            patch.object(bot, "notify_admin_manual_order", new=AsyncMock(return_value=1)) as notify,
        ):
            await bot.process_manual_wallet_purchase(query, context, self.user_id, nonce)
            await bot.process_manual_wallet_purchase(query, context, self.user_id, nonce)
        self.assertEqual(notify.await_count, 1)
        self.assertEqual(bot.user_wallet[self.user_id], 5.0)

    async def test_concurrent_admin_notification_attempts_send_once(self):
        order = self.buy_manual("notify-concurrent")["order"]
        calls = 0

        async def delayed_notify(_bot, _order):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)
            return 1

        with (
            patch.object(bot, "save_bot_state_strict"),
            patch.object(bot, "notify_admin_manual_order", side_effect=delayed_notify),
        ):
            await asyncio.gather(
                bot.ensure_manual_order_admin_notification(SimpleNamespace(), order),
                bot.ensure_manual_order_admin_notification(SimpleNamespace(), order),
            )
        self.assertEqual(calls, 1)

    async def test_admin_creates_manual_product_with_category_and_no_credentials(self):
        admin = self.admin_user()
        bot.ensure_user(admin.id, admin)
        bot.admin_temp[admin.id] = {
            "name": "AWS Manual Setup",
            "icon": "☁️",
            "category_id": bot.DEFAULT_CATEGORY_ID,
            "delivery_type": bot.MANUAL_DELIVERY,
            "support_instructions": "Send requirements.",
            "month": "N/A",
            "price": 12.5,
            "details": ["Manual setup"],
            "display_stock": 50,
        }
        query = SimpleNamespace(
            from_user=admin,
            answer=AsyncMock(),
            data="admin_confirm_add_product",
            message=SimpleNamespace(),
        )
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()):
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace(bot=SimpleNamespace()))
        created = next(product for product in bot.PRODUCTS.values() if product.get("name") == "AWS Manual Setup")
        self.assertEqual(created["delivery_type"], bot.MANUAL_DELIVERY)
        self.assertEqual(created["category_id"], bot.DEFAULT_CATEGORY_ID)
        self.assertEqual(created["accounts"], [])
        self.assertEqual(created["support_instructions"], "Send requirements.")

    async def test_refund_callback_notifies_customer_once(self):
        order = self.buy_manual()["order"]
        admin = self.admin_user()
        bot.ensure_user(admin.id, admin)
        telegram_bot = SimpleNamespace(send_message=AsyncMock())
        context = SimpleNamespace(bot=telegram_bot)
        query = SimpleNamespace(
            from_user=admin,
            answer=AsyncMock(),
            data=f"manual_fulfill_refund_{order['id']}",
            message=SimpleNamespace(),
        )
        with (
            patch.object(bot, "save_bot_state_strict"),
            patch.object(bot, "send_inline_from_callback", new=AsyncMock()),
        ):
            await bot.handle_callback(SimpleNamespace(callback_query=query), context)
            await bot.handle_callback(SimpleNamespace(callback_query=query), context)
        self.assertEqual(telegram_bot.send_message.await_count, 1)
        message = telegram_bot.send_message.await_args.args[1]
        self.assertIn("Order Cancelled", message)
        self.assertIn("New Wallet Balance", message)

    async def test_unauthorized_manual_admin_callback_is_rejected(self):
        customer = SimpleNamespace(id=self.user_id, username="customer", first_name="Customer", last_name="", is_bot=False)
        bot.ensure_user(customer.id, customer)
        query = SimpleNamespace(
            from_user=customer,
            answer=AsyncMock(),
            data="manual_fulfill_list_pending_0",
            message=SimpleNamespace(),
        )
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace(bot=SimpleNamespace()))
        self.assertIn("not allowed", sender.await_args.args[1].lower())

    async def test_unauthorized_manual_product_admin_callback_is_rejected(self):
        customer = SimpleNamespace(id=self.user_id, username="customer", first_name="Customer", last_name="", is_bot=False)
        bot.ensure_user(customer.id, customer)
        query = SimpleNamespace(
            from_user=customer,
            answer=AsyncMock(),
            data="admin_edit_delivery_type_menu",
            message=SimpleNamespace(),
        )
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace(bot=SimpleNamespace()))
        self.assertIn("not allowed", sender.await_args.args[1].lower())
        self.assertNotEqual(bot.user_state[self.user_id].get("step"), "admin_edit_delivery_type_pick")


if __name__ == "__main__":
    unittest.main()
