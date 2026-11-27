"""Seller 3 inventory/notification parity; no real supplier orders or balances."""
import asyncio
import copy
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot
import jero_api
from test_jero import JeroBotBase, completed

REAL_REFRESH = bot.jero_refresh_inventory


class Seller3LiveTests(JeroBotBase, unittest.IsolatedAsyncioTestCase):
    def admin_texts(self):
        return [str(c.args[1] if len(c.args) > 1 else c.kwargs.get("text", ""))
                for c in self.sent.await_args_list
                if (c.args[0] if c.args else c.kwargs.get("chat_id")) in bot.ADMIN_IDS]

    async def test_refresh_updates_15_to_10_to_zero_without_touching_other_provider(self):
        mapping = self.add_jero(stock=15)
        other = self.add_jero(stock=77, provider="seller_2")
        for count in (10, 0):
            product = jero_api.normalize_product({"id": 12, "title": "Jero Netflix", "price": 1, "stock": count})
            with patch.object(bot, "fetch_buyer_api_products", return_value=([product], 1)), \
                    patch.object(bot, "fetch_buyer_api_balance", return_value={"balance": 100}):
                self.assertTrue(await REAL_REFRESH(self.context.bot, "seller_3", force=True))
            self.assertEqual(mapping["last_stock"], count)
            self.assertEqual(other["last_stock"], 77)
        row = bot.api_shop_product_row(mapping, user_id=self.user_id)[0][0]
        self.assertIn("Out of stock", row.text)
        self.assertNotIn("api_shop_buy_", str(bot.api_shop_product_details_keyboard(mapping).to_dict()))

    async def test_unknown_stock_does_not_keep_old_stock_or_allow_purchase(self):
        mapping = self.add_jero(stock=15)
        product = jero_api.normalize_product({"id": 12, "title": "Jero Netflix", "price": 1})
        bot.sync_api_product_mapping_validity([product], provider_id="seller_3")
        self.assertIsNone(mapping["last_stock"])
        self.assertTrue(bot.validate_api_shop_purchase(mapping, 1, self.user_id))

    async def test_startup_enables_30_second_sync_once_and_preserves_admin_off(self):
        status = bot.get_seller_api_provider_status("seller_3")
        status.update(live_stock_sync_initialized=False, enabled=False, interval_seconds=None)
        other = copy.deepcopy(bot.get_seller_api_provider_status("seller_2"))
        bot.initialize_jero_stock_refresh()
        self.assertTrue(status["enabled"])
        self.assertEqual(bot.seller_api_refresh_interval_seconds("seller_3"), 30)
        self.assertIn("30 seconds", bot.render_seller_api_auto_refresh_status("seller_3"))
        status["enabled"] = False
        bot.initialize_jero_stock_refresh()
        self.assertFalse(status["enabled"])
        self.assertEqual(bot.get_seller_api_provider_status("seller_2"), other)

    async def test_confirm_refreshes_stock_before_debit_and_blocks_sold_out(self):
        mapping = self.add_jero(stock=15)
        draft = await self.confirm(mapping)
        async def sold_out(*args, **kwargs):
            mapping["last_stock"] = 0
            return True
        with patch.object(bot, "jero_refresh_inventory", side_effect=sold_out) as refresh:
            result = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(result, {})
        self.assertEqual(bot.user_wallet[self.user_id], 20)
        self.assertEqual(self.client.created, [])
        refresh.assert_awaited_once_with(self.context.bot, "seller_3", force=True)

    async def test_unavailable_inventory_never_debits_or_sends_order(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping)
        with patch.object(bot, "jero_refresh_inventory", new_callable=AsyncMock, return_value=False):
            self.assertEqual(await bot.jero_confirm_draft(self.context, self.user_id, draft["id"]), {})
        self.assertEqual(bot.user_wallet[self.user_id], 20)
        self.assertEqual(self.client.created, [])

    async def test_success_notification_has_provider_order_and_no_credentials(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping)
        self.client.create_results.append(completed(["secret-user|secret-password"]))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        text = " ".join(self.admin_texts())
        for expected in ("API Product Sold", "seller_3", record["external_order_id"], "Quantity:", "Customer total:"):
            self.assertIn(expected, text)
        self.assertNotIn("secret-password", text)
        count = len(self.admin_texts())
        await bot.jero_after_update(self.context.bot, record)
        await bot.jero_reconcile_once(self.context.bot)
        self.assertEqual(len(self.admin_texts()), count)

    async def test_all_rejections_notify_admin_and_refund_once(self):
        for index, code in enumerate(("out_of_stock", "product_not_found", "rate_limited")):
            mapping = self.add_jero(product_id=str(20 + index))
            draft = await self.confirm(mapping)
            self.client.create_results.append(jero_api.JeroAPIError("rejected", code))
            before = bot.user_wallet[self.user_id]
            record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
            self.assertEqual(bot.user_wallet[self.user_id], before)
            self.assertEqual(record["state"], "failed")
            self.assertIn(code, " ".join(self.admin_texts()))
            await bot.jero_after_update(self.context.bot, record)
            self.assertEqual(bot.user_wallet[self.user_id], before)
            if code == "out_of_stock":
                self.assertEqual(mapping["last_stock"], 0)

    async def test_timeout_immediately_notifies_support_then_completion_notifies_success(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping)
        self.client.create_results.append(jero_api.JeroAPIError("timeout", "timeout", retryable=True, order_may_exist=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "unknown")
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)
        self.assertIn("API ORDER NEEDS SUPPORT", " ".join(self.admin_texts()))
        self.assertIn(bot.SUPPORT_USERNAME, " ".join(self.texts()))
        self.client.lookup_results.append(completed(["u|p"]))
        await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "completed")
        self.assertIn("API Product Sold", " ".join(self.admin_texts()))
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)

    async def test_failed_admin_recipient_retries_without_duplicate_to_successful_recipient(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping)
        self.client.create_results.append(completed(["u|p"]))
        attempts = []
        async def send(chat_id, text, **kwargs):
            attempts.append(chat_id)
            if chat_id == 202 and attempts.count(202) == 1:
                raise RuntimeError("temporary Telegram error")
        with patch.object(bot, "ADMIN_IDS", [101, 202]), \
                patch.object(self.context.bot, "send_message", side_effect=send):
            record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
            await bot.jero_reconcile_once(self.context.bot)
            await bot.jero_reconcile_once(self.context.bot)
        self.assertEqual(attempts.count(101), 1)
        self.assertEqual(attempts.count(202), 2)
        self.assertTrue(record["admin_notified"])

    async def test_view_cache_is_short_but_confirm_forces_refresh(self):
        status = bot.get_seller_api_provider_status("seller_3")
        status["last_success_at"] = datetime.now().isoformat()
        with patch.object(bot, "run_seller_api_refresh", new_callable=AsyncMock,
                          return_value={"success": True}) as refresh:
            self.assertTrue(await REAL_REFRESH(self.context.bot, "seller_3"))
            refresh.assert_not_awaited()
            self.assertTrue(await REAL_REFRESH(self.context.bot, "seller_3", force=True))
            refresh.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
