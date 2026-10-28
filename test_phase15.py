import asyncio
import copy
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot


class Phase15StateMixin:
    def setUp(self):
        self.saved = {
            "products": copy.deepcopy(bot.PRODUCTS),
            "product_order": list(bot.product_order),
            "shop_order": list(bot.shop_order),
            "mappings": copy.deepcopy(bot.api_product_mappings),
            "profiles": copy.deepcopy(bot.user_profiles),
            "store_context": copy.deepcopy(bot.user_storefront_context),
            "user_state": copy.deepcopy(bot.user_state),
            "user_mode": copy.deepcopy(bot.user_mode),
            "admin_temp": copy.deepcopy(bot.admin_temp),
            "wallet": copy.deepcopy(bot.user_wallet),
            "orders": copy.deepcopy(bot.all_orders),
            "transactions": copy.deepcopy(bot.all_transactions),
        }
        bot.PRODUCTS.clear()
        bot.product_order.clear()
        bot.shop_order.clear()
        bot.api_product_mappings.clear()
        bot.user_profiles.clear()
        bot.user_storefront_context.clear()
        self.user_id = 15015
        bot.user_profiles[self.user_id] = {"language": "en"}

    def tearDown(self):
        bot.PRODUCTS.clear()
        bot.PRODUCTS.update(self.saved["products"])
        bot.product_order[:] = self.saved["product_order"]
        bot.shop_order[:] = self.saved["shop_order"]
        bot.api_product_mappings.clear()
        bot.api_product_mappings.update(self.saved["mappings"])
        bot.user_profiles.clear()
        bot.user_profiles.update(self.saved["profiles"])
        bot.user_storefront_context.clear()
        bot.user_storefront_context.update(self.saved["store_context"])
        bot.user_state.clear()
        bot.user_state.update(self.saved["user_state"])
        bot.user_mode.clear()
        bot.user_mode.update(self.saved["user_mode"])
        bot.admin_temp.clear()
        bot.admin_temp.update(self.saved["admin_temp"])
        bot.user_wallet.clear()
        bot.user_wallet.update(self.saved["wallet"])
        bot.all_orders[:] = self.saved["orders"]
        bot.all_transactions[:] = self.saved["transactions"]

    def add_local(self, product_id, name, price, delivery=bot.AUTOMATIC_DELIVERY, **extra):
        product = {
            "name": name,
            "price": price,
            "month": "1 Month",
            "details": ["Details"],
            "accounts": [{"raw_line": "stock"}],
            "display_stock": 1,
            "category_id": bot.DEFAULT_CATEGORY_ID,
            "delivery_type": delivery,
            "icon": "📦",
        }
        product.update(extra)
        bot.PRODUCTS[product_id] = product
        bot.product_order.append(product_id)
        bot.shop_order.append(f"product:{product_id}")
        return product

    def add_api(self, api_id="api-1", provider="default", **extra):
        mapping = {
            "provider_id": provider,
            "api_product_id": api_id,
            "name": f"API {api_id}",
            "enabled": True,
            "category_id": bot.DEFAULT_CATEGORY_ID,
            "selling_price": 7.5,
            "last_stock": 5,
            "icon": "⚡",
        }
        mapping.update(extra)
        key = bot.seller_api_mapping_key(api_id, provider)
        bot.api_product_mappings[key] = mapping
        bot.shop_order.append(bot.api_product_shop_order_item(api_id, provider))
        return mapping


class Phase15CatalogTests(Phase15StateMixin, unittest.TestCase):
    def test_requested_catalog_sizes_have_exact_page_shapes(self):
        for count in (0, 1, 9, 10, 11, 20, 21, 100, 500):
            with self.subTest(count=count):
                bot.PRODUCTS.clear()
                bot.product_order.clear()
                bot.shop_order.clear()
                bot.api_product_mappings.clear()
                for index in range(count):
                    self.add_local(f"edge-{index}", f"Edge Product {index:03d}", index + 1)
                context = bot._storefront_context(self.user_id, reset=True)
                expected_pages = max(1, (count + 9) // 10)
                for page in range(expected_pages):
                    context["page"] = page
                    items, safe_page, pages, total = bot.storefront_page(self.user_id, context)
                    expected_items = 0 if count == 0 else min(10, count - page * 10)
                    self.assertEqual((len(items), safe_page, pages, total), (expected_items, page, expected_pages, count))
                keyboard = bot.storefront_keyboard(self.user_id, context, styled=False)
                labels = [button.text for row in keyboard.inline_keyboard for button in row]
                self.assertIn(f"{expected_pages} / {expected_pages}", labels)
                self.assertNotIn("Next ➡️", labels)

    def test_fixed_ten_item_pages_and_boundaries(self):
        for index in range(23):
            self.add_local(f"p{index:02d}", f"Product {index:02d}", index + 1)
        context = bot._storefront_context(self.user_id, reset=True)

        first, page, pages, count = bot.storefront_page(self.user_id, context)
        self.assertEqual((len(first), page, pages, count), (10, 0, 3, 23))
        self.assertEqual([item["id"] for item in first], [f"p{i:02d}" for i in range(10)])

        context["page"] = 1
        second, page, pages, _ = bot.storefront_page(self.user_id, context)
        self.assertEqual(len(second), 10)
        self.assertEqual([item["id"] for item in second], [f"p{i:02d}" for i in range(10, 20)])

        context["page"] = 99
        final, page, pages, _ = bot.storefront_page(self.user_id, context)
        self.assertEqual((len(final), page, pages), (3, 2, 3))
        labels = [button.text for row in bot.storefront_keyboard(self.user_id, context).inline_keyboard for button in row]
        self.assertFalse(any("Next" in label for label in labels))
        self.assertTrue(any("Previous" in label for label in labels))

        context["page"] = -4
        bot.storefront_page(self.user_id, context)
        labels = [button.text for row in bot.storefront_keyboard(self.user_id, context).inline_keyboard for button in row]
        self.assertFalse(any("Previous" in label for label in labels))

    def test_shop_keyboard_is_direct_and_has_exactly_ten_product_buttons(self):
        for index in range(12):
            self.add_local(f"p{index}", f"Direct {index}", index + 1)
        keyboard = bot.shop_categories_keyboard(self.user_id, styled=False)
        callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
        product_callbacks = [value for value in callbacks if value and value.startswith("sf_v_")]
        self.assertEqual(len(product_callbacks), 10)
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in product_callbacks))
        self.assertFalse(any(value and value.startswith("shop_category_") for value in callbacks))
        self.assertIn("SUPREME LEADER STORE", bot.render_shop_menu_text(self.user_id))

    def test_search_exact_prefix_partial_casefold_and_pagination(self):
        self.add_local("exact", "ChatGPT Plus", 10, search_keywords=["openai", "assistant"])
        self.add_local("prefix", "ChatGPT Team", 20)
        self.add_local("partial", "Premium ChatGPT Workspace", 30)
        for index in range(12):
            self.add_local(f"cloud{index}", f"Cloud Service {index}", 5)

        context = bot._storefront_context(self.user_id, reset=True)
        context["query"] = "cHaTgPt PlUs"
        self.assertEqual(bot.storefront_results(self.user_id, context)[0]["id"], "exact")
        context["query"] = "chatg"
        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)[:2]], ["exact", "prefix"])
        context["query"] = "workspace"
        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)], ["partial"])
        context["query"] = "OPENAI"
        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)], ["exact"])
        context["query"] = "cloud"
        first, _, pages, count = bot.storefront_page(self.user_id, context)
        self.assertEqual((len(first), pages, count), (10, 2, 12))
        context["page"] = 1
        self.assertEqual(len(bot.storefront_page(self.user_id, context)[0]), 2)

    def test_empty_and_no_result_search_are_safe(self):
        self.add_local("visible", "Visible Product", 3)
        context = bot._storefront_context(self.user_id, reset=True)
        context["query"] = ""
        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)], ["visible"])
        context["query"] = "definitely absent"
        items, page, pages, total = bot.storefront_page(self.user_id, context)
        self.assertEqual((items, page, pages, total), ([], 0, 1, 0))
        keyboard = bot.storefront_keyboard(self.user_id, context, styled=False)
        self.assertTrue(any(button.callback_data == "sf_qc" for row in keyboard.inline_keyboard for button in row))

    def test_numeric_price_name_featured_priority_and_delivery_filters(self):
        self.add_local("ten", "Zulu", 10)
        self.add_local("two", "alpha", 2, delivery=bot.MANUAL_DELIVERY)
        self.add_local("hundred", "Beta", 100, storefront_featured=True, storefront_priority=5)
        self.add_local("featured", "Gamma", 50, storefront_featured=True, storefront_priority=1)
        context = bot._storefront_context(self.user_id, reset=True)

        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)[:2]], ["featured", "hundred"])
        context["sort"] = "price_low"
        self.assertEqual([item["price"] for item in bot.storefront_results(self.user_id, context)], [2, 10, 50, 100])
        context["sort"] = "price_high"
        self.assertEqual([item["price"] for item in bot.storefront_results(self.user_id, context)], [100, 50, 10, 2])
        context["sort"] = "name"
        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)], ["two", "hundred", "featured", "ten"])
        context.update({"filter": "manual", "page": 8})
        self.assertEqual([item["id"] for item in bot.storefront_results(self.user_id, context)], ["two"])
        self.assertEqual(bot.storefront_page(self.user_id, context)[1], 0)
        context["filter"] = "instant"
        self.assertNotIn("two", [item["id"] for item in bot.storefront_results(self.user_id, context)])

    def test_unified_catalog_visibility_and_correct_detail_callbacks(self):
        self.add_local("auto", "Automatic", 2)
        self.add_local("manual", "Manual", 3, delivery=bot.MANUAL_DELIVERY)
        self.add_local("hidden", "Hidden", 4, hidden=True)
        self.add_local("disabled", "Disabled", 5, enabled=False)
        self.add_local("invalid-price", "Invalid Price", 0)
        self.add_api("mapped")
        self.add_api("api-disabled", enabled=False)
        self.add_api("api-missing", seller_missing=True)
        self.add_api("api-invalid-price", selling_price=0)

        items = bot.storefront_results(self.user_id, bot._storefront_context(self.user_id, reset=True))
        self.assertEqual({item["id"] for item in items}, {"auto", "manual", "mapped"})
        callbacks = {item["id"]: item["callback"] for item in items}
        self.assertEqual(callbacks["auto"], f"sf_v_{bot.local_storefront_callback_token('auto')}")
        self.assertEqual(callbacks["manual"], f"sf_v_{bot.local_storefront_callback_token('manual')}")
        self.assertEqual(callbacks["mapped"], f"api_shop_view_{bot.api_shop_callback_token('mapped')}")

    def test_multiple_providers_are_not_merged_and_localized_names_are_searchable(self):
        self.add_api("shared", "default", name="Provider One Product")
        self.add_api("shared", "seller_2", name="Provider Two Product", name_i18n={"es": "Producto Dos"})
        bot.user_profiles[self.user_id]["language"] = "es"
        context = bot._storefront_context(self.user_id, reset=True)
        items = bot.storefront_results(self.user_id, context)
        shared = [item for item in items if item["id"] == "shared"]
        self.assertEqual(len(shared), 2)
        self.assertEqual(len({item["callback"] for item in shared}), 2)
        context["query"] = "producto dos"
        results = bot.storefront_results(self.user_id, context)
        self.assertEqual([(item["provider_id"], item["id"]) for item in results], [("seller_2", "shared")])

    def test_two_users_have_independent_navigation_context(self):
        second_user = self.user_id + 1
        bot.user_profiles[second_user] = {"language": "de"}
        first = bot._storefront_context(self.user_id, reset=True)
        second = bot._storefront_context(second_user, reset=True)
        first.update({"query": "alpha", "sort": "price_low", "filter": "manual", "page": 3})
        second.update({"query": "beta", "sort": "name", "filter": "instant", "page": 7})
        self.assertEqual(bot._storefront_context(self.user_id), first)
        self.assertEqual(bot._storefront_context(second_user), second)
        self.assertIsNot(first, second)

    def test_same_user_context_is_isolated_between_chats(self):
        first = bot._storefront_context(self.user_id, reset=True, chat_id=1001)
        second = bot._storefront_context(self.user_id, reset=True, chat_id=2002)
        first.update({"query": "first chat", "sort": "price_low"})
        second.update({"query": "second chat", "sort": "name"})
        self.assertEqual(bot._storefront_context(self.user_id, chat_id=1001)["query"], "first chat")
        self.assertEqual(bot._storefront_context(self.user_id, chat_id=2002)["query"], "second chat")
        self.assertIsNot(first, second)

    def test_newest_is_stable_for_legacy_products_without_timestamps(self):
        for index in range(30):
            self.add_local(f"legacy-{index}", "Same Legacy Name", 1)
        context = bot._storefront_context(self.user_id, reset=True)
        context["sort"] = "newest"
        first = [item["id"] for item in bot.storefront_results(self.user_id, context)]
        second = [item["id"] for item in bot.storefront_results(self.user_id, context)]
        self.assertEqual(first, second)
        self.assertEqual(first, [f"legacy-{index}" for index in range(30)])

    def test_navigation_cache_is_bounded(self):
        bot.user_storefront_context.clear()
        for user_id in range(bot.STOREFRONT_CONTEXT_LIMIT + 25):
            bot._storefront_context(user_id)
        self.assertLessEqual(len(bot.user_storefront_context), bot.STOREFRONT_CONTEXT_LIMIT)

    def test_storefront_does_not_expose_supplier_metadata(self):
        self.add_api(
            "secure", custom_name="Customer Product", api_cost=0.01,
            api_key="SECRET", provider_url="https://supplier.invalid/private",
        )
        context = bot._storefront_context(self.user_id, reset=True)
        item = bot.storefront_results(self.user_id, context)[0]
        keyboard = bot.storefront_keyboard(self.user_id, context, styled=False)
        rendered = " ".join(button.text for row in keyboard.inline_keyboard for button in row)
        self.assertEqual(item["name"], "Customer Product")
        self.assertNotIn("SECRET", rendered)
        self.assertNotIn("supplier.invalid", rendered)
        self.assertNotIn("0.01", rendered)

    def test_all_new_storefront_callback_payloads_fit_telegram_limit(self):
        self.add_local("local", "Local", 1)
        self.add_api("api", provider="seller_2")
        context = bot._storefront_context(self.user_id, reset=True)
        keyboards = [
            bot.storefront_keyboard(self.user_id, context, styled=False),
            bot.storefront_sort_filter_keyboard(self.user_id, context),
            bot.storefront_admin_page_keyboard(0),
        ]
        for keyboard in keyboards:
            for row in keyboard.inline_keyboard:
                for button in row:
                    if button.callback_data:
                        self.assertLessEqual(len(button.callback_data.encode("utf-8")), 64, button.callback_data)

    def test_featured_priority_persist_and_refresh_preserves_preferences(self):
        product = self.add_local("local", "Local", 1, storefront_featured=True, storefront_priority=4)
        mapping = self.add_api("mapped", storefront_featured=True, storefront_priority=2)
        snapshot = bot.build_state_snapshot()
        self.assertTrue(snapshot["PRODUCTS"]["local"]["storefront_featured"])
        self.assertEqual(snapshot["api_product_mappings"]["mapped"]["storefront_priority"], 2)
        bot.apply_loaded_state(copy.deepcopy(bot._json_safe(snapshot)))
        product = bot.PRODUCTS["local"]
        mapping = bot.api_product_mappings["mapped"]
        self.assertEqual((product["storefront_featured"], product["storefront_priority"]), (True, 4))
        self.assertEqual((mapping["storefront_featured"], mapping["storefront_priority"]), (True, 2))

        fresh = {"provider_id": "default", "api_product_id": "mapped", "name": "Updated", "last_stock": 9}
        with patch.object(bot, "_api_product_mapping_snapshot", return_value=fresh):
            bot.sync_api_product_mapping_validity([{"id": "mapped"}], provider_id="default")
        self.assertTrue(mapping["storefront_featured"])
        self.assertEqual(mapping["storefront_priority"], 2)
        self.assertTrue(product["storefront_featured"])

    def test_newly_fetched_provider_product_is_not_auto_published(self):
        snapshot = {
            "provider_id": "default", "api_product_id": "new-api", "name": "New API Product",
            "api_cost": 1.0, "last_stock": 10,
        }
        with (
            patch.object(bot, "_buyer_api_product_id", return_value="new-api"),
            patch.object(bot, "_api_product_mapping_snapshot", return_value=snapshot),
        ):
            mapping = bot.update_api_product_mapping({"id": "new-api"})
        self.assertFalse(mapping["enabled"])
        self.assertNotIn("new-api", [item["id"] for item in bot.build_customer_storefront_catalog(self.user_id)])

    def test_navigation_does_not_mutate_financial_or_history_data(self):
        self.add_local("local", "Local", 1)
        bot.user_wallet[self.user_id] = 44.0
        before = (copy.deepcopy(bot.user_wallet), copy.deepcopy(bot.all_orders), copy.deepcopy(bot.all_transactions))
        context = bot._storefront_context(self.user_id, reset=True)
        context.update({"query": "loc", "sort": "price_high", "filter": "all", "page": 10})
        bot.storefront_page(self.user_id, context)
        bot.storefront_keyboard(self.user_id, context, styled=False)
        self.assertEqual(before, (bot.user_wallet, bot.all_orders, bot.all_transactions))

    def test_all_six_languages_have_phase15_labels(self):
        for language in ("en", "es", "ru", "tr", "fr", "de"):
            for key in ("store_title", "search_products", "sort_filter", "previous", "next", "empty_store"):
                value = bot.tr(language, key)
                self.assertTrue(value)
                self.assertNotEqual(value, key)


class Phase15NavigationTests(Phase15StateMixin, unittest.IsolatedAsyncioTestCase):
    async def test_two_users_can_navigate_concurrently_without_state_mix(self):
        for index in range(25):
            self.add_local(
                f"p{index}", f"Product {index}", index + 1,
                delivery=bot.MANUAL_DELIVERY if index % 2 else bot.AUTOMATIC_DELIVERY,
            )
        users = [
            SimpleNamespace(id=self.user_id, username="one", first_name="One", last_name="", is_bot=False),
            SimpleNamespace(id=self.user_id + 1, username="two", first_name="Two", last_name="", is_bot=False),
        ]
        for user in users:
            bot.ensure_user(user.id, user)
            bot.user_state[user.id] = {"step": "shop"}
        first = bot._storefront_context(users[0].id, reset=True)
        second = bot._storefront_context(users[1].id, reset=True)
        first.update({"query": "product", "sort": "price_high", "filter": "all", "page": 0})
        second.update({"query": "product", "sort": "name", "filter": "all", "page": 0})
        queries = [
            SimpleNamespace(from_user=users[0], answer=AsyncMock(), data="sf_p_1", edit_message_text=AsyncMock(), message=SimpleNamespace(reply_text=AsyncMock())),
            SimpleNamespace(from_user=users[1], answer=AsyncMock(), data="sf_f_manual", edit_message_text=AsyncMock(), message=SimpleNamespace(reply_text=AsyncMock())),
        ]
        await asyncio.gather(*(
            bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace()) for query in queries
        ))
        self.assertEqual(first, {"query": "product", "sort": "price_high", "filter": "all", "page": 1})
        self.assertEqual(second, {"query": "product", "sort": "name", "filter": "manual", "page": 0})

    async def test_shop_command_immediately_sends_products(self):
        for index in range(11):
            self.add_local(f"p{index}", f"Product {index}", index + 1)
        user = SimpleNamespace(id=self.user_id, username="customer", first_name="Customer", last_name="", is_bot=False)
        message = SimpleNamespace(text="/shop", reply_text=AsyncMock(), from_user=user)
        update = SimpleNamespace(effective_user=user, message=message)
        with patch.object(bot, "ensure_channel_access", new=AsyncMock(return_value=True)):
            await bot.client_menu_command(update, SimpleNamespace())
        kwargs = message.reply_text.await_args.kwargs
        callbacks = [button.callback_data for row in kwargs["reply_markup"].inline_keyboard for button in row]
        self.assertEqual(len([value for value in callbacks if value and value.startswith("sf_v_")]), 10)
        self.assertFalse(any(value and value.startswith("shop_category_") for value in callbacks))

    async def test_pagination_edits_same_message_and_handles_not_modified(self):
        for index in range(12):
            self.add_local(f"p{index}", f"Product {index}", index + 1)
        user = SimpleNamespace(id=self.user_id)
        source = SimpleNamespace(
            from_user=user,
            edit_message_text=AsyncMock(side_effect=Exception("Message is not modified")),
            message=SimpleNamespace(reply_text=AsyncMock()),
        )
        await bot.send_shop_cards_message(source, from_callback=True)
        source.edit_message_text.assert_awaited_once()
        source.message.reply_text.assert_not_awaited()

    async def test_store_message_edit_failure_uses_safe_reply_fallback(self):
        self.add_local("p1", "Fallback Product", 1)
        source = SimpleNamespace(
            from_user=SimpleNamespace(id=self.user_id),
            edit_message_text=AsyncMock(side_effect=[Exception("expired"), Exception("still expired")]),
            message=SimpleNamespace(reply_text=AsyncMock()),
        )
        await bot.send_shop_cards_message(source, from_callback=True)
        self.assertEqual(source.edit_message_text.await_count, 2)
        source.message.reply_text.assert_awaited_once()

    async def test_page_callback_preserves_search_sort_and_filter(self):
        for index in range(25):
            self.add_local(f"p{index}", f"Cloud Product {index}", index + 1)
        context_state = bot._storefront_context(self.user_id, reset=True)
        context_state.update({"query": "cloud", "sort": "price_high", "filter": "instant", "page": 0})
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        query = SimpleNamespace(
            from_user=user, answer=AsyncMock(), data="sf_p_1",
            edit_message_text=AsyncMock(), message=SimpleNamespace(reply_text=AsyncMock()),
        )
        with patch.object(bot, "save_bot_state"):
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace())
        self.assertEqual(context_state, {"query": "cloud", "sort": "price_high", "filter": "instant", "page": 1})
        query.edit_message_text.assert_awaited_once()

    async def test_admin_can_toggle_featured_and_set_priority(self):
        self.add_local("local", "Placement Product", 9)
        admin_id = next(iter(bot.ADMIN_IDS))
        admin = SimpleNamespace(id=admin_id, username="admin", first_name="Admin", last_name="", is_bot=False)
        bot.ensure_user(admin_id, admin)
        target = bot.storefront_admin_targets()[0]
        token = bot.storefront_admin_token(target[0])
        callback_query = SimpleNamespace(
            from_user=admin, answer=AsyncMock(), data=f"admin_sf_feat_{token}", message=SimpleNamespace(),
        )
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()):
            await bot.handle_callback(SimpleNamespace(callback_query=callback_query), SimpleNamespace())
        self.assertTrue(bot.PRODUCTS["local"]["storefront_featured"])

        bot.admin_temp[admin_id]["storefront_target_token"] = token
        bot.user_state[admin_id] = {"step": "admin_storefront_priority_input"}
        message = SimpleNamespace(text="12", reply_text=AsyncMock())
        update = SimpleNamespace(effective_user=admin, message=message)
        await bot.handle_text(update, SimpleNamespace())
        self.assertEqual(bot.PRODUCTS["local"]["storefront_priority"], 12)
        self.assertIn("priority updated", message.reply_text.await_args.args[0].lower())

        clear_query = SimpleNamespace(
            from_user=admin, answer=AsyncMock(), data=f"admin_sf_pc_{token}", message=SimpleNamespace(),
        )
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()):
            await bot.handle_callback(SimpleNamespace(callback_query=clear_query), SimpleNamespace())
        self.assertNotIn("storefront_priority", bot.PRODUCTS["local"])

    async def test_priority_boundaries_and_admin_authorization(self):
        self.add_local("local", "Priority Product", 9)
        token = bot.storefront_admin_token(bot.storefront_admin_targets()[0][0])
        admin_id = next(iter(bot.ADMIN_IDS))
        admin = SimpleNamespace(id=admin_id, username="admin", first_name="Admin", last_name="", is_bot=False)
        bot.ensure_user(admin_id, admin)
        for value in (0, 999999):
            bot.admin_temp[admin_id]["storefront_target_token"] = token
            bot.user_state[admin_id] = {"step": "admin_storefront_priority_input"}
            message = SimpleNamespace(text=str(value), reply_text=AsyncMock())
            await bot.handle_text(SimpleNamespace(effective_user=admin, message=message), SimpleNamespace())
            self.assertEqual(bot.PRODUCTS["local"]["storefront_priority"], value)
        for value in (-1, 1000000):
            bot.admin_temp[admin_id]["storefront_target_token"] = token
            bot.user_state[admin_id] = {"step": "admin_storefront_priority_input"}
            message = SimpleNamespace(text=str(value), reply_text=AsyncMock())
            await bot.handle_text(SimpleNamespace(effective_user=admin, message=message), SimpleNamespace())
            self.assertIn("invalid priority", message.reply_text.await_args.args[0].lower())

        customer = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        query = SimpleNamespace(
            from_user=customer, answer=AsyncMock(), data=f"admin_sf_feat_{token}", message=SimpleNamespace(),
        )
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace())
        self.assertIn("not allowed", sender.await_args.args[1].lower())

    async def test_search_callback_does_not_replace_active_purchase_workflow(self):
        self.add_local("local", "Local", 1)
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        bot.ensure_user(self.user_id, user)
        active = {"step": "manual_purchase_confirm", "product_id": "local", "manual_purchase_nonce": "keep-me"}
        bot.user_state[self.user_id] = dict(active)
        query = SimpleNamespace(from_user=user, answer=AsyncMock(), data="sf_q", message=SimpleNamespace())
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace())
        self.assertEqual(bot.user_state[self.user_id], active)
        sender.assert_not_awaited()

    async def test_search_text_from_another_chat_is_not_consumed(self):
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        bot.ensure_user(self.user_id, user)
        bot.user_state[self.user_id] = {
            "step": "storefront_search_input", "store_chat_id": 111, "store_message_id": 5,
        }
        message = SimpleNamespace(text="should not become search", chat_id=222, reply_text=AsyncMock())
        update = SimpleNamespace(effective_user=user, effective_chat=SimpleNamespace(id=222), message=message)
        await bot.handle_text(update, SimpleNamespace())
        self.assertEqual(bot.user_state[self.user_id]["step"], "storefront_search_input")
        self.assertEqual(bot._storefront_context(self.user_id, chat_id=111)["query"], "")

    async def test_stale_callback_survives_reorder_and_opens_original_product(self):
        self.add_local("first", "First Product", 1)
        self.add_local("second", "Second Product", 2)
        token = bot.local_storefront_callback_token("first")
        bot.product_order[:] = ["second", "first"]
        bot.shop_order[:] = ["product:second", "product:first"]
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        query = SimpleNamespace(from_user=user, answer=AsyncMock(), data=f"sf_v_{token}", message=SimpleNamespace())
        with patch.object(bot, "send_shop_inline_with_style_fallback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace())
        self.assertIn("First Product", sender.await_args.args[1])
        self.assertNotIn("Second Product", sender.await_args.args[1])

    async def test_product_becoming_hidden_or_deleted_fails_safely(self):
        product = self.add_local("local", "Disappearing Product", 5)
        token = bot.local_storefront_callback_token("local")
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)

        product["hidden"] = True
        hidden_query = SimpleNamespace(from_user=user, answer=AsyncMock(), data=f"sf_v_{token}", message=SimpleNamespace())
        with patch.object(bot, "send_shop_inline_with_style_fallback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=hidden_query), SimpleNamespace())
        self.assertIn("no longer available", sender.await_args.args[1].lower())
        self.assertNotEqual(bot.user_state.get(self.user_id, {}).get("step"), "buy_qty_select")

        legacy_query = SimpleNamespace(from_user=user, answer=AsyncMock(), data="shop_buy_local", message=SimpleNamespace())
        with patch.object(bot, "send_shop_inline_with_style_fallback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=legacy_query), SimpleNamespace())
        self.assertIn("no longer available", sender.await_args.args[1].lower())

        quantity_query = SimpleNamespace(from_user=user, answer=AsyncMock(), data="buy_qty_local_1", message=SimpleNamespace())
        with patch.object(bot, "send_shop_inline_with_style_fallback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=quantity_query), SimpleNamespace())
        self.assertIn("no longer available", sender.await_args.args[1].lower())

        bot.PRODUCTS.pop("local")
        deleted_query = SimpleNamespace(from_user=user, answer=AsyncMock(), data=f"sf_v_{token}", message=SimpleNamespace())
        with patch.object(bot, "send_shop_inline_with_style_fallback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=deleted_query), SimpleNamespace())
        self.assertIn("no longer available", sender.await_args.args[1].lower())

    async def test_store_navigation_wrappers_skip_persistent_writes(self):
        self.add_local("local", "Local", 1)
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        bot.ensure_user(self.user_id, user)
        bot.user_state[self.user_id] = {"step": "shop"}
        query = SimpleNamespace(
            from_user=user, answer=AsyncMock(), data="sf_p_0",
            edit_message_text=AsyncMock(), message=SimpleNamespace(reply_text=AsyncMock()),
        )
        with patch.object(bot, "save_bot_state") as saver:
            await bot.handle_callback_persistent(SimpleNamespace(callback_query=query), SimpleNamespace())
        saver.assert_not_called()

    async def test_product_buttons_open_existing_local_and_api_details(self):
        self.add_local("local", "Local Detail", 9)
        self.add_api("mapped", name="Mapped Detail")
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        sender = AsyncMock()
        with patch.object(bot, "send_shop_inline_with_style_fallback", new=sender):
            local_query = SimpleNamespace(
                from_user=user, answer=AsyncMock(),
                data=f"sf_v_{bot.local_storefront_callback_token('local')}", message=SimpleNamespace(),
            )
            await bot.handle_callback(SimpleNamespace(callback_query=local_query), SimpleNamespace())
            self.assertIn("Local Detail", sender.await_args.args[1])

            api_query = SimpleNamespace(
                from_user=user, answer=AsyncMock(), data=f"api_shop_view_{bot.api_shop_callback_token('mapped')}",
                message=SimpleNamespace(),
            )
            await bot.handle_callback(SimpleNamespace(callback_query=api_query), SimpleNamespace())
            self.assertIn("Mapped Detail", sender.await_args.args[1])


if __name__ == "__main__":
    unittest.main()
