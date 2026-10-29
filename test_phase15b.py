import copy
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot
from test_phase15 import Phase15StateMixin


def product_callbacks(keyboard):
    return [
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data
        and button.callback_data.startswith(("shop_buy_", "shop_notify_", "api_shop_view_"))
    ]


def root_catalog_callbacks(keyboard):
    return [
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data
        and button.callback_data.startswith((
            "shop_buy_", "shop_notify_", "api_shop_view_", "shop_category_",
        ))
    ]


class Phase15BOrderingTests(Phase15StateMixin, unittest.TestCase):
    def _reset_mock_catalog(self):
        bot.PRODUCTS.clear()
        bot.product_order.clear()
        bot.shop_order.clear()
        bot.api_product_mappings.clear()
        bot.CATEGORIES.clear()
        bot.CATEGORIES[bot.DEFAULT_CATEGORY_ID] = {
            "id": bot.DEFAULT_CATEGORY_ID,
            "name": bot.DEFAULT_CATEGORY_NAME,
            "icon": "🗂",
            "order": 0,
        }
        bot.category_order[:] = [bot.DEFAULT_CATEGORY_ID]

    def test_actual_root_keyboard_has_exact_unique_product_page_sizes(self):
        for count, expected in ((13, (10, 3)), (15, (10, 5)), (20, (10, 10))):
            with self.subTest(count=count):
                self._reset_mock_catalog()
                for index in range(count):
                    self.add_local(f"p{index:02d}", f"Product {index:02d}", index + 1)
                first = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False, page=0))
                second = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False, page=1))
                self.assertEqual((len(first), len(second)), expected)
                self.assertEqual(len(first), len(set(first)))
                self.assertEqual(len(second), len(set(second)))
                self.assertFalse(set(first) & set(second))

    def test_actual_root_keyboard_paginates_mixed_catalog_items_in_shop_order(self):
        for count, expected_sizes in ((13, (10, 3)), (18, (10, 8))):
            with self.subTest(count=count):
                self._reset_mock_catalog()
                expected_order = []
                for index in range(count):
                    if index % 4 == 1:
                        category_id = f"folder-{index:02d}"
                        self.add_category(category_id, f"Folder {index:02d}")
                        expected_order.append(f"shop_category_{category_id}")
                    else:
                        product_id = f"product-{index:02d}"
                        self.add_local(product_id, f"Product {index:02d}", index + 1)
                        expected_order.append(f"shop_buy_{product_id}")
                pages = [
                    root_catalog_callbacks(
                        bot.shop_categories_keyboard(self.user_id, styled=False, page=page)
                    )
                    for page in (0, 1)
                ]
                self.assertEqual(tuple(len(page) for page in pages), expected_sizes)
                self.assertEqual(pages[0] + pages[1], expected_order)
                self.assertFalse(set(pages[0]) & set(pages[1]))

    def test_actual_keyboard_deduplicates_18_raw_aliases_to_15_before_slicing(self):
        names = ["ChatGPT Codex Token", "Claude Token", "Gmail"]
        for index in range(12):
            self.add_local(f"local-{index:02d}", f"Local {index:02d}", index + 1)
        for index, name in enumerate(names):
            self.add_api(f"api-{index}", provider="seller-two", name=name)
        # Put each legacy alias before its provider-aware reference. The raw
        # order contains 18 product references, but only 15 canonical products.
        for index in reversed(range(3)):
            bot.shop_order.insert(5, f"api_product:api-{index}")
        raw_product_references = [
            item for item in bot.shop_order if item.startswith(("product:", "api_product:"))
        ]
        self.assertEqual(len(raw_product_references), 18)

        self.add_category("manual", "Manual Services", icon="🤝")
        self.add_local(
            "manual-one", "Manual Service", 9, delivery=bot.MANUAL_DELIVERY,
            category_id="manual",
        )
        # An unlisted disabled record exercises visibility filtering without
        # changing the 18 raw references above.
        bot.PRODUCTS["disabled"] = {
            "name": "Disabled", "price": 1, "category_id": bot.DEFAULT_CATEGORY_ID,
            "enabled": False, "accounts": [{"raw_line": "stock"}], "display_stock": 1,
        }
        bot.product_order.append("disabled")

        first_keyboard = bot.shop_categories_keyboard(self.user_id, styled=False, page=0)
        second_keyboard = bot.shop_categories_keyboard(self.user_id, styled=False, page=1)
        first = product_callbacks(first_keyboard)
        second = product_callbacks(second_keyboard)
        self.assertEqual((len(first), len(second)), (10, 5))
        self.assertEqual(len(first + second), 15)
        self.assertEqual(len(set(first + second)), 15)
        self.assertFalse(set(first) & set(second))
        self.assertEqual(
            product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False, page=0)), first
        )

        # Folder buttons share root pagination but are never counted as products.
        for keyboard in (first_keyboard, second_keyboard):
            callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
            indicators = [
                button.text for row in keyboard.inline_keyboard for button in row
                if button.callback_data == "noop" and "/" in button.text
            ]
            self.assertEqual(indicators, ["1 / 2"] if keyboard is first_keyboard else ["2 / 2"])
        self.assertNotIn(
            "shop_category_manual",
            [button.callback_data for row in first_keyboard.inline_keyboard for button in row],
        )
        self.assertIn(
            "shop_category_manual",
            [button.callback_data for row in second_keyboard.inline_keyboard for button in row],
        )

        local_callback = next(value for value in first + second if value.startswith("shop_buy_"))
        self.assertIn(local_callback.replace("shop_buy_", ""), bot.PRODUCTS)
        api_callbacks = [value for value in first + second if value.startswith("api_shop_view_")]
        self.assertEqual(len(api_callbacks), 3)
        for callback in api_callbacks:
            mapping = bot.find_api_shop_mapping(callback.replace("api_shop_view_", "", 1))
            self.assertIsNotNone(mapping)
            self.assertEqual(bot.mapping_provider_id(mapping), "seller-two")

    def test_fifteen_catalog_rows_paginate_folders_once_instead_of_repeating(self):
        for index in range(12):
            self.add_local(f"p{index:02d}", f"Product {index:02d}", index + 1)
        repeated_names = ["ChatGPT Codex Token", "Claude Token", "Gmail"]
        for index, name in enumerate(repeated_names):
            self.add_category(f"folder-{index}", name)

        keyboards = [
            bot.shop_categories_keyboard(self.user_id, styled=False, page=page) for page in (0, 1)
        ]
        products = [product_callbacks(keyboard) for keyboard in keyboards]
        folders = [
            [
                button.callback_data for row in keyboard.inline_keyboard for button in row
                if button.callback_data and button.callback_data.startswith("shop_category_")
            ]
            for keyboard in keyboards
        ]
        self.assertEqual([len(page) for page in products], [10, 2])
        self.assertEqual([len(root_catalog_callbacks(keyboard)) for keyboard in keyboards], [10, 5])
        self.assertFalse(set(products[0]) & set(products[1]))
        self.assertEqual(folders[0], [])
        self.assertEqual(len(folders[1]), 3)
        self.assertEqual(len(set(folders[0] + folders[1])), 3)

    def test_repeated_root_references_and_named_examples_render_once(self):
        names = ["ChatGPT Codex Token", "Claude Token", "Gmail"]
        for index, name in enumerate(names):
            self.add_local(f"named-{index}", name, index + 1)
        bot.shop_order.extend([
            "product:named-0", "product:named-1", "product:named-2",
        ])
        callbacks = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False))
        self.assertEqual(callbacks, ["shop_buy_named-0", "shop_buy_named-1", "shop_buy_named-2"])
        self.assertEqual(len(callbacks), len(set(callbacks)))

    def test_twenty_unique_products_have_two_non_overlapping_pages(self):
        for index in range(20):
            self.add_local(f"p{index:02d}", f"Product {index:02d}", index + 1)
        first = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False, page=0))
        second = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False, page=1))
        self.assertEqual(first, [f"shop_buy_p{index:02d}" for index in range(10)])
        self.assertEqual(second, [f"shop_buy_p{index:02d}" for index in range(10, 20)])
        self.assertFalse(set(first) & set(second))

    def test_same_name_distinct_local_products_remain_separate(self):
        self.add_local("variant-a", "Same Name", 1)
        self.add_local("variant-b", "Same Name", 2)
        callbacks = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False))
        self.assertEqual(callbacks, ["shop_buy_variant-a", "shop_buy_variant-b"])

    def test_same_upstream_id_from_different_providers_remains_separate(self):
        self.add_api("shared", provider="seller-two", name="Shared Product")
        self.add_api("shared", provider="default", name="Shared Product")
        callbacks = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False))
        self.assertEqual(len(callbacks), 2)
        self.assertEqual(len(set(callbacks)), 2)

    def test_legacy_and_provider_aware_aliases_deduplicate_canonically(self):
        self.add_api("alias", provider="seller-two")
        bot.shop_order.insert(0, "api_product:alias")
        configured = [item for item in bot.configured_shop_order_items() if item.startswith("api_product:")]
        self.assertEqual(configured, ["api_product:seller-two|alias"])
        self.assertEqual(len(product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False))), 1)

    def test_disabled_local_restores_original_relative_order(self):
        products = [self.add_local(f"p{index}", f"P{index}", 1) for index in range(3)]
        products[1]["enabled"] = False
        self.assertEqual(
            product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False)),
            ["shop_buy_p0", "shop_buy_p2"],
        )
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), [
            "product:p0", "product:p1", "product:p2",
        ])
        products[1]["enabled"] = True
        self.assertEqual(
            product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False)),
            ["shop_buy_p0", "shop_buy_p1", "shop_buy_p2"],
        )

    def test_disabled_mapped_product_restores_original_relative_order(self):
        first = self.add_api("first")
        middle = self.add_api("middle")
        last = self.add_api("last")
        middle["enabled"] = False
        bot.sync_api_product_shop_order(middle)
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), [
            "api_product:first", "api_product:middle", "api_product:last",
        ])
        middle["enabled"] = True
        bot.sync_api_product_shop_order(middle)
        callbacks = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False))
        self.assertEqual(callbacks, [
            f"api_shop_view_{bot.api_shop_callback_token('first')}",
            f"api_shop_view_{bot.api_shop_callback_token('middle')}",
            f"api_shop_view_{bot.api_shop_callback_token('last')}",
        ])
        self.assertTrue(first["enabled"] and last["enabled"])

    def test_missing_and_restored_mapping_keeps_manual_position(self):
        self.add_api("first")
        target = self.add_api("target")
        bot.move_configured_product_to_position("api_product:target", 1)
        expected = list(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID))
        bot.sync_api_product_mapping_validity([], provider_id="default")
        self.assertTrue(target["seller_missing"])
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), expected)
        fresh = {"provider_id": "default", "api_product_id": "target", "name": "Target", "last_stock": 8}
        with patch.object(bot, "_api_product_mapping_snapshot", return_value=fresh):
            bot.sync_api_product_mapping_validity([{"id": "target"}], provider_id="default")
        self.assertFalse(target["seller_missing"])
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), expected)

    def test_new_products_append_without_reordering_existing_products(self):
        self.add_local("first", "First", 1)
        self.add_local("second", "Second", 1)
        before = list(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID))
        bot.PRODUCTS["new"] = {
            "name": "New", "price": 1, "category_id": bot.DEFAULT_CATEGORY_ID,
            "accounts": [{"raw_line": "stock"}], "display_stock": 1,
        }
        bot.product_order.append("new")
        after = bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)
        self.assertEqual(after[:2], before)
        self.assertEqual(after[-1], "product:new")

    def test_move_up_down_and_exact_position_shift_without_swapping_data(self):
        for index in range(5):
            self.add_local(f"p{index}", f"P{index}", 1)
        self.assertTrue(bot.move_shop_order_item("product:p3", -1))
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), [
            "product:p0", "product:p1", "product:p3", "product:p2", "product:p4",
        ])
        self.assertTrue(bot.move_shop_order_item("product:p3", 1))
        self.assertTrue(bot.move_configured_product_to_position("product:p4", 2))
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), [
            "product:p0", "product:p4", "product:p1", "product:p2", "product:p3",
        ])
        self.assertEqual(set(bot.PRODUCTS), {f"p{index}" for index in range(5)})

    def test_move_to_page_slot_uses_configured_items_including_disabled(self):
        for index in range(25):
            self.add_local(f"p{index:02d}", f"P{index:02d}", 1)
        bot.PRODUCTS["p05"]["enabled"] = False
        self.assertTrue(bot.move_configured_product_to_position("product:p24", (2 - 1) * 10 + 3))
        configured = bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)
        self.assertEqual(configured[12], "product:p24")
        visible = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False, page=1))
        self.assertEqual(len(visible), 10)
        self.assertIn("shop_buy_p24", visible)

    def test_manual_position_persists_through_state_snapshot_reload(self):
        for index in range(4):
            self.add_local(f"p{index}", f"P{index}", 1)
        bot.move_configured_product_to_position("product:p3", 1)
        snapshot = copy.deepcopy(bot._json_safe(bot.build_state_snapshot()))
        bot.apply_loaded_state(snapshot)
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)[:4], [
            "product:p3", "product:p0", "product:p1", "product:p2",
        ])

    def test_two_concurrent_reorders_are_serialized_without_lost_or_duplicate_items(self):
        for index in range(6):
            self.add_local(f"p{index}", f"P{index}", 1)
        barrier = Barrier(2)

        def move(item, position):
            barrier.wait()
            return bot.move_configured_product_to_position(item, position)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda args: move(*args), [
                ("product:p5", 1), ("product:p4", 2),
            ]))
        configured = bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)
        self.assertEqual(results, [True, True])
        self.assertIn(configured, [
            ["product:p5", "product:p4", "product:p0", "product:p1", "product:p2", "product:p3"],
            ["product:p5", "product:p0", "product:p4", "product:p1", "product:p2", "product:p3"],
        ])
        self.assertEqual(len(configured), len(set(configured)))

    def test_provider_refresh_concurrent_with_reorder_preserves_both_updates(self):
        self.add_api("first")
        target = self.add_api("target")
        barrier = Barrier(2)
        products = [
            {"id": "first", "name": "First", "stock": 9, "price": 1},
            {"id": "target", "name": "Target Refreshed", "stock": 7, "price": 2},
        ]

        def refresh():
            barrier.wait()
            return bot.sync_api_product_mapping_validity(products, provider_id="default")

        def reorder():
            barrier.wait()
            return bot.move_configured_product_to_position("api_product:target", 1)

        with ThreadPoolExecutor(max_workers=2) as pool:
            refresh_future = pool.submit(refresh)
            reorder_future = pool.submit(reorder)
            refresh_result = refresh_future.result()
            self.assertTrue(reorder_future.result())
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)[0], "api_product:target")
        self.assertEqual(target["name"], "Target Refreshed")
        self.assertFalse(target.get("seller_missing"))
        self.assertGreaterEqual(refresh_result["updated"], 1)

    def test_stock_and_visibility_transitions_do_not_destroy_position(self):
        self.add_local("first", "First", 1)
        target = self.add_local("target", "Target", 1)
        self.add_local("last", "Last", 1)
        expected = list(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID))
        target["accounts"] = []
        target["display_stock"] = 0
        target["hidden"] = True
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), expected)
        target["accounts"] = [{"raw_line": "restored"}]
        target["display_stock"] = 1
        target["hidden"] = False
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), expected)

    def test_position_operations_do_not_mutate_wallet_order_or_history_data(self):
        self.add_local("first", "First", 1)
        self.add_api("mapped")
        bot.user_wallet[self.user_id] = 91.25
        bot.all_orders.append({"id": "order-a", "status": "Completed"})
        bot.all_transactions.append({"id": "tx-a", "amount": 4.5})
        protected = (
            copy.deepcopy(bot.user_wallet), copy.deepcopy(bot.all_orders),
            copy.deepcopy(bot.all_transactions),
        )
        bot.move_configured_product_to_position("api_product:mapped", 1)
        bot.sync_api_product_mapping_validity([], provider_id="default")
        self.assertEqual(
            (bot.user_wallet, bot.all_orders, bot.all_transactions), protected
        )

    def test_temporarily_missing_mapping_remains_admin_manageable(self):
        self.add_api("stale", provider="seller-two")
        bot.sync_api_product_mapping_validity([], provider_id="seller-two")
        item = "api_product:seller-two|stale"
        detail = bot.render_store_position_detail(self.user_id, item)
        self.assertIn("Mapped API", detail)
        self.assertIn("seller-two|stale", detail)
        self.assertTrue(bot.move_configured_product_to_position(item, 1))

    def test_legacy_stale_and_duplicate_references_are_not_destructively_rewritten(self):
        self.add_local("one", "One", 1)
        bot.shop_order[:] = ["stale:unknown", "product:one", "product:one"]
        before = list(bot.shop_order)
        callbacks = product_callbacks(bot.shop_categories_keyboard(self.user_id, styled=False))
        self.assertEqual(callbacks, ["shop_buy_one"])
        self.assertEqual(bot.shop_order, before)

    def test_category_lists_stay_separate_and_category_buttons_remain(self):
        self.add_category("manual", "Manual Services", icon="🤝")
        self.add_local("root", "Root", 1)
        self.add_local("manual", "Manual", 1, delivery=bot.MANUAL_DELIVERY, category_id="manual")
        root_keyboard = bot.shop_categories_keyboard(self.user_id, styled=False)
        root_callbacks = [button.callback_data for row in root_keyboard.inline_keyboard for button in row]
        self.assertIn("shop_category_manual", root_callbacks)
        self.assertEqual(product_callbacks(root_keyboard), ["shop_buy_root"])
        self.assertEqual(
            product_callbacks(bot.shop_menu_keyboard(self.user_id, "manual", styled=False)),
            ["shop_buy_manual"],
        )

    def test_admin_position_manager_exposes_status_source_category_and_identity(self):
        self.add_category("services", "Services")
        local = self.add_local(
            "manual", "Manual Service", 1, delivery=bot.MANUAL_DELIVERY,
            category_id="services", enabled=False,
        )
        mapping = self.add_api("mapped", provider="seller-two", category_id="services")
        menu_callbacks = [
            button.callback_data for row in bot.admin_products_keyboard().inline_keyboard for button in row
        ]
        self.assertIn("admin_pos_lists", menu_callbacks)
        local_detail = bot.render_store_position_detail(self.user_id, "product:manual")
        api_detail = bot.render_store_position_detail(self.user_id, "api_product:seller-two|mapped")
        self.assertIn("Status:</b> Disabled", local_detail)
        self.assertIn("Source:</b> Manual", local_detail)
        self.assertIn("Category:</b> Services", local_detail)
        self.assertIn("Identity:</b> <code>manual</code>", local_detail)
        self.assertIn("Source:</b> Mapped API", api_detail)
        self.assertIn("seller-two|mapped", api_detail)
        self.assertFalse(local["enabled"])
        self.assertTrue(mapping["enabled"])
        bot.admin_temp[self.user_id] = {"store_position_category": "services", "store_position_page": 0}
        for keyboard in (
            bot.store_position_lists_keyboard(),
            bot.store_position_page_keyboard(self.user_id),
            bot.store_position_detail_keyboard(self.user_id, "api_product:seller-two|mapped"),
        ):
            for row in keyboard.inline_keyboard:
                for button in row:
                    if button.callback_data:
                        self.assertLessEqual(len(button.callback_data.encode("utf-8")), 64)

    def test_root_position_manager_includes_and_reorders_folders_with_products(self):
        self.add_local("first", "First", 1)
        category = self.add_category("accounts", "Accounts")
        self.add_local("last", "Last", 1)
        expected = ["product:first", "category:accounts", "product:last"]
        self.assertEqual(bot.configured_store_position_items(bot.DEFAULT_CATEGORY_ID), expected)

        folder_item = "category:accounts"
        folder_token = bot.store_position_token(folder_item)
        self.assertEqual(bot.find_store_position_item(folder_token), folder_item)
        detail = bot.render_store_position_detail(self.user_id, folder_item)
        self.assertIn("Source:</b> Folder", detail)
        self.assertIn("Identity:</b> <code>accounts</code>", detail)
        self.assertTrue(bot.move_shop_order_item(folder_item, -1))
        self.assertEqual(bot.configured_store_position_items(bot.DEFAULT_CATEGORY_ID)[0], folder_item)
        self.assertTrue(bot.move_configured_store_item_to_position(folder_item, 3))
        self.assertEqual(bot.configured_store_position_items(bot.DEFAULT_CATEGORY_ID)[2], folder_item)
        self.assertIs(bot.CATEGORIES["accounts"], category)

        bot.admin_temp[self.user_id] = {
            "store_position_category": bot.DEFAULT_CATEGORY_ID,
            "store_position_page": 0,
        }
        callbacks = [
            button.callback_data
            for row in bot.store_position_page_keyboard(self.user_id).inline_keyboard
            for button in row
        ]
        self.assertIn(f"admin_pos_view_{folder_token}", callbacks)


class Phase15BAdminTests(Phase15StateMixin, unittest.IsolatedAsyncioTestCase):
    async def test_position_callbacks_are_admin_only(self):
        self.add_local("local", "Local", 1)
        user = SimpleNamespace(id=self.user_id, username="u", first_name="U", last_name="", is_bot=False)
        query = SimpleNamespace(from_user=user, answer=AsyncMock(), data="admin_pos_lists", message=SimpleNamespace())
        with patch.object(bot, "send_inline_from_callback", new=AsyncMock()) as sender:
            await bot.handle_callback(SimpleNamespace(callback_query=query), SimpleNamespace())
        self.assertIn("not allowed", sender.await_args.args[1].lower())

    async def test_admin_move_to_page_slot_input_and_status_details(self):
        for index in range(15):
            self.add_local(f"p{index}", f"P{index}", 1)
        bot.PRODUCTS["p4"]["enabled"] = False
        token = bot.store_position_token("product:p14")
        bot.admin_temp[self.user_id] = {
            "store_position_category": bot.DEFAULT_CATEGORY_ID,
            "store_position_target": token,
        }
        bot.user_state[self.user_id] = {"step": "admin_store_page_slot_input"}
        message = SimpleNamespace(text="2,3", reply_text=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=self.user_id), message=message)
        with patch.object(bot, "is_admin", return_value=True):
            await bot.handle_text(update, SimpleNamespace())
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)[12], "product:p14")
        self.assertIn("Position:</b> 13", message.reply_text.await_args.args[0])
        disabled_detail = bot.render_store_position_detail(self.user_id, "product:p4")
        self.assertIn("Status:</b> Disabled", disabled_detail)
        self.assertIn("Source:</b> Local", disabled_detail)

    async def test_admin_move_to_exact_position_input(self):
        for index in range(6):
            self.add_local(f"p{index}", f"P{index}", 1)
        token = bot.store_position_token("product:p5")
        bot.admin_temp[self.user_id] = {
            "store_position_category": bot.DEFAULT_CATEGORY_ID,
            "store_position_target": token,
        }
        bot.user_state[self.user_id] = {"step": "admin_store_position_input"}
        message = SimpleNamespace(text="2", reply_text=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=self.user_id), message=message)
        with patch.object(bot, "is_admin", return_value=True):
            await bot.handle_text(update, SimpleNamespace())
        self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID)[1], "product:p5")
        self.assertIn("Position:</b> 2", message.reply_text.await_args.args[0])

    async def test_admin_folder_move_to_exact_root_position(self):
        self.add_local("first", "First", 1)
        category = self.add_category("accounts", "Accounts")
        self.add_local("last", "Last", 1)
        token = bot.store_position_token("category:accounts")
        bot.admin_temp[self.user_id] = {
            "store_position_category": bot.DEFAULT_CATEGORY_ID,
            "store_position_target": token,
        }
        bot.user_state[self.user_id] = {"step": "admin_store_position_input"}
        message = SimpleNamespace(text="1", reply_text=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=self.user_id), message=message)
        with patch.object(bot, "is_admin", return_value=True):
            await bot.handle_text(update, SimpleNamespace())
        self.assertEqual(
            bot.configured_store_position_items(bot.DEFAULT_CATEGORY_ID)[0], "category:accounts"
        )
        self.assertIs(bot.CATEGORIES["accounts"], category)
        self.assertIn("Position:</b> 1", message.reply_text.await_args.args[0])
        self.assertIn("Source:</b> Folder", message.reply_text.await_args.args[0])

    async def test_invalid_position_page_and_slot_inputs_do_not_change_order(self):
        for index in range(4):
            self.add_local(f"p{index}", f"P{index}", 1)
        token = bot.store_position_token("product:p3")
        original = list(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID))
        cases = [
            ("admin_store_position_input", "0", "invalid position"),
            ("admin_store_position_input", "word", "invalid position"),
            ("admin_store_page_slot_input", "0,3", "invalid page or slot"),
            ("admin_store_page_slot_input", "2,11", "invalid page or slot"),
            ("admin_store_page_slot_input", "bad", "invalid page and slot"),
        ]
        for step, text, expected in cases:
            with self.subTest(text=text):
                bot.admin_temp[self.user_id] = {
                    "store_position_category": bot.DEFAULT_CATEGORY_ID,
                    "store_position_target": token,
                }
                bot.user_state[self.user_id] = {"step": step}
                message = SimpleNamespace(text=text, reply_text=AsyncMock())
                update = SimpleNamespace(effective_user=SimpleNamespace(id=self.user_id), message=message)
                with patch.object(bot, "is_admin", return_value=True):
                    await bot.handle_text(update, SimpleNamespace())
                self.assertIn(expected, message.reply_text.await_args.args[0].lower())
                self.assertEqual(bot.configured_product_order_items(bot.DEFAULT_CATEGORY_ID), original)


if __name__ == "__main__":
    unittest.main()
