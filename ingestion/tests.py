from django.test import TestCase
from django.utils import timezone

import csv
import os
import tempfile
from importlib import import_module
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test.utils import CaptureQueriesContext
from selectolax.parser import HTMLParser

from catalog.search_normalizer import build_search_text
from catalog.models import Category, CategoryAlias, Product, Store
from comparison.models import MatchReview
from crawlers import CRAWLER_MODULES, CRAWLER_RUN_ORDER
from crawlers.sklavenitis.sklavenitis_category_listing import (
    detect_unit_of_measure,
    parse_listing_article,
)
from ingestion.models import CrawlerRun, PriceHistory, StoreListing
from ingestion.services.importer import import_rows_for_store


class CrawlerRegistryTests(TestCase):
    def test_registered_crawlers_expose_ingestion_interface(self):
        for module_path in CRAWLER_MODULES.values():
            crawler = import_module(module_path)
            self.assertTrue(hasattr(crawler, "ROOT_CATEGORIES"))
            self.assertTrue(callable(getattr(crawler, "to_category_slug", None)))
            self.assertTrue(callable(getattr(crawler, "to_category_url", None)))
            self.assertTrue(callable(getattr(crawler, "crawl_category_listing", None)))


class SklavenitisCrawlerParsingTests(TestCase):
    def test_detect_unit_of_measure_recognizes_kilo_abbreviation(self):
        self.assertEqual(detect_unit_of_measure("/κιλ"), "kilos")

    def test_main_price_unit_only_card_keeps_unit_price_when_analytics_has_pack_price(self):
        tree = HTMLParser(
            """
            <div
              data-plugin-analyticsimpressions='{"Call":{"ecommerce":{"items":[{"item_id":"sku-1","item_name":"Test product","item_brand":"Brand","price":"0,16"}]}}}'
            >
              <a class="absLink" href="/test-product"></a>
              <div class="priceWrp">
                <div class="main-price">
                  <span class="price">0,62 €</span>
                  <span>/κιλ</span>
                </div>
              </div>
            </div>
            """
        )
        article = tree.css_first("div[data-plugin-analyticsimpressions]")

        row = parse_listing_article(article, "freska-froyta-lachanika")

        self.assertIsNotNone(row)
        self.assertAlmostEqual(row.final_price, 0.16, places=2)
        self.assertAlmostEqual(row.final_unit_price, 0.62, places=2)
        self.assertEqual(row.unit_of_measure, "kilos")


class RunAllDailyIngestionCommandTests(TestCase):
    def test_max_pages_must_be_positive(self):
        with patch("ingestion.management.commands.run_all_daily_ingestion.call_command") as mock_call:
            with self.assertRaisesMessage(
                CommandError,
                "--max-pages must be a positive integer.",
            ):
                call_command("run_all_daily_ingestion", "--max-pages", "0")

        mock_call.assert_not_called()

    def test_runs_all_crawlers_in_expected_order(self):
        observed_calls: list[tuple[str, dict[str, object]]] = []

        def fake_call_command(command_name, **kwargs):
            observed_calls.append((command_name, kwargs))

        with patch("ingestion.management.commands.run_all_daily_ingestion.call_command", side_effect=fake_call_command):
            call_command("run_all_daily_ingestion", "--max-pages", "3", "--run-matcher")

        self.assertEqual(
            observed_calls,
            [
                (
                    "run_daily_ingestion",
                    {
                        "store": store,
                        "max_pages": 3,
                        "run_matcher": True,
                    },
                )
                for store in CRAWLER_RUN_ORDER
            ],
        )

    def test_stops_on_first_failing_crawler(self):
        observed_stores: list[str] = []

        def fake_call_command(command_name, **kwargs):
            observed_stores.append(str(kwargs["store"]))
            if kwargs["store"] == "mymarket":
                raise CommandError("boom")

        with patch("ingestion.management.commands.run_all_daily_ingestion.call_command", side_effect=fake_call_command):
            with self.assertRaisesMessage(CommandError, "boom"):
                call_command("run_all_daily_ingestion")

        self.assertEqual(observed_stores, ["ab", "bazaar", "mymarket"])


class RunDailyIngestionCommandTests(TestCase):
    def test_max_pages_must_be_positive(self):
        with patch("ingestion.management.commands.run_daily_ingestion.import_module") as mock_import:
            with self.assertRaisesMessage(
                CommandError,
                "--max-pages must be a positive integer.",
            ):
                call_command("run_daily_ingestion", "--store", "ab", "--max-pages", "0")

        mock_import.assert_not_called()


class RunListingMatcherCommandTests(TestCase):
    def test_reconsider_matched_requires_include_matched(self):
        with patch("ingestion.management.commands.run_listing_matcher.match_store_listings") as mock_match:
            with self.assertRaisesMessage(
                CommandError,
                "--reconsider-matched requires --include-matched.",
            ):
                call_command("run_listing_matcher", "--reconsider-matched")

        mock_match.assert_not_called()

    def test_limit_must_be_positive(self):
        with patch("ingestion.management.commands.run_listing_matcher.match_store_listings") as mock_match:
            with self.assertRaisesMessage(
                CommandError,
                "--limit must be a positive integer.",
            ):
                call_command("run_listing_matcher", "--limit", "0")

        mock_match.assert_not_called()

    def test_progress_every_must_be_positive(self):
        with patch("ingestion.management.commands.run_listing_matcher.match_store_listings") as mock_match:
            with self.assertRaisesMessage(
                CommandError,
                "--progress-every must be a positive integer.",
            ):
                call_command("run_listing_matcher", "--progress-every", "0")

        mock_match.assert_not_called()


class ImportPipelineTests(TestCase):
    def _write_csv(self, rows):
        headers = []
        for row in rows:
            for key in row.keys():
                if key not in headers:
                    headers.append(key)

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
        self.addCleanup(lambda: os.path.exists(tmp.name) and os.unlink(tmp.name))
        writer = csv.DictWriter(tmp, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        tmp.close()
        return tmp.name

    def test_importing_same_csv_twice_updates_without_duplicates(self):
        first_rows = [
            {
                "name": "ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 425gr",
                "sku": "sku-1",
                "brand": "Φρεσκούλης",
                "root_category": "freska-froyta-lachanika",
                "url": "https://example.com/a",
                "final_price": "2.10",
                "final_unit_price": "4.9412",
                "hidden_price": "2.10",
                "hidden_unit_price": "4.9412",
                "discount_percent": "10",
                "one_plus_one": "false",
                "two_plus_one": "false",
                "promo_text": "Weekend deal",
                "offer": "true",
            },
            {
                "name": "Ντομάτες 1kg",
                "sku": "sku-2",
                "brand": "",
                "root_category": "freska-froyta-lachanika",
                "url": "https://example.com/b",
                "final_price": "1.99",
                "offer": "false",
            },
        ]
        second_rows = [
            {
                "name": "ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 425gr",
                "sku": "sku-1",
                "brand": "Φρεσκούλης",
                "root_category": "freska-froyta-lachanika",
                "url": "https://example.com/a",
                "final_price": "1.95",
                "final_unit_price": "4.5882",
                "hidden_price": "0.98",
                "hidden_unit_price": "2.2941",
                "discount_percent": "20",
                "one_plus_one": "true",
                "two_plus_one": "false",
                "promo_text": "1+1 offer",
                "offer": "true",
            },
            {
                "name": "Ντομάτες 1kg",
                "sku": "sku-2",
                "brand": "",
                "root_category": "freska-froyta-lachanika",
                "url": "https://example.com/b",
                "final_price": "1.99",
                "offer": "false",
            },
        ]

        first_file = self._write_csv(first_rows)
        second_file = self._write_csv(second_rows)

        call_command("import_store_csv", "--store", "sklavenitis", "--file", first_file)
        call_command("import_store_csv", "--store", "sklavenitis", "--file", second_file)

        self.assertEqual(StoreListing.objects.count(), 2)
        updated_listing = StoreListing.objects.get(store_sku="sku-1")
        unchanged_listing = StoreListing.objects.get(store_sku="sku-2")
        self.assertEqual(str(updated_listing.final_price), "1.95")
        self.assertEqual(str(updated_listing.hidden_price), "0.98")
        self.assertEqual(str(updated_listing.hidden_unit_price), "2.29")
        self.assertEqual(updated_listing.discount_percent, 20)
        self.assertTrue(updated_listing.one_plus_one)
        self.assertFalse(updated_listing.two_plus_one)
        self.assertEqual(updated_listing.promo_text, "1+1 offer")
        self.assertEqual(updated_listing.root_category, "freska-froyta-lachanika")
        self.assertTrue(updated_listing.offer)
        self.assertFalse(unchanged_listing.offer)

        self.assertEqual(PriceHistory.objects.count(), 4)
        self.assertEqual(CrawlerRun.objects.count(), 2)
        self.assertTrue(all(run.status == CrawlerRun.Status.SUCCESS for run in CrawlerRun.objects.all()))

    def test_import_preloads_matching_listings_in_single_select(self):
        rows = [
            {
                "name": "Product 1",
                "sku": "batch-sku-1",
                "url": "https://example.com/batch-1",
                "final_price": "1.10",
            },
            {
                "name": "Product 2",
                "sku": "batch-sku-2",
                "url": "https://example.com/batch-2",
                "final_price": "1.20",
            },
            {
                "name": "Product 3",
                "sku": "batch-sku-3",
                "url": "https://example.com/batch-3",
                "final_price": "1.30",
            },
        ]

        with CaptureQueriesContext(connection) as queries:
            summary = import_rows_for_store(
                store_name="bazaar",
                rows=rows,
            )

        self.assertEqual(summary.created, 3)
        listing_selects = [
            query["sql"]
            for query in queries.captured_queries
            if query["sql"].lstrip().upper().startswith("SELECT")
            and "INGESTION_STORELISTING" in query["sql"].upper()
        ]
        self.assertEqual(len(listing_selects), 1)

    def test_missing_listings_are_marked_inactive_on_new_run(self):
        first_rows = [
            {
                "name": "Μαρούλι 1τεμ",
                "sku": "sku-10",
                "root_category": "frouta-lachanika",
                "url": "https://example.com/l1",
                "final_price": "0.99",
                "offer": "false",
            },
            {
                "name": "Αγγούρι 1τεμ",
                "sku": "sku-11",
                "root_category": "frouta-lachanika",
                "url": "https://example.com/l2",
                "final_price": "0.79",
                "offer": "false",
            },
        ]
        second_rows = [
            {
                "name": "Μαρούλι 1τεμ",
                "sku": "sku-10",
                "root_category": "frouta-lachanika",
                "url": "https://example.com/l1",
                "final_price": "0.95",
                "offer": "true",
            },
        ]

        first_file = self._write_csv(first_rows)
        second_file = self._write_csv(second_rows)

        call_command("import_store_csv", "--store", "mymarket", "--file", first_file)
        call_command("import_store_csv", "--store", "mymarket", "--file", second_file)

        still_active = StoreListing.objects.get(store_sku="sku-10")
        now_inactive = StoreListing.objects.get(store_sku="sku-11")
        self.assertTrue(still_active.is_active)
        self.assertFalse(now_inactive.is_active)

        self.assertEqual(PriceHistory.objects.count(), 3)
        runs = list(CrawlerRun.objects.filter(store__name="mymarket").order_by("started_at"))
        self.assertEqual(len(runs), 2)
        self.assertEqual([run.items_seen for run in runs], [2, 1])
        self.assertTrue(all(run.status == CrawlerRun.Status.SUCCESS for run in runs))

    def test_run_matcher_from_import_reconsiders_already_linked_listings_without_churn(self):
        first_summary = import_rows_for_store(
            store_name="sklavenitis",
            rows=[
                {
                    "name": "Μήλα Gala Εισαγωγής",
                    "sku": "sku-30",
                    "root_category": "freska-froyta-lachanika",
                    "url": "https://example.com/sku-30",
                    "final_price": "2.50",
                }
            ],
            snapshot_at=timezone.now(),
            run_matcher=True,
            source_label="test:first",
        )
        listing = StoreListing.objects.get(store_sku="sku-30")
        self.assertIsNotNone(listing.product_id)
        self.assertEqual(first_summary.matcher_processed, 1)

        second_summary = import_rows_for_store(
            store_name="sklavenitis",
            rows=[
                {
                    "name": "Μήλα Gala Εισαγωγής",
                    "sku": "sku-30",
                    "root_category": "freska-froyta-lachanika",
                    "url": "https://example.com/sku-30",
                    "final_price": "2.40",
                }
            ],
            snapshot_at=timezone.now(),
            run_matcher=True,
            source_label="test:second",
        )
        listing.refresh_from_db()

        self.assertEqual(second_summary.updated, 1)
        self.assertEqual(second_summary.matcher_processed, 1)
        self.assertEqual(second_summary.matcher_auto_matched, 0)
        self.assertEqual(second_summary.matcher_review_created, 0)
        self.assertEqual(second_summary.matcher_created_products, 0)
        self.assertEqual(Product.objects.count(), 1)
        self.assertEqual(MatchReview.objects.count(), 0)

    def test_run_matcher_from_import_can_reassign_changed_matched_listing(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        store = Store.objects.create(name="sklavenitis")
        CategoryAlias.objects.create(
            store=store,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )

        wrong_product = Product.objects.create(
            canonical_name="Ανανάς ΦΡΕΣΚΟΥΛΗΣ 200g",
            brand_normalized="freskoulis",
            quantity_value="200",
            quantity_unit="g",
            category=fruits,
        )
        target_product = Product.objects.create(
            canonical_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value="200",
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=store,
            store_sku="sku-reconsider",
            store_name="Ανανάς ΦΡΕΣΚΟΥΛΗΣ 200g",
            store_brand="Φρεσκούλης",
            source_category="freska-froyta-lachanika",
            url="https://example.com/sku-reconsider",
            final_price="2.50",
            product=wrong_product,
        )

        summary = import_rows_for_store(
            store_name="sklavenitis",
            rows=[
                {
                    "name": "ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
                    "sku": "sku-reconsider",
                    "brand": "Φρεσκούλης",
                    "root_category": "freska-froyta-lachanika",
                    "url": "https://example.com/sku-reconsider",
                    "final_price": "2.40",
                }
            ],
            snapshot_at=timezone.now(),
            run_matcher=True,
            source_label="test:reconsider",
        )
        listing.refresh_from_db()

        self.assertEqual(summary.updated, 1)
        self.assertEqual(summary.matcher_processed, 1)
        self.assertEqual(summary.matcher_auto_matched, 1)
        self.assertEqual(summary.matcher_review_created, 0)
        self.assertEqual(summary.matcher_created_products, 0)
        self.assertEqual(listing.product_id, target_product.id)


class StoreListingModelTests(TestCase):
    def test_save_updates_search_store_name_when_using_update_fields(self):
        store = Store.objects.create(name="ab")
        listing = StoreListing.objects.create(
            store=store,
            store_sku="search-store-name",
            store_name="Fresh milk",
            url="https://example.com/search-store-name",
            final_price="1.20",
        )

        listing.store_name = "Φρεσκούλης Σαλάτα"
        listing.save(update_fields=["store_name"])
        listing.refresh_from_db()

        self.assertEqual(
            listing.search_store_name,
            build_search_text("Φρεσκούλης Σαλάτα"),
        )
