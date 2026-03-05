from django.test import TestCase
from django.utils import timezone

import csv
import os
import tempfile
from importlib import import_module
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError

from catalog.models import Category, CategoryAlias, Product, Store
from comparison.models import MatchReview
from crawlers import CRAWLER_MODULES, CRAWLER_RUN_ORDER
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


class RunAllDailyIngestionCommandTests(TestCase):
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
        self.assertEqual(str(updated_listing.final_price), "1.95")

        self.assertEqual(PriceHistory.objects.count(), 4)
        self.assertEqual(CrawlerRun.objects.count(), 2)
        self.assertTrue(all(run.status == CrawlerRun.Status.SUCCESS for run in CrawlerRun.objects.all()))

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
