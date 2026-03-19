from __future__ import annotations

from dataclasses import asdict
from importlib import import_module
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from crawlers import CRAWLER_MODULES
from ingestion.services.importer import import_rows_for_store


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = row.get("url") or f"{row.get('sku') or ''}|{row.get('name') or ''}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


class Command(BaseCommand):
    help = "Run crawler(s) and import their output through the ingestion pipeline."

    def add_arguments(self, parser):
        parser.add_argument(
            "--store",
            required=True,
            choices=sorted(CRAWLER_MODULES.keys()),
            help="Store crawler to execute.",
        )
        parser.add_argument(
            "--category",
            action="append",
            help="Optional category slug; repeat for multiple categories.",
        )
        parser.add_argument(
            "--max-pages",
            type=int,
            default=500,
            help="Max pagination pages per category.",
        )
        parser.add_argument(
            "--run-matcher",
            action="store_true",
            help="Run matcher for changed listings after import.",
        )
        parser.add_argument(
            "--save-combined-csv",
            help="Optional output path for combined crawl rows.",
        )

    def handle(self, *args, **options):
        if options["max_pages"] <= 0:
            raise CommandError("--max-pages must be a positive integer.")

        store = options["store"]
        module_path = CRAWLER_MODULES[store]

        try:
            crawler = import_module(module_path)
        except Exception as exc:  # pragma: no cover
            raise CommandError(f"Failed to import crawler module {module_path}: {exc}") from exc

        categories = options["category"] or list(getattr(crawler, "ROOT_CATEGORIES", []))
        if not categories:
            raise CommandError("No categories configured for crawler.")

        all_rows: list[dict[str, Any]] = []
        for category in categories:
            root_slug = crawler.to_category_slug(category)
            root_listing = crawler.to_category_url(root_slug)
            root_category_builder = getattr(crawler, "to_root_category", None)
            if callable(root_category_builder):
                root_category = root_category_builder(category) or root_slug
            else:
                root_category = root_slug

            self.stdout.write(
                f"Crawling category={root_slug} root_category={root_category} url={root_listing}"
            )
            crawled_rows = crawler.crawl_category_listing(
                root_listing=root_listing,
                root_category=root_category,
                max_pages=options["max_pages"],
            )
            self.stdout.write(f"Collected {len(crawled_rows)} rows from {root_slug}")
            all_rows.extend(asdict(row) for row in crawled_rows)

        rows = _dedupe_rows(all_rows)
        deduped_count = len(all_rows) - len(rows)
        if deduped_count > 0:
            self.stdout.write(
                f"Deduped {deduped_count} duplicate rows (kept {len(rows)} unique rows)."
            )
        else:
            self.stdout.write(f"Prepared {len(rows)} unique rows for import.")
        if not rows:
            raise CommandError("Crawler returned zero rows; refusing to import.")

        csv_output = options.get("save_combined_csv")
        if csv_output:
            import csv

            destination = Path(csv_output).expanduser()
            destination.parent.mkdir(parents=True, exist_ok=True)
            fieldnames = list(rows[0].keys())
            with destination.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
            self.stdout.write(f"Saved combined CSV to {destination}")

        summary = import_rows_for_store(
            store_name=store,
            rows=rows,
            snapshot_at=timezone.now(),
            run_matcher=options["run_matcher"],
            source_label=f"crawler:{store}",
            matcher_progress_every=100,
            matcher_progress_callback=self.stdout.write if options["run_matcher"] else None,
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Run complete (run_id={summary.crawler_run_id}, seen={summary.items_seen}, "
                f"created={summary.created}, updated={summary.updated}, "
                f"unchanged={summary.unchanged}, deactivated={summary.deactivated}, "
                f"errors={summary.errored_rows})."
            )
        )
        if options["run_matcher"]:
            self.stdout.write(
                "Matcher "
                f"(processed={summary.matcher_processed}, auto={summary.matcher_auto_matched}, "
                f"review={summary.matcher_review_created}, new_products={summary.matcher_created_products})."
            )
