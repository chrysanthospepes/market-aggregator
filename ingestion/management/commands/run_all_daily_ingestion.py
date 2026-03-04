from __future__ import annotations

from pathlib import Path

from django.core.management import call_command
from django.core.management.base import BaseCommand

from crawlers import CRAWLER_RUN_ORDER


class Command(BaseCommand):
    help = "Run all store crawlers sequentially through the daily ingestion pipeline."

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-pages",
            type=int,
            default=500,
            help="Max pagination pages per category for each store crawler.",
        )
        parser.add_argument(
            "--run-matcher",
            action="store_true",
            help="Run matcher for changed listings after each store import.",
        )
        parser.add_argument(
            "--save-combined-csv-dir",
            help="Optional directory where each store's combined crawl CSV will be written.",
        )

    def handle(self, *args, **options):
        csv_dir = options.get("save_combined_csv_dir")
        csv_root = Path(csv_dir).expanduser() if csv_dir else None

        for index, store in enumerate(CRAWLER_RUN_ORDER, start=1):
            self.stdout.write(f"[{index}/{len(CRAWLER_RUN_ORDER)}] Starting store={store}")

            command_kwargs = {
                "store": store,
                "max_pages": options["max_pages"],
                "run_matcher": options["run_matcher"],
            }
            if csv_root is not None:
                command_kwargs["save_combined_csv"] = str(csv_root / f"{store}.csv")

            call_command("run_daily_ingestion", **command_kwargs)

            self.stdout.write(self.style.SUCCESS(f"[{index}/{len(CRAWLER_RUN_ORDER)}] Finished store={store}"))
