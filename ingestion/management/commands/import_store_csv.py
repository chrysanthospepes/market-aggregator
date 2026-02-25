from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ingestion.services.importer import import_rows_for_store, read_csv_rows


class Command(BaseCommand):
    help = "Import a store CSV with idempotent upserts and snapshot bookkeeping."

    def add_arguments(self, parser):
        parser.add_argument("--store", required=True, help="Store slug/name, e.g. sklavenitis")
        parser.add_argument("--file", required=True, help="CSV file path")
        parser.add_argument(
            "--snapshot-at",
            help="Optional snapshot timestamp in ISO-8601 format.",
        )
        parser.add_argument(
            "--run-matcher",
            action="store_true",
            help="Run matching logic only for changed/new listings after import.",
        )

    def handle(self, *args, **options):
        file_path = Path(options["file"]).expanduser()
        if not file_path.exists():
            raise CommandError(f"CSV file does not exist: {file_path}")

        snapshot = None
        if options.get("snapshot_at"):
            parsed = parse_datetime(options["snapshot_at"])
            if parsed is None:
                raise CommandError("Invalid --snapshot-at value. Use ISO-8601 datetime.")
            if timezone.is_naive(parsed):
                parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
            snapshot = parsed

        rows = read_csv_rows(file_path)
        if not rows:
            self.stdout.write(self.style.WARNING("CSV has no rows; nothing imported."))
            return

        try:
            summary = import_rows_for_store(
                store_name=options["store"],
                rows=rows,
                snapshot_at=snapshot,
                run_matcher=options["run_matcher"],
                source_label=f"csv:{file_path.name}",
            )
        except Exception as exc:  # pragma: no cover - surfaced as command error path
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            self.style.SUCCESS(
                "Import complete "
                f"(run_id={summary.crawler_run_id}, seen={summary.items_seen}, "
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
