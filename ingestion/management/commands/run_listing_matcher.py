from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from ingestion.models import StoreListing
from matching.matcher import match_store_listings


class Command(BaseCommand):
    help = "Run listing-to-product matching for unmatched or selected listings."

    def add_arguments(self, parser):
        parser.add_argument(
            "--store",
            help="Optional store name filter.",
        )
        parser.add_argument(
            "--listing-id",
            action="append",
            type=int,
            help="Optional specific listing id; repeat for many.",
        )
        parser.add_argument(
            "--include-matched",
            action="store_true",
            help="Include listings that are already linked to a product.",
        )
        parser.add_argument(
            "--reconsider-matched",
            action="store_true",
            help=(
                "When combined with --include-matched, temporarily exclude the current product "
                "from candidate search so strong alternatives can replace it."
            ),
        )
        parser.add_argument(
            "--include-inactive",
            action="store_true",
            help="Include inactive listings.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Optional cap on number of processed listings.",
        )
        parser.add_argument(
            "--progress-every",
            type=int,
            default=100,
            help="Print matcher progress after every N processed listings.",
        )

    def handle(self, *args, **options):
        if options["reconsider_matched"] and not options["include_matched"]:
            raise CommandError("--reconsider-matched requires --include-matched.")
        if options.get("limit") is not None and options["limit"] <= 0:
            raise CommandError("--limit must be a positive integer.")
        if options["progress_every"] <= 0:
            raise CommandError("--progress-every must be a positive integer.")

        listing_ids = options.get("listing_id")
        only_unmatched = not options["include_matched"]

        if options.get("store"):
            filtered_ids = list(
                StoreListing.objects.filter(store__name=options["store"]).values_list("id", flat=True)
            )
            if listing_ids:
                requested = set(listing_ids)
                listing_ids = [listing_id for listing_id in filtered_ids if listing_id in requested]
            else:
                listing_ids = filtered_ids

        summary = match_store_listings(
            listing_ids=listing_ids,
            only_unmatched=only_unmatched,
            include_inactive=options["include_inactive"],
            limit=options.get("limit"),
            reconsider_matched=options["reconsider_matched"],
            progress_every=options["progress_every"],
            progress_callback=self.stdout.write,
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Matcher complete (processed={summary.processed}, auto={summary.auto_matched}, "
                f"review={summary.review_created}, new_products={summary.created_products})."
            )
        )
