from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q

from catalog.models import Product
from catalog.services.product_images import ensure_product_image_from_listing


class Command(BaseCommand):
    help = "Download and attach product images from linked store listings when missing."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            help="Optional max number of products to process.",
        )

    def handle(self, *args, **options):
        queryset = Product.objects.filter(Q(image__isnull=True) | Q(image="")).order_by("id")
        if options.get("limit"):
            queryset = queryset[: options["limit"]]

        processed = 0
        downloaded = 0
        skipped = 0

        for product in queryset:
            processed += 1
            listing = (
                product.store_listings.filter(is_active=True)
                .exclude(image_url__isnull=True)
                .exclude(image_url="")
                .order_by("id")
                .first()
            )
            if listing is None:
                skipped += 1
                continue

            if ensure_product_image_from_listing(product=product, listing=listing):
                downloaded += 1
            else:
                skipped += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Backfill complete (processed={processed}, downloaded={downloaded}, skipped={skipped})."
            )
        )
