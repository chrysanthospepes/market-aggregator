from __future__ import annotations

from dataclasses import dataclass

from django.db import transaction

from catalog.services.product_images import ensure_product_image_from_listing
from comparison.models import MatchReview
from matching.matcher import create_forced_product_for_listing


@dataclass(frozen=True)
class ApproveMatchReviewsResult:
    approved: int = 0
    auto_rejected: int = 0


@dataclass(frozen=True)
class RejectMatchReviewsResult:
    rejected: int = 0
    forced_new_products: int = 0
    auto_rejected: int = 0


def reject_pending_reviews_for_listing(
    *,
    listing_id: int,
    exclude_review_id: int | None = None,
) -> int:
    queryset = MatchReview.objects.filter(
        store_listing_id=listing_id,
        status=MatchReview.Status.PENDING,
    )
    if exclude_review_id is not None:
        queryset = queryset.exclude(id=exclude_review_id)
    return queryset.update(status=MatchReview.Status.REJECTED)


def approve_match_reviews(queryset) -> ApproveMatchReviewsResult:
    approved = 0
    auto_rejected = 0

    with transaction.atomic():
        for review in queryset.select_related("store_listing", "candidate_product"):
            listing = review.store_listing
            listing.product = review.candidate_product
            listing.save(update_fields=["product"])
            ensure_product_image_from_listing(product=listing.product, listing=listing)

            if review.status != MatchReview.Status.APPROVED:
                review.status = MatchReview.Status.APPROVED
                review.save(update_fields=["status"])
                approved += 1

            auto_rejected += reject_pending_reviews_for_listing(
                listing_id=listing.id,
                exclude_review_id=review.id,
            )

    return ApproveMatchReviewsResult(
        approved=approved,
        auto_rejected=auto_rejected,
    )


def reject_match_reviews(queryset) -> RejectMatchReviewsResult:
    rejected = 0
    forced_new_products = 0
    auto_rejected = 0
    listing_to_new_product: dict[int, int] = {}

    with transaction.atomic():
        for review in queryset.select_related("store_listing"):
            listing = review.store_listing
            if listing.id not in listing_to_new_product:
                new_product = create_forced_product_for_listing(listing)
                listing.product = new_product
                listing.save(update_fields=["product"])
                ensure_product_image_from_listing(product=listing.product, listing=listing)
                listing_to_new_product[listing.id] = new_product.id
                forced_new_products += 1
                auto_rejected += MatchReview.objects.filter(
                    store_listing=listing,
                    status=MatchReview.Status.PENDING,
                ).update(status=MatchReview.Status.REJECTED)

            if review.status != MatchReview.Status.REJECTED:
                review.status = MatchReview.Status.REJECTED
                review.save(update_fields=["status"])
                rejected += 1

    return RejectMatchReviewsResult(
        rejected=rejected,
        forced_new_products=forced_new_products,
        auto_rejected=auto_rejected,
    )
