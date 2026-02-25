from django.contrib import admin
from django.db import transaction

from comparison.models import MatchReview
from matching.matcher import create_forced_product_for_listing


@admin.register(MatchReview)
class MatchReviewAdmin(admin.ModelAdmin):
    list_display = ["id", "store_listing", "candidate_product", "score", "status"]
    list_filter = ["status"]
    search_fields = [
        "store_listing__store_name",
        "candidate_product__canonical_name",
        "notes",
    ]
    autocomplete_fields = ["store_listing", "candidate_product"]
    actions = ["approve_selected_reviews", "reject_selected_reviews"]

    def _reject_pending_for_listing(self, review: MatchReview) -> int:
        return MatchReview.objects.filter(
            store_listing=review.store_listing,
            status=MatchReview.Status.PENDING,
        ).exclude(id=review.id).update(status=MatchReview.Status.REJECTED)

    @admin.action(description="Approve selected reviews and link listing -> candidate")
    def approve_selected_reviews(self, request, queryset):
        approved = 0
        auto_rejected = 0

        with transaction.atomic():
            for review in queryset.select_related("store_listing", "candidate_product"):
                listing = review.store_listing
                listing.product = review.candidate_product
                listing.save(update_fields=["product"])

                if review.status != MatchReview.Status.APPROVED:
                    review.status = MatchReview.Status.APPROVED
                    review.save(update_fields=["status"])
                    approved += 1

                auto_rejected += self._reject_pending_for_listing(review)

        self.message_user(
            request,
            f"Approved {approved} review(s). Auto-rejected {auto_rejected} conflicting pending review(s).",
        )

    @admin.action(description="Reject selected reviews")
    def reject_selected_reviews(self, request, queryset):
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

        self.message_user(
            request,
            f"Rejected {rejected} review(s). Created {forced_new_products} new product(s) "
            f"and auto-rejected {auto_rejected} pending review(s).",
        )

    def save_model(self, request, obj, form, change):
        previous_status = None
        if change and obj.pk:
            previous_status = (
                MatchReview.objects.filter(pk=obj.pk).values_list("status", flat=True).first()
            )

        with transaction.atomic():
            super().save_model(request, obj, form, change)

            if obj.status == MatchReview.Status.APPROVED:
                listing = obj.store_listing
                listing.product = obj.candidate_product
                listing.save(update_fields=["product"])
                self._reject_pending_for_listing(obj)
            elif (
                change
                and previous_status != MatchReview.Status.REJECTED
                and obj.status == MatchReview.Status.REJECTED
            ):
                listing = obj.store_listing
                listing.product = create_forced_product_for_listing(listing)
                listing.save(update_fields=["product"])
                MatchReview.objects.filter(
                    store_listing=listing,
                    status=MatchReview.Status.PENDING,
                ).update(status=MatchReview.Status.REJECTED)
