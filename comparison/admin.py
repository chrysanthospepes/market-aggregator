from django.contrib import admin
from django.db import transaction

from comparison.models import ListingProductReport, MatchReview
from comparison.review_actions import approve_match_reviews, reject_match_reviews


@admin.register(MatchReview)
class MatchReviewAdmin(admin.ModelAdmin):
    list_display = ["id", "store_listing", "candidate_product", "score", "status"]
    list_filter = ["status", "store_listing__store"]
    ordering = ["status", "-score", "id"]
    list_select_related = ["store_listing__store", "candidate_product"]
    search_fields = [
        "store_listing__store__name",
        "store_listing__store_sku",
        "store_listing__store_name",
        "candidate_product__canonical_name",
        "notes",
    ]
    autocomplete_fields = ["store_listing", "candidate_product"]
    actions = ["approve_selected_reviews", "reject_selected_reviews"]

    @admin.action(description="Approve selected reviews and link listing -> candidate")
    def approve_selected_reviews(self, request, queryset):
        result = approve_match_reviews(queryset)

        self.message_user(
            request,
            "Approved "
            f"{result.approved} review(s). Auto-rejected {result.auto_rejected} "
            "conflicting pending review(s).",
        )

    @admin.action(description="Reject selected reviews")
    def reject_selected_reviews(self, request, queryset):
        result = reject_match_reviews(queryset)

        self.message_user(
            request,
            f"Rejected {result.rejected} review(s). Created {result.forced_new_products} "
            f"new product(s) and auto-rejected {result.auto_rejected} pending review(s).",
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
                approve_match_reviews(MatchReview.objects.filter(pk=obj.pk))
            elif (
                change
                and previous_status != MatchReview.Status.REJECTED
                and obj.status == MatchReview.Status.REJECTED
            ):
                reject_match_reviews(MatchReview.objects.filter(pk=obj.pk))


@admin.register(ListingProductReport)
class ListingProductReportAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "store_listing",
        "reported_product",
        "reassigned_product",
        "status",
        "report_count",
        "last_reported_at",
        "resolved_by",
    ]
    list_filter = ["status", "store_listing__store", "resolved_by"]
    ordering = ["status", "-last_reported_at", "-id"]
    list_select_related = [
        "store_listing__store",
        "reported_product",
        "reassigned_product",
        "resolved_by",
    ]
    date_hierarchy = "last_reported_at"
    search_fields = [
        "store_listing__store__name",
        "store_listing__store_sku",
        "store_listing__store_name",
        "reported_product__canonical_name",
        "reassigned_product__canonical_name",
        "resolved_by__username",
    ]
    autocomplete_fields = [
        "store_listing",
        "reported_product",
        "reassigned_product",
        "resolved_by",
    ]
