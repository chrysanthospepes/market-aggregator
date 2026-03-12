from django.contrib import admin

from comparison.models import MatchReview
from comparison.review_actions import approve_match_reviews, reject_match_reviews


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
