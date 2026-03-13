from django.conf import settings
from django.db import models
from django.db.models import Q
from django.utils import timezone


class MatchReview(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "pending"
        APPROVED = "approved", "approved"
        REJECTED = "rejected", "rejected"

    store_listing = models.ForeignKey(
        "ingestion.StoreListing",
        on_delete=models.CASCADE,
        related_name="match_reviews",
    )
    candidate_product = models.ForeignKey(
        "catalog.Product",
        on_delete=models.CASCADE,
        related_name="match_reviews",
    )
    score = models.DecimalField(max_digits=5, decimal_places=4)
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["status", "-score"]
        indexes = [
            models.Index(fields=["status", "-score"], name="idx_matchreview_queue"),
        ]
        constraints = [
            models.CheckConstraint(
                condition=Q(score__gte=0) & Q(score__lte=1),
                name="ck_matchreview_score_range",
            ),
            models.UniqueConstraint(
                fields=["store_listing", "candidate_product"],
                name="uq_matchreview_listing_candidate",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.store_listing_id}->{self.candidate_product_id} ({self.status})"


class ListingProductReport(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "pending"
        REASSIGNED = "reassigned", "reassigned"
        DISMISSED = "dismissed", "dismissed"

    store_listing = models.ForeignKey(
        "ingestion.StoreListing",
        on_delete=models.CASCADE,
        related_name="listing_product_reports",
    )
    reported_product = models.ForeignKey(
        "catalog.Product",
        on_delete=models.SET_NULL,
        related_name="listing_product_reports",
        null=True,
        blank=True,
    )
    reassigned_product = models.ForeignKey(
        "catalog.Product",
        on_delete=models.SET_NULL,
        related_name="listing_product_reassignments",
        null=True,
        blank=True,
    )
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    report_count = models.PositiveIntegerField(default=1)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    last_reported_at = models.DateTimeField(default=timezone.now, db_index=True)
    resolved_at = models.DateTimeField(null=True, blank=True, db_index=True)
    resolved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="resolved_listing_product_reports",
        null=True,
        blank=True,
    )

    class Meta:
        ordering = ["status", "-last_reported_at", "-id"]
        indexes = [
            models.Index(
                fields=["status", "-last_reported_at"],
                name="idx_listingreport_queue",
            ),
            models.Index(
                fields=["store_listing", "status"],
                name="idx_lpr_listing_status",
            ),
        ]
        constraints = [
            models.CheckConstraint(
                condition=Q(report_count__gte=1),
                name="ck_listingreport_count_positive",
            ),
            models.UniqueConstraint(
                fields=["store_listing"],
                condition=Q(status="pending"),
                name="uq_listingreport_pending_listing",
            ),
        ]

    def __str__(self) -> str:
        return f"report:{self.store_listing_id} ({self.status})"
