from django.db import models
from django.db.models import Q


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
