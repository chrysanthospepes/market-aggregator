from django.db import models
from django.db.models import F, Q
from django.utils import timezone


class StoreListing(models.Model):
    store = models.ForeignKey(
        "catalog.Store",
        on_delete=models.CASCADE,
        related_name="listings",
    )
    store_sku = models.CharField(max_length=128, null=True, blank=True)
    store_name = models.CharField(max_length=512)
    store_brand = models.CharField(max_length=255, null=True, blank=True)
    url = models.URLField(max_length=2000, null=True, blank=True)
    image_url = models.URLField(max_length=2000, null=True, blank=True)
    final_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
    )
    final_unit_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
    )
    hidden_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
    )
    hidden_unit_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
    )
    original_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
    )
    original_unit_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
    )
    source_category = models.CharField(max_length=128, null=True, blank=True, db_index=True)
    root_category = models.CharField(max_length=255, null=True, blank=True, db_index=True)
    unit_of_measure = models.CharField(max_length=64, null=True, blank=True)
    discount_percent = models.PositiveIntegerField(null=True, blank=True)
    offer = models.BooleanField(default=False)
    one_plus_one = models.BooleanField(default=False)
    two_plus_one = models.BooleanField(default=False)
    promo_text = models.CharField(max_length=512, null=True, blank=True)
    snapshot_at = models.DateTimeField(default=timezone.now, db_index=True)
    last_seen_at = models.DateTimeField(default=timezone.now, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)
    product = models.ForeignKey(
        "catalog.Product",
        on_delete=models.SET_NULL,
        related_name="store_listings",
        null=True,
        blank=True,
    )

    class Meta:
        ordering = ["store__name", "store_name"]
        indexes = [
            models.Index(fields=["store", "is_active"], name="idx_listing_store_active"),
            models.Index(fields=["product", "is_active"], name="idx_listing_product_active"),
            models.Index(fields=["store", "last_seen_at"], name="idx_listing_store_seen"),
        ]
        constraints = [
            models.CheckConstraint(
                condition=~Q(store_name=""),
                name="ck_listing_name_nonempty",
            ),
            models.CheckConstraint(
                condition=(
                    (Q(store_sku__isnull=False) & ~Q(store_sku=""))
                    | (Q(url__isnull=False) & ~Q(url=""))
                ),
                name="ck_listing_id_source_present",
            ),
            models.CheckConstraint(
                condition=Q(final_price__gte=0) | Q(final_price__isnull=True),
                name="ck_listing_final_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(final_unit_price__gte=0) | Q(final_unit_price__isnull=True),
                name="ck_listing_final_unit_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(hidden_price__gte=0) | Q(hidden_price__isnull=True),
                name="ck_listing_hidden_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(hidden_unit_price__gte=0) | Q(hidden_unit_price__isnull=True),
                name="ck_listing_hidden_unit_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(original_price__gte=0) | Q(original_price__isnull=True),
                name="ck_listing_original_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(original_unit_price__gte=0) | Q(original_unit_price__isnull=True),
                name="ck_listing_original_unit_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(last_seen_at__gte=F("snapshot_at")),
                name="ck_listing_last_seen_gte_snapshot",
            ),
            models.UniqueConstraint(
                fields=["store", "store_sku"],
                condition=Q(store_sku__isnull=False) & ~Q(store_sku=""),
                name="uq_listing_store_sku",
            ),
            models.UniqueConstraint(
                fields=["store", "url"],
                condition=(
                    Q(url__isnull=False)
                    & ~Q(url="")
                    & (Q(store_sku__isnull=True) | Q(store_sku=""))
                ),
                name="uq_listing_store_url_no_sku",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.store.name}: {self.store_name}"


class CrawlerRun(models.Model):
    class Status(models.TextChoices):
        RUNNING = "running", "running"
        SUCCESS = "success", "success"
        FAILED = "failed", "failed"
        PARTIAL = "partial", "partial"

    store = models.ForeignKey(
        "catalog.Store",
        on_delete=models.CASCADE,
        related_name="crawler_runs",
    )
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.RUNNING,
    )
    error_summary = models.TextField(blank=True)
    items_seen = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["store", "-started_at"], name="idx_run_store_started"),
            models.Index(fields=["status"], name="idx_run_status"),
        ]
        constraints = [
            models.CheckConstraint(
                condition=Q(finished_at__isnull=True) | Q(finished_at__gte=F("started_at")),
                name="ck_run_finished_after_started",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.store.name} {self.started_at.isoformat()} ({self.status})"


class PriceHistory(models.Model):
    store_listing = models.ForeignKey(
        StoreListing,
        on_delete=models.CASCADE,
        related_name="price_history",
    )
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    unit_price = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    captured_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        ordering = ["-captured_at"]
        indexes = [
            models.Index(
                fields=["store_listing", "-captured_at"],
                name="idx_pricehistory_listing_time",
            ),
        ]
        constraints = [
            models.CheckConstraint(
                condition=Q(price__gte=0) | Q(price__isnull=True),
                name="ck_ph_price_nonneg",
            ),
            models.CheckConstraint(
                condition=Q(unit_price__gte=0) | Q(unit_price__isnull=True),
                name="ck_ph_unit_price_nonneg",
            ),
            models.UniqueConstraint(
                fields=["store_listing", "captured_at"],
                name="uq_pricehistory_listing_captured",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.store_listing_id} @ {self.captured_at.isoformat()}"
