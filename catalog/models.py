from django.db import models
from django.db.models import Q
from django.utils.translation import get_language

from catalog.search_normalizer import build_search_text


def _normalize_source_slug(value: str) -> str:
    normalized = (value or "").strip().lower()
    normalized = normalized.strip("/")
    normalized = normalized.replace("_", "-")
    normalized = " ".join(normalized.split()).replace(" ", "-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized


class Store(models.Model):
    name = models.CharField(max_length=64, unique=True)

    class Meta:
        ordering = ["name"]
        constraints = [
            models.CheckConstraint(
                condition=~Q(name=""),
                name="ck_store_name_nonempty",
            ),
        ]

    def __str__(self) -> str:
        return self.name


class Category(models.Model):
    name = models.CharField(max_length=128, unique=True)
    name_en = models.CharField(max_length=128, blank=True, default="")
    slug = models.SlugField(max_length=128, unique=True)

    class Meta:
        ordering = ["name"]
        constraints = [
            models.CheckConstraint(
                condition=~Q(name=""),
                name="ck_category_name_nonempty",
            ),
            models.CheckConstraint(
                condition=~Q(slug=""),
                name="ck_category_slug_nonempty",
            ),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    def display_name(self) -> str:
        language_code = (get_language() or "").lower()
        if language_code.startswith("en") and self.name_en:
            return self.name_en
        return self.name


class Product(models.Model):
    class QuantityUnit(models.TextChoices):
        G = "g", "g"
        KG = "kg", "kg"
        ML = "ml", "ml"
        L = "l", "l"
        TEMAXIO = "temaxio", "temaxio"

    canonical_name = models.CharField(max_length=255)
    brand_normalized = models.CharField(max_length=128, null=True, blank=True)
    quantity_value = models.DecimalField(
        max_digits=10,
        decimal_places=3,
        null=True,
        blank=True,
    )
    quantity_unit = models.CharField(
        max_length=16,
        choices=QuantityUnit.choices,
        null=True,
        blank=True,
    )
    normalized_key = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
    )
    search_name = models.TextField(blank=True, default="")
    image = models.FileField(upload_to="products/", null=True, blank=True)
    category = models.ForeignKey(
        Category,
        on_delete=models.PROTECT,
        related_name="products",
        null=True,
        blank=True,
    )

    class Meta:
        ordering = ["canonical_name"]
        constraints = [
            models.CheckConstraint(
                condition=~Q(canonical_name=""),
                name="ck_product_name_nonempty",
            ),
            models.CheckConstraint(
                condition=Q(quantity_value__gt=0) | Q(quantity_value__isnull=True),
                name="ck_product_qty_positive",
            ),
            models.CheckConstraint(
                condition=(
                    (
                        Q(quantity_value__isnull=True)
                        & (Q(quantity_unit__isnull=True) | Q(quantity_unit=""))
                    )
                    | (
                        Q(quantity_value__isnull=False)
                        & Q(quantity_unit__isnull=False)
                        & ~Q(quantity_unit="")
                    )
                ),
                name="ck_product_qty_pair",
            ),
            models.UniqueConstraint(
                fields=["normalized_key"],
                condition=Q(normalized_key__isnull=False) & ~Q(normalized_key=""),
                name="uq_product_norm_key_present",
            ),
        ]

    def save(self, *args, **kwargs):
        self.search_name = build_search_text(self.canonical_name)
        update_fields = kwargs.get("update_fields")
        if update_fields is not None:
            fields = set(update_fields)
            if "canonical_name" in fields or "search_name" in fields:
                fields.add("search_name")
            kwargs["update_fields"] = tuple(fields)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.canonical_name


class CategoryAlias(models.Model):
    store = models.ForeignKey(
        Store,
        on_delete=models.CASCADE,
        related_name="category_aliases",
        null=True,
        blank=True,
    )
    source_slug = models.CharField(max_length=128)
    category = models.ForeignKey(
        Category,
        on_delete=models.PROTECT,
        related_name="aliases",
    )

    class Meta:
        ordering = ["source_slug", "store__name"]
        constraints = [
            models.CheckConstraint(
                condition=~Q(source_slug=""),
                name="ck_categoryalias_source_nonempty",
            ),
            models.UniqueConstraint(
                fields=["store", "source_slug"],
                name="uq_categoryalias_store_source",
            ),
            models.UniqueConstraint(
                fields=["source_slug"],
                condition=Q(store__isnull=True),
                name="uq_categoryalias_global_source",
            ),
        ]

    def save(self, *args, **kwargs):
        self.source_slug = _normalize_source_slug(self.source_slug)
        super().save(*args, **kwargs)
        from catalog.category_mapping import resolve_category_id_for_source

        resolve_category_id_for_source.cache_clear()

    def delete(self, *args, **kwargs):
        super().delete(*args, **kwargs)
        from catalog.category_mapping import resolve_category_id_for_source

        resolve_category_id_for_source.cache_clear()

    def __str__(self) -> str:
        if self.store_id:
            return f"{self.store.name}:{self.source_slug} -> {self.category.slug}"
        return f"*:{self.source_slug} -> {self.category.slug}"
