from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional

from django.db import IntegrityError
from django.db.models import Q
from rapidfuzz import fuzz

from catalog.category_mapping import resolve_category_id_for_source
from catalog.models import Category, Product
from comparison.models import MatchReview
from ingestion.models import StoreListing
from matching.normalizer import (
    Quantity,
    build_normalized_key,
    normalize_brand,
    normalize_listing_text,
    normalize_text,
)


AUTO_TIER_A_SIMILARITY = 0.92
AUTO_TIER_B_SIMILARITY = 0.95
MANUAL_REVIEW_MIN_SCORE = 0.80


@dataclass
class MatchResult:
    processed: int = 0
    auto_matched: int = 0
    review_created: int = 0
    created_products: int = 0


@dataclass(frozen=True)
class CandidateScore:
    product: Product
    score: Decimal
    name_similarity: float
    brand_exact: bool
    quantity_exact: bool


def _to_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return fuzz.token_sort_ratio(left, right) / 100.0


def _quantity_exact(left: Optional[Quantity], product: Product) -> bool:
    if left is None:
        return False
    if product.quantity_value is None or not product.quantity_unit:
        return False
    return left.value == product.quantity_value and left.unit == product.quantity_unit


def _score_candidate(listing: StoreListing, product: Product) -> CandidateScore:
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    product_name = product.canonical_name
    product_brand = normalize_brand(product.brand_normalized)
    product_name_norm = normalize_text(product_name)

    name_similarity = _to_ratio(listing_norm.normalized_name, product_name_norm)
    brand_exact = bool(
        listing_norm.brand_normalized
        and product_brand
        and listing_norm.brand_normalized == product_brand
    )
    quantity_exact = _quantity_exact(listing_norm.quantity, product=product)

    score = (Decimal("0.35") * Decimal(int(brand_exact))) + (
        Decimal("0.30") * Decimal(int(quantity_exact))
    ) + (Decimal("0.35") * Decimal(str(name_similarity)))

    return CandidateScore(
        product=product,
        score=score,
        name_similarity=name_similarity,
        brand_exact=brand_exact,
        quantity_exact=quantity_exact,
    )


def _candidate_queryset(listing: StoreListing):
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    candidates = Product.objects.all()
    listing_category_id = _resolve_listing_category_id(listing)

    if listing.source_category and listing_category_id is None:
        return Product.objects.none()
    if listing_category_id is not None:
        candidates = candidates.filter(category_id=listing_category_id)

    if listing_norm.quantity is not None:
        candidates = candidates.filter(
            quantity_value=listing_norm.quantity.value,
            quantity_unit=listing_norm.quantity.unit,
        )
    if listing_norm.brand_normalized:
        candidates = candidates.filter(
            Q(brand_normalized=listing_norm.brand_normalized)
            | Q(brand_normalized__isnull=True)
            | Q(brand_normalized="")
        )
    return candidates[:300]


def _create_or_get_product_for_listing(listing: StoreListing) -> tuple[Product, bool]:
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    quantity_value = listing_norm.quantity.value if listing_norm.quantity else None
    quantity_unit = listing_norm.quantity.unit if listing_norm.quantity else None
    category = _resolve_listing_category(listing)

    if listing_norm.normalized_key:
        existing = Product.objects.filter(normalized_key=listing_norm.normalized_key).first()
        if existing:
            return existing, False

    product = Product(
        canonical_name=listing.store_name,
        brand_normalized=listing_norm.brand_normalized,
        quantity_value=quantity_value,
        quantity_unit=quantity_unit,
        normalized_key=listing_norm.normalized_key,
        category=category,
    )
    try:
        product.save()
        return product, True
    except IntegrityError:
        if listing_norm.normalized_key:
            existing = Product.objects.filter(normalized_key=listing_norm.normalized_key).first()
            if existing:
                return existing, False
        raise


def create_forced_product_for_listing(listing: StoreListing) -> Product:
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    quantity_value = listing_norm.quantity.value if listing_norm.quantity else None
    quantity_unit = listing_norm.quantity.unit if listing_norm.quantity else None
    category = _resolve_listing_category(listing)
    return Product.objects.create(
        canonical_name=listing.store_name,
        brand_normalized=listing_norm.brand_normalized,
        quantity_value=quantity_value,
        quantity_unit=quantity_unit,
        normalized_key=None,
        category=category,
    )


def _resolve_listing_category_id(listing: StoreListing) -> Optional[int]:
    if not listing.source_category:
        return None
    return resolve_category_id_for_source(
        store_id=listing.store_id,
        source_category=listing.source_category,
    )


def _resolve_listing_category(listing: StoreListing) -> Optional[Category]:
    category_id = _resolve_listing_category_id(listing)
    if category_id is None:
        return None
    return Category.objects.filter(id=category_id).first()


def _best_candidate(listing: StoreListing) -> Optional[CandidateScore]:
    best: Optional[CandidateScore] = None
    for product in _candidate_queryset(listing):
        candidate = _score_candidate(listing=listing, product=product)
        if best is None or candidate.score > best.score:
            best = candidate
    return best


def _set_listing_product(listing: StoreListing, product: Product) -> None:
    if listing.product_id == product.id:
        return
    listing.product = product
    listing.save(update_fields=["product"])
    MatchReview.objects.filter(store_listing=listing).delete()


def _create_review(listing: StoreListing, candidate: CandidateScore) -> None:
    MatchReview.objects.update_or_create(
        store_listing=listing,
        candidate_product=candidate.product,
        defaults={
            "score": candidate.score.quantize(Decimal("0.0001")),
            "status": MatchReview.Status.PENDING,
            "notes": (
                f"brand_exact={candidate.brand_exact}, "
                f"quantity_exact={candidate.quantity_exact}, "
                f"name_similarity={candidate.name_similarity:.3f}"
            ),
        },
    )


def match_store_listings(
    *,
    listing_ids: Optional[Iterable[int]] = None,
    only_unmatched: bool = True,
    include_inactive: bool = False,
    limit: Optional[int] = None,
) -> MatchResult:
    queryset = StoreListing.objects.select_related("product", "store").order_by("id")
    if listing_ids is not None:
        queryset = queryset.filter(id__in=listing_ids)
    if only_unmatched:
        queryset = queryset.filter(product__isnull=True)
    if not include_inactive:
        queryset = queryset.filter(is_active=True)
    if limit is not None:
        queryset = queryset[:limit]

    result = MatchResult()
    for listing in queryset:
        result.processed += 1
        best = _best_candidate(listing)

        if best is not None:
            if best.brand_exact and best.quantity_exact and best.name_similarity >= AUTO_TIER_A_SIMILARITY:
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                continue

            if best.quantity_exact and best.name_similarity >= AUTO_TIER_B_SIMILARITY:
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                continue

            if best.score >= Decimal(str(MANUAL_REVIEW_MIN_SCORE)):
                _create_review(listing, best)
                result.review_created += 1
                continue

        product, created = _create_or_get_product_for_listing(listing)
        _set_listing_product(listing, product)
        if created:
            result.created_products += 1

    return result


def build_product_normalized_key(
    *,
    canonical_name: str,
    brand_normalized: Optional[str],
    quantity_value: Optional[Decimal],
    quantity_unit: Optional[str],
) -> Optional[str]:
    quantity = None
    if quantity_value is not None and quantity_unit:
        quantity = Quantity(value=quantity_value, unit=quantity_unit)
    return build_normalized_key(
        name=canonical_name,
        brand=brand_normalized,
        quantity=quantity,
    )
