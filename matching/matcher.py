from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from time import monotonic
from typing import Iterable, Optional
from collections.abc import Callable

from django.db import IntegrityError
from django.db.models import Q
from rapidfuzz import fuzz

from catalog.category_mapping import resolve_category_id_for_source
from catalog.models import Category, Product
from catalog.services.product_images import ensure_product_image_from_listing
from comparison.models import MatchReview
from ingestion.models import StoreListing
from matching.normalizer import (
    Quantity,
    build_normalized_key,
    has_organic_marker,
    normalize_brand,
    normalize_listing_text,
    normalize_text,
    tokenize_name,
)


AUTO_TIER_A_SIMILARITY = 0.90
AUTO_TIER_B_SIMILARITY = 0.93
MANUAL_REVIEW_MIN_SCORE = 0.80
MAX_CANDIDATES = 1200
SOFT_VARIANT_TOKENS = {
    "ετοιμη",
    "κλασικη",
    "κλασικο",
    "κλασικος",
    "mix",
    "μιξ",
    "mini",
    "family",
}


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
    token_sort_similarity: float
    token_set_similarity: float
    token_overlap: float
    shared_token_count: int
    brand_score: float
    quantity_score: float
    category_score: float
    organic_score: float
    category_compatible: bool
    organic_compatible: bool
    category_resolved: bool
    listing_is_organic: bool
    product_is_organic: bool
    contradictory_tokens: bool
    listing_unique_token_count: int
    product_unique_token_count: int


def _token_overlap_ratio(left: str, right: str) -> float:
    left_tokens = set(tokenize_name(left))
    right_tokens = set(tokenize_name(right))
    if not left_tokens or not right_tokens:
        return 0.0

    smaller = min(len(left_tokens), len(right_tokens))
    if smaller < 3:
        return 0.0

    overlap = len(left_tokens & right_tokens) / smaller
    if overlap < 0.67:
        return 0.0
    return overlap


def _to_decimal_score(value: float) -> Decimal:
    return Decimal(str(round(value, 6)))


def _brand_similarity_score(
    listing_brand: Optional[str],
    product_brand: Optional[str],
    listing_name: str,
) -> float:
    if not listing_brand and not product_brand:
        return 0.40
    if not listing_brand or not product_brand:
        if product_brand and product_brand in listing_name:
            # Infer brand presence from listing name when explicit brand is missing.
            return 0.90
        return 0.45
    if listing_brand == product_brand:
        return 1.0
    return max(
        fuzz.ratio(listing_brand, product_brand) / 100.0,
        fuzz.partial_ratio(listing_brand, product_brand) / 100.0,
    )


def _quantity_similarity_score(left: Optional[Quantity], product: Product) -> float:
    if left is None and (product.quantity_value is None or not product.quantity_unit):
        return 0.60
    if left is None or product.quantity_value is None or not product.quantity_unit:
        return 0.45
    if left.unit != product.quantity_unit:
        return 0.0
    if left.value == product.quantity_value:
        return 1.0

    larger = max(left.value, product.quantity_value)
    if larger == 0:
        return 0.0
    diff_ratio = float(abs(left.value - product.quantity_value) / larger)
    if diff_ratio <= 0.02:
        return 0.90
    if diff_ratio <= 0.08:
        return 0.70
    if diff_ratio <= 0.20:
        return 0.40
    return 0.0


def _category_context_for_listing(
    listing: StoreListing,
    product: Product,
) -> tuple[float, bool, bool]:
    listing_category_id = _resolve_listing_category_id(listing)
    if listing_category_id is None:
        if listing.source_category:
            return 0.65, True, False
        return 0.80, True, False

    if product.category_id is None:
        return 0.70, True, True
    if product.category_id == listing_category_id:
        return 1.0, True, True
    return 0.0, False, True


def _organic_context_for_listing(
    listing: StoreListing,
    product: Product,
) -> tuple[float, bool, bool, bool]:
    listing_is_organic = has_organic_marker(listing.store_name)
    product_is_organic = has_organic_marker(product.canonical_name)
    compatible = listing_is_organic == product_is_organic
    return (
        1.0 if compatible else 0.0,
        compatible,
        listing_is_organic,
        product_is_organic,
    )


def _is_candidate_compatible(candidate: CandidateScore) -> bool:
    return candidate.category_compatible and candidate.organic_compatible


def _strong_unique_tokens(tokens: set[str], other_tokens: set[str]) -> set[str]:
    return {
        token
        for token in (tokens - other_tokens)
        if len(token) >= 4 and token not in SOFT_VARIANT_TOKENS
    }


def _has_contradictory_tokens(
    listing_tokens: set[str],
    product_tokens: set[str],
) -> tuple[bool, int, int]:
    listing_unique = _strong_unique_tokens(listing_tokens, product_tokens)
    product_unique = _strong_unique_tokens(product_tokens, listing_tokens)
    contradictory = bool(listing_unique and product_unique and len(listing_tokens & product_tokens) <= 2)
    return contradictory, len(listing_unique), len(product_unique)


def _score_candidate(listing: StoreListing, product: Product) -> CandidateScore:
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    product_name = product.canonical_name
    product_brand = normalize_brand(product.brand_normalized)
    product_name_norm = normalize_text(product_name)

    token_sort = fuzz.token_sort_ratio(listing_norm.normalized_name, product_name_norm) / 100.0
    token_set = fuzz.token_set_ratio(listing_norm.normalized_name, product_name_norm) / 100.0
    token_overlap = _token_overlap_ratio(listing_norm.normalized_name, product_name_norm)
    listing_tokens = set(tokenize_name(listing_norm.normalized_name))
    product_tokens = set(tokenize_name(product_name_norm))
    shared_token_count = len(listing_tokens & product_tokens)
    contradictory_tokens, listing_unique_count, product_unique_count = _has_contradictory_tokens(
        listing_tokens,
        product_tokens,
    )
    name_similarity = max(token_sort, token_overlap)
    if token_set > name_similarity and shared_token_count >= 2:
        name_similarity = token_set
    brand_score = _brand_similarity_score(
        listing_brand=listing_norm.brand_normalized,
        product_brand=product_brand,
        listing_name=listing_norm.normalized_name,
    )
    quantity_score = _quantity_similarity_score(listing_norm.quantity, product=product)
    category_score, category_compatible, category_resolved = _category_context_for_listing(
        listing=listing,
        product=product,
    )
    organic_score, organic_compatible, listing_is_organic, product_is_organic = _organic_context_for_listing(
        listing=listing,
        product=product,
    )

    score = (
        Decimal("0.40") * _to_decimal_score(name_similarity)
        + Decimal("0.20") * _to_decimal_score(brand_score)
        + Decimal("0.25") * _to_decimal_score(quantity_score)
        + Decimal("0.10") * _to_decimal_score(category_score)
        + Decimal("0.05") * _to_decimal_score(organic_score)
    )

    return CandidateScore(
        product=product,
        score=score,
        name_similarity=name_similarity,
        token_sort_similarity=token_sort,
        token_set_similarity=token_set,
        token_overlap=token_overlap,
        shared_token_count=shared_token_count,
        brand_score=brand_score,
        quantity_score=quantity_score,
        category_score=category_score,
        organic_score=organic_score,
        category_compatible=category_compatible,
        organic_compatible=organic_compatible,
        category_resolved=category_resolved,
        listing_is_organic=listing_is_organic,
        product_is_organic=product_is_organic,
        contradictory_tokens=contradictory_tokens,
        listing_unique_token_count=listing_unique_count,
        product_unique_token_count=product_unique_count,
    )


def _candidate_queryset(listing: StoreListing, *, reconsider_matched: bool = False):
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    candidates = Product.objects.all()
    same_store_condition = Q(store_listings__store_id=listing.store_id)

    # Do not match an unmatched listing against products already represented by the same store.
    # This keeps same-store canonical collisions from collapsing many distinct store listings.
    if listing.product_id:
        if reconsider_matched:
            candidates = candidates.exclude(id=listing.product_id).exclude(same_store_condition)
        else:
            candidates = candidates.exclude(same_store_condition & ~Q(id=listing.product_id))
    else:
        candidates = candidates.exclude(same_store_condition)

    listing_category_id = _resolve_listing_category_id(listing)

    if listing_category_id is not None:
        candidates = candidates.filter(
            Q(category_id=listing_category_id) | Q(category__isnull=True)
        )

    if listing_norm.brand_normalized:
        brand_candidates = candidates.filter(
            Q(brand_normalized=listing_norm.brand_normalized)
            | Q(brand_normalized__isnull=True)
            | Q(brand_normalized="")
        )
        if brand_candidates.exists():
            candidates = brand_candidates

    if listing_norm.quantity is not None:
        quantity_candidates = candidates.filter(
            Q(quantity_unit=listing_norm.quantity.unit)
            | Q(quantity_unit__isnull=True)
            | Q(quantity_unit="")
        )
        if quantity_candidates.exists():
            candidates = quantity_candidates

    return candidates.distinct().order_by("id")[:MAX_CANDIDATES]


def _is_category_compatible_for_listing(listing: StoreListing, product: Product) -> bool:
    _, compatible, _ = _category_context_for_listing(listing=listing, product=product)
    return compatible


def _is_organic_compatible_for_listing(listing: StoreListing, product: Product) -> bool:
    _, compatible, _, _ = _organic_context_for_listing(listing=listing, product=product)
    return compatible


def _has_other_listing_from_same_store(listing: StoreListing, product: Product) -> bool:
    return product.store_listings.filter(store_id=listing.store_id).exclude(id=listing.id).exists()


def _create_or_get_product_for_listing(listing: StoreListing) -> tuple[Product, bool]:
    listing_norm = normalize_listing_text(name=listing.store_name, brand=listing.store_brand)
    quantity_value = listing_norm.quantity.value if listing_norm.quantity else None
    quantity_unit = listing_norm.quantity.unit if listing_norm.quantity else None
    category = _resolve_listing_category(listing)
    normalized_key = listing_norm.normalized_key

    if normalized_key:
        existing = Product.objects.filter(normalized_key=normalized_key).first()
        if existing:
            if _has_other_listing_from_same_store(listing=listing, product=existing):
                normalized_key = None
            elif (
                _is_category_compatible_for_listing(listing, existing)
                and _is_organic_compatible_for_listing(listing, existing)
            ):
                return existing, False
            else:
                normalized_key = None

    product = Product(
        canonical_name=listing.store_name,
        brand_normalized=listing_norm.brand_normalized,
        quantity_value=quantity_value,
        quantity_unit=quantity_unit,
        normalized_key=normalized_key,
        category=category,
    )
    try:
        product.save()
        return product, True
    except IntegrityError:
        if normalized_key:
            existing = Product.objects.filter(normalized_key=normalized_key).first()
            if (
                existing
                and not _has_other_listing_from_same_store(listing=listing, product=existing)
                and _is_category_compatible_for_listing(listing, existing)
                and _is_organic_compatible_for_listing(listing, existing)
            ):
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


def _best_candidate(listing: StoreListing, *, reconsider_matched: bool = False) -> Optional[CandidateScore]:
    best: Optional[CandidateScore] = None
    best_compatible = False
    for product in _candidate_queryset(listing, reconsider_matched=reconsider_matched):
        candidate = _score_candidate(listing=listing, product=product)
        candidate_compatible = _is_candidate_compatible(candidate)
        if (
            best is None
            or (candidate_compatible and not best_compatible)
            or (candidate_compatible == best_compatible and candidate.score > best.score)
        ):
            best = candidate
            best_compatible = candidate_compatible
    return best


def _set_listing_product(listing: StoreListing, product: Product) -> None:
    if listing.product_id == product.id:
        ensure_product_image_from_listing(product=product, listing=listing)
        return
    listing.product = product
    listing.save(update_fields=["product"])
    ensure_product_image_from_listing(product=product, listing=listing)
    MatchReview.objects.filter(store_listing=listing).delete()


def _create_review(listing: StoreListing, candidate: CandidateScore) -> None:
    MatchReview.objects.update_or_create(
        store_listing=listing,
        candidate_product=candidate.product,
        defaults={
            "score": candidate.score.quantize(Decimal("0.0001")),
            "status": MatchReview.Status.PENDING,
            "notes": (
                f"name_similarity={candidate.name_similarity:.3f}, "
                f"token_sort={candidate.token_sort_similarity:.3f}, "
                f"token_set={candidate.token_set_similarity:.3f}, "
                f"token_overlap={candidate.token_overlap:.3f}, "
                f"shared_tokens={candidate.shared_token_count}, "
                f"brand_score={candidate.brand_score:.3f}, "
                f"quantity_score={candidate.quantity_score:.3f}, "
                f"category_score={candidate.category_score:.3f}, "
                f"organic_score={candidate.organic_score:.3f}, "
                f"organic_compatible={candidate.organic_compatible}, "
                f"listing_is_organic={candidate.listing_is_organic}, "
                f"product_is_organic={candidate.product_is_organic}, "
                f"contradictory_tokens={candidate.contradictory_tokens}, "
                f"listing_unique_tokens={candidate.listing_unique_token_count}, "
                f"product_unique_tokens={candidate.product_unique_token_count}, "
                f"resolved_category={candidate.category_resolved}"
            ),
        },
    )


def _should_auto_tier_a(candidate: CandidateScore) -> bool:
    return (
        candidate.category_resolved
        and _is_candidate_compatible(candidate)
        and not candidate.contradictory_tokens
        and candidate.name_similarity >= AUTO_TIER_A_SIMILARITY
        and candidate.brand_score >= 0.95
        and candidate.quantity_score >= 0.95
    )


def _should_auto_tier_b(candidate: CandidateScore) -> bool:
    return (
        candidate.category_resolved
        and _is_candidate_compatible(candidate)
        and not candidate.contradictory_tokens
        and candidate.name_similarity >= AUTO_TIER_B_SIMILARITY
        and candidate.quantity_score >= 0.80
        and candidate.score >= Decimal("0.86")
    )


def _should_auto_tier_c(candidate: CandidateScore) -> bool:
    return (
        candidate.category_resolved
        and _is_candidate_compatible(candidate)
        and not candidate.contradictory_tokens
        and candidate.brand_score >= 0.90
        and candidate.quantity_score >= 0.95
        and candidate.name_similarity >= 0.76
        and candidate.shared_token_count >= 2
        and candidate.score >= Decimal("0.84")
    )


def _should_auto_tier_d(candidate: CandidateScore) -> bool:
    return (
        candidate.category_resolved
        and _is_candidate_compatible(candidate)
        and not candidate.contradictory_tokens
        and candidate.brand_score <= 0.50
        and candidate.quantity_score >= 0.95
        and candidate.name_similarity >= 0.96
        and candidate.score >= Decimal("0.86")
    )


def _is_no_brand_no_quantity_pair(candidate: CandidateScore) -> bool:
    return (
        candidate.brand_score <= 0.41
        and candidate.quantity_score >= 0.59
        and candidate.quantity_score <= 0.61
    )


def _should_auto_tier_e(candidate: CandidateScore) -> bool:
    return (
        candidate.category_resolved
        and _is_candidate_compatible(candidate)
        and not candidate.contradictory_tokens
        and candidate.category_score >= 0.99
        and _is_no_brand_no_quantity_pair(candidate)
        and candidate.token_overlap >= 0.80
        and candidate.name_similarity >= 0.92
        and candidate.score >= Decimal("0.78")
    )


def _should_go_to_review(candidate: CandidateScore) -> bool:
    if not _is_candidate_compatible(candidate):
        return False
    if not candidate.category_resolved:
        return False
    if candidate.quantity_score == 0.0:
        # Hard quantity mismatch should not be queued as a fuzzy review candidate.
        return False
    if candidate.name_similarity < 0.74:
        return False
    if candidate.shared_token_count < 2 and candidate.token_overlap < 0.67:
        return False
    if candidate.brand_score < 0.65 and candidate.quantity_score < 0.70:
        return False
    return candidate.score >= Decimal(str(MANUAL_REVIEW_MIN_SCORE))


def match_store_listings(
    *,
    listing_ids: Optional[Iterable[int]] = None,
    only_unmatched: bool = True,
    include_inactive: bool = False,
    limit: Optional[int] = None,
    reconsider_matched: bool = False,
    progress_every: Optional[int] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
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

    total = queryset.count()
    result = MatchResult()
    started_at = monotonic()
    effective_progress_every = progress_every if progress_every and progress_every > 0 else None

    def emit_progress() -> None:
        if progress_callback is None:
            return
        elapsed = monotonic() - started_at
        progress_callback(
            "Matcher progress: "
            f"{result.processed}/{total} processed, auto={result.auto_matched}, "
            f"review={result.review_created}, new_products={result.created_products}, "
            f"elapsed={elapsed:.1f}s"
        )

    def maybe_emit_progress() -> None:
        if effective_progress_every and result.processed % effective_progress_every == 0:
            emit_progress()

    if progress_callback is not None:
        progress_callback(f"Matcher starting (total={total}).")

    for listing in queryset:
        result.processed += 1
        best = _best_candidate(listing, reconsider_matched=reconsider_matched)
        had_existing_product = listing.product_id is not None

        if best is not None:
            if _should_auto_tier_a(best):
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                maybe_emit_progress()
                continue

            if _should_auto_tier_b(best):
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                maybe_emit_progress()
                continue

            if _should_auto_tier_c(best):
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                maybe_emit_progress()
                continue

            if _should_auto_tier_d(best):
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                maybe_emit_progress()
                continue

            if _should_auto_tier_e(best):
                _set_listing_product(listing, best.product)
                result.auto_matched += 1
                maybe_emit_progress()
                continue

            if _should_go_to_review(best):
                if not (reconsider_matched and had_existing_product):
                    _create_review(listing, best)
                    result.review_created += 1
                maybe_emit_progress()
                continue

        # In reconsider mode, keep the current match unless we found a strong auto candidate above.
        if reconsider_matched and had_existing_product:
            maybe_emit_progress()
            continue

        product, created = _create_or_get_product_for_listing(listing)
        _set_listing_product(listing, product)
        if created:
            result.created_products += 1

        maybe_emit_progress()

    if total and progress_callback is not None and (
        effective_progress_every is None or result.processed % effective_progress_every != 0
    ):
        emit_progress()

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
