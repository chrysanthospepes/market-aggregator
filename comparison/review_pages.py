from __future__ import annotations

from django.core.paginator import Paginator
from django.db.models import Case, Count, IntegerField, Prefetch, Q, Value, When
from django.http import HttpRequest
from django.utils.http import urlencode
from django.utils.translation import gettext_lazy as _

from catalog.category_mapping import resolve_category_id_for_source
from catalog.models import Category, Product, Store
from catalog.search_normalizer import build_search_forms
from comparison.models import ListingProductReport, MatchReview
from comparison.view_helpers import (
    listing_offer_label,
    product_quantity_label,
    store_display_name,
    store_icon_url,
    token_form_query,
)
from ingestion.models import StoreListing

REVIEW_QUEUE_LISTINGS_PER_PAGE = 12
LISTING_REPORTS_PER_PAGE = 12
MATCH_REVIEW_NOTE_LABELS: dict[str, str] = {
    "name_similarity": _("Name similarity"),
    "token_sort": _("Token sort"),
    "token_set": _("Token set"),
    "token_overlap": _("Token overlap"),
    "shared_tokens": _("Shared tokens"),
    "brand_score": _("Brand"),
    "quantity_score": _("Quantity"),
    "category_score": _("Category"),
    "organic_score": _("Organic"),
    "organic_compatible": _("Organic compatible"),
    "listing_is_organic": _("Listing organic"),
    "product_is_organic": _("Product organic"),
    "contradictory_tokens": _("Contradictory tokens"),
    "listing_unique_tokens": _("Listing-only tokens"),
    "product_unique_tokens": _("Product-only tokens"),
    "resolved_category": _("Resolved category"),
}
MATCH_REVIEW_DECIMAL_KEYS = {
    "name_similarity",
    "token_sort",
    "token_set",
    "token_overlap",
    "brand_score",
    "quantity_score",
    "category_score",
    "organic_score",
}
MATCH_REVIEW_BOOLEAN_KEYS = {
    "organic_compatible",
    "listing_is_organic",
    "product_is_organic",
    "contradictory_tokens",
    "resolved_category",
}


def parse_selected_store_id(raw_value: str | None) -> int | None:
    try:
        selected_store_id = int((raw_value or "").strip() or "")
    except (AttributeError, TypeError, ValueError):
        return None
    if selected_store_id <= 0:
        return None
    return selected_store_id


def queue_filters_query(*, search_query: str, selected_store_id: int | None) -> str:
    params: list[tuple[str, str]] = []
    if search_query:
        params.append(("q", search_query))
    if selected_store_id is not None:
        params.append(("store", str(selected_store_id)))
    return urlencode(params)


def _pending_store_options(*, pending_store_filter: Q) -> list[Store]:
    stores = list(
        Store.objects.filter(pending_store_filter)
        .annotate(
            pending_listing_count=Count(
                "listings",
                filter=pending_store_filter,
                distinct=True,
            ),
        )
        .order_by("name")
    )
    for store in stores:
        store.display_name = store_display_name(store.name)
    return stores


def _parse_match_review_notes(notes: str) -> list[dict[str, str]]:
    parsed: list[dict[str, str]] = []
    for segment in (notes or "").split(", "):
        if "=" not in segment:
            continue
        key, raw_value = segment.split("=", 1)
        value = raw_value
        if key in MATCH_REVIEW_DECIMAL_KEYS:
            try:
                value = f"{float(raw_value):.3f}"
            except (TypeError, ValueError):
                value = raw_value
        elif key in MATCH_REVIEW_BOOLEAN_KEYS:
            value = str(_("Yes")) if raw_value == "True" else str(_("No"))
        parsed.append(
            {
                "key": key,
                "label": MATCH_REVIEW_NOTE_LABELS.get(key, key.replace("_", " ").title()),
                "value": value,
            }
        )
    return parsed


def listing_resolved_category_id(listing: StoreListing) -> int | None:
    if not listing.source_category:
        return None
    return resolve_category_id_for_source(
        store_id=listing.store_id,
        source_category=listing.source_category,
    )


def build_listing_report_entry(report: ListingProductReport) -> dict[str, object]:
    listing = report.store_listing
    current_product = listing.product
    reported_product = report.reported_product

    return {
        "report": report,
        "listing": listing,
        "listing_store_display_name": store_display_name(listing.store.name),
        "listing_store_icon_url": store_icon_url(listing.store.name),
        "listing_offer_label": listing_offer_label(listing),
        "current_product": current_product,
        "current_product_quantity": product_quantity_label(current_product),
        "reported_product": reported_product,
        "reported_product_quantity": product_quantity_label(reported_product),
        "resolved_category_id": listing_resolved_category_id(listing),
    }


def _is_fallback_listing_name(listing: StoreListing) -> bool:
    listing_name = (listing.store_name or "").strip()
    if not listing_name:
        return True

    fallback_values = {
        (listing.store_sku or "").strip(),
        (listing.url or "").strip(),
    }
    fallback_values.discard("")
    return listing_name in fallback_values


def listing_report_default_candidate_query(report: ListingProductReport) -> str:
    listing = report.store_listing
    candidate_values: list[object] = []
    if not _is_fallback_listing_name(listing):
        candidate_values.append(listing.store_name)
    candidate_values.extend(
        [
            report.reported_product,
            listing.product,
            listing.store_brand,
            listing.store_sku,
            listing.url,
        ]
    )

    for value in candidate_values:
        if isinstance(value, Product):
            if value.canonical_name:
                return value.canonical_name
            continue
        text = (value or "").strip()
        if text:
            return text
    return ""


def _listing_report_preferred_category_id(report: ListingProductReport) -> int | None:
    listing = report.store_listing
    for category_id in [
        listing_resolved_category_id(listing),
        getattr(listing.product, "category_id", None),
        getattr(report.reported_product, "category_id", None),
    ]:
        if category_id:
            return category_id
    return None


def listing_report_candidate_products(
    *,
    report: ListingProductReport,
    query: str,
) -> list[Product]:
    search_query_forms = build_search_forms(query)
    if not search_query_forms:
        return []

    primary_form_tokens = [token for token in search_query_forms[0].split() if token]
    primary_token = primary_form_tokens[0] if primary_form_tokens else search_query_forms[0]
    preferred_category_id = _listing_report_preferred_category_id(report)
    listing = report.store_listing

    query_filter = (
        token_form_query("search_name", search_query_forms)
        | token_form_query("canonical_name", search_query_forms)
        | token_form_query("brand_normalized", search_query_forms)
    )

    candidates = (
        Product.objects.select_related("category")
        .exclude(id=listing.product_id)
        .filter(query_filter)
        .annotate(
            category_priority=Case(
                When(category_id=preferred_category_id, then=Value(0)),
                default=Value(1),
                output_field=IntegerField(),
            ),
            match_priority=Case(
                When(search_name__istartswith=primary_token, then=Value(0)),
                When(canonical_name__istartswith=primary_token, then=Value(1)),
                When(search_name__icontains=primary_token, then=Value(2)),
                When(canonical_name__icontains=primary_token, then=Value(3)),
                When(brand_normalized__icontains=primary_token, then=Value(4)),
                default=Value(5),
                output_field=IntegerField(),
            ),
        )
        .order_by("category_priority", "match_priority", "canonical_name")[:12]
    )
    return list(candidates)


def build_match_review_queue_context(request: HttpRequest) -> dict[str, object]:
    search_query = (request.GET.get("q") or "").strip()
    selected_store_id = parse_selected_store_id(request.GET.get("store"))

    active_candidate_listings = (
        StoreListing.objects.select_related("store")
        .filter(is_active=True)
        .order_by("store__name", "final_price", "store_name", "id")
    )
    review_queryset = MatchReview.objects.filter(status=MatchReview.Status.PENDING).select_related(
        "store_listing__store",
        "store_listing__product__category",
        "candidate_product__category",
    ).prefetch_related(
        Prefetch(
            "candidate_product__store_listings",
            queryset=active_candidate_listings,
            to_attr="review_queue_active_listings",
        )
    )

    if selected_store_id is not None:
        review_queryset = review_queryset.filter(store_listing__store_id=selected_store_id)
    if search_query:
        review_queryset = review_queryset.filter(
            Q(store_listing__store_name__icontains=search_query)
            | Q(store_listing__store_brand__icontains=search_query)
            | Q(store_listing__source_category__icontains=search_query)
            | Q(candidate_product__canonical_name__icontains=search_query)
            | Q(candidate_product__brand_normalized__icontains=search_query)
            | Q(candidate_product__category__name__icontains=search_query)
            | Q(candidate_product__category__slug__icontains=search_query)
        )

    review_rows = list(review_queryset.order_by("store_listing_id", "-score", "id"))
    groups_by_listing_id: dict[int, dict[str, object]] = {}
    resolved_category_ids: set[int] = set()

    for review in review_rows:
        listing = review.store_listing
        resolved_category_id = listing_resolved_category_id(listing)
        if resolved_category_id is not None:
            resolved_category_ids.add(resolved_category_id)

        entry = groups_by_listing_id.setdefault(
            listing.id,
            {
                "listing": listing,
                "listing_store_display_name": store_display_name(listing.store.name),
                "listing_store_icon_url": store_icon_url(listing.store.name),
                "listing_offer_label": listing_offer_label(listing),
                "listing_quantity_label": listing.unit_of_measure or None,
                "resolved_category_id": resolved_category_id,
                "current_product": listing.product,
                "current_product_quantity": product_quantity_label(listing.product),
                "reviews": [],
                "top_score": review.score,
            },
        )

        candidate_product = review.candidate_product
        candidate_active_listings = list(
            getattr(candidate_product, "review_queue_active_listings", [])
        )
        for candidate_listing in candidate_active_listings:
            candidate_listing.store_display_name = store_display_name(candidate_listing.store.name)

        entry["reviews"].append(
            {
                "review": review,
                "candidate": candidate_product,
                "candidate_quantity": product_quantity_label(candidate_product),
                "candidate_active_listings": candidate_active_listings,
                "score_breakdown": _parse_match_review_notes(review.notes),
            }
        )

    categories_by_id = {
        category.id: category
        for category in Category.objects.filter(id__in=resolved_category_ids)
    }
    queue_entries = list(groups_by_listing_id.values())
    for entry in queue_entries:
        entry["resolved_category"] = categories_by_id.get(entry["resolved_category_id"])
        entry["review_count"] = len(entry["reviews"])

    queue_entries.sort(key=lambda entry: (-entry["top_score"], entry["listing"].id))

    paginator = Paginator(queue_entries, REVIEW_QUEUE_LISTINGS_PER_PAGE)
    page_obj = paginator.get_page(request.GET.get("page"))

    pending_store_filter = Q(listings__match_reviews__status=MatchReview.Status.PENDING)
    stores = _pending_store_options(pending_store_filter=pending_store_filter)
    filters_query = queue_filters_query(
        search_query=search_query,
        selected_store_id=selected_store_id,
    )

    return {
        "page_obj": page_obj,
        "stores": stores,
        "search_query": search_query,
        "selected_store_id": selected_store_id,
        "filters_query": filters_query,
        "visible_listing_count": len(queue_entries),
        "visible_review_count": len(review_rows),
    }


def build_listing_report_queue_context(request: HttpRequest) -> dict[str, object]:
    search_query = (request.GET.get("q") or "").strip()
    selected_store_id = parse_selected_store_id(request.GET.get("store"))

    report_queryset = ListingProductReport.objects.filter(
        status=ListingProductReport.Status.PENDING
    ).select_related(
        "store_listing__store",
        "store_listing__product__category",
        "reported_product__category",
    )

    if selected_store_id is not None:
        report_queryset = report_queryset.filter(store_listing__store_id=selected_store_id)
    if search_query:
        report_queryset = report_queryset.filter(
            Q(store_listing__store_name__icontains=search_query)
            | Q(store_listing__store_brand__icontains=search_query)
            | Q(store_listing__source_category__icontains=search_query)
            | Q(store_listing__product__canonical_name__icontains=search_query)
            | Q(reported_product__canonical_name__icontains=search_query)
        )

    report_rows = list(report_queryset.order_by("-last_reported_at", "-id"))
    queue_entries = [build_listing_report_entry(report) for report in report_rows]
    resolved_category_ids = {
        entry["resolved_category_id"]
        for entry in queue_entries
        if entry["resolved_category_id"] is not None
    }
    categories_by_id = {
        category.id: category
        for category in Category.objects.filter(id__in=resolved_category_ids)
    }
    for entry in queue_entries:
        entry["resolved_category"] = categories_by_id.get(entry["resolved_category_id"])

    paginator = Paginator(queue_entries, LISTING_REPORTS_PER_PAGE)
    page_obj = paginator.get_page(request.GET.get("page"))

    pending_store_filter = Q(
        listings__listing_product_reports__status=ListingProductReport.Status.PENDING
    )
    stores = _pending_store_options(pending_store_filter=pending_store_filter)
    filters_query = queue_filters_query(
        search_query=search_query,
        selected_store_id=selected_store_id,
    )

    return {
        "page_obj": page_obj,
        "stores": stores,
        "search_query": search_query,
        "selected_store_id": selected_store_id,
        "filters_query": filters_query,
        "visible_report_count": len(queue_entries),
    }
