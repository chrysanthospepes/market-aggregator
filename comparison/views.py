from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import (
    BooleanField,
    Case,
    Count,
    DecimalField,
    Exists,
    F,
    IntegerField,
    OuterRef,
    Prefetch,
    Q,
    Subquery,
    Value,
    When,
)
from django.db.models.functions import Coalesce, Greatest
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.http import urlencode, url_has_allowed_host_and_scheme
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_POST

from catalog.category_mapping import resolve_category_id_for_source
from catalog.models import Category, Product, Store
from catalog.search_normalizer import build_search_forms
from comparison.models import ListingProductReport, MatchReview
from comparison.pricing import (
    PRICE_PROFILE_OPTIONS,
    PRICE_PROFILE_PARAM,
    adjusted_price_expression,
    apply_price_profile_value,
    get_price_profile,
    parse_price_profile,
    price_profile_applies_to_store,
)
from comparison.review_actions import approve_match_reviews, reject_match_reviews
from ingestion.models import StoreListing

HOME_PRODUCTS_PER_STORE = 20
PRODUCTS_PER_PAGE = 20
REVIEW_QUEUE_LISTINGS_PER_PAGE = 12
LISTING_REPORTS_PER_PAGE = 12
DEFAULT_SORT = "price_asc"
SORT_OPTIONS: tuple[tuple[str, str], ...] = (
    ("relevance", _("Relevance")),
    ("price_asc", _("Increasing price")),
    ("price_desc", _("Declining price")),
    ("unit_price_asc", _("Increasing unit price")),
    ("unit_price_desc", _("Declining unit price")),
    ("discount_desc", _("Declining discount")),
)
OFFER_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("no_offer", _("No offer")),
    ("discount_0_20", _("Up to 20%")),
    ("discount_21_40", _("From 21% up to 40%")),
    ("discount_41_plus", _("From 41% up to max")),
    ("one_plus_one", "1 + 1"),
    ("two_plus_one", "2 + 1"),
)
STORE_DISPLAY_NAME_BY_KEY: dict[str, str] = {
    "ab": "ΑΒ Βασιλόπουλος",
    "bazaar": "Bazaar",
    "kritikos": "Κρητικός",
    "masoutis": "Μασούτης",
    "mymarket": "My Market",
    "sklavenitis": "Σκλαβενίτης",
}
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


def _listing_offer_condition() -> Q:
    return (
        Q(offer=True)
        | Q(discount_percent__gt=0)
        | Q(one_plus_one=True)
        | Q(two_plus_one=True)
    )


def _store_icon_url(store_name: str | None) -> str | None:
    normalized_name = (store_name or "").strip().lower()
    if not normalized_name:
        return None
    return f"{settings.MEDIA_URL}stores/{normalized_name}.png"


def _store_display_name(store_name: str | None) -> str:
    normalized_name = (store_name or "").strip().lower()
    return STORE_DISPLAY_NAME_BY_KEY.get(normalized_name, store_name or "")


def _sale_icon_url(
    *,
    discount_percent: int | None,
    one_plus_one: bool,
    two_plus_one: bool,
) -> str | None:
    if one_plus_one:
        return f"{settings.MEDIA_URL}discounts/offer-1plus1.svg"
    if two_plus_one:
        return f"{settings.MEDIA_URL}discounts/offer-2plus1.svg"
    if discount_percent is None:
        return None
    try:
        pct = int(discount_percent)
    except (TypeError, ValueError):
        return None
    if pct <= 0:
        return None
    pct = min(pct, 100)
    return f"{settings.MEDIA_URL}discounts/discount-{pct:03d}.svg"


def _parse_selected_store_ids(raw_values: list[str]) -> list[int]:
    selected: list[int] = []
    for raw in raw_values:
        try:
            store_id = int(raw)
        except (TypeError, ValueError):
            continue
        if store_id > 0:
            selected.append(store_id)
    return sorted(set(selected))


def _selected_store_query(selected_store_ids: list[int]) -> str:
    return "".join(f"&stores={store_id}" for store_id in selected_store_ids)


def _parse_selected_offer_filters(raw_values: list[str]) -> list[str]:
    valid_offer_filter_values = {value for value, _ in OFFER_FILTER_OPTIONS}
    selected: list[str] = []
    for raw in raw_values:
        value = (raw or "").strip()
        if value in valid_offer_filter_values:
            selected.append(value)
    return list(dict.fromkeys(selected))


def _parse_sort(raw_value: str | None) -> str:
    valid_sort_values = {value for value, _ in SORT_OPTIONS}
    value = (raw_value or "").strip()
    if value in valid_sort_values:
        return value
    return DEFAULT_SORT


def _token_form_query(field_name: str, search_query_forms: list[str]) -> Q:
    combined = Q()
    for query_form in search_query_forms:
        tokens = [token for token in query_form.split() if token]
        if not tokens:
            continue
        form_query = Q()
        for token in tokens:
            form_query &= Q(**{f"{field_name}__icontains": token})
        combined |= form_query
    return combined


def _sort_options_for_query(has_search_query: bool) -> tuple[tuple[str, str], ...]:
    if has_search_query:
        return SORT_OPTIONS
    return tuple(option for option in SORT_OPTIONS if option[0] != "relevance")


def _offer_filter_condition(offer_filter: str) -> Q:
    if offer_filter == "no_offer":
        return (
            Q(cheapest_one_plus_one=False)
            & Q(cheapest_two_plus_one=False)
            & Q(cheapest_discount_percent__isnull=True)
            & Q(cheapest_offer=False)
        )
    if offer_filter == "discount_0_20":
        return Q(cheapest_discount_percent__gte=1, cheapest_discount_percent__lte=20)
    if offer_filter == "discount_21_40":
        return Q(cheapest_discount_percent__gte=21, cheapest_discount_percent__lte=40)
    if offer_filter == "discount_41_plus":
        return Q(cheapest_discount_percent__gte=41)
    if offer_filter == "one_plus_one":
        return Q(cheapest_one_plus_one=True)
    if offer_filter == "two_plus_one":
        return Q(cheapest_two_plus_one=True)
    return Q()


def _selected_filters_query(
    *,
    store_ids: list[int],
    offer_filters: list[str],
    category_filter: str,
    search_query: str,
    price_profile: str,
) -> str:
    params: list[tuple[str, str]] = []
    if price_profile:
        params.append((PRICE_PROFILE_PARAM, price_profile))
    if search_query:
        params.append(("q", search_query))
    if category_filter:
        params.append(("category", category_filter))
    for offer_filter in offer_filters:
        params.append(("offer_filter", offer_filter))
    for store_id in store_ids:
        params.append(("stores", str(store_id)))
    if not params:
        return ""
    return "&" + urlencode(params, doseq=True)


def _selected_price_profile_query(price_profile: str) -> str:
    if not price_profile:
        return ""
    return urlencode([(PRICE_PROFILE_PARAM, price_profile)])


def _set_listing_display_prices(listing: StoreListing, *, price_profile: str) -> None:
    store_name = listing.store.name if getattr(listing, "store_id", None) else None
    listing.store_display_name = _store_display_name(store_name)
    listing.display_final_price = apply_price_profile_value(
        listing.final_price,
        store_name=store_name,
        price_profile=price_profile,
    )
    listing.display_final_unit_price = apply_price_profile_value(
        listing.final_unit_price,
        store_name=store_name,
        price_profile=price_profile,
    )
    listing.display_original_price = apply_price_profile_value(
        listing.original_price,
        store_name=store_name,
        price_profile=price_profile,
    )
    listing.display_original_unit_price = apply_price_profile_value(
        listing.original_unit_price,
        store_name=store_name,
        price_profile=price_profile,
    )
    listing.price_profile_applies = price_profile_applies_to_store(
        store_name=store_name,
        price_profile=price_profile,
    )


def _set_product_display_prices(product, *, price_profile: str) -> None:
    product.display_final_price = getattr(product, "cheapest_final_price", None)
    product.display_final_unit_price = getattr(product, "cheapest_final_unit_price", None)
    product.display_original_price = getattr(product, "cheapest_original_price", None)
    product.display_original_unit_price = getattr(product, "cheapest_original_unit_price", None)
    product.price_profile_applies = price_profile_applies_to_store(
        store_name=getattr(product, "cheapest_store_name", None),
        price_profile=price_profile,
    )


@dataclass(frozen=True)
class OffsetPage:
    object_list: list
    number: int
    per_page: int
    _has_next: bool

    def has_next(self) -> bool:
        return self._has_next

    def has_previous(self) -> bool:
        return self.number > 1

    def has_other_pages(self) -> bool:
        return self.has_previous() or self.has_next()

    def next_page_number(self) -> int:
        return self.number + 1

    def previous_page_number(self) -> int:
        return self.number - 1


def _paginate_queryset_without_count(queryset, *, raw_page: str | None, per_page: int) -> OffsetPage:
    try:
        page_number = int((raw_page or "").strip() or "1")
    except (AttributeError, TypeError, ValueError):
        page_number = 1
    if page_number <= 0:
        page_number = 1

    offset = (page_number - 1) * per_page
    rows = list(queryset[offset : offset + per_page + 1])

    # Avoid an empty page on out-of-range page numbers without issuing a full COUNT(*).
    if page_number > 1 and not rows:
        page_number = 1
        rows = list(queryset[: per_page + 1])

    has_next = len(rows) > per_page
    return OffsetPage(
        object_list=rows[:per_page],
        number=page_number,
        per_page=per_page,
        _has_next=has_next,
    )


def home(request):
    selected_price_profile = parse_price_profile(request.GET.get(PRICE_PROFILE_PARAM))
    selected_price_profile_meta = get_price_profile(selected_price_profile)
    categories = sorted(Category.objects.all(), key=lambda category: category.display_name.lower())

    stores = Store.objects.filter(listings__is_active=True).order_by("name").distinct()
    store_sections: list[dict[str, object]] = []

    for store in stores:
        listings = (
            StoreListing.objects.select_related("product", "store")
            .filter(
                store=store,
                is_active=True,
                product__isnull=False,
            )
        )
        offer_listings = list(listings.filter(_listing_offer_condition()).order_by("?")[:HOME_PRODUCTS_PER_STORE])

        remaining_slots = HOME_PRODUCTS_PER_STORE - len(offer_listings)
        non_offer_listings: list[StoreListing] = []
        if remaining_slots > 0:
            selected_ids = [listing.id for listing in offer_listings]
            non_offer_listings = list(
                listings.exclude(id__in=selected_ids).order_by("?")[:remaining_slots]
            )

        picked_listings = offer_listings + non_offer_listings
        if not picked_listings:
            continue

        for listing in picked_listings:
            listing.sale_icon_url = _sale_icon_url(
                discount_percent=listing.discount_percent,
                one_plus_one=listing.one_plus_one,
                two_plus_one=listing.two_plus_one,
            )
            _set_listing_display_prices(listing, price_profile=selected_price_profile)

        store_sections.append(
            {
                "store": store,
                "store_display_name": _store_display_name(store.name),
                "store_icon_url": _store_icon_url(store.name),
                "listings": picked_listings,
            }
        )

    return render(
        request,
        "comparison/home.html",
        {
            "categories": categories,
            "store_sections": store_sections,
            "price_profile_options": PRICE_PROFILE_OPTIONS,
            "selected_price_profile": selected_price_profile,
            "selected_price_profile_meta": selected_price_profile_meta,
            "selected_price_profile_query": _selected_price_profile_query(selected_price_profile),
        },
    )


def product_list(request):
    requested_sort = request.GET.get("sort")
    sort = _parse_sort(requested_sort)
    search_query = (request.GET.get("q") or "").strip()
    search_query_forms = build_search_forms(search_query)
    if search_query_forms and not (requested_sort or "").strip():
        sort = "relevance"
    elif not search_query_forms and sort == "relevance":
        sort = DEFAULT_SORT
    sort_options = _sort_options_for_query(bool(search_query_forms))
    requested_category_filter = (request.GET.get("category") or "").strip()
    selected_category_filter = "" if search_query_forms else requested_category_filter
    selected_store_ids = _parse_selected_store_ids(request.GET.getlist("stores"))
    selected_offer_filters = _parse_selected_offer_filters(request.GET.getlist("offer_filter"))
    selected_price_profile = parse_price_profile(request.GET.get(PRICE_PROFILE_PARAM))
    selected_price_profile_meta = get_price_profile(selected_price_profile)
    category_products = Product.objects.filter(
        category_id=OuterRef("pk"),
        store_listings__is_active=True,
    )
    if selected_store_ids:
        category_products = category_products.filter(store_listings__store_id__in=selected_store_ids)
    available_categories = list(
        Category.objects.annotate(has_available_product=Exists(category_products))
        .filter(has_available_product=True)
        .order_by("name")
    )
    available_categories.sort(key=lambda category: category.display_name.lower())
    selected_category_slug = selected_category_filter
    if selected_category_filter.isdigit():
        category_slug = (
            Category.objects.filter(id=int(selected_category_filter))
            .values_list("slug", flat=True)
            .first()
        )
        if category_slug:
            selected_category_slug = category_slug
    selected_active_listings = StoreListing.objects.filter(product_id=OuterRef("pk"), is_active=True)
    active_listings = StoreListing.objects.filter(product_id=OuterRef("pk"), is_active=True)
    if selected_store_ids:
        selected_active_listings = selected_active_listings.filter(store_id__in=selected_store_ids)
        active_listings = active_listings.filter(store_id__in=selected_store_ids)

    active_listings = active_listings.annotate(
        effective_hidden_price=adjusted_price_expression(
            "hidden_price",
            store_field_name="store__name",
            price_profile=selected_price_profile,
        ),
        effective_final_price=adjusted_price_expression(
            "final_price",
            store_field_name="store__name",
            price_profile=selected_price_profile,
        ),
        effective_original_price=adjusted_price_expression(
            "original_price",
            store_field_name="store__name",
            price_profile=selected_price_profile,
        ),
        effective_hidden_unit_price=adjusted_price_expression(
            "hidden_unit_price",
            store_field_name="store__name",
            price_profile=selected_price_profile,
        ),
        effective_final_unit_price=adjusted_price_expression(
            "final_unit_price",
            store_field_name="store__name",
            price_profile=selected_price_profile,
        ),
        effective_original_unit_price=adjusted_price_expression(
            "original_unit_price",
            store_field_name="store__name",
            price_profile=selected_price_profile,
        ),
    )

    cheapest_sort_price_listing = (
        active_listings.annotate(
            sort_price=Coalesce(
                "effective_hidden_price",
                "effective_final_price",
                output_field=DecimalField(max_digits=12, decimal_places=4),
            )
        )
        .exclude(sort_price__isnull=True)
        .order_by("sort_price", "id")
    )
    cheapest_price_listing = active_listings.exclude(effective_final_price__isnull=True).order_by(
        "effective_final_price",
        "id",
    )
    cheapest_unit_price_listing = (
        active_listings.annotate(
            sort_unit_price=Coalesce(
                "effective_hidden_unit_price",
                "effective_final_unit_price",
                output_field=DecimalField(max_digits=12, decimal_places=4),
            )
        )
        .exclude(sort_unit_price__isnull=True)
        .order_by("sort_unit_price", "id")
    )

    display_product_annotations = {
        "cheapest_final_price": Subquery(cheapest_price_listing.values("effective_final_price")[:1]),
        "cheapest_original_price": Subquery(cheapest_price_listing.values("effective_original_price")[:1]),
        "cheapest_final_unit_price": Subquery(
            cheapest_price_listing.values("effective_final_unit_price")[:1]
        ),
        "cheapest_original_unit_price": Subquery(
            cheapest_price_listing.values("effective_original_unit_price")[:1]
        ),
        "cheapest_store_name": Subquery(cheapest_price_listing.values("store__name")[:1]),
        "cheapest_unit_of_measure": Subquery(cheapest_price_listing.values("unit_of_measure")[:1]),
        "cheapest_discount_percent": Subquery(cheapest_price_listing.values("discount_percent")[:1]),
        "cheapest_one_plus_one": Subquery(cheapest_price_listing.values("one_plus_one")[:1]),
        "cheapest_two_plus_one": Subquery(cheapest_price_listing.values("two_plus_one")[:1]),
    }
    needs_offer_annotations = bool(selected_offer_filters) or sort == "discount_desc"
    needs_price_sort_annotations = sort in {"price_asc", "price_desc", "discount_desc"}
    needs_unit_sort_annotations = sort in {"unit_price_asc", "unit_price_desc"}

    products = (
        Product.objects.annotate(has_selected_listing=Exists(selected_active_listings))
        .filter(has_selected_listing=True)
    )

    if needs_offer_annotations:
        products = products.annotate(
            cheapest_discount_percent=Subquery(cheapest_price_listing.values("discount_percent")[:1]),
            cheapest_one_plus_one=Subquery(cheapest_price_listing.values("one_plus_one")[:1]),
            cheapest_two_plus_one=Subquery(cheapest_price_listing.values("two_plus_one")[:1]),
            cheapest_offer=Coalesce(
                Subquery(cheapest_price_listing.values("offer")[:1]),
                Value(False),
                output_field=BooleanField(),
            ),
        )

    if needs_price_sort_annotations:
        products = products.annotate(
            cheapest_sort_price=Subquery(cheapest_sort_price_listing.values("sort_price")[:1]),
        ).annotate(
            no_price_sort=Case(
                When(cheapest_sort_price__isnull=True, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
        )

    if needs_unit_sort_annotations:
        products = products.annotate(
            lowest_sort_unit_price=Subquery(cheapest_unit_price_listing.values("sort_unit_price")[:1]),
        ).annotate(
            no_unit_price_sort=Case(
                When(lowest_sort_unit_price__isnull=True, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
        )

    if sort == "discount_desc":
        products = products.annotate(
            discount_sort_group=Case(
                When(cheapest_discount_percent__gt=0, then=Value(0)),
                When(cheapest_one_plus_one=True, then=Value(1)),
                When(cheapest_two_plus_one=True, then=Value(2)),
                default=Value(3),
                output_field=IntegerField(),
            ),
            discount_sort_value=Case(
                When(cheapest_discount_percent__isnull=True, then=Value(-1)),
                default=F("cheapest_discount_percent"),
                output_field=IntegerField(),
            ),
        )

    if selected_category_filter:
        if selected_category_filter.isdigit():
            products = products.filter(category_id=int(selected_category_filter))
        else:
            products = products.filter(category__slug=selected_category_filter)

    available_offer_filters = [
        {
            "value": value,
            "label": label,
        }
        for value, label in OFFER_FILTER_OPTIONS
    ]

    if selected_offer_filters:
        selected_offer_q = Q()
        for offer_filter in selected_offer_filters:
            selected_offer_q |= _offer_filter_condition(offer_filter)
        products = products.filter(selected_offer_q)

    if search_query_forms:
        product_search_q = _token_form_query("search_name", search_query_forms)
        primary_form_tokens = [token for token in search_query_forms[0].split() if token]
        primary_token = primary_form_tokens[0] if primary_form_tokens else search_query_forms[0]

        listing_search_listings = StoreListing.objects.filter(
            product_id=OuterRef("pk"),
            is_active=True,
        ).filter(_token_form_query("search_store_name", search_query_forms))
        if selected_store_ids:
            listing_search_listings = listing_search_listings.filter(store_id__in=selected_store_ids)
        products = (
            products.annotate(
                listing_search_match=Exists(listing_search_listings),
            )
            .filter(product_search_q | Q(listing_search_match=True))
            .annotate(
                product_search_score=Case(
                    When(search_name__istartswith=primary_token, then=Value(120)),
                    When(search_name__icontains=primary_token, then=Value(100)),
                    default=Value(0),
                    output_field=IntegerField(),
                ),
                listing_search_score=Case(
                    When(listing_search_match=True, then=Value(80)),
                    default=Value(0),
                    output_field=IntegerField(),
                ),
            )
            .annotate(
                search_score=Greatest(
                    Coalesce("product_search_score", Value(0)),
                    Coalesce("listing_search_score", Value(0)),
                    output_field=IntegerField(),
                )
            )
        )

    if sort == "relevance":
        if search_query_forms:
            sort_ordering = ["-search_score", "canonical_name"]
        else:
            sort_ordering = ["canonical_name"]
    elif sort == "price_asc":
        sort_ordering = [
            "no_price_sort",
            "cheapest_sort_price",
            "canonical_name",
        ]
    elif sort == "price_desc":
        sort_ordering = [
            "no_price_sort",
            "-cheapest_sort_price",
            "canonical_name",
        ]
    elif sort == "unit_price_asc":
        sort_ordering = [
            "no_unit_price_sort",
            "lowest_sort_unit_price",
            "canonical_name",
        ]
    elif sort == "unit_price_desc":
        sort_ordering = [
            "no_unit_price_sort",
            "-lowest_sort_unit_price",
            "canonical_name",
        ]
    elif sort == "discount_desc":
        sort_ordering = [
            "discount_sort_group",
            "-discount_sort_value",
            "no_price_sort",
            "cheapest_sort_price",
            "canonical_name",
        ]
    else:
        sort_ordering = ["canonical_name"]
    products = products.order_by(*sort_ordering)

    page_id_obj = _paginate_queryset_without_count(
        products.values_list("id", flat=True),
        raw_page=request.GET.get("page"),
        per_page=PRODUCTS_PER_PAGE,
    )
    page_product_ids = list(page_id_obj.object_list)
    page_products: list[Product] = []
    if page_product_ids:
        page_products_by_id = {
            product.id: product
            for product in Product.objects.select_related("category")
            .filter(id__in=page_product_ids)
            .annotate(**display_product_annotations)
        }
        page_products = [
            page_products_by_id[product_id]
            for product_id in page_product_ids
            if product_id in page_products_by_id
        ]
    page_obj = OffsetPage(
        object_list=page_products,
        number=page_id_obj.number,
        per_page=page_id_obj.per_page,
        _has_next=page_id_obj._has_next,
    )
    active_listing_counts = {
        product_id: count
        for product_id, count in StoreListing.objects.filter(
            product_id__in=[product.id for product in page_products],
            is_active=True,
        )
        .values_list("product_id")
        .annotate(count=Count("id"))
    }
    for product in page_products:
        product.store_icon_url = _store_icon_url(getattr(product, "cheapest_store_name", None))
        product.sale_icon_url = _sale_icon_url(
            discount_percent=getattr(product, "cheapest_discount_percent", None),
            one_plus_one=bool(getattr(product, "cheapest_one_plus_one", False)),
            two_plus_one=bool(getattr(product, "cheapest_two_plus_one", False)),
        )
        product.active_listing_count = active_listing_counts.get(product.id, 0)
        _set_product_display_prices(product, price_profile=selected_price_profile)

    available_store_listings = StoreListing.objects.filter(
        store_id=OuterRef("pk"),
        is_active=True,
        product_id__isnull=False,
    )
    stores = list(
        Store.objects.annotate(has_available_listing=Exists(available_store_listings))
        .filter(has_available_listing=True)
        .order_by("name")
    )
    for store in stores:
        store.display_name = _store_display_name(store.name)

    return render(
        request,
        "comparison/product_list.html",
        {
            "products": page_products,
            "page_obj": page_obj,
            "sort": sort,
            "sort_options": sort_options,
            "stores": stores,
            "selected_store_ids": selected_store_ids,
            "selected_store_query": _selected_store_query(selected_store_ids),
            "selected_offer_filters": selected_offer_filters,
            "available_offer_filters": available_offer_filters,
            "available_categories": available_categories,
            "selected_category_slug": selected_category_slug,
            "search_query": search_query,
            "price_profile_options": PRICE_PROFILE_OPTIONS,
            "selected_price_profile": selected_price_profile,
            "selected_price_profile_meta": selected_price_profile_meta,
            "selected_price_profile_query": _selected_price_profile_query(selected_price_profile),
            "selected_filters_query": _selected_filters_query(
                store_ids=selected_store_ids,
                offer_filters=selected_offer_filters,
                category_filter=selected_category_filter,
                search_query=search_query,
                price_profile=selected_price_profile,
            ),
            "selected_category_filter": selected_category_filter,
        },
    )


def product_detail(request, product_id: int):
    selected_price_profile = parse_price_profile(request.GET.get(PRICE_PROFILE_PARAM))
    selected_price_profile_meta = get_price_profile(selected_price_profile)
    product = get_object_or_404(Product.objects.select_related("category"), id=product_id)
    listings = (
        StoreListing.objects.select_related("store")
        .filter(product=product, is_active=True)
        .order_by("store__name", "store_name")
    )
    listing_rows = list(listings)
    pending_report_counts_by_listing_id = {
        row["store_listing_id"]: row["report_count"]
        for row in ListingProductReport.objects.filter(
            store_listing_id__in=[listing.id for listing in listing_rows],
            status=ListingProductReport.Status.PENDING,
        ).values("store_listing_id", "report_count")
    }
    for listing in listing_rows:
        _set_listing_display_prices(listing, price_profile=selected_price_profile)
        listing.pending_product_report_count = pending_report_counts_by_listing_id.get(listing.id, 0)
    return render(
        request,
        "comparison/product_detail.html",
        {
            "product": product,
            "listings": listing_rows,
            "price_profile_options": PRICE_PROFILE_OPTIONS,
            "selected_price_profile": selected_price_profile,
            "selected_price_profile_meta": selected_price_profile_meta,
            "selected_price_profile_query": _selected_price_profile_query(selected_price_profile),
        },
    )


def _safe_next_url(request, *, default_url: str) -> str:
    next_url = (request.POST.get("next") or request.GET.get("next") or "").strip()
    if next_url and url_has_allowed_host_and_scheme(
        url=next_url,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return next_url
    return default_url


@require_POST
def report_product_listing(request, product_id: int):
    redirect_target = _safe_next_url(
        request,
        default_url=reverse("product-detail", args=[product_id]),
    )
    listing = get_object_or_404(
        StoreListing.objects.select_related("product"),
        id=request.POST.get("listing_id"),
        product_id=product_id,
        is_active=True,
    )
    now = timezone.now()
    pending_report = ListingProductReport.objects.filter(
        store_listing=listing,
        status=ListingProductReport.Status.PENDING,
    ).first()

    if pending_report is None:
        ListingProductReport.objects.create(
            store_listing=listing,
            reported_product=listing.product,
            last_reported_at=now,
        )
        messages.success(request, "The listing was reported for admin review.")
    else:
        ListingProductReport.objects.filter(pk=pending_report.pk).update(
            report_count=F("report_count") + 1,
            reported_product=listing.product,
            last_reported_at=now,
        )
        messages.success(
            request,
            "The listing was already pending review, and the report count was increased.",
        )

    return redirect(redirect_target)


def product_offers(request, product_id: int):
    selected_price_profile = parse_price_profile(request.GET.get(PRICE_PROFILE_PARAM))
    selected_price_profile_meta = get_price_profile(selected_price_profile)
    product = get_object_or_404(Product, id=product_id)
    listings = (
        StoreListing.objects.select_related("store")
        .filter(product=product, is_active=True)
        .order_by("store__name", "id")
    )

    payload = {
        "product": {
            "id": product.id,
            "canonical_name": product.canonical_name,
            "brand_normalized": product.brand_normalized,
            "category": {
                "id": product.category_id,
                "name": product.category.name if product.category_id else None,
                "display_name": product.category.display_name if product.category_id else None,
                "slug": product.category.slug if product.category_id else None,
            },
            "quantity_value": str(product.quantity_value) if product.quantity_value is not None else None,
            "quantity_unit": product.quantity_unit,
            "normalized_key": product.normalized_key,
        },
        "price_profile": {
            "key": selected_price_profile or None,
            "label": selected_price_profile_meta.label if selected_price_profile_meta else None,
            "description": (
                selected_price_profile_meta.description if selected_price_profile_meta else None
            ),
        },
        "offers": [
            {
                "store": listing.store.name,
                "listing_id": listing.id,
                "store_name": listing.store_name,
                "url": listing.url,
                "image_url": listing.image_url,
                "final_price": str(listing.final_price) if listing.final_price is not None else None,
                "final_unit_price": (
                    str(listing.final_unit_price) if listing.final_unit_price is not None else None
                ),
                "original_price": str(listing.original_price) if listing.original_price is not None else None,
                "original_unit_price": (
                    str(listing.original_unit_price)
                    if listing.original_unit_price is not None
                    else None
                ),
                "effective_final_price": (
                    str(
                        apply_price_profile_value(
                            listing.final_price,
                            store_name=listing.store.name,
                            price_profile=selected_price_profile,
                        )
                    )
                    if listing.final_price is not None
                    else None
                ),
                "effective_final_unit_price": (
                    str(
                        apply_price_profile_value(
                            listing.final_unit_price,
                            store_name=listing.store.name,
                            price_profile=selected_price_profile,
                        )
                    )
                    if listing.final_unit_price is not None
                    else None
                ),
                "effective_original_price": (
                    str(
                        apply_price_profile_value(
                            listing.original_price,
                            store_name=listing.store.name,
                            price_profile=selected_price_profile,
                        )
                    )
                    if listing.original_price is not None
                    else None
                ),
                "effective_original_unit_price": (
                    str(
                        apply_price_profile_value(
                            listing.original_unit_price,
                            store_name=listing.store.name,
                            price_profile=selected_price_profile,
                        )
                    )
                    if listing.original_unit_price is not None
                    else None
                ),
                "unit_of_measure": listing.unit_of_measure,
                "offer": listing.offer,
                "price_profile_applies": price_profile_applies_to_store(
                    store_name=listing.store.name,
                    price_profile=selected_price_profile,
                ),
                "last_updated": listing.last_seen_at.isoformat(),
            }
            for listing in listings
        ],
    }
    return JsonResponse(payload)


def _format_decimal_compact(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _product_quantity_label(product: Product | None) -> str | None:
    if product is None or product.quantity_value is None or not product.quantity_unit:
        return None
    quantity_value = _format_decimal_compact(product.quantity_value)
    if quantity_value is None:
        return None
    return f"{quantity_value} {product.quantity_unit}"


def _listing_offer_label(listing: StoreListing) -> str | None:
    if listing.one_plus_one:
        return "1 + 1"
    if listing.two_plus_one:
        return "2 + 1"
    if listing.discount_percent:
        return f"-{listing.discount_percent}%"
    if listing.promo_text:
        return listing.promo_text
    if listing.offer:
        return str(_("Offer"))
    return None


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


def _review_queue_filters_query(*, search_query: str, selected_store_id: int | None) -> str:
    params: list[tuple[str, str]] = []
    if search_query:
        params.append(("q", search_query))
    if selected_store_id is not None:
        params.append(("store", str(selected_store_id)))
    return urlencode(params)


def _review_queue_redirect_target(request) -> str:
    return _safe_next_url(
        request,
        default_url=reverse("match-review-queue"),
    )


def _listing_resolved_category_id(listing: StoreListing) -> int | None:
    if not listing.source_category:
        return None
    return resolve_category_id_for_source(
        store_id=listing.store_id,
        source_category=listing.source_category,
    )


def _build_listing_report_entry(report: ListingProductReport) -> dict[str, object]:
    listing = report.store_listing
    current_product = listing.product
    reported_product = report.reported_product

    return {
        "report": report,
        "listing": listing,
        "listing_store_display_name": _store_display_name(listing.store.name),
        "listing_store_icon_url": _store_icon_url(listing.store.name),
        "listing_offer_label": _listing_offer_label(listing),
        "current_product": current_product,
        "current_product_quantity": _product_quantity_label(current_product),
        "reported_product": reported_product,
        "reported_product_quantity": _product_quantity_label(reported_product),
        "resolved_category_id": _listing_resolved_category_id(listing),
    }


def _listing_report_filters_query(*, search_query: str, selected_store_id: int | None) -> str:
    params: list[tuple[str, str]] = []
    if search_query:
        params.append(("q", search_query))
    if selected_store_id is not None:
        params.append(("store", str(selected_store_id)))
    return urlencode(params)


def _listing_report_default_candidate_query(report: ListingProductReport) -> str:
    listing = report.store_listing
    for value in [listing.store_name, listing.store_brand, report.reported_product]:
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
        _listing_resolved_category_id(listing),
        getattr(listing.product, "category_id", None),
        getattr(report.reported_product, "category_id", None),
    ]:
        if category_id:
            return category_id
    return None


def _listing_report_candidate_products(
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
        _token_form_query("search_name", search_query_forms)
        | _token_form_query("canonical_name", search_query_forms)
        | _token_form_query("brand_normalized", search_query_forms)
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


@staff_member_required(login_url="admin:login")
def match_review_queue(request):
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        redirect_target = _review_queue_redirect_target(request)

        if action == "approve":
            review = MatchReview.objects.filter(
                id=request.POST.get("review_id"),
                status=MatchReview.Status.PENDING,
            ).first()
            if review is None:
                messages.error(request, "The selected review is no longer pending.")
            else:
                result = approve_match_reviews(MatchReview.objects.filter(pk=review.pk))
                messages.success(
                    request,
                    "Approved "
                    f"{result.approved} review and auto-rejected {result.auto_rejected} "
                    "conflicting pending review(s).",
                )
            return redirect(redirect_target)

        if action == "reject_listing":
            queryset = MatchReview.objects.filter(
                store_listing_id=request.POST.get("listing_id"),
                status=MatchReview.Status.PENDING,
            )
            if not queryset.exists():
                messages.error(request, "This listing no longer has pending reviews.")
            else:
                result = reject_match_reviews(queryset)
                messages.success(
                    request,
                    f"Rejected {result.rejected} pending review(s) and created "
                    f"{result.forced_new_products} new product(s).",
                )
            return redirect(redirect_target)

        messages.error(request, "Unknown review action.")
        return redirect(redirect_target)

    search_query = (request.GET.get("q") or "").strip()
    try:
        selected_store_id = int(request.GET.get("store", "").strip() or "")
    except (TypeError, ValueError):
        selected_store_id = None
    if selected_store_id is not None and selected_store_id <= 0:
        selected_store_id = None

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
        resolved_category_id = None
        if listing.source_category:
            resolved_category_id = resolve_category_id_for_source(
                store_id=listing.store_id,
                source_category=listing.source_category,
            )
        if resolved_category_id is not None:
            resolved_category_ids.add(resolved_category_id)

        entry = groups_by_listing_id.setdefault(
            listing.id,
            {
                "listing": listing,
                "listing_store_display_name": _store_display_name(listing.store.name),
                "listing_store_icon_url": _store_icon_url(listing.store.name),
                "listing_offer_label": _listing_offer_label(listing),
                "listing_quantity_label": listing.unit_of_measure or None,
                "resolved_category_id": resolved_category_id,
                "current_product": listing.product,
                "current_product_quantity": _product_quantity_label(listing.product),
                "reviews": [],
                "top_score": review.score,
            },
        )

        candidate_product = review.candidate_product
        candidate_active_listings = list(
            getattr(candidate_product, "review_queue_active_listings", [])
        )
        for candidate_listing in candidate_active_listings:
            candidate_listing.store_display_name = _store_display_name(candidate_listing.store.name)

        entry["reviews"].append(
            {
                "review": review,
                "candidate": candidate_product,
                "candidate_quantity": _product_quantity_label(candidate_product),
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
        store.display_name = _store_display_name(store.name)

    filters_query = _review_queue_filters_query(
        search_query=search_query,
        selected_store_id=selected_store_id,
    )

    return render(
        request,
        "comparison/match_review_queue.html",
        {
            "page_obj": page_obj,
            "stores": stores,
            "search_query": search_query,
            "selected_store_id": selected_store_id,
            "filters_query": filters_query,
            "visible_listing_count": len(queue_entries),
            "visible_review_count": len(review_rows),
        },
    )


@staff_member_required(login_url="admin:login")
def listing_report_queue(request):
    search_query = (request.GET.get("q") or "").strip()
    try:
        selected_store_id = int(request.GET.get("store", "").strip() or "")
    except (TypeError, ValueError):
        selected_store_id = None
    if selected_store_id is not None and selected_store_id <= 0:
        selected_store_id = None

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
    queue_entries = [_build_listing_report_entry(report) for report in report_rows]
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
        store.display_name = _store_display_name(store.name)

    filters_query = _listing_report_filters_query(
        search_query=search_query,
        selected_store_id=selected_store_id,
    )

    return render(
        request,
        "comparison/listing_report_queue.html",
        {
            "page_obj": page_obj,
            "stores": stores,
            "search_query": search_query,
            "selected_store_id": selected_store_id,
            "filters_query": filters_query,
            "visible_report_count": len(queue_entries),
        },
    )


@staff_member_required(login_url="admin:login")
def listing_report_detail(request, report_id: int):
    report = get_object_or_404(
        ListingProductReport.objects.select_related(
            "store_listing__store",
            "store_listing__product__category",
            "reported_product__category",
        ),
        id=report_id,
        status=ListingProductReport.Status.PENDING,
    )
    return_to = _safe_next_url(
        request,
        default_url=reverse("listing-report-queue"),
    )

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        redirect_target = _safe_next_url(
            request,
            default_url=reverse("listing-report-queue"),
        )
        report = ListingProductReport.objects.select_related(
            "store_listing__product",
        ).filter(
            id=report_id,
            status=ListingProductReport.Status.PENDING,
        ).first()
        if report is None:
            messages.error(request, "The selected report is no longer pending.")
            return redirect(redirect_target)

        if action == "reassign":
            try:
                target_product_id = int(request.POST.get("target_product_id", "").strip())
            except (AttributeError, TypeError, ValueError):
                target_product_id = None
            target_product = (
                Product.objects.select_related("category").filter(id=target_product_id).first()
                if target_product_id is not None
                else None
            )
            if target_product is None:
                messages.error(request, "Select a valid product for reassignment.")
                return redirect(reverse("listing-report-detail", args=[report_id]))
            if target_product.id == report.store_listing.product_id:
                messages.error(
                    request,
                    "The listing is already linked to that product. Dismiss the report instead.",
                )
                return redirect(reverse("listing-report-detail", args=[report_id]))

            listing = report.store_listing
            listing.product = target_product
            listing.save(update_fields=["product"])
            report.reassigned_product = target_product
            report.status = ListingProductReport.Status.REASSIGNED
            report.resolved_at = timezone.now()
            report.resolved_by = request.user
            report.save(
                update_fields=[
                    "reassigned_product",
                    "status",
                    "resolved_at",
                    "resolved_by",
                ]
            )
            messages.success(
                request,
                f"Reassigned the listing to '{target_product.canonical_name}'.",
            )
            return redirect(redirect_target)

        if action == "dismiss":
            report.status = ListingProductReport.Status.DISMISSED
            report.resolved_at = timezone.now()
            report.resolved_by = request.user
            report.save(update_fields=["status", "resolved_at", "resolved_by"])
            messages.success(request, "Dismissed the listing report.")
            return redirect(redirect_target)

        messages.error(request, "Unknown report action.")
        return redirect(reverse("listing-report-detail", args=[report_id]))

    entry = _build_listing_report_entry(report)
    resolved_category_id = entry["resolved_category_id"]
    entry["resolved_category"] = (
        Category.objects.filter(id=resolved_category_id).first()
        if resolved_category_id is not None
        else None
    )

    candidate_query = (
        (request.GET.get("candidate_q") or "").strip()
        or _listing_report_default_candidate_query(report)
    )
    candidate_products = _listing_report_candidate_products(
        report=report,
        query=candidate_query,
    )
    for candidate in candidate_products:
        candidate.quantity_label = _product_quantity_label(candidate)

    return render(
        request,
        "comparison/listing_report_detail.html",
        {
            "entry": entry,
            "candidate_query": candidate_query,
            "candidate_products": candidate_products,
            "return_to": return_to,
        },
    )
