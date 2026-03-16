from __future__ import annotations

from dataclasses import dataclass

from django.db.models import (
    BooleanField,
    Case,
    Count,
    DecimalField,
    Exists,
    F,
    IntegerField,
    OuterRef,
    Q,
    Subquery,
    Value,
    When,
)
from django.db.models.functions import Coalesce, Greatest
from django.http import HttpRequest
from django.utils.http import urlencode
from django.utils.translation import gettext_lazy as _

from catalog.models import Category, Product, Store
from catalog.search_normalizer import build_search_forms
from comparison.pricing import (
    PRICE_PROFILE_OPTIONS,
    PRICE_PROFILE_PARAM,
    adjusted_price_expression,
    get_price_profile,
    parse_price_profile,
)
from comparison.view_helpers import (
    sale_icon_url,
    selected_price_profile_query,
    set_product_display_prices,
    store_display_name,
    store_icon_url,
    token_form_query,
)
from ingestion.models import StoreListing

PRODUCTS_PER_PAGE = 20
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


def build_product_list_context(request: HttpRequest) -> dict[str, object]:
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
        product_search_q = token_form_query("search_name", search_query_forms)
        primary_form_tokens = [token for token in search_query_forms[0].split() if token]
        primary_token = primary_form_tokens[0] if primary_form_tokens else search_query_forms[0]

        listing_search_listings = StoreListing.objects.filter(
            product_id=OuterRef("pk"),
            is_active=True,
        ).filter(token_form_query("search_store_name", search_query_forms))
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
        product.store_icon_url = store_icon_url(getattr(product, "cheapest_store_name", None))
        product.sale_icon_url = sale_icon_url(
            discount_percent=getattr(product, "cheapest_discount_percent", None),
            one_plus_one=bool(getattr(product, "cheapest_one_plus_one", False)),
            two_plus_one=bool(getattr(product, "cheapest_two_plus_one", False)),
        )
        product.active_listing_count = active_listing_counts.get(product.id, 0)
        set_product_display_prices(product, price_profile=selected_price_profile)

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
        store.display_name = store_display_name(store.name)

    return {
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
        "selected_price_profile_query": selected_price_profile_query(selected_price_profile),
        "selected_filters_query": _selected_filters_query(
            store_ids=selected_store_ids,
            offer_filters=selected_offer_filters,
            category_filter=selected_category_filter,
            search_query=search_query,
            price_profile=selected_price_profile,
        ),
        "selected_category_filter": selected_category_filter,
    }
