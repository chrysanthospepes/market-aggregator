from __future__ import annotations

from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import (
    BooleanField,
    Case,
    Count,
    F,
    IntegerField,
    Min,
    OuterRef,
    Q,
    Subquery,
    Value,
    When,
)
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils.http import urlencode

from catalog.models import Category, Product, Store
from ingestion.models import StoreListing

HOME_PRODUCTS_PER_STORE = 20
PRODUCTS_PER_PAGE = 20
DEFAULT_SORT = "price_asc"
SORT_OPTIONS: tuple[tuple[str, str], ...] = (
    ("price_asc", "Increasing price"),
    ("price_desc", "Declining price"),
    ("unit_price_asc", "Increasing unit price"),
    ("unit_price_desc", "Declining unit price"),
    ("discount_desc", "Declining discount"),
)
OFFER_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("no_offer", "No offer"),
    ("discount_0_20", "Up to 20%"),
    ("discount_21_40", "From 21% up to 40%"),
    ("discount_41_plus", "From 41% up to max"),
    ("one_plus_one", "1 + 1"),
    ("two_plus_one", "2 + 1"),
)


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
) -> str:
    params: list[tuple[str, str]] = []
    if category_filter:
        params.append(("category", category_filter))
    for offer_filter in offer_filters:
        params.append(("offer_filter", offer_filter))
    for store_id in store_ids:
        params.append(("stores", str(store_id)))
    if not params:
        return ""
    return "&" + urlencode(params, doseq=True)


def home(request):
    categories = Category.objects.order_by("name")
    selected_category_filter = (request.GET.get("category") or "").strip()
    if selected_category_filter and not categories.filter(slug=selected_category_filter).exists():
        selected_category_filter = ""

    stores = Store.objects.filter(listings__is_active=True).order_by("name").distinct()
    store_sections: list[dict[str, object]] = []

    for store in stores:
        listings = (
            StoreListing.objects.select_related("product")
            .filter(
                store=store,
                is_active=True,
                product__isnull=False,
            )
        )
        if selected_category_filter:
            listings = listings.filter(product__category__slug=selected_category_filter)
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

        store_sections.append(
            {
                "store": store,
                "store_icon_url": _store_icon_url(store.name),
                "listings": picked_listings,
            }
        )

    return render(
        request,
        "comparison/home.html",
        {
            "categories": categories,
            "selected_category_filter": selected_category_filter,
            "store_sections": store_sections,
        },
    )


def product_list(request):
    sort = _parse_sort(request.GET.get("sort"))
    selected_category_filter = (request.GET.get("category") or "").strip()
    selected_store_ids = _parse_selected_store_ids(request.GET.getlist("stores"))
    selected_offer_filters = _parse_selected_offer_filters(request.GET.getlist("offer_filter"))
    active_listings = StoreListing.objects.filter(product_id=OuterRef("pk"), is_active=True)
    all_active_listing_filter = Q(store_listings__is_active=True)
    selected_listing_filter = all_active_listing_filter
    if selected_store_ids:
        active_listings = active_listings.filter(store_id__in=selected_store_ids)
        selected_listing_filter &= Q(store_listings__store_id__in=selected_store_ids)

    cheapest_sort_price_listing = (
        active_listings.annotate(sort_price=Coalesce("hidden_price", "final_price"))
        .exclude(sort_price__isnull=True)
        .order_by("sort_price", "id")
    )
    cheapest_price_listing = active_listings.exclude(final_price__isnull=True).order_by(
        "final_price",
        "id",
    )

    products = (
        Product.objects.select_related("category")
        .annotate(
            active_listing_count=Count(
                "store_listings",
                filter=all_active_listing_filter,
                distinct=True,
            ),
            selected_store_listing_count=Count(
                "store_listings",
                filter=selected_listing_filter,
                distinct=True,
            ),
            lowest_sort_unit_price=Min(
                Coalesce("store_listings__hidden_unit_price", "store_listings__final_unit_price"),
                filter=selected_listing_filter
                & (
                    Q(store_listings__hidden_unit_price__isnull=False)
                    | Q(store_listings__final_unit_price__isnull=False)
                ),
            ),
            lowest_final_unit_price=Min(
                "store_listings__final_unit_price",
                filter=selected_listing_filter & Q(store_listings__final_unit_price__isnull=False),
            ),
            cheapest_sort_price=Subquery(cheapest_sort_price_listing.values("sort_price")[:1]),
            cheapest_final_price=Subquery(cheapest_price_listing.values("final_price")[:1]),
            cheapest_original_price=Subquery(cheapest_price_listing.values("original_price")[:1]),
            cheapest_final_unit_price=Subquery(cheapest_price_listing.values("final_unit_price")[:1]),
            cheapest_original_unit_price=Subquery(
                cheapest_price_listing.values("original_unit_price")[:1]
            ),
            cheapest_store_name=Subquery(cheapest_price_listing.values("store__name")[:1]),
            cheapest_unit_of_measure=Subquery(
                cheapest_price_listing.values("unit_of_measure")[:1]
            ),
            cheapest_discount_percent=Subquery(
                cheapest_price_listing.values("discount_percent")[:1]
            ),
            cheapest_one_plus_one=Subquery(cheapest_price_listing.values("one_plus_one")[:1]),
            cheapest_two_plus_one=Subquery(cheapest_price_listing.values("two_plus_one")[:1]),
            cheapest_offer=Coalesce(
                Subquery(cheapest_price_listing.values("offer")[:1]),
                Value(False),
                output_field=BooleanField(),
            ),
            no_unit_price_sort=Case(
                When(lowest_sort_unit_price__isnull=True, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
            no_price_sort=Case(
                When(cheapest_sort_price__isnull=True, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            ),
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
        .filter(selected_store_listing_count__gt=0)
    )

    if selected_category_filter:
        if selected_category_filter.isdigit():
            products = products.filter(category_id=int(selected_category_filter))
        else:
            products = products.filter(category__slug=selected_category_filter)

    offer_counts = products.aggregate(
        no_offer=Count("id", filter=_offer_filter_condition("no_offer")),
        discount_0_20=Count("id", filter=_offer_filter_condition("discount_0_20")),
        discount_21_40=Count("id", filter=_offer_filter_condition("discount_21_40")),
        discount_41_plus=Count("id", filter=_offer_filter_condition("discount_41_plus")),
        one_plus_one=Count("id", filter=_offer_filter_condition("one_plus_one")),
        two_plus_one=Count("id", filter=_offer_filter_condition("two_plus_one")),
    )
    available_offer_filters = [
        {
            "value": value,
            "label": label,
            "count": int(offer_counts.get(value) or 0),
        }
        for value, label in OFFER_FILTER_OPTIONS
        if int(offer_counts.get(value) or 0) > 0 or value in selected_offer_filters
    ]

    if selected_offer_filters:
        selected_offer_q = Q()
        for offer_filter in selected_offer_filters:
            selected_offer_q |= _offer_filter_condition(offer_filter)
        products = products.filter(selected_offer_q)

    if sort == "price_asc":
        products = products.order_by(
            "no_price_sort",
            "cheapest_sort_price",
            "canonical_name",
        )
    elif sort == "price_desc":
        products = products.order_by(
            "no_price_sort",
            "-cheapest_sort_price",
            "canonical_name",
        )
    elif sort == "unit_price_asc":
        products = products.order_by(
            "no_unit_price_sort",
            "lowest_sort_unit_price",
            "canonical_name",
        )
    elif sort == "unit_price_desc":
        products = products.order_by(
            "no_unit_price_sort",
            "-lowest_sort_unit_price",
            "canonical_name",
        )
    elif sort == "discount_desc":
        products = products.order_by(
            "discount_sort_group",
            "-discount_sort_value",
            "no_price_sort",
            "cheapest_sort_price",
            "canonical_name",
        )
    else:
        products = products.order_by("canonical_name")

    paginator = Paginator(products, PRODUCTS_PER_PAGE)
    page_obj = paginator.get_page(request.GET.get("page"))
    page_products = list(page_obj.object_list)
    for product in page_products:
        product.store_icon_url = _store_icon_url(getattr(product, "cheapest_store_name", None))
        product.sale_icon_url = _sale_icon_url(
            discount_percent=getattr(product, "cheapest_discount_percent", None),
            one_plus_one=bool(getattr(product, "cheapest_one_plus_one", False)),
            two_plus_one=bool(getattr(product, "cheapest_two_plus_one", False)),
        )

    stores = (
        Store.objects.filter(listings__is_active=True)
        .annotate(
            active_product_count=Count(
                "listings__product_id",
                filter=Q(listings__is_active=True, listings__product_id__isnull=False),
                distinct=True,
            ),
        )
        .order_by("name")
        .distinct()
    )

    return render(
        request,
        "comparison/product_list.html",
        {
            "products": page_products,
            "page_obj": page_obj,
            "sort": sort,
            "sort_options": SORT_OPTIONS,
            "stores": stores,
            "selected_store_ids": selected_store_ids,
            "selected_store_query": _selected_store_query(selected_store_ids),
            "selected_offer_filters": selected_offer_filters,
            "available_offer_filters": available_offer_filters,
            "selected_filters_query": _selected_filters_query(
                store_ids=selected_store_ids,
                offer_filters=selected_offer_filters,
                category_filter=selected_category_filter,
            ),
            "selected_category_filter": selected_category_filter,
        },
    )


def product_detail(request, product_id: int):
    product = get_object_or_404(Product.objects.select_related("category"), id=product_id)
    listings = (
        StoreListing.objects.select_related("store")
        .filter(product=product, is_active=True)
        .order_by("store__name", "store_name")
    )
    return render(
        request,
        "comparison/product_detail.html",
        {
            "product": product,
            "listings": listings,
        },
    )


def product_offers(request, product_id: int):
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
                "slug": product.category.slug if product.category_id else None,
            },
            "quantity_value": str(product.quantity_value) if product.quantity_value is not None else None,
            "quantity_unit": product.quantity_unit,
            "normalized_key": product.normalized_key,
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
                "unit_of_measure": listing.unit_of_measure,
                "offer": listing.offer,
                "last_updated": listing.last_seen_at.isoformat(),
            }
            for listing in listings
        ],
    }
    return JsonResponse(payload)
