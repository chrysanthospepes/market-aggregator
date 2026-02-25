from __future__ import annotations

from django.db.models import Case, Count, IntegerField, Min, OuterRef, Q, Subquery, Value, When
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render

from catalog.models import Product
from ingestion.models import StoreListing


def product_list(request):
    sort = request.GET.get("sort", "name")
    active_listings = StoreListing.objects.filter(
        product_id=OuterRef("pk"),
        is_active=True,
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
                filter=Q(store_listings__is_active=True),
                distinct=True,
            ),
            lowest_final_unit_price=Min(
                "store_listings__final_unit_price",
                filter=Q(
                    store_listings__is_active=True,
                    store_listings__final_unit_price__isnull=False,
                ),
            ),
            cheapest_final_price=Subquery(cheapest_price_listing.values("final_price")[:1]),
            cheapest_original_price=Subquery(cheapest_price_listing.values("original_price")[:1]),
            cheapest_final_unit_price=Subquery(cheapest_price_listing.values("final_unit_price")[:1]),
            cheapest_original_unit_price=Subquery(
                cheapest_price_listing.values("original_unit_price")[:1]
            ),
            no_unit_price_sort=Case(
                When(lowest_final_unit_price__isnull=True, then=Value(1)),
                default=Value(0),
                output_field=IntegerField(),
            )
        )
        .filter(active_listing_count__gt=0)
    )

    if sort == "unit_price_asc":
        products = products.order_by(
            "no_unit_price_sort",
            "lowest_final_unit_price",
            "canonical_name",
        )
    else:
        products = products.order_by("canonical_name")

    return render(
        request,
        "comparison/product_list.html",
        {
            "products": products,
            "sort": sort,
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
