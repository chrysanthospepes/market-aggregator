from __future__ import annotations

from django.http import JsonResponse
from django.shortcuts import get_object_or_404

from catalog.models import Product
from ingestion.models import StoreListing


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
