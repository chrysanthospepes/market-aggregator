from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db.models import Q
from django.utils.http import urlencode
from django.utils.translation import gettext_lazy as _

from catalog.models import Product
from comparison.pricing import (
    apply_price_profile_value,
    price_profile_applies_to_store,
)
from ingestion.models import StoreListing


STORE_DISPLAY_NAME_BY_KEY: dict[str, str] = {
    "ab": "ΑΒ Βασιλόπουλος",
    "bazaar": "Bazaar",
    "kritikos": "Κρητικός",
    "masoutis": "Μασούτης",
    "mymarket": "My Market",
    "sklavenitis": "Σκλαβενίτης",
}


def listing_offer_condition() -> Q:
    return (
        Q(offer=True)
        | Q(discount_percent__gt=0)
        | Q(one_plus_one=True)
        | Q(two_plus_one=True)
    )


def store_icon_url(store_name: str | None) -> str | None:
    normalized_name = (store_name or "").strip().lower()
    if not normalized_name:
        return None
    return f"{settings.MEDIA_URL}stores/{normalized_name}.png"


def store_display_name(store_name: str | None) -> str:
    normalized_name = (store_name or "").strip().lower()
    return STORE_DISPLAY_NAME_BY_KEY.get(normalized_name, store_name or "")


def sale_icon_url(
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


def selected_price_profile_query(price_profile: str) -> str:
    if not price_profile:
        return ""
    return urlencode([("price_profile", price_profile)])


def set_listing_display_prices(listing: StoreListing, *, price_profile: str) -> None:
    store_name = listing.store.name if getattr(listing, "store_id", None) else None
    listing.store_display_name = store_display_name(store_name)
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


def set_product_display_prices(product, *, price_profile: str) -> None:
    product.display_final_price = getattr(product, "cheapest_final_price", None)
    product.display_final_unit_price = getattr(product, "cheapest_final_unit_price", None)
    product.display_original_price = getattr(product, "cheapest_original_price", None)
    product.display_original_unit_price = getattr(product, "cheapest_original_unit_price", None)
    product.price_profile_applies = price_profile_applies_to_store(
        store_name=getattr(product, "cheapest_store_name", None),
        price_profile=price_profile,
    )


def token_form_query(field_name: str, search_query_forms: list[str]) -> Q:
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


def format_decimal_compact(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def product_quantity_label(product: Product | None) -> str | None:
    if product is None or product.quantity_value is None or not product.quantity_unit:
        return None
    quantity_value = format_decimal_compact(product.quantity_value)
    if quantity_value is None:
        return None
    return f"{quantity_value} {product.quantity_unit}"


def listing_offer_label(listing: StoreListing) -> str | None:
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
