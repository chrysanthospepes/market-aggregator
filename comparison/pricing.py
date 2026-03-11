from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from django.db.models import Case, DecimalField, ExpressionWrapper, F, Value, When

PRICE_PROFILE_PARAM = "price_profile"
DEFAULT_PRICE_PROFILE = ""
KRITIKOS_ELIGIBLE_HOUSEHOLD_PROFILE = "kritikos_eligible_households"
_DISPLAY_PRICE_QUANTUM = Decimal("0.01")


@dataclass(frozen=True)
class PriceProfile:
    key: str
    label: str
    description: str
    store_name: str
    multiplier: Decimal


PRICE_PROFILES: dict[str, PriceProfile] = {
    KRITIKOS_ELIGIBLE_HOUSEHOLD_PROFILE: PriceProfile(
        key=KRITIKOS_ELIGIBLE_HOUSEHOLD_PROFILE,
        label="Kritikos -10%",
        description=(
            "Applies an extra 10% to Kritikos listings only, on top of any existing store "
            "discounts."
        ),
        store_name="kritikos",
        multiplier=Decimal("0.90"),
    ),
}

PRICE_PROFILE_OPTIONS: tuple[tuple[str, str], ...] = (
    (DEFAULT_PRICE_PROFILE, "Standard pricing"),
    (
        KRITIKOS_ELIGIBLE_HOUSEHOLD_PROFILE,
        PRICE_PROFILES[KRITIKOS_ELIGIBLE_HOUSEHOLD_PROFILE].label,
    ),
)


def _annotated_price_field() -> DecimalField:
    return DecimalField(max_digits=12, decimal_places=4)


def parse_price_profile(raw_value: str | None) -> str:
    value = (raw_value or "").strip()
    if value in PRICE_PROFILES:
        return value
    return DEFAULT_PRICE_PROFILE


def get_price_profile(price_profile: str) -> PriceProfile | None:
    return PRICE_PROFILES.get(price_profile)


def price_profile_applies_to_store(*, store_name: str | None, price_profile: str) -> bool:
    profile = get_price_profile(price_profile)
    if profile is None:
        return False
    normalized_store_name = (store_name or "").strip().lower()
    return normalized_store_name == profile.store_name


def apply_price_profile_value(
    value: Decimal | None,
    *,
    store_name: str | None,
    price_profile: str,
) -> Decimal | None:
    if value is None:
        return None
    profile = get_price_profile(price_profile)
    if profile is None:
        return value
    normalized_store_name = (store_name or "").strip().lower()
    if normalized_store_name != profile.store_name:
        return value
    return (value * profile.multiplier).quantize(_DISPLAY_PRICE_QUANTUM, rounding=ROUND_HALF_UP)


def adjusted_price_expression(
    field_name: str,
    *,
    store_field_name: str,
    price_profile: str,
):
    profile = get_price_profile(price_profile)
    if profile is None:
        return F(field_name)
    return Case(
        When(
            **{
                store_field_name: profile.store_name,
                f"{field_name}__isnull": False,
            },
            then=ExpressionWrapper(
                F(field_name) * Value(profile.multiplier),
                output_field=_annotated_price_field(),
            ),
        ),
        default=F(field_name),
        output_field=_annotated_price_field(),
    )
