from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional


_MULTISPACE_RE = re.compile(r"\s+")
_NON_WORD_RE = re.compile(r"[^\w\s]", re.UNICODE)
_QUANTITY_RE = re.compile(
    r"(?<!\d)(\d+(?:[.,]\d+)?)\s*(kg|kgr|kilo|g|gr|gram|ml|l|lt|tem|tmx|τεμ|τμχ|τεμαχιο|τεμαχια|κιλο|κιλα|κιλου|γρ|γραμ|λιτρο|λιτρα|λιτρου)\b",
    re.IGNORECASE,
)
_INLINE_QUANTITY_TOKEN_RE = re.compile(
    r"^\d+(?:[.,]\d+)?(?:kg|kgr|kilo|g|gr|gram|ml|l|lt|tem|tmx|τεμ|τμχ|τεμαχιο|τεμαχια|κιλο|κιλα|κιλου|γρ|γραμ|λιτρο|λιτρα|λιτρου)$",
    re.IGNORECASE,
)

_STOPWORDS = {
    "και",
    "σε",
    "με",
    "απο",
    "για",
    "το",
    "τη",
    "της",
    "των",
    "ο",
    "η",
    "οι",
    "τα",
    "συσκευασια",
    "ποιοτητα",
    "ελληνικα",
    "ελληνικη",
    "σαλατα",
    "price",
    "timh",
    "τιμη",
    "κιλου",
    "τελικη",
    "αρχικη",
}

_BRAND_VARIANTS = {
    "φρεσκουλης": "freskoulis",
    "φρεσκουλησ": "freskoulis",
    "freskoulis": "freskoulis",
}

_BRAND_DESCRIPTOR_TOKENS = {
    "σαλατες",
    "salads",
    "salad",
    "products",
    "προιοντα",
    "τροφιμα",
    "foods",
    "food",
}

_TOKEN_CANONICAL_MAP = {
    # Common orthographic variance in Greek product titles.
    "κλασσικη": "κλασικη",
    "κλασσικος": "κλασικος",
    "κλασσικο": "κλασικο",
    "κλασσικα": "κλασικα",
    # "έτοιμη" and "γεύμα" are frequently used interchangeably in ready-salad titles.
    "γευμα": "ετοιμη",
}

_ORGANIC_MARKERS = {
    "bio",
    "organic",
    "organics",
}


@dataclass(frozen=True)
class Quantity:
    value: Decimal
    unit: str


@dataclass(frozen=True)
class NormalizedListingText:
    normalized_name: str
    brand_normalized: Optional[str]
    quantity: Optional[Quantity]
    normalized_key: Optional[str]


def _to_decimal(raw: str) -> Optional[Decimal]:
    cleaned = raw.strip().replace(",", ".")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _normalize_quantity_text(value: str) -> str:
    lowered = strip_accents((value or "").lower().replace("\xa0", " "))
    collapsed = re.sub(r"[^\w\s\.,]", " ", lowered, flags=re.UNICODE)
    return _MULTISPACE_RE.sub(" ", collapsed).strip()


def _clean_decimal(value: Decimal) -> Decimal:
    quantized = value.quantize(Decimal("0.001"))
    if quantized == quantized.to_integral():
        return quantized.quantize(Decimal("1"))
    return quantized.normalize()


def _decimal_str(value: Decimal) -> str:
    normalized = _clean_decimal(value)
    return format(normalized, "f").rstrip("0").rstrip(".") if "." in format(normalized, "f") else format(normalized, "f")


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_text(value: str) -> str:
    lowered = strip_accents((value or "").lower().replace("\xa0", " "))
    collapsed = _NON_WORD_RE.sub(" ", lowered)
    return _MULTISPACE_RE.sub(" ", collapsed).strip()


def normalize_brand(brand: Optional[str]) -> Optional[str]:
    normalized = normalize_text(brand or "")
    if not normalized:
        return None
    direct = _BRAND_VARIANTS.get(normalized)
    if direct is not None:
        return direct

    tokens = normalized.split()
    if len(tokens) >= 2:
        head = _BRAND_VARIANTS.get(tokens[0], tokens[0])
        tail = [_BRAND_VARIANTS.get(token, token) for token in tokens[1:]]
        if all(token in _BRAND_DESCRIPTOR_TOKENS for token in tail):
            return head

    return normalized


def has_organic_marker(value: str) -> bool:
    normalized = normalize_text(value or "")
    if not normalized:
        return False
    for token in normalized.split():
        if token in _ORGANIC_MARKERS:
            return True
        if token == "βιο":
            return True
        if token.startswith("βιολογ"):
            return True
    return False


def _normalize_quantity(value: Decimal, unit_token: str) -> Optional[Quantity]:
    token = normalize_text(unit_token)
    if token in {"kg", "kgr", "kilo", "κιλο", "κιλα", "κιλου"}:
        return Quantity(value=_clean_decimal(value * Decimal("1000")), unit="g")
    if token in {"g", "gr", "gram", "γρ", "γραμ"}:
        return Quantity(value=_clean_decimal(value), unit="g")
    if token in {"l", "lt", "λιτρο", "λιτρα", "λιτρου"}:
        return Quantity(value=_clean_decimal(value * Decimal("1000")), unit="ml")
    if token in {"ml"}:
        return Quantity(value=_clean_decimal(value), unit="ml")
    if token in {"tem", "tmx", "τεμ", "τμχ", "τεμαχιο", "τεμαχια"}:
        return Quantity(value=_clean_decimal(value), unit="temaxio")
    return None


def extract_quantity(value: str) -> Optional[Quantity]:
    normalized = _normalize_quantity_text(value or "")
    match = _QUANTITY_RE.search(normalized)
    if not match:
        return None
    amount_raw, unit_raw = match.groups()
    amount = _to_decimal(amount_raw)
    if amount is None or amount <= 0:
        return None
    return _normalize_quantity(amount, unit_raw)


def tokenize_name(value: str) -> list[str]:
    normalized = normalize_text(value or "")
    tokens = []
    for token in normalized.split():
        token = _TOKEN_CANONICAL_MAP.get(token, token)
        if token in _STOPWORDS:
            continue
        if token.isnumeric():
            continue
        if _INLINE_QUANTITY_TOKEN_RE.match(token):
            continue
        if token in {"g", "gr", "kg", "ml", "l", "lt", "τεμ", "tem", "tmx"}:
            continue
        tokens.append(token)
    return tokens


def build_normalized_key(
    name: str,
    brand: Optional[str],
    quantity: Optional[Quantity],
) -> Optional[str]:
    tokens = tokenize_name(name)
    if not tokens:
        return None
    brand_part = normalize_brand(brand) or "no-brand"
    quantity_part = "no-qty"
    if quantity is not None:
        quantity_part = f"{_decimal_str(quantity.value)}{quantity.unit}"
    key = f"{brand_part}|{quantity_part}|{' '.join(tokens[:10])}"
    return key[:255]


def normalize_listing_text(name: str, brand: Optional[str]) -> NormalizedListingText:
    normalized_name = normalize_text(name)
    quantity = extract_quantity(name)
    brand_normalized = normalize_brand(brand)
    normalized_key = build_normalized_key(name=name, brand=brand, quantity=quantity)
    return NormalizedListingText(
        normalized_name=normalized_name,
        brand_normalized=brand_normalized,
        quantity=quantity,
        normalized_key=normalized_key,
    )
