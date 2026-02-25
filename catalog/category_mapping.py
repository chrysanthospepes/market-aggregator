from __future__ import annotations

from functools import lru_cache
from typing import Optional

from catalog.models import Category, CategoryAlias


def normalize_source_category(value: Optional[str]) -> Optional[str]:
    normalized = (value or "").strip().lower()
    normalized = normalized.strip("/")
    normalized = normalized.replace("_", "-")
    normalized = " ".join(normalized.split()).replace(" ", "-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized or None


@lru_cache(maxsize=4096)
def resolve_category_id_for_source(*, store_id: int, source_category: str) -> Optional[int]:
    normalized = normalize_source_category(source_category)
    if not normalized:
        return None

    mapped_id = (
        CategoryAlias.objects.filter(store_id=store_id, source_slug=normalized)
        .values_list("category_id", flat=True)
        .first()
    )
    if mapped_id:
        return mapped_id

    mapped_id = (
        CategoryAlias.objects.filter(store__isnull=True, source_slug=normalized)
        .values_list("category_id", flat=True)
        .first()
    )
    if mapped_id:
        return mapped_id

    return Category.objects.filter(slug=normalized).values_list("id", flat=True).first()
