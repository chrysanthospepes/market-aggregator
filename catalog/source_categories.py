from __future__ import annotations


def normalize_source_category(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    normalized = normalized.strip("/")
    normalized = normalized.replace("_", "-")
    normalized = " ".join(normalized.split()).replace(" ", "-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized or None
