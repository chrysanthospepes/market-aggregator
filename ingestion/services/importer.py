from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

from django.db import transaction
from django.utils import timezone

from catalog.category_mapping import normalize_source_category
from catalog.models import Store
from ingestion.models import CrawlerRun, PriceHistory, StoreListing


_NULLISH = {"", "null", "none", "nan", "n/a", "-"}


@dataclass
class ImportSummary:
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    deactivated: int = 0
    errored_rows: int = 0
    items_seen: int = 0
    crawler_run_id: Optional[int] = None
    matcher_processed: int = 0
    matcher_auto_matched: int = 0
    matcher_review_created: int = 0
    matcher_created_products: int = 0


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _NULLISH:
        return None
    return text


def _parse_decimal(value: Any) -> Optional[Decimal]:
    text = _clean_str(value)
    if text is None:
        return None
    normalized = text.replace("€", "").replace(" ", "")
    if "," in normalized and "." in normalized and normalized.rfind(",") > normalized.rfind("."):
        normalized = normalized.replace(".", "").replace(",", ".")
    elif "," in normalized and "." not in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _parse_bool(value: Any) -> Optional[bool]:
    text = _clean_str(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    return None


def _parse_int(value: Any) -> Optional[int]:
    text = _clean_str(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _pick(row: dict[str, Any], *keys: str) -> Any:
    lower_map = {key.lower(): value for key, value in row.items()}
    for key in keys:
        if key in row:
            return row[key]
        lowered = key.lower()
        if lowered in lower_map:
            return lower_map[lowered]
    return None


def _offer_text(row: dict[str, Any]) -> Optional[str]:
    raw_offer = _pick(row, "offer")
    if raw_offer is None:
        raw_offer = _pick(row, "promo", "promotion")

    bool_offer = _parse_bool(raw_offer)
    one_plus_one = _parse_bool(_pick(row, "one_plus_one"))
    discount_percent = _parse_int(_pick(row, "discount_percent"))

    if one_plus_one:
        return "1+1"
    if bool_offer and discount_percent is not None:
        return f"-{abs(discount_percent)}%"
    if bool_offer:
        return "offer"

    explicit_offer = _clean_str(raw_offer)
    if explicit_offer and explicit_offer.lower() not in {"true", "false"}:
        return explicit_offer[:255]
    return None


def _normalize_row(row: dict[str, Any], snapshot_at: datetime) -> Optional[dict[str, Any]]:
    store_name = _clean_str(_pick(row, "name", "store_name", "title"))
    sku = _clean_str(_pick(row, "sku", "store_sku"))
    url = _clean_str(_pick(row, "url", "product_url"))
    if not store_name:
        if sku:
            store_name = sku
        elif url:
            store_name = url
        else:
            return None

    return {
        "store_sku": sku,
        "store_name": store_name[:512],
        "store_brand": _clean_str(_pick(row, "brand", "store_brand")),
        "url": url,
        "image_url": _clean_str(_pick(row, "image_url", "image")),
        "final_price": _parse_decimal(_pick(row, "final_price", "price")),
        "final_unit_price": _parse_decimal(_pick(row, "final_unit_price", "unit_price")),
        "original_price": _parse_decimal(_pick(row, "original_price")),
        "original_unit_price": _parse_decimal(_pick(row, "original_unit_price")),
        "source_category": normalize_source_category(
            _clean_str(_pick(row, "root_category", "source_category", "category_slug", "category"))
        ),
        "unit_of_measure": _clean_str(_pick(row, "unit_of_measure", "uom")),
        "offer": _offer_text(row),
        "snapshot_at": snapshot_at,
        "last_seen_at": snapshot_at,
        "is_active": True,
    }


def _find_listing(store: Store, sku: Optional[str], url: Optional[str]) -> Optional[StoreListing]:
    listing = None
    if sku:
        listing = StoreListing.objects.filter(store=store, store_sku=sku).first()
    if listing is None and url:
        listing = StoreListing.objects.filter(store=store, url=url).first()
    return listing


def read_csv_rows(csv_path: str | Path) -> list[dict[str, Any]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def import_rows_for_store(
    *,
    store_name: str,
    rows: list[dict[str, Any]],
    snapshot_at: Optional[datetime] = None,
    run_matcher: bool = False,
    source_label: Optional[str] = None,
) -> ImportSummary:
    snapshot = snapshot_at or timezone.now()
    summary = ImportSummary()
    changed_listing_ids: set[int] = set()
    seen_ids: set[int] = set()

    store, _ = Store.objects.get_or_create(name=store_name)
    crawler_run = CrawlerRun.objects.create(
        store=store,
        status=CrawlerRun.Status.RUNNING,
        error_summary=f"source={source_label}" if source_label else "",
    )
    summary.crawler_run_id = crawler_run.id

    error_messages: list[str] = []

    try:
        with transaction.atomic():
            for row_number, raw in enumerate(rows, start=1):
                normalized = _normalize_row(raw, snapshot_at=snapshot)
                if normalized is None:
                    summary.errored_rows += 1
                    error_messages.append(f"row {row_number}: missing name/sku/url")
                    continue

                sku = normalized["store_sku"]
                url = normalized["url"]
                listing = _find_listing(store=store, sku=sku, url=url)
                if listing is None:
                    listing = StoreListing.objects.create(store=store, **normalized)
                    summary.created += 1
                    changed_listing_ids.add(listing.id)
                else:
                    updated_fields = []
                    for field, value in normalized.items():
                        if getattr(listing, field) != value:
                            setattr(listing, field, value)
                            updated_fields.append(field)
                    if updated_fields:
                        listing.save(update_fields=updated_fields)
                        summary.updated += 1
                        changed_listing_ids.add(listing.id)
                    else:
                        summary.unchanged += 1

                seen_ids.add(listing.id)

                PriceHistory.objects.get_or_create(
                    store_listing=listing,
                    captured_at=snapshot,
                    defaults={
                        "price": listing.final_price,
                        "unit_price": listing.final_unit_price,
                    },
                )

            summary.deactivated = StoreListing.objects.filter(
                store=store,
                is_active=True,
            ).exclude(id__in=seen_ids).update(is_active=False)

    except Exception as exc:
        crawler_run.status = CrawlerRun.Status.FAILED
        crawler_run.finished_at = timezone.now()
        crawler_run.error_summary = f"{crawler_run.error_summary}\n{exc}".strip()
        crawler_run.items_seen = len(seen_ids)
        crawler_run.save(
            update_fields=["status", "finished_at", "error_summary", "items_seen"],
        )
        raise

    summary.items_seen = len(seen_ids)
    crawler_run.items_seen = summary.items_seen
    crawler_run.finished_at = timezone.now()
    if summary.errored_rows and summary.items_seen:
        crawler_run.status = CrawlerRun.Status.PARTIAL
    elif summary.errored_rows and not summary.items_seen:
        crawler_run.status = CrawlerRun.Status.FAILED
    else:
        crawler_run.status = CrawlerRun.Status.SUCCESS

    if error_messages:
        details = "\n".join(error_messages[:20])
        crawler_run.error_summary = f"{crawler_run.error_summary}\n{details}".strip()

    crawler_run.save(
        update_fields=["items_seen", "finished_at", "status", "error_summary"],
    )

    if run_matcher and changed_listing_ids:
        from matching.matcher import match_store_listings

        match_summary = match_store_listings(
            listing_ids=changed_listing_ids,
            only_unmatched=False,
            include_inactive=False,
        )
        summary.matcher_processed = match_summary.processed
        summary.matcher_auto_matched = match_summary.auto_matched
        summary.matcher_review_created = match_summary.review_created
        summary.matcher_created_products = match_summary.created_products

    return summary
