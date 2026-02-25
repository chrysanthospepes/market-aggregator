from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
from django.core.files.base import ContentFile

from catalog.models import Product
from ingestion.models import StoreListing


_IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".bmp",
    ".tiff",
}
_CONTENT_TYPE_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
}


def _image_extension_from_response(response: httpx.Response, image_url: str) -> Optional[str]:
    content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if content_type in _CONTENT_TYPE_TO_EXT:
        return _CONTENT_TYPE_TO_EXT[content_type]

    if content_type and not content_type.startswith("image/"):
        return None

    suffix = Path(urlparse(image_url).path).suffix.lower()
    if suffix in _IMAGE_EXTENSIONS:
        return suffix
    return None


def ensure_product_image_from_listing(
    *,
    product: Product,
    listing: StoreListing,
    timeout_seconds: float = 20.0,
) -> bool:
    if product.image:
        return False

    image_url = (listing.image_url or "").strip()
    if not image_url:
        return False

    try:
        response = httpx.get(image_url, timeout=timeout_seconds, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError:
        return False

    extension = _image_extension_from_response(response, image_url=image_url)
    if extension is None:
        return False

    digest = hashlib.sha1(image_url.encode("utf-8")).hexdigest()[:12]
    filename = f"product-{product.id}-{digest}{extension}"
    product.image.save(filename, ContentFile(response.content), save=False)
    product.save(update_fields=["image"])
    return True
