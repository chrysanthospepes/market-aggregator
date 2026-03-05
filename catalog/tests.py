import shutil
import tempfile
from decimal import Decimal
from unittest.mock import Mock, patch

from django.core.files.base import ContentFile
from django.test import TestCase

from catalog.models import Product, Store
from catalog.services.product_images import ensure_product_image_from_listing
from ingestion.models import StoreListing
from matching.normalizer import (
    build_normalized_key,
    extract_quantity,
    has_organic_marker,
    normalize_text,
)


class NormalizationTests(TestCase):
    def test_normalize_text_handles_greek_accents_and_case(self):
        self.assertEqual(normalize_text("ΦΡΕΣΚΟΎΛΗΣ ΙΤΑΛΙΚΉ"), "φρεσκουλης ιταλικη")

    def test_extract_quantity_parses_425gr(self):
        quantity = extract_quantity("Σαλάτα Ιταλική 425gr")
        self.assertIsNotNone(quantity)
        assert quantity is not None
        self.assertEqual(str(quantity.value), "425")
        self.assertEqual(quantity.unit, "g")

    def test_extract_quantity_converts_kg_to_g(self):
        quantity = extract_quantity("Σαλάτα 0.425kg")
        self.assertIsNotNone(quantity)
        assert quantity is not None
        self.assertEqual(str(quantity.value), "425")
        self.assertEqual(quantity.unit, "g")

    def test_normalize_brand_collapses_descriptor_suffixes(self):
        from matching.normalizer import normalize_brand

        self.assertEqual(normalize_brand("ΦΡΕΣΚΟΥΛΗΣ"), "freskoulis")
        self.assertEqual(normalize_brand("ΦΡΕΣΚΟΥΛΗΣ ΣΑΛΑΤΕΣ"), "freskoulis")
        self.assertEqual(normalize_brand("Freskoulis Salads"), "freskoulis")

    def test_build_normalized_key_treats_geyma_and_etoimi_as_equivalent(self):
        qty_a = extract_quantity("Φρεσκούλης Έτοιμη Σαλάτα Italian 220g")
        qty_b = extract_quantity("Φρεσκούλης Σαλάτα Γεύμα Italian 220γρ")
        key_a = build_normalized_key(
            name="Φρεσκούλης Έτοιμη Σαλάτα Italian 220g",
            brand="Φρεσκούλης",
            quantity=qty_a,
        )
        key_b = build_normalized_key(
            name="Φρεσκούλης Σαλάτα Γεύμα Italian 220γρ",
            brand="Φρεσκούλης",
            quantity=qty_b,
        )
        self.assertEqual(key_a, key_b)

    def test_build_normalized_key_treats_brand_descriptor_suffix_as_equivalent(self):
        qty_a = extract_quantity("ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g")
        qty_b = extract_quantity("Φρεσκούλης Σαλάτα Ιταλική 200γρ.")
        key_a = build_normalized_key(
            name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand="ΦΡΕΣΚΟΥΛΗΣ",
            quantity=qty_a,
        )
        key_b = build_normalized_key(
            name="Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            brand="ΦΡΕΣΚΟΥΛΗΣ ΣΑΛΑΤΕΣ",
            quantity=qty_b,
        )
        self.assertEqual(key_a, key_b)

    def test_has_organic_marker_detects_greek_and_latin_markers(self):
        self.assertTrue(has_organic_marker("Βιολογικές ντομάτες"))
        self.assertTrue(has_organic_marker("BIO tomatoes"))
        self.assertFalse(has_organic_marker("Ντομάτες"))


class ProductImageTests(TestCase):
    def setUp(self):
        self.media_root = tempfile.mkdtemp(prefix="market-aggregator-test-media-")
        self.addCleanup(lambda: shutil.rmtree(self.media_root, ignore_errors=True))
        self.store = Store.objects.create(name="test-store")

    def _create_listing(self, *, image_url: str) -> StoreListing:
        product = Product.objects.create(canonical_name="Test product")
        return StoreListing.objects.create(
            store=self.store,
            store_sku=f"sku-{image_url.split('/')[-1]}",
            store_name="Store listing",
            url="https://example.com/listing",
            image_url=image_url,
            final_price=Decimal("1.00"),
            product=product,
        )

    @patch("catalog.services.product_images.httpx.get")
    def test_downloads_image_when_product_has_none(self, mock_get):
        listing = self._create_listing(image_url="https://cdn.example.com/image.jpg")
        product = listing.product
        response = Mock()
        response.headers = {"Content-Type": "image/jpeg"}
        response.content = b"fake-image-bytes"
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        with self.settings(MEDIA_ROOT=self.media_root):
            changed = ensure_product_image_from_listing(product=product, listing=listing)

        product.refresh_from_db()
        self.assertTrue(changed)
        self.assertTrue(bool(product.image))
        self.assertTrue(product.image.name.startswith("products/product-"))
        mock_get.assert_called_once()

    @patch("catalog.services.product_images.httpx.get")
    def test_does_not_download_again_when_product_already_has_image(self, mock_get):
        listing = self._create_listing(image_url="https://cdn.example.com/image.jpg")
        product = listing.product

        with self.settings(MEDIA_ROOT=self.media_root):
            product.image.save("existing.jpg", ContentFile(b"old-image"), save=True)
            changed = ensure_product_image_from_listing(product=product, listing=listing)

        product.refresh_from_db()
        self.assertFalse(changed)
        self.assertTrue(product.image.name.endswith("existing.jpg"))
        mock_get.assert_not_called()
