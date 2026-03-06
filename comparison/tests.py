from django.test import TestCase

from decimal import Decimal

from django.contrib.admin.sites import AdminSite
from django.test import RequestFactory
from django.urls import reverse
from catalog.models import Category, CategoryAlias, Product, Store
from comparison.admin import MatchReviewAdmin
from comparison.models import MatchReview
from ingestion.models import StoreListing
from matching.matcher import match_store_listings


class MatcherTests(TestCase):
    def setUp(self):
        self.sklavenitis = Store.objects.create(name="sklavenitis")
        self.mymarket = Store.objects.create(name="mymarket")

    def test_positive_match_for_known_pair(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        product = Product.objects.create(
            canonical_name="Φρεσκούλης ιταλική σαλάτα 425gr",
            brand_normalized="freskoulis",
            quantity_value=Decimal("425"),
            quantity_unit="g",
            normalized_key="freskoulis|425g|φρεσκουλης ιταλικη",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-58",
            store_name="ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 425gr",
            store_brand="Φρεσκούλης",
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/58",
            final_price=Decimal("2.10"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(listing.product_id, product.id)

    def test_brandless_variant_name_pair_auto_matches_same_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        product = Product.objects.create(
            canonical_name="Μπαρμπα Στάθης Σαλάτα Κλασσική 250gr",
            brand_normalized=None,
            quantity_value=Decimal("250"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-salad",
            store_name="Σαλάτα Κλασική ΜΠΑΡΜΠΑ ΣΤΑΘΗΣ Μαρούλι Φρέσκο Κρεμμυδάκι & Άνηθος 250g",
            store_brand=None,
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/salad",
            final_price=Decimal("2.20"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(summary.review_created, 0)
        self.assertEqual(summary.created_products, 0)
        self.assertEqual(listing.product_id, product.id)

    def test_no_brand_no_quantity_strong_name_auto_matches(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        product = Product.objects.create(
            canonical_name="Μήλα Gala Εισαγωγής",
            brand_normalized=None,
            quantity_value=None,
            quantity_unit=None,
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-apples",
            store_name="Μήλα Gala εισαγωγής",
            store_brand=None,
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/apples",
            final_price=Decimal("2.50"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(summary.review_created, 0)
        self.assertEqual(summary.created_products, 0)
        self.assertEqual(listing.product_id, product.id)

    def test_no_brand_no_quantity_short_generic_name_does_not_auto_match(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        existing = Product.objects.create(
            canonical_name="Μήλα",
            brand_normalized=None,
            quantity_value=None,
            quantity_unit=None,
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-apples-generic",
            store_name="Μήλα Κόκκινα",
            store_brand=None,
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/apples-generic",
            final_price=Decimal("2.10"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 0)
        self.assertEqual(summary.created_products, 1)
        self.assertNotEqual(listing.product_id, existing.id)

    def test_unmatched_listing_does_not_match_product_already_used_by_same_store(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        existing_product = Product.objects.create(
            canonical_name="Μήλα Gala Εισαγωγής",
            brand_normalized=None,
            quantity_value=None,
            quantity_unit=None,
            category=fruits,
        )
        StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-existing",
            store_name="Μήλα Gala Εισαγωγής",
            store_brand=None,
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/existing",
            final_price=Decimal("2.40"),
            product=existing_product,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-new",
            store_name="Μήλα Gala Εισαγωγής",
            store_brand=None,
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/new",
            final_price=Decimal("2.50"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 0)
        self.assertEqual(summary.review_created, 0)
        self.assertEqual(summary.created_products, 1)
        self.assertIsNotNone(listing.product_id)
        self.assertNotEqual(listing.product_id, existing_product.id)

    def test_similar_name_but_different_pack_size_creates_new_product(self):
        existing = Product.objects.create(
            canonical_name="Φρεσκούλης ιταλική σαλάτα 425gr",
            brand_normalized="freskoulis",
            quantity_value=Decimal("425"),
            quantity_unit="g",
            normalized_key="freskoulis|425g|φρεσκουλης ιταλικη",
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="sku-34",
            store_name="ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 1kg",
            store_brand="Φρεσκούλης",
            url="https://example.com/mymarket/34",
            final_price=Decimal("3.40"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.created_products, 1)
        self.assertNotEqual(listing.product_id, existing.id)

    def test_category_mapping_is_used_for_new_products_and_matching_scope(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        drinks = Category.objects.create(name="Ποτά", slug="pota")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        existing_wrong_category = Product.objects.create(
            canonical_name="Φρεσκούλης ιταλική σαλάτα 425gr",
            brand_normalized="freskoulis",
            quantity_value=Decimal("425"),
            quantity_unit="g",
            normalized_key="freskoulis|425g|φρεσκουλης ιταλικη",
            category=drinks,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-cat",
            store_name="ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 425gr",
            store_brand="Φρεσκούλης",
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/cat",
            final_price=Decimal("2.30"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.created_products, 1)
        self.assertNotEqual(listing.product_id, existing_wrong_category.id)
        self.assertEqual(listing.product.category_id, fruits.id)

    def test_near_miss_goes_to_review_instead_of_silent_new_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        product = Product.objects.create(
            canonical_name="Φρεσκούλης ιταλική σαλάτα 425gr",
            brand_normalized="freskoulis",
            quantity_value=Decimal("425"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-near",
            store_name="ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 500gr",
            store_brand="Φρεσκούλης",
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/near",
            final_price=Decimal("2.30"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()
        review = MatchReview.objects.filter(store_listing=listing, candidate_product=product).first()

        self.assertEqual(summary.review_created, 1)
        self.assertIsNone(listing.product_id)
        self.assertIsNotNone(review)
        assert review is not None
        self.assertEqual(review.status, MatchReview.Status.PENDING)

    def test_unresolved_source_category_does_not_create_review_noise(self):
        product = Product.objects.create(
            canonical_name="Αγγούρι 1τεμ",
            brand_normalized=None,
            quantity_value=Decimal("1"),
            quantity_unit="temaxio",
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="sku-unresolved",
            store_name="Αγγούρι 1τεμ",
            source_category="unknown-category-slug",
            url="https://example.com/mymarket/unresolved",
            final_price=Decimal("0.90"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()
        review = MatchReview.objects.filter(store_listing=listing).first()

        self.assertEqual(summary.review_created, 0)
        self.assertIsNone(review)
        self.assertEqual(summary.created_products, 1)
        self.assertIsNotNone(listing.product_id)
        self.assertNotEqual(listing.product_id, product.id)

    def test_brand_and_quantity_only_overlap_does_not_auto_match_wrong_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        bazaar = Store.objects.create(name="bazaar")
        CategoryAlias.objects.create(
            store=bazaar,
            source_slug="froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        wrong_existing_product = Product.objects.create(
            canonical_name="Ανανάς ΦΡΕΣΚΟΥΛΗΣ 220g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("220"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=bazaar,
            store_sku="baz-italian-220",
            store_name="Φρεσκούλης Έτοιμη Σαλάτα Italian 220g",
            store_brand="Φρεσκούλης",
            source_category="froyta-lachanika",
            url="https://example.com/bazaar/italian-220",
            final_price=Decimal("2.80"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 0)
        self.assertIsNotNone(listing.product_id)
        self.assertNotEqual(listing.product_id, wrong_existing_product.id)

    def test_similar_meal_title_auto_matches_with_shared_tokens(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        bazaar = Store.objects.create(name="bazaar")
        kritikos = Store.objects.create(name="kritikos")
        CategoryAlias.objects.create(
            store=bazaar,
            source_slug="froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=kritikos,
            source_slug="manabikh",
            category=fruits,
        )
        product = Product.objects.create(
            canonical_name="Φρεσκούλης Έτοιμη Σαλάτα Italian 220g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("220"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=kritikos,
            store_sku="krit-italian-meal-220",
            store_name="ΦΡΕΣΚΟΥΛΗΣ Γεύμα Italian Τυρί & Τοματίνια 220g",
            store_brand="Φρεσκούλης",
            source_category="manabikh",
            url="https://example.com/kritikos/italian-meal-220",
            final_price=Decimal("2.90"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(listing.product_id, product.id)

    def test_include_matched_keeps_current_product_without_reconsider_flag(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        target_product = Product.objects.create(
            canonical_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-target-200",
            store_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            store_brand="Φρεσκούλης",
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/italian-200",
            final_price=Decimal("2.30"),
            product=target_product,
        )
        wrong_product = Product.objects.create(
            canonical_name="Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="sku-current-200",
            store_name="Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            store_brand="Φρεσκούλης",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/italian-200",
            final_price=Decimal("2.25"),
            product=wrong_product,
        )

        match_store_listings(
            listing_ids=[listing.id],
            only_unmatched=False,
        )
        listing.refresh_from_db()

        self.assertEqual(listing.product_id, wrong_product.id)

    def test_reconsider_matched_can_reassign_listing_to_better_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        target_product = Product.objects.create(
            canonical_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-target-200",
            store_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            store_brand="Φρεσκούλης",
            source_category="freska-froyta-lachanika",
            url="https://example.com/sklavenitis/italian-200",
            final_price=Decimal("2.30"),
            product=target_product,
        )
        wrong_product = Product.objects.create(
            canonical_name="Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="sku-current-200",
            store_name="Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            store_brand="Φρεσκούλης",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/italian-200",
            final_price=Decimal("2.25"),
            product=wrong_product,
        )

        match_store_listings(
            listing_ids=[listing.id],
            only_unmatched=False,
            reconsider_matched=True,
        )
        listing.refresh_from_db()

        self.assertEqual(listing.product_id, target_product.id)

    def test_brand_descriptor_suffix_does_not_split_same_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        product = Product.objects.create(
            canonical_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="mm-italian-200",
            store_name="Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            store_brand="ΦΡΕΣΚΟΥΛΗΣ ΣΑΛΑΤΕΣ",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/italian-200",
            final_price=Decimal("2.20"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(listing.product_id, product.id)

    def test_non_bio_listing_does_not_auto_match_bio_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        bio_product = Product.objects.create(
            canonical_name="BIO ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="mm-italian-200-nonbio",
            store_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            store_brand="Φρεσκούλης",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/italian-200-nonbio",
            final_price=Decimal("2.20"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 0)
        self.assertIsNotNone(listing.product_id)
        self.assertNotEqual(listing.product_id, bio_product.id)

    def test_bio_listing_does_not_auto_match_non_bio_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        non_bio_product = Product.objects.create(
            canonical_name="ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="mm-italian-200-bio",
            store_name="BIO ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            store_brand="Φρεσκούλης",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/italian-200-bio",
            final_price=Decimal("2.20"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 0)
        self.assertIsNotNone(listing.product_id)
        self.assertNotEqual(listing.product_id, non_bio_product.id)

    def test_bio_listing_auto_matches_bio_product(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        bio_product = Product.objects.create(
            canonical_name="Βιολογική ΦΡΕΣΚΟΥΛΗΣ Σαλάτα Ιταλική 200g",
            brand_normalized="freskoulis",
            quantity_value=Decimal("200"),
            quantity_unit="g",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="mm-italian-200-bio-match",
            store_name="BIO Φρεσκούλης Σαλάτα Ιταλική 200γρ.",
            store_brand="Φρεσκούλης",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/italian-200-bio-match",
            final_price=Decimal("2.20"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(listing.product_id, bio_product.id)

    def test_contradictory_tokens_block_auto_match_even_with_same_brand_and_quantity(self):
        fruits = Category.objects.create(name="Φρούτα & Λαχανικά", slug="frouta-lachanika")
        CategoryAlias.objects.create(
            store=self.sklavenitis,
            source_slug="freska-froyta-lachanika",
            category=fruits,
        )
        CategoryAlias.objects.create(
            store=self.mymarket,
            source_slug="frouta-lachanika",
            category=fruits,
        )
        existing = Product.objects.create(
            canonical_name="Coca Cola Zero 330ml",
            brand_normalized="coca cola",
            quantity_value=Decimal("330"),
            quantity_unit="ml",
            category=fruits,
        )
        listing = StoreListing.objects.create(
            store=self.mymarket,
            store_sku="mm-coke-light-330",
            store_name="Coca Cola Light 330ml",
            store_brand="Coca Cola",
            source_category="frouta-lachanika",
            url="https://example.com/mymarket/coke-light-330",
            final_price=Decimal("1.10"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()
        review = MatchReview.objects.filter(store_listing=listing, candidate_product=existing).first()

        self.assertEqual(summary.auto_matched, 0)
        self.assertEqual(summary.review_created, 1)
        self.assertIsNone(listing.product_id)
        self.assertIsNotNone(review)


class ComparisonApiTests(TestCase):
    def test_product_offers_endpoint_returns_active_store_offers(self):
        store_a = Store.objects.create(name="sklavenitis")
        store_b = Store.objects.create(name="mymarket")
        product = Product.objects.create(canonical_name="Ντομάτα 1kg")

        StoreListing.objects.create(
            store=store_a,
            store_sku="sku-a",
            store_name="Ντομάτα 1kg",
            source_category="frouta-lachanika",
            url="https://example.com/a",
            final_price=Decimal("1.10"),
            product=product,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store_b,
            store_sku="sku-b",
            store_name="Ντομάτα 1kg",
            source_category="frouta-lachanika",
            url="https://example.com/b",
            final_price=Decimal("1.20"),
            product=product,
            is_active=False,
        )

        response = self.client.get(f"/api/products/{product.id}/offers")
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertEqual(payload["product"]["id"], product.id)
        self.assertEqual(len(payload["offers"]), 1)
        self.assertEqual(payload["offers"][0]["store"], "sklavenitis")


class ComparisonHtmlViewsTests(TestCase):
    def _create_product_with_listing(
        self,
        *,
        store: Store,
        name: str,
        sku: str,
        final_price: str = "1.00",
        final_unit_price: str | None = None,
    ) -> Product:
        product = Product.objects.create(canonical_name=name)
        listing_kwargs = {
            "store": store,
            "store_sku": sku,
            "store_name": name,
            "url": f"https://example.com/{sku}",
            "final_price": Decimal(final_price),
            "product": product,
            "is_active": True,
        }
        if final_unit_price is not None:
            listing_kwargs["final_unit_price"] = Decimal(final_unit_price)
        StoreListing.objects.create(**listing_kwargs)
        return product

    def test_product_list_shows_products_with_active_listings_only(self):
        store = Store.objects.create(name="sklavenitis")
        visible_product = Product.objects.create(canonical_name="Ντομάτα 1kg")
        hidden_product = Product.objects.create(canonical_name="Μήλο 1kg")

        StoreListing.objects.create(
            store=store,
            store_sku="sku-visible",
            store_name="Ντομάτα 1kg",
            url="https://example.com/visible",
            final_price=Decimal("1.00"),
            product=visible_product,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store,
            store_sku="sku-hidden",
            store_name="Μήλο 1kg",
            url="https://example.com/hidden",
            final_price=Decimal("1.20"),
            product=hidden_product,
            is_active=False,
        )

        response = self.client.get(reverse("product-list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ντομάτα 1kg")
        self.assertNotContains(response, "Μήλο 1kg")
        self.assertContains(response, reverse("product-detail", args=[visible_product.id]))

    def test_product_detail_shows_linked_active_listings(self):
        store_a = Store.objects.create(name="sklavenitis")
        store_b = Store.objects.create(name="mymarket")
        product = Product.objects.create(canonical_name="Αγγούρι 1τεμ")

        StoreListing.objects.create(
            store=store_a,
            store_sku="sku-a",
            store_name="Αγγούρι Σκλαβενίτης",
            url="https://example.com/a",
            final_price=Decimal("0.90"),
            product=product,
            is_active=True,
            offer="offer",
        )
        StoreListing.objects.create(
            store=store_b,
            store_sku="sku-b",
            store_name="Αγγούρι Mymarket",
            url="https://example.com/b",
            final_price=Decimal("0.85"),
            product=product,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store_b,
            store_sku="sku-c",
            store_name="Αγγούρι inactive",
            url="https://example.com/c",
            final_price=Decimal("0.70"),
            product=product,
            is_active=False,
        )

        response = self.client.get(reverse("product-detail", args=[product.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Αγγούρι Σκλαβενίτης")
        self.assertContains(response, "Αγγούρι Mymarket")
        self.assertNotContains(response, "Αγγούρι inactive")
        self.assertContains(response, "Back to products")

    def test_product_detail_unit_price_shows_unit_suffix(self):
        store = Store.objects.create(name="sklavenitis")
        product = Product.objects.create(canonical_name="Milk 1L")
        StoreListing.objects.create(
            store=store,
            store_sku="milk-1l",
            store_name="Milk 1L",
            url="https://example.com/milk-1l",
            final_price=Decimal("1.20"),
            final_unit_price=Decimal("1.20"),
            unit_of_measure="liters",
            product=product,
            is_active=True,
        )

        response = self.client.get(reverse("product-detail", args=[product.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "1.20€/λίτρο")

    def test_product_list_can_sort_by_lowest_final_unit_price(self):
        store = Store.objects.create(name="sklavenitis")
        low = Product.objects.create(canonical_name="Low unit price")
        high = Product.objects.create(canonical_name="High unit price")
        no_unit = Product.objects.create(canonical_name="No unit price")

        StoreListing.objects.create(
            store=store,
            store_sku="low",
            store_name="Low listing",
            url="https://example.com/low",
            final_price=Decimal("2.50"),
            final_unit_price=Decimal("1.5000"),
            product=low,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store,
            store_sku="high",
            store_name="High listing",
            url="https://example.com/high",
            final_price=Decimal("2.80"),
            final_unit_price=Decimal("3.2000"),
            product=high,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store,
            store_sku="none",
            store_name="None listing",
            url="https://example.com/none",
            final_price=Decimal("1.20"),
            product=no_unit,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"), {"sort": "unit_price_asc"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "1.50")
        self.assertContains(response, "3.20")
        content = response.content.decode("utf-8")
        self.assertLess(content.index("Low unit price"), content.index("High unit price"))
        self.assertLess(content.index("High unit price"), content.index("No unit price"))

    def test_product_list_price_sort_uses_hidden_price_for_ordering_only(self):
        store = Store.objects.create(name="sklavenitis")
        low_visible_high_hidden = Product.objects.create(canonical_name="Low visible high hidden")
        high_visible_low_hidden = Product.objects.create(canonical_name="High visible low hidden")

        StoreListing.objects.create(
            store=store,
            store_sku="visible-low",
            store_name="visible-low",
            url="https://example.com/visible-low",
            final_price=Decimal("1.00"),
            hidden_price=Decimal("5.00"),
            product=low_visible_high_hidden,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store,
            store_sku="visible-high",
            store_name="visible-high",
            url="https://example.com/visible-high",
            final_price=Decimal("4.00"),
            hidden_price=Decimal("0.90"),
            product=high_visible_low_hidden,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"), {"sort": "price_asc"})

        self.assertEqual(response.status_code, 200)
        ordered_names = [product.canonical_name for product in response.context["products"]]
        self.assertEqual(
            ordered_names,
            ["High visible low hidden", "Low visible high hidden"],
        )
        self.assertContains(response, "1.00€")
        self.assertContains(response, "4.00€")

    def test_product_list_unit_price_sort_uses_hidden_unit_price_for_ordering_only(self):
        store = Store.objects.create(name="sklavenitis")
        low_visible_high_hidden = Product.objects.create(canonical_name="Low unit visible high hidden")
        high_visible_low_hidden = Product.objects.create(canonical_name="High unit visible low hidden")

        StoreListing.objects.create(
            store=store,
            store_sku="unit-visible-low",
            store_name="unit-visible-low",
            url="https://example.com/unit-visible-low",
            final_price=Decimal("1.00"),
            final_unit_price=Decimal("1.00"),
            hidden_unit_price=Decimal("6.00"),
            product=low_visible_high_hidden,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store,
            store_sku="unit-visible-high",
            store_name="unit-visible-high",
            url="https://example.com/unit-visible-high",
            final_price=Decimal("1.20"),
            final_unit_price=Decimal("3.00"),
            hidden_unit_price=Decimal("0.80"),
            product=high_visible_low_hidden,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"), {"sort": "unit_price_asc"})

        self.assertEqual(response.status_code, 200)
        ordered_names = [product.canonical_name for product in response.context["products"]]
        self.assertEqual(
            ordered_names,
            ["High unit visible low hidden", "Low unit visible high hidden"],
        )
        self.assertContains(response, "1.00€/τεμάχιο")
        self.assertContains(response, "3.00€/τεμάχιο")

    def test_product_list_can_sort_by_declining_price(self):
        store = Store.objects.create(name="sklavenitis")
        low = Product.objects.create(canonical_name="Low price")
        high = Product.objects.create(canonical_name="High price")

        StoreListing.objects.create(
            store=store,
            store_sku="low-price",
            store_name="Low listing",
            url="https://example.com/low-price",
            final_price=Decimal("1.10"),
            final_unit_price=Decimal("1.10"),
            product=low,
            is_active=True,
        )
        StoreListing.objects.create(
            store=store,
            store_sku="high-price",
            store_name="High listing",
            url="https://example.com/high-price",
            final_price=Decimal("3.25"),
            final_unit_price=Decimal("3.25"),
            product=high,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"), {"sort": "price_desc"})

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertLess(content.index("High price"), content.index("Low price"))

    def test_product_list_can_sort_by_declining_discount_with_offer_priority(self):
        store = Store.objects.create(name="sklavenitis")

        def create_listing(name: str, sku: str, **listing_kwargs):
            product = Product.objects.create(canonical_name=name)
            StoreListing.objects.create(
                store=store,
                store_sku=sku,
                store_name=name,
                url=f"https://example.com/{sku}",
                final_price=Decimal("1.00"),
                final_unit_price=Decimal("1.00"),
                product=product,
                is_active=True,
                **listing_kwargs,
            )

        create_listing("Discount 20", "disc-20", offer="-20%", discount_percent=20)
        create_listing("Discount 45", "disc-45", offer="-45%", discount_percent=45)
        create_listing("One plus one", "offer-1p1", offer="1+1", one_plus_one=True)
        create_listing("Two plus one", "offer-2p1", offer="2+1", two_plus_one=True)
        create_listing("No offer", "offer-none", offer=None)

        response = self.client.get(reverse("product-list"), {"sort": "discount_desc"})

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertLess(content.index("Discount 45"), content.index("Discount 20"))
        self.assertLess(content.index("Discount 20"), content.index("One plus one"))
        self.assertLess(content.index("One plus one"), content.index("Two plus one"))
        self.assertLess(content.index("Two plus one"), content.index("No offer"))

    def test_product_list_paginates_to_twenty_items(self):
        store = Store.objects.create(name="sklavenitis")
        for i in range(1, 22):
            self._create_product_with_listing(
                store=store,
                name=f"Product {i:03d}",
                sku=f"sku-{i:03d}",
            )

        response = self.client.get(reverse("product-list"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["products"]), 20)
        self.assertEqual(response.context["page_obj"].number, 1)
        self.assertTrue(response.context["page_obj"].has_next())
        self.assertContains(response, "Page 1 of 2")
        self.assertContains(response, "Product 001")
        self.assertContains(response, "Product 020")
        self.assertNotContains(response, "Product 021")

    def test_product_list_second_page_shows_remaining_items(self):
        store = Store.objects.create(name="sklavenitis")
        for i in range(1, 22):
            self._create_product_with_listing(
                store=store,
                name=f"Product {i:03d}",
                sku=f"sku-{i:03d}",
            )

        response = self.client.get(reverse("product-list"), {"page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["products"]), 1)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertTrue(response.context["page_obj"].has_previous())
        self.assertContains(response, "Page 2 of 2")
        self.assertContains(response, "Product 021")
        self.assertNotContains(response, "Product 020")

    def test_product_list_can_filter_by_selected_store(self):
        store_a = Store.objects.create(name="ab")
        store_b = Store.objects.create(name="bazaar")

        self._create_product_with_listing(
            store=store_a,
            name="Only AB",
            sku="ab-only",
        )
        self._create_product_with_listing(
            store=store_b,
            name="Only Bazaar",
            sku="bazaar-only",
        )
        product_both = self._create_product_with_listing(
            store=store_a,
            name="AB and Bazaar",
            sku="both-ab",
        )
        StoreListing.objects.create(
            store=store_b,
            store_sku="both-bazaar",
            store_name="AB and Bazaar",
            url="https://example.com/both-bazaar",
            final_price=Decimal("1.10"),
            product=product_both,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"), {"stores": [store_a.id]})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Only AB")
        self.assertContains(response, "AB and Bazaar")
        self.assertNotContains(response, "Only Bazaar")
        self.assertEqual(set(response.context["selected_store_ids"]), {store_a.id})
        filtered_both = next(p for p in response.context["products"] if p.id == product_both.id)
        self.assertEqual(filtered_both.active_listing_count, 2)
        self.assertContains(response, "2 stores")
        self.assertContains(response, f'value="{store_a.id}"')
        self.assertEqual(response.context["sort"], "price_asc")
        self.assertIn(f"&stores={store_a.id}", response.context["selected_filters_query"])

    def test_selected_stores_drive_product_card_prices_only(self):
        sklavenitis = Store.objects.create(name="sklavenitis")
        kritikos = Store.objects.create(name="kritikos")
        mymarket = Store.objects.create(name="mymarket")
        product = Product.objects.create(canonical_name="Shared product")

        StoreListing.objects.create(
            store=sklavenitis,
            store_sku="shared-skl",
            store_name="Shared product Sklavenitis",
            url="https://example.com/shared-skl",
            final_price=Decimal("2.00"),
            final_unit_price=Decimal("2.00"),
            product=product,
            is_active=True,
        )
        StoreListing.objects.create(
            store=kritikos,
            store_sku="shared-kri",
            store_name="Shared product Kritikos",
            url="https://example.com/shared-kri",
            final_price=Decimal("1.00"),
            final_unit_price=Decimal("1.00"),
            product=product,
            is_active=True,
        )
        StoreListing.objects.create(
            store=mymarket,
            store_sku="shared-my",
            store_name="Shared product Mymarket",
            url="https://example.com/shared-my",
            final_price=Decimal("0.80"),
            final_unit_price=Decimal("0.80"),
            product=product,
            is_active=True,
        )

        response_all = self.client.get(reverse("product-list"))
        product_all = next(p for p in response_all.context["products"] if p.id == product.id)
        self.assertEqual(product_all.cheapest_final_unit_price, Decimal("0.80"))
        self.assertEqual(product_all.cheapest_final_price, Decimal("0.80"))

        response_skl_only = self.client.get(reverse("product-list"), {"stores": [sklavenitis.id]})
        product_skl = next(p for p in response_skl_only.context["products"] if p.id == product.id)
        self.assertEqual(product_skl.cheapest_final_unit_price, Decimal("2.00"))
        self.assertEqual(product_skl.cheapest_final_price, Decimal("2.00"))
        self.assertEqual(product_skl.active_listing_count, 3)

        response_two = self.client.get(
            reverse("product-list"),
            {"stores": [sklavenitis.id, kritikos.id]},
        )
        product_two = next(p for p in response_two.context["products"] if p.id == product.id)
        self.assertEqual(product_two.cheapest_final_unit_price, Decimal("1.00"))
        self.assertEqual(product_two.cheapest_final_price, Decimal("1.00"))
        self.assertEqual(product_two.active_listing_count, 3)

    def test_offer_filter_options_respect_selected_store_scope(self):
        store_a = Store.objects.create(name="ab")
        store_b = Store.objects.create(name="bazaar")

        product_no_offer = Product.objects.create(canonical_name="No offer product")
        StoreListing.objects.create(
            store=store_a,
            store_sku="no-offer-a",
            store_name="No offer product",
            url="https://example.com/no-offer-a",
            final_price=Decimal("1.00"),
            final_unit_price=Decimal("1.00"),
            offer=None,
            product=product_no_offer,
            is_active=True,
        )

        product_two_plus_one = Product.objects.create(canonical_name="2+1 product")
        StoreListing.objects.create(
            store=store_b,
            store_sku="two-plus-one-b",
            store_name="2+1 product",
            url="https://example.com/two-plus-one-b",
            final_price=Decimal("2.00"),
            final_unit_price=Decimal("2.00"),
            offer="2+1",
            two_plus_one=True,
            product=product_two_plus_one,
            is_active=True,
        )

        response_all = self.client.get(reverse("product-list"))
        self.assertContains(response_all, "2 + 1")

        response_store_a = self.client.get(reverse("product-list"), {"stores": [store_a.id]})
        self.assertContains(response_store_a, "No offer")
        self.assertNotContains(response_store_a, "2 + 1")

    def test_offer_filter_applies_selected_bucket(self):
        store = Store.objects.create(name="sklavenitis")

        def create_listing(name: str, sku: str, **listing_kwargs):
            product = Product.objects.create(canonical_name=name)
            StoreListing.objects.create(
                store=store,
                store_sku=sku,
                store_name=name,
                url=f"https://example.com/{sku}",
                final_price=Decimal("1.00"),
                final_unit_price=Decimal("1.00"),
                product=product,
                is_active=True,
                **listing_kwargs,
            )
            return product

        create_listing("No offer", "offer-none", offer=None)
        create_listing("Discount 15", "offer-15", offer="-15%", discount_percent=15)
        create_listing("Discount 30", "offer-30", offer="-30%", discount_percent=30)
        create_listing("Discount 50", "offer-50", offer="-50%", discount_percent=50)
        create_listing("One plus one", "offer-1p1", offer="1+1", one_plus_one=True)
        create_listing("Two plus one", "offer-2p1", offer="2+1", two_plus_one=True)

        response_21_40 = self.client.get(reverse("product-list"), {"offer_filter": "discount_21_40"})
        self.assertContains(response_21_40, "Discount 30")
        self.assertNotContains(response_21_40, "Discount 15")
        self.assertNotContains(response_21_40, "Discount 50")

        response_no_offer = self.client.get(reverse("product-list"), {"offer_filter": "no_offer"})
        self.assertContains(response_no_offer, "No offer")
        self.assertNotContains(response_no_offer, "Discount 30")
        self.assertNotContains(response_no_offer, "One plus one")

        response_2p1 = self.client.get(reverse("product-list"), {"offer_filter": "two_plus_one"})
        self.assertContains(response_2p1, "Two plus one")
        self.assertNotContains(response_2p1, "One plus one")

    def test_offer_filter_supports_multiple_selected_buckets(self):
        store = Store.objects.create(name="sklavenitis")

        def create_listing(name: str, sku: str, **listing_kwargs):
            product = Product.objects.create(canonical_name=name)
            StoreListing.objects.create(
                store=store,
                store_sku=sku,
                store_name=name,
                url=f"https://example.com/{sku}",
                final_price=Decimal("1.00"),
                final_unit_price=Decimal("1.00"),
                product=product,
                is_active=True,
                **listing_kwargs,
            )

        create_listing("Discount 30", "offer-30", offer="-30%", discount_percent=30)
        create_listing("Two plus one", "offer-2p1", offer="2+1", two_plus_one=True)
        create_listing("No offer", "offer-none", offer=None)

        response = self.client.get(
            reverse("product-list"),
            {"offer_filter": ["discount_21_40", "two_plus_one"]},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Discount 30")
        self.assertContains(response, "Two plus one")
        self.assertEqual(
            {product.canonical_name for product in response.context["products"]},
            {"Discount 30", "Two plus one"},
        )
        self.assertEqual(
            set(response.context["selected_offer_filters"]),
            {"discount_21_40", "two_plus_one"},
        )
        self.assertContains(response, 'value="discount_21_40"')
        self.assertContains(response, 'value="two_plus_one"')

    def test_product_list_card_shows_price_and_struck_original_values(self):
        store = Store.objects.create(name="sklavenitis")
        product = Product.objects.create(canonical_name="Card product")
        StoreListing.objects.create(
            store=store,
            store_sku="sku-card",
            store_name="Card listing",
            url="https://example.com/card",
            final_price=Decimal("1.50"),
            original_price=Decimal("2.10"),
            final_unit_price=Decimal("1.2345"),
            original_unit_price=Decimal("1.9000"),
            product=product,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Card product")
        self.assertContains(response, "<s>2.10€</s>", html=True)
        self.assertContains(response, "1.23€/τεμάχιο")
        self.assertContains(response, "<s>1.90€/τεμάχιο</s>", html=True)
        self.assertContains(response, "1 store")

    def test_product_list_card_shows_store_and_sale_badges_on_image(self):
        store = Store.objects.create(name="kritikos")
        product = Product.objects.create(canonical_name="Badge product")
        StoreListing.objects.create(
            store=store,
            store_sku="badge-product",
            store_name="Badge product",
            url="https://example.com/badge-product",
            final_price=Decimal("1.50"),
            final_unit_price=Decimal("1.50"),
            discount_percent=20,
            offer="-20%",
            product=product,
            is_active=True,
        )

        response = self.client.get(reverse("product-list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/media/stores/kritikos.png")
        self.assertContains(response, "/media/discounts/discount-020.svg")


class MatchReviewAdminTests(TestCase):
    def test_approve_action_links_listing_and_rejects_other_pending(self):
        store = Store.objects.create(name="sklavenitis")
        listing = StoreListing.objects.create(
            store=store,
            store_sku="sku-review",
            store_name="Προϊόν δοκιμή 500g",
            source_category="frouta-lachanika",
            url="https://example.com/review",
            final_price=Decimal("1.00"),
        )
        candidate_a = Product.objects.create(canonical_name="Product A")
        candidate_b = Product.objects.create(canonical_name="Product B")

        review_a = MatchReview.objects.create(
            store_listing=listing,
            candidate_product=candidate_a,
            score=Decimal("0.9100"),
            status=MatchReview.Status.PENDING,
        )
        review_b = MatchReview.objects.create(
            store_listing=listing,
            candidate_product=candidate_b,
            score=Decimal("0.8900"),
            status=MatchReview.Status.PENDING,
        )

        admin = MatchReviewAdmin(MatchReview, AdminSite())
        admin.message_user = lambda *args, **kwargs: None
        request = RequestFactory().post("/admin/comparison/matchreview/")
        queryset = MatchReview.objects.filter(id=review_a.id)

        admin.approve_selected_reviews(request, queryset)

        listing.refresh_from_db()
        review_a.refresh_from_db()
        review_b.refresh_from_db()

        self.assertEqual(listing.product_id, candidate_a.id)
        self.assertEqual(review_a.status, MatchReview.Status.APPROVED)
        self.assertEqual(review_b.status, MatchReview.Status.REJECTED)

    def test_reject_action_creates_new_product_and_links_listing(self):
        store = Store.objects.create(name="mymarket")
        listing = StoreListing.objects.create(
            store=store,
            store_sku="sku-reject",
            store_name="Σαλάτα δοκιμή 425gr",
            store_brand="Φρεσκούλης",
            source_category="frouta-lachanika",
            url="https://example.com/reject",
            final_price=Decimal("2.40"),
        )
        candidate = Product.objects.create(canonical_name="Existing candidate")
        other_candidate = Product.objects.create(canonical_name="Other candidate")
        review = MatchReview.objects.create(
            store_listing=listing,
            candidate_product=candidate,
            score=Decimal("0.8800"),
            status=MatchReview.Status.PENDING,
        )
        other_review = MatchReview.objects.create(
            store_listing=listing,
            candidate_product=other_candidate,
            score=Decimal("0.8500"),
            status=MatchReview.Status.PENDING,
        )

        initial_product_count = Product.objects.count()

        admin = MatchReviewAdmin(MatchReview, AdminSite())
        admin.message_user = lambda *args, **kwargs: None
        request = RequestFactory().post("/admin/comparison/matchreview/")

        admin.reject_selected_reviews(request, MatchReview.objects.filter(id=review.id))

        listing.refresh_from_db()
        review.refresh_from_db()
        other_review.refresh_from_db()

        self.assertEqual(review.status, MatchReview.Status.REJECTED)
        self.assertEqual(other_review.status, MatchReview.Status.REJECTED)
        self.assertIsNotNone(listing.product_id)
        self.assertNotIn(listing.product_id, [candidate.id, other_candidate.id])
        self.assertEqual(Product.objects.count(), initial_product_count + 1)
