from django.test import TestCase

from decimal import Decimal

from django.contrib.admin.sites import AdminSite
from django.test import RequestFactory
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
        product = Product.objects.create(
            canonical_name="Φρεσκούλης ιταλική σαλάτα 425gr",
            brand_normalized="freskoulis",
            quantity_value=Decimal("425"),
            quantity_unit="g",
            normalized_key="freskoulis|425g|φρεσκουλης ιταλικη",
        )
        listing = StoreListing.objects.create(
            store=self.sklavenitis,
            store_sku="sku-58",
            store_name="ΦΡΕΣΚΟΥΛΗΣ ΙΤΑΛΙΚΗ ΣΑΛΑΤΑ 425gr",
            store_brand="Φρεσκούλης",
            url="https://example.com/sklavenitis/58",
            final_price=Decimal("2.10"),
        )

        summary = match_store_listings(listing_ids=[listing.id], only_unmatched=True)
        listing.refresh_from_db()

        self.assertEqual(summary.auto_matched, 1)
        self.assertEqual(listing.product_id, product.id)

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
