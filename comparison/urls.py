from django.urls import path

from comparison import views


urlpatterns = [
    path("", views.home, name="home"),
    path("reviews/pending/", views.match_review_queue, name="match-review-queue"),
    path("reviews/reported-listings/", views.listing_report_queue, name="listing-report-queue"),
    path(
        "reviews/reported-listings/<int:report_id>/",
        views.listing_report_detail,
        name="listing-report-detail",
    ),
    path("products/", views.product_list, name="product-list"),
    path("products/<int:product_id>/", views.product_detail, name="product-detail"),
    path(
        "products/<int:product_id>/report-listing/",
        views.report_product_listing,
        name="report-product-listing",
    ),
    path("api/products/<int:product_id>/offers", views.product_offers, name="product-offers"),
]
