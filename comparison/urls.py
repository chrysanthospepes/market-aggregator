from django.urls import path

from comparison import views


urlpatterns = [
    path("products/", views.product_list, name="product-list"),
    path("products/<int:product_id>/", views.product_detail, name="product-detail"),
    path("api/products/<int:product_id>/offers", views.product_offers, name="product-offers"),
]
