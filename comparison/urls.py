from django.urls import path

from comparison import views


urlpatterns = [
    path("api/products/<int:product_id>/offers", views.product_offers, name="product-offers"),
]
