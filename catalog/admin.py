from django.contrib import admin

from catalog.models import Category, CategoryAlias, Product, Store


@admin.register(Store)
class StoreAdmin(admin.ModelAdmin):
    search_fields = ["name"]
    list_display = ["id", "name"]


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    search_fields = ["name", "slug"]
    list_display = ["id", "name", "slug"]
    prepopulated_fields = {"slug": ("name",)}


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    search_fields = ["canonical_name", "brand_normalized", "normalized_key"]
    list_display = [
        "id",
        "canonical_name",
        "brand_normalized",
        "quantity_value",
        "quantity_unit",
        "category",
    ]
    list_filter = ["quantity_unit", "category"]


@admin.register(CategoryAlias)
class CategoryAliasAdmin(admin.ModelAdmin):
    search_fields = ["source_slug", "category__name", "category__slug", "store__name"]
    list_display = ["id", "store", "source_slug", "category"]
    list_filter = ["store", "category"]
