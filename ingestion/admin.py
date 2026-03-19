from django.contrib import admin

from ingestion.models import CrawlerRun, PriceHistory, StoreListing


@admin.register(StoreListing)
class StoreListingAdmin(admin.ModelAdmin):
    search_fields = ["store_name", "store_brand", "store_sku", "url"]
    list_select_related = ["store", "product"]
    list_display = [
        "id",
        "store",
        "store_name",
        "store_sku",
        "source_category",
        "final_price",
        "final_unit_price",
        "is_active",
        "product",
        "last_seen_at",
    ]
    list_filter = ["store", "is_active", "product"]
    autocomplete_fields = ["product"]


@admin.register(PriceHistory)
class PriceHistoryAdmin(admin.ModelAdmin):
    list_select_related = ["store_listing__store"]
    list_display = ["id", "store_listing", "price", "unit_price", "captured_at"]
    list_filter = ["captured_at"]
    search_fields = ["store_listing__store_name", "store_listing__store__name"]


@admin.register(CrawlerRun)
class CrawlerRunAdmin(admin.ModelAdmin):
    list_select_related = ["store"]
    list_display = ["id", "store", "started_at", "finished_at", "status", "items_seen"]
    list_filter = ["store", "status"]
    search_fields = ["store__name", "error_summary"]
