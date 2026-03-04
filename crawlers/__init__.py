CRAWLER_MODULES = {
    "ab": "crawlers.ab.ab_category_listing",
    "bazaar": "crawlers.bazaar.bazaar_category_listing",
    "kritikos": "crawlers.kritikos.kritikos_category_listing",
    "masoutis": "crawlers.masoutis.masoutis_category_listing",
    "mymarket": "crawlers.mymarket.mymarket_category_listing",
    "sklavenitis": "crawlers.sklavenitis.sklavenitis_category_listing",
}

CRAWLER_RUN_ORDER = [
    "ab",
    "bazaar",
    "mymarket",
    "masoutis",
    "kritikos",
    "sklavenitis",
]

__all__ = ["CRAWLER_MODULES", "CRAWLER_RUN_ORDER"]
