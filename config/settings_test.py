from .settings import *  # noqa: F401,F403


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "test.sqlite3",  # noqa: F405
    }
}

CATALOG_AUTO_SEED_STORES = False
CATALOG_AUTO_SEED_CATEGORIES = False
CATALOG_AUTO_SEED_CATEGORY_ALIASES = False
