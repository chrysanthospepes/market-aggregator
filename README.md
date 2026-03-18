# Market Aggregator

Django application for aggregating supermarket listings from multiple stores, importing daily crawl snapshots, matching equivalent listings into canonical products, and exposing comparison pages and APIs.

## What Is Implemented

- Multi-store crawler integration for:
  - `ab`
  - `bazaar`
  - `mymarket`
  - `masoutis`
  - `kritikos`
  - `sklavenitis`
- Idempotent import pipeline for raw store listings
- Daily crawl observability with `CrawlerRun`
- Price snapshot history with `PriceHistory`
- Canonical product matching with auto-match, manual review, and forced-new-product flows
- Product comparison HTML pages and JSON API
- Product image download and backfill support
- Django admin for stores, categories, listings, crawler runs, price history, and match reviews

## Project Structure

```text
market-aggregator/
  catalog/       Core catalog models, category mapping, image helpers
  comparison/    Match review admin, product pages, product offers API
  config/        Django settings and URL config
  crawlers/      One package per store crawler
  ingestion/     Listing import pipeline and management commands
  matching/      Name normalization and listing-to-product matcher
```

## Core Data Model

### Catalog

- `Store`
  - Store name, unique
  - Default stores are auto-seeded on app startup (`ab`, `bazaar`, `mymarket`, `masoutis`, `kritikos`, `sklavenitis`)
  - Additional stores are still created lazily during import with `get_or_create`
- `Category`
  - Canonical category name and slug
  - Default categories are auto-seeded on app startup
- `CategoryAlias`
  - Maps store-specific source category slugs to canonical categories
  - Default store-specific aliases are auto-seeded on app startup
  - Supports store-specific aliases and global aliases
- `Product`
  - Canonical shared product
  - Stores normalized brand, quantity, category, and optional product image

### Ingestion

- `StoreListing`
  - One listing per store product row
  - Unique by `store + store_sku` when SKU exists
  - Fallback uniqueness by `store + url` when SKU is missing
  - Keeps raw store naming and pricing fields
  - Can be linked to a canonical `Product`
- `CrawlerRun`
  - Tracks one import run per store
  - Records status, timing, errors, and number of seen items
- `PriceHistory`
  - Snapshot table keyed by listing and capture time

### Comparison

- `MatchReview`
  - Manual review queue for ambiguous matches
  - Status values: `pending`, `approved`, `rejected`

## Setup

### Requirements

- Python
- PostgreSQL for normal app usage
- A virtual environment with dependencies from `requirements.txt`

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Database Configuration

Default settings use PostgreSQL and read:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_HOST`
- `POSTGRES_PORT`

Default values are defined in [config/settings.py](/home/cpepes/dev/projects/market-aggregator/config/settings.py).

### Run the App

```bash
./.venv/bin/python manage.py migrate
./.venv/bin/python manage.py createsuperuser
./.venv/bin/python manage.py runserver
```

Important routes:

- `/admin/`
- `/products/`
- `/products/<product_id>/`
- `/api/products/<product_id>/offers`

## Crawler Layout

Each crawler now lives in its own package:

- [crawlers/ab](/home/cpepes/dev/projects/market-aggregator/crawlers/ab)
- [crawlers/bazaar](/home/cpepes/dev/projects/market-aggregator/crawlers/bazaar)
- [crawlers/mymarket](/home/cpepes/dev/projects/market-aggregator/crawlers/mymarket)
- [crawlers/masoutis](/home/cpepes/dev/projects/market-aggregator/crawlers/masoutis)
- [crawlers/kritikos](/home/cpepes/dev/projects/market-aggregator/crawlers/kritikos)
- [crawlers/sklavenitis](/home/cpepes/dev/projects/market-aggregator/crawlers/sklavenitis)

The shared crawler registry lives in [crawlers/__init__.py](/home/cpepes/dev/projects/market-aggregator/crawlers/__init__.py).

Each crawler is expected to expose:

- `ROOT_CATEGORIES`
- `to_category_slug(...)`
- `to_category_url(...)`
- `crawl_category_listing(...)`
- optionally `to_root_category(...)`

## Import and Ingestion Flow

### CSV Import

Use this when you already have a CSV:

```bash
./.venv/bin/python manage.py import_store_csv --store sklavenitis --file data.csv
./.venv/bin/python manage.py import_store_csv --store mymarket --file data.csv --run-matcher
```

Behavior:

- Reads rows from CSV
- Creates the `Store` automatically if it does not exist yet
- Upserts `StoreListing`
- Writes a `CrawlerRun`
- Writes `PriceHistory`
- Marks missing listings as inactive instead of deleting them
- Optionally runs the matcher for changed/new listings only

### Single-Store Daily Ingestion

Use this to run one crawler and import its output:

```bash
./.venv/bin/python manage.py run_daily_ingestion --store ab
./.venv/bin/python manage.py run_daily_ingestion --store bazaar --max-pages 5
./.venv/bin/python manage.py run_daily_ingestion --store kritikos --run-matcher
./.venv/bin/python manage.py run_daily_ingestion --store sklavenitis --save-combined-csv /tmp/sklavenitis.csv
```

Behavior:

- Imports the selected crawler module from the crawler registry
- Crawls one or more categories
- Converts crawler dataclass rows to dictionaries
- Deduplicates rows by `url`, or by `sku|name` when URL is missing
- Refuses to import an empty crawl
- Imports rows through the shared ingestion service

### Sequential All-Store Daily Ingestion

Use this to run all six crawlers one after the other:

```bash
./.venv/bin/python manage.py run_all_daily_ingestion
./.venv/bin/python manage.py run_all_daily_ingestion --max-pages 10
./.venv/bin/python manage.py run_all_daily_ingestion --run-matcher
./.venv/bin/python manage.py run_all_daily_ingestion --save-combined-csv-dir /tmp/crawler-csvs
```

Execution order is fixed:

1. `ab`
2. `bazaar`
3. `mymarket`
4. `masoutis`
5. `kritikos`
6. `sklavenitis`

The next crawler starts only after the previous one finishes. If one store fails, the command stops there.

## Matching Pipeline

The matcher lives in [matching/matcher.py](/home/cpepes/dev/projects/market-aggregator/matching/matcher.py) and uses helpers from [matching/normalizer.py](/home/cpepes/dev/projects/market-aggregator/matching/normalizer.py).

### What It Does

For each eligible `StoreListing`, the matcher:

- normalizes the listing name and brand
- extracts quantity and unit
- builds a normalized key
- scopes candidate `Product` rows by category, brand, and quantity where possible
- scores the best candidate using name, brand, quantity, and category similarity

### Possible Outcomes

- Auto-match to an existing `Product`
- Create a `MatchReview` for manual review
- Create a new canonical `Product`

### Important Rules

- Hard quantity mismatches are not queued as review candidates
- Same-store collisions are avoided so one store does not collapse multiple distinct listings into one product
- Category aliases influence both candidate selection and new product category assignment

### Running the Matcher Manually

```bash
./.venv/bin/python manage.py run_listing_matcher
./.venv/bin/python manage.py run_listing_matcher --store ab
./.venv/bin/python manage.py run_listing_matcher --listing-id 123
./.venv/bin/python manage.py run_listing_matcher --include-matched --include-inactive
```

## Match Review Workflow

Manual review is handled through Django admin on `MatchReview` and through two staff review queues:

- `/reviews/pending/`
- `/reviews/reported-listings/`

### Approve

- Links the listing to the chosen candidate product
- Downloads the product image when possible
- Rejects other pending reviews for the same listing

### Reject

- Creates a new product from the listing
- Links the listing to that new product
- Rejects other pending reviews for the same listing

## Category Mapping

Source categories are normalized and resolved in this order:

1. Store-specific `CategoryAlias`
2. Global `CategoryAlias`
3. Direct match to `Category.slug`

This logic lives in [catalog/category_mapping.py](/home/cpepes/dev/projects/market-aggregator/catalog/category_mapping.py).

## Product Images

When a listing gets linked to a product, the app tries to copy the listing image into the product image field if the product does not already have one.

Backfill command:

```bash
./.venv/bin/python manage.py backfill_product_images
./.venv/bin/python manage.py backfill_product_images --limit 100
```

## Comparison UI and API

### HTML

- `/products/`
  - Lists products that currently have at least one active linked listing
  - Supports default name sort and `?sort=unit_price_asc`
- `/products/<id>/`
  - Shows the active linked listings for one canonical product

### JSON

- `/api/products/<id>/offers`
  - Returns product data plus all active linked store offers

### Price Profiles

Comparison pages and the offers API accept a `price_profile` query parameter.

Current supported profile:

- `kritikos_eligible_households`
  - Applies an extra 10% discount to Kritikos listings only

Useful local examples:

```text
/products/?price_profile=kritikos_eligible_households
/products/123/?price_profile=kritikos_eligible_households
/api/products/123/offers?price_profile=kritikos_eligible_households
```

## Admin Coverage

Registered in Django admin:

- `Store`
- `Category`
- `CategoryAlias`
- `Product`
- `StoreListing`
- `CrawlerRun`
- `PriceHistory`
- `MatchReview`

This supports both operational work and manual review.

## Testing

There is a dedicated SQLite test settings module in [config/settings_test.py](/home/cpepes/dev/projects/market-aggregator/config/settings_test.py).

Run the current test suite with:

```bash
./.venv/bin/python manage.py test --settings=config.settings_test
```

Targeted examples:

```bash
./.venv/bin/python manage.py test ingestion.tests --settings=config.settings_test
./.venv/bin/python manage.py test comparison.tests --settings=config.settings_test
./.venv/bin/python manage.py test catalog.tests --settings=config.settings_test
```

Single-class examples:

```bash
./.venv/bin/python manage.py test comparison.tests.ComparisonAdminConfigTests --settings=config.settings_test
./.venv/bin/python manage.py test ingestion.tests.StoreListingModelTests --settings=config.settings_test
```

Tests currently cover:

- crawler registry compatibility
- sequential all-store ingestion order
- idempotent imports
- inactive listing handling
- matching behavior
- match review workflow
- comparison API and HTML views

## Current Workflow Summary

1. Run a store crawler or import a CSV.
2. Persist or update `StoreListing` rows.
3. Record a `CrawlerRun` and `PriceHistory`.
4. Optionally run the matcher.
5. Auto-link strong matches.
6. Queue ambiguous matches in `MatchReview`.
7. Review unresolved matches in Django admin.
8. Browse results through `/products/` or `/api/products/<id>/offers`.

## Notes

- Default runtime settings expect PostgreSQL.
- Test runs are easier with `--settings=config.settings_test` because that uses SQLite.
