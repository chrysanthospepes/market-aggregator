from __future__ import annotations

from django.conf import settings
from django.db import IntegrityError, OperationalError, ProgrammingError

from catalog.category_mapping import normalize_source_category
from catalog.models import Category, CategoryAlias, Store


DEFAULT_STORE_NAMES = (
    "ab",
    "bazaar",
    "mymarket",
    "masoutis",
    "kritikos",
    "sklavenitis",
)

DEFAULT_CATEGORIES = (
    ("Φρούτα & Λαχανικά", "Fruits & Vegetables", "frouta-lachanika"),
    ("Κρέατα & Ψάρια", "Meat & Fish", "kreata-psaria"),
    ("Είδη Ψυγείου", "Chilled Foods", "eidi-psigeiou"),
    ("Κατεψυγμένα", "Frozen Foods", "katepsygmena"),
    ("Αποθήκη Τροφίμων & Ξηρά Τροφή", "Pantry & Dry Food", "apothiki-trofimon-xira-trofi"),
    ("Ποτά & Αλκοόλ", "Drinks & Alcohol", "pota-alkool"),
    ("Προσωπική Φροντίδα", "Personal Care", "prosopiki-frontida"),
    ("Βρεφικά", "Baby", "vrefika"),
    ("Οικιακά & Καθαριότητα", "Household & Cleaning", "oikiaka-kathariotita"),
    ("Κατοικίδια", "Pets", "katoikidia"),
    ("Διάφορα", "Miscellaneous", "diafora"),
)

DEFAULT_CATEGORY_ALIASES = (
    ("sklavenitis", "freska-froyta-lachanika", "frouta-lachanika"),
    ("mymarket", "frouta-lachanika", "frouta-lachanika"),
    ("kritikos", "manabikh", "frouta-lachanika"),
    ("ab", "oporopoleio", "frouta-lachanika"),
    ("bazaar", "froyta-lachanika", "frouta-lachanika"),
    ("masoutis", "manabiko", "frouta-lachanika"),
    ("sklavenitis", "fresko-kreas", "kreata-psaria"),
    ("sklavenitis", "fresko-psari-thalassina", "kreata-psaria"),
    ("mymarket", "fresko-kreas-psari", "kreata-psaria"),
    ("kritikos", "fresko-kreas", "kreata-psaria"),
    ("ab", "fresko-kreas-and-psaria", "kreata-psaria"),
    ("bazaar", "kreas-poylerika", "kreata-psaria"),
    ("masoutis", "kreopwleio", "kreata-psaria"),
    ("sklavenitis", "galata-rofimata-chymoi-psygeioy", "eidi-psigeiou"),
    ("sklavenitis", "giaoyrtia-kremes-galaktos-epidorpia-sygeioy", "eidi-psigeiou"),
    ("sklavenitis", "ayga-voytyro-nopes-zymes-zomoi", "eidi-psigeiou"),
    ("sklavenitis", "turokomika-futika-anapliromata", "eidi-psigeiou"),
    ("sklavenitis", "allantika", "eidi-psigeiou"),
    ("sklavenitis", "etoima-geymata", "eidi-psigeiou"),
    ("mymarket", "galaktokomika-eidi-psygeiou", "eidi-psigeiou"),
    ("mymarket", "tyria-allantika-deli", "eidi-psigeiou"),
    ("kritikos", "allantika", "eidi-psigeiou"),
    ("kritikos", "turokomika", "eidi-psigeiou"),
    ("kritikos", "galaktokomika", "eidi-psigeiou"),
    ("kritikos", "eidh-psugeiou", "eidi-psigeiou"),
    ("ab", "galaktokomika-fytika-rofimata-and-eidi-psygeioy", "eidi-psigeiou"),
    ("ab", "tyria-fytika-anapliromata-and-allantika", "eidi-psigeiou"),
    ("ab", "etoima-geymata", "eidi-psigeiou"),
    ("bazaar", "galaktokomika-eidi-rygeioy", "eidi-psigeiou"),
    ("bazaar", "tyria-tyrokomika", "eidi-psigeiou"),
    ("bazaar", "allantika-delicatessen", "eidi-psigeiou"),
    ("bazaar", "fytika", "eidi-psigeiou"),
    ("masoutis", "eidh-psugeiou", "eidi-psigeiou"),
    ("sklavenitis", "katepsygmena", "katepsygmena"),
    ("mymarket", "katepsygmena-trofima", "katepsygmena"),
    ("kritikos", "katapsuxh", "katepsygmena"),
    ("ab", "katepsygmena-trofima", "katepsygmena"),
    ("bazaar", "kataryxi", "katepsygmena"),
    ("masoutis", "eidh-katapsukshs", "katepsygmena"),
    ("sklavenitis", "trofima-pantopoleioy", "apothiki-trofimon-xira-trofi"),
    ("sklavenitis", "xiroi-karpoi-snak", "apothiki-trofimon-xira-trofi"),
    ("sklavenitis", "mpiskota-sokolates-zacharodi", "apothiki-trofimon-xira-trofi"),
    ("sklavenitis", "eidi-proinoy-rofimata", "apothiki-trofimon-xira-trofi"),
    ("sklavenitis", "eidi-artozacharoplasteioy", "apothiki-trofimon-xira-trofi"),
    ("mymarket", "trofima", "apothiki-trofimon-xira-trofi"),
    ("mymarket", "proino-rofimata-kafes", "apothiki-trofimon-xira-trofi"),
    ("mymarket", "artozacharoplasteio-snacks", "apothiki-trofimon-xira-trofi"),
    ("kritikos", "pantopwleio", "apothiki-trofimon-xira-trofi"),
    ("ab", "vasika-typopoiimena-trofima", "apothiki-trofimon-xira-trofi"),
    ("ab", "proino-snacking-and-rofimata", "apothiki-trofimon-xira-trofi"),
    ("ab", "artos-zacharoplasteio", "apothiki-trofimon-xira-trofi"),
    ("bazaar", "pantopoleio", "apothiki-trofimon-xira-trofi"),
    ("bazaar", "glyka-almyra-snak-zacharodi", "apothiki-trofimon-xira-trofi"),
    ("bazaar", "proino-kafes-rofimata", "apothiki-trofimon-xira-trofi"),
    ("bazaar", "artozacharoplasteio", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "snack-kshroi-karpoi", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "prwina", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "artozaxaroplasteio", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "zaxarwdh-mpiskota", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "eidh-pantopwleiou", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "zumarika-ospria", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "dressing", "apothiki-trofimon-xira-trofi"),
    ("masoutis", "konserboeidh", "apothiki-trofimon-xira-trofi"),
    ("sklavenitis", "kava", "pota-alkool"),
    ("sklavenitis", "anapsyktika-nera-chymoi", "pota-alkool"),
    ("mymarket", "mpyres-anapsyktika-krasia-pota", "pota-alkool"),
    ("kritikos", "kaba", "pota-alkool"),
    ("ab", "kava-anapsyktika-nera-xiroi-karpoi", "pota-alkool"),
    ("bazaar", "kava", "pota-alkool"),
    ("masoutis", "kaba", "pota-alkool"),
    ("sklavenitis", "kallyntika-eidi-prosopikis-ygieinis", "prosopiki-frontida"),
    ("mymarket", "prosopiki-frontida", "prosopiki-frontida"),
    ("kritikos", "proswpikh-frontida", "prosopiki-frontida"),
    ("ab", "eidi-prosopikis-peripoiisis", "prosopiki-frontida"),
    ("bazaar", "ygeia-and-omorfia", "prosopiki-frontida"),
    ("masoutis", "proswpikh-peripoihsh", "prosopiki-frontida"),
    ("sklavenitis", "vrefikes-paidikes-trofes", "vrefika"),
    ("mymarket", "frontida-gia-to-moro-sas", "vrefika"),
    ("kritikos", "brefika", "vrefika"),
    ("ab", "ola-gia-to-moro", "vrefika"),
    ("bazaar", "vrefika", "vrefika"),
    ("masoutis", "brefikh-frontida", "vrefika"),
    ("sklavenitis", "aporrypantika-eidi-katharismoy", "oikiaka-kathariotita"),
    ("sklavenitis", "chartika-panes-servietes", "oikiaka-kathariotita"),
    ("sklavenitis", "eidi-oikiakis-chrisis", "oikiaka-kathariotita"),
    ("mymarket", "oikiaki-frontida-chartika", "oikiaka-kathariotita"),
    ("mymarket", "kouzina-mikrosyskeves-spiti", "oikiaka-kathariotita"),
    ("kritikos", "kathariothta", "oikiaka-kathariotita"),
    ("kritikos", "oikiakh-xrhsh", "oikiaka-kathariotita"),
    ("ab", "Katharistika-Chartika-and-eidi-spitioy", "oikiaka-kathariotita"),
    ("bazaar", "kathariotita-oikiaka-eidi", "oikiaka-kathariotita"),
    ("masoutis", "ugieinh-xartika", "oikiaka-kathariotita"),
    ("masoutis", "eidh-katharismou", "oikiaka-kathariotita"),
    ("masoutis", "eidh-oikiakhs", "oikiaka-kathariotita"),
    ("sklavenitis", "trofes-eidi-gia-katoikidia", "katoikidia"),
    ("mymarket", "frontida-gia-to-katoikidio-sas", "katoikidia"),
    ("kritikos", "pet-shop", "katoikidia"),
    ("ab", "gia-katoikidia", "katoikidia"),
    ("bazaar", "pet-shop", "katoikidia"),
    ("masoutis", "katoikidia", "katoikidia"),
    ("sklavenitis", "eidi-mias-chrisis-eidi-parti", "diafora"),
    ("sklavenitis", "chartopoleio", "diafora"),
    ("mymarket", "epochiaka", "diafora"),
)

_stores_seeded = False
_categories_seeded = False
_category_aliases_seeded = False


def ensure_default_stores(*, force: bool = False) -> None:
    global _stores_seeded

    if not getattr(settings, "CATALOG_AUTO_SEED_STORES", True):
        return
    if _stores_seeded and not force:
        return

    try:
        Store.objects.bulk_create(
            [Store(name=name) for name in DEFAULT_STORE_NAMES],
            ignore_conflicts=True,
        )
    except (OperationalError, ProgrammingError):
        # DB might not be available yet or migrations may not have created tables.
        return

    _stores_seeded = True


def ensure_default_categories(*, force: bool = False) -> None:
    global _categories_seeded

    if not getattr(settings, "CATALOG_AUTO_SEED_CATEGORIES", True):
        return
    if _categories_seeded and not force:
        return

    try:
        Category.objects.bulk_create(
            [Category(name=name, name_en=name_en, slug=slug) for name, name_en, slug in DEFAULT_CATEGORIES],
            ignore_conflicts=True,
        )
    except (OperationalError, ProgrammingError):
        # DB might not be available yet or migrations may not have created tables.
        return

    _categories_seeded = True


def ensure_default_category_aliases(*, force: bool = False) -> None:
    global _category_aliases_seeded

    if not getattr(settings, "CATALOG_AUTO_SEED_CATEGORY_ALIASES", True):
        return
    if _category_aliases_seeded and not force:
        return

    try:
        store_names = sorted({store_name for store_name, _, _ in DEFAULT_CATEGORY_ALIASES if store_name})
        category_slugs = sorted({category_slug for _, _, category_slug in DEFAULT_CATEGORY_ALIASES})
        stores_by_name = {store.name: store for store in Store.objects.filter(name__in=store_names)}
        categories_by_slug = {
            category.slug: category
            for category in Category.objects.filter(slug__in=category_slugs)
        }

        for store_name, source_slug, category_slug in DEFAULT_CATEGORY_ALIASES:
            category = categories_by_slug.get(category_slug)
            if category is None:
                continue

            store = stores_by_name.get(store_name)
            if store is None:
                continue

            normalized_source_slug = normalize_source_category(source_slug)
            if not normalized_source_slug:
                continue

            try:
                alias, created = CategoryAlias.objects.get_or_create(
                    store=store,
                    source_slug=normalized_source_slug,
                    defaults={"category": category},
                )
            except IntegrityError:
                alias = CategoryAlias.objects.filter(
                    store=store,
                    source_slug=normalized_source_slug,
                ).first()
                if alias is None:
                    continue
                created = False

            if not created and alias.category_id != category.id:
                alias.category = category
                alias.save(update_fields=["category"])
    except (OperationalError, ProgrammingError):
        # DB might not be available yet or migrations may not have created tables.
        return

    _category_aliases_seeded = True


def ensure_default_catalog_seed_data(*, force: bool = False) -> None:
    ensure_default_stores(force=force)
    ensure_default_categories(force=force)
    ensure_default_category_aliases(force=force)


def ensure_default_catalog_seed_data_post_migrate(**_: object) -> None:
    ensure_default_catalog_seed_data(force=True)
