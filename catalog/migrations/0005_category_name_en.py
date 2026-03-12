from django.db import migrations, models


DEFAULT_CATEGORY_NAME_EN_BY_SLUG = {
    "frouta-lachanika": "Fruits & Vegetables",
    "kreata-psaria": "Meat & Fish",
    "eidi-psigeiou": "Chilled Foods",
    "katepsygmena": "Frozen Foods",
    "apothiki-trofimon-xira-trofi": "Pantry & Dry Food",
    "pota-alkool": "Drinks & Alcohol",
    "prosopiki-frontida": "Personal Care",
    "vrefika": "Baby",
    "oikiaka-kathariotita": "Household & Cleaning",
    "katoikidia": "Pets",
    "diafora": "Miscellaneous",
}


def backfill_category_english_names(apps, schema_editor):
    Category = apps.get_model("catalog", "Category")
    for slug, name_en in DEFAULT_CATEGORY_NAME_EN_BY_SLUG.items():
        Category.objects.filter(slug=slug, name_en="").update(name_en=name_en)


class Migration(migrations.Migration):

    dependencies = [
        ("catalog", "0004_product_search_name"),
    ]

    operations = [
        migrations.AddField(
            model_name="category",
            name="name_en",
            field=models.CharField(blank=True, default="", max_length=128),
        ),
        migrations.RunPython(backfill_category_english_names, migrations.RunPython.noop),
    ]
