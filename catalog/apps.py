from django.apps import AppConfig
from django.db.models.signals import post_migrate


class CatalogConfig(AppConfig):
    name = "catalog"

    def ready(self) -> None:
        from catalog.startup import (
            ensure_default_catalog_seed_data,
            ensure_default_catalog_seed_data_post_migrate,
        )

        ensure_default_catalog_seed_data()
        post_migrate.connect(
            ensure_default_catalog_seed_data_post_migrate,
            sender=self,
            dispatch_uid="catalog.ensure_default_catalog_seed_data",
        )
