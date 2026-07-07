"""One-off: write metadata/version-hint.text for every namespace.table in the
catalog. Useful after adopting the version-hint pattern on tables that already
have data.

Run inside the dagster container:
    docker compose exec dagster-webserver python -m scripts.backfill_version_hints
"""

from energy_pipeline.iceberg_utils import write_version_hint
from energy_pipeline.resources import IcebergCatalogResource


def main() -> None:
    catalog = IcebergCatalogResource().get()
    for namespace in catalog.list_namespaces():
        for identifier in catalog.list_tables(namespace):
            table = catalog.load_table(identifier)
            version = write_version_hint(table)
            print(f"{'.'.join(identifier)}: version-hint = {version}")


if __name__ == "__main__":
    main()
