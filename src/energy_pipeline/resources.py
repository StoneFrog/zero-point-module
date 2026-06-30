"""Dagster resources: external systems exposed to assets via dependency injection.

Why resources: assets stay pure (input -> output), and we can swap (e.g. point
the catalog at a test Nessie branch) without rewriting the asset code. This is
how Dagster supports unit testing and multi-environment deploys.
"""

from __future__ import annotations

from functools import cached_property

from dagster import ConfigurableResource
from pyiceberg.catalog import Catalog, load_catalog

from energy_pipeline.config import settings
from energy_pipeline.entsoe.client import EntsoeClient


class EntsoeResource(ConfigurableResource):
    """Wraps the ENTSO-E HTTP client. Reads token from settings on init."""

    use_fixture: bool = True

    def client(self) -> EntsoeClient:
        return EntsoeClient(
            api_token=settings.entsoe_api_token,
            base_url=settings.entsoe_base_url,
            use_fixture=self.use_fixture or not settings.entsoe_api_token,
        )


class IcebergCatalogResource(ConfigurableResource):
    """Loads a PyIceberg catalog client backed by the Iceberg REST catalog.

    The catalog stores only metadata pointers; data files live in S3 (MinIO).
    Any Iceberg-REST-compatible catalog works here — currently the Apache
    reference implementation (tabulario/iceberg-rest).
    """

    @cached_property
    def _catalog(self) -> Catalog:
        return load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": settings.iceberg_catalog_uri,
                "warehouse": f"s3://{settings.lake_bucket}/warehouse",
                "s3.endpoint": settings.s3_endpoint,
                "s3.access-key-id": settings.minio_root_user,
                "s3.secret-access-key": settings.minio_root_password,
                "s3.region": settings.s3_region,
                "s3.path-style-access": "true",
            },
        )

    def get(self) -> Catalog:
        return self._catalog
