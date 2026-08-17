"""Gold layer: Iceberg schema + table lifecycle for prices_daily_stats.

One row per (delivery_date, bidding_zone) with stats useful for dashboards and
the future smart-home decision logic (cheapest hour, peak hour, daily spread).

Phase 3: the SQL that computes this table moved to dbt
(dbt_project/models/gold/int_gold_prices_daily_stats.sql). This module keeps
just the Iceberg-side contract — schema, partition spec, table lifecycle, and
the Arrow schema used to cast dbt's output before writing — since that's what
assets/gold_dbt.py's publish step needs, and it's the part of this codebase
that's been hardened over many iterations (see git log) and isn't worth
re-deriving elsewhere.
"""

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

from energy_pipeline.assets.silver import NAMESPACE
from energy_pipeline.config import settings

GOLD_TABLE_NAME = "prices_daily_stats"
GOLD_TABLE_IDENTIFIER = (NAMESPACE, GOLD_TABLE_NAME)

GOLD_SCHEMA = Schema(
    NestedField(1, "delivery_date", DateType(), required=True),
    NestedField(2, "bidding_zone", StringType(), required=True),
    NestedField(3, "min_price_eur_per_mwh", DoubleType(), required=True),
    NestedField(4, "max_price_eur_per_mwh", DoubleType(), required=True),
    NestedField(5, "avg_price_eur_per_mwh", DoubleType(), required=True),
    NestedField(6, "spread_eur_per_mwh", DoubleType(), required=True),
    NestedField(7, "cheapest_hour_utc", TimestamptzType(), required=True),
    NestedField(8, "peak_hour_utc", TimestamptzType(), required=True),
    NestedField(9, "n_intervals", IntegerType(), required=True),
    NestedField(10, "computed_at_utc", TimestamptzType(), required=True),
    identifier_field_ids=[1, 2],
)

# delivery_date is already day-granular, so identity transform = day partition.
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="delivery_date"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="bidding_zone"),
)

# Derived from GOLD_SCHEMA rather than hand-declared. dbt's
# int_gold_prices_daily_stats model produces the same columns via DuckDB,
# whose inferred types (e.g. int64 for COUNT(*)) don't always match
# Iceberg's — see LEARNING.md's "Type-coercion friction" note — so the
# publish step still casts to this before writing, but there's now exactly
# one place (GOLD_SCHEMA) declaring what the columns and types actually are.
GOLD_ARROW_SCHEMA = GOLD_SCHEMA.as_arrow()


def ensure_gold_table(catalog: Catalog):
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(GOLD_TABLE_IDENTIFIER)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=GOLD_TABLE_IDENTIFIER,
            schema=GOLD_SCHEMA,
            partition_spec=GOLD_PARTITION_SPEC,
            location=f"s3://{settings.lake_bucket}/gold/{GOLD_TABLE_NAME}",
        )
