"""Gold layer: Iceberg schema + table lifecycle for prices_cheapest_windows.

Designed for smart-home load shifting: "what's the cheapest 2-hour block to run
the dishwasher today?" / "where do I plug in the EV for the cheapest 8 hours?"

One row per (delivery_date, bidding_zone, window_hours) holding the start of
the cheapest contiguous window of that length and its average price.

Phase 3: the SQL that computes this table moved to dbt
(dbt_project/models/gold/int_gold_cheapest_windows.sql, including the
UNION-ALL-per-window-size trick this module used to build with an f-string —
see that model's comment). This module keeps just the Iceberg-side contract,
same as gold.py.
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

GOLD_WINDOWS_TABLE_NAME = "prices_cheapest_windows"
GOLD_WINDOWS_TABLE_IDENTIFIER = (NAMESPACE, GOLD_WINDOWS_TABLE_NAME)

# Window sizes (hours) that match common household devices.
# Dishwasher ~2h, washing machine ~1-3h, EV charging ~4-8h, heat pump batch ~3-6h.
# Also the default for dbt's `window_hours` var — see assets/gold_dbt.py.
WINDOW_HOURS = (1, 2, 3, 4, 6, 8)

GOLD_WINDOWS_SCHEMA = Schema(
    NestedField(1, "delivery_date", DateType(), required=True),
    NestedField(2, "bidding_zone", StringType(), required=True),
    NestedField(3, "window_hours", IntegerType(), required=True),
    NestedField(4, "window_start_utc", TimestamptzType(), required=True),
    NestedField(5, "window_end_utc", TimestamptzType(), required=True),
    NestedField(6, "avg_price_eur_per_mwh", DoubleType(), required=True),
    NestedField(7, "computed_at_utc", TimestamptzType(), required=True),
    identifier_field_ids=[1, 2, 3],
)

GOLD_WINDOWS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="delivery_date"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="bidding_zone"),
)

# Derived from GOLD_WINDOWS_SCHEMA — see GOLD_ARROW_SCHEMA in gold.py for why
# the publish step casts to this before writing, and why this is derived
# rather than hand-declared.
GOLD_WINDOWS_ARROW_SCHEMA = GOLD_WINDOWS_SCHEMA.as_arrow()


def ensure_windows_table(catalog: Catalog):
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(GOLD_WINDOWS_TABLE_IDENTIFIER)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=GOLD_WINDOWS_TABLE_IDENTIFIER,
            schema=GOLD_WINDOWS_SCHEMA,
            partition_spec=GOLD_WINDOWS_PARTITION_SPEC,
            location=f"s3://{settings.lake_bucket}/gold/{GOLD_WINDOWS_TABLE_NAME}",
        )
