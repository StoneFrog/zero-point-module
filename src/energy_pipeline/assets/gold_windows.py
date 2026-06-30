"""Gold layer: cheapest contiguous price window per (delivery_date, zone, duration).

Designed for smart-home load shifting: "what's the cheapest 2-hour block to run
the dishwasher today?" / "where do I plug in the EV for the cheapest 8 hours?"

Produces one row per (delivery_date, bidding_zone, window_hours) holding the
start of the cheapest contiguous window of that length and its average price.
"""

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from dagster import (
    AssetIn,
    MetadataValue,
    Output,
    asset,
)
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo
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

from energy_pipeline.assets.bronze import daily_partitions
from energy_pipeline.assets.gold import _configure_duckdb_for_minio
from energy_pipeline.assets.silver import NAMESPACE, silver_prices_hourly
from energy_pipeline.config import settings
from energy_pipeline.resources import IcebergCatalogResource

GOLD_WINDOWS_TABLE_NAME = "prices_cheapest_windows"
GOLD_WINDOWS_TABLE_IDENTIFIER = (NAMESPACE, GOLD_WINDOWS_TABLE_NAME)

# Window sizes (hours) that match common household devices.
# Dishwasher ~2h, washing machine ~1-3h, EV charging ~4-8h, heat pump batch ~3-6h.
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


def _build_windows_sql(window_hours: tuple[int, ...]) -> str:
    """Construct one UNION ALL branch per window size.

    Window frame bounds must be constants in standard SQL — that's why we can't
    just CROSS JOIN against a window_sizes table; the frame can't reference a
    runtime column. So we generate the SQL with literal sizes.
    """
    branches = []
    for n in window_hours:
        branches.append(
            f"""
            SELECT
                delivery_date,
                bidding_zone,
                {n}                                                AS window_hours,
                ts_utc                                             AS window_start_utc,
                AVG(price_eur_per_mwh) OVER w_{n}                  AS window_avg,
                COUNT(*)               OVER w_{n}                  AS window_count
            FROM silver
            WHERE resolution_minutes = 60
            WINDOW w_{n} AS (
                PARTITION BY delivery_date, bidding_zone
                ORDER BY ts_utc
                ROWS BETWEEN CURRENT ROW AND {n - 1} FOLLOWING
            )
            """
        )
    return " UNION ALL ".join(branches)


@asset(
    partitions_def=daily_partitions,
    ins={"_silver": AssetIn(silver_prices_hourly.key)},
    group_name="gold",
    compute_kind="duckdb",
    description=(
        "For each (delivery_date, bidding_zone, window_hours) the start time and average "
        "price of the cheapest contiguous N-hour block in the day. Source for smart-home "
        "load-shifting decisions (when to run the dishwasher, when to charge the EV)."
    ),
)
def gold_cheapest_windows(
    context,
    iceberg: IcebergCatalogResource,
    _silver,
) -> Output[None]:
    delivery_day = datetime.fromisoformat(context.partition_key).date()
    catalog = iceberg.get()

    silver_table = catalog.load_table((NAMESPACE, "prices_hourly"))
    arrow_silver = silver_table.scan(
        row_filter=EqualTo("delivery_date", delivery_day)
    ).to_arrow()

    if arrow_silver.num_rows == 0:
        context.log.warning(f"Silver had no rows for {delivery_day}; skipping windows.")
        return Output(None, metadata={"rows_written": MetadataValue.int(0)})

    con = duckdb.connect()
    _configure_duckdb_for_minio(con)
    con.register("silver", arrow_silver)

    windows_sql = _build_windows_sql(WINDOW_HOURS)
    final = con.execute(
        f"""
        WITH all_windows AS (
            {windows_sql}
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY delivery_date, bidding_zone, window_hours
                    ORDER BY window_avg ASC, window_start_utc ASC
                ) AS rn
            FROM all_windows
            -- Discard incomplete windows clipped by the end of the day.
            WHERE window_count = window_hours
        )
        SELECT
            delivery_date,
            bidding_zone,
            window_hours,
            window_start_utc,
            window_start_utc + INTERVAL (window_hours) HOUR  AS window_end_utc,
            window_avg                                       AS avg_price_eur_per_mwh
        FROM ranked
        WHERE rn = 1
        """
    ).arrow()

    computed_at = datetime.now(tz=timezone.utc).replace(microsecond=0)
    computed_at_col = pa.array(
        [computed_at] * final.num_rows, type=pa.timestamp("us", tz="UTC")
    )
    final = final.append_column("computed_at_utc", computed_at_col)

    final = final.cast(
        pa.schema(
            [
                pa.field("delivery_date", pa.date32(), nullable=False),
                pa.field("bidding_zone", pa.string(), nullable=False),
                pa.field("window_hours", pa.int32(), nullable=False),
                pa.field("window_start_utc", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("window_end_utc", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("avg_price_eur_per_mwh", pa.float64(), nullable=False),
                pa.field("computed_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
            ]
        )
    )

    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    try:
        table = catalog.load_table(GOLD_WINDOWS_TABLE_IDENTIFIER)
    except NoSuchTableError:
        table = catalog.create_table(
            identifier=GOLD_WINDOWS_TABLE_IDENTIFIER,
            schema=GOLD_WINDOWS_SCHEMA,
            partition_spec=GOLD_WINDOWS_PARTITION_SPEC,
            location=f"s3://{settings.lake_bucket}/gold/{GOLD_WINDOWS_TABLE_NAME}",
        )

    table.overwrite(final, overwrite_filter=EqualTo("delivery_date", delivery_day))

    return Output(
        None,
        metadata={
            "delivery_date": MetadataValue.text(delivery_day.isoformat()),
            "rows_written": MetadataValue.int(final.num_rows),
            "window_sizes": MetadataValue.json(list(WINDOW_HOURS)),
            "snapshot_id": MetadataValue.text(
                str(table.refresh().current_snapshot().snapshot_id)
            ),
        },
    )
