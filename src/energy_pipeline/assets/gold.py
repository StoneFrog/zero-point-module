"""Gold layer: business-ready aggregates from silver.

One row per (delivery_date, bidding_zone) with stats useful for dashboards and
the future smart-home decision logic (cheapest hour, peak hour, daily spread).

Computed in-process with DuckDB scanning the silver Iceberg table directly.
DuckDB is embedded — no separate engine needed. If gold ever outgrows a single
machine we swap DuckDB for Trino without changing the table or the SQL.
"""

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from dagster import (
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
from energy_pipeline.assets.silver import (
    NAMESPACE,
    silver_prices_hourly,
)
from energy_pipeline.config import settings
from energy_pipeline.resources import IcebergCatalogResource

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


def _ensure_gold_table(catalog: Catalog):
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


def _configure_duckdb_for_minio(con: duckdb.DuckDBPyConnection) -> None:
    """Tell DuckDB how to reach MinIO. Reused on every connection."""
    endpoint = settings.s3_endpoint.replace("http://", "").replace("https://", "")
    use_ssl = settings.s3_endpoint.startswith("https://")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{settings.minio_root_user}';")
    con.execute(f"SET s3_secret_access_key='{settings.minio_root_password}';")
    con.execute(f"SET s3_region='{settings.s3_region}';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
    con.execute("SET s3_url_style='path';")


@asset(
    partitions_def=daily_partitions,
    # Data flows via Iceberg — no value passed through Dagster's I/O manager.
    deps=[silver_prices_hourly],
    group_name="gold",
    compute_kind="duckdb",
    description=(
        "Daily price statistics per zone (min/max/avg/spread/peak/cheapest hour). "
        "Source for dashboards and smart-home decision logic."
    ),
)
def gold_prices_daily_stats(
    context,
    iceberg: IcebergCatalogResource,
) -> Output[None]:
    delivery_day = datetime.fromisoformat(context.partition_key).date()
    catalog = iceberg.get()

    silver_table = catalog.load_table((NAMESPACE, "prices_hourly"))
    # PyIceberg 0.8.x wants ISO strings, not `datetime.date`, for DateType literals.
    delivery_day_lit = delivery_day.isoformat()
    # Use the explicit batch-reader API — documented return type is a reader —
    # and materialise batches into a Table ourselves.
    scan = silver_table.scan(row_filter=EqualTo("delivery_date", delivery_day_lit))
    batches = list(scan.to_arrow_batch_reader())
    context.log.info(f"silver batches: {len(batches)}")
    arrow_silver = pa.Table.from_batches(batches) if batches else None

    if arrow_silver is None or arrow_silver.num_rows == 0:
        context.log.warning(f"Silver had no rows for {delivery_day}; skipping gold.")
        return Output(None, metadata={"rows_written": MetadataValue.int(0)})

    con = duckdb.connect()
    _configure_duckdb_for_minio(con)
    con.register("silver", arrow_silver)

    agg = con.execute(
        """
        WITH ranked AS (
            SELECT
                delivery_date,
                bidding_zone,
                ts_utc,
                price_eur_per_mwh,
                ROW_NUMBER() OVER (
                    PARTITION BY delivery_date, bidding_zone
                    ORDER BY price_eur_per_mwh ASC, ts_utc ASC
                ) AS rn_cheap,
                ROW_NUMBER() OVER (
                    PARTITION BY delivery_date, bidding_zone
                    ORDER BY price_eur_per_mwh DESC, ts_utc ASC
                ) AS rn_peak
            FROM silver
        )
        SELECT
            delivery_date,
            bidding_zone,
            MIN(price_eur_per_mwh)                                          AS min_price_eur_per_mwh,
            MAX(price_eur_per_mwh)                                          AS max_price_eur_per_mwh,
            AVG(price_eur_per_mwh)                                          AS avg_price_eur_per_mwh,
            MAX(price_eur_per_mwh) - MIN(price_eur_per_mwh)                 AS spread_eur_per_mwh,
            MAX(CASE WHEN rn_cheap = 1 THEN ts_utc END)                     AS cheapest_hour_utc,
            MAX(CASE WHEN rn_peak  = 1 THEN ts_utc END)                     AS peak_hour_utc,
            COUNT(*)                                                        AS n_intervals
        FROM ranked
        GROUP BY delivery_date, bidding_zone
        """
    ).fetch_arrow_table()
    context.log.info(f"agg type: {type(agg).__module__}.{type(agg).__name__}")

    computed_at = datetime.now(tz=timezone.utc).replace(microsecond=0)
    computed_at_col = pa.array([computed_at] * agg.num_rows, type=pa.timestamp("us", tz="UTC"))
    agg = agg.append_column("computed_at_utc", computed_at_col)

    # Cast to match Iceberg-derived Arrow types.
    agg = agg.cast(
        pa.schema(
            [
                pa.field("delivery_date", pa.date32(), nullable=False),
                pa.field("bidding_zone", pa.string(), nullable=False),
                pa.field("min_price_eur_per_mwh", pa.float64(), nullable=False),
                pa.field("max_price_eur_per_mwh", pa.float64(), nullable=False),
                pa.field("avg_price_eur_per_mwh", pa.float64(), nullable=False),
                pa.field("spread_eur_per_mwh", pa.float64(), nullable=False),
                pa.field("cheapest_hour_utc", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("peak_hour_utc", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("n_intervals", pa.int32(), nullable=False),
                pa.field("computed_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
            ]
        )
    )

    gold_table = _ensure_gold_table(catalog)
    gold_table.overwrite(agg, overwrite_filter=EqualTo("delivery_date", delivery_day_lit))

    return Output(
        None,
        metadata={
            "delivery_date": MetadataValue.text(delivery_day.isoformat()),
            "rows_written": MetadataValue.int(agg.num_rows),
            "snapshot_id": MetadataValue.text(
                str(gold_table.refresh().current_snapshot().snapshot_id)
            ),
        },
    )
