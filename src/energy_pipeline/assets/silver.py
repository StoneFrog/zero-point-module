"""Silver layer: parsed, typed prices in an Iceberg table.

We read the bronze XML files for the partition, parse each into typed
PricePoint records, then upsert into the Iceberg table by overwriting only the
slice for that delivery_date. Idempotent: re-running a partition produces the
same final state.

Iceberg gives us: schema enforcement, atomic snapshot swaps, time travel,
hidden partitioning, and the ability to evolve schema/partitioning later
without rewriting historical files.
"""

from datetime import date, datetime, timezone

import pyarrow as pa
import s3fs
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
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

from energy_pipeline.assets.bronze import bronze_entsoe_day_ahead, daily_partitions
from energy_pipeline.config import settings
from energy_pipeline.entsoe.parser import parse_day_ahead_xml
from energy_pipeline.entsoe.zones import ZONES_BY_CODE
from energy_pipeline.resources import IcebergCatalogResource

NAMESPACE = "energy"
TABLE_NAME = "prices_hourly"
TABLE_IDENTIFIER = (NAMESPACE, TABLE_NAME)

# Field IDs are stable across schema changes — that's how Iceberg tracks
# rename/drop without rewriting data. Pick them once and don't reuse.
SILVER_SCHEMA = Schema(
    NestedField(1, "ts_utc", TimestamptzType(), required=True),
    NestedField(2, "delivery_date", DateType(), required=True),
    NestedField(3, "bidding_zone", StringType(), required=True),
    NestedField(4, "resolution_minutes", IntegerType(), required=True),
    NestedField(5, "price_eur_per_mwh", DoubleType(), required=True),
    NestedField(6, "currency", StringType(), required=True),
    NestedField(7, "measure_unit", StringType(), required=True),
    NestedField(8, "ingested_at_utc", TimestamptzType(), required=True),
    identifier_field_ids=[1, 3],  # logical primary key
)

# Day-level partitioning matches our daily overwrite pattern: a re-run rewrites
# exactly one tiny file per zone, no write amplification. Tradeoff: many small
# files over time — periodic compaction is on the roadmap (Phase 2).
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="day"),
    PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="bidding_zone"),
)

# PyArrow schema mirroring the Iceberg schema. Used to build the in-memory
# table we then hand to PyIceberg's writer.
SILVER_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("delivery_date", pa.date32(), nullable=False),
        pa.field("bidding_zone", pa.string(), nullable=False),
        pa.field("resolution_minutes", pa.int32(), nullable=False),
        pa.field("price_eur_per_mwh", pa.float64(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("measure_unit", pa.string(), nullable=False),
        pa.field("ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def _ensure_table(catalog: Catalog):
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(TABLE_IDENTIFIER)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=TABLE_IDENTIFIER,
            schema=SILVER_SCHEMA,
            partition_spec=SILVER_PARTITION_SPEC,
            location=f"s3://{settings.lake_bucket}/silver/{TABLE_NAME}",
        )


def _s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.minio_root_user,
        secret=settings.minio_root_password,
        client_kwargs={"endpoint_url": settings.s3_endpoint},
    )


def _read_bronze_partition(delivery_day: date) -> dict[str, bytes]:
    """Return {zone_code: xml_bytes} for the given partition."""
    fs = _s3_fs()
    prefix = (
        f"{settings.lake_bucket}/bronze/entsoe/day_ahead/"
        f"delivery_date={delivery_day.isoformat()}/"
    )
    out: dict[str, bytes] = {}
    for entry in fs.ls(prefix):
        # entry like '<bucket>/bronze/.../zone=PL'
        if not entry.endswith("/"):
            entry = entry + "/"
        zone_part = entry.rstrip("/").split("/")[-1]
        if not zone_part.startswith("zone="):
            continue
        zone_code = zone_part[len("zone=") :]
        raw_path = entry + "raw.xml"
        if fs.exists(raw_path):
            with fs.open(raw_path, "rb") as f:
                out[zone_code] = f.read()
    return out


@asset(
    partitions_def=daily_partitions,
    # deps= (not ins=): the real "output" of bronze is XML in MinIO, not the
    # Output(dict) we return. Using deps avoids Dagster's I/O manager trying
    # to load bronze's None-typed return from disk.
    deps=[bronze_entsoe_day_ahead],
    group_name="silver",
    compute_kind="iceberg",
    description=(
        "Hourly day-ahead prices, parsed from bronze XML and stored as an Iceberg table. "
        "Partitioned by day(ts_utc) + bidding_zone. Re-running a partition is a "
        "transactional upsert (overwrite by delivery_date)."
    ),
)
def silver_prices_hourly(
    context,
    iceberg: IcebergCatalogResource,
) -> Output[None]:
    delivery_day = datetime.fromisoformat(context.partition_key).date()

    bronze_files = _read_bronze_partition(delivery_day)
    if not bronze_files:
        context.log.warning(f"No bronze files for {delivery_day}")
        return Output(None, metadata={"rows_written": MetadataValue.int(0)})

    ingested_at = datetime.now(tz=timezone.utc).replace(microsecond=0)
    rows: list[dict] = []
    parse_failures: list[dict] = []

    for zone_code, xml_bytes in bronze_files.items():
        if zone_code not in ZONES_BY_CODE:
            context.log.warning(f"Unknown zone in bronze: {zone_code}")
            continue
        try:
            points = parse_day_ahead_xml(xml_bytes)
        except Exception as exc:
            parse_failures.append({"zone": zone_code, "error": str(exc)[:200]})
            continue
        for p in points:
            rows.append(
                {
                    "ts_utc": p.ts_utc,
                    "delivery_date": delivery_day,
                    "bidding_zone": zone_code,
                    "resolution_minutes": p.resolution_minutes,
                    "price_eur_per_mwh": p.price_eur_per_mwh,
                    "currency": p.currency,
                    "measure_unit": p.measure_unit,
                    "ingested_at_utc": ingested_at,
                }
            )

    if not rows:
        context.log.warning(f"No rows parsed for {delivery_day}")
        return Output(None, metadata={"rows_written": MetadataValue.int(0)})

    arrow_table = pa.Table.from_pylist(rows, schema=SILVER_ARROW_SCHEMA)
    catalog = iceberg.get()
    table = _ensure_table(catalog)
    # Atomic: delete rows for this delivery_date, then append new ones, in one snapshot.
    # PyIceberg 0.8.x rejects `datetime.date` as an EqualTo literal for DateType
    # columns — pass ISO string, which the literal factory recognises.
    table.overwrite(arrow_table, overwrite_filter=EqualTo("delivery_date", delivery_day.isoformat()))

    return Output(
        None,
        metadata={
            "delivery_date": MetadataValue.text(delivery_day.isoformat()),
            "rows_written": MetadataValue.int(len(rows)),
            "zones_parsed": MetadataValue.int(len(bronze_files) - len(parse_failures)),
            "parse_failures": MetadataValue.json(parse_failures),
            "snapshot_id": MetadataValue.text(str(table.refresh().current_snapshot().snapshot_id)),
        },
    )
