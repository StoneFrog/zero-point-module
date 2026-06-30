"""Bronze layer: raw ENTSO-E XML, stored byte-for-byte in MinIO.

Why immutable raw storage: silver/gold logic changes over time. Keeping the
unmodified upstream payload means you can always re-derive the cleaned layers
without re-paying API quotas or losing history.

Layout: s3://{bucket}/bronze/entsoe/day_ahead/delivery_date=YYYY-MM-DD/zone=PL/raw.xml
The Hive-style key=value path is convention; downstream tools can introspect it.
"""

from datetime import datetime

import s3fs
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    asset,
)

from energy_pipeline.config import settings
from energy_pipeline.entsoe.zones import ZONES
from energy_pipeline.resources import EntsoeResource

# Backfill horizon: ENTSO-E day-ahead data goes back to ~2015. Starting from
# 2020-01-01 gives us six years of history for forecasting + comparison.
# A full backfill is ~6y * 365d * 40 zones = ~88k API calls (ENTSO-E rate-limits
# at 400/min, so ~4h of API time). Launch via Dagster's Backfills UI.
daily_partitions = DailyPartitionsDefinition(start_date="2020-01-01")


def _s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.minio_root_user,
        secret=settings.minio_root_password,
        client_kwargs={"endpoint_url": settings.s3_endpoint},
    )


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    compute_kind="entsoe",
    description=(
        "Raw ENTSO-E day-ahead price XML responses. One XML file per (delivery_date, "
        "zone). Stored exactly as received from the API; never mutated."
    ),
)
def bronze_entsoe_day_ahead(
    context: AssetExecutionContext,
    entsoe: EntsoeResource,
) -> Output[dict]:
    delivery_day = datetime.fromisoformat(context.partition_key).date()
    client = entsoe.client()
    fs = _s3_fs()

    written: list[str] = []
    failed: list[dict[str, str]] = []
    total_bytes = 0

    for zone in ZONES:
        try:
            xml_bytes = client.fetch_day_ahead_prices(zone.eic, delivery_day)
        except Exception as exc:
            context.log.warning(f"Fetch failed for {zone.code} ({zone.name}): {exc}")
            failed.append({"zone": zone.code, "error": str(exc)[:200]})
            continue

        key = (
            f"{settings.lake_bucket}/bronze/entsoe/day_ahead/"
            f"delivery_date={delivery_day.isoformat()}/zone={zone.code}/raw.xml"
        )
        with fs.open(key, "wb") as f:
            f.write(xml_bytes)
        written.append(zone.code)
        total_bytes += len(xml_bytes)

    return Output(
        value={
            "delivery_date": delivery_day.isoformat(),
            "zones_written": written,
            "zones_failed": failed,
        },
        metadata={
            "delivery_date": MetadataValue.text(delivery_day.isoformat()),
            "zones_written_count": MetadataValue.int(len(written)),
            "zones_failed_count": MetadataValue.int(len(failed)),
            "total_bytes": MetadataValue.int(total_bytes),
            "s3_prefix": MetadataValue.path(
                f"s3://{settings.lake_bucket}/bronze/entsoe/day_ahead/"
                f"delivery_date={delivery_day.isoformat()}/"
            ),
        },
    )
