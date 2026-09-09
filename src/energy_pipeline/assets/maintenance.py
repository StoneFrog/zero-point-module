"""Lakehouse file-count monitoring.

Day+zone partitioning on silver/gold (see silver.py) writes one small Parquet
file per (day, zone) per commit. Under the current daily-overwrite pattern
that never grows *within* a partition — each re-run replaces the one file for
that partition, it doesn't add to it — but the partition count itself grows
by roughly (zones × 365) files a year, and each file holds at most a day's
worth of hourly rows. That's the "many small files" tradeoff LEARNING.md
flags under day partitioning.

We don't auto-compact here. PyIceberg 0.8.1 (pinned in pyproject.toml) has no
native `rewrite_data_files`/compaction action — the feature request has been
open upstream since apache/iceberg-python#1092 and still wasn't part of the
0.9.0 release. The alternative, hand-rolling file consolidation against
PyIceberg's low-level transaction API, isn't something we're willing to ship
without a live catalog to test it against (getting it wrong risks silently
losing rows). So instead: watch file count / average file size per table and
surface it in Dagster, so a human can judge when it's worth a manual
rebuild (e.g. a one-off DuckDB CTAS + table recreation) or when to revisit
this once PyIceberg ships real compaction support.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    DefaultScheduleStatus,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from energy_pipeline.config import settings
from energy_pipeline.iceberg_utils import count_data_files

# (label, table's S3 prefix relative to the lake bucket) — must match the
# `location=` each asset passes to `catalog.create_table(...)`.
MONITORED_TABLES = (
    ("silver.prices_hourly", "silver/prices_hourly"),
    ("gold.prices_daily_stats", "gold/prices_daily_stats"),
    ("gold.prices_cheapest_windows", "gold/prices_cheapest_windows"),
)

# Below this average file size, a table has enough tiny files to be worth a
# manual look. Chosen as "clearly smaller than one row-group's worth of
# hourly price data would need to be", not a hard operational threshold.
SMALL_FILE_THRESHOLD_BYTES = 1_000_000


@asset(
    group_name="maintenance",
    compute_kind="s3",
    description=(
        "File count and average file size per managed Iceberg table, from a direct S3 "
        "listing. Monitoring only — see the module docstring for why this doesn't "
        "compact automatically."
    ),
    check_specs=[
        AssetCheckSpec(
            name="small_file_check",
            asset="lake_file_health",
            description=(
                "WARN when a table's average file size drops below "
                f"{SMALL_FILE_THRESHOLD_BYTES:,} bytes, i.e. it's accumulating many tiny "
                "files worth a manual compaction pass."
            ),
        )
    ],
)
def lake_file_health(context):
    stats: dict[str, dict[str, int]] = {}
    for label, table_prefix in MONITORED_TABLES:
        file_count, total_bytes = count_data_files(settings.lake_bucket, table_prefix)
        avg_bytes = total_bytes // file_count if file_count else 0
        stats[label] = {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "avg_file_size_bytes": avg_bytes,
        }
        context.log.info(
            f"{label}: {file_count} files, {total_bytes} bytes, avg {avg_bytes} bytes/file"
        )

    small_file_tables = {
        label: s["avg_file_size_bytes"]
        for label, s in stats.items()
        if s["file_count"] > 0 and s["avg_file_size_bytes"] < SMALL_FILE_THRESHOLD_BYTES
    }
    yield AssetCheckResult(
        check_name="small_file_check",
        passed=not small_file_tables,
        severity=AssetCheckSeverity.WARN,
        metadata={"small_file_tables": MetadataValue.json(small_file_tables)},
    )

    metadata = {}
    for label, s in stats.items():
        metadata[f"{label}/file_count"] = MetadataValue.int(s["file_count"])
        metadata[f"{label}/avg_file_size_bytes"] = MetadataValue.int(s["avg_file_size_bytes"])

    yield Output(stats, metadata=metadata)


lake_file_health_job = define_asset_job(
    name="lake_file_health_job",
    selection=[lake_file_health],
    description="Report Iceberg table file counts / sizes for small-file monitoring.",
)

lake_file_health_schedule = ScheduleDefinition(
    job=lake_file_health_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="Europe/Brussels",
    description=(
        "Weekly (Monday 06:00 Brussels) file-count / small-file check across managed tables."
    ),
    default_status=DefaultScheduleStatus.RUNNING,
)
