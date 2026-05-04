"""Top-level Dagster definitions: assets + resources + schedule.

Dagster discovers everything via this single Definitions object.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from energy_pipeline.assets import bronze, gold, silver
from energy_pipeline.config import settings
from energy_pipeline.resources import EntsoeResource, IcebergCatalogResource

all_assets = load_assets_from_modules([bronze, silver, gold])

# One job that materialises bronze -> silver -> gold for the latest partition.
# Dagster figures out the order from the asset dependency graph.
daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    selection=AssetSelection.all(),
    description="End-to-end refresh: ENTSO-E -> bronze -> silver -> gold for one delivery day.",
)

# ENTSO-E publishes day-ahead at ~12:45 CET; run at 13:30 to be safe.
# Cron is in UTC inside containers; 13:30 CET = 11:30 UTC (winter) / 12:30 UTC (summer).
# We use 12:30 UTC and accept a 1h delay in winter — simpler than DST-aware logic.
daily_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="30 12 * * *",
    description="Run the daily pipeline at 12:30 UTC (after ENTSO-E day-ahead publication).",
)

defs = Definitions(
    assets=all_assets,
    jobs=[daily_pipeline_job],
    schedules=[daily_schedule],
    resources={
        "entsoe": EntsoeResource(use_fixture=settings.entsoe_use_fixture),
        "iceberg": IcebergCatalogResource(),
    },
)
