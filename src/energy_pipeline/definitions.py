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

from energy_pipeline.assets import bronze, gold, gold_windows, silver
from energy_pipeline.config import settings
from energy_pipeline.resources import EntsoeResource, IcebergCatalogResource

all_assets = load_assets_from_modules([bronze, silver, gold, gold_windows])

# One job that materialises bronze -> silver -> gold for the latest partition.
# Dagster figures out the order from the asset dependency graph.
daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    selection=AssetSelection.all(),
    description="End-to-end refresh: ENTSO-E -> bronze -> silver -> gold for one delivery day.",
)

# ENTSO-E publishes day-ahead at ~12:45 Brussels local time (CET in winter,
# CEST in summer). Schedule against the market timezone so the absolute UTC
# offset shifts automatically across DST; Dagster handles the conversion.
daily_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="30 13 * * *",
    execution_timezone="Europe/Brussels",
    description="Run the daily pipeline at 13:30 Brussels local time, ~45min after ENTSO-E day-ahead publication.",
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
