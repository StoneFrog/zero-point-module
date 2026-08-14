"""Top-level Dagster definitions: assets + resources + schedule.

Dagster discovers everything via this single Definitions object.
"""

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource

from energy_pipeline.assets import bronze, gold_dbt, maintenance, silver
from energy_pipeline.assets.maintenance import lake_file_health_job, lake_file_health_schedule
from energy_pipeline.config import settings
from energy_pipeline.dbt_resource import dbt_project
from energy_pipeline.resources import EntsoeResource, IcebergCatalogResource
from energy_pipeline.sensors import pipeline_failure_sensor

# gold.py / gold_windows.py aren't in this list: since Phase 3 they hold only
# the Iceberg schema/table-lifecycle helpers gold_dbt.py's publish assets
# import, not @asset-decorated functions of their own — see those modules'
# docstrings.
all_assets = load_assets_from_modules([bronze, silver, gold_dbt])
maintenance_assets = load_assets_from_modules([maintenance])

# One job that materialises bronze -> silver -> dbt gold models -> Iceberg
# for the latest partition. Dagster figures out the order from the asset
# dependency graph.
# Scoped to `all_assets` (not AssetSelection.all()) so it doesn't try to pull
# in the unpartitioned maintenance assets below — those run on their own
# schedule via lake_file_health_job.
daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    selection=AssetSelection.assets(*all_assets),
    description=(
        "End-to-end refresh: ENTSO-E -> bronze -> silver -> dbt gold -> Iceberg for one "
        "delivery day."
    ),
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
    assets=[*all_assets, *maintenance_assets],
    jobs=[daily_pipeline_job, lake_file_health_job],
    schedules=[daily_schedule, lake_file_health_schedule],
    sensors=[pipeline_failure_sensor],
    resources={
        "entsoe": EntsoeResource(use_fixture=settings.entsoe_use_fixture),
        "iceberg": IcebergCatalogResource(),
        # profiles_dir explicit rather than relying on cwd-defaulting to the
        # project dir — profiles.yml lives there (see dbt_resource.py).
        "dbt": DbtCliResource(project_dir=dbt_project, profiles_dir=dbt_project.project_dir),
    },
)
