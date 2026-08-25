"""Gold layer (Phase 3): dbt computes it, these assets publish it to Iceberg.

dbt (dbt_project/models/gold/) owns the SQL, docs, and tests for the two gold
tables — that's the "replace gold-layer Python with dbt models" goal from
LEARNING.md's Phase 3. `gold_dbt_assets` runs `dbt build`, which reads silver
straight out of Iceberg (`iceberg_scan()` in the staging model — see
profiles.yml) and transforms it into DuckDB tables in a scratch `.duckdb`
file. The two plain `@asset`s below then read that scratch file back out and
push it into Iceberg using the exact same PyIceberg overwrite() pattern the
old Python gold assets used (see git log for the trail of PyIceberg/Arrow
type fixes that pattern took to get right).

dbt reads Iceberg but doesn't write it — deliberately: we had no way to
verify, in the environment this was built in (no network, no Docker), that a
dbt-duckdb Iceberg-write path would preserve the exact
schema/partition-spec/identifier-fields already committed to in
gold.py/gold_windows.py. Revisit once this has been run against the live
stack — see LEARNING.md's Phase 3 note.
"""

import json
from datetime import datetime

import duckdb
import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    MetadataValue,
    Output,
    asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
from pyiceberg.expressions import EqualTo

from energy_pipeline.assets.bronze import daily_partitions
from energy_pipeline.assets.gold import GOLD_ARROW_SCHEMA, ensure_gold_table
from energy_pipeline.assets.gold_windows import (
    GOLD_WINDOWS_ARROW_SCHEMA,
    WINDOW_HOURS,
    ensure_windows_table,
)
from energy_pipeline.dbt_resource import dbt_project
from energy_pipeline.iceberg_utils import write_version_hint
from energy_pipeline.quality import check_gold_stats_consistency
from energy_pipeline.resources import IcebergCatalogResource

DBT_DUCKDB_PATH = dbt_project.project_dir / "target" / "gold.duckdb"


class _BareNameDbtTranslator(DagsterDbtTranslator):
    """Asset key = bare dbt model name, no folder-based prefix.

    Keeps e.g. `int_gold_prices_daily_stats` addressable as a single-segment
    AssetKey so the publish assets below can `deps=` on it unambiguously,
    rather than relying on dagster-dbt's default (folder-prefixed) key
    scheme. Sources are the one exception: `silver_prices_hourly` (see
    dbt_project/models/staging/_sources.yml) needs to resolve to the *same*
    AssetKey as the real Dagster-native `silver_prices_hourly` asset, which
    is exactly what the base translator's handling of `meta.dagster.asset_key`
    already does — overriding it here would just break that mapping.
    """

    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            return super().get_asset_key(dbt_resource_props)
        return AssetKey(dbt_resource_props["name"])


@dbt_assets(
    manifest=dbt_project.manifest_path,
    partitions_def=daily_partitions,
    dagster_dbt_translator=_BareNameDbtTranslator(),
)
def gold_dbt_assets(context, dbt: DbtCliResource):
    delivery_day = datetime.fromisoformat(context.partition_key).date()
    dbt_vars = {"delivery_date": delivery_day.isoformat(), "window_hours": list(WINDOW_HOURS)}
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()


def _read_dbt_table(model_name: str, delivery_day_lit: str) -> pa.Table:
    con = duckdb.connect(str(DBT_DUCKDB_PATH), read_only=True)
    try:
        # model_name is always one of this module's own hardcoded literals
        # below, never external input.
        return con.execute(
            f"select * from {model_name} where delivery_date = ?", [delivery_day_lit]
        ).fetch_arrow_table()
    finally:
        con.close()


@asset(
    partitions_def=daily_partitions,
    deps=[AssetKey("int_gold_prices_daily_stats")],
    group_name="gold",
    compute_kind="iceberg",
    description=(
        "Daily price statistics per zone (min/max/avg/spread/peak/cheapest hour), computed "
        "by dbt and published here to Iceberg. Source for dashboards and smart-home decision "
        "logic."
    ),
    check_specs=[
        AssetCheckSpec(
            name="stats_consistency",
            asset="gold_prices_daily_stats",
            description=(
                "Every row satisfies min <= avg <= max and spread == max - min. Also enforced "
                "as a dbt singular test (assert_gold_daily_stats_bounds.sql) during the dbt "
                "build step above; this is a belt-and-suspenders check right before publish. "
                "WARN, not blocking."
            ),
        )
    ],
)
def gold_prices_daily_stats(context, iceberg: IcebergCatalogResource):
    delivery_day = datetime.fromisoformat(context.partition_key).date()
    delivery_day_lit = delivery_day.isoformat()

    agg = _read_dbt_table("int_gold_prices_daily_stats", delivery_day_lit)
    if agg.num_rows == 0:
        context.log.warning(f"dbt produced no rows for {delivery_day}; skipping publish.")
        yield AssetCheckResult(
            check_name="stats_consistency", passed=True, severity=AssetCheckSeverity.WARN
        )
        yield Output(None, metadata={"rows_written": MetadataValue.int(0)})
        return

    stats_columns = [
        "min_price_eur_per_mwh",
        "max_price_eur_per_mwh",
        "avg_price_eur_per_mwh",
        "spread_eur_per_mwh",
    ]
    stats_rows = agg.select(stats_columns).to_pylist()
    passed, violations = check_gold_stats_consistency(stats_rows)
    yield AssetCheckResult(
        check_name="stats_consistency",
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "violations": MetadataValue.int(violations),
            "rows_checked": MetadataValue.int(len(stats_rows)),
        },
    )

    agg = agg.cast(GOLD_ARROW_SCHEMA)

    catalog = iceberg.get()
    gold_table = ensure_gold_table(catalog)
    gold_table.overwrite(agg, overwrite_filter=EqualTo("delivery_date", delivery_day_lit))
    gold_table = gold_table.refresh()
    version = write_version_hint(gold_table)

    yield Output(
        None,
        metadata={
            "delivery_date": MetadataValue.text(delivery_day.isoformat()),
            "rows_written": MetadataValue.int(agg.num_rows),
            "snapshot_id": MetadataValue.text(str(gold_table.current_snapshot().snapshot_id)),
            "version_hint": MetadataValue.int(version),
        },
    )


@asset(
    partitions_def=daily_partitions,
    deps=[AssetKey("int_gold_cheapest_windows")],
    group_name="gold",
    compute_kind="iceberg",
    description=(
        "For each (delivery_date, bidding_zone, window_hours) the start time and average "
        "price of the cheapest contiguous N-hour block in the day, computed by dbt and "
        "published here to Iceberg. Source for smart-home load-shifting decisions."
    ),
)
def gold_cheapest_windows(context, iceberg: IcebergCatalogResource):
    delivery_day = datetime.fromisoformat(context.partition_key).date()
    delivery_day_lit = delivery_day.isoformat()

    final = _read_dbt_table("int_gold_cheapest_windows", delivery_day_lit)
    if final.num_rows == 0:
        context.log.warning(f"dbt produced no rows for {delivery_day}; skipping publish.")
        return Output(None, metadata={"rows_written": MetadataValue.int(0)})

    final = final.cast(GOLD_WINDOWS_ARROW_SCHEMA)

    catalog = iceberg.get()
    windows_table = ensure_windows_table(catalog)
    windows_table.overwrite(final, overwrite_filter=EqualTo("delivery_date", delivery_day_lit))
    windows_table = windows_table.refresh()
    version = write_version_hint(windows_table)

    return Output(
        None,
        metadata={
            "delivery_date": MetadataValue.text(delivery_day.isoformat()),
            "rows_written": MetadataValue.int(final.num_rows),
            "window_sizes": MetadataValue.json(list(WINDOW_HOURS)),
            "snapshot_id": MetadataValue.text(str(windows_table.current_snapshot().snapshot_id)),
            "version_hint": MetadataValue.int(version),
        },
    )
