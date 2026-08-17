"""Bridges silver (Iceberg) to dbt's input (a local Parquet file).

Lives here — not in assets/gold_dbt.py — because it's about staging dbt's
*input*, not gold's output: it reads silver, dbt reads what this writes. See
dbt_project/models/staging/stg_silver_prices_hourly.sql on the other side of
the handoff, and dbt_project/profiles.yml's header comment for why a local
Parquet file instead of DuckDB's `iceberg_scan()`.
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import EqualTo

from energy_pipeline.assets.silver import NAMESPACE, SILVER_ARROW_SCHEMA, TABLE_NAME
from energy_pipeline.dbt_resource import dbt_project
from energy_pipeline.resources import IcebergCatalogResource

SILVER_PARQUET_PATH = dbt_project.project_dir / "target" / "silver_partition.parquet"

# Subset of SILVER_ARROW_SCHEMA (silver.py) — only the columns the dbt
# staging model reads — selected by name rather than re-declared, so it
# can't drift from silver's actual schema.
STAGING_COLUMNS = (
    "ts_utc",
    "delivery_date",
    "bidding_zone",
    "resolution_minutes",
    "price_eur_per_mwh",
)
SILVER_STAGING_SCHEMA = pa.schema([SILVER_ARROW_SCHEMA.field(name) for name in STAGING_COLUMNS])


def write_silver_partition_parquet(iceberg: IcebergCatalogResource, delivery_day_lit: str) -> int:
    """Scan one partition's silver rows out of Iceberg, hand them to dbt as Parquet.

    Same PyIceberg scan pattern the old Python gold assets used. Always
    writes a (possibly empty) file with the right schema, so dbt's
    `read_parquet()` never hits a missing-file error. Returns the row count.
    """
    catalog = iceberg.get()
    silver_table = catalog.load_table((NAMESPACE, TABLE_NAME))
    scan = silver_table.scan(row_filter=EqualTo("delivery_date", delivery_day_lit))
    batches = list(scan.to_arrow_batch_reader())
    arrow_silver = pa.Table.from_batches(batches) if batches else None

    if arrow_silver is None or arrow_silver.num_rows == 0:
        arrow_silver = pa.Table.from_pylist([], schema=SILVER_STAGING_SCHEMA)
    else:
        arrow_silver = arrow_silver.select(SILVER_STAGING_SCHEMA.names)

    SILVER_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow_silver, str(SILVER_PARQUET_PATH))
    return arrow_silver.num_rows
