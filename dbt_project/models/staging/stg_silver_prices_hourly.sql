{{ config(materialized='ephemeral') }}

-- One partition's worth of silver rows, read straight from the Iceberg
-- table's S3 location via DuckDB's iceberg extension — no catalog round
-- trip needed for reads (same as README's "Inspecting the lake from the
-- CLI" example). See profiles.yml's header comment for the duckdb version
-- this needs.
--
-- `delivery_date` is required, not defaulted: every dbt invocation must be
-- explicit about which partition it's building, mirroring the Dagster
-- daily-partitioned execution model upstream (see assets/gold_dbt.py, which
-- passes it via `--vars`).

select
    ts_utc,
    delivery_date,
    bidding_zone,
    resolution_minutes,
    price_eur_per_mwh
from iceberg_scan('s3://{{ env_var("LAKE_BUCKET") }}/silver/prices_hourly')
where delivery_date = date '{{ var("delivery_date") }}'
