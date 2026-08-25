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
--
-- The line below registers this model's dependency on the
-- silver_prices_hourly *dbt source* (see _sources.yml) for dagster-dbt's
-- asset graph — dbt's Jinja renderer still evaluates Jinja expressions
-- written inside a SQL comment (unlike dbt's dedicated Jinja-comment
-- syntax, which is stripped entirely before evaluation and would silently
-- drop this), so the dependency gets recorded in the manifest even though
-- source()'s actual return value (a plain table reference) isn't usable
-- here — iceberg_scan() below needs a literal S3 path, not a table name.
-- Without this line, gold_dbt_assets has no recorded dependency on
-- silver_prices_hourly at all, and Dagster has no reason to run silver
-- before gold in the same job.
-- {{ source('lake', 'silver_prices_hourly') }}

select
    ts_utc,
    delivery_date,
    bidding_zone,
    resolution_minutes,
    price_eur_per_mwh
from iceberg_scan('s3://{{ env_var("LAKE_BUCKET") }}/silver/prices_hourly')
where delivery_date = date '{{ var("delivery_date") }}'
