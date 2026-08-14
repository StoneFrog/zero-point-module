{{ config(materialized='ephemeral') }}

-- One partition's worth of silver rows. assets/gold_dbt.py reads them from
-- Iceberg via PyIceberg (same scan the old Python gold assets used) and
-- writes them to this local Parquet file *before* invoking dbt — see that
-- module and profiles.yml's header comment for why dbt reads a plain local
-- file here instead of calling `iceberg_scan()` directly.

select
    ts_utc,
    delivery_date,
    bidding_zone,
    resolution_minutes,
    price_eur_per_mwh
from read_parquet('{{ var("silver_parquet_path") }}')
