-- Daily price statistics per zone (min/max/avg/spread/peak/cheapest hour).
-- Ported from the old assets/gold.py Python asset — same SQL, now dbt-owned
-- (tested + documented in _gold.yml). Published to Iceberg by the
-- `gold_prices_daily_stats` Dagster asset in assets/gold_dbt.py, which reads
-- this table back out; dbt doesn't write Iceberg directly (see LEARNING.md's
-- Phase 3 note for why).
--
-- `int_` prefix (dbt's convention for an intermediate model) keeps this
-- model's Dagster asset name distinct from the Iceberg-publishing asset,
-- which keeps the plain `gold_prices_daily_stats` name for continuity with
-- the rest of the docs/dashboards.

with ranked as (
    select
        delivery_date,
        bidding_zone,
        ts_utc,
        price_eur_per_mwh,
        row_number() over (
            partition by delivery_date, bidding_zone
            order by price_eur_per_mwh asc, ts_utc asc
        ) as rn_cheap,
        row_number() over (
            partition by delivery_date, bidding_zone
            order by price_eur_per_mwh desc, ts_utc asc
        ) as rn_peak
    from {{ ref('stg_silver_prices_hourly') }}
)

select
    delivery_date,
    bidding_zone,
    min(price_eur_per_mwh)                                   as min_price_eur_per_mwh,
    max(price_eur_per_mwh)                                   as max_price_eur_per_mwh,
    avg(price_eur_per_mwh)                                   as avg_price_eur_per_mwh,
    max(price_eur_per_mwh) - min(price_eur_per_mwh)          as spread_eur_per_mwh,
    max(case when rn_cheap = 1 then ts_utc end)              as cheapest_hour_utc,
    max(case when rn_peak  = 1 then ts_utc end)              as peak_hour_utc,
    count(*)                                                 as n_intervals,
    current_timestamp                                        as computed_at_utc
from ranked
group by delivery_date, bidding_zone
