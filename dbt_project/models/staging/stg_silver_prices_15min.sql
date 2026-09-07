{{ config(materialized='ephemeral') }}

-- One partition's worth of silver rows, read through the Iceberg REST
-- catalog attached as `lake` in profiles.yml. `source()` resolves to
-- lake.energy.prices_hourly, so this is a plain table reference and the
-- catalog decides which snapshot it means — the same catalog PyIceberg
-- commits through in assets/silver.py, which is what makes reader and writer
-- agree by construction rather than by convention.
--
-- Every row out of this model is exactly one 15-minute interval, whatever
-- the publisher sent. Zones publish on different grids — most on a
-- 15-minute market time unit, a couple (CH, IE_SEM) still hourly — and a
-- single zone can even change grid mid-day across an MTU transition. Left
-- as-is that variability leaks into every downstream model: a window frame
-- counted in rows means something different per zone, and `avg(price)` is
-- only time-weighted while a zone's resolution happens to be uniform.
-- Re-gridding once, here, means gold never has to ask what resolution a row
-- came from.
--
-- Deliberately *not* done in silver: silver's job is to record faithfully
-- what ENTSO-E published, and expanding an hourly price into four
-- quarter-hourly rows there would make a genuinely hourly zone
-- indistinguishable from one that priced each quarter-hour flat. The
-- published resolution stays recorded in silver's own `resolution_minutes`
-- column; this model is where it stops mattering.
--
-- `delivery_date` is required, not defaulted: every dbt invocation must be
-- explicit about which partition it's building, mirroring the Dagster
-- daily-partitioned execution model upstream (see assets/gold_dbt.py, which
-- passes it via `--vars`).

with published as (

    select
        ts_utc,
        delivery_date,
        bidding_zone,
        resolution_minutes,
        price_eur_per_mwh
    from {{ source('lake', 'silver_prices_hourly') }}
    where delivery_date = date '{{ var("delivery_date") }}'

)

select
    -- One row per 15-minute step within the published interval: an hourly
    -- price becomes four quarter-hours carrying that same price.
    published.ts_utc + (step.i * interval 15 minute) as ts_utc,
    published.delivery_date,
    published.bidding_zone,
    -- Constant by construction — kept so the grid is self-describing to
    -- anything reading this CTE, not because the publisher said 15.
    15                                              as resolution_minutes,
    published.price_eur_per_mwh
from published
-- greatest(..., 1) so a resolution finer than 15 minutes can't silently
-- delete rows by producing an empty range. Nothing publishes finer than
-- 15 today; if that changes, silver's row_integrity check fails on the
-- interval count rather than this quietly dropping data.
cross join range(0, greatest(published.resolution_minutes // 15, 1)) as step(i)
