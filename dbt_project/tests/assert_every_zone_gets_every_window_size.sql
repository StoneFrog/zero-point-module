-- Singular test: fails (returns rows) if a zone with enough intervals for a
-- given window size didn't get a cheapest window for it.
--
-- This is the regression test for the bug that made this layer look healthy
-- while being almost entirely empty: int_gold_cheapest_windows carried
-- `where resolution_minutes = 60`, which — once 38 of 40 zones moved to a
-- 15-minute market time unit — silently dropped every one of them and left 12
-- rows where ~240 were meant. Nothing failed; the model just quietly stopped
-- covering Europe. A missing zone is only visible against an expectation, so
-- that expectation is written down here.

{% set window_hours = var('window_hours') %}

with zone_days as (

    select delivery_date, bidding_zone, count(*) as n_intervals
    from {{ ref('stg_silver_prices_15min') }}
    group by 1, 2

),

sizes as (
    {% for n in window_hours %}
    select {{ n }} as window_hours
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

expected as (

    select zone_days.delivery_date, zone_days.bidding_zone, sizes.window_hours
    from zone_days
    cross join sizes
    -- A day too short to hold the window legitimately produces no row.
    where zone_days.n_intervals >= sizes.window_hours * 4

)

select expected.*
from expected
left join {{ ref('int_gold_cheapest_windows') }} published
  on  published.delivery_date = expected.delivery_date
  and published.bidding_zone  = expected.bidding_zone
  and published.window_hours  = expected.window_hours
where published.bidding_zone is null
