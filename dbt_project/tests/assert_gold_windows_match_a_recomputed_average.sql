-- Singular test: fails (returns rows) if a published cheapest window doesn't
-- match what the underlying 15-minute grid actually says.
--
-- int_gold_cheapest_windows computes its averages with a row-counted window
-- frame (`rows between current row and 4N-1 following`). This recomputes each
-- published window the other way round — a plain time-range join on
-- [window_start_utc, window_end_utc) — so a frame spanning the wrong number of
-- intervals shows up as either a wrong interval count or a wrong average.
--
-- The frame length is the specific thing under test: while the model assumed
-- hourly rows it used `4N-1` against a 15-minute grid and would have averaged
-- a quarter of the intended span.

with recomputed as (

    select
        w.delivery_date,
        w.bidding_zone,
        w.window_hours,
        w.window_start_utc,
        w.avg_price_eur_per_mwh as published_avg,
        avg(s.price_eur_per_mwh) as recomputed_avg,
        count(*)                 as n_intervals
    from {{ ref('int_gold_cheapest_windows') }} w
    join {{ ref('stg_silver_prices_15min') }} s
      on  s.delivery_date  = w.delivery_date
      and s.bidding_zone   = w.bidding_zone
      and s.ts_utc        >= w.window_start_utc
      and s.ts_utc         < w.window_end_utc
    group by 1, 2, 3, 4, 5

)

select *
from recomputed
where n_intervals != window_hours * 4
   or abs(published_avg - recomputed_avg) > 1e-6
