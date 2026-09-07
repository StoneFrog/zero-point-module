-- Singular test: fails (returns rows) if stg_silver_prices_15min is not a
-- uniform 15-minute grid for every (delivery_date, bidding_zone).
--
-- This is the invariant int_gold_cheapest_windows depends on. That model's
-- frame counts *rows*, so an N-hour window is 4N rows only while every row is
-- exactly one quarter-hour: a duplicated timestamp, a gap, or a row that kept
-- its published 60-minute resolution would each make the frame span the wrong
-- amount of time while still returning a plausible-looking price.
--
-- date_diff of 0 catches duplicates, anything other than 15 catches both gaps
-- and an un-expanded coarse interval. The last row of each zone-day has no
-- successor, hence the null guard.

with ordered as (

    select
        delivery_date,
        bidding_zone,
        ts_utc,
        resolution_minutes,
        lead(ts_utc) over (
            partition by delivery_date, bidding_zone order by ts_utc
        ) as next_ts_utc
    from {{ ref('stg_silver_prices_15min') }}

)

select *
from ordered
where resolution_minutes != 15
   or (next_ts_utc is not null and date_diff('minute', ts_utc, next_ts_utc) != 15)
