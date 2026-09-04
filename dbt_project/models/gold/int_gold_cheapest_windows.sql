-- Cheapest contiguous N-hour price window per (delivery_date, bidding_zone,
-- window_hours). Ported from assets/gold_windows.py's `_build_windows_sql` +
-- main query — same DuckDB SQL (including the `INTERVAL (window_hours) HOUR`
-- computed-interval syntax, which needs a column reference, not a Jinja
-- literal), just generated with Jinja instead of an f-string, and now
-- dbt-tested/documented. Published to Iceberg by the `gold_cheapest_windows`
-- Dagster asset in assets/gold_dbt.py.
--
-- Window frame bounds must be constants in standard SQL, so each window size
-- gets its own UNION ALL branch (can't CROSS JOIN a window-sizes table and
-- have the frame reference a runtime column) — see the Jinja loop below.

{% set window_hours = var('window_hours', [1, 2, 3, 4, 6, 8]) %}

with all_windows as (
    {% for n in window_hours %}
    select
        delivery_date,
        bidding_zone,
        {{ n }} as window_hours,
        ts_utc as window_start_utc,
        avg(price_eur_per_mwh) over w_{{ n }} as window_avg,
        count(*) over w_{{ n }} as window_count
    from {{ ref('stg_silver_prices_15min') }}
    where resolution_minutes = 60
    window w_{{ n }} as (
        partition by delivery_date, bidding_zone
        order by ts_utc
        rows between current row and {{ n - 1 }} following
    )
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by delivery_date, bidding_zone, window_hours
            order by window_avg asc, window_start_utc asc
        ) as rn
    from all_windows
    -- Discard incomplete windows clipped by the end of the day.
    where window_count = window_hours
)

select
    delivery_date,
    bidding_zone,
    window_hours,
    window_start_utc,
    window_start_utc + INTERVAL (window_hours) HOUR  as window_end_utc,
    window_avg                                       as avg_price_eur_per_mwh,
    current_timestamp                                as computed_at_utc
from ranked
where rn = 1
