-- Singular test: fails (returns rows) if any row violates
-- min <= avg <= max, or spread != max - min. Mirrors
-- energy_pipeline.quality.check_gold_stats_consistency, which the
-- gold_prices_daily_stats Dagster asset also runs as a belt-and-suspenders
-- check right before publishing to Iceberg.

select *
from {{ ref('int_gold_prices_daily_stats') }}
where not (
    min_price_eur_per_mwh <= avg_price_eur_per_mwh
    and avg_price_eur_per_mwh <= max_price_eur_per_mwh
)
or abs(spread_eur_per_mwh - (max_price_eur_per_mwh - min_price_eur_per_mwh)) > 1e-6
