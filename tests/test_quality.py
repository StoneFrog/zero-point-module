"""Unit tests for the pure data-quality checks (no S3/Iceberg/Dagster needed)."""

from datetime import datetime, timezone

from energy_pipeline.quality import (
    check_gold_stats_consistency,
    check_silver_row_integrity,
    check_zone_completeness,
)


def _row(zone: str, interval: int, resolution: int = 60, price: float | None = 50.0) -> dict:
    """Build a row for the `interval`-th slot of the day at the given resolution."""
    minutes_from_midnight = interval * resolution
    hour, minute = divmod(minutes_from_midnight, 60)
    return {
        "ts_utc": datetime(2026, 5, 4, hour % 24, minute, tzinfo=timezone.utc),
        "bidding_zone": zone,
        "resolution_minutes": resolution,
        "price_eur_per_mwh": price,
    }


def test_zone_completeness_passes_when_required_zones_present():
    passed, missing = check_zone_completeness(["PL", "DE_LU", "FR"])
    assert passed
    assert missing == []


def test_zone_completeness_flags_missing_required_zone():
    passed, missing = check_zone_completeness(["FR", "BE"])
    assert not passed
    assert missing == ["PL", "DE_LU"]


def test_silver_row_integrity_passes_for_a_clean_24h_day():
    rows = [_row("PL", h) for h in range(24)]
    passed, details = check_silver_row_integrity(rows)
    assert passed
    assert details == {"duplicate_keys": 0, "null_prices": 0, "zones_out_of_bounds": []}


def test_silver_row_integrity_tolerates_dst_short_day():
    # Spring-forward: 23 hourly intervals instead of 24.
    rows = [_row("DE_LU", h) for h in range(23)]
    passed, _ = check_silver_row_integrity(rows)
    assert passed


def test_silver_row_integrity_tolerates_quarter_hourly_resolution():
    rows = [_row("DE_LU", i, resolution=15) for i in range(96)]
    passed, _ = check_silver_row_integrity(rows)
    assert passed


def test_silver_row_integrity_flags_duplicate_key():
    rows = [_row("PL", 0), _row("PL", 0)]
    passed, details = check_silver_row_integrity(rows)
    assert not passed
    assert details["duplicate_keys"] == 1


def test_silver_row_integrity_flags_null_price():
    rows = [_row("PL", h, price=None) for h in range(24)]
    passed, details = check_silver_row_integrity(rows)
    assert not passed
    assert details["null_prices"] == 24


def test_silver_row_integrity_flags_zone_with_too_few_intervals():
    rows = [_row("PL", h) for h in range(3)]
    passed, details = check_silver_row_integrity(rows)
    assert not passed
    assert details["zones_out_of_bounds"] == ["PL"]


def test_gold_stats_consistency_passes_for_valid_row():
    rows = [
        {
            "min_price_eur_per_mwh": 10.0,
            "max_price_eur_per_mwh": 90.0,
            "avg_price_eur_per_mwh": 50.0,
            "spread_eur_per_mwh": 80.0,
        }
    ]
    passed, violations = check_gold_stats_consistency(rows)
    assert passed
    assert violations == 0


def test_gold_stats_consistency_flags_avg_outside_min_max():
    rows = [
        {
            "min_price_eur_per_mwh": 10.0,
            "max_price_eur_per_mwh": 90.0,
            "avg_price_eur_per_mwh": 100.0,
            "spread_eur_per_mwh": 80.0,
        }
    ]
    passed, violations = check_gold_stats_consistency(rows)
    assert not passed
    assert violations == 1


def test_gold_stats_consistency_flags_wrong_spread():
    rows = [
        {
            "min_price_eur_per_mwh": 10.0,
            "max_price_eur_per_mwh": 90.0,
            "avg_price_eur_per_mwh": 50.0,
            "spread_eur_per_mwh": 79.0,
        }
    ]
    passed, violations = check_gold_stats_consistency(rows)
    assert not passed
    assert violations == 1
