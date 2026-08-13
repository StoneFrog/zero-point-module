"""Pure data-quality validation logic, shared by the bronze/silver/gold assets.

Kept dependency-light (no Dagster, S3, or Iceberg imports) and free of I/O so
it's cheap to unit test without a running stack. The assets call these and
wrap the results as `AssetCheckResult`s via `check_specs=` — see bronze.py,
silver.py, gold.py.
"""

from collections import Counter

# Zones we never want silently missing from a day's ingest. Not every zone
# needs to succeed every day (ENTSO-E has occasional per-zone outages), but
# these two anchor most downstream use cases (see LEARNING.md's "AssetCheck
# gates" note).
REQUIRED_ZONES = ("PL", "DE_LU")


def check_zone_completeness(zones_written: list[str]) -> tuple[bool, list[str]]:
    """Did the required zones show up in bronze for this partition?

    Returns (passed, missing_required_zones).
    """
    written = set(zones_written)
    missing = [z for z in REQUIRED_ZONES if z not in written]
    return not missing, missing


def check_silver_row_integrity(rows: list[dict]) -> tuple[bool, dict]:
    """Validate parsed silver rows before they're written to Iceberg.

    Checks the identifier_field_ids invariant (ts_utc, bidding_zone) has no
    duplicates, no null prices (violates the NOT NULL schema), and that each
    zone's interval count is plausible for its resolution — hourly zones
    should have ~24 intervals (23/25 across DST transitions), quarter-hourly
    zones ~96 (see LEARNING.md's sub-hourly-resolution note on DE-LU).
    """
    seen_keys: set[tuple] = set()
    duplicate_keys = 0
    null_prices = 0
    count_by_zone: Counter[str] = Counter()
    resolution_by_zone: dict[str, int] = {}

    for row in rows:
        key = (row["ts_utc"], row["bidding_zone"])
        if key in seen_keys:
            duplicate_keys += 1
        seen_keys.add(key)

        if row["price_eur_per_mwh"] is None:
            null_prices += 1

        zone = row["bidding_zone"]
        count_by_zone[zone] += 1
        resolution_by_zone.setdefault(zone, row["resolution_minutes"])

    out_of_bounds_zones = []
    for zone, count in count_by_zone.items():
        resolution = resolution_by_zone[zone] or 60
        expected = round(24 * 60 / resolution)
        tolerance = max(4, expected // 20)  # DST shifts by one interval or so
        if abs(count - expected) > tolerance:
            out_of_bounds_zones.append(zone)

    details = {
        "duplicate_keys": duplicate_keys,
        "null_prices": null_prices,
        "zones_out_of_bounds": out_of_bounds_zones,
    }
    passed = duplicate_keys == 0 and null_prices == 0 and not out_of_bounds_zones
    return passed, details


def check_gold_stats_consistency(rows: list[dict], tolerance: float = 1e-6) -> tuple[bool, int]:
    """Sanity-check gold_prices_daily_stats arithmetic.

    Each row must satisfy min <= avg <= max and spread == max - min. Expects
    dicts with keys min_price_eur_per_mwh / max_price_eur_per_mwh /
    avg_price_eur_per_mwh / spread_eur_per_mwh.
    """
    violations = 0
    for row in rows:
        lo = row["min_price_eur_per_mwh"]
        hi = row["max_price_eur_per_mwh"]
        avg = row["avg_price_eur_per_mwh"]
        spread = row["spread_eur_per_mwh"]
        if not (lo <= avg <= hi):
            violations += 1
        elif abs(spread - (hi - lo)) > tolerance:
            violations += 1
    return violations == 0, violations
