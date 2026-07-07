"""Parser unit tests against the bundled fixture."""

from datetime import datetime, timezone
from pathlib import Path

from energy_pipeline.entsoe.parser import parse_day_ahead_xml

FIXTURE = Path(__file__).parent / "fixtures" / "entsoe_day_ahead_PL_2026-05-04.xml"


def test_parses_24_hourly_points():
    points = parse_day_ahead_xml(FIXTURE.read_bytes())
    assert len(points) == 24


def test_first_point_is_correct():
    points = parse_day_ahead_xml(FIXTURE.read_bytes())
    first = points[0]
    assert first.ts_utc == datetime(2026, 5, 3, 22, 0, tzinfo=timezone.utc)
    assert first.resolution_minutes == 60
    assert first.price_eur_per_mwh == 72.15
    assert first.currency == "EUR"
    assert first.measure_unit == "MWH"


def test_points_are_strictly_increasing_in_time():
    points = parse_day_ahead_xml(FIXTURE.read_bytes())
    timestamps = [p.ts_utc for p in points]
    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == len(timestamps)
