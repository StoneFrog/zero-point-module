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


# --- 15-minute MTU era: curveType A03 sparse positions ---

SPARSE_A03 = Path(__file__).parent / "fixtures" / "entsoe_day_ahead_RS_2026-09-01_sparse_a03.xml"


def test_a03_sparse_positions_expand_to_full_coverage():
    """RS publishes 24 Points on a 15-minute grid: each holds for 4 intervals.

    Read one-Point-per-interval this would be a 24-row day at 15-minute
    resolution — a quarter of the day, wrongly spaced.
    """
    points = parse_day_ahead_xml(SPARSE_A03.read_bytes())
    assert len(points) == 96
    assert {p.resolution_minutes for p in points} == {15}

    timestamps = [p.ts_utc for p in points]
    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == 96

    # Contiguous 15-minute steps, no gaps left behind by the expansion.
    steps = {(b - a).total_seconds() for a, b in zip(timestamps, timestamps[1:])}
    assert steps == {900.0}

    # Each published price is carried across its four quarter-hours.
    assert [p.price_eur_per_mwh for p in points[:4]] == [points[0].price_eur_per_mwh] * 4


def test_non_a03_curves_are_not_forward_filled():
    """A gap under a fixed-size curve is missing data, not a wide block."""
    xml = b"""<?xml version="1.0"?>
    <Publication_MarketDocument
        xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
      <TimeSeries>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A01</curveType>
        <Period>
          <timeInterval>
            <start>2026-09-01T00:00Z</start>
            <end>2026-09-01T04:00Z</end>
          </timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><price.amount>10</price.amount></Point>
          <Point><position>4</position><price.amount>40</price.amount></Point>
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>"""
    points = parse_day_ahead_xml(xml)
    assert [p.price_eur_per_mwh for p in points] == [10.0, 40.0]
