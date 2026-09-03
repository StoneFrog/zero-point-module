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


def _two_blocks(second_price: str) -> bytes:
    """A document repeating one interval twice, as several zones now do."""
    block = """
      <TimeSeries>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A03</curveType>
        <Period>
          <timeInterval>
            <start>2026-09-01T00:00Z</start>
            <end>2026-09-01T02:00Z</end>
          </timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><price.amount>10</price.amount></Point>
          <Point><position>2</position><price.amount>%s</price.amount></Point>
        </Period>
      </TimeSeries>"""
    doc = (
        '<?xml version="1.0"?>'
        '<Publication_MarketDocument'
        ' xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">'
        + (block % "20")
        + (block % second_price)
        + "</Publication_MarketDocument>"
    )
    return doc.encode()


def test_identical_repeated_blocks_are_collapsed():
    points = parse_day_ahead_xml(_two_blocks("20"))
    assert [p.price_eur_per_mwh for p in points] == [10.0, 20.0]


def test_conflicting_repeated_blocks_are_left_for_the_quality_check():
    """Same interval, different prices is a real disagreement — don't guess."""
    points = parse_day_ahead_xml(_two_blocks("99"))
    timestamps = [p.ts_utc for p in points]
    assert len(timestamps) == 4
    assert len(set(timestamps)) == 2  # duplicates survive, row_integrity reports them


# --- 15-minute MTU era: several auctions per zone ---

TWO_SEQUENCES = (
    Path(__file__).parent / "fixtures" / "entsoe_day_ahead_DE_LU_2026-09-01_two_sequences.xml"
)


def test_only_the_sdac_sequence_is_kept():
    """DE_LU publishes SDAC (sequence 1) and EXAA's 10:15 auction (sequence 2).

    They disagree on every one of the 96 points, so taking both would both
    double the day and mix two different auctions' prices together.
    """
    points = parse_day_ahead_xml(TWO_SEQUENCES.read_bytes())
    assert len(points) == 96
    assert len({p.ts_utc for p in points}) == 96
    # 163.91 is sequence 1's first price; sequence 2 opens at 157.14.
    assert points[0].price_eur_per_mwh == 163.91


def _sequenced(*sequences: str | None) -> bytes:
    """A document carrying one one-point block per given sequence position."""
    parts = []
    for index, sequence in enumerate(sequences):
        tag = (
            ""
            if sequence is None
            else f"<classificationSequence_AttributeInstanceComponent.position>"
            f"{sequence}"
            f"</classificationSequence_AttributeInstanceComponent.position>"
        )
        parts.append(
            f"""
      <TimeSeries>
        {tag}
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A03</curveType>
        <Period>
          <timeInterval>
            <start>2026-09-01T00:00Z</start>
            <end>2026-09-01T01:00Z</end>
          </timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><price.amount>{index}</price.amount></Point>
        </Period>
      </TimeSeries>"""
        )
    doc = (
        '<?xml version="1.0"?>'
        '<Publication_MarketDocument'
        ' xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">'
        + "".join(parts)
        + "</Publication_MarketDocument>"
    )
    return doc.encode()


def test_untagged_block_wins_when_no_sequence_1_is_published():
    """DK_2 sends [None, "2"] — the untagged series is the primary one."""
    points = parse_day_ahead_xml(_sequenced(None, "2"))
    assert [p.price_eur_per_mwh for p in points] == [0.0]


def test_lowest_sequence_wins_when_there_is_no_primary():
    """ES sends tagged-only combinations; don't let response order decide."""
    points = parse_day_ahead_xml(_sequenced("3", "2"))
    assert [p.price_eur_per_mwh for p in points] == [1.0]  # the "2" block


def test_single_auction_zones_are_untouched():
    points = parse_day_ahead_xml(_sequenced("2"))
    assert [p.price_eur_per_mwh for p in points] == [0.0]
