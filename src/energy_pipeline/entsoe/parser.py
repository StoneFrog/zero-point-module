"""Parse ENTSO-E day-ahead price XML into typed price points.

ENTSO-E's response is "Publication_MarketDocument": one or more TimeSeries, each
containing one or more Periods, each containing Points indexed by `position`.
Position 1 corresponds to `timeInterval/start`, position 2 = start + resolution,
and so on.

Since Europe's move to a 15-minute market time unit, `PT15M` is the common
resolution and `PT60M` the exception (a few zones, e.g. CH and IE_SEM, still
publish hourly).

`curveType` decides what a Point covers. Under A03 ("variable sized block") a
Point's price holds from its own position until the position of the *next*
Point, so positions are deliberately sparse: a zone publishing hourly prices on
a 15-minute grid sends 24 Points at positions 1, 5, 9, … and each one stands for
four intervals. Reading those Points one-per-interval would silently produce a
short, wrongly-spaced day, so A03 blocks are expanded here.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

# "Variable sized block": a Point covers every interval up to the next Point's
# position. Any other curve type (A01 "sequential fixed size block" being the
# usual one) means one Point == one interval, and a gap there is missing data
# rather than a wide block — so expansion is gated on this value specifically,
# to avoid inventing prices for a genuinely incomplete series.
CURVE_VARIABLE_SIZED_BLOCK = "A03"


@dataclass(frozen=True)
class PricePoint:
    ts_utc: datetime  # interval start, UTC
    resolution_minutes: int  # 60 or 15
    price_eur_per_mwh: float
    currency: str  # almost always EUR
    measure_unit: str  # almost always MWH


@dataclass(frozen=True)
class _Block:
    """One Period, carrying the TimeSeries attributes needed to interpret it."""

    curve_type: str
    currency: str
    measure_unit: str
    start: datetime
    end: datetime | None
    resolution_minutes: int
    points: tuple[tuple[int, float], ...]  # (position, price), document order


def _parse_resolution(text: str) -> int:
    """ISO-8601 duration like 'PT60M' -> 60 minutes."""
    if not text.startswith("PT") or not text.endswith("M"):
        raise ValueError(f"Unsupported resolution: {text}")
    return int(text[2:-1])


def _parse_iso_utc(text: str) -> datetime:
    """ENTSO-E timestamps end in 'Z' for UTC. Python 3.11+ parses Z natively."""
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _collect_blocks(root: ET.Element) -> list[_Block]:
    """Flatten the document into one _Block per Period."""
    blocks: list[_Block] = []

    for ts in root.findall("ns:TimeSeries", NS):
        currency = (ts.findtext("ns:currency_Unit.name", namespaces=NS) or "EUR").strip()
        measure_unit = (ts.findtext("ns:price_Measure_Unit.name", namespaces=NS) or "MWH").strip()
        curve_type = (ts.findtext("ns:curveType", namespaces=NS) or "").strip()

        for period in ts.findall("ns:Period", NS):
            interval = period.find("ns:timeInterval", NS)
            if interval is None:
                continue
            start_text = interval.findtext("ns:start", namespaces=NS)
            end_text = interval.findtext("ns:end", namespaces=NS)
            resolution_text = period.findtext("ns:resolution", namespaces=NS)
            if not start_text or not resolution_text:
                continue

            points: list[tuple[int, float]] = []
            for point in period.findall("ns:Point", NS):
                position_text = point.findtext("ns:position", namespaces=NS)
                price_text = point.findtext("ns:price.amount", namespaces=NS)
                if not position_text or not price_text:
                    continue
                points.append((int(position_text), float(price_text)))

            blocks.append(
                _Block(
                    curve_type=curve_type,
                    currency=currency,
                    measure_unit=measure_unit,
                    start=_parse_iso_utc(start_text),
                    end=_parse_iso_utc(end_text) if end_text else None,
                    resolution_minutes=_parse_resolution(resolution_text),
                    points=tuple(points),
                )
            )

    return blocks


def _drop_duplicate_blocks(blocks: list[_Block]) -> list[_Block]:
    """Collapse Periods that publish byte-identical content twice.

    Several zones return the same interval twice over: same start, same
    resolution, same Points. Taken at face value that doubles every row and
    breaks the (ts_utc, bidding_zone) identity silver relies on. Only exact
    repeats are collapsed — two blocks covering the same interval with
    *different* prices are a real disagreement, and are left in place so
    silver's row_integrity check reports them instead of this quietly picking
    a winner.
    """
    seen: set[tuple] = set()
    kept: list[_Block] = []
    for block in blocks:
        key = (block.start, block.end, block.resolution_minutes, block.points)
        if key in seen:
            continue
        seen.add(key)
        kept.append(block)
    return kept


def _expand(block: _Block) -> list[PricePoint]:
    """Turn one block's Points into one PricePoint per interval it covers."""
    intervals_in_block: int | None = None
    if block.end is not None:
        span_minutes = (block.end - block.start).total_seconds() / 60
        intervals_in_block = int(span_minutes // block.resolution_minutes)

    ordered = sorted(block.points)
    out: list[PricePoint] = []

    for index, (position, price) in enumerate(ordered):
        if block.curve_type == CURVE_VARIABLE_SIZED_BLOCK:
            if index + 1 < len(ordered):
                # Holds until the next published position.
                next_position = ordered[index + 1][0]
            elif intervals_in_block is not None:
                # Last Point runs to the end of the declared time interval.
                next_position = intervals_in_block + 1
            else:
                next_position = position + 1
        else:
            next_position = position + 1

        # Never let a malformed or out-of-order position swallow a Point.
        next_position = max(next_position, position + 1)

        for covered in range(position, next_position):
            out.append(
                PricePoint(
                    ts_utc=block.start
                    + timedelta(minutes=block.resolution_minutes * (covered - 1)),
                    resolution_minutes=block.resolution_minutes,
                    price_eur_per_mwh=price,
                    currency=block.currency,
                    measure_unit=block.measure_unit,
                )
            )

    return out


def parse_day_ahead_xml(xml_bytes: bytes) -> list[PricePoint]:
    """Extract all price points from a Publication_MarketDocument response."""
    root = ET.fromstring(xml_bytes)

    points: list[PricePoint] = []
    for block in _drop_duplicate_blocks(_collect_blocks(root)):
        points.extend(_expand(block))

    points.sort(key=lambda p: p.ts_utc)
    return points
