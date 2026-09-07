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

import re
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

# Where several day-ahead auctions publish for one bidding zone, ENTSO-E
# separates them with classificationSequence_AttributeInstanceComponent.position.
# Sequence 1 is the single day-ahead coupling (SDAC) result — the price this
# pipeline is about. Sequence 2 is EXAA's separate 10:15 CE(S)T auction, a
# different auction for the same delivery hours, which is why DE_LU's two
# blocks disagree on every point rather than being redundant copies.
SDAC_SEQUENCE = "1"


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

    sequence: str | None
    curve_type: str
    currency: str
    measure_unit: str
    start: datetime
    end: datetime | None
    resolution_minutes: int
    points: tuple[tuple[int, float], ...]  # (position, price), document order


# ISO-8601 durations, restricted to the hour/minute forms ENTSO-E uses for a
# market time unit: PT15M, PT30M, PT60M, and PT1H — which is the same duration
# as PT60M written the other legal way, and which zones do send. Anything
# outside this shape is rejected rather than guessed at: a resolution read
# wrongly doesn't lose one value, it shifts every point of the day onto the
# wrong timestamp, and a loud failure is recoverable where that isn't.
#
# Deliberately excluded: seconds (PT900S), and day/week durations (P1D) that
# ENTSO-E defines for other document types but not for day-ahead prices.
# Neither has ever appeared here, and inventing untested handling for them
# would trade a clear error for a silent one.
_RESOLUTION_PATTERN = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?")


def _parse_resolution(text: str) -> int:
    """ISO-8601 duration like 'PT60M' or 'PT1H' -> 60 minutes."""
    match = _RESOLUTION_PATTERN.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"Unsupported resolution: {text!r}")

    hours, minutes = match.groups()
    total = int(hours or 0) * 60 + int(minutes or 0)
    if total <= 0:
        # 'PT' with no components, or an explicit 'PT0M'. Zero would divide by
        # zero when sizing a block, so it fails here with a readable message.
        raise ValueError(f"Unsupported resolution: {text!r}")
    return total


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
        sequence = ts.findtext(
            "ns:classificationSequence_AttributeInstanceComponent.position", namespaces=NS
        )
        sequence = sequence.strip() if sequence else None

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
                    sequence=sequence,
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


def _select_price_sequence(blocks: list[_Block]) -> list[_Block]:
    """Keep one auction's blocks when a zone publishes several.

    Only filters when the document actually mixes sequences — a zone with a
    single auction (the common case, and every zone before the 15-minute MTU
    go-live) is untouched, sequence tag or not.
    """
    sequences = {block.sequence for block in blocks}
    if len(sequences) <= 1:
        return blocks

    if SDAC_SEQUENCE in sequences:
        keep = SDAC_SEQUENCE
    elif None in sequences:
        # Some zones label only the *extra* auctions and leave the primary
        # series untagged (DK_2 sends [None, "2"]), so an untagged block
        # alongside tagged ones is the primary one.
        keep = None
    else:
        # No primary in sight: take the lowest-numbered sequence rather than
        # document order, so the choice doesn't depend on response ordering.
        keep = min(sequences, key=lambda s: (not s.isdigit(), int(s) if s.isdigit() else 0, s))

    return [block for block in blocks if block.sequence == keep]


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
    blocks = _select_price_sequence(_collect_blocks(root))
    for block in _drop_duplicate_blocks(blocks):
        points.extend(_expand(block))

    points.sort(key=lambda p: p.ts_utc)
    return points
