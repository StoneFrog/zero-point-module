"""Parse ENTSO-E day-ahead price XML into typed price points.

ENTSO-E's response is "Publication_MarketDocument": one or more TimeSeries, each
containing one or more Periods, each containing Points indexed by `position`.
Position 1 corresponds to `timeInterval/start`, position 2 = start + resolution,
and so on.

Resolution is usually PT60M (hourly) but can be PT15M (quarter-hourly, common
in DE-LU since 2025-10).
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}


@dataclass(frozen=True)
class PricePoint:
    ts_utc: datetime  # interval start, UTC
    resolution_minutes: int  # 60 or 15
    price_eur_per_mwh: float
    currency: str  # almost always EUR
    measure_unit: str  # almost always MWH


def _parse_resolution(text: str) -> int:
    """ISO-8601 duration like 'PT60M' -> 60 minutes."""
    if not text.startswith("PT") or not text.endswith("M"):
        raise ValueError(f"Unsupported resolution: {text}")
    return int(text[2:-1])


def _parse_iso_utc(text: str) -> datetime:
    """ENTSO-E timestamps end in 'Z' for UTC. Python 3.11+ parses Z natively."""
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def parse_day_ahead_xml(xml_bytes: bytes) -> list[PricePoint]:
    """Extract all price points from a Publication_MarketDocument response."""
    root = ET.fromstring(xml_bytes)
    points: list[PricePoint] = []

    for ts in root.findall("ns:TimeSeries", NS):
        currency = (ts.findtext("ns:currency_Unit.name", namespaces=NS) or "EUR").strip()
        measure_unit = (ts.findtext("ns:price_Measure_Unit.name", namespaces=NS) or "MWH").strip()

        for period in ts.findall("ns:Period", NS):
            interval = period.find("ns:timeInterval", NS)
            if interval is None:
                continue
            start_text = interval.findtext("ns:start", namespaces=NS)
            resolution_text = period.findtext("ns:resolution", namespaces=NS)
            if not start_text or not resolution_text:
                continue

            start = _parse_iso_utc(start_text)
            resolution_min = _parse_resolution(resolution_text)

            for point in period.findall("ns:Point", NS):
                position_text = point.findtext("ns:position", namespaces=NS)
                price_text = point.findtext("ns:price.amount", namespaces=NS)
                if not position_text or not price_text:
                    continue
                position = int(position_text)
                ts_utc = start + timedelta(minutes=resolution_min * (position - 1))
                points.append(
                    PricePoint(
                        ts_utc=ts_utc,
                        resolution_minutes=resolution_min,
                        price_eur_per_mwh=float(price_text),
                        currency=currency,
                        measure_unit=measure_unit,
                    )
                )

    return points
