"""silver's bronze reader must survive a partition bronze never wrote.

Every zone's fetch can fail at once — an ENTSO-E outage, an expired token —
and bronze then creates no keys under the partition prefix at all. s3fs raises
FileNotFoundError for a missing prefix rather than returning [], so without a
guard the step dies with an opaque traceback instead of reaching silver's own
"No bronze files" branch, which exists precisely to report that case.

Everything here is exercised through a fake filesystem: the layout contract
(`.../delivery_date=<day>/zone=<code>/raw.xml`) is what matters, not s3fs.
"""

import io
from datetime import date

from energy_pipeline.assets import silver
from energy_pipeline.config import settings

DAY = date(2026, 9, 1)
PREFIX = f"{settings.lake_bucket}/bronze/entsoe/day_ahead/delivery_date={DAY.isoformat()}/"


class _FakeS3:
    """Just enough s3fs surface for _read_bronze_partition.

    `entries=None` means the prefix does not exist, which is what s3fs signals
    with FileNotFoundError rather than an empty listing.
    """

    def __init__(self, entries: list[str] | None, files: dict[str, bytes] | None = None) -> None:
        self._entries = entries
        self._files = files or {}

    def ls(self, prefix: str) -> list[str]:
        if self._entries is None:
            raise FileNotFoundError(prefix)
        # s3fs returns full keys with no trailing slash, e.g.
        # 'lake/bronze/entsoe/day_ahead/delivery_date=2026-09-01/zone=PL'.
        return [prefix + entry for entry in self._entries]

    def exists(self, path: str) -> bool:
        return path in self._files

    def open(self, path: str, mode: str = "rb"):
        return io.BytesIO(self._files[path])


def _patch_fs(monkeypatch, fs: _FakeS3) -> None:
    monkeypatch.setattr(silver, "_s3_fs", lambda: fs)


def test_missing_partition_prefix_returns_no_zones(monkeypatch):
    """The guard under test: a prefix bronze never created is an empty day."""
    _patch_fs(monkeypatch, _FakeS3(entries=None))
    assert silver._read_bronze_partition(DAY) == {}


def test_zones_with_raw_xml_are_returned(monkeypatch):
    _patch_fs(
        monkeypatch,
        _FakeS3(
            entries=["zone=PL", "zone=DE_LU"],
            files={
                PREFIX + "zone=PL/raw.xml": b"<pl/>",
                PREFIX + "zone=DE_LU/raw.xml": b"<de/>",
            },
        ),
    )
    assert silver._read_bronze_partition(DAY) == {"PL": b"<pl/>", "DE_LU": b"<de/>"}


def test_zone_without_raw_xml_is_skipped(monkeypatch):
    """A half-written zone directory must not surface as an empty document."""
    _patch_fs(
        monkeypatch,
        _FakeS3(
            entries=["zone=PL", "zone=FR"],
            files={PREFIX + "zone=PL/raw.xml": b"<pl/>"},
        ),
    )
    assert silver._read_bronze_partition(DAY) == {"PL": b"<pl/>"}


def test_entries_that_are_not_zone_partitions_are_ignored(monkeypatch):
    """Only `zone=` keys are data; anything else under the prefix is noise."""
    _patch_fs(
        monkeypatch,
        _FakeS3(
            entries=["_temporary", "zone=PL"],
            files={
                PREFIX + "_temporary/raw.xml": b"<junk/>",
                PREFIX + "zone=PL/raw.xml": b"<pl/>",
            },
        ),
    )
    assert silver._read_bronze_partition(DAY) == {"PL": b"<pl/>"}


def test_empty_partition_listing_returns_no_zones(monkeypatch):
    """The prefix exists but holds nothing — same outcome as no prefix at all."""
    _patch_fs(monkeypatch, _FakeS3(entries=[]))
    assert silver._read_bronze_partition(DAY) == {}
