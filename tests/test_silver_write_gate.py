"""A partition that fails row_integrity must not reach `table.overwrite()`.

This runs the real silver_prices_hourly: the real parser, the real
check_silver_row_integrity, the real guard, and the real Dagster engine. Only
the two edges that need a live stack are replaced — bronze comes from a fixture
in memory instead of S3, and the Iceberg table is a stand-in that records
whether `overwrite` was called instead of writing anything.

That recording is the whole point. Every other way of asking this question
answers it indirectly: an engine-level test proves dagster raises but never
touches our code, and a live run proves the snapshot didn't move but needs S3,
a catalog and a partition with data. Here, `table.overwrite` either got called
or it didn't, and the assertion is that one fact.

It also stays honest through a change in *which* layer stops the write. Two
protect it today — dagster raising at the yield, and the `if not passed: return`
guard behind it — and this passes as long as either one holds, which is the
actual requirement.

The layers differ in one way the data-safety tests can't see: dagster raising
makes the run *fail*, while the guard alone lets it report success with
`rows_written: 0`. That's an alerting difference, not a correctness one — a
corrupt partition stops being loud rather than starting to be wrong. It isn't
pinned with a test, because the only way to reach it is a deliberate bump of
the exact-pinned dagster, whose release notes would carry a change to blocking
semantics in bold. The last test below covers the version of that risk we can
actually cause ourselves.
"""

import contextlib
import re
from pathlib import Path

import pytest
from dagster import DagsterInstance, materialize
from dagster._core.errors import DagsterAssetCheckFailedError

from energy_pipeline.assets import silver
from energy_pipeline.assets.silver import silver_prices_hourly

PARTITION = "2026-05-04"
FIXTURE = Path(__file__).parent / "fixtures" / "entsoe_day_ahead_PL_2026-05-04.xml"


def _healthy_xml() -> bytes:
    """24 distinct hourly points for one zone — passes row_integrity."""
    return FIXTURE.read_bytes()


def _duplicated_xml() -> bytes:
    """The same day published twice with one price changed.

    Conflicting repeats are deliberately *not* collapsed by the parser (only
    byte-identical ones are), precisely so row_integrity sees them — so this is
    the realistic way a partition fails, not a synthetic one. Result: 48 points
    across 24 timestamps, i.e. 24 duplicate (ts_utc, bidding_zone) keys.
    """
    raw = _healthy_xml()
    block = re.search(rb"<TimeSeries>.*</TimeSeries>", raw, re.DOTALL).group(0)
    conflicting = re.sub(
        rb"<price\.amount>[\d.]+</price\.amount>",
        b"<price.amount>999.99</price.amount>",
        block,
        count=1,
    )
    return raw.replace(block, block + conflicting, 1)


class _RecordingTable:
    """Stands in for the Iceberg table; records writes rather than doing them."""

    def __init__(self, overwrites: list) -> None:
        self._overwrites = overwrites

    def overwrite(self, arrow_table, overwrite_filter=None):
        self._overwrites.append(arrow_table.num_rows)

    def refresh(self):
        return self

    def current_snapshot(self):
        class _Snapshot:
            snapshot_id = 1

        return _Snapshot()


class _StubCatalogResource:
    def get(self):
        return object()  # only ever handed to the patched _ensure_table


@pytest.fixture
def overwrites(monkeypatch):
    """Patch silver's three live-stack seams; yield the write log."""
    calls: list[int] = []
    monkeypatch.setattr(silver, "_ensure_table", lambda catalog: _RecordingTable(calls))
    monkeypatch.setattr(silver, "write_version_hint", lambda table: 1)
    return calls


def _run(xml: bytes, monkeypatch):
    monkeypatch.setattr(silver, "_read_bronze_partition", lambda day: {"PL": xml})
    return materialize(
        [silver_prices_hourly],
        partition_key=PARTITION,
        resources={"iceberg": _StubCatalogResource()},
        instance=DagsterInstance.ephemeral(),
    )


def test_duplicate_keys_never_reach_the_overwrite(overwrites, monkeypatch):
    """The guarantee: a bad partition leaves whatever was committed alone.

    The raise is suppressed rather than asserted on purpose. Which layer stops
    the write is not this test's business — dagster aborting at the yield and
    the `if not passed: return` guard are both acceptable answers, and pinning
    one would make this fail on a change that is still perfectly safe. It fails
    only if *neither* holds, which is the thing actually worth knowing.
    """
    with contextlib.suppress(DagsterAssetCheckFailedError):
        _run(_duplicated_xml(), monkeypatch)

    assert overwrites == [], "silver wrote a partition that failed row_integrity"


def test_a_healthy_partition_is_written(overwrites, monkeypatch):
    """The other half — the gate must not be stopping good data too."""
    assert _run(_healthy_xml(), monkeypatch).success
    assert overwrites == [24]


def test_silver_row_integrity_is_declared_blocking():
    """The half of the contract that is ours to get wrong.

    Dropping `blocking=True` leaves the data safe — the guard still stops the
    write — but the run goes green on a corrupt partition, so nobody chases the
    bad upstream data. Unlike a dagster behaviour change, this is a one-line
    edit someone could plausibly make while tidying.

    Severity is chosen at yield time in silver.py rather than in the spec, so
    only the blocking half is declarative and checkable here.
    """
    specs = {spec.name: spec for spec in silver_prices_hourly.check_specs}
    assert "row_integrity" in specs
    assert specs["row_integrity"].blocking is True
