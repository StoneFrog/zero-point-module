"""A failing row_integrity must fail the *run*, not just skip the write.

Bad rows reaching Iceberg is guarded twice over, so it isn't what these tests
are about. If dagster keeps raising DagsterAssetCheckFailedError at the yield,
the engine aborts silver before `table.overwrite()`. If it ever stopped, the
`if not passed: return` guard below that yield would catch it instead —
verified, not assumed: the same shape with a non-blocking check writes nothing.
Two independent layers, and the data is safe either way.

What is *not* guarded twice is how the failure is reported, and that is what
this file pins:

    raising today      run FAILS, gold never starts
    if it stopped      run SUCCEEDS with rows_written 0, and gold proceeds
                       to compute over the previous silver snapshot

The second is a bad day. Yesterday's prices published as today's, with a green
run and no check failure anywhere downstream to contradict it — because
row_integrity did pass, on the rows that were skipped. That degradation is
silent, which is the only kind worth spending a pinning test on: a Dagster
upgrade is the one moment it could happen, and the suite is where we'd want to
hear about it.

Deliberately built on a throwaway asset. The behaviour belongs to the engine,
and keeping S3, Iceberg and the catalog out of it keeps this in the unit suite
where it actually gets run.

Not pinned here, on purpose: that a *passing* check lets the write through,
that WARN doesn't abort, that a non-blocking ERROR doesn't abort. All true, all
verified while writing this, none worth a test — each fails loudly and
immediately (a pipeline that stops dead, or a bronze zone outage that starts
failing runs). Loud failures announce themselves; tests are for the quiet ones.
"""

import pytest
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    DagsterInstance,
    Output,
    asset,
    materialize,
)
from dagster._core.errors import DagsterAssetCheckFailedError

from energy_pipeline.assets.silver import silver_prices_hourly


def test_failed_blocking_error_check_fails_the_run():
    """If this stops raising, a corrupt partition becomes a green run.

    The data would still be safe — silver's own guard would stop the write —
    but the run would report success, and gold would go on to build from the
    previous snapshot as though nothing were wrong.
    """

    @asset(check_specs=[AssetCheckSpec(name="gate", asset="guarded", blocking=True)])
    def guarded(context):
        yield AssetCheckResult(
            check_name="gate", passed=False, severity=AssetCheckSeverity.ERROR
        )
        yield Output(None)

    with pytest.raises(DagsterAssetCheckFailedError):
        materialize([guarded], instance=DagsterInstance.ephemeral())


def test_silver_row_integrity_is_declared_blocking():
    """The half of the contract that is ours to get wrong.

    The test above pins what dagster does with a blocking check; this pins that
    silver still asks for one. Severity is chosen at yield time in silver.py
    rather than in the spec, so only the blocking half is declarative here.
    """
    specs = {spec.name: spec for spec in silver_prices_hourly.check_specs}
    assert "row_integrity" in specs
    assert specs["row_integrity"].blocking is True
