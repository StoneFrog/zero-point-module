"""Only a *blocking* + *ERROR* check stops an asset writing. Pinned here.

This is the assumption silver_prices_hourly's safety rests on: `row_integrity`
is yielded before `table.overwrite()`, and a failure has to abort the function
before it reaches that call. What actually enforces that is Dagster, not our
code — yielding a failed blocking ERROR check raises DagsterAssetCheckFailedError
from inside the engine, so the asset never resumes and the explicit
`if not passed: return` guard below is unreachable (see that guard's comment,
and LEARNING.md).

Two reasons to pin library behaviour rather than assume it:

- A Dagster upgrade that stopped raising would silently move us back to the
  older, broken arrangement — bad rows written, only downstream steps skipped,
  materialization still reported as successful. Nothing else in the suite would
  notice.
- The severity/blocking pairing is load-bearing and easy to get wrong. Dropping
  `row_integrity` to WARN, while leaving `blocking=True` in place, reads like a
  small change and quietly removes the protection entirely.

Deliberately built on throwaway assets rather than silver itself: the behaviour
under test belongs to the engine, and reproducing it without S3, Iceberg or a
catalog keeps it in the unit suite where it will actually be run.
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


def _asset_that_writes_after_a_failing_check(name: str, *, blocking: bool, severity):
    """An asset shaped like silver: check first, then the side effect.

    `wrote` stands in for `table.overwrite()` — whether it ends up non-empty is
    exactly the question of whether bad rows would have reached Iceberg.
    """
    wrote: list[str] = []

    @asset(name=name, check_specs=[AssetCheckSpec(name="gate", asset=name, blocking=blocking)])
    def _asset(context):
        yield AssetCheckResult(check_name="gate", passed=False, severity=severity)
        wrote.append("overwrite")
        yield Output(None)

    return _asset, wrote


def test_failed_blocking_error_check_aborts_before_the_write():
    """The property silver depends on."""
    guarded, wrote = _asset_that_writes_after_a_failing_check(
        "guarded", blocking=True, severity=AssetCheckSeverity.ERROR
    )

    with pytest.raises(DagsterAssetCheckFailedError):
        materialize([guarded], instance=DagsterInstance.ephemeral())

    assert wrote == [], "the asset resumed past a failed blocking check and wrote anyway"


def test_passing_blocking_check_lets_the_write_through():
    """The other half: the gate must not block a healthy partition."""
    wrote: list[str] = []

    @asset(check_specs=[AssetCheckSpec(name="gate", asset="healthy", blocking=True)])
    def healthy(context):
        yield AssetCheckResult(
            check_name="gate", passed=True, severity=AssetCheckSeverity.ERROR
        )
        wrote.append("overwrite")
        yield Output(None)

    assert materialize([healthy], instance=DagsterInstance.ephemeral()).success
    assert wrote == ["overwrite"]


def test_warn_severity_does_not_abort_even_when_blocking():
    """Why bronze survives a total upstream outage.

    zone_completeness is WARN, so all 41 zones failing leaves the run green and
    silver still re-derives from the bronze XML already in the lake. The same
    mechanism is why downgrading row_integrity to WARN would silently disarm
    it — `blocking=True` alone buys nothing.
    """
    warned, wrote = _asset_that_writes_after_a_failing_check(
        "warned", blocking=True, severity=AssetCheckSeverity.WARN
    )

    assert materialize([warned], instance=DagsterInstance.ephemeral()).success
    assert wrote == ["overwrite"]


def test_non_blocking_error_check_does_not_abort():
    """The arrangement this project used to have, kept visible.

    A failing ERROR check that isn't blocking reports the problem and lets the
    write happen anyway — the materialization still shows as successful, and a
    later independent run of gold reads the bad rows with no gate at all.
    """
    reported, wrote = _asset_that_writes_after_a_failing_check(
        "reported", blocking=False, severity=AssetCheckSeverity.ERROR
    )

    assert materialize([reported], instance=DagsterInstance.ephemeral()).success
    assert wrote == ["overwrite"]


def test_silver_row_integrity_is_declared_blocking():
    """Ties the pinned semantics to the asset that relies on them.

    The tests above prove blocking+ERROR aborts; this proves silver actually
    asks for it. Severity is chosen at yield time in silver.py, not in the
    spec, so only the blocking half is declarative and checkable here.
    """
    specs = {spec.name: spec for spec in silver_prices_hourly.check_specs}
    assert "row_integrity" in specs
    assert specs["row_integrity"].blocking is True
