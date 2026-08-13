"""Unit test for the alert payload shape (no live Dagster run needed)."""

from dataclasses import dataclass

from energy_pipeline.sensors import build_alert_payload


@dataclass
class _FakeRun:
    job_name: str
    run_id: str
    tags: dict


@dataclass
class _FakeFailureEvent:
    message: str


@dataclass
class _FakeContext:
    dagster_run: _FakeRun
    failure_event: _FakeFailureEvent


def test_build_alert_payload_includes_job_and_run_id():
    context = _FakeContext(
        dagster_run=_FakeRun(job_name="daily_pipeline", run_id="abc123", tags={}),
        failure_event=_FakeFailureEvent(message="boom"),
    )
    payload = build_alert_payload(context)
    assert "daily_pipeline" in payload["text"]
    assert "abc123" in payload["text"]
    assert "boom" in payload["text"]


def test_build_alert_payload_includes_partition_when_present():
    context = _FakeContext(
        dagster_run=_FakeRun(
            job_name="daily_pipeline", run_id="abc123", tags={"dagster/partition": "2026-05-04"}
        ),
        failure_event=_FakeFailureEvent(message="boom"),
    )
    payload = build_alert_payload(context)
    assert "2026-05-04" in payload["text"]
