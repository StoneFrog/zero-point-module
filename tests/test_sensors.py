"""Alerting: the payload the sensor builds, and whether it actually delivers.

No live Dagster run needed — build_alert_payload and deliver_alert are split out
of the sensor precisely so both halves can be exercised directly.
"""

from dataclasses import dataclass

import httpx
from pydantic import SecretStr

from energy_pipeline import sensors
from energy_pipeline.config import Settings
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


# --- webhook delivery ---

WEBHOOK = "https://hooks.slack.com/services/T000/B000/verysecrettoken"


class _Log:
    """Captures what the sensor would write to Dagster's event log."""

    def __init__(self) -> None:
        self.errors: list[str] = []

    def error(self, message: str) -> None:
        self.errors.append(message)


def _with_webhook(monkeypatch, url: str) -> None:
    monkeypatch.setattr(sensors, "settings", Settings(alert_webhook_url=SecretStr(url)))


def test_alert_is_posted_to_the_configured_webhook(monkeypatch):
    sent = {}

    def fake_post(url, json, timeout):
        sent["url"], sent["json"], sent["timeout"] = url, json, timeout
        return httpx.Response(200, request=httpx.Request("POST", url))

    _with_webhook(monkeypatch, WEBHOOK)
    monkeypatch.setattr(httpx, "post", fake_post)

    sensors.deliver_alert({"text": "run failed"}, _Log())

    assert sent["url"] == WEBHOOK
    assert sent["json"] == {"text": "run failed"}
    assert sent["timeout"] == 10.0


def test_nothing_is_posted_when_no_webhook_is_configured(monkeypatch):
    """The default. An unset webhook is a supported setup, not a broken one.

    The call is *recorded* rather than raised on: deliver_alert catches
    Exception so a dead endpoint can't take the sensor down, which means an
    exception raised in here would be swallowed and logged instead of failing
    the test.
    """
    calls = []

    def fake_post(url, json, timeout):
        calls.append(url)
        return httpx.Response(200, request=httpx.Request("POST", url or "http://x"))

    _with_webhook(monkeypatch, "")
    monkeypatch.setattr(httpx, "post", fake_post)

    sensors.deliver_alert({"text": "run failed"}, _Log())

    assert calls == [], "posted to an empty webhook URL"


def test_a_dead_webhook_does_not_take_the_sensor_down(monkeypatch):
    """One failed run must not become a failed run plus a failed sensor.

    Dagster surfaces a raising sensor as its own error and stops evaluating it,
    so an endpoint being down would cost us the alerting for every *later*
    failure too — the opposite of what this exists for.
    """

    def fake_post(url, json, timeout):
        request = httpx.Request("POST", url)
        raise httpx.HTTPStatusError(
            f"Server error '503' for url '{url}'",
            request=request,
            response=httpx.Response(503, request=request),
        )

    _with_webhook(monkeypatch, WEBHOOK)
    monkeypatch.setattr(httpx, "post", fake_post)
    log = _Log()

    sensors.deliver_alert({"text": "run failed"}, log)  # must not raise

    assert len(log.errors) == 1
    assert "verysecrettoken" not in log.errors[0], "the webhook URL is the credential"
    assert "***" in log.errors[0]
