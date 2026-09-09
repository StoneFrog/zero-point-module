"""Operational alerting: notice when a run in this repo's jobs fails.

No external alerting service is wired up here (no Slack/PagerDuty creds in
.env.example) — a failed run is always visible in the Dagster UI and daemon
logs regardless. If ALERT_WEBHOOK_URL is set, we additionally POST a small
JSON payload shaped to work as-is with a Slack incoming webhook (top-level
"text" field); anything else that speaks generic JSON webhooks (Discord via
its Slack-compatible endpoint, a custom relay, etc.) can read the same
payload. Left unset by default, so the pipeline has no external dependency
out of the box.
"""

import httpx
from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from energy_pipeline.config import settings
from energy_pipeline.redaction import redact


def build_alert_payload(context: RunFailureSensorContext) -> dict:
    run = context.dagster_run
    partition = run.tags.get("dagster/partition", "")
    partition_suffix = f" partition={partition}" if partition else ""
    text = (
        f":rotating_light: energy-pipeline run failed — job={run.job_name}"
        f"{partition_suffix} run_id={run.run_id}\n{context.failure_event.message}"
    )
    return {"text": text}


def deliver_alert(payload: dict, log) -> None:
    """POST the payload to ALERT_WEBHOOK_URL, if one is configured.

    Never raises: a webhook that is down must not turn one failed run into a
    failing sensor as well.
    """
    # Unwrapped once — the raw URL is needed for both the POST and the
    # redaction below.
    webhook_url = settings.alert_webhook_url.get_secret_value()
    if not webhook_url:
        return

    try:
        response = httpx.post(webhook_url, json=payload, timeout=10.0)
        response.raise_for_status()
    except Exception as exc:
        # For a Slack-style incoming webhook the URL *is* the credential, and
        # httpx embeds it in its error messages — an unscrubbed log line here
        # would persist that secret into Dagster's event log every time the
        # endpoint is down.
        log.error(f"Failed to deliver alert webhook: {redact(str(exc), webhook_url)}")


@run_failure_sensor(
    description="Log (and optionally webhook-alert) when any job run in this repo fails.",
    default_status=DefaultSensorStatus.RUNNING,
)
def pipeline_failure_sensor(context: RunFailureSensorContext) -> None:
    payload = build_alert_payload(context)
    context.log.error(payload["text"])
    deliver_alert(payload, context.log)
