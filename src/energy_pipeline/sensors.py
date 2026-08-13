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
from dagster import RunFailureSensorContext, run_failure_sensor

from energy_pipeline.config import settings


def build_alert_payload(context: RunFailureSensorContext) -> dict:
    run = context.dagster_run
    partition = run.tags.get("dagster/partition", "")
    partition_suffix = f" partition={partition}" if partition else ""
    text = (
        f":rotating_light: energy-pipeline run failed — job={run.job_name}"
        f"{partition_suffix} run_id={run.run_id}\n{context.failure_event.message}"
    )
    return {"text": text}


@run_failure_sensor(
    description="Log (and optionally webhook-alert) when any job run in this repo fails.",
)
def pipeline_failure_sensor(context: RunFailureSensorContext) -> None:
    payload = build_alert_payload(context)
    context.log.error(payload["text"])

    if not settings.alert_webhook_url:
        return

    try:
        response = httpx.post(settings.alert_webhook_url, json=payload, timeout=10.0)
        response.raise_for_status()
    except Exception as exc:
        context.log.error(f"Failed to deliver alert webhook: {exc}")
