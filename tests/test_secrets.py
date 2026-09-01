"""Secrets must stay out of reprs, out of logs, and out of falsy-check traps.

Every credential in Settings is a SecretStr, which masks it in reprs and model
dumps. Reading one back always goes through .get_secret_value(), so these tests
pin both the masking and the unwrapping.
"""

import httpx
import pytest
from pydantic import SecretStr

from energy_pipeline.config import Settings
from energy_pipeline.redaction import redact

SECRET_FIELDS = ("entsoe_api_token", "minio_root_password", "alert_webhook_url")


@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_credential_fields_are_secret_str(field):
    assert isinstance(getattr(Settings(), field), SecretStr)


@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_credentials_do_not_leak_via_repr_or_dump(field):
    value = "sentinel-credential-value"
    settings = Settings(**{field: value})
    assert value not in repr(settings)
    assert value not in str(settings.model_dump())
    assert getattr(settings, field).get_secret_value() == value


def test_empty_secret_is_falsy():
    """SecretStr defines __len__, so `not secret` does mean "unset" here.

    Pinned because the "no token -> use the bundled fixture" and "no webhook ->
    skip alerting" paths both hinge on an empty credential reading as unset.
    """
    assert not SecretStr("")
    assert SecretStr("x")
    assert not SecretStr("").get_secret_value()


def test_webhook_error_is_redacted_before_logging():
    """A Slack-style webhook URL is itself the credential; httpx logs the URL."""
    url = "https://hooks.slack.com/services/T000/B000/verysecrettoken"
    request = httpx.Request("POST", url)
    exc = httpx.HTTPStatusError(
        f"Server error '503' for url '{url}'",
        request=request,
        response=httpx.Response(503, request=request),
    )

    scrubbed = redact(str(exc), url)
    assert "verysecrettoken" not in scrubbed
    assert "***" in scrubbed
