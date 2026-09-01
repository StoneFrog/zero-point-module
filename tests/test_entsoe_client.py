"""The ENTSO-E security token must never reach a URL, a log line, or an error.

bronze_entsoe_day_ahead logs `str(exc)` for every zone whose fetch fails, and
Dagster persists those log lines to Postgres — so a credential anywhere in a
stringifiable part of the request becomes a credential at rest. These tests
pin both layers: the token travels in a header (so URLs are clean), and the
redaction backstop still works if it ever ends up somewhere stringifiable.
"""

from datetime import date

import httpx
import pytest

from energy_pipeline.entsoe.client import EntsoeApiError, EntsoeClient
from energy_pipeline.redaction import redact

TOKEN = "s3cr3t-token-value"
EIC = "10YPL-AREA-----S"
DAY = date(2026, 8, 30)


def _client(handler, monkeypatch) -> EntsoeClient:
    """An EntsoeClient whose HTTP calls are served by `handler`, no retries."""
    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def _with_transport(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", _with_transport)
    client = EntsoeClient(api_token=TOKEN)
    # Otherwise tenacity backs off for ~30s before re-raising.
    client.fetch_day_ahead_prices.retry.stop = lambda _: True
    return client


def test_token_is_sent_as_a_header_and_never_in_the_url(monkeypatch):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["header"] = request.headers.get("SECURITY_TOKEN")
        return httpx.Response(200, content=b"<xml/>")

    assert _client(handler, monkeypatch).fetch_day_ahead_prices(EIC, DAY) == b"<xml/>"
    assert seen["header"] == TOKEN, "token must be sent via the SECURITY_TOKEN header"
    assert TOKEN not in seen["url"]
    assert "securityToken" not in seen["url"]


def test_http_error_carries_no_token(monkeypatch):
    """A 503 is the exact case that leaked: bronze logs str(exc) per zone."""
    client = _client(lambda r: httpx.Response(503, text="unavailable"), monkeypatch)

    with pytest.raises(EntsoeApiError) as excinfo:
        client.fetch_day_ahead_prices(EIC, DAY)

    reachable = f"{excinfo.value}{excinfo.value.__cause__}{excinfo.value.__context__}"
    assert TOKEN not in reachable


def test_transport_error_carries_no_token(monkeypatch):
    def handler(request):
        raise httpx.ReadTimeout("timed out", request=request)

    client = _client(handler, monkeypatch)

    with pytest.raises(EntsoeApiError) as excinfo:
        client.fetch_day_ahead_prices(EIC, DAY)

    reachable = f"{excinfo.value}{excinfo.value.__cause__}{excinfo.value.__context__}"
    assert TOKEN not in reachable


def test_redact_backstop_strips_the_token():
    assert redact(f"boom ?securityToken={TOKEN}&x=1", TOKEN) == "boom ?securityToken=***&x=1"
    # An empty token must not turn every message into asterisks.
    assert redact("boom", "") == "boom"
