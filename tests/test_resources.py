"""EntsoeResource decides, per run, whether we call ENTSO-E or read a fixture.

Getting this wrong is silent in both directions: a resource that keeps the
SecretStr wrapper hands the client an object whose str() is "**********" and
every request 401s, while one that misreads an unset token as configured
sends live requests with no credential instead of falling back to the bundled
fixture. Both faults only surface as upstream errors, so they are pinned here.
"""

from datetime import date

import pytest
from pydantic import SecretStr

from energy_pipeline import resources
from energy_pipeline.config import Settings
from energy_pipeline.entsoe.parser import parse_day_ahead_xml

TOKEN = "configured-token-value"


def _with_settings(monkeypatch, **overrides) -> None:
    """Swap the module-level settings singleton for this test only.

    Explicit kwargs outrank the environment in pydantic-settings, so this is
    independent of whatever .env happens to hold on the machine running it.
    """
    monkeypatch.setattr(resources, "settings", Settings(**overrides))


def test_client_receives_the_unwrapped_token(monkeypatch):
    """A SecretStr reaching the client would be sent as its masked repr."""
    _with_settings(monkeypatch, entsoe_api_token=SecretStr(TOKEN))
    client = resources.EntsoeResource(use_fixture=False).client()
    assert client._api_token == TOKEN
    assert not isinstance(client._api_token, SecretStr)


def test_configured_token_means_live_requests(monkeypatch):
    _with_settings(monkeypatch, entsoe_api_token=SecretStr(TOKEN))
    assert resources.EntsoeResource(use_fixture=False).client()._use_fixture is False


def test_unset_token_falls_back_to_the_fixture(monkeypatch):
    """No credential configured is a local/dev run, not a run of 401s."""
    _with_settings(monkeypatch, entsoe_api_token=SecretStr(""))
    assert resources.EntsoeResource(use_fixture=False).client()._use_fixture is True


def test_use_fixture_wins_even_with_a_token_configured(monkeypatch):
    """The resource config is the explicit override; a token doesn't defeat it."""
    _with_settings(monkeypatch, entsoe_api_token=SecretStr(TOKEN))
    assert resources.EntsoeResource(use_fixture=True).client()._use_fixture is True


def test_base_url_comes_from_settings(monkeypatch):
    _with_settings(
        monkeypatch,
        entsoe_api_token=SecretStr(TOKEN),
        entsoe_base_url="https://example.invalid/api",
    )
    assert resources.EntsoeResource().client()._base_url == "https://example.invalid/api"


@pytest.mark.parametrize("token", ["", TOKEN])
def test_fixture_mode_never_touches_the_network(monkeypatch, token):
    """Whichever way fixture mode was reached, the fetch stays offline.

    Any real HTTP attempt would fail against `example.invalid`; a parseable
    day of prices coming back proves the client short-circuited to the
    bundled document instead.
    """
    _with_settings(
        monkeypatch,
        entsoe_api_token=SecretStr(token),
        entsoe_base_url="https://example.invalid/api",
    )
    client = resources.EntsoeResource(use_fixture=True).client()
    raw = client.fetch_day_ahead_prices("10YPL-AREA-----S", date(2026, 9, 1))
    assert len(parse_day_ahead_xml(raw)) == 24
