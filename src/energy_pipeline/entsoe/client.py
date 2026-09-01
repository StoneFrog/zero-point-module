"""Minimal ENTSO-E Transparency Platform REST client.

Why hand-rolled instead of using `entsoe-py`: the API surface we need is tiny
(one endpoint, one document type) and writing it ourselves makes the data flow
visible. Swap to `entsoe-py` later if we add many more report types.

API docs: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
"""

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from energy_pipeline.redaction import redact

# ENTSO-E aligns the "delivery day" to Brussels local time (CET/CEST) across
# the entire European day-ahead market. zoneinfo handles DST transitions.
_MARKET_TZ = ZoneInfo("Europe/Brussels")

# Document type for day-ahead prices (Article 12.1.D of the Transparency Regulation).
DOCUMENT_TYPE_DAY_AHEAD = "A44"

FIXTURE_PATH = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "entsoe_day_ahead_PL_2026-05-04.xml"


def _format_period(dt: datetime) -> str:
    """ENTSO-E expects YYYYMMDDhhmm in UTC."""
    return dt.strftime("%Y%m%d%H%M")


class EntsoeApiError(RuntimeError):
    """An upstream ENTSO-E failure, with the security token stripped out.

    Belt-and-braces. The token is sent as a header (see
    fetch_day_ahead_prices), so it should never reach a URL or an httpx error
    message in the first place — but bronze_entsoe_day_ahead logs `str(exc)`
    for every failed zone straight into the Dagster event log and Postgres, so
    the redaction stays as a second line of defence against a future change
    that puts the credential back into a stringifiable place.
    """


class EntsoeClient:
    """Synchronous client for ENTSO-E Transparency Platform.

    Single-purpose: fetch day-ahead prices for one bidding zone for one delivery
    day. The bronze asset wraps this to fan out across all zones.
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://web-api.tp.entsoe.eu/api",
        use_fixture: bool = False,
        timeout_s: float = 30.0,
    ) -> None:
        self._api_token = api_token
        self._base_url = base_url
        self._use_fixture = use_fixture
        self._timeout_s = timeout_s

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def fetch_day_ahead_prices(self, eic: str, delivery_day: date) -> bytes:
        """Return the raw XML response for a delivery-day price publication.

        Returns bytes (not parsed) on purpose: bronze stores raw exactly as the
        upstream sent it. Parsing happens in silver.
        """
        if self._use_fixture or not self._api_token:
            return FIXTURE_PATH.read_bytes()

        # The ENTSO-E "delivery day" is the 24h window starting at 00:00
        # Brussels local time. In UTC that's 23:00 the day before (winter, CET)
        # or 22:00 the day before (summer, CEST). zoneinfo computes the correct
        # offset for each date including DST transitions.
        local_midnight = datetime.combine(delivery_day, time.min, tzinfo=_MARKET_TZ)
        period_start = local_midnight.astimezone(timezone.utc)
        period_end = (local_midnight + timedelta(days=1)).astimezone(timezone.utc)

        # The token goes in a header, NOT in `params`. ENTSO-E accepts either
        # `?securityToken=` or the `SECURITY_TOKEN` header, and the query-param
        # form puts a live credential inside every URL — which httpx copies
        # into its exception messages and its INFO-level "HTTP Request: GET
        # <url>" log line, and which bronze then writes to the Dagster event
        # log. Keeping the URL credential-free removes that whole class of
        # leak instead of redacting it at each call site.
        headers = {"SECURITY_TOKEN": self._api_token}

        params = {
            "documentType": DOCUMENT_TYPE_DAY_AHEAD,
            "in_Domain": eic,
            "out_Domain": eic,
            "periodStart": _format_period(period_start),
            "periodEnd": _format_period(period_end),
        }

        with httpx.Client(timeout=self._timeout_s) as client:
            try:
                response = client.get(self._base_url, params=params, headers=headers)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                redacted = redact(str(exc), self._api_token)
            else:
                return response.content

        # Raised *outside* the except block deliberately. Raising inside it
        # would chain the httpx error onto __context__, and that object's own
        # message still holds the token — `raise ... from None` only hides it
        # from printed tracebacks, it doesn't drop the reference. Out here
        # there's no active exception to chain, so nothing reachable from this
        # error carries the credential.
        raise EntsoeApiError(redacted)
