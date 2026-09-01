"""Scrub credentials out of strings that are about to be logged.

Both places this is used log an exception message on a failed HTTP call, and
both of those calls carry a credential (ENTSO-E's token, the alert webhook's
secret URL). Dagster persists asset/sensor log lines to Postgres, so an
unscrubbed message is a credential at rest. The real defence is keeping
secrets out of URLs in the first place — see entsoe/client.py — and this is
the backstop for what slips through.
"""


def redact(message: str, secret: str) -> str:
    """Replace every occurrence of `secret` in `message` with `***`.

    A falsy secret is a no-op: substituting an empty string would otherwise
    turn every message into asterisks.
    """
    return message.replace(secret, "***") if secret else message
