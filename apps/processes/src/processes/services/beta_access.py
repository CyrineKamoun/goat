"""Beta-tool access policy.

Resolves which email domains may see beta tools (those flagged x-ui-beta) in
the toolbox. The resolution is generic: it has no knowledge of specific tools,
so any tool marked beta in the goatlib registry is gated the same way.

Source of truth is a Windmill variable (configured on the Windmill variables
page). The resolved domain set is cached briefly so opening the toolbox
doesn't round-trip to Windmill every time.
"""

import logging
import time

from processes.config import normalize_email_domains, settings
from processes.services.windmill_client import WindmillClient, WindmillError

logger = logging.getLogger(__name__)

# How long a resolved allowlist stays cached before we re-read the Windmill
# variable. Short enough that toggling the variable takes effect promptly.
_CACHE_TTL_SECONDS = 60.0

# (expires_at_monotonic, domains) or None when nothing is cached yet.
_cache: tuple[float, set[str]] | None = None


def _invalidate_cache() -> None:
    """Drop the cached allowlist (exposed for tests)."""
    global _cache
    _cache = None


async def get_beta_email_domains(windmill: WindmillClient) -> set[str]:
    """Return the set of allowlisted email domains for beta tools.

    Reads the Windmill variable at BETA_USER_EMAIL_DOMAINS_WM_PATH (cached for
    a short TTL). Returns an empty set when the variable is unset/unreachable,
    so beta tools stay hidden from everyone.
    """
    global _cache
    now = time.monotonic()
    if _cache is not None and _cache[0] > now:
        return _cache[1]

    raw: str | None = ""
    try:
        raw = await windmill.get_variable(settings.BETA_USER_EMAIL_DOMAINS_WM_PATH)
    except WindmillError as e:
        # Windmill unreachable/erroring: fail closed (hide beta) and don't
        # cache, so we retry on the next request rather than sticking empty.
        logger.warning(f"Could not resolve beta allowlist from Windmill: {e}")
        return set()

    domains = normalize_email_domains(raw)
    _cache = (now + _CACHE_TTL_SECONDS, domains)
    return domains


def is_beta_user_email(email: str | None, domains: set[str]) -> bool:
    """Return True if the email's domain is in the allowlist.

    Returns False when the email is missing/malformed or the allowlist is
    empty, so beta tools stay hidden unless a domain is explicitly opted in.
    """
    if not email or not domains:
        return False
    _, _, domain = email.rpartition("@")
    return bool(domain) and domain.lower() in domains
