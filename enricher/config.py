"""Environment-driven configuration.

All values are read from environment variables at startup via `load()` and
exposed as module-level constants. Parsing is defensive: an unparsable value
logs a warning and falls back to the default; configuration never crashes the
process.
"""

import logging
import os

log = logging.getLogger(__name__)

# Defaults (the env var names below are the exact keys).
BIND = "0.0.0.0"
PORT = 8080
KOITO_URL = "http://koito:4110"
KOITO_DB_PATH = "/data/koito/koito.db"
MB_URL = "https://musicbrainz.org"
MB_USER_AGENT = "koito-mbz-enricher/0.1.0 (+https://github.com/koito-mbz-enricher)"
MB_RATE_LIMIT_SECONDS = 1.0
POST_FORWARD_DELAY_MS = 1000
NEGATIVE_CACHE_SIZE = 10000
NEGATIVE_CACHE_TTL_DAYS = 7
LOG_LEVEL = "INFO"


def _env(name, default, cast, what):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return cast(raw)
    except ValueError:
        log.warning("Invalid %s for %s (%r); using default %r", what, name, raw, default)
        return default


def load() -> None:
    """Read environment variables into the module-level constants. Idempotent."""
    global BIND, PORT, KOITO_URL, KOITO_DB_PATH, MB_URL, MB_USER_AGENT
    global MB_RATE_LIMIT_SECONDS, POST_FORWARD_DELAY_MS
    global NEGATIVE_CACHE_SIZE, NEGATIVE_CACHE_TTL_DAYS, LOG_LEVEL

    BIND = _env("BIND", BIND, str, "string")
    PORT = _env("PORT", PORT, int, "integer")
    KOITO_URL = _env("KOITO_URL", KOITO_URL, str, "string").rstrip("/")
    KOITO_DB_PATH = _env("KOITO_DB_PATH", KOITO_DB_PATH, str, "string")
    MB_URL = _env("MB_URL", MB_URL, str, "string").rstrip("/")
    MB_USER_AGENT = _env("MB_USER_AGENT", MB_USER_AGENT, str, "string")
    MB_RATE_LIMIT_SECONDS = _env("MB_RATE_LIMIT_SECONDS", MB_RATE_LIMIT_SECONDS, float, "float")
    POST_FORWARD_DELAY_MS = _env("POST_FORWARD_DELAY_MS", POST_FORWARD_DELAY_MS, int, "integer")
    NEGATIVE_CACHE_SIZE = _env("NEGATIVE_CACHE_SIZE", NEGATIVE_CACHE_SIZE, int, "integer")
    NEGATIVE_CACHE_TTL_DAYS = _env("NEGATIVE_CACHE_TTL_DAYS", NEGATIVE_CACHE_TTL_DAYS, int, "integer")
    LOG_LEVEL = _env("LOG_LEVEL", LOG_LEVEL, str, "string")
