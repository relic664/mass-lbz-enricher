"""MusicBrainz WS/2 client, title/artist matching, and payload rewrite.

Everything MusicBrainz-related the service needs lives here: one
``GET /ws/2/recording`` search call, candidate selection, MBID extraction, and
the merge-only rewrite of the ListenBrainz ``track_metadata`` that Koito
consumes. The negative cache records "MB had no match" so known-missing tracks
are not re-queried within the TTL.
"""

import json
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass, field

from . import config
from .limiter import MBRateLimiter

log = logging.getLogger(__name__)

_TRAILING_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")
_ARTIST_SPLIT_RE = re.compile(r",\s*|\s+feat\.?\s+|\s+&\s+", re.IGNORECASE)
_HTTP_TIMEOUT = 15


@dataclass
class Enrichment:
    """MBIDs recovered for one listen, in Koito's preferred shapes."""

    recording_mbid: str | None = None
    release_mbid: str | None = None
    release_group_mbid: str | None = None
    artist_mbids: list[str] = field(default_factory=list)
    artist_credits: list[tuple[str, str]] = field(default_factory=list)  # (name, mbid) in credit order


class MusicBrainzUnavailable(Exception):
    """MusicBrainz could not be queried (429 after retry, 5xx, or network error)."""


def normalize_title(s: str) -> str:
    """Lowercase, collapse whitespace, strip one trailing parenthetical.

    mass can append ``(version)``-style suffixes to track names
    (ScrobblerConfig.suffix_version); the parenthetical is not part of the
    title for matching purposes.
    """
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return _TRAILING_PAREN_RE.sub("", s)


def artist_variants(s: str) -> list[str]:
    """Variant artist strings, most specific first.

    Index ``[0]`` is the *first-artist segment*: the part of the string before
    the first split on ``,``, `` feat. ``, or `` & `` (case-insensitive); the
    full string follows when it differs. All call sites (MB query 2, the
    Koito-DB segment query, the negative-cache key) contract on ``[0]`` being
    that segment, so ``"Daft Punk feat. Pharrell Williams"`` yields
    ``["daft punk"-ish segment first, full]``. Deduped, non-empty.
    """
    s = s.strip()
    if not s:
        return []
    segment = _ARTIST_SPLIT_RE.split(s, maxsplit=1)[0].strip()
    out = []
    for variant in (segment, s):
        if variant and variant not in out:
            out.append(variant)
    return out


def _filled(value) -> bool:
    """True iff a payload field carries at least one non-empty string."""
    if isinstance(value, list):
        return bool(value) and all(isinstance(x, str) and x for x in value)
    return bool(value)


def needs_enrichment(track_metadata: dict) -> bool:
    """True iff any of ``additional_info``'s artist/recording/release MBIDs is missing or empty."""
    ai = track_metadata.get("additional_info") or {}
    return not (
        _filled(ai.get("artist_mbids")) and _filled(ai.get("recording_mbid")) and _filled(ai.get("release_mbid"))
    )


def rewrite_payload(track_metadata: dict, e: Enrichment) -> None:
    """Merge enrichment into ``track_metadata`` in place. Never overwrites what
    mass already sent (a library track's artist_mbids are authoritative).

    ``additional_info`` only receives missing fields; ``mbid_mapping`` is set
    unconditionally as Koito's fallback source, and ``mbid_mapping.artists`` is
    Koito's highest-priority artist association source.
    """
    ai = track_metadata.setdefault("additional_info", {})
    if not _filled(ai.get("recording_mbid")) and e.recording_mbid:
        ai["recording_mbid"] = e.recording_mbid
    if not _filled(ai.get("release_mbid")) and e.release_mbid:
        ai["release_mbid"] = e.release_mbid
    if not _filled(ai.get("release_group_mbid")) and e.release_group_mbid:
        ai["release_group_mbid"] = e.release_group_mbid
    if not _filled(ai.get("artist_mbids")) and e.artist_mbids:
        ai["artist_mbids"] = list(e.artist_mbids)

    mm = track_metadata.setdefault("mbid_mapping", {})
    mm["recording_mbid"] = e.recording_mbid
    mm["release_mbid"] = e.release_mbid
    mm["artist_mbids"] = list(e.artist_mbids)
    mm["artists"] = [{"artist_mbid": m, "artist_credit_name": n} for n, m in e.artist_credits]


class NegativeCache:
    """Bounded TTL LRU of "MusicBrainz had no match" lookups.

    Key: ``(artist_variants(artist)[0].lower(), normalize_title(title))``.
    Value: monotonic timestamp of when the miss was recorded.
    """

    def __init__(self, maxsize=None, ttl_seconds=None):
        self._maxsize = maxsize if maxsize is not None else config.NEGATIVE_CACHE_SIZE
        self._ttl = ttl_seconds if ttl_seconds is not None else config.NEGATIVE_CACHE_TTL_DAYS * 86400
        self._data = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def _key(artist, title):
        variants = artist_variants(artist)
        return (variants[0].lower() if variants else artist.lower(), normalize_title(title))

    def is_negative(self, artist, title) -> bool:
        key = self._key(artist, title)
        with self._lock:
            ts = self._data.get(key)
            if ts is None:
                return False
            if time.monotonic() - ts > self._ttl:
                del self._data[key]
                return False
            self._data.move_to_end(key)
            return True

    def mark(self, artist, title) -> None:
        key = self._key(artist, title)
        with self._lock:
            self._data[key] = time.monotonic()
            self._data.move_to_end(key)
            while len(self._data) > self._maxsize:
                self._data.popitem(last=False)

    def configure(self, maxsize, ttl_seconds) -> None:
        with self._lock:
            self._maxsize = maxsize
            self._ttl = ttl_seconds


negative_cache = NegativeCache()


def configure_negative_cache(maxsize, ttl_days) -> None:
    negative_cache.configure(maxsize, ttl_days * 86400)


def is_negative(artist, title) -> bool:
    return negative_cache.is_negative(artist, title)


def mark_negative(artist, title) -> None:
    negative_cache.mark(artist, title)


def _lucene_escape(s: str) -> str:
    return s.replace('"', '\\"')


class MusicBrainzClient:
    """Minimal MusicBrainz WS/2 recording-search client (stdlib only)."""

    def __init__(self, base_url=None, user_agent=None, limiter=None):
        self._base = (base_url or config.MB_URL).rstrip("/")
        self._user_agent = user_agent or config.MB_USER_AGENT
        self._limiter = limiter if limiter is not None else MBRateLimiter()

    # -- public -----------------------------------------------------------

    def search(self, artist, title, duration_s=None, release=None) -> Enrichment | None:
        """Find the recording, at most 3 MB requests.

        Query 1: exact ``recording`` + ``artist``. If no title-matching
        candidate: Query 2 with the first-artist segment. If still none:
        Query 3 title-only, requiring the artist name to appear in the chosen
        candidate's artist-credit. Returns None when MB has no match; raises
        ``MusicBrainzUnavailable`` on transport/HTTP failures (never marks a
        negative entry for those).
        """
        esc_title = _lucene_escape(title)
        steps = [(f'recording:"{esc_title}" AND artist:"{_lucene_escape(artist)}"', False)]
        variants = artist_variants(artist)
        if len(variants) > 1:  # segment differs from the full string
            steps.append((f'recording:"{esc_title}" AND artist:"{_lucene_escape(variants[0])}"', False))
        steps.append((f'recording:"{esc_title}"', True))  # title-only: artist must appear in credits

        for query, require_artist in steps:
            data = self._search(query)
            if data is None:
                continue  # no recordings at all -> next query
            best = self._select(data.get("recordings") or [], title, duration_s)
            if best is None:
                continue  # no title-matching candidate -> next query
            if require_artist and not self._artist_in_credits(best, artist):
                return None
            return self._enrichment_from_recording(best, release)
        return None

    # -- internals --------------------------------------------------------

    def _search(self, query) -> dict | None:
        """One query with a single 429 retry. None = zero recordings."""
        url = self._base + "/ws/2/recording?" + urllib.parse.urlencode(
            {"query": query, "fmt": "json", "limit": 10}
        )
        for _ in range(2):
            result = self._http_get(url)
            if result is None:
                raise MusicBrainzUnavailable(f"network error for {url}")
            status, headers, body = result
            if status == 429:
                retry_after = self._retry_after(headers)
                log.warning("MusicBrainz rate-limited (429); retrying in %.1fs", retry_after)
                time.sleep(retry_after)
                continue
            if status != 200:
                log.warning("MusicBrainz returned HTTP %d for query %r", status, query)
                raise MusicBrainzUnavailable(f"HTTP {status}")
            try:
                return json.loads(body)
            except ValueError:
                log.warning("MusicBrainz returned invalid JSON for query %r", query)
                raise MusicBrainzUnavailable("invalid JSON")
        log.warning("MusicBrainz still rate-limited (429) after retry; forwarding unenriched")
        raise MusicBrainzUnavailable("429 after retry")

    def _http_get(self, url):
        """GET with limiter + UA. Returns (status, headers, body) or None on network error."""
        self._limiter.wait()
        req = urllib.request.Request(
            url, headers={"User-Agent": self._user_agent, "Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                return resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            log.warning("MusicBrainz request failed: %s", e)
            return None

    @staticmethod
    def _retry_after(headers) -> float:
        try:
            seconds = float(headers.get("Retry-After"))
        except (TypeError, ValueError):
            seconds = 5.0
        return min(max(seconds, 0.0), 30.0) + 0.5  # never block scrobbling for long

    def _select(self, recordings, title, duration_s):
        """Best candidate: title must normalize-match; prefer length proximity
        (when duration is known), then non-empty releases. None = no match."""
        norm = normalize_title(title)
        candidates = [r for r in recordings if normalize_title(r.get("title") or "") == norm]
        if not candidates:
            return None
        if duration_s:
            close = [r for r in candidates if self._length_close(r, duration_s)]
            if close:
                candidates = close
        with_releases = [r for r in candidates if r.get("releases")]
        if with_releases:
            candidates = with_releases
        return candidates[0]

    @staticmethod
    def _length_close(recording, duration_s) -> bool:
        try:
            length_ms = int(recording.get("length"))
        except (TypeError, ValueError):
            return False
        return abs(length_ms / 1000.0 - duration_s) <= 2

    @staticmethod
    def _artist_in_credits(recording, artist) -> bool:
        needles = [v.lower() for v in artist_variants(artist)]
        if not needles:
            return False
        hay = " | ".join(
            (c.get("artist") or {}).get("name") or c.get("name") or ""
            for c in recording.get("artist-credit") or []
        ).lower()
        return any(n in hay for n in needles)

    @staticmethod
    def _enrichment_from_recording(recording, release) -> Enrichment:
        artist_mbids: list[str] = []
        artist_credits: list[tuple[str, str]] = []
        seen: set[str] = set()
        for credit in recording.get("artist-credit") or []:
            artist = credit.get("artist") or {}
            mbid = artist.get("id")
            if not mbid or mbid in seen:
                continue
            seen.add(mbid)
            # Never emit a null artist_credit_name: Multi Scrobbler's
            # findDelimiters() crashes on it. MB always sends names in practice.
            name = credit.get("name") or artist.get("name") or ""
            artist_mbids.append(mbid)
            artist_credits.append((name, mbid))

        chosen = None
        if release:
            needle = release.strip().lower()
            for r in recording.get("releases") or []:
                if (r.get("title") or "").strip().lower() == needle:
                    chosen = r
                    break
        if chosen is None and recording.get("releases"):
            chosen = recording["releases"][0]

        release_mbid = chosen.get("id") if chosen else None
        release_group_mbid = (chosen.get("release-group") or {}).get("id") if chosen else None
        return Enrichment(
            recording_mbid=recording.get("id"),
            release_mbid=release_mbid,
            release_group_mbid=release_group_mbid,
            artist_mbids=artist_mbids,
            artist_credits=artist_credits,
        )
