"""Read-only lookups against Koito's SQLite database.

Koito stores the MBIDs it has already discovered, so a listen whose track
already exists there never needs to hit MusicBrainz again. The lookup joins
Koito's migration-created views (``tracks_with_title``,
``releases_with_title``, ``artists_with_name``); a missing view or table is
treated as a miss, never a crash — the cache degrades to "always query MB",
which stays correct.
"""

import logging
import sqlite3

from . import config
from .enrich import Enrichment, artist_variants

log = logging.getLogger(__name__)

_LOOKUP_SQL = """
SELECT t.musicbrainz_id, r.musicbrainz_id, a.musicbrainz_id
FROM tracks_with_title twt
JOIN tracks t ON t.id = twt.id
JOIN releases_with_title rwt ON rwt.id = t.release_id
JOIN releases r ON r.id = rwt.id
JOIN artist_tracks at2 ON at2.track_id = t.id
JOIN artists_with_name awn ON awn.id = at2.artist_id
JOIN artists a ON a.id = awn.id
WHERE lower(twt.title) = lower(?) AND lower(awn.name) = lower(?)
"""


class KoitoDBLookup:
    """Fresh read-only connection per lookup; failures degrade to a miss."""

    def __init__(self, db_path=None):
        self._db_path = db_path or config.KOITO_DB_PATH

    def lookup(self, artist, title, release=None) -> Enrichment | None:
        try:
            return self._lookup(artist, title, release)
        except Exception:
            log.debug("Koito DB lookup failed for %r / %r", artist, title, exc_info=True)
            return None

    def _lookup(self, artist, title, release) -> Enrichment | None:
        # Match order, first non-empty row wins:
        # (1) full artist + title (+ release, when given)
        # (2) full artist + title
        # (3) first-artist segment + title
        variants = artist_variants(artist)
        segment = variants[0] if variants else artist

        # NOTE: the SQL's placeholders are (title, artist_name), then release.
        queries = []
        if release:
            queries.append((_LOOKUP_SQL + " AND lower(rwt.title) = lower(?)", (title, artist, release)))
        queries.append((_LOOKUP_SQL, (title, artist)))
        if segment != artist:
            queries.append((_LOOKUP_SQL, (title, segment)))

        for sql, params in queries:
            enrichment = self._query(sql, params)
            if enrichment is not None:
                return enrichment
        return None

    def _query(self, sql, params) -> Enrichment | None:
        conn = None
        try:
            conn = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        except sqlite3.OperationalError:
            # WAL/-shm edge case (e.g. the file is momentarily locked): retry rw,
            # the compose mount is rw anyway. SELECT-only either way.
            try:
                conn = sqlite3.connect(f"file:{self._db_path}?mode=rw", uri=True)
            except sqlite3.OperationalError:
                log.debug("Koito DB %s not openable (ro or rw)", self._db_path, exc_info=True)
                return None
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        for recording_mbid, release_mbid, artist_mbid in rows:
            if recording_mbid and release_mbid and artist_mbid:
                return Enrichment(
                    recording_mbid=recording_mbid,
                    release_mbid=release_mbid,
                    artist_mbids=[artist_mbid],
                    # Koito skips mapping entries with an empty credit name; the
                    # plain artist_mbids list still carries the association.
                    artist_credits=[(None, artist_mbid)],
                )
        return None
