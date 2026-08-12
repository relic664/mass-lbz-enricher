"""Read-only lookups against Koito's SQLite database.

Koito stores the MBIDs it has already discovered, so a listen whose track
already exists there never needs to hit MusicBrainz again. The lookup joins
Koito's migration-created views (``tracks_with_title``,
``releases_with_title``, ``artists_with_name``); a missing view or table is
treated as a miss, never a crash — the cache degrades to "always query MB",
which stays correct.

mass comma-joins multi-artist tracks ("Taylor Swift, Gracie Abrams"), so the
lookup is two-step: find the first track matching (title, artist[, release]),
then fetch **all** of that track's artists, primary (first-credited) first.
"""

import logging
import sqlite3

from . import config
from .enrich import Enrichment, artist_variants

log = logging.getLogger(__name__)

# Finds the first track matching (title, artist[, release]); one row per track.
_TRACK_SQL = """
SELECT t.id, t.musicbrainz_id, r.musicbrainz_id
FROM tracks_with_title twt
JOIN tracks t ON t.id = twt.id
JOIN releases_with_title rwt ON rwt.id = t.release_id
JOIN releases r ON r.id = rwt.id
JOIN artist_tracks at2 ON at2.track_id = t.id
JOIN artists_with_name awn ON awn.id = at2.artist_id
WHERE lower(twt.title) = lower(?) AND lower(awn.name) = lower(?)
{release_clause}
ORDER BY t.id
LIMIT 1
"""

# All artists of one track, primary (first-credited) artist first.
_ARTISTS_SQL = """
SELECT awn.musicbrainz_id, awn.name
FROM artist_tracks at2
JOIN artists_with_name awn ON awn.id = at2.artist_id
WHERE at2.track_id = ?
ORDER BY at2.is_primary DESC, awn.id
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
        # Match order, first hit wins:
        # (1) full artist + title (+ release, when given)
        # (2) full artist + title
        # (3) first-artist segment + title (mass joins artists with ", ")
        variants = artist_variants(artist)
        segment = variants[0] if variants else artist

        if release:
            enrichment = self._query(title, artist, release)
            if enrichment is not None:
                return enrichment
        enrichment = self._query(title, artist)
        if enrichment is not None:
            return enrichment
        if segment != artist:
            return self._query(title, segment)
        return None

    def _query(self, title, artist, release=None) -> Enrichment | None:
        conn = self._connect()
        if conn is None:
            return None
        try:
            if release:
                sql = _TRACK_SQL.format(release_clause=" AND lower(rwt.title) = lower(?)")
                params = (title, artist, release)
            else:
                sql = _TRACK_SQL.format(release_clause="")
                params = (title, artist)
            row = conn.execute(sql, params).fetchone()
            if row is None:
                return None
            track_id, recording_mbid, release_mbid = row
            if not recording_mbid or not release_mbid:
                return None
            artists = [
                (name, mbid)
                for mbid, name in conn.execute(_ARTISTS_SQL, (track_id,)).fetchall()
                if mbid
            ]
        finally:
            conn.close()
        if not artists:
            return None
        return Enrichment(
            recording_mbid=recording_mbid,
            release_mbid=release_mbid,
            artist_mbids=[m for _, m in artists],
            # Primary alias names from the artists_with_name view (NOT NULL in
            # the schema) — never null: a null artist_credit_name crashes some
            # LBZ consumers (Multi Scrobbler).
            artist_credits=artists,
        )

    def _connect(self):
        try:
            return sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        except sqlite3.OperationalError:
            # WAL/-shm edge case (e.g. the file is momentarily locked): retry rw,
            # the compose mount is rw anyway. SELECT-only either way.
            try:
                return sqlite3.connect(f"file:{self._db_path}?mode=rw", uri=True)
            except sqlite3.OperationalError:
                log.debug("Koito DB %s not openable (ro or rw)", self._db_path, exc_info=True)
                return None
