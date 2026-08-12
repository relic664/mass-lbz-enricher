"""HTTP middleware: mass ListenBrainz -> enrichment -> Koito.

Two endpoints, mirroring the ListenBrainz API surface mass uses:
  GET  /1/validate-token   transparent proxy to Koito
  POST /1/submit-listens   enrich missing MBIDs, then forward to Koito

The submit pipeline runs under ``limiter.pipeline`` so MB queries and Koito's
downstream MusicBrainz queries are serialized and spaced apart.
"""

import json
import logging
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config
from .enrich import (
    MusicBrainzClient,
    MusicBrainzUnavailable,
    configure_negative_cache,
    is_negative,
    mark_negative,
    needs_enrichment,
    rewrite_payload,
)
from .koitodb import KoitoDBLookup
from .limiter import pipeline

log = logging.getLogger(__name__)

_KOITO_FORWARD_TIMEOUT = 30
_KOITO_PROXY_TIMEOUT = 15


class Handler(BaseHTTPRequestHandler):
    server_version = "koito-mbz-enricher/0.1.0"
    protocol_version = "HTTP/1.1"

    client: MusicBrainzClient | None = None
    lookup: KoitoDBLookup | None = None

    def log_message(self, fmt, *args):
        log.info("http: " + fmt, *args)

    # -- routing ----------------------------------------------------------

    def do_GET(self):
        if self.path.split("?")[0] == "/1/validate-token":
            self._validate_token()
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path.split("?")[0] == "/1/submit-listens":
            self._submit_listens()
        else:
            self._send_json(404, {"error": "not found"})

    # -- endpoints --------------------------------------------------------

    @staticmethod
    def _lbz_url(endpoint: str) -> str:
        """Upstream URL for an LBZ endpoint (``submit-listens`` / ``validate-token``).

        Koito serves these under ``/apis/listenbrainz/1/...``; standard
        ListenBrainz-compatible endpoints (Multi Scrobbler, ListenBrainz itself)
        serve them at ``/1/...`` -> set ``KOITO_LBZ_BASE_PATH`` to "".
        """
        base = config.KOITO_LBZ_BASE_PATH  # already stripped of slashes in config
        prefix = f"/{base}/1" if base else "/1"
        return f"{config.KOITO_URL}{prefix}/{endpoint}"

    def _validate_token(self):
        url = self._lbz_url("validate-token")
        req = urllib.request.Request(
            url, headers={"Authorization": self.headers.get("Authorization") or ""}
        )
        try:
            with urllib.request.urlopen(req, timeout=_KOITO_PROXY_TIMEOUT) as resp:
                self._send_bytes(resp.status, resp.read(), resp.headers.get("Content-Type") or "application/json")
        except urllib.error.HTTPError as e:
            self._send_bytes(e.code, e.read(), e.headers.get("Content-Type") or "application/json")
        except Exception:
            log.warning("Koito validate-token unreachable at %s", config.KOITO_URL, exc_info=True)
            self._send_json(502, {"error": "koito unreachable"})

    def _submit_listens(self):
        with pipeline:
            rewrote = False
            queried_mb = False

            body = self._read_body()
            try:
                req = json.loads(body)
            except (ValueError, TypeError):
                self._send_json(400, {"error": "invalid json"})
                return
            if not isinstance(req, dict) or not isinstance(req.get("payload"), list):
                self._send_json(400, {"error": "invalid json"})
                return

            for item in req["payload"]:
                if not isinstance(item, dict):
                    continue
                tm = item.get("track_metadata")
                if not isinstance(tm, dict):
                    continue
                if not needs_enrichment(tm):
                    continue  # fully populated (library track): untouched

                artist = (tm.get("artist_name") or "").strip()
                title = (tm.get("track_name") or "").strip()
                release = tm.get("release_name") or None
                if not artist or not title:
                    continue  # cannot enrich without names; forward as-is

                if is_negative(artist, title):
                    continue  # MB already answered "no match" for this track

                enrichment = Handler.lookup.lookup(artist, title, release)
                if enrichment is not None:
                    rewrite_payload(tm, enrichment)
                    rewrote = True
                    continue  # Koito DB cache hit: no MB query

                ai = tm.get("additional_info") or {}
                duration_s = ai.get("duration")  # seconds, per Koito's parse
                if not duration_s and ai.get("duration_ms"):
                    duration_s = ai["duration_ms"] / 1000.0

                queried_mb = True  # the search below always fires >= 1 MB request
                try:
                    enrichment = Handler.client.search(artist, title, duration_s, release)
                except MusicBrainzUnavailable:
                    continue  # forward unenriched; do NOT poison the negative cache
                if enrichment is not None:
                    rewrite_payload(tm, enrichment)
                    rewrote = True
                else:
                    mark_negative(artist, title)

            out_body = json.dumps(req).encode("utf-8") if rewrote else body
            status, resp_body, resp_ct = self._forward(out_body)
            self._send_bytes(status, resp_body, resp_ct)

            if queried_mb:
                # After forwarding, so Koito's own MusicBrainz queries land in
                # this gap before the middleware's next MB query.
                time.sleep(config.POST_FORWARD_DELAY_MS / 1000.0)

    # -- plumbing ---------------------------------------------------------

    def _read_body(self) -> bytes:
        length = self.headers.get("Content-Length")
        if length:
            try:
                length = int(length)
            except ValueError:
                length = 0
        else:
            length = 0
        return self.rfile.read(length) if length > 0 else self.rfile.read()

    def _forward(self, body):
        url = self._lbz_url("submit-listens")
        headers = {
            "Authorization": self.headers.get("Authorization") or "",
            "Content-Type": self.headers.get("Content-Type") or "application/json",
        }
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=_KOITO_FORWARD_TIMEOUT) as resp:
                return resp.status, resp.read(), resp.headers.get("Content-Type") or "application/json"
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers.get("Content-Type") or "application/json"
        except Exception:
            log.warning("Koito submit-listens unreachable at %s", config.KOITO_URL, exc_info=True)
            return 502, b'{"error": "koito unreachable"}', "application/json"

    def _send_json(self, status, obj):
        self._send_bytes(status, json.dumps(obj).encode("utf-8"), "application/json")

    def _send_bytes(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    config.load()
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    configure_negative_cache(config.NEGATIVE_CACHE_SIZE, config.NEGATIVE_CACHE_TTL_DAYS)
    Handler.client = MusicBrainzClient()
    Handler.lookup = KoitoDBLookup()

    server = ThreadingHTTPServer((config.BIND, config.PORT), Handler)
    log.info(
        "koito-mbz-enricher listening on %s:%d (MB %s, Koito %s, Koito DB %s)",
        config.BIND,
        config.PORT,
        config.MB_URL,
        config.KOITO_URL,
        config.KOITO_DB_PATH,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
