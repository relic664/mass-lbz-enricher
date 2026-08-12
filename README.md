# koito-mbz-enricher

MusicBrainz enrichment middleware between Music Assistant and Koito.

Music Assistant's ListenBrainz scrobbler only fills MBIDs when the played track
is a library item. Non-library tracks are submitted with **no**
artist/recording/release MBIDs, which degrades Koito's scrobble accuracy. This
service sits between mass and Koito: it fills the missing MBIDs
(`additional_info` **and** `mbid_mapping`) before Koito ingests the listen,
paced at ≤1 QPS against MusicBrainz, and reusing **Koito's own SQLite database**
(mounted, effectively read-only) as the persistent cache so already-known
tracks never re-query MusicBrainz. No changes to mass or Koito are required.

## How it works

```
mass --(ListenBrainz API)--> koito-mbz-enricher --(ListenBrainz API)--> Koito
                                     |
                          +----------+----------+
                          | Koito DB (cache)    |
                          | MusicBrainz WS/2    |
                          +----------+----------+
```

For each listen missing MBIDs:

1. Negative cache hit (`MB had no match`, in-memory LRU, 7-day TTL) → forward
   untouched.
2. Koito DB hit → enrich from the cached row (no MB query).
3. MusicBrainz search (≤3 requests/listen, ≥1 s apart, single 429 retry) →
   enrich; no match → record negative cache entry.

Fully populated listens (library tracks) are proxied byte-for-byte with no MB
query and no delay. After any request that queried MB, the next request is
delayed `POST_FORWARD_DELAY_MS` so Koito's own downstream MB queries land in
the gap before the middleware's next query.

## Deployment

1. Create an API key in Koito's web UI (Settings → API keys).
2. In Music Assistant, open the **ListenBrainz Scrobbler** plugin and set:
   - **User Token**: the Koito API key from step 1 (unchanged behavior),
   - **Base URL** (advanced): `http://koito-mbz-enricher:8080`.
3. Run this service on the same compose project/network as Koito:

   ```yaml
   # docker-compose.yml (this repo) — join it with your Koito + mass compose
   services:
     koito-mbz-enricher:
       build: .
       ports: ["8080:8080"]
       volumes:
         - ./koito:/data/koito   # same host path Koito's compose mounts (./koito:/etc/koito)
   ```

   `docker compose up -d`

The middleware only SELECTs against `koito.db`; it never writes to it. If Koito
runs on a separate host, mount its `koito.db` (or accept the cache degrading to
permanent miss — every new listen then queries MB at 1 QPS, which is still
correct, just slower).

## Configuration (env vars)

| Variable | Default | Meaning |
| --- | --- | --- |
| `BIND` / `PORT` | `0.0.0.0` / `8080` | Listen address |
| `KOITO_URL` | `http://koito:4110` | Koito base URL |
| `KOITO_DB_PATH` | `/data/koito/koito.db` | Koito SQLite file (read-only) |
| `MB_URL` | `https://musicbrainz.org` | MusicBrainz WS/2 base |
| `MB_USER_AGENT` | `koito-mbz-enricher/0.1.0 (+…)` | **Set your contact here** — MB policy requires an identifying UA |
| `MB_RATE_LIMIT_SECONDS` | `1.0` | Minimum gap between MB requests (MB's 1 QPS policy) |
| `POST_FORWARD_DELAY_MS` | `1000` | Sleep after forwarding a listen that queried MB. Raise to 2000–3000 if Koito ever sees MB 429s |
| `NEGATIVE_CACHE_SIZE` | `10000` | In-memory "MB had no match" LRU size |
| `NEGATIVE_CACHE_TTL_DAYS` | `7` | How long a no-match is remembered |
| `LOG_LEVEL` | `INFO` | Logging level |

## Accepting the change

Play a **non-library** track in mass. In Koito's web UI, the track should now
show a MusicBrainz ID after the first play. Play it again — no new MB requests
should occur (Koito DB cache hit). If Koito ever reports MB 429s, raise
`POST_FORWARD_DELAY_MS` to 2000–3000; no code change needed.

## Layout

- `enricher/main.py` — HTTP server + serialized submit pipeline
- `enricher/enrich.py` — MusicBrainz WS/2 client, matching, payload rewrite, negative cache
- `enricher/koitodb.py` — Koito SQLite read-only lookup
- `enricher/limiter.py` — MB rate limiter + pipeline lock
- `enricher/config.py` — environment configuration

Stdlib-only Python 3.12, zero third-party dependencies.
