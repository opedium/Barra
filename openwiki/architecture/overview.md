# Architecture overview

DouyinBarrage is split into a collector/runtime path and a dashboard/query path. The two surfaces share the same storage and parsing layer.

## High-level architecture

1. `main.py` starts the collector and handles CLI room selection, signal handling, and multi-room orchestration.
2. `service/fetcher.py` owns the Douyin live connection lifecycle: room lookup, cookie loading, signature generation, WebSocket connect/reconnect, heartbeat, watchdogs, and message dispatch.
3. `base/parser.py` turns protobuf payloads into structured records, applies dedup and business rules, and writes to the database/buffered outputs.
4. `base/output.py` handles asynchronous logging and the multi-room status panel so ingest work is not blocked by console I/O.
5. `app.py` serves the Flask dashboard, JSON APIs, CSV exports, authentication gates, and streamer management.

## Runtime layers

### CLI collector

`main.py` is the user-facing entrypoint. It validates live-room IDs, loads `rooms.txt`, supports interactive selection, and spawns one `DouyinBarrage` instance per room.

Important behaviors:

- room entries may be commented out with `#`
- missing room names are backfilled after the first successful room-info lookup
- multi-room runs keep a shared shutdown path so Ctrl+C stops every active collector

### Collector engine

`service/fetcher.py` is the core runtime. It combines several concerns that would otherwise be easy to split incorrectly:

- HTTP setup and Douyin metadata fetches
- cookie loading and login-state detection
- `sign.js`-backed request signing
- WebSocket connection management and reconnect policy
- heartbeat, watchdog, and live-wait behavior
- dual-WebSocket merge mode for gap filling and dedup

The recent history matters here: the collector gained PostgreSQL-aware persistence, dual-WS merge support, and more defensive handling around silent or broken connections. Those changes explain why the runtime has both connection-management code and fairly rich local state.

### Parsing and storage

`base/parser.py` is the most important source of business rules. It parses protobuf frames into event records, handles gift deduplication and gift-price correction, manages session lifecycle, and exposes the query functions used by the dashboard.

The parser is also where many of the repository's “why” decisions live:

- gift messages are deduplicated by delta rather than raw repeat counts
- combo/stream-stop edge cases are handled so final gift totals are not lost
- leaderboard queries support multiple cross-session time windows
- analytics queries derive retention, big spenders, and silent whales from session history

The file now carries PostgreSQL-compatible query logic as well as the older SQLite-shaped access pattern via compatibility wrappers.

### Output and observability

`base/output.py` decouples logging from the hot path by buffering records and draining them asynchronously. It also renders the multi-room status panel in the console and adds room labels to log messages.

This exists because live traffic and UI work can otherwise back up the collector; buffered logging keeps the ingest loop focused on parsing and persistence.

### Web dashboard

`app.py` is the Flask app and API layer. It reads from the parser/query layer and presents both HTML templates and programmatic endpoints.

Notable responsibilities:

- auth gate and login session handling
- streamer list management and enable/disable toggles
- session, user, chat, anonymous, million, and leaderboard views
- gift price inspection and recalculation tooling
- analytics pages for retention, big spenders, and silent whales
- CSV exports for the major views
- SSE/event buffering for live UI updates

## Data flow

```text
Douyin live room
  -> WebSocket frames / HTTP metadata
  -> service.fetcher.DouyinBarrage
  -> base.messages protobuf decode
  -> base.parser handlers + dedup + session logic
  -> SQLite/PostgreSQL storage + CSV/JSONL outputs
  -> app.py dashboard queries / exports / charts
```

## Current implementation signals from git history

A few recent commits explain the current shape of the code:

- **0aad8ee** — migration from SQLite to PostgreSQL; introduced DB compatibility wrappers and SQL rewrites.
- **821cd3d** — dual WebSocket merge system, subscription rewrite, badge images, gift registry, and social-event recording.
- **2466ec8** — multi-cookie management in the settings UI.
- **34a5315** and **761dd0e** — leaderboard correctness fixes, especially around cross-session filtering and session-count sorting.

When changing architecture-sensitive behavior, check the commit history around these areas first; many functions exist because a prior bug or protocol change required them.
