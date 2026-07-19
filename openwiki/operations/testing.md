# Operations and testing

## Configuration files

- `config.yaml` — main runtime config for collector, network, and web-panel settings.
- `rooms.txt` — collector-side room list for CLI runs.
- `cookie.txt` / optional secondary cookie file — login state for richer data capture.
- `data/` — persistent runtime output, including the database and per-room artifacts.

Do not document or commit secrets. The repo already treats cookie files as sensitive.

## Runbook notes

### If the collector appears stuck

Check these in order:

1. Are the WebSocket and HTTP timeouts too aggressive for the current network?
2. Is the room actually live, or is the collector in wait-live mode?
3. Is a watchdog firing because the stream has data but no meaningful business events?
4. Is the signature or cookie state stale?
5. If dual-WS mode is enabled, verify both connection paths and cookie files.

### If gift totals look wrong

The recent history suggests three common causes:

- repeat-count delta handling regressed
- a combo finalization edge case was missed during stop/reconnect
- a limited/skin gift needs a price override or registry update

### If dashboard queries look wrong

Check the parser/query layer before the templates. The current repo has had leaderboard fixes for:

- cross-session period filtering
- sorting by sessions versus consumption
- filtering invalid or empty user IDs

### If web login behaves unexpectedly

The web app can be password-protected through `config.yaml`. API requests and page requests are handled differently when auth is enabled.

## Migration history that matters

A major repository change was the move from SQLite to PostgreSQL in commit `0aad8ee`.

That migration introduced:

- PostgreSQL DDL and placeholder differences
- a connection-pool wrapper that still behaves like the prior connection API
- SQL rewrites for strict GROUP BY behavior
- helper scripts for migration and server setup

Later fixes adjusted type handling and write paths, so when changing persistence logic, review the recent history around `0aad8ee` and `1532ea9`.

## Validation guidance

There is no single canonical test suite surfaced in the source excerpts, so validation is usually practical:

- run the collector in a controlled room or against known sample data
- verify the Flask app starts and key pages render
- inspect session rows, leaderboard views, and gift-price/audit pages after a sample ingest
- compare output files and DB rows when changing parser or dedup behavior

Recommended targeted checks when editing major areas:

- **collector/network changes** — verify connect/reconnect, heartbeat, and room wait behavior
- **parser/storage changes** — confirm message counts, session creation/end, and gift dedup totals
- **dashboard/query changes** — check HTML and JSON endpoints together
- **gift-price changes** — verify registry lookups, audit data, and recalculation paths

## Historical notes worth keeping in mind

Recent commits show that the project repeatedly tightened correctness around:

- duplicate gift counting
- session boundaries and ghost sessions
- websocket reconnect stability
- leaderboard filtering
- multi-cookie support
- PostgreSQL compatibility

If you change one of those areas, prefer a focused manual verification over broad assumptions.
