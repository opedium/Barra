# Operations and testing

This repository combines runtime operations, configuration, and manual validation more than formal unit testing.

## Configuration and runtime inputs

- `config.yaml` controls logging, output toggles, timeouts, reconnect policy, and web-panel settings.
- `rooms.txt` lists enabled rooms for `main.py` and supports commented-out disabled lines.
- `cookie.txt` is a local secret-bearing file and should remain untracked.
- `data/.flask_secret` is created automatically by the web app to preserve Flask sessions across restarts.

## Migration and deployment history

The repo went through a major storage change in commit `0aad8ee`: SQLite support was migrated to PostgreSQL with compatibility wrappers and a data migration script.

Operational consequences:

- SQL placeholder style and type handling can differ between backends
- schema changes should be checked against both parser writes and panel reads
- DB initialization and migration utilities matter more than they did in the earlier SQLite-only design

## Common failure modes

### Collector stalls or goes silent

If the collector appears connected but stops producing useful messages, check the watchdog behavior in `service/fetcher.py`.

Symptoms may include:

- no new chat/gift/interactive messages
- repeated reconnects
- a connection that seems alive but is not delivering business events

### Gift totals look wrong

Gift handling is intentionally stateful. If totals drift, inspect the delta-based dedup path, combo finalization, and any recent changes to registry/override logic.

### Leaderboard numbers look inconsistent

Recent git history shows repeated fixes to leaderboard filtering and sorting. When results look off, compare the query parameters used by the API with the intended period and session scope.

### Dashboard auth or session issues

The Flask app stores its secret in `data/.flask_secret`. Deleting it resets sessions, which can look like login or auth instability.

## Manual validation checklist

- start a collector run for a known room
- confirm it connects, logs, and writes output
- start the Flask panel and open home, sessions, leaderboard, user, and analytics pages
- exercise a CSV export route
- if auth is enabled, confirm login gate behavior for HTML and API routes
- if you changed the database layer, verify the current data still renders after restart

## Practical command ideas

The main docs and code comments point to simple sanity checks rather than a large automated suite:

- `python main.py <live_id>` for a known live room
- `python app.py --port 8080` for the dashboard
- a quick Python syntax compile check on edited modules

Use the exact commands that match your change area; there is no single catch-all test script documented in the repo.
