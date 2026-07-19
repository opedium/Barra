# OpenWiki quickstart

This wiki documents the DouyinBarrage repository: a Douyin live-room barrage collector plus a Flask dashboard for browsing sessions, users, chat, gifts, leaderboards, analytics, and gift pricing.

Start here if you need to understand the system quickly:

- **[Architecture overview](architecture/overview.md)** — how the collector, parser, storage, and web panel fit together.
- **[Workflows](workflows/usage.md)** — how to run one room, many rooms, or the web panel; how room config and cookies are used.
- **[Domain concepts](domain/concepts.md)** — sessions, message types, leaderboard periods, analytics, and gift-price data.
- **[Operations and testing](operations/testing.md)** — config, migration history, common failure modes, and validation commands.
- **[Source map](source-map.md)** — where to look in the repository when changing a particular area.

## What this repository does

The project has two main surfaces:

1. **Collector/runtime entrypoint** (`main.py`, `service/fetcher.py`) — connects to Douyin live WebSocket streams, decodes protobuf frames, deduplicates noisy events, and writes CSV/JSONL plus database records.
2. **Web dashboard** (`app.py`, `templates/`) — serves live and historical views over the collected data, including sessions, chat, anonymous messages, leaderboards, upgrades, gift prices, analytics, and streamer management.

The codebase has evolved from a smaller SQLite-based collector into a richer system with PostgreSQL support, dual-WebSocket merging, gift registry and pricing workflows, and a broader dashboard. Recent commits are the best guide to current behavior.

## Repository shape at a glance

- `main.py` — CLI bootstrap for collector runs and multi-room selection.
- `app.py` — Flask app and API routes for the dashboard.
- `base/` — config, parsing, output, deduplication, and database/query logic.
- `service/` — network helpers, signature generation, and the collector runtime.
- `templates/` — dashboard pages.
- `docs/` — historical feature/spec notes; useful context for why some flows exist.
- `scripts/` — one-off utilities and build helpers.
- `config.yaml` and `rooms.txt` — runtime configuration and room inventory.

## How the system is organized

The collector side reads live Douyin WebSocket traffic, parses protobuf payloads, and persists normalized data. The web side reads from the same storage layer to render views and expose JSON/CSV exports. A few important patterns recur across the repo:

- **Config-driven behavior** — `config.yaml` controls output toggles, reconnect behavior, network timeouts, and web settings.
- **Session-first storage** — the parser organizes data around live sessions and the web UI queries sessions heavily.
- **Noisy live data defense** — dedup caches, delta-based gift counting, watch-dog logic, and merge tracking are used to keep counts stable.
- **Operational convenience** — room lists, cookie management, and web controls are built in so the collector can run unattended.

## Best next pages

- Read **[Architecture overview](architecture/overview.md)** first if you want the mental model.
- Read **[Workflows](workflows/usage.md)** first if you want to run or modify the app safely.
- Read **[Domain concepts](domain/concepts.md)** if you are changing queries, metrics, or UI pages.

## Backlog

- **API reference** — the Flask surface is broad enough that a full endpoint catalog would be noisy; add it only if endpoint-level work becomes frequent.
- **Schema atlas** — the database/query layer is substantial and changes often; defer a table-by-table reference until a future update needs it.
- **UI walkthrough** — the current page set is best understood through the source map and targeted source reads rather than a standalone page tour.
