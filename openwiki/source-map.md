# Source map

A short map from repository areas to the wiki pages that explain them.

## Entry points

- `main.py` → collector CLI and room selection logic. See [Workflows](workflows/usage.md).
- `app.py` → Flask dashboard, APIs, CSV exports, auth, streamer control. See [Architecture overview](architecture/overview.md) and [Domain concepts](domain/concepts.md).

## Core runtime

- `service/fetcher.py` → live connection management, WebSocket handling, watch-dogs, live waiting, dual-WS merge. See [Architecture overview](architecture/overview.md).
- `service/network.py` → Douyin HTTP helpers, room lookup, cookie/signature helpers.
- `service/signer.py` → signature generation via `sign.js`.

## Parsing, storage, and output

- `base/parser.py` → protobuf event parsing, dedup, session logic, gift registry/pricing logic, and database queries.
- `base/output.py` → asynchronous logging and status-panel rendering.
- `base/utils.py` → config, cookie parsing, and formatting helpers.
- `base/dedup.py` → auxiliary dedup and merge-tracking support for dual-WS mode.
- `base/messages.py` → protobuf message definitions.
- `base/gift_registry_data.py` → built-in gift registry seed data.

## Web pages and UI

- `templates/` → dashboard HTML pages (`index`, `sessions`, `leaderboard`, `analytics`, `gift_prices`, etc.).
- `docs/` → historical notes, feature writeups, and design plans that explain why some workflows exist.

## Operations and configuration

- `config.yaml` → runtime and web settings.
- `rooms.txt` → collector room inventory.
- `cookie.txt` / secondary cookie file → login state.
- `data/` → runtime database and room artifacts.
- `scripts/` → migration/setup and helper scripts; useful when reproducing deployment or data conversion steps.

## When to read what

- Change collector behavior? Start with `service/fetcher.py`, then `base/parser.py`.
- Change dashboard views or exports? Start with `app.py` and the related query functions in `base/parser.py`.
- Change gift pricing? Start with `base/parser.py`, `app.py`, and `base/gift_registry_data.py`.
- Change runtime configuration? Start with `config.yaml`, `main.py`, and `service/fetcher.py`.

## Historical context

If something looks odd, check recent git history before assuming it's accidental. Recent commits explain many of the current code shapes:

- PostgreSQL migration and compatibility wrappers
- dual WebSocket merge support
- cookie management improvements
- leaderboard filtering/sorting fixes
- gift counting and combo finalization fixes
