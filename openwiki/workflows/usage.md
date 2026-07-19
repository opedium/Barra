# Workflows

This page describes the main ways engineers interact with the repository.

## 1. Start the collector

The collector entrypoint is `main.py`.

Typical modes:

- **Interactive single-room run** — launch without arguments and choose a room from `rooms.txt`.
- **Direct room run** — pass a live-room ID on the command line.
- **Multi-room run** — choose multiple configured rooms; each room gets its own collector instance and thread.
- **Live-stop vs wait-live** — when a room ends, the collector either exits or waits for the next broadcast depending on config/CLI.

What to watch out for:

- `rooms.txt` entries must be valid Douyin live-room IDs.
- Commented lines are treated as disabled rooms.
- Missing room names are written back after the first successful room lookup.
- `cookie.txt` is optional for basic capture, but it improves coverage of richer data such as gifts and login-specific fields.

## 2. Collector runtime behavior

`service/fetcher.py` manages the live connection loop. The important operational flow is:

1. load config and cookies
2. resolve room metadata and signature inputs
3. connect WebSocket(s)
4. process PushFrame / protobuf payloads
5. emit parsed events to storage and output buffers
6. keep heartbeats and watchdogs running
7. reconnect or wait for live state as needed

The collector is deliberately defensive:

- it uses timeouts and reconnect backoff
- it has watchdogs for silent connections and “data but no business messages” cases
- it can merge data from a secondary WebSocket for coverage
- it buffers output so hot-path message handling is not blocked by logging or console refreshes

## 3. Run the dashboard

`app.py` starts the Flask panel.

The web app provides:

- a home/dashboard view
- sessions and session-detail pages
- user, chat, anonymous, million, and upgrades pages
- leaderboard pages and CSV exports
- gift price management and audit views
- analytics views for retention, big spenders, and silent whales
- streamer start/stop/toggle APIs
- cookie/login and dual-WebSocket configuration APIs

The dashboard can be protected with a password from `config.yaml`.

## 4. Manage rooms and streamers

There are two related room-management flows:

- **`rooms.txt`** is the collector-side list used by `main.py` for command-line runs.
- **Streamer configuration in the web app** persists enabled/disabled state in the database and is surfaced in the UI.

The repo has gradually moved toward a more stateful web-managed workflow, so when editing one path, check whether the other one needs the same business rule.

## 5. Handle cookies and login state

Cookies are loaded from a file path configured in `config.yaml`.

Practical notes:

- `cookie.txt` is gitignored; do not commit live credentials.
- cookie management was expanded in recent commits to support multiple cookie files and round-robin-like assignment behavior.
- if login-specific data looks incomplete, inspect cookie handling before assuming the parsing logic is wrong.

## 6. Change leaderboard or analytics behavior

Most dashboard metrics are backed by query functions in `base/parser.py` and routed through `app.py`.

When you change one of these views, check the corresponding query and export route together:

- leaderboard → `query_leaderboard()` and `/api/leaderboard*`
- sessions → `query_sessions()` and session detail/end/delete routes
- users → `query_user()`, `query_user_detail()`, `query_user_timeline()`
- analytics → `query_user_retention()`, `query_big_spenders()`, `query_silent_whales()`

Recent leaderboard fixes show that period filtering and sort order are easy to regress, especially across session vs cross-session modes.
