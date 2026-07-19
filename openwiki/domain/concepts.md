# Domain concepts

This page captures the vocabulary that recurs across the collector and dashboard.

## Session

A session is one contiguous capture period for a room. Session-level data drives most of the panel: session lists, session detail pages, per-session exports, and leaderboard filters that scope to one broadcast.

## Message families

The collector parses live Douyin traffic into normalized message families such as:

- chat
- gifts
- likes
- member/room entry events
- social events such as follow/share
- fans club activity
- emoji and room-control messages
- statistics and room-status messages
- leaderboard and rank messages

The exact set is broader than the user-facing pages, but these are the families that drive the main views and persistence paths.

## Gift dedup and pricing

Gift processing is one of the most stateful parts of the system.

Important concepts:

- **delta dedup** — repeated combo gifts are counted by increment rather than raw repeat count
- **registry-backed price resolution** — known gifts are resolved through bundled registry data and database-backed updates
- **price overrides** — some limited/skin gifts need explicit price overrides because upstream payloads report a base gift rather than the actual priced variant
- **gift-price management** — the web UI exposes gift price inspection, edits, bulk recalculation, and audit trails

## Leaderboards

Leaderboards are not just a single ranking table; they are filtered views over different periods and scopes.

The current code supports query shapes such as:

- session-scoped leaderboards
- month-based or cross-session periods
- custom date ranges
- different sort modes, including consume-based and session-count-based rankings

Recent git history shows that period filtering and sort order have been easy to get subtly wrong, so these query semantics are worth checking before making changes.

## Analytics

The dashboard includes higher-level analytics pages derived from session history:

- retention by period/tier
- big spenders
- silent whales

These views build on the same underlying session and contribution tables as the leaderboard pages, so they should be treated as derived reporting, not separate source-of-truth data.

## Streamer management

The app keeps track of streamers/rooms in two related places:

- `rooms.txt` for collector startup
- database-backed streamer configuration for dashboard management

That split exists because the collector needs a lightweight bootstrap list, while the web app needs state that survives restarts.

## Backlog

- **Table-by-table schema catalog** — there are enough query paths to justify it eventually, but the current wiki keeps the core concepts here instead of splitting into many thin pages.
