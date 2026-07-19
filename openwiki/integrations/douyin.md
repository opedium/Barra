# Douyin integration

This repository depends on Douyin live-room endpoints for both collection and enrichment.

## What the collector uses

The collector in `service/fetcher.py` and `service/network.py` relies on a few Douyin-specific steps:

- room metadata lookup before a WebSocket connection is established
- request signing through `service/signer.py` and `sign.js`
- cookie-backed sessions for richer data such as gift details
- live WebSocket frames that are unpacked into protobuf messages
- extra HTTP lookups for room/user enrichment when the dashboard needs it

## Why this integration is fragile

The repository history shows repeated fixes around collector stability and protocol changes. That usually means one of three things changed upstream:

- Douyin request signing / anti-bot behavior
- live message payload structure
- dedup or session assumptions on the consumer side

Treat the integration as a compatibility layer rather than a stable public API.

## Cookie behavior

Cookies are optional for basic capture, but they matter for authenticated coverage.

- `cookie.txt` is the primary local cookie input
- a secondary cookie path may be used for dual-WS mode
- cookie parsing accepts browser-exported strings and jar-style formats
- keep cookie files private; they may contain sensitive login state

## Signing and request construction

`service/signer.py` and `sign.js` exist because certain Douyin requests require a generated signature. The collector builds request headers and signed URLs before connecting.

If signing breaks, symptoms usually look like:

- connection failures before the WebSocket is established
- blocked/denied room access
- missing room or user metadata

## Enrichment requests

`service/network.py` wraps a small set of HTTP calls used to enrich the dashboard and parser output. These are rate-limited in the web app to avoid excessive retries.

## Change checklist

When changing this integration, verify:

- room lookup still succeeds for live rooms
- signed requests still connect and return data
- cookie-based access still works with existing browser exports
- the dashboard still resolves user and room metadata without spamming Douyin
