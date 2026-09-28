# Local Upstox Market Gateway

A memory-bounded service that maintains two Upstox Market Data Feed V3 connections and redistributes normalized LTPC and option-Greeks events to authenticated systems on a private network.

The production container does not run Chrome, hold Upstox application credentials, place orders, or write to PostgreSQL. Host-side tooling performs OAuth login and supplies a short-lived token to the container as a read-only Docker secret.

## Current subscription plan

The planner streams the official daily NSE and BSE instrument masters instead of loading them fully into memory. On **2026-09-28**, the current universe measured:

| Universe | Count |
|---|---:|
| Unique NSE/BSE F&O underlyings | 220 |
| Nearest future for each underlying | 220 |
| All listed futures | 659 |
| All listed options | 41,125 |
| Nifty nearest two option expiries | 478 |
| Sensex nearest two option expiries | 600 |

Streaming every listed derivative contract is not possible within Upstox's normal limits. The implemented default is:

### Connection 1: LTPC

```text
220 F&O underlying spot instruments
+ 220 nearest futures
+ 1,078 selected Nifty/Sensex options
= 1,518 LTPC subscriptions
```

### Connection 2: Option Greeks

```text
478 Nifty options across the nearest two expiries
+ 600 Sensex options across the nearest two expiries
= 1,078 option-Greeks subscriptions
```

Because both categories are active, Upstox's combined limits apply:

| Category | Provider limit | Gateway safety limit | Current |
|---|---:|---:|---:|
| LTPC | 2,000 | 1,900 | 1,518 |
| Option Greeks | 2,000 | 1,900 | 1,078 |
| Connections | 2 | 2 | 2 |

This layout is more memory- and bandwidth-efficient than `full`: options are subscribed on both connections to obtain prices and Greeks without receiving unnecessary market depth and OHLC payloads.

Three option expiries across both indices currently require 1,552 option contracts and would raise LTPC to 1,992 instruments. Although that is just below the provider limit, it leaves almost no growth margin, so the gateway deliberately rejects it under the 1,900-key operational safety threshold.

## Selection behavior

At startup and every six hours, the gateway:

1. Downloads the official compressed NSE and BSE instrument masters.
2. Parses them incrementally with `ijson`.
3. Discovers every unique underlying represented in `NSE_FO` and `BSE_FO`.
4. Selects the nearest non-expired future for each underlying.
5. Selects all listed CE and PE contracts for the first two active Nifty and Sensex expiries.
6. Deduplicates keys and validates both provider limits before connecting.
7. Rebuilds the metadata map and safely replaces both upstream connections.

The planner fails closed rather than silently truncating an option chain.

Configuration controls:

```yaml
FUTURES_PER_UNDERLYING: "1"       # Supported: 1-3
OPTION_EXPIRIES_PER_INDEX: "2"    # Limit-checked dynamically
GATEWAY_TARGET_INDICES: NIFTY,SENSEX
CATALOG_REFRESH_SECONDS: "21600"
```

Increasing futures or expiries may be rejected as the live instrument universe changes.

## Architecture

```text
Host login tool
    └── protected daily token
              │ Docker secret
              ▼
┌─────────────────────────────────────────────────────┐
│ 300 MB Upstox gateway container                     │
│                                                     │
│ Daily instrument masters → subscription planner     │
│                                  │                  │
│                   ┌──────────────┴──────────────┐   │
│                   ▼                             ▼   │
│          LTPC connection               Greeks connection
│          1,518 keys                     1,078 keys
│                   └──────────────┬──────────────┘   │
│                                  ▼                  │
│                        compact normalizer           │
│                                  ▼                  │
│                  latest-only snapshot store         │
│                                  ▼                  │
│            REST snapshots + WebSocket fan-out       │
└──────────────────────────────────┬──────────────────┘
                                   ▼
                      private-network consumers
```

Memory controls include:

- Streaming compressed instrument parsing
- No pandas, pyarrow, Selenium, or Chrome in the runtime image
- One Uvicorn worker
- Four-frame upstream WebSocket queues
- 4 MiB maximum upstream frame
- WebSocket compression disabled
- Compact LTPC/Greeks normalization; market depth is discarded
- One latest snapshot per instrument
- 64-event bounded queue per local client
- Oldest local event discarded when a client cannot keep up
- 300 MB container limit and 128 MB reservation

A bounded live validation with both upstream connections reached **55.4 MiB peak RSS** on macOS while holding 1,518 merged snapshots. This provides substantial headroom, but the deployed Linux container should still be monitored because allocator and workload behavior differ by platform and client count.

## Repository layout

```text
live_feed/
├── .env                         # Host-only OAuth credentials, mode 0600
├── .runtime/
│   ├── upstox_token.json        # Host-generated token, mode 0600
│   ├── gateway_api_key          # Local consumer credential, mode 0600
│   └── instruments/             # Local-development instrument cache
├── .venv/                       # Host login/development environment
├── Dockerfile
├── compose.yaml
├── requirements.txt             # Host login + runtime dependencies
├── requirements-runtime.txt     # Browser-free container dependencies
├── sample_client.py
└── upstox_live_feed/
    ├── api.py
    ├── auth.py                  # Host-only browser login
    ├── client.py                # One-instrument diagnostic client
    ├── feed_connection.py       # Bounded multi-key runtime connection
    ├── gateway.py
    ├── gateway_config.py
    ├── gateway_service.py
    ├── gateway_state.py
    ├── instrument_catalog.py
    └── token_reader.py
```

`.env`, `.runtime/`, `.venv/`, and feed captures are excluded from Git and the Docker build context.

## Prepare the daily token on a machine with Chrome

```bash
cd /Users/ranauadi/Desktop/personal/live_feed
source .venv/bin/activate
python -m upstox_live_feed --force-login --login-only
```

This performs OAuth login and updates `.runtime/upstox_token.json` without printing the token or opening a market feed.

For a remote server, securely copy only these two runtime secret files into the server's `live_feed/.runtime/` folder:

```text
upstox_token.json
gateway_api_key
```

The container does not need `.env`, mobile number, PIN, TOTP secret, API key, API secret, Selenium, or Chrome.

After refreshing or replacing the token, recreate the service so Docker remounts the updated secret inode:

```bash
docker compose up -d --force-recreate
```

## Build and run under the 300 MB limit

```bash
cd /path/to/live_feed
docker compose up -d --build
```

On `192.168.1.40`, Docker Hub connectivity timed out while resolving the portable Python image. That server therefore uses the checked-in override with its existing local Python base and UID `1000`, which can read the owner-only mounted secrets:

```bash
cd /home/aditya/live_feed
docker compose -f compose.yaml -f compose.server40.yaml up -d --build
```

The server-specific base affects image composition, not the gateway command or the enforced 300 MB runtime limit. Use the portable Dockerfile on hosts that can reach Docker Hub normally.

Inspect status:

```bash
docker compose ps
docker compose logs --tail=100 market-gateway
curl http://127.0.0.1:8080/health
```

The health endpoint intentionally requires no credential and exposes only operational counts and connection state. It returns `degraded` while the catalog, token, or an upstream connection is unavailable.

The Compose service applies:

- `mem_limit: 300m`
- `mem_reservation: 128m`
- One CPU
- Non-root UID/GID 10001
- Read-only root filesystem
- All Linux capabilities dropped
- `no-new-privileges`
- 16 MiB bounded `/tmp`
- Persistent named volume for instrument masters
- Read-only Docker secrets

## Local REST API

Read the local API key into your shell without printing it:

```bash
export GATEWAY_API_KEY="$(cat .runtime/gateway_api_key)"
```

Subscription plan:

```bash
curl -H "X-API-Key: $GATEWAY_API_KEY" \
  http://127.0.0.1:8080/v1/plan
```

Paginated instrument metadata:

```bash
curl -H "X-API-Key: $GATEWAY_API_KEY" \
  'http://127.0.0.1:8080/v1/instruments?offset=0&limit=100'
```

Latest snapshot:

```bash
curl -G -H "X-API-Key: $GATEWAY_API_KEY" \
  --data-urlencode 'instrument_key=NSE_INDEX|Nifty 50' \
  http://127.0.0.1:8080/v1/snapshot
```

Option snapshots merge the latest LTPC and Greeks updates received from the two upstream connections.

## Local WebSocket API

Credentials are sent in the first WebSocket message, not in the URL:

```json
{
  "api_key": "LOCAL_GATEWAY_KEY",
  "instrument_keys": [
    "NSE_INDEX|Nifty 50"
  ]
}
```

A client can explicitly request all selected instruments:

```json
{
  "api_key": "LOCAL_GATEWAY_KEY",
  "all": true
}
```

Use the included sample client:

```bash
export GATEWAY_URL='ws://127.0.0.1:8080/v1/stream'
export GATEWAY_API_KEY="$(cat .runtime/gateway_api_key)"
export INSTRUMENT_KEYS='NSE_INDEX|Nifty 50'
python sample_client.py
```

For another LAN machine, replace `127.0.0.1` with the gateway server's private address. Do not expose port 8080 directly to the public internet. Use WireGuard or Tailscale for remote access and terminate HTTPS/WSS with a reverse proxy if traffic crosses an untrusted network.

## Failure and refresh behavior

- Upstream reconnects use exponential delay, jitter, and a small token bucket to prevent connection storms.
- Only idempotent feed authorization and subscription establishment are repeated.
- A missing/expired token produces a degraded health state; the container never attempts browser login.
- Instrument changes are applied by periodically rebuilding the complete plan and replacing both connections.
- Slow local consumers cannot block Upstox; their oldest queued events are discarded and counted in `/health`.
- The latest REST snapshot remains available while an upstream connection is temporarily unavailable.

## Host diagnostic client

The earlier one-instrument test remains available outside Docker:

```bash
source .venv/bin/activate
python -m upstox_live_feed --max-messages 2
```

## Known limitations

- There is no historical replay or PostgreSQL writer yet.
- Local WebSocket delivery is best effort; disconnected clients do not receive missed events.
- A single local API key is shared by consumers. Per-client credentials can be added later.
- Docker secrets are remounted by recreating the container after token refresh.
- The official SDK transitively installs the unusually named third-party `uuid` package, although Python has a standard-library module of that name. Runtime code does not import it directly; the SDK is retained for the maintained V3 protobuf schema.

## GitHub push deployment

The local repository uses `git@github.com:adityachm/market-data-relay.git`. A push to `main` triggers `.github/workflows/publish-image.yml`, which builds the portable `Dockerfile` on a GitHub-hosted runner and publishes two GHCR tags:

```text
ghcr.io/adityachm/market-data-relay:<full-commit-sha>
ghcr.io/adityachm/market-data-relay:main
```

Server40 runs `/home/aditya/live_feed/deploy/deploy.sh` from cron every two minutes. The script:

1. Locks to prevent overlapping deployments.
2. Pulls the current `main` image without disrupting pull failures.
3. Skips deployment when the image digest is unchanged.
4. Starts the candidate with `compose.production.yaml`.
5. Requires `/health` to report `status=ok` within 120 seconds.
6. Marks unhealthy image IDs so cron does not repeatedly redeploy them.
7. Retags and restores the previous container image when health validation fails.

Deployment activity is available through the system journal:

```bash
ssh aditya@192.168.1.40
journalctl -t market-data-relay-deploy --since '30 minutes ago'
```

The first publication is activated by committing and pushing locally:

```bash
git push -u origin main
```

Git never contains `.env`, the Upstox token, the gateway API key, virtual environments, instrument caches, or captured market data. GitHub Actions receives no Upstox credentials; it only builds source into an image. Runtime secrets remain under `/home/aditya/live_feed/.runtime/` on server40.

## Protocol references

- [Upstox Market Data Feed V3](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/)
- [Upstox instruments](https://upstox.com/developer/api-documentation/instruments/)
- [Upstox authentication](https://upstox.com/developer/api-documentation/authentication/)
- [Official Upstox Python SDK](https://github.com/upstox/upstox-python)

Upstox documentation was summarized and rephrased for licensing compliance.
