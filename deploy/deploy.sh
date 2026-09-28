#!/usr/bin/env bash
set -euo pipefail

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ROOT_DIR="${MARKET_RELAY_ROOT:-/home/aditya/live_feed}"
IMAGE="${GATEWAY_IMAGE:-ghcr.io/adityachm/market-data-relay:main}"
CONTAINER="live_feed-market-gateway-1"
ROLLBACK_IMAGE="local/market-data-relay:rollback"
STATE_DIR="$ROOT_DIR/.runtime/deploy"
FAILED_IMAGE_FILE="$STATE_DIR/failed-image"
LOCK_FILE="$STATE_DIR/deploy.lock"
HEALTH_URL="http://127.0.0.1:8080/health"

mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"
exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

log() {
    logger -t market-data-relay-deploy -- "$*"
    printf '%s\n' "$*"
}

healthy() {
    curl --fail --silent --show-error --max-time 3 "$HEALTH_URL" \
        | grep --quiet '"status":"ok"'
}

wait_until_healthy() {
    local attempt
    for attempt in $(seq 1 60); do
        if healthy; then
            return 0
        fi
        sleep 2
    done
    return 1
}

cd "$ROOT_DIR"

if ! docker pull "$IMAGE"; then
    log "Image pull unavailable; preserving the current gateway"
    exit 0
fi

candidate_id=$(docker image inspect "$IMAGE" --format '{{.Id}}')
current_id=$(docker inspect "$CONTAINER" --format '{{.Image}}' 2>/dev/null || true)
failed_id=$(cat "$FAILED_IMAGE_FILE" 2>/dev/null || true)

if [[ "$candidate_id" == "$current_id" ]]; then
    exit 0
fi
if [[ -n "$failed_id" && "$candidate_id" == "$failed_id" ]]; then
    log "Skipping image previously marked unhealthy: $candidate_id"
    exit 0
fi

previous_id="$current_id"
log "Deploying image $candidate_id"
GATEWAY_IMAGE="$IMAGE" docker compose -f compose.production.yaml up -d --remove-orphans

if wait_until_healthy; then
    rm -f "$FAILED_IMAGE_FILE"
    log "Deployment healthy: $candidate_id"
    exit 0
fi

printf '%s\n' "$candidate_id" > "$FAILED_IMAGE_FILE"
chmod 600 "$FAILED_IMAGE_FILE"
log "Deployment failed health checks: $candidate_id"

if [[ -z "$previous_id" ]]; then
    log "No previous image is available for rollback"
    exit 1
fi

docker tag "$previous_id" "$ROLLBACK_IMAGE"
GATEWAY_IMAGE="$ROLLBACK_IMAGE" docker compose -f compose.production.yaml up -d --remove-orphans
if wait_until_healthy; then
    log "Rollback healthy: $previous_id"
else
    log "Rollback did not become healthy: $previous_id"
fi
exit 1
