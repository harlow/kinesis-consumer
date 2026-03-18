#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE_FILE="$ROOT/examples/consumer-group-ddb/docker-compose.yml"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "$COMPOSE_FILE" "$@"
    return
  fi
  echo "docker compose or docker-compose is required" >&2
  exit 1
}

cleanup() {
  compose down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

compose up -d kinesis dynamodb >/dev/null

# Give the containers a moment so the tests don't burn retries on startup churn.
sleep 2

cd "$ROOT"
RUN_EXAMPLE_INTEGRATION=1 go test ./integration -count=1 -timeout 12m -v
