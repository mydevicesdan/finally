#!/usr/bin/env bash
# Start FinAlly in Docker (macOS/Linux). Idempotent: safe to run repeatedly.
#
# Usage: scripts/start_mac.sh [--build] [--no-open]
#   --build    force a rebuild of the image
#   --no-open  don't open the browser
set -euo pipefail

IMAGE="finally"
CONTAINER="finally"
VOLUME="finally-data"
PORT="${FINALLY_PORT:-8000}"
URL="http://localhost:${PORT}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

BUILD=false
OPEN=true
for arg in "$@"; do
  case "$arg" in
    --build) BUILD=true ;;
    --no-open) OPEN=false ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; exit 1 ;;
  esac
done

if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Start Docker and try again." >&2
  exit 1
fi

if [ "$BUILD" = true ] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Building image '$IMAGE'..."
  docker build -t "$IMAGE" "$ROOT_DIR"
fi

# Replace any existing container (running or stopped) so the latest image is used.
if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "Removing existing container '$CONTAINER'..."
  docker rm -f "$CONTAINER" >/dev/null
fi

ENV_ARGS=()
if [ -f "$ENV_FILE" ]; then
  ENV_ARGS=(--env-file "$ENV_FILE")
else
  echo "Warning: $ENV_FILE not found. Continuing without it (AI chat needs OPENROUTER_API_KEY; see .env.example)." >&2
fi

echo "Starting container '$CONTAINER'..."
docker run -d \
  --name "$CONTAINER" \
  -v "${VOLUME}:/app/db" \
  -p "${PORT}:8000" \
  ${ENV_ARGS[@]+"${ENV_ARGS[@]}"} \
  "$IMAGE" >/dev/null

echo -n "Waiting for FinAlly to become healthy"
for _ in $(seq 1 30); do
  if curl -fsS "${URL}/api/health" >/dev/null 2>&1; then
    echo " ok"
    break
  fi
  echo -n "."
  sleep 1
done
echo

echo "FinAlly is running at ${URL}"
echo "Stop it with: scripts/stop_mac.sh"

if [ "$OPEN" = true ]; then
  if command -v open >/dev/null 2>&1; then
    open "$URL" >/dev/null 2>&1 || true
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$URL" >/dev/null 2>&1 || true
  fi
fi
