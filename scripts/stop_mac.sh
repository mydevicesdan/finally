#!/usr/bin/env bash
# Stop FinAlly (macOS/Linux). Removes the container but keeps the finally-data volume.
set -euo pipefail

CONTAINER="finally"

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  docker rm -f "$CONTAINER" >/dev/null
  echo "FinAlly stopped. Data is kept in the 'finally-data' volume."
else
  echo "FinAlly is not running."
fi
