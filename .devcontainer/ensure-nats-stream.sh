#!/usr/bin/env bash
set -euo pipefail

NATS_URL="${NATS_URL:-nats://nats:4222}"
STREAM_NAME="SENSOR_DATA_STREAM"
SUBJECT="sensor.*.*.*"

for _ in $(seq 1 30); do
  if nats --server "$NATS_URL" server ping 1 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

nats --server "$NATS_URL" stream add "$STREAM_NAME" \
  --subjects "$SUBJECT" \
  --retention limits \
  --storage file \
  --defaults >/dev/null
echo "Created stream: $STREAM_NAME"