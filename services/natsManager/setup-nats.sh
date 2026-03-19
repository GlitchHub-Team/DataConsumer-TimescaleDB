#!/bin/sh
set -eu

NATS_URL="${NATS_URL:-nats://nats:4222}"
STREAM_NAME="SENSOR_DATA_STREAM"
SUBJECT="sensor.*.*.*"
CREDS_PATH="/stream_setupper.creds"
CA_PATH="/ca.pem"

echo "Waiting for NATS to be ready..."
for _ in $(seq 1 30); do
  if nats --server "$NATS_URL" --creds "$CREDS_PATH" --tlsca "$CA_PATH" stream ls >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

nats --server "$NATS_URL" --creds "$CREDS_PATH" --tlsca "$CA_PATH" \
  stream add "$STREAM_NAME" \
  --subjects "$SUBJECT" \
  --retention limits \
  --storage file \
  --defaults || echo "Stream already exists or error creating it"

echo "Setup completed!"