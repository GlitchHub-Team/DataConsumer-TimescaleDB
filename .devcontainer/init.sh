#!/usr/bin/env bash
set -euo pipefail

if [ -f go.mod ]; then
  echo "Downloading Go modules..."
  go mod download
fi

echo "Tool versions:"
go version
nats --version
psql --version
jq --version
yq --version
golangci-lint --version | head -n 1
