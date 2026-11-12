#!/usr/bin/env bash
# Lists which of the lab's environment variables are set (names only, never values).
set -euo pipefail
for name in SHOP_PG_URL MYSQL_URL KAFKA_BOOTSTRAP; do
  if [ -n "${!name:-}" ]; then echo "$name: set"; else echo "$name: not set"; fi
done
