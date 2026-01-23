#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$APP_ROOT/panel.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" >/dev/null 2>&1; then
    kill "$PID"
    echo "Control panel stopped, PID: $PID"
  else
    echo "Control panel not running: $PID"
  fi
  rm -f "$PID_FILE"
else
  echo "panel.pid not found."
fi
