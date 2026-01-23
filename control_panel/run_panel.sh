#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$APP_ROOT/logs"
PID_FILE="$APP_ROOT/panel.pid"

PANEL_HOST="${PANEL_HOST:-0.0.0.0}"
PANEL_PORT="${PANEL_PORT:-9000}"
PANEL_TOKEN="${PANEL_TOKEN:-}"

if [[ -z "$PANEL_TOKEN" ]]; then
  if [[ -t 0 ]]; then
    read -r -p "Enter control token (required): " PANEL_TOKEN
  fi
  if [[ -z "$PANEL_TOKEN" ]]; then
    echo "PANEL_TOKEN is required."
    exit 1
  fi
fi

PY_CMD="python3"
if ! command -v python3 >/dev/null 2>&1; then
  PY_CMD="python"
fi

mkdir -p "$LOG_DIR"

export PANEL_HOST
export PANEL_PORT
export PANEL_TOKEN

nohup "$PY_CMD" "$APP_ROOT/control_panel/panel.py" \
  > "$LOG_DIR/panel.out" 2>&1 &
echo $! > "$PID_FILE"
echo "Control panel started: http://$PANEL_HOST:$PANEL_PORT/?token=$PANEL_TOKEN"
