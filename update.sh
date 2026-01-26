#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "   CapMap ??????"
echo "   (???? data/ ? .streamlit/ ??)"
echo "========================================="

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_ROOT"

if ! command -v git >/dev/null 2>&1; then
  echo "??? git????? git?"
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "?????? Git ????????"
  exit 1
fi

REMOTE="${REMOTE:-origin}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"

if ! git remote get-url "$REMOTE" >/dev/null 2>&1; then
  echo "????????$REMOTE"
  echo "???????"
  git remote -v
  exit 1
fi

STATUS="$(git status --porcelain)"
if [[ -n "$STATUS" ]]; then
  echo "????????????"
  echo "$STATUS"
  if [[ -t 0 ]]; then
    read -r -p "????????(y/N) " ANSWER
    case "${ANSWER,,}" in
      y|yes) ;;
      *) echo "??????"; exit 1 ;;
    esac
  else
    echo "???????????????????"
    exit 1
  fi
fi

echo "?????????$REMOTE/$BRANCH"
git fetch "$REMOTE" --prune
if ! git pull --ff-only "$REMOTE" "$BRANCH"; then
  echo "????????????????????"
  echo "????git status"
  exit 1
fi

if [[ -t 0 ]]; then
  read -r -p "???? Python ???(y/N) " ANSWER
  case "${ANSWER,,}" in
    y|yes)
      VENV_DIR="$APP_ROOT/.venv"
      if [[ -x "$VENV_DIR/bin/pip" ]]; then
        "$VENV_DIR/bin/pip" install -r "$APP_ROOT/app/requirements.txt"
      else
        echo "??????? .venv/bin/pip????????"
      fi
      ;;
  esac
fi

if [[ -t 0 && -x "$APP_ROOT/run.sh" ]]; then
  read -r -p "???????(y/N) " ANSWER
  case "${ANSWER,,}" in
    y|yes)
      if [[ -x "$APP_ROOT/stop.sh" ]]; then
        bash "$APP_ROOT/stop.sh" || true
      fi
      bash "$APP_ROOT/run.sh"
      ;;
  esac
fi

echo "? ?????"
