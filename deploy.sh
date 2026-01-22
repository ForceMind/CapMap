#!/usr/bin/env bash
set -euo pipefail

# 服务器一键部署脚本（中文）
APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$APP_ROOT/app"
VENV_DIR="$APP_ROOT/.venv"
LOG_DIR="$APP_ROOT/logs"

PORT="${PORT:-8501}"
BASE_PATH="${BASE_PATH:-capmap}"
HOST="${HOST:-0.0.0.0}"
PIP_INDEX_URL="${PIP_INDEX_URL:-}"
PIP_EXTRA_INDEX_URL="${PIP_EXTRA_INDEX_URL:-}"

if [[ -t 0 ]]; then
  read -r -p "请输入端口（默认 $PORT）: " INPUT_PORT
  if [[ -n "${INPUT_PORT:-}" ]]; then
    PORT="$INPUT_PORT"
  fi

  read -r -p "请输入二级地址（默认 $BASE_PATH，不要带 /）: " INPUT_PATH
  if [[ -n "${INPUT_PATH:-}" ]]; then
    BASE_PATH="$INPUT_PATH"
  fi

  read -r -p "可选：请输入 pip 源（留空使用默认，如 https://pypi.org/simple）: " INPUT_PIP
  if [[ -n "${INPUT_PIP:-}" ]]; then
    PIP_INDEX_URL="$INPUT_PIP"
  fi
fi

# Streamlit 的 baseUrlPath 不能包含前导 /
BASE_PATH="${BASE_PATH#/}"

echo "开始部署：端口=$PORT，二级地址=/$BASE_PATH/"

PY_CMD=()
if command -v python3 >/dev/null 2>&1; then
  PY_CMD=(python3)
elif command -v python >/dev/null 2>&1; then
  PY_CMD=(python)
elif command -v py >/dev/null 2>&1; then
  PY_CMD=(py -3)
else
  echo "未找到 Python，请先安装 Python 3。"
  exit 1
fi

"${PY_CMD[@]}" -m venv "$VENV_DIR"

OS_NAME="$(uname -s 2>/dev/null || echo unknown)"
case "$OS_NAME" in
  MINGW*|MSYS*|CYGWIN*) VENV_BIN="$VENV_DIR/Scripts" ;;
  *) VENV_BIN="$VENV_DIR/bin" ;;
esac

PIP_ARGS=()
if [[ -n "$PIP_INDEX_URL" ]]; then
  PIP_ARGS+=(--index-url "$PIP_INDEX_URL")
fi
if [[ -n "$PIP_EXTRA_INDEX_URL" ]]; then
  PIP_ARGS+=(--extra-index-url "$PIP_EXTRA_INDEX_URL")
fi

"$VENV_BIN/pip" install --upgrade pip

if ! "$VENV_BIN/pip" install "${PIP_ARGS[@]}" -r "$APP_DIR/requirements.txt"; then
  echo "依赖安装失败，尝试使用官方 PyPI 源重新安装..."
  "$VENV_BIN/pip" install --index-url https://pypi.org/simple -r "$APP_DIR/requirements.txt"
fi

mkdir -p "$APP_ROOT/.streamlit" "$LOG_DIR"

cat > "$APP_ROOT/.streamlit/config.toml" <<EOF
[server]
address = "$HOST"
port = $PORT
baseUrlPath = "$BASE_PATH"
headless = true
enableCORS = false
enableXsrfProtection = false
EOF

cat > "$APP_ROOT/run.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$APP_ROOT/app"
VENV_DIR="$APP_ROOT/.venv"
LOG_DIR="$APP_ROOT/logs"
PID_FILE="$APP_ROOT/streamlit.pid"

cd "$APP_ROOT"
mkdir -p "$LOG_DIR"

nohup "$VENV_DIR/bin/streamlit" run "$APP_DIR/app.py" \
  --server.headless true \
  > "$LOG_DIR/streamlit.out" 2>&1 &
echo $! > "$PID_FILE"
echo "已启动，PID：$(cat "$PID_FILE")"
EOF

cat > "$APP_ROOT/stop.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

PID_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/streamlit.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" >/dev/null 2>&1; then
    kill "$PID"
    echo "已停止，PID：$PID"
  else
    echo "进程未运行：$PID"
  fi
  rm -f "$PID_FILE"
else
  echo "未找到 PID 文件。"
fi
EOF

chmod +x "$APP_ROOT/run.sh" "$APP_ROOT/stop.sh"
echo "部署完成。运行：$APP_ROOT/run.sh"
