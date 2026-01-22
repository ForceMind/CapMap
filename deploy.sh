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

if [[ -t 0 ]]; then
  read -r -p "请输入端口（默认 $PORT）: " INPUT_PORT
  if [[ -n "${INPUT_PORT:-}" ]]; then
    PORT="$INPUT_PORT"
  fi

  read -r -p "请输入二级地址（默认 $BASE_PATH，不要带 /）: " INPUT_PATH
  if [[ -n "${INPUT_PATH:-}" ]]; then
    BASE_PATH="$INPUT_PATH"
  fi
fi

# Streamlit 的 baseUrlPath 不能包含前导 /
BASE_PATH="${BASE_PATH#/}"

echo "开始部署：端口=$PORT，二级地址=/$BASE_PATH/"

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"

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
