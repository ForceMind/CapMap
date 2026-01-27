#!/usr/bin/env bash
set -euo pipefail

# 服务器一键部署脚本（中文）
echo "========================================="
echo "   CapMap 一键部署脚本"
echo "========================================="

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$APP_ROOT/app"
VENV_DIR="$APP_ROOT/.venv"
LOG_DIR="$APP_ROOT/logs"

PORT="${PORT:-8501}"
BASE_PATH="${BASE_PATH:-capmap}"
HOST="${HOST:-0.0.0.0}"
PIP_INDEX_URL="${PIP_INDEX_URL:-}"
PIP_EXTRA_INDEX_URL="${PIP_EXTRA_INDEX_URL:-}"

NGINX_SETUP="${NGINX_SETUP:-yes}"
NGINX_PORT="${NGINX_PORT:-80}"
NGINX_SERVER_NAME="${NGINX_SERVER_NAME:-_}"
NGINX_CONF="${NGINX_CONF:-/etc/nginx/conf.d/capmap.conf}"
PANEL_ENABLE="${PANEL_ENABLE:-no}"
PANEL_HOST="${PANEL_HOST:-0.0.0.0}"
PANEL_PORT="${PANEL_PORT:-9000}"
PANEL_TOKEN="${PANEL_TOKEN:-}"
PANEL_PID_FILE="$APP_ROOT/panel.pid"
PANEL_LOG="$LOG_DIR/panel.out"
PANEL_APP="$APP_ROOT/control_panel/panel.py"

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

  read -r -p "是否配置 Nginx 反代（默认 Y）: " INPUT_NGX
  if [[ -n "${INPUT_NGX:-}" ]]; then
    NGINX_SETUP="$INPUT_NGX"
  fi

  read -r -p "Nginx 监听端口（默认 $NGINX_PORT）: " INPUT_NGX_PORT
  if [[ -n "${INPUT_NGX_PORT:-}" ]]; then
    NGINX_PORT="$INPUT_NGX_PORT"
  fi

  read -r -p "Nginx server_name（默认 _，可填域名或 IP）: " INPUT_NGX_NAME
  if [[ -n "${INPUT_NGX_NAME:-}" ]]; then
    NGINX_SERVER_NAME="$INPUT_NGX_NAME"
  fi

  read -r -p "是否启动控制台（默认 N）: " INPUT_PANEL
  if [[ -n "${INPUT_PANEL:-}" ]]; then
    PANEL_ENABLE="$INPUT_PANEL"
  fi

  if [[ "${PANEL_ENABLE,,}" == "y" || "${PANEL_ENABLE,,}" == "yes" ]]; then
    read -r -p "控制台端口（默认 $PANEL_PORT）: " INPUT_PANEL_PORT
    if [[ -n "${INPUT_PANEL_PORT:-}" ]]; then
      PANEL_PORT="$INPUT_PANEL_PORT"
    fi
    read -r -p "控制台访问口令(必填): " INPUT_PANEL_TOKEN
    if [[ -n "${INPUT_PANEL_TOKEN:-}" ]]; then
      PANEL_TOKEN="$INPUT_PANEL_TOKEN"
    fi
  fi
fi

# Streamlit 的 baseUrlPath 不能包含前导 /
BASE_PATH="${BASE_PATH#/}"

echo "开始部署：端口=$PORT，二级地址=/$BASE_PATH/"

OS_ID="unknown"
OS_NAME="unknown"
if [[ -f /etc/os-release ]]; then
  . /etc/os-release
  OS_ID="${ID:-unknown}"
  OS_NAME="${NAME:-unknown}"
fi
echo "系统：$OS_NAME"

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  fi
fi

need_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    if [[ -z "$SUDO" ]]; then
      echo "缺少 sudo，无法安装系统依赖。请使用 root 或 sudo 运行此脚本。"
      exit 1
    fi
  fi
}

install_python() {
  need_root
  if command -v dnf >/dev/null 2>&1; then
    $SUDO dnf install -y python3.11 python3.11-pip python3.11-devel || $SUDO dnf install -y python3 python3-pip
  elif command -v yum >/dev/null 2>&1; then
    $SUDO yum install -y python3.11 python3.11-pip python3.11-devel || $SUDO yum install -y python3 python3-pip
  elif command -v apt-get >/dev/null 2>&1; then
    $SUDO apt-get update
    $SUDO apt-get install -y python3.11 python3.11-venv python3.11-dev python3-pip || $SUDO apt-get install -y python3 python3-venv python3-pip
  elif command -v zypper >/dev/null 2>&1; then
    $SUDO zypper -n install python3 python3-pip
  elif command -v pacman >/dev/null 2>&1; then
    $SUDO pacman -Sy --noconfirm python python-pip
  elif command -v apk >/dev/null 2>&1; then
    $SUDO apk add --no-cache python3 py3-pip
  else
    echo "未找到可用的包管理器，请手动安装 Python 3.10+。"
    exit 1
  fi
}

install_nginx() {
  need_root
  if command -v dnf >/dev/null 2>&1; then
    $SUDO dnf install -y nginx
  elif command -v yum >/dev/null 2>&1; then
    $SUDO yum install -y nginx
  elif command -v apt-get >/dev/null 2>&1; then
    $SUDO apt-get update
    $SUDO apt-get install -y nginx
  elif command -v zypper >/dev/null 2>&1; then
    $SUDO zypper -n install nginx
  elif command -v pacman >/dev/null 2>&1; then
    $SUDO pacman -Sy --noconfirm nginx
  elif command -v apk >/dev/null 2>&1; then
    $SUDO apk add --no-cache nginx
  else
    echo "未找到可用的包管理器，请手动安装 Nginx。"
    exit 1
  fi
}

pick_python() {
  for c in python3.12 python3.11 python3.10 python3.9 python3 python; do
    if command -v "$c" >/dev/null 2>&1; then
      echo "$c"
      return 0
    fi
  done
  return 1
}

check_python_version() {
  "$1" - <<'PY' >/dev/null 2>&1
import sys
min_major, min_minor = 3, 10
sys.exit(0 if (sys.version_info.major, sys.version_info.minor) >= (min_major, min_minor) else 1)
PY
}

PYTHON_EXE="$(pick_python || true)"
if [[ -z "$PYTHON_EXE" ]]; then
  echo "未检测到 Python，尝试安装..."
  install_python
  PYTHON_EXE="$(pick_python || true)"
fi

if [[ -z "$PYTHON_EXE" ]]; then
  echo "未找到 Python，请先安装 Python 3.10+。"
  exit 1
fi

if ! check_python_version "$PYTHON_EXE"; then
  echo "当前 Python 版本过低，尝试安装更高版本..."
  install_python
  PYTHON_EXE="$(pick_python || true)"
  if [[ -z "$PYTHON_EXE" ]] || ! check_python_version "$PYTHON_EXE"; then
    echo "Akshare 需要 Python 3.10+，当前不满足。"
    exit 1
  fi
fi

echo "使用 Python：$PYTHON_EXE"

if ! "$PYTHON_EXE" -m venv "$VENV_DIR"; then
  echo "venv 创建失败，尝试安装 venv 组件..."
  need_root
  VENV_PKG="python3-venv"
  if [[ "$PYTHON_EXE" =~ ^python3\.[0-9]+$ ]]; then
    VENV_PKG="${PYTHON_EXE}-venv"
  fi
  if command -v apt-get >/dev/null 2>&1; then
    $SUDO apt-get update
    $SUDO apt-get install -y "$VENV_PKG" || $SUDO apt-get install -y python3-venv
  elif command -v dnf >/dev/null 2>&1; then
    $SUDO dnf install -y python3-venv || true
  elif command -v yum >/dev/null 2>&1; then
    $SUDO yum install -y python3-venv || true
  fi
  "$PYTHON_EXE" -m venv "$VENV_DIR" || { echo "venv 创建失败。"; exit 1; }
fi

OS_NAME_SHORT="$(uname -s 2>/dev/null || echo unknown)"
case "$OS_NAME_SHORT" in
  MINGW*|MSYS*|CYGWIN*) VENV_BIN="$VENV_DIR/Scripts" ;;
  *) VENV_BIN="$VENV_DIR/bin" ;;
esac

if [[ ! -x "$VENV_BIN/pip" ]]; then
  "$VENV_BIN/python" -m ensurepip --upgrade || true
fi

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

[browser]
gatherUsageStats = false
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
  --browser.gatherUsageStats false \
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

if [[ "${NGINX_SETUP,,}" == "y" || "${NGINX_SETUP,,}" == "yes" ]]; then
  if ! command -v nginx >/dev/null 2>&1; then
    echo "未发现 Nginx，尝试安装..."
    install_nginx
  fi

  need_root
  $SUDO mkdir -p "$(dirname "$NGINX_CONF")"
  $SUDO tee "$NGINX_CONF" >/dev/null <<EOF
server {
    listen $NGINX_PORT;
    server_name $NGINX_SERVER_NAME;
    location /$BASE_PATH/ {
        proxy_pass http://127.0.0.1:$PORT/;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOF
  $SUDO nginx -t
  if command -v systemctl >/dev/null 2>&1; then
    $SUDO systemctl enable nginx
    $SUDO systemctl restart nginx
  else
    $SUDO nginx -s reload
  fi
  echo "Nginx 已配置，请访问：http://服务器IP/$BASE_PATH/"
else
  echo "已跳过 Nginx 配置。"
fi

if [[ "${PANEL_ENABLE,,}" == "y" || "${PANEL_ENABLE,,}" == "yes" ]]; then
  if [[ -z "$PANEL_TOKEN" ]]; then
    echo "未设置控制台口令，已跳过控制台启动。"
  elif [[ ! -f "$PANEL_APP" ]]; then
    echo "未找到控制台程序：$PANEL_APP"
  else
    PANEL_PY="$VENV_BIN/python"
    if [[ ! -x "$PANEL_PY" ]]; then
      PANEL_PY="$PYTHON_EXE"
    fi
    if [[ -f "$PANEL_PID_FILE" ]]; then
      PANEL_PID="$(cat "$PANEL_PID_FILE" 2>/dev/null || true)"
      if [[ -n "${PANEL_PID:-}" ]] && kill -0 "$PANEL_PID" >/dev/null 2>&1; then
        echo "控制台已在运行，PID：$PANEL_PID"
      else
        rm -f "$PANEL_PID_FILE"
      fi
    fi
    if [[ ! -f "$PANEL_PID_FILE" ]]; then
      export PANEL_HOST PANEL_PORT PANEL_TOKEN
      nohup "$PANEL_PY" "$PANEL_APP" > "$PANEL_LOG" 2>&1 &
      echo $! > "$PANEL_PID_FILE"
      echo "控制台已启动：http://服务器IP:$PANEL_PORT/?token=$PANEL_TOKEN"
    fi
  fi
else
  echo "已跳过控制台启动。"
fi

echo "部署完成。运行：$APP_ROOT/run.sh"

