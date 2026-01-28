#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "   CapMap 一键更新脚本"
echo "   (不会删除 data/ 与 .streamlit/ 配置)"
echo "========================================="
echo "提示：直接回车将保留当前配置"

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_ROOT"
BIYING_LICENCE="${BIYING_LICENCE:-}"

if ! command -v git >/dev/null 2>&1; then
  echo "未找到 git，请先安装 git。"
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "当前目录不是 Git 仓库，无法更新。"
  exit 1
fi

REMOTE="${REMOTE:-origin}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"

if ! git remote get-url "$REMOTE" >/dev/null 2>&1; then
  echo "未找到远程仓库：$REMOTE"
  echo "当前已有远程："
  git remote -v
  exit 1
fi

pick_python() {
  for c in python3 python; do
    if command -v "$c" >/dev/null 2>&1; then
      echo "$c"
      return 0
    fi
  done
  return 1
}

load_existing_licence() {
  if [[ -n "${BIYING_LICENCE:-}" ]]; then
    return 0
  fi
  local py
  py="$(pick_python || true)"
  if [[ -z "$py" ]]; then
    return 0
  fi
  local cfg_path="$APP_ROOT/data/provider_config.json"
  if [[ ! -f "$cfg_path" ]]; then
    return 0
  fi
  BIYING_LICENCE="$("$py" - <<'PY'
import json
import os
path = os.environ.get('CFG_PATH')
if not path or not os.path.exists(path):
    print('', end='')
    raise SystemExit(0)
try:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f) if f else {}
except Exception:
    data = {}
value = data.get('biying_licence') or data.get('licence') or data.get('license') or ''
print(value, end='')
PY
CFG_PATH="$cfg_path")"
}

write_provider_config() {
  if [[ -z "${BIYING_LICENCE:-}" ]]; then
    return 0
  fi
  local py
  py="$(pick_python || true)"
  if [[ -z "$py" ]]; then
    echo "未找到 Python，跳过必盈 licence 写入。"
    return 0
  fi
  local cfg_path="$APP_ROOT/data/provider_config.json"
  BIYING_LICENCE_INPUT="$BIYING_LICENCE" PROVIDER_CONFIG_PATH="$cfg_path" "$py" - <<'PY'
import json
import os

licence = os.environ.get("BIYING_LICENCE_INPUT", "").strip()
path = os.environ.get("PROVIDER_CONFIG_PATH", "").strip()
if not licence or not path:
    raise SystemExit(0)

os.makedirs(os.path.dirname(path), exist_ok=True)
data = {}
if os.path.exists(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) if f else {}
    except Exception:
        data = {}

order = data.get("provider_order")
if not isinstance(order, list) or not order:
    data["provider_order"] = ["biying", "akshare"]

data["biying_licence"] = licence
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False)
PY
  echo "已写入必盈 licence 到 $cfg_path"
}

STATUS="$(git status --porcelain)"
if [[ -n "$STATUS" ]]; then
  echo "⚠️  检测到本地有未提交的代码修改："
  echo "$STATUS"
  echo "-----------------------------------"
  
  if [[ -t 0 ]]; then
    echo "选项："
    echo " 1) [推荐] 强制覆盖本地修改 (Reset Hard)"
    echo " 2) 尝试保留修改并更新 (Stash -> Pull -> Pop)"
    echo " 3) 取消更新"
    read -r -p "请选择 (默认1): " CHOICE
    CHOICE="${CHOICE:-1}"
    
    case "$CHOICE" in
      1)
        echo "正在强制重置本地代码..."
        git fetch "$REMOTE"
        git reset --hard "$REMOTE/$BRANCH"
        echo "✅ 本地代码已重置为远程最新版本。"
        # We need to continue execution to install deps if needed, but pull is done via reset
        PULL_DONE=1
        ;;
      2)
        echo "正在暂存本地修改..."
        git stash
        echo "正在拉取更新..."
        git pull "$REMOTE" "$BRANCH"
        echo "正在恢复本地修改..."
        git stash pop || echo "⚠️  恢复修改时发生冲突，请手动解决。"
        PULL_DONE=1
        ;;
      *)
        echo "已取消。"
        exit 1
        ;;
    esac
  else
    echo "非交互模式下检测到未提交修改，已中止。"
    exit 1
  fi
fi

if [[ -z "${PULL_DONE:-}" ]]; then
  echo "正在拉取最新代码：$REMOTE/$BRANCH"
  git fetch "$REMOTE" --prune
  if ! git pull --ff-only "$REMOTE" "$BRANCH"; then
    echo "❌ 自动更新失败 (无法快进)。"
    if [[ -t 0 ]]; then
       read -r -p "是否尝试强制重置(Reset Hard)以解决此问题？(y/N) " FORCE_RST
       if [[ "${FORCE_RST,,}" == "y" ]]; then
          git reset --hard "$REMOTE/$BRANCH"
          echo "✅ 已强制重置为远程最新版本。"
       else
          exit 1
       fi
    else
       exit 1
    fi
  fi
fi

if [[ -t 0 ]]; then
  load_existing_licence
  read -r -p "可选：请输入必盈 licence（回车保留当前，留空跳过）: " INPUT_LICENCE
  if [[ -n "${INPUT_LICENCE:-}" ]]; then
    BIYING_LICENCE="$INPUT_LICENCE"
    write_provider_config
  fi
fi

if [[ -t 0 ]]; then
  read -r -p "是否更新 Python 依赖？(y/N) " ANSWER
  case "${ANSWER,,}" in
    y|yes)
      VENV_DIR="$APP_ROOT/.venv"
      if [[ -x "$VENV_DIR/bin/pip" ]]; then
        "$VENV_DIR/bin/pip" install -r "$APP_ROOT/app/requirements.txt"
      else
        echo "未找到虚拟环境 .venv/bin/pip，跳过依赖更新。"
      fi
      ;;
  esac
fi

if [[ -t 0 && -x "$APP_ROOT/run.sh" ]]; then
  read -r -p "是否重启服务？(y/N) " ANSWER
  case "${ANSWER,,}" in
    y|yes)
      if [[ -x "$APP_ROOT/stop.sh" ]]; then
        bash "$APP_ROOT/stop.sh" || true
      fi
      bash "$APP_ROOT/run.sh"
      ;;
  esac
fi

echo "✅ 更新完成。"

