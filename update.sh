#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "   CapMap 一键更新脚本"
echo "   (不会删除 data/ 与 .streamlit/ 配置)"
echo "========================================="

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
  echo "检测到本地有未提交修改："
  echo "$STATUS"
  if [[ -t 0 ]]; then
    read -r -p "仍要继续更新吗？(y/N) " ANSWER
    case "${ANSWER,,}" in
      y|yes) ;;
      *) echo "已取消更新。"; exit 1 ;;
    esac
  else
    echo "非交互模式下检测到未提交修改，已中止。"
    exit 1
  fi
fi

echo "正在拉取最新代码：$REMOTE/$BRANCH"
git fetch "$REMOTE" --prune
if ! git pull --ff-only "$REMOTE" "$BRANCH"; then
  echo "更新失败：需要手动处理冲突或分支不同步。"
  echo "请执行：git status"
  exit 1
fi

if [[ -t 0 ]]; then
  read -r -p "可选：请输入必盈 licence（留空跳过）: " INPUT_LICENCE
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

