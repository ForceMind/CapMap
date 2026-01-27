#!/usr/bin/env python3
import os
import time
import json
import html
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, quote

APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RUN_SCRIPT = os.path.join(APP_ROOT, "run.sh")
STOP_SCRIPT = os.path.join(APP_ROOT, "stop.sh")
PID_FILE = os.path.join(APP_ROOT, "streamlit.pid")
LOG_FILE = os.path.join(APP_ROOT, "logs", "streamlit.out")
APP_LOG_FILE = os.path.join(APP_ROOT, "logs", "app.log")

HOST = os.environ.get("PANEL_HOST", "0.0.0.0")
PORT = int(os.environ.get("PANEL_PORT", "9000"))
TOKEN = os.environ.get("PANEL_TOKEN", "").strip()


def _read_pid():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return None


def _is_running():
    pid = _read_pid()
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _tail_log(path, max_lines=120, max_bytes=20000):
    if not os.path.exists(path):
        return "日志文件不存在。"
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read().decode("utf-8", errors="replace")
        lines = data.splitlines()[-max_lines:]
        return "\n".join(lines) if lines else "日志为空。"
    except Exception as e:
        return f"读取日志失败: {e}"


def _run_cmd(cmd):
    return subprocess.Popen(cmd, cwd=APP_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _auth_ok(query, headers):
    if not TOKEN:
        return True
    token = ""
    if query and "token" in query:
        token = query["token"][0]
    if not token:
        token = headers.get("X-Panel-Token", "")
    return token == TOKEN


def _status_payload():
    running = _is_running()
    pid = _read_pid() if running else None
    return {
        "running": running,
        "pid": pid,
        "timestamp": int(time.time()),
    }


def _html_page(query):
    status = _status_payload()
    token_q = ""
    if TOKEN and query and "token" in query:
        token_q = f"?token={query['token'][0]}"
    running_text = "运行中" if status["running"] else "未运行"
    pid_text = f"PID: {status['pid']}" if status["running"] else ""
    msg = ""
    if query and "msg" in query:
        msg = query["msg"][0]
    msg_html = f"<div class=\"msg\">{html.escape(msg)}</div>" if msg else ""
    app_log_text = _tail_log(APP_LOG_FILE, max_lines=200, max_bytes=60000)
    raw_log_text = _tail_log(LOG_FILE, max_lines=120, max_bytes=30000)

    return f"""<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <title>CapMap 启动控制台</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 24px; max-width: 900px; margin: 0 auto; }}
    .status {{ padding: 12px; background: #f3f5f7; border-radius: 8px; }}
    .msg { padding: 10px 12px; margin: 10px 0; background: #fff8e1; border: 1px solid #ffe0b2; border-radius: 6px; color: #6d4c41; }
    .btn {{ padding: 10px 16px; margin-right: 8px; border: none; border-radius: 6px; cursor: pointer; }}
    .start {{ background: #2e7d32; color: #fff; }}
    .stop {{ background: #c62828; color: #fff; }}
    .restart {{ background: #1565c0; color: #fff; }}
    pre.log { background: #111; color: #ddd; padding: 12px; border-radius: 8px; max-height: 360px; overflow-y: auto; white-space: pre-wrap; }
    small {{ color: #666; }}
  </style>
</head>
<body>
  <h1>CapMap 启动控制台</h1>
  <div class="status">
    <div><strong>Streamlit 状态：</strong> {running_text} {pid_text}</div>
    <div><small>提示：请先运行 deploy.sh 生成 run.sh / stop.sh。</small></div>
  </div>
  {msg_html}
  <p>
    <form method="post" action="/start{token_q}" style="display:inline">
      <button class="btn start" type="submit">启动</button>
    </form>
    <form method="post" action="/stop{token_q}" style="display:inline">
      <button class="btn stop" type="submit">停止</button>
    </form>
    <form method="post" action="/restart{token_q}" style="display:inline">
      <button class="btn restart" type="submit">重启</button>
    </form>
  </p>
  <h3>应用日志 (app.log)</h3>
  <pre class="log">{app_log_text}</pre>
  <details>
    <summary>查看原始日志 (streamlit.out)</summary>
    <pre class="log">{raw_log_text}</pre>
  </details>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, content_type="text/html; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.wfile.write(body)

    def _redirect(self, query, msg=""):
        parts = []
        if TOKEN and query and "token" in query:
            parts.append("token=" + quote(query["token"][0]))
        if msg:
            parts.append("msg=" + quote(msg))
        location = "/"
        if parts:
            location += "?" + "&".join(parts)
        self.send_response(303)
        self.send_header("Location", location)
        self.end_headers()

    def _auth_or_403(self, query):
        if not _auth_ok(query, self.headers):
            self._send(403, "无权限")
            return False
        return True

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if not self._auth_or_403(query):
            return
        if parsed.path in ("/", "/index.html"):
            self._send(200, _html_page(query))
        elif parsed.path == "/status":
            self._send(200, json.dumps(_status_payload()), "application/json")
        else:
            self._send(404, "未找到")

    def do_POST(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if not self._auth_or_403(query):
            return

        if parsed.path == "/start":
            if not os.path.exists(RUN_SCRIPT):
                self._send(400, "\u672a\u627e\u5230 run.sh\uff0c\u8bf7\u5148\u8fd0\u884c deploy.sh\u3002")
                return
            if _is_running():
                self._redirect(query, "Streamlit \u5df2\u5728\u8fd0\u884c")
                return
            _run_cmd(["bash", RUN_SCRIPT])
            self._redirect(query, "\u5df2\u542f\u52a8")
            return

        if parsed.path == "/stop":
            if not os.path.exists(STOP_SCRIPT):
                self._send(400, "\u672a\u627e\u5230 stop.sh\uff0c\u8bf7\u5148\u8fd0\u884c deploy.sh\u3002")
                return
            if not _is_running():
                self._redirect(query, "Streamlit \u672a\u8fd0\u884c")
                return
            _run_cmd(["bash", STOP_SCRIPT])
            self._redirect(query, "\u5df2\u505c\u6b62")
            return

        if parsed.path == "/restart":
            if not os.path.exists(RUN_SCRIPT) or not os.path.exists(STOP_SCRIPT):
                self._send(400, "\u672a\u627e\u5230 run.sh/stop.sh\uff0c\u8bf7\u5148\u8fd0\u884c deploy.sh\u3002")
                return
            if _is_running():
                _run_cmd(["bash", STOP_SCRIPT])
                time.sleep(1)
            _run_cmd(["bash", RUN_SCRIPT])
            self._redirect(query, "\u5df2\u91cd\u542f")
            return

        self._send(404, "未找到")


def main():
    httpd = HTTPServer((HOST, PORT), Handler)
    print(f"控制台已启动: http://{HOST}:{PORT}/")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
