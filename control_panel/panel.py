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
    token_str = ""
    token_q = ""
    if TOKEN and query and "token" in query:
        token_str = query['token'][0]
        token_q = f"?token={token_str}"
    
    running_text = "运行中" if status["running"] else "未运行"
    pid_text = f"PID: {status['pid']}" if status["running"] else ""
    msg = ""
    if query and "msg" in query:
        msg = query["msg"][0]
    
    # Initial log content
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
    .msg {{ padding: 10px 12px; margin: 10px 0; background: #fff8e1; border: 1px solid #ffe0b2; border-radius: 6px; color: #6d4c41; display: none; }}
    .btn {{ padding: 10px 16px; margin-right: 8px; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; transition: opacity 0.2s; }}
    .btn:disabled {{ opacity: 0.6; cursor: not-allowed; }}
    .start {{ background: #2e7d32; color: #fff; }}
    .stop {{ background: #c62828; color: #fff; }}
    .restart {{ background: #1565c0; color: #fff; }}
    pre.log {{ background: #111; color: #ddd; padding: 12px; border-radius: 8px; max-height: 360px; overflow-y: auto; white-space: pre-wrap; font-family: Consolas, monospace; font-size: 13px; }}
    small {{ color: #666; }}
    .header {{ display: flex; align-items: center; justify-content: space-between; }}
    .auto-refresh-label {{ font-size: 12px; color: #666; display: flex; align-items: center; gap: 4px; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>CapMap 启动控制台</h1>
  </div>
  
  <div class="status">
    <div id="status-text"><strong>Streamlit 状态：</strong> {running_text} {pid_text}</div>
    <div><small>提示：请先运行 deploy.sh 生成 run.sh / stop.sh。</small></div>
  </div>
  
  <div id="msg-box" class="msg">{html.escape(msg)}</div>
  
  <p>
    <button id="btn-start" type="button" class="btn start" onclick="doAction('start')">启动</button>
    <button id="btn-stop" type="button" class="btn stop" onclick="doAction('stop')">停止</button>
    <button id="btn-restart" type="button" class="btn restart" onclick="doAction('restart')">重启</button>
  </p>
  
  <div class="header">
    <h3>应用日志 (app.log)</h3>
    <label class="auto-refresh-label">
        <input type="checkbox" id="auto-scroll" checked> 自动滚动
    </label>
  </div>
  <pre id="app-log" class="log">{app_log_text}</pre>
  
  <details>
    <summary>查看原始日志 (streamlit.out)</summary>
    <pre id="raw-log" class="log">{raw_log_text}</pre>
  </details>

<script>
const TOKEN_PARAM = "{token_q}";
const MSG_BOX = document.getElementById('msg-box');

function showMsg(text) {{
    MSG_BOX.innerText = text;
    MSG_BOX.style.display = 'block';
    setTimeout(() => {{ MSG_BOX.style.display = 'none'; }}, 5000);
}}

async function fetchStatus() {{
    try {{
        const res = await fetch('/status' + (TOKEN_PARAM ? TOKEN_PARAM : ''));
        if (res.ok) {{
            const data = await res.json();
            const text = data.running ? "运行中 PID: " + data.pid : "未运行";
            document.getElementById('status-text').innerHTML = "<strong>Streamlit 状态：</strong> " + text;
        }}
    }} catch (e) {{ console.error(e); }}
}}

async function fetchLogs() {{
    try {{
        const res = await fetch('/logs' + (TOKEN_PARAM ? TOKEN_PARAM : ''));
        if (res.ok) {{
            const data = await res.json();
            const appLog = document.getElementById('app-log');
            const rawLog = document.getElementById('raw-log');
            
            // Handle auto-scroll for app log
            const shouldScroll = document.getElementById('auto-scroll').checked;
            const isNearBottom = appLog.scrollHeight - appLog.scrollTop - appLog.clientHeight < 50;

            if (appLog.innerText !== data.app_log) {{
                appLog.innerText = data.app_log;
                if (shouldScroll && isNearBottom) {{
                    appLog.scrollTop = appLog.scrollHeight;
                }}
            }}
            
            if (rawLog.innerText !== data.raw_log) {{
                rawLog.innerText = data.raw_log;
            }}
        }}
    }} catch (e) {{ console.error(e); }}
}}

async function doAction(action) {{
    const btn = document.getElementById('btn-' + action);
    const originalText = btn.innerText;
    btn.disabled = true;
    btn.innerText = "执行中...";
    
    try {{
        const url = '/' + action + (TOKEN_PARAM ? TOKEN_PARAM + '&ajax=1' : '?ajax=1');
        const res = await fetch(url, {{ method: 'POST' }});
        
        const contentType = res.headers.get("content-type");
        if (contentType && contentType.includes("application/json")) {{
            const data = await res.json();
            if (data.success || res.ok) {{
                showMsg(data.msg || "操作成功");
                fetchStatus();
            }} else {{
                showMsg("错误: " + (data.msg || "未知错误"));
            }}
        }} else {{
            if (res.redirected || res.ok) {{
                 window.location.reload();
            }} else {{
                 showMsg("状态异常: " + res.status);
            }}
        }}
    }} catch (e) {{
        showMsg("请求失败: " + e);
    }} finally {{
        btn.disabled = false;
        btn.innerText = originalText;
    }}
}}

// Scroll to bottom on load
const appLog = document.getElementById('app-log');
appLog.scrollTop = appLog.scrollHeight;

// Auto refresh
setInterval(fetchStatus, 3000);
setInterval(fetchLogs, 2000);

</script>
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
        
    def _json_resp(self, data, code=200):
        self._send(code, json.dumps(data), "application/json")

    def _auth_or_403(self, query):
        if not _auth_ok(query, self.headers):
            self._send(403, "无权限")
            return False
        return True

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path
        
        if not self._auth_or_403(query):
            return
            
        if path in ("/", "/index.html"):
            self._send(200, _html_page(query))
        elif path == "/status":
            self._send(200, json.dumps(_status_payload()), "application/json")
        elif path == "/logs":
            app_log = _tail_log(APP_LOG_FILE, max_lines=200, max_bytes=60000)
            raw_log = _tail_log(LOG_FILE, max_lines=120, max_bytes=30000)
            self._json_resp({"app_log": app_log, "raw_log": raw_log})
        else:
            self._send(404, "未找到")

    def do_POST(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path
        is_ajax = "ajax" in query and query["ajax"][0] == "1"

        if not self._auth_or_403(query):
            return

        msg = ""
        success = False

        if path == "/start":
            if not os.path.exists(RUN_SCRIPT):
                msg = "\u672a\u627e\u5230 run.sh\uff0c\u8bf7\u5148\u8fd0\u884c deploy.sh\u3002"
            elif _is_running():
                msg = "Streamlit \u5df2\u5728\u8fd0\u884c"
                success = True
            else:
                _run_cmd(["bash", RUN_SCRIPT])
                msg = "\u5df2\u542f\u52a8"
                success = True

        elif path == "/stop":
            if not os.path.exists(STOP_SCRIPT):
                msg = "\u672a\u627e\u5230 stop.sh\uff0c\u8bf7\u5148\u8fd0\u884c deploy.sh\u3002"
            elif not _is_running():
                msg = "Streamlit \u672a\u8fd0\u884c"
                success = True
            else:
                _run_cmd(["bash", STOP_SCRIPT])
                msg = "\u5df2\u505c\u6b62"
                success = True

        elif path == "/restart":
            if not os.path.exists(RUN_SCRIPT) or not os.path.exists(STOP_SCRIPT):
                msg = "\u672a\u627e\u5230 run.sh/stop.sh\uff0c\u8bf7\u5148\u8fd0\u884c deploy.sh\u3002"
            else:
                if _is_running():
                    _run_cmd(["bash", STOP_SCRIPT])
                    time.sleep(1)
                _run_cmd(["bash", RUN_SCRIPT])
                msg = "\u5df2\u91cd\u542f"
                success = True
        else:
            self._send(404, "未找到")
            return

        if is_ajax:
            self._json_resp({"success": success, "msg": msg}, code=200 if success else 400)
        else:
            # Always return JSON for POST to avoid browser navigation issues
            self._json_resp({"success": success, "msg": msg}, code=200 if success else 400)


def main():
    httpd = HTTPServer((HOST, PORT), Handler)
    print(f"控制台已启动: http://{HOST}:{PORT}/")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
