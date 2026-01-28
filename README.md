# 服务器部署说明（Linux）

本目录为服务器独立部署包，使用独立端口运行，支持二级地址访问，例如：
`https://your-domain.com/capmap/`

## 一键部署
1) 上传本目录到服务器。
git clone -b Sever https://gh-proxy.org/https://github.com/ForceMind/CapMap.git
cd CapMap
2) 运行：
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```
   按提示输入端口、二级地址、pip 源，并可选是否配置 Nginx 反代。
3) 启动：
   ```bash
   ./run.sh
   ```

## Python 版本要求
Akshare 需要 Python 3.10+，脚本会自动检测并尝试安装更高版本。

## 修改端口或二级地址
可用环境变量直接指定：
```bash
PORT=8501 BASE_PATH=capmap ./deploy.sh
```
注意：`BASE_PATH` 不要带 `/` 。

## pip 源与 akshare
部分镜像源没有 `akshare`，脚本失败会自动改用官方 PyPI。
也可指定：
```bash
PIP_INDEX_URL=https://pypi.org/simple ./deploy.sh
```

## Nginx 反代（脚本自动配置）
脚本默认会尝试安装/配置 Nginx，并写入：`/etc/nginx/conf.d/capmap.conf`，
如果你已有 80 端口的现有站点，请把 `NGINX_SETUP` 设为 `no` 并手动合并 location。

也可用环境变量指定：
```bash
NGINX_SETUP=yes NGINX_PORT=80 NGINX_SERVER_NAME=_ ./deploy.sh
```

## 停止服务
```bash
./stop.sh
```

## 查看日志
```bash
tail -f logs/streamlit.out
```

## 数据缓存与备份
日线缓存：`data/csi300_history_cache.parquet`。
分时缓存：`data/min_cache/`（按日期/周期自动保存，避免重复拉取）。
侧边栏提供“数据备份与恢复”，可生成 zip 下载，迁移时上传恢复。


## 启动控制台（可选）
控制台口令就是你自己设置的 `PANEL_TOKEN`，用于访问控制台页面。
建议设置强口令并妥善保存。未设置口令时不会启动控制台。

控制台用于在网页中启动/停止 Streamlit，无需登录终端。

通过 deploy.sh 启动（推荐）：
```bash
PANEL_ENABLE=yes PANEL_TOKEN=your_token ./deploy.sh
```
访问：`http://服务器IP:9000/?token=your_token`

手动启动（已部署但未开启控制台）：
```bash
chmod +x control_panel/run_panel.sh control_panel/stop_panel.sh
PANEL_TOKEN=your_token control_panel/run_panel.sh
```

停止控制台：
```bash
control_panel/stop_panel.sh
```
控制台日志：`logs/panel.out`


streamlit run app/app.py