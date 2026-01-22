# 服务器部署说明（Linux）

本目录为服务器独立部署包，使用独立端口运行，支持二级地址访问，例如：
`https://your-domain.com/capmap/`

## 一键部署
1) 上传本目录到服务器。
2) 运行：
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```
   按提示输入端口、二级地址、pip 源（可回车使用默认）。
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

## Nginx 二级地址反代
把 `nginx_subpath.conf` 片段加入现有 Nginx 配置，
并确保 `BASE_PATH` 与 location 路径一致（如 `/capmap/`）。

## 停止服务
```bash
./stop.sh
```

