# 服务器部署说明（Linux）

本目录为服务器版独立部署包，使用独立端口运行，并支持二级地址访问，例如：
`https://your-domain.com/capmap/`

## 一键部署
1) 将本目录上传到你的 Linux 服务器。
2) 执行：
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```
   按提示输入端口和二级地址（可直接回车使用默认值）。
3) 启动应用：
   ```bash
   ./run.sh
   ```

## 修改端口或二级地址
可用环境变量直接指定：
```bash
PORT=8501 BASE_PATH=capmap ./deploy.sh
```
注意：`BASE_PATH` 不要带 `/`，脚本会自动规范化。

## Nginx 二级地址反代
把 `nginx_subpath.conf` 里的片段加入你现有的 Nginx server 配置中，
并确保 `BASE_PATH` 与 location 路径一致（例如 `/capmap/`）。

## 停止服务
```bash
./stop.sh
```
