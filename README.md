# 构建日志查看器

本地样例构建的实时日志页：FastAPI 后端 + 原生 JS 前端 + SSE。
可以启动/停止一次本地样例构建，边跑边看标准输出与错误输出，每行带行号。

## 关键行为

| 需求 | 实现 |
| --- | --- |
| 网页启动本地样例构建 | `POST /api/build/start` 用 `asyncio.create_subprocess_exec` 跑 `app/sample_build.py`（约 15 秒，stdout 进度 + stderr 警告） |
| 实时看到 stdout/stderr 与行号 | 两个 pump 任务逐行解码（UTF-8 增量解码、支持 `\n` / `\r\n` / `\r`、无换行的尾行也会落盘），SSE 按行号推送 |
| 无人观看时也继续采集 | 采集与查看完全解耦：pump 只写共享环形缓冲，从不等待客户端；没有订阅者时照常写入 |
| 服务端只保留最近 200 行 | `LogBuffer(capacity=200)`（`BUILDLOG_CAPACITY` 可调），行号全局单调递增、永不复用 |
| 重连从最后显示的行号之后补读 | 页面用 `?after=<最后渲染的行号>` 重连，服务端按序回放 `seq > after` 的所有仍保留的行 |
| 补读期间的新行也接上 | 每轮循环「先取唤醒事件、再读缓冲、再等待」，读到就立即续发；不会有读写间隙丢行，行号去重保证每行只显示一次 |
| 断线太久/太慢导致行被挤出 | 服务端发 `gap` 事件，带精确的 `first`/`last`；页面渲染「日志缺失：第 X–Y 行」，并从最早仍保留的行继续 |
| 慢连接不拖住采集与其他窗口 | SSE 生成器只读共享缓冲；每个客户端独立游标，慢客户端最多掉出窗口并收到 gap，绝不 back-pressure 到构建；每批发完 `await asyncio.sleep(0)` 让出事件循环 |
| 待发日志不能无限积压 | 服务端只有 200 行环形缓冲（唯一队列）；单帧最多 200 行；前端 DOM 上限 1200 行并自动裁剪 |

另外处理了两种容易漏掉的情况：

* **服务重启**：行号只在一次进程生命周期内有效。服务端给出 `session_id`，
  页面重连时带上 `epoch`；若服务端换了进程，页面清空旧行、从当前保留窗口重新开始，
  而不是把「重启」误报成日志缺失。
* **游标超出当前缓冲**：若 `after` 大于本次进程产出的最大行号（例如重启后仍带旧游标），
  服务端会让客户端从最早保留行重新开始，不会让页面永久卡住。

## 启动

### docker compose

```bash
docker compose up --build
# 打开 http://localhost:8000  （可用 BUILDLOG_PORT 改端口）
```

### 本地直接跑

```bash
python3 -m pip install -r requirements.txt
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 日志页 |
| POST | `/api/build/start` | 启动样例构建；已在运行时返回 409 |
| POST | `/api/build/cancel` | 停止当前构建（子进程被 kill） |
| GET | `/api/status` | 状态、行号范围、缓冲占用、session_id |
| GET | `/api/logs/stream?after=N` | SSE：`hello` / `lines` / `gap` / `status` / `end`，SSE `id` 为行号 |
| GET | `/api/logs/tail?after=N&limit=K` | 一次性读取，便于脚本或测试 |
| GET | `/healthz` | 健康检查（容器 HEALTHCHECK 使用） |

浏览器若没带 `after`（首次打开），服务端从「最早仍保留行之前」开始发送，
这样首屏能直接看到保留的日志，又不会为没见过面的构建误报缺失区间。

## 页面操作

* **启动样例构建 / 停止**
* **模拟断线**：主动关闭 EventSource；构建继续采集
* **重连**：从最后显示的行号之后续读
* 网络异常时自动重连（0.5s 起指数退避，上限 8s），依旧从最后显示的行开始
* 新行到达时若用户已向上滚动，不会强行拉回底部，右下角出现「回到最新」

## 测试与自测

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest tests/ -q      # 后端 23 项：缓冲语义 + 真 uvicorn 上的 SSE 端到端
python3 selftest.py              # 交付自测：断线续看 / 缓冲溢出 / 慢连接，逐项打印 ok
(cd tests/js && npm install && npm test)   # 前端 11 项（jsdom 驱动真实 app.js）
```

`tests/` 覆盖：行号单调、gap 精确区间、断开期间继续采集、重连首行、补读与实时行拼接无重无漏、
慢读者收到 gap 后从最早保留行继续、卡住的连接不影响构建时长与第二个窗口、
并发两个窗口内容一致、409 冲突、取消、失败退出码、无换行尾行、非法游标、重启后旧游标。

## 文件

```
app/main.py        FastAPI 应用工厂：路由、SSE 循环、游标/epoch 处理
app/buildlog.py    LogBuffer（200 行环形 + 行号 + gap）与 BuildRunner（子进程采集）
app/sample_build.py 样例构建
app/static/        index.html / style.css / app.js
tests/             后端测试、jsdom 前端测试、合成构建脚本
selftest.py        交付自测脚本
Dockerfile / docker-compose.yml
```

## 已知边界

* **单实例、单机**：状态在进程内存里。多副本部署时各副本有各自的缓冲与构建；没有跨进程共享。
  页面通过 `epoch` 感知「换了进程」，但不会跟随到别的副本。
* **服务重启后历史日志不保留**（需求只要求最近 200 行，未要求持久化）；正在跑的构建随进程结束。
* 只支持一个构建同时运行；第二个启动请求返回 409（页面按钮在运行时也会禁用）。
* 页面本身没有鉴权；默认只适合本机/内网使用。若对外暴露，应在前面加认证与 TLS。
* 构建完成后 SSE 连接保持打开以便继续接收下一次构建；只有服务关闭时才发 `end` 并断开。
