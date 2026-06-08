# MarisaProxy — 基于 io_uring 的高性能 HTTP 反向代理网关

## 项目简介

MarisaProxy 是一个基于 Linux **io_uring** 异步 I/O 框架开发的高性能 HTTP 反向代理网关。采用**多 Worker + SO_REUSEPORT** 架构，每个 Worker 拥有独立的 io_uring 实例和事件循环，实现真正的无锁并行处理。

设计目标：
- **极致性能**：充分发挥 io_uring 零拷贝、内核态 I/O 的优势
- **无锁架构**：每个 Worker 独立运行，线程本地连接池，零共享状态
- **线程本地连接池**：每个 Worker 独立维护后端连接，无需任何同步机制
- **独立健康检查**：健康检查使用独立 TCP 连接，绝不干扰业务连接池

### 核心特性

- **io_uring 异步 I/O**：使用 Linux 5.1+ 的 io_uring 接口，支持 IOSQE_BUFFER_SELECT 自动缓冲区管理
- **多 Worker 并行**：SO_REUSEPORT 多进程/线程模型，内核自动负载均衡
- **线程本地连接池**：每个 Worker 拥有独立的 BackendPool，后端连接完全隔离，无需锁或原子操作
- **独占式后端分配**：acquire() 从空闲队列取出后端，release() 放回，天然避免并发写入同一 socket
- **独立健康检查**：定时器触发后，为每个后端创建独立 TCP 连接执行 `GET /health`，不复用业务连接池
- **JWT 令牌鉴权**：基于 jwt-cpp 头文件库，HS256 签名验证
- **IP 限流**：滑动窗口算法，每 IP 每秒可自定义最大请求数
- **CPU 亲和性**：支持绑定 Worker 到指定 CPU，优化缓存局部性
- **零拷贝设计**：io_uring Provided Buffers 减少内存拷贝
- **后端 Keep-Alive**：后端连接复用，避免反复 TCP 握手和 REGISTER 开销

## 系统要求

- Linux 内核 ≥ 5.1（io_uring 支持）
- C++17 兼容的编译器（GCC ≥ 8, Clang ≥ 7）
- CMake ≥ 3.10
- OpenSSL ≥ 1.1（jwt-cpp 依赖 HMAC/SHA256）
- liburing（io_uring 用户态接口库）

### 安装依赖（Ubuntu/Debian）

```bash
sudo apt install build-essential cmake libssl-dev liburing-dev
```

## 构建方式

```bash
# 克隆项目
git clone <repository_url>
cd Proxy

# 创建构建目录
mkdir -p build && cd build

# 配置（CMake 自动查找 OpenSSL 和 liburing）
cmake ..

# 编译
make -j$(nproc)

# 编译产物
# ./build/Proxy             — 网关主程序
# ./build/cli/cli_client    — CLI 测试客户端（后端模拟/客户端模拟/QPS压测三合一）
```

## 项目结构

```
Proxy/
├── include/                  # 头文件目录
│   ├── auth.h               # JWT 令牌鉴权封装（HS256）
│   ├── backend_pool.h       # 线程本地后端连接池（BackendPool）
│   ├── gateway.h            # 网关编排器（多 Worker 管理/优雅关闭）
│   ├── rate_limiter.h       # 基于 IP 的滑动窗口限流器
│   ├── worker.h             # Worker 工作线程（io_uring 事件循环/状态机/健康检查）
│   ├── mylog.h              # 无锁日志模块
│   ├── json/                # nlohmann/json 头文件库
│   ├── jwt-cpp/             # jwt-cpp 头文件库
│   ├── liburing/            # liburing 头文件
│   └── picojson/            # picojson 头文件库
├── src/                      # 源代码目录
│   ├── main.cpp             # 程序入口，信号处理，配置加载
│   ├── gateway.cpp          # Gateway 编排器实现
│   ├── worker.cpp           # Worker 事件循环/状态机/健康检查实现
│   ├── backend_pool.cpp     # BackendPool 连接池实现
│   └── mylog.cpp            # 日志模块实现
├── cli/                      # CLI 测试客户端
│   ├── cli_client.cpp       # 后端模拟 + 客户端模拟 + QPS 压测（含健康检查监听器）
│   └── CMakeLists.txt       # 子项目构建配置
├── config/                   # 配置文件目录
│   ├── config.json          # 网关配置（含健康检查端口）
│   ├── backend.json         # 后端注册配置
│   └── client.json          # 客户端请求配置
├── log/                      # 日志输出目录（已 gitignore）
├── build/                    # 构建输出目录（已 gitignore）
├── CMakeLists.txt            # 顶层 CMake 构建配置
├── CLAUDE.md                 # AI 辅助开发规范
└── README.md                 # 项目说明
```

## 架构设计

### 整体架构

```
┌──────────────────────────────────────────────────────────┐
│                       Gateway                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌───────────┐ │
│  │   Worker 0       │  │   Worker 1       │  │ Worker N  │ │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ...      │ │
│  │  │ io_uring₀  │  │  │ │ io_uring₁  │  │  │           │ │
│  │  │ Ring=1024  │  │  │ │ Ring=1024  │  │  │           │ │
│  │  │BackendPool │  │  │ │BackendPool │  │  │           │ │
│  │  │ 健康检查   │  │  │ │ 健康检查   │  │  │           │ │
│  │  └───────────┘  │  │  └───────────┘  │  │           │ │
│  │  SO_REUSEPORT    │  │  SO_REUSEPORT    │  │           │ │
│  └────────┬────────┘  └────────┬────────┘  └─────┬─────┘ │
│           │                    │                  │        │
└───────────┼────────────────────┼──────────────────┼────────┘
            │                    │                  │
      ┌─────┴─────┐        ┌─────┴─────┐      ┌─────┴─────┐
      │ Client    │        │ Client    │      │ Client    │
      │ :8888     │        │ :8888     │      │ :8888     │
      └───────────┘        └───────────┘      └───────────┘
```

### 状态机流转

**客户端请求链路（Connection: close）：**
```
Client Connect → ACCEPTING
  ↓
READING_CLIENT  ← 读取 HTTP 请求
  ↓
（鉴权/限流检查）
  ↓
ACQUIRE_BACKEND ← BackendPool 获取后端 fd（acquire 独占）
  ↓
WRITING_BACKEND ← 将请求转发给后端（支持 partial write）
  ↓
READING_BACKEND ← 读取后端响应（IOSQE_BUFFER_SELECT）
  ↓
WRITING_CLIENT  ← 将响应写回客户端
  ↓
CLOSE           ← 关闭连接
```

**后端注册链路（Keep-Alive）：**
```
Backend Connect → ACCEPTING
  ↓
BACKEND_REGISTER  ← 读取 REGISTER 消息
  ↓
BACKEND_IDLE      ← 进入空闲，等待被分配请求
  ↓
（被选中处理请求 → WRITING_BACKEND → READING_BACKEND → BACKEND_IDLE）
```

**健康检查链路（独立 TCP 连接，每 5 秒）：**
```
HEALTH_CHECK_TIMER  ← io_uring IORING_OP_TIMEOUT 完成
  ↓
遍历所有 BACKEND_IDLE 的后端
  ↓
对每个后端：
  socket() → connect(backend_ip:health_check_port)
    → send("GET /health")
    → recv("200 OK" 检查)
    → close()
  ↓
健康 → 连接池保持不变
不健康 → backend_pool_.remove(backend_fd) + close_connection()
```

### BackendPool 线程本地连接池设计

```
┌────────────────────────────────────────────────────┐
│              Worker 内部（单线程）                    │
│                                                     │
│  BackendPool                                         │
│  ┌──────────────────────────────────────────────┐  │
│  │  entries_ (fd → PoolEntry)                    │  │
│  │  ┌──────────┬──────────┬──────────┬────────┐ │  │
│  │  │ fd=10    │ fd=11    │ fd=12    │ fd=13  │ │  │
│  │  │ /api     │ /api     │ /api     │ /img   │ │  │
│  │  │ in_use=✓ │ in_use=✗ │ in_use=✗ │ in_use=✗│ │  │
│  │  └──────────┴──────────┴──────────┴────────┘ │  │
│  │                                                │  │
│  │  idle_queue_ (FIFO 空闲 fd)                    │  │
│  │  ┌──────┬──────┬──────┐                        │  │
│  │  │ fd=11│ fd=12│ fd=13│  ← acquire 从头取      │  │
│  │  └──────┴──────┴──────┘     release 放回尾      │  │
│  └──────────────────────────────────────────────┘  │
│                                                     │
│  操作：                                              │
│  add(fd, prefix, auth, rate)    ← 后端注册时        │
│  acquire(path, &auth, &rate)    ← 客户端请求时      │
│  release(fd)                    ← 响应完成后        │
│  remove(fd)                     ← 连接断开时        │
└────────────────────────────────────────────────────┘
```

BackendPool 的核心思想：
1. **线程本地，无需锁**：每个 Worker 拥有自己的 BackendPool，单线程访问，零同步开销
2. **FIFO 公平调度**：`acquire` 从空闲队列头部取，`release` 放回尾部，公平分配
3. **独占式后端分配**：`acquire` 将后端标记为 `in_use=true` 并移出空闲队列，天然避免同一 fd 被并发写入
4. **最长前缀匹配**：`acquire(path)` 遍历空闲队列，找到前缀最匹配的后端
5. **零竞态条件**：没有共享指针、没有 COW、没有 deep copy，每个 Worker 完全独立

### 健康检查架构

健康检查使用**完全独立于业务连接池**的 TCP 连接：

```
┌──────────────────────────────────────────────────────┐
│                  Worker 内部                           │
│                                                       │
│  每 5 秒：io_uring IORING_OP_TIMEOUT 完成            │
│    ↓                                                  │
│  perform_health_checks()                              │
│    ↓                                                  │
│  收集所有 BACKEND_IDLE 的后端 fd                      │
│    ↓                                                  │
│  ┌────────────────────────────────────────────────┐  │
│  │ 对每个后端:                                     │  │
│  │  1. getpeername(backend_fd) → 获取后端 IP       │  │
│  │  2. socket(AF_INET, SOCK_STREAM, 0)            │  │
│  │  3. connect(backend_ip:health_check_port)       │  │
│  │  4. send("GET /health HTTP/1.1\r\n...")        │  │
│  │  5. recv() → 检查 "200 OK"                      │  │
│  │  6. close() → 立即关闭健康检查连接              │  │
│  │                                                 │  │
│  │  健康 → 连接池保持不变                          │  │
│  │  不健康 → backend_pool_.remove(backend_fd)     │  │
│  │          + close_connection()                   │  │
│  └────────────────────────────────────────────────┘  │
│                                                       │
│  关键设计：                                           │
│  - 业务连接池的 fd 从未被健康检查使用                 │
│  - 健康检查连接使用 Connection: close                 │
│  - 2 秒超时保护，避免阻塞事件循环                    │
└──────────────────────────────────────────────────────┘
```

## 配置说明

### 网关配置 (`config/config.json`)

```json
{
    "client_port": 8888,           // 客户端监听端口
    "backend_port": 9999,          // 后端注册端口
    "ring_size": 1024,             // io_uring 队列深度
    "worker_processes": 8,         // Worker 线程数量
    "worker_cpu_affinity": [       // CPU 亲和性掩码（与 Worker 逐一对应）
        "00000001","00000010",     // Worker 0→CPU0, Worker 1→CPU1
        "00000100","00001000",     // Worker 2→CPU2, Worker 3→CPU3
        "00010000","00100000",     // Worker 4→CPU4, Worker 5→CPU5
        "01000000","10000000"      // Worker 6→CPU6, Worker 7→CPU7
    ],
    "health_check_port": 9090,     // 健康检查端口（后端需在此端口监听 /health）
    "log_level": 1,                // 日志级别 (0=DEBUG, 1=INFO, 2=WARN, 3=ERROR, 4=FATAL)
    "log_file": "./log/gateway.log" // 日志文件路径
}
```

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `client_port` | 客户端 HTTP 请求监听端口 | 8888 |
| `backend_port` | 后端 TCP 注册监听端口 | 9999 |
| `ring_size` | io_uring 队列深度 | 1024 |
| `worker_processes` | Worker 线程数量 | 1 |
| `worker_cpu_affinity` | CPU 亲和性掩码数组 | `[]` |
| `health_check_port` | 后端健康检查 HTTP 端口 | 9090 |
| `log_level` | 日志级别 (0=DEBUG ~ 4=FATAL) | 1 |
| `log_file` | 日志输出文件路径 | `./log/gateway.log` |

### 后端配置 (`config/backend.json`)

```json
{
    "mode": "backend",             // 运行模式
    "prefix": "/api",              // 注册路径前缀
    "need_auth": false,            // 是否需要 JWT 鉴权
    "need_rate_limit": false,      // 是否需要 IP 限流
    "listen_port": 9090,           // 本地监听端口
    "gw_host": "127.0.0.1",       // 网关地址
    "gw_backend_port": 9999        // 网关后端端口
}
```

### 客户端配置 (`config/client.json`)

```json
{
    "mode": "client",              // 运行模式
    "method": "GET",               // HTTP 方法
    "path": "/api/hello",          // 请求路径
    "host": "test.com",            // Host 头
    "body": "",                    // 请求体（可选）
    "gw_host": "127.0.0.1",       // 网关地址
    "gw_client_port": 8888         // 网关客户端端口
}
```

## 快速上手

### 1. 启动网关

```bash
./build/Proxy -c ./config/config.json
```

网关启动后：
- 在 `8888` 端口监听客户端 HTTP 请求
- 在 `9999` 端口监听后端 TCP 注册
- 每 5 秒定期对已注册后端执行健康检查（连接 `health_check_port`）

### 2. 注册后端（新终端）

```bash
# 不需要鉴权和限流
./build/cli/cli_client -c ./config/backend.json

# 需要鉴权和限流（修改 backend.json 中 need_auth/need_rate_limit 为 true）
```

注册成功后，网关会将匹配该路径前缀的请求转发至此后端连接。

### 3. 发送客户端请求（新终端）

```bash
./build/cli/cli_client -c ./config/client.json
```

### 4. QPS 压测（一键式）

```bash
# 直接运行，无需配置文件
./build/cli/cli_client --benchmark --threads 10 --duration 20

# 自定义参数
./build/cli/cli_client --benchmark \
    --threads 20 \
    --duration 30 \
    --port 8888 \
    --backend-port 9999 \
    --health-check-port 9090 \
    --prefix /api \
    --path /api/hello

# 启用鉴权和限流
./build/cli/cli_client --benchmark --auth 1 --rate-limit 1
```

压测模式下，CLI 会自动：
1. 在 `health_check_port` 启动健康检查 HTTP 监听器（响应 `200 OK`）
2. 启动多路后端连接线程池（自动重连 + 重新注册）
3. 等待后端就绪后，启动多线程客户端并发发送 HTTP 请求
4. 达到指定时间后，统计并输出 QPS 结果

### 5. 优雅关闭

按下 `Ctrl+C` 发送 SIGINT 信号，网关会：
1. 向所有 Worker 发送 `SIGUSR1` 唤醒信号
2. 等待所有 Worker 完成当前请求后退出
3. 清理所有连接上下文和 io_uring 资源
4. 如果 3 秒内未完成，发送 `SIGKILL` 强制终止

## 路由注册协议

后端通过 TCP 连接到网关的 `backend_port`（默认 9999）后，发送如下格式的注册消息：

```
REGISTER /api/auth auth=1 rate=1
```

| 字段 | 说明 | 值 |
|------|------|-----|
| `REGISTER` | 注册命令 | 固定前缀 |
| `/api/auth` | 路径前缀 | URL 路径 |
| `auth=1` | 需要 JWT 鉴权 | `1`/`true` 或 `0`/`false` |
| `rate=1` | 需要 IP 限流 | `1`/`true` 或 `0`/`false` |

**Keep-Alive 行为**：
- 注册完成后，后端连接进入 `BACKEND_IDLE` 空闲状态
- 网关会在此连接上直接写入转发请求（`WRITING_BACKEND`）
- 后端读取请求 → 处理 → 发回响应 → 网关读取响应后发回给客户端
- 后端连接保持在 `BACKEND_IDLE`，继续等待下一个请求
- 多个后端可以注册同一前缀，网关按轮询分配请求

## CLI 测试客户端详解

`cli_client` 是一个三合一多功能工具：

### 后端模式 (`mode: "backend"`)

模拟后端服务：
1. TCP 连接到网关 `gw_backend_port`
2. 发送 `REGISTER` 消息注册路由前缀
3. 在 `listen_port` 上监听 HTTP 请求（从注册连接读取转发请求）
4. 处理请求并返回 HTTP 200 响应
5. 支持 Keep-Alive 持续处理多个请求

### 客户端模式 (`mode: "client"`)

模拟客户端请求：
1. TCP 连接到网关 `gw_client_port`
2. 发送预配置的 HTTP 请求
3. 读取并打印 HTTP 响应
4. 支持自定义 method、path、host、body

### 压测模式 (`mode: "benchmark"` 或 `--benchmark`)

自包含 QPS 性能测试（无需外部依赖）：

```
┌──────────────────────────────────────────────────────────────────┐
│                     Benchmark 压测架构                             │
│                                                                   │
│  ┌─────────────────────┐        ┌─────────────────────────────┐  │
│  │ 健康检查监听器线程   │        │ 后端连接池 (N 个线程)      │  │
│  │ :9090 (GET /health)◄├───┐    │ ┌───────────────────────┐  │  │
│  │ 返回 200 OK         │   │    │ │ 连接1 → REGISTER → IDLE│  │  │
│  └─────────────────────┘   │    │ │ 连接2 → REGISTER → IDLE│  │  │
│                            │    │ │ ...                   │  │  │
│  ┌─────────────────────┐   │    │ │ 连接N → REGISTER → IDLE│  │  │
│  │ 网关 Proxy          │◄──┼────┤ └───────────────────────┘  │  │
│  │ :8888 (客户端)      │   │    └─────────────────────────────┘  │
│  │ :9999 (后端注册)    │   │                                      │
│  │ 健康检查: 每5秒     ├───┘                                      │
│  └─────────────────────┘                                          │
│        ↑                                                          │
│  ┌─────────────────────┐                                          │
│  │ 客户端线程池 (M个)   │                                          │
│  │ 每个线程: 循环请求   │                                          │
│  └─────────────────────┘                                          │
└──────────────────────────────────────────────────────────────────┘
```

#### 压测配置项

| CLI 参数 | JSON 配置键 | 说明 | 默认值 |
|----------|-----------|------|--------|
| `--host` | `gw_host` | 网关地址 | `127.0.0.1` |
| `--port` | `gw_client_port` | 网关客户端端口 | `8888` |
| `--backend-port` | `gw_backend_port` | 网关后端端口 | `9999` |
| `--health-check-port` | `health_check_port` | 健康检查端口 | `9090` |
| `--prefix` | `prefix` | 后端注册前缀 | `/` |
| `--method` | `method` | HTTP 方法 | `GET` |
| `--path` | `path` | 请求路径 | 同 prefix |
| `--header-host` | `host` | Host 头 | `test.com` |
| `--threads` | `threads` | 客户端线程数 | `10` |
| `--duration` | `duration` | 压测持续时间（秒） | `20` |
| `--auth` | `need_auth` | 启用 JWT 鉴权 | `false` |
| `--rate-limit` | `need_rate_limit` | 启用 IP 限流 | `false` |

#### 健康检查监听器

在压测模式下，CLI 会自动启动一个健康检查 HTTP 服务器：

- 在 `health_check_port`（默认 9090）监听
- 接受 `GET /health` 请求
- 返回 `HTTP/1.1 200 OK`
- 使用非阻塞 accept + 100ms 轮询
- 在压测结束后自动关闭

## 运行测试

```bash
# 一键测试脚本
cd /home/marisa/code1/Proxy

# 启动网关（后台运行）
./build/Proxy -c ./config/config.json &

# 注册后端（后台运行）
./build/cli/cli_client -c ./config/backend.json &

# 发送客户端请求
./build/cli/cli_client -c ./config/client.json

# 清理
kill %1 %2
```

### QPS 压测

```bash
# 启动网关
./build/Proxy -c ./config/config.json &

# 运行压测（2 线程 / 5 秒）
./build/cli/cli_client --benchmark --threads 2 --duration 5

# 运行压测（10 线程 / 20 秒，推荐）
./build/cli/cli_client --benchmark --threads 10 --duration 20

# 停止网关
kill %1
```

### 性能参考

以下为单核 CPU 压测结果（所有请求在单个 Worker 上处理）：

| 配置 | 总请求数 | 成功率 | QPS |
|------|---------|--------|-----|
| 1-core, 10 threads, 20s | 248,787 | 100% | 12,439 |
| 2-core, 10 threads, 20s | 316,921 | 99.98% | 15,842 |

> **测试环境**：启用 AIO, C++17, O2 优化, io_uring 队列深度 1024, Provided Buffers 64 个

## 健康检查机制

### 设计原则

健康检查使用**完全独立于业务连接池**的 TCP 连接，这是为了：

1. **零干扰**：业务连接池中的所有连接专用于处理用户请求，健康检查绝不触碰这些 fd
2. **准确性**：健康检查连接真实模拟客户端视角的访问，检测后端服务的真实可用性
3. **隔离性**：即使健康检查连接出现问题（如超时、挂起），也不会影响已有的业务连接

### 实现细节

```
定时器触发（每 5 秒）
  ↓
perform_health_checks()
  ↓
┌── 收集所有 BACKEND_IDLE 的后端 ──┐
│                                   │
│  for each backend fd:             │
│    getpeername() → 获取 IP 地址    │
│    socket() → 创建新 TCP 连接     │
│    connect(ip:health_check_port)  │
│    send(GET /health)              │
│    recv() → 检查 "200 OK"         │
│    close() → 立即关闭             │
│                                   │
│    健康？→ 连接池保持不变           │
│    不健康？→ backend_pool_.remove(fd) │
│              + close_connection()    │
└───────────────────────────────────┘
```

关键设计决策：
- **独立 `socket()` → `connect()`**：每次健康检查都创建全新的 TCP 连接，绝不复用已有 fd
- **`Connection: close`**：确保后端发完响应后立即关闭连接，不留下半打开连接
- **同步阻塞 I/O**：由于健康检查在 io_uring 定时器回调中执行，使用同步 `send()`/`recv()`，但设置了 2 秒超时保护
- **`getpeername()` 动态获取地址**：从后端连接的 fd 中提取对端 IP，无需额外存储
- **健康检查端口独立于业务端口**：`health_check_port`（默认 9090）与 `backend_port`（默认 9999）分离

## 开发指南

### 添加新功能

1. **连接池策略**：修改 [`include/backend_pool.h`](include/backend_pool.h) 和 [`src/backend_pool.cpp`](src/backend_pool.cpp) 中的 `BackendPool` 类，调整 `acquire()` 的前缀匹配算法
2. **鉴权方式**：修改 [`include/auth.h`](include/auth.h)，支持更多 JWT 算法或自定义鉴权
3. **限流策略**：修改 [`include/rate_limiter.h`](include/rate_limiter.h)，支持令牌桶等算法
4. **协议支持**：修改 [`src/worker.cpp`](src/worker.cpp) 中的 `parse_http_request()`，支持 HTTP/2 或 WebSocket
5. **健康检查逻辑**：修改 [`src/worker.cpp`](src/worker.cpp) 中的 `perform_health_checks()` 和 `submit_health_check_timeout()`

### 代码风格

- C++17 标准
- 使用 `snake_case` 命名变量和函数
- 使用 `PascalCase` 命名类和结构体
- 全量中文 Doxygen 注释
- if/else 替代三目运算符以提高可读性
- 清晰的变量命名，避免单字母缩写

### AI 辅助开发

项目包含 [`CLAUDE.md`](CLAUDE.md) 文件，定义了 AI 辅助编码的规范：
- 分析 → 规划 → 执行 → 验证 的迭代流程
- 简单优先，不做过度设计
- 外科手术式精确修改，不重写无关代码

## 依赖的第三方库

| 库 | 用途 | 集成方式 |
|-----|------|---------|
| [liburing](https://github.com/axboe/liburing) | io_uring 用户态接口 | 系统库链接 |
| [nlohmann/json](https://github.com/nlohmann/json) | JSON 解析 | 头文件包含 |
| [jwt-cpp](https://github.com/Thalhammer/jwt-cpp) | JWT 令牌处理 | 头文件包含 |
| [picojson](https://github.com/kazuho/picojson) | JSON 解析（备用） | 头文件包含 |
| OpenSSL | HMAC/SHA256 密码学运算 | 系统库链接 |

## 许可证

[MIT License](LICENSE)
