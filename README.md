# MarisaProxy — 基于 io_uring 的高性能 HTTP 反向代理网关

## 项目简介

MarisaProxy 是一个基于 Linux **io_uring** 异步 I/O 框架开发的高性能 HTTP 反向代理网关。采用**多 Worker + SO_REUSEPORT** 架构，每个 Worker 拥有独立的 io_uring 实例和事件循环，实现真正的无锁并行处理。

### 核心特性

- **io_uring 异步 I/O**：使用 Linux 5.1+ 的 io_uring 接口，支持 IOSQE_BUFFER_SELECT 自动缓冲区管理
- **多 Worker 并行**：SO_REUSEPORT 多进程/线程模型，内核自动负载均衡
- **前缀树路由**：Trie 路由器支持最长前缀匹配，O(n) 时间复杂度
- **JWT 令牌鉴权**：基于 jwt-cpp 头文件库，HS256 签名验证
- **IP 限流**：滑动窗口算法，每 IP 每秒可自定义最大请求数
- **CPU 亲和性**：支持绑定 Worker 到指定 CPU，优化缓存局部性
- **零拷贝设计**：io_uring Provided Buffers 减少内存拷贝

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
# ./build/cli/cli_client    — CLI 测试客户端
```

## 项目结构

```
Proxy/
├── include/                  # 头文件目录
│   ├── auth.h               # JWT 令牌鉴权封装
│   ├── gateway.h            # 网关编排器（多 Worker 管理）
│   ├── rate_limiter.h       # 基于 IP 的滑动窗口限流器
│   ├── router.h             # 前缀树（Trie）路由器
│   ├── worker.h             # Worker 工作线程（io_uring 事件循环）
│   ├── mylog.h              # 日志模块
│   ├── json/                # nlohmann/json 头文件库
│   ├── jwt-cpp/             # jwt-cpp 头文件库
│   ├── liburing/            # liburing 头文件
│   └── picojson/            # picojson 头文件库
├── src/                      # 源代码目录
│   ├── main.cpp             # 程序入口，信号处理，配置加载
│   ├── gateway.cpp          # Gateway 编排器实现
│   ├── worker.cpp           # Worker 事件循环实现
│   ├── router.cpp           # Trie 路由器实现
│   ├── mylog.cpp            # 日志模块实现
│   └── channel.cpp          # 通道模块实现
├── cli/                      # CLI 测试客户端
│   ├── cli_client.cpp       # 后端模拟 + 客户端模拟
│   └── CMakeLists.txt       # 子项目构建配置
├── config/                   # 配置文件目录
│   ├── config.json          # 网关配置
│   ├── backend.json         # 后端注册配置
│   └── client.json          # 客户端请求配置
├── log/                      # 日志输出目录（已 gitignore）
├── build/                    # 构建输出目录（已 gitignore）
├── CMakeLists.txt            # 顶层 CMake 构建配置
└── README.md                 # 项目说明
```

## 架构设计

### 整体架构

```
┌──────────────────────────────────────────────────┐
│                    Gateway                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐ │
│  │ Worker 0   │  │ Worker 1   │  │ Worker N   │ │
│  │ io_uring₀  │  │ io_uring₁  │  │ io_uringₙ  │ │
│  │ SO_REUSEPORT│  │ SO_REUSEPORT│  │ SO_REUSEPORT│ │
│  └────────────┘  └────────────┘  └────────────┘ │
│         │                │                │       │
└─────────┼────────────────┼────────────────┼───────┘
          │                │                │
    ┌─────┴─────┐    ┌─────┴─────┐    ┌─────┴─────┐
    │ Client    │    │ Client    │    │ Client    │
    │ :8888     │    │ :8888     │    │ :8888     │
    └───────────┘    └───────────┘    └───────────┘
```

### 状态机流转

**客户端请求链路：**
```
ACCEPTING → READING_CLIENT ──→ WRITING_BACKEND
                              → READING_BACKEND → WRITING_CLIENT → CLOSE
```

**后端注册链路：**
```
ACCEPTING → BACKEND_REGISTER → BACKEND_IDLE
```

### 鉴权/限流链路

```
Client HTTP Request
  ↓
parse_http_request()  ← 提取 method、path、Authorization Bearer Token
  ↓
router_.lookup(path)  ← Trie 最长前缀匹配
  ↓
┌─ need_auth? ──→ verify_token() ── 失败 → 401 Unauthorized
│                 成功 → 继续
└─ need_rate_limit? ──→ rate_limiter_.allow(ip) ── 超限 → 429 Too Many Requests
                       允许 → 继续
  ↓
Forward to backend (WRITING_BACKEND)
```

## 配置说明

### 网关配置 (`config/config.json`)

```json
{
    "client_port": 8888,           // 客户端监听端口
    "backend_port": 9999,          // 后端注册端口
    "ring_size": 1024,             // io_uring 队列深度
    "worker_processes": 2,         // Worker 线程数量
    "worker_cpu_affinity": ["0001", "0010"],  // CPU 亲和性掩码
    "log_level": 1,                // 日志级别 (0=DEBUG ~ 4=FATAL)
    "log_file": "./log/gateway.log" // 日志文件路径
}
```

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

### 2. 注册后端（新终端）

```bash
# 不需要鉴权和限流
./build/cli/cli_client -c ./config/backend.json

# 需要鉴权和限流（修改 backend.json 中 need_auth/need_rate_limit 为 true）
```

### 3. 发送客户端请求（新终端）

```bash
./build/cli/cli_client -c ./config/client.json
```

### 4. 优雅关闭

按下 `Ctrl+C` 发送 SIGINT 信号，网关会等待所有 Worker 退出后清理资源。

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

## 开发指南

### 添加新功能

1. **路由相关**：修改 `include/router.h` 和 `src/router.cpp` 中的 `RouteInfo` 结构体或 `TrieRouter` 类
2. **鉴权方式**：修改 `include/auth.h`，支持更多 JWT 算法或自定义鉴权
3. **限流策略**：修改 `include/rate_limiter.h`，支持令牌桶等算法
4. **协议支持**：修改 `src/worker.cpp` 中的 `parse_http_request()`，支持 HTTP/2 或 WebSocket

### 代码风格

- C++17 标准
- 使用 `snake_case` 命名变量和函数
- 使用 `PascalCase` 命名类和结构体
- 全量中文 Doxygen 注释
- if/else 替代三目运算符以提高可读性
- 清晰的变量命名，避免单字母缩写

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
