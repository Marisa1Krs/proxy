#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <fcntl.h>

#include "json/json.hpp"

using json = nlohmann::json;

/**
 * @brief 基于 io_uring 网关的 CLI 测试客户端
 *
 * 三种工作模式：
 *   backend  模式: 模拟后端服务，连接 9999 注册路由，接收转发请求并响应
 *   client   模式: 模拟客户端，连接 8888 发送 HTTP 请求并打印响应
 *   benchmark模式: 自包含压测，同时扮演后端 + 多线程客户端，统计 QPS
 *
 * 使用方式：
 *   1. 启动网关: ./Proxy
 *   2. 注册后端: ./cli_client -c ./config/backend.json
 *   3. 发送请求: ./cli_client -c ./config/client.json
 *   4. 压测:     ./cli_client --benchmark --path /api/hello
 *
 * 配置文件示例 (backend.json):
 * @code
 * {
 *     "mode": "backend",
 *     "prefix": "/api",
 *     "listen_port": 9090,
 *     "gw_host": "127.0.0.1",
 *     "gw_backend_port": 9999
 * }
 * @endcode
 *
 * 配置文件示例 (client.json):
 * @code
 * {
 *     "mode": "client",
 *     "method": "GET",
 *     "path": "/api/hello",
 *     "host": "test.com",
 *     "body": "",
 *     "gw_host": "127.0.0.1",
 *     "gw_client_port": 8888
 * }
 * @endcode
 */

// ==================== 常量 ====================
static constexpr size_t BUFFER_SIZE = 4096;

// ==================== 工具函数 ====================

/**
 * @brief 创建 TCP 连接
 * @param host  目标主机
 * @param port  目标端口
 * @return  socket fd，失败返回 -1
 */
static int tcp_connect(const std::string& host, int port) {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        std::cerr << "[ERR] socket 创建失败: " << strerror(errno) << std::endl;
        return -1;
    }

    struct sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);

    int address_convert_result =
        inet_pton(AF_INET, host.c_str(), &server_address.sin_addr);
    if (address_convert_result <= 0) {
        std::cerr << "[ERR] 无效地址: " << host << std::endl;
        ::close(socket_fd);
        return -1;
    }

    int connect_result = connect(
        socket_fd,
        (struct sockaddr*)&server_address,
        sizeof(server_address));
    if (connect_result < 0) {
        std::cerr << "[ERR] 连接 " << host << ":" << port
                  << " 失败: " << strerror(errno) << std::endl;
        ::close(socket_fd);
        return -1;
    }

    return socket_fd;
}

/**
 * @brief 创建 TCP 监听 socket
 * @param port  监听端口
 * @return  listen fd，失败返回 -1
 */
static int tcp_listen(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cerr << "[ERR] socket 创建失败: " << strerror(errno) << std::endl;
        return -1;
    }

    int socket_option = 1;
    setsockopt(listen_fd, SOL_SOCKET,
               SO_REUSEADDR, &socket_option, sizeof(socket_option));

    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "[ERR] bind 端口 " << port
                  << " 失败: " << strerror(errno) << std::endl;
        ::close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, 10) < 0) {
        std::cerr << "[ERR] listen 失败: " << strerror(errno) << std::endl;
        ::close(listen_fd);
        return -1;
    }

    return listen_fd;
}

/**
 * @brief 接收所有数据直到连接关闭
 * @param fd      socket fd
 * @param buffer  接收缓冲区
 * @param size    缓冲区大小
 * @return        接收到的字节数，失败返回 -1
 */
static int recv_all(int fd, char* buffer, size_t size) {
    size_t total_bytes_received = 0;
    while (total_bytes_received < size - 1) {
        ssize_t bytes_received = read(
            fd,
            buffer + total_bytes_received,
            size - 1 - total_bytes_received);

        if (bytes_received < 0) {
            if (errno == EINTR) {
                continue;
            }
            // EAGAIN/EWOULDBLOCK 表示超时（SO_RCVTIMEO），
            // 返回已收到的部分数据而不是报错
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "[ERR] read 失败: " << strerror(errno) << std::endl;
            return -1;
        }

        if (bytes_received == 0) {
            // 连接关闭
            break;
        }

        total_bytes_received += bytes_received;
    }

    buffer[total_bytes_received] = '\0';
    return static_cast<int>(total_bytes_received);
}

/**
 * @brief 发送所有数据
 * @param fd     socket fd
 * @param data   数据指针
 * @param length 数据长度
 * @return       成功返回 true
 */
static bool send_all(int fd, const char* data, size_t length) {
    size_t total_bytes_sent = 0;
    while (total_bytes_sent < length) {
        ssize_t bytes_sent = write(
            fd,
            data + total_bytes_sent,
            length - total_bytes_sent);

        if (bytes_sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "[ERR] write 失败: " << strerror(errno) << std::endl;
            return false;
        }

        total_bytes_sent += bytes_sent;
    }

    return true;
}

// ==================== 用法打印 ====================

static void print_usage(const char* program_name) {
    std::cout << "用法: " << program_name << " [选项]" << std::endl;
    std::cout << std::endl;
    std::cout << "通用选项:" << std::endl;
    std::cout << "  -c, --config PATH   配置文件路径" << std::endl;
    std::cout << "  -h, --help          显示此帮助信息" << std::endl;
    std::cout << std::endl;
    std::cout << "压测选项（直接运行，无需配置文件）:" << std::endl;
    std::cout << "  --benchmark         以压测模式运行（QPS 测试）" << std::endl;
    std::cout << "  --host HOST         网关地址 (默认: 127.0.0.1)" << std::endl;
    std::cout << "  --port PORT         网关客户端端口 (默认: 8888)" << std::endl;
    std::cout << "  --backend-port PORT 网关后端端口 (默认: 9999)" << std::endl;
    std::cout << "  --prefix PREFIX     后端注册路径前缀 (默认: /)" << std::endl;
    std::cout << "  --method METHOD     HTTP 方法 (默认: GET)" << std::endl;
    std::cout << "  --path PATH         请求路径 (默认: 同 --prefix)" << std::endl;
    std::cout << "  --header-host HOST  请求 Host 头 (默认: test.com)" << std::endl;
    std::cout << "  --threads N         压测线程数 (默认: 10)" << std::endl;
    std::cout << "  --duration N        压测持续时间秒数 (默认: 20)" << std::endl;
    std::cout << "  --health-check-port PORT  后端健康检查端口 (默认: 9090)" << std::endl;
    std::cout << std::endl;
    std::cout << "配置文件支持三种 mode:" << std::endl;
    std::cout << "  \"backend\"   以后端模式运行（模拟后端服务）" << std::endl;
    std::cout << "  \"client\"    以客户端模式运行（模拟客户端请求）" << std::endl;
    std::cout << "  \"benchmark\" 以压测模式运行（QPS 性能测试）" << std::endl;
    std::cout << std::endl;
    std::cout << "backend 模式配置项:" << std::endl;
    std::cout << "  mode            必须为 \"backend\"" << std::endl;
    std::cout << "  prefix          注册路径前缀 (默认: /)" << std::endl;
    std::cout << "  need_auth       是否需要 JWT 令牌鉴权 (默认: false)" << std::endl;
    std::cout << "  need_rate_limit 是否需要 IP 限流 (默认: false)" << std::endl;
    std::cout << "  listen_port     本地监听端口，接收转发请求 (默认: 9090)" << std::endl;
    std::cout << "  gw_host         网关地址 (默认: 127.0.0.1)" << std::endl;
    std::cout << "  gw_backend_port  网关后端端口 (默认: 9999)" << std::endl;
    std::cout << std::endl;
    std::cout << "client 模式配置项:" << std::endl;
    std::cout << "  mode            必须为 \"client\"" << std::endl;
    std::cout << "  method          HTTP 方法 (默认: GET)" << std::endl;
    std::cout << "  path            请求路径 (默认: /)" << std::endl;
    std::cout << "  host            请求 Host (默认: test.com)" << std::endl;
    std::cout << "  body            请求体 (可选)" << std::endl;
    std::cout << "  gw_host         网关地址 (默认: 127.0.0.1)" << std::endl;
    std::cout << "  gw_client_port  网关客户端端口 (默认: 8888)" << std::endl;
    std::cout << std::endl;
    std::cout << "benchmark 模式配置项:" << std::endl;
    std::cout << "  mode            必须为 \"benchmark\"" << std::endl;
    std::cout << "  method          HTTP 方法 (默认: GET)" << std::endl;
    std::cout << "  path            请求路径 (默认: /)" << std::endl;
    std::cout << "  host            请求 Host (默认: test.com)" << std::endl;
    std::cout << "  gw_host         网关地址 (默认: 127.0.0.1)" << std::endl;
    std::cout << "  gw_client_port  网关客户端端口 (默认: 8888)" << std::endl;
    std::cout << "  gw_backend_port 网关后端端口 (默认: 9999)" << std::endl;
    std::cout << "  prefix          后端注册路径前缀 (默认: /)" << std::endl;
    std::cout << "  need_auth       是否需要 JWT 鉴权 (默认: false)" << std::endl;
    std::cout << "  need_rate_limit 是否需要 IP 限流 (默认: false)" << std::endl;
    std::cout << "  health_check_port  后端健康检查端口 (默认: 9090)" << std::endl;
    std::cout << "  threads         压测线程数 (默认: 10)" << std::endl;
    std::cout << "  duration        压测持续时间秒数 (默认: 20)" << std::endl;
    std::cout << std::endl;
    std::cout << "示例:" << std::endl;
    std::cout << "  " << program_name << " -c ./config/backend.json" << std::endl;
    std::cout << "  " << program_name << " -c ./config/client.json" << std::endl;
    std::cout << "  " << program_name << " --benchmark --threads 10 --duration 20" << std::endl;
    std::cout << "  " << program_name << " --benchmark --prefix /api --path /api/hello" << std::endl;
    std::cout << "  " << program_name << " --benchmark --health-check-port 9090" << std::endl;
}

// ==================== 后端模式 ====================

/**
 * @brief 后端模式主逻辑
 *
 * 注意：网关会通过后端注册时建立的 TCP 连接（gateway_fd）直接写入转发请求，
 * 因此后端应从 gateway_fd 读取，而非从 listen_fd accept 新连接。
 * 当前实现仅处理一次请求后退出。
 */
static int run_backend_mode(const std::string& prefix,
                             int listen_port,
                             const std::string& gateway_host,
                             int gateway_backend_port,
                             bool need_auth,
                             bool need_rate_limit) {

    // ---- 步骤 1: 创建本地监听 socket ----
    int listen_fd = tcp_listen(listen_port);
    if (listen_fd < 0) {
        return 1;
    }

    std::cout << "[Backend] 本地监听端口 " << listen_port
              << " (fd=" << listen_fd << ")" << std::endl;

    // ---- 步骤 2: 连接网关后端端口 ----
    int gateway_fd = tcp_connect(gateway_host, gateway_backend_port);
    if (gateway_fd < 0) {
        ::close(listen_fd);
        return 1;
    }

    std::cout << "[Backend] 已连接网关 " << gateway_host
              << ":" << gateway_backend_port
              << " (fd=" << gateway_fd << ")" << std::endl;

    // ---- 步骤 3: 发送注册消息（含鉴权和限流标志） ----
    char flags_buffer[128];
    snprintf(flags_buffer, sizeof(flags_buffer),
             " auth=%d rate=%d",
             need_auth ? 1 : 0,
             need_rate_limit ? 1 : 0);

    std::string register_message =
        "REGISTER " + prefix + flags_buffer + "\n";

    bool send_ok = send_all(
        gateway_fd,
        register_message.data(),
        register_message.size());
    if (!send_ok) {
        ::close(gateway_fd);
        ::close(listen_fd);
        return 1;
    }

    std::cout << "[Backend] 已注册前缀: \"" << prefix << "\""
              << " (auth=" << (need_auth ? "ON" : "OFF")
              << ", rate=" << (need_rate_limit ? "ON" : "OFF") << ")"
              << std::endl;

    // ---- 步骤 4: 在 gateway_fd 上等待转发请求 ----
    // 网关会将客户端请求通过本连接（gateway_fd）直接写入
    std::cout << "[Backend] 等待转发请求..." << std::endl;

    char read_buffer[BUFFER_SIZE] = {0};
    int bytes_received = recv_all(gateway_fd, read_buffer, BUFFER_SIZE);
    if (bytes_received < 0) {
        ::close(gateway_fd);
        ::close(listen_fd);
        return 1;
    }

    std::cout << "[Backend] ==== 收到的请求 =====" << std::endl;
    std::cout << read_buffer << std::endl;
    std::cout << "[Backend] ====================" << std::endl;

    // ---- 步骤 5: 构建 HTTP 响应 ----
    std::string response_body =
        std::string("{\"status\":\"ok\",\"message\":\"Hello from backend!\",\"prefix\":\"")
        + prefix + "\"}";

    int body_length = response_body.size();
    std::string http_response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: " + std::to_string(body_length) + "\r\n"
        "Connection: close\r\n"
        "\r\n"
        + response_body;

    // ---- 步骤 6: 发送响应 ----
    bool response_sent = send_all(
        gateway_fd,
        http_response.data(),
        http_response.size());

    if (response_sent) {
        std::cout << "[Backend] 已发送响应 ("
                  << http_response.size() << " 字节)" << std::endl;
    }

    // ---- 步骤 7: 等待连接关闭 ----
    // 网关发送完响应给客户端后会关闭本连接（Connection: close）
    char dummy_buffer[64];
    while (read(gateway_fd, dummy_buffer, sizeof(dummy_buffer)) > 0) {
        // 持续读取直到连接关闭
    }

    // ---- 步骤 8: 清理 ----
    ::close(gateway_fd);
    ::close(listen_fd);

    std::cout << "[Backend] 完成" << std::endl;
    return 0;
}

// ==================== 客户端模式 ====================

/**
 * @brief 客户端模式主逻辑
 *
 * 1. 连接网关客户端端口
 * 2. 构建并发送 HTTP 请求
 * 3. 接收并打印响应
 */
static int run_client_mode(const std::string& method,
                            const std::string& path,
                            const std::string& host,
                            const std::string& body,
                            const std::string& gateway_host,
                            int gateway_client_port) {

    // ---- 步骤 1: 连接网关客户端端口 ----
    int gateway_fd = tcp_connect(gateway_host, gateway_client_port);
    if (gateway_fd < 0) {
        return 1;
    }

    std::cout << "[Client] 已连接网关 " << gateway_host
              << ":" << gateway_client_port
              << " (fd=" << gateway_fd << ")" << std::endl;

    // ---- 步骤 2: 构建 HTTP 请求 ----
    std::string http_request;

    if (body.empty()) {
        // GET 请求（无 body）
        http_request =
            method + " " + path + " HTTP/1.1\r\n"
            "Host: " + host + "\r\n"
            "User-Agent: ProxyCLIClient/1.0\r\n"
            "Accept: */*\r\n"
            "Connection: close\r\n"
            "\r\n";
    } else {
        // POST/PUT 等带 body 的请求
        http_request =
            method + " " + path + " HTTP/1.1\r\n"
            "Host: " + host + "\r\n"
            "User-Agent: ProxyCLIClient/1.0\r\n"
            "Accept: */*\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: " + std::to_string(body.size()) + "\r\n"
            "Connection: close\r\n"
            "\r\n"
            + body;
    }

    // ---- 步骤 3: 发送请求 ----
    std::cout << "[Client] ==== 发送请求 =====" << std::endl;
    std::cout << http_request << std::endl;
    std::cout << "[Client] ===================" << std::endl;

    bool request_sent = send_all(
        gateway_fd,
        http_request.data(),
        http_request.size());

    if (!request_sent) {
        ::close(gateway_fd);
        return 1;
    }

    std::cout << "[Client] 已发送 " << http_request.size() << " 字节" << std::endl;

    // ---- 步骤 4: 读取响应 ----
    char response_buffer[BUFFER_SIZE] = {0};
    int bytes_received = recv_all(
        gateway_fd, response_buffer, BUFFER_SIZE);
    if (bytes_received < 0) {
        ::close(gateway_fd);
        return 1;
    }

    // ---- 步骤 5: 打印响应 ----
    std::cout << "[Client] ==== 收到响应 =====" << std::endl;
    std::cout << response_buffer << std::endl;
    std::cout << "[Client] ===================" << std::endl;
    std::cout << "[Client] 收到 " << bytes_received << " 字节" << std::endl;

    // ---- 步骤 6: 清理 ----
    ::close(gateway_fd);
    return 0;
}

// ==================== 压测模式 ====================

/**
 * @brief 压测配置参数
 */
struct BenchmarkConfig {
    std::string gateway_host          = "127.0.0.1";
    int    gateway_client_port        = 8888;
    int    gateway_backend_port       = 9999;
    int    health_check_port          = 9090;
    std::string register_prefix       = "/";
    bool   need_auth                  = false;
    bool   need_rate_limit            = false;
    std::string http_method           = "GET";
    std::string request_path          = "";
    std::string request_host          = "test.com";
    int    thread_count               = 10;
    int    duration_seconds           = 20;
};

/**
 * @brief 压测停止标志（共享于所有线程）
 */
struct BenchmarkControl {
    std::atomic<bool> backend_shutdown{false};
    std::atomic<bool> backend_ready{false};
    std::atomic<bool> health_check_shutdown{false};
};

/**
 * @brief 压测结果计数器（线程安全）
 */
struct BenchmarkResult {
    std::atomic<uint64_t> success_count{0};
    std::atomic<uint64_t> error_count{0};
    std::atomic<uint64_t> backend_reconnects{0};
    std::atomic<uint64_t> backend_errors{0};

    // 详细失败分类（压测客户端用）
    std::atomic<uint64_t> connect_failures{0};     // tcp_connect 失败
    std::atomic<uint64_t> send_failures{0};        // 发送请求失败
    std::atomic<uint64_t> recv_failures{0};        // recv_all 返回 <=0
    std::atomic<uint64_t> recv_econnreset{0};      // 接收时 ECONNRESET
    std::atomic<uint64_t> recv_etimedout{0};       // 接收超时 (无数据)
    std::atomic<uint64_t> recv_eof{0};             // 连接直接关闭 (无数据)
    std::atomic<uint64_t> recv_other{0};           // 其他接收错误
    std::atomic<uint64_t> response_failures{0};    // 响应不含 "200 OK"
};

/**
 * @brief 持久化后端线程
 *
 * 由于网关每个请求处理后都会关闭后端连接（close_connection 关闭 backend_fd），
 * 后端线程需要在每次请求后重新连接 + 重新注册路由。
 *
 * 工作循环：
 *   1. TCP 连接到网关后端端口 (9999)
 *   2. 发送 REGISTER 注册消息
 *   3. 等待转发请求（read 阻塞）
 *   4. 读取 HTTP 请求，构建并发送 HTTP 200 响应
 *   5. 等待连接关闭（read 返回 0）
 *   6. 回到步骤 1
 */
/**
 * @brief 单个后端连接线程（Keep-Alive 模式）
 *
 * 每个线程独立运行：连接 → 注册 → 循环处理请求（read→响应→read→...）→ 断开 → 重连。
 * 网关现在支持后端 keep-alive（handle_client_write 不关闭后端连接），
 * 因此后端可以持续在同一连接上接收并处理多个请求，无需每次重连。
 */
static void benchmark_backend_connection(const BenchmarkConfig& config,
                                          BenchmarkResult& result,
                                          BenchmarkControl& control) {

    char read_buffer[BUFFER_SIZE];
    std::string register_message =
        "REGISTER " + config.register_prefix + " auth=0 rate=0\n";

    // 外层循环：连接 → 注册 → 内层循环（请求处理）→ 断开 → 重连
    while (!control.backend_shutdown.load(std::memory_order_relaxed)) {

        // ---- 连接网关后端端口 ----
        int gateway_fd = tcp_connect(config.gateway_host,
                                      config.gateway_backend_port);
        if (gateway_fd < 0) {
            result.backend_errors.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // ---- 发送 REGISTER 注册消息 ----
        if (!send_all(gateway_fd, register_message.data(),
                      register_message.size())) {
            ::close(gateway_fd);
            result.backend_errors.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        result.backend_reconnects.fetch_add(1, std::memory_order_relaxed);

        // ---- 设置 1 秒接收超时，避免 shutdown 时无限阻塞 ----
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(gateway_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // ---- 内层循环：Keep-Alive 请求处理 ----
        // 在同一连接上反复读取并响应请求，直到连接断开
        bool connection_alive = true;
        while (connection_alive &&
               !control.backend_shutdown.load(std::memory_order_relaxed)) {

            // ---- 等待并读取转发请求 ----
            memset(read_buffer, 0, BUFFER_SIZE);
            int bytes_received = read(gateway_fd, read_buffer, BUFFER_SIZE - 1);

            if (bytes_received <= 0) {
                // 连接关闭（read 返回 0）或超时（EAGAIN）
                connection_alive = false;
                break;
            }

            read_buffer[bytes_received] = '\0';

            // ---- 构建 HTTP 200 响应 ----
            std::string response_body =
                "{\"status\":\"ok\",\"message\":\"benchmark\"}";
            int body_length = static_cast<int>(response_body.size());
            // 使用 Connection: keep-alive 让网关保持连接
            std::string http_response =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                "Content-Length: " + std::to_string(body_length) + "\r\n"
                "Connection: keep-alive\r\n"
                "\r\n"
                + response_body;

            if (!send_all(gateway_fd, http_response.data(),
                          http_response.size())) {
                // 发送失败，连接可能已断开
                connection_alive = false;
                break;
            }

            // ---- 继续循环，读取下一个请求 ----
        }

        ::close(gateway_fd);
        // 连接断开，进入外层循环：重新连接 + 重新注册
    }
}

/**
 * @brief 压测后端线程池管理
 *
 * 启动多个后端连接线程，每个独立维护自己的 TCP 连接和路由注册。
 * 连接池大小为 client_thread_count * 2，确保有足够后端连接可用。
 * 当任意一个后端线程首次成功注册后，标记后端就绪。
 */
static void benchmark_backend_worker(const BenchmarkConfig& config,
                                      BenchmarkResult& result,
                                      BenchmarkControl& control) {

    control.backend_ready.store(false, std::memory_order_relaxed);
    std::atomic<bool> first_ready{false};

    // 连接池大小 = 客户端线程数 * 6（最少 16 个，最多 64 个）
    // 多 Worker 场景下（8 Worker × 8 后端 = 64），
    // 确保每个 Worker 有足够后端处理突发并发
    int pool_size = std::max(config.thread_count * 6, 16);
    if (pool_size > 64) pool_size = 64;

    std::cout << "[Bench-Backend] 启动 " << pool_size << " 路后端连接..."
              << std::endl;

    std::vector<std::thread> pool;
    for (int i = 0; i < pool_size; ++i) {
        pool.emplace_back(benchmark_backend_connection,
                           std::ref(config),
                           std::ref(result),
                           std::ref(control));
    }

    // 等待至少一个后端连接成功注册（最多 5 秒）
    for (int wait_loop = 0; wait_loop < 50; ++wait_loop) {
        if (result.backend_reconnects.load(std::memory_order_relaxed) > 0) {
            control.backend_ready.store(true, std::memory_order_release);
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 即使没有后端就绪也标记（尝试过的最坏情况）
    if (!control.backend_ready.load()) {
        control.backend_ready.store(true, std::memory_order_release);
        std::cerr << "[Bench-Backend] 警告：后端注册可能超时" << std::endl;
    }

    // 等待退出信号
    while (!control.backend_shutdown.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 等待所有连接线程退出
    for (auto& t : pool) {
        if (t.joinable()) t.join();
    }

    std::cout << "[Bench-Backend] 后端线程池(" << pool_size << "路)已退出"
              << std::endl;
}

/**
 * @brief 单个压测客户端线程
 *
 * 在指定时间内循环向网关发送 HTTP 请求。
 * 每个请求独立建立 TCP 连接（与 Connection: close 行为一致）。
 */
static void benchmark_client_worker(const BenchmarkConfig& config,
                                     BenchmarkResult& result,
                                     BenchmarkControl& control) {
    char response_buffer[BUFFER_SIZE] = {0};

    // 预构造 HTTP 请求
    std::string http_request =
        config.http_method + " " + config.request_path + " HTTP/1.1\r\n"
        "Host: " + config.request_host + "\r\n"
        "User-Agent: ProxyBenchmark/1.0\r\n"
        "Accept: */*\r\n"
        "Connection: close\r\n"
        "\r\n";

    // 计算截止时间
    auto deadline_time = std::chrono::steady_clock::now()
                         + std::chrono::seconds(config.duration_seconds);

    // 循环发送请求直到超时
    while (std::chrono::steady_clock::now() < deadline_time) {

        // ---- 建立 TCP 连接到网关客户端端口 ----
        int socket_fd = tcp_connect(
            config.gateway_host, config.gateway_client_port);
        if (socket_fd < 0) {
            result.error_count.fetch_add(1, std::memory_order_relaxed);
            result.connect_failures.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        // ---- 设置接收超时 5 秒 ----
        struct timeval receive_timeout;
        receive_timeout.tv_sec  = 5;
        receive_timeout.tv_usec = 0;
        setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,
                   &receive_timeout, sizeof(receive_timeout));

        // ---- 发送 HTTP 请求 ----
        bool send_ok = send_all(
            socket_fd,
            http_request.data(),
            http_request.size());
        if (!send_ok) {
            ::close(socket_fd);
            result.error_count.fetch_add(1, std::memory_order_relaxed);
            result.send_failures.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        // ---- 读取 HTTP 响应 ----
        int bytes_received = recv_all(
            socket_fd, response_buffer, BUFFER_SIZE);

        ::close(socket_fd);

        // 检查响应状态码
        if (bytes_received > 0) {
            std::string response_text(response_buffer,
                                       std::min(bytes_received, (int)BUFFER_SIZE - 1));
            if (response_text.find("200 OK") != std::string::npos) {
                result.success_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                result.error_count.fetch_add(1, std::memory_order_relaxed);
                result.response_failures.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            result.error_count.fetch_add(1, std::memory_order_relaxed);
            result.recv_failures.fetch_add(1, std::memory_order_relaxed);
            // 追踪具体的接收错误原因
            if (bytes_received == 0) {
                result.recv_eof.fetch_add(1, std::memory_order_relaxed);
            } else {
                // bytes_received == -1: 检查 errno
                // EAGAIN 和 EWOULDBLOCK 在 Linux 上值相同
                int recv_err = errno;
                switch (recv_err) {
                    case ECONNRESET:
                        result.recv_econnreset.fetch_add(1, std::memory_order_relaxed);
                        break;
                    case EAGAIN:
                        result.recv_etimedout.fetch_add(1, std::memory_order_relaxed);
                        break;
                    default:
                        result.recv_other.fetch_add(1, std::memory_order_relaxed);
                        break;
                }
            }
        }
    }
}

/**
 * @brief 健康检查监听线程
 *
 * 监听 health_check_port，对 GET /health 请求返回 HTTP 200 OK。
 * 模拟后端服务的健康检查端点，供网关独立 TCP 连接探活使用。
 * 采用非阻塞 accept + 轮询，以便能及时响应 shutdown 信号。
 */
static void benchmark_health_check_listener(const BenchmarkConfig& config,
                                             BenchmarkControl& control) {
    int listen_fd = tcp_listen(config.health_check_port);
    if (listen_fd < 0) {
        std::cerr << "[HealthCheck] 无法在端口 " << config.health_check_port
                  << " 上监听" << std::endl;
        return;
    }

    // 设为非阻塞模式（使 accept 在无连接时立即返回 EAGAIN）
    int flags = ::fcntl(listen_fd, F_GETFL, 0);
    ::fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    std::cout << "[HealthCheck] 健康检查监听器已启动 (端口 "
              << config.health_check_port << ")" << std::endl;

    while (!control.health_check_shutdown.load(std::memory_order_relaxed)) {
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd,
                                 (struct sockaddr*)&client_addr,
                                 &addr_len);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            break;
        }

        // 读取请求（丢弃内容，仅用于触发一次完整 HTTP 交互）
        char buf[256];
        ::read(client_fd, buf, sizeof(buf) - 1);

        // 响应 HTTP 200 OK
        const char* response =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 2\r\n"
            "Connection: close\r\n"
            "\r\n"
            "OK";
        ::write(client_fd, response, std::strlen(response));

        ::close(client_fd);
    }

    ::close(listen_fd);
    std::cout << "[HealthCheck] 健康检查监听器已退出" << std::endl;
}

/**
 * @brief 压测模式主逻辑
 *
 * 自包含压测方案：
 * 1. 启动健康检查监听线程（模拟后端 /health 端点）
 * 2. 启动持久化后端线程（自动重连 + 重新注册路由）
 * 3. 等待后端就绪后，启动多线程客户端并发发送 HTTP 请求
 * 4. 等待指定时间后，通知后端线程退出
 * 5. 停止健康检查监听器
 * 6. 统计 QPS
 */
static int run_benchmark_mode(const BenchmarkConfig& config) {
    std::cout << "========================================" << std::endl;
    std::cout << "  MarisaProxy QPS 压测" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "  网关地址:       " << config.gateway_host << std::endl;
    std::cout << "  客户端端口:     " << config.gateway_client_port << std::endl;
    std::cout << "  后端端口:       " << config.gateway_backend_port << std::endl;
    std::cout << "  注册前缀:       " << config.register_prefix << std::endl;
    std::cout << "  请求方式:       " << config.http_method
              << " " << config.request_path << std::endl;
    std::cout << "  Host 头:        " << config.request_host << std::endl;
    std::cout << "  客户端线程数:   " << config.thread_count << std::endl;
    std::cout << "  持续时间:       " << config.duration_seconds << " 秒" << std::endl;
    std::cout << "========================================" << std::endl;

    // ---- 共享控制标志和结果 ----
    BenchmarkControl control;
    BenchmarkResult  result;

    // ---- 启动健康检查监听线程 ----
    std::thread health_check_thread(benchmark_health_check_listener,
                                     std::ref(config),
                                     std::ref(control));

    // ---- 启动持久化后端线程 ----
    std::thread backend_thread(benchmark_backend_worker,
                                std::ref(config),
                                std::ref(result),
                                std::ref(control));

    // 等待后端首次注册完成（最多 3 秒）
    for (int wait_loop = 0; wait_loop < 30; ++wait_loop) {
        if (control.backend_ready.load(std::memory_order_acquire)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!control.backend_ready.load()) {
        std::cerr << "[Bench] 后端注册超时，请确保网关已启动" << std::endl;
        control.backend_shutdown.store(true, std::memory_order_relaxed);
        if (backend_thread.joinable()) {
            backend_thread.join();
        }
        return 1;
    }

    std::cout << "[Bench] 后端已就绪，启动压测..." << std::endl;

    // 等待一下确保网关已完全处理完 REGISTER
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // ---- 启动压测客户端线程 ----
    std::vector<std::thread> client_threads;
    client_threads.reserve(config.thread_count);
    for (int thread_index = 0; thread_index < config.thread_count; ++thread_index) {
        client_threads.emplace_back(benchmark_client_worker,
                                     std::ref(config),
                                     std::ref(result),
                                     std::ref(control));
    }

    // ---- 等待所有客户端线程完成 ----
    for (auto& thread : client_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // ---- 通知后端线程退出 ----
    control.backend_shutdown.store(true, std::memory_order_relaxed);
    if (backend_thread.joinable()) {
        backend_thread.join();
    }

    // ---- 停止健康检查监听器 ----
    control.health_check_shutdown.store(true, std::memory_order_relaxed);
    if (health_check_thread.joinable()) {
        health_check_thread.join();
    }

    // ---- 计算并输出结果 ----
    uint64_t total_success   = result.success_count.load();
    uint64_t total_errors    = result.error_count.load();
    uint64_t total_requests  = total_success + total_errors;
    double   qps_raw         = static_cast<double>(total_success)
                               / static_cast<double>(config.duration_seconds);

    std::cout << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "  压测结果" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "  总请求数:      " << total_requests << std::endl;
    std::cout << "  成功:          " << total_success << std::endl;
    std::cout << "  失败:          " << total_errors << std::endl;
    std::cout << "  成功率:        " << std::fixed << std::setprecision(2)
              << (total_requests > 0
                  ? (100.0 * total_success / total_requests) : 0.0)
              << "%" << std::endl;
    std::cout << "  ├─ 连接失败:    " << result.connect_failures.load() << std::endl;
    std::cout << "  ├─ 发送失败:    " << result.send_failures.load() << std::endl;
    std::cout << "  ├─ 接收失败:    " << result.recv_failures.load() << std::endl;
    std::cout << "  │  ├─ EOF:      " << result.recv_eof.load() << std::endl;
    std::cout << "  │  ├─ RST:      " << result.recv_econnreset.load() << std::endl;
    std::cout << "  │  ├─ 超时:     " << result.recv_etimedout.load() << std::endl;
    std::cout << "  │  └─ 其他:     " << result.recv_other.load() << std::endl;
    std::cout << "  └─ 响应不含200: " << result.response_failures.load() << std::endl;
    std::cout << "  后端重连次数:  " << result.backend_reconnects.load() << std::endl;
    std::cout << "  后端错误次数:  " << result.backend_errors.load() << std::endl;
    std::cout << "  QPS:           " << std::fixed << std::setprecision(2)
              << qps_raw << " req/s" << std::endl;
    std::cout << "========================================" << std::endl;

    return (total_errors == 0) ? 0 : 1;
}

// ==================== JSON 配置读取 ====================

/**
 * @brief 从 JSON 配置文件读取并执行
 */
static int run_from_config(const json& config) {
    std::string mode = config.value("mode", "");

    if (mode == "backend") {
        std::string route_prefix   = config.value("prefix", "/");
        int listen_port            = config.value("listen_port", 9090);
        std::string gateway_host   = config.value("gw_host", "127.0.0.1");
        int gateway_backend_port   = config.value("gw_backend_port", 9999);
        bool need_auth             = config.value("need_auth", false);
        bool need_rate_limit       = config.value("need_rate_limit", false);

        std::cout << "[Backend] 前缀: \"" << route_prefix << "\"" << std::endl;
        std::cout << "[Backend] 监听端口: " << listen_port << std::endl;
        std::cout << "[Backend] 鉴权: " << (need_auth ? "ON" : "OFF") << std::endl;
        std::cout << "[Backend] 限流: " << (need_rate_limit ? "ON" : "OFF") << std::endl;
        std::cout << "[Backend] 网关地址: " << gateway_host << std::endl;
        std::cout << "[Backend] 网关后端端口: " << gateway_backend_port << std::endl;

        return run_backend_mode(route_prefix,
                                listen_port,
                                gateway_host,
                                gateway_backend_port,
                                need_auth,
                                need_rate_limit);
    }

    if (mode == "client") {
        std::string http_method  = config.value("method", "GET");
        std::string request_path = config.value("path", "/");
        std::string request_host = config.value("host", "test.com");
        std::string request_body = config.value("body", "");
        std::string gateway_host = config.value("gw_host", "127.0.0.1");
        int gateway_client_port  = config.value("gw_client_port", 8888);

        std::cout << "[Client] 方法: " << http_method << std::endl;
        std::cout << "[Client] 路径: " << request_path << std::endl;
        std::cout << "[Client] Host: " << request_host << std::endl;
        if (!request_body.empty()) {
            std::cout << "[Client] Body: " << request_body << std::endl;
        }
        std::cout << "[Client] 网关地址: " << gateway_host << std::endl;
        std::cout << "[Client] 网关客户端端口: " << gateway_client_port << std::endl;

        return run_client_mode(http_method,
                               request_path,
                               request_host,
                               request_body,
                               gateway_host,
                               gateway_client_port);
    }

    if (mode == "benchmark") {
        BenchmarkConfig bench_config;
        bench_config.http_method         = config.value("method", "GET");
        bench_config.request_path        = config.value("path", "/");
        bench_config.request_host        = config.value("host", "test.com");
        bench_config.gateway_host        = config.value("gw_host", "127.0.0.1");
        bench_config.gateway_client_port = config.value("gw_client_port", 8888);
        bench_config.gateway_backend_port = config.value("gw_backend_port", 9999);
        bench_config.health_check_port   = config.value("health_check_port", 9090);
        bench_config.register_prefix     = config.value("prefix", "/");
        bench_config.need_auth           = config.value("need_auth", false);
        bench_config.need_rate_limit     = config.value("need_rate_limit", false);
        bench_config.thread_count        = config.value("threads", 10);
        bench_config.duration_seconds    = config.value("duration", 20);

        return run_benchmark_mode(bench_config);
    }

    std::cerr << "[ERR] 配置文件中 mode 无效: \"" << mode
              << "\"（请使用 \"backend\" 或 \"client\" 或 \"benchmark\"）" << std::endl;
    return 1;
}

// ==================== 入口 ====================

int main(int argc, char* argv[]) {
    std::string config_path;
    bool        benchmark_mode = false;
    BenchmarkConfig bench_config;

    // 解析命令行参数（支持 -c 配置文件和 --benchmark 直接压测两种方式）
    for (int arg_index = 1; arg_index < argc; ++arg_index) {
        std::string current_arg(argv[arg_index]);

        if (current_arg == "-h" || current_arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }

        if (current_arg == "--benchmark") {
            benchmark_mode = true;
            continue;
        }

        if ((current_arg == "-c" || current_arg == "--config") &&
            arg_index + 1 < argc) {
            config_path = argv[++arg_index];
            continue;
        }

        // --benchmark 模式的参数
        if (benchmark_mode) {
            if (current_arg == "--host" && arg_index + 1 < argc) {
                bench_config.gateway_host = argv[++arg_index];
                continue;
            }
            if (current_arg == "--port" && arg_index + 1 < argc) {
                bench_config.gateway_client_port = std::stoi(argv[++arg_index]);
                continue;
            }
            if (current_arg == "--backend-port" && arg_index + 1 < argc) {
                bench_config.gateway_backend_port = std::stoi(argv[++arg_index]);
                continue;
            }
            if (current_arg == "--prefix" && arg_index + 1 < argc) {
                bench_config.register_prefix = argv[++arg_index];
                continue;
            }
            if (current_arg == "--method" && arg_index + 1 < argc) {
                bench_config.http_method = argv[++arg_index];
                continue;
            }
            if (current_arg == "--path" && arg_index + 1 < argc) {
                bench_config.request_path = argv[++arg_index];
                continue;
            }
            if (current_arg == "--header-host" && arg_index + 1 < argc) {
                bench_config.request_host = argv[++arg_index];
                continue;
            }
            if (current_arg == "--threads" && arg_index + 1 < argc) {
                bench_config.thread_count = std::stoi(argv[++arg_index]);
                continue;
            }
            if (current_arg == "--duration" && arg_index + 1 < argc) {
                bench_config.duration_seconds = std::stoi(argv[++arg_index]);
                continue;
            }
            if (current_arg == "--health-check-port" && arg_index + 1 < argc) {
                bench_config.health_check_port = std::stoi(argv[++arg_index]);
                continue;
            }
            if (current_arg == "--auth" && arg_index + 1 < argc) {
                bench_config.need_auth = (std::string(argv[++arg_index]) == "1" ||
                                          std::string(argv[arg_index]) == "true");
                continue;
            }
            if (current_arg == "--rate-limit" && arg_index + 1 < argc) {
                bench_config.need_rate_limit = (std::string(argv[++arg_index]) == "1" ||
                                                 std::string(argv[arg_index]) == "true");
                continue;
            }
        }

        std::cerr << "[ERR] 未知参数: " << current_arg << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    // 优先处理 --benchmark 直接压测模式
    if (benchmark_mode) {
        // 如果未指定 --path，默认使用注册前缀
        if (bench_config.request_path.empty()) {
            bench_config.request_path = bench_config.register_prefix;
        }
        return run_benchmark_mode(bench_config);
    }

    // 否则走配置文件模式
    if (config_path.empty()) {
        std::cerr << "[ERR] 请指定配置文件路径（使用 -c 选项）"
                  << "或使用 --benchmark 直接运行压测" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    // 读取 JSON 配置文件
    std::ifstream config_file(config_path);
    if (!config_file.is_open()) {
        std::cerr << "[ERR] 无法打开配置文件: " << config_path << std::endl;
        return 1;
    }

    json config_data;
    try {
        config_file >> config_data;
    } catch (const json::parse_error& parse_error) {
        std::cerr << "[ERR] JSON 解析失败: " << parse_error.what() << std::endl;
        return 1;
    }

    return run_from_config(config_data);
}
