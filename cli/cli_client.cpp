#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "json/json.hpp"

using json = nlohmann::json;

/**
 * @brief 基于 io_uring 网关的 CLI 测试客户端
 *
 * 两种工作模式，通过 JSON 配置文件指定：
 *   backend 模式: 模拟后端服务，连接 9999 端口注册路由，接收转发请求并响应
 *   client  模式: 模拟客户端，连接 8888 端口发送 HTTP 请求并打印响应
 *
 * 使用方式：
 *   1. 启动网关: ./Proxy
 *   2. 注册后端: ./cli_client -c ./config/backend.json
 *   3. 发送请求: ./cli_client -c ./config/client.json
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
    std::cout << "用法: " << program_name << " [-c 配置文件路径]" << std::endl;
    std::cout << std::endl;
    std::cout << "选项:" << std::endl;
    std::cout << "  -c, --config PATH   配置文件路径" << std::endl;
    std::cout << "  -h, --help          显示此帮助信息" << std::endl;
    std::cout << std::endl;
    std::cout << "配置文件支持两种 mode:" << std::endl;
    std::cout << "  \"backend\"  以后端模式运行（模拟后端服务）" << std::endl;
    std::cout << "  \"client\"   以客户端模式运行（模拟客户端请求）" << std::endl;
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
    std::cout << "示例:" << std::endl;
    std::cout << "  " << program_name << " -c ./config/backend.json" << std::endl;
    std::cout << "  " << program_name << " -c ./config/client.json" << std::endl;
}

// ==================== 后端模式 ====================

/**
 * @brief 后端模式主逻辑
 *
 * 1. 创建本地监听 socket（等待网关转发请求）
 * 2. 连接网关后端端口，注册路径前缀
 * 3. 接受网关转发的 HTTP 请求
 * 4. 解析请求并返回响应
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

    // ---- 步骤 4: 等待网关转发请求 ----
    std::cout << "[Backend] 等待转发请求..." << std::endl;

    struct sockaddr_in client_address{};
    socklen_t address_length = sizeof(client_address);
    int client_fd = accept(
        listen_fd,
        (struct sockaddr*)&client_address,
        &address_length);

    if (client_fd < 0) {
        std::cerr << "[ERR] accept 失败: " << strerror(errno) << std::endl;
        ::close(gateway_fd);
        ::close(listen_fd);
        return 1;
    }

    char client_ip_string[INET_ADDRSTRLEN];
    inet_ntop(AF_INET,
              &client_address.sin_addr,
              client_ip_string,
              sizeof(client_ip_string));

    std::cout << "[Backend] 收到转发请求来自 "
              << client_ip_string
              << ":" << ntohs(client_address.sin_port)
              << " (fd=" << client_fd << ")" << std::endl;

    // ---- 步骤 5: 读取转发过来的 HTTP 请求 ----
    char read_buffer[BUFFER_SIZE] = {0};
    int bytes_received = recv_all(client_fd, read_buffer, BUFFER_SIZE);
    if (bytes_received < 0) {
        ::close(client_fd);
        ::close(gateway_fd);
        ::close(listen_fd);
        return 1;
    }

    std::cout << "[Backend] ==== 收到的请求 =====" << std::endl;
    std::cout << read_buffer << std::endl;
    std::cout << "[Backend] ====================" << std::endl;

    // ---- 步骤 6: 构建 HTTP 响应 ----
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

    // ---- 步骤 7: 发送响应 ----
    bool response_sent = send_all(
        client_fd,
        http_response.data(),
        http_response.size());

    if (response_sent) {
        std::cout << "[Backend] 已发送响应 ("
                  << http_response.size() << " 字节)" << std::endl;
    }

    // ---- 步骤 8: 清理 ----
    ::close(client_fd);
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

    std::cerr << "[ERR] 配置文件中 mode 无效: \"" << mode
              << "\"（请使用 \"backend\" 或 \"client\"）" << std::endl;
    return 1;
}

// ==================== 入口 ====================

int main(int argc, char* argv[]) {
    std::string config_path;

    // 只解析 -c/--config 和 -h/--help
    for (int arg_index = 1; arg_index < argc; ++arg_index) {
        std::string current_arg(argv[arg_index]);

        if (current_arg == "-h" || current_arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }

        if ((current_arg == "-c" || current_arg == "--config") &&
            arg_index + 1 < argc) {
            config_path = argv[++arg_index];
            continue;
        }

        std::cerr << "[ERR] 未知参数: " << current_arg << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (config_path.empty()) {
        std::cerr << "[ERR] 请指定配置文件路径（使用 -c 选项）" << std::endl;
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
