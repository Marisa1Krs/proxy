#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

/**
 * @brief 基于 io_uring 网关的 CLI 测试客户端
 *
 * 两种工作模式：
 *   --backend MODE:  模拟后端服务，连接 9999 端口注册路由，接收转发请求并响应
 *   --client  MODE:  模拟客户端，连接 8888 端口发送 HTTP 请求并打印响应
 *
 * 使用方式：
 *   1. 启动网关: ./Proxy
 *   2. 注册后端: ./cli_client --backend --prefix /api --listen 9090
 *   3. 发送请求: ./cli_client --client --path /api/hello
 *
 * 后端模式流程：
 *   ① 创建监听 socket 等待网关转发请求（如 listen 9090）
 *   ② 连接网关 9999 端口，注册路径前缀
 *   ③ 收到转发请求后解析并返回 HTTP 响应
 *
 * 客户端模式流程：
 *   ① 连接网关 8888 端口
 *   ② 发送 HTTP 请求
 *   ③ 打印响应内容
 */

// ==================== 配置默认值 ====================
static constexpr int DEFAULT_GATEWAY_CLIENT_PORT = 8888;   // 网关客户端端口
static constexpr int DEFAULT_GATEWAY_BACKEND_PORT = 9999;  // 网关后端注册端口
static constexpr size_t BUFFER_SIZE = 4096;                // 缓冲区大小

// ==================== 工具函数 ====================

/**
 * @brief 创建 TCP 连接
 * @param host  目标主机
 * @param port  目标端口
 * @return  socket fd，失败返回 -1
 */
static int tcp_connect(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "[ERR] socket 创建失败: " << strerror(errno) << std::endl;
        return -1;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "[ERR] 无效地址: " << host << std::endl;
        ::close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[ERR] 连接 " << host << ":" << port
                  << " 失败: " << strerror(errno) << std::endl;
        ::close(fd);
        return -1;
    }

    return fd;
}

/**
 * @brief 创建 TCP 监听 socket
 * @param port  监听端口
 * @return  listen fd，失败返回 -1
 */
static int tcp_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "[ERR] socket 创建失败: " << strerror(errno) << std::endl;
        return -1;
    }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[ERR] bind 端口 " << port << " 失败: " << strerror(errno) << std::endl;
        ::close(fd);
        return -1;
    }

    if (listen(fd, 10) < 0) {
        std::cerr << "[ERR] listen 失败: " << strerror(errno) << std::endl;
        ::close(fd);
        return -1;
    }

    return fd;
}

/**
 * @brief 接收所有数据直到连接关闭
 * @param fd     socket fd
 * @param buf    接收缓冲区
 * @param size   缓冲区大小
 * @return       接收到的字节数，失败返回 -1
 */
static int recv_all(int fd, char* buf, size_t size) {
    size_t total = 0;
    while (total < size - 1) {
        ssize_t n = read(fd, buf + total, size - 1 - total);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[ERR] read 失败: " << strerror(errno) << std::endl;
            return -1;
        }
        if (n == 0) break;  // 连接关闭
        total += n;
    }
    buf[total] = '\0';
    return static_cast<int>(total);
}

/**
 * @brief 发送所有数据
 * @param fd    socket fd
 * @param data  数据指针
 * @param len   数据长度
 * @return      成功返回 true
 */
static bool send_all(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, data + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[ERR] write 失败: " << strerror(errno) << std::endl;
            return false;
        }
        sent += n;
    }
    return true;
}

// ==================== 用法打印 ====================

static void print_usage(const char* prog) {
    std::cout << "用法: " << prog << " <模式> [选项]" << std::endl;
    std::cout << std::endl;
    std::cout << "模式:" << std::endl;
    std::cout << "  --backend  以后端模式运行（模拟后端服务）" << std::endl;
    std::cout << "  --client   以客户端模式运行（模拟客户端请求）" << std::endl;
    std::cout << std::endl;
    std::cout << "后端模式选项:" << std::endl;
    std::cout << "  --prefix PREFIX     注册路径前缀 (默认: /)" << std::endl;
    std::cout << "  --listen PORT       本地监听端口，接收转发请求 (默认: 9090)" << std::endl;
    std::cout << "  --response FILE     响应文件路径 (默认: 返回 200 OK)" << std::endl;
    std::cout << "  --gw-port PORT      网关后端端口 (默认: 9999)" << std::endl;
    std::cout << std::endl;
    std::cout << "客户端模式选项:" << std::endl;
    std::cout << "  --path PATH         请求路径 (默认: /)" << std::endl;
    std::cout << "  --method METHOD     HTTP 方法 (默认: GET)" << std::endl;
    std::cout << "  --body TEXT         请求体 (可选)" << std::endl;
    std::cout << "  --host HOST         请求 Host (默认: test.com)" << std::endl;
    std::cout << "  --gw-port PORT      网关客户端端口 (默认: 8888)" << std::endl;
    std::cout << std::endl;
    std::cout << "示例:" << std::endl;
    std::cout << "  # 终端1: 启动网关" << std::endl;
    std::cout << "  " << prog << " --backend --prefix /api --listen 9090" << std::endl;
    std::cout << std::endl;
    std::cout << "  # 终端2: 注册后端并等待请求" << std::endl;
    std::cout << "  ./build/cli_client --backend --prefix /api --listen 9090" << std::endl;
    std::cout << std::endl;
    std::cout << "  # 终端3: 发送客户端请求" << std::endl;
    std::cout << "  ./build/cli_client --client --path /api/hello" << std::endl;
}

// ==================== 后端模式 ====================

/**
 * @brief 后端模式主逻辑
 *
 * 1. 创建本地监听 socket（等待网关转发请求）
 * 2. 连接网关 9999 端口，注册路径前缀
 * 3. 接受网关转发的 HTTP 请求
 * 4. 解析请求并返回响应
 */
static int run_backend_mode(const std::string& prefix, int listen_port,
                            int gw_backend_port) {
    // 1. 创建本地监听 socket
    int listen_fd = tcp_listen(listen_port);
    if (listen_fd < 0) return 1;

    std::cout << "[Backend] 本地监听端口 " << listen_port
              << " (fd=" << listen_fd << ")" << std::endl;

    // 2. 连接网关 9999 端口
    int gw_fd = tcp_connect("127.0.0.1", gw_backend_port);
    if (gw_fd < 0) {
        ::close(listen_fd);
        return 1;
    }
    std::cout << "[Backend] 已连接网关 127.0.0.1:" << gw_backend_port
              << " (fd=" << gw_fd << ")" << std::endl;

    // 3. 发送注册消息
    std::string reg_msg = "REGISTER " + prefix + "\n";
    if (!send_all(gw_fd, reg_msg.data(), reg_msg.size())) {
        ::close(gw_fd);
        ::close(listen_fd);
        return 1;
    }
    std::cout << "[Backend] 已注册前缀: \"" << prefix << "\"" << std::endl;

    // 4. 等待网关转发请求
    std::cout << "[Backend] 等待转发请求..." << std::endl;

    struct sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    int client_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &addr_len);
    if (client_fd < 0) {
        std::cerr << "[ERR] accept 失败: " << strerror(errno) << std::endl;
        ::close(gw_fd);
        ::close(listen_fd);
        return 1;
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    std::cout << "[Backend] 收到转发请求来自 " << client_ip
              << ":" << ntohs(client_addr.sin_port)
              << " (fd=" << client_fd << ")" << std::endl;

    // 5. 读取转发过来的 HTTP 请求
    char buf[BUFFER_SIZE] = {0};
    int bytes_read = recv_all(client_fd, buf, BUFFER_SIZE);
    if (bytes_read < 0) {
        ::close(client_fd);
        ::close(gw_fd);
        ::close(listen_fd);
        return 1;
    }

    std::cout << "[Backend] ==== 收到的请求 =====" << std::endl;
    std::cout << buf << std::endl;
    std::cout << "[Backend] ====================" << std::endl;

    // 6. 构建 HTTP 响应
    std::string response_body =
        std::string("{\"status\":\"ok\",\"message\":\"Hello from backend!\",\"prefix\":\"") +
        prefix + "\"}";
    int body_len = response_body.size();

    std::string http_response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: " +
        std::to_string(body_len) +
        "\r\n"
        "Connection: close\r\n"
        "\r\n" +
        response_body;

    // 7. 发送响应
    if (send_all(client_fd, http_response.data(), http_response.size())) {
        std::cout << "[Backend] 已发送响应 (" << http_response.size()
                  << " 字节)" << std::endl;
    }

    // 8. 清理
    ::close(client_fd);
    ::close(gw_fd);
    ::close(listen_fd);

    std::cout << "[Backend] 完成" << std::endl;
    return 0;
}

// ==================== 客户端模式 ====================

/**
 * @brief 客户端模式主逻辑
 *
 * 1. 连接网关 8888 端口
 * 2. 构建并发送 HTTP 请求
 * 3. 接收并打印响应
 */
static int run_client_mode(const std::string& method, const std::string& path,
                           const std::string& host, const std::string& body,
                           int gw_client_port) {
    // 1. 连接网关 8888 端口
    int gw_fd = tcp_connect("127.0.0.1", gw_client_port);
    if (gw_fd < 0) return 1;

    std::cout << "[Client] 已连接网关 127.0.0.1:" << gw_client_port
              << " (fd=" << gw_fd << ")" << std::endl;

    // 2. 构建 HTTP 请求
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
            "\r\n" +
            body;
    }

    // 3. 发送请求
    std::cout << "[Client] ==== 发送请求 =====" << std::endl;
    std::cout << http_request << std::endl;
    std::cout << "[Client] ===================" << std::endl;

    if (!send_all(gw_fd, http_request.data(), http_request.size())) {
        ::close(gw_fd);
        return 1;
    }
    std::cout << "[Client] 已发送 " << http_request.size() << " 字节" << std::endl;

    // 4. 读取响应
    char buf[BUFFER_SIZE] = {0};
    int bytes_read = recv_all(gw_fd, buf, BUFFER_SIZE);
    if (bytes_read < 0) {
        ::close(gw_fd);
        return 1;
    }

    // 5. 打印响应
    std::cout << "[Client] ==== 收到响应 =====" << std::endl;
    std::cout << buf << std::endl;
    std::cout << "[Client] ===================" << std::endl;
    std::cout << "[Client] 收到 " << bytes_read << " 字节" << std::endl;

    // 6. 清理
    ::close(gw_fd);
    return 0;
}

// ==================== 入口 ====================

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    std::string mode = argv[1];

    // ---- 后端模式 ----
    if (mode == "--backend") {
        std::string prefix = "/";
        int listen_port = 9090;
        int gw_backend_port = DEFAULT_GATEWAY_BACKEND_PORT;

        for (int i = 2; i < argc; ++i) {
            std::string arg(argv[i]);
            if ((arg == "--prefix") && i + 1 < argc) {
                prefix = argv[++i];
            } else if ((arg == "--listen") && i + 1 < argc) {
                listen_port = std::stoi(argv[++i]);
            } else if ((arg == "--gw-port") && i + 1 < argc) {
                gw_backend_port = std::stoi(argv[++i]);
            } else {
                std::cerr << "[ERR] 未知参数: " << arg << std::endl;
                print_usage(argv[0]);
                return 1;
            }
        }

        std::cout << "[Backend] 前缀: \"" << prefix << "\"" << std::endl;
        std::cout << "[Backend] 监听端口: " << listen_port << std::endl;
        std::cout << "[Backend] 网关后端端口: " << gw_backend_port << std::endl;

        return run_backend_mode(prefix, listen_port, gw_backend_port);
    }

    // ---- 客户端模式 ----
    if (mode == "--client") {
        std::string method = "GET";
        std::string path = "/";
        std::string host = "test.com";
        std::string body;
        int gw_client_port = DEFAULT_GATEWAY_CLIENT_PORT;

        for (int i = 2; i < argc; ++i) {
            std::string arg(argv[i]);
            if ((arg == "--method") && i + 1 < argc) {
                method = argv[++i];
            } else if ((arg == "--path") && i + 1 < argc) {
                path = argv[++i];
            } else if ((arg == "--host") && i + 1 < argc) {
                host = argv[++i];
            } else if ((arg == "--body") && i + 1 < argc) {
                body = argv[++i];
            } else if ((arg == "--gw-port") && i + 1 < argc) {
                gw_client_port = std::stoi(argv[++i]);
            } else {
                std::cerr << "[ERR] 未知参数: " << arg << std::endl;
                print_usage(argv[0]);
                return 1;
            }
        }

        std::cout << "[Client] 方法: " << method << std::endl;
        std::cout << "[Client] 路径: " << path << std::endl;
        std::cout << "[Client] Host: " << host << std::endl;
        if (!body.empty()) {
            std::cout << "[Client] Body: " << body << std::endl;
        }
        std::cout << "[Client] 网关客户端端口: " << gw_client_port << std::endl;

        return run_client_mode(method, path, host, body, gw_client_port);
    }

    // ---- 未知模式 ----
    std::cerr << "[ERR] 未知模式: " << mode << std::endl;
    print_usage(argv[0]);
    return 1;
}
