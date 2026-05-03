#include <csignal>
#include <cstring>
#include <iostream>

#include "gateway.h"
#include "mylog.h"

// 全局网关指针，用于信号处理
static Gateway* g_gateway = nullptr;

/**
 * @brief 信号处理函数
 *
 * 捕获 SIGINT (Ctrl+C) 和 SIGTERM 信号，优雅关闭网关。
 */
static void signal_handler(int sig) {
    LOG_INFO("接收到信号 %d (%s)，正在关闭...", sig, strsignal(sig));

    if (g_gateway) {
        g_gateway->stop();
    }
}

/**
 * @brief 打印用法信息
 */
static void print_usage(const char* prog) {
    std::cout << "用法: " << prog
              << " [选项]" << std::endl;
    std::cout << "选项:" << std::endl;
    std::cout << "  -c, --client-port PORT   客户端监听端口 (默认: 8888)" << std::endl;
    std::cout << "  -b, --backend-port PORT  后端服务监听端口 (默认: 9999)" << std::endl;
    std::cout << "  -s, --ring-size SIZE     io_uring 队列深度 (默认: 1024)" << std::endl;
    std::cout << "  -l, --log-level LEVEL    日志级别 (0=DEBUG,1=INFO,2=WARN,3=ERROR,4=FATAL, 默认: 1)" << std::endl;
    std::cout << "  -f, --log-file PATH      日志文件路径 (默认: ./log/gateway.log)" << std::endl;
    std::cout << "  -h, --help               显示此帮助信息" << std::endl;
    std::cout << std::endl;
    std::cout << "示例:" << std::endl;
    std::cout << "  " << prog << std::endl;
    std::cout << "  " << prog << " -c 8080 -b 9090" << std::endl;
    std::cout << "  " << prog << " --client-port 8888 --backend-port 9999 --log-level 0"
              << std::endl;
}

int main(int argc, char* argv[]) {
    // 默认参数
    int client_port = 8888;
    int backend_port = 9999;
    int ring_size = 1024;
    int log_level = LOG_INFO;
    std::string log_file = "./log/gateway.log";

    // 解析命令行参数
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);

        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if ((arg == "-c" || arg == "--client-port") && i + 1 < argc) {
            client_port = std::stoi(argv[++i]);
        } else if ((arg == "-b" || arg == "--backend-port") && i + 1 < argc) {
            backend_port = std::stoi(argv[++i]);
        } else if ((arg == "-s" || arg == "--ring-size") && i + 1 < argc) {
            ring_size = std::stoi(argv[++i]);
        } else if ((arg == "-l" || arg == "--log-level") && i + 1 < argc) {
            log_level = std::stoi(argv[++i]);
        } else if ((arg == "-f" || arg == "--log-file") && i + 1 < argc) {
            log_file = argv[++i];
        } else {
            std::cerr << "[Main] 未知参数: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }

    // 初始化日志系统（bufSize=4：前端缓存4条日志即触发刷盘，保证日志及时输出）
    mylog::init(log_file, 4, static_cast<LogLevel>(log_level));

    // 注册信号处理
    struct sigaction sa{};
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    // 忽略 SIGPIPE（防止 write 到已关闭连接时进程退出）
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("========================================");
    LOG_INFO("  io_uring Gateway v1.0.0");
    LOG_INFO("========================================");
    LOG_INFO("客户端端口: %d", client_port);
    LOG_INFO("后端端口:   %d", backend_port);
    LOG_INFO("Ring 大小:  %d", ring_size);
    LOG_INFO("日志文件:   %s", log_file.c_str());
    LOG_INFO("日志级别:   %s", g_log_level_names[log_level]);
    LOG_INFO("========================================");

    try {
        Gateway gateway(client_port, backend_port, ring_size);
        g_gateway = &gateway;
        gateway.run();
    } catch (const std::exception& e) {
        LOG_ERROR("致命错误: %s", e.what());
        std::cerr << "[Main] 致命错误: " << e.what() << std::endl;
        mylog::destroy();
        return 1;
    }

    LOG_INFO("网关已退出");

    // 销毁日志模块，确保所有日志刷出
    mylog::destroy();
    return 0;
}
