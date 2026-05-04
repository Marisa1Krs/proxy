#include <csignal>
#include <cstring>
#include <iostream>
#include <fstream>

#include "json/json.hpp"
#include "gateway.h"
#include "mylog.h"

using json = nlohmann::json;

/**
 * @brief 全局网关指针，用于信号处理
 *
 * 信号处理函数需要访问 Gateway 实例来调用 stop()。
 * 由于信号处理函数是 C 风格回调，无法通过参数传递，
 * 因此使用全局指针。
 */
static Gateway* g_gateway = nullptr;

/**
 * @brief SIGUSR1 处理函数（空函数）
 *
 * 用于唤醒阻塞在 io_uring_wait_cqe 中的 Worker 线程。
 * Gateway::stop() 调用 pthread_kill(worker_thread, SIGUSR1) 触发此处理函数，
 * 使 io_uring_wait_cqe 返回 -EINTR，Worker 线程随后检查 running_ 标志并退出。
 */
static void sigusr1_handler(int /*signal_number*/) {
    // 无需实际操作，仅用于中断 io_uring_wait_cqe 系统调用
}

/**
 * @brief 信号处理函数
 *
 * 捕获 SIGINT (Ctrl+C) 和 SIGTERM 信号，优雅关闭网关。
 */
static void signal_handler(int signal_number) {
    LOG_INFO("接收到信号 %d (%s)，正在关闭...",
             signal_number, strsignal(signal_number));

    if (g_gateway != nullptr) {
        g_gateway->stop();
    }
}

/**
 * @brief 打印用法信息
 */
static void print_usage(const char* program_name) {
    std::cout << "用法: " << program_name
              << " [-c 配置文件路径]" << std::endl;
    std::cout << "  -c, --config PATH   配置文件路径"
              << " (默认: ./config/config.json)" << std::endl;
    std::cout << "  -h, --help          显示此帮助信息" << std::endl;
}

int main(int argc, char* argv[]) {
    // 默认配置文件路径
    std::string config_path = "./config/config.json";

    // 解析命令行参数（只支持 -c/--config 和 -h/--help）
    for (int arg_index = 1; arg_index < argc; ++arg_index) {
        std::string current_argument(argv[arg_index]);

        if (current_argument == "-h" || current_argument == "--help") {
            print_usage(argv[0]);
            return 0;
        }

        if ((current_argument == "-c" || current_argument == "--config") &&
            arg_index + 1 < argc) {
            config_path = argv[++arg_index];
            continue;
        }

        std::cerr << "[Main] 未知参数: " << current_argument << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    // 读取 JSON 配置文件
    std::ifstream config_file(config_path);
    if (!config_file.is_open()) {
        std::cerr << "[Main] 无法打开配置文件: " << config_path << std::endl;
        return 1;
    }

    json config_data;
    try {
        config_file >> config_data;
    } catch (const json::parse_error& parse_error) {
        std::cerr << "[Main] JSON 解析失败: " << parse_error.what() << std::endl;
        return 1;
    }

    // 提取配置项（含默认值）
    int client_port       = config_data.value("client_port", 8888);
    int backend_port      = config_data.value("backend_port", 9999);
    int ring_size         = config_data.value("ring_size", 1024);
    int worker_count      = config_data.value("worker_processes", 1);
    int log_level         = config_data.value("log_level", 1);
    std::string log_file  = config_data.value("log_file", "./log/gateway.log");

    // 解析 CPU 亲和性掩码配置
    std::vector<std::string> cpu_affinity_masks;
    if (config_data.contains("worker_cpu_affinity")) {
        auto& affinity_array = config_data["worker_cpu_affinity"];
        for (const auto& affinity_mask : affinity_array) {
            cpu_affinity_masks.push_back(
                affinity_mask.get<std::string>());
        }
    }

    // 初始化日志系统
    mylog::init(log_file, 4, static_cast<LogLevel>(log_level));

    // 注册信号处理
    struct sigaction signal_action{};
    std::memset(&signal_action, 0, sizeof(signal_action));

    // SIGUSR1：用于唤醒 Worker 线程（空处理函数，仅中断 io_uring_wait_cqe）
    signal_action.sa_handler = sigusr1_handler;
    sigaction(SIGUSR1, &signal_action, nullptr);

    // SIGINT/SIGTERM：优雅关闭
    signal_action.sa_handler = signal_handler;
    sigaction(SIGINT, &signal_action, nullptr);
    sigaction(SIGTERM, &signal_action, nullptr);

    // 忽略 SIGPIPE（防止 write 到已关闭连接时进程退出）
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("========================================");
    LOG_INFO("  io_uring Gateway v2.0.0 (多 Worker)");
    LOG_INFO("========================================");
    LOG_INFO("配置文件:   %s", config_path.c_str());
    LOG_INFO("客户端端口: %d", client_port);
    LOG_INFO("后端端口:   %d", backend_port);
    LOG_INFO("Ring 大小:  %d", ring_size);
    LOG_INFO("Worker 数:  %d", worker_count);

    if (!cpu_affinity_masks.empty()) {
        std::string affinity_string;
        for (size_t mask_index = 0;
             mask_index < cpu_affinity_masks.size();
             ++mask_index) {
            if (mask_index > 0) {
                affinity_string += ", ";
            }
            affinity_string += cpu_affinity_masks[mask_index];
        }
        LOG_INFO("CPU 亲和:  [%s]", affinity_string.c_str());
    }

    LOG_INFO("日志文件:   %s", log_file.c_str());
    LOG_INFO("日志级别:   %s", g_log_level_names[log_level]);
    LOG_INFO("========================================");

    // 将 Gateway 放在独立作用域，确保其在 mylog::destroy() 之前析构
    {
        Gateway gateway(client_port,
                        backend_port,
                        ring_size,
                        worker_count,
                        cpu_affinity_masks);

        if (gateway.init_code() != Gateway::INIT_OK) {
            LOG_ERROR("网关初始化失败 (错误码: %d)", gateway.init_code());
            std::cerr << "[Main] 网关初始化失败 (错误码: "
                      << gateway.init_code() << ")" << std::endl;
            mylog::destroy();
            return 1;
        }

        g_gateway = &gateway;
        gateway.run();
    }

    LOG_INFO("网关已退出");

    // 销毁日志模块，确保所有日志刷出
    mylog::destroy();
    return 0;
}
