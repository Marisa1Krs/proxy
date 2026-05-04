#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <vector>

class Worker;

/**
 * @brief 网关编排器 —— 管理多个 SO_REUSEPORT Worker 线程
 *
 * 核心架构（多线程 + SO_REUSEPORT）：
 * @code
 *   Gateway（编排器）
 *     ├── Worker 0 Thread ── io_uring₀ ── listen_fd 8888 (SO_REUSEPORT)
 *     │                                   └── listen_fd 9999 (SO_REUSEPORT)
 *     ├── Worker 1 Thread ── io_uring₁ ── listen_fd 8888 (SO_REUSEPORT)
 *     │                                   └── listen_fd 9999 (SO_REUSEPORT)
 *     └── ...
 * @endcode
 *
 * 每个 Worker 拥有完全独立的：
 *   - io_uring 实例
 *   - 监听套接字（SO_REUSEPORT，内核自动负载均衡）
 *   - Provided Buffers 池（64 × 4KB）
 *   - 路由表（TrieRouter）
 *   - 连接上下文映射
 *
 * 后端需在每个 Worker 上分别注册路由：
 *   连接到 9999 端口时，内核会分配到不同 Worker，
 *   因此需要多次连接以覆盖所有 Worker。
 */
class Gateway {
public:
    // ==================== 初始化状态码 ====================
    enum InitCode : int {
        INIT_OK         = 0,   ///< 初始化成功
        INIT_WORKER_ERR = -1,  ///< Worker 初始化失败
    };

    /**
     * @brief 构造网关编排器
     * @param client_port         客户端监听端口（默认 8888）
     * @param backend_port        后端监听端口（默认 9999）
     * @param ring_size           io_uring 队列深度（默认 1024）
     * @param worker_count        Worker 线程数量（默认 1）
     * @param cpu_affinity_masks  每个 Worker 的 CPU 亲和性掩码（可选）
     */
    explicit Gateway(int client_port = 8888,
                     int backend_port = 9999,
                     int ring_size = 1024,
                     int worker_count = 1,
                     const std::vector<std::string>& cpu_affinity_masks = {});
    ~Gateway();

    Gateway(const Gateway&) = delete;
    Gateway& operator=(const Gateway&) = delete;

    /// 获取初始化状态码
    InitCode init_code() const { return init_code_; }

    /// 启动所有 Worker 线程并等待所有线程结束
    void run();

    /// 请求所有 Worker 停止事件循环
    void stop();

private:
    InitCode init_code_;

    int client_port_;          ///< 客户端监听端口
    int backend_port_;         ///< 后端监听端口
    int ring_size_;            ///< io_uring 队列深度
    int worker_count_;         ///< Worker 线程数量

    /// 每个 Worker 的 CPU 亲和性掩码字符串列表
    std::vector<std::string> cpu_affinity_masks_;

    /// Worker 对象指针列表（拥有权）
    std::vector<Worker*> workers_;

    /// Worker 线程句柄列表
    std::vector<std::thread*> threads_;
};
