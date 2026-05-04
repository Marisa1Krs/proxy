#pragma once

#include <liburing.h>
#include <atomic>
#include <cstring>
#include <pthread.h>
#include <string>
#include <unordered_map>

#include "auth.h"
#include "rate_limiter.h"
#include "route_manager.h"

/**
 * @brief 工作线程 —— 独立的事件循环（SO_REUSEPORT + 独立 io_uring）
 *
 * 每个 Worker 拥有自己完全独立的：
 *   - io_uring 实例
 *   - SO_REUSEPORT 监听套接字（同一个端口可被多个 Worker 同时监听）
 *   - Provided Buffers 池（64 个 4KB 缓冲区）
 *   - 路由表（RouteManager + COW RouteTable，只读访问）
 *   - 连接上下文映射表
 *
 * 核心架构：状态机 + IOSQE_BUFFER_SELECT
 *
 * 状态流转（客户端请求）：
 *   ACCEPTING → READING_CLIENT → WRITING_BACKEND
 *                               → READING_BACKEND → WRITING_CLIENT → 关闭
 *
 * 状态流转（后端注册）：
 *   ACCEPTING → BACKEND_REGISTER → BACKEND_IDLE
 *
 * 健康检查（每 5 秒）：
 *   HEALTH_CHECK_TIMER → 遍历 BACKEND_IDLE → WRITING_BACKEND
 *                      → READING_BACKEND → BACKEND_IDLE / 移除路由
 */
class Worker {
public:
    // ==================== 初始化状态码 ====================
    enum InitCode : int {
        INIT_OK           = 0,   ///< 初始化成功
        INIT_URING_ERR    = -1,  ///< io_uring 队列初始化失败
        INIT_CLIENT_ERR   = -2,  ///< 客户端监听端口绑定失败
        INIT_BACKEND_ERR  = -3,  ///< 后端监听端口绑定失败
    };

    /**
     * @brief 构造 Worker
     * @param client_port       客户端监听端口（默认 8888）
     * @param backend_port      后端监听端口（默认 9999）
     * @param ring_size         io_uring 队列深度（默认 1024）
     * @param worker_id         Worker 编号（日志用）
     * @param health_check_port 健康检查端口（默认 9090）
     */
    Worker(int client_port, int backend_port, int ring_size, int worker_id,
           int health_check_port = 9090);
    ~Worker();

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    /// 获取初始化状态码
    InitCode init_code() const { return init_code_; }

    /// 工作线程的主循环：初始化 → 提交 accept → 事件循环
    void worker_loop();

    /// 请求停止事件循环
    void stop() { running_ = false; }

    /// 检查 Worker 是否仍在运行
    bool is_running() const { return running_; }

    /// 获取线程 native handle（用于优雅退出时发送唤醒信号）
    pthread_t thread_handle() const { return thread_handle_; }

    /**
     * @brief 设置 CPU 亲和性掩码
     * @param mask 二进制掩码字符串，如 "0001" 表示绑定到 CPU 0
     *
     * 掩码格式：从低位到高位对应 CPU 0~N，'1' 表示绑定该 CPU。
     * 示例：0001 → CPU 0, 0010 → CPU 1, 0101 → CPU 0 + CPU 2
     */
    void set_cpu_affinity(const std::string& mask);

private:
    // ==================== 常量 ====================
    static constexpr int BUFFER_SIZE    = 4096;  ///< 单缓冲区大小
    static constexpr int NUM_BUFFERS    = 64;    ///< 缓冲区总数
    static constexpr int BUF_GROUP      = 1;     ///< 缓冲区组 ID
    static constexpr int MAX_RING_SIZE  = 4096;  ///< io_uring 最大队列深度
    static constexpr int BACKLOG        = 128;   ///< listen 积压数

    /// 健康检查请求字符串（GET /health，Connection: close 确保后端发完即关）
    static constexpr const char* HEALTH_CHECK_REQUEST =
        "GET /health HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Connection: close\r\n"
        "\r\n";

    /// 健康检查间隔（秒）
    static constexpr int HEALTH_CHECK_INTERVAL_SEC = 5;

    // ==================== 连接上下文 ====================
    /**
     * @brief 请求上下文（状态机）
     *
     * 每个客户端或后端连接对应一个 RequestContext。
     * 通过 state 字段驱动 I/O 操作的状态转换，
     * 所有状态转换完全由事件处理器触发。
     */
    struct RequestContext {
        /// 状态枚举
        enum State : int {
            ACCEPTING,           ///< 监听套接字：等待 accept 完成
            READING_CLIENT,      ///< 从客户端读取 HTTP 请求
            WRITING_BACKEND,     ///< 将客户端请求写入后端
            READING_BACKEND,     ///< 从后端读取 HTTP 响应
            WRITING_CLIENT,      ///< 将后端响应写回客户端
            BACKEND_REGISTER,    ///< 读取后端注册消息
            BACKEND_IDLE,        ///< 后端注册完毕，等待被分配请求
            HEALTH_CHECK_TIMER,  ///< 健康检查定时器已触发
        };

        State state;                ///< 当前状态
        int fd;                     ///< 当前 I/O 操作的 fd
        int client_fd;              ///< 客户端 fd（后端上下文中为 -1）
        int backend_fd;             ///< 后端 fd（客户端上下文中为 -1）
        int bid;                    ///< 当前使用的缓冲区 ID（回收用）
        char* buffer;               ///< 数据缓冲区指针
        int bytes_written;          ///< 已写入字节数（处理 partial write）
        int total_bytes;            ///< 需写入的总字节数
        std::string path;           ///< HTTP 请求路径（路由匹配用）
        std::string client_ip;      ///< 客户端 IP 地址（限流用）

        // ---- 后端保持连接（keep-alive）字段 ----
        std::string registered_prefix;   ///< 后端注册的路径前缀（如 "/"）
        bool registered_auth       = false; ///< 后端注册的鉴权标志
        bool registered_rate_limit = false; ///< 后端注册的限流标志

        RequestContext()
            : state(ACCEPTING)
            , fd(-1)
            , client_fd(-1)
            , backend_fd(-1)
            , bid(-1)
            , buffer(nullptr)
            , bytes_written(0)
            , total_bytes(0) {}
    };

    // ==================== 成员变量 ====================

    InitCode init_code_;

    // ---- 配置 ----
    int worker_id_;
    int client_port_;
    int backend_port_;
    int ring_size_;
    std::string cpu_affinity_mask_;

    // ---- io_uring ----
    io_uring ring_;

    // ---- 监听套接字（SO_REUSEPORT：多个 Worker 可绑定同一端口） ----
    int listen_fd_client_;
    int listen_fd_backend_;

    // ---- 预分配缓冲区池 ----
    char buffers_[NUM_BUFFERS][BUFFER_SIZE];
    bool buffer_free_[NUM_BUFFERS];

    // ---- COW 路由表管理器 + 限流 ----
    RouteManager route_manager_;
    RateLimiter rate_limiter_;

    // ---- accept 上下文（成员变量，不 heap 分配） ----
    RequestContext accept_client_ctx_;
    RequestContext accept_backend_ctx_;
// ---- 健康检查定时器上下文（成员变量） ----
RequestContext health_check_timer_ctx_;

// ---- 健康检查端口 ----
int health_check_port_;


    // ---- fd → RequestContext 映射 ----
    std::unordered_map<int, RequestContext*> ctx_by_fd_;

    // ---- 线程控制 ----
    pthread_t thread_handle_;
    std::atomic<bool> running_;

    // ==================== 内部方法 ====================

    // ---- HTTP 解析工具 ----

    /**
     * @brief 解析 HTTP 请求，提取方法、路径和 Authorization 头
     * @param request_buffer  HTTP 请求缓冲区（非空终止）
     * @param buffer_length   缓冲区有效长度
     * @param method          [输出] HTTP 方法（GET/POST 等）
     * @param path            [输出] 请求路径
     * @param auth_header     [输出] 提取出的 Bearer Token（不含 "Bearer " 前缀）
     */
    static void parse_http_request(const char* request_buffer,
                                    int buffer_length,
                                    std::string& method,
                                    std::string& path,
                                    std::string& auth_header);

    /**
     * @brief 发送 HTTP 错误响应
     * @param context     请求上下文
     * @param status_code HTTP 状态码（如 401, 429, 502）
     * @param status_text 状态文本（如 "Unauthorized"）
     * @param body        响应体文本
     */
    void send_http_error(RequestContext* context,
                          int status_code,
                          const char* status_text,
                          const char* body);

    // ---- 监听初始化 ----

    /// 创建并绑定监听套接字（带 SO_REUSEPORT），失败返回 -1
    int create_listener(int port);

    // ---- 缓冲区池管理 ----

    /// 提交 IORING_OP_PROVIDE_BUFFERS 将缓冲区注册到内核
    void provide_buffer_to_kernel(int buffer_id);

    /// 初始化缓冲池：将所有缓冲区注册到内核
    void init_buffer_pool();

    /// 从 CQE 中提取 buffer_id，查找对应缓冲区指针
    char* buffer_from_bid(int buffer_id) {
        return buffers_[buffer_id];
    }

    /// 提交 PROVIDE_BUFFERS 回收指定 buffer_id 的缓冲区
    void submit_recycle_buffer(int buffer_id);

    // ---- 提交 I/O 请求 ----

    /// 提交 accept 请求
    void submit_accept(RequestContext* context);

    /// 提交 read 请求（使用 IOSQE_BUFFER_SELECT）
    void submit_read_buf_select(RequestContext* context);

    /// 提交 write 请求
    void submit_write(RequestContext* context,
                      const char* buffer, int length);

    /// 提交 write 请求（使用 context 中的 buffer + bytes_written + total_bytes）
    void submit_write_remain(RequestContext* context);
// ---- 健康检查 ----

/// 提交 IORING_OP_TIMEOUT（5 秒间隔）
void submit_health_check_timeout();

/// 遍历所有已注册后端，为每个后端创建独立 TCP 连接执行健康检查
void perform_health_checks();


    // ---- 事件处理器 ----

    /// accept 完成：创建新连接上下文，提交首次 read
    void handle_accept(RequestContext* listen_context, int new_fd);

    /// 客户端 read 完成：提取 HTTP 路径，鉴权/限流检查，转发到后端
    void handle_client_read(RequestContext* context, int bytes_read);

    /// 客户端 write 完成：处理 partial write 或关闭连接
    void handle_client_write(RequestContext* context, int bytes_written);

    /// 后端 read 完成：将响应写回客户端（或处理健康检查响应）
    void handle_backend_read(RequestContext* context, int bytes_read);

    /// 后端 write 完成：处理 partial write 或发起后端 read
    void handle_backend_write(RequestContext* context, int bytes_written);

    /// 后端注册 read 完成：解析 "REGISTER /prefix auth=1 rate=1"
    void handle_backend_register(RequestContext* context, int bytes_read);

    // ---- 辅助方法 ----

    /// 分配后端 fd 给客户端上下文（从 COW 路由表查找）
    bool assign_backend(RequestContext* client_context);

    /// 关闭连接并清理上下文（通知 RouteManager 移除路由）
    void close_connection(RequestContext* context);

    /// 获取状态名称（用于日志输出）
    static const char* state_name(typename RequestContext::State state);

    /// 从 HTTP 请求行中提取路径（简化版本）
    static std::string extract_path(const char* buffer, int length);
};
