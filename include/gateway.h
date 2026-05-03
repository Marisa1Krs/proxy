#pragma once

#include <liburing.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "router.h"
#include "channel.h"

/**
 * @brief 基于 io_uring 的异步网关
 *
 * 核心职责：
 *   1. 监听 8888 端口，接收客户端连接
 *   2. 监听 9999 端口，接收后端服务连接并注册路由
 *   3. 使用前缀树（Trie）进行路径路由
 *   4. 通过 io_uring 实现全异步的双向数据转发
 *
 * 工作流程：
 *   ┌─────────┐  HTTP请求   ┌──────────┐  转发   ┌────────┐
 *   │  客户端  │ ──────────▶ │  Gateway  │ ──────▶ │  后端  │
 *   │ (port N) │             │ port 8888 │         │ port M │
 *   └─────────┘             │ port 9999 │         └────────┘
 *                            │ (注册端口) │
 *   ┌─────────┐  注册路由   └──────────┘
 *   │  后端   │ ──────────▶    ▲
 *   │ (注册)  │                │ TrieRouter
 *   └─────────┘                │
 */
class Gateway {
public:
    /**
     * @brief 构造网关
     * @param client_port   客户端监听端口（默认 8888）
     * @param backend_port  后端服务监听端口（默认 9999）
     * @param ring_size     io_uring 队列深度（默认 1024）
     */
    explicit Gateway(int client_port = 8888,
                     int backend_port = 9999,
                     int ring_size = 1024);
    ~Gateway();

    // 禁止拷贝
    Gateway(const Gateway&) = delete;
    Gateway& operator=(const Gateway&) = delete;

    /**
     * @brief 启动事件循环（永不返回，除非异常）
     */
    void run();

    /**
     * @brief 优雅停止（线程安全）
     */
    void stop();

private:
    // ==================== 常量 ====================
    static constexpr int MAX_RING_SIZE = 4096;
    static constexpr int BACKLOG = 128;

    // ==================== io_uring ====================
    io_uring ring_;
    int ring_size_;

    // ==================== 监听套接字 ====================
    int listen_fd_client_;   // 8888：客户端接入
    int listen_fd_backend_;  // 9999：后端服务接入

    // ==================== 路由表 ====================
    TrieRouter router_;

    // ==================== 通道管理 ====================
    /// client_fd → Channel 映射
    std::unordered_map<int, Channel*> channels_by_client_;
    /// backend_fd → Channel 映射
    std::unordered_map<int, Channel*> channels_by_backend_;

    // ==================== 运行状态 ====================
    volatile bool running_;

    // ==================== I/O 上下文枚举 ====================
    /// 操作类型，嵌入到 io_uring 的 user_data 中
    enum class OpType : uint64_t {
        ACCEPT_CLIENT,       ///< Accept 客户端连接（listen_fd_client_）
        ACCEPT_BACKEND,      ///< Accept 后端连接（listen_fd_backend_）
        READ_CLIENT,         ///< 从客户端读取数据
        WRITE_CLIENT,        ///< 向客户端写入数据
        READ_BACKEND,        ///< 从后端读取数据
        WRITE_BACKEND,       ///< 向后端写入数据
        BACKEND_REGISTER,    ///< 读取后端注册消息
    };

    /// I/O 请求上下文，通过 io_uring_sqe_set_data 绑定
    struct IOContext {
        OpType op;              ///< 操作类型
        int fd;                 ///< 操作关联的 fd
        Channel* channel;       ///< 所属 Channel（部分操作可为 nullptr）
        char* buf;              ///< 数据缓冲区指针
        size_t buf_len;         ///< 缓冲区大小

        IOContext(OpType op, int fd, Channel* ch = nullptr,
                  char* buf = nullptr, size_t buf_len = 0)
            : op(op), fd(fd), channel(ch), buf(buf), buf_len(buf_len) {}
    };

    // ==================== 内部方法 ====================

    /// 初始化监听套接字（SO_REUSEADDR, bind, listen）
    int create_listener(int port);

    /// 提交 Accept 请求到 io_uring
    void submit_accept(int listen_fd, OpType op_type);

    /// 提交 Read 请求到 io_uring
    void submit_read(int fd, char* buf, size_t len, OpType op_type, Channel* ch);

    /// 提交 Write 请求到 io_uring
    void submit_write(int fd, const char* buf, size_t len, OpType op_type, Channel* ch);

    // ==================== 事件处理器 ====================

    /// Accept 完成回调
    void handle_accept(IOContext* ctx, int new_fd);

    /// 客户端 Read 完成回调
    void handle_client_read(IOContext* ctx, int bytes_read);

    /// 客户端 Write 完成回调
    void handle_client_write(IOContext* ctx);

    /// 后端 Read 完成回调
    void handle_backend_read(IOContext* ctx, int bytes_read);

    /// 后端 Write 完成回调
    void handle_backend_write(IOContext* ctx);

    /// 后端注册消息处理
    void handle_backend_register(IOContext* ctx, int bytes_read);

    /// 分配后端连接给 Channel
    bool assign_backend_to_channel(Channel* ch);

    // ==================== 辅助方法 ====================

    /// 获取 OpType 的字符串表示（日志用）
    static const char* op_type_str(OpType op);

    /// 关闭指定 Channel 并清理资源
    void close_channel(Channel* ch);

    /// 从 HTTP 请求中提取路径（简单解析第一行）
    static std::string extract_path(const char* buf, int len);

    /// 销毁 I/O 上下文
    static void destroy_io_context(IOContext* ctx);
};
