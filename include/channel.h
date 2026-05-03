#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

/**
 * @brief 代理通道
 *
 * 管理一个客户端连接与一个后端服务连接之间的配对关系。
 * 每个 Channel 持有一对缓冲区，用于双向数据转发：
 *   - client_to_backend_buf_: 客户端 → 网关 → 后端（请求转发）
 *   - backend_to_client_buf_: 后端 → 网关 → 客户端（响应转发）
 *
 * 生命周期：
 *   1. 客户端连接 8888 端口，accept 后创建 Channel
 *   2. 读取客户端请求，解析路径，通过 Trie 分配后端
 *   3. 创建与后端的配对，开始双向转发
 *   4. 任意一端断开时，关闭两端连接，销毁 Channel
 */
class Channel {
public:
    /// 缓冲区大小（4KB，对于大多数 HTTP 请求/响应足够）
    static constexpr size_t BUFFER_SIZE = 4096;

    /// 转发方向
    enum class Direction {
        CLIENT_TO_BACKEND,  ///< 客户端 → 后端（请求）
        BACKEND_TO_CLIENT   ///< 后端 → 客户端（响应）
    };

    /**
     * @brief 构造代理通道
     * @param client_fd  客户端连接 fd
     * @param backend_fd 后端服务连接 fd（-1 表示尚未分配）
     */
    explicit Channel(int client_fd, int backend_fd = -1);
    ~Channel();

    // 禁止拷贝
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    // ---------- 访问器 ----------

    int client_fd() const { return client_fd_; }
    int backend_fd() const { return backend_fd_; }

    void set_backend_fd(int fd) { backend_fd_ = fd; }

    /// 获取指定方向的读取缓冲区（数据来源端的缓冲区）
    char* read_buf(Direction dir);

    /// 获取指定方向的写入缓冲区（数据目标端的缓冲区）
    char* write_buf(Direction dir);

    /// 获取指定方向的读取 fd（数据从哪读）
    int read_fd(Direction dir) const;

    /// 获取指定方向的写入 fd（数据往哪写）
    int write_fd(Direction dir) const;

    /// 获取指定方向的数据长度
    int data_len(Direction dir) const { return data_len_[dir_idx(dir)]; }

    /// 设置指定方向的数据长度
    void set_data_len(Direction dir, int len) { data_len_[dir_idx(dir)] = len; }

    // ---------- 生命周期管理 ----------

    bool is_closed() const { return closed_; }
    void mark_closed() { closed_ = true; }

    /// 关闭两端连接并标记为已关闭
    void close_both_ends();

    /// 重置 Channel 状态（为下一次转发做准备）
    void reset_buffers();

    /// 获取 Channel ID（用于日志）
    uint64_t id() const { return id_; }

private:
    int client_fd_;
    int backend_fd_;
    bool closed_;
    uint64_t id_;

    /// 缓冲区：客户端 → 后端（请求）
    char client_to_backend_buf_[BUFFER_SIZE];
    /// 缓冲区：后端 → 客户端（响应）
    char backend_to_client_buf_[BUFFER_SIZE];

    /// 各方向当前有效数据长度
    int data_len_[2];

    /// 全局 Channel ID 计数器
    static uint64_t next_id_;

    /// 方向 → 数组索引
    static int dir_idx(Direction dir) {
        return (dir == Direction::CLIENT_TO_BACKEND) ? 0 : 1;
    }
};
