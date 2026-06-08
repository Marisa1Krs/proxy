#pragma once

#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief 后端连接池（每个 Worker 独立拥有，线程本地，无需锁）
 *
 * 替代原先的全局 COW RouteTable 方案。
 * 每个 Worker 维护属于自己的后端连接池，不再共享任何 fd 数据。
 *
 * 设计要点：
 *   - 单线程使用（在 Worker 事件循环中访问），无锁无原子操作
 *   - FIFO 空闲队列：acquire 从头取，release 放回尾部
 *   - 最长前缀匹配查找后端
 *   - 一个后端 prefix 可对应多个 fd（连接池），实现并发
 */
class BackendPool {
public:
    BackendPool() = default;
    ~BackendPool() = default;

    BackendPool(const BackendPool&) = delete;
    BackendPool& operator=(const BackendPool&) = delete;

    // ==================== 写接口 ====================

    /**
     * @brief 添加一个后端连接到池中（后端注册时调用）
     * @param fd              后端连接的 socket fd
     * @param prefix          后端注册的路径前缀（如 "/api"）
     * @param need_auth       是否需要鉴权
     * @param need_rate_limit 是否需要限流
     */
    void add(int fd, const std::string& prefix,
             bool need_auth = false, bool need_rate_limit = false);

    /**
     * @brief 释放一个后端连接回空闲池（请求处理完成后调用）
     * @param fd 后端 fd
     */
    void release(int fd);

    /**
     * @brief 移除一个后端连接（连接断开时调用）
     * @param fd 后端 fd
     */
    void remove(int fd);

    /**
     * @brief 清空所有后端连接
     */
    void clear();

    // ==================== 读接口 ====================

    /**
     * @brief 从空闲池中获取一个匹配路径的后端连接
     * @param path            客户端请求路径（如 "/api/users"）
     * @param need_auth       [输出] 该路由是否需要鉴权
     * @param need_rate_limit [输出] 该路由是否需要限流
     * @return 后端 fd，-1 表示无可用空闲连接
     *
     * 采用最长前缀匹配：
     *   - 注册了 "/api" 和 "/api/users"
     *   - 查询 "/api/users/profile" 匹配 "/api/users"
     *   - 查询 "/api/orders" 匹配 "/api"
     */
    int acquire(const std::string& path,
                bool& need_auth, bool& need_rate_limit);

    /**
     * @brief 获取所有空闲后端 fd（用于健康检查遍历）
     */
    std::vector<int> get_idle_fds() const;

    /**
     * @brief 获取空闲连接数
     */
    size_t idle_count() const { return idle_queue_.size(); }

    /**
     * @brief 判断某个 fd 是否在池中（含忙碌中）
     */
    bool contains(int fd) const {
        return entries_.find(fd) != entries_.end();
    }

private:
    /**
     * @brief 池条目：存储后端连接的注册信息
     */
    struct PoolEntry {
        std::string prefix;            ///< 路径前缀
        bool need_auth       = false;  ///< 鉴权标志
        bool need_rate_limit = false;  ///< 限流标志
        bool in_use          = false;  ///< 是否正在被使用
    };

    /// 所有后端连接：fd → 条目（含忙碌中的）
    std::unordered_map<int, PoolEntry> entries_;

    /// 空闲后端 fd 队列（FIFO，仅含 in_use=false 的 fd）
    std::deque<int> idle_queue_;
};
