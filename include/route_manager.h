#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "route_table.h"

/**
 * @brief COW（Copy-on-Write）路由表管理器
 *
 * 核心职责：
 *   1. 持有当前路由表的 shared_ptr，Worker 通过 get_table() 只读访问
 *   2. 提供原子化的修改操作（insert/remove），内部执行 COW：
 *      Copy  → 深拷贝当前表
 *      Update → 在副本上修改
 *      Swap  → 原子替换全局指针（mutex 保护）
 *      旧表在最后一个持有者释放后自动销毁（shared_ptr 引用计数）
 *
 * 线程安全设计：
 *   - 读操作（get_table）：仅拷贝 shared_ptr，持有锁微秒级
 *   - 写操作（insert/remove）：深拷贝期间持有锁，COW 期间其他读等待
 *   - 写写互斥：只能串行执行 COW
 */
class RouteManager {
public:
    RouteManager();

    RouteManager(const RouteManager&) = delete;
    RouteManager& operator=(const RouteManager&) = delete;

    ~RouteManager() = default;

    // ==================== 读接口（Worker 调用） ====================

    /**
     * @brief 获取当前路由表的只读快照
     * @return shared_ptr<const RouteTable> 当前路由表
     *
     * 调用者拿到 shared_ptr 后可以安全读取，
     * 即使 Manager 在后面进行了 COW 替换，此对象依然有效。
     */
    std::shared_ptr<const RouteTable> get_table() const;

    // ==================== 写接口（Worker/Manager 调用，COW） ====================

    /**
     * @brief 原子插入路由（COW）
     * @param prefix          路径前缀
     * @param backend_fd      后端 fd
     * @param need_auth       鉴权标志
     * @param need_rate_limit 限流标志
     *
     * 步骤：
     *   1. 获取当前表的副本
     *   2. 深拷贝一份新表
     *   3. 在新表上执行 insert
     *   4. 原子替换 current_table_
     *   5. 旧表引用计数递减，无人用时自动销毁
     */
    void insert_backend(const std::string& prefix, int backend_fd,
                        bool need_auth = false,
                        bool need_rate_limit = false);

    /**
     * @brief 原子删除后端路由（COW）
     * @param backend_fd 要删除的后端 fd
     *
     * 步骤同上，在新表上执行 remove_by_backend。
     */
    void remove_backend(int backend_fd);

private:
    /// 保护 current_table_ 读写互斥
    mutable std::mutex mutex_;

    /// 当前活跃的路由表（Worker 们正在读的版本）
    std::shared_ptr<const RouteTable> current_table_;
};
