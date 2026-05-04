#pragma once

#include <memory>
#include <string>
#include <vector>

#include "router.h"

/**
 * @brief 不可变路由表（COW 模式）
 *
 * 包装 TrieRouter，只暴露只读接口给 Worker。
 * Manager 在后台通过深拷贝创建新版本，原子替换全局指针。
 * Worker 通过 shared_ptr<const RouteTable> 安全并发读取。
 */
class RouteTable {
public:
    RouteTable() = default;

    /// 深拷贝构造
    RouteTable(const RouteTable& other)
        : router_(other.router_) {}

    /// 深拷贝赋值
    RouteTable& operator=(const RouteTable& other) {
        if (this != &other) {
            router_ = other.router_;
        }
        return *this;
    }

    // ==================== 只读接口（Worker 可访问） ====================

    /// 查找最长前缀匹配的路由
    RouteInfo lookup(const std::string& path) const {
        return router_.lookup(path);
    }

    /// 获取所有已注册路由
    std::vector<std::pair<std::string, RouteInfo>>
    get_all_routes() const {
        return router_.get_all_routes();
    }

    // ==================== 写接口（仅 Manager 在私有副本上调用） ====================

    /// 插入路由
    void insert(const std::string& prefix, int backend_fd,
                bool need_auth = false,
                bool need_rate_limit = false) {
        router_.insert(prefix, backend_fd, need_auth, need_rate_limit);
    }

    /// 删除指定后端 fd 的所有路由
    void remove_by_backend(int backend_fd) {
        router_.remove_by_backend(backend_fd);
    }

private:
    TrieRouter router_;
};
