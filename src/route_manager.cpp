#include "route_manager.h"
#include "mylog.h"

RouteManager::RouteManager()
    : current_table_(std::make_shared<const RouteTable>()) {

    LOG_DEBUG("[RouteManager] 已创建，初始路由表为空");
}

std::shared_ptr<const RouteTable> RouteManager::get_table() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_table_;
}

void RouteManager::insert_backend(const std::string& prefix,
                                   int backend_fd,
                                   bool need_auth,
                                   bool need_rate_limit) {
    if (backend_fd < 0 || prefix.empty()) {
        return;
    }

    // ---- COW: Copy → Update → Atomic Swap ----
    std::lock_guard<std::mutex> lock(mutex_);

    // 1. Copy：深拷贝当前路由表
    auto new_table = std::make_shared<RouteTable>(*current_table_);

    // 2. Update：在副本上修改
    new_table->insert(prefix, backend_fd, need_auth, need_rate_limit);

    // 3. Atomic Swap：原子替换指针（const 版本）
    std::shared_ptr<const RouteTable> new_const = std::move(new_table);
    current_table_.swap(new_const);

    // 4. 旧表（new_const 现指向旧版本）引用计数递减
    //    当所有 Worker 释放其持有的 shared_ptr 后自动销毁

    LOG_DEBUG("[RouteManager] 插入路由: %s → fd=%d (COW 完成)",
              prefix.c_str(), backend_fd);
}

void RouteManager::remove_backend(int backend_fd) {
    if (backend_fd < 0) {
        return;
    }

    // ---- COW: Copy → Update → Atomic Swap ----
    std::lock_guard<std::mutex> lock(mutex_);

    // 1. Copy：深拷贝当前路由表
    auto new_table = std::make_shared<RouteTable>(*current_table_);

    // 2. Update：在副本上删除路由
    new_table->remove_by_backend(backend_fd);

    // 3. Atomic Swap
    std::shared_ptr<const RouteTable> new_const = std::move(new_table);
    current_table_.swap(new_const);

    LOG_DEBUG("[RouteManager] 删除路由: fd=%d (COW 完成)", backend_fd);
}
