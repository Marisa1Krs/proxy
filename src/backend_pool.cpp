#include "backend_pool.h"

#include <algorithm>

// ==================== 添加后端连接 ====================

void BackendPool::add(int fd, const std::string& prefix,
                      bool need_auth, bool need_rate_limit) {
    // 如果 fd 已存在，先移除旧的
    auto it = entries_.find(fd);
    if (it != entries_.end()) {
        // 如果旧条目在空闲队列中，移除它
        auto& old_entry = it->second;
        if (!old_entry.in_use) {
            auto qit = std::find(idle_queue_.begin(), idle_queue_.end(), fd);
            if (qit != idle_queue_.end()) {
                idle_queue_.erase(qit);
            }
        }
    }

    // 创建新条目（初始为空闲状态）
    PoolEntry entry;
    entry.prefix           = prefix;
    entry.need_auth        = need_auth;
    entry.need_rate_limit  = need_rate_limit;
    entry.in_use           = false;

    entries_[fd] = std::move(entry);
    idle_queue_.push_back(fd);
}

// ==================== 获取后端连接（最长前缀匹配） ====================

int BackendPool::acquire(const std::string& path,
                         bool& need_auth, bool& need_rate_limit) {
    int best_fd    = -1;
    size_t best_len = 0;
    int best_index  = -1;

    // 遍历空闲队列，找最长前缀匹配
    for (size_t i = 0; i < idle_queue_.size(); ++i) {
        int fd = idle_queue_[i];
        auto it = entries_.find(fd);
        if (it == entries_.end()) continue;  // 防御

        const PoolEntry& entry = it->second;
        const std::string& prefix = entry.prefix;

        // 检查 path 是否以 prefix 开头
        if (path.size() >= prefix.size() &&
            path.compare(0, prefix.size(), prefix) == 0) {
            // 更长的前缀优先
            if (prefix.size() > best_len) {
                best_fd    = fd;
                best_len   = prefix.size();
                best_index = static_cast<int>(i);
            }
        }
    }

    if (best_fd < 0) {
        return -1;  // 无匹配的空闲连接
    }

    // 从空闲队列移除
    idle_queue_.erase(idle_queue_.begin() + best_index);

    // 标记为忙碌
    entries_[best_fd].in_use = true;

    // 输出标志位
    need_auth       = entries_[best_fd].need_auth;
    need_rate_limit = entries_[best_fd].need_rate_limit;

    return best_fd;
}

// ==================== 释放后端连接 ====================

void BackendPool::release(int fd) {
    auto it = entries_.find(fd);
    if (it == entries_.end()) {
        return;  // 未知 fd，忽略
    }

    // 标记为空闲
    it->second.in_use = false;

    // 放回空闲队列尾部（FIFO）
    idle_queue_.push_back(fd);
}

// ==================== 移除后端连接 ====================

void BackendPool::remove(int fd) {
    auto it = entries_.find(fd);
    if (it == entries_.end()) {
        return;  // 未知 fd，忽略
    }

    // 如果在空闲队列中，移除
    if (!it->second.in_use) {
        auto qit = std::find(idle_queue_.begin(), idle_queue_.end(), fd);
        if (qit != idle_queue_.end()) {
            idle_queue_.erase(qit);
        }
    }

    // 从条目映射中移除
    entries_.erase(it);
}

// ==================== 获取空闲 fd 列表 ====================

std::vector<int> BackendPool::get_idle_fds() const {
    std::vector<int> result;
    result.reserve(idle_queue_.size());
    for (int fd : idle_queue_) {
        result.push_back(fd);
    }
    return result;
}

// ==================== 清空 ====================

void BackendPool::clear() {
    entries_.clear();
    idle_queue_.clear();
}
