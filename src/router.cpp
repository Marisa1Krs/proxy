#include "router.h"
#include <functional>
#include <iostream>

TrieRouter::TrieRouter()
    : root_(new TrieNode()) {}

TrieRouter::~TrieRouter() {
    delete root_;
    root_ = nullptr;
}

// 移动构造函数
TrieRouter::TrieRouter(TrieRouter&& other) noexcept
    : root_(other.root_) {
    other.root_ = new TrieNode();  // 保留有效状态
}

// 移动赋值运算符
TrieRouter& TrieRouter::operator=(TrieRouter&& other) noexcept {
    if (this != &other) {
        delete root_;
        root_ = other.root_;
        other.root_ = new TrieNode();
    }
    return *this;
}

void TrieRouter::insert(const std::string& prefix, int backend_fd) {
    if (prefix.empty() || backend_fd < 0) return;

    TrieNode* cur = root_;

    for (char ch : prefix) {
        auto it = cur->children.find(ch);
        if (it == cur->children.end()) {
            cur->children[ch] = new TrieNode();
        }
        cur = cur->children[ch];
    }

    // 更新当前节点为路由终点
    cur->backend_fd = backend_fd;
    cur->is_endpoint = true;
}

int TrieRouter::lookup(const std::string& path) const {
    if (path.empty()) return -1;

    TrieNode* cur = root_;
    int matched_fd = -1;

    for (char ch : path) {
        auto it = cur->children.find(ch);
        if (it == cur->children.end()) {
            break;  // 无更长的匹配路径
        }
        cur = it->second;

        // 当前节点是一个有效路由终点 → 记录（继续找更长匹配）
        if (cur->is_endpoint) {
            matched_fd = cur->backend_fd;
        }
    }

    return matched_fd;
}

bool TrieRouter::remove(const std::string& prefix) {
    if (prefix.empty()) return false;

    TrieNode* cur = root_;

    for (char ch : prefix) {
        auto it = cur->children.find(ch);
        if (it == cur->children.end()) {
            return false;  // 路径不存在
        }
        cur = it->second;
    }

    if (!cur->is_endpoint) {
        return false;  // 该路径不是路由终点
    }

    cur->is_endpoint = false;
    cur->backend_fd = -1;
    return true;
}

void TrieRouter::remove_by_backend(int backend_fd) {
    if (backend_fd < 0) return;

    std::string prefix;
    // 使用辅助函数递归删除
    std::function<void(TrieNode*)> dfs = [&](TrieNode* node) {
        for (auto& [ch, child] : node->children) {
            prefix.push_back(ch);
            if (child->is_endpoint && child->backend_fd == backend_fd) {
                child->is_endpoint = false;
                child->backend_fd = -1;
            }
            dfs(child);
            prefix.pop_back();
        }
    };

    dfs(root_);
}

std::vector<std::pair<std::string, int>> TrieRouter::get_all_routes() const {
    std::vector<std::pair<std::string, int>> result;
    std::string current_prefix;
    collect_routes(root_, current_prefix, result);
    return result;
}

void TrieRouter::collect_routes(TrieNode* node, std::string& current_prefix,
                                std::vector<std::pair<std::string, int>>& result) const {
    if (node->is_endpoint) {
        result.emplace_back(current_prefix, node->backend_fd);
    }

    for (auto& [ch, child] : node->children) {
        current_prefix.push_back(ch);
        collect_routes(child, current_prefix, result);
        current_prefix.pop_back();
    }
}

void TrieRouter::delete_node(TrieNode* node) {
    if (!node) return;
    for (auto& [ch, child] : node->children) {
        delete_node(child);
    }
    delete node;
}
