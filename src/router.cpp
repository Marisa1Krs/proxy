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
    // 将源对象的 root_ 替换为一个新节点，保持其有效状态
    other.root_ = new TrieNode();
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

// 拷贝构造函数（深拷贝）
TrieRouter::TrieRouter(const TrieRouter& other)
    : root_(new TrieNode()) {
    // 使用 get_all_routes 获取所有路由，然后逐条插入
    auto all_routes = other.get_all_routes();
    for (const auto& [prefix, info] : all_routes) {
        insert(prefix, info.backend_fd,
               info.need_auth, info.need_rate_limit);
    }
}

// 拷贝赋值运算符（深拷贝）
TrieRouter& TrieRouter::operator=(const TrieRouter& other) {
    if (this != &other) {
        // 清除当前所有节点
        delete root_;
        root_ = new TrieNode();

        // 从 other 逐条插入
        auto all_routes = other.get_all_routes();
        for (const auto& [prefix, info] : all_routes) {
            insert(prefix, info.backend_fd,
                   info.need_auth, info.need_rate_limit);
        }
    }
    return *this;
}

void TrieRouter::insert(const std::string& prefix,
                         int backend_fd,
                         bool need_auth,
                         bool need_rate_limit) {
    // 空前缀或无效 fd 直接返回
    if (prefix.empty() || backend_fd < 0) {
        return;
    }

    TrieNode* current_node = root_;

    // 逐字符遍历前缀，在 Trie 中创建路径
    for (char current_char : prefix) {
        auto child_iterator = current_node->children.find(current_char);
        if (child_iterator == current_node->children.end()) {
            // 该字符路径不存在，创建新节点
            current_node->children[current_char] = new TrieNode();
        }
        current_node = current_node->children[current_char];
    }

    // 将当前节点标记为路由终点，添加后端 fd 到列表
    // 支持多个后端注册同一前缀（并发处理）
    current_node->backend_fds.push_back(backend_fd);
    current_node->is_endpoint = true;
    current_node->need_auth = need_auth;
    current_node->need_rate_limit = need_rate_limit;
}

RouteInfo TrieRouter::lookup(const std::string& path) const {
    RouteInfo matched_route;

    // 空路径直接返回无效路由
    if (path.empty()) {
        return matched_route;
    }

    TrieNode* current_node = root_;

    // 逐字符遍历路径，记录最长匹配的路由
    for (char path_char : path) {
        auto child_iterator = current_node->children.find(path_char);

        // 该字符无对应子节点，停止遍历
        if (child_iterator == current_node->children.end()) {
            break;
        }

        current_node = child_iterator->second;

        // 当前节点是一个有效路由终点 → 记录（继续找更长匹配）
        if (current_node->is_endpoint && !current_node->backend_fds.empty()) {
            matched_route.backend_fd = current_node->backend_fds.front();
            matched_route.need_auth = current_node->need_auth;
            matched_route.need_rate_limit = current_node->need_rate_limit;
        }
    }

    return matched_route;
}

bool TrieRouter::remove(const std::string& prefix) {
    if (prefix.empty()) {
        return false;
    }

    TrieNode* current_node = root_;

    // 沿路径找到目标节点
    for (char path_char : prefix) {
        auto child_iterator = current_node->children.find(path_char);
        if (child_iterator == current_node->children.end()) {
            // 路径不存在
            return false;
        }
        current_node = child_iterator->second;
    }

    // 该节点不是路由终点，无法删除
    if (!current_node->is_endpoint) {
        return false;
    }

    // 清除路由终点标记和所有后端
    current_node->is_endpoint = false;
    current_node->backend_fds.clear();
    return true;
}

void TrieRouter::remove_by_backend(int backend_fd) {
    if (backend_fd < 0) {
        return;
    }

    // 使用 DFS 递归遍历所有节点，从后端 fd 列表中移除匹配项
    std::string current_prefix;
    std::function<void(TrieNode*)> depth_first_search =
        [&](TrieNode* node) {
            for (auto& [character, child_node] : node->children) {
                current_prefix.push_back(character);

                if (child_node->is_endpoint) {
                    // 从该节点的后端列表中移除指定 fd
                    auto& fds = child_node->backend_fds;
                    for (auto it = fds.begin(); it != fds.end(); ) {
                        if (*it == backend_fd) {
                            it = fds.erase(it);
                        } else {
                            ++it;
                        }
                    }
                    // 如果后端列表为空，清除路由终点标记
                    if (fds.empty()) {
                        child_node->is_endpoint = false;
                    }
                }

                // 递归遍历子节点
                depth_first_search(child_node);

                current_prefix.pop_back();
            }
        };

    depth_first_search(root_);
}

std::vector<std::pair<std::string, RouteInfo>>
TrieRouter::get_all_routes() const {
    std::vector<std::pair<std::string, RouteInfo>> all_routes;
    std::string current_prefix;
    collect_routes(root_, current_prefix, all_routes);
    return all_routes;
}

void TrieRouter::collect_routes(
    TrieNode* node,
    std::string& current_prefix,
    std::vector<std::pair<std::string, RouteInfo>>& result) const {

    // 如果当前节点是路由终点，为每个后端 fd 生成一个路由条目
    if (node->is_endpoint) {
        for (int fd : node->backend_fds) {
            RouteInfo route_info;
            route_info.backend_fd = fd;
            route_info.need_auth = node->need_auth;
            route_info.need_rate_limit = node->need_rate_limit;
            result.emplace_back(current_prefix, route_info);
        }
    }

    // 递归遍历所有子节点
    for (auto& [character, child_node] : node->children) {
        current_prefix.push_back(character);
        collect_routes(child_node, current_prefix, result);
        current_prefix.pop_back();
    }
}

void TrieRouter::delete_node(TrieNode* node) {
    if (node == nullptr) {
        return;
    }
    for (auto& [character, child_node] : node->children) {
        delete_node(child_node);
    }
    delete node;
}
