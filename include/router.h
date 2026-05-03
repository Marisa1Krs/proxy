#pragma once

#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief 前缀树（Trie）路由器
 *
 * 用于存储 URL 路径前缀到后端连接的映射关系。
 * 后端服务连接到 9999 端口后，通过本路由器注册其所处理的路径前缀。
 * 当客户端请求到达时，通过路径查找对应的后端连接。
 *
 * 示例：
 *   insert("/api/users", backend_fd_1)
 *   insert("/api/orders", backend_fd_2)
 *   lookup("/api/users/profile") → backend_fd_1
 */
class TrieRouter {
public:
    TrieRouter();
    ~TrieRouter();

    // 禁用拷贝
    TrieRouter(const TrieRouter&) = delete;
    TrieRouter& operator=(const TrieRouter&) = delete;

    // 允许移动
    TrieRouter(TrieRouter&& other) noexcept;
    TrieRouter& operator=(TrieRouter&& other) noexcept;

    /**
     * @brief 插入路由：路径前缀 → 后端 fd
     * @param prefix  路径前缀，如 "/api/users"
     * @param backend_fd  后端连接的文件描述符
     */
    void insert(const std::string& prefix, int backend_fd);

    /**
     * @brief 查找最匹配的路由
     * @param path  完整路径，如 "/api/users/profile"
     * @return 后端 fd，未找到返回 -1
     *
     * 采用最长前缀匹配策略：
     *   - 路由表中有 "/api" 和 "/api/users"
     *   - 查询 "/api/users/profile" → 返回 "/api/users" 对应的 fd
     *   - 查询 "/api/orders" → 返回 "/api" 对应的 fd
     */
    int lookup(const std::string& path) const;

    /**
     * @brief 删除指定前缀的路由
     */
    bool remove(const std::string& prefix);

    /**
     * @brief 删除指定后端 fd 的所有路由
     */
    void remove_by_backend(int backend_fd);

    /**
     * @brief 获取已注册的所有路由（用于调试/日志）
     */
    std::vector<std::pair<std::string, int>> get_all_routes() const;

private:
    /// 前缀树节点
    struct TrieNode {
        std::unordered_map<char, TrieNode*> children;
        int backend_fd;            // -1 表示该节点不是有效路由终点
        bool is_endpoint;          // 该节点是否为一个注册的路由终点

        TrieNode()
            : backend_fd(-1)
            , is_endpoint(false) {}

        ~TrieNode() {
            for (auto& [ch, child] : children) {
                delete child;
            }
        }
    };

    TrieNode* root_;

    /// 递归收集所有路由（辅助函数）
    void collect_routes(TrieNode* node, std::string& current_prefix,
                        std::vector<std::pair<std::string, int>>& result) const;

    /// 递归删除节点的辅助函数
    void delete_node(TrieNode* node);
};
