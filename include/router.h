#pragma once

#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief 路由信息结构体
 *
 * 包含后端连接的文件描述符以及该路由的鉴权/限流配置。
 * 每个注册到网关的后端服务都对应一个 RouteInfo。
 */
struct RouteInfo {
    int backend_fd = -1;          ///< 后端连接的 socket fd，-1 表示无效
    bool need_auth = false;       ///< 是否需要 JWT 令牌鉴权
    bool need_rate_limit = false; ///< 是否需要 IP 限流

    /// 判断该路由是否有效（即是否有合法的后端 fd）
    bool valid() const {
        return backend_fd >= 0;
    }
};

/**
 * @brief 前缀树（Trie）路由器
 *
 * 用于存储 URL 路径前缀到后端连接的映射关系。
 * 后端服务连接到网关的注册端口后，通过本路由器注册其所处理的路径前缀。
 * 当客户端 HTTP 请求到达时，通过请求路径查找对应的后端连接。
 *
 * 每个路由可配置两个标志：
 *   - need_auth:        是否需要进行 JWT 令牌鉴权
 *   - need_rate_limit:  是否需要进行 IP 限流
 *
 * 使用示例：
 * @code
 *   router.insert("/api/users", backend_fd_1, true, true);
 *   router.insert("/static",    backend_fd_2, false, false);
 *   RouteInfo result = router.lookup("/api/users/profile");
 *   // result.backend_fd = backend_fd_1, result.need_auth = true
 * @endcode
 */
class TrieRouter {
public:
    TrieRouter();
    ~TrieRouter();

    // 禁止拷贝
    TrieRouter(const TrieRouter&) = delete;
    TrieRouter& operator=(const TrieRouter&) = delete;

    // 允许移动
    TrieRouter(TrieRouter&& other) noexcept;
    TrieRouter& operator=(TrieRouter&& other) noexcept;

    /**
     * @brief 插入路由：路径前缀 → 后端 fd + 配置标志
     * @param prefix          路径前缀，如 "/api/users"
     * @param backend_fd      后端连接的文件描述符
     * @param need_auth       是否需要进行 JWT 令牌鉴权
     * @param need_rate_limit 是否需要进行 IP 限流
     */
    void insert(const std::string& prefix, int backend_fd,
                bool need_auth = false,
                bool need_rate_limit = false);

    /**
     * @brief 查找最匹配的路由（返回完整 RouteInfo）
     * @param path  请求的完整路径，如 "/api/users/profile"
     * @return RouteInfo 结构体（包含 backend_fd 和标志位）
     *         RouteInfo::valid() 为 false 表示未找到匹配路由
     *
     * 采用最长前缀匹配策略：
     *   - 路由表中有 "/api" 和 "/api/users"
     *   - 查询 "/api/users/profile" → 返回 "/api/users" 的 RouteInfo
     *   - 查询 "/api/orders"       → 返回 "/api" 的 RouteInfo
     *   - 查询 "/other"            → 返回无效 RouteInfo
     */
    RouteInfo lookup(const std::string& path) const;

    /**
     * @brief 删除指定前缀的路由
     * @param prefix  要删除的路径前缀
     * @return true   删除成功
     * @return false  该路径不存在或不是路由终点
     */
    bool remove(const std::string& prefix);

    /**
     * @brief 删除指定后端 fd 的所有路由
     * @param backend_fd  后端连接的文件描述符
     *
     * 当后端断开连接时调用此方法，清理该后端注册的所有路由。
     */
    void remove_by_backend(int backend_fd);

    /**
     * @brief 获取已注册的所有路由（用于调试/日志输出）
     * @return 路径前缀与 RouteInfo 的配对列表
     */
    std::vector<std::pair<std::string, RouteInfo>>
    get_all_routes() const;

private:
    /**
     * @brief 前缀树节点
     *
     * 每个节点代表路径中的一个字符。
     * is_endpoint 为 true 时，该节点是一个注册的路由终点，
     * 包含对应的后端 fd 和鉴权/限流配置。
     */
    struct TrieNode {
        /// 子节点映射表：字符 → 子节点指针
        std::unordered_map<char, TrieNode*> children;

        int backend_fd       = -1;   ///< 后端 fd（仅当 is_endpoint 时有效）
        bool is_endpoint     = false;///< 该节点是否为一个注册的路由终点
        bool need_auth       = false;///< 是否需要 JWT 鉴权
        bool need_rate_limit = false;///< 是否需要 IP 限流

        TrieNode() = default;
        ~TrieNode() {
            for (auto& [character, child] : children) {
                delete child;
            }
        }
    };

    TrieNode* root_;  ///< 前缀树根节点

    /// 递归收集所有路由（辅助函数）
    void collect_routes(
        TrieNode* node,
        std::string& current_prefix,
        std::vector<std::pair<std::string, RouteInfo>>& result) const;

    /// 递归删除节点及其所有子节点（辅助函数）
    void delete_node(TrieNode* node);
};
