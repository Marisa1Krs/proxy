#pragma once

#include <chrono>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>

/**
 * @brief 基于 IP 地址的滑动窗口限流器
 *
 * 使用滑动窗口算法，记录每个 IP 在窗口时间内的请求次数。
 * 如果请求次数超过配置的最大值，则拒绝后续请求。
 *
 * 线程安全：内部使用互斥锁保护所有计数器的并发访问。
 *
 * 默认配置：每个 IP 每秒最多允许 60 个请求。
 * 可通过构造函数参数调整窗口大小和最大请求数。
 *
 * 使用示例：
 * @code
 *   RateLimiter limiter(100, 1000);  // 每秒最多 100 个请求
 *   if (limiter.allow("192.168.1.1")) {
 *       // 处理请求
 *   } else {
 *       // 返回 429 Too Many Requests
 *   }
 * @endcode
 */
class RateLimiter {
public:
    /**
     * @brief 构造限流器
     * @param max_requests  滑动窗口内允许的最大请求数（默认 60）
     * @param window_ms     滑动窗口的时间长度，单位毫秒（默认 1000，即 1 秒）
     */
    explicit RateLimiter(int max_requests = 60, int window_ms = 1000)
        : max_requests_(max_requests)
        , window_ms_(window_ms) {}

    ~RateLimiter() = default;

    // 禁止拷贝
    RateLimiter(const RateLimiter&) = delete;
    RateLimiter& operator=(const RateLimiter&) = delete;

    /**
     * @brief 检查某个 IP 的请求是否允许通过
     *
     * 执行以下操作：
     *   1. 获取当前时间
     *   2. 加锁保护内部数据结构
     *   3. 清理该 IP 在窗口之外的所有过期时间戳记录
     *   4. 检查当前窗口内的请求数是否超过限制
     *   5. 如果未超限，记录当前请求的时间戳并返回 true
     *
     * @param ip  客户端 IP 地址（字符串形式）
     * @return true  请求允许通过
     * @return false 请求被限流拒绝（超过速率限制）
     */
    bool allow(const std::string& ip) {
        auto current_time = std::chrono::steady_clock::now();

        // 加锁保护内部数据结构
        std::lock_guard<std::mutex> lock(mutex_);

        // 获取该 IP 的时间戳队列
        auto& timestamps = records_[ip];

        // 清理窗口之外的所有过期记录
        auto cutoff_time =
            current_time - std::chrono::milliseconds(window_ms_);
        while (!timestamps.empty() && timestamps.front() < cutoff_time) {
            timestamps.pop_front();
        }

        // 检查当前窗口内的请求数是否超过限制
        if (static_cast<int>(timestamps.size()) >= max_requests_) {
            return false;
        }

        // 记录当前请求的时间戳
        timestamps.push_back(current_time);
        return true;
    }

    /**
     * @brief 重置指定 IP 的限流记录
     *
     * 清除该 IP 的所有历史请求记录，使其限流计数器归零。
     *
     * @param ip  要重置的客户端 IP 地址
     */
    void reset(const std::string& ip) {
        std::lock_guard<std::mutex> lock(mutex_);
        records_.erase(ip);
    }

    /**
     * @brief 获取指定 IP 在当前窗口内的请求数
     *
     * 注意：该函数仅统计当前记录的数量，不执行过期清理。
     * 返回值可能包含部分已过期但尚未被 allow() 清理的记录。
     *
     * @param ip  客户端 IP 地址
     * @return    当前窗口内的请求数量
     */
    int count(const std::string& ip) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto iterator = records_.find(ip);
        if (iterator == records_.end()) {
            return 0;
        }
        return static_cast<int>(iterator->second.size());
    }

private:
    int max_requests_;  ///< 滑动窗口内允许的最大请求数
    int window_ms_;     ///< 滑动窗口的时间长度（毫秒）

    mutable std::mutex mutex_;  ///< 保护 records_ 的互斥锁

    /// IP 地址到请求时间戳队列的映射表
    /// 每个 IP 对应一个按时间排序的请求时间戳双端队列
    std::unordered_map<
        std::string,
        std::deque<std::chrono::steady_clock::time_point>> records_;
};
