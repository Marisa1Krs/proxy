#pragma once

#include <cstdio>
#include <string>

#include "jwt-cpp/jwt.h"

/**
 * @brief JWT 令牌鉴权工具
 *
 * 使用 jwt-cpp 头文件库（HS256 算法）验证 Bearer Token。
 * HMAC 签名密钥硬编码为 "MarisaProxy_Super_Secret"。
 *
 * 典型用法：
 * @code
 *   std::string token = extract_bearer_token(authorization_header);
 *   if (!verify_token(token)) {
 *       // 返回 401 Unauthorized
 *   }
 *   int64_t user_id = get_user_id(token);  // 可选：提取用户 ID
 * @endcode
 */

/// JWT HMAC 签名密钥（HS256 算法）
static constexpr const char* JWT_SECRET = "MarisaProxy_Super_Secret";

/**
 * @brief 验证 JWT Token 的签名和有效性
 *
 * 使用 HS256 算法和预设的 JWT_SECRET 密钥进行签名验证。
 * 自动检查令牌的过期时间（exp）、签发者（iss）等标准声明。
 *
 * @param token_string  Bearer Token 字符串（不含 "Bearer " 前缀）
 * @return true   鉴权通过，令牌签名有效且未过期
 * @return false  鉴权失败（签名无效、令牌已过期、格式错误等）
 */
inline bool verify_token(const std::string& token_string) {
    try {
        // 第一步：解析 Token 字符串为结构化对象
        auto decoded_token = jwt::decode(token_string);

        // 第二步：构造校验器，指定允许的算法和密码
        auto token_verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::hs256{JWT_SECRET});

        // 第三步：执行校验（自动比对 HMAC 签名、检查 exp 过期时间）
        token_verifier.verify(decoded_token);

        return true;

    } catch (const std::exception& error) {
        fprintf(stderr, "[Auth] 鉴权失败: %s\n", error.what());
        return false;
    }
}

/**
 * @brief 从 JWT Token 中提取用户 ID
 *
 * 从 Token 的 payload 中读取 "user_id" 声明。
 * 注意：user_id 在 JWT payload 中应为数字类型。
 *
 * @param token_string  Bearer Token 字符串
 * @return int64_t  用户 ID 值，提取失败返回 -1
 */
inline int64_t get_user_id(const std::string& token_string) {
    try {
        auto decoded_token = jwt::decode(token_string);
        auto user_id_claim =
            decoded_token.get_payload_claim("user_id");

        // 检查声明类型是否为数字
        if (user_id_claim.get_type() == jwt::json::type::number) {
            return static_cast<int64_t>(user_id_claim.as_integer());
        }

        return -1;

    } catch (const std::exception& error) {
        fprintf(stderr, "[Auth] 获取 user_id 失败: %s\n", error.what());
        return -1;
    }
}

/**
 * @brief 从 Authorization 头中提取 Bearer Token
 *
 * 输入格式应为 "Bearer xxx.yyy.zzz"，
 * 返回值仅为 "xxx.yyy.zzz" 部分（不含 "Bearer " 前缀）。
 *
 * @param auth_header  Authorization 头的完整值
 * @return std::string 提取出的 Token 字符串，
 *                     如果未找到 Bearer Token 则返回空字符串
 */
inline std::string extract_bearer_token(
    const std::string& auth_header) {

    const std::string bearer_prefix = "Bearer ";

    // 检查字符串是否以 "Bearer " 开头
    if (auth_header.size() > bearer_prefix.size() &&
        auth_header.compare(0, bearer_prefix.size(),
                            bearer_prefix) == 0) {
        return auth_header.substr(bearer_prefix.size());
    }

    return "";
}
