cmake_minimum_required(VERSION 3.10)
project(Proxy VERSION 1.0.0 LANGUAGES CXX)

# ==================== 编译选项 ====================

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2 -pthread")

# 项目自身头文件目录（包含 liburing 和 nlohmann/json 的头文件）
include_directories(${PROJECT_SOURCE_DIR}/include)

# ==================== 依赖查找 ====================

# OpenSSL：jwt-cpp 依赖 HMAC/SHA256 等密码学函数
find_package(OpenSSL REQUIRED)

# liburing：io_uring 用户态接口库
# 优先查找动态库，找不到则使用完整路径的版本化库
find_library(LIBURING_LIB uring
    PATHS /usr/lib/x86_64-linux-gnu
    NO_DEFAULT_PATH)
if(NOT LIBURING_LIB)
    set(LIBURING_LIB "/usr/lib/x86_64-linux-gnu/liburing.so.2")
endif()

# ==================== 网关主程序 ====================

set(GATEWAY_SOURCE_FILES
    ${PROJECT_SOURCE_DIR}/src/main.cpp
    ${PROJECT_SOURCE_DIR}/src/gateway.cpp
    ${PROJECT_SOURCE_DIR}/src/worker.cpp
    ${PROJECT_SOURCE_DIR}/src/router.cpp
    ${PROJECT_SOURCE_DIR}/src/route_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/mylog.cpp
)

add_executable(${PROJECT_NAME} ${GATEWAY_SOURCE_FILES})

target_link_libraries(${PROJECT_NAME}
    ${LIBURING_LIB}
    pthread
    OpenSSL::SSL
    OpenSSL::Crypto
)

# ==================== CLI 测试客户端（子目录） ====================

add_subdirectory(cli)

# ==================== 调试信息 ====================

message(STATUS "项目名称: ${PROJECT_NAME}")
message(STATUS "网关源码: ${GATEWAY_SOURCE_FILES}")
message(STATUS "liburing 路径: ${LIBURING_LIB}")
message(STATUS "OpenSSL 版本: ${OPENSSL_VERSION}")
