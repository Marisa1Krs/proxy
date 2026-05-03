cmake_minimum_required(VERSION 3.10)
project(Proxy VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2 -pthread")

# Include directories (项目自身头文件 + liburing 头文件)
include_directories(${PROJECT_SOURCE_DIR}/include)

# 查找 liburing 库（先找 .so 再找版本化文件）
find_library(LIBURING_LIB uring PATHS /usr/lib/x86_64-linux-gnu NO_DEFAULT_PATH)
if(NOT LIBURING_LIB)
    # 如果 -luring 找不到，使用完整路径
    set(LIBURING_LIB "/usr/lib/x86_64-linux-gnu/liburing.so.2")
endif()

# ==================== 网关主程序 ====================
set(GATEWAY_SOURCES
    ${PROJECT_SOURCE_DIR}/src/main.cpp
    ${PROJECT_SOURCE_DIR}/src/gateway.cpp
    ${PROJECT_SOURCE_DIR}/src/channel.cpp
    ${PROJECT_SOURCE_DIR}/src/router.cpp
    ${PROJECT_SOURCE_DIR}/src/mylog.cpp
)

add_executable(${PROJECT_NAME} ${GATEWAY_SOURCES})
target_link_libraries(${PROJECT_NAME}
    ${LIBURING_LIB}
    pthread     # mylog 使用 std::thread 需要 pthread
)

# ==================== CLI 测试客户端（子目录） ====================
add_subdirectory(cli)

# ==================== 调试信息 ====================
message(STATUS "Project: ${PROJECT_NAME}")
message(STATUS "Gateway sources: ${GATEWAY_SOURCES}")
message(STATUS "liburing: ${LIBURING_LIB}")
