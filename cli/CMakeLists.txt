# CLI 测试客户端子项目
# 不依赖 io_uring 和 mylog，仅使用标准 C++ 和 POSIX socket

add_executable(cli_client
    ${CMAKE_CURRENT_SOURCE_DIR}/cli_client.cpp
)

target_link_libraries(cli_client
    pthread
)
