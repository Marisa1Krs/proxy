#include "channel.h"
#include "mylog.h"
#include <unistd.h>
#include <cstring>
#include <iostream>

uint64_t Channel::next_id_ = 1;

Channel::Channel(int client_fd, int backend_fd)
    : client_fd_(client_fd)
    , backend_fd_(backend_fd)
    , closed_(false)
    , id_(next_id_++) {

    // 初始化缓冲区
    std::memset(client_to_backend_buf_, 0, BUFFER_SIZE);
    std::memset(backend_to_client_buf_, 0, BUFFER_SIZE);
    data_len_[0] = 0;
    data_len_[1] = 0;

    LOG_DEBUG("Channel #%lu 创建: client_fd=%d, backend_fd=%d",
              id_, client_fd, backend_fd);
}

Channel::~Channel() {
    if (!closed_) {
        close_both_ends();
    }
    LOG_DEBUG("Channel #%lu 销毁", id_);
}

char* Channel::read_buf(Direction dir) {
    // 读取缓冲区 = 数据来源方的缓冲区
    return (dir == Direction::CLIENT_TO_BACKEND)
               ? client_to_backend_buf_
               : backend_to_client_buf_;
}

char* Channel::write_buf(Direction dir) {
    // 写入缓冲区 = 数据目标方的缓冲区
    // CLIENT_TO_BACKEND: 从客户端读 → 写到后端 → 使用 client_to_backend_buf_
    // BACKEND_TO_CLIENT: 从后端读 → 写到客户端 → 使用 backend_to_client_buf_
    return (dir == Direction::CLIENT_TO_BACKEND)
               ? client_to_backend_buf_
               : backend_to_client_buf_;
}

int Channel::read_fd(Direction dir) const {
    // CLIENT_TO_BACKEND: 从客户端 fd 读取
    // BACKEND_TO_CLIENT: 从后端 fd 读取
    return (dir == Direction::CLIENT_TO_BACKEND) ? client_fd_ : backend_fd_;
}

int Channel::write_fd(Direction dir) const {
    // CLIENT_TO_BACKEND: 向后端 fd 写入
    // BACKEND_TO_CLIENT: 向客户端 fd 写入
    return (dir == Direction::CLIENT_TO_BACKEND) ? backend_fd_ : client_fd_;
}

void Channel::close_both_ends() {
    if (closed_) return;
    closed_ = true;

    if (client_fd_ >= 0) {
        ::close(client_fd_);
        LOG_DEBUG("Channel #%lu 关闭客户端 fd=%d", id_, client_fd_);
        client_fd_ = -1;
    }

    if (backend_fd_ >= 0) {
        ::close(backend_fd_);
        LOG_DEBUG("Channel #%lu 关闭后端 fd=%d", id_, backend_fd_);
        backend_fd_ = -1;
    }

    LOG_INFO("Channel #%lu 已关闭", id_);
}

void Channel::reset_buffers() {
    data_len_[0] = 0;
    data_len_[1] = 0;
    // 不需要 memset 清零，data_len 控制有效数据范围
}
