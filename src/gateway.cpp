#include "gateway.h"
#include "mylog.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

// ==================== 构造 / 析构 ====================

Gateway::Gateway(int client_port, int backend_port, int ring_size)
    : ring_size_(ring_size)
    , listen_fd_client_(-1)
    , listen_fd_backend_(-1)
    , running_(false) {

    // 1. 设置 io_uring 参数
    struct io_uring_params params{};
    std::memset(&params, 0, sizeof(params));

    // 2. 初始化 io_uring
    if (io_uring_queue_init_params(ring_size_, &ring_, &params) < 0) {
        throw std::runtime_error("io_uring_queue_init 失败: " +
                                 std::string(strerror(errno)));
    }

    LOG_INFO("io_uring 初始化成功, ring size=%d, features=0x%x",
             ring_size_, params.features);

    // 3. 创建两个监听端口
    listen_fd_client_ = create_listener(client_port);
    listen_fd_backend_ = create_listener(backend_port);

    LOG_INFO("监听客户端端口: %d (fd=%d)", client_port, listen_fd_client_);
    LOG_INFO("监听后端端口:   %d (fd=%d)", backend_port, listen_fd_backend_);
    LOG_INFO("网关启动完成，等待连接...");
}

Gateway::~Gateway() {
    stop();

    // 关闭所有 Channel
    for (auto& [fd, ch] : channels_by_client_) {
        delete ch;
    }
    channels_by_client_.clear();
    channels_by_backend_.clear();

    // 关闭监听套接字
    if (listen_fd_client_ >= 0) {
        ::close(listen_fd_client_);
        listen_fd_client_ = -1;
    }
    if (listen_fd_backend_ >= 0) {
        ::close(listen_fd_backend_);
        listen_fd_backend_ = -1;
    }

    // 销毁 io_uring
    io_uring_queue_exit(&ring_);

    LOG_INFO("网关已关闭");
}

// ==================== 公共接口 ====================

void Gateway::run() {
    running_ = true;

    // 提交初始 Accept 请求
    submit_accept(listen_fd_client_, OpType::ACCEPT_CLIENT);
    submit_accept(listen_fd_backend_, OpType::ACCEPT_BACKEND);

    while (running_) {
        // 批量提交所有待处理的 SQE
        int submitted = io_uring_submit(&ring_);
        if (submitted < 0) {
            LOG_ERROR("io_uring_submit 失败: %s", strerror(-submitted));
            continue;
        }

        // 等待内核完成事件
        io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret < 0) {
            if (ret == -EINTR) continue;  // 被信号中断，重试
            LOG_ERROR("io_uring_wait_cqe 失败: %s", strerror(-ret));
            break;
        }

        // 取出 IOContext
        auto* ctx = static_cast<IOContext*>(io_uring_cqe_get_data(cqe));
        int res = cqe->res;  // 操作结果

        // 标记 CQE 已消费
        io_uring_cqe_seen(&ring_, cqe);

        // 处理完成事件
        if (res < 0) {
            // 操作失败
            LOG_WARN("%s 操作失败 (fd=%d): %s",
                     op_type_str(ctx->op), ctx->fd, strerror(-res));

            // 如果是 Channel 相关操作失败，关闭 Channel
            if (ctx->channel) {
                close_channel(ctx->channel);
            }
            destroy_io_context(ctx);
            continue;
        }

        // 根据操作类型分发
        switch (ctx->op) {
            case OpType::ACCEPT_CLIENT:
            case OpType::ACCEPT_BACKEND:
                handle_accept(ctx, res);
                break;
            case OpType::READ_CLIENT:
                handle_client_read(ctx, res);
                break;
            case OpType::WRITE_CLIENT:
                handle_client_write(ctx);
                break;
            case OpType::READ_BACKEND:
                handle_backend_read(ctx, res);
                break;
            case OpType::WRITE_BACKEND:
                handle_backend_write(ctx);
                break;
            case OpType::BACKEND_REGISTER:
                handle_backend_register(ctx, res);
                break;
        }
    }
}

void Gateway::stop() {
    running_ = false;
}

// ==================== 内部方法：监听初始化 ====================

int Gateway::create_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        throw std::runtime_error("socket 创建失败: " +
                                 std::string(strerror(errno)));
    }

    // SO_REUSEADDR：允许端口重用
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        ::close(fd);
        throw std::runtime_error("setsockopt SO_REUSEADDR 失败: " +
                                 std::string(strerror(errno)));
    }

    struct sockaddr_in addr{};
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("bind 端口 " + std::to_string(port) +
                                 " 失败: " + std::string(strerror(errno)));
    }

    if (listen(fd, BACKLOG) < 0) {
        ::close(fd);
        throw std::runtime_error("listen 失败: " +
                                 std::string(strerror(errno)));
    }

    return fd;
}

// ==================== 内部方法：提交 I/O 请求 ====================

void Gateway::submit_accept(int listen_fd, OpType op_type) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_WARN("获取 SQE 失败 (accept, fd=%d)", listen_fd);
        return;
    }

    auto* ctx = new IOContext(op_type, listen_fd);
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, ctx);
}

void Gateway::submit_read(int fd, char* buf, size_t len, OpType op_type,
                          Channel* ch) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_WARN("获取 SQE 失败 (read, fd=%d)", fd);
        return;
    }

    auto* ctx = new IOContext(op_type, fd, ch, buf, len);
    io_uring_prep_read(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ctx);
}

void Gateway::submit_write(int fd, const char* buf, size_t len, OpType op_type,
                           Channel* ch) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_WARN("获取 SQE 失败 (write, fd=%d)", fd);
        return;
    }

    // write 需要一个非 const 缓冲区（liburing 接口要求），但实际不会修改
    auto* ctx = new IOContext(op_type, fd, ch, const_cast<char*>(buf), len);
    io_uring_prep_write(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, ctx);
}

// ==================== 事件处理器 ====================

void Gateway::handle_accept(IOContext* ctx, int new_fd) {
    OpType op_type = ctx->op;
    int listen_fd = ctx->fd;

    // 删除旧的 IOContext（accept 已完成）
    destroy_io_context(ctx);

    // 继续接受下一个连接
    submit_accept(listen_fd, op_type);

    // 设置新连接为非阻塞
    int flags = fcntl(new_fd, F_GETFL, 0);
    fcntl(new_fd, F_SETFL, flags | O_NONBLOCK);

    if (op_type == OpType::ACCEPT_CLIENT) {
        // 客户端连接：创建 Channel，开始读取请求
        LOG_INFO("新客户端连接: fd=%d", new_fd);

        auto* ch = new Channel(new_fd);
        channels_by_client_[new_fd] = ch;

        // 提交读取客户端请求
        submit_read(new_fd, ch->read_buf(Channel::Direction::CLIENT_TO_BACKEND),
                    Channel::BUFFER_SIZE - 1, OpType::READ_CLIENT, ch);

    } else {
        // 后端连接：读取注册消息
        LOG_INFO("新后端连接: fd=%d", new_fd);

        // 为后端分配一个临时缓冲区（4096 字节，用于注册消息）
        auto* reg_buf = new char[Channel::BUFFER_SIZE];
        auto* reg_ctx = new IOContext(OpType::BACKEND_REGISTER, new_fd,
                                      nullptr, reg_buf, Channel::BUFFER_SIZE);
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe) {
            io_uring_prep_read(sqe, new_fd, reg_buf, Channel::BUFFER_SIZE - 1, 0);
            io_uring_sqe_set_data(sqe, reg_ctx);
        } else {
            delete[] reg_buf;
            delete reg_ctx;
            ::close(new_fd);
        }
    }
}

void Gateway::handle_client_read(IOContext* ctx, int bytes_read) {
    if (bytes_read == 0) {
        // 客户端断开连接
        LOG_INFO("客户端断开: fd=%d", ctx->fd);
        if (ctx->channel) {
            close_channel(ctx->channel);
        }
        destroy_io_context(ctx);
        return;
    }

    Channel* ch = ctx->channel;
    if (!ch) {
        destroy_io_context(ctx);
        return;
    }

    // 保存数据长度
    ch->set_data_len(Channel::Direction::CLIENT_TO_BACKEND, bytes_read);
    destroy_io_context(ctx);

    // 检查是否已经有配对的 backend
    if (ch->backend_fd() < 0) {
        // 尝试从 Trie 路由表中分配后端
        if (!assign_backend_to_channel(ch)) {
            // 没有可用后端，返回错误给客户端
            const char* err_msg =
                "HTTP/1.1 502 Bad Gateway\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 22\r\n"
                "Connection: close\r\n"
                "\r\n"
                "No available backend.\r\n";

            LOG_WARN("Channel #%lu 无可用后端，返回 502", ch->id());
            submit_write(ch->client_fd(), err_msg, std::strlen(err_msg),
                         OpType::WRITE_CLIENT, ch);
            return;
        }
    }

    // 提取路径并确认路由仍然有效
    std::string path = extract_path(ch->read_buf(Channel::Direction::CLIENT_TO_BACKEND),
                                    bytes_read);
    int backend_fd = router_.lookup(path);

    if (backend_fd < 0 || backend_fd != ch->backend_fd()) {
        // 路由已变更或后端已断开
        const char* err_msg =
            "HTTP/1.1 502 Bad Gateway\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 21\r\n"
            "Connection: close\r\n"
            "\r\n"
            "Backend unavailable.\r\n";

        LOG_WARN("Channel #%lu 后端不可用 (path=%s), 返回 502", ch->id(), path.c_str());
        submit_write(ch->client_fd(), err_msg, std::strlen(err_msg),
                     OpType::WRITE_CLIENT, ch);
        return;
    }

    LOG_DEBUG("Channel #%lu 转发请求: path=%s, %d 字节 -> 后端 fd=%d",
              ch->id(), path.c_str(), bytes_read, backend_fd);

    // 转发请求数据到后端
    submit_write(ch->backend_fd(),
                 ch->read_buf(Channel::Direction::CLIENT_TO_BACKEND),
                 bytes_read, OpType::WRITE_BACKEND, ch);
}

void Gateway::handle_client_write(IOContext* ctx) {
    Channel* ch = ctx->channel;
    destroy_io_context(ctx);

    if (!ch || ch->is_closed()) {
        // Channel 已关闭或已标记关闭
        delete ch;
        return;
    }

    // 响应已写回客户端，关闭连接（短连接模式）
    LOG_DEBUG("Channel #%lu 响应已写回客户端, 关闭连接", ch->id());
    close_channel(ch);
}

void Gateway::handle_backend_read(IOContext* ctx, int bytes_read) {
    if (bytes_read == 0) {
        // 后端断开连接
        LOG_INFO("后端断开: fd=%d", ctx->fd);
        if (ctx->channel) {
            close_channel(ctx->channel);
        }
        destroy_io_context(ctx);
        return;
    }

    Channel* ch = ctx->channel;
    if (!ch) {
        destroy_io_context(ctx);
        return;
    }

    // 保存数据长度
    ch->set_data_len(Channel::Direction::BACKEND_TO_CLIENT, bytes_read);
    destroy_io_context(ctx);

    LOG_DEBUG("Channel #%lu 收到后端响应: %d 字节 -> 转发回客户端",
              ch->id(), bytes_read);

    // 将后端响应转发回客户端
    submit_write(ch->client_fd(),
                 ch->read_buf(Channel::Direction::BACKEND_TO_CLIENT),
                 bytes_read, OpType::WRITE_CLIENT, ch);
}

void Gateway::handle_backend_write(IOContext* ctx) {
    Channel* ch = ctx->channel;
    destroy_io_context(ctx);

    if (!ch || ch->is_closed()) {
        delete ch;
        return;
    }

    LOG_DEBUG("Channel #%lu 请求已转发到后端, 读取响应", ch->id());

    // 请求已转发到后端，现在读取后端响应
    submit_read(ch->backend_fd(),
                ch->read_buf(Channel::Direction::BACKEND_TO_CLIENT),
                Channel::BUFFER_SIZE - 1, OpType::READ_BACKEND, ch);
}

void Gateway::handle_backend_register(IOContext* ctx, int bytes_read) {
    if (bytes_read <= 0) {
        int err_fd = ctx->fd;
        LOG_WARN("后端注册失败: fd=%d, bytes=%d", err_fd, bytes_read);
        if (ctx->buf) delete[] ctx->buf;
        destroy_io_context(ctx);
        ::close(err_fd);
        return;
    }

    int backend_fd = ctx->fd;
    char* buf = ctx->buf;
    buf[bytes_read] = '\0';  // 确保字符串终止

    // 解析注册消息
    // 格式: "REGISTER /path/prefix\n" 或 "/path/prefix\n"
    std::string msg(buf);
    std::string prefix;

    // 去除末尾的换行符/回车符
    while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r')) {
        msg.pop_back();
    }

    if (msg.find("REGISTER ") == 0) {
        prefix = msg.substr(9);  // 跳过 "REGISTER "
    } else {
        prefix = msg;  // 直接是路径前缀
    }

    // 验证路径前缀格式
    if (prefix.empty() || prefix[0] != '/') {
        LOG_WARN("无效的注册路径: \"%s\" (fd=%d)", prefix.c_str(), backend_fd);
        delete[] buf;
        destroy_io_context(ctx);
        ::close(backend_fd);
        return;
    }

    // 注册到路由表
    router_.insert(prefix, backend_fd);
    LOG_INFO("后端注册成功: fd=%d, prefix=\"%s\"", backend_fd, prefix.c_str());

    // 打印当前路由表
    auto routes = router_.get_all_routes();
    LOG_INFO("当前路由表 (%zu 条):", routes.size());
    for (const auto& [p, fd] : routes) {
        LOG_INFO("  %s -> fd=%d", p.c_str(), fd);
    }

    delete[] buf;
    destroy_io_context(ctx);

    // 后端注册完成后，保持连接存活
    // 注意：此简化版本中，后端连接保持打开，后续转发时直接使用
}

// ==================== 辅助方法 ====================

bool Gateway::assign_backend_to_channel(Channel* ch) {
    if (!ch || ch->client_fd() < 0) return false;

    // 从客户端缓冲区提取路径
    char* buf = ch->read_buf(Channel::Direction::CLIENT_TO_BACKEND);
    int len = ch->data_len(Channel::Direction::CLIENT_TO_BACKEND);
    if (len <= 0) return false;

    std::string path = extract_path(buf, len);
    if (path.empty()) return false;

    // 在路由表中查找后端
    int backend_fd = router_.lookup(path);
    if (backend_fd < 0) return false;

    // 分配后端给 Channel
    ch->set_backend_fd(backend_fd);
    channels_by_backend_[backend_fd] = ch;

    LOG_INFO("Channel #%lu 分配后端: path=\"%s\", backend_fd=%d",
             ch->id(), path.c_str(), backend_fd);
    return true;
}

std::string Gateway::extract_path(const char* buf, int len) {
    if (!buf || len <= 0) return "";

    // 简单解析 HTTP 请求的第一行: "METHOD /path HTTP/1.1"
    // 找到第一个空格后的内容
    std::string request(buf, len);

    // 找到第一个空格（METHOD 后的空格）
    size_t first_space = request.find(' ');
    if (first_space == std::string::npos) return "";

    // 找到第二个空格（path 后的空格）
    size_t second_space = request.find(' ', first_space + 1);
    if (second_space == std::string::npos) {
        // 有些请求可能没有协议版本
        return request.substr(first_space + 1);
    }

    return request.substr(first_space + 1, second_space - first_space - 1);
}

void Gateway::close_channel(Channel* ch) {
    if (!ch || ch->is_closed()) return;

    LOG_INFO("关闭 Channel #%lu", ch->id());

    // 从映射表中移除
    int client_fd = ch->client_fd();
    int backend_fd = ch->backend_fd();

    if (client_fd >= 0) {
        channels_by_client_.erase(client_fd);
    }
    if (backend_fd >= 0) {
        channels_by_backend_.erase(backend_fd);
    }

    // 关闭两端连接
    ch->close_both_ends();
    delete ch;
}

void Gateway::destroy_io_context(IOContext* ctx) {
    delete ctx;
}

const char* Gateway::op_type_str(OpType op) {
    switch (op) {
        case OpType::ACCEPT_CLIENT:    return "ACCEPT_CLIENT";
        case OpType::ACCEPT_BACKEND:   return "ACCEPT_BACKEND";
        case OpType::READ_CLIENT:      return "READ_CLIENT";
        case OpType::WRITE_CLIENT:     return "WRITE_CLIENT";
        case OpType::READ_BACKEND:     return "READ_BACKEND";
        case OpType::WRITE_BACKEND:    return "WRITE_BACKEND";
        case OpType::BACKEND_REGISTER: return "BACKEND_REGISTER";
        default:                       return "UNKNOWN";
    }
}
