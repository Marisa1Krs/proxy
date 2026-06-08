#include "worker.h"
#include "mylog.h"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <map>
#include <vector>

// ==================== 构造 / 析构 ====================
//
// Worker 构造函数：初始化配置参数、标记所有缓冲区为可用状态。
// 注意：io_uring 和监听套接字的初始化在 worker_loop() 中完成，
// 因为后者运行在 Worker 线程上下文中。
//

Worker::Worker(int client_port, int backend_port, int ring_size, int worker_id,
               int health_check_port)
    : init_code_(INIT_OK)
    , worker_id_(worker_id)
    , client_port_(client_port)
    , backend_port_(backend_port)
    , ring_size_(ring_size)
    , health_check_port_(health_check_port)
    , listen_fd_client_(-1)
    , listen_fd_backend_(-1)
    , thread_handle_(0)
    , running_(false) {

    // 初始化缓冲区空闲标记，所有缓冲区初始都可用
    for (int buffer_index = 0; buffer_index < NUM_BUFFERS; ++buffer_index) {
        buffer_free_[buffer_index] = true;
    }
}

Worker::~Worker() {
    stop();

    // ---- 清理流程 ----
    // 1. 关闭所有连接上下文中的 socket fd（跳过监听套接字）
    // 2. 关闭监听套接字
    // 3. 退出 io_uring 队列
    for (auto& [fd, context] : ctx_by_fd_) {
        if (fd == listen_fd_client_ || fd == listen_fd_backend_) {
            continue;
        }
        if (context->fd >= 0) {
            ::close(context->fd);
        }
        delete context;
    }
    ctx_by_fd_.clear();

    // 关闭监听套接字
    if (listen_fd_client_ >= 0) {
        ::close(listen_fd_client_);
        listen_fd_client_ = -1;
    }
    if (listen_fd_backend_ >= 0) {
        ::close(listen_fd_backend_);
        listen_fd_backend_ = -1;
    }

    io_uring_queue_exit(&ring_);
    LOG_INFO("[Worker %d] 已关闭", worker_id_);
}

// ==================== CPU 亲和性 ====================

void Worker::set_cpu_affinity(const std::string& mask) {
    cpu_affinity_mask_ = mask;
}

// ==================== 工作线程主循环 ====================
//
// worker_loop() 是每个 Worker 线程的入口函数，执行以下步骤：
//   0. 绑定 CPU 亲和性（如果配置了掩码）
//   1. 初始化 io_uring 实例（队列深度 = ring_size_）
//   2. 初始化 Provided Buffers 池（64 个 4KB 缓冲区）
//   3. 创建两个 SO_REUSEPORT 监听套接字（客户端端口 + 后端端口）
//   4. 初始化 Accept 上下文
//   5. 提交初始 Accept 请求（开始接受新连接）
//   6. 初始化健康检查定时器
//   7. 进入事件循环（io_uring_submit → io_uring_wait_cqe → 状态机派发）
//
// 注意：io_uring 是异步 I/O 框架，所有 I/O 操作（accept/read/write）
// 都通过提交 SQE（Submission Queue Entry）发起，通过 CQE（Completion
// Queue Entry）获取结果。状态机驱动每个连接的生命周期。
//

void Worker::worker_loop() {
    // 记录当前线程句柄，用于 Gateway::stop() 发送唤醒信号
    thread_handle_ = pthread_self();

    // ---- 0. 绑定 CPU 亲和性 ----
    if (!cpu_affinity_mask_.empty()) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);

        // 解析二进制掩码字符串：从低位到高位对应 CPU 0~N
        // 例如 "0001" → CPU 0, "0010" → CPU 1, "0101" → CPU 0 + CPU 2
        int mask_length = static_cast<int>(cpu_affinity_mask_.size());
        for (int bit_index = 0; bit_index < mask_length; ++bit_index) {
            // cpu_affinity_mask_ 是字符串如 "0001"，最右侧字符是最低位（CPU 0）
            if (cpu_affinity_mask_[mask_length - 1 - bit_index] == '1') {
                CPU_SET(bit_index, &cpuset);
            }
        }

        int affinity_result = pthread_setaffinity_np(
            thread_handle_, sizeof(cpuset), &cpuset);
        if (affinity_result == 0) {
            LOG_INFO("[Worker %d] 绑定 CPU 亲和性: mask=%s",
                     worker_id_, cpu_affinity_mask_.c_str());
        } else {
            LOG_WARN("[Worker %d] 绑定 CPU 亲和性失败 (mask=%s): %s",
                     worker_id_, cpu_affinity_mask_.c_str(),
                     strerror(affinity_result));
        }
    }

    // ---- 1. 初始化 io_uring ----
    struct io_uring_params params{};
    std::memset(&params, 0, sizeof(params));

    if (io_uring_queue_init_params(ring_size_, &ring_, &params) < 0) {
        LOG_ERROR("[Worker %d] io_uring_queue_init 失败: %s",
                  worker_id_, strerror(errno));
        init_code_ = INIT_URING_ERR;
        return;
    }
    LOG_INFO("[Worker %d] io_uring 初始化成功, ring size=%d, features=0x%x",
             worker_id_, ring_size_, params.features);

    // ---- 2. 初始化 Provided Buffers 池 ----
    init_buffer_pool();

    // ---- 3. 创建两个监听端口（SO_REUSEPORT） ----
    listen_fd_client_ = create_listener(client_port_);
    if (listen_fd_client_ < 0) {
        LOG_ERROR("[Worker %d] 创建客户端监听端口 %d 失败", worker_id_, client_port_);
        io_uring_queue_exit(&ring_);
        init_code_ = INIT_CLIENT_ERR;
        return;
    }

    listen_fd_backend_ = create_listener(backend_port_);
    if (listen_fd_backend_ < 0) {
        LOG_ERROR("[Worker %d] 创建后端监听端口 %d 失败", worker_id_, backend_port_);
        ::close(listen_fd_client_);
        listen_fd_client_ = -1;
        io_uring_queue_exit(&ring_);
        init_code_ = INIT_BACKEND_ERR;
        return;
    }

    LOG_INFO("[Worker %d] 监听客户端端口: %d (fd=%d), 后端端口: %d (fd=%d)",
             worker_id_, client_port_, listen_fd_client_,
             backend_port_, listen_fd_backend_);

    // ---- 4. 初始化 Accept 上下文 ----
    accept_client_ctx_.state = RequestContext::ACCEPTING;
    accept_client_ctx_.fd = listen_fd_client_;
    accept_backend_ctx_.state = RequestContext::ACCEPTING;
    accept_backend_ctx_.fd = listen_fd_backend_;

    // ---- 5. 提交初始 Accept 请求 ----
    submit_accept(&accept_client_ctx_);
    submit_accept(&accept_backend_ctx_);

    // ---- 6. 初始化健康检查定时器 ----
    health_check_timer_ctx_.state = RequestContext::HEALTH_CHECK_TIMER;
    health_check_timer_ctx_.fd = -1;
    submit_health_check_timeout();

    // ---- 7. 进入事件循环 ----
    running_ = true;
    LOG_INFO("[Worker %d] 开始事件循环", worker_id_);

    //
    // ================ 核心事件循环 ================
    //
    // 循环流程：
    //   1. io_uring_submit()  → 批量提交所有待处理的 SQE
    //   2. io_uring_wait_cqe() → 等待内核完成至少一个操作
    //   3. 根据 CQE 中的上下文状态，派发给对应的事件处理器
    //
    // 状态机派发优先级：
    //   a) ACCEPTING（新连接）→ handle_accept()
    //   b) HEALTH_CHECK_TIMER → perform_health_checks()
    //   c) 操作失败（res < 0）→ close_connection()
    //   d) READING_CLIENT/READING_BACKEND:
    //      从 completion_flags 中提取 IOSQE_BUFFER_SELECT 分配的 buffer_id
    //   e) WRITING_BACKEND / WRITING_CLIENT / BACKEND_REGISTER
    //
    // 注意：READING_CLIENT 和 READING_BACKEND 使用 IOSQE_BUFFER_SELECT，
    // 由内核自动提供缓冲区，无需用户预先分配。CQE 的 flags 高位 16 位存
    // 储 buffer_id，用于回收缓冲区。
    //
    while (running_) {
        // 批量提交所有待处理的 SQE
        // io_uring_submit() 会将 SQ（Submission Queue）中所有待处理的
        // SQE 一次性提交给内核，减少系统调用次数。
        int submitted_count = io_uring_submit(&ring_);
        if (submitted_count < 0) {
            LOG_ERROR("[Worker %d] io_uring_submit 失败: %s",
                      worker_id_, strerror(-submitted_count));
            continue;
        }

        // 等待内核完成事件
        // io_uring_wait_cqe() 阻塞直到至少有一个 CQE 可用。
        // 当 Gateway::stop() 发送 SIGUSR1 时，wait 会被 -EINTR 中断，
        // 此时检查 running_ 标志决定是否退出循环。
        io_uring_cqe* completion_event;
        int wait_result = io_uring_wait_cqe(&ring_, &completion_event);
        if (wait_result < 0) {
            if (wait_result == -EINTR) {
                // 被信号中断（如 SIGUSR1 唤醒），继续检查 running_ 标志
                continue;
            }
            LOG_ERROR("[Worker %d] io_uring_wait_cqe 失败: %s",
                      worker_id_, strerror(-wait_result));
            break;
        }

        // 从 CQE 中取出上下文指针和操作结果
        // io_uring_cqe_get_data() 返回提交 SQE 时通过 io_uring_sqe_set_data()
        // 设置的 RequestContext 指针。
        auto* request_context =
            static_cast<RequestContext*>(io_uring_cqe_get_data(completion_event));
        int operation_result   = completion_event->res;   // 操作返回值（如 read 字节数、accept 新 fd）
        int completion_flags   = completion_event->flags;  // 完成标志（含 buffer_id）
        io_uring_cqe_seen(&ring_, completion_event);        // 通知内核该 CQE 已消费

        // 跳过没有上下文的完成事件
        // PROVIDE_BUFFERS 操作（缓冲区回收）的 data 为 nullptr，无需处理
        if (request_context == nullptr) {
            continue;
        }

        // ========== 状态机派发 ==========
        //
        // 根据请求上下文的当前 state 字段进行条件判断：
        //   第一优先级：ACCEPTING（新连接接入）
        //   第二优先级：HEALTH_CHECK_TIMER（定时健康检查）
        //   第三优先级：操作结果检查（res < 0 则关闭连接）
        //   第四优先级：具体状态派发（switch）

        // --- 第一优先级：Accept 完成 ---
        // ACCEPTING 上下文是成员变量（accept_client_ctx_ / accept_backend_ctx_），
        // 不通过 heap 分配。accept 成功返回新连接的 fd。
        if (request_context->state == RequestContext::ACCEPTING) {
            if (operation_result < 0) {
                LOG_WARN("[Worker %d] accept 失败 (fd=%d): %s",
                         worker_id_, request_context->fd, strerror(-operation_result));
                submit_accept(request_context);  // 重试
            } else {
                handle_accept(request_context, operation_result);
            }
            continue;
        }

        // --- 第二优先级：健康检查定时器 ---
        // io_uring 的 IORING_OP_TIMEOUT 到期后触发。
        // 每次触发后重新提交下一次超时请求。
        if (request_context->state == RequestContext::HEALTH_CHECK_TIMER) {
            if (running_) {
                perform_health_checks();
                submit_health_check_timeout();
            }
            continue;
        }

        // --- 第三优先级：操作失败处理 ---
        // 所有非 ACCEPTING/HEALTH_CHECK 操作失败（如 read/write 返回负值）
        // 统一走 close_connection() 清理资源。
        if (operation_result < 0) {
            LOG_WARN("[Worker %d] %s 操作失败 (fd=%d): %s",
                     worker_id_, state_name(request_context->state),
                     request_context->fd, strerror(-operation_result));
            close_connection(request_context);
            continue;
        }

        // --- 第四优先级：根据当前状态派发 ---
        // 各状态对应的事件处理器：
        //   READING_CLIENT    → handle_client_read()   解析 HTTP 请求，转发到后端
        //   WRITING_BACKEND   → handle_backend_write()  处理 partial write
        //   READING_BACKEND   → handle_backend_read()   接收后端响应，写回客户端
        //   WRITING_CLIENT    → handle_client_write()   写完后 keep-alive / 关闭
        //   BACKEND_REGISTER  → handle_backend_register() 解析 REGISTER 协议
        //
        // 对于使用 IOSQE_BUFFER_SELECT 的读操作（READING_CLIENT 和
        // READING_BACKEND），内核自动分配缓冲区，buffer_id 存储在
        // CQE flags 的高 16 位，需提取后设置到 context 中。
        switch (request_context->state) {
            case RequestContext::READING_CLIENT: {
                // 从 completion_flags 提取 IOSQE_BUFFER_SELECT 分配的 buffer_id
                int buffer_id = (completion_flags >> 16) & 0xFFFF;
                request_context->bid = buffer_id;
                request_context->buffer = buffer_from_bid(buffer_id);
                handle_client_read(request_context, operation_result);
                break;
            }
            case RequestContext::WRITING_BACKEND:
                // 客户端的请求数据已提交到后端；处理 partial write 或发起后端 read
                handle_backend_write(request_context, operation_result);
                break;

            case RequestContext::READING_BACKEND: {
                // 同上，提取 buffer_id
                int buffer_id = (completion_flags >> 16) & 0xFFFF;
                request_context->bid = buffer_id;
                request_context->buffer = buffer_from_bid(buffer_id);
                handle_backend_read(request_context, operation_result);
                break;
            }
            case RequestContext::WRITING_CLIENT:
                // 后端响应已写回客户端；处理 partial write 或 keep-alive 逻辑
                handle_client_write(request_context, operation_result);
                break;

            case RequestContext::BACKEND_REGISTER: {
                // 后端注册消息读取完成；解析 REGISTER 协议
                int buffer_id = (completion_flags >> 16) & 0xFFFF;
                request_context->bid = buffer_id;
                request_context->buffer = buffer_from_bid(buffer_id);
                handle_backend_register(request_context, operation_result);
                break;
            }
            default:
                LOG_WARN("[Worker %d] 未知状态: %d (fd=%d)",
                         worker_id_, request_context->state, request_context->fd);
                close_connection(request_context);
                break;
        }
    }

    LOG_INFO("[Worker %d] 事件循环已退出", worker_id_);
}

// ==================== 监听初始化（SO_REUSEPORT） ====================

int Worker::create_listener(int port) {
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd < 0) {
        LOG_ERROR("[Worker %d] socket 创建失败 (端口 %d): %s",
                  worker_id_, port, strerror(errno));
        return -1;
    }

    int socket_option = 1;

    // SO_REUSEADDR：允许地址重用
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR,
                   &socket_option, sizeof(socket_option)) < 0) {
        LOG_ERROR("[Worker %d] setsockopt SO_REUSEADDR 失败 (端口 %d): %s",
                  worker_id_, port, strerror(errno));
        ::close(listen_fd);
        return -1;
    }

    // SO_REUSEPORT：允许多个进程/线程绑定同一端口
    // 内核会在所有监听同一端口的 Worker 之间均匀分发新连接
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT,
                   &socket_option, sizeof(socket_option)) < 0) {
        LOG_ERROR("[Worker %d] setsockopt SO_REUSEPORT 失败 (端口 %d): %s",
                  worker_id_, port, strerror(errno));
        ::close(listen_fd);
        return -1;
    }

    struct sockaddr_in address{};
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        LOG_ERROR("[Worker %d] bind 端口 %d 失败: %s",
                  worker_id_, port, strerror(errno));
        ::close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, BACKLOG) < 0) {
        LOG_ERROR("[Worker %d] listen 失败 (端口 %d): %s",
                  worker_id_, port, strerror(errno));
        ::close(listen_fd);
        return -1;
    }

    return listen_fd;
}

// ==================== 缓冲区池管理 ====================

void Worker::init_buffer_pool() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_ERROR("[Worker %d] 获取 SQE 失败，无法初始化缓冲池", worker_id_);
        return;
    }

    io_uring_prep_provide_buffers(
        sqe, buffers_, BUFFER_SIZE, NUM_BUFFERS, BUF_GROUP, 0);
    io_uring_sqe_set_data(sqe, nullptr);

    LOG_INFO("[Worker %d] 缓冲池初始化: %d 个 %d 字节缓冲区 (组 %d)",
             worker_id_, NUM_BUFFERS, BUFFER_SIZE, BUF_GROUP);
}

void Worker::provide_buffer_to_kernel(int buffer_id) {
    if (buffer_id < 0 || buffer_id >= NUM_BUFFERS) {
        return;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_ERROR("[Worker %d] 获取 SQE 失败，无法回收缓冲区 bid=%d",
                  worker_id_, buffer_id);
        return;
    }

    io_uring_prep_provide_buffers(
        sqe, buffers_[buffer_id], BUFFER_SIZE, 1, BUF_GROUP, buffer_id);
    io_uring_sqe_set_data(sqe, nullptr);
    buffer_free_[buffer_id] = false;
}

void Worker::submit_recycle_buffer(int buffer_id) {
    if (buffer_id < 0 || buffer_id >= NUM_BUFFERS) {
        return;
    }
    provide_buffer_to_kernel(buffer_id);
}

// ==================== 提交 I/O 请求 ====================

void Worker::submit_accept(RequestContext* context) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_ERROR("[Worker %d] 获取 SQE 失败，无法提交 accept", worker_id_);
        return;
    }

    io_uring_prep_accept(sqe, context->fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, context);
}

void Worker::submit_read_buf_select(RequestContext* context) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_ERROR("[Worker %d] 获取 SQE 失败，无法提交 read", worker_id_);
        return;
    }

    io_uring_prep_recv(sqe, context->fd, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = BUF_GROUP;
    io_uring_sqe_set_data(sqe, context);
}

void Worker::submit_write(RequestContext* context,
                          const char* buffer, int length) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_ERROR("[Worker %d] 获取 SQE 失败，无法提交 write", worker_id_);
        return;
    }

    io_uring_prep_send(sqe, context->fd, buffer, length, 0);
    io_uring_sqe_set_data(sqe, context);
}

void Worker::submit_write_remain(RequestContext* context) {
    const char* remaining_buffer =
        context->buffer + context->bytes_written;
    int remaining_length =
        context->total_bytes - context->bytes_written;

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_ERROR("[Worker %d] 获取 SQE 失败，无法提交 partial write", worker_id_);
        return;
    }

    io_uring_prep_send(sqe, context->fd,
                       remaining_buffer, remaining_length, 0);
    io_uring_sqe_set_data(sqe, context);
}

// ==================== HTTP 解析工具 ====================

void Worker::parse_http_request(const char* request_buffer, int buffer_length,
                                std::string& method, std::string& path,
                                std::string& auth_header) {
    method.clear();
    path.clear();
    auth_header.clear();

    if (buffer_length <= 0) {
        return;
    }

    // 将缓冲区转为可安全处理的字符串（截断到有效长度）
    std::string raw_request(request_buffer, buffer_length);

    // ---- 解析请求行: "METHOD /path HTTP/1.1\r\n" ----
    size_t line_end_position = raw_request.find("\r\n");
    if (line_end_position == std::string::npos) {
        return;
    }

    std::string request_line =
        raw_request.substr(0, line_end_position);

    // 提取 METHOD（第一个空格之前的部分）
    size_t first_space_position = request_line.find(' ');
    if (first_space_position == std::string::npos) {
        return;
    }
    method = request_line.substr(0, first_space_position);

    // 提取 PATH（第一个空格和第二个空格之间的部分）
    size_t second_space_position =
        request_line.find(' ', first_space_position + 1);
    if (second_space_position == std::string::npos) {
        return;
    }
    path = request_line.substr(
        first_space_position + 1,
        second_space_position - first_space_position - 1);

    // ---- 解析头部: "Key: Value\r\n" ----
    size_t header_scan_position = line_end_position + 2;  // 跳过请求行后的 \r\n
    while (header_scan_position < raw_request.size()) {
        size_t header_end_position =
            raw_request.find("\r\n", header_scan_position);
        if (header_end_position == std::string::npos) {
            break;
        }

        // 空行表示头部结束
        if (header_end_position == header_scan_position) {
            break;
        }

        std::string header_line = raw_request.substr(
            header_scan_position,
            header_end_position - header_scan_position);

        // 查找 ": " 分隔符
        size_t colon_position = header_line.find(": ");
        if (colon_position != std::string::npos) {
            std::string header_key =
                header_line.substr(0, colon_position);
            std::string header_value =
                header_line.substr(colon_position + 2);

            // 查找 Authorization 头（区分大小写兼容）
            if (header_key.size() == 15 &&
                (header_key == "Authorization" ||
                 header_key == "authorization")) {

                // 提取 Bearer Token（去掉 "Bearer " 前缀）
                const std::string bearer_prefix = "Bearer ";
                if (header_value.size() > bearer_prefix.size() &&
                    header_value.compare(0, bearer_prefix.size(),
                                         bearer_prefix) == 0) {
                    auth_header =
                        header_value.substr(bearer_prefix.size());
                }
            }
        }

        header_scan_position = header_end_position + 2;
    }
}

void Worker::send_http_error(RequestContext* context,
                             int status_code,
                             const char* status_text,
                             const char* body) {
    int total_length = snprintf(context->buffer, BUFFER_SIZE,
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n"
        "%s",
        status_code, status_text,
        strlen(body),
        body);

    if (total_length <= 0) {
        LOG_ERROR("[Worker %d] send_http_error snprintf 失败", worker_id_);
        close_connection(context);
        return;
    }

    context->state = RequestContext::WRITING_CLIENT;
    context->fd = context->client_fd;
    context->total_bytes = total_length;
    context->bytes_written = 0;
    submit_write(context, context->buffer, total_length);
}

// ==================== 事件处理器 ====================

void Worker::handle_accept(RequestContext* listen_context, int new_fd) {
    bool is_client_connection =
        (listen_context == &accept_client_ctx_);

    LOG_INFO("[Worker %d] 新连接 %s, fd=%d",
             worker_id_,
             is_client_connection ? "客户端" : "后端",
             new_fd);

    // 获取客户端 IP 地址（用于后续限流）
    std::string client_ip;
    if (is_client_connection) {
        struct sockaddr_in peer_address{};
        socklen_t address_length = sizeof(peer_address);
        int peer_result = getpeername(
            new_fd,
            (struct sockaddr*)&peer_address,
            &address_length);

        if (peer_result == 0) {
            char ip_string[INET_ADDRSTRLEN];
            inet_ntop(AF_INET,
                      &peer_address.sin_addr,
                      ip_string,
                      sizeof(ip_string));
            client_ip = ip_string;
        } else {
            client_ip = "unknown";
        }
    }

    // 创建新连接上下文
    auto* connection_context = new RequestContext();
    connection_context->fd = new_fd;
    connection_context->bytes_written = 0;
    connection_context->total_bytes = 0;

    if (is_client_connection) {
        connection_context->state = RequestContext::READING_CLIENT;
        connection_context->client_fd = new_fd;
        connection_context->backend_fd = -1;
        connection_context->client_ip = client_ip;
    } else {
        connection_context->state = RequestContext::BACKEND_REGISTER;
        connection_context->client_fd = -1;
        connection_context->backend_fd = new_fd;
    }

    // 注册到 fd 映射表
    ctx_by_fd_[new_fd] = connection_context;

    // 为新连接提交首次 read（使用 IOSQE_BUFFER_SELECT）
    submit_read_buf_select(connection_context);

    // 重新为监听 fd 提交 accept
    submit_accept(listen_context);
}

void Worker::handle_client_read(RequestContext* context, int bytes_read) {
    //
    // === 客户端 HTTP 请求处理 ===
    //
    // 这是网关最核心的函数之一，处理流程：
    //   1. 校验读取结果 → 2. 解析 HTTP 请求 → 3. 路由查找
    //   4. 无可用后端 → 加入等待队列 → 5. JWT 鉴权 → 6. IP 限流
    //   7. COW 移除后端路由 → 8. 转发请求到后端
    //
    if (bytes_read <= 0) {
        LOG_WARN("[Worker %d] 客户端读取到 %d 字节，关闭连接 (fd=%d)",
                 worker_id_, bytes_read, context->fd);
        close_connection(context);
        return;
    }

    // 计算安全长度（不超过缓冲区大小 - 1，确保有位置放 '\0'）
    int safe_buffer_length;
    if (bytes_read < BUFFER_SIZE - 1) {
        safe_buffer_length = bytes_read;
    } else {
        safe_buffer_length = BUFFER_SIZE - 1;
    }
    context->buffer[safe_buffer_length] = '\0';

    LOG_DEBUG("[Worker %d] 收到客户端请求 (fd=%d, %d 字节):\n%s",
              worker_id_, context->client_fd,
              bytes_read, context->buffer);

    // 解析 HTTP 请求：提取方法、路径、Authorization 头
    std::string http_method;
    std::string http_path;
    std::string auth_token;
    parse_http_request(context->buffer, safe_buffer_length,
                       http_method, http_path, auth_token);
    context->path = http_path;

    // 检查是否能解析出路径
    if (context->path.empty()) {
        LOG_WARN("[Worker %d] 无法解析 HTTP 请求路径 (fd=%d)",
                 worker_id_, context->client_fd);
        send_http_error(context, 400, "Bad Request",
                        "400 Bad Request");
        return;
    }

    LOG_INFO("[Worker %d] 客户端 (fd=%d) %s %s",
             worker_id_, context->client_fd,
             http_method.c_str(), context->path.c_str());

    // ---- 从连接池获取后端 ----
    // BackendPool::acquire() 从空闲队列中按最长前缀匹配获取后端 fd。
    // 由于 BackendPool 是 Worker 线程本地的，无需任何锁或原子操作。
    // acquire() 会将匹配的后端从空闲队列移除并标记为忙碌（in_use=true）。
    bool need_auth = false, need_rate_limit = false;
    int backend_fd = backend_pool_.acquire(
        context->path, need_auth, need_rate_limit);

    if (backend_fd < 0) {
        // 所有后端连接都忙碌，请求排队
        LOG_INFO("[Worker %d] 无可用后端，请求排队: %s (client_fd=%d)",
                 worker_id_, context->path.c_str(),
                 context->client_fd);
        context->total_bytes = bytes_read;
        pending_clients_.push_back(context);
        return;
    }

    // ---- JWT 令牌鉴权 ----
    // 使用 HS256（HMAC-SHA256）算法验证 Bearer Token。
    // 如果路径注册了 need_auth=true，所有请求必须携带有效的
    // Authorization: Bearer <token> 头，否则返回 401。
    if (need_auth) {
        if (auth_token.empty()) {
            LOG_WARN("[Worker %d] 需要鉴权但缺少 Authorization 头 (fd=%d, path=%s)",
                     worker_id_, context->client_fd,
                     context->path.c_str());
            send_http_error(context, 401, "Unauthorized",
                            "401 Unauthorized: Missing or invalid Authorization header");
            return;
        }

        bool token_valid = verify_token(auth_token);
        if (!token_valid) {
            LOG_WARN("[Worker %d] JWT 鉴权失败 (fd=%d, path=%s)",
                     worker_id_, context->client_fd,
                     context->path.c_str());
            send_http_error(context, 401, "Unauthorized",
                            "401 Unauthorized: Invalid token");
            return;
        }

        LOG_DEBUG("[Worker %d] JWT 鉴权通过 (fd=%d, path=%s)",
                  worker_id_, context->client_fd,
                  context->path.c_str());
    }

    // ---- IP 限流检查 ----
    // 使用滑动窗口算法，按 IP 统计请求次数。
    // 如果路径注册了 need_rate_limit=true，超过阈值返回 429。
    if (need_rate_limit) {
        bool request_allowed =
            rate_limiter_.allow(context->client_ip);
        if (!request_allowed) {
            LOG_WARN("[Worker %d] IP %s 触发限流 (fd=%d, path=%s)",
                     worker_id_, context->client_ip.c_str(),
                     context->client_fd, context->path.c_str());
            send_http_error(context, 429, "Too Many Requests",
                            "429 Too Many Requests: Rate limit exceeded");
            return;
        }
    }

    //
    // === 转发请求到后端（BackendPool 独占模式） ===
    //
    // BackendPool::acquire() 已从空闲队列中移除了该后端 fd 并标记为忙碌，
    // 因此当前请求独占该后端连接，同一 Worker 内的其他请求不会重复获取。
    // 由于每个 Worker 拥有自己的连接池，不存在跨 Worker 的竞态条件。
    //
    // 生命周期：
    //   ① acquire() 获取 fd + 标记忙碌（本函数）
    //   ② 提交写请求到后端
    //   ③ 收到后端响应后 release() 放回空闲池（handle_client_write）
    //   ④ process_pending_requests() 处理等待队列
    //
    LOG_INFO("[Worker %d] 路由匹配: %s → backend_fd=%d (auth=%s, rate=%s)",
             worker_id_, context->path.c_str(), backend_fd,
             need_auth ? "ON" : "OFF",
             need_rate_limit ? "ON" : "OFF");

    context->backend_fd = backend_fd;
    context->state = RequestContext::WRITING_BACKEND;
    context->fd = backend_fd;
    context->total_bytes = bytes_read;
    context->bytes_written = 0;
    submit_write(context, context->buffer, bytes_read);
}

void Worker::handle_backend_write(RequestContext* context,
                                   int bytes_written) {
    //
    // === 客户端的请求数据已写入后端 ===
    //
    // 如果一次 write 没写完所有数据（partial write），
    // 继续提交剩余部分的写请求。
    // 全部写完后，回收缓冲区并发起后端读。
    //
    context->bytes_written += bytes_written;

    // 处理 partial write：TCP 不保证一次 send 写完所有数据
    if (context->bytes_written < context->total_bytes) {
        submit_write_remain(context);
        return;
    }

    LOG_DEBUG("[Worker %d] 请求已全部发往后端 (fd=%d, %d 字节)",
              worker_id_, context->backend_fd, context->total_bytes);

    // 回收客户端请求的 Provided Buffer，归还内核供复用
    submit_recycle_buffer(context->bid);
    context->bid = -1;
    context->buffer = nullptr;

    // 状态转换：WRITING_BACKEND → READING_BACKEND
    // 现在从该后端连接读取 HTTP 响应
    context->state = RequestContext::READING_BACKEND;
    context->fd = context->backend_fd;
    context->total_bytes = 0;
    context->bytes_written = 0;
    submit_read_buf_select(context);
}

void Worker::handle_backend_read(RequestContext* context,
                                  int bytes_read) {
    //
    // === 后端响应已返回 ===
    //
    // 将后端返回的 HTTP 响应数据通过 io_uring 写回客户端。
    // 注意：这里假设后端响应在一个 read 内完整到达（<= 4KB）。
    // 对于大响应需要多个 read 循环，当前实现简化处理。
    //
    if (bytes_read <= 0) {
        LOG_WARN("[Worker %d] 后端读取到 %d 字节，关闭连接 (fd=%d)",
                 worker_id_, bytes_read, context->backend_fd);
        close_connection(context);
        return;
    }

    // 计算安全长度
    int safe_buffer_length;
    if (bytes_read < BUFFER_SIZE - 1) {
        safe_buffer_length = bytes_read;
    } else {
        safe_buffer_length = BUFFER_SIZE - 1;
    }
    context->buffer[safe_buffer_length] = '\0';

    LOG_DEBUG("[Worker %d] 收到后端响应 (fd=%d, %d 字节):\n%s",
              worker_id_, context->backend_fd,
              bytes_read, context->buffer);

    // 状态转换：READING_BACKEND → WRITING_CLIENT
    // 将后端响应数据写回客户端连接
    context->state = RequestContext::WRITING_CLIENT;
    context->fd = context->client_fd;
    context->total_bytes = bytes_read;
    context->bytes_written = 0;
    submit_write(context, context->buffer, bytes_read);
}

// 前向声明
static void safe_close_fd(int fd, int listen_fd_client, int listen_fd_backend);

void Worker::handle_client_write(RequestContext* context,
                                  int bytes_written) {
    //
    // === 后端响应已全部写回客户端 ===
    //
    // 处理流程：
    //   1. 处理 partial write（如果未写完）
    //   2. 回收缓冲区
    //   3. Keep-Alive：将后端设回 BACKEND_IDLE
    //   4. 通过 COW 将后端连接重新插入路由表
    //   5. 处理等待队列中的请求
    //   6. 关闭客户端连接，清理客户端上下文
    //
    context->bytes_written += bytes_written;

    // 处理 partial write
    if (context->bytes_written < context->total_bytes) {
        submit_write_remain(context);
        return;
    }

    LOG_DEBUG("[Worker %d] 响应已全部发回客户端 (fd=%d, %d 字节)",
              worker_id_, context->client_fd, context->total_bytes);

    // 回收后端响应数据的 Provided Buffer
    if (context->bid >= 0) {
        submit_recycle_buffer(context->bid);
        context->bid = -1;
        context->buffer = nullptr;
    }

    //
    // === Keep-Alive 机制 ===
    //
    // 后端连接是持久的（keep-alive），响应完成后不关闭，
    // 而是将后端重新插入路由表，供下一个请求复用。
    //
    // 流程：
    //   ① 查找后端自己的 RequestContext（在 ctx_by_fd_ 中）
    //   ② 状态设为 BACKEND_IDLE，表示空闲可用
    //   ③ 通过 BackendPool::release() 放回空闲队列（使用注册时保存的 prefix）
    //   ④ 调用 process_pending_requests() 处理等待队列
    //   ⑤ 关闭客户端连接（客户端短连接，请求-响应完成后即关闭）
    //   ⑥ 删除客户端的 RequestContext
    //
    int backend_fd = context->backend_fd;
    int client_fd  = context->client_fd;

    // 步骤 ①：查找后端上下文
    auto backend_it = ctx_by_fd_.find(backend_fd);
    if (backend_it != ctx_by_fd_.end()) {
        RequestContext* backend_ctx = backend_it->second;

        // 步骤 ②：后端设为空闲
        backend_ctx->state = RequestContext::BACKEND_IDLE;

        // 步骤 ③：将后端释放回连接池
        // BackendPool::release() 将 fd 标记为空闲并放回空闲队列尾部。
        // 这样下次 acquire() 时又能拿到该后端。
        backend_pool_.release(backend_fd);

        // 步骤 ④：后端已重新可用，处理等待队列
        // 注意：process_pending_requests() 会尝试从队列头部取出
        // 等待的请求，如果找到可用后端则立即转发。
        process_pending_requests();
    } else {
        // 后端上下文已消失（可能已断开连接或被 close_connection 清理）
        LOG_WARN("[Worker %d] 后端 fd=%d 上下文已不存在，关闭连接",
                 worker_id_, backend_fd);
        close_connection(context);
        return;
    }

    // 步骤 ⑤：关闭客户端连接
    // 客户端 HTTP/1.1 请求完成，关闭连接（不支持 HTTP/1.1 keep-alive）
    safe_close_fd(client_fd, listen_fd_client_, listen_fd_backend_);
    ctx_by_fd_.erase(client_fd);

    // 步骤 ⑥：删除客户端的 RequestContext（heap 分配的对象）
    // 注意：accept 上下文是成员变量，不能 delete
    if (context != &accept_client_ctx_ &&
        context != &accept_backend_ctx_) {
        delete context;
    }
}

void Worker::handle_backend_register(RequestContext* context,
                                      int bytes_read) {
    if (bytes_read <= 0) {
        LOG_WARN("[Worker %d] 后端注册读取到 %d 字节，关闭 (fd=%d)",
                 worker_id_, bytes_read, context->backend_fd);
        close_connection(context);
        return;
    }

    // 计算安全长度
    int safe_buffer_length;
    if (bytes_read < BUFFER_SIZE - 1) {
        safe_buffer_length = bytes_read;
    } else {
        safe_buffer_length = BUFFER_SIZE - 1;
    }
    context->buffer[safe_buffer_length] = '\0';

    LOG_DEBUG("[Worker %d] 后端注册消息 (fd=%d): %s",
              worker_id_, context->backend_fd, context->buffer);

    std::string raw_message(context->buffer, safe_buffer_length);

    // 去除末尾的换行符
    while (!raw_message.empty() &&
           (raw_message.back() == '\n' || raw_message.back() == '\r')) {
        raw_message.pop_back();
    }

    // 检查是否是 REGISTER 消息
    const std::string register_prefix = "REGISTER ";
    size_t register_pos = raw_message.find(register_prefix);
    if (register_pos == std::string::npos) {
        LOG_WARN("[Worker %d] 无效的后端注册消息: %s",
                 worker_id_, raw_message.c_str());

        // 回收注册消息的缓冲区
        if (context->bid >= 0) {
            submit_recycle_buffer(context->bid);
            context->bid = -1;
            context->buffer = nullptr;
        }

        context->state = RequestContext::BACKEND_IDLE;
        return;
    }

    // 提取 "REGISTER " 后面的内容
    std::string remainder =
        raw_message.substr(register_pos + register_prefix.size());

    // 解析前缀（第一个空格前的部分是路径前缀）
    size_t first_space = remainder.find(' ');
    std::string route_prefix;
    bool need_auth = false;
    bool need_rate_limit = false;

    if (first_space == std::string::npos) {
        // 只有前缀，无标志：REGISTER /prefix
        route_prefix = remainder;
    } else {
        // 有标志：REGISTER /prefix auth=1 rate=1
        route_prefix = remainder.substr(0, first_space);
        std::string flag_section = remainder.substr(first_space + 1);

        // 解析键值对标志
        size_t scan_position = 0;
        while (scan_position < flag_section.size()) {
            // 查找 '=' 分隔符
            size_t equals_position =
                flag_section.find('=', scan_position);
            if (equals_position == std::string::npos) {
                break;
            }

            // 提取键名
            std::string key_name =
                flag_section.substr(scan_position,
                                    equals_position - scan_position);

            // 提取值（到下一个空格或字符串末尾）
            size_t next_space =
                flag_section.find(' ', equals_position + 1);
            std::string value;
            if (next_space == std::string::npos) {
                value = flag_section.substr(equals_position + 1);
                scan_position = flag_section.size();
            } else {
                value = flag_section.substr(
                    equals_position + 1,
                    next_space - equals_position - 1);
                scan_position = next_space + 1;
            }

            // 设置标志
            if (key_name == "auth") {
                need_auth = (value == "1" || value == "true");
            } else if (key_name == "rate") {
                need_rate_limit = (value == "1" || value == "true");
            }
        }
    }

    // 将后端连接添加到当前 Worker 的连接池
    // BackendPool::add() 直接在当前 Worker 的线程本地池中操作，无需锁。
    if (!route_prefix.empty()) {
        backend_pool_.add(context->backend_fd, route_prefix,
                          need_auth, need_rate_limit);
        // 保存注册信息，用于后续日志/调试
        context->registered_prefix    = route_prefix;
        context->registered_auth       = need_auth;
        context->registered_rate_limit = need_rate_limit;
        LOG_INFO("[Worker %d] 后端注册路由: %s → backend_fd=%d (auth=%s, rate=%s)",
                 worker_id_, route_prefix.c_str(), context->backend_fd,
                 need_auth ? "ON" : "OFF",
                 need_rate_limit ? "ON" : "OFF");
    }

    // 回收注册消息的缓冲区
    if (context->bid >= 0) {
        submit_recycle_buffer(context->bid);
        context->bid = -1;
        context->buffer = nullptr;
    }

    // 后端进入空闲状态
    context->state = RequestContext::BACKEND_IDLE;
    LOG_INFO("[Worker %d] 后端 (fd=%d) 注册完成，进入空闲",
             worker_id_, context->backend_fd);

    // 新后端已注册，处理等待队列中的请求
    process_pending_requests();
}

// ==================== 辅助方法 ====================

bool Worker::assign_backend(RequestContext* client_context) {
    bool need_auth = false, need_rate_limit = false;
    int backend_fd = backend_pool_.acquire(
        client_context->path, need_auth, need_rate_limit);
    if (backend_fd < 0) {
        return false;
    }
    client_context->backend_fd = backend_fd;
    return true;
}

void Worker::process_pending_requests() {
    //
    // === 处理等待队列中的请求 ===
    //
    // 此函数在以下时机被调用：
    //   1. handle_client_write() 中，后端响应写完客户端后重新插入路由表时
    //   2. handle_backend_register() 中，新后端注册完成时
    //
    // 策略：FIFO 队列 + 非阻塞处理
    //   - 从队列头部取出等待最久的请求
    //   - 检查客户端连接是否仍然有效（可能已超时断开）
    //   - 查找可用后端（每次重新获取最新路由表）
    //   - 如果无可用后端，停止处理（队列顺序不变，等待下次被调用）
    //
    // 注意：每次循环都重新获取路由表，因为：
    //   a) 可能有多个后端同时完成并重新插入路由表
    //   b) 当前 Worker 线程不是唯一会调用此函数的线程
    //
    if (pending_clients_.empty()) {
        return;
    }

    LOG_DEBUG("[Worker %d] 处理等待队列，当前 %zu 个请求排队",
              worker_id_, pending_clients_.size());

    // 遍历等待队列（FIFO 顺序）
    while (!pending_clients_.empty()) {
        RequestContext* client_ctx = pending_clients_.front();

        // 步骤 1：检查客户端是否已断开
        // 使用 fcntl(F_GETFD) 非侵入式检查 fd 是否有效，
        // 避免在已关闭的 fd 上执行 I/O 操作。
        if (::fcntl(client_ctx->client_fd, F_GETFD) == -1) {
            // 客户端已断开连接，清理并跳过
            LOG_DEBUG("[Worker %d] 等待队列中的客户端 (fd=%d) 已断开",
                      worker_id_, client_ctx->client_fd);
            pending_clients_.pop_front();
            // 回收其占用的 Provided Buffer
            if (client_ctx->bid >= 0) {
                submit_recycle_buffer(client_ctx->bid);
                client_ctx->bid = -1;
                client_ctx->buffer = nullptr;
            }
            // 从映射表移除并释放上下文
            ctx_by_fd_.erase(client_ctx->client_fd);
            if (client_ctx != &accept_client_ctx_ &&
                client_ctx != &accept_backend_ctx_) {
                delete client_ctx;
            }
            continue;
        }

        // 步骤 2：从连接池获取可用后端
        // BackendPool 是线程本地的，直接 acquire 即可
        bool need_auth = false, need_rate_limit = false;
        int backend_fd = backend_pool_.acquire(
            client_ctx->path, need_auth, need_rate_limit);

        if (backend_fd < 0) {
            // 仍然没有可用空闲连接，停止处理
            // 保持队列中剩余元素的顺序，等待下一次被调用
            LOG_DEBUG("[Worker %d] 等待队列: 仍无可用后端，剩余 %zu 个请求",
                      worker_id_, pending_clients_.size());
            break;
        }

        // 步骤 3：找到可用后端，出队处理
        pending_clients_.pop_front();

        LOG_INFO("[Worker %d] 等待队列出队: %s → backend_fd=%d (client_fd=%d)",
                 worker_id_, client_ctx->path.c_str(),
                 backend_fd, client_ctx->client_fd);

        // 步骤 4：设置上下文并提交写请求
        // 复用缓冲区中已保存的请求数据（total_bytes 在入队时已设置）
        client_ctx->backend_fd = backend_fd;
        client_ctx->state = RequestContext::WRITING_BACKEND;
        client_ctx->fd = backend_fd;
        client_ctx->bytes_written = 0;
        submit_write(client_ctx, client_ctx->buffer, client_ctx->total_bytes);
    }
}

/**
 * @brief 安全关闭文件描述符（跳过监听套接字）
 */
static void safe_close_fd(int fd,
                          int listen_fd_client,
                          int listen_fd_backend) {
    if (fd >= 0 &&
        fd != listen_fd_client &&
        fd != listen_fd_backend) {
        ::close(fd);
    }
}

void Worker::close_connection(RequestContext* context) {
    //
    // === 关闭连接并清理资源 ===
    //
    // 这是连接生命周期的终结函数。执行以下清理：
    //   1. 从路由表中移除该后端（如果已注册）
    //   2. 关闭所有关联的 socket fd（跳过监听套接字）
    //   3. 从 ctx_by_fd_ 映射表中移除所有关联 fd
    //   4. 删除 RequestContext（heap 分配的对象）
    //
    // 注意：对于客户端连接，context->backend_fd 可能为 -1；
    // 对于后端连接，context->client_fd 可能为 -1。
    // current_fd 也可能等于 client_fd 或 backend_fd 之一，
    // 因此关闭和移除时需要去重。
    //
    if (!context) {
        return;
    }

    int current_fd = context->fd;
    int client_fd = context->client_fd;
    int backend_fd = context->backend_fd;

    LOG_DEBUG("[Worker %d] 关闭连接: fd=%d, client_fd=%d, backend_fd=%d, state=%s",
              worker_id_, current_fd, client_fd, backend_fd,
              state_name(context->state));

    // 步骤 1：从连接池中移除后端（如果有）
    // BackendPool::remove() 会将 fd 从空闲队列和条目映射中一并移除。
    if (backend_fd >= 0) {
        backend_pool_.remove(backend_fd);
        LOG_INFO("[Worker %d] 后端 fd=%d 断开，已从连接池移除",
                 worker_id_, backend_fd);
    }

    // 步骤 2：关闭所有关联的 socket（跳过监听套接字）
    // 注意去重：current_fd 可能等于 client_fd 或 backend_fd
    safe_close_fd(current_fd, listen_fd_client_, listen_fd_backend_);
    if (client_fd != current_fd) {
        safe_close_fd(client_fd, listen_fd_client_, listen_fd_backend_);
    }
    if (backend_fd != current_fd && backend_fd != client_fd) {
        safe_close_fd(backend_fd, listen_fd_client_, listen_fd_backend_);
    }

    // 步骤 3：从 fd 映射表中移除
    if (current_fd >= 0) {
        ctx_by_fd_.erase(current_fd);
    }
    if (client_fd >= 0 && client_fd != current_fd) {
        ctx_by_fd_.erase(client_fd);
    }
    if (backend_fd >= 0 &&
        backend_fd != current_fd &&
        backend_fd != client_fd) {
        ctx_by_fd_.erase(backend_fd);
    }

    // 步骤 4：释放 RequestContext 内存
    // accept 上下文是 Worker 类的成员变量，不能 delete
    if (context == &accept_client_ctx_ ||
        context == &accept_backend_ctx_) {
        return;
    }

    delete context;
}

const char* Worker::state_name(typename RequestContext::State state) {
    switch (state) {
        case RequestContext::ACCEPTING:
            return "ACCEPTING";
        case RequestContext::READING_CLIENT:
            return "READING_CLIENT";
        case RequestContext::WRITING_BACKEND:
            return "WRITING_BACKEND";
        case RequestContext::READING_BACKEND:
            return "READING_BACKEND";
        case RequestContext::WRITING_CLIENT:
            return "WRITING_CLIENT";
        case RequestContext::BACKEND_REGISTER:
            return "BACKEND_REGISTER";
        case RequestContext::BACKEND_IDLE:
            return "BACKEND_IDLE";
        case RequestContext::HEALTH_CHECK_TIMER:
            return "HEALTH_CHECK_TIMER";
        default:
            return "UNKNOWN";
    }
}

std::string Worker::extract_path(const char* buffer, int /*length*/) {
    // 跳过 METHOD，提取 PATH
    // 格式: "GET /path HTTP/1.1"
    const char* path_start = std::strchr(buffer, ' ');
    if (path_start == nullptr) {
        return "/";
    }
    path_start++;  // 跳过空格

    const char* path_end = std::strchr(path_start, ' ');
    if (path_end == nullptr) {
        return "/";
    }

    return std::string(path_start, path_end - path_start);
}

// ==================== 健康检查 ====================

void Worker::submit_health_check_timeout() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        LOG_WARN("[Worker %d] 获取 SQE 失败，无法提交健康检查定时器", worker_id_);
        return;
    }

    struct __kernel_timespec ts;
    ts.tv_sec  = HEALTH_CHECK_INTERVAL_SEC;
    ts.tv_nsec = 0;

    io_uring_prep_timeout(sqe, &ts, 0, 0);
    io_uring_sqe_set_data(sqe, &health_check_timer_ctx_);
}

void Worker::perform_health_checks() {
    LOG_DEBUG("[Worker %d] 执行健康检查...", worker_id_);

    // ---- 第 1 步：收集所有待检查的后端 fd ----
    // 注意：不能在遍历 ctx_by_fd_ 的过程中调用 close_connection()，
    // 因为 close_connection() 会 erase 当前迭代器指向的元素
    std::vector<int> backends_to_check;
    backends_to_check.reserve(ctx_by_fd_.size());
    for (const auto& [fd, ctx] : ctx_by_fd_) {
        // 只检查空闲的后端连接
        if (ctx->state != RequestContext::BACKEND_IDLE) continue;
        if (fd == listen_fd_client_ || fd == listen_fd_backend_) continue;
        if (ctx == &accept_client_ctx_ || ctx == &accept_backend_ctx_) continue;
        if (ctx == &health_check_timer_ctx_) continue;
        backends_to_check.push_back(fd);
    }

    // ---- 第 2 步：逐个创建独立 TCP 连接进行健康检查 ----
    for (int backend_fd : backends_to_check) {
        auto it = ctx_by_fd_.find(backend_fd);
        if (it == ctx_by_fd_.end()) continue;
        RequestContext* ctx = it->second;

        // 二次检查状态（可能在收集期间变化）
        if (ctx->state != RequestContext::BACKEND_IDLE) continue;

        // 通过 getpeername() 获取后端 IP 地址
        struct sockaddr_in peer_addr{};
        socklen_t addr_len = sizeof(peer_addr);
        if (::getpeername(backend_fd,
                          (struct sockaddr*)&peer_addr,
                          &addr_len) != 0) {
            LOG_WARN("[Worker %d] 健康检查: getpeername 失败 (fd=%d), errno=%d",
                     worker_id_, backend_fd, errno);
            close_connection(ctx);
            continue;
        }

        // ---- 创建独立 TCP 连接 ----
        int hc_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (hc_fd < 0) {
            LOG_WARN("[Worker %d] 健康检查: socket() 失败 (fd=%d), errno=%d",
                     worker_id_, backend_fd, errno);
            continue;
        }

        // 设置 2 秒超时（连接和收发）
        struct timeval tv;
        tv.tv_sec = 2;
        tv.tv_usec = 0;
        ::setsockopt(hc_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        ::setsockopt(hc_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // 连接到后端的健康检查端口（而非业务端口）
        peer_addr.sin_port = ::htons(
            static_cast<uint16_t>(health_check_port_));

        int rc = ::connect(hc_fd,
                           (struct sockaddr*)&peer_addr,
                           sizeof(peer_addr));
        if (rc != 0) {
            LOG_WARN("[Worker %d] 健康检查: connect(%s:%d) 失败, "
                     "移除后端 fd=%d",
                     worker_id_,
                     ::inet_ntoa(peer_addr.sin_addr),
                     health_check_port_,
                     backend_fd);
            ::close(hc_fd);
            close_connection(ctx);
            continue;
        }

        // ---- 发送 GET /health 请求 ----
        int req_len = static_cast<int>(std::strlen(HEALTH_CHECK_REQUEST));
        ssize_t sent = ::send(hc_fd, HEALTH_CHECK_REQUEST, req_len, 0);
        if (sent != req_len) {
            LOG_WARN("[Worker %d] 健康检查: send() 失败 (fd=%d), 移除后端",
                     worker_id_, backend_fd);
            ::close(hc_fd);
            close_connection(ctx);
            continue;
        }

        // ---- 读取响应 ----
        char response_buf[256];
        ssize_t bytes_read = ::read(hc_fd,
                                    response_buf,
                                    sizeof(response_buf) - 1);

        // 关闭健康检查连接（无论成败）
        ::close(hc_fd);

        if (bytes_read > 0) {
            response_buf[bytes_read] = '\0';
            if (std::strstr(response_buf, "200 OK") != nullptr) {
                LOG_DEBUG("[Worker %d] 健康检查通过: fd=%d",
                          worker_id_, backend_fd);
                // 健康，路由保持不变
            } else {
                LOG_WARN("[Worker %d] 健康检查失败 (fd=%d): "
                         "响应中未找到 200 OK",
                         worker_id_, backend_fd);
                close_connection(ctx);
            }
        } else {
            LOG_WARN("[Worker %d] 健康检查失败 (fd=%d): "
                     "读取响应失败, bytes=%zd",
                     worker_id_, backend_fd, bytes_read);
            close_connection(ctx);
        }
    }
}
