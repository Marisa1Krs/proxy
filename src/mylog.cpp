#include "mylog.h"
#include <stdarg.h>

// 初始化静态成员
mylog* mylog::_ptr = nullptr;
std::mutex mylog::_initMutex;

// 构造函数（创建目录 + 打开文件 + 预分配缓冲区）
mylog::mylog(const std::string& filePath, size_t bufSize, LogLevel logLevel)
    : logDir(filePath), bufSize(bufSize), logLevel(logLevel),
      running(true), fd(-1), watermark(bufSize * 8) {
    // 若指定文件路径，创建目录并打开文件
    if (!filePath.empty()) {
        // 提取目录部分（如 "/a/b/log.txt" → "/a/b"）
        size_t pos = filePath.find_last_of('/');
        if (pos != std::string::npos) {
            std::string dir = filePath.substr(0, pos);
            if (!createDirectory(dir)) {
                std::cerr << "[mylog] 目录创建失败：" << dir << std::endl;
            }
        }

        // 打开日志文件（O_CREAT|O_RDWR|O_APPEND，权限 0666）
        fd = open(filePath.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
        if (fd == -1) {
            std::cerr << "[mylog] 日志文件打开失败！path=" << filePath
                      << ", errno=" << errno << ", msg=" << strerror(errno) << std::endl;
        } else {
            std::cout << "[mylog] 日志文件打开成功：" << filePath << ", fd=" << fd << std::endl;
        }
    } else {
        std::cout << "[mylog] 未指定日志文件，仅输出到终端" << std::endl;
    }

    // 预分配所有缓冲区的初始容量，减少运行时扩容次数
    front_write_buf.reserve(bufSize);
    front_swap_buf.reserve(bufSize);
    back_recv_buf.reserve(bufSize);
    back_flush_buf.reserve(bufSize);

    // 启动后台消费线程（必须在所有初始化完成后启动）
    helper = std::thread(std::bind(&mylog::worker, this));
}

// 析构函数（安全停止线程 + 刷新剩余日志 + 释放资源）
mylog::~mylog() {
    {
        // 1. 标记停止
        std::unique_lock<std::mutex> lock(logMutex);
        running = false;

        // 2. 将前端缓冲区中剩余的数据全部刷到后端接收缓冲区
        if (!front_write_buf.empty()) {
            front_write_buf.swap(front_swap_buf);
        }
        if (!front_swap_buf.empty()) {
            front_swap_buf.swap(back_recv_buf);
        }
    }

    // 3. 唤醒消费线程，处理剩余数据后退出
    cv_flush.notify_one();

    // 4. 等待后台线程结束（确保所有日志消费完成）
    if (helper.joinable()) {
        helper.join();
    }

    // 5. 关闭文件描述符
    if (fd != -1) {
        close(fd);
        fd = -1;
    }
    std::cout << "[mylog] 日志模块已关闭" << std::endl;
}

// 初始化单例（线程安全）
void mylog::init(const std::string& filePath, size_t bufSize, LogLevel logLevel) {
    if (_ptr == nullptr) {
        std::lock_guard<std::mutex> lock(_initMutex);  // 双重检查锁定，避免并发初始化
        if (_ptr == nullptr) {
            _ptr = new mylog(filePath, bufSize, logLevel);
        }
    }
}

// 获取单例指针
mylog* mylog::getPtr() {
    if (_ptr == nullptr) {
        std::cerr << "[mylog] 错误：未调用 init() 初始化日志模块！" << std::endl;
        abort();  // 未初始化直接崩溃，避免后续空指针访问
    }
    return _ptr;
}

// ============================================================
// 写入前端缓冲区
// 功能：自动扩容 + 水位线检查 + 触发前后端缓冲交换
// ============================================================
void mylog::writeBuffer(const std::string& logtext) {
    std::unique_lock<std::mutex> lock(logMutex);

    // ===== 水位线检查 =====
    // 如果前端写缓冲区已满（超过水位线），直接丢弃该日志
    if (front_write_buf.size() >= watermark) {
        return;
    }

    // ===== 自动扩容 =====
    // 当缓冲区大小达到 bufSize 阈值时，翻倍扩容（但不超过水位线）
    if (front_write_buf.size() >= bufSize) {
        size_t new_cap = std::min(front_write_buf.capacity() * 2, watermark);
        if (new_cap > front_write_buf.capacity()) {
            front_write_buf.reserve(new_cap);
        }
    }

    // 写入日志
    front_write_buf.push_back(logtext);

    // ===== 触发前后端缓冲交换 =====
    // 当前端写缓冲区达到 bufSize 阈值时，将数据通过 swap 交换给后端
    if (front_write_buf.size() >= bufSize) {
        // 步骤1：前端双缓冲交换（写缓冲区 → 交换缓冲区）
        // 此时 front_write_buf 变空，可继续写入；front_swap_buf 持有待处理数据
        front_write_buf.swap(front_swap_buf);

        // 步骤2：前端交换缓冲区 → 后端接收缓冲区
        // 将数据传递给后端线程
        front_swap_buf.swap(back_recv_buf);

        // 通知后台线程有数据可消费
        cv_flush.notify_one();
    }
}

// ============================================================
// 后台消费线程（后端双缓冲：接收 → 刷盘 → 写文件）
// ============================================================
void mylog::worker() {
    while (true) {
        std::unique_lock<std::mutex> lock(logMutex);

        // 等待数据：后端接收缓冲区为空时阻塞，直到被唤醒
        // 同时检查 running 标志，避免在停止后无限等待
        while (back_recv_buf.empty() && running) {
            cv_flush.wait(lock);
        }

        if (!back_recv_buf.empty()) {
            // 后端双缓冲交换：接收缓冲区 → 刷盘缓冲区
            // back_recv_buf 变空，可接收下一批数据
            // back_flush_buf 持有待写入文件的数据
            back_recv_buf.swap(back_flush_buf);

            // 解锁：允许前端线程继续写入和交换
            lock.unlock();

            // === 消费 back_flush_buf（无锁操作，提高效率）===
            for (const auto& log : back_flush_buf) {
                // 控制台输出
                printf("%s\n", log.c_str());

                // 文件写入：fd 有效时才写
                if (fd != -1) {
                    ssize_t write_len = ::write(fd, log.c_str(), log.size());
                    if (write_len == -1) {
                        std::cerr << "[mylog] 文件写入失败！errno=" << errno
                                  << ", msg=" << strerror(errno) << std::endl;
                    }
                }
            }

            // 清空刷盘缓冲区，释放内存
            back_flush_buf.clear();

            // 收缩容量：如果缓冲区膨胀过大，回收内存回到初始大小
            if (back_flush_buf.capacity() > bufSize * 2) {
                std::vector<std::string>().swap(back_flush_buf);  // 释放全部内存
                back_flush_buf.reserve(bufSize);                  // 重新分配初始容量
            }
        } else if (!running) {
            // 没有数据且已停止，退出循环
            break;
        }
    }

    // === 最终检查：退出前消费完后端接收缓冲区中剩余的日志 ===
    std::unique_lock<std::mutex> lock(logMutex);
    if (!back_recv_buf.empty()) {
        back_recv_buf.swap(back_flush_buf);
    }
    lock.unlock();

    if (!back_flush_buf.empty()) {
        for (const auto& log : back_flush_buf) {
            printf("%s\n", log.c_str());
            if (fd != -1) {
                ::write(fd, log.c_str(), log.size());
            }
        }
        back_flush_buf.clear();
    }
    std::cout << "[mylog] 消费线程已退出" << std::endl;
}

// 写入日志（处理可变参数 + 日志格式拼接）
void mylog::writeLog(LogLevel level, const char* file, int line, const char* fmt, ...) {
    // 1. 校验参数：日志级别低于阈值/参数无效，直接返回
    if (level < logLevel || level >= LOG_LEVEL_MAX || fmt == nullptr) {
        return;
    }

    // 2. 拼接日志头部（时间 + 级别 + 文件行号）
    char log_buf[4096] = {0};
    size_t buf_pos = 0;

    // 2.1 获取当前时间（精确到毫秒）
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);  // 线程安全的时间转换

    char time_buf[32] = {0};
    snprintf(time_buf, sizeof(time_buf),
             "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000);

    // 2.2 拼接时间 + 级别 + 文件行号（提取文件名，去掉路径）
    const char* filename = strrchr(file, '/') ? strrchr(file, '/') + 1 : file;
    buf_pos += snprintf(log_buf + buf_pos, sizeof(log_buf) - buf_pos,
                       "Marisa⭐Daze[%s] [%s] [%s:%d] ",
                       time_buf, g_log_level_names[level], filename, line);

    // 3. 拼接日志内容（处理可变参数）
    va_list args;
    va_start(args, fmt);
    buf_pos += vsnprintf(log_buf + buf_pos, sizeof(log_buf) - buf_pos, fmt, args);
    va_end(args);

    // 4. 补充换行符（确保每条日志独立一行）
    if (buf_pos < sizeof(log_buf) - 1) {
        log_buf[buf_pos++] = '\n';
        log_buf[buf_pos] = '\0';
    }

    // 5. 写入前端缓冲区（自动扩容 + 水位线检查 + 触发前后端交换）
    writeBuffer(std::string(log_buf));
}

// 辅助函数：递归创建目录（支持多级目录）
bool mylog::createDirectory(const std::string& path) {
    // 目录已存在，直接返回成功
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode);
    }

    // 递归创建父目录（如 "a/b/c" → 先创建 "a"，再 "a/b"，最后 "a/b/c"）
    size_t pos = path.find_last_of('/');
    if (pos != std::string::npos) {
        std::string parent_dir = path.substr(0, pos);
        if (!createDirectory(parent_dir)) {
            return false;
        }
    }

    // 创建当前目录（权限 0755：所有者读写执行，其他读执行）
    if (mkdir(path.c_str(), 0755) == -1) {
        std::cerr << "[mylog] 创建目录失败！path=" << path
                  << ", errno=" << errno << ", msg=" << strerror(errno) << std::endl;
        return false;
    }
    return true;
}

// ============================================================
// 强制刷新：将前端缓冲区中所有日志强制刷到后端并等待写入完成
// ============================================================
void mylog::flush() {
    std::unique_lock<std::mutex> lock(logMutex);

    // 如果前端写缓冲区有数据，通过双缓冲交换刷到后端
    if (!front_write_buf.empty()) {
        front_write_buf.swap(front_swap_buf);
    }
    if (!front_swap_buf.empty()) {
        front_swap_buf.swap(back_recv_buf);
    }

    // 通知后台线程消费
    lock.unlock();
    cv_flush.notify_one();

    // 等待后台线程处理完（通过检测 back_recv_buf 为空且 back_flush_buf 为空）
    // 简单实现：短暂 sleep 让 worker 有机会运行
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

// ============================================================
// 销毁单例：强制刷新所有日志后销毁日志模块
// ============================================================
void mylog::destroy() {
    if (_ptr == nullptr) return;

    // 强制刷新剩余日志
    _ptr->flush();

    // 销毁单例
    delete _ptr;
    _ptr = nullptr;
}