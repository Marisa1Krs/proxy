#include "gateway.h"
#include "worker.h"
#include "mylog.h"

#include <csignal>
#include <iostream>

// ==================== 构造 / 析构 ====================

Gateway::Gateway(int client_port,
                 int backend_port,
                 int ring_size,
                 int worker_count,
                 const std::vector<std::string>& cpu_affinity_masks,
                 int health_check_port)
    : init_code_(INIT_OK)
    , client_port_(client_port)
    , backend_port_(backend_port)
    , ring_size_(ring_size)
    , worker_count_(worker_count)
    , cpu_affinity_masks_(cpu_affinity_masks)
    , health_check_port_(health_check_port) {

    // 确保至少有一个 Worker 线程
    if (worker_count_ <= 0) {
        worker_count_ = 1;
    }

    // 创建所有 Worker 对象
    workers_.reserve(worker_count_);
    for (int worker_index = 0;
         worker_index < worker_count_;
         ++worker_index) {

        auto* worker = new Worker(
            client_port_,
            backend_port_,
            ring_size_,
            worker_index,
            health_check_port_);

        // 设置 CPU 亲和性（如果配置了对应的掩码）
        bool has_cpu_affinity =
            (worker_index < static_cast<int>(cpu_affinity_masks_.size())) &&
            (!cpu_affinity_masks_[worker_index].empty());

        if (has_cpu_affinity) {
            worker->set_cpu_affinity(
                cpu_affinity_masks_[worker_index]);
        }

        workers_.push_back(worker);
    }

    LOG_INFO("已创建 %d 个 Worker 线程", worker_count_);
}

Gateway::~Gateway() {
    stop();

    // 等待所有线程退出
    for (auto* thread : threads_) {
        if (thread != nullptr && thread->joinable()) {
            thread->join();
        }
        delete thread;
    }
    threads_.clear();

    // 销毁所有 Worker
    for (auto* worker : workers_) {
        delete worker;
    }
    workers_.clear();
}

// ==================== 公共接口 ====================

void Gateway::run() {
    // 启动所有 Worker 线程
    threads_.reserve(worker_count_);
    for (auto* worker : workers_) {
        auto* thread = new std::thread([worker]() {
            worker->worker_loop();
        });
        threads_.push_back(thread);
    }

    LOG_INFO("所有 Worker 线程已启动，等待退出...");

    // 等待所有 Worker 线程结束
    for (auto* thread : threads_) {
        if (thread != nullptr && thread->joinable()) {
            thread->join();
        }
    }

    LOG_INFO("所有 Worker 线程已退出");
}

void Gateway::stop() {
    LOG_INFO("正在停止所有 Worker...");

    // 1. 通知所有 Worker 停止（设置 running_ = false）
    for (auto* worker : workers_) {
        worker->stop();
    }

    // 2. 向每个 Worker 线程发送 SIGUSR1，唤醒 io_uring_wait_cqe
    //    事件循环收到 -EINTR 后会检查 running_ 标志并退出
    for (auto* worker : workers_) {
        pthread_t thread_handle = worker->thread_handle();
        if (thread_handle != 0) {
            pthread_kill(thread_handle, SIGUSR1);
        }
    }
}
