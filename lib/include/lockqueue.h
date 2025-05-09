#pragma once
#include <queue>
#include <thread>
#include <mutex> // pthread_mutex_t
#include <condition_variable> // pthread_condition_t

// 异步写日志的日志队列
template<typename T>
class LockQueue {
public:
    void Push(const T &data);
    T& Pop();
private:
    std::queue<T> m_queue; // 队列
    std::mutex m_mutex; // 互斥锁
    std::condition_variable m_condvariable; // 条件变量
};