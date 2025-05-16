#pragma once

#include <functional>
#include <string>
#include <vector>
#include <chrono>
#include <mutex>
#include <queue>
#include <condition_variable>

const bool DEBUG = true;

void DPrintf(const char *format, ...);

template <class F>
class ScopeGuard {
public:
    explicit ScopeGuard(F&& f) : m_func(std::forward<F>(f)) {}
    ~ScopeGuard() { m_func(); }
    
    ScopeGuard(const ScopeGuard&) = delete;
    ScopeGuard& operator=(const ScopeGuard&) = delete;
    
private:
    F m_func;
};

// 辅助函数用于类型推导
template <class F>
ScopeGuard<F> makeGuard(F&& f) {
    return ScopeGuard<F>(std::forward<F>(f));
}

void myAssert(bool condition, std::string message); // 断言函数

template <typename... Args>
std::string format(const char* format_str, Args... args) {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // "\0"
    if (size_s <= 0) { throw std::runtime_error("Error during formatting."); }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}

std::chrono::_V2::system_clock::time_point now(); // 获取当前时间

template <typename T>
class LockQueue {
public:
    // 多个worker线程都会写日志queue
    void Push(const T& data) {
        std::lock_guard<std::mutex> lock(m_mutex);  //使用lock_gurad，即RAII的思想保证锁正确释放
        m_queue.push(data);
        m_condvariable.notify_one();
    }

    // 一个线程读日志queue，写日志文件
    T Pop() {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty()) {
            // 日志队列为空，线程进入wait状态
            m_condvariable.wait(lock);  //这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
        }
        T data = m_queue.front();
        m_queue.pop();
        return data;
    }

    bool timeOutPop(int timeout, T* ResData)  // 添加一个超时时间参数，默认为 50 毫秒
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        // 获取当前时间点，并计算出超时时刻
        auto now = std::chrono::system_clock::now();
        auto timeout_time = now + std::chrono::milliseconds(timeout);

        // 在超时之前，不断检查队列是否为空
        while (m_queue.empty()) {
            // 如果已经超时了，就返回一个空对象
            if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout) {
                return false;
            } else {
                continue;
            }
        }

        T data = m_queue.front();
        m_queue.pop();
        *ResData = data;
        return true;
    }

private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_condvariable;
};

void sleepNMilliseconds(int N); // 睡眠函数

const int debugMul = 1;  // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 25 * debugMul;  // 心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 10 * debugMul;     // apply日志的间隔时间

std::chrono::milliseconds getRandomizedElectionTimeout(); // 获取随机的选举超时时间

const int minRandomizedElectionTime = 300 * debugMul;  // ms
const int maxRandomizedElectionTime = 500 * debugMul;  // ms

class Op {
public:
    std::string Operation;  // "Get" "Put" "Append"
    std::string Key;
    std::string Value;
    std::string ClientId;  //客户端号码
    int RequestId;         //客户端号码请求的Request的序列号，为了保证线性一致性
    std::string asString() const;

    bool parseFromString(std::string str);

    friend std::ostream& operator<<(std::ostream& os, const Op& obj);

private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& Operation;
        ar& Key;
        ar& Value;
        ar& ClientId;
        ar& RequestId;
    }
};