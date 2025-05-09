#pragma once
#include "lockqueue.h"
#include <string>

enum LogLevel {
    INFO, // 普通信息
    ERROR, // 错误信息
};

// MpRpc框架提供的日志系统
class Logger {
public:
    // 获取单例对象
    static Logger& GetInstance();

    // 设置日志级别
    void SetLogLevel(LogLevel level);
    // 写日志
    void Log(std::string msg);
private:
    int m_logLevel; // 日志级别
    LockQueue<std::string> m_lckQue; // 日志缓冲队列
    
    Logger();
    Logger(const Logger&) = delete;
    Logger(Logger&&) = delete;
};

// 定义宏