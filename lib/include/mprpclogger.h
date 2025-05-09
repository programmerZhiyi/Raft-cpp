#pragma once
#include <glog/logging.h>
#include <string>

// 采用RAII的思想
class MprpcLogger {
public:
    // 构造函数，自动初始化glog
    explicit MprpcLogger(const char *argv0) {
        google::InitGoogleLogging(argv0);
        FLAGS_colorlogtostderr = true; // 彩色输出
        FLAGS_logtostderr = true; // 输出到标准错误
    }
    ~MprpcLogger() {
        google::ShutdownGoogleLogging();
    }
    // 提供静态日志方法
    static void Info(const std::string &message) {
        LOG(INFO) << message;
    }
    static void Warning(const std::string &message) {
        LOG(WARNING) << message;
    }
    static void Error(const std::string &message) {
        LOG(ERROR) << message;
    }
    static void Fatal(const std::string &message) {
        LOG(FATAL) << message;
    }
// 禁用拷贝构造函数和重载赋值函数
private:
    MprpcLogger(const MprpcLogger &) = delete;
    MprpcLogger &operator=(const MprpcLogger &) = delete;
};