#pragma once

#include "mprpcconfig.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include <mutex>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <string>

// mprpc框架的基础类，负责框架的一些初始化操作
class MprpcApplication {
public:
    static void Init(int argc, char **argv);
    static MprpcApplication& GetInstance();
    static MprpcConfig& GetConfig();
    static void deleteInstance();
private:
    static MprpcConfig m_config;
    static MprpcApplication *m_application; // 全局唯一单例访问对象
    static std::mutex m_mutex; // 线程安全锁
    MprpcApplication() {}
    ~MprpcApplication() {}
    MprpcApplication(const MprpcApplication&) = delete;
    MprpcApplication(MprpcApplication&&) = delete;
};