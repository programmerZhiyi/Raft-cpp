#include "zookeeperutil.h"
#include "mprpcapplication.h"
#include <semaphore.h>
#include <iostream>
#include "mprpclogger.h"

// 全局的watcher观察器   zkserver给zkclient的通知
void global_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_SESSION_EVENT) { // 回调的消息类型是和会话相关的消息类型
        if (state == ZOO_CONNECTED_STATE) {
            sem_t *sem = (sem_t*)zoo_get_context(zh);
            sem_post(sem); // 连接成功，释放信号量
        }
    }
}

ZkClient::ZkClient() : m_zhandle(nullptr) {    
}

ZkClient::~ZkClient() {
    if (m_zhandle != nullptr) {
        zookeeper_close(m_zhandle); // 关闭句柄，释放资源
    }
}

// 连接zkserver
void ZkClient::Start() {
    std::string host = MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    std::string connstr = host + ":" + port;

    /*
    zookeeper_mt: 多线程版本
    zookeeper的API客户端程序提供了三个线程
    API调用线程
    网络I/O线程 pthread_create poll
    watcher回调线程
    */
    m_zhandle = zookeeper_init(connstr.c_str(), global_watcher, 30000, nullptr, nullptr, 0);
    if (m_zhandle == nullptr) {
        LOG(ERROR) << "zookeeper_init error! errno:" << errno;
        exit(EXIT_FAILURE);
    }

    sem_t sem;
    sem_init(&sem, 0, 0); // 初始化信号量
    zoo_set_context(m_zhandle, &sem); // 设置信号量到zkclient的上下文中

    sem_wait(&sem); // 等待连接成功的信号
    LOG(INFO) << "zookeeper connect success!"; // 连接成功
}

void ZkClient::Create(const char *path, const char *data, int datalen, int state) {
    char path_buffer[128];
    int bufferlen = sizeof(path_buffer);
    int flag;
    flag = zoo_exists(m_zhandle, path, 0, nullptr);
    if (ZNONODE == flag) {
        flag = zoo_create(m_zhandle, path, data, datalen, &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK) {
            LOG(INFO) << "create znode success! path:" << path;
        } else {
            LOG(ERROR) << "create znode error! path:" << path << " errno:" << flag;
            exit(EXIT_FAILURE);
        }
    }
}

// 根据指定的path， 获取znode节点的数据
std::string ZkClient::GetData(const char *path) {
    char buffer[64];
    int bufferlen = sizeof(buffer);
    int flag = zoo_get(m_zhandle, path, 0, buffer, &bufferlen, nullptr);
    if (flag == ZOK) {
        return buffer;
    } else {
        LOG(ERROR) << "get znode data error! path:" << path << " errno:" << flag;
        return "";
    }
}