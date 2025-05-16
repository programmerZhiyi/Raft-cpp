#pragma once

#include <semaphore.h>
#include <zookeeper/zookeeper.h>
#include <string>

// 封装的zk客户端类
class ZkClient {
public:
    ZkClient();
    ~ZkClient();
    // zkclient启动连接zkserver
    void Start();
    // 在zkserver上创建一个节点
    void Create(const char *path, const char *data, int datalen, int state = 0);
    // 根据参数指定的znode节点路径，获取节点的数据
    std::string GetData(const char *path);
    // 判断节点是否存在
    bool Exists(const char *path);
    // 设置节点数据
    bool SetData(const char *path, const char *data, int datalen);
private:
    // zk的客户端句柄
    zhandle_t *m_zhandle;
};