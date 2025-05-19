#pragma once

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include "zookeeperutil.h"
#include <string>

class MprpcChannel : public google::protobuf::RpcChannel {
public:
    // 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据序列化和网络发送
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done);
    MprpcChannel(std::string ip, short port, bool connectNow);
private:
    std::string ip; // 远端rpc服务节点的ip
    short port; // 远端rpc服务节点的端口
    int m_clientfd; // 客户端socket文件描述符
    bool newConnect(const char *ip, uint16_t port, std::string *errMsg);
};