#include "mprpcchannel.h"
#include <string>
#include "rpcheader.pb.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <error.h>
#include "mprpcapplication.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include "mprpccontroller.h"
#include <memory>
#include "mprpclogger.h"

std::mutex g_data_mutex; // 线程安全锁

/*
header_size + service_name method_name args_size + args
*/
// 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
    google::protobuf::RpcController *controller,
    const google::protobuf::Message *request,
    google::protobuf::Message *response,
    google::protobuf::Closure *done) {
    
    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name(); // service_name
    std::string method_name = method->name(); // method_name

    // 获取参数的序列化字符串长度 args_size
    uint32_t arg_size = 0;
    std::string args_str;
    if (request->SerializeToString(&args_str)) {
        arg_size = args_str.size();
    } else {
        controller->SetFailed("serialize request error!");
        return;
    }

    // 定义rpc的请求header
    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(arg_size);

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (rpcHeader.SerializeToString(&rpc_header_str)) {
        header_size = rpc_header_str.size();
    } else {
        controller->SetFailed("serialize rpc_header error!");
        return;
    }

    // 组织待发送的rpc请求的字符串
    std::string send_rpc_str;
    send_rpc_str.insert(0, std::string((char*)&header_size, 4)); // 前4个字节是header_size
    send_rpc_str += rpc_header_str; // header
    send_rpc_str += args_str; // args

    // 打印调试信息
    std::cout << "=======================================" << std::endl;
    std::cout << "header_size: " << header_size << std::endl;
    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
    std::cout << "service_name: " << service_name << std::endl;
    std::cout << "method_name: " << method_name << std::endl;
    std::cout << "args_str: " << args_str << std::endl;
    std::cout << "=========================================" << std::endl;

    // 使用tcp编程，完成rpc方法的远程调用
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == clientfd) {
        char errstr[512] = {0};
        sprintf(errstr, "create socket error! errno:%d", errno);
        controller->SetFailed(errstr);
        LOG(ERROR) << "socket error:" << errstr;  // 记录错误日志
        return;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());

    // 连接rpc服务端
    if (-1 == connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
        close(clientfd);
        char errstr[512] = {0};
        sprintf(errstr, "connect error! errno:%d", errno);
        controller->SetFailed(errstr);
        LOG(ERROR) << "connect server error" << errstr;  // 记录错误日志
        return;
    }

    // 发送rpc请求的字符流
    if (-1 == send(clientfd, send_rpc_str.c_str(), send_rpc_str.size(), 0)) {
        close(clientfd);
        char errstr[512] = {0};
        sprintf(errstr, "send error! errno:%d", errno);
        controller->SetFailed(errstr);
        return;
    }

    // 接收rpc响应的字符流
    char recv_buf[1024] = {0};
    int recv_size = 0;
    if (-1 == (recv_size = recv(clientfd, recv_buf, 1024, 0))) {
        close(clientfd);
        char errstr[512] = {0};
        sprintf(errstr, "recv error! errno:%d", errno);
        controller->SetFailed(errstr);
        return;
    }

    // 反序列化rpc响应的字符流
    //std::string response_str(recv_buf, 0, recv_size); // bug出现问题，recv_buf中遇到了\0，导致字符串截断
    //if (!response->ParseFromString(response_str)) {
    if (!response->ParseFromArray(recv_buf, recv_size)) {
        close(clientfd);
        char errstr[2048] = {0};
        sprintf(errstr, "parse response error! errno:%s", recv_buf);
        controller->SetFailed(errstr);
        return;
    }

    close(clientfd);
}

MprpcChannel::MprpcChannel(std::string ip, short port) : ip(ip), port(port) {
    m_clientfd = -1;
    // 连接远端rpc服务节点
    std::string errMsg;
    auto rt = newConnect(ip.c_str(), port, &errMsg);
    int tryConut = 3;
    while (!rt && tryConut > 0) {
        std::cout << "connect to server error! ip:" << ip << " port:" << port << " errMsg:" << errMsg << std::endl;
        rt = newConnect(ip.c_str(), port, &errMsg);
        --tryConut;
    }
}

bool MprpcChannel::newConnect(const char *ip, uint16_t port, std::string *errMsg) {
    m_clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == m_clientfd) {
        char errtxt[512] = {0};
        sprintf(errtxt, "create socket error! errno:%d", errno);
        *errMsg = errtxt;
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);

    // 连接rpc服务端
    if (-1 == connect(m_clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
        close(m_clientfd);
        char errtxt[512] = {0};
        sprintf(errtxt, "create socket error! errno:%d", errno);
        *errMsg = errtxt;
        return false;
    }
    return true;
}