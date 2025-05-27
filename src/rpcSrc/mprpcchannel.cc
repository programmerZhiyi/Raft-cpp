#include "mprpcchannel.h"
#include <string>
#include "rpcheader.pb.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <error.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include "mprpccontroller.h"
#include <memory>
#include "mprpclogger.h"
#include "config.h"


/*
header_size + service_name method_name args_size + args
*/
// 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
    google::protobuf::RpcController *controller,
    const google::protobuf::Message *request,
    google::protobuf::Message *response,
    google::protobuf::Closure *done) {

    if (m_clientfd == -1) {
        std::string errMsg;
        bool rt = newConnect(ip.c_str(), port, &errMsg);
        if (!rt) {
            DPrintf("[func-MprpcChannel::CallMethod]重连接ip：{%s} port{%d}失败", ip.c_str(), port);
            controller->SetFailed(errMsg);
            return;
        } else {
            DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", ip.c_str(), port);
        }
    }
    
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

    // 发送rpc请求
    //失败会重试连接再发送，重试连接失败会直接return
    while (-1 == send(m_clientfd, send_rpc_str.c_str(), send_rpc_str.size(), 0)) {
        char errtxt[512] = {0};
        sprintf(errtxt, "send error! errno:%d", errno);
        std::cout << "尝试重新连接，对方ip：" << ip << " 对方端口" << port << std::endl;
        close(m_clientfd);
        m_clientfd = -1;
        std::string errMsg;
        bool rt = newConnect(ip.c_str(), port, &errMsg);
        if (!rt) {
            controller->SetFailed(errMsg);
            return;
        }
    }

    // 接收rpc响应的字符流
    char recv_buf[1024] = {0};
    int recv_size = 0;
    if (-1 == (recv_size = recv(m_clientfd, recv_buf, 1024, 0))) {
        close(m_clientfd);
        char errstr[512] = {0};
        sprintf(errstr, "recv error! errno:%d", errno);
        controller->SetFailed(errstr);
        return;
    }

    // 反序列化rpc响应的字符流
    //std::string response_str(recv_buf, 0, recv_size); // bug出现问题，recv_buf中遇到了\0，导致字符串截断
    //if (!response->ParseFromString(response_str)) {
    if (!response->ParseFromArray(recv_buf, recv_size)) {
        // close(m_clientfd);
        char errstr[2048] = {0};
        sprintf(errstr, "parse response error! errno:%s", recv_buf);
        controller->SetFailed(errstr);
        return;
    }

    //close(m_clientfd);
}

MprpcChannel::MprpcChannel(std::string ip, short port, bool connectNow) : ip(ip), port(port), m_clientfd(-1) {
    if (!connectNow) {
        return;
    }
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
    //int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    m_clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == m_clientfd) {
        char errtxt[512] = {0};
        sprintf(errtxt, "create socket error! errno:%d", errno);
        //m_clientfd = -1;
        *errMsg = errtxt;
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);
    printf("[DEBUG] Connecting to %s:%d\n", ip, port);
    // 连接rpc服务端
    if (-1 == connect(m_clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr))) {
        close(m_clientfd);
        char errtxt[512] = {0};
        sprintf(errtxt, "connect error! errno:%d", errno);
        m_clientfd = -1;
        *errMsg = errtxt;
        return false;
    }
    return true;
}
