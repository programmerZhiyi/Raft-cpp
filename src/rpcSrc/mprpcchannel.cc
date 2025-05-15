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

    // 读取配置文件rpcserver的信息
    // std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    // uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    // rpc调用方想调用service_name的method_name服务，需要查询zk上该服务所在的host信息
    ZkClient zkCli;
    zkCli.Start();
    std::string method_path = "/" + service_name + "/" + method_name;
    std::unique_lock<std::mutex> lock(g_data_mutex); // 加锁，保证线程安全
    std::string host_data = zkCli.GetData(method_path.c_str());
    lock.unlock(); // 解锁
    if (host_data == "") {
        controller->SetFailed(method_path + " is not exist!");
        LOG(ERROR) << method_path + " is not exist!";  // 记录错误日志
        return;
    }
    int idx = host_data.find(":");
    if (idx == -1) {
        controller->SetFailed(method_path + " address is invalid!");
        LOG(ERROR) << method_path + " address is invalid!";  // 记录错误日志
        return;
    }

    std::string ip = host_data.substr(0, idx);
    uint16_t port = atoi(host_data.substr(idx + 1, host_data.size()).c_str());

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

// MprpcChannel::MprpcChannel() {
// }