#include <iostream>
#include "mprpcapplication.h"
#include "kv.pb.h"
#include "mprpclogger.h"

int main(int argc, char **argv) {
    // 整个程序启动以后，想使用mprpc框架来享受rpc服务调用，一定需要先调用框架的初始化函数（只初始化一次）
    MprpcLogger logger("MyRPC");
    MprpcApplication::Init(argc, argv);

    // 演示调用远程发布的rpc方法Login
    kv::KVService_Stub stub(new MprpcChannel());
    // rpc方法的请求参数
    kv::GetRequest request;
    request.set_key("name");
    // rpc方法的响应参数
    kv::GetResponse response;
    // 发起rpc方法的调用   同步的rpc调用过程   MprpcChannel::callmethod
    MprpcController controller;
    // stub.Put(&controller, &request, &response, nullptr); // RpcChannel->RpcChannel::callMethod 集中来做所有rpc方法调用的参数序列化和网络发送
    
    // // 一次rpc调用完成，读调用的结果
    // if (controller.Failed()) {
    //     std::cout << "rpc call failed! error: " << controller.ErrorText() << std::endl;
    // } else {
    //     if (true == response.success()) {
    //         std::cout << "rpc call success!" << std::endl;
    //         std::cout << "put success!" << std::endl;
    //         std::cout << "key: " << request.key() << std::endl;
    //         std::cout << "value: " << request.value() << std::endl;
    //         std::cout << "version: " << request.version() << std::endl;
    //     } else {
    //         std::cout << "rpc call failed! error: " << response.error() << std::endl;
    //     }
    // }
    stub.Get(&controller, &request, &response, nullptr); // RpcChannel->RpcChannel::callMethod 集中来做所有rpc方法调用的参数序列化和网络发送
    if (controller.Failed()) {
        std::cout << "rpc call failed! error: " << controller.ErrorText() << std::endl;
    } else {
        if (response.error() == "ErrNoKey") {
            std::cout << "rpc call success!" << std::endl;
            std::cout << "get failed! error: " << response.error() << std::endl;
        } else {
            std::cout << "rpc call success!" << std::endl;
            std::cout << "key: " << request.key() << std::endl;
            std::cout << "value: " << response.value() << std::endl;
            std::cout << "version: " << response.version() << std::endl;
        }
    }
    return 0;
}