#include <iostream>
#include <string>
#include "friend.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include <vector>

class FriendService : public example::FriendServiceRpc {
public:
    std::vector<std::string> GetFriendsList(uint32_t id) {
        std::cout << "doing local service: GetFriendsList" << std::endl;
        std::cout << "id: " << id << std::endl;
        return {"zhangsan", "lisi", "wangwu"};
    }

    // 重写基类FriendServiceRpc的虚函数
    void GetFriendsList(::google::protobuf::RpcController* controller,
                        const example::GetFriendsListRequest* request,
                        example::GetFriendsListResponse* response,
                        ::google::protobuf::Closure* done) {
        // 框架给业务上报了请求参数GetFriendsListRequest， 应用获取响应数据做本地业务
        uint32_t id = request->id();

        // 做本地业务
        std::vector<std::string> friends = GetFriendsList(id);

        // 把响应写入 包括错误码、错误消息、返回值
        example::ResultCode* code = response->mutable_result();
        code->set_errcode(0);
        code->set_errmsg("");
        for (const auto& friend_name : friends) {
            //response->add_friends(friend_name);
            std::string* friend_ptr = response->add_friends();
            *friend_ptr = friend_name;
        }

        // 执行回调操作   执行响应对象数据的序列号和网络发送（都是由框架来完成的）
        done->Run();
    }
};

int main(int argc, char **argv) {
    // 调用框架的初始化操作
    MprpcApplication::Init(argc, argv);

    // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
    RpcProvider provider;
    provider.NotifyService(new FriendService());

    // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
    provider.Run();

    return 0;
}