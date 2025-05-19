#include "raftServerRpcUtil.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

// kvserver不同于raft节点之间，kvserver的rpc是用于clerk向kvserver调用，不会被调用，因此只用写caller功能，不用写callee功能
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port) {
    stub = new raftKVRpcSpace::kvServerRpc_Stub(new MprpcChannel(ip, port, false));
}

raftServerRpcUtil::~raftServerRpcUtil() { 
    delete stub; 
}

bool raftServerRpcUtil::Get(raftKVRpcSpace::GetArgs *GetArgs, raftKVRpcSpace::GetReply *reply) {
    MprpcController controller;
    stub->Get(&controller, GetArgs, reply, nullptr);
    return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcSpace::PutAppendArgs *args, raftKVRpcSpace::PutAppendReply *reply) {
    MprpcController controller;
    stub->PutAppend(&controller, args, reply, nullptr);
    if (controller.Failed()) {
        std::cout << controller.ErrorText() << std::endl;
    }
    return !controller.Failed();
}
