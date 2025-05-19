#pragma once

#include "kvServerRpc.pb.h"
#include <string>

class raftServerRpcUtil {
private:
    raftKVRpcSpace::kvServerRpc_Stub* stub;

public:

    //响应其他节点的方法
    bool Get(raftKVRpcSpace::GetArgs* GetArgs, raftKVRpcSpace::GetReply* reply);
    bool PutAppend(raftKVRpcSpace::PutAppendArgs* args, raftKVRpcSpace::PutAppendReply* reply);

    raftServerRpcUtil(std::string ip, short port);
    ~raftServerRpcUtil();
};