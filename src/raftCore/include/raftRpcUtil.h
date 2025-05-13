#pragma once

#include "raftRpc.pb.h"
#include <string>

class RaftRpcUtil {
private:
    raftRpcSpace::raftRpc_Stub *stub_;
public:
    bool AppendEntries(raftRpcSpace::AppendEntriesArguments *request, raftRpcSpace::AppendEntriesResults *response);
    bool RequestVote(raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response);
    bool InstallSnapshot(raftRpcSpace::InstallSnapshotArguments *request, raftRpcSpace::InstallSnapshotResults *response);
    RaftRpcUtil(std::string ip, short port);
    ~RaftRpcUtil();
};