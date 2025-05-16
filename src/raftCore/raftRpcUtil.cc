#include "raftRpcUtil.h"

#include "mprpccontroller.h"
#include "mprpcapplication.h"

bool RaftRpcUtil::AppendEntries(raftRpcSpace::AppendEntriesArguments *request, raftRpcSpace::AppendEntriesResults *response) {
    MprpcController controller;
    stub_->AppendEntries(&controller, request, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response) {
    MprpcController controller;
    stub_->RequestVote(&controller, request, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcSpace::InstallSnapshotArguments *request, raftRpcSpace::InstallSnapshotResults *response) {
    MprpcController controller;
    stub_->InstallSnapshot(&controller, request, response, nullptr);
    return !controller.Failed();
}

RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {
    stub_ = new raftRpcSpace::raftRpc_Stub(new MprpcChannel(ip, port));
}

RaftRpcUtil::~RaftRpcUtil() {
    delete stub_;
}