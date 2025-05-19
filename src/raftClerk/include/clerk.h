#pragma once

#include <vector>
#include "raftServerRpcUtil.h"
#include <memory>

class Clerk {
private:
    std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers; 
    std::string m_clientId; //保存所有raft节点的fd
    int m_requestId;
    int m_recentLeaderId;  //只是有可能是领导

    std::string Uuid(); //用于返回随机的clientId

    void PutAppend(std::string key, std::string value, std::string op);
public:
    //对外暴露的三个功能和初始化
    void Init(std::string configFileName);
    std::string Get(std::string key);

    void Put(std::string key, std::string value);
    void Append(std::string key, std::string value);

    Clerk();
};