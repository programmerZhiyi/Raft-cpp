#include "clerk.h"
#include "zookeeperutil.h"
#include "config.h"

std::mutex g_data_mutex; // 从zk获取数据时加锁

std::string Clerk::Uuid() {
    return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
}  //用于返回随机的clientId

void Clerk::PutAppend(std::string key, std::string value, std::string op) {
    m_requestId++;
    auto requestId = m_requestId;
    auto server = m_recentLeaderId;
    while (true) {
        raftKVRpcSpace::PutAppendArgs args;
        args.set_key(key);
        args.set_value(value);
        args.set_op(op);
        args.set_clientid(m_clientId);
        args.set_requestid(requestId);
        raftKVRpcSpace::PutAppendReply reply;
        bool ok = m_servers[server]->PutAppend(&args, &reply);
        // if (ok) {
        //     std::cout << "【Clerk::PutAppend】PutAppend成功" << std::endl;
        //     return;
        // }
        // return;
        if (!ok || reply.err() == ErrWrongLeader) {
            DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}", server, (server + 1) % m_servers.size(), op.c_str());
            if (!ok) {
                DPrintf("重试原因 ，rpc失败 ，");
                //break;
            }
            if (reply.err() == ErrWrongLeader) {
                DPrintf("重试原因：非leader");
            }
            server = (server + 1) % m_servers.size();  // try the next server
            continue;
        }
        if (reply.err() == OK) {
            m_recentLeaderId = server;
            return;
        }
    }
}

void Clerk::Init() {
    //获取所有raft节点ip、port ，并进行连接
    std::vector<std::pair<std::string, short> > ipPortVt;
    ZkClient zkCli;
    zkCli.Start();
    for (int i = 0; i < INT_MAX - 1; ++i) {
        std::string node = "/Raftnode" + std::to_string(i);
        if (!zkCli.Exists(node.c_str())) {
            break;
        }
        std::unique_lock<std::mutex> lock(g_data_mutex);
        std::string nodeInfor = zkCli.GetData(node.c_str());
        lock.unlock();
        int idx = nodeInfor.find(":");
        std::string nodeIp = nodeInfor.substr(0, idx);
        std::string nodePortStr = nodeInfor.substr(idx + 1);
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));
    }
    //进行连接
    for (const auto& item : ipPortVt) {
        std::string ip = item.first;
        short port = item.second;
        auto* rpc = new raftServerRpcUtil(ip, port);
        m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
    }
}

std::string Clerk::Get(std::string key) {
    m_requestId++;
    auto requestId = m_requestId;
    int server = m_recentLeaderId;
    raftKVRpcSpace::GetArgs args;
    args.set_key(key);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    while (true) {
        raftKVRpcSpace::GetReply reply;
        bool ok = m_servers[server]->Get(&args, &reply);
        if (!ok || reply.err() == ErrWrongLeader) {  //会一直重试，因为requestId没有改变，因此可能会因为RPC的丢失或者其他情况导致重试，kvserver层来保证不重复执行（线性一致性）
            server = (server + 1) % m_servers.size();
            continue;
        }
        if (reply.err() == ErrNoKey) {
            return "";
        }
        if (reply.err() == OK) {
            m_recentLeaderId = server;
            return reply.value();
        }
    }
    return "";
}

void Clerk::Put(std::string key, std::string value) {
    PutAppend(key, value, "Put");
}
void Clerk::Append(std::string key, std::string value) {
    PutAppend(key, value, "Append");
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {
}