#pragma once

#include "kvServerRpc.pb.h"
#include "raft.h"
#include "skipList.h"

class KvServer : raftKVRpcSpace::kvServerRpc {
private:
    std::mutex m_mtx;
    int m_me;
    std::shared_ptr<Raft> m_raftNode;
    std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer和raft节点的通信管道
    int m_maxRaftState;                               // raft节点的最大状态

    std::string m_serializedKVData;  // 序列化后的kv数据
    SkipList<std::string, std::string> m_skipList;
    std::unordered_map<std::string, std::string> m_kvDB;

    std::unordered_map<int, LockQueue<Op> *> waitApplyCh; // waitApplyCh是一个map，键是int，值是Op类型的管道

    std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //一个kV服务器可能连接多个client

    int m_lastSnapShotRaftLogIndex;

    friend class boost::serialization::access;
public:
    KvServer() = delete;

    KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

    //void StartKVServer();

    void DprintfKVDB();

    void ExecuteAppendOpOnKVDB(Op op);

    void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

    void ExecutePutOpOnKVDB(Op op);

    void Get(const raftKVRpcSpace::GetArgs *args, raftKVRpcSpace::GetReply *reply); 

    void GetCommandFromRaft(ApplyMsg message);

    bool ifRequestDuplicate(std::string ClientId, int RequestId); // 检查请求是否重复

    // clerk 使用RPC远程调用
    void PutAppend(const raftKVRpcSpace::PutAppendArgs *args, raftKVRpcSpace::PutAppendReply *reply);

    ////一直等待raft传来的applyCh
    void ReadRaftApplyCommandLoop();

    void ReadSnapShotToInstall(std::string snapshot);

    bool SendMessageToWaitChan(const Op &op, int raftIndex);

    // 检查是否需要制作快照，需要的话就向raft之下制作快照
    void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

    // raft节点向kvServer发送快照
    void GetSnapShotFromRaft(ApplyMsg message);

    std::string MakeSnapShot();

    void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcSpace::PutAppendArgs *request,
                    ::raftKVRpcSpace::PutAppendReply *response, ::google::protobuf::Closure *done) override;

    void Get(google::protobuf::RpcController *controller, const ::raftKVRpcSpace::GetArgs *request,
            ::raftKVRpcSpace::GetReply *response, ::google::protobuf::Closure *done) override;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)  //这里面写需要序列话和反序列化的字段
    {
        ar &m_serializedKVData;

        ar &m_lastRequestId;
    }

    std::string getSnapshotData();

    void parseFromString(const std::string &str);
};