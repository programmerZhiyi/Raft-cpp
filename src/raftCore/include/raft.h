#pragma once

#include <memory>
#include "raftRpcUtil.h"
#include "Persister.h"
#include "config.h"
#include "ApplyMsg.h"
#include <boost/serialization/access.hpp>           // 用于 friend class boost::serialization::access
#include <boost/serialization/vector.hpp>           // 用于序列化 std::vector
#include <boost/serialization/string.hpp>           // 用于序列化 std::string
#include <unordered_map>
#include <boost/serialization/unordered_map.hpp> 

constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

// 投票状态
constexpr int Killed = 0;
constexpr int Voted = 1; // 本轮已经投过票了
constexpr int Expire = 2; // 投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcSpace::raftRpc {
private:
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
    std::shared_ptr<Persister> m_persister;
    int m_me; // 当前节点的ID
    int m_currentTerm; // 当前任期
    int m_votedFor;
    std::vector<raftRpcSpace::LogEntry> m_logs;
    int m_commitIndex;
    int m_lastApplied; // 已经汇报给状态机的log的index
    std::vector<int> m_nextIndex;
    std::vector<int> m_matchIndex;
    enum Status {Follower, Candidate, Leader};
    Status m_status;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // client从这里取日志，client和raft通信的接口
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
    std::chrono::_V2::system_clock::time_point m_lastResetHeartBeatTime;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    // 协程
    //std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

    class BoostPersistRaftNode {
    public:
        friend class boost::serialization::access;
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar &m_currentTerm;
            ar &m_votedFor;
            ar &m_lastSnapshotIncludeIndex;
            ar &m_lastSnapshotIncludeTerm;
            ar &m_logs;
        }
        int m_currentTerm;
        int m_votedFor;
        int m_lastSnapshotIncludeIndex;
        int m_lastSnapshotIncludeTerm;
        std::vector<std::string> m_logs;
        std::unordered_map<std::string, int> umap;
    };
public:
    void AppendEntries(const raftRpcSpace::AppendEntriesArguments* request, raftRpcSpace::AppendEntriesResults *response);
    void applierTicker();
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludeIndex, std::string snapshot);
    void doElection();
    void doHeartBeat();
    void electionTimeOutTicker();
    std::vector<ApplyMsg> getApplyLogs();
    int getNewCommandIndex();
    void getPrevLogInfo(int server, int *preIndex, int *preTerm);
    void GetState(int *term, bool *isLeader);
    void InstallSnapshot(const raftRpcSpace::InstallSnapshotArguments* request, raftRpcSpace::InstallSnapshotResults *response);
    void leaderHeartBeatTicker();
    void leaderSendSnapShot(int server);
    void leaderUpdateCommitIndex();
    bool matchLog(int logIndex, int logTerm);
    void persist();
    void RequestVote(const raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response);
    bool UpToDate(int index, int term);
    int getLastLogIndex();
    int getLastLogTerm();
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    int getLogTermFromLogIndex(int logIndex);
    int GetRaftStateSize();
    int getSlicesIndexFromLogIndex(int logIndex);

    bool sendRequestVote(int server, std::shared_ptr<raftRpcSpace::RequestVoteArguments> args,
                        std::shared_ptr<raftRpcSpace::RequestVoteResults> reply, std::shared_ptr<int> votedNum);
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcSpace::AppendEntriesArguments> args,
                            std::shared_ptr<raftRpcSpace::AppendEntriesResults> reply, std::shared_ptr<int> appendNums);

    // rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
    // ，避免使用pthread_create，因此专门写一个函数来执行
    void pushMsgToKvServer(ApplyMsg msg);
    void readPersist(std::string data);
    std::string persistData();

    //void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
    void Snapshot(int index, std::string snapshot);

    void AppendEntries(google::protobuf::RpcController *controller, const raftRpcSpace::AppendEntriesArguments *request, raftRpcSpace::AppendEntriesResults *response, ::google::protobuf::Closure *done) override;
    void InstallSnapshot(google::protobuf::RpcController *controller, const raftRpcSpace::InstallSnapshotArguments *request, raftRpcSpace::InstallSnapshotResults *response, ::google::protobuf::Closure *done) override;
    void RequestVote(google::protobuf::RpcController *controller, const raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response, ::google::protobuf::Closure *done) override;

    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyCh);
};