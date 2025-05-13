#include "raft.h"
#include "config.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <thread>

void Raft::AppendEntries(const raftRpcSpace::AppendEntriesArguments* request, raftRpcSpace::AppendEntriesResults *response) {
    std::lock_guard<std::mutex> locker(m_mtx);
    response->set_appstate(AppNormal); // 能接收到代表网络是正常的
    // 不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term
    if (request->term() < m_currentTerm) {
        response->set_success(false);
        response->set_term(m_currentTerm);
        response->set_updatenextindex(-100); // 论文中， 让领导人可以及时更新自己
        // rf 表示当前 Raft 节点对象（Raft 类的实例）本身
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, request->leaderid(), request->term(), m_me, m_currentTerm);
        return;
    }
    auto guard = makeGuard([&]() {
        persist();
    });
    if (request->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = request->term();
        m_votedFor = -1;
    }
    myAssert(request->term() == m_currentTerm, format("assert {request.Term == rf.currentTerm} failed"));
    m_status = Follower; // 变成follower
    m_lastResetElectionTime = now();

    // 比较日志，日志有3种情况
    if (request->prevlogindex() > getLastLogIndex()) { // 日志太新了
        response->set_success(false);
        response->set_term(m_currentTerm);
        response->set_updatenextindex(getLastLogIndex() + 1);
        return;
    } else if (request->prevlogindex() < m_lastSnapshotIncludeIndex) { // 日志太旧了
        response->set_success(false);
        response->set_term(m_currentTerm);
        response->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
        return;
    } 

    if (matchLog(request->prevlogindex(), request->prevlogterm())) {
        for (int i = 0; i < request->entries_size(); ++i) {
            auto log = request->entries(i);
            if (log.logindex() > getLastLogIndex()) {
                // 超过就直接添加日志
                m_logs.push_back(log);
            } else {
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() && m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
                    // 如果term相同，但是命令不同，说明有冲突
                    myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), request->leaderid(),
                                 log.command()));
                }
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
                    // 如果term不同，说明需要更新
                    m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
                }
            }
        }
        myAssert(
            getLastLogIndex() >= request->prevlogindex() + request->entries_size(),
            format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                m_me, getLastLogIndex(), request->prevlogindex(), request->entries_size()));
        if (request->leadercommit() > m_commitIndex) {
            // 更新commitIndex
            m_commitIndex = std::min(request->leadercommit(), getLastLogIndex());
        }
        myAssert(getLastLogIndex() >= m_commitIndex,
                format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                        getLastLogIndex(), m_commitIndex));

        response->set_success(true);
        response->set_term(m_currentTerm);
        return;
    } else {
        response->set_updatenextindex(request->prevlogindex());
        for (int index = request->prevlogindex(); index > m_lastSnapshotIncludeIndex; --index) {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(request->prevlogindex())) {
                // 寻找日志匹配加速
                response->set_updatenextindex(index + 1);
                break;
            }
        }
        return;
    }
}
void Raft::applierTicker() {
    while (true) {
        m_mtx.lock();
        if (m_status == Leader) {
        DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
                m_commitIndex);
        }
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();
        if (!applyMsgs.empty()) {
            DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver报告的applyMsgs长度为：{%d}", m_me, applyMsgs.size());
        }
        for (auto& message : applyMsgs) {
            applyChan->Push(message);
        }
        // usleep(1000 * ApplyInterval);
        sleepNMilliseconds(ApplyInterval);
    }
}
bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludeIndex, std::string snapshot);
void Raft::doElection() {
    std::lock_guard<std::mutex> g(m_mtx);

    if (m_status != Leader) {
        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
        //当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
        //重竞选超时，term也会增加的
        m_status = Candidate;
        ///开始新一轮的选举
        m_currentTerm += 1;
        m_votedFor = m_me;  //即是自己给自己投，也避免candidate给同任期的candidate投
        persist();
        std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 使用 make_shared 函数初始化，避免内存分配失败
        //	重新设置定时器
        m_lastResetElectionTime = now();
        //	发布RequestVote RPC
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            int lastLogIndex = -1, lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标

            std::shared_ptr<raftRpcSpace::RequestVoteArguments> request = std::make_shared<raftRpcSpace::RequestVoteArguments>();
            request->set_term(m_currentTerm);
            request->set_candidateid(m_me);
            request->set_lastlogindex(lastLogIndex);
            request->set_lastlogterm(lastLogTerm);
            auto response = std::make_shared<raftRpcSpace::RequestVoteResults>();

            //使用匿名函数执行避免其拿到锁

            std::thread t(&Raft::sendRequestVote, this, i, request, response, votedNum);  // 创建新线程并执行函数，并传递参数
            t.detach();
        }
    }
}
void doHeartBeat();
void Raft::electionTimeOutTicker() {
    while (true) {
        /**
         * 如果不睡眠，那么对于leader，这个函数会一直空转，浪费cpu。且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
         */
        while (m_status == Leader) {
            usleep(HeartBeatTimeout);  //定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
        }
        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{}; // 初始化了一个纳秒级别的时间间隔对象。
        std::chrono::system_clock::time_point wakeTime{}; // 记录时间节点
        {
            m_mtx.lock();
            wakeTime = now();
            suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
            m_mtx.unlock();
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();
            // 因为没有超过睡眠时间继续睡眠
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                        << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                        << std::endl;
            std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                        << std::endl;
        }

        if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
            //说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
            continue;
        }
        doElection();
    }
}
std::vector<ApplyMsg> Raft::getApplyLogs() {
    std::vector<ApplyMsg> applyMsgs;
    myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                        m_me, m_commitIndex, getLastLogIndex()));

    while (m_lastApplied < m_commitIndex) {
        m_lastApplied++;
        myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
                format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                        m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true;
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}
int getNewCommandIndex();
void getPrevLogInfo(int server, int *preIndex, int *preTerm);
void GetState(int *term, bool *isLeader);
void InstallSnapshot(const raftRpcSpace::InstallSnapshotArguments* request, raftRpcSpace::InstallSnapshotResults *response);
void leaderHeartBeatTicker();
void leaderSendSnapShot(int server);
void leaderUpdateCommitIndex();
bool Raft::matchLog(int logIndex, int logTerm) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
}
void persist();
void Raft::RequestVote(const raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response) {
    std::lock_guard<std::mutex> lg(m_mtx);
    auto guard = makeGuard([&]() {
        persist();
    });
    //对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
    // reason: 出现网络分区，该竞选者已经OutOfDate(过时）
    if (response->term() < m_currentTerm) {
        response->set_term(m_currentTerm);
        response->set_votestate(Expire);
        response->set_votegranted(false);
        return;
    }
    // 如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
    if (request->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = request->term();
        m_votedFor = -1; // 这里的目的是期望它投票继续
    }
    myAssert(request->term() == m_currentTerm, format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
    //	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了

    int lastLogTerm = getLastLogTerm();
    //只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
    if (!UpToDate(request->lastlogindex(), request->lastlogterm())) {
        // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
        //日志太旧了
        if (args->lastlogterm() < lastLogTerm) {
        //                    DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
        //                    candidate_lastlog_term{%v} < lastlog_term{%v}\n", rf.me, args.CandidateId, args.LastLogTerm,
        //                    lastLogTerm)
        } else {
        //            DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
        //            candidate_log_index{%v} < log_index{%v}\n", rf.me, args.CandidateId, args.LastLogIndex,
        //            rf.getLastLogIndex())
        }
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    }
    // todo ： 啥时候会出现rf.votedFor == args.CandidateId ，就算candidate选举超时再选举，其term也是不一样的呀
    //     当因为网络质量不好导致的请求丢失重发就有可能！！！！
    if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because has voted\n",
        //        rf.me, args.CandidateId)
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    } else {
        m_votedFor = args->candidateid();
        m_lastResetElectionTime = now();  //认为必须要在投出票的时候才重置定时器，
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.votedFor)
        reply->set_term(m_currentTerm);
        reply->set_votestate(Normal);
        reply->set_votegranted(true);

        return;
    }
}
bool UpToDate(int index, int term);
int Raft::getLastLogIndex() {
    int lastLogIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}
int getLastLogTerm();
void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm) {
    if (m_logs.empty()) {
        *lastLogIndex = m_lastSnapshotIncludeIndex;
        *lastLogTerm = m_lastSnapshotIncludeTerm;
    } else {
        *lastLogIndex = m_logs.back().logindex();
        *lastLogTerm = m_logs.back().logterm();
    }
}
int Raft::getLogTermFromLogIndex(int logIndex) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();

    myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
    
    if (logIndex == m_lastSnapshotIncludeIndex) {
        return m_lastSnapshotIncludeTerm;
    } else {
        // 这里需要注意，logIndex是从1开始的，而m_logs是从0开始的
        return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
    }
}
int GetRaftStateSize();
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
    myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                                m_me, logIndex, lastLogIndex));
    int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
    return SliceIndex;
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcSpace::RequestVoteArguments> args,
                    std::shared_ptr<raftRpcSpace::RequestVoteResults> reply, std::shared_ptr<int> votedNum) {
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 开始", m_me, m_currentTerm, getLastLogIndex());
    //这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
    // ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 完毕，耗时:{%d} ms", m_me, m_currentTerm, getLastLogIndex(), now() - start);

    if (!ok) {
        return ok;  //RPC通信失败就立即返回，避免资源浪费
    }
    // 对回应进行处理
    std::lock_guard<std::mutex> lg(m_mtx);
    if (reply->term() > m_currentTerm) {
        m_status = Follower;  //三变：身份，term，和投票
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist();
        return true;
    } else if (reply->term() < m_currentTerm) {
        return true;
    }
    myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

    if (!reply->votegranted()) {
        return true;
    }

    *votedNum = *votedNum + 1;

    if (*votedNum >= m_peers.size() / 2 + 1) {
        //变成leader
        *votedNum = 0;
        if (m_status == Leader) {
            //如果已经是leader了，那么是就是了，不会进行下一步处理了k
            myAssert(false, format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
        }
        //	第一次变成leader，初始化状态和nextIndex、matchIndex
        m_status = Leader;

        DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm, getLastLogIndex());

        int lastLogIndex = getLastLogIndex();
        for (int i = 0; i < m_nextIndex.size(); i++) {
            m_nextIndex[i] = lastLogIndex + 1;  //有效下标从1开始，因此要+1
            m_matchIndex[i] = 0;                //每换一个领导都是从0开始
        }
        std::thread t(&Raft::doHeartBeat, this);  //马上向其他节点宣告自己就是leader
        t.detach();

        persist();
    }
    return true;
}
bool sendAppendEntries(int server, std::shared_ptr<raftRpcSpace::AppendEntriesArguments> args,
                        std::shared_ptr<raftRpcSpace::AppendEntriesResults> reply, std::shared_ptr<int> appendNums);

// rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
// ，避免使用pthread_create，因此专门写一个函数来执行
//void pushMsgToKvServer(ApplyMsg msg);
void Raft::readPersist(std::string data) {
    if (data.empty()) {
        return;
    }
    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    // read class state from archive
    BoostPersistRaftNode boostPersistRaftNode;
    ia >> boostPersistRaftNode;

    m_currentTerm = boostPersistRaftNode.m_currentTerm;
    m_votedFor = boostPersistRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
    m_logs.clear();
    for (auto& item : boostPersistRaftNode.m_logs) {
        raftRpcSpace::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
}
std::string persistData();

//void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);
void Snapshot(int index, std::string snapshot);

void AppendEntries(google::protobuf::RpcController *controller, const raftRpcSpace::AppendEntriesArguments *request, raftRpcSpace::AppendEntriesResults *response, ::google::protobuf::Closure *done) override;
void InstallSnapshot(google::protobuf::RpcController *controller, const raftRpcSpace::InstallSnapshotArguments *request, raftRpcSpace::InstallSnapshotResults *response, ::google::protobuf::Closure *done) override;
void RequestVote(google::protobuf::RpcController *controller, const raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response, ::google::protobuf::Closure *done) override;

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister, std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    m_peers = peers; // 与其他节点沟通的rpc类
    m_persister = persister; // 持久化类
    m_me = me; // 当前节点的ID
    m_mtx.lock();

    applyChan = applyCh; // client从这里取日志，client和raft通信的接口
    m_currentTerm = 0; // 当前任期
    m_status = Follower; // 当前状态
    m_commitIndex = 0; // 已经提交的日志的index
    m_lastApplied = 0; // 已经汇报给状态机的log的index
    m_logs.clear(); 
    for (int i = 0; i < m_peers.size(); ++i) {
        m_matchIndex.push_back(0); // 表示没有日志条目已提交或已应用
        m_nextIndex.push_back(0);
    }
    m_votedFor = -1; // 表示没有投票给任何人
    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now();
    m_lastResetHeartBeatTime = now();

    readPersist(m_persister->ReadRaftState()); // 读取持久化的状态
    if (m_lastSnapshotIncludeIndex > 0) { // 如果有快照
        m_lastApplied = m_lastSnapshotIncludeIndex; 
    }

    DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);
    
    m_mtx.unlock();
    // 启动协程
    //m_ioManager = std::make_unique<monsoon::IOManager>(1)(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

    // m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
    // m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });

    std::thread t(&Raft::applierTicker, this);
    t.detach();
}