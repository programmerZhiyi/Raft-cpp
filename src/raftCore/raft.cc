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
        sleepNMilliseconds(ApplyInterval);
    }
}
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludeIndex, std::string snapshot) {
    return true;
}
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
void Raft::doHeartBeat() {
    std::lock_guard<std::mutex> g(m_mtx); //加锁，防止多线程同时访问

    if (m_status == Leader) {
        DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
        auto appendNums = std::make_shared<int>(1);  //表示成功接收心跳或日志复制的追随者节点数

        //对Follower（除了自己外的所有节点发送AE）
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                continue;
            }
            DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            // 日志压缩加入后要判断是发送快照还是发送AE
            // 如果nextIndex小于等于快照的index，说明需要发送快照
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
                std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行b函数，并传递参数
                t.detach();
                continue;
            }
            //构造发送值
            int preLogIndex = -1;
            int PrevLogTerm = -1;
            getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
            std::shared_ptr<raftRpcSpace::AppendEntriesArguments> appendEntriesArgs = std::make_shared<raftRpcSpace::AppendEntriesArguments>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(PrevLogTerm);
            appendEntriesArgs->clear_entries();
            appendEntriesArgs->set_leadercommit(m_commitIndex);
            if (preLogIndex != m_lastSnapshotIncludeIndex) {
                for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
                    raftRpcSpace::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = m_logs[j];  
                }
            } else {
                for (const auto& item : m_logs) {
                    raftRpcSpace::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
                    *sendEntryPtr = item;  
                }
            }
            int lastLogIndex = getLastLogIndex();
            // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
                    format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                            appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
            //构造返回值
            const std::shared_ptr<raftRpcSpace::AppendEntriesResults> appendEntriesReply = std::make_shared<raftRpcSpace::AppendEntriesResults>();
            appendEntriesReply->set_appstate(Disconnected); // 未与其他raft节点建立连接

            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply, appendNums);  // 创建新线程并执行b函数，并传递参数
            t.detach();
        }
        m_lastResetHeartBeatTime = now();  // leader发送心跳，就不是随机时间了
    }
}
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
int Raft::getNewCommandIndex() {
    //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}
void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm) {
    // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
        //要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}
void Raft::GetState(int *term, bool *isLeader) {
    m_mtx.lock();
    auto guard = makeGuard([&]() {
        m_mtx.unlock();
    });
    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}
void Raft::InstallSnapshot(const raftRpcSpace::InstallSnapshotArguments* request, raftRpcSpace::InstallSnapshotResults *response) {
    m_mtx.lock();
    auto guard = makeGuard([&]() {
        m_mtx.unlock();
    });
    if (request->term() < m_currentTerm) {
        response->set_term(m_currentTerm);
        return;
    }
    if (request->term() > m_currentTerm) {
        //后面两种情况都要接收日志
        m_currentTerm = request->term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
    }
    m_status = Follower;
    m_lastResetElectionTime = now();
    // 过时的快照
    if (request->lastincludedindex() <= m_lastSnapshotIncludeIndex) {
        return;
    }
    //截断日志，修改commitIndex和lastApplied
    //截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
    auto lastLogIndex = getLastLogIndex();

    if (lastLogIndex > request->lastincludedindex()) {
        m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(request->lastincludedindex()) + 1);
    } else {
        m_logs.clear();
    }
    m_commitIndex = std::max(m_commitIndex, request->lastincludedindex());
    m_lastApplied = std::max(m_lastApplied, request->lastincludedindex());
    m_lastSnapshotIncludeIndex = request->lastincludedindex();
    m_lastSnapshotIncludeTerm = request->lastincludedterm();

    response->set_term(m_currentTerm);
    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = request->data();
    msg.SnapshotTerm = request->lastincludedterm();
    msg.SnapshotIndex = request->lastincludedindex();

    std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
    t.detach();
    m_persister->Save(persistData(), request->data());
}
void Raft::leaderHeartBeatTicker() {
    while (true) {
        //  避免不是领导者情况，cpu空转，浪费资源
        while (m_status != Leader) {
            usleep(1000 * HeartBeatTimeout);
        }
        static std::atomic<int32_t> atomicCount = 0; // 线程安全的原子计数器
        // 表示当前线程需要睡眠的时间，计算方式基于心跳超时时间（HeartBeatTimeout）和上次重置心跳时间（m_lastResetHearBeatTime）
        // 目的：用于动态调整睡眠时间，避免线程频繁检查状态导致cpu空转
        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            wakeTime = now();
            suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHeartBeatTime - wakeTime;
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                        << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                        << std::endl;
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                        << " 毫秒\033[0m" << std::endl;
            ++atomicCount;
        }

        if (std::chrono::duration<double, std::milli>(m_lastResetHeartBeatTime - wakeTime).count() > 0) {
            //睡眠的这段时间有重置定时器，没有超时，再次睡眠
            continue;
        }
        doHeartBeat();
    }
}
void Raft::leaderSendSnapShot(int server) {
    m_mtx.lock();
    raftRpcSpace::InstallSnapshotArguments args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastincludedindex(m_lastSnapshotIncludeIndex);
    args.set_lastincludedterm(m_lastSnapshotIncludeTerm);
    args.set_data(m_persister->ReadSnapshot());

    raftRpcSpace::InstallSnapshotResults reply;
    m_mtx.unlock();
    bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
    m_mtx.lock();
    auto guard = makeGuard([&]() {
        m_mtx.unlock();
    });
    if (!ok) {
        return;
    }
    if (m_status != Leader || m_currentTerm != args.term()) {
        return;  //中间释放过锁，可能状态已经改变了
    }
    //	无论什么时候都要判断term
    if (reply.term() > m_currentTerm) {
        //三变
        m_currentTerm = reply.term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        m_lastResetElectionTime = now();
        return;
    }
    m_matchIndex[server] = args.lastincludedindex();
    m_nextIndex[server] = m_matchIndex[server] + 1;
}
void Raft::leaderUpdateCommitIndex() {
    m_commitIndex = m_lastSnapshotIncludeIndex;
    // for index := rf.commitIndex+1;index < len(rf.log);index++ {
    // for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
    for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
        int sum = 0;
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                sum += 1;
                continue;
            }
            if (m_matchIndex[i] >= index) {
                sum += 1;
            }
        }

        // 只有当前term有新提交的，才会更新commitIndex
        if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
            m_commitIndex = index;
            break;
        }
    }
}
bool Raft::matchLog(int logIndex, int logTerm) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
}
void Raft::persist() {
    auto data = persistData();
    m_persister->SaveRaftState(data);
}
void Raft::RequestVote(const raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response) {
    std::lock_guard<std::mutex> lg(m_mtx);
    auto guard = makeGuard([&]() {
        persist();
    });
    //对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
    //该竞选者已经OutOfDate(过时）
    if (request->term() < m_currentTerm) {
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
        response->set_term(m_currentTerm);
        response->set_votestate(Voted);
        response->set_votegranted(false);
        return;
    }
    // 如果当前节点已经投票给了其他候选人，或者已经投票给了自己，那么就不再投票
    if (m_votedFor != -1 && m_votedFor != request->candidateid()) {
        response->set_term(m_currentTerm);
        response->set_votestate(Voted);
        response->set_votegranted(false);
        return;
    } else {
        m_votedFor = request->candidateid();
        m_lastResetElectionTime = now();  //认为必须要在投出票的时候才重置定时器，
        response->set_term(m_currentTerm);
        response->set_votestate(Normal);
        response->set_votegranted(true);

        return;
    }
}
bool Raft::UpToDate(int index, int term) { // 这个函数的作用是判断当前节点的日志是否比给定的日志更新
    int lastIndex = -1;
    int lastTerm = -1;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    return term > lastTerm || (term == lastTerm && index >= lastIndex);
}
int Raft::getLastLogIndex() {
    int lastLogIndex = -1;
    int _ = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &_);
    return lastLogIndex;
}
int Raft::getLastLogTerm() {
    int _ = -1;
    int lastLogTerm = -1;
    getLastLogIndexAndTerm(&_, &lastLogTerm);
    return lastLogTerm;
}
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
int Raft::GetRaftStateSize() {
    return m_persister->RaftStateSize();
}
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
            m_nextIndex[i] = lastLogIndex + 1;  //
            m_matchIndex[i] = 0;                //每换一个领导都是从0开始
        }
        std::thread t(&Raft::doHeartBeat, this);  //马上向其他节点宣告自己就是leader
        t.detach();

        persist();
    }
    return true;
}
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcSpace::AppendEntriesArguments> args,
                        std::shared_ptr<raftRpcSpace::AppendEntriesResults> reply, std::shared_ptr<int> appendNums) {
    
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 ， args->entries_size():{%d}", m_me, server, args->entries_size());
    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());
    //这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
    if (!ok) {
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失败", m_me, server);
        return ok;
    }
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
    if (reply->appstate() == Disconnected) {
        return ok;
    }
    std::lock_guard<std::mutex> lg1(m_mtx);

    //对reply进行处理
    // 对于rpc通信，无论什么时候都要检查term
    if (reply->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        return ok;
    } else if (reply->term() < m_currentTerm) {
        DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(), m_me, m_currentTerm);
        return ok;
    }

    if (m_status != Leader) {
        //如果不是leader，那么就不要对返回的情况进行处理了
        return ok;
    }
    // term相等

    myAssert(reply->term() == m_currentTerm, format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
    if (!reply->success()) {
        //日志不匹配，正常来说就是index要往前-1，既然能到这里，第一个日志（idnex = 1）发送后肯定是匹配的，因此不用考虑变成负数 因为真正的环境不会知道是服务器宕机还是发生网络分区了
        if (reply->updatenextindex() != -100) { // -100是特殊标记，用于优化leader的回退逻辑
            DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me, server, reply->updatenextindex());
            m_nextIndex[server] = reply->updatenextindex();  //失败是不更新mathIndex的
        }
    } else {
        *appendNums = *appendNums + 1;
        DPrintf("---------------------------tmp------------------------- 节点{%d}返回true,当前*appendNums{%d}", server, *appendNums);
        // rf.matchIndex[server] = len(args.Entries) //只要返回一个响应就对其matchIndex应该对其做出反应，
        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
        m_nextIndex[server] = m_matchIndex[server] + 1;
        int lastLogIndex = getLastLogIndex();

        myAssert(m_nextIndex[server] <= lastLogIndex + 1,
                format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                        m_logs.size(), server, lastLogIndex));
        if (*appendNums >= 1 + m_peers.size() / 2) {
            //可以commit了
            *appendNums = 0; //重置
            if (args->entries_size() > 0) {
                DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                        args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
            }

            // 只有当前term的日志被大多数追随者同步后，才会被提交
            if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
                DPrintf(
                    "---------------------------tmp------------------------- 当前term有log成功提交，更新leader的m_commitIndex "
                    "from{%d} to{%d}",
                    m_commitIndex, args->prevlogindex() + args->entries_size());

                m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
            }
            myAssert(m_commitIndex <= lastLogIndex,
                    format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                            m_commitIndex));
        }
    }
    return ok;
}

// rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
// ，避免使用pthread_create，因此专门写一个函数来执行
void Raft::pushMsgToKvServer(ApplyMsg msg) {
    applyChan->Push(msg);
}
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
std::string Raft::persistData() {
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_votedFor = m_votedFor;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    for (auto& item : m_logs) {
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << boostPersistRaftNode;
    return ss.str();
}

void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader) {
    std::lock_guard<std::mutex> lg1(m_mtx);
    if (m_status != Leader) {
        DPrintf("[func-Start-rf{%d}]  is not leader");
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }

    raftRpcSpace::LogEntry newLogEntry;
    newLogEntry.set_command(command.asString());
    newLogEntry.set_logterm(m_currentTerm);
    newLogEntry.set_logindex(getNewCommandIndex());
    m_logs.emplace_back(newLogEntry);

    int lastLogIndex = getLastLogIndex();

    // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
    DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
    persist();
    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
}
void Raft::Snapshot(int index, std::string snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
        DPrintf(
            "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
            "smaller ",
            m_me, index, m_lastSnapshotIncludeIndex);
        return;
    }
    auto lastLogIndex = getLastLogIndex();  //为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

    //制造完此快照后剩余的所有日志
    int newLastSnapshotIncludeIndex = index;
    int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
    std::vector<raftRpcSpace::LogEntry> trunckedLogs;
    for (int i = index + 1; i <= getLastLogIndex(); i++) {
        trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
    }
    m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
    m_logs = trunckedLogs;
    m_commitIndex = std::max(m_commitIndex, index);
    m_lastApplied = std::max(m_lastApplied, index);

    m_persister->Save(persistData(), snapshot);

    DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
            m_lastSnapshotIncludeTerm, m_logs.size());
    myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
            format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                    m_lastSnapshotIncludeIndex, lastLogIndex));
}

void Raft::AppendEntries(google::protobuf::RpcController *controller, const raftRpcSpace::AppendEntriesArguments *request, raftRpcSpace::AppendEntriesResults *response, ::google::protobuf::Closure *done) {
    AppendEntries(request, response);
    done->Run();
}
void Raft::InstallSnapshot(google::protobuf::RpcController *controller, const raftRpcSpace::InstallSnapshotArguments *request, raftRpcSpace::InstallSnapshotResults *response, ::google::protobuf::Closure *done) {
    InstallSnapshot(request, response);
    done->Run();
}
void Raft::RequestVote(google::protobuf::RpcController *controller, const raftRpcSpace::RequestVoteArguments *request, raftRpcSpace::RequestVoteResults *response, ::google::protobuf::Closure *done) {
    RequestVote(request, response);
    done->Run();
}

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