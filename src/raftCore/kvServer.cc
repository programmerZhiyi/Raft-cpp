#include "kvServer.h"
#include <thread>
#include "rpcprovider.h"

std::mutex g_data_mutex; // 从zk获取数据时加锁

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) {

    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);
    m_me = me;
    m_maxRaftState = maxraftstate;

    applyChan = std::make_shared<LockQueue<ApplyMsg> >();

    m_raftNode = std::make_shared<Raft>();
    // clerk层面 kvserver开启rpc接受功能
    // 同时raft与raft节点之间也要开启rpc功能，因此有两个注册
    std::thread t1([this, port]() -> void {
        // provider是一个rpc网络服务对象。把Service对象发布到rpc节点上
        RpcProvider provider;
        provider.NotifyService(this);
        provider.NotifyService(this->m_raftNode.get());
        // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
        provider.Run(m_me, port);
    });
    t1.detach();

    ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
    ////这里使用睡眠来保证
    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6);
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
    //获取所有raft节点ip、port ，并进行连接  ,要排除自己

    std::vector<std::pair<std::string, short> > ipPortVt;
    ZkClient zkCli;
    zkCli.Start();
    for (int i = 0; i < INT_MAX - 1; ++i) {
        std::string node = "/Raft/node" + std::to_string(i);
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
    std::vector<std::shared_ptr<RaftRpcUtil>> servers;
    //进行连接
    for (int i = 0; i < ipPortVt.size(); ++i) {
        if (i == m_me) {
            servers.push_back(nullptr);
            continue;
        }
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

        std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
    }
    sleep(ipPortVt.size() - me);  //等待所有节点相互连接成功，再启动raft
    m_raftNode->init(servers, m_me, persister, applyChan);
    // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的
    
    //kvdb初始化
    m_lastSnapShotRaftLogIndex = 0;
    auto snapshot = persister->ReadSnapshot();
    if (!snapshot.empty()) {
        ReadSnapShotToInstall(snapshot);
    }
    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);  //马上向其他节点宣告自己就是leader
    t2.join();  //由于ReadRaftApplyCommandLoop一直不会結束，达到一直卡在这的目的
}

// void KvServer::StartKVServer() {

// }

void KvServer::DprintfKVDB() {
    if (!DEBUG) {
        return;
    }
    std::lock_guard<std::mutex> lg(m_mtx);
    auto guard = makeGuard([&]() {
        m_skipList.display_list();
    });
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
    m_mtx.lock();

    m_skipList.insert_set_element(op.Key, op.Value);

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
    m_mtx.lock();
    *value = "";
    *exist = false;
    if (m_skipList.search_element(op.Key, *value)) {
        *exist = true;
    }

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();

    DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
    m_mtx.lock();
    m_skipList.insert_set_element(op.Key, op.Value);

    m_lastRequestId[op.ClientId] = op.RequestId;
    m_mtx.unlock();


    DprintfKVDB();
}

// 处理来自clerk的Get RPC
void KvServer::Get(const raftKVRpcSpace::GetArgs *args, raftKVRpcSpace::GetReply *reply) {
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftIndex = -1;
    int _ = -1;
    bool isLeader = false;
    m_raftNode->Start(op, &raftIndex, &_, &isLeader);  // raftIndex：raft预计的logIndex，虽然是预计，但是正确情况下是准确的，op的具体内容对raft来说 是隔离的

    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

    m_mtx.lock();

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];

    m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

    Op raftCommitOp;

    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        int _ = -1;
        bool isLeader = false;
        m_raftNode->GetState(&_, &isLeader);

        if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
            //如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
            // 不会违反线性一致性
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        } else {
            reply->set_err(ErrWrongLeader);  //返回这个，其实就是让clerk换一个节点重试
        }
    } else {
        // raft已经提交了该command（op），可以正式开始执行了
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
    m_mtx.lock();
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
    Op op;
    op.parseFromString(message.Command);

    DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
        return;
    }
}

// 检查请求是否重复
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
        return false;
    }
    return RequestId <= m_lastRequestId[ClientId];
}

// clerk 使用RPC远程调用
void KvServer::PutAppend(const raftKVRpcSpace::PutAppendArgs *args, raftKVRpcSpace::PutAppendReply *reply) {
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();
    int raftIndex = -1;
    int _ = -1;
    bool isleader = false;

    m_raftNode->Start(op, &raftIndex, &_, &isleader);

    if (!isleader) {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
            "not leader",
            m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

        reply->set_err(ErrWrongLeader);
        return;
    }
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
        "leader ",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
    m_mtx.lock();
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];

    m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

    Op raftCommitOp;

    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            reply->set_err(OK);  // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
        } else {
            reply->set_err(ErrWrongLeader);  ///这里返回这个的目的让clerk重新尝试
        }
    } else {
        DPrintf(
            "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
            "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
            m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            //可能发生leader的变更导致日志被覆盖，因此必须检查
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }

    m_mtx.lock();

    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    m_mtx.unlock();
}

////一直等待raft传来的applyCh
void KvServer::ReadRaftApplyCommandLoop() {
    while (true) {
        auto message = applyChan->Pop(); // 阻塞等待raft传来的applyCh
        DPrintf(
        "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);

        if (message.CommandValid) {
            GetCommandFromRaft(message);
        }
        if (message.SnapshotValid) {
            GetSnapShotFromRaft(message);
        }
    }
}

void KvServer::ReadSnapShotToInstall(std::string snapshot) {
    if (snapshot.empty()) {
        return;
    }
    parseFromString(snapshot);
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        return false;
    }
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}

// 检查是否需要制作快照，需要的话就向raft之下制作快照
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
    }
}

// raft节点向kvServer发送快照
void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
        ReadSnapShotToInstall(message.Snapshot);
    }
}

std::string KvServer::MakeSnapShot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcSpace::PutAppendArgs *request,
                ::raftKVRpcSpace::PutAppendReply *response, ::google::protobuf::Closure *done) {
    PutAppend(request, response);
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcSpace::GetArgs *request,
        ::raftKVRpcSpace::GetReply *response, ::google::protobuf::Closure *done) {
    Get(request, response);
    done->Run();    
}

std::string KvServer::getSnapshotData() {
    m_serializedKVData = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
}

void KvServer::parseFromString(const std::string &str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);
    m_serializedKVData.clear();
}