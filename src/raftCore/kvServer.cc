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

    void StartKVServer();

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