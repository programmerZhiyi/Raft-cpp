#include "rpcprovider.h"
#include "rpcheader.pb.h"
#include "mprpclogger.h"
#include <netdb.h>
#include <arpa/inet.h>

/*
service_name => service描述
                       =>   service*记录服务对象
                       method_name => method方法对象
json   protobuf
*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
void RpcProvider::NotifyService(google::protobuf::Service *service) {
    ServiceInfo service_info;

    // 获取了服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();
    // 获取服务对象service的方法的数量
    int methodCnt = pserviceDesc->method_count();

    std::cout << "service_name:" << service_name << std::endl;

    for (int i = 0; i < methodCnt; ++i) {
        // 获取了服务对象指定下标的服务方法的描述（抽象描述）   UserService Login
        const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        service_info.m_methodMap.insert({method_name, pmethodDesc});

        std::cout << "method_name:" << method_name << std::endl;
    }
    service_info.m_service = service;
    m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务发布节点，开始提供rpc远程网络调用服务
void RpcProvider::Run(int nodeIndex, short port) {
    // 获取可用ip
    char *ipC;
    char hname[128];
    struct hostent *hent;
    gethostname(hname, sizeof(hname));
    hent = gethostbyname(hname);
    for (int i = 0; hent->h_addr_list[i]; ++i) {
        ipC = inet_ntoa(*(struct in_addr *)hent->h_addr_list[i]); // 获取ip地址
    }
    std::string ip(ipC);
    // std::string ip = "127.0.0.1"; // 固定使用 localhost 进行测试

    muduo::net::InetAddress address(ip, port);

    // 创建TcpServer对象
    server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");
    // 绑定连接回调和消息读写回调方法   分离了网络代码和业务代码
    server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

    // 设置muduo库的线程数量
    server->setThreadNum(4);

    // 把当前rpc节点上要发布的服务全部注册到zk上面，让rpc client可以从zk上发现服务
    ZkClient zkCli;
    zkCli.Start();
    std::string node = "/Raftnode" + std::to_string(nodeIndex);
    // 创建节点
    char node_data[128] = {0};
    sprintf(node_data, "%s:%d", ip.c_str(), port);
    if (zkCli.Exists(node.c_str())) {
        // 节点已经存在，设置数据
        zkCli.SetData(node.c_str(), node_data, sizeof(node_data));
    } else {
        // 节点不存在，创建节点
        zkCli.Create(node.c_str(), node_data, sizeof(node_data), ZOO_EPHEMERAL);
    }

    // rpc服务端准备启动，打印信息
    std::cout << "RpcProvider start service at ip:" << ip << " port:" << port << std::endl;

    // 启动网络服务
    server->start();
    m_eventLoop.loop();
}

// 新的socket连接回调
void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn) {
    if (!conn->connected()) {
        // 和rpc client的连接断开了
        conn->shutdown();
    }
}

/*
在框架内部，RpcProvider和RpcConsumer协商好之间通信用的protobuf数据类型
service_name method_name args    定义proto的message类型，进行数据头的序列化和反序列化
                                 service_name method_name args_size
16UserServiceLoginzhang san123456

header_size(4个字节) + header_str + args_str
std::string   insert和copy方法
*/
// 已建立连接用户的读写事件回调 如果远程有一个rpc服务的调用请求，那么OnMessage方法就会响应
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp time) {
    // 网络上接收的远程rpc调用请求的字符流   Login args
    std::string recv_buf = buffer->retrieveAllAsString();

    // 从字符流中读取前4个字节的内容
    uint32_t header_size = 0;
    recv_buf.copy((char*)&header_size, 4, 0);

    // 根据header_size读取数据头的原始字符流，反序列化数据，得到rpc请求的详细参数
    std::string rpc_header_str = recv_buf.substr(4, header_size);
    mprpc::RpcHeader rpcHeader;
    std::string service_name;
    std::string method_name;
    uint32_t args_size;
    if (rpcHeader.ParseFromString(rpc_header_str)) {
        // 数据头反序列化成功
        service_name = rpcHeader.service_name();
        method_name = rpcHeader.method_name();
        args_size = rpcHeader.args_size();
    } else {
        // 数据头反序列化失败
        MprpcLogger::Error("rpcHeader parse error!");
        return;
    }
    // 获取rpc方法参数的字符流数据
    std::string args_str = recv_buf.substr(4 + header_size, args_size);

    // 获取service对象和method对象
    auto it = m_serviceMap.find(service_name);
    if (it == m_serviceMap.end()) {
        std::cout << "service_name:" << service_name << " not found!" << std::endl;
        return;
    }

    auto it_method = it->second.m_methodMap.find(method_name);
    if (it_method == it->second.m_methodMap.end()) {
        std::cout << service_name << ":" << method_name << " not found!" << std::endl;
        return;
    }

    google::protobuf::Service *service = it->second.m_service; // 获取service对象
    const google::protobuf::MethodDescriptor *method = it_method->second; // 获取method对象

    // 生成rpc方法调用的请求request和响应response参数
    google::protobuf::Message *request = service->GetRequestPrototype(method).New();
    if (!request->ParseFromString(args_str)) {
        std::cout << "args_str:" << args_str << " parse error!" << std::endl;
        return;
    }
    google::protobuf::Message *response = service->GetResponsePrototype(method).New();

    // 给下面的method方法的调用，绑定一个Closure的回调函数
    google::protobuf::Closure *done = google::protobuf::NewCallback<RpcProvider, const muduo::net::TcpConnectionPtr&, google::protobuf::Message*>(this, &RpcProvider::SendRpcResponse, conn, response);


    // 在框架上根据远端rpc请求，调用当前rpc节点上发布的方法
    // new UserService().Login(controller, request, response, done)
    service->CallMethod(method, nullptr, request, response, done);
}

// Closure的回调操作，用于序列化rpc的响应和网络发送
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *response) {
    std::string response_str;
    if (response->SerializeToString(&response_str)) {
        // 序列化成功后，通过网络把rpc方法执行的结果发送回rpc的调用方
        conn->send(response_str);
    } else {
        std::cout << "serialize response error!" << std::endl;
    }
}

// void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp) {
//   // 网络上接收的远程rpc调用请求的字符流    Login args
//   std::string recv_buf = buffer->retrieveAllAsString();

//   // 使用protobuf的CodedInputStream来解析数据流
//   google::protobuf::io::ArrayInputStream array_input(recv_buf.data(), recv_buf.size());
//   google::protobuf::io::CodedInputStream coded_input(&array_input);
//   uint32_t header_size{};

//   coded_input.ReadVarint32(&header_size);  // 解析header_size

//   // 根据header_size读取数据头的原始字符流，反序列化数据，得到rpc请求的详细信息
//   std::string rpc_header_str;
//   mprpc::RpcHeader rpcHeader;
//   std::string service_name;
//   std::string method_name;

//   // 设置读取限制，不必担心数据读多
//   google::protobuf::io::CodedInputStream::Limit msg_limit = coded_input.PushLimit(header_size);
//   coded_input.ReadString(&rpc_header_str, header_size);
//   // 恢复之前的限制，以便安全地继续读取其他数据
//   coded_input.PopLimit(msg_limit);
//   uint32_t args_size{};
//   if (rpcHeader.ParseFromString(rpc_header_str)) {
//     // 数据头反序列化成功
//     service_name = rpcHeader.service_name();
//     method_name = rpcHeader.method_name();
//     args_size = rpcHeader.args_size();
//   } else {
//     // 数据头反序列化失败
//     std::cout << "rpc_header_str:" << rpc_header_str << " parse error!" << std::endl;
//     return;
//   }

//   // 获取rpc方法参数的字符流数据
//   std::string args_str;
//   // 直接读取args_size长度的字符串数据
//   bool read_args_success = coded_input.ReadString(&args_str, args_size);

//   if (!read_args_success) {
//     // 处理错误：参数数据读取失败
//     return;
//   }

//   // 打印调试信息
//   //    std::cout << "============================================" << std::endl;
//   //    std::cout << "header_size: " << header_size << std::endl;
//   //    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
//   //    std::cout << "service_name: " << service_name << std::endl;
//   //    std::cout << "method_name: " << method_name << std::endl;
//   //    std::cout << "args_str: " << args_str << std::endl;
//   //    std::cout << "============================================" << std::endl;

//   // 获取service对象和method对象
//   auto it = m_serviceMap.find(service_name);
//   if (it == m_serviceMap.end()) {
//     std::cout << "服务：" << service_name << " is not exist!" << std::endl;
//     std::cout << "当前已经有的服务列表为:";
//     for (auto item : m_serviceMap) {
//       std::cout << item.first << " ";
//     }
//     std::cout << std::endl;
//     return;
//   }

//   auto mit = it->second.m_methodMap.find(method_name);
//   if (mit == it->second.m_methodMap.end()) {
//     std::cout << service_name << ":" << method_name << " is not exist!" << std::endl;
//     return;
//   }

//   google::protobuf::Service *service = it->second.m_service;       // 获取service对象  new UserService
//   const google::protobuf::MethodDescriptor *method = mit->second;  // 获取method对象  Login

//   // 生成rpc方法调用的请求request和响应response参数,由于是rpc的请求，因此请求需要通过request来序列化
//   google::protobuf::Message *request = service->GetRequestPrototype(method).New();
//   if (!request->ParseFromString(args_str)) {
//     std::cout << "request parse error, content:" << args_str << std::endl;
//     return;
//   }
//   google::protobuf::Message *response = service->GetResponsePrototype(method).New();

//   // 给下面的method方法的调用，绑定一个Closure的回调函数
//   // closure是执行完本地方法之后会发生的回调，因此需要完成序列化和反向发送请求的操作
//   google::protobuf::Closure *done =
//       google::protobuf::NewCallback<RpcProvider, const muduo::net::TcpConnectionPtr &, google::protobuf::Message *>(
//           this, &RpcProvider::SendRpcResponse, conn, response);

//   // 在框架上根据远端rpc请求，调用当前rpc节点上发布的方法
//   // new UserService().Login(controller, request, response, done)

//   /*
//   为什么下面这个service->CallMethod 要这么写？或者说为什么这么写就可以直接调用远程业务方法了
//   这个service在运行的时候会是注册的service
//   // 用户注册的service类 继承 .protoc生成的serviceRpc类 继承 google::protobuf::Service
//   // 用户注册的service类里面没有重写CallMethod方法，是 .protoc生成的serviceRpc类 里面重写了google::protobuf::Service中
//   的纯虚函数CallMethod，而 .protoc生成的serviceRpc类 会根据传入参数自动调取 生成的xx方法（如Login方法），
//   由于xx方法被 用户注册的service类 重写了，因此这个方法运行的时候会调用 用户注册的service类 的xx方法
//   真的是妙呀
//   */
//   //真正调用方法
//   service->CallMethod(method, nullptr, request, response, done);
// }

