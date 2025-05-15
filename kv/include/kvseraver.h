#include "kv.pb.h"
#include <google/protobuf/service.h>
#include <unordered_map>
#include <string>
#include <mutex>

class KVServiceImpl : public kv::KVService {
private:
    std::unordered_map<std::string, std::pair<std::string, int>> kv_store;
    std::mutex mu;

public:
    void Put(::google::protobuf::RpcController* controller,
                       const ::kv::PutRequest* request,
                       ::kv::PutResponse* response,
                       ::google::protobuf::Closure* done) {
        std::lock_guard<std::mutex> lock(mu);
        auto& key = request->key();
        int req_ver = request->version();

        auto it = kv_store.find(key);
        if (it == kv_store.end()) {
            if (req_ver != 0) {
                response->set_success(false);
                response->set_error("ErrNoKey");
                done->Run();
                return;
            }
            kv_store[key] = {request->value(), 1};
        } else {
            if (it->second.second != req_ver) {
                response->set_success(false);
                response->set_error("ErrVersion");
                done->Run();
                return;
            }
            it->second = {request->value(), req_ver + 1};
        }

        response->set_success(true);
        done->Run();
        return;
    }

    void Get(::google::protobuf::RpcController* controller,
                       const ::kv::GetRequest* request,
                       ::kv::GetResponse* response,
                       ::google::protobuf::Closure* done) {
        std::lock_guard<std::mutex> lock(mu);
        auto it = kv_store.find(request->key());
        if (it == kv_store.end()) {
            response->set_error("ErrNoKey");
        } else {
            response->set_value(it->second.first);
            response->set_version(it->second.second);
        }
        done->Run();
        return;
    }
};
