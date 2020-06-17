#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <functional>

#include "defines.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

namespace {
  zhandle_t* zkhandle;
  std::vector<std::string> datanodes_addr;
}

class KvStoreMasterNode {
 public:
  KvStoreMasterNode(std::shared_ptr<grpc::Channel> channel)
      : stub_(kvStore::KvNodeService::NewStub(channel)) {}

  std::string SayHello(const std::string& user) {
    kvStore::HelloRequest request;
    request.set_name(user);

    kvStore::HelloReply reply;
    grpc::ClientContext context;

    grpc::Status status = stub_->SayHello(&context, request, &reply);
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<kvStore::KvNodeService::Stub> stub_;
};

class KvMasterServiceImpl final : public kvStore::KvNodeService::Service {
    grpc::Status SayHello(grpc::ServerContext* context, const kvStore::HelloRequest* request, 
                    kvStore::HelloReply* reply) override {
      std::string suffix("master");
      reply->set_message(request->name() + suffix);

      KvStoreMasterNode masternode(grpc::CreateChannel(
          "localhost:50052", grpc::InsecureChannelCredentials()));
      std::string user("Hello ");
      std::string datanode_reply = masternode.SayHello(user);
      std::cout << "Greeter received: " << datanode_reply << std::endl;

      return grpc::Status::OK;
    }

    grpc::Status RequestPut(grpc::ServerContext *context,
                      const kvStore::KeyValuePair *keyValue,
                      kvStore::RequestResult *result) override {
      std::cout << "Received put request" << std::endl;
      return RedirectToDatanode(keyValue->key(), result);
    }

    grpc::Status RequestRead(grpc::ServerContext *context,
                       const kvStore::KeyString *keyString,
                       kvStore::RequestResult *result) override {
      std::cout << "Received read request" << std::endl;
      return RedirectToDatanode(keyString->key(), result);
    }

    grpc::Status RequestDelete(grpc::ServerContext *context,
                         const kvStore::KeyString *keyString,
                         kvStore::RequestResult *result) override {
      std::cout << "Received delete request" << std::endl;
      return RedirectToDatanode(keyString->key(), result);
    }

  private:
    std::map<std::string, std::string> dict;

    grpc::Status RedirectToDatanode(const std::string& key, kvStore::RequestResult *result) {
      if(datanodes_addr.empty()) return grpc::Status::CANCELLED;
      result->set_err(kvdefs::REDIRECT);
      result->set_value(datanodes_addr[Key2Index(key)]);
      return grpc::Status::OK;
    }

    std::size_t Key2Index(const std::string& key) {
      std::hash<std::string> hasher;
      return hasher(key) % datanodes_addr.size();
    }
};

static void RunServer(const std::string& server_addr) {
    KvMasterServiceImpl service;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_addr << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

void cleanup() {
  kvdefs::del_znode_recursive(zkhandle, "/master");
  zookeeper_close(zkhandle);
}

// zk callbacks
void zktest_string_completion(int rc, const String_vector* strings, const void *data) {

}

void zkwatcher_callback(zhandle_t* zh, int type, int state,
        const char* path, void* watcherCtx) {
  std::cout << "something happeded" << std::endl;
  if(type == ZOO_CHILD_EVENT) {
    std::cout << "child event: " << path << std::endl;
    String_vector children;

    if (zoo_get_children(zh, "/master", 0, &children) == ZOK) {
      std::vector<std::string> new_datanodes_addr;
      for (int i = 0; i < children.count; ++i) {
        std::string child_path = "/master/";
        child_path += children.data[i];

        char buf[100];
        int buf_len;

        zoo_get(zh, child_path.c_str(), 0, buf, &buf_len, NULL);
        new_datanodes_addr.emplace_back(buf);
      }
      datanodes_addr.swap(new_datanodes_addr);
    }
  }

  zoo_awget_children(zh, "/master", zkwatcher_callback, nullptr, zktest_string_completion, nullptr);
}

// handle ctrl-c
void sig_handler(int sig) {
  if(sig == SIGINT) {
    cleanup();
    exit(EXIT_SUCCESS);
  }
}

// main
int main(int argc, char** argv) {
  std::string server_addr = "0.0.0.0:50051";

  zkhandle = zookeeper_init("0.0.0.0:2181",
            zkwatcher_callback, 10000, 0, nullptr, 0);
  if(!zkhandle) {
    std::cerr << "Failed connecting to zk server." << std::endl;
    exit(EXIT_FAILURE);
  }

  int ret = zoo_create(zkhandle, "/master", server_addr.c_str(), server_addr.length(), &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
  if(ret) {
    std::cerr << "Failed creating znode: " << ret << std::endl;
    cleanup();
    exit(EXIT_FAILURE);
  }

  signal(SIGINT, sig_handler);

  RunServer(server_addr);

  return 0;
}