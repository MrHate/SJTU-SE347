#include <iostream>
#include <map>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

using kvStore::HelloRequest;
using kvStore::HelloReply;
using kvStore::KvNodeService;

class KvStoreMasterNode {
 public:
  KvStoreMasterNode(std::shared_ptr<Channel> channel)
      : stub_(KvNodeService::NewStub(channel)) {}

  std::string SayHello(const std::string& user) {
    HelloRequest request;
    request.set_name(user);

    HelloReply reply;
    ClientContext context;

    Status status = stub_->SayHello(&context, request, &reply);
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<KvNodeService::Stub> stub_;
};

class KvMasterServiceImpl final : public KvNodeService::Service {
    Status SayHello(ServerContext* context, const HelloRequest* request, 
                    HelloReply* reply) override {
      std::string suffix("master");
      reply->set_message(request->name() + suffix);

      KvStoreMasterNode masternode(grpc::CreateChannel(
          "localhost:50052", grpc::InsecureChannelCredentials()));
      std::string user("Hello ");
      std::string datanode_reply = masternode.SayHello(user);
      std::cout << "Greeter received: " << datanode_reply << std::endl;

      return Status::OK;
    }

    Status RequestPut(ServerContext *context,
                      const kvStore::KeyValuePair *keyValue,
                      kvStore::RequestResult *result) override {
      dict[keyValue->key()] = keyValue->value();
      result->set_err(0);
      result->set_value(keyValue->key() + ":" + keyValue->value());

      return Status::OK;
    }

    Status RequestRead(ServerContext *context,
                       const kvStore::KeyString *keyString,
                       kvStore::RequestResult *result) override {
      if (dict.count(keyString->key())) {
        result->set_err(0);
        result->set_value(dict[keyString->key()]);
      } else {
        result->set_err(1);
      }

      return Status::OK;
    }

    Status RequestDelete(ServerContext *context,
                         const kvStore::KeyString *keyString,
                         kvStore::RequestResult *result) override {
      if (dict.count(keyString->key())) {
        dict.erase(dict.find(keyString->key()));
        result->set_err(0);
      } else {
        result->set_err(1);
      }

      return Status::OK;
    }

  private:
    std::map<std::string, std::string> dict;
};

static void RunServer() {
    std::string server_address("0.0.0.0:50051");
    KvMasterServiceImpl service;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    RunServer();

    return 0;
}