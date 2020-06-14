#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "defines.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

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
      RedirectToDatanode(keyValue->key(), result);
      return grpc::Status::OK;
    }

    grpc::Status RequestRead(grpc::ServerContext *context,
                       const kvStore::KeyString *keyString,
                       kvStore::RequestResult *result) override {
      std::cout << "Received read request" << std::endl;
      RedirectToDatanode(keyString->key(), result);
      return grpc::Status::OK;
    }

    grpc::Status RequestDelete(grpc::ServerContext *context,
                         const kvStore::KeyString *keyString,
                         kvStore::RequestResult *result) override {
      std::cout << "Received delete request" << std::endl;
      RedirectToDatanode(keyString->key(), result);
      return grpc::Status::OK;
    }

  private:
    std::map<std::string, std::string> dict;

    void RedirectToDatanode(const std::string& key, kvStore::RequestResult *result) {
      result->set_err(kvdefs::REDIRECT);
      result->set_value("localhost:50052");
    }
};

static void RunServer() {
    std::string server_address("0.0.0.0:50051");
    KvMasterServiceImpl service;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    RunServer();

    return 0;
}