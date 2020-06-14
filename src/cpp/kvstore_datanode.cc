#include <iostream>
#include <memory>
#include <string>
#include <map>

#include "defines.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

class KvNodeServiceImpl final : public kvStore::KvNodeService::Service {
    grpc::Status SayHello(grpc::ServerContext* context, const kvStore::HelloRequest* request, 
                    kvStore::HelloReply* reply) override {
        std::string suffix("datanode");
        reply->set_message(request->name() + suffix);
        return grpc::Status::OK;
    }

    grpc::Status RequestPut(grpc::ServerContext *context,
                      const kvStore::KeyValuePair *keyValue,
                      kvStore::RequestResult *result) override {
      std::cout << "Received put request" << std::endl;

      dict[keyValue->key()] = keyValue->value();
      result->set_err(kvdefs::OK);
      result->set_value(keyValue->key() + ":" + keyValue->value());

      return grpc::Status::OK;
    }

    grpc::Status RequestRead(grpc::ServerContext *context,
                       const kvStore::KeyString *keyString,
                       kvStore::RequestResult *result) override {
      std::cout << "Received read request" << std::endl;

      if (dict.count(keyString->key())) {
        result->set_err(kvdefs::OK);
        result->set_value(dict[keyString->key()]);
      } else {
        result->set_err(kvdefs::NOTFOUND);
      }

      return grpc::Status::OK;
    }

    grpc::Status RequestDelete(grpc::ServerContext *context,
                         const kvStore::KeyString *keyString,
                         kvStore::RequestResult *result) override {
      std::cout << "Received delete request" << std::endl;

      if (dict.count(keyString->key())) {
        dict.erase(dict.find(keyString->key()));
        result->set_err(kvdefs::OK);
      } else {
        result->set_err(kvdefs::NOTFOUND);
      }

      return grpc::Status::OK;
    }

  private:
    std::map<std::string, std::string> dict;
};

void RunServer() {
    std::string server_address("0.0.0.0:50052");
    KvNodeServiceImpl service;

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