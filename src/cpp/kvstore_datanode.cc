#include <iostream>
#include <memory>
#include <string>
#include <map>
#include <sstream>

#include "defines.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

namespace {
  zhandle_t* zkhandle;
  int my_data_id;
  std::string my_znode_path;
  std::string my_server_addr;
}

class KvDataServiceImpl final : public kvStore::KvNodeService::Service {
  grpc::Status SayHello(grpc::ServerContext *context,
                        const kvStore::HelloRequest *request,
                        kvStore::HelloReply *reply) override {
    std::string suffix("datanode");
    reply->set_message(request->name() + suffix);
    return grpc::Status::OK;
  }

  grpc::Status Request(grpc::ServerContext *context,
                          const kvStore::KeyValuePair *keyValue,
                          kvStore::RequestResult *result) override {
    std::cout << "received request: " << keyValue->op() << std::endl;
    switch(keyValue->op()) {
      case kvdefs::PUT:
        dict[keyValue->key()] = keyValue->value();
        result->set_err(kvdefs::OK);
        result->set_value(keyValue->key() + ":" + keyValue->value());
        break;
      
      case kvdefs::READ:
        if (dict.count(keyValue->key())) {
          result->set_err(kvdefs::OK);
          result->set_value(dict[keyValue->key()]);
        } else {
          result->set_err(kvdefs::NOTFOUND);
        }
        break;

      case kvdefs::DELETE:
        if (dict.count(keyValue->key())) {
          dict.erase(dict.find(keyValue->key()));
          result->set_err(kvdefs::OK);
        } else {
          result->set_err(kvdefs::NOTFOUND);
        }
        break;

      default:
        return grpc::Status::CANCELLED;
    }

    return grpc::Status::OK;
  }

private:
  std::map<std::string, std::string> dict;
};

void RunServer(const std::string& server_addr) {
  KvDataServiceImpl service;

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
  zookeeper_close(zkhandle);
}

// zk callbacks
void zkwatcher_callback(zhandle_t* zh, int type, int state,
        const char* path, void* watcherCtx) {

}

// handle ctrl-c
void sig_handler(int sig) {
  if(sig == SIGINT) {
    cleanup();
    exit(EXIT_SUCCESS);
  }
}

int main(int argc, char** argv) {
  // check "--target" argument
  std::string arg_str("--target");
  if (argc > 1) {
    std::string arg_val = argv[1];
    size_t start_pos = arg_val.find(arg_str);
    if (start_pos != std::string::npos) {
      start_pos += arg_str.size();
      if (arg_val[start_pos] == '=') {
        my_server_addr = arg_val.substr(start_pos + 1);
      } else {
        std::cout << "The only correct argument syntax is --target=" << std::endl;
        return 0;
      }
    } else {
      std::cout << "The only acceptable argument is --target=" << std::endl;
      return 0;
    }
  } else {
    exit(EXIT_FAILURE);
  }

  my_data_id = 1;

  std::stringstream strm;
  strm << "/master/data" << my_data_id;
  my_znode_path = strm.str();
  
  zkhandle = zookeeper_init("0.0.0.0:2181",
            zkwatcher_callback, 10000, 0, nullptr, 0);
  if(!zkhandle) {
    std::cerr << "Failed connecting to zk server." << std::endl;
    exit(EXIT_FAILURE);
  }

  int ret = zoo_create(zkhandle, my_znode_path.c_str(), my_server_addr.c_str(), my_server_addr.length(), 
                       &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, nullptr, 0);
  if(ret == ZNODEEXISTS) {
    my_znode_path += "_backup";
    ret = zoo_create(zkhandle, my_znode_path.c_str(), my_server_addr.c_str(), my_server_addr.length(),
                     &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL|ZOO_SEQUENCE, nullptr, 0);
  }
  else if(ret) {
    std::cerr << "Failed creating znode: " << ret << std::endl;
    cleanup();
    exit(EXIT_FAILURE);
  }

  signal(SIGINT, sig_handler);

  RunServer(my_server_addr);

  return 0;
}