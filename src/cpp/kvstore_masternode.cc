#include <iostream>
#include <memory>
#include <string>
#include <map>
#include <vector>
#include <functional>
#include <unistd.h>

#include "defines.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

using strmap_t = std::map<std::string, std::string>;

zhandle_t* zkhandle = nullptr;
strmap_t datanodes_addr;
std::string server_addr = "";

class MasterRequester {
public:
  MasterRequester(std::shared_ptr<grpc::Channel> channel)
      : stub_(kvStore::KvNodeService::NewStub(channel)) {}

  int RequestLogVersion() {
    kvStore::RequestContent request;
    request.set_op(kvdefs::LOGVERSION);

    kvStore::RequestResult reply;
    grpc::ClientContext context;

    grpc::Status status = stub_->Request(&context, request, &reply);

    if (status.ok() && reply.err() == kvdefs::OK) {
      return std::stoi(reply.value());
    } else {
      std::cerr << "request version failed" << std::endl;
      return -1;
    }
  }

  void RequestPrimarySync() {
    kvStore::RequestContent request;
    request.set_op(kvdefs::PRIMARY);

    kvStore::RequestResult reply;
    grpc::ClientContext context;

    grpc::Status status = stub_->Request(&context, request, &reply);
  }

  void RequestLogClone(const std::string& target) {
    kvStore::RequestContent request;
    request.set_op(kvdefs::CLONE);
    request.set_value(target);

    kvStore::RequestResult reply;
    grpc::ClientContext context;

    grpc::Status status = stub_->Request(&context, request, &reply);
  }

private:
  std::unique_ptr<kvStore::KvNodeService::Stub> stub_;
};

class KvMasterServiceImpl final : public kvStore::KvNodeService::Service {
    grpc::Status SayHello(grpc::ServerContext* context, const kvStore::HelloRequest* request, 
                    kvStore::HelloReply* reply) override {
      std::string suffix("master");
      reply->set_message(request->name() + suffix);
      std::string user("Hello ");

      return grpc::Status::OK;
    }

    grpc::Status Request(grpc::ServerContext *context,
                         const kvStore::RequestContent *keyValue,
                         kvStore::RequestResult *result) override {
      // std::cout << "Received request" << std::endl;
      return RedirectToDatanode(keyValue->key(), result);
    }

  private:
    std::map<std::string, std::string> dict;

    grpc::Status RedirectToDatanode(const std::string& key, kvStore::RequestResult *result) {
      if(datanodes_addr.empty()) return grpc::Status::CANCELLED;
      result->set_err(kvdefs::REDIRECT);
      result->set_value(datanodes_addr[Key2Node(key)]);
      return grpc::Status::OK;
    }

    std::string Key2Node(const std::string& key) {
      std::hash<std::string> hasher;
      std::string res("data");
      res += std::to_string(hasher(key) % datanodes_addr.size() + 1);
      return res;
    }
};

void RunServer(const std::string& server_addr) {
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
      strmap_t new_datanodes_addr;  // updated datanode router map
      std::vector<std::string> new_datanode_group;  // record the new datanode added in this turn
      std::map<std::string, int> log_versions;  // check the latest log version to find primaries

      for (int i = 0; i < children.count; ++i) {
        std::string child_path("/master/"),
                    child_name(children.data[i]),
                    child_node(kvdefs::extract_data_node(children.data[i]));
        child_path += child_name;

        char buf[50] = {0};
        int buf_len;

        zoo_get(zh, child_path.c_str(), 0, buf, &buf_len, NULL);

        // check if being new added datanode group
        if(datanodes_addr.count(child_node) == 0) {
          new_datanode_group.push_back(child_node);
        }

        // added router entry into new router map
        if(new_datanodes_addr.count(child_node) == 0) {
          MasterRequester client(grpc::CreateChannel(
              buf, grpc::InsecureChannelCredentials()));
          log_versions[child_node] = client.RequestLogVersion();
          new_datanodes_addr[child_node] = buf;
          std::cout << "added " << child_name << " as " << child_node << " to " << buf << std::endl;
        } else {
          MasterRequester client(grpc::CreateChannel(
              buf, grpc::InsecureChannelCredentials()));
          int v = client.RequestLogVersion();
          assert(v >= 0);
          assert(log_versions.count(child_node));
          if(log_versions[child_node] < v) {
            new_datanodes_addr[child_node] = buf;
            log_versions[child_node] = v;
            std::cout << "set " << child_name << " as " << child_node << " to " << buf << std::endl;
          }
        }
      }

      // check new datanodes and do clone sync if any
      // use cluster-level global sequence to ensure log version consensus
      // sync all the logs from existing datanodes to the new ones
      for (const auto& e : datanodes_addr) {
        MasterRequester client(grpc::CreateChannel(
            e.second, grpc::InsecureChannelCredentials()));
        for (const std::string& new_datanode : new_datanode_group) {
          client.RequestLogClone(new_datanodes_addr[new_datanode]);
        }
      }

      // update datanode router
      datanodes_addr.swap(new_datanodes_addr);

      // sending primarys complete sync request
      for (const auto& e : datanodes_addr) {
        MasterRequester client(grpc::CreateChannel(
            e.second, grpc::InsecureChannelCredentials()));
        client.RequestPrimarySync();
      }
    }
  }

  // re-register watcher function
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
  std::string zk_local_addr = "0.0.0.0:";
  // parse args
  {
    int o = -1;
    const char *optstring = "t:z:";
    while ((o = getopt(argc, argv, optstring)) != -1) {
      switch (o) {
        case 't':
          server_addr = optarg;
          break;
        case 'z':
          zk_local_addr += optarg;
          break;
      }
    }
    if (server_addr.empty() || zk_local_addr.size() < 8) {
      std::cerr << "Must set -t <addr> -z <port>" << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  zkhandle = zookeeper_init(zk_local_addr.c_str(),
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