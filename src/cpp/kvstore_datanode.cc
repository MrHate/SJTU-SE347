#include <iostream>
#include <memory>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <unistd.h>

#include "defines.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include "kvstore.grpc.pb.h"

// globals
zhandle_t* zkhandle = nullptr;
int my_data_id = -1;
std::string my_znode_path = "";
std::string my_server_addr = "";

std::vector<kvStore::SyncContent> log_ents;
std::map<std::string, std::string> dict;
std::vector<std::string> backups;

// forward declarations
int AppendLog(const kvStore::RequestContent *req);
int AppendLog(const kvStore::SyncContent *sync);
grpc::Status ApplyLog(kvStore::RequestResult *result);

// classes
class SyncRequester {
public:
  SyncRequester(std::shared_ptr<grpc::Channel> channel)
      : stub_(kvStore::KvNodeService::NewStub(channel)) {}

  int DoSync(const kvStore::SyncContent &request) {
    kvStore::SyncResult reply;
    grpc::ClientContext context;

    grpc::Status status = stub_->Sync(&context, request, &reply);

    if (status.ok()) {
      return reply.err();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl
                << "RPC failed" << std::endl;
      return kvdefs::SYNC_FAIL;
    }
  }

private:
  std::unique_ptr<kvStore::KvNodeService::Stub> stub_;
};

class KvDataServiceImpl final : public kvStore::KvNodeService::Service {
  grpc::Status SayHello(grpc::ServerContext *context,
                        const kvStore::HelloRequest *request,
                        kvStore::HelloReply *reply) override {
    std::string suffix("datanode");
    reply->set_message(request->name() + suffix);
    return grpc::Status::OK;
  }

  grpc::Status Request(grpc::ServerContext *context,
                       const kvStore::RequestContent *req,
                       kvStore::RequestResult *result) override {
    std::cout << "received request: " << req->op() << std::endl;

    grpc::Status ret = grpc::Status::OK;

    // Immediately return result if it is read request (or maybe flush log
    // before); Update request (put and del) should be entered into log and then
    // do 2pc consensus.
    if (req->op() == kvdefs::READ) {
      if (dict.count(req->key())) {
        result->set_err(kvdefs::OK);
        result->set_value(dict[req->key()]);
      } else {
        result->set_err(kvdefs::NOTFOUND);
      }
    } else if (req->op() == kvdefs::LOGVERSION) {
      if (log_ents.empty())
        result->set_value("0");
      else
        result->set_value(std::to_string(log_ents.back().index()));
      result->set_err(kvdefs::OK);
    } else {
      AppendLog(req);
      // 2pc:
      // send sync reqeust to backups,
      // apply log on receiving success responses of the majority
      int sum = 0, retrys = 0;
      kvStore::SyncContent &ent = log_ents.back();
      while (retrys < 3 && backups.size() && sum <= backups.size() / 2) {
        sum = 0;
        ent.set_index(ent.index() + 1);
        for (auto &addr : backups) {
          std::cout << "syncing " << addr << std::endl;
          SyncRequester client(
              grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
          if (client.DoSync(ent) == kvdefs::SYNC_SUCC)
            ++sum;
        }
        ++ retrys;
      }
      ret = ApplyLog(result);

      // append empty ent to be the primary node
      kvStore::SyncContent empty_ent;
      empty_ent.set_index(log_ents.back().index() + 1);
      log_ents.push_back(empty_ent);
    }

    return ret;
  }

  grpc::Status Sync(grpc::ServerContext *context,
                    const kvStore::SyncContent *ent,
                    kvStore::SyncResult *result) override {
    std::cout << "received sync request" << std::endl;
    int sync_ret = AppendLog(ent);
    result->set_err(sync_ret);
    if (sync_ret == kvdefs::SYNC_SUCC) {
      kvStore::RequestResult result;
      return ApplyLog(&result);
    }
    return grpc::Status::OK;
  }
};

int AppendLog(const kvStore::RequestContent *req) {
  kvStore::SyncContent ent;
  if (log_ents.empty()) {
    ent.set_index(0);
  } else {
    ent.set_index(log_ents.back().index() + 1);
  }
  kvStore::RequestContent *req_clone = new kvStore::RequestContent(*req);
  ent.set_allocated_req(req_clone);
  log_ents.push_back(ent);

  return kvdefs::SYNC_SUCC;
}

int AppendLog(const kvStore::SyncContent *sync) {
  if (log_ents.empty() || log_ents.back().index() < sync->index()) {
    log_ents.push_back(*sync);
    return kvdefs::SYNC_SUCC;
  }

  return kvdefs::SYNC_FAIL;
}

grpc::Status ApplyLog(kvStore::RequestResult *result) {
  assert(log_ents.size());

  const kvStore::RequestContent *req = &(log_ents.back().req());
  switch (req->op()) {
  case kvdefs::PUT:
    dict[req->key()] = req->value();
    result->set_err(kvdefs::OK);
    result->set_value(req->key() + ":" + req->value());
    break;

  case kvdefs::DELETE:
    if (dict.count(req->key())) {
      dict.erase(dict.find(req->key()));
      result->set_err(kvdefs::OK);
    } else {
      result->set_err(kvdefs::NOTFOUND);
    }
    break;

  case kvdefs::READ:
  default:
    std::cout << "failed here " << __LINE__ << std::endl;
    return grpc::Status::CANCELLED;
  }

  return grpc::Status::OK;

}

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
void zktest_string_completion(int rc, const String_vector *strings,
                              const void *data) {}

void zkwatcher_callback(zhandle_t* zh, int type, int state,
        const char* path, void* watcherCtx) {

  std::cout << "something happeded" << std::endl;
  if (type == ZOO_CHILD_EVENT) {
    std::cout << "child event: " << path << std::endl;
    String_vector children;
    std::string my_znode("data");
    my_znode += std::to_string(my_data_id);

    if (zoo_get_children(zh, "/master", 0, &children) == ZOK) {
      std::vector<std::string> new_backups;
      for (int i = 0; i < children.count; ++i) {
        std::string child_path("/master/"), child_name(children.data[i]),
            child_node(kvdefs::extract_data_node(children.data[i]));
        child_path += child_name;

        char buf[50] = {0};
        int buf_len;

        zoo_get(zh, child_path.c_str(), 0, buf, &buf_len, NULL);
        if (child_node == my_znode && std::string(buf) != my_server_addr) {
          new_backups.emplace_back(buf);
          std::cout << "added backup " << child_name << " with addr: " << buf
                    << std::endl;
        }
      }
      backups.swap(new_backups);
    }
  }

  // re-register watcher function
  zoo_awget_children(zh, "/master", zkwatcher_callback, nullptr,
                     zktest_string_completion, nullptr);
}

// handle ctrl-c
void sig_handler(int sig) {
  if(sig == SIGINT) {
    cleanup();
    exit(EXIT_SUCCESS);
  }
}

int main(int argc, char** argv) {
  // parse args
  {
    int o = -1;
    const char *optstring = "t:i:";
    while ((o = getopt(argc, argv, optstring)) != -1) {
      switch (o) {
        case 't':
          my_server_addr = optarg;
          break;
        case 'i':
          my_data_id = atoi(optarg);
          break;
      }
    }
    if (my_data_id < 0 || my_server_addr.empty()) {
      std::cerr << "Must set -t <addr> -i <id>" << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  // generate znode path
  {
    std::stringstream strm;
    strm << "/master/data" << my_data_id;
    my_znode_path = strm.str();
  }
  
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