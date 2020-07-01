#include <iostream>
#include <memory>
#include <string>
#include <map>
#include <unistd.h>

#include "defines.h"

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"

// for random string generating
const char g_alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

class KvTesterClient {
 public:
   KvTesterClient(std::shared_ptr<grpc::Channel> channel)
       : stub_(kvStore::KvNodeService::NewStub(channel)),
         old_stub_(nullptr) {}

   int SayHello(const std::string &user) {
     kvStore::HelloRequest request;
     request.set_name(user);

     kvStore::HelloReply reply;
     grpc::ClientContext context;

     grpc::Status status = stub_->SayHello(&context, request, &reply);

     if (status.ok()) {
       std::cout << "Greeter received: " << reply.message() << std::endl;
       return 0;
     } else {
       std::cout << status.error_code() << ": " << status.error_message()
                 << std::endl
                 << "RPC failed" << std::endl;
       return 1;
     }
  }

  void RequestPut(const std::string &key, const std::string &value) {
    kvStore::RequestContent keyValue;
    keyValue.set_key(key);
    keyValue.set_value(value);
    keyValue.set_op(kvdefs::PUT);
    kvStore::RequestResult result;
    grpc::ClientContext context;

    grpc::Status status = stub_->Request(&context, keyValue, &result);
    if(!status.ok() || result.err() == kvdefs::FAILED) {
      std::cout << "Put request failed." << std::endl;
      exit(1);
    }

    if(result.err() == kvdefs::REDIRECT)  {
      RedirectToDatanode(result.value());
      RequestPut(key, value);
      RedirectToMasternode();
    }
  }

  void RequestRead(const std::string &key) {
    kvStore::RequestContent keyString;
    keyString.set_key(key);
    keyString.set_value("");
    keyString.set_op(kvdefs::READ);
    kvStore::RequestResult result;
    grpc::ClientContext context;

    grpc::Status status = stub_->Request(&context, keyString, &result);
    if(!status.ok() || result.err() == kvdefs::FAILED) {
      std::cout << "Read request failed." << std::endl;
      exit(1);
    }

    if (result.err() == kvdefs::OK) {
      if(dict.count(key) && dict[key] != result.value()) {
        std::cout << "read wrong value" << std::endl;
        exit(1);
      }
      else if(dict.count(key) == 0) {
        std::cout << "element should be removed" << std::endl;
        exit(1);
      }
    } else if (result.err() == kvdefs::NOTFOUND) {
      if(dict.count(key)) {
        std::cout << "element not found" << std::endl;
        exit(1);
      }
    } else if (result.err() == kvdefs::REDIRECT) {
      RedirectToDatanode(result.value());
      RequestRead(key);
      RedirectToMasternode();
    }
  }

  void RequestDelete(const std::string &key) {
    kvStore::RequestContent keyString;
    keyString.set_key(key);
    keyString.set_value("");
    keyString.set_op(kvdefs::DELETE);
    kvStore::RequestResult result;
    grpc::ClientContext context;

    grpc::Status status = stub_->Request(&context, keyString, &result);
    if(!status.ok() || result.err() == kvdefs::FAILED) {
      std::cout << "Delete request failed." << std::endl;
      exit(1);
    }

    if (result.err() == kvdefs::NOTFOUND) {
      std::cout << "not found" << std::endl;
      exit(1);
    } else if (result.err() == kvdefs::REDIRECT) {
      RedirectToDatanode(result.value());
      RequestDelete(key);
      RedirectToMasternode();
    }
  }

  void DoRandomRequest() {
    int req_type = rand() % 3;
    switch(req_type) {
      case 0: // delete then read
        if(dict.size()) { // if dict is empty, do write then read test
          auto it = dict.begin();
          std::advance(it, rand() % dict.size());
          const std::string key = it->first;
          dict.erase(it);
          std::cout << "delete then read key: " << key << " ...";
          RequestDelete(key);
          RequestRead(key);
          std::cout << "OK" << std::endl;
          break;
        }
      
      case 1: // update then read
        if(dict.size()) {
          auto it = dict.begin();
          std::advance(it, rand() % dict.size());
          const std::string value = GenRandomString();
          const std::string key = it->first;
          dict[key] = value;
          std::cout << "update then read key: " << key << " ...";
          RequestPut(key, value);
          RequestRead(key);
          std::cout << "OK" << std::endl;
          break;
        }
        
      case 2: // write then read
        {
          const std::string key = GenRandomString(),
                            value = GenRandomString();
          dict[key] = value;
          std::cout << "write then read key: " << key << " ...";
          RequestPut(key, value);
          RequestRead(key);
          std::cout << "OK" << std::endl;
        }
        break;

      default:
        assert(0);
    }
  }

private:
  std::unique_ptr<kvStore::KvNodeService::Stub> stub_;
  std::unique_ptr<kvStore::KvNodeService::Stub> old_stub_;

  std::map<std::string, std::string> dict;

  void RedirectToDatanode(const std::string& addr) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        addr, grpc::InsecureChannelCredentials());
    std::unique_ptr<kvStore::KvNodeService::Stub> new_stub(kvStore::KvNodeService::NewStub(channel));
    stub_.swap(new_stub);
    old_stub_.swap(new_stub);
  }

  void RedirectToMasternode() {
    stub_.swap(old_stub_);
    old_stub_.reset(nullptr);
  }

  std::string GenRandomString() {

    std::string res = "";
    for (int i = 0; i < 10; ++i) {
        res.push_back(g_alphanum[rand() % (sizeof(g_alphanum) - 1)]);
    }

    return res;
  }
};

int main(int argc, char** argv) {
  std::string target_str;
  std::size_t req_count = 100;
  // parse args
  {
    int o = -1;
    const char *optstring = "t:n:";
    while ((o = getopt(argc, argv, optstring)) != -1) {
      switch (o) {
        case 't':
          target_str = optarg;
          break;
        case 'n':
          req_count = atoi(optarg);
          break;
      }
    }
    if (target_str.empty()) {
      std::cerr << "Must set -t <addr>" << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  // establish connection then do hello check
  KvTesterClient client(grpc::CreateChannel(
      target_str, grpc::InsecureChannelCredentials()));
  std::string user("Hello ");
  if (client.SayHello(user)) {
    std::cout << "failed saying hello" << std::endl;
    return 1;
  }

  for(int i=0 ;i < req_count; ++i) {
    std::cout << i+1 << "/" << req_count << " ";
    client.DoRandomRequest();
  }

  std::cout << "Client tests passed." << std::endl;

  return 0;
}