#include <iostream>
#include <memory>
#include <string>

#include "defines.h"

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"

class KvStoreClient {
 public:
   KvStoreClient(std::shared_ptr<grpc::Channel> channel)
       : stub_(kvStore::KvNodeService::NewStub(channel)),
         old_stub_(nullptr) {}

   int SayHello(const std::string &user) {
     kvStore::HelloRequest request;
     request.set_name(user);

     kvStore::HelloReply reply;
     grpc::ClientContext context;

     grpc::Status status = stub_->SayHello(&context, request, &reply);

     if (status.ok()) {
       std::cout << "Greeter received: " << reply.message();
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
      return;
    }

    if(result.err() == kvdefs::OK) {
      std::cout << "Put request success." << std::endl;
    }
    else if(result.err() == kvdefs::REDIRECT)  {
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
      return;
    }

    if (result.err() == kvdefs::OK) {
      std::cout << result.value() << std::endl;
    } else if (result.err() == kvdefs::NOTFOUND) {
      std::cout << "not found" << std::endl;
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
      return;
    }

    if (result.err() == kvdefs::OK) {
      std::cout << "Delete request success." << std::endl;
    } else if (result.err() == kvdefs::NOTFOUND) {
      std::cout << "not found" << std::endl;
    } else if (result.err() == kvdefs::REDIRECT) {
      RedirectToDatanode(result.value());
      RequestDelete(key);
      RedirectToMasternode();
    }
  }

private:
  std::unique_ptr<kvStore::KvNodeService::Stub> stub_;
  std::unique_ptr<kvStore::KvNodeService::Stub> old_stub_;

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
};

int main(int argc, char** argv) {

  // check "--target" argument
  std::string target_str;
  std::string arg_str("--target");
  if (argc > 1) {
    std::string arg_val = argv[1];
    size_t start_pos = arg_val.find(arg_str);
    if (start_pos != std::string::npos) {
      start_pos += arg_str.size();
      if (arg_val[start_pos] == '=') {
        target_str = arg_val.substr(start_pos + 1);
      } else {
        std::cout << "The only correct argument syntax is --target=" << std::endl;
        return 0;
      }
    } else {
      std::cout << "The only acceptable argument is --target=" << std::endl;
      return 0;
    }
  } else {
    target_str = "localhost:50051";
  }

  // establish connection then do hello check
  KvStoreClient client(grpc::CreateChannel(
      target_str, grpc::InsecureChannelCredentials()));
  std::string user("Hello ");
  if (client.SayHello(user))
    return 1;

  // hello check passed, loop client operation
  std::cout << "Connect established with " << target_str << std::endl
            << "=====================================" << std::endl
            << "(p)ut <key> <value>" << std::endl
            << "(d)elete <key>" << std::endl
            << "(r)ead <key>" << std::endl
            << "(q)uit" << std::endl
            << "=====================================" << std::endl;
  char op;
  std::string key, value;
  while (!std::cin.eof()) {
    std::cin >> op;

    switch (op) {
    case 'p':
      std::cin >> key >> value;
      client.RequestPut(key, value);
      std::cin.clear();
      std::cin.ignore(INT_MAX, '\n');
      break;

    case 'd':
      std::cin >> key;
      client.RequestDelete(key);
      std::cin.clear();
      std::cin.ignore(INT_MAX, '\n');
      break;

    case 'r':
      std::cin >> key;
      client.RequestRead(key);
      std::cin.clear();
      std::cin.ignore(INT_MAX, '\n');
      break;

    case 'q':
      return 0;

    default:
      std::cout << "invalid operation !" << std::endl;
      std::cin.clear();
      std::cin.ignore(INT_MAX, '\n');
    }
  }

  return 0;
}