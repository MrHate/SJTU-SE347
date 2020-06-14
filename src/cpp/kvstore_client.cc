#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"

class KvStoreClient {
 public:
   KvStoreClient(std::shared_ptr<grpc::Channel> channel)
       : stub_(kvStore::KvMasternodeService::NewStub(channel)) {}

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
    kvStore::KeyValuePair keyValue;
    keyValue.set_key(key);
    keyValue.set_value(value);
    kvStore::RequestResult result;
    grpc::ClientContext context;

    grpc::Status status = stub_->RequestPut(&context, keyValue, &result);
    if(status.ok() && result.err() == 0) {
      std::cout << "Put request success." << std::endl;
    }
    else {
      std::cout << "Put request failed." << std::endl;
    }
  }

  void RequestRead(const std::string &key) {
    kvStore::KeyString keyString;
    keyString.set_key(key);
    kvStore::RequestResult result;
    grpc::ClientContext context;

    grpc::Status status = stub_->RequestRead(&context, keyString, &result);
    if (status.ok() && result.err() == 0) {
      std::cout << result.value() << std::endl;
    }
    else {
      std::cout << "Read request failed." << std::endl;
    }
  }

  void RequestDelete(const std::string &key) {
    kvStore::KeyString keyString;
    keyString.set_key(key);
    kvStore::RequestResult result;
    grpc::ClientContext context;

    grpc::Status status = stub_->RequestDelete(&context, keyString, &result);
    if (status.ok() && result.err() == 0) {
      std::cout << result.value() << std::endl;
    }
    else {
      std::cout << "Delete request failed." << std::endl;
    }
  }

private:
  std::unique_ptr<kvStore::KvMasternodeService::Stub> stub_;
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
            << "(r)ead key" << std::endl
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
      break;

    case 'd':
      std::cin >> key;
      client.RequestDelete(key);
      break;

    case 'r':
      std::cin >> key;
      client.RequestRead(key);
      break;

    case 'q':
      return 0;

    default:
      std::cout << "invalid operation !" << std::endl;
      std::cin.clear();
    }
  }

  return 0;
}