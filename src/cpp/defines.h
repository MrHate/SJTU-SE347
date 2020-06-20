#include <zookeeper/zookeeper.h>

namespace kvdefs {

enum REQUEST_ERR_NO {
  OK = 0,
  NOTFOUND,
  REDIRECT,
  FAILED
};

enum NOTIFICATOIN_NO {
  SETPRIMARY = 100
};

enum REQUEST_OP_NO { PUT = 200, READ, DELETE, LOGVERSION, PRIMARY };

enum SYNC_ERR_NO {
  SYNC_SUCC = 300,
  SYNC_FAIL
};

int del_znode_recursive(zhandle_t* zh, const char* path);
std::string extract_data_node(const char* buf);

}