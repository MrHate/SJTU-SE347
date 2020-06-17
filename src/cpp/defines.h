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

int del_znode_recursive(zhandle_t* zh, const char* path);

}