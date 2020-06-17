#include <string>
#include "defines.h"

int kvdefs::del_znode_recursive(zhandle_t* zh, const char* path) {
  String_vector children;
  if (zoo_get_children(zh, path, 0, &children) == ZOK) {
    for (int i = 0; i < children.count; ++i) {
      std::string child_path(path);
      child_path += "/";
      child_path += children.data[i];

      int ret = kvdefs::del_znode_recursive(zh, child_path.c_str());
      if(ret != ZOK) return ret;
    }
  }
  return zoo_delete(zh, path, -1);
}

std::string kvdefs::extract_data_node(const char* buf) {
  std::string sbuf(buf);
  std::size_t pos = sbuf.find("_backup");
  if(pos == std::string::npos) return sbuf;
  return sbuf.substr(0, pos);
}