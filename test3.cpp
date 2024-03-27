#include "rlib/rdma_ctrl.hpp"
#include <iostream>

using namespace rdmaio;
int main(){
  MemoryAttr local_log_buffer_mr;
  // GetMRMeta
  while (QP::get_remote_mr("127.0.0.1", 12344, 105, &local_log_buffer_mr) != SUCC) {
    usleep(2000);
  }
  std::cout << "Test passed!" << std::endl;
}
  