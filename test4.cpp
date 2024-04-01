#include <iostream>
#include <cstdint>
#include <cassert>

#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

using node_id_t = int;        // Machine id type
using frame_id_t = int32_t;  // frame id type
using mr_id_t = int;          // Memory region id type

const mr_id_t SERVER_TEST_ID = 104; // !提示：这里还没注册呢
const mr_id_t CLIENT_TEST_MR_ID = 100;

struct TestItem {
  int page_id;
  bool page_valid = false; // if the page is valid, if not, the page is being read from disk and flush to the memstore
  bool valid; // if the slot is empty, valid: exits value in the slot

  TestItem():valid(0) {}
  TestItem(int page_id) :page_id(page_id), page_valid(false), valid(1) {}
} Aligned8;

const uint64_t PER_THREAD_ALLOC_SIZE = (size_t)500 * 1024 * 1024;

const uint64_t TestItemSize = (4096 - 8) / sizeof(TestItem);

struct TestNode{
  uint64_t lock_;
  // 空闲页面的环形缓冲区
  TestItem free_list_buffer_[TestItemSize];
};

#define thread_num 16

//这里测试一下rdma功能
int main() {
  TestNode *test_node = new TestNode();
  
  int server_node_id = 0;
  int local_port = 12345;
  auto rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);

  RDMA_ASSERT(
    rdma_ctrl->register_memory(SERVER_TEST_ID, 
      (char*)test_node, 4096, rdma_ctrl->get_device()) == true);
  memset(test_node, 0, 4096);

  while (true) {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q') {
      return false;
    } else if (ch == 'c') {
      return true;
    } else {
      printf("Type c to run another round, type q if you want to exit :)\n");
    }
    usleep(2000);
  }
  return 0;
}