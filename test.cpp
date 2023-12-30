#include <iostream>
#include <cstdint>
#include <cassert>

#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

#define MAX_FREE_LIST_BUFFER_SIZE 20480 // 20480*4k = 80M
using node_id_t = int;        // Machine id type
using frame_id_t = int32_t;  // frame id type
using mr_id_t = int;          // Memory region id type

const mr_id_t SERVER_PAGETABLE_RING_FREE_FRAME_BUFFER_ID = 104; // !提示：这里还没注册呢
const mr_id_t CLIENT_MR_ID = 100;

const uint64_t PER_THREAD_ALLOC_SIZE = (size_t)500 * 1024 * 1024;

struct RingBufferItem{
  node_id_t node_id;
  frame_id_t frame_id;
  bool valid;
} Aligned8;

struct RingFreeFrameBuffer{
  // 空闲页面的环形缓冲区
  RingBufferItem free_list_buffer_[MAX_FREE_LIST_BUFFER_SIZE];
  uint64_t head_ = 0;
  uint64_t tail_ = 0;
  int64_t buffer_item_num_ = 0;
};

//这里测试一下rdma功能
int main() {
  RingFreeFrameBuffer *ring_buffer = new RingFreeFrameBuffer();

  int server_node_id = 0;
  int local_port = 12345;
  auto rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(
    rdma_ctrl->register_memory(SERVER_PAGETABLE_RING_FREE_FRAME_BUFFER_ID, 
      (char*)ring_buffer, sizeof(RingFreeFrameBuffer), rdma_ctrl->get_device()) == true);
  memset(ring_buffer, 0, sizeof(RingFreeFrameBuffer));

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