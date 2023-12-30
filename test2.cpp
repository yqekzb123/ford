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
  int local_port = 12346;
  int remote_port = 12345;
  auto rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  
  // Open device
  auto opened_rnic = rdma_ctrl->open_device(idx);

  // Alloc Client RMDA buffer 
  size_t global_mr_size = PER_THREAD_ALLOC_SIZE * 0.2; // 100MB enough
  // Register a buffer to the previous opened device.
  char* global_mr = (char*)malloc(global_mr_size);
  memset(global_mr, 0, global_mr_size);
  RDMA_ASSERT(rdma_ctrl->register_memory(CLIENT_MR_ID, global_mr, global_mr_size, opened_rnic));

  MemoryAttr local_ring_free_frame_buffer_mr;
  // GetMRMeta
  while (QP::get_remote_mr("127.0.0.1", remote_port, SERVER_PAGETABLE_RING_FREE_FRAME_BUFFER_ID, &local_ring_free_frame_buffer_mr) != SUCC) {
    usleep(2000);
  }

  // Build QPs with itself
  MemoryAttr local_mr = rdma_ctrl->get_local_mr(CLIENT_MR_ID);
  //! 这里create_rc_idx(0, 0)存疑 
  RCQP* connect_local_ring_qp = rdma_ctrl->create_rc_qp(create_rc_idx(10086, 0), opened_rnic, &local_mr);
  assert(connect_local_ring_qp != nullptr);
  ConnStatus rc;
  do {
    rc = connect_local_ring_qp->connect("127.0.0.1", remote_port);
    if (rc == SUCC) {
      connect_local_ring_qp->bind_remote_mr(local_ring_free_frame_buffer_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
      RDMA_LOG(INFO) << "PageTable QP connected! with local Ring page free buffer";
    }
    usleep(2000);
  } while (rc != SUCC);

  // 访问
  uint64_t ring_buffer_base_off = 0;
  uint64_t ring_buffer_head_off = sizeof(RingBufferItem) * MAX_FREE_LIST_BUFFER_SIZE;
  uint64_t ring_buffer_tail_off = ring_buffer_head_off + sizeof(uint64_t);
  uint64_t ring_buffer_item_num_off = ring_buffer_tail_off + sizeof(uint64_t);

  char* faa_cnt_buf = global_mr;
  char* faa_head_buf = global_mr + 8;

  rc = connect_local_ring_qp->post_faa(faa_cnt_buf, ring_buffer_head_off, 1, IBV_SEND_SIGNALED);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "VictimPageThread";
  }
  rc = connect_local_ring_qp->post_faa(faa_head_buf, ring_buffer_item_num_off, 1, IBV_SEND_SIGNALED);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "VictimPageThread";
  }
  ibv_wc wc{};
  rc = connect_local_ring_qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
  }

  return 0;
}