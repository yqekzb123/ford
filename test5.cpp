#include <iostream>
#include <cstdint>
#include <cassert>
#include <thread>
#include <random>

#include "rlib/rdma_ctrl.hpp"
#include "dtx/doorbell.h"
#include "scheduler/corotine_scheduler.h"

using namespace rdmaio;

using node_id_t = int;        // Machine id type
using frame_id_t = int32_t;  // frame id type
using mr_id_t = int;          // Memory region id type

const mr_id_t SERVER_TEST_ID = 104; // !提示：这里还没注册呢
const mr_id_t CLIENT_TEST_MR_ID = 100;

static constexpr uint64_t EXCLUSIVE_LOCKED_ = 0xFF00000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED_ = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED_ = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS_ = 0xFF00000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED_ = 0xFFFFFFFFFFFFFFFF; // -1

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

#define thread_num 2
#define coro_num 2

void task(int thread_id, RdmaCtrl* rdma_ctrl, RNicHandler* opened_rnic, int remote_port) {
  auto coro_sched = new CoroutineScheduler(thread_id, coro_num, false);

  // Alloc Client RMDA buffer 
  size_t global_mr_size = PER_THREAD_ALLOC_SIZE * 0.2; // 100MB enough
  // Register a buffer to the previous opened device.
  char* global_mr = (char*)malloc(global_mr_size);
  memset(global_mr, 0, global_mr_size);
  RDMA_ASSERT(rdma_ctrl->register_memory(CLIENT_TEST_MR_ID, global_mr, global_mr_size, opened_rnic));

  MemoryAttr local_ring_free_frame_buffer_mr;
  // GetMRMeta
  while (QP::get_remote_mr("127.0.0.1", remote_port, SERVER_TEST_ID, &local_ring_free_frame_buffer_mr) != SUCC) {
    usleep(2000);
  }

  // Build QPs with itself
  MemoryAttr local_mr = rdma_ctrl->get_local_mr(CLIENT_TEST_MR_ID);
  RCQP* connect_local_ring_qp = rdma_ctrl->create_rc_qp(create_rc_idx(10086+thread_id, 0), opened_rnic, &local_mr);
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

  char* faa_cas_buf = global_mr;
  char* faa_read_buf = global_mr + 8;
  int pending_qp = 0;
  for(int i=0; i<200000; i++){
    // ExclusiveLock_SharedMutex_Batch doorbell;
    // doorbell.SetLockReq(faa_cas_buf, 0);
    // doorbell.SetReadReq(faa_read_buf, 0, 4096);
    // doorbell.SendReqs(coro_sched, connect_local_ring_qp, 1);
    // coro_sched->Yield()
    rc = connect_local_ring_qp->post_cas(faa_cas_buf, 0, UNLOCKED_, EXCLUSIVE_LOCKED_, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
    }
    rc = connect_local_ring_qp->post_send(IBV_WR_RDMA_READ ,faa_read_buf, 0, 4096, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << "VictimPageThread";
    }
    pending_qp++;
    pending_qp++;
    while (pending_qp!=0){
        ibv_wc wc{};
        rc = connect_local_ring_qp->poll_till_completion(wc, no_timeout);
        pending_qp--;
    }
    if(*(uint64_t*)faa_cas_buf == UNLOCKED_) {
        TestNode* node = (TestNode*)faa_read_buf;
        int target_i = rand() % TestItemSize;
        int j;
        for(j=0; j< TestItemSize; j++){
            if(node->free_list_buffer_[j].page_id == target_i && node->free_list_buffer_[j].valid == true){
                if(node->free_list_buffer_[j].page_valid == false){
                    std::cout << "page is not valid: "<< target_i << std::endl;
                }
                break;
            }
        }
        int off = 0;
        if(j==TestItemSize){
            for(int k=0; k<TestItemSize; k++){
                if(node->free_list_buffer_[k].valid == false){
                    node->free_list_buffer_[k].page_id = target_i;
                    node->free_list_buffer_[k].valid = true;
                    off = (uint64_t)&node->free_list_buffer_[k] - (uint64_t)node;
                    rc = connect_local_ring_qp->post_send(IBV_WR_RDMA_WRITE, faa_read_buf + off, sizeof(TestItem), off, IBV_SEND_SIGNALED);
                    pending_qp++;
                    break;
                }
            }
        }
        rc = connect_local_ring_qp->post_faa(faa_cas_buf, 0, EXCLUSIVE_UNLOCK_TO_BE_ADDED_, IBV_SEND_SIGNALED);
        pending_qp++;
        usleep(50);
        if(off != 0){
            TestItem* item = (TestItem*)(faa_read_buf + off);
            item->page_valid = true;
            rc = connect_local_ring_qp->post_send(IBV_WR_RDMA_WRITE, faa_read_buf + off, sizeof(TestItem), off, IBV_SEND_SIGNALED);
            pending_qp++;
        }
    }
  }
}

//这里测试一下rdma功能
int main() {

  std::vector<std::thread> thread_vec;
  
  int server_node_id = 0;
  int local_port = 12346;
  int remote_port = 12345;
  auto rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  
  // Open device
  auto opened_rnic = rdma_ctrl->open_device(idx);

  for(int i=0; i<thread_num; i++) {
    thread_vec.emplace_back([i, rdma_ctrl, opened_rnic, remote_port](){
        task(i, rdma_ctrl.get(), opened_rnic, remote_port);
    });
  }
  for(auto& th : thread_vec) {
    th.join();
  }
  
  // check the result
  usleep(1000000);

  std::cout << "Test passed!" << std::endl;

  return 0;
}