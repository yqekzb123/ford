// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <atomic>
#include <cassert>

#include "base/common.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

const uint64_t PER_THREAD_ALLOC_SIZE = (size_t)500 * 1024 * 1024;

// This allocator is a global one which manages all the RDMA regions in this machine

// |                   | <- t1 start
// |                   |
// |                   |
// |                   |
// |                   | <- t1 end. t2 start
// |                   |
// |                   |
// |                   |
// |                   | <- t2 end. t3 start

class RDMARegionAllocator {
 public:
  RDMARegionAllocator(RdmaCtrlPtr global_rdma_ctrl, RNicHandler* opened_rnic, t_id_t thread_num_per_machine) {
    size_t global_mr_size = (size_t)thread_num_per_machine * PER_THREAD_ALLOC_SIZE;
    // Register a buffer to the previous opened device. It's DRAM in compute pools
    global_mr = (char*)malloc(global_mr_size);
    thread_num = thread_num_per_machine;
    memset(global_mr, 0, global_mr_size);
    RDMA_ASSERT(global_rdma_ctrl->register_memory(CLIENT_MR_ID, global_mr, global_mr_size, opened_rnic));
  }

  ~RDMARegionAllocator() {
    if (global_mr) free(global_mr);
  }

  ALWAYS_INLINE
  std::pair<char*, char*> GetThreadLocalRegion(t_id_t tid) {
    assert(tid < thread_num);
    return std::make_pair(global_mr + tid * PER_THREAD_ALLOC_SIZE, global_mr + (tid + 1) * PER_THREAD_ALLOC_SIZE);
  }

 private:
  char* global_mr;  // memory region
  t_id_t thread_num;
  size_t log_buf_size;
};
