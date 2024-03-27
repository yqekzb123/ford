// Author: Huang Chunyue
// Copyright (c) 2023

#pragma once

#include <fcntl.h>     
#include <sys/stat.h>  
#include <unistd.h>    
#include <atomic>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>

#include "base/common.h"
#include "log/log_manager.h"
#include "disk_manager.h"
#include "allocator/buffer_allocator.h"

struct StorageRDMALogBuffer{
  char buffer_[RDMA_LOG_BUFFER_SIZE];
  uint64_t head_ = 0;
  uint64_t tail_ = 0;
  int64_t log_char_num_ = 0;
};

struct StoragePageLogCnt{
  int64_t log_cnt_[MAX_PAGE_NUM_PER_RIGION];
};

struct StorageMemoryRegion{
  StorageRDMALogBuffer log_buffer;
  StoragePageLogCnt page_log_cnt;
}Aligned8;

struct StorageMeta {
  offset_t log_base_off;
  offset_t log_head_off;
  offset_t log_tail_off;
  offset_t log_cnt_off;
  offset_t page_cnt_off;

  StorageMeta(offset_t log_base_off,
           offset_t log_head_off,
           offset_t log_tail_off,
           offset_t log_cnt_off,
           offset_t page_cnt_off): 
                log_base_off(log_base_off),
                log_head_off(log_head_off),
                log_tail_off(log_tail_off),
                log_cnt_off(log_cnt_off),
                page_cnt_off(page_cnt_off){}

  StorageMeta() {}
} Aligned8;

class StorageRDMAMemoryManager { 
  private:
    StorageMemoryRegion* mem_region_;
    StorageRDMALogBuffer* log_buffer_;
    StoragePageLogCnt* page_log_cnt_;
    LogManager* log_manager_;
    DiskManager* disk_manager_;

    RDMABufferAllocator* rdma_buffer_allocator;
    RdmaCtrlPtr client_rdma_ctrl;
    RdmaCtrlPtr server_rdma_ctrl;
    RNicHandler* opened_rnic;
    RCQP* connect_log_buffer_qp;
    MemoryAttr local_log_buffer_mr{};

  public:
    StorageRDMAMemoryManager(LogManager* log_manager, DiskManager* disk_manager)
        : log_manager_(log_manager), disk_manager_(disk_manager){
      // 1. new log buffer
      mem_region_ = new StorageMemoryRegion();
      memset(mem_region_, 0, sizeof(StorageMemoryRegion));
      log_buffer_ = &mem_region_->log_buffer;
      page_log_cnt_ = &mem_region_->page_log_cnt;

      // 2. build connect with local log buffer
      BuildConnectWithLogBuffer();
      
      std::thread async_flush_thread([this]{
        AsyncFlush();
      });
      async_flush_thread.detach();
    }

    ~StorageRDMAMemoryManager() {
      client_rdma_ctrl->destroy_rc_qp();
      server_rdma_ctrl->destroy_rc_qp();
      delete log_buffer_;
    }

    void BuildConnectWithLogBuffer();

    void AsyncFlush();
    
    char* GetMemRegion(){
      return (char*)mem_region_;
    }

  public:
    // 设置环形缓冲区元信息的offset，便于RDMA访问
    static const offset_t log_base_off = 0;
    static const offset_t log_head_off = sizeof(char) * MAX_FREE_LIST_BUFFER_SIZE;
    static const offset_t log_tail_off = log_head_off + sizeof(uint64_t);
    static const offset_t log_cnt_off = log_tail_off + sizeof(uint64_t);
    static const offset_t page_cnt_off = log_cnt_off + sizeof(int64_t);
};