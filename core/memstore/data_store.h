#pragma once
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <unordered_map>
#include <vector>
#include <mutex>

#include "memstore/data_item.h"
#include "memstore/mem_store.h"
#include "storage/disk_manager.h"
#include "util/errors.h"
#include "base/page.h"

// 这里！！！！！！！！！！！！！！！！！！！！！！
// 逻辑开始混沌了，，，，
// 注意区分是哪个bufferpool，思路，把现在这个bufferpool放到test里
// 之后重新写一个bufferpool，内存层的
// 磁盘里是没有bufferpool的 也不需要record 这些都是为了测试用的

// 然后在新的bufferpool里面，完成缓冲区驱逐策略的逻辑
// 一个bufferpool里面，只需要有page的data，也不需要pageID这些东西，因为这个是在页表维护的

struct DataStoreMeta {
  // Virtual address of the data store, used to calculate the distance
  // between some HashNodes with the table for traversing
  // the linked list
  uint64_t data_store_ptr;

  // Offset of the data store, relative to the RDMA local_mr
  offset_t base_off;

  // Total hash buckets
  uint64_t page_num;

  // Size of data store node
  size_t node_size;

  DataStoreMeta(uint64_t data_ptr,
                uint64_t page_num,
                size_t node_size,
                offset_t base_off):
                        data_store_ptr(data_store_ptr),
                        base_off(base_off),
                        page_num(page_num),
                        node_size(node_size) {}
  DataStoreMeta(){}
} Aligned8;

class DataStore {
   private:
    size_t pool_page_num_;      // buffer_pool中可容纳页面的个数，即帧的个数
    MemPage *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    offset_t base_off_;         // buffer_pool在RDMA内存中的偏移量

   public:
    DataStore(size_t pool_page_num_, MemStoreAllocParam* param)
        : pool_page_num_(pool_page_num_) {
        // 为buffer pool分配一块连续的内存空间
        assert(pool_page_num_ > 0);
        char* data_store_ptr = param->mem_store_start + param->mem_store_alloc_offset;
        param->mem_store_alloc_offset += pool_page_num_ * PAGE_SIZE;
        pages_ = (MemPage*)data_store_ptr;
        base_off_ = (uint64_t)pages_ - (uint64_t)param->mem_store_start;
    }

    ~DataStore() {
        delete[] pages_;
    }

   public: 
   char* GetAddrPtr(){
    return (char*)pages_;
   }

   offset_t GetBaseOff(){
    return base_off_;
   }

   size_t GetPageNum(){
    return pool_page_num_;
   }

    size_t GetNodeSize(){
      return sizeof(MemPage);
    }
};