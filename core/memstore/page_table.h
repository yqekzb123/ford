// Author: Huang Chunyue
// Copyright (c) 2023

#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <cassert>
#include <thread>
#include <mutex>
#include <list>
#include <unordered_map>
#include <condition_variable>

#include "memstore/mem_store.h"
#include "allocator/buffer_allocator.h"
#include "rlib/rdma_ctrl.hpp"
#include "base/common.h"
#include "base/page.h"
#include "util/hash.h"
#include "util/debug.h"

#define MAX_FREE_LIST_VICTIM_SIZE 20000

// 实现页表 -- std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
// 页表的作用是：通过页号PageId找到对应的帧号frame_id_t

struct PageAddress
{
  node_id_t node_id;
  frame_id_t frame_id;
};

struct PageTableItem {
  PageId page_id;
  PageAddress page_address;
  uint64_t rwcount = 0; // if the page is being modified
  bool page_valid = false; // if the page is valid, if not, the page is being read from disk and flush to the memstore
  timestamp_t last_access_time = 0; 
  // pay attention: valid is just for the slot, not for the page itself
  bool valid; // if the slot is empty, valid: exits value in the slot

  PageTableItem():valid(0) {}

  PageTableItem(PageId page_id, PageAddress page_address) :page_id(page_id), page_address(page_address), rwcount(0),
    page_valid(false), valid(1) {}
} Aligned8;

struct PageTableMeta {
  // Virtual address of the page table, used to calculate the distance
  // between some HashNodes with the table for traversing
  // the linked list
  uint64_t page_table_ptr;

  // Offset of the index, relative to the RDMA local_mr
  offset_t base_off;

  offset_t expand_base_off;
  
  // Total hash buckets
  uint64_t bucket_num;

  // Size of index node
  size_t node_size;

  offset_t free_ring_base_off;
  offset_t free_ring_head_off;
  offset_t free_ring_tail_off;
  offset_t free_ring_cnt_off;

  PageTableMeta(uint64_t page_table_ptr,
           uint64_t bucket_num,
           size_t node_size,
           offset_t base_off,
           offset_t expand_base_off,
           offset_t free_ring_base_off,
           offset_t free_ring_head_off,
           offset_t free_ring_tail_off,
           offset_t free_ring_cnt_off) : page_table_ptr(page_table_ptr),
                                base_off(base_off),
                                bucket_num(bucket_num),
                                node_size(node_size),
                                expand_base_off(expand_base_off),
                                free_ring_base_off(free_ring_base_off),
                                free_ring_head_off(free_ring_head_off),
                                free_ring_tail_off(free_ring_tail_off),
                                free_ring_cnt_off(free_ring_cnt_off) {}
  PageTableMeta() {}
} Aligned8;

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

// 计算每个哈希桶节点可以存放多少个rids
const int MAX_PAGETABLE_ITEM_NUM_PER_NODE = (PAGE_SIZE - sizeof(page_id_t) - sizeof(lock_t) - sizeof(short*) * NEXT_NODE_COUNT) / (sizeof(PageTableItem) );

// A PageTableNode is a bucket
// 这里注意：sizeof(PageTableNode)非4096，这可能可以有效较少RNIC的哈希碰撞，ref sigmod23 guide，
// 若后续持久化，应该持久化4K整页
struct PageTableNode {
  lock_t lock; 
  // node id
  page_id_t page_id;

  PageTableItem page_table_items[MAX_PAGETABLE_ITEM_NUM_PER_NODE];

  short next_expand_node_id[NEXT_NODE_COUNT] = {-1};
  // PageTableNode* next;
} Aligned4096;

class PageTableStore {
 public:
  PageTableStore(uint64_t bucket_num, MemStoreAllocParam* param)
      :base_off(0), bucket_num(bucket_num), page_table_ptr(nullptr), node_num(bucket_num), connect_local_ring_qp(nullptr){

    assert(bucket_num > 0);
    page_table_size = (bucket_num) * sizeof(PageTableNode);
    region_start_ptr = param->mem_region_start;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + page_table_size + sizeof(uint64_t) <= (uint64_t)param->mem_store_reserve);

    // fill_page_count是指针，指向额外分配页面的数量，安排已分配页面数量的位置，在地址索引空间的头部
    // 额外指开始分配了bucket_num数量的bucket_key, 如果bucket已满，则需要在保留空间中新建桶
    fill_page_count = (uint64_t*)(param->mem_store_start + param->mem_store_alloc_offset);
    *fill_page_count = bucket_num;
    param->mem_store_alloc_offset += sizeof(uint64_t);

    // 安排哈希表的位置
    page_table_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    param->mem_store_alloc_offset += page_table_size;

    base_off = (uint64_t)page_table_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);

    expand_base_off = (uint64_t)param->mem_store_reserve - (uint64_t)region_start_ptr;

    assert(page_table_ptr != nullptr);
    memset(page_table_ptr, 0, page_table_size);
    
    for(int i=0; i<bucket_num; i++){
      PageTableNode* node = (PageTableNode*)(i * sizeof(PageTableNode) + page_table_ptr);
      node->next_expand_node_id[0] = -1;
      node->next_expand_node_id[1] = -1;
      node->next_expand_node_id[2] = -1;
      node->next_expand_node_id[3] = -1;
      node->next_expand_node_id[4] = -1;
      node->page_id = i;
    }

    bucket_array = (PageTableNode*)page_table_ptr;

    // 这里初始化Freelist和线程替换函数
    ReveiveMeta();
    for(int i=0; i<manager_data_nodes.size(); i++){
      for(int j=0; j<data_node_frame_nums[i]; j++){
        free_list_.push_back(std::make_pair(manager_data_nodes[i], j));
      }
    }
    stop_victim = true;
    std::thread victim_thread_thread([this] {VictimPageThread();});
    victim_thread_thread.detach();
  }

  offset_t GetBaseOff() const {
    return base_off;
  }

  off64_t GetExpandBaseOff() const {
    return expand_base_off;
  }

  uint64_t GetPageTableMetaSize() const {
    return sizeof(PageTableMeta);
  }

  uint64_t GetBucketNum() const {
    return bucket_num;
  }

  char* GetAddrPtr() const {
    return page_table_ptr;
  }

  uint64_t PageTableNodeSize() const {
    return sizeof(PageTableNode);
  }

  uint64_t PageTableSize() const {
    return page_table_size;
  }

  uint64_t GetHash(PageId page_id) {
    return MurmurHash64A(page_id.Get(), 0xdeadbeef) % bucket_num;
  }

  char* GetRingBufferPtr(){
    return (char*)&ring_free_frame_buffer_.free_list_buffer_; //返回环形缓冲区的地址
  }

  size_t GetRingBufferSize(){
    return sizeof(RingFreeFrameBuffer);
  }

  PageAddress LocalGetPageFrame(PageId page_id);

  bool LocalInsertPageTableItem(PageId page_id, PageAddress page_address, MemStoreReserveParam* param);

  bool LocalDeletePageTableItem(PageId page_id);
  
  void VictimPageThread();
  void FillFreeListThread();
  node_id_t GetDataStoreMeta(std::string& remote_ip, int remote_port); 
  void ReveiveMeta();
  void BuildConnectWithRingBuffer();
  void SetRDMACtl(RdmaCtrlPtr rdma_ctrl){global_rdma_ctrl = rdma_ctrl;}

 private:
  // The offset in the RDMA region
  // Attention: the base_off is offset of fisrt index bucket
  offset_t base_off;
  offset_t expand_base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to value in the table
  char* page_table_ptr;
  PageTableNode* bucket_array;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t page_table_size;

  // Start of the index region address, for installing remote offset for index item
  char* region_start_ptr;

  // 地址中，已经被分配的页面数量
  uint64_t* fill_page_count;

  // 空闲页面链表
  // std::unordered_map<node_id_t, std::list<frame_id_t>> free_list_;
  // std::unordered_map<node_id_t, std::mutex> free_list_mutex_;

  // 空闲页面链表
  std::list<std::pair<node_id_t, frame_id_t>> free_list_;
  std::mutex free_list_mutex_;

  std::thread victim_page_thread_;
  bool stop_victim = false;
  std::mutex victim_mutex;
  std::condition_variable cv;
  RingFreeFrameBuffer ring_free_frame_buffer_;

  // RDMA原子操作和操作系统的原子操作是不兼容的
  // 因此PageTable对环形缓冲区加锁也需要使用RDMA原语，因此这里需要两个qp

  RdmaCtrlPtr global_rdma_ctrl;
  RNicHandler* opened_rnic;
  RCQP* connect_local_ring_qp;
  RCQP* connect_local_page_table_qp;

  RDMABufferAllocator* rdma_buffer_allocator;

  MemoryAttr local_ring_free_frame_buffer_mr{};
  MemoryAttr local_page_table_mr{};

  // Nodes managed by the page table
  std::vector<node_id_t> manager_data_nodes;
  std::vector<size_t> data_node_frame_nums;

public:
  // 设置环形缓冲区元信息的offset，便于RDMA访问
  const offset_t ring_buffer_base_off = 0;
  const offset_t ring_buffer_head_off = sizeof(RingBufferItem) * MAX_FREE_LIST_BUFFER_SIZE;
  const offset_t ring_buffer_tail_off = ring_buffer_head_off + sizeof(uint64_t);
  const offset_t ring_buffer_item_num_off = ring_buffer_tail_off + sizeof(uint64_t);
};
