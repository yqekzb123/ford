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

#include "memstore/mem_store.h"
#include "rlib/rdma_ctrl.hpp"
#include "base/common.h"
#include "base/page.h"
#include "util/hash.h"
#include "util/debug.h"

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

  // Total hash buckets
  uint64_t bucket_num;

  // Size of index node
  size_t node_size;

  // Nodes managed by the page table
  std::vector<node_id_t> manager_data_nodes;
  std::vector<offset_t> data_node_base_offs;
  std::vector<size_t> data_node_frame_nums;

  PageTableMeta(uint64_t page_table_ptr,
           uint64_t bucket_num,
           size_t node_size,
           offset_t base_off) : page_table_ptr(page_table_ptr),
                                base_off(base_off),
                                bucket_num(bucket_num),
                                node_size(node_size) {}
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
} Aligned8;

class PageTableStore {
 public:
  PageTableStore(uint64_t bucket_num, MemStoreAllocParam* param, RCQP *local_qp)
      :base_off(0), bucket_num(bucket_num), page_table_ptr(nullptr), node_num(0), qp(local_qp) {

    assert(bucket_num > 0);
    index_size = (bucket_num) * sizeof(PageTableNode);
    region_start_ptr = param->mem_region_start;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + index_size + sizeof(uint64_t) <= (uint64_t)param->mem_store_reserve);

    // fill_page_count是指针，指向额外分配页面的数量，安排已分配页面数量的位置，在地址索引空间的头部
    // 额外指开始分配了bucket_num数量的bucket_key, 如果bucket已满，则需要在保留空间中新建桶
    fill_page_count = (uint64_t*)&param->mem_store_start + param->mem_store_alloc_offset;
    *fill_page_count = 0;
    param->mem_store_alloc_offset += sizeof(uint64_t);

    // 安排哈希表的位置
    page_table_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    param->mem_store_alloc_offset += index_size;

    base_off = (uint64_t)page_table_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);

    assert(page_table_ptr != nullptr);
    memset(page_table_ptr, 0, index_size);
    
    bucket_array = (PageTableNode*)page_table_ptr;
  }

  offset_t GetBaseOff() const {
    return base_off;
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

  uint64_t IndexSize() const {
    return index_size;
  }

  uint64_t GetHash(PageId page_id) {
    return MurmurHash64A(page_id.Get(), 0xdeadbeef) % bucket_num;
  }

  PageAddress LocalGetPageFrame(PageId page_id);

  bool LocalInsertPageTableItem(PageId page_id, PageAddress page_address, MemStoreReserveParam* param);

  bool LocalDeletePageTableItem(PageId page_id);
  
  void VictimPageThread();
 private:
  // The offset in the RDMA region
  // Attention: the base_off is offset of fisrt index bucket
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to value in the table
  char* page_table_ptr;
  PageTableNode* bucket_array;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t index_size;

  // Start of the index region address, for installing remote offset for index item
  char* region_start_ptr;

  // 地址中，已经被分配的页面数量
  uint64_t* fill_page_count;

  // 空闲页面链表
  std::unordered_map<node_id_t, std::list<frame_id_t>> free_list_;
  std::unordered_map<node_id_t, std::mutex> free_list_mutex_;
  std::thread victim_page_thread_;
  bool stop_victim = false;
  RCQP* qp;
  RingFreeFrameBuffer ring_free_frame_buffer_;

  // 设置环形缓冲区元信息的offset，便于RDMA访问
  offset_t ring_buffer_base_off;
  offset_t ring_buffer_head_off;
  offset_t ring_buffer_tail_off;
  offset_t ring_buffer_item_num_off;
};

void PageTableStore::VictimPageThread(){
  // 这里需要一个线程，不断的将超时页面放入空闲页面链表中
  std::thread victim_page_thread_ = std::thread([this] {
    char* cas_buf = new char[sizeof(lock_t)];
    char* faa_buf = new char[sizeof(lock_t)];
    while (!stop_victim) {
      for(int i=0; i<* fill_page_count; i++){
        offset_t offset = base_off + i * sizeof(PageTableNode);

        // ****************************************************************************************
        // 这里手写了一个RDMACAS + RDMARead的操作，因为这是memstore的操作，不在DTX中，因此无法使用DTX的接口
        // 这里需要注意，这里的RDMACAS因为和本地原子操作和RDMA原子操作不兼容，因此需要使用RDMACAS来加锁
        auto rc = qp->post_cas(cas_buf, offset, UNLOCKED, EXCLUSIVE_LOCKED, IBV_SEND_SIGNALED);
        if (rc != SUCC) {
          RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
          return false;
        }
        ibv_wc wc{};
        rc = qp->poll_till_completion(wc, no_timeout);
        if (rc != SUCC) {
          RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
          return false;
        }
        
        if( *(lock_t*)cas_buf == EXCLUSIVE_LOCKED){
          // 加锁成功
          PageTableNode* node = (PageTableNode*)(offset + page_table_ptr);
          timestamp_t min_timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() - 5;
          for(int j=0; j<MAX_PAGETABLE_ITEM_NUM_PER_NODE; j++){
            if(node->page_table_items[j].valid == true){
              // 该页面是有效页面
              if(node->page_table_items[j].rwcount == 0 && node->page_table_items[j].last_access_time < min_timestamp){
                // 该页面没有被修改，可以被替换
                // 将该页面放入空闲页面链表中
                free_list_mutex_[node->page_table_items[j].page_address.node_id].lock();
                free_list_[node->page_table_items[j].page_address.node_id].push_back(node->page_table_items[j].page_address.frame_id);
                free_list_mutex_[node->page_table_items[j].page_address.node_id].unlock();
                // 将该页面从页表中删除
                node->page_table_items[j].valid = false;
              }
            }
          }

          // 释放锁
          rc = qp->post_faa(faa_buf, offset, EXCLUSIVE_UNLOCK_TO_BE_ADDED, IBV_SEND_SIGNALED);
          if (rc != SUCC) {
            RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
            return false;
          }
        }
      }
    }    
  });

  while (true) {
    // 从空闲页面链表中取出空闲页面或者从页表中将超时的页面替换掉，放入环形缓冲区从head开始
    char* faa_cnt_buf = new char[sizeof(int64_t)];
    char* faa_head_buf = new char[sizeof(uint64_t)];
    auto rc = qp->post_faa(faa_cnt_buf, ring_buffer_head_off, 1, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
    }
    rc = qp->post_faa(faa_head_buf, ring_buffer_item_num_off, 1, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
    }
    ibv_wc wc{};
    rc = qp->poll_till_completion(wc, no_timeout);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
    }

    if(*(int64_t*)faa_cnt_buf >= MAX_FREE_LIST_BUFFER_SIZE -1){
      // buffer is full
      auto rc = qp->post_faa(faa_cnt_buf, ring_buffer_head_off, -1, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
      }
      rc = qp->post_faa(faa_head_buf, ring_buffer_item_num_off, -1, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
      }
      ibv_wc wc{};
      rc = qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    else{
      // buffer is not full
      for(auto& node_id: free_list_){
        free_list_mutex_[node_id.first].lock();
        if(node_id.second.size() > 0){
          uint64_t head = (*(uint64_t*)faa_head_buf) % MAX_FREE_LIST_BUFFER_SIZE;
          ring_free_frame_buffer_.free_list_buffer_[head] = {node_id.first, node_id.second.front(), true};
          node_id.second.pop_front();
        }
        free_list_mutex_[node_id.first].unlock();
        // TODO: 这里或许可以动态调节超时的时间
      }
    }
  }
}

ALWAYS_INLINE
PageAddress PageTableStore::LocalGetPageFrame(PageId page_id) {
  uint64_t hash = GetHash(page_id);
  short next_expand_node_id;
  PageTableNode* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);
  do {
    for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
      if (node->page_table_items[i].page_id == page_id && node->page_table_items[i].valid == true) {
        // 返回帧号
        return node->page_table_items[i].page_address;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (PageTableNode*)((bucket_num + next_expand_node_id) * sizeof(PageTableNode) + page_table_ptr);
  } while( next_expand_node_id >= 0);
  return {-1, INVALID_FRAME_ID};  // failed to found one
}

ALWAYS_INLINE
bool PageTableStore::LocalInsertPageTableItem(PageId page_id, PageAddress page_address, MemStoreReserveParam* param) {

  PageAddress find_exits = LocalGetPageFrame(page_id);
  // exits same key
  if(find_exits.frame_id == INVALID_FRAME_ID) return false;

  uint64_t hash = GetHash(page_id);
  auto* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);

  // Find
  while (true) {
    for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++){
      if(node->page_table_items[i].valid == false){
        node->page_table_items[i].page_id = page_id;
        node->page_table_items[i].page_address = page_address;
        node->page_table_items[i].valid = true;
        return true;
      }
    }

    if (node->next_expand_node_id[0] <= 0) break;
    node = (PageTableNode*)((bucket_num + node->next_expand_node_id[0]) * sizeof(PageTableNode) + page_table_ptr);
  }

  // Allocate
  // RDMA_LOG(INFO) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum/bucket: " << ITEM_NUM_PER_NODE;
  assert((uint64_t)param->mem_store_reserve + param->mem_store_reserve_offset <= (uint64_t)param->mem_store_end);
  auto* new_node = (PageTableNode*)(param->mem_store_reserve + param->mem_store_reserve_offset);
  param->mem_store_reserve_offset += sizeof(PageTableNode);
  memset(new_node, 0, sizeof(PageTableNode));
  new_node->page_table_items[0].page_id = page_id;
  new_node->page_table_items[0].page_address = page_address;
  new_node->page_table_items[0].valid = true;
  new_node->next_expand_node_id[0] = -1;
  new_node->page_id = node_num;
  node->next_expand_node_id[0] = node_num - bucket_num;
  
  node_num++;
  *fill_page_count = *fill_page_count + 1;
  return true;
}

ALWAYS_INLINE
bool PageTableStore::LocalDeletePageTableItem(PageId page_id) {
  uint64_t hash = GetHash(page_id);
  auto* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);
  short next_expand_node_id;
  do{
    for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
      if(node->page_table_items[i].page_id == page_id){ 
        // find it 
        node->page_table_items[i].valid = 0;
        return true;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (PageTableNode*)((bucket_num + next_expand_node_id) * sizeof(PageTableNode) + page_table_ptr);
  } while( next_expand_node_id >= 0);
  return false;
}
