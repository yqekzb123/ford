// Author: Huang Chunyue
// Copyright (c) 2023

#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <cassert>

#include "memstore/mem_store.h"
#include "base/common.h"
#include "base/page.h"
#include "util/hash.h"
#include "util/debug.h"

// 实现页表 -- std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
// 页表的作用是：通过页号PageId找到对应的帧号frame_id_t

struct PageTableItem {
  PageId page_id;
  frame_id_t frame_id;
  uint8_t valid; // if the slot is empty, valid: exits value in the slot

  PageTableItem():valid(0) {}

  PageTableItem(PageId page_id, frame_id_t frame_id) :page_id(page_id), frame_id(frame_id), valid(1) {}
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

  PageTableMeta(uint64_t page_table_ptr,
           uint64_t bucket_num,
           size_t node_size,
           offset_t base_off) : page_table_ptr(page_table_ptr),
                                base_off(base_off),
                                bucket_num(bucket_num),
                                node_size(node_size) {}
  PageTableMeta() {}
} Aligned8;


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
  PageTableStore(uint64_t bucket_num, MemStoreAllocParam* param)
      :base_off(0), bucket_num(bucket_num), page_table_ptr(nullptr), node_num(0) {

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

  frame_id_t LocalGetPageFrame(PageId page_id);

  bool LocalInsertPageTableItem(PageId page_id, frame_id_t frame_id, MemStoreReserveParam* param);

  bool LocalDeletePageTableItem(PageId page_id);

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

  // 地址索引中，已经被分配的页面数量
  uint64_t* fill_page_count;

};


ALWAYS_INLINE
frame_id_t PageTableStore::LocalGetPageFrame(PageId page_id) {
  uint64_t hash = GetHash(page_id);
  short next_expand_node_id;
  PageTableNode* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);
  do {
    for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
      if (node->page_table_items[i].page_id == page_id && node->page_table_items[i].valid == true) {
        // 返回帧号
        return node->page_table_items[i].frame_id;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (PageTableNode*)((bucket_num + next_expand_node_id) * sizeof(PageTableNode) + page_table_ptr);
  } while( next_expand_node_id >= 0);
  return -1;  // failed to found one
}

ALWAYS_INLINE
bool PageTableStore::LocalInsertPageTableItem(PageId page_id, frame_id_t frame_id, MemStoreReserveParam* param) {

  frame_id_t find_exits = LocalGetPageFrame(page_id);
  // exits same key
  if(find_exits >= 0) return false;

  uint64_t hash = GetHash(page_id);
  auto* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);

  // Find
  while (true) {
    for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++){
      if(node->page_table_items[i].valid == false){
        node->page_table_items[i].page_id = page_id;
        node->page_table_items[i].frame_id = frame_id;
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
  new_node->page_table_items[0].frame_id = frame_id;
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
