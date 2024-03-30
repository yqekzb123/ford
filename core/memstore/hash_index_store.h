// Author: Huang chunyue
// Copyright (c) 2023

#pragma once

#include <cassert>

#include "base/page.h"
#include "base/common.h"
#include "memstore/data_item.h"
#include "memstore/mem_store.h"
#include "util/hash.h"

struct IndexItem {
  itemkey_t key;
  Rid rid;
  uint8_t valid; // if the slot is empty, valid: exits value in the slot
  // lock_t lock; // if the slot is locked

  IndexItem():valid(0) {}

  IndexItem(itemkey_t k, Rid rid) :key(k), rid(rid), valid(1) {}
} Aligned8;

struct IndexMeta {
  // To which table this index belongs
  table_id_t table_id;

  // Virtual address of the index, used to calculate the distance
  // between some HashNodes with the table for traversing
  // the linked list
  uint64_t index_ptr;

  // Offset of the index, relative to the RDMA local_mr
  offset_t base_off;

  offset_t expand_base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // Size of index node
  size_t node_size;

  IndexMeta(table_id_t table_id,
           uint64_t data_ptr,
           uint64_t bucket_num,
           size_t node_size,
           offset_t base_off,
           offset_t expand_base_off) : table_id(table_id),
                                index_ptr(data_ptr),
                                base_off(base_off),
                                bucket_num(bucket_num),
                                node_size(node_size),
                                expand_base_off(expand_base_off) {}
  IndexMeta() {}
} Aligned8;


// 计算每个哈希桶节点可以存放多少个rids
const int MAX_RIDS_NUM_PER_NODE = (BUCKET_SIZE - sizeof(page_id_t) - sizeof(lock_t) - sizeof(short*) * NEXT_NODE_COUNT) / (sizeof(IndexItem) );

// A IndexNode is a bucket
// 这里注意：sizeof(IndexNode)是4080而非4096，这可能可以有效较少RNIC的哈希碰撞，ref sigmod23 guide，
// 若后续持久化，应该持久化4K整页
struct IndexNode {
  lock_t lock; 
  // node id
  page_id_t page_id;

  IndexItem index_items[MAX_RIDS_NUM_PER_NODE];

  short next_expand_node_id[NEXT_NODE_COUNT] = {-1};
  // IndexNode* next;
} Aligned2048;

class IndexStore {
 public:
  IndexStore(table_id_t table_id, uint64_t bucket_num, MemStoreAllocParam* param, MemStoreReserveParam* param_reserve)
      :table_id(table_id), base_off(0), bucket_num(bucket_num), index_ptr(nullptr), node_num(bucket_num) {

    assert(bucket_num > 0);
    index_size = (bucket_num) * sizeof(IndexNode);
    region_start_ptr = param->mem_region_start;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + index_size <= (uint64_t)param->mem_store_reserve);

    // fill_page_count是指针，指向额外分配页面的数量，安排已分配页面数量的位置，在地址索引空间的头部
    // 额外指开始分配了bucket_num数量的bucket_key, 如果bucket已满，则需要在保留空间中新建桶
    // fill_page_count = (uint64_t*)&param->mem_store_start + param->mem_store_alloc_offset;
    // *fill_page_count = 0;
    // param->mem_store_alloc_offset += sizeof(uint64_t);

    // 安排哈希表的位置
    index_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    param->mem_store_alloc_offset += index_size;

    base_off = (uint64_t)index_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);
    expand_base_off = (uint64_t)param_reserve->mem_store_reserve - (uint64_t)region_start_ptr;

    assert(index_ptr != nullptr);
    memset(index_ptr, 0, index_size);
    
    for(int i=0; i<bucket_num; i++){
      IndexNode* node = (IndexNode*)(i * sizeof(IndexNode) + index_ptr);
      node->next_expand_node_id[0] = -1;
      node->next_expand_node_id[1] = -1;
      node->next_expand_node_id[2] = -1;
      node->next_expand_node_id[3] = -1;
      node->next_expand_node_id[4] = -1;
      node->page_id = i;
    }

    bucket_array = (IndexNode*)index_ptr;

    // 安排额外的空间，用于扩展哈希表
    expand_region_base_ptr = param_reserve->mem_store_reserve;
  }

  table_id_t GetTableID() const {
    return table_id;
  }

  offset_t GetBaseOff() const {
    return base_off;
  }

  offset_t GetExpandBaseOff() const {
    return expand_base_off;
  }
  
  uint64_t GetIndexNodeSize() const {
    return sizeof(IndexNode);
  }

  uint64_t GetBucketNum() const {
    return bucket_num;
  }

  char* GetIndexPtr() const {
    return index_ptr;
  }

  // offset_t GetItemRemoteOffset(const void* item_ptr) const {
  //   return (uint64_t)item_ptr - (uint64_t)region_start_ptr;
  // }

  uint64_t IndexSize() const {
    return index_size;
  }

  uint64_t GetHash(itemkey_t key) {
    return MurmurHash64A(key, 0xdeadbeef) % bucket_num;
  }

  Rid LocalGetIndexRid(itemkey_t key);

  bool LocalInsertKeyRid(itemkey_t key, const Rid& rid, MemStoreReserveParam* param);

  // bool LocalPutKeyRid(itemkey_t key, const Rid& rid, MemStoreReserveParam* param);

  bool LocalDelete(itemkey_t key);

 private:
  // To which table this hash store belongs
  table_id_t table_id;
  
  // The offset in the RDMA region
  // Attention: the base_off is offset of fisrt index bucket
  offset_t base_off;
  
  offset_t expand_base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to value in the table
  char* index_ptr;
  IndexNode* bucket_array;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t index_size;

  // Start of the index region address, for installing remote offset for index item
  char* region_start_ptr;

  char* expand_region_base_ptr;
};

ALWAYS_INLINE
Rid IndexStore::LocalGetIndexRid(itemkey_t key) {
  uint64_t hash = GetHash(key);
  short next_expand_node_id;
  IndexNode* node = (IndexNode*)(hash * sizeof(IndexNode) + index_ptr);
  do {
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
      if (node->index_items[i].key == key && node->index_items[i].valid == true) {
        return node->index_items[i].rid;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (IndexNode*)(next_expand_node_id * sizeof(IndexNode) + expand_region_base_ptr);
  } while( next_expand_node_id >= 0);
  return {INVALID_PAGE_ID, -1};  // failed to found one
}

ALWAYS_INLINE
bool IndexStore::LocalInsertKeyRid(itemkey_t key, const Rid& rid, MemStoreReserveParam* param) {

  Rid find_exits = LocalGetIndexRid(key);
  // exits same key
  if(find_exits.page_no_ != INVALID_PAGE_ID) return false;

  uint64_t hash = GetHash(key);
  auto* node = (IndexNode*)(hash * sizeof(IndexNode) + index_ptr);

  // Find
  while (true) {
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
      if(node->index_items[i].valid == false){
        node->index_items[i].key = key;
        node->index_items[i].rid = rid;
        node->index_items[i].valid = true;
        return true;
      }
    }

    if (node->next_expand_node_id[0] < 0) break;
    node = (IndexNode*)(node->next_expand_node_id[0] * sizeof(IndexNode) + expand_region_base_ptr);
  }

  // Allocate
  // RDMA_LOG(INFO) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum/bucket: " << ITEM_NUM_PER_NODE;
  assert((uint64_t)param->mem_store_reserve + param->mem_store_reserve_offset + sizeof(IndexNode) <= (uint64_t)param->mem_store_end);
  auto* new_node = (IndexNode*)(param->mem_store_reserve + param->mem_store_reserve_offset);
  param->mem_store_reserve_offset += sizeof(IndexNode);
  memset(new_node, 0, sizeof(IndexNode));
  new_node->index_items[0].key = key;
  new_node->index_items[0].rid = rid;
  new_node->index_items[0].valid = true;
  new_node->next_expand_node_id[0] = -1;
  new_node->next_expand_node_id[1] = -1;
  new_node->next_expand_node_id[2] = -1;
  new_node->next_expand_node_id[3] = -1;
  new_node->next_expand_node_id[4] = -1;
  new_node->page_id = node_num;
  // node->next_expand_node_id[0] = node_num - bucket_num;
  node->next_expand_node_id[0] = param->mem_store_reserve_offset / sizeof(IndexNode) - 1;
  
  node_num++;
  return true;
}

ALWAYS_INLINE
bool IndexStore::LocalDelete(itemkey_t key) {
  uint64_t hash = GetHash(key);
  auto* node = (IndexNode*)(hash * sizeof(IndexNode) + index_ptr);
  short next_expand_node_id;
  do{
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
      if(node->index_items[i].key == key){ 
        // find it 
        node->index_items[i].valid = 0;
        return true;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (IndexNode*)(next_expand_node_id * sizeof(IndexNode) + expand_region_base_ptr);
  } while( next_expand_node_id >= 0);
  return false;
}