// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2022

#pragma once

#include <cassert>

#include "memstore/data_item.h"
#include "memstore/mem_store.h"
#include "util/hash.h"

#define OFFSET_NOT_FOUND -1
#define OFFSET_FOUND 0
#define VERSION_TOO_OLD -2  // The new version < old version

#define SLOT_NOT_FOUND -1
#define SLOT_INV -2
#define SLOT_LOCKED -3
#define SLOT_FOUND 0


const int NEXT_NODE_COUNT = 4;
const int ADDR_NUM_PER_NODE = (PAGE_SIZE - sizeof(short) * NEXT_NODE_COUNT) / sizeof(AddrMeta);

struct AddrMeta {
  // 对应的Pageid
  page_id_t id;
  // 对应的Page所在节点
  int node_id;
  // 对应的Page所在地址
  uint64_t page_ptr;

  AddrMeta(page_id_t id,
           int node_id,
           uint64_t page_ptr) : id(id),
                                node_id(node_id),
                                page_ptr(page_ptr) {}
  AddrMeta() {}
} Aligned8;

// A AddrNode is a bucket
struct AddrNode {
  // A AddrMeta is a slot
  AddrMeta addr_meta[ADDR_NUM_PER_NODE];
  short next_node_id[NEXT_NODE_COUNT];
  // AddrNode* next;
} Aligned8;

class AddrStore {
 public:
  AddrStore(uint64_t bucket_num, MemStoreAllocParam* param)
      : base_off(0), bucket_num(bucket_num), addr_ptr(nullptr), node_num(0) {
    assert(bucket_num > 0);
    addr_size = (bucket_num) * sizeof(AddrNode);
    region_start_ptr = param->mem_region_start;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + addr_size <= (uint64_t)param->mem_store_reserve);

    // 安排已分配页面数量的位置，在地址索引空间的头部
    fill_page_count = (uint64_t*)&param->mem_store_start + param->mem_store_alloc_offset;
    *fill_page_count = 0;
    param->mem_store_alloc_offset += sizeof(uint64_t);

    // 安排哈希表的位置
    addr_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    param->mem_store_alloc_offset += addr_size;

    base_off = (uint64_t)addr_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);

    assert(addr_ptr != nullptr);
    memset(addr_ptr, 0, addr_size);
    bucket_array = (AddrNode*)addr_ptr;
  }

  offset_t GetBaseOff() const {
    return base_off;
  }

  uint64_t GetAddrMetaSize() const {
    return sizeof(AddrMeta);
  }

  uint64_t GetBucketNum() const {
    return bucket_num;
  }

  char* GetAddrPtr() const {
    return addr_ptr;
  }

  // offset_t GetItemRemoteOffset(const void* item_ptr) const {
  //   return (uint64_t)item_ptr - (uint64_t)region_start_ptr;
  // }

  uint64_t TableSize() const {
    return addr_size;
  }

  uint64_t GetHash(page_id_t key) {
    return MurmurHash64A(key, 0xdeadbeef) % bucket_num;
  }

  AddrMeta* LocalGet(page_id_t key);

  AddrMeta* LocalInsert(page_id_t key, const AddrMeta& addr_meta, MemStoreReserveParam* param);

  AddrMeta* LocalPut(page_id_t key, const const AddrMeta& addr_meta, MemStoreReserveParam* param);

  bool LocalDelete(page_id_t key);

 private:
  // The offset in the RDMA region
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to value in the table
  char* addr_ptr;
  AddrNode* bucket_array;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t addr_size;

  // Start of the memory region address, for installing remote offset for data item
  char* region_start_ptr;

  // 地址索引中，已经被分配的页面数量
  uint64_t* fill_page_count;

};

/*
ALWAYS_INLINE
AddrMeta* AddrStore::LocalGet(page_id_t key) {
  uint64_t hash = GetHash(key);
  AddrNode* node = (AddrNode*)(hash * sizeof(AddrNode) + addr_ptr);
  while (node) {
    for (auto& addr_meta : node->addr_meta) {
      if (addr_meta.id == key) {
        return &addr_meta;
      }
    }
    // node = node->next;
  }
  return nullptr;  // failed to found one
}

ALWAYS_INLINE
AddrMeta* AddrStore::LocalInsert(page_id_t key, const AddrMeta& addr_meta, MemStoreReserveParam* param) {
  uint64_t hash = GetHash(key);
  auto* node = (AddrNode*)(hash * sizeof(AddrNode) + addr_ptr);

  // Find
  while (node) {
    for (auto& item : node->addr_meta) {
      if (!item.valid) {
        item = addr_meta;
        item.valid = 1;
        return &item;
      }
    }
    if (!node->next) break;
    node = node->next;
  }

  // Allocate
  // RDMA_LOG(INFO) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum/bucket: " << ITEM_NUM_PER_NODE;
  assert((uint64_t)param->mem_store_reserve + param->mem_store_reserve_offset <= (uint64_t)param->mem_store_end);
  auto* new_node = (AddrNode*)(param->mem_store_reserve + param->mem_store_reserve_offset);
  param->mem_store_reserve_offset += sizeof(AddrNode);
  memset(new_node, 0, sizeof(AddrNode));
  new_node->addr_meta[0] = addr_meta;
  new_node->addr_meta[0].valid = 1;
  new_node->next = nullptr;
  node->next = new_node;
  node_num++;
  return &(new_node->addr_meta[0]);
}

ALWAYS_INLINE
AddrMeta* AddrStore::LocalPut(page_id_t key, const AddrMeta& addr_meta, MemStoreReserveParam* param) {
  DataItem* res;
  if ((res = LocalGet(key)) != nullptr) {
    // KV pair has already exist, then update
    *res = data_item;
    return res;
  }
  // Insert
  return LocalInsert(key, data_item, param);
}

ALWAYS_INLINE
bool AddrStore::LocalDelete(page_id_t key) {
  uint64_t hash = GetHash(key);
  auto* node = (HashNode*)(hash * sizeof(HashNode) + data_ptr);
  for (auto& data_item : node->data_items) {
    if (data_item.valid && data_item.key == key) {
      data_item.valid = 0;
      return true;
    }
  }
  node = node->next;
  while (node) {
    for (auto& data_item : node->data_items) {
      if (data_item.valid && data_item.key == key) {
        data_item.valid = 0;
        return true;
      }
    }
    node = node->next;
  }
  return false;  // Failed to find one to be deleted
}
*/