// Author: Huang chunyue
// Copyright (c) 2023

#pragma once

#include <cassert>

#include "base/page.h"
#include "base/common.h"
#include "memstore/data_item.h"
#include "memstore/mem_store.h"
#include "util/hash.h"

/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1 , RANGE = 2};

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
   public:

    LockDataId() {}

    /* 表级锁 */
    LockDataId(table_id_t table_id, LockDataType type) {
        assert(type == LockDataType::TABLE);
        table_id_ = table_id;
        type_ = type;
        itemkey_ = 0;
    }

    /* 范围锁或行锁 */
    LockDataId(table_id_t table_id, const itemkey_t &itemkey, LockDataType type) {
        assert(type == LockDataType::RANGE || type == LockDataType::RECORD);
        table_id_ = table_id;
        itemkey_ = itemkey;
        type_ = type;
    }

    inline int64_t Get() const {
        if (type_ == LockDataType::TABLE) {
            // table_id_
            return static_cast<int64_t>(table_id_);
        } else {
            // // table_id_, itemkey_
            return ((static_cast<int64_t>(type_)) << 62) | ((static_cast<int64_t>(table_id_)) << 48) |
                   itemkey_;
        }
    }

    bool operator==(const LockDataId &other) const {
        if (type_ != other.type_) return false;
        if (table_id_ != other.table_id_) return false;
        return itemkey_ == other.itemkey_;
    }

    table_id_t table_id_;
    itemkey_t itemkey_;
    LockDataType type_;
};

struct LockItem {
  LockDataId key;
  
  lock_t lock; // 读写锁
  
  uint8_t valid; // if the slot is empty, valid: exits value in the slot

  LockItem():valid(0) {}

  LockItem(LockDataId k, lock_t lock) :key(k), lock(lock), valid(1) {}
} Aligned8;

struct LockTableMeta {

  // Virtual address of the lock_table_ptr, used to calculate the distance
  // between some HashNodes with the table for traversing
  // the linked list
  uint64_t lock_table_ptr;

  // Offset of the lock_table_ptr, relative to the RDMA local_mr
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // Size of lock_table_ptr node
  size_t node_size;

  LockTableMeta(uint64_t lock_table_ptr,
           uint64_t bucket_num,
           size_t node_size,
           offset_t base_off) : lock_table_ptr(lock_table_ptr),
                                base_off(base_off),
                                bucket_num(bucket_num),
                                node_size(node_size) {}
  LockTableMeta() {}
} Aligned8;

// 计算每个哈希桶节点可以存放多少个rids
const int MAX_LOCKS_NUM_PER_NODE = (PAGE_SIZE - sizeof(page_id_t) - sizeof(lock_t) - sizeof(short*) * NEXT_NODE_COUNT) / (sizeof(LockItem) );

// A LockNode is a bucket
// 这里注意：sizeof(LockNode)是4064而非4096，这可能可以有效较少RNIC的哈希碰撞，ref sigmod23 guide，
// 若后续持久化，应该持久化4K整页
struct LockNode {
  lock_t latch; 
  // node id
  page_id_t page_id;

  LockItem lock_items[MAX_LOCKS_NUM_PER_NODE];

  short next_expand_node_id[NEXT_NODE_COUNT] = {-1};
  // LockNode* next;
} Aligned8;

class LockTableStore {
 public:
  LockTableStore(uint64_t bucket_num, MemStoreAllocParam* param)
      :base_off(0), bucket_num(bucket_num), lockitem_ptr(nullptr), node_num(0) {

    assert(bucket_num > 0);
    locktable_size = (bucket_num) * sizeof(LockNode);
    region_start_ptr = param->mem_region_start;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + locktable_size + sizeof(uint64_t) <= (uint64_t)param->mem_store_reserve);

    // fill_page_count是指针，指向额外分配页面的数量，安排已分配页面数量的位置，在地址索引空间的头部
    // 额外指开始分配了bucket_num数量的bucket_key, 如果bucket已满，则需要在保留空间中新建桶
    fill_page_count = (uint64_t*)&param->mem_store_start + param->mem_store_alloc_offset;
    *fill_page_count = 0;
    param->mem_store_alloc_offset += sizeof(uint64_t);

    // 安排哈希表的位置
    lockitem_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    param->mem_store_alloc_offset += locktable_size;

    base_off = (uint64_t)lockitem_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);

    assert(lockitem_ptr != nullptr);
    memset(lockitem_ptr, 0, locktable_size);
    
    bucket_array = (LockNode*)lockitem_ptr;
  }

  offset_t GetBaseOff() const {
    return base_off;
  }

  uint64_t GetLockTableMetaSize() const {
    return sizeof(LockTableMeta);
  }

  uint64_t GetBucketNum() const {
    return bucket_num;
  }

  char* GetAddrPtr() const {
    return lockitem_ptr;
  }

  uint64_t LockTableSize() const {
    return locktable_size;
  }

  uint64_t GetHash(LockDataId key) {
    return MurmurHash64A(key.Get(), 0xdeadbeef) % bucket_num;
  }

  // 锁表无需本地初始化

 private:
  // The offset in the RDMA region
  // Attention: the base_off is offset of fisrt lock table bucket
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to value in the table
  char* lockitem_ptr;
  LockNode* bucket_array;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t locktable_size;

  // Start of the lock table region address, for installing remote offset for lock table item
  char* region_start_ptr;

  // 锁表中，已经被分配的页面数量
  uint64_t* fill_page_count;

};
