// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <brpc/channel.h>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "base/common.h"
#include "cache/addr_cache.h"
#include "cache/lock_status.h"
#include "cache/version_status.h"
#include "cache/index_cache.h"
#include "cache/page_table_cache.h"
#include "connection/meta_manager.h"
#include "connection/qp_manager.h"
#include "dtx/doorbell.h"
#include "dtx/structs.h"
#include "memstore/hash_store.h"
#include "memstore/hash_index_store.h"
#include "memstore/lock_table_store.h"
#include "memstore/page_table.h"
#include "util/debug.h"
#include "util/hash.h"
#include "util/json_config.h"
#include "bench_dtx.h"
#include "log/log_record.h"
#include "txn/batch_txn.h"

// for buffer pool fetch page
enum class FetchPageType {
  kReadPage,
  kInsertRecord,
  kDeleteRecord,
  kUpdateRecord
};

/* One-sided RDMA-enabled distributed transaction processing */
class DTX {
 public:
  /************ Interfaces for applications ************/
  void TxBegin(tx_id_t txid);

  void AddToReadOnlySet(DataItemPtr item);

  void AddToReadWriteSet(DataItemPtr item);

  void ClearReadOnlySet();

  void ClearReadWriteSet();

  void AddToReadOnlySet(DataItemPtr item, LVersionPtr version);

  void AddToReadWriteSet(DataItemPtr item, LVersionPtr version);

  bool TxLocalExe(coro_yield_t& yield, bool fail_abort = true);

  bool TxLocalCommit(coro_yield_t& yield, BenchDTX* dtx_with_bench);

  bool UnLockLocalRW();

  bool TxLocalAbort(coro_yield_t& yield);

  bool TxExe(coro_yield_t& yield, bool fail_abort = true);

  bool TxCommit(coro_yield_t& yield);

  void TxAbort(coro_yield_t& yield);

  bool TxReadWriteTxnExe(coro_yield_t& yield, bool fail_abort = true); //一写多读场景下，执行写事务方法

  bool TxReadOnlyTxnExe(coro_yield_t& yield, bool fail_abort = true); //一写多读场景下，执行只读事务方法

  bool TxReadWriteTxnCommit(coro_yield_t& yield); 

  bool TxReadOnlyTxnCommit(); 

  bool TxReadWriteAbort();

  bool TxReadOnlyAbort();
  /*****************************************************/

 public:
  void TxAbortReadOnly(coro_yield_t& yield);

  void TxAbortReadWrite(coro_yield_t& yield);

  void RemoveLastROItem();

 public:
  DTX(MetaManager* meta_man,
      QPManager* qp_man,
      VersionCache* status,
      LockCache* lock_table,
      t_id_t tid,
      coro_id_t coroid,
      CoroutineScheduler* sched,
      RDMABufferAllocator* rdma_buffer_allocator,
      LogOffsetAllocator* log_offset_allocator,
      AddrCache* addr_buf,
      IndexCache* index_cache,
      PageTableCache* page_table_cache,
      std::list<PageAddress>* free_page_list, 
      std::mutex* free_page_list_mutex,
      brpc::Channel* data_channel,
      brpc::Channel* log_channel);
  ~DTX() {
    Clean();
  }

 public:
  size_t GetAddrCacheSize() {
    return addr_cache->TotalAddrSize();
  }

  bool LockRemoteRO(coro_yield_t& yield);  // 基线方法，对所有操作加锁
  bool LockRemoteRW(coro_yield_t& yield);  // 基线方法，对所有操作加锁
  bool ReadRemote(coro_yield_t& yield);  // 基线方法，读数据
  bool WriteRemote(coro_yield_t& yield);  // 基线方法，将写好的数据刷下去
  void Unpin(coro_yield_t& yield);  // 基线方法，将写好的数据刷下去

  bool LockLocalRO(coro_yield_t& yield);  // 在本地对只读操作加锁
  bool LockLocalRW(coro_yield_t& yield);  // 在本地对读写操作加锁

  bool ExeLocalRO(coro_yield_t& yield);  // 在本地执行只读操作
  bool ExeLocalRW(coro_yield_t& yield);  // 在本地执行读写操作

  bool LocalValidate(coro_yield_t& yield);  //本地验证/加锁之类的
  bool LocalCommit(coro_yield_t& yield, BenchDTX* dtx_with_bench);  //本地提交
  bool LocalAbort(coro_yield_t& yield);  //本地提交

  // batch操作完后，各个事务重新计算值，重新提交
  bool ReExeLocalRO(coro_yield_t& yield);  // 在本地执行只读操作
  bool ReExeLocalRW(coro_yield_t& yield);  // 在本地执行读写操作

  // bool ExeBatchRW(coro_yield_t& yield);  // 批次在远程读取数据
  // bool BatchValidate(coro_yield_t& yield);  //读回数据后，本地验证和重新计算数据
  
  // 发送日志到存储层
  BatchTxnLog batch_txn_log;
  brpc::Channel* storage_data_channel;
  brpc::Channel* storage_log_channel;
  
  void SendLogToStoragePool(uint64_t bid, brpc::CallId* cid); // use for rpc
  void SendLogToStoragePool(uint64_t bid); // use for rdma

 private:
  void Abort();

  void Clean();  // Clean data sets after commit/abort
 public:
  // for hash index
  std::vector<Rid> GetHashIndexBatch(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key);
  std::vector<Rid> GetHashIndex(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key);

  bool InsertHashIndex(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key, std::vector<Rid> rids);

  bool DeleteHashIndex(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key);

  // for lock table
  bool LockSharedOnTable(coro_yield_t& yield, std::vector<table_id_t> table_id);
  
  bool LockExclusiveOnTable(coro_yield_t& yield, std::vector<table_id_t> table_id);

  bool LockSharedOnRecord(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key);

  bool LockExclusiveOnRecord(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key);

  bool LockSharedOnRange(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key);

  bool LockExclusiveOnRange(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key);

  bool UnlockShared(coro_yield_t& yield);

  bool UnlockExclusive(coro_yield_t& yield);

  // not use any more
  // bool UnlockShared(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs);
  // bool UnlockExclusive(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs);

  struct UnpinPageArgs{
    // 写回数据的地址
    PageAddress page_addr;
    FetchPageType type;
    char* page;
    int offset;
    int size;
  };
  std::vector<char*> FetchPage(coro_yield_t &yield, batch_id_t request_batch_id);
  bool UnpinPage(coro_yield_t &yield, std::vector<PageId>& ids,  std::vector<FetchPageType>& types);
  
  std::vector<DataItemPtr> FetchTuple(coro_yield_t &yield, std::vector<table_id_t> table_id, std::vector<Rid> rids, std::vector<FetchPageType> types, batch_id_t request_batch_id);

  bool WriteTuple(coro_yield_t &yield, std::vector<table_id_t> &table_id, std::vector<Rid> &rids, 
    std::vector<int>& rid_map_pageid_idx, std::vector<FetchPageType> &types, std::vector<DataItemPtr> &data, batch_id_t request_batch_id);

 private:
  // 用来记录每次要批获取hash node latch的offset
  std::unordered_set<NodeOffset> pending_hash_node_latch_offs;

  std::vector<NodeOffset> total_hash_node_offs_vec;
  std::vector<int> pending_hash_node_latch_idx;
 public:

  // for page table
  PageAddress GetFreePageSlot();
  PageAddress InsertPageTableIntoHashNodeList(std::vector<char*>& local_hash_nodes_vec, 
        PageId page_id, bool is_write, int last_idx, int all_page_id_idx,
        std::unordered_map<int, int>& hold_latch_to_previouse_node_off);

  std::vector<PageAddress> GetPageAddrOrAddIntoPageTable(coro_yield_t& yield, std::vector<bool> is_write, 
        std::vector<bool>& need_fetch_from_disk, std::vector<bool>& now_valid, std::vector<int>& pending_map_all_index); 
        //pengding_map_all_index是记录获取rid的全局地址

  void UnpinPageTable(coro_yield_t& yield, std::vector<PageId> page_ids, std::vector<bool> is_write);

  // for private function for LockManager, 实际执行批量加锁的函数
  std::vector<LockDataId> LockShared(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs);
  std::vector<LockDataId> LockSharedBatch(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs);
  std::vector<LockDataId> LockExclusive(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs);
  std::vector<LockDataId> LockExclusiveBatch(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs);
  
  // for rwlatch in hash node
  enum class QPType {
    kPageTable,
    kLockTable,
    kHashIndex
  };
  
  bool InsertSharedLockIntoHashNodeList(std::vector<char*>& local_hash_nodes_vec, 
        LockDataId lockdataid, int last_idx, offset_t expand_base_off,
        std::unordered_map<int, int>& hold_latch_to_previouse_node_off);
  bool InsertExclusiveLockIntoHashNodeList(std::vector<char*>& local_hash_nodes_vec, 
        LockDataId lockdataid, int last_idx, offset_t expand_base_off,
        std::unordered_map<int, int>& hold_latch_to_previouse_node_off);
  std::vector<int> ShardLockHashNode(coro_yield_t& yield, QPType qptype, std::vector<char*>& local_hash_nodes, std::vector<char*>& faa_bufs);
  void ShardUnLockHashNode(NodeOffset node_off, QPType qptype);
  // Exclusive lock hash node 是一个关键路径，因此需要切换到其他协程，也需要记录下来哪些桶已经上锁成功以及RDMA操作返回值在本机的地址
  std::vector<NodeOffset> ExclusiveLockHashNode(coro_yield_t& yield, QPType qptype, std::unordered_map<NodeOffset, char*>& local_hash_nodes, 
            std::unordered_map<NodeOffset, char*>& cas_bufs);
  std::vector<int> ExclusiveLockHashNode(coro_yield_t& yield, QPType qptype, std::vector<char*>& local_hash_nodes, 
            std::vector<char*>& cas_bufs);
  void ExclusiveUnlockHashNode_NoWrite(coro_yield_t& yield, NodeOffset node_off, QPType qptype);
  void ExclusiveUnlockHashNode_WithWrite(NodeOffset node_off, char* write_back_data, QPType qptype);
  void ExclusiveUnlockHashNode_WithWriteItems(NodeOffset node_off, char* write_back_item, offset_t item_offset, size_t size, QPType qptype);
  void ExclusiveUnlockHashNode_RemoteWriteItem(node_id_t node_id, offset_t item_offset, char* write_back_item, size_t size, QPType qptype);

  DataItemPtr GetDataItemFromPage(table_id_t table_id, char* data, Rid rid);

 public:
  tx_id_t tx_id;  // Transaction ID

  t_id_t t_id;  // Thread ID

  coro_id_t coro_id;  // Coroutine ID

  batch_id_t batch_id; // 

 public:
  // For statistics
  struct timespec tx_start_time;

  std::vector<uint64_t> lock_durations;  // us

  std::vector<uint64_t> invisible_durations;  // us

  std::vector<uint64_t> invisible_reread;  // times

  size_t hit_local_cache_times;

  size_t miss_local_cache_times;

  MetaManager* global_meta_man;  // Global metadata manager

  CoroutineScheduler* coro_sched;  // Thread local coroutine scheduler
  
 public:
  QPManager* thread_qp_man;  // Thread local qp connection manager. Each transaction thread has one

  RDMABufferAllocator* thread_rdma_buffer_alloc;  // Thread local RDMA buffer allocator

  LogOffsetAllocator* thread_remote_log_offset_alloc;  // Thread local remote log offset generator

  TXStatus tx_status;

  std::vector<DataSetItem> read_only_set;

  std::vector<DataSetItem> read_write_set;

  std::vector<size_t> not_eager_locked_rw_set;  // For eager logging

  std::vector<size_t> locked_rw_set;  // For release lock during abort

  AddrCache* addr_cache;
  IndexCache* index_cache;
  PageTableCache* page_table_cache;

  // For backup-enabled read. Which backup is selected (the backup index, not the backup's machine id)
  size_t select_backup;

  // For validate the version for insertion
  std::vector<OldVersionForInsert> old_version_for_insert;

  struct pair_hash {
    inline std::size_t operator()(const std::pair<node_id_t, offset_t>& v) const {
      return v.first * 31 + v.second;
    }
  };

  // Avoid inserting to the same slot in one transaction
  std::unordered_set<std::pair<node_id_t, offset_t>, pair_hash> inserted_pos;

  // Global <table, key> version table
  VersionCache* global_vcache;

  // Global <key, lock> lock table
  LockCache* global_lcache;

  std::list<PageAddress>* free_page_list;
  std::mutex* free_page_list_mutex;

  // std::unordered_set<LockDataId> hold_exclusive_lock_data_id;
  // std::unordered_set<LockDataId> hold_shared_lock_data_id;

  // only used for baseline
  std::vector<table_id_t> all_tableid;
  std::vector<itemkey_t> all_keyid;
  std::vector<Rid> all_rids;

  // use for both baseline and batch
  std::vector<PageId> all_page_ids; // 固定的
  std::vector<PageId> pending_read_all_page_ids; // 动态变化的
  std::vector<FetchPageType> all_types; // 固定的
  // 索引，记录每一个要访问的数据所在的all_page_ids的位置
  std::vector<int> rid_map_pageid_idx;
  // 与all_page_ids一一对应
  struct LockStatusItem {
    LockDataId lock_data_id;
    char* localaddr;
    NodeOffset node_off;
  };
  
  std::vector<std::pair<char*, NodeOffset>> page_table_item_localaddr_and_remote_offset;
  std::vector<std::pair<char*, PageAddress>> page_data_localaddr_and_remote_offset;
  std::vector<LockStatusItem> shared_lock_item_localaddr_and_remote_offset;
  std::vector<LockStatusItem> exclusive_lock_item_localaddr_and_remote_offset;
};

/*************************************************************
 ************************************************************
 *********** Implementations of interfaces in DTX ***********
 ************************************************************
 **************************************************************/

ALWAYS_INLINE
void DTX::TxBegin(tx_id_t txid) {
  Clean();  // Clean the last transaction states
  tx_id = txid;
}

ALWAYS_INLINE
void DTX::AddToReadOnlySet(DataItemPtr item) {
  DataSetItem data_set_item(item);
  // DataSetItem data_set_item{.item_ptr = std::move(item), .is_fetched = false, .is_logged = false, .read_which_node = -1, .bkt_idx = -1};
  read_only_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::AddToReadWriteSet(DataItemPtr item) {
  DataSetItem data_set_item(item);
  // DataSetItem data_set_item{.item_ptr = std::move(item), .is_fetched = false, .is_logged = false, .read_which_node = -1, .bkt_idx = -1};
  // printf("DTX.h:275\n");
  read_write_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::ClearReadOnlySet() {
  read_only_set.clear();
}

ALWAYS_INLINE
void DTX::ClearReadWriteSet() {
  read_write_set.clear();
}

ALWAYS_INLINE
void DTX::AddToReadOnlySet(DataItemPtr item, LVersionPtr version) {
  DataSetItem data_set_item(item,version);
  // DataSetItem data_set_item{.item_ptr = std::move(item), .version_ptr = std::move(version), .is_fetched = false, .is_logged = false, .read_which_node = -1, .bkt_idx = -1};
  read_only_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::AddToReadWriteSet(DataItemPtr item, LVersionPtr version) {
  DataSetItem data_set_item(item,version);
  // {.item_ptr = std::move(item), .version_ptr = std::move(version), .is_fetched = false, .is_logged = false, .read_which_node = -1, .bkt_idx = -1};
  read_write_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::TxAbortReadOnly(coro_yield_t& yield) {
  // Application actively aborts the tx
  // User abort tx in the middle time of tx exe
  assert(read_write_set.empty());
  TxAbort(yield);
  read_only_set.clear();
}

ALWAYS_INLINE
void DTX::TxAbortReadWrite(coro_yield_t& yield) { 
  TxAbort(yield);
  Abort(); 
}

ALWAYS_INLINE
void DTX::RemoveLastROItem() { read_only_set.pop_back(); }

ALWAYS_INLINE
void DTX::Clean() {
  read_only_set.clear();
  read_write_set.clear();
  not_eager_locked_rw_set.clear();
  locked_rw_set.clear();
  old_version_for_insert.clear();
  inserted_pos.clear();
}
