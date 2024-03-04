// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <atomic>
#include <unordered_map>
#include <string>

#include "base/common.h"
#include "memstore/hash_store.h"
#include "memstore/data_store.h"
#include "memstore/hash_index_store.h"
#include "memstore/lock_table_store.h"
#include "memstore/page_table.h"
#include "rlib/rdma_ctrl.hpp"
// #include "record/rm_file_handle.h"

using namespace rdmaio;

// const size_t LOG_BUFFER_SIZE = 1024 * 1024 * 512;

// 这个结构体作用是在计算层维护Table的元信息, 用于计算层和内存层交互
// 这个结构体不同于RmFileHandle，RmFileHandle是一个页，存放了一些固定的信息和动态的元信息比如next_free_page
// 这里维护一个固定的元信息，以减少频繁去FileHandle中读取的开销
struct TableMeta {
  int record_size_;
  int num_records_per_page_;
  int bitmap_size_;
};

struct RemoteNode {
  node_id_t node_id;
  std::string ip;
  int port;
};
// 感觉是用来和内存层交互的？
class MetaManager {
 public:
  MetaManager(std::string bench_name);

  node_id_t GetRemoteDataStoreMeta(std::string& remote_ip, int remote_port);

  node_id_t GetRemotePageTableStoreMeta(std::string& remote_ip, int remote_port);

  node_id_t GetRemoteLockTableStoreMeta(std::string& remote_ip, int remote_port);

  node_id_t GetRemoteHashIndexStoreMeta(std::string& remote_ip, int remote_port);

  void GetRemoteDataNodeMR(const RemoteNode& node);

  void GetRemotePageNodeMR(const RemoteNode& node);

  void GetRemoteLockNodeMR(const RemoteNode& node);

  void GetRemoteIndexNodeMR(const RemoteNode& node);

  // get global_rdma_ctrl
  ALWAYS_INLINE
  RdmaCtrlPtr GetGlobalRdmaCtrl() {
    return global_rdma_ctrl;
  }

  // get rnic
  ALWAYS_INLINE
  RNicHandler* GetOpenedRnic() {
    return opened_rnic;
  }

  /*** Memory Store Metadata ***/
  // ALWAYS_INLINE
  // const HashMeta& GetPrimaryHashMetaWithTableID(const table_id_t table_id) const {
  //   auto search = primary_hash_metas.find(table_id);
  //   assert(search != primary_hash_metas.end());
  //   return search->second;
  // }

  // ALWAYS_INLINE
  // const std::vector<HashMeta>* GetBackupHashMetasWithTableID(const table_id_t table_id) const {
  //   // if (backup_hash_metas.empty()) {
  //   //   return nullptr;
  //   // }
  //   // auto search = backup_hash_metas.find(table_id);
  //   // assert(search != backup_hash_metas.end());
  //   // return &(search->second);
  //   return &(backup_hash_metas[table_id]);
  // }

  /*** Node ID Metadata ***/
  ALWAYS_INLINE
  node_id_t GetPrimaryNodeID(const table_id_t table_id) const {
    auto search = primary_table_nodes.find(table_id);
    assert(search != primary_table_nodes.end());
    return search->second;
  }

  /*** Page Addr Node ID Metadata ***/
  ALWAYS_INLINE
  node_id_t GetPageAddrNodeID(const page_id_t id) const {
    RemoteNode node = remote_pagetable_nodes[0]; 
    return node.node_id;
  }

  ALWAYS_INLINE
  const std::vector<node_id_t>* GetBackupNodeID(const table_id_t table_id) {
    // if (backup_table_nodes.empty()) {
    //   return nullptr;
    // }
    // auto search = backup_table_nodes.find(table_id);
    // assert(search != backup_table_nodes.end());
    // return &(search->second);
    return &(backup_table_nodes[table_id]);
  }

  // /*** RDMA Memory Region Metadata ***/
  ALWAYS_INLINE
  const MemoryAttr& GetaDataNodeMR(const node_id_t node_id) const {
    auto mrsearch = remote_data_mrs.find(node_id);
    assert(mrsearch != remote_data_mrs.end());
    return mrsearch->second;
  }

  ALWAYS_INLINE
  const MemoryAttr& GetPageTableMR(const node_id_t node_id) const {
    auto mrsearch = remote_page_table_mrs.find(node_id);
    assert(mrsearch != remote_page_table_mrs.end());
    return mrsearch->second;
  }

  ALWAYS_INLINE
  const MemoryAttr& GetPageTableRingbufferMR(const node_id_t node_id) const {
    auto mrsearch = remote_page_table_ringbuffer_mrs.find(node_id);
    assert(mrsearch != remote_page_table_ringbuffer_mrs.end());
    return mrsearch->second;
  }

  ALWAYS_INLINE
  const MemoryAttr& GetLockTableMR(const node_id_t node_id) const {
    auto mrsearch = remote_locktable_mrs.find(node_id);
    assert(mrsearch != remote_locktable_mrs.end());
    return mrsearch->second;
  }

  ALWAYS_INLINE
  const MemoryAttr& GetHashIndexMR(const node_id_t node_id) const {
    auto mrsearch = remote_hashindex_mrs.find(node_id);
    assert(mrsearch != remote_hashindex_mrs.end());
    return mrsearch->second;
  }

  /*** Hash Index Node id ***/
  ALWAYS_INLINE
  node_id_t GetHashIndexNode(const table_id_t table_id) const {
    return hash_index_nodes[table_id];
  }

  /*** Hash Index Meta ***/
  ALWAYS_INLINE
  const IndexMeta& GetHashIndexMeta(const table_id_t table_id) const {
    return hash_index_meta[table_id];
  }

  ALWAYS_INLINE
  const offset_t GetHashIndexExpandBase(const table_id_t table_id) const {
    auto meta = GetHashIndexMeta(table_id);
    return meta.expand_base_off;
  }

  /*** Lock Table Node id ***/
  ALWAYS_INLINE
  node_id_t GetLockTableNode(const table_id_t table_id) const {
    int index = table_id % remote_locktable_nodes.size();
    return remote_locktable_nodes[index].node_id;
  }

  /*** Lock Table Meta ***/
  ALWAYS_INLINE
  const LockTableMeta& GetLockTableMeta(const table_id_t table_id) const {
    // 这里的逻辑需要修正， 首先根据table_id找到node_id，然后根据node_id找到meta
    node_id_t node_id = GetLockTableNode(table_id);
    auto search = lock_table_meta.find(node_id);
    assert(search != lock_table_meta.end());
    return search->second;
  }

  ALWAYS_INLINE
  const offset_t GetLockTableExpandBase(const table_id_t table_id) const {
    LockTableMeta meta = GetLockTableMeta(table_id);
    return meta.expand_base_off;
  }

  /*** Page Table Meta ***/
  ALWAYS_INLINE
  const std::vector<node_id_t>& GetPageTableNode() const {
    return page_table_nodes;
  }

  /*** Page Table Meta ***/
  ALWAYS_INLINE
  const PageTableMeta& GetPageTableMeta(const node_id_t node_id) const {
    return page_table_meta[node_id];
  }
  /*** Page Table Meta ***/
  const offset_t GetPageTableExpandBase(const node_id_t node_id) const {
    auto meta = GetPageTableMeta(node_id);
    return meta.expand_base_off;
  }
  const offset_t GetFreeRingBase(const node_id_t node_id) const {
    auto meta = GetPageTableMeta(node_id);
    return meta.free_ring_base_off;
  }
  const offset_t GetFreeRingHead(const node_id_t node_id) const {
    auto meta = GetPageTableMeta(node_id);
    return meta.free_ring_head_off;
  }
  const offset_t GetFreeRingTail(const node_id_t node_id) const {
    auto meta = GetPageTableMeta(node_id);
    return meta.free_ring_tail_off;
  }
  const offset_t GetFreeRingCnt(const node_id_t node_id) const {
    auto meta = GetPageTableMeta(node_id);
    return meta.free_ring_cnt_off;
  }

  /*** Table Meta ***/
  const std::string GetTableName(const table_id_t table_id) const {
    return table_name_map.at(table_id);
  }
  const TableMeta& GetTableMeta(const table_id_t table_id) const {
    return table_meta_map.at(table_id);
  }

  const offset_t GetDataOff(const node_id_t node_id) const {
    auto search = data_metas.find(node_id);
    assert(search != data_metas.end());
    return search->second.base_off;
  }
  
 private:
  // std::unordered_map<table_id_t, HashMeta> primary_hash_metas;

  // std::unordered_map<table_id_t, std::vector<HashMeta>> backup_hash_metas;

  // std::vector<HashMeta> backup_hash_metas[MAX_DB_TABLE_NUM];

  std::unordered_map<table_id_t, node_id_t> primary_table_nodes;

  // std::unordered_map<table_id_t, std::vector<node_id_t>> backup_table_nodes;

  std::vector<node_id_t> backup_table_nodes[MAX_DB_TABLE_NUM];

  std::unordered_map<node_id_t, MemoryAttr> remote_hash_mrs;
  std::unordered_map<node_id_t, MemoryAttr> remote_log_mrs;

  std::unordered_map<node_id_t, MemoryAttr> remote_data_mrs;
  std::unordered_map<node_id_t, MemoryAttr> remote_page_table_mrs;
  std::unordered_map<node_id_t, MemoryAttr> remote_page_table_ringbuffer_mrs;
  std::unordered_map<node_id_t, MemoryAttr> remote_locktable_mrs;
  std::unordered_map<node_id_t, MemoryAttr> remote_hashindex_mrs;

  IndexMeta hash_index_meta[MAX_DB_TABLE_NUM];
  node_id_t hash_index_nodes[MAX_DB_TABLE_NUM];

  std::unordered_map<node_id_t, LockTableMeta> lock_table_meta;
  // std::unordered_map<table_id_t, node_id_t> lock_table_nodes;
  // std::unordered_map<node_id_t, offset_t> lock_node_expanded_base_off;

  PageTableMeta page_table_meta[MAX_REMOTE_NODE_NUM];
  std::vector<node_id_t> page_table_nodes;

  std::vector<node_id_t> data_nodes;
  std::unordered_map<node_id_t, DataStoreMeta> data_metas;
  // std::unordered_map<node_id_t, offset_t> data_base_off;

  std::unordered_map<table_id_t, std::string> table_name_map;
  std::unordered_map<table_id_t, TableMeta> table_meta_map;

  std::unordered_map<node_id_t, offset_t> data_off;

 public:
  node_id_t local_machine_id;

 public:
  // Used by QP manager and RDMA Region
  RdmaCtrlPtr global_rdma_ctrl;

  std::vector<RemoteNode> remote_data_nodes;
  std::vector<RemoteNode> remote_pagetable_nodes;
  std::vector<RemoteNode> remote_locktable_nodes;
  std::vector<RemoteNode> remote_hashindex_nodes;
  
  RNicHandler* opened_rnic;

  // Below are some parameteres from json file
  int64_t txn_system;
};
