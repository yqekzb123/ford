// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <atomic>
#include <unordered_map>

#include "base/common.h"
#include "memstore/hash_store.h"
#include "memstore/hash_index_store.h"
#include "memstore/lock_table_store.h"
#include "memstore/page_table.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

// const size_t LOG_BUFFER_SIZE = 1024 * 1024 * 512;

struct RemoteNode {
  node_id_t node_id;
  std::string ip;
  int port;
};
// 感觉是用来和内存层交互的？
class MetaManager {
 public:
  MetaManager();

  node_id_t GetMemStoreMeta(std::string& remote_ip, int remote_port);

  node_id_t GetAddrStoreMeta(std::string& remote_ip, int remote_port);

  void GetMRMeta(const RemoteNode& node);

  /*** Memory Store Metadata ***/
  ALWAYS_INLINE
  const HashMeta& GetPrimaryHashMetaWithTableID(const table_id_t table_id) const {
    auto search = primary_hash_metas.find(table_id);
    assert(search != primary_hash_metas.end());
    return search->second;
  }

  ALWAYS_INLINE
  const std::vector<HashMeta>* GetBackupHashMetasWithTableID(const table_id_t table_id) const {
    // if (backup_hash_metas.empty()) {
    //   return nullptr;
    // }
    // auto search = backup_hash_metas.find(table_id);
    // assert(search != backup_hash_metas.end());
    // return &(search->second);
    return &(backup_hash_metas[table_id]);
  }

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
    RemoteNode node = page_addr_nodes[0]; 
    return node.node_id;
  }

  /*** Memory Store Metadata ***/
  ALWAYS_INLINE
  const uint64_t GetPageAddrTableBucketNumWithNodeID(const node_id_t node_id) const {
    auto search = page_addr_node_bucket_num.find(node_id);
    assert(search != page_addr_node_bucket_num.end());
    return search->second;
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

  ALWAYS_INLINE
  const MemoryAttr& GetRemoteLogMR(const node_id_t node_id) const {
    auto mrsearch = remote_log_mrs.find(node_id);
    assert(mrsearch != remote_log_mrs.end());
    return mrsearch->second;
  }

  /*** RDMA Memory Region Metadata ***/
  ALWAYS_INLINE
  const MemoryAttr& GetRemoteHashMR(const node_id_t node_id) const {
    auto mrsearch = remote_hash_mrs.find(node_id);
    assert(mrsearch != remote_hash_mrs.end());
    return mrsearch->second;
  }

  /*** Hash Index Node id ***/
  ALWAYS_INLINE
  node_id_t GetHashIndexNode(const table_id_t table_id) const {
    auto search = hash_index_nodes.find(table_id);
    assert(search != hash_index_nodes.end());
    return search->second;
  }

  /*** Hash Index Meta ***/
  ALWAYS_INLINE
  const IndexMeta& GetHashIndexMeta(const table_id_t table_id) const {
    auto search = hash_index_meta.find(table_id);
    assert(search != hash_index_meta.end());
    return search->second;
  }

  /*** Lock Table Node id ***/
  ALWAYS_INLINE
  node_id_t GetLockTableNode(const table_id_t table_id) const {
    auto search = lock_table_nodes.find(table_id);
    assert(search != lock_table_nodes.end());
    return search->second;
  }

  /*** Lock Table Meta ***/
  ALWAYS_INLINE
  const LockTableMeta& GetLockTableMeta(const table_id_t table_id) const {
    auto search = lock_table_meta.find(table_id);
    assert(search != lock_table_meta.end());
    return search->second;
  }

  /*** Lock Table Meta ***/
  ALWAYS_INLINE
  const std::vector<node_id_t>& GetAllMemNodes() const {
    return all_memstore_nodes;
  }

  /*** Page Table Meta ***/
  ALWAYS_INLINE
  const PageTableMeta& GetPageTableMeta(const node_id_t node_id) const {
    auto search = page_table_meta.find(node_id);
    assert(search != page_table_meta.end());
    return search->second;
  }

 private:
  std::unordered_map<table_id_t, HashMeta> primary_hash_metas;

  // std::unordered_map<table_id_t, std::vector<HashMeta>> backup_hash_metas;

  std::vector<HashMeta> backup_hash_metas[MAX_DB_TABLE_NUM];

  std::unordered_map<table_id_t, node_id_t> primary_table_nodes;

  // std::unordered_map<table_id_t, std::vector<node_id_t>> backup_table_nodes;

  std::vector<node_id_t> backup_table_nodes[MAX_DB_TABLE_NUM];

  std::unordered_map<node_id_t, MemoryAttr> remote_hash_mrs;

  std::unordered_map<node_id_t, MemoryAttr> remote_log_mrs;

  std::vector<RemoteNode> page_addr_nodes;
  std::unordered_map<node_id_t, uint64_t> page_addr_node_bucket_num;

  std::unordered_map<table_id_t, IndexMeta> hash_index_meta;
  std::unordered_map<table_id_t, node_id_t> hash_index_nodes;

  std::unordered_map<table_id_t, LockTableMeta> lock_table_meta;
  std::unordered_map<table_id_t, node_id_t> lock_table_nodes;

  std::vector<node_id_t> all_memstore_nodes;
  std::unordered_map<node_id_t, PageTableMeta> page_table_meta;

  node_id_t local_machine_id;

 public:
  // Used by QP manager and RDMA Region
  RdmaCtrlPtr global_rdma_ctrl;

  std::vector<RemoteNode> remote_nodes;

  RNicHandler* opened_rnic;

  // Below are some parameteres from json file
  int64_t txn_system;
};
