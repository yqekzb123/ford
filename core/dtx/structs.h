// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include "memstore/hash_store.h"
#include "rlib/rdma_ctrl.hpp"

extern node_id_t g_machine_id;
extern node_id_t g_machine_num;

enum DTX_SYS : int {
  FaRM = 0,
  DrTMH = 1,
  FORD = 2,
  LOCAL = 3, // FORD with localized metadata including locks and versions
  BASELINE = 4,
  OUR = 5
};

enum TXStatus : int {
  TX_INIT = 0,  // Transaction initialization
  TX_EXE,       // Transaction execution, read only
  TX_LOCK,      // Transaction execution, read+lock
  TX_VAL,       // Transaction validate
  TX_COMMIT,    // Commit primary and backups
  TX_ABORT      // Aborted transaction
};

enum ValStatus : int {
  RDMA_ERROR = -1,  // Validation network error
  NO_NEED_VAL = 0,  // Do not need validation, i.e., the coroutine does not need to yield CPU
  NEED_VAL = 1,     // Need validation, i.e., the coroutine needs to yield CPU
  MUST_ABORT = 2    // The data version must be changed and hence no validation is needed
};

// Following are stuctures for maintaining coroutine's state, similar to context switch
class DTX;
struct LVersion{
    // VersionType type; 
    // void* txn; //实际上是DTX结构
    DTX *txn; // 标记是哪个事务写的
    tx_id_t tx_id;  //标记事务号
    LVersion* next;
    DataItem* value; // 实际的值，可以是空
    bool has_value;

    LVersion() {
        next = nullptr;
        value = nullptr;
        has_value = false;
    }

    void SetVersionDTX(DTX *t, tx_id_t tid) {
        txn = t;
        tx_id = tid;
    }

    void SetDataItem(DataItemPtr data) {
        value = new DataItem();
        memcpy(value, data.get(), sizeof(DataItem));
        has_value = true;
    }

    void CopyDataItemToNext(){
        assert(next != nullptr);
        assert(value != nullptr);
        // std::shared_ptr<DataItem> p(new DataItem());
        next->value = new DataItem();
        memcpy(next->value, value, sizeof(DataItem));
    }
};
using LVersionPtr = std::shared_ptr<LVersion>;

struct DataSetItem {
  DataSetItem(DataItemPtr item) {
    item_ptr = std::move(item);
    is_fetched = false;
    is_logged = false;
    read_which_node = -1;
    bkt_idx = -1;
  }
  DataSetItem(DataItemPtr item, LVersionPtr version) {
    item_ptr = std::move(item);
    version_ptr = std::move(version);
    is_fetched = false;
    is_logged = false;
    read_which_node = -1;
    bkt_idx = -1;
  }
  DataItemPtr item_ptr;
  LVersionPtr version_ptr; // ! batch执行会用
  bool is_fetched;
  bool is_logged;
  node_id_t read_which_node;  // From which node this data item is read. This is a node id, e.g., 0, 1, 2...
  int64_t bkt_idx; // The bkt idx of local lock table
 
  // 新内容
  DTX* read_version_dtx;
  // void* read_version_dtx; // 对于只读操作来说，需要读取哪个事务写的版本，实际上是DTX结构
};

struct OldVersionForInsert {
  table_id_t table_id;
  itemkey_t key;
  version_t version;
};

struct LockAddr {
  node_id_t node_id;
  uint64_t lock_addr;
};

// For coroutines
struct DirectRead {
  RCQP* qp;
  DataSetItem* item;
  char* buf;
  node_id_t remote_node;
};

struct HashRead {
  RCQP* qp;
  DataSetItem* item;
  char* buf;
  node_id_t remote_node;
  const HashMeta meta;
};

struct InvisibleRead {
  RCQP* qp;
  char* buf;
  uint64_t off;
};

struct CasRead {
  RCQP* qp;
  DataSetItem* item;
  char* cas_buf;
  char* data_buf;
  node_id_t primary_node_id;
};

struct InsertOffRead {
  RCQP* qp;
  DataSetItem* item;
  char* buf;
  node_id_t remote_node;
  const HashMeta meta;
  offset_t node_off;
};

struct ValidateRead {
  RCQP* qp;
  DataSetItem* item;
  char* cas_buf;
  char* version_buf;
  bool has_lock_in_validate;
};

struct Lock {
  RCQP* qp;
  DataSetItem* item;
  char* cas_buf;
  uint64_t lock_off;
};

struct Unlock {
  char* cas_buf;
};

struct Version {
  DataSetItem* item;
  char* version_buf;
};

struct CommitWrite {
  node_id_t node_id;
  uint64_t lock_off;
};

struct NodeOffset {
    node_id_t nodeId;
    offset_t offset;
    // 重载==函数
    bool operator==(const NodeOffset& other) const {
        return nodeId == other.nodeId && offset == other.offset;
    }
};

namespace std {
    template <>
    struct hash<NodeOffset> {
        std::size_t operator()(const NodeOffset& s) const noexcept {
            std::size_t h1 = std::hash<node_id_t>{}(s.nodeId);
            std::size_t h2 = std::hash<offset_t>{}(s.offset);
            return h1 ^ (h2 << 1);
        }
    };
}