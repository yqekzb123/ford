// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include "connection/meta_manager.h"

// This QPManager builds qp connections (compute node <-> memory node) for each txn thread in each compute node
class QPManager {
 public:
  QPManager(t_id_t global_tid) : global_tid(global_tid) {}

  void BuildQPConnection(MetaManager* meta_man);

  void BuildDataNodeQPConnection(MetaManager* meta_man);

  void BuildPageNodeQPConnection(MetaManager* meta_man);

  void BuildLockNodeQPConnection(MetaManager* meta_man);

  void BuildIndexNodeQPConnection(MetaManager* meta_man);
  
  ALWAYS_INLINE
  RCQP* GetRemoteDataQPWithNodeID(const node_id_t node_id) const {
    return data_qps[node_id];
  }

  ALWAYS_INLINE
  void GetRemoteDataQPsWithNodeIDs(const std::vector<node_id_t>* node_ids, std::vector<RCQP*>& qps) {
    for (node_id_t node_id : *node_ids) {
      RCQP* qp = data_qps[node_id];
      if (qp) {
        qps.push_back(qp);
      }
    }
  }

  ALWAYS_INLINE
  RCQP* GetRemoteLogQPWithNodeID(const node_id_t node_id) const {
    return log_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemoteLockQPWithNodeID(const node_id_t node_id) const {
    return lock_table_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemotePageTableQPWithNodeID(const node_id_t node_id) const {
    return page_table_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemotePageRingbufferQPWithNodeID(const node_id_t node_id) const {
    return page_ringbuffer_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemoteIndexQPWithNodeID(const node_id_t node_id) const {
    return index_qps[node_id];
  }
  
  // *****************
  ALWAYS_INLINE
  auto GetLockQPPtrWithNodeID() const {
    return lock_table_qps;
  }

  ALWAYS_INLINE
  auto GetPageTableQPPtrWithNodeID() const {
    return page_table_qps;
  }

  ALWAYS_INLINE
  auto GetPageRingbufferQPPtrWithNodeID() const {
    return page_ringbuffer_qps;
  }

  ALWAYS_INLINE
  auto GetIndexQPPtrWithNodeID() const {
    return index_qps;
  }

 private:
  RCQP* data_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* log_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* page_table_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* page_ringbuffer_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* lock_table_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* index_qps[MAX_REMOTE_NODE_NUM]{nullptr};
  
  t_id_t global_tid;
};
