// Author: Ming Zhang
// Copyright (c) 2022

#include "connection/qp_manager.h"


void QPManager::BuildDataNodeQPConnection(MetaManager* meta_man) {
  for (const auto& remote_node : meta_man->remote_data_nodes) {
    // Note that each remote machine has one MemStore mr and one Log mr
    MemoryAttr remote_data_node_mr = meta_man->GetaDataNodeMR(remote_node.node_id);

    // Build QPs with one remote machine (this machine can be a primary or a backup)
    // Create the thread local queue pair
    MemoryAttr local_mr = meta_man->global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
    RCQP* data_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid),
                                                             meta_man->opened_rnic,
                                                             &local_mr);

    // Queue pair connection, exchange queue pair info via TCP
    ConnStatus rc;
    do {
      rc = data_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        data_qp->bind_remote_mr(remote_data_node_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
        data_qps[remote_node.node_id] = data_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Data QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);
  }
}

void QPManager::BuildPageNodeQPConnection(MetaManager* meta_man) {
  for (const auto& remote_node : meta_man->remote_pagetable_nodes) {
    // Note that each remote machine has one MemStore mr and one Log mr
    MemoryAttr remote_page_table_mr = meta_man->GetPageTableMR(remote_node.node_id);
    MemoryAttr remote_page_ringbuffer_mr = meta_man->GetPageTableRingbufferMR(remote_node.node_id);

    // Build QPs with one remote machine (this machine can be a primary or a backup)
    // Create the thread local queue pair
    MemoryAttr local_mr = meta_man->global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
    RCQP* page_table_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid * 2),
                                                             meta_man->opened_rnic,
                                                             &local_mr);

    RCQP* page_ringbuffer_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid * 2 + 1),
                                                            meta_man->opened_rnic,
                                                            &local_mr);

    // Queue pair connection, exchange queue pair info via TCP
    ConnStatus rc;
    do {
      rc = page_table_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        page_table_qp->bind_remote_mr(remote_page_table_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
        page_table_qps[remote_node.node_id] = page_table_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Data QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);

    do {
      rc = page_ringbuffer_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        page_ringbuffer_qp->bind_remote_mr(remote_page_ringbuffer_mr);  // Bind the log mr as the default remote mr for convenient parameter passing
        page_ringbuffer_qps[remote_node.node_id] = page_ringbuffer_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Log QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);
  }
}

void QPManager::BuildLockNodeQPConnection(MetaManager* meta_man) {
  for (const auto& remote_node : meta_man->remote_locktable_nodes) {
    // Note that each remote machine has one MemStore mr and one Log mr
    MemoryAttr remote_locktable_node_mr = meta_man->GetLockTableMR(remote_node.node_id);

    // Build QPs with one remote machine (this machine can be a primary or a backup)
    // Create the thread local queue pair
    MemoryAttr local_mr = meta_man->global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
    RCQP* locktable_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid),
                                                             meta_man->opened_rnic,
                                                             &local_mr);

    // Queue pair connection, exchange queue pair info via TCP
    ConnStatus rc;
    do {
      rc = locktable_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        locktable_qp->bind_remote_mr(remote_locktable_node_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
        lock_table_qps[remote_node.node_id] = locktable_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Data QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);
  }
}

void QPManager::BuildIndexNodeQPConnection(MetaManager* meta_man) {
  for (const auto& remote_node : meta_man->remote_hashindex_nodes) {
    // Note that each remote machine has one MemStore mr and one Log mr
    MemoryAttr remote_data_node_mr = meta_man->GetHashIndexMR(remote_node.node_id);

    // Build QPs with one remote machine (this machine can be a primary or a backup)
    // Create the thread local queue pair
    MemoryAttr local_mr = meta_man->global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
    RCQP* index_qp = meta_man->global_rdma_ctrl->create_rc_qp(create_rc_idx(remote_node.node_id, (int)global_tid),
                                                             meta_man->opened_rnic,
                                                             &local_mr);

    // Queue pair connection, exchange queue pair info via TCP
    ConnStatus rc;
    do {
      rc = index_qp->connect(remote_node.ip, remote_node.port);
      if (rc == SUCC) {
        index_qp->bind_remote_mr(remote_data_node_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
        index_qps[remote_node.node_id] = index_qp;
        // RDMA_LOG(INFO) << "Thread " << global_tid << ": Data QP connected! with remote node: " << remote_node.node_id << " ip: " << remote_node.ip;
      }
      usleep(2000);
    } while (rc != SUCC);
  }
}

void QPManager::BuildQPConnection(MetaManager* meta_man) {
  BuildDataNodeQPConnection(meta_man);
  BuildPageNodeQPConnection(meta_man);
  BuildLockNodeQPConnection(meta_man);
  BuildIndexNodeQPConnection(meta_man);
  return ;
}