// Author: Hongyao Zhao
// Copyright (c) 2023

#include "dtx/dtx.h"
#include "worker/global.h"

//! 本地生成读写集，本地执行并发控制
bool DTX::ExeLocalRO(coro_yield_t& yield) {
  // You can read from primary or backup
  std::vector<DirectRead> pending_direct_ro;
  // std::vector<HashRead> pending_hash_ro;

  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对只读操作加锁
    bool success = localdata->LockShared();
    if (!success) return false;
    // !将只读操作存入操作集
    item.read_version_dtx = localdata->GetTailVersion()->txn;
  }

  return true;
}

bool DTX::ExeLocalRW(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对只读操作加锁
    bool success = localdata->LockShared();
    if (!success) return false;
    // !对当前读取到的最新版本打标记
    item.read_version_dtx = localdata->GetTailVersion()->txn;
  }

  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto localdata = local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // !加锁
    bool success = localdata->LockExclusive();
    if (!success) return false;
    // !创建新版本
    localdata->CreateNewVersion(this);
  }

#if COMMIT_TOGETHER
  ParallelUndoLog();
#endif

  return true;
}

bool DTX::LocalCommit(coro_yield_t& yield, BenchDTX* dtx_with_bench) {
  bool res = true;
  //! 1.将生成好的读写集，塞到batch中
  local_batch_store.InsertTxn(dtx_with_bench);

  //! 2.本地释放锁
  for (auto& item : read_only_set) {
    auto localdata = local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    localdata->UnlockShared();
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    auto localdata = local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    localdata->UnlockExclusive();
  }
  
  return res;
}

bool DTX::ReExeLocalRO(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !遍历版本，获取到需要的值
    auto* fetched_item = localdata->GetDTXVersion(item.read_version_dtx);
    auto* it = item.item_ptr.get();
    auto* ver = item.version_ptr.get();
    it = fetched_item->value.get();
    ver = fetched_item;
    // 这里先假设了读取到的是对的
  }
  
  return true;
}

bool DTX::ReExeLocalRW(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !遍历版本，获取到需要的值
    auto* fetched_item = localdata->GetDTXVersion(item.read_version_dtx);
    auto* it = item.item_ptr.get();
    auto* ver = item.version_ptr.get();
    it = fetched_item->value.get();
    ver = fetched_item;
    // 这里先假设了读取到的是对的
  }

  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto localdata = local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // !获取自己写入的版本
    auto* fetched_item = localdata->GetDTXVersionWithDataItem(this);
    auto* it = read_write_set[i].item_ptr.get();
    auto* ver = read_write_set[i].version_ptr.get();
    it = fetched_item->value.get();
    ver = fetched_item;
  }
  return true;
  // !接下来得进行tpcc等负载计算了
}

void DTX::ParallelUndoLog() {
  // Write the old data from read write set
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be recorded
      log_size += DataItemSize;
    }
  }
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);

  offset_t cur = 0;
  *((tx_id_t*)(written_log_buf + cur)) = tx_id;
  cur += sizeof(tx_id);
  *((t_id_t*)(written_log_buf + cur)) = t_id;
  cur += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      memcpy(written_log_buf + cur, (char*)(set_it.item_ptr.get()), DataItemSize);
      cur += DataItemSize;
      set_it.is_logged = true;
    }
  }

  // Write undo logs to all memory nodes
  for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
    offset_t log_offset = thread_remote_log_offset_alloc->GetNextLogOffset(i, log_size);
    RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
    coro_sched->RDMALog(coro_id, tx_id, qp, written_log_buf, log_offset, log_size);
  }
}

void DTX::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *((lock_t*)unlock_buf) = 0;
  for (auto& index : locked_rw_set) {
    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf, sizeof(lock_t), it->GetRemoteLockAddr(), 0);
    if (rc != SUCC) {
      RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " unlock fails during abortion";
    }
  }
  tx_status = TXStatus::TX_ABORT;
}