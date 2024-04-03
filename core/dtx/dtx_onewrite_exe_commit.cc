// Author: Chunyue Huang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "worker/global.h"

bool DTX::TxReadWriteTxnExe(coro_yield_t& yield, bool fail_abort) {
  DEBUG_TIME("dtx_onewrite_exe_commit.cc:8, exe a new write txn %ld\n", tx_id);
  batch_id = tx_id;
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;

  // 读写事务可能包括读集和写集
  // 锁机制不区分读集和写集
  if (read_write_set.empty() && read_only_set.empty()) {
    return true;
  }
  for (auto& item : read_only_set) {
    auto it = item.item_ptr;
    all_tableid.push_back(it->table_id);
    all_keyid.push_back(it->key);
  }
  for (auto& item : read_write_set) {
    auto it = item.item_ptr;
    all_tableid.push_back(it->table_id);
    all_keyid.push_back(it->key);
  }

  assert(global_meta_man->txn_system == DTX_SYS::ONE_WRITE);

  #if OPEN_TIME
  // Run our system
  // 计时
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);
  #endif  

  if (!LockLocalRW(yield)) {
    TxAbort(yield);
    return false;
  } 

  #if OPEN_TIME
  struct timespec tx_lock_rw_time;
  clock_gettime(CLOCK_REALTIME, &tx_lock_rw_time);
  double lock_rw_usec = (tx_lock_rw_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_lock_rw_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
  #endif

  if (!ReadRemote(yield)) {
    TxAbort(yield);
    return false;
  }

  #if OPEN_TIME
  struct timespec tx_read_time;
  clock_gettime(CLOCK_REALTIME, &tx_read_time);
  double read_usec = (tx_read_time.tv_sec - tx_lock_rw_time.tv_sec) * 1000000 + (double)(tx_read_time.tv_nsec - tx_lock_rw_time.tv_nsec) / 1000;
  DEBUG_TIME("dtx_base_exe_commit.cc:46, exe a new txn %ld, lock_rw_usec: %lf, read_usec: %lf\n", tx_id, lock_rw_usec, read_usec);
  #endif

  return true;
ABORT:
  if (fail_abort) TxAbort(yield);
  return false;
}

bool DTX::TxReadWriteTxnCommit(coro_yield_t& yield) {
  /*!
    Baseline's commit protocol
    */
  #if OPEN_TIME
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);
  #endif

  #if LOG_RPC_OR_RDMA
  brpc::CallId cid;
  SendLogToStoragePool(tx_id, &cid);
  #else
  SendLogToStoragePool(tx_id);
  #endif

  #if OPEN_TIME
  struct timespec tx_send_log_time;
  clock_gettime(CLOCK_REALTIME, &tx_send_log_time);
  double send_log_usec = (tx_send_log_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_send_log_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
  #endif

  if (!read_write_set.empty()) {
    WriteRemote(yield);
  }

  #if OPEN_TIME
  struct timespec tx_write_time;
  clock_gettime(CLOCK_REALTIME, &tx_write_time);
  double write_usec = (tx_write_time.tv_sec - tx_send_log_time.tv_sec) * 1000000 + (double)(tx_write_time.tv_nsec - tx_send_log_time.tv_nsec) / 1000;
  #endif

  Unpin(yield);

  #if OPEN_TIME
  struct timespec tx_unpin_time;
  clock_gettime(CLOCK_REALTIME, &tx_unpin_time);
  double unpin_usec = (tx_unpin_time.tv_sec - tx_write_time.tv_sec) * 1000000 + (double)(tx_unpin_time.tv_nsec - tx_write_time.tv_nsec) / 1000;
  #endif

  UnLockLocalRW();

  #if OPEN_TIME
  struct timespec tx_unlock_time;
  clock_gettime(CLOCK_REALTIME, &tx_unlock_time);
  double unlock_usec = (tx_unlock_time.tv_sec - tx_unpin_time.tv_sec) * 1000000 + (double)(tx_unlock_time.tv_nsec - tx_unpin_time.tv_nsec) / 1000;
  DEBUG_TIME("dtx_base_exe_commit.cc:80, exe a new txn %ld, write_usec: %lf, unpin_usec: %lf, send_log_usec: %lf, unlock_usec: %lf\n", tx_id, write_usec, unpin_usec, send_log_usec, unlock_usec);
  #endif

  #if LOG_RPC_OR_RDMA
  //!! brpc同步
  brpc::Join(cid);
  #endif
  
  // printf("txn: %ld, commit\n", tx_id);
  return true;
}

bool DTX::TxReadWriteAbort() {
  UnLockLocalRW();
  Abort();
}

bool DTX::TxReadOnlyTxnExe(coro_yield_t& yield, bool fail_abort) {
  DEBUG_TIME("dtx_onewrite_exe_commit.cc:8, exe a new write txn %ld\n", tx_id);
  batch_id = tx_id;
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;

  // 读写事务可能包括读集和写集
  // 锁机制不区分读集和写集
  if (read_write_set.empty() && read_only_set.empty()) {
    return true;
  }
  assert(read_write_set.empty());

  for (auto& item : read_only_set) {
    auto it = item.item_ptr;
    all_tableid.push_back(it->table_id);
    all_keyid.push_back(it->key);
  }

  assert(global_meta_man->txn_system == DTX_SYS::ONE_WRITE);

  #if OPEN_TIME
  // Run our system
  // 计时
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);
  #endif  

  if (!ReadRemote(yield)) {
    TxReadOnlyAbort();
    return false;
  }

  #if OPEN_TIME
  struct timespec tx_read_time;
  clock_gettime(CLOCK_REALTIME, &tx_read_time);
  double read_usec = (tx_read_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_read_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
  DEBUG_TIME("dtx_base_exe_commit.cc:46, exe a new txn %ld, lock_rw_usec: %lf, read_usec: %lf\n", tx_id, lock_rw_usec, read_usec);
  #endif

  return true;
ABORT:
  if (fail_abort) Abort();
  return false;
}

bool DTX::TxReadOnlyTxnCommit() {
  return true;
}

bool DTX::TxReadOnlyAbort() {
  Abort();
  return true;
}