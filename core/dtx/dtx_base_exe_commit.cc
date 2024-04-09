// Author: Hongyao Zhao
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "worker/global.h"
#include "exception.h"

// #include ""
bool DTX::TxExe(coro_yield_t& yield, bool fail_abort) {
  // try {
    DEBUG_TIME("dtx_base_exe_commit.cc:8, exe a new txn %ld\n", tx_id);
    batch_id = tx_id;
    // Start executing transaction
    tx_status = TXStatus::TX_EXE;
    // 锁机制不区分读集和写集
    if (read_write_set.empty() && read_only_set.empty()) {
      return true;
    }
    all_tableid.clear();
    all_keyid.clear();
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

    assert(global_meta_man->txn_system != DTX_SYS::OUR);
    #if OPEN_TIME
    // Run our system
    // 计时
    struct timespec tx_start_time;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    #endif

    // 获取索引
    all_rids = GetHashIndex(yield, all_tableid, all_keyid);
    for(int i=0; i<all_rids.size(); i++){
      if(all_rids[i].page_no_ == INVALID_PAGE_ID){
        // remove the invalid item
        all_tableid.erase(all_tableid.begin() + i);
        all_keyid.erase(all_keyid.begin() + i);
        all_rids.erase(all_rids.begin() + i);
        if(i < read_only_set.size())
          read_only_set.erase(read_only_set.begin() + i);
        else
          read_write_set.erase(read_write_set.begin() + (i - read_only_set.size())
        );
        i--;
        tx_status = TXStatus::TX_VAL_NOTFOUND;
      }
    }

    #if OPEN_TIME
    struct timespec tx_get_index_time;
    clock_gettime(CLOCK_REALTIME, &tx_get_index_time);
    double get_index_usec = (tx_get_index_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_get_index_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
    #endif
    if (!LockRemoteRO(yield)) {
      // printf("LockRemoteRO failed\n");
      TxAbort(yield);
      return false;
    } 

    #if OPEN_TIME
    struct timespec tx_lock_ro_time;
    clock_gettime(CLOCK_REALTIME, &tx_lock_ro_time);
    double lock_ro_usec = (tx_lock_ro_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_lock_ro_time.tv_nsec - tx_get_index_time.tv_nsec) / 1000;
    #endif
    if (!LockRemoteRW(yield)) {
      // printf("LockRemoteRW failed\n");
      TxAbort(yield);
      return false;
    }

    #if OPEN_TIME
    struct timespec tx_lock_rw_time;
    clock_gettime(CLOCK_REALTIME, &tx_lock_rw_time);
    double lock_rw_usec = (tx_lock_rw_time.tv_sec - tx_lock_ro_time.tv_sec) * 1000000 + (double)(tx_lock_rw_time.tv_nsec - tx_lock_ro_time.tv_nsec) / 1000;
    #endif
    if (!ReadRemote(yield)) {
      // printf("Read failed\n");
      TxAbort(yield);
      return false;
    }

    #if OPEN_TIME
    struct timespec tx_read_time;
    clock_gettime(CLOCK_REALTIME, &tx_read_time);
    double read_usec = (tx_read_time.tv_sec - tx_lock_rw_time.tv_sec) * 1000000 + (double)(tx_read_time.tv_nsec - tx_lock_rw_time.tv_nsec) / 1000;
    DEBUG_TIME("dtx_base_exe_commit.cc:46, exe a new txn %ld, read_index_usec: %lf, \
      lock_ro_usec: %lf, lock_rw_usec: %lf, read_usec: %lf\n", tx_id, get_index_usec, lock_ro_usec, lock_rw_usec, read_usec);
    #endif
  // }
  // catch(const AbortException& e) {
  //   TxAbort(yield);
  //   return false;
  // }
  return true;
// ABORT:
  // if (fail_abort) TxAbort(yield);
  // return false;
}

bool DTX::TxCommit(coro_yield_t& yield) {
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

  #if OPEN_TIME
  struct timespec tx_send_log_time;
  clock_gettime(CLOCK_REALTIME, &tx_send_log_time);
  double send_log_usec = (tx_send_log_time.tv_sec - tx_unpin_time.tv_sec) * 1000000 + (double)(tx_send_log_time.tv_nsec - tx_unpin_time.tv_nsec) / 1000;
  #endif
  UnlockShared(yield);
  UnlockExclusive(yield);

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
  
  tx_status = TXStatus::TX_COMMIT;
  // printf("txn: %ld, commit\n", tx_id);
  return true;
}

void DTX::TxAbort(coro_yield_t& yield) {
  UnlockShared(yield);
  UnlockExclusive(yield);
  Abort();
}

bool DTX::LockRemoteRO(coro_yield_t& yield) {
  // Issue reads
  if(read_only_set.empty()) return true;
  std::vector<table_id_t> readonly_tableid(all_tableid.begin(), all_tableid.begin() + read_only_set.size());
  std::vector<itemkey_t> readonly_keyid(all_keyid.begin(), all_keyid.begin() + read_only_set.size());
  // debug
  // for(int i=0; i<read_only_set.size(); i++){
  //   printf("lock ro, table_id: %d, item_id: %d", readonly_tableid[i], readonly_keyid[i]);
  // }
  bool res = LockSharedOnRecord(yield, std::move(readonly_tableid), std::move(readonly_keyid));
  return res;
}

bool DTX::LockRemoteRW(coro_yield_t& yield) {
  // Issue writes
  if(read_write_set.empty()) return true;
  std::vector<table_id_t> readwrite_tableid(all_tableid.begin() + read_only_set.size(), all_tableid.end());
  std::vector<itemkey_t> readwrite_keyid(all_keyid.begin() + read_only_set.size(), all_keyid.end());
  // debug
  // for(int i=0; i<read_write_set.size(); i++){
  //   printf("lock rw, table_id: %d, item_id: %d", readwrite_tableid[i], readwrite_keyid[i]);
  // }
  bool res = LockExclusiveOnRecord(yield, std::move(readwrite_tableid), std::move(readwrite_keyid));
  return res;
}

bool DTX::ReadRemote(coro_yield_t& yield) {
  #if OPEN_TIME
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);
  #endif

  // 获取数据项
  std::vector<FetchPageType> fetch_type(all_tableid.size(), FetchPageType::kReadPage);
  std::vector<DataItemPtr> data_list = FetchTuple(yield, all_tableid, all_rids, fetch_type, tx_id);
  
  #if OPEN_TIME
  struct timespec tx_fetch_time;
  clock_gettime(CLOCK_REALTIME, &tx_fetch_time);
  double fetch_usec = (tx_fetch_time.tv_sec - tx_get_index_time.tv_sec) * 1000000 + (double)(tx_fetch_time.tv_nsec - tx_get_index_time.tv_nsec) / 1000;
  DEBUG_TIME("dtx_base_exe_commit.cc:168, exe a new txn %ld, get_index_usec: %lf, fetch_usec: %lf\n", tx_id, get_index_usec, fetch_usec);
  #endif
  
  if (data_list.empty()) return false;
  // !接下来需要将数据项塞入读写集里
  for (auto fetch_item : data_list) {
    auto fit = fetch_item;
    bool find = false;

    for (auto& read_item : read_only_set) {
      if (read_item.is_fetched) continue;
      // auto rit = read_item.item_ptr;
      auto* rit = read_item.item_ptr.get();
      if (rit->table_id == fit->table_id &&
          rit->key == fit->key) {
        *rit = *fit;
        read_item.is_fetched = true;
        find = true;
        break;
      }
    }
    if (find) continue;

    for (auto& write_item : read_write_set) {
      if (write_item.is_fetched) continue;
      auto* wit = write_item.item_ptr.get();
      if (wit->table_id == fit->table_id &&
          wit->key == fit->key) {
        *wit = *fit;
        write_item.is_fetched = true;
        break;
      }
    }
  }
  return true;
}

bool DTX::WriteRemote(coro_yield_t& yield) {
  std::vector<DataItemPtr> new_data_list;
  new_data_list.reserve(read_write_set.size());
  for (auto& write_item : read_write_set) {
    auto it = write_item.item_ptr;
    new_data_list.push_back(it);
  }
  std::vector<table_id_t> tid_list(all_tableid.begin() + read_only_set.size(), all_tableid.end());
  std::vector<Rid> id_list(all_rids.begin() + read_only_set.size(), all_rids.end());
  std::vector<FetchPageType> fetch_type(read_write_set.size(), FetchPageType::kUpdateRecord); // 目前只是简单的更新，之后考虑插入和删除

  std::vector<int> rw_rid_map_pageid_idx(rid_map_pageid_idx.begin() + read_only_set.size(), rid_map_pageid_idx.end());
  WriteTuple(yield, tid_list, id_list, rw_rid_map_pageid_idx, fetch_type, new_data_list, tx_id);

  return true;
}

void DTX::Unpin(coro_yield_t& yield){
  // std::vector<PageId> page_ids;
  // std::vector<FetchPageType> types(all_tableid.size(), FetchPageType::kReadPage);
  // for(int i = 0; i < all_tableid.size(); i++){
  //   PageId page_id;
  //   page_id.table_id = all_tableid[i];
  //   page_id.page_no = all_rids[i].page_no_;
  //   page_ids.push_back(page_id);
  // }

  UnpinPage(yield, all_page_ids, all_types);
}