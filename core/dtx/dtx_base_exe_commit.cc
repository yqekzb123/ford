// Author: Hongyao Zhao
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "worker/global.h"
// #include ""
bool DTX::TxExe(coro_yield_t& yield, bool fail_abort) {
  printf("dtx_base_exe_commit.cc:8, exe a new txn %ld\n", tx_id);
  batch_id = tx_id;
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;
  // 锁机制不区分读集和写集
  if (read_write_set.empty() && read_only_set.empty()) {
    return true;
  }

  assert(global_meta_man->txn_system != DTX_SYS::OUR);
  // Run our system
  if (!LockRemoteRO(yield)) {
    goto ABORT;
  } 
  if (!LockRemoteRW(yield)) {
    goto ABORT;
  }
  if (!ReadRemote(yield)) {
    goto ABORT;
  }
  return true;
ABORT:
  if (fail_abort) TxAbort(yield);
  return false;
}

bool DTX::TxCommit(coro_yield_t& yield) {
  /*!
    Baseline's commit protocol
    */
  if (!read_write_set.empty()) {
    WriteRemote(yield);
  }
  Unpin(yield);
  SendLogToStoragePool(tx_id);
  UnlockShared(yield, hold_shared_lock_data_id, hold_shared_lock_node_offs);
  UnlockExclusive(yield, hold_exclusive_lock_data_id, hold_exclusive_lock_node_offs);

  return true;
}

void DTX::TxAbort(coro_yield_t& yield) {
  UnlockShared(yield, hold_shared_lock_data_id, hold_shared_lock_node_offs);
  UnlockExclusive(yield, hold_exclusive_lock_data_id, hold_exclusive_lock_node_offs);
  Abort();
}

bool DTX::LockRemoteRO(coro_yield_t& yield) {
  // Issue reads
  std::vector<table_id_t> readonly_tableid;
  std::vector<itemkey_t> readonly_keyid;
  if(read_only_set.empty()) return true;
  for (auto& item : read_only_set) {
    auto it = item.item_ptr;
    readonly_tableid.push_back(it->table_id);
    readonly_keyid.push_back(it->key);
  }
  bool res = LockSharedOnRecord(yield, readonly_tableid, readonly_keyid);
  return res;
}

bool DTX::LockRemoteRW(coro_yield_t& yield) {
  // Issue writes
  std::vector<table_id_t> readwrite_tableid;
  std::vector<itemkey_t> readwrite_keyid;
  if(read_write_set.empty()) return true;
  for (auto& item : read_write_set) {
    auto it = item.item_ptr;
    readwrite_tableid.push_back(it->table_id);
    readwrite_keyid.push_back(it->key);
  }
  bool res = LockSharedOnRecord(yield, readwrite_tableid, readwrite_keyid);
  return res;
}

bool DTX::ReadRemote(coro_yield_t& yield) {
  // 获取索引
  std::vector<table_id_t> all_tableid;
  std::vector<itemkey_t> all_keyid;
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
  temp_index = GetHashIndex(yield, all_tableid, all_keyid);
  if (temp_index.empty()) return false;
  // 获取数据项
  std::vector<Rid> id_list;
  std::vector<table_id_t> tid_list;
  std::vector<FetchPageType> fetch_type;
  for (auto& rid_map : temp_index) {
    table_id_t tid = rid_map.first;
    for (auto& rid : rid_map.second) {
      id_list.push_back(rid.second);
      tid_list.push_back(tid);
      fetch_type.push_back(FetchPageType::kReadPage);
    }
  }
  std::vector<DataItemPtr> data_list = FetchTuple(yield, tid_list, id_list, fetch_type, tx_id);
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
  std::vector<Rid> id_list;
  std::vector<table_id_t> tid_list;
  std::vector<DataItemPtr> new_data_list;

  std::vector<FetchPageType> fetch_type;

  for (auto& write_item : read_write_set) {
    auto it = write_item.item_ptr;
    tid_list.push_back(it->table_id);
    id_list.push_back(temp_index[it->table_id][it->key]);
    new_data_list.push_back(it);
    fetch_type.push_back(FetchPageType::kUpdateRecord); // 目前只是简单的更新，之后考虑插入和删除
  }

  WriteTuple(yield, tid_list, id_list, fetch_type, new_data_list, tx_id);
  tid_list.clear();
  id_list.clear();
  new_data_list.clear();

  return true;
}

void DTX::Unpin(coro_yield_t& yield){
  std::vector<PageId> page_ids;
  std::vector<FetchPageType> types;
  for (auto& rid_map : temp_index) {
    table_id_t tid = rid_map.first;
    for (auto& rid : rid_map.second) {
      PageId page_id;
      page_id.table_id = tid;
      page_id.page_no = rid.second.page_no_;
      page_ids.push_back(page_id);
      types.push_back(FetchPageType::kReadPage);
    }
  }
  UnpinPage(yield, page_ids, types);
}