// Author: Hongyao Zhao
// Copyright (c) 2023

#include "batch/local_batch.h"
#include "worker/global.h"
#include "dtx/dtx.h"
#include "base/page.h"

bool LocalBatch::ExeBatchRW(coro_yield_t& yield) {
  if (batch_id == WARMUP_BATCHCNT) {
    clock_gettime(CLOCK_REALTIME, &msr_start);
  }
  // 先随便整个dtx结构出来
  bool res = true;
  BenchDTX* first_bdtx = txn_list.front();
  DTX* first_dtx = first_bdtx->dtx;
  // printf("local_batch.cc:14 execute batch %ld\n", batch_id);
  //! 0.统计只读和读写的操作列表
  std::vector<table_id_t> readonly_tableid;
  std::vector<itemkey_t> readonly_keyid;
  int i = 0;
  for (auto& dtx : txn_list) {
    i++;
    if(dtx->dtx->read_only_set.empty()) continue;
    for (auto& item : dtx->dtx->read_only_set) {
      auto it = item.item_ptr;
      readonly_tableid.push_back(it->table_id);
      readonly_keyid.push_back(it->key);
    }
  }
  std::vector<table_id_t> readwrite_tableid;
  std::vector<itemkey_t> readwrite_keyid;
  i=0;
  for (auto& dtx : txn_list) {
    i++;
    if(dtx->dtx->read_write_set.empty()) continue;
    for (auto& item : dtx->dtx->read_write_set) {
      auto it = item.item_ptr;
      readonly_tableid.push_back(it->table_id);
      readonly_keyid.push_back(it->key);
    }
  }
  
  std::vector<table_id_t> all_tableid(readonly_tableid);
  all_tableid.insert(all_tableid.end(),readwrite_tableid.begin(),readwrite_tableid.end());
  std::vector<itemkey_t> all_keyid(readonly_keyid);
  all_keyid.insert(all_keyid.end(),readwrite_keyid.begin(),readwrite_keyid.end());
  
  // 打点计时1
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);

  //! 1. 对事务访问的数据项加锁
  // 只读加锁
  res = first_dtx->LockSharedOnRecord(yield, readonly_tableid, readonly_keyid);
  if (!res) {
    // !失败以后所有操作解锁
    return res;
  }
  // 读写加锁
  res = first_dtx->LockExclusiveOnRecord(yield, readwrite_tableid, readwrite_keyid);
  if (!res) {
    // !失败以后所有操作解锁
    return res;
  }
  // 计时2
  struct timespec tx_lock_time;
  clock_gettime(CLOCK_REALTIME, &tx_lock_time);
  double lock_usec = (tx_lock_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_lock_time.tv_nsec - tx_start_time.tv_nsec) / 1000;

  //! 2. 获取数据项索引
  auto index = first_dtx->GetHashIndex(yield, all_tableid, all_keyid);
  // 计时3
  struct timespec tx_index_time;
  clock_gettime(CLOCK_REALTIME, &tx_index_time);
  double index_usec = (tx_index_time.tv_sec - tx_lock_time.tv_sec) * 1000000 + (double)(tx_index_time.tv_nsec - tx_lock_time.tv_nsec) / 1000;

  //! 3. 读取数据项
  auto data_list = ReadData(yield, first_dtx,index);
  // 计时4
  struct timespec tx_read_time;
  clock_gettime(CLOCK_REALTIME, &tx_read_time);
  double read_usec = (tx_read_time.tv_sec - tx_index_time.tv_sec) * 1000000 + (double)(tx_read_time.tv_nsec - tx_index_time.tv_nsec) / 1000;

  //! 4. 从页中取出数据，将数据存入local，还没想好存到哪
  for (auto item : data_list) {
    LocalData* data_item = local_data_store.GetData(item->table_id, item->key);
    // printf("local_batch.cc:70, set the first value of local data table %ld key %ld ptr %p\n",item->table_id, item->key, data_item);
    data_item->SetFirstVersion(item);
    // printf("local_batch.cc:72, local data table %ld key %ld ptr %p\n",item->table_id, item->key, data_item);
  }

  //! 5. 根据数据项进行本地计算
  for (auto& dtx : txn_list) {
    // !连续的本地计算,这里还需要增加tpcc等负载的运算内容 
    res = dtx->TxReCaculate(yield);
  }
  // 计时5
  struct timespec tx_recalculate_time;
  clock_gettime(CLOCK_REALTIME, &tx_recalculate_time);
  double recalculate_usec = (tx_recalculate_time.tv_sec - tx_read_time.tv_sec) * 1000000 + (double)(tx_recalculate_time.tv_nsec - tx_read_time.tv_nsec) / 1000;

  // std::vector<DataItemPtr> new_data_list;
  //! 6. 将确定的数据，利用RDMA刷入页中
  FlushWrite(yield, first_dtx,data_list,index);

  StatCommit();

  // 计时6
  struct timespec tx_flush_time;
  clock_gettime(CLOCK_REALTIME, &tx_flush_time);
  double flush_usec = (tx_flush_time.tv_sec - tx_recalculate_time.tv_sec) * 1000000 + (double)(tx_flush_time.tv_nsec - tx_recalculate_time.tv_nsec) / 1000;

  //! 7. Unpin pages
  Unpin(yield, first_dtx, index);
  // 计时7
  struct timespec tx_unpin_time;
  clock_gettime(CLOCK_REALTIME, &tx_unpin_time);
  double unpin_usec = (tx_unpin_time.tv_sec - tx_flush_time.tv_sec) * 1000000 + (double)(tx_unpin_time.tv_nsec - tx_flush_time.tv_nsec) / 1000;

  //! 8. 写日志到存储层
  first_dtx->SendLogToStoragePool(batch_id);
  // 计时8
  struct timespec tx_log_time;
  clock_gettime(CLOCK_REALTIME, &tx_log_time);
  double log_usec = (tx_log_time.tv_sec - tx_unpin_time.tv_sec) * 1000000 + (double)(tx_log_time.tv_nsec - tx_unpin_time.tv_nsec) / 1000;

  //! 8. 释放锁
  first_dtx->UnlockShared(yield, first_dtx->hold_shared_lock_data_id, first_dtx->hold_shared_lock_node_offs);
  first_dtx->UnlockExclusive(yield, first_dtx->hold_exclusive_lock_data_id, first_dtx->hold_exclusive_lock_node_offs);
  // 计时9
  struct timespec tx_unlock_time;
  clock_gettime(CLOCK_REALTIME, &tx_unlock_time);
  double unlock_usec = (tx_unlock_time.tv_sec - tx_log_time.tv_sec) * 1000000 + (double)(tx_unlock_time.tv_nsec - tx_log_time.tv_nsec) / 1000;

  // 记录提交事务
  struct timespec tx_end_time;
  clock_gettime(CLOCK_REALTIME, &tx_end_time);
  
  if (batch_id > WARMUP_BATCHCNT) {
    for (auto& dtx : txn_list) {
      double tx_usec = (tx_end_time.tv_sec - dtx->dtx->tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - dtx->dtx->tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
      // !清理事务
      delete dtx;
    }
    if (stat_committed_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      stop_run = true;
    }
  }

  // 输出计时
  // printf("local_batch.cc:164 1. lock %lf, 2. index %lf, 3. read %lf, 4. recalculate %lf, 5. flush %lf, 6. unpin %lf, 7. log %lf, 8. unlock %lf (us)\n", lock_usec, index_usec, read_usec, recalculate_usec, flush_usec, unpin_usec, log_usec, unlock_usec);
  // printf("local_batch.cc:95 execute batch %ld complete\n", batch_id);
  return res;
}

std::vector<DataItemPtr> LocalBatch::ReadData(coro_yield_t& yield, DTX* first_dtx, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index) {
  // 遍历index，获取其中的页表地址
  std::vector<Rid> id_list;
  std::vector<table_id_t> tid_list;
  std::vector<FetchPageType> fetch_type;
  for (auto& rid_map : index) {
    table_id_t tid = rid_map.first;
    for (auto& rid : rid_map.second) {
      id_list.push_back(rid.second);
      tid_list.push_back(tid);
      fetch_type.push_back(FetchPageType::kReadPage);
    }
  }
  std::vector<DataItemPtr> data_list = first_dtx->FetchTuple(yield, tid_list, id_list, fetch_type, batch_id);
  return data_list;
}

void LocalBatch::Unpin(coro_yield_t& yield, DTX* first_dtx, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index){
  std::vector<PageId> page_ids;
  std::vector<FetchPageType> types;
  for (auto& rid_map : index) {
    table_id_t tid = rid_map.first;
    for (auto& rid : rid_map.second) {
      PageId page_id;
      page_id.table_id = tid;
      page_id.page_no = rid.second.page_no_;
      page_ids.push_back(page_id);
      types.push_back(FetchPageType::kReadPage);
    }
  }
  first_dtx->UnpinPage(yield, page_ids, types);
}

bool LocalBatch::FlushWrite(coro_yield_t& yield, DTX* first_dtx, std::vector<DataItemPtr>& data_list, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>>& index) {
  std::vector<Rid> id_list;
  std::vector<table_id_t> tid_list;
  std::vector<DataItemPtr> new_data_list;

  std::vector<FetchPageType> fetch_type;

  for (auto item : data_list) {
    tid_list.push_back(item->table_id);
    id_list.push_back(index[item->table_id][item->key]);
    LocalData* data_item = local_data_store.GetData(item->table_id, item->key);
    LVersion* v = data_item->GetTailVersion();
    DataItem* data = new DataItem();
    memcpy(data, v->value, sizeof(DataItem));
    DataItemPtr itemPtr(data);
    new_data_list.push_back(itemPtr);
    fetch_type.push_back(FetchPageType::kUpdateRecord); // 目前只是简单的更新，之后考虑插入和删除
  }
  first_dtx->WriteTuple(yield, tid_list, id_list, fetch_type, new_data_list, batch_id);
  tid_list.clear();
  id_list.clear();
  new_data_list.clear();

  return true;
}

bool LocalBatch::StatCommit() {
  commit_times += txn_list.size();
  // for (auto& dtx : txn_list) {
  //   dtx->StatCommit();
  // }
}
