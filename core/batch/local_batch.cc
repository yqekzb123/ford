// Author: Hongyao Zhao
// Copyright (c) 2023

#include "batch/local_batch.h"
#include "worker/global.h"
#include "dtx/dtx.h"
#include "base/page.h"
bool LocalBatch::GetReadWriteSet(coro_yield_t& yield){
  all_tableid.clear();
  all_keyid.clear();
  all_type.clear();
  readonly_tableid.clear();
  readonly_keyid.clear();
  readwrite_tableid.clear();
  readwrite_keyid.clear();

  std::unordered_map<LocalDataKey,uint64_t,LocalDataHash,LocalDataEqual> all_map; // 去重使用
  int i = 0;
  for (int j = 0; j < LOCAL_BATCH_TXN_SIZE; j ++) {
    BenchDTX* dtx = txn_list[j];
    i++;
    if(dtx->dtx->read_only_set.empty()) continue;
    for (auto& item : dtx->dtx->read_only_set) {
      auto it = item.item_ptr;
      LocalDataKey key(it->table_id,it->key);
      if(all_map.count(key) != 0) continue;
      else {
        all_map[key] = all_tableid.size();
        all_tableid.emplace_back(it->table_id);
        all_keyid.emplace_back(it->key);
        all_type.emplace_back(0);
      }
    }
  }
  i=0;
  for (int j = 0; j < LOCAL_BATCH_TXN_SIZE; j ++) {
    BenchDTX* dtx = txn_list[j];
    i++;
    if(dtx->dtx->read_write_set.empty()) continue;
    for (auto& item : dtx->dtx->read_write_set) {
      auto it = item.item_ptr;
      LocalDataKey key(it->table_id,it->key);
      if(all_map.count(key) != 0) {
        uint64_t index = all_map[key];
        if (all_type[index] != 1) {
          all_type[index] = 1;
        }
      } else {
        all_map[key] = all_tableid.size();
        all_tableid.emplace_back(it->table_id);
        all_keyid.emplace_back(it->key);
        all_type.emplace_back(1);
      }
    }
  }
  
  for (int j = 0; j < all_tableid.size(); j++) {
    if (all_type[j] == 0) {
      readonly_tableid.push_back(all_tableid[j]);
      readonly_keyid.push_back(all_keyid[j]);
    } else {
      readwrite_tableid.push_back(all_tableid[j]);
      readwrite_keyid.push_back(all_keyid[j]);
    }
  }
  
  // printf("local_batch.cc:51 thread %ld execute batch %ld, has %ld key\n", thread_gid, batch_id, all_keyid.size());
}

bool LocalBatch::ExeBatchRW(coro_yield_t& yield) {
  stat_attempted_tx_total+=LOCAL_BATCH_TXN_SIZE;
  if (batch_id == WARMUP_BATCHCNT) {
    clock_gettime(CLOCK_REALTIME, &msr_start);
  }
  // 先随便整个dtx结构出来
  bool res = true;
  BenchDTX* first_bdtx = txn_list[0];
  DTX* first_dtx = first_bdtx->dtx;
  
  //! 0.统计只读和读写的操作列表
  GetReadWriteSet(yield);
  #if OPEN_TIME
  // 打点计时1
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);
  #endif

  // //! 1. 对事务访问的数据项加锁
  // 只读加锁
  res = first_dtx->LockSharedOnRecord(yield, readonly_tableid, readonly_keyid);
  if (!res) {
    // !失败以后所有操作解锁
    first_dtx->UnlockShared(yield);
    stat_aborted_tx_total+=LOCAL_BATCH_TXN_SIZE;
    return res;
  }
  // 读写加锁
  res = first_dtx->LockExclusiveOnRecord(yield, readwrite_tableid, readwrite_keyid);
  if (!res) {
    // !失败以后所有操作解锁
    first_dtx->UnlockShared(yield);
    first_dtx->UnlockExclusive(yield);
    stat_aborted_tx_total+=LOCAL_BATCH_TXN_SIZE;
    return res;
  }

  #if OPEN_TIME
  // 计时2
  struct timespec tx_lock_time;
  clock_gettime(CLOCK_REALTIME, &tx_lock_time);
  double lock_usec = (tx_lock_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_lock_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
  #endif

  //! 2. 获取数据项索引
  all_rids = first_dtx->GetHashIndex(yield, all_tableid, all_keyid);
  assert(!all_rids.empty());

  #if OPEN_TIME
  // 计时3
  struct timespec tx_index_time;
  clock_gettime(CLOCK_REALTIME, &tx_index_time);
  double index_usec = (tx_index_time.tv_sec - tx_lock_time.tv_sec) * 1000000 + (double)(tx_index_time.tv_nsec - tx_lock_time.tv_nsec) / 1000;
  #endif

  //! 3. 读取数据项
  auto data_list = ReadData(yield, first_dtx);

  #if OPEN_TIME
  // 计时4
  struct timespec tx_read_time;
  clock_gettime(CLOCK_REALTIME, &tx_read_time);
  double read_usec = (tx_read_time.tv_sec - tx_index_time.tv_sec) * 1000000 + (double)(tx_read_time.tv_nsec - tx_index_time.tv_nsec) / 1000;
  #endif

  // ! 4. 从页中取出数据，将数据存入local，还没想好存到哪
  for (auto item : data_list) {
    LocalData* data_item = local_data_store.GetData(item->table_id, item->key);
    // printf("local_batch.cc:70, set the first value of local data table %ld key %ld ptr %p\n",item->table_id, item->key, data_item);
    data_item->SetFirstVersion(item);
  }

  //! 5. 根据数据项进行本地计算
  for (int i = 0; i < LOCAL_BATCH_TXN_SIZE; i ++) {
    BenchDTX* dtx = txn_list[i];
    res = dtx->TxReCaculate(yield);
  }

  #if OPEN_TIME
  // 计时5
  struct timespec tx_recalculate_time;
  clock_gettime(CLOCK_REALTIME, &tx_recalculate_time);
  double recalculate_usec = (tx_recalculate_time.tv_sec - tx_read_time.tv_sec) * 1000000 + (double)(tx_recalculate_time.tv_nsec - tx_read_time.tv_nsec) / 1000;
  #endif

  // //! 6. 将确定的数据，利用RDMA刷入页中
  FlushWrite(yield, first_dtx, data_list);

  StatCommit();

  #if OPEN_TIME
  // 计时6
  struct timespec tx_flush_time;
  clock_gettime(CLOCK_REALTIME, &tx_flush_time);
  double flush_usec = (tx_flush_time.tv_sec - tx_recalculate_time.tv_sec) * 1000000 + (double)(tx_flush_time.tv_nsec - tx_recalculate_time.tv_nsec) / 1000;
  #endif

  //! 7. Unpin pages
  Unpin(yield, first_dtx);

  #if OPEN_TIME
  // 计时7
  struct timespec tx_unpin_time;
  clock_gettime(CLOCK_REALTIME, &tx_unpin_time);
  double unpin_usec = (tx_unpin_time.tv_sec - tx_flush_time.tv_sec) * 1000000 + (double)(tx_unpin_time.tv_nsec - tx_flush_time.tv_nsec) / 1000;
  #endif

  //! 8. 写日志到存储层
  #if LOG_RPC_OR_RDMA
  brpc::CallId cid;
  first_dtx->SendLogToStoragePool(batch_id, &cid);
  #else
  first_dtx->SendLogToStoragePool(batch_id);
  #endif

  #if OPEN_TIME
  // 计时8
  struct timespec tx_log_time;
  clock_gettime(CLOCK_REALTIME, &tx_log_time);
  double log_usec = (tx_log_time.tv_sec - tx_unpin_time.tv_sec) * 1000000 + (double)(tx_log_time.tv_nsec - tx_unpin_time.tv_nsec) / 1000;
  #endif

  //! 8. 释放锁
  first_dtx->UnlockShared(yield);
  first_dtx->UnlockExclusive(yield);

  //!! brpc同步
  #if LOG_RPC_OR_RDMA
  brpc::Join(cid);
  #endif

  #if OPEN_TIME
  // 计时9
  struct timespec tx_unlock_time;
  clock_gettime(CLOCK_REALTIME, &tx_unlock_time);
  double unlock_usec = (tx_unlock_time.tv_sec - tx_log_time.tv_sec) * 1000000 + (double)(tx_unlock_time.tv_nsec - tx_log_time.tv_nsec) / 1000;
  #endif

  // 记录提交事务
  struct timespec tx_end_time;
  clock_gettime(CLOCK_REALTIME, &tx_end_time);
  
  if (batch_id > WARMUP_BATCHCNT) {
    for (int i = 0; i < LOCAL_BATCH_TXN_SIZE; i ++) {
      BenchDTX* dtx = txn_list[i];
      double tx_usec = (tx_end_time.tv_sec - dtx->dtx->tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - dtx->dtx->tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      stop_run = true;
    }
  }
  for (int i = 0; i < LOCAL_BATCH_TXN_SIZE; i ++) {
    BenchDTX* dtx = txn_list[i];
    delete dtx;
  }
  // 输出计时
  // printf("local_batch.cc:226 1. lock %lf, 2. index %lf, 3. read %lf, 4. recalculate %lf, 5. flush %lf, 6. unpin %lf, 7. log %lf, 8. unlock %lf (us)\n", lock_usec, index_usec, read_usec, recalculate_usec, flush_usec, unpin_usec, log_usec, unlock_usec);
  // printf("local_batch.cc:227 execute batch %ld complete\n", batch_id);
  return res;
}

std::vector<DataItemPtr> LocalBatch::ReadData(coro_yield_t& yield, DTX* first_dtx) {
  std::vector<FetchPageType> fetch_type(all_rids.size(), FetchPageType::kReadPage);
  std::vector<DataItemPtr> data_list = first_dtx->FetchTuple(yield, all_tableid, all_rids, fetch_type, batch_id);
  return data_list;
}

void LocalBatch::Unpin(coro_yield_t& yield, DTX* first_dtx){
  first_dtx->UnpinPage(yield, first_dtx->all_page_ids, first_dtx->all_types);
}

bool LocalBatch::FlushWrite(coro_yield_t& yield, DTX* first_dtx, std::vector<DataItemPtr>& data_list) {
  // !note 获取需要区分rw事务和ro事务
  std::vector<DataItemPtr> new_data_list;
  std::vector<FetchPageType> fetch_type(all_rids.size(), FetchPageType::kUpdateRecord);
  for (auto item : data_list) {
    LocalData* data_item = local_data_store.GetData(item->table_id, item->key);
    LVersion v = data_item->GetTailVersion();
    DataItem* data = new DataItem();
    memcpy(data, v.value, sizeof(DataItem));
    DataItemPtr itemPtr(data);
    new_data_list.push_back(itemPtr);
  }

  first_dtx->WriteTuple(yield, all_tableid, all_rids, first_dtx->rid_map_pageid_idx, fetch_type, new_data_list, batch_id);
  new_data_list.clear();

  return true;
}

bool LocalBatch::StatCommit() {
  commit_times += current_txn_cnt;
  // for (auto& dtx : txn_list) {
  //   dtx->StatCommit();
  // }
}
