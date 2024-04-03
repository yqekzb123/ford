// Author: Hongyao Zhao
// Copyright (c) 2023

#include "dtx/dtx.h"
#include "worker/global.h"

//! 本地生成读写集，本地执行并发控制
bool DTX::LockLocalRO(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  for (auto& item : read_only_set) {
    if (item.is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对只读操作加锁
    bool success = localdata->LockShared();
    if (!success) {
      shared_lock_abort_cnt ++;
      // printf("txn %ld add shared lock on table %ld key %ld failed, the original lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
      return false;
    } else {
      // printf("txn %ld add shared lock on table %ld key %ld success, now the lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
      item.is_local_locked = true;
    }
  }
  return true;
}

bool DTX::LockLocalRW(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对只读操作加锁
    bool success = localdata->LockShared();
    if (!success) {
      shared_lock_abort_cnt ++;
      // printf("txn %ld add shared lock on table %ld key %ld failed, the original lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
      return false;
    } else {
      // printf("txn %ld add shared lock on table %ld key %ld success, now the lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
      item.is_local_locked = true;
    }
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // !加锁
    bool success = localdata->LockExclusive();
    if (!success) {
      exlusive_lock_abort_cnt ++;
      // printf("txn %ld add exlusive lock on table %ld key %ld failed, the original lock %ld\n", tx_id, read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key, localdata->lock);
      return false;
    } else {
      // printf("txn %ld add exclusive lock on table %ld key %ld success, now the lock %ld\n", tx_id, read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key, localdata->lock);
      read_write_set[i].is_local_locked = true;
    }
  }
  return true;
}

// 这个函数用于读写事务的本地解锁
bool DTX::UnLockLocalRW() {
  //! 2.本地释放锁
  for (auto& item : read_only_set) {
    if (!item.is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    localdata->UnlockShared();
    // printf("txn %ld release shared lock on table %ld key %ld success, now the lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (!read_write_set[i].is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    localdata->UnlockExclusive();
    // printf("txn %ld release exclusive lock on table %ld key %ld success, now the lock %ld\n", tx_id, read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key, localdata->lock);
  }
  return true;
}

bool DTX::ExeLocalRO(coro_yield_t& yield, uint64_t bid) {
  std::vector<DirectRead> pending_direct_ro;
  auto batch = local_batch_store[bid]->GetBatchByIndex(batch_index,batch_id);
  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    // 如果不是这个batch管理的
    // if (item.bid != bid) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // printf("dtx_local.cc:69 txn %p tid %ld try to get version for table %ld key %ld data %p in batch %ld\n", this, tx_id, item.item_ptr.get()->table_id, item.item_ptr.get()->key,localdata,batch_id);
    // !将只读操作存入操作集
    item.read_version_dtx = localdata->GetTailVersion().txn;
    item.version_index = localdata->version_cnt;
  }

  return true;
}

bool DTX::ExeLocalRW(coro_yield_t& yield, uint64_t bid) {
  auto batch = local_batch_store[bid]->GetBatchByIndex(batch_index,batch_id);
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    // 如果不是这个batch管理的
    // if (item.bid != bid) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // printf("dtx_local.cc:83 txn %p tid %ld try to get version for table %ld key %ld data %p in batch %ld\n", this, tx_id, item.item_ptr.get()->table_id, item.item_ptr.get()->key,localdata,batch_id);
    // !对当前读取到的最新版本打标记
    item.read_version_dtx = localdata->GetTailVersion().txn;
    item.version_index = localdata->version_cnt;
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    // 如果不是这个batch管理的
    // if (read_write_set[i].bid != bid) continue;
    auto localdata = batch->local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // printf("dtx_local.cc:91 txn %p tid %ld try to rewrite version for table %ld key %ld data %p in batch %ld\n", this, tx_id, read_write_set[i].item_ptr.get()->table_id, read_write_set[i].item_ptr.get()->key,localdata,batch_id);
    // !创建新版本
    // localdata->CreateNewVersion(this);
    read_write_set[i].version_index = localdata->version_cnt;
    // printf("dtx_local.cc:69 txn %p tid %ld create new version for table %ld key %ld data %p in batch %ld\n", this, tx_id, read_write_set[i].item_ptr.get()->table_id, read_write_set[i].item_ptr.get()->key,localdata,batch_id);
  }
  return true;
}

bool DTX::DividIntoBatch(coro_yield_t& yield, BenchDTX* dtx_with_bench) {
  // todo：形成聚簇
  // 目前先实现成随便扔到一个batch里
  auto batch = local_batch_store[thread_gid]->InsertTxn(dtx_with_bench);
  if (batch == nullptr) {
    // printf("dtx_local.cc:103, thread %ld insert txn failed\n", thread_gid);
    return false;
  }
  batch_id = batch->batch_id;
  batch_index = batch_id % local_batch_store[thread_gid]->max_batch_cnt;
  // if (read_write_set.empty()) {
  //   ExeLocalRO(yield,thread_gid);
  // } else {
  //   ExeLocalRW(yield,thread_gid);
  // }
  batch->EndInsertTxn();
  
  // for (auto& bid : batch_list) { 
  //   auto batch = local_batch_store[bid]->InsertTxn(dtx_with_bench);
  //   if (batch == nullptr) {
  //     // printf("dtx_local.cc:103, thread %ld insert txn failed\n", thread_gid);
  //     return false;
  //   }
  //   batch_id = batch->batch_id;
  //   batch_index = batch_id % local_batch_store[thread_gid]->max_batch_cnt;
  //   if (read_write_set.empty()) {
  //     ExeLocalRO(yield,bid);
  //   } else {
  //     ExeLocalRW(yield,bid);
  //   }
  //   batch->EndInsertTxn();
  // }
  return true;
}

bool DTX::LocalCommit(coro_yield_t& yield, BenchDTX* dtx_with_bench) {
  bool res = true;
  //! 1.将生成好的读写集，塞到batch中
  // 根据逻辑分区，将事务分到不同batch.
  bool result = DividIntoBatch(yield, dtx_with_bench);
  if (!result) return result;
  // printf("dtx_local.cc:77 insert dtx %ld into batch %ld \n", tx_id, batch_id);
  //! 2.本地释放锁
  for (auto& item : read_only_set) {
    if (!item.is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    localdata->UnlockShared();
    // printf("txn %ld release shared lock on table %ld key %ld success, now the lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (!read_write_set[i].is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    localdata->UnlockExclusive();
    // printf("txn %ld release exclusive lock on table %ld key %ld success, now the lock %ld\n", tx_id, read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key, localdata->lock);
  }
  return res;
}

bool DTX::LocalAbort(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (!item.is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    localdata->UnlockShared();
    // printf("txn %ld release shared lock on table %ld key %ld success, now the lock %ld\n", tx_id, item.item_ptr.get()->table_id,item.item_ptr.get()->key, localdata->lock);
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (!read_write_set[i].is_local_locked) continue;
    auto localdata = local_lock_store.GetLock(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    localdata->UnlockExclusive();
    // printf("txn %ld release exclusive lock on table %ld key %ld success, now the lock %ld\n", tx_id, read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key, localdata->lock);
  }
  Abort();
  return true;
}

bool DTX::ReExeLocalRO(coro_yield_t& yield) {
  auto batch = local_batch_store[thread_gid]->GetBatchByIndex(batch_index,batch_id);
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !遍历版本，获取到需要的值
    auto fetched_item = localdata->GetTailVersion();
    auto* it = item.item_ptr.get();
    // it = fetched_item->value.get();
    *it = *fetched_item.value;
    // 这里先假设了读取到的是对的
    // printf("dtx_local.cc:211 txn %p tid %ld try to get version for table %ld key %ld data %p in batch %ld value index %ld value %s\n", this, tx_id, item.item_ptr.get()->table_id, item.item_ptr.get()->key,localdata,batch_id,item.version_index,fetched_item.value);
  }
  
  return true;
}

bool DTX::ReExeLocalRW(coro_yield_t& yield) {
  auto batch = local_batch_store[thread_gid]->GetBatchByIndex(batch_index,batch_id);
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // 拿到版本后，开始执行
    auto fetched_item = localdata->GetTailVersion();
    auto* it = item.item_ptr.get();
    *it = *fetched_item.value;
    // 这里先假设了读取到的是对的
    // printf("dtx_local.cc:227 txn %p tid %ld try to get version for table %ld key %ld data %p in batch %ld value index %ld value %s\n", this, tx_id, item.item_ptr.get()->table_id, item.item_ptr.get()->key,localdata,batch_id,0,fetched_item.value);
  }

  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // !获取自己写入的版本
    auto fetched_item = localdata->GetTailVersion();
    auto* it = read_write_set[i].item_ptr.get();
    assert(fetched_item.value->value[0] != 0);
    *it = *fetched_item.value;
    // printf("dtx_local.cc:238 txn %p tid %ld try to rewrite version for table %ld key %ld data %p in batch %ld value index %ld value %s\n", this, tx_id, read_write_set[i].item_ptr.get()->table_id, read_write_set[i].item_ptr.get()->key,localdata,batch_id,0,fetched_item.value);
  }
  return true;
  // !接下来得进行tpcc等负载计算了
}


// bool DTX::ReExeLocalRO(coro_yield_t& yield) {
//   auto batch = local_batch_store[thread_gid]->GetBatchByIndex(batch_index,batch_id);
//   for (auto& item : read_only_set) {
//     if (item.is_fetched) continue;
//     auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
//     // !遍历版本，获取到需要的值
//     auto fetched_item = localdata->GetDTXVersion(item.read_version_dtx, item.version_index);
//     auto* it = item.item_ptr.get();
//     // it = fetched_item->value.get();
//     *it = *fetched_item.value;
//     // 这里先假设了读取到的是对的
//     printf("dtx_local.cc:156 txn %p tid %ld try to get version for table %ld key %ld data %p in batch %ld value index %ld value %s\n", this, tx_id, item.item_ptr.get()->table_id, item.item_ptr.get()->key,localdata,batch_id,item.version_index,fetched_item.value);
//   }
  
//   return true;
// }

// bool DTX::ReExeLocalRW(coro_yield_t& yield) {
//   auto batch = local_batch_store[thread_gid]->GetBatchByIndex(batch_index,batch_id);
//   for (auto& item : read_only_set) {
//     if (item.is_fetched) continue;
//     auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
//     // !遍历版本，获取到需要的值
//     auto fetched_item = localdata->GetDTXVersion(item.read_version_dtx, item.version_index);
//     auto* it = item.item_ptr.get();
//     // auto* ver = item.version_ptr.get();
//     *it = *fetched_item.value;
//     // it = fetched_item->value.get();
//     // item.version_ptr = std::make_shared<LVersion>(fetched_item);
//     // 这里先假设了读取到的是对的
//     printf("dtx_local.cc:173 txn %p tid %ld try to get version for table %ld key %ld data %p in batch %ld value index %ld value %s\n", this, tx_id, item.item_ptr.get()->table_id, item.item_ptr.get()->key,localdata,batch_id,item.version_index,fetched_item.value);
//   }

//   for (size_t i = 0; i < read_write_set.size(); i++) {
//     if (read_write_set[i].is_fetched) continue;
//     auto localdata = batch->local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
//     // !获取自己写入的版本
//     auto fetched_item = localdata->GetDTXVersionWithDataItem(this, read_write_set[i].version_index);
//     auto* it = read_write_set[i].item_ptr.get();
//     // auto* ver = read_write_set[i].version_ptr.get();
//     // it = fetched_item->value.get();
//     assert(fetched_item.value->value[0] != 0);
//     *it = *fetched_item.value;
//     printf("dtx_local.cc:187 txn %p tid %ld try to rewrite version for table %ld key %ld data %p in batch %ld value index %ld value %s\n", this, tx_id, read_write_set[i].item_ptr.get()->table_id, read_write_set[i].item_ptr.get()->key,localdata,batch_id,read_write_set[i].version_index,fetched_item.value);
//   }
//   return true;
//   // !接下来得进行tpcc等负载计算了
// }
