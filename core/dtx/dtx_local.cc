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
    if (item.is_fetched) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对只读操作加锁
    bool success = localdata->LockShared();
    if (!success) return false;
  }
  return true;
}

bool DTX::LockLocalRW(coro_yield_t& yield) {
  // printf("dtx_local.cc:30\n");
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对只读操作加锁
    bool success = localdata->LockShared();
    if (!success) return false;
  }
  // printf("dtx_local.cc:40\n");
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto localdata = local_lock_store.GetLock(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // !加锁
    bool success = localdata->LockExclusive();
    if (!success) return false;
  }
  printf("dtx_local.cc:50\n");
  return true;
}

bool DTX::ExeLocalRO(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  auto batch = local_batch_store.GetBatchById(batch_id);
  // Issue reads
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " issue read ro";
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !将只读操作存入操作集
    item.read_version_dtx = localdata->GetTailVersion()->txn;
  }

  return true;
}

bool DTX::ExeLocalRW(coro_yield_t& yield) {
  // printf("dtx_local.cc:30\n");
  auto batch = local_batch_store.GetBatchById(batch_id);
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    // !对当前读取到的最新版本打标记
    item.read_version_dtx = localdata->GetTailVersion()->txn;
  }
  // printf("dtx_local.cc:40\n");
  for (size_t i = 0; i < read_write_set.size(); i++) {
    // printf("dtx_local.cc:42\n");
    if (read_write_set[i].is_fetched) continue;
    // printf("dtx_local.cc:44\n");
    auto localdata = batch->local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    // printf("dtx_local.cc:46\n");
    // !创建新版本
    localdata->CreateNewVersion(this);
  }
  printf("dtx_local.cc:50\n");
  return true;
}

bool DTX::LocalCommit(coro_yield_t& yield, BenchDTX* dtx_with_bench) {
  printf("dtx_local.cc:54\n");
  bool res = true;
  //! 1.将生成好的读写集，塞到batch中
  auto batch = local_batch_store.InsertTxn(dtx_with_bench);
  batch_id = batch->batch_id;
  if (read_write_set.empty()) {
    ExeLocalRO(yield);
  } else {
    ExeLocalRW(yield);
  }
  batch->EndInsertTxn();

  //! 2.本地释放锁
  for (auto& item : read_only_set) {
    auto localdata = local_lock_store.GetLock(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
    localdata->UnlockShared();
  }
  for (size_t i = 0; i < read_write_set.size(); i++) {
    auto localdata = local_lock_store.GetLock(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
    localdata->UnlockExclusive();
  }
  return res;
}

bool DTX::ReExeLocalRO(coro_yield_t& yield) {
  auto batch = local_batch_store.GetBatchById(batch_id);
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
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
  auto batch = local_batch_store.GetBatchById(batch_id);
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto localdata = batch->local_data_store.GetData(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
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
    auto localdata = batch->local_data_store.GetData(read_write_set[i].item_ptr.get()->table_id,read_write_set[i].item_ptr.get()->key);
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
