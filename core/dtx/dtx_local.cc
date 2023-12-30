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
