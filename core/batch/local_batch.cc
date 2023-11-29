// Author: Hongyao Zhao
// Copyright (c) 2023

#include "batch/local_batch.h"
#include "worker/global.h"
#include "dtx/dtx.h"
#include "base/page.h"

bool LocalBatch::ExeBatchRW(coro_yield_t& yield) {
  // 先随便整个dtx结构出来
  bool res = true;
  BenchDTX* first_bdtx = txn_list.front();
  DTX* first_dtx = first_bdtx->dtx;
  
  //! 0.统计只读和读写的操作列表
  std::vector<table_id_t> readonly_tableid;
  std::vector<itemkey_t> readonly_keyid;
  for (auto& dtx : txn_list) {
    for (auto& item : dtx->dtx->read_only_set) {
      auto it = item.item_ptr;
      readonly_tableid.push_back(it->table_id);
      readonly_keyid.push_back(it->key);
    }
  }
  std::vector<table_id_t> readwrite_tableid;
  std::vector<itemkey_t> readwrite_keyid;
  for (auto& dtx : txn_list) {
    for (auto& item : dtx->dtx->read_write_set) {
      auto it = item.item_ptr;
      readonly_tableid.push_back(it->table_id);
      readonly_keyid.push_back(it->key);
    }
  }
  
  std::vector<table_id_t> all_tableid;
  std::vector<itemkey_t> all_keyid;
  std::merge(readonly_tableid.begin(), readonly_tableid.end(),
             readwrite_tableid.begin(), readwrite_tableid.end(),
             all_tableid.begin());
  std::merge(readonly_keyid.begin(), readonly_keyid.end(),
             readwrite_keyid.begin(), readwrite_keyid.end(),
             all_keyid.begin());
  
  //! 1. 对事务访问的数据项加锁
  // 只读加锁
  res = first_dtx->LockSharedOnRecord(yield, readonly_tableid, readonly_keyid);
  if (!res) {
    // !失败以后所有操作解锁
    // first_dtx->UnlockSharedOnRecord(yield, readonly_tableid, readonly_keyid);
    return res;
  }
  // 读写加锁
  res = first_dtx->LockExclusiveOnRecord(yield, readwrite_tableid, readwrite_keyid);
  if (!res) {
    // !失败以后所有操作解锁
    // first_dtx->Unlock();
    // ExclusiveOnRecord(yield, readonly_tableid, readonly_keyid);
    return res;
  }
  //! 2. 获取数据项索引
  auto index = first_dtx->GetHashIndex(yield, all_tableid, all_keyid);
  //! 3. 读取数据项
  auto data_list = ReadData(yield, first_dtx,index);
  //! 4. 从页中取出数据，将数据存入local，还没想好存到哪
  for (auto& item : data_list) {
    LocalData* data_item = local_data_store.GetData(item->table_id, item->key);
    data_item->SetFirstVersion(item.get());
  }

  //! 5. 根据数据项进行本地计算
  for (auto& dtx : txn_list) {
    // !连续的本地计算,这里还需要增加tpcc等负载的运算内容 
    res = dtx->TxReCaculate(yield);
  }

  //! 6. 将确定的数据，利用RDMA刷入页中
  FlushWrite(yield, first_dtx,data_list,index);

  return res;
}

std::vector<DataItemPtr> LocalBatch::ReadData(coro_yield_t& yield, DTX* first_dtx, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index) {
  // 遍历index，获取其中的页表地址
  std::vector<Rid> id_list;
  std::vector<table_id_t> tid_list;
  std::vector<DTX::FetchPageType> fetch_type;
  std::vector<PageAddress> page_address;
  for (auto& rid_map : index) {
    table_id_t tid = rid_map.first;
    for (auto& rid : rid_map.second) {
      id_list.push_back(rid.second);
      tid_list.push_back(tid);
      fetch_type.push_back(DTX::FetchPageType::kReadPage);
    }
  }
  std::vector<DataItemPtr> data_list = first_dtx->FetchTuple(yield, tid_list, id_list, fetch_type, batch_id, page_address);
  return data_list;
}

bool LocalBatch::FlushWrite(coro_yield_t& yield, DTX* first_dtx, std::vector<DataItemPtr> data_list, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index) {
  std::vector<Rid> id_list;
  std::vector<table_id_t> tid_list;
  std::vector<DataItemPtr> data_list;

  std::vector<DTX::FetchPageType> fetch_type;
  std::vector<PageAddress> page_address;

  for (auto& item : data_list) {
    tid_list.push_back(item->table_id);
    id_list.push_back(index[item->table_id][item->key]);
    LocalData* data_item = local_data_store.GetData(item->table_id, item->key);
    LVersion* v = data_item->GetTailVersion();
    data_list.push_back(v->value);
  }
  first_dtx->WriteTuple(yield, tid_list, id_list, fetch_type, data_list,batch_id, page_address);
  return true;
}