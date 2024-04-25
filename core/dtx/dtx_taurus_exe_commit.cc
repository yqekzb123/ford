// Author: Chunyue Huang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "worker/global.h"
#include "exception.h"
#include "taurusmm/glm.pb.h"
#include "taurusmm/master_log.pb.h"
#include <unordered_set>

bool DTX::TxTaurusTxnExe(coro_yield_t& yield, bool fail_abort) {
  try {
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

    assert(global_meta_man->txn_system == DTX_SYS::TAURUS);
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

    if (!LockTaurus(yield)) {
      // printf("LockRemoteRO failed\n");
      TxTaurusAbort(yield);
      return false;
    } 

    #if OPEN_TIME
    struct timespec tx_lock_rw_time;
    clock_gettime(CLOCK_REALTIME, &tx_lock_rw_time);
    double lock_rw_usec = (tx_lock_rw_time.tv_sec - tx_get_index_time.tv_sec) * 1000000 + (double)(tx_lock_rw_time.tv_nsec - tx_get_index_time.tv_nsec) / 1000;
    #endif

    if (!TaurusRead()) {
      TxTaurusAbort(yield);
      return false;
    }

    #if OPEN_TIME
    struct timespec tx_read_time;
    clock_gettime(CLOCK_REALTIME, &tx_read_time);
    double read_usec = (tx_read_time.tv_sec - tx_lock_rw_time.tv_sec) * 1000000 + (double)(tx_read_time.tv_nsec - tx_lock_rw_time.tv_nsec) / 1000;
    DEBUG_TIME("dtx_base_exe_commit.cc:46, exe a new txn %ld, read_index_usec: %lf, \
       lock_usec: %lf, read_usec: %lf\n", tx_id, get_index_usec, lock_rw_usec, read_usec);
    #endif
  }
  catch(const AbortException& e) {
    TxTaurusAbort(yield);
    return false;
  }
  return true;
// ABORT:
  // if (fail_abort) TxTaurusAbort(yield);
  // return false;
}

bool DTX::TxTaurusTxnCommit(coro_yield_t& yield) {
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
  
  // 发送日志到其他master
  SendLogToMasters();

  #if OPEN_TIME
  struct timespec tx_send_log_time;
  clock_gettime(CLOCK_REALTIME, &tx_send_log_time);
  double send_log_usec = (tx_send_log_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_send_log_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
  #endif

  TaurusUnpin();

  #if OPEN_TIME
  struct timespec tx_unpin_time;
  clock_gettime(CLOCK_REALTIME, &tx_unpin_time);
  double unpin_usec = (tx_unpin_time.tv_sec - tx_send_log_time.tv_sec) * 1000000 + (double)(tx_unpin_time.tv_nsec - tx_send_log_time.tv_nsec) / 1000;
  #endif
  
  UnlockTaurus(yield);

  #if OPEN_TIME
  struct timespec tx_unlock_time;
  clock_gettime(CLOCK_REALTIME, &tx_unlock_time);
  double unlock_usec = (tx_unlock_time.tv_sec - tx_unpin_time.tv_sec) * 1000000 + (double)(tx_unlock_time.tv_nsec - tx_unpin_time.tv_nsec) / 1000;
  DEBUG_TIME("dtx_base_exe_commit.cc:80, exe a new txn %ld, unpin_usec: %lf, send_log_usec: %lf, unlock_usec: %lf\n", tx_id, unpin_usec, send_log_usec, unlock_usec);
  #endif

  #if LOG_RPC_OR_RDMA
  //!! brpc同步
  brpc::Join(cid);
  #endif

  tx_status = TXStatus::TX_COMMIT;
  // printf("txn: %ld, commit\n", tx_id);
  return true;
}

void DTX::TxTaurusAbort(coro_yield_t& yield) {
  TaurusUnpin();
  UnlockTaurus(yield);
  Abort();
}

// Taurus对操作加锁
bool DTX::LockTaurus(coro_yield_t& yield){
    // 在本地对读集和写集加行锁
    if(!LockLocalRW(yield)) return false;
    // 在远程加页锁
    assert(all_rids.size() == all_tableid.size());
    // 生成要远程加锁的pageid
    std::vector<PageId> page_ids;
    for(int i=0; i<read_only_set.size(); i++){
        PageId page_id;
        page_id.table_id = all_tableid[i];
        page_id.page_no = all_rids[i].page_no_;
        // 在这里去重
        if(std::find(page_ids.begin(), page_ids.end(), page_id) != page_ids.end()){
            continue;
        } else{
            page_ids.push_back(page_id);
        }
    }
    std::vector<PageId> page_ids2;
    for(int i=read_only_set.size(); i<all_tableid.size(); i++){
        PageId page_id;
        page_id.table_id = all_tableid[i];
        page_id.page_no = all_rids[i].page_no_;
        // 在这里去重
        if(std::find(page_ids2.begin(), page_ids2.end(), page_id) != page_ids2.end()){
            continue;
        } else{
            page_ids2.push_back(page_id);
        }
    }

    // lock remote use glm
    glm_service::GLMService_Stub stub(glm_channel);
    glm_service::LockRequest request;
    glm_service::LockResponse* response = new glm_service::LockResponse();
    brpc::Controller* cntl = new brpc::Controller;

    for(int i=0; i<page_ids.size(); i++){
        request.add_shared_lock_id();
        request.mutable_shared_lock_id(i)->set_table_id(page_ids[i].table_id);
        request.mutable_shared_lock_id(i)->set_page_id(page_ids[i].page_no);
    }
    for(int i=0; i<page_ids2.size(); i++){
        request.add_exclusive_lock_id();
        request.mutable_exclusive_lock_id(i)->set_table_id(page_ids2[i].table_id);
        request.mutable_exclusive_lock_id(i)->set_page_id(page_ids2[i].page_no);
    }
    stub.LockPage(cntl, &request, response, NULL);
    bool ret = true;
    if(response->shared_fail_request_loc() == -1){
        // lock success
        for(int j=0; j<page_ids.size(); j++){
            // 记录获取的读锁
            glm_hold_sharedlock_page_ids.push_back(page_ids[j]);
        }
    } else{
        for(int j=0; j<response->shared_fail_request_loc(); j++){
            // 记录获取的读锁
            glm_hold_sharedlock_page_ids.push_back(page_ids[j]);
        }
        ret = false;
    }
    if(response->exclusive_fail_request_loc() == -1){
        // lock success
        for(int j=0; j<page_ids2.size(); j++){
            // 记录获取的读锁
            glm_hold_exclusivelock_page_ids.push_back(page_ids2[j]);
        }
    } else{
        for(int j=0; j<response->exclusive_fail_request_loc(); j++){
            // 记录获取的读锁
            glm_hold_exclusivelock_page_ids.push_back(page_ids2[j]);
        }
        ret = false;
    }
    return ret;
}

bool DTX::UnlockTaurus(coro_yield_t& yield){
    // 在远程解锁
    // lock remote use glm
    glm_service::GLMService_Stub stub(glm_channel);
    glm_service::UnLockRequest request;
    glm_service::UnLockResponse response;
    brpc::Controller* cntl = new brpc::Controller;

    for(int i=0; i<glm_hold_sharedlock_page_ids.size(); i++){
        request.add_shared_unlock_id();
        request.mutable_shared_unlock_id(i)->set_table_id(glm_hold_sharedlock_page_ids[i].table_id);
        request.mutable_shared_unlock_id(i)->set_page_id(glm_hold_sharedlock_page_ids[i].page_no);
    }
    for(int i=0; i<glm_hold_exclusivelock_page_ids.size(); i++){
        request.add_exclusive_unlock_id();
        request.mutable_exclusive_unlock_id(i)->set_table_id(glm_hold_exclusivelock_page_ids[i].table_id);
        request.mutable_exclusive_unlock_id(i)->set_page_id(glm_hold_exclusivelock_page_ids[i].page_no);
    }
    stub.UnLockPage(cntl, &request, &response, NULL);
    glm_hold_exclusivelock_page_ids.clear();
    glm_hold_sharedlock_page_ids.clear();

    UnLockLocalRW();
}

bool DTX::TaurusRead() {
  #if OPEN_TIME
  struct timespec tx_start_time;
  clock_gettime(CLOCK_REALTIME, &tx_start_time);
  #endif

  // 获取数据项
  std::vector<DataItemPtr> data_list;
  assert(all_rids.size() == all_tableid.size());
  for(int i=0; i<all_rids.size(); i++){
    PageId read_page_id{all_tableid[i], all_rids[i].page_no_};
    Page* page = master_buffer_pool_manager->fetch_page(read_page_id);
    all_pages.push_back(page);
    page->RLatch();
    auto data_item = GetDataItemFromPage(all_tableid[i], page->get_data(), all_rids[i]);
    data_list.push_back(data_item);
  }

  // TODO 获取日志

  // TODO 回放日志

  #if OPEN_TIME
  struct timespec tx_fetch_time;
  clock_gettime(CLOCK_REALTIME, &tx_fetch_time);
  double fetch_usec = (tx_fetch_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_fetch_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
  DEBUG_TIME("dtx_base_exe_commit.cc:168, exe a new txn %ld, fetch_usec: %lf\n", tx_id, fetch_usec);
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

void DTX::TaurusUnpin(){
  for(int i=0; i<all_pages.size(); i++){
    all_pages[i]->RUnlatch();
    master_buffer_pool_manager->unpin_page(all_pages[i]->get_page_id());
  }
}

void DTX::SendLogToMasters(){
  auto ms_nodes = global_meta_man->remote_taurus_master_nodes;
  brpc::ChannelOptions options;
  options.use_rdma = false;
  options.protocol = "baidu_std";
  options.connection_type = "";
  options.timeout_ms = 0x7fffffff;
  options.max_retry = 3;
  for(int i=0; i<ms_nodes.size(); i++){
    brpc::Channel taurus_ms_channel;
    std::string ms_node = ms_nodes[i].ip + ":" + std::to_string(ms_nodes[i].port);
    if(taurus_ms_channel.Init(ms_node.c_str(), &options) != 0) {
      RDMA_LOG(FATAL) << "Fail to initialize channel";
    }
    mslog_service::MS2MSLogRequest request;
    mslog_service::MS2MSLogResponse response;
    brpc::Controller cntl;
    mslog_service::MSLogService_Stub stub(&taurus_ms_channel);

    request.set_ms_id(g_machine_id);
    request.set_lsn(tx_id);
    stub.WriteLSNToMS(&cntl, &request, &response, NULL);
  }
}