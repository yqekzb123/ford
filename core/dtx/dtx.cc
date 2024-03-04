// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

DTX::DTX(MetaManager* meta_man,
         QPManager* qp_man,
         VersionCache* status,
         LockCache* lock_table,
         t_id_t tid,
         coro_id_t coroid,
         CoroutineScheduler* sched,
         RDMABufferAllocator* rdma_buffer_allocator,
         LogOffsetAllocator* remote_log_offset_allocator,
         AddrCache* addr_buf,
         std::list<PageAddress>* _free_page_list, 
         std::mutex* _free_page_list_mutex,
         brpc::Channel* data_channel,
         brpc::Channel* log_channel) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  thread_qp_man = qp_man;
  global_vcache = status;
  global_lcache = lock_table;
  thread_rdma_buffer_alloc = rdma_buffer_allocator;
  tx_status = TXStatus::TX_INIT;

  free_page_list = _free_page_list;
  free_page_list_mutex = _free_page_list_mutex;

  select_backup = 0;
  // thread_remote_log_offset_alloc = remote_log_offset_allocator;
  addr_cache = addr_buf;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;

  storage_data_channel = data_channel;
  storage_log_channel = log_channel;
}

void DTX::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  // char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  // *((lock_t*)unlock_buf) = 0;
  // for (auto& index : locked_rw_set) {
  //   auto& it = read_write_set[index].item_ptr;
  //   node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
  //   RCQP* primary_qp = thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
  //   auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf, sizeof(lock_t), it->GetRemoteLockAddr(), 0);
  //   if (rc != SUCC) {
  //     RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id << " unlock fails during abortion";
  //   }
  // }
  tx_status = TXStatus::TX_ABORT;
}