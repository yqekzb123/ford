// Author: Hongyao Zhao
// Copyright (c) 2023
#include "global.h"
#include "local_exec/local_lock.h"
#include "batch/local_batch.h"

LocalLockStore local_lock_store;
LocalBatchStore **local_batch_store;
MinHashStore min_hash_store;

uint64_t commit_times = 0;

int WARMUP_BATCHCNT = 100;
int LOCAL_BATCH_TXN_SIZE = 50;

__thread size_t ATTEMPTED_NUM;
__thread bool stop_run = false;
// Performance measurement (thread granularity)
__thread struct timespec msr_start;
__thread struct timespec msr_end;
__thread double* timer;
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number
__thread uint64_t stat_aborted_tx_total = 0;  // Aborted transaction number

// const coro_id_t BATCH_TXN_ID = 0;

// Stat the commit rate
__thread uint64_t* thread_local_try_times;
__thread uint64_t* thread_local_commit_times;

// Stat the abort
uint64_t shared_lock_abort_cnt;
uint64_t exlusive_lock_abort_cnt;

__thread MetaManager* meta_man;
__thread QPManager* qp_man;

__thread VersionCache* status;
__thread LockCache* lock_table;

__thread t_id_t thread_gid;
__thread t_id_t thread_num;

__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler

__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;
__thread AddrCache* addr_cache;
__thread IndexCache* index_cache;
__thread PageTableCache* page_table_cache;

__thread std::list<PageAddress>* free_page_list;
__thread std::mutex* free_page_list_mutex;

__thread brpc::Channel* data_channel;
__thread brpc::Channel* log_channel;