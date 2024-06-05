// Author: Hongyao Zhao
// Copyright (c) 2023
#pragma once 
#include "local_exec/local_cluster.h"

#define BATCH_CORO_TIMES 1

extern MinHashStore min_hash_store;

extern int LOCAL_BATCH_TXN_SIZE;

extern __thread MetaManager* meta_man;
extern __thread QPManager* qp_man;

extern __thread VersionCache* status;
extern __thread LockCache* lock_table;

extern __thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler

extern __thread RDMABufferAllocator* rdma_buffer_allocator;
extern __thread LogOffsetAllocator* log_offset_allocator;
extern __thread AddrCache* addr_cache;
extern __thread IndexCache* index_cache;
extern __thread PageTableCache* page_table_cache;

extern __thread std::list<PageAddress>* free_page_list;
extern __thread std::mutex* free_page_list_mutex;

extern __thread brpc::Channel* data_channel;
extern __thread brpc::Channel* log_channel;