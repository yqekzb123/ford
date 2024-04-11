// Author: Hongyao Zhao
// Copyright (c) 2023
#pragma once 
#include "local_exec/local_lock.h"
#include "batch/local_batch.h"

#define OPEN_TIME false
#define RETURN_BEFORE true
// true: RPC, false: RDMA
#define LOG_RPC_OR_RDMA false 
#define SYS_ONE_WRITE true

#define DEBUG_TIME(...) \
  if(OPEN_TIME) { \
    fprintf(stdout,__VA_ARGS__); \
    fflush(stdout); \
  }

extern LocalLockStore local_lock_store;
extern LocalBatchStore **local_batch_store;

// Stat the commit rate
extern uint64_t commit_times;

extern int WARMUP_BATCHCNT;
extern __thread size_t ATTEMPTED_NUM;
extern __thread bool stop_run;
// Performance measurement (thread granularity)
extern __thread struct timespec msr_start;
extern __thread struct timespec msr_end;
extern __thread double* timer;
extern __thread uint64_t stat_attempted_tx_total;  // Issued transaction number
extern __thread uint64_t stat_committed_tx_total;  // Committed transaction number
extern __thread uint64_t stat_aborted_tx_total;  // Aborted transaction number
// const coro_id_t BATCH_TXN_ID = 0;

// Stat the commit rate
extern __thread uint64_t* thread_local_try_times;
extern __thread uint64_t* thread_local_commit_times;

extern uint64_t shared_lock_abort_cnt;
extern uint64_t exlusive_lock_abort_cnt;

extern __thread t_id_t thread_gid;

// uint64_t GetBid(uint64_t key);
