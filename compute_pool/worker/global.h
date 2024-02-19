// Author: Hongyao Zhao
// Copyright (c) 2023
#pragma once 
#include "local_exec/local_lock.h"
#include "batch/local_batch.h"

extern LocalLockStore local_lock_store;
extern LocalBatchStore local_batch_store;

// Stat the commit rate
extern uint64_t commit_times;

extern __thread size_t ATTEMPTED_NUM;
extern __thread bool stop_run;
// Performance measurement (thread granularity)
extern __thread struct timespec msr_start, msr_end;
extern __thread double* timer;
extern __thread uint64_t stat_attempted_tx_total;  // Issued transaction number
extern __thread uint64_t stat_committed_tx_total;  // Committed transaction number
// const coro_id_t BATCH_TXN_ID = 0;

// Stat the commit rate
extern __thread uint64_t* thread_local_try_times;
extern __thread uint64_t* thread_local_commit_times;