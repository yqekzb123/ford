// Author: Hongyao Zhao
// Copyright (c) 2023
#include "global.h"
#include "local_exec/local_lock.h"
#include "batch/local_batch.h"

LocalLockStore local_lock_store;
LocalBatchStore local_batch_store;

uint64_t commit_times = 0;

int WARMUP_BATCHCNT = 1000;

__thread size_t ATTEMPTED_NUM;
bool stop_run = false;
// Performance measurement (thread granularity)
struct timespec msr_start;
__thread struct timespec msr_end;
__thread double* timer;
__thread uint64_t stat_attempted_tx_total = 0;  // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0;  // Committed transaction number

// const coro_id_t BATCH_TXN_ID = 0;

// Stat the commit rate
__thread uint64_t* thread_local_try_times;
__thread uint64_t* thread_local_commit_times;

// Stat the abort
uint64_t shared_lock_abort_cnt;
uint64_t exlusive_lock_abort_cnt;