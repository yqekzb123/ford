// Author: Hongyao Zhao
// Copyright (c) 2023
#pragma once 
#include "local_exec/local_cluster.h"

#define BATCH_CORO_TIMES 1

extern MinHashStore min_hash_store;

extern int LOCAL_BATCH_TXN_SIZE;

extern __thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler