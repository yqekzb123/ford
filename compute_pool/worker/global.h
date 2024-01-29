// Author: Hongyao Zhao
// Copyright (c) 2023
#pragma once 
#include "local_exec/local_lock.h"
#include "batch/local_batch.h"

extern LocalLockStore local_lock_store;
extern LocalBatchStore local_batch_store;

// Stat the commit rate
extern uint64_t commit_times;
