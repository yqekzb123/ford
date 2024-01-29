// Author: Hongyao Zhao
// Copyright (c) 2023
#include "global.h"
#include "local_exec/local_lock.h"
#include "batch/local_batch.h"

LocalLockStore local_lock_store;
LocalBatchStore local_batch_store;

uint64_t commit_times = 0;
