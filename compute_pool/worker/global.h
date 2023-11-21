// Author: Hongyao Zhao
// Copyright (c) 2023
#pragma once 
#include "local_exec/local_data.h"
#include "batch/local_batch.h"

LocalDataStore local_data_store;
LocalBatchStore local_batch_store;
node_id_t g_machine_id;
node_id_t g_machine_num;