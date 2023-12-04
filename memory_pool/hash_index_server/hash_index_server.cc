// Author: huangdund
// Copyrigth (c) 2023

#include "hash_index_server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"

void HashIndexServer::AllocMem() {
  RDMA_LOG(INFO) << "Start allocating memory...";
  hash_index_bucket_buffer = (char*)malloc(hash_buf_size);
  assert(hash_index_bucket_buffer);
  RDMA_LOG(INFO) << "Alloc DRAM data region success!";

  offset_t reserve_start = hash_buf_size * 0.75;  // reserve 1/4 for hash conflict in case of full bucket
  hash_index_reserve_buffer = hash_index_bucket_buffer + reserve_start;
}

void HashIndexServer::InitMem() {
  RDMA_LOG(INFO) << "Start initializing memory...";
  memset(hash_index_bucket_buffer, 0, hash_buf_size);
  RDMA_LOG(INFO) << "Init DRAM data region success!";
}

void HashIndexServer::InitRDMA() {
  RDMA_LOG(INFO) << "Start initializing RDMA...";
  rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(
      rdma_ctrl->register_memory(SERVER_HASH_INDEX_ID, hash_index_bucket_buffer, hash_buf_size, rdma_ctrl->get_device()) == true);
  RDMA_LOG(INFO) << "Register memory success!";
}

// All servers need to load index
void HashIndexServer::LoadIndex(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload) {
  /************************************* Load Index ***************************************/
  RDMA_LOG(INFO) << "Start loading database hash index...";
  // Init index
  MemStoreAllocParam mem_store_alloc_param(hash_index_bucket_buffer, hash_index_bucket_buffer, 0, hash_index_reserve_buffer);
  MemStoreReserveParam mem_store_reserve_param(hash_index_reserve_buffer, 0, hash_index_bucket_buffer + hash_buf_size);
  if (workload == "TATP") {
    tatp_server = new TATP();
    tatp_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  } else if (workload == "SmallBank") {
    smallbank_server = new SmallBank();
    smallbank_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  } else if (workload == "TPCC") {
    tpcc_server = new TPCC();
    tpcc_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  } else if (workload == "MICRO") {
    micro_server = new MICRO();
    micro_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  }
  RDMA_LOG(INFO) << "Loading table successfully!";
}

