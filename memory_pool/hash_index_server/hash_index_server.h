// Author: huangdund
// Copyrigth (c) 2023

#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "memstore/hash_index_store.h"
#include "rlib/rdma_ctrl.hpp"

// Load DB
#include "micro/micro_db.h"
#include "smallbank_batch/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"

using namespace rdmaio;

class HashIndexServer {
 public:
  HashIndexServer(int nid, int local_port, int local_meta_port, size_t hash_buf_size)
      : server_node_id(nid),
        local_port(local_port),
        local_meta_port(local_meta_port),
        hash_buf_size(hash_buf_size){}

  ~HashIndexServer() {
    RDMA_LOG(INFO) << "Do server cleaning...";
    if (tatp_server) {
      delete tatp_server;
      RDMA_LOG(INFO) << "delete tatp tables";
    }

    if (smallbank_server) {
      delete smallbank_server;
      RDMA_LOG(INFO) << "delete smallbank tables";
    }

    if (tpcc_server) {
      delete tpcc_server;
      RDMA_LOG(INFO) << "delete tpcc tables";
    }

    if (micro_server) {
      delete micro_server;
      RDMA_LOG(INFO) << "delete micro tables";
    }

  }

  void AllocMem();

  void InitMem();

  void InitRDMA();

  void LoadIndex(node_id_t machine_id, node_id_t machine_num, std::string& workload);

  void SendMeta(node_id_t machine_id, std::string& workload, size_t compute_node_num);

  void PrepareHashMeta(node_id_t machine_id, std::string& workload, char** hash_meta_buffer, size_t& total_meta_size);

  void SendHashMeta(char* hash_meta_buffer, size_t& total_meta_size);

  void CleanTable();

  void CleanQP();

  bool Run();

 private:
  const int server_node_id;

  const int local_port;

  const int local_meta_port;

  const size_t hash_buf_size;

  RdmaCtrlPtr rdma_ctrl;

  // The start address of the whole hash store space
  char* hash_index_bucket_buffer;
  
  // The start address of the reserved space in hash store. For insertion in case of conflict in a full bucket
  char* hash_index_reserve_buffer;

  // For server-side workload
  TATP* tatp_server = nullptr;

  SmallBank* smallbank_server = nullptr;

  TPCC* tpcc_server = nullptr;
  
  MICRO* micro_server = nullptr;
};
