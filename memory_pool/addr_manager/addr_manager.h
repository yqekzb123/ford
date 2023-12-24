// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "memstore/data_item.h"
#include "memstore/page_store.h"
#include "memstore/hash_store.h"
#include "memstore/hash_index_store.h"
#include "rlib/rdma_ctrl.hpp"
#include "base/page.h"

// Load DB
#include "micro/micro_db.h"
#include "smallbank/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"

using namespace rdmaio;

class AddrManager {
 public:
  AddrManager(int nid, int local_port, int local_meta_port, size_t index_size, size_t txn_list_size)
      : server_node_id(nid),
        local_port(local_port),
        local_meta_port(local_meta_port),
        index_size(index_size),
        txn_list_size(txn_list_size),
        use_pm(0),
        addr_index(nullptr),
        txn_list(nullptr) {}

  ~AddrManager() {
    RDMA_LOG(INFO) << "Do server cleaning...";
    if (addr_index) {
      free(addr_index);
      RDMA_LOG(INFO) << "Free address index";
    }
    if (txn_list) {
      free(txn_list);
      RDMA_LOG(INFO) << "free transaction list";
    }
  }

  void AllocMem();

  void InitMem();

  void InitIndex(){};

  void InitAddr();

  void InitRDMA();

  void SendMeta(node_id_t machine_id, size_t compute_node_num);

  void PrepareHashMeta(node_id_t machine_id, std::string& workload, char** hash_meta_buffer, size_t& total_meta_size);

  void SendHashMeta(char* hash_meta_buffer, size_t& total_meta_size){};

  void CleanTable(){};

  void CleanQP();

  bool Run();

 private:
  const int server_node_id;

  const int local_port;

  const int local_meta_port;

  const size_t index_size;

  const size_t txn_list_size;

  const int use_pm;

  RdmaCtrlPtr rdma_ctrl;

  // The page cache address index
  char* addr_index;
  AddrStore* addr_store; // 也是上面的addr_index索引
  // The transaction operation list
  char* txn_list;

  // For server-side workload
  TATP* tatp_server = nullptr;

  SmallBank* smallbank_server = nullptr;

  TPCC* tpcc_server = nullptr;
  
  MICRO* micro_server = nullptr;
};
