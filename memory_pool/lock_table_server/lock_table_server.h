// Author: huangdund
// Copyrigth (c) 2023

#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "memstore/lock_table_store.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

class LockTableServer {
 public:
  LockTableServer(int nid, int local_port, int local_meta_port, size_t lock_table_buf_size)
      : server_node_id(nid),
        local_port(local_port),
        local_meta_port(local_meta_port),
        lock_table_buf_size(lock_table_buf_size){}

  ~LockTableServer() {
    RDMA_LOG(INFO) << "Do server cleaning...";
  }

  void AllocMem();

  void InitMem();

  void InitRDMA();

  void LoadLockTable(int bucket_num);

  void SendMeta(node_id_t machine_id, size_t compute_node_num);

  void PrepareLockTableMeta(node_id_t machine_id, char** hash_meta_buffer, size_t& total_meta_size);

  void SendLockTableMeta(char* hash_meta_buffer, size_t& total_meta_size);

  void CleanLockTable();

  void CleanQP();

  bool Run();

 private:
  const int server_node_id;

  const int local_port;

  const int local_meta_port;

  const size_t lock_table_buf_size;

  RdmaCtrlPtr rdma_ctrl;

  LockTableStore* locktable_store;

  // The start address of the whole hash store space
  char* lock_table_bucket_buffer;
  
  // The start address of the reserved space in hash store. For insertion in case of conflict in a full bucket
  char* lock_table_reserve_buffer;
};
