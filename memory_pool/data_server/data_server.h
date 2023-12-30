// Author: huangdund
// Copyrigth (c) 2023

#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "memstore/data_store.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

class DataStoreServer {
 public:
  DataStoreServer(int nid, int local_port, int local_meta_port, size_t data_page_buffer_size)
      : server_node_id(nid),
        local_port(local_port),
        local_meta_port(local_meta_port),
        data_page_buffer_size(data_page_buffer_size){}

  ~DataStoreServer() {
    RDMA_LOG(INFO) << "Do server cleaning...";
  }

  void AllocMem();

  void InitMem();

  void InitRDMA();

  void LoadDataStore(int page_num);

  void SendMeta(node_id_t machine_id, size_t compute_node_num);

  void PrepareDataStoreMeta(node_id_t machine_id, char** hash_meta_buffer, size_t& total_meta_size);

  void SendDataStoreMeta(char* hash_meta_buffer, size_t& total_meta_size);

  void CleanDataStore();

  void CleanQP();

  bool Run();

 private:
  const int server_node_id;

  const int local_port;

  const int local_meta_port;

  const size_t data_page_buffer_size;

  RdmaCtrlPtr rdma_ctrl;

  DataStore* data_store;

  // The start address of the data store
  char* data_page_buffer;
  char* data_page_reserve_buffer;
};
