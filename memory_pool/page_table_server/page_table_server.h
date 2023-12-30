// Author: huangdund
// Copyrigth (c) 2023

#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "memstore/page_table.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

class PageTableServer {
 public:
  PageTableServer(int nid, int local_port, int local_meta_port, size_t page_table_buffer_size)
      : server_node_id(nid),
        local_port(local_port),
        local_meta_port(local_meta_port),
        page_table_buffer_size(page_table_buffer_size){}

  ~PageTableServer() {
    RDMA_LOG(INFO) << "Do server cleaning...";
  }

  void AllocMem();

  void InitMem();

  void InitRDMA();

  void LoadPageTableStore();

  void ConnectWithRingBuffer();
  
  void SendMeta(node_id_t machine_id, size_t compute_node_num);

  void PreparePageTableStoreMeta(node_id_t machine_id, char** hash_meta_buffer, size_t& total_meta_size);

  void SendPageTableStoreMeta(char* hash_meta_buffer, size_t& total_meta_size);

  void CleanPageTableStore();

  void CleanQP();

  bool Run();

 private:
  const int server_node_id;

  const int local_port;

  const int local_meta_port;

  const size_t page_table_buffer_size;

  RdmaCtrlPtr rdma_ctrl;

  PageTableStore* page_table_store;

  // The start address of the data store
  char* page_table_buffer;
  char* page_table_reserve_buffer;
};
