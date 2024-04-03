// Author: Ming Zhang
// Copyright (c) 2022

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"
#include "util/bitmap.h"

// All servers need to load data
void LoadData(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload,
                      RmManager* rm_manager) {
  /************************************* Load Data ***************************************/
  RDMA_LOG(INFO) << "Start loading database data...";
  if (workload == "TATP") {
    TATP* tatp_server = new TATP(rm_manager);
    tatp_server->LoadTable(machine_id, machine_num);
  } else if (workload == "SmallBank") {
    SmallBank* smallbank_server = new SmallBank(rm_manager);
    smallbank_server->LoadTable(machine_id, machine_num);
  } else if (workload == "TPCC") {
    TPCC* tpcc_server = new TPCC(rm_manager);
    tpcc_server->LoadTable(machine_id, machine_num);
  } else if (workload == "MICRO") {
    MICRO* micro_server = new MICRO(rm_manager);
    micro_server->LoadTable(machine_id, machine_num);
  } else if (workload == "ycsb") {
    YCSB* ycsb_server = new YCSB(rm_manager);
    ycsb_server->LoadTable(machine_id, machine_num);
  }else{
    RDMA_LOG(ERROR) << "Unsupported workload: " << workload;
    assert(false);
  }
  RDMA_LOG(INFO) << "Loading table successfully!";
}

void Server::InitRDMA() {
  RDMA_LOG(INFO) << "Start initializing RDMA...";
  rdma_ctrl = std::make_shared<RdmaCtrl>(machine_id_, local_rdma_port_);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(
      rdma_ctrl->register_memory(SERVER_STORAGE_ID, rdma_mem_manager_->GetMemRegion(), 
        sizeof(StorageMemoryRegion), rdma_ctrl->get_device()) == true);
  RDMA_LOG(INFO) << "Register memory success!";
}

void Server::SendMeta(node_id_t machine_id, size_t compute_node_num) {
  // Prepare LockTable meta
  char* storage_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareStorageMeta(machine_id, &storage_meta_buffer, total_meta_size);
  assert(storage_meta_buffer != nullptr);
  assert(total_meta_size != 0);

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++) {
    SendStorageMeta(storage_meta_buffer, total_meta_size);
  }
  free(storage_meta_buffer);
}

void Server::PrepareStorageMeta(node_id_t machine_id, char** storage_meta_buffer, size_t& total_meta_size) {
  // Get LockTable meta
  StorageMeta* storage_meta;
  storage_meta = new StorageMeta((uint64_t)StorageRDMAMemoryManager::log_base_off,
                                 (uint64_t)StorageRDMAMemoryManager::log_head_off,
                                 (uint64_t)StorageRDMAMemoryManager::log_tail_off,
                                 (uint64_t)StorageRDMAMemoryManager::log_cnt_off,
                                 (uint64_t)StorageRDMAMemoryManager::page_cnt_off);

  int storage_meta_len = sizeof(StorageMeta);
  total_meta_size = sizeof(machine_id) + storage_meta_len + sizeof(MEM_STORE_META_END);
  RDMA_LOG(INFO) << "storage total_meta_size: " << total_meta_size;

  *storage_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *storage_meta_buffer;

  // Fill primary hash meta
  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  
  memcpy(local_buf, (char*)storage_meta, storage_meta_len);

  local_buf += storage_meta_len;
  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}

void Server::SendStorageMeta(char* hash_meta_buffer, size_t& total_meta_size) {
  //> Using TCP to send hash meta
  /* --------------- Initialize socket ---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port_);    // change host little endian to big endian
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0) {
    RDMA_LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server binds socket success";
  int max_listen_num = 10;
  if (listen(listen_socket, max_listen_num) < 0) {
    RDMA_LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server listens success";
  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*) &client_addr, &client_socket_length);
  if (from_client_socket < 0) {
    RDMA_LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server accepts success";

  /* --------------- Sending hash metadata ----------------- */
  auto retlen = send(from_client_socket, hash_meta_buffer, total_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "Server sends hash meta error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server sends hash meta success";
  size_t recv_ack_size = 100;
  char* recv_buf = (char*)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);
  if (strcmp(recv_buf, "[ACK]hash_meta_received_from_client") != 0) {
    std::string ack(recv_buf);
    RDMA_LOG(ERROR) << "Client receives hash meta error. Received ack is: " << ack;
  }

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

bool Server::Run() {
  // Now server just waits for user typing quit to finish
  // Server's CPU is not used during one-sided RDMA requests from clients
  printf("====================================================================================================\n");
  printf(
      "Server now runs as a disaggregated mode. No CPU involvement during RDMA-based transaction processing\n"
      "Type c to run another round, type q if you want to exit :)\n");
  while (true) {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q') {
      return false;
    } else if (ch == 'c') {
      return true;
    } else {
      printf("Type c to run another round, type q if you want to exit :)\n");
    }
    usleep(2000);
  }
}

int main(int argc, char* argv[]) {
    // Configure of this server
    std::string config_filepath = "../../../config/storage_node_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);

    auto local_node = json_config.get("local_storage_node");
    node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
    node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
    assert(machine_id >= 0 && machine_id < machine_num);
    int local_rdma_port = (int)local_node.get("local_rdma_port").get_int64();
    int local_rpc_port = (int)local_node.get("local_rpc_port").get_int64();
    int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
    auto log_buf_size_GB = local_node.get("log_buf_size_GB").get_uint64();
    bool use_rdma = (bool)local_node.get("use_rdma").get_bool();
    std::string workload = local_node.get("workload").get_str();

    auto compute_nodes = json_config.get("remote_compute_nodes");
    auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
    size_t compute_node_num = compute_node_ips.size();

    // 在这里开始构造disk_manager, log_manager, server
    auto disk_manager = std::make_shared<DiskManager>();
    auto log_replay = std::make_shared<LogReplay>(disk_manager.get()); 
    auto log_manager = std::make_shared<LogManager>(disk_manager.get(), log_replay.get());
    
    // Init table in disk
    auto buffer_mgr = std::make_shared<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
    auto rm_manager = std::make_shared<RmManager>(disk_manager.get(), buffer_mgr.get());
    LoadData(machine_id, machine_num, workload, rm_manager.get());
    buffer_mgr->flush_all_pages();

    auto rdma_mem_manager_ = std::make_shared<StorageRDMAMemoryManager>(log_manager.get(), disk_manager.get());
    auto server = std::make_shared<Server>(machine_id, local_rdma_port, local_rpc_port, local_meta_port, use_rdma, \
      compute_node_num, disk_manager.get(), log_manager.get(), rdma_mem_manager_.get());
    
    return 0;
}