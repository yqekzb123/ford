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
    tatp_server = new TATP(nullptr); // not need rmmanager
    // TODO
  } else if (workload == "SmallBank") {
    smallbank_server = new SmallBank(nullptr);
    smallbank_server->LoadIndex(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  } else if (workload == "TPCC") {
    tpcc_server = new TPCC();
    // tpcc_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  } else if (workload == "MICRO") {
    micro_server = new MICRO(nullptr);
    // micro_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  }
  RDMA_LOG(INFO) << "Loading table successfully!";
}

void HashIndexServer::CleanIndex() {
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
}

void HashIndexServer::CleanQP() {
  rdma_ctrl->destroy_rc_qp();
}

void HashIndexServer::SendMeta(node_id_t machine_id, std::string& workload, size_t compute_node_num) {
  // Prepare hash meta
  char* hash_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareIndexMeta(machine_id, workload, &hash_meta_buffer, total_meta_size);
  assert(hash_meta_buffer != nullptr);
  assert(total_meta_size != 0);

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++) {
    SendIndexMeta(hash_meta_buffer, total_meta_size);
  }
  free(hash_meta_buffer);
}

void HashIndexServer::PrepareIndexMeta(node_id_t machine_id, std::string& workload, char** hash_meta_buffer, size_t& total_meta_size) {
  // Get all hash meta
  std::vector<IndexMeta*> hash_index_meta_vec;
  std::vector<IndexStore*> all_index_store;

  if (workload == "TATP") {
    // all_index_store = tatp_server->GetPrimaryHashStore();
  } else if (workload == "SmallBank") {
    all_index_store = smallbank_server->GetAllIndexStore();
  } else if (workload == "TPCC") {
    // all_index_store = tpcc_server->GetPrimaryHashStore();
  } else if (workload == "MICRO") {
    // all_index_store = micro_server->GetPrimaryHashStore();
  }

  for (auto& hash_table : all_index_store) {
    auto* hash_meta = new IndexMeta(hash_table->GetTableID(),
                                   (uint64_t)hash_table->GetIndexPtr(),
                                   hash_table->GetBucketNum(),
                                   hash_table->GetIndexNodeSize(),
                                   hash_table->GetBaseOff(),
                                   hash_table->GetExpandBaseOff());
    hash_index_meta_vec.emplace_back(hash_meta);
  }

  int hash_meta_len = sizeof(IndexMeta);
  size_t hash_index_meta_num = hash_index_meta_vec.size();
  RDMA_LOG(INFO) << "primary hash meta num: " << hash_index_meta_num;
  total_meta_size = sizeof(hash_index_meta_num) + sizeof(machine_id) + hash_index_meta_num * hash_meta_len + sizeof(MEM_STORE_META_END);
  *hash_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *hash_meta_buffer;

  // Fill primary hash meta
  *((size_t*)local_buf) = hash_index_meta_num;
  local_buf += sizeof(hash_index_meta_num);
  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  for (size_t i = 0; i < hash_index_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)hash_index_meta_vec[i], hash_meta_len);
  }
  local_buf += hash_index_meta_num * hash_meta_len;
  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}


void HashIndexServer::SendIndexMeta(char* hash_meta_buffer, size_t& total_meta_size) {
  //> Using TCP to send hash meta
  /* --------------- Initialize socket ---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port);    // change host little endian to big endian
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

bool HashIndexServer::Run() {
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
  std::string config_filepath = "../../../config/memory_index_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_index_node");
  node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);
  int local_port = (int)local_node.get("local_port").get_int64();
  int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
  std::string workload = local_node.get("workload").get_str();
  auto mem_size_GB = local_node.get("mem_size_GB").get_uint64();

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
  size_t compute_node_num = compute_node_ips.size();

  size_t mem_size = (size_t)1024 * 1024 * 1024 * mem_size_GB;
  size_t hash_buf_size = mem_size;  // Currently, we support the hash structure

  auto server = std::make_shared<HashIndexServer>(machine_id, local_port, local_meta_port, hash_buf_size);
  server->AllocMem();
  server->InitMem();
  server->LoadIndex(machine_id, machine_num, workload);
  server->SendMeta(machine_id, workload, compute_node_num);
  server->InitRDMA();
  bool run_next_round = server->Run();

  // Continue to run the next round. RDMA does not need to be inited twice
  while (run_next_round) {
    server->InitMem();
    server->CleanIndex();
    server->CleanQP();
    server->LoadIndex(machine_id, machine_num, workload);
    server->SendMeta(machine_id, workload, compute_node_num);
    run_next_round = server->Run();
  }

  // Stat the cpu utilization
  auto pid = getpid();
  std::string copy_cmd = "cp /proc/" + std::to_string(pid) + "/stat ./";
  system(copy_cmd.c_str());

  copy_cmd = "cp /proc/uptime ./";
  system(copy_cmd.c_str());
  return 0;
}