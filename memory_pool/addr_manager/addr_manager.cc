// Author: Ming Zhang
// Copyright (c) 2022

#include "addr_manager.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"

void AddrManager::AllocMem() {
  RDMA_LOG(INFO) << "Start allocating memory...";
  addr_index = (char*)malloc(index_size);
  assert(addr_index);
  RDMA_LOG(INFO) << "Alloc DRAM data region success!";

  txn_list = (char*)malloc(txn_list_size);
  assert(txn_list);
}

void AddrManager::InitMem() {
  RDMA_LOG(INFO) << "Start initializing memory...";
  memset(addr_index, 0, index_size);
  memset(txn_list, 0, txn_list_size);
  RDMA_LOG(INFO) << "Initialize memory success!";
}

void AddrManager::InitRDMA() {
  /************************************* RDMA+PM Initialization ***************************************/
  RDMA_LOG(INFO) << "Start initializing RDMA...";
  rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1};  // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(
      rdma_ctrl->register_memory(SERVER_HASH_BUFF_ID, addr_index, index_size, rdma_ctrl->get_device()) == true);
  RDMA_ASSERT(
      rdma_ctrl->register_memory(SERVER_LOG_BUFF_ID, txn_list, txn_list_size, rdma_ctrl->get_device()) == true);
  RDMA_LOG(INFO) << "Register memory success!";
}

void AddrManager::CleanQP() {
  rdma_ctrl->destroy_rc_qp();
}

void AddrManager::InitAddr() {
  MemStoreAllocParam mem_store_alloc_param(addr_index, addr_index, 0, addr_index);
  uint64_t bucket_size = ADDR_HASH_BUCKET_NUM;
  addr_store = new AddrStore(bucket_size, &mem_store_alloc_param);
}

void AddrManager::SendMeta(node_id_t machine_id, size_t compute_node_num) {
  // Prepare hash meta
  size_t total_meta_size = sizeof(node_id_t) + sizeof(uint64_t);
  char* addr_meta_buffer = (char*)malloc(total_meta_size);
  char* off = addr_meta_buffer;
  
  // 生成要发送的日志
  // 目前只有节点号和桶的数量
  node_id_t* node = (node_id_t*) off;
  *node = machine_id;
  off += sizeof(node_id_t);
  uint64_t* bucket_num = (uint64_t*) off;
  *bucket_num = addr_store->GetBucketNum();

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++) {
    SendHashMeta(addr_meta_buffer, total_meta_size);
  }
  free(addr_meta_buffer);
}

void AddrManager::PrepareHashMeta(node_id_t machine_id, std::string& workload, char** hash_meta_buffer, size_t& total_meta_size) {
  // Get all hash meta
  std::vector<HashMeta*> primary_hash_meta_vec;
  std::vector<HashMeta*> backup_hash_meta_vec;
  std::vector<HashStore*> all_priamry_tables;
  std::vector<HashStore*> all_backup_tables;

  if (workload == "TATP") {
    all_priamry_tables = tatp_server->GetPrimaryHashStore();
    all_backup_tables = tatp_server->GetBackupHashStore();
  } else if (workload == "SmallBank") {
    all_priamry_tables = smallbank_server->GetPrimaryHashStore();
    all_backup_tables = smallbank_server->GetBackupHashStore();
  } else if (workload == "TPCC") {
    all_priamry_tables = tpcc_server->GetPrimaryHashStore();
    all_backup_tables = tpcc_server->GetBackupHashStore();
  } else if (workload == "MICRO") {
    all_priamry_tables = micro_server->GetPrimaryHashStore();
    all_backup_tables = micro_server->GetBackupHashStore();
  }

  for (auto& hash_table : all_priamry_tables) {
    auto* hash_meta = new HashMeta(hash_table->GetTableID(),
                                   (uint64_t)hash_table->GetDataPtr(),
                                   hash_table->GetBucketNum(),
                                   hash_table->GetHashNodeSize(),
                                   hash_table->GetBaseOff());
    primary_hash_meta_vec.emplace_back(hash_meta);
  }
  for (auto& hash_table : all_backup_tables) {
    auto* hash_meta = new HashMeta(hash_table->GetTableID(),
                                   (uint64_t)hash_table->GetDataPtr(),
                                   hash_table->GetBucketNum(),
                                   hash_table->GetHashNodeSize(),
                                   hash_table->GetBaseOff());
    backup_hash_meta_vec.emplace_back(hash_meta);
  }

  int hash_meta_len = sizeof(HashMeta);
  size_t primary_hash_meta_num = primary_hash_meta_vec.size();
  RDMA_LOG(INFO) << "primary hash meta num: " << primary_hash_meta_num;
  size_t backup_hash_meta_num = backup_hash_meta_vec.size();
  RDMA_LOG(INFO) << "backup hash meta num: " << backup_hash_meta_num;
  total_meta_size = sizeof(primary_hash_meta_num) + sizeof(backup_hash_meta_num) + sizeof(machine_id) + primary_hash_meta_num * hash_meta_len + backup_hash_meta_num * hash_meta_len + sizeof(MEM_STORE_META_END);
  *hash_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *hash_meta_buffer;

  // Fill primary hash meta
  *((size_t*)local_buf) = primary_hash_meta_num;
  local_buf += sizeof(primary_hash_meta_num);
  *((size_t*)local_buf) = backup_hash_meta_num;
  local_buf += sizeof(backup_hash_meta_num);
  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  for (size_t i = 0; i < primary_hash_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)primary_hash_meta_vec[i], hash_meta_len);
  }
  local_buf += primary_hash_meta_num * hash_meta_len;
  // Fill backup hash meta
  for (size_t i = 0; i < backup_hash_meta_num; i++) {
    memcpy(local_buf + i * hash_meta_len, (char*)backup_hash_meta_vec[i], hash_meta_len);
  }
  local_buf += backup_hash_meta_num * hash_meta_len;
  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}

bool AddrManager::Run() {
  // Now addr_manager just waits for user typing quit to finish
  // AddrManager's CPU is not used during one-sided RDMA requests from clients
  printf("====================================================================================================\n");
  printf(
      "AddrManager now runs as a disaggregated mode. No CPU involvement during RDMA-based transaction processing\n"
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
  // Configure of this addr_manager
  std::string config_filepath = "../../../config/memory_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_address_node = json_config.get("local_address_node");
  node_id_t machine_num = (node_id_t)local_address_node.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)local_address_node.get("machine_id").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);
  int local_port = (int)local_address_node.get("local_port").get_int64();
  int local_meta_port = (int)local_address_node.get("local_meta_port").get_int64();
  int use_pm = 0;
  auto index_size_GB = local_address_node.get("index_size_GB").get_uint64();
  auto txn_list_size_GB = local_address_node.get("txn_list_size_GB").get_uint64();

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
  size_t compute_node_num = compute_node_ips.size();

  size_t index_size = (size_t)1024 * 1024 * 1024 * index_size_GB;
  size_t txn_list_size = (size_t)1024 * 1024 * 1024 * txn_list_size_GB;

  auto addr_manager = std::make_shared<AddrManager>(machine_id, local_port, local_meta_port, index_size, txn_list_size, use_pm);
  // todo: 1. 初始化页面地址哈希索引
  // todo: 2. 初始化事务操作列表 X
  // todo: 3. 将哈希索引和事务操作列表的元信息发送给计算节点
  addr_manager->AllocMem();
  addr_manager->InitMem();
  // addr_manager->LoadData(machine_id, machine_num, workload);
  addr_manager->SendMeta(machine_id, compute_node_num);
  addr_manager->InitRDMA();
  bool run_next_round = addr_manager->Run();

  // Continue to run the next round. RDMA does not need to be inited twice
  while (run_next_round) {
    addr_manager->InitMem();
    addr_manager->CleanTable();
    addr_manager->CleanQP();
    // addr_manager->LoadData(machine_id, machine_num, workload);
    addr_manager->SendMeta(machine_id, compute_node_num);
    run_next_round = addr_manager->Run();
  }

  // Stat the cpu utilization
  auto pid = getpid();
  std::string copy_cmd = "cp /proc/" + std::to_string(pid) + "/stat ./";
  system(copy_cmd.c_str());

  copy_cmd = "cp /proc/uptime ./";
  system(copy_cmd.c_str());
  return 0;
}