// Author: Ming Zhang
// Copyright (c) 2022

#include "connection/meta_manager.h"

#include "util/json_config.h"

MetaManager::MetaManager(std::string bench_name) {
  // init table name and table id map
  if (bench_name == "tatp") {
    table_name_map[0] = "TATP_subscriber";
    table_name_map[1] = "TATP_sec_subcriber";
    table_name_map[2] = "TATP_special_facility";
    table_name_map[3] = "TATP_access_info";
    table_name_map[4] = "TATP_call_forwarding";

    // TODO
  } else if (bench_name == "smallbank") {
    table_name_map[0] = "SmallBank_savings";
    table_name_map[1] = "SmallBank_checking";

    TableMeta meta;
    meta.record_size_ = sizeof(DataItem);
    meta.num_records_per_page_ = (8 * (PAGE_SIZE - 1 - 20) + 1) / (1 + (meta.record_size_ + sizeof(itemkey_t)) * 8);
    meta.bitmap_size_ = (meta.num_records_per_page_ + 8 - 1) / 8;
    
    table_meta_map[0] = meta;
    table_meta_map[1] = meta;
  } else if (bench_name == "tpcc") {
    table_name_map[0] = "TPCC_warehouse";
    table_name_map[1] = "TPCC_district";
    table_name_map[2] = "TPCC_customer";
    table_name_map[3] = "TPCC_history";
    table_name_map[4] = "TPCC_new_order";
    table_name_map[5] = "TPCC_order";
    table_name_map[6] = "TPCC_order_line";
    table_name_map[7] = "TPCC_item";
    table_name_map[8] = "TPCC_stock";
    table_name_map[9] = "TPCC_customer_index";
    table_name_map[10] = "TPCC_order_index";
    // TODO
  } else if (bench_name == "micro"){
    table_name_map[0] = "MICRO_micro";
    // TODO
  }

  // Read config json file
  std::string config_filepath = "../../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto local_node = json_config.get("local_compute_node");
  local_machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  txn_system = local_node.get("txn_system").get_int64();
  RDMA_LOG(INFO) << "Run " << (txn_system == 0 ? "FaRM" : (txn_system == 1 ? "DrTM+H" : (txn_system == 2 ? "FORD" : "FORD-LOCAL")));

  auto hashindex_nodes = json_config.get("remote_hashindex_node_nodes");
  auto remote_hashindex_ips = hashindex_nodes.get("remote_hashindex_node_ips");                // Array
  auto remote_hashindex_ports = hashindex_nodes.get("remote_hashindex_node_port");            // Array Used for RDMA exchanges
  auto remote_hashindex_meta_ports = hashindex_nodes.get("remote_hashindex_node_meta_ports");  // Array Used for transferring datastore metas

  auto locktable_nodes = json_config.get("remote_locktable_node_nodes");
  auto remote_locktable_ips = locktable_nodes.get("remote_locktable_node_ips");                // Array
  auto remote_locktable_ports = locktable_nodes.get("remote_locktable_node_port");            // Array Used for RDMA exchanges
  auto remote_locktable_meta_ports = locktable_nodes.get("remote_locktable_node_meta_ports");  // Array Used for transferring datastore metas

  auto dn_nodes = json_config.get("remote_data_node_nodes");
  auto remote_data_ips = dn_nodes.get("remote_data_node_ips");                // Array
  auto remote_data_ports = dn_nodes.get("remote_data_node_port");            // Array Used for RDMA exchanges
  auto remote_data_meta_ports = dn_nodes.get("remote_data_node_meta_ports");  // Array Used for transferring datastore metas

  auto pagetable_nodes = json_config.get("remote_page_table_nodes");
  auto remote_pagetable_ips = pagetable_nodes.get("remote_page_table_node_ips");                // Array
  auto remote_pagetable_ports = pagetable_nodes.get("remote_page_table_node_port");            // Array Used for RDMA exchanges
  auto remote_pagetable_meta_ports = pagetable_nodes.get("remote_page_table_node_meta_port");  // Array Used for transferring datastore metas

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_data_ips.size(); index++) {
    std::string remote_ip = remote_data_ips.get(index).get_str();
    int remote_meta_port = (int)remote_data_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetRemoteDataStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      std::cerr << "Thread " << std::this_thread::get_id() << " GetMemStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    }
    int remote_port = (int)remote_data_ports.get(index).get_int64();
    remote_data_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }
  RDMA_LOG(INFO) << "All data meta received";

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_hashindex_ips.size(); index++) {
    std::string remote_ip = remote_hashindex_ips.get(index).get_str();
    int remote_meta_port = (int)remote_hashindex_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetRemoteHashIndexStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      std::cerr << "Thread " << std::this_thread::get_id() << " GetMemStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    }
    int remote_port = (int)remote_hashindex_ports.get(index).get_int64();
    remote_hashindex_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }
  RDMA_LOG(INFO) << "All hasindex meta received";

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_locktable_ips.size(); index++) {
    std::string remote_ip = remote_locktable_ips.get(index).get_str();
    int remote_meta_port = (int)remote_locktable_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetRemoteLockTableStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      std::cerr << "Thread " << std::this_thread::get_id() << " GetAddrStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    }
    int remote_port = (int)remote_locktable_ports.get(index).get_int64();
    remote_locktable_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }
  RDMA_LOG(INFO) << "All lock table meta received";

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_pagetable_ips.size(); index++) {
    std::string remote_ip = remote_pagetable_ips.get(index).get_str();
    int remote_meta_port = (int)remote_pagetable_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetRemotePageTableStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      std::cerr << "Thread " << std::this_thread::get_id() << " GetAddrStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    }
    int remote_port = (int)remote_pagetable_ports.get(index).get_int64();
    remote_pagetable_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
    page_table_nodes.push_back(remote_machine_id);
  }
  RDMA_LOG(INFO) << "All page table meta received";

  // RDMA setup
  int local_port = (int)local_node.get("local_port").get_int64();
  global_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_port);

  // Using the first RNIC's first port
  RdmaCtrl::DevIdx idx;
  idx.dev_id = 0;
  idx.port_id = 1;

  // Open device
  opened_rnic = global_rdma_ctrl->open_device(idx);
  for (auto& remote_node : remote_data_nodes) {
    GetRemoteDataNodeMR(remote_node);
  }
  for (auto& remote_node : remote_hashindex_nodes) {
    GetRemoteIndexNodeMR(remote_node);
  }
  for (auto& remote_node : remote_locktable_nodes) {
    GetRemoteLockNodeMR(remote_node);
  }
  for (auto& remote_node : remote_pagetable_nodes) {
    GetRemotePageNodeMR(remote_node);
  }
  RDMA_LOG(INFO) << "client: All remote mr meta received!";
}

node_id_t MetaManager::GetRemoteDataStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving hash metadata ----------------- */
  size_t hash_meta_size = (size_t) 1024;
  char* recv_buf = (char*)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  
  // Get number of meta
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);
  // Get the `end of file' indicator: finish transmitting

  // hcy note: need to add code about dtx receive meta from memstore
  DataStoreMeta meta;
  memcpy(&meta, snooper, sizeof(DataStoreMeta));
  snooper += sizeof(DataStoreMeta);
  data_metas[remote_machine_id] = meta;
  assert(*(uint64_t*)snooper == MEM_STORE_META_END);
  free(recv_buf);
  return remote_machine_id;
}

node_id_t MetaManager::GetRemotePageTableStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving PageTable metadata ----------------- */
  size_t hash_meta_size = (size_t)1024;
  char* recv_buf = (char*)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  // Now only recieve the machine id
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);

  PageTableMeta meta;
  memcpy(&meta, snooper, sizeof(PageTableMeta));
  snooper += sizeof(PageTableMeta);
  page_table_meta[remote_machine_id] = meta;
  assert(*(uint64_t*)snooper == MEM_STORE_META_END);
  free(recv_buf);
  return remote_machine_id;
}

node_id_t MetaManager::GetRemoteLockTableStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving LockTable metadata ----------------- */
  size_t hash_meta_size = (size_t)1024;
  char* recv_buf = (char*)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  // Now only recieve the machine id
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);

  LockTableMeta meta;
  memcpy(&meta, snooper, sizeof(LockTableMeta));
  snooper += sizeof(LockTableMeta);
  lock_table_meta[remote_machine_id] = meta;
  assert(*(uint64_t*)snooper == MEM_STORE_META_END);
  free(recv_buf);
  return remote_machine_id;
}

node_id_t MetaManager::GetRemoteHashIndexStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving HashIndex metadata ----------------- */
  size_t hash_meta_size = (size_t)1024;
  char* recv_buf = (char*)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;

  // Get number of meta
  size_t hash_index_meta_num = *((size_t*)snooper);
  snooper += sizeof(hash_index_meta_num);
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);

  for(size_t i = 0; i < hash_index_meta_num; i++) {
    IndexMeta meta;
    memcpy(&meta, snooper, sizeof(IndexMeta));
    snooper += sizeof(IndexMeta);
    hash_index_meta[meta.table_id] = meta;
    hash_index_nodes[meta.table_id] = remote_machine_id;
  }

  assert(*(uint64_t*)snooper == MEM_STORE_META_END);
  free(recv_buf);
  return remote_machine_id;
}

void MetaManager::GetRemoteDataNodeMR(const RemoteNode& node) {
  // Get remote node's memory region information via TCP
  MemoryAttr remote_data_mr{};

  while (QP::get_remote_mr(node.ip, node.port, SERVER_DATA_ID, &remote_data_mr) != SUCC) {
    usleep(2000);
  }
  remote_data_mrs[node.node_id] = remote_data_mr;
}

void MetaManager::GetRemotePageNodeMR(const RemoteNode& node) {
  // Get remote node's memory region information via TCP
  MemoryAttr remote_page_table_mr{}, remote_page_table_ringbuffer_mr{};

  while (QP::get_remote_mr(node.ip, node.port, SERVER_PAGETABLE_ID, &remote_page_table_mr) != SUCC) {
    usleep(2000);
  }
  while (QP::get_remote_mr(node.ip, node.port, SERVER_PAGETABLE_RING_FREE_FRAME_BUFFER_ID, &remote_page_table_ringbuffer_mr) != SUCC) {
    usleep(2000);
  }
  remote_page_table_mrs[node.node_id] = remote_page_table_mr;
  remote_page_table_ringbuffer_mrs[node.node_id] = remote_page_table_ringbuffer_mr;
}

void MetaManager::GetRemoteLockNodeMR(const RemoteNode& node) {
  // Get remote node's memory region information via TCP
  MemoryAttr remote_locktable_mr{};

  while (QP::get_remote_mr(node.ip, node.port, SERVER_LOCK_TABLE_ID, &remote_locktable_mr) != SUCC) {
    usleep(2000);
  }
  remote_locktable_mrs[node.node_id] = remote_locktable_mr;
}

void MetaManager::GetRemoteIndexNodeMR(const RemoteNode& node) {
  // Get remote node's memory region information via TCP
  MemoryAttr remote_hashindex_mr{};

  while (QP::get_remote_mr(node.ip, node.port, SERVER_HASH_INDEX_ID, &remote_hashindex_mr) != SUCC) {
    usleep(2000);
  }
  remote_hashindex_mrs[node.node_id] = remote_hashindex_mr;
}