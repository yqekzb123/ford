// Author: Ming Zhang
// Copyright (c) 2022

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>

#include "util/json_config.h"
#include "util/bitmap.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"

// Load DB
#include "micro/micro_db.h"
#include "smallbank/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"

// All servers need to load data
void Server::LoadData(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload) {
  // /************************************* Load Data ***************************************/
  // RDMA_LOG(INFO) << "Start loading database data...";
  // // Init tables
  // MemStoreAllocParam mem_store_alloc_param(hash_buffer, hash_buffer, 0, hash_reserve_buffer);
  // MemStoreReserveParam mem_store_reserve_param(hash_reserve_buffer, 0, hash_buffer + hash_buf_size);
  // if (workload == "TATP") {
  //   tatp_server = new TATP();
  //   // tatp_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  // } else if (workload == "SmallBank") {
  //   smallbank_server = new SmallBank();
  //   smallbank_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  // } else if (workload == "TPCC") {
  //   tpcc_server = new TPCC();
  //   // tpcc_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  // } else if (workload == "MICRO") {
  //   micro_server = new MICRO();
  //   // micro_server->LoadTable(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  // }
  RDMA_LOG(INFO) << "Loading table successfully!";
}

int main(int argc, char* argv[]) {
    // Configure of this server
    std::string config_filepath = "../../../config/storage_node_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);

    auto local_node = json_config.get("local_storage_node");
    node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
    node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
    assert(machine_id >= 0 && machine_id < machine_num);
    int local_port = (int)local_node.get("local_port").get_int64();
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
    

    // used for test
    disk_manager->create_file("table");
    int fd = disk_manager->open_file("table");
    RmFileHdr file_hdr{};
    file_hdr.record_size_ = 8;
    file_hdr.num_pages_ = 1;
    file_hdr.first_free_page_no_ = RM_NO_PAGE;
    file_hdr.num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (file_hdr.record_size_ + sizeof(itemkey_t)) * BITMAP_WIDTH);
    file_hdr.bitmap_size_ = (file_hdr.num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
    disk_manager->write_page(fd, RM_FILE_HDR_PAGE, (char*)&file_hdr, sizeof(file_hdr));
    disk_manager->close_file(fd);

    auto server = std::make_shared<Server>(local_port, use_rdma, disk_manager.get(), log_manager.get());

}