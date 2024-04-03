// Author: Hongyao Zhao
// Copyright (c) 2024

#include "ycsb_db.h"

#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */

void YCSB::LoadIndex(node_id_t node_id, node_id_t num_server, 
                          MemStoreAllocParam* mem_store_alloc_param,
                          MemStoreReserveParam* mem_store_reserve_param) {
  // Initiate Index in memory node
  if ((node_id_t)YCSBTableType::kMainTable % num_server == node_id) {
    printf("Hash Index: Initializing SAVINGS table index\n");
    std::string config_filepath = "../../../workload/ycsb/ycsb_tables/maintable.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto table_config = json_config.get("index");
    main_table_index = new IndexStore((table_id_t)YCSBTableType::kMainTable,
                                       table_config.get("bkt_num").get_uint64(),
                                       mem_store_alloc_param, mem_store_reserve_param);
    PopulateIndexMainTable(mem_store_reserve_param);
    index_store_ptrs.push_back(main_table_index);
  }
}


void YCSB::PopulateIndexMainTable(MemStoreReserveParam* mem_store_reserve_param) {
  /* Populate the tables */
  itemkey_t item_key;
  Rid rid;
  std::ifstream input_file;
  input_file.open("../../storage_pool/server/" + bench_name + "_main_index.txt");
  if(!input_file.is_open()) {
    RDMA_LOG(ERROR) << "Error: cannot open file %s\n", (bench_name + "_main_index.txt").c_str();
    assert(false);
  }
  while(input_file >> item_key >> rid.page_no_ >> rid.slot_no_) {
    std::cout << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
    main_table_index->LocalInsertKeyRid(item_key, rid, mem_store_reserve_param);
  }
  input_file.close();
  return;
}

void YCSB::LoadTable(node_id_t node_id, node_id_t num_server) {
  // Initiate + Populate table for primary role
  if ((node_id_t)YCSBTableType::kMainTable % num_server == node_id) {
    printf("Primary: Initializing Main table\n");
    PopulateMainTable();
  }

  // Initiate + Populate table for backup role
  if (BACKUP_DEGREE < num_server) {
    for (node_id_t i = 1; i <= BACKUP_DEGREE; i++) {
      if ((node_id_t)YCSBTableType::kMainTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing SAVINGS table\n");
        PopulateMainTable();
      }
    }
  }
}

int YCSB::LoadRecord(RmFileHandle* file_handle,
                          itemkey_t item_key,
                          void* val_ptr,
                          size_t val_size,
                          table_id_t table_id,
                          std::ofstream& indexfile
                          ) {
  assert(val_size <= MAX_ITEM_SIZE);
  /* Insert into Disk */
  DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t*)val_ptr);
  char* item_char = (char*)malloc(item_to_be_inserted.GetSerializeSize());
  item_to_be_inserted.Serialize(item_char);
  Rid rid = file_handle->insert_record(item_key, item_char, nullptr);
  // record index
  indexfile << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
  free(item_char);
  return 1;
}

void YCSB::PopulateMainTable() {
  /* All threads must execute the loop below deterministically */
  rm_manager->create_file(bench_name + "_main", sizeof(DataItem));
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_main");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_main_index.txt");
  /* Populate the tables */
  for (uint32_t i = 0; i < num_accounts_global; i++) {
    // Savings
    ycsb_key_t ycsb_key;
    ycsb_key.key = (uint64_t)i;

    ycsb_val_t ycsb_val;
    memset(ycsb_val.F0, 1, YCSB_VAL_SIZE);

    LoadRecord(table_file.get(), ycsb_key.key,
               (void*)&ycsb_val, sizeof(ycsb_val_t),
                (table_id_t)YCSBTableType::kMainTable,
                indexfile);
  }
  indexfile.close();
}
