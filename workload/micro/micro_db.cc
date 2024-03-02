// Author: Ming Zhang
// Copyright (c) 2022

#include "micro/micro_db.h"
#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */

void MICRO::LoadIndex(node_id_t node_id, node_id_t num_server, 
                      MemStoreAllocParam* mem_store_alloc_param, 
                      MemStoreReserveParam* mem_store_reserve_param) {
  // Initiate Index for MICRO table
  if ((node_id_t)MicroTableType::kMicroTable % num_server == node_id) {
    printf("Hash Index: Initializing MICRO table index\n");
    std::string config_filepath = "../../../workload/micro/micro_tables/micro.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto index_config = json_config.get("index");
    micro_table_index = new IndexStore((table_id_t)MicroTableType::kMicroTable,
                                       index_config.get("bkt_num").get_uint64(),
                                       mem_store_alloc_param, mem_store_reserve_param);
    PopulateIndexMicroTable(mem_store_reserve_param);
    index_store_ptrs.push_back(micro_table_index);
  }
}

void MICRO::LoadTable(node_id_t node_id, node_id_t num_server) {
  // Initiate + Populate table for primary role
  if ((node_id_t)MicroTableType::kMicroTable % num_server == node_id) {
    printf("Primary: Initializing MICRO table\n");
    PopulateMicroTable(); 
  }

  // Initiate + Populate table for backup role
  if (BACKUP_DEGREE < num_server) {
    for (node_id_t i = 1; i <= BACKUP_DEGREE; i++) {
      if ((node_id_t)MicroTableType::kMicroTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing MICRO table\n");
        PopulateMicroTable();
      }
    }
  }
}

int MICRO::LoadRecord(RmFileHandle* file_handle,
                      itemkey_t item_key,
                      void* val_ptr,
                      size_t val_size,
                      table_id_t table_id,
                      std::ofstream& indexfile
                      )  {
  assert(val_size <= MAX_ITEM_SIZE);
  /* Insert into Disk */
  DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t*)val_ptr);
  char* item_char = (char*)malloc(item_to_be_inserted.GetSerializeSize());
  item_to_be_inserted.Serialize(item_char);
  Rid rid = file_handle->insert_record(item_key, item_char, nullptr);
  // Record index
  indexfile << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
  free(item_char);
  return 1;
}

void MICRO::PopulateMicroTable() {
  rm_manager->create_file(bench_name + "_micro", sizeof(DataItem));
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_micro");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_micro_index.txt");

  for (uint64_t id = 0; id < num_keys_global; id++) {
    micro_key_t micro_key;
    micro_key.micro_id = id;

    micro_val_t micro_val;
    for (int i = 0; i < 5; i++) {
      micro_val.magic[i] = micro_magic + i;
    }

    LoadRecord(table_file.get(), micro_key.item_key, 
              (void*)&micro_val, sizeof(micro_val_t), 
              (table_id_t)MicroTableType::kMicroTable, indexfile);
  }
  indexfile.close();
}

void MICRO::PopulateIndexMicroTable(MemStoreReserveParam* mem_store_reserve_param) {
  /* Populate the tables */
  itemkey_t item_key;
  Rid rid;
  std::ifstream input_file;
  input_file.open("../../storage_pool/server/" + bench_name + "_micro_index.txt");
  if (!input_file.is_open()) {
    RDMA_LOG(ERROR) << "Error: cannot open file %s\n", (bench_name + "_micro_index.txt").c_str();
    assert(false);
  }
  while (input_file >> item_key >> rid.page_no_ >> rid.slot_no_) {
    std::cout << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
    micro_table_index->LocalInsertKeyRid(item_key, rid, mem_store_reserve_param); // Assuming LocalInsertKeyRid is a method to insert a key and Rid into the index
  }
  input_file.close();
  return ;
}
