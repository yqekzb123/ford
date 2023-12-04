// Author: Ming Zhang
// Copyright (c) 2022

#include "smallbank_db.h"

#include "unistd.h"
#include "util/json_config.h"

/* Called by main. Only initialize here. The worker threads will populate. */
void SmallBank::LoadTable(node_id_t node_id, node_id_t num_server) {
  // Initiate + Populate table for primary role
  if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == node_id) {
    printf("Primary: Initializing SAVINGS table\n");
    // std::string config_filepath = "../../../workload/smallbank/smallbank_tables/savings.json";
    // auto json_config = JsonConfig::load_file(config_filepath);
    // auto table_config = json_config.get("table");
    // savings_table = new HashStore((table_id_t)SmallBankTableType::kSavingsTable,
    //                               table_config.get("bkt_num").get_uint64(),
    //                               mem_store_alloc_param);

    PopulateSavingsTable();
    primary_table_ptrs.push_back(savings_table);
  }
  if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == node_id) {
    printf("Primary: Initializing CHECKING table\n");
    // std::string config_filepath = "../../../workload/smallbank/smallbank_tables/checking.json";
    // auto json_config = JsonConfig::load_file(config_filepath);
    // auto table_config = json_config.get("table");
    // checking_table = new HashStore((table_id_t)SmallBankTableType::kCheckingTable,
    //                                table_config.get("bkt_num").get_uint64(),
    //                                mem_store_alloc_param);
    PopulateCheckingTable();
    primary_table_ptrs.push_back(checking_table);
  }

  // Initiate + Populate table for backup role
  if (BACKUP_DEGREE < num_server) {
    for (node_id_t i = 1; i <= BACKUP_DEGREE; i++) {
      if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing SAVINGS table\n");
        // std::string config_filepath = "../../../workload/smallbank/smallbank_tables/savings.json";
        // auto json_config = JsonConfig::load_file(config_filepath);
        // auto table_config = json_config.get("table");
        // savings_table = new HashStore((table_id_t)SmallBankTableType::kSavingsTable,
        //                               table_config.get("bkt_num").get_uint64(),
        //                               mem_store_alloc_param);
        PopulateSavingsTable();
        backup_table_ptrs.push_back(savings_table);
      }
      if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == (node_id - i + num_server) % num_server) {
        printf("Backup: Initializing CHECKING table\n");
        // std::string config_filepath = "../../../workload/smallbank/smallbank_tables/checking.json";
        // auto json_config = JsonConfig::load_file(config_filepath);
        // auto table_config = json_config.get("table");
        // checking_table = new HashStore((table_id_t)SmallBankTableType::kCheckingTable,
        //                                table_config.get("bkt_num").get_uint64(),
        //                                mem_store_alloc_param);
        PopulateCheckingTable();
        backup_table_ptrs.push_back(checking_table);
      }
    }
  }
}

int SmallBank::LoadRecord(RmFileHandle* file_handle,
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

void SmallBank::PopulateSavingsTable() {
  /* All threads must execute the loop below deterministically */
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_savings");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_savings_index.txt");
  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Savings
    smallbank_savings_key_t savings_key;
    savings_key.acct_id = (uint64_t)acct_id;

    smallbank_savings_val_t savings_val;
    savings_val.magic = smallbank_savings_magic;
    savings_val.bal = 1000000000ull;

    LoadRecord(table_file.get(), savings_key.item_key,
               (void*)&savings_val, sizeof(smallbank_savings_val_t),
                (table_id_t)SmallBankTableType::kSavingsTable,
                indexfile);
  }
  indexfile.close();
}

void SmallBank::PopulateCheckingTable( ) {
  /* All threads must execute the loop below deterministically */
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_checking");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_checking_index.txt");
  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Checking
    smallbank_checking_key_t checking_key;
    checking_key.acct_id = (uint64_t)acct_id;

    smallbank_checking_val_t checking_val;
    checking_val.magic = smallbank_checking_magic;
    checking_val.bal = 1000000000ull;

    LoadRecord(table_file.get(), checking_key.item_key,
               (void*)&checking_val, sizeof(smallbank_checking_val_t),
                (table_id_t)SmallBankTableType::kSavingsTable,
                indexfile);
  }
}