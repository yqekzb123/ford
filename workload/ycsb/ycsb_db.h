// Author: Hongyao Zhao
// Copyright (c) 2024

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "config/table_type.h"
#include "memstore/hash_store.h"
#include "memstore/hash_index_store.h"
#include "util/fast_random.h"
#include "util/json_config.h"
#include "math.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
// YCSB table keys and values
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/*
 * CHECKING table
 */
union ycsb_key_t {
  uint64_t key;
  

  ycsb_key_t() {
    key = 0;
  }
};
static_assert(sizeof(ycsb_key_t) == sizeof(uint64_t), "");

#define YCSB_VAL_SIZE 1000
struct ycsb_val_t {
  char F0[YCSB_VAL_SIZE];
};
static_assert(sizeof(ycsb_val_t) == YCSB_VAL_SIZE, "");

#define YCSB_TX_TYPES 3
enum class YCSBTxType : int {
  kRW,
  kScan,
  kInsert
};


const std::string YCSB_TX_NAME[YCSB_TX_TYPES] = {"RW", "Scan", "Insert"};

// Table id
enum class YCSBTableType : uint64_t {
  kMainTable = TABLE_YCSB
};

class YCSB {
 public:
  std::string bench_name;

  uint32_t total_thread_num;

  uint32_t num_accounts_global;

  double zipf;

  double txn_write,tup_write;

  double scan_rate,insert_rate;

  double zeta_2_theta;
  double denom = 0;
  uint64_t the_n = 0;

  /* Tables */
  HashStore* main_table;

  std::vector<HashStore*> primary_table_ptrs;

  std::vector<HashStore*> backup_table_ptrs;

  /* Indexs */
  IndexStore* main_table_index;

  std::vector<IndexStore*> index_store_ptrs;

  RmManager* rm_manager;

  // For server usage: Provide interfaces to servers for loading tables
  // Also for client usage: Provide interfaces to clients for generating ids during tests
  YCSB(RmManager* rm_manager): rm_manager(rm_manager) {
    bench_name = "ycsb";
    // Used for populate table (line num) and get account
    std::string config_filepath = "../../../config/ycsb_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("ycsb");
    num_accounts_global = conf.get("num_accounts").get_uint64();
    zipf = conf.get("zipf").get_double();
    txn_write = conf.get("txn_write").get_double();
    tup_write = conf.get("tup_write").get_double();

    scan_rate = conf.get("scan_rate").get_double();
    insert_rate = conf.get("insert_rate").get_double();

    /* Up to 2 billion accounts */
    assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);

    main_table = nullptr;

    zeta_2_theta = zeta(2, zipf);
    the_n = num_accounts_global - 1;
		denom = zeta(the_n, zipf);

  }

  ~YCSB() {
    if (main_table) delete main_table;
  }

  YCSBTxType* CreateWorkgenArray() {
    YCSBTxType* workgen_arr = new YCSBTxType[100];

    int i = 0, j = 0;

    j += scan_rate;
    for (; i < j; i++) workgen_arr[i] = YCSBTxType::kScan;

    j += insert_rate;
    for (; i < j; i++) workgen_arr[i] = YCSBTxType::kInsert;

    // j += FREQUENCY_DEPOSIT_CHECKING;
    for (; i < 100; i++) workgen_arr[i] = YCSBTxType::kRW;

    // assert(i == 100 && j == 100);
    return workgen_arr;
  }

  /*
   * Generators for new account IDs. Called once per transaction because
   * we need to decide hot-or-not per transaction, not per account.
   */
  double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++) sum += pow(1.0 / i, theta);
    return sum;
  }

  inline void get_key(uint64_t n, double theta, uint64_t* seed, uint64_t* key_id) {
    // zipf
	  assert(the_n == n);
    assert(theta == zipf);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) / (1 - zeta_2_theta / zetan);
    double u = (double)(FastRand(seed) % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1) {
      *key_id = 1;
      return;
    }
    if (uz < 1 + pow(0.5, theta)) {
      *key_id = 2;
      return;
    }
    // return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
    *key_id = 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
  }

  inline void get_key(uint64_t* seed, uint64_t* key_id) {
    get_key(num_accounts_global - 1,zipf,seed,key_id);
  }

  void LoadTable(node_id_t node_id,
                 node_id_t num_server);

  // For server-side usage
  void LoadIndex(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 MemStoreReserveParam* mem_store_reserve_param);

  void PopulateMainTable();
  void PopulateIndexMainTable(MemStoreReserveParam* mem_store_reserve_param);

  int LoadRecord(RmFileHandle* file_handle,
                 itemkey_t item_key,
                 void* val_ptr,
                 size_t val_size,
                 table_id_t table_id,
                 std::ofstream& indexfile);

  ALWAYS_INLINE
  std::vector<HashStore*> GetPrimaryHashStore() {
    return primary_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<HashStore*> GetBackupHashStore() {
    return backup_table_ptrs;
  }

  ALWAYS_INLINE
  std::vector<IndexStore*> GetAllIndexStore() {
    return index_store_ptrs;
  }
};
