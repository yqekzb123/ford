// Author: Ming Zhang
// Copyright (c) 2022
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>

#include "config/table_type.h"
#include "memstore/hash_index_store.h"

#include "util/fast_random.h"
#include "util/json_config.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

#define TX_HOT 90 /* Percentage of txns that use accounts from hotspot */

// Smallbank table keys and values
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/*
 * SAVINGS table.
 */
union smallbank_savings_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_savings_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_savings_key_t) == sizeof(uint64_t), "");

struct smallbank_savings_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_savings_val_t) == sizeof(uint64_t), "");

/*
 * CHECKING table
 */
union smallbank_checking_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_checking_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_checking_key_t) == sizeof(uint64_t), "");

struct smallbank_checking_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_checking_val_t) == sizeof(uint64_t), "");

// Magic numbers for debugging. These are unused in the spec.
#define SmallBank_MAGIC 97 /* Some magic number <= 255 */
#define smallbank_savings_magic (SmallBank_MAGIC)
#define smallbank_checking_magic (SmallBank_MAGIC + 1)

// Helpers for generating workload
#define SmallBank_TX_TYPES 6
enum class SmallBankTxType : int {
  kAmalgamate,
  kBalance,
  kDepositChecking,
  kSendPayment,
  kTransactSaving,
  kWriteCheck,
};


const std::string SmallBank_TX_NAME[SmallBank_TX_TYPES] = {"Amalgamate", "Balance", "DepositChecking", \
"SendPayment", "TransactSaving", "WriteCheck"};

// Table id
enum class SmallBankTableType : uint64_t {
  kSavingsTable = TABLE_SMALLBANK,
  kCheckingTable,
};

class SmallBank {
 public:
  std::string bench_name;

  uint32_t total_thread_num;

  uint32_t num_accounts_global, num_hot_global;

  /* Indexs */
  IndexStore* savings_table_index;

  IndexStore* checking_table_index;

  std::vector<IndexStore*> index_store_ptrs;

  // std::vector<IndexStore*> backup_table_ptrs;

  RmManager* rm_manager;

  // For server usage: Provide interfaces to servers for loading tables
  // Also for client usage: Provide interfaces to clients for generating ids during tests
  SmallBank(RmManager* rm_manager): rm_manager(rm_manager) {
    bench_name = "SmallBank";
    // Used for populate table (line num) and get account
    std::string config_filepath = "../../../config/smallbank_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("smallbank");
    num_accounts_global = conf.get("num_accounts").get_uint64();
    num_hot_global = conf.get("num_hot_accounts").get_uint64();

    /* Up to 2 billion accounts */
    assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);

    savings_table_index = nullptr;
    checking_table_index = nullptr;
  }

  ~SmallBank() {
    if (savings_table_index) delete savings_table_index;
    if (checking_table_index) delete checking_table_index;
  }

  SmallBankTxType* CreateWorkgenArray() {
    SmallBankTxType* workgen_arr = new SmallBankTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_AMALGAMATE;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kAmalgamate;

    j += FREQUENCY_BALANCE;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kBalance;

    j += FREQUENCY_DEPOSIT_CHECKING;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kDepositChecking;

    j += FREQUENCY_SEND_PAYMENT;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kSendPayment;

    j += FREQUENCY_TRANSACT_SAVINGS;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kTransactSaving;

    j += FREQUENCY_WRITE_CHECK;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kWriteCheck;

    assert(i == 100 && j == 100);
    return workgen_arr;
  }

  /*
   * Generators for new account IDs. Called once per transaction because
   * we need to decide hot-or-not per transaction, not per account.
   */
  inline void get_account(uint64_t* seed, uint64_t* acct_id) const {
    if (FastRand(seed) % 100 < TX_HOT) {
      *acct_id = FastRand(seed) % num_hot_global;
    } else {
      *acct_id = FastRand(seed) % num_accounts_global;
    }
  }

  inline void get_two_accounts(uint64_t* seed, uint64_t* acct_id_0, uint64_t* acct_id_1) const {
    if (FastRand(seed) % 100 < TX_HOT) {
      *acct_id_0 = FastRand(seed) % num_hot_global;
      *acct_id_1 = FastRand(seed) % num_hot_global;
      while (*acct_id_1 == *acct_id_0) {
        *acct_id_1 = FastRand(seed) % num_hot_global;
      }
    } else {
      *acct_id_0 = FastRand(seed) % num_accounts_global;
      *acct_id_1 = FastRand(seed) % num_accounts_global;
      while (*acct_id_1 == *acct_id_0) {
        *acct_id_1 = FastRand(seed) % num_accounts_global;
      }
    }
  }

  void LoadTable(node_id_t node_id, node_id_t num_server);

  // For server-side usage
  void LoadIndex(node_id_t node_id,
                 node_id_t num_server,
                 MemStoreAllocParam* mem_store_alloc_param,
                 MemStoreReserveParam* mem_store_reserve_param);

  void PopulateSavingsTable();

  void PopulateCheckingTable();

  void PopulateIndexSavingsTable(MemStoreReserveParam* mem_store_reserve_param);

  void PopulateIndexCheckingTable(MemStoreReserveParam* mem_store_reserve_param);

  int LoadRecord(RmFileHandle* file_handle,
                 itemkey_t item_key,
                 void* val_ptr,
                 size_t val_size,
                 table_id_t table_id,
                 std::ofstream& indexfile);

  ALWAYS_INLINE
  std::vector<IndexStore*> GetAllIndexStore() {
    return index_store_ptrs;
  }
  
};
