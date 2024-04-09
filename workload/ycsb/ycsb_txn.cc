// Author: Ming Zhang
// Copyright (c) 2022

#include "ycsb/ycsb_txn.h"

/******************** The business logic (Transaction) start ********************/
bool YCSBDTX::TxLocalRW(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  bool read_only = FastRand(seed) % 100 < ycsb_client->txn_write;
  std::set<uint64_t> key_set;
  for (int i = 0; i < QUERY_CNT; i++) {
    uint64_t key;
    ycsb_client->get_key(seed,&key);
    // 加一个不能有重复key的代码
    while (key_set.count(key) == 1) {
      ycsb_client->get_key(seed,&key);
      assert(key <= ycsb_client->num_accounts_global);
    }
    key_set.insert(key);
    ycsb_key_t ykey;
    ykey.key = key;
    auto obj_0 = std::make_shared<DataItem>((table_id_t)YCSBTableType::kMainTable, ykey.key);
    r1.obj[i] = obj_0;
    bool is_read = FastRand(seed) % 100 < ycsb_client->tup_write;
    if (is_read || read_only) {
      dtx->AddToReadOnlySet(obj_0);
    } else {
      dtx->AddToReadWriteSet(obj_0);
    }
    // printf("ycsb_txn.cc:31 txn %ld %s key %ld, key range %ld\n",tx_id, (is_read || read_only) ? "read":"write", key,ycsb_client->num_accounts_global);
  }
  
  if (!dtx->TxLocalExe(yield)) return false;

  bool commit_status = dtx->TxLocalCommit(yield, this);
  return commit_status;
}

bool YCSBDTX::TxReCaculateRW(coro_yield_t& yield) {
  dtx->ReExeLocalRW(yield);  

  for (int i = 0; i < QUERY_CNT; i++) {
    ycsb_val_t* yval = (ycsb_val_t*) r1.obj[i]->value;
    if (yval->F0[0] != 1) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
    }
  }
  return true;
}

/******************** The business logic (Transaction) start ********************/
bool YCSBDTX::TxRW(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  bool read_only = FastRand(seed) % 100 < ycsb_client->txn_write;
  std::set<uint64_t> key_set;
  for (int i = 0; i < QUERY_CNT; i++) {
    uint64_t key;
    // 加一个不能有重复key的代码
    ycsb_client->get_key(seed,&key);
    while (key_set.count(key) == 1) {
      ycsb_client->get_key(seed,&key);
    }
    key_set.insert(key);
    ycsb_key_t ykey;
    ykey.key = key;
    auto obj_0 = std::make_shared<DataItem>((table_id_t)YCSBTableType::kMainTable, ykey.key);
    r1.obj[i] = obj_0;

    bool is_read = FastRand(seed) % 100 < ycsb_client->tup_write;
    if (is_read || read_only) {
      dtx->AddToReadOnlySet(obj_0);
    } else {
      dtx->AddToReadWriteSet(obj_0);
    }
  }
  
  if (!dtx->TxExe(yield)) return false;

  for (int i = 0; i < QUERY_CNT; i++) {
    ycsb_val_t* yval = (ycsb_val_t*) r1.obj[i]->value;
    if (yval->F0[0] != 1) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Calculate the sum of saving and checking kBalance */
bool YCSBDTX::TxScan(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // dtx->TxBegin(tx_id);

  // /* Transaction parameters */
  // uint64_t acct_id;
  // smallbank_client->get_account(seed, &acct_id);

  // /* Read from savings and checking tables */
  // smallbank_savings_key_t sav_key;
  // sav_key.acct_id = acct_id;
  // auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  // dtx->AddToReadOnlySet(sav_obj);

  // smallbank_checking_key_t chk_key;
  // chk_key.acct_id = acct_id;
  // auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  // dtx->AddToReadOnlySet(chk_obj);

  // if (!dtx->TxExe(yield)) return false;

  // smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  // smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  // if (sav_val->magic != smallbank_savings_magic) {
  //   RDMA_LOG(INFO) << "read value: " << sav_val;
  //   RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  // }
  // if (chk_val->magic != smallbank_checking_magic) {
  //   RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  // }
  // // assert(sav_val->magic == smallbank_savings_magic);
  // // assert(chk_val->magic == smallbank_checking_magic);

  // bool commit_status = dtx->TxCommit(yield);
  bool commit_status = true;
  return commit_status;
}

/* Add $1.3 to acct_id's checking account */
bool YCSBDTX::TxInsert(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  // dtx->TxBegin(tx_id);

  // /* Transaction parameters */
  // uint64_t acct_id;
  // smallbank_client->get_account(seed, &acct_id);
  // float amount = 1.3;

  // /* Read from checking table */
  // smallbank_checking_key_t chk_key;
  // chk_key.acct_id = acct_id;
  // auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  // dtx->AddToReadWriteSet(chk_obj);

  // if (!dtx->TxExe(yield)) return false;

  // /* If we are here, execution succeeded and we have a lock*/
  // smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  // if (chk_val->magic != smallbank_checking_magic) {
  //   RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  // }
  // // assert(chk_val->magic == smallbank_checking_magic);

  // chk_val->bal += amount; /* Update checking kBalance */

  // bool commit_status = dtx->TxCommit(yield);
  bool commit_status = true;
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/