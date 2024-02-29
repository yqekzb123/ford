// Author: Ming Zhang
// Copyright (c) 2022

#include "smallbank/smallbank_txn.h"

/******************** The business logic (Transaction) start ********************/

bool SmallBankDTX::TxLocalAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = TypeAmalgamate;
  /* Transaction parameters */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1);

  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  a1.sav_obj_0 = std::make_shared<DataItem>((table_id_t)
  SmallBankTableType::kSavingsTable, sav_key_0.item_key);
  dtx->AddToReadWriteSet(a1.sav_obj_0);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  a1.chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(a1.chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  a1.chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(a1.chk_obj_1);

  if (!dtx->TxLocalExe(yield)) return false;

  // printf("smallbank_txn.cc:41\n");
  // 如果成功了，本地提交。
  // 只是将事务塞入batch中
  bool commit_status = dtx->TxLocalCommit(yield, this);
  return commit_status;
}

bool SmallBankDTX::TxReCaculateAmalgamate(coro_yield_t& yield) {
  dtx->ReExeLocalRW(yield);
  
  /* If we are here, execution succeeded and we have locks */
  smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)a1.sav_obj_0->value;
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)a1.chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)a1.chk_obj_1->value;
  if (sav_val_0->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(sav_val_0->magic == smallbank_savings_magic);
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;

  // bool commit_status = dtx->TxCommit(yield);
  return true;
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::TxLocalBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = TypeBalance;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  b1.sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(b1.sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  b1.chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadOnlySet(b1.chk_obj);

  if (!dtx->TxLocalExe(yield)) return false;

  bool commit_status = dtx->TxLocalCommit(yield,this);
  return commit_status;
}

bool SmallBankDTX::TxReCaculateBalance(coro_yield_t& yield) {
  dtx->ReExeLocalRO(yield);

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)b1.sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)b1.chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  return true;
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::TxLocalDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = TypeDepositChecking;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  d1.chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(d1.chk_obj);

  if (!dtx->TxLocalExe(yield)) return false;

  bool commit_status = dtx->TxLocalCommit(yield,this);
  return commit_status;
}

bool SmallBankDTX::TxReCaculateDepositChecking(coro_yield_t& yield) {
  dtx->ReExeLocalRW(yield);

  /* If we are here, execution succeeded and we have a lock*/
  float amount = 1.3;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)d1.chk_obj->value;
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(chk_val->magic == smallbank_checking_magic);

  chk_val->bal += amount; /* Update checking kBalance */
  return true;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::TxLocalSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = TypeSendPayment;
  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1);
  float amount = 5.0;

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  s1.chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(s1.chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  s1.chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(s1.chk_obj_1);

  if (!dtx->TxLocalExe(yield)) return false;

  bool commit_status = dtx->TxLocalCommit(yield,this);
  return commit_status;
}

bool SmallBankDTX::TxReCaculateSendPayment(coro_yield_t& yield) {
  dtx->ReExeLocalRW(yield);
  
  float amount = 5.0;
  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)s1.chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)s1.chk_obj_1->value;
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  if (chk_val_0->bal < amount) {
    dtx->TxAbortReadWrite();
    return false;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */
  return true;
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::TxLocalTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = TypeTransactSaving;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);
  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  t1.sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadWriteSet(t1.sav_obj);
  if (!dtx->TxLocalExe(yield)) return false;

  bool commit_status = dtx->TxLocalCommit(yield,this);
  return commit_status;
}

bool SmallBankDTX::TxReCaculateTransactSaving(coro_yield_t& yield) {
  dtx->ReExeLocalRW(yield);

  float amount = 20.20;
  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)t1.sav_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  sav_val->bal += amount; /* Update saving kBalance */

  return true;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::TxLocalWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = TypeWriteCheck;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  w1.sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(w1.sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  w1.chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(w1.chk_obj);

  if (!dtx->TxLocalExe(yield)) return false;

  bool commit_status = dtx->TxLocalCommit(yield,this);
  return commit_status;
}

bool SmallBankDTX::TxReCaculateWriteCheck(coro_yield_t& yield) {
  dtx->ReExeLocalRW(yield);

  float amount = 5.0;
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)w1.sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)w1.chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  return true;
}

/******************** The batched business logic (Transaction) end ********************/

bool SmallBankDTX::TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1);

  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  auto sav_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key_0.item_key);
  dtx->AddToReadWriteSet(sav_obj_0);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(chk_obj_1);

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have locks */
  smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)sav_obj_0->value;
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (sav_val_0->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val_0->magic == smallbank_savings_magic);
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;

  // return true;
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadOnlySet(chk_obj);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;

  // return true;
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(chk_obj);

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock*/
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(chk_val->magic == smallbank_checking_magic);

  chk_val->bal += amount; /* Update checking kBalance */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
  // return true;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1);
  float amount = 5.0;

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(chk_obj_1);

  if (!dtx->TxExe(yield)) return false;

  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (chk_val_0->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  if (chk_val_0->bal < amount) {
    dtx->TxAbortReadWrite();
    return false;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;

  // return true;
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);
  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadWriteSet(sav_obj);
  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  sav_val->bal += amount; /* Update saving kBalance */

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
  // return true;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx) {
  dtx->TxBegin(tx_id);

  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id);
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(chk_obj);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    RDMA_LOG(INFO) << "read value: " << sav_val;
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
  // return true;
}

/******************** The business logic (Transaction) end ********************/