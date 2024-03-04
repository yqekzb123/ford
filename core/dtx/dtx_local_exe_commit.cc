// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "worker/global.h"
// #include ""
bool DTX::TxLocalExe(coro_yield_t& yield, bool fail_abort) {
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;
  // printf("dtx_local_exe_commit.cc:10 new dtx %ld\n");
  if (read_write_set.empty() && read_only_set.empty()) {
    return true;
  }
  // return true;
  assert(global_meta_man->txn_system == DTX_SYS::OUR);
  // Run our system
  if (read_write_set.empty()) {
    if (LockLocalRO(yield)) {
      return true;
    }
    else {
      goto ABORT;
    }
  } else {
    if (LockLocalRW(yield)){
      return true;
    } 
    else {
      goto ABORT;
    }
  }
  return true;

ABORT:
  if (fail_abort) LocalAbort(yield);
  return false;
}

bool DTX::TxLocalCommit(coro_yield_t& yield, BenchDTX* dtx_with_bench) {
  // Only read one item
  // printf("dtx_local_exe_commit.cc:39\n");
  if (read_write_set.empty() && read_only_set.size() == 1) {
    return true;
  }

  bool commit_stat;

  /*!
    OUR's commit protocol
    */
  
  commit_stat = LocalCommit(yield,dtx_with_bench);
  if (commit_stat) {
    return true;
  } else {
    goto ABORT;
  }
  return true;
ABORT:
  LocalAbort(yield);
  return false;
}
