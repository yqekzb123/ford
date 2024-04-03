// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/worker.h"
#include <time.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <brpc/channel.h>
#include "storage/storage_service.pb.h"

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "dtx/dtx.h"
#include "micro/micro_txn.h"
#include "smallbank/smallbank_txn.h"
#include "tatp/tatp_txn.h"
#include "tpcc/tpcc_txn.h"
#include "ycsb/ycsb_txn.h"
#include "util/latency.h"
#include "util/zipf.h"

#include "global.h"
#include "batch/local_batch.h"

using namespace std::placeholders;

// All the functions are executed in each thread
std::mutex mux;

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;
extern std::vector<double> lock_durations;
extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> ab_rate;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;

extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:32348", "IP address of server");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

__thread uint64_t seed;                        // Thread-global random seed
__thread FastRandom* random_generator = NULL;  // Per coroutine random generator

__thread t_id_t thread_local_id;
__thread t_id_t thread_num;

__thread TATP* tatp_client = nullptr;
__thread SmallBank* smallbank_client = nullptr;
__thread TPCC* tpcc_client = nullptr;
__thread YCSB* ycsb_client = nullptr;

__thread MetaManager* meta_man;
__thread QPManager* qp_man;

__thread VersionCache* status;
__thread LockCache* lock_table;

__thread std::list<PageAddress>* free_page_list;
__thread std::mutex* free_page_list_mutex;

__thread RDMABufferAllocator* rdma_buffer_allocator;
__thread LogOffsetAllocator* log_offset_allocator;
__thread AddrCache* addr_cache;
__thread IndexCache* index_cache;
__thread PageTableCache* page_table_cache;

__thread TATPTxType* tatp_workgen_arr;
__thread SmallBankTxType* smallbank_workgen_arr;
__thread TPCCTxType* tpcc_workgen_arr;
__thread YCSBTxType* ycsb_workgen_arr;

__thread coro_id_t coro_num;
__thread coro_id_t batch_coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler

// For MICRO benchmark
__thread ZipfGen* zipf_gen = nullptr;
__thread bool is_skewed;
__thread uint64_t data_set_size;
__thread uint64_t num_keys_global;
__thread uint64_t write_ratio;

__thread brpc::Channel* data_channel;
__thread brpc::Channel* log_channel;

const coro_id_t POLL_ROUTINE_ID = 0;            // The poll coroutine ID

void RecordTpLat(double msr_sec) {
  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;
  double abort_rate = (double) stat_aborted_tx_total/stat_attempted_tx_total;

  std::sort(timer, timer + stat_committed_tx_total);
  double percentile_50 = timer[stat_committed_tx_total / 2];
  double percentile_99 = timer[stat_committed_tx_total * 99 / 100];

  std::cout << "RecordTpLat......" << std::endl;

  mux.lock();
  tid_vec.push_back(thread_gid);
  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  medianlat_vec.push_back(percentile_50);
  taillat_vec.push_back(percentile_99);
  ab_rate.push_back(abort_rate);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

void BatchExec(coro_yield_t& yield, coro_id_t coro_id) {
  while (true) {
    local_batch_store[thread_gid]->ExeBatch(yield, coro_id);
    coro_sched->YieldBatch(yield, coro_id);
    if (stop_run) {
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      // printf("worker.cc:130, thread %ld try to stop msr_sec %f\n", thread_gid, msr_sec);
      break;
    }
  }
}

// Coroutine 0 in each thread does polling
void PollCompletion(coro_yield_t& yield) {
  while (true) {
    coro_sched->PollCompletion();
    Coroutine* next = coro_sched->coro_head->next_coro;
    if (next->coro_id != POLL_ROUTINE_ID) {
      coro_sched->RunCoroutine(yield, next);
    }
    if (stop_run) {
      if (meta_man->txn_system == DTX_SYS::OUR) {
        double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
        RecordTpLat(msr_sec);
        // printf("worker.cc:148, thread %ld try to stop msr_sec %f\n", thread_gid, msr_sec);
      }
      break;
    }
  }
}

// Run actual transactions
void RunTATP(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // ! 新的执行逻辑中，每次循环都需要创建一个新的txn
    DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     index_cache,
                     page_table_cache,
                     free_page_list,
                     free_page_list_mutex,
                     data_channel,
                     log_channel);


    // Guarantee that each coroutine has a different seed
    TATPTxType tx_type = tatp_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    switch (tx_type) {
      case TATPTxType::kGetSubsciberData: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxGetSubsciberData(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kGetNewDestination: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxGetNewDestination(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kGetAccessData: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxGetAccessData(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kUpdateSubscriberData: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxUpdateSubscriberData(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kUpdateLocation: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxUpdateLocation(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kInsertCallForwarding: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxInsertCallForwarding(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case TATPTxType::kDeleteCallForwarding: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = TxDeleteCallForwarding(tatp_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }

    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    else{
      // printf("worker.cc: RunTATP, tx %ld not committed\n", iter);
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
    /********************************** Stat end *****************************************/
  }
  // delete dtx;
}

void RunSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // ! 新的执行逻辑中，每次循环都需要创建一个新的txn
    DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     index_cache,
                     page_table_cache,
                     free_page_list,
                     free_page_list_mutex,
                     data_channel,
                     log_channel);

    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    SmallBankDTX* bench_dtx = new SmallBankDTX();
    bench_dtx->dtx = dtx;
    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    dtx->tx_start_time = tx_start_time;
    // printf("worker.cc:326, start a new txn\n");
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxBalance(smallbank_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxSendPayment(smallbank_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    else{
      // printf("worker.cc: RunSmallBank, tx %ld not committed\n", iter);
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
    /********************************** Stat end *****************************************/
    delete dtx;
  }
  // delete dtx;
}

void RunLocalSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  int execute_cnt = 0;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // ! 新的执行逻辑中，每次循环都需要创建一个新的txn
    DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     index_cache,
                     page_table_cache,
                     free_page_list,
                     free_page_list_mutex,
                     data_channel,
                     log_channel);

    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    // stat_attempted_tx_total++;

    SmallBankDTX* bench_dtx = new SmallBankDTX();
    bench_dtx->dtx = dtx;
    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    dtx->tx_start_time = tx_start_time;
    // printf("worker.cc:326\n");
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalAmalgamate(smallbank_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalBalance(smallbank_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalDepositChecking(smallbank_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalSendPayment(smallbank_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalTransactSaving(smallbank_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalWriteCheck(smallbank_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    if (tx_committed) execute_cnt++;
    if (execute_cnt < WORKER_EXE_LOCAL_TXN_CNT) continue;
    else {
      execute_cnt = 0;
      // printf("worker.cc:499, thread %ld complete %d local txn\n", thread_gid, WORKER_EXE_LOCAL_TXN_CNT);
      coro_sched->LocalTxnYield(yield, coro_id);
      continue;
    }
    
    /********************************** Stat end *****************************************/
  }
  // delete dtx;
}

void RunTPCC(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  int execute_cnt = 0;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // ! 新的执行逻辑中，每次循环都需要创建一个新的txn
    DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     index_cache,
                     page_table_cache,
                     free_page_list,
                     free_page_list_mutex,
                     data_channel,
                     log_channel);

    // Guarantee that each coroutine has a different seed
    TPCCTxType tx_type = tpcc_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    switch (tx_type) {
      case TPCCTxType::kDelivery: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "] [Delivery] thread id: " << thread_gid << " coro id: " << coro_id;
        tx_committed = TxDelivery(tpcc_client, random_generator, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
      } break;
      case TPCCTxType::kNewOrder: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "] [NewOrder] thread id: " << thread_gid << " coro id: " << coro_id;
        tx_committed = TxNewOrder(tpcc_client, random_generator, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
      } break;
      case TPCCTxType::kOrderStatus: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "] [OrderStatus] thread id: " << thread_gid << " coro id: " << coro_id;
        tx_committed = TxOrderStatus(tpcc_client, random_generator, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
      } break;
      case TPCCTxType::kPayment: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "] [Payment] thread id: " << thread_gid << " coro id: " << coro_id;
        tx_committed = TxPayment(tpcc_client, random_generator, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
      } break;
      case TPCCTxType::kStockLevel: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "] [StockLevel] thread id: " << thread_gid << " coro id: " << coro_id;
        tx_committed = TxStockLevel(tpcc_client, random_generator, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
      } break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }

    // 此时只是本地执行完毕,batch还没做完，所以stats都得等等
    // 下面的直接没执行
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    else{
      printf("worker.cc 602: RunTPCC, tx %ld not committed\n", iter);
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
    /********************************** Stat end *****************************************/
  }
  // delete dtx;
}

void RunMICRO(coro_yield_t& yield, coro_id_t coro_id) {
  double total_msr_us = 0;
  #if 0
  // Each coroutine has a dtx: Each coroutine is a coordinator
  DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     free_page_list,
                     free_page_list_mutex);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;

  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    tx_committed = TxLockContention(zipf_gen, &seed, yield, iter, dtx, is_skewed, data_set_size, num_keys_global, write_ratio);
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

      total_msr_us = msr_sec * 1000000;

      double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
      double tx_tput = (double)stat_committed_tx_total / msr_sec;

      std::string thread_num_coro_num;
      if (coro_num < 10) {
        thread_num_coro_num = std::to_string(thread_num) + "_0" + std::to_string(coro_num);
      } else {
        thread_num_coro_num = std::to_string(thread_num) + "_" + std::to_string(coro_num);
      }
      std::string log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/output.txt";

      std::ofstream output_of;
      output_of.open(log_file_path, std::ios::app);

      std::sort(timer, timer + stat_committed_tx_total);
      double percentile_50 = timer[stat_committed_tx_total / 2];
      double percentile_99 = timer[stat_committed_tx_total * 99 / 100];
      mux.lock();
      tid_vec.push_back(thread_gid);
      attemp_tp_vec.push_back(attemp_tput);
      tp_vec.push_back(tx_tput);
      medianlat_vec.push_back(percentile_50);
      taillat_vec.push_back(percentile_99);
      mux.unlock();
      output_of << tx_tput << " " << percentile_50 << " " << percentile_99 << std::endl;
      output_of.close();
      // std::cout << tx_tput << " " << percentile_50 << " " << percentile_99 << std::endl;

      // Output the local addr cache miss rate
      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/miss_rate.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << double(dtx->miss_local_cache_times) / (dtx->hit_local_cache_times + dtx->miss_local_cache_times) << std::endl;
      output_of.close();

      log_file_path = "../../../bench_results/MICRO/" + thread_num_coro_num + "/cache_size.txt";
      output_of.open(log_file_path, std::ios::app);
      output_of << dtx->GetAddrCacheSize() << std::endl;
      output_of.close();

      break;
    }
    
  }

  std::string thread_num_coro_num;
  if (coro_num < 10) {
    thread_num_coro_num = std::to_string(thread_num) + "_0" + std::to_string(coro_num);
  } else {
    thread_num_coro_num = std::to_string(thread_num) + "_" + std::to_string(coro_num);
  }
  uint64_t total_duration = 0;
  double average_lock_duration = 0;

  // only for test
#if LOCK_WAIT

  for (auto duration : dtx->lock_durations) {
    total_duration += duration;
  }

  std::string total_lock_duration_file = "../../../bench_results/MICRO/" + thread_num_coro_num + "/total_lock_duration.txt";
  std::ofstream of;
  of.open(total_lock_duration_file, std::ios::app);
  std::sort(dtx->lock_durations.begin(), dtx->lock_durations.end());
  auto min_lock_duration = dtx->lock_durations.empty() ? 0 : dtx->lock_durations[0];
  auto max_lock_duration = dtx->lock_durations.empty() ? 0 : dtx->lock_durations[dtx->lock_durations.size() - 1];
  average_lock_duration = dtx->lock_durations.empty() ? 0 : (double)total_duration / dtx->lock_durations.size();
  lock_durations[thread_local_id] = average_lock_duration;
  of << thread_gid << " " << average_lock_duration << " " << max_lock_duration << std::endl;
  of.close();
#endif

  // only for test
#if INV_BUSY_WAIT
  total_duration = 0;
  for (auto duration : dtx->invisible_durations) {
    total_duration += duration;
  }
  std::string total_inv_duration_file = "../../../bench_results/MICRO/" + thread_num_coro_num + "/total_inv_duration.txt";
  std::ofstream ofs;
  ofs.open(total_inv_duration_file, std::ios::app);
  std::sort(dtx->invisible_durations.begin(), dtx->invisible_durations.end());
  auto min_inv_duration = dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[0];
  auto max_inv_duration = dtx->invisible_durations.empty() ? 0 : dtx->invisible_durations[dtx->invisible_durations.size() - 1];
  auto average_inv_duration = dtx->invisible_durations.empty() ? 0 : (double)total_duration / dtx->invisible_durations.size();

  double total_execution_time = 0;
  for (uint64_t i = 0; i < stat_committed_tx_total; i++) {
    total_execution_time += timer[i];
  }

  uint64_t re_read_times = 0;
  for (uint64_t i = 0; i < dtx->invisible_reread.size(); i++) {
    re_read_times += dtx->invisible_reread[i];
  }

  uint64_t avg_re_read_times = dtx->invisible_reread.empty() ? 0 : re_read_times / dtx->invisible_reread.size();

  auto average_execution_time = (total_execution_time / stat_committed_tx_total) * 1000000;  // us

  ofs << thread_gid << " " << average_inv_duration << " " << max_inv_duration << " " << average_execution_time << " " << avg_re_read_times << " " << double(total_duration / total_execution_time) << " " << (double)(total_duration / total_msr_us) << std::endl;

  ofs.close();
  #endif

  /********************************** Stat end *****************************************/

  delete dtx;
  #endif
}

void RunYCSB(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // ! 新的执行逻辑中，每次循环都需要创建一个新的txn
    DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     index_cache,
                     page_table_cache,
                     free_page_list,
                     free_page_list_mutex,
                     data_channel,
                     log_channel);

    YCSBTxType tx_type = ycsb_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    YCSBDTX* bench_dtx = new YCSBDTX();
    bench_dtx->dtx = dtx;
    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    dtx->tx_start_time = tx_start_time;
    // printf("worker.cc:326, start a new txn\n");
    switch (tx_type) {
      case YCSBTxType::kRW: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxRW(ycsb_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case YCSBTxType::kScan: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxScan(ycsb_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case YCSBTxType::kInsert: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxInsert(ycsb_client, &seed, yield, iter, dtx);
        if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    else{
      // printf("worker.cc: RunSmallBank, tx %ld not committed\n", iter);
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM) {
      // A coroutine calculate the total execution time and exits
      clock_gettime(CLOCK_REALTIME, &msr_end);
      // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
      double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
      RecordTpLat(msr_sec);
      break;
    }
    /********************************** Stat end *****************************************/
    delete dtx;
  }
  // delete dtx;
}

void RunLocalYCSB(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  int execute_cnt = 0;
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  while (true) {
    // ! 新的执行逻辑中，每次循环都需要创建一个新的txn
    DTX* dtx = new DTX(meta_man,
                     qp_man,
                     status,
                     lock_table,
                     thread_gid,
                     coro_id,
                     coro_sched,
                     rdma_buffer_allocator,
                     log_offset_allocator,
                     addr_cache,
                     index_cache,
                     page_table_cache,
                     free_page_list,
                     free_page_list_mutex,
                     data_channel,
                     log_channel);

    YCSBTxType tx_type = ycsb_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    // stat_attempted_tx_total++;

    YCSBDTX* bench_dtx = new YCSBDTX();
    bench_dtx->dtx = dtx;
    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    dtx->tx_start_time = tx_start_time;
    // printf("worker.cc:326\n");
    switch (tx_type) {
      case YCSBTxType::kRW: {
        thread_local_try_times[uint64_t(tx_type)]++;
        tx_committed = bench_dtx->TxLocalRW(ycsb_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case YCSBTxType::kScan: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // tx_committed = bench_dtx->TxLocalScan(ycsb_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case YCSBTxType::kInsert: {
        thread_local_try_times[uint64_t(tx_type)]++;
        // tx_committed = bench_dtx->TxLocalInsert(ycsb_client, &seed, yield, iter, dtx);
        // if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    if (tx_committed) execute_cnt++;
    if (execute_cnt < WORKER_EXE_LOCAL_TXN_CNT) continue;
    else {
      execute_cnt = 0;
      // printf("worker.cc:499, thread %ld complete %d local txn\n", thread_gid, WORKER_EXE_LOCAL_TXN_CNT);
      coro_sched->LocalTxnYield(yield, coro_id);
      continue;
    }
    
    /********************************** Stat end *****************************************/
  }
  // delete dtx;
}

void run_thread(thread_params* params,
                TATP* tatp_cli,
                SmallBank* smallbank_cli,
                TPCC* tpcc_cli,
                YCSB* ycsb_cli) {
  auto bench_name = params->bench_name;
  std::string config_filepath = "../../../config/" + bench_name + "_config.json";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
  
  if (bench_name == "tatp") {
    tatp_client = tatp_cli;
    tatp_workgen_arr = tatp_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TATP_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TATP_TX_TYPES]();
  } else if (bench_name == "smallbank") {
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if (bench_name == "tpcc") {
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  } else if (bench_name == "ycsb") {
    ycsb_client = ycsb_cli;
    ycsb_workgen_arr = ycsb_client->CreateWorkgenArray();
    thread_local_try_times = new uint64_t[YCSB_TX_TYPES]();
    thread_local_commit_times = new uint64_t[YCSB_TX_TYPES]();
  }

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  status = params->global_status;
  lock_table = params->global_lcache;

  free_page_list = params->free_list; 
  free_page_list_mutex = params->free_page_list_mutex;

  coro_num = (coro_id_t)params->coro_num;
  batch_coro_num = (coro_id_t) params->batch_coro_num;
  assert(batch_coro_num + 1 < coro_num);
  local_batch_store[thread_gid] = new LocalBatchStore(batch_coro_num * BATCH_CORO_TIMES);

  if(meta_man->txn_system == DTX_SYS::OUR) {
    coro_sched = new CoroutineScheduler(thread_gid, coro_num, true);
  }else{
    coro_sched = new CoroutineScheduler(thread_gid, coro_num, false);
  }

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  addr_cache = new AddrCache();
  index_cache = new IndexCache();
  page_table_cache = new PageTableCache();

  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);
  // log_offset_allocator = new LogOffsetAllocator(thread_gid, params->total_thread_num);
  timer = new double[ATTEMPTED_NUM]();

  // Initialize Zipf generator for MICRO benchmark
  if (bench_name == "micro") {
    uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    std::string micro_config_filepath = "../../../config/micro_config.json";
    auto json_config = JsonConfig::load_file(micro_config_filepath);
    auto micro_conf = json_config.get("micro");
    num_keys_global = align_pow2(micro_conf.get("num_keys").get_int64());
    auto zipf_theta = micro_conf.get("zipf_theta").get_double();
    is_skewed = micro_conf.get("is_skewed").get_bool();
    write_ratio = micro_conf.get("write_ratio").get_uint64();
    data_set_size = micro_conf.get("data_set_size").get_uint64();
    zipf_gen = new ZipfGen(num_keys_global, zipf_theta, zipf_seed & zipf_seed_mask);
  }

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  // if (meta_man->txn_system == DTX_SYS::OUR) seed = 0xdeadbeef;
  // else 
    seed = 0xdeadbeef + thread_gid;

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (meta_man->txn_system == DTX_SYS::OUR) {
      if (coro_i == POLL_ROUTINE_ID) {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
      } else if (coro_i > POLL_ROUTINE_ID && coro_i <= POLL_ROUTINE_ID + batch_coro_num) {
        // 只有batch的模式会需要batch协程
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(BatchExec, _1, coro_i));
        printf("worker.cc:889, thread %ld coro %ld exec batch\n",thread_gid,coro_i);
      } else {
        if (bench_name == "tatp") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTATP, _1, coro_i));
        } else if (bench_name == "smallbank") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunLocalSmallBank, _1, coro_i));
        } else if (bench_name == "tpcc") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i));
        } else if (bench_name == "micro") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunMICRO, _1, coro_i));
        } else if (bench_name == "ycsb") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunLocalYCSB, _1, coro_i));
        }
      }
    } else {
      if (coro_i == POLL_ROUTINE_ID) {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
      } else {
        if (bench_name == "tatp") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTATP, _1, coro_i));
        } else if (bench_name == "smallbank") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBank, _1, coro_i));
        } else if (bench_name == "tpcc") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i));
        } else if (bench_name == "micro") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunMICRO, _1, coro_i));
        } else if (bench_name == "ycsb") {
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunYCSB, _1, coro_i));
        }
      }
    }
   
  }
  data_channel = new brpc::Channel();
  log_channel = new brpc::Channel();
  // Init Brpc channel
  brpc::ChannelOptions options;
  // brpc::Channel channel;
  options.use_rdma = false;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms;
  options.max_retry = FLAGS_max_retry;
  if(data_channel->Init(FLAGS_server.c_str(), &options) != 0) {
      RDMA_LOG(FATAL) << "Fail to initialize channel";
  }
  if(log_channel->Init(FLAGS_server.c_str(), &options) != 0) {
      RDMA_LOG(FATAL) << "Fail to initialize channel";
  }
  
  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  std::cout << "Thread: " << thread_gid << " Build qp connection" << std::endl;
  qp_man->BuildQPConnection(meta_man);

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num) {
    usleep(2000);  // wait for all threads connections
  }

  // Start the first coroutine
  coro_sched->coro_array[0].func();

  // Stop running
  stop_run = true;

  // RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " << rdma_buffer_allocator->loop_times;

  // printf("thread %ld Finish, Tps: %ld\n", thread_gid, stat_committed_tx_total);

  // Clean
  delete[] timer;
  delete addr_cache;
  if (tatp_workgen_arr) delete[] tatp_workgen_arr;
  if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
  if (tpcc_workgen_arr) delete[] tpcc_workgen_arr;
  if (ycsb_workgen_arr) delete[] ycsb_workgen_arr;
  if (random_generator) delete[] random_generator;
  if (zipf_gen) delete zipf_gen;
  delete coro_sched;
  delete thread_local_try_times;
  delete thread_local_commit_times;
}
