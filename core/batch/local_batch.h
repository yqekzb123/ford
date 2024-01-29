// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
// #include "worker/global.h"
#include "dtx/dtx.h" 
#include "bench_dtx.h" 
#include "local_data.h"

#define LOCAL_BATCH_TXN_SIZE 100
#define BATCH_TXN_ID 1
class LocalBatch{
public:
    batch_id_t batch_id;
    pthread_mutex_t latch;
    std::vector<BenchDTX*> txn_list; 
    int current_txn_cnt;    // 当前batch中已经绑定的事务数量

    int start_commit_txn_cnt;   // 刚刚插入的事务数量
    int finish_commit_txn_cnt;  // 已经在batch的版本链中填入数据的事务
    LocalBatch(batch_id_t id) {
        current_txn_cnt = 0;
        start_commit_txn_cnt = 0;
        finish_commit_txn_cnt = 0;
        batch_id = id;
        pthread_mutex_init(&latch, nullptr);
        txn_list.clear();
    }
    bool InsertTxn(BenchDTX* txn) {
        txn->dtx->coro_id = BATCH_TXN_ID;
        pthread_mutex_lock(&latch);
        if (current_txn_cnt < LOCAL_BATCH_TXN_SIZE) {
            txn_list.push_back(txn);
            // txn_list[current_batch_cnt] = txn;
            current_txn_cnt++;
            start_commit_txn_cnt++;
            pthread_mutex_unlock(&latch);
            return true;
        } else {
            pthread_mutex_unlock(&latch);
            return false;
        }
    }
    void EndInsertTxn() {
        pthread_mutex_lock(&latch);
        finish_commit_txn_cnt++;
        pthread_mutex_unlock(&latch);
    }
    bool CanExec() {
        bool r1 = start_commit_txn_cnt == finish_commit_txn_cnt;
        bool r2 = current_txn_cnt >= LOCAL_BATCH_TXN_SIZE;
        return r1 && r2;
    }
    bool ExeBatchRW(coro_yield_t& yield);
    std::vector<DataItemPtr> ReadData(coro_yield_t& yield, DTX* first_dtx, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index);
    bool FlushWrite(coro_yield_t& yield, DTX* first_dtx, std::vector<DataItemPtr>& data_list, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>>& index);
    bool StatCommit();
    LocalDataStore local_data_store;
private:
};

class LocalBatchStore{ 
public:  
    LocalBatchStore(){
        batch_id_count = 0;
        pthread_mutex_init(&latch, nullptr);
    }

    batch_id_t GenerateBatchID() {
        batch_id_t id = ATOM_FETCH_ADD(batch_id_count,1);
        id = id * g_machine_num + g_machine_id;
        return id;
    }

    LocalBatch* GetBatch() {
        if (!local_store.empty()) {
            LocalBatch* batch = local_store.front();
            if (batch->CanExec()) {
                local_store.erase(local_store.begin());
                return batch;
            }
        } 
        return nullptr;
    }

    LocalBatch* GetBatchById(batch_id_t id) {
        if (now_exec!=nullptr && now_exec->batch_id == id) return now_exec;
        for (auto it = local_store.rbegin(); it != local_store.rend(); ++it) {
            auto batch = *it;
            if (batch->batch_id == id) return batch;
        }
        
        assert(false);
    }


    LocalBatch* InsertTxn(BenchDTX* txn) {
        LocalBatch *batch = nullptr;
        if (!local_store.empty()) {
            batch = local_store.back();
            if (batch->InsertTxn(txn)) return batch;
        } 
        CreateBatch();
        batch = local_store.back();
        batch->InsertTxn(txn);
        return batch;
    }

    void CreateBatch() {
        pthread_mutex_lock(&latch);
        batch_id_t batch_id = GenerateBatchID();
        LocalBatch* batch = new LocalBatch(batch_id);
        local_store.push_back(batch);
        printf("local_batch.h:112, create new batch, batch id %ld\n",batch_id);
        pthread_mutex_unlock(&latch);
    }

    void ExeBatch(coro_yield_t& yield) {
        if (now_exec == nullptr) {
            now_exec = GetBatch();
            if (now_exec == nullptr) {
                return;
            }
            printf("local_batch.h:121, exe batch %ld\n", now_exec->batch_id);
        }
        now_exec->ExeBatchRW(yield);
    }
private:
    pthread_mutex_t latch;
    std::vector<LocalBatch*> local_store;
    batch_id_t batch_id_count;

    LocalBatch* now_exec;
};
