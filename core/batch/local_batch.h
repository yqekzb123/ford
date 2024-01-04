// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
// #include "worker/global.h"
#include "dtx/dtx.h" 
#include "bench_dtx.h" 

#define LOCAL_BATCH_TXN_SIZE 100

class LocalBatch{
public:
    batch_id_t batch_id;
    pthread_mutex_t latch;
    std::vector<BenchDTX*> txn_list; 
    int current_batch_cnt;
    LocalBatch(batch_id_t id) {
        current_batch_cnt = 0;
        batch_id = id;
        pthread_mutex_init(&latch, nullptr);
        txn_list.clear();
    }
    bool InsertTxn(BenchDTX* txn) {
        pthread_mutex_lock(&latch);
        if (current_batch_cnt < LOCAL_BATCH_TXN_SIZE) {
            txn_list.push_back(txn);
            // txn_list[current_batch_cnt] = txn;
            current_batch_cnt++;
            pthread_mutex_unlock(&latch);
            return true;
        } else {
            pthread_mutex_unlock(&latch);
            return false;
        }
    }
    bool ExeBatchRW(coro_yield_t& yield);
    std::vector<DataItemPtr> ReadData(coro_yield_t& yield, DTX* first_dtx, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index);
    bool FlushWrite(coro_yield_t& yield, DTX* first_dtx, std::vector<DataItemPtr> data_list, std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> index);
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
            local_store.erase(local_store.begin());
            return batch;
        } else {
            return nullptr;
        }
    }
    bool InsertTxn(BenchDTX* txn) {
        LocalBatch *batch = nullptr;
        if (!local_store.empty()) {
            batch = local_store.back();
            if (batch->InsertTxn(txn)) return true;
        } 
        CreateBatch();
        batch = local_store.back();
        batch->InsertTxn(txn);
        return true;
    }
    void CreateBatch() {
        pthread_mutex_lock(&latch);
        batch_id_t batch_id = GenerateBatchID();
        LocalBatch* batch = new LocalBatch(batch_id);
        local_store.push_back(batch);
        pthread_mutex_unlock(&latch);
    }

    void ExeBatch(coro_yield_t& yield) {
        if (now_exec == nullptr) {
            now_exec = GetBatch();
            if (now_exec == nullptr) {
                return;
            }
        }
        now_exec->ExeBatchRW(yield);
    }
private:
    pthread_mutex_t latch;
    std::vector<LocalBatch*> local_store;
    batch_id_t batch_id_count;

    LocalBatch* now_exec;
};
