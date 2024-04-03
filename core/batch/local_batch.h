// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
// #include "worker/global.h"
#include "dtx/dtx.h" 
#include "bench_dtx.h" 
#include "local_data.h"

// #define BATCH_TXN_ID 1
#define BATCH_CORO_TIMES 1

extern int LOCAL_BATCH_TXN_SIZE;
class LocalBatch{
private:
    std::vector<table_id_t> all_tableid;
    std::vector<itemkey_t> all_keyid;
    std::vector<int> all_type; // 用于标记每一个下标的读写集是读还是写

    std::vector<table_id_t> readonly_tableid;
    std::vector<itemkey_t> readonly_keyid;
    std::vector<table_id_t> readwrite_tableid;
    std::vector<itemkey_t> readwrite_keyid;

    std::vector<Rid> all_rids;
public:
    batch_id_t batch_id;
    coro_id_t coro_id;
    pthread_mutex_t latch;
    BenchDTX** txn_list;
    int current_txn_cnt;    // 当前batch中已经绑定的事务数量
    int start_commit_txn_cnt;   // 刚刚插入的事务数量
    int finish_commit_txn_cnt;  // 已经在batch的版本链中填入数据的事务

    LocalDataStore local_data_store;

    LocalBatch() {
        current_txn_cnt = 0;
        start_commit_txn_cnt = 0;
        finish_commit_txn_cnt = 0;
        batch_id = 0;
        coro_id = 0;
        txn_list = (BenchDTX**)malloc(sizeof(BenchDTX*) * LOCAL_BATCH_TXN_SIZE);
        pthread_mutex_init(&latch, nullptr);
    }
    void SetBatchID(batch_id_t id) { 
        batch_id = id;
    }
    void SetBatchCoroID(coro_id_t id) {
        coro_id = id;
    }
    LocalBatch(batch_id_t id) {
        LocalBatch();
        SetBatchID(id);
    }

    bool InsertTxn(BenchDTX* txn) {
        txn->dtx->coro_id = coro_id;
        // printf("local_batch.h:59 txn %p coro id %ld\n", txn->dtx->tx_id, txn->dtx->coro_id);
        pthread_mutex_lock(&latch);
        if (current_txn_cnt < LOCAL_BATCH_TXN_SIZE) {
            txn_list[current_txn_cnt] = txn;
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
    void Clean() {
        current_txn_cnt = 0;
        start_commit_txn_cnt = 0;
        finish_commit_txn_cnt = 0;
        batch_id = 0;
    }

    bool ExeBatchRW(coro_yield_t& yield);
    bool GetReadWriteSet(coro_yield_t& yield);
    std::vector<DataItemPtr> ReadData(coro_yield_t& yield, DTX* first_dtx);
    void Unpin(coro_yield_t& yield, DTX* first_dtx);
    bool FlushWrite(coro_yield_t& yield, DTX* first_dtx, std::vector<DataItemPtr>& data_list);
    bool StatCommit();


};

class LocalBatchStore{ 
private:
    inline int GetBatchCoroutineID(coro_id_t coro_id) {
        return coro_id - 1;
    }
public:  
    LocalBatchStore(int coroutine_cnt){
        batch_id_count = 0;
        pthread_mutex_init(&latch, nullptr);
        local_store = (LocalBatch**)malloc(sizeof(LocalBatch*)*coroutine_cnt);
        max_batch_cnt=coroutine_cnt;
        for (int i = 0; i < coroutine_cnt; i++) {
            local_store[i] = new LocalBatch();
            local_store[i]->SetBatchID(GenerateBatchID());
            local_store[i]->SetBatchCoroID(i+1);
        }
    }

    batch_id_t GenerateBatchID() {
        batch_id_t id = ATOM_FETCH_ADD(batch_id_count,1);
        id = id * g_machine_num + g_machine_id;
        return id;
    }

    LocalBatch* GetBatch(coro_id_t coro_id) {
        for (int i = 1; i <= BATCH_CORO_TIMES; i++) {
            int index = GetBatchCoroutineID(coro_id) * i;
            if (local_store[index]->CanExec()) return local_store[index];
        }
        return nullptr;
    }

    LocalBatch* GetBatchByIndex(int index, batch_id_t id) {
        if (local_store[index]->batch_id == id) return local_store[index];
        assert(false);
    }

    LocalBatch* InsertTxn(BenchDTX* txn) {
        LocalBatch *batch = nullptr;
        int index = batch_id_count%max_batch_cnt;
        if (local_store[index]->CanExec() &&
            local_store[index]->batch_id == batch_id_count){
            int new_index = (batch_id_count+1)%max_batch_cnt;
            // 此时新的batch已经满了
            if (local_store[new_index]->CanExec()) {
                return nullptr;
            } else {
                batch = local_store[new_index];
            }
        } else {
            batch = local_store[index];
        }
        batch->InsertTxn(txn);
        return batch;
    }


    void ExeBatch(coro_yield_t& yield, coro_id_t coro_id) {
        LocalBatch* exec = GetBatch(coro_id);
        if (exec == nullptr) {
            // printf("local_batch.h:145, no exec\n");
            return;
        }
        // printf("local_batch.h:148, exe batch %ld\n", exec->batch_id);
        exec->ExeBatchRW(yield);
        exec->Clean();
        exec->SetBatchID(GenerateBatchID());
    }
private:
    pthread_mutex_t latch;
    LocalBatch** local_store;
public:
    int max_batch_cnt;
private:
    // std::vector<LocalBatch*> local_store;
    batch_id_t batch_id_count;
};
