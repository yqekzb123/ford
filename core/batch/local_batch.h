// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
#include "dtx/dtx.h" 
#include "bench_dtx.h" 

#define LOCAL_BATCH_TXN_SIZE 100

class LocalBatch{
public:
    batch_id_t batch_id;
    lock_t latch; 
    std::vector<BenchDTX*> txn_list; 
    int current_batch_cnt;
    LocalBatch() {
        current_batch_cnt = 0;
    }
    bool InsertTxn(BenchDTX* txn) {
        if (current_batch_cnt < LOCAL_BATCH_TXN_SIZE) {
            txn_list.push_back(txn);
            // txn_list[current_batch_cnt] = txn;
            current_batch_cnt++;
            return true;
        } else return false;
    }
    bool ExeBatchRW(coro_yield_t& yield);
private:
    bool IssueReadRO(std::vector<DirectRead>& pending_direct_ro, std::vector<HashRead>& pending_hash_ro);
    bool IssueReadLock(std::vector<CasRead>& pending_cas_rw,
                        std::vector<HashRead>& pending_hash_rw,
                        std::vector<InsertOffRead>& pending_insert_off_rw);
    bool IssueReadRW(std::vector<DirectRead>& pending_direct_rw,
                      std::vector<HashRead>& pending_hash_rw,
                      std::vector<InsertOffRead>& pending_insert_off_rw);
    ValStatus IssueLocalValidate(std::vector<ValidateRead>& pending_validate);
    bool IssueRemoteValidate(std::vector<ValidateRead>& pending_validate);
    bool IssueCommitAll(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);
    bool IssueCommitAllFullFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);
    bool IssueCommitAllSelectFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);
    bool IssueCommitAllBatchSelectFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf);
};

class LocalBatchStore{ 
public:  
    LocalBatchStore(){
        batch_id_count = 0;
    }

    batch_id_t GenerateBatchID() {
        batch_id_t id = ATOM_FETCH_ADD(batch_id_count,1);
        id = id * g_machine_num + g_machine_id;
        return id;
    }

    LocalBatch* GetBatch() {
        LocalBatch* batch = local_store.front();
        local_store.erase(local_store.begin());
        return batch;
    }
    bool InsertTxn(BenchDTX* txn) {
        LocalBatch *batch = local_store.back();
        if (batch->InsertTxn(txn)) return true;
        else {
            CreateBatch();
            batch = local_store.back();
            batch->InsertTxn(txn);
            return true;
        }
    }
    void CreateBatch() {
        LocalBatch* batch = (LocalBatch*)malloc(sizeof(LocalBatch));
        batch->current_batch_cnt = 0;
        batch_id_t batch_id = GenerateBatchID();
        batch->batch_id = batch_id;
        local_store.push_back(batch);
    }

private:
    std::vector<LocalBatch*> local_store;
    batch_id_t batch_id_count;
};
