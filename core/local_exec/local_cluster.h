// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
#include "dtx/dtx.h" 
#include "bench_dtx.h" 
#include <boost/functional/hash.hpp>
#include <iostream>
#include <immintrin.h>

#define MAX_MIN_HASH_CNT 3
#define TXN_CLUSTER_CNT 1000

inline uint64_t MinHash0(table_id_t tid, itemkey_t key) {
  return std::hash<table_id_t>()(tid) ^ std::hash<itemkey_t>()(key);
}
inline uint64_t MinHash1(table_id_t tid, itemkey_t key) {
  return std::hash<table_id_t>()(tid) ^ std::hash<itemkey_t>()(key) ^ tid ^ key;
}
inline uint64_t MinHash2(table_id_t tid, itemkey_t key) {
  return std::hash<table_id_t>()(tid) * std::hash<itemkey_t>()(key);
}

class MinHashVector {
public:
  uint64_t min_hash[MAX_MIN_HASH_CNT];
  MinHashVector(BenchDTX * txn) {
    for (auto& item : txn->dtx->read_only_set) {
      uint64_t hash0 = MinHash0(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
      uint64_t hash1 = MinHash1(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
      uint64_t hash2 = MinHash2(item.item_ptr.get()->table_id,item.item_ptr.get()->key);
      min_hash[0] = min_hash[0] < hash0 ? min_hash[0] : hash0;
      min_hash[1] = min_hash[1] < hash1 ? min_hash[1] : hash1;
      min_hash[2] = min_hash[2] < hash2 ? min_hash[2] : hash2;
    }
  }
  uint64_t GetKey() {
    uint64_t hash_value;
    for (int i = 0; i < MAX_MIN_HASH_CNT; i++) {
      hash_value ^= std::hash<uint64_t>()(min_hash[i]);
    }
  }
};

class TxnCluster {
public:
    // std::queue<BenchDTX*, std::deque<BenchDTX*, std::allocator<BenchDTX*>, TXN_CLUSTER_CNT>> myQueue;
    // std::queue<BenchDTX*> txn_cluster;
    BenchDTX* txn_cluster[TXN_CLUSTER_CNT];
    std::atomic<int> tail;
    std::atomic<int> head;
    // uint64_t tail;
    // uint64_t head;
    pthread_mutex_t latch;
    t_id_t batch_thread_id;
    
    TxnCluster() {
        memset(txn_cluster, 0, sizeof(txn_cluster));
        pthread_mutex_init(&latch, nullptr);
        tail=0;
        head=0;
        batch_thread_id = -1;
    }
    bool InsertTxn(BenchDTX* txn) {
        bool ret = false;
        // pthread_mutex_lock(&latch);
        assert(tail.load() >= head.load());
        if (tail.load() - head.load() < TXN_CLUSTER_CNT) {
            int index = tail.fetch_add(1);
            if (index - head.load() < TXN_CLUSTER_CNT) {
                txn_cluster[tail % TXN_CLUSTER_CNT] = txn;
                ret = true;
            } else {
                tail.fetch_sub(1);
            }
        }        
        // pthread_mutex_unlock(&latch);
        return ret;
    }

    BenchDTX* GetTxn() {
        // pthread_mutex_lock(&latch);
        BenchDTX* result = nullptr;
        assert(tail.load() >= head.load());
        if (head.load() < tail.load()) {
            result = txn_cluster[head % TXN_CLUSTER_CNT];
            txn_cluster[head % TXN_CLUSTER_CNT] = nullptr;
            head.fetch_add(1);
        }
        // pthread_mutex_unlock(&latch);
        return result;
    }

    bool Bind(t_id_t btid) {
        bool ret = false;
        pthread_mutex_lock(&latch);
        if (batch_thread_id == -1) {
            batch_thread_id = btid;
            ret = true;
        }
        pthread_mutex_unlock(&latch);
        return ret;
    }
};

using MinHashKey = uint64_t;
using MinHashTable = std::unordered_map<MinHashKey,TxnCluster*>;

class MinHashStore{ 
public:  
    MinHashStore(){
        min_hash_table.clear();
    }

    bool InsertTxn(BenchDTX* txn) {
        MinHashVector min_hash(txn);
        MinHashKey key = min_hash.GetKey();
        TxnCluster* cluster = min_hash_table[key];
        if (cluster == nullptr) {
            cluster = new TxnCluster();
            min_hash_table[key] = cluster;
        }
        return cluster->InsertTxn(txn);
    }

    TxnCluster* GetNewTxnCluster(t_id_t thread_gid) {
        for (auto it = min_hash_table.begin(); it != min_hash_table.end(); ++it) {
            TxnCluster* cluster = it->second;
            if (cluster->Bind(thread_gid)) return cluster;
        }
    }

private:
    MinHashTable min_hash_table;
};
