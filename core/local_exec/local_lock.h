// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
#include "dtx/dtx.h" 
#include "dtx/structs.h"
#include <tbb/concurrent_hash_map.h>

class LocalLock{
public:
    table_id_t table_id;
    itemkey_t key;
    lock_t lock; // 读写锁
    LocalLock() {
        lock = 0;
    }

    LocalLock(table_id_t tid, itemkey_t k) {
        table_id = tid;
        key = k;
        lock = 0;
        // printf("init lock of table %ld key %ld, now the lock %ld\n", table_id, key, lock);
    }

    bool LockShared() {
        lock_t oldlock = lock;
        lock_t newlock = oldlock + 1;
        
        bool result = ATOM_CAS(lock, oldlock, newlock);
        // printf("try to add shared lock on table %ld key %ld, now the lock %ld\n", table_id, key, lock);
        return result;
    }

    bool LockExclusive() {
        bool result =  ATOM_CAS(lock, UNLOCKED, EXCLUSIVE_LOCKED);
        // printf("try to add exclusive lock on table %ld key %ld, now the lock %ld\n", table_id, key, lock);
        return result;
    }

    bool UnlockShared() {
        ATOM_SUB_FETCH(lock,1);
        // printf("try to release shared lock on table %ld key %ld, now the lock %ld\n", table_id, key, lock);
    }

    bool UnlockExclusive() {
        lock = UNLOCKED;
        // printf("try to release exclusive lock on table %ld key %ld, now the lock %ld\n", table_id, key, lock);
    }
};

// using LocalLockTable = std::unordered_map<itemkey_t,LocalLock*>;
using LocalLockTable = tbb::concurrent_hash_map<itemkey_t,LocalLock*>;

class LocalLockStore{ 
public:  
    LocalLockStore(){
        local_lock.clear();
    }
    
    LocalLock* GetLock(table_id_t table_id, itemkey_t key) {
        LocalLock* lock = nullptr;
        LocalLockTable* table = nullptr;
        tbb::concurrent_hash_map<table_id_t,LocalLockTable*>::accessor accessor;
        if (local_lock.find(accessor, table_id)) {
            table = accessor->second;
            LocalLockTable::accessor accessor2;
            if (table->find(accessor2, key)) {
                lock = accessor2->second;
            }
        }
        else{
            table = new LocalLockTable();
            local_lock.insert(std::make_pair(table_id,table));
        }
        if (lock == nullptr) {
            // 如果data不存在，则自动创建一个临时的
            lock = new LocalLock(table_id, key);
            table->insert(std::make_pair(key,lock));
        }
        // printf("local_data.h:122\n");
        return lock;
    }

private:
    // std::unordered_map<table_id_t,LocalLockTable*> local_lock;
    tbb::concurrent_hash_map<table_id_t,LocalLockTable*> local_lock;
};
