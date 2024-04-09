// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
#include "dtx/dtx.h" 
#include "dtx/structs.h"

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
        return true;
    }

    bool UnlockExclusive() {
        lock = UNLOCKED;
        // printf("try to release exclusive lock on table %ld key %ld, now the lock %ld\n", table_id, key, lock);
        return true;
    }
};

using LocalLockTable = std::unordered_map<itemkey_t,LocalLock*>;
class LocalLockStore{ 
public:  
    LocalLockStore(){
        local_lock.clear();
    }
    
    LocalLock* GetLock(table_id_t table_id, itemkey_t key) {
        LocalLock* lock = nullptr;
        LocalLockTable table = local_lock[table_id];
        lock = table[key];
        if (lock == nullptr) {
            // 如果data不存在，则自动创建一个临时的
            lock = new LocalLock(table_id, key);
            table.insert(std::make_pair(key,lock));
        }
        // printf("local_data.h:122\n");
        return lock;
    }

private:
    std::unordered_map<table_id_t,LocalLockTable> local_lock;
};
