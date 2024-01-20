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
    }

    bool LockShared() {
        lock_t oldlock = lock;
        lock_t newlock = oldlock + 1;
        
        lock_t previousValue = ATOM_CAS(lock, oldlock, newlock);
        return previousValue == oldlock;
    }

    bool LockExclusive() {
        // lock_t old_lock = ;
        return ATOM_CAS(lock, UNLOCKED, EXCLUSIVE_LOCKED);
    }

    bool UnlockShared() {
        ATOM_SUB_FETCH(lock,1);
    }

    bool UnlockExclusive() {
        lock = UNLOCKED;
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
        // printf("local_data.h:112\n");
        LocalLockTable table = local_lock[table_id];
        // printf("local_data.h:114\n");
        lock = table[key];
        // printf("local_data.h:116\n");
        if (lock == nullptr) {
            // 如果data不存在，则自动创建一个临时的
            lock = new LocalLock();
            // (LocalData*)malloc(sizeof(LocalData));
            table.insert(std::make_pair(key,lock));
        }
        // printf("local_data.h:122\n");
        return lock;
    }

private:
    std::unordered_map<table_id_t,LocalLockTable> local_lock;
};
