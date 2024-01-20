// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
#include "dtx/dtx.h" 
#include "dtx/structs.h"

class LocalData{
public:
    table_id_t table_id;
    itemkey_t key;
    LVersion *versions;
    LVersion *tail_version;

    lock_t lock; // 读写锁
    LocalData() {
        versions = (LVersion*)malloc(sizeof(LVersion));
        tail_version = versions;
        versions->has_value = false;
        versions->txn = nullptr;
        versions->next = nullptr;
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

    bool CreateNewVersion(DTX *txn) {
        LVersion *newv = (LVersion*)malloc(sizeof(LVersion));
        newv->next = nullptr;
        newv->has_value = false;
        newv->SetVersionDTX(txn);
        if (versions == nullptr) {
            versions = newv;
            tail_version = newv;
        } else {
            tail_version->next = newv;
            tail_version = newv;
        }     
    }

    LVersion * GetDTXVersion(DTX *txn) {
        LVersion* ptr = versions;
        while(true) {
            if (ptr->txn == txn) return ptr;
            if (ptr->next == nullptr) return nullptr;
            ptr = ptr->next;
        }
    }

    LVersion * GetDTXVersionWithDataItem(DTX *txn) {
        LVersion* ptr = versions->next;
        LVersion* last = versions;
        while(true) {
            if (ptr->txn == txn) {
                last->CopyDataItemToNext();
                return ptr;
            }
            if (ptr->next == nullptr) return nullptr;
            last = ptr;
            ptr = ptr->next;
        }
    }

    LVersion * GetTailVersion() {
        return tail_version;
    }

    // 设置头部version的值，然后事务重新计算
    bool SetFirstVersion(DataItem* data) {
        versions->SetDataItem(data);
    }
};

using LocalDataTable = std::unordered_map<itemkey_t,LocalData*>;
class LocalDataStore{ 
public:  
    LocalDataStore(){
        local_store.clear();
    }
    
    LocalData* GetData(table_id_t table_id, itemkey_t key) {
        LocalData* data = nullptr;
        // printf("local_data.h:112\n");
        LocalDataTable table = local_store[table_id];
        // printf("local_data.h:114\n");
        data = table[key];
        // printf("local_data.h:116\n");
        if (data == nullptr) {
            // 如果data不存在，则自动创建一个临时的
            data = new LocalData();
            // (LocalData*)malloc(sizeof(LocalData));
            table.insert(std::make_pair(key,data));
        }
        // printf("local_data.h:122\n");
        return data;
    }

    bool InsertData(table_id_t table_id, itemkey_t key, LocalData* data){
        LocalDataTable table = local_store.at(table_id);
        if (table.at(key) != nullptr) return false; // 这个数据项已经有了.
        table.insert(std::make_pair(key,data));
        return true;
    }

private:
    std::unordered_map<table_id_t,LocalDataTable> local_store;
};
