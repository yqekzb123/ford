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

    LocalData() {
        versions = (LVersion*)malloc(sizeof(LVersion));
        tail_version = versions;
        versions->has_value = false;
        versions->txn = nullptr;
        versions->next = nullptr;
    }

    bool CreateNewVersion(DTX *txn) {
        LVersion *newv = (LVersion*)malloc(sizeof(LVersion));
        newv->next = nullptr;
        newv->has_value = false;
        newv->SetVersionDTX(txn,txn->tx_id);
        if (versions == nullptr) {
            versions = newv;
            tail_version = newv;
        } else {
            tail_version->next = newv;
            tail_version = newv;
            assert(versions->next != nullptr);
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
        assert(ptr != nullptr);
        while(true) {
            if (ptr->tx_id == txn->tx_id) {
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
    bool SetFirstVersion(DataItemPtr data) {
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
        LocalDataTable table = local_store[table_id];
        // printf("local_data.h:104, find table id %ld ptr %p\n", table_id, table);
        data = table[key]; 
        if (data == nullptr) {
            // 如果data不存在，则自动创建一个临时的
            data = new LocalData();
            // (LocalData*)malloc(sizeof(LocalData));
            local_store[table_id].insert(std::make_pair(key,data));
            // printf("local_data.h:113, create non-exist key %ld data %p\n", key, data);
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
