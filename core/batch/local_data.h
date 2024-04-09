// Author: hongyao zhao
// Copyright (c) 2023

#pragma once
#include "dtx/dtx.h" 
#include "dtx/structs.h"

#define VERSION_CNT 1
#define LOCAL_DATA_BUCKET 10000

class LocalData{
public:
    table_id_t table_id;
    itemkey_t key;
    // 目前都先设置为固定版本长度
    // LVersion versions[VERSION_CNT];
    LVersion versions;
    int version_cnt;

    LocalData() {
        version_cnt = 0;
    }
    LocalData(table_id_t tid, itemkey_t k) : LocalData() {
        table_id = tid;
        key = k;
    }

    void clear() {
        version_cnt = 0;
    }

    LVersion GetTailVersion() {
        return versions;
    }

    // 设置头部version的值，然后事务重新计算
    bool SetFirstVersion(DataItemPtr data) {
        if (versions.value == nullptr) versions.value = new DataItem();
        assert(data.get()->value[0] != 0);
        memcpy(versions.value, data.get(), sizeof(DataItem));
        versions.has_value = true;
        // printf("local_data.h:66 table %ld key %ld set first value %p %s %s\n",table_id,key,versions.value,versions.value->value,data->value);
        return true;
    }
};

struct LocalDataKey {
    table_id_t _tid;
    itemkey_t _key;
    LocalDataKey(table_id_t tid, itemkey_t key) {
        _tid = tid;
        _key = key;
    }
};

struct LocalDataHash {
    std::size_t operator()(const LocalDataKey& LKey) const {
        return std::hash<table_id_t>()(LKey._tid)  << 1 ^ (std::hash<itemkey_t>()(LKey._key));
    }
};
struct LocalDataEqual {
    bool operator()(const LocalDataKey& LKey, const LocalDataKey& RKey) const {
        return LKey._tid == RKey._tid && LKey._key == RKey._key;
    }
};


class LocalDataStore{ 
public:  
    LocalDataStore(){
        // local_store.clear();
        local_store.reserve(LOCAL_DATA_BUCKET);
    }
    
    LocalData* GetData(table_id_t table_id, itemkey_t key) {
        LocalData* data = local_store[LocalDataKey(table_id,key)];
        // LocalDataTable table = local_store[std::make_pair(table_id,key)];
        // data = table[key]; 
        if (data == nullptr) {
            data = new LocalData(table_id, key);
            local_store[LocalDataKey(table_id,key)] = data;
            data = local_store[LocalDataKey(table_id,key)];
            // printf("local_data.h:103 %p create new data item tid %ld, key %ld, data %p\n", local_store,table_id,key,data);
        }
        return data;
    }

private:
    // std::unordered_map<table_id_t,LocalDataTable> local_store;

    std::unordered_map<LocalDataKey,LocalData*,LocalDataHash,LocalDataEqual> local_store;
};
