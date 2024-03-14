// Author: huangdund
// Copyright (c) 2023

#include "dtx/dtx.h"

struct index_request_list_item{
    table_id_t table_id;
    itemkey_t item_key;
    int index;
};

// 如果出现初始桶中没有itemkey的情况，似乎无法使用桶尾部的多个指针
// 并行加多个锁，因为可能会造成死锁, 无法保证按顺序加锁
std::vector<Rid> DTX::GetHashIndexBatch(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key) {
    
    assert(pending_hash_node_latch_idx.size() == 0);

    total_hash_node_offs_vec.clear();
    assert(total_hash_node_offs_vec.size() == 0);
    std::vector<std::vector<index_request_list_item>> find_index_request_list_vec;
    // 计算每个itemkey的hash值和对应的NodeOffset
    for(int i=0; i<table_id.size(); i++){
        auto hash_meta = global_meta_man->GetHashIndexMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetHashIndexNode(table_id[i]);
        auto hash = MurmurHash64A(item_key[i], 0xdeadbeef) % hash_meta.bucket_num;
        offset_t offset = hash_meta.base_off + hash * sizeof(IndexNode);
        NodeOffset node_off{remote_node_id, offset};

        auto index = std::find(total_hash_node_offs_vec.begin(), total_hash_node_offs_vec.end(), node_off);
        if(index == total_hash_node_offs_vec.end()){
            // not find
            total_hash_node_offs_vec.push_back(node_off);
            find_index_request_list_vec.push_back(std::vector<index_request_list_item>(1, {table_id[i], item_key[i], i}));
        }else{
            // find
            find_index_request_list_vec[index - total_hash_node_offs_vec.begin()].push_back({table_id[i], item_key[i], i});
        }
    }

    std::vector<Rid> res(table_id.size(), {INVALID_PAGE_ID, -1});

    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> faa_bufs_vec(total_hash_node_offs_vec.size(), nullptr);

    // init local_hash_nodes and faa_bufs, and find_index_request_list , and pending_hash_node_latch_offs
    // 这里的node_offs是已经去重的了
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));
        faa_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        pending_hash_node_latch_idx.push_back(i);
    }

    // std::vector<NodeOffset> unlock_shared_node_off;
    // std::vector<NodeOffset> hold_node_off_latch;
    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_idx.size()!=0) {
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ShardLockHashNode(yield, QPType::kHashIndex, local_hash_nodes_vec, faa_bufs_vec);
        // init hold_node_off_latch
        // for(auto idx : succ_node_off_idx ){
        //     hold_node_off_latch.push_back(total_hash_node_offs_vec[idx]);
        // }

        for(auto idx : succ_node_off_idx ){
            // read now
            IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_nodes_vec[idx]);
            // 遍历这个node_off上的所有请求即所有的hash index item 
            // 如果找到, 就从列表中移除
            for(auto it = find_index_request_list_vec[idx].begin(); it != find_index_request_list_vec[idx].end(); ){
                // find hash index item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (index_node->index_items[i].key == (*it).item_key && index_node->index_items[i].valid == true) {
                        // find
                        res[(*it).index] = index_node->index_items[i].rid;
                        find_index_request_list_vec[idx].erase(it);
                        is_find = true;
                        break;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(find_index_request_list_vec[idx].size() == 0){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    ShardUnLockHashNode(release_node_off, QPType::kHashIndex);
                    if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if HashIndex not exist, find next bucket

                // // for debug
                // std::cout << "*** not find item: table:" << find_index_request_list[node_off].front().first << "item id: " 
                //     << find_index_request_list[node_off].front().second << std::endl;

                node_id_t node_id = global_meta_man->GetHashIndexNode(find_index_request_list_vec[idx].front().table_id);
                auto expand_node_id = index_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetHashIndexExpandBase(find_index_request_list_vec[idx].front().table_id);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(IndexNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    for(auto index_request: find_index_request_list_vec[idx]){
                        // not find
                        res[index_request.index] = {INVALID_PAGE_ID, -1};
                    }

                    // unlock
                    auto release_idx = idx;
                    auto release_node_off = total_hash_node_offs_vec[release_idx];
                    while(true){
                        ShardUnLockHashNode(release_node_off, QPType::kHashIndex);
                        if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                        release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                        release_node_off = total_hash_node_offs_vec[release_idx];
                    }
                }
                else{
                    // alloc next node read buffer and cas buffer
                    NodeOffset next_node_off{node_id, next_off};
                    total_hash_node_offs_vec.push_back(next_node_off);
                    int new_idx = total_hash_node_offs_vec.size() - 1;
                    pending_hash_node_latch_idx.push_back(new_idx);
                    find_index_request_list_vec.push_back(find_index_request_list_vec[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(faa_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode)));
                    faa_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }
    assert(pending_hash_node_latch_offs.size() == 0);
    // // assert(hold_node_off_latch.size() == 0);
    // // 检查请求的HashIndex是否都被处理了
    // for(int i=0; i<table_id.size(); i++){
    //     // std::cout << "/// table_id: " << table_id[i] << " item_key: " << item_key[i] << " rid: " << res[table_id[i]][item_key[i]].page_no_ << " " << res[table_id[i]][item_key[i]].slot_no_ << std::endl;
    //     assert(res[i].page_no_ != INVALID_PAGE_ID); 
    // }
    return res;
}


// 如果出现初始桶中没有itemkey的情况，似乎无法使用桶尾部的多个指针
// 并行加多个锁，因为可能会造成死锁, 无法保证按顺序加锁
std::vector<Rid> DTX::GetHashIndex(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key) {
    assert(pending_hash_node_latch_idx.size() == 0);

    total_hash_node_offs_vec.clear();
    assert(total_hash_node_offs_vec.size() == 0);
    total_hash_node_offs_vec.resize(table_id.size());
    std::vector<int> total_idx_map_to_table_id(table_id.size(), -1);

    // cache
    std::vector<bool> is_cache(table_id.size(), false);
    std::vector<char*> cache_index_item_vec(table_id.size(), nullptr);
    
    // 计算每个itemkey的hash值和对应的NodeOffset
    bool if_yield = false;
    for(int i=0; i<table_id.size(); i++){
        auto hash_meta = global_meta_man->GetHashIndexMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetHashIndexNode(table_id[i]);

        // caculate cache
        auto cache_off = index_cache->Search(remote_node_id, table_id[i], item_key[i]);
        if(cache_off != INDEX_NOT_FOUND){
            is_cache[i] = true;
            cache_index_item_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexItem));
            auto qp = thread_qp_man->GetRemoteIndexQPWithNodeID(remote_node_id);
            coro_sched->RDMARead(coro_id, qp, cache_index_item_vec[i], cache_off, sizeof(IndexItem));
            if_yield = true;
        }
        else{
            auto hash = MurmurHash64A(item_key[i], 0xdeadbeef) % hash_meta.bucket_num;
            offset_t offset = hash_meta.base_off + hash * sizeof(IndexNode);
            NodeOffset node_off{remote_node_id, offset};
            total_hash_node_offs_vec[i] = node_off;
            pending_hash_node_latch_idx.push_back(i);
            total_idx_map_to_table_id[i] = i;
        }
    }

    std::vector<Rid> res(table_id.size(), {INVALID_PAGE_ID, -1});
    
    if(if_yield) coro_sched->Yield(yield, coro_id);

    // 检查缓存正确性
    for(int i=0; i<table_id.size(); i++){
        if(is_cache[i]){
            auto index_item = reinterpret_cast<IndexItem*>(cache_index_item_vec[i]);
            if(index_item->key == item_key[i] && index_item->valid == true){
                // printf("cache hit\n");
                res[i] = index_item->rid;
            }
            else{
                auto hash_meta = global_meta_man->GetHashIndexMeta(table_id[i]);
                auto remote_node_id = global_meta_man->GetHashIndexNode(table_id[i]);
                auto hash = MurmurHash64A(item_key[i], 0xdeadbeef) % hash_meta.bucket_num;
                offset_t offset = hash_meta.base_off + hash * sizeof(IndexNode);
                NodeOffset node_off{remote_node_id, offset};
                total_hash_node_offs_vec[i] = node_off;
                pending_hash_node_latch_idx.push_back(i);
            }
        } 
    }

    if(pending_hash_node_latch_idx.size() == 0){
        return res;
    }

    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> faa_bufs_vec(total_hash_node_offs_vec.size(), nullptr);

    // init local_hash_nodes and faa_bufs, and find_index_request_list , and pending_hash_node_latch_offs
    // 这里的node_offs是已经去重的了
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));
        faa_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    }

    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_idx.size()!=0) {
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ShardLockHashNode(yield, QPType::kHashIndex, local_hash_nodes_vec, faa_bufs_vec);
        for(auto idx : succ_node_off_idx ){
            // read now
            IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_nodes_vec[idx]);
            // 遍历这个node_off上的所有请求即所有的hash index item            
            // find hash index item
            bool is_find = false;
            for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                if (index_node->index_items[i].key == item_key[idx] && index_node->index_items[i].valid == true) {
                    // find
                    res[total_idx_map_to_table_id[idx]] = index_node->index_items[i].rid;
                    is_find = true;
                    // cache index
                    index_cache->Insert(total_hash_node_offs_vec[idx].nodeId, table_id[idx], item_key[idx], 
                        total_hash_node_offs_vec[idx].offset + (offset_t)&index_node->index_items[i] - (offset_t)index_node);
                    break;
                }
            }
            if(is_find){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    ShardUnLockHashNode(release_node_off, QPType::kHashIndex);
                    if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // // for debug
                // std::cout << "*** not find item: table:" << find_index_request_list[node_off].front().first << "item id: " 
                //     << find_index_request_list[node_off].front().second << std::endl;

                node_id_t node_id = global_meta_man->GetHashIndexNode(table_id[idx]);
                auto expand_node_id = index_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetHashIndexExpandBase(table_id[idx]);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(IndexNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    res[total_idx_map_to_table_id[idx]] = {INVALID_PAGE_ID, -1};

                    // unlock
                    auto release_idx = idx;
                    auto release_node_off = total_hash_node_offs_vec[release_idx];
                    while(true){
                        ShardUnLockHashNode(release_node_off, QPType::kHashIndex);
                        if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                        release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                        release_node_off = total_hash_node_offs_vec[release_idx];
                    }
                }
                else{
                    // alloc next node read buffer and cas buffer
                    NodeOffset next_node_off{node_id, next_off};
                    total_hash_node_offs_vec.push_back(next_node_off);
                    int new_idx = total_hash_node_offs_vec.size() - 1;
                    pending_hash_node_latch_idx.push_back(new_idx);
                    table_id.push_back(table_id[idx]);
                    item_key.push_back(item_key[idx]);
                    total_idx_map_to_table_id.push_back(total_idx_map_to_table_id[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(faa_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode)));
                    faa_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }
    assert(pending_hash_node_latch_offs.size() == 0);
    // // assert(hold_node_off_latch.size() == 0);
    // // 检查请求的HashIndex是否都被处理了
    // for(int i=0; i<res.size(); i++){
    //     // std::cout << "/// table_id: " << table_id[i] << " item_key: " << item_key[i] << " rid: " << res[table_id[i]][item_key[i]].page_no_ << " " << res[table_id[i]][item_key[i]].slot_no_ << std::endl;
    //     if(res[i].page_no_ == INVALID_PAGE_ID){
    //         RDMA_LOG(FATAL) << "HashIndex: fail to find hash index item";
    //     }
    // }
    return res;
}

bool DTX::InsertHashIndex(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key, std::vector<Rid> rid) {
     // 这里不检查是否已经存在, 由上层保证

     // 计算每个itemkey的hash值和对应的NodeOffset
    std::vector<NodeOffset> node_offs;
    for(int i=0; i<table_id.size(); i++){
        auto hash_meta = global_meta_man->GetHashIndexMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetHashIndexNode(table_id[i]);
        auto hash = MurmurHash64A(item_key[i], 0xdeadbeef) % hash_meta.bucket_num;
        offset_t node_off = hash_meta.base_off + hash * sizeof(IndexNode);
        node_offs.push_back(NodeOffset{remote_node_id, node_off});
    }

    assert(pending_hash_node_latch_offs.size() == 0);
    std::unordered_map<NodeOffset, char*> local_hash_nodes;
    std::unordered_map<NodeOffset, char*> cas_bufs;
    std::unordered_map<NodeOffset, std::list<std::pair<std::pair<table_id_t, itemkey_t>, Rid>>> insert_index_request_list;    

    // init local_hash_nodes and cas_bufs, and find_index_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<node_offs.size(); i++){
        auto node_off = node_offs[i];
        if(local_hash_nodes.find(node_off) == local_hash_nodes.end()){
            local_hash_nodes[node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));
        }
        if(cas_bufs.find(node_off) == cas_bufs.end()){
            cas_bufs[node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        }
        insert_index_request_list[node_off].push_back(std::make_pair(std::make_pair(table_id[i], item_key[i]), rid[i]));
        pending_hash_node_latch_offs.emplace(node_off);
    }
    std::unordered_set<NodeOffset> unlock_node_off_with_write;
    std::unordered_set<NodeOffset> hold_node_off_latch;
    std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_offs.size()!=0) {
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off = ExclusiveLockHashNode(yield, QPType::kHashIndex, local_hash_nodes, cas_bufs);
        // init hold_node_off_latch
        for(auto node_off : succ_node_off ){
            hold_node_off_latch.emplace(node_off);
        }

        for(auto node_off : succ_node_off ){
            // read now
            IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_nodes[node_off]);
            // 遍历这个node_off上的所有请求即所有的hash index item 
            // 如果找到, 就从列表中移除
            for(auto it = insert_index_request_list[node_off].begin(); it != insert_index_request_list[node_off].end(); ){
                // find empty slot to insert
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (index_node->index_items[i].valid == false) {
                        // find empty slot
                        index_node->index_items[i].key = (*it).first.second;
                        index_node->index_items[i].rid = (*it).second;
                        index_node->index_items[i].valid = true;
                        // erase会返回下一个元素的迭代器
                        it = insert_index_request_list[node_off].erase(it);
                        is_find = true;
                        break;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(insert_index_request_list[node_off].size() == 0){
                // release latch and write back
                auto release_node_off = node_off;
                while(true){
                    unlock_node_off_with_write.emplace(release_node_off);
                    if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
                    release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if HashIndex not exist, find next bucket
                node_id_t node_id = global_meta_man->GetHashIndexNode(insert_index_request_list[node_off].front().first.first);
                auto expand_node_id = index_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetHashIndexExpandBase(insert_index_request_list[node_off].front().first.first);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(IndexNode);
                if(expand_node_id < 0){
                    // find to the bucket end, no space to insert
                    // TODO: RDMA ALLOC A NEW HASH INDEX NODE
                    RDMA_LOG(ERROR) <<  "HashIndex:hash index item bucket is full";
                    auto release_node_off = node_off;
                    while(true){
                        unlock_node_off_with_write.emplace(release_node_off);
                        if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
                        release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
                    }
                }
                else{
                    // alloc next node read buffer and cas buffer
                    NodeOffset next_node_off{node_id, next_off};
                    pending_hash_node_latch_offs.emplace(next_node_off);
                    insert_index_request_list.emplace(next_node_off, insert_index_request_list.at(node_off));
                    hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
                    assert(local_hash_nodes.count(next_node_off) == 0);
                    assert(cas_bufs.count(next_node_off) == 0);
                    local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));
                    cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
                }
            }
        }
        // release all latch and write back
        for (auto node_off : unlock_node_off_with_write){
            ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes[node_off], QPType::kHashIndex);
            hold_node_off_latch.erase(node_off);
        }
        unlock_node_off_with_write.clear();
    }
    // 这里所有的latch都已经释放了
    assert(pending_hash_node_latch_offs.size() == 0);
    assert(hold_node_off_latch.size() == 0);
    return true;
}

bool DTX::DeleteHashIndex(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> item_key) {
    // 这里不检查是否已经存在, 由上层保证

     // 计算每个itemkey的hash值和对应的NodeOffset
    std::vector<NodeOffset> node_offs;
    for(int i=0; i<table_id.size(); i++){
        auto hash_meta = global_meta_man->GetHashIndexMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetHashIndexNode(table_id[i]);
        auto hash = MurmurHash64A(item_key[i], 0xdeadbeef) % hash_meta.bucket_num;
        offset_t node_off = hash_meta.base_off + hash * sizeof(IndexNode);
        node_offs.push_back(NodeOffset{remote_node_id, node_off});
    }

    assert(pending_hash_node_latch_offs.size() == 0);
    std::unordered_map<NodeOffset, char*> local_hash_nodes;
    std::unordered_map<NodeOffset, char*> cas_bufs;
    std::unordered_map<NodeOffset, std::list<std::pair<table_id_t, itemkey_t>>> delete_index_request_list;    

    // init local_hash_nodes and cas_bufs, and find_index_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<node_offs.size(); i++){
        auto node_off = node_offs[i];
        if(local_hash_nodes.find(node_off) == local_hash_nodes.end()){
            local_hash_nodes[node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));
        }
        if(cas_bufs.find(node_off) == cas_bufs.end()){
            cas_bufs[node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        }
        delete_index_request_list[node_off].push_back(std::make_pair(table_id[i], item_key[i]));
        pending_hash_node_latch_offs.emplace(node_off);
    }
    std::unordered_set<NodeOffset> unlock_node_off_with_write;
    std::unordered_set<NodeOffset> hold_node_off_latch;
    std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_offs.size()!=0) {
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off = ExclusiveLockHashNode(yield, QPType::kHashIndex, local_hash_nodes, cas_bufs);
        // init hold_node_off_latch
        for(auto node_off : succ_node_off ){
            hold_node_off_latch.emplace(node_off);
        }

        for(auto node_off : succ_node_off ){
            // read now
            IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_nodes[node_off]);
            // 遍历这个node_off上的所有请求即所有的hash index item 
            // 如果找到, 就从列表中移除
            for(auto it = delete_index_request_list[node_off].begin(); it != delete_index_request_list[node_off].end(); ){
                // find empty slot to insert
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (index_node->index_items[i].key == (*it).second && index_node->index_items[i].valid == true) {
                        // find it
                        index_node->index_items[i].key = 0;
                        index_node->index_items[i].rid = {INVALID_PAGE_ID, -1};
                        index_node->index_items[i].valid = false;
                        // erase会返回下一个元素的迭代器
                        it = delete_index_request_list[node_off].erase(it);
                        is_find = true;
                        break;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(delete_index_request_list[node_off].size() == 0){
                // release latch and write back
                auto release_node_off = node_off;
                while(true){
                    unlock_node_off_with_write.emplace(release_node_off);
                    if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
                    release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if HashIndex not exist, find next bucket
                node_id_t node_id = global_meta_man->GetHashIndexNode(delete_index_request_list[node_off].front().first);
                auto expand_node_id = index_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetHashIndexExpandBase(delete_index_request_list[node_off].front().first);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(IndexNode);
                if(expand_node_id < 0){
                    // fail to find hash index item to delete
                    RDMA_LOG(INFO) <<  "HashIndex: fail to find hash index item to delete";
                    auto release_node_off = node_off;
                    while(true){
                        unlock_node_off_with_write.emplace(release_node_off);
                        if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
                        release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
                    }
                }
                else{
                    // alloc next node read buffer and cas buffer
                    NodeOffset next_node_off{node_id, next_off};
                    pending_hash_node_latch_offs.emplace(next_node_off);
                    delete_index_request_list.emplace(next_node_off, delete_index_request_list.at(node_off));
                    hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
                    assert(local_hash_nodes.count(next_node_off) == 0);
                    assert(cas_bufs.count(next_node_off) == 0);
                    local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));
                    cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
                }
            }
        }
        // release all latch and write back
        for (auto node_off : unlock_node_off_with_write){
            ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes[node_off], QPType::kHashIndex);
            hold_node_off_latch.erase(node_off);
        }
        unlock_node_off_with_write.clear();
    }
    // 这里所有的latch都已经释放了
    assert(pending_hash_node_latch_offs.size() == 0);
    assert(hold_node_off_latch.size() == 0);
    return true;
}