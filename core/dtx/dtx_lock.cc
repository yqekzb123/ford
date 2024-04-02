// Author: huangdund
// Copyright (c) 2023

#include "dtx/dtx.h"
#include "exception.h"

// 这里锁表与哈希索引实现不同的点在于哈希索引要求索引数据全量存放
// 而锁表只有正在持有的锁是有用的，因此如果出现哈希桶已满的情况
// 可以选择将一个未上锁的数据项移除，并将新的数据项插入
// 这样可以避免哈希桶扩容，减少内存开销

// 辅助函数，给定一个哈希桶链的最后一个桶的偏移地址，用来在这个桶链的空闲位置插入一个共享锁
// 这个函数是要在lock_data_id上的桶链上选择一个空闲的位置上锁,
// 不在此函数内释放锁, 因为可能有多个lock_data_id在同一个桶链需要上锁   
bool DTX::InsertSharedLockIntoHashNodeList(std::vector<char*>& local_hash_nodes_vec, 
        LockDataId lockdataid, int last_idx, offset_t expand_base_off,
        std::unordered_map<int, int>& hold_latch_to_previouse_node_off){
    
    int idx = last_idx;
    std::unordered_map<int, int> hold_latch_to_next_node_off;
    while(hold_latch_to_previouse_node_off.count(idx) != 0){
        hold_latch_to_next_node_off[hold_latch_to_previouse_node_off.at(idx)] = idx;
        idx = hold_latch_to_previouse_node_off.at(idx);
    }

    // find empty slot to insert
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
    NodeOffset node_off = total_hash_node_offs_vec[idx];
    while (true) {
        // find lock item
        for(int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
            if (lock_node->lock_items[i].lock == UNLOCKED || lock_node->lock_items[i].valid == false) {
                // 在这里记录lock table item远程地址
                NodeOffset remote_off = {node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node};
                LockStatusItem lock_status_item = {lockdataid, local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, remote_off}; 
                shared_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);  
                // hold_shared_lock_data_id.emplace(lockdataid);            
                lock_node->lock_items[i].key = lockdataid;
                lock_node->lock_items[i].lock = 1;
                lock_node->lock_items[i].valid = true;
                // write remote here
                ExclusiveUnlockHashNode_RemoteWriteItem(remote_off.nodeId, remote_off.offset, 
                    local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, sizeof(LockItem), QPType::kLockTable);
                return true;
            }
        }
        auto expand_node_id = lock_node->next_expand_node_id[0];
        if(expand_node_id < 0){
            //TODO: no space for lock
            RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
            return false;
        }
        // 计算下一个桶的偏移地址
        idx = hold_latch_to_next_node_off[idx];
        node_off = total_hash_node_offs_vec[idx];
        lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
    }
    return true;
}

bool DTX::InsertExclusiveLockIntoHashNodeList(std::vector<char*>& local_hash_nodes_vec, 
        LockDataId lockdataid, int last_idx, offset_t expand_base_off,
        std::unordered_map<int, int>& hold_latch_to_previouse_node_off){
    
    int idx = last_idx;
    std::unordered_map<int, int> hold_latch_to_next_node_off;
    while(hold_latch_to_previouse_node_off.count(idx) != 0){
        hold_latch_to_next_node_off[hold_latch_to_previouse_node_off.at(idx)] = idx;
        idx = hold_latch_to_previouse_node_off.at(idx);
    }

    // find empty slot to insert
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
    NodeOffset node_off = total_hash_node_offs_vec[idx];
    while (true) {
        // find lock item
        for(int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
            if (lock_node->lock_items[i].lock == UNLOCKED || lock_node->lock_items[i].valid == false) {
                // 在这里记录lock table item远程地址
                NodeOffset remote_off = {node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node};
                LockStatusItem lock_status_item = {lockdataid, local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, remote_off}; 
                exclusive_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);  
                // hold_exclusive_lock_data_id.emplace(lockdataid);
                lock_node->lock_items[i].key = lockdataid;
                lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                lock_node->lock_items[i].valid = true;
                // write remote here
                ExclusiveUnlockHashNode_RemoteWriteItem(remote_off.nodeId, remote_off.offset, 
                    local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, sizeof(LockItem), QPType::kLockTable);
                return true;
            }
            if (lock_node->lock_items[i].lock == UNLOCKED || lock_node->lock_items[i].valid == false) {
                lock_node->lock_items[i].key = lockdataid;
                lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                lock_node->lock_items[i].valid = true;
                return true;
            }
        }
        auto expand_node_id = lock_node->next_expand_node_id[0];
        if(expand_node_id < 0){
            //TODO: no space for lock
            RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
            return false;
        }
        // 计算下一个桶的偏移地址
        idx = hold_latch_to_next_node_off[idx];
        node_off = total_hash_node_offs_vec[idx];
        lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
    }
    return true;
}

struct lock_table_request_list_item{
    LockDataId lock_data;
    int index;
};

// 这个函数是要对std::vector<LockDataId> lock_data_id上共享锁, 它们的偏移量分别是std::vector<offset_t> node_off
// 这里的offset是可能重复的, 返回No-wait上锁失败的所有LockDataID可以尝试多次
std::vector<LockDataId> DTX::LockSharedBatch(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){

    total_hash_node_offs_vec.clear();
    assert(total_hash_node_offs_vec.size() == 0);

    std::vector<std::vector<lock_table_request_list_item>> lock_request_list;
    lock_request_list.reserve(node_offs.size());
    total_hash_node_offs_vec.reserve(node_offs.size());

    // init total_hash_node_offs_vec
    for(int i=0; i<node_offs.size(); i++){
        auto index = std::find(total_hash_node_offs_vec.begin(), total_hash_node_offs_vec.end(), node_offs[i]);
        if(index == total_hash_node_offs_vec.end()){
            // not find
            total_hash_node_offs_vec.push_back(node_offs[i]);
            lock_request_list.emplace_back(std::vector<lock_table_request_list_item>({{lock_data_id[i], i}}));
        }else{
            // find
            lock_request_list[index - total_hash_node_offs_vec.begin()].push_back({lock_data_id[i], i});
        }
    }
    
    assert(pending_hash_node_latch_idx.size() == 0);
    pending_hash_node_latch_idx = std::vector<int>(total_hash_node_offs_vec.size());
    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> cas_bufs_vec(total_hash_node_offs_vec.size(), nullptr);
    // init local_hash_nodes and cas_bufs, and get_pagetable_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
        cas_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        pending_hash_node_latch_idx[i] = i;
    }

    // std::unordered_set<NodeOffset> unlock_node_off_with_write;
    // std::unordered_set<NodeOffset> unlock_node_off_no_write;
    // std::unordered_set<NodeOffset> hold_node_off_latch;

    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>
    std::vector<LockDataId> ret_lock_fail_data_id;
    int lock_data_id_size = lock_data_id.size();
    int last_hold_lock_cnt = shared_lock_item_localaddr_and_remote_offset.size();
    shared_lock_item_localaddr_and_remote_offset.reserve(last_hold_lock_cnt + lock_data_id.size());

    while (pending_hash_node_latch_idx.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ExclusiveLockHashNode(yield, QPType::kLockTable, local_hash_nodes_vec, cas_bufs_vec);
        // init hold_node_off_latch
        // for(auto node_off : succ_node_off ){
        //     hold_node_off_latch.emplace(node_off);
        // }

        for(auto idx : succ_node_off_idx ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
            NodeOffset node_off = total_hash_node_offs_vec[idx];
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            for(auto it = lock_request_list[idx].begin(); it != lock_request_list[idx].end(); ){
                // find lock item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (lock_node->lock_items[i].key == it->lock_data && lock_node->lock_items[i].valid == true) {
                        // not exclusive lock
                        if((lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS) == UNLOCKED){
                            // 记录lock table item远程地址
                            LockStatusItem lock_status_item = {it->lock_data, local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, 
                                NodeOffset{node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node}}; 
                            shared_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);                      
                            // hold_shared_lock_data_id.emplace(it->lock_data);
                            // lock shared lock
                            lock_node->lock_items[i].lock += 1;
                        }
                        else{
                            // LockDataId already locked
                            ret_lock_fail_data_id.emplace_back(it->lock_data);
                        }
                        // erase from lock_request_list
                        lock_request_list[idx].erase(it);
                        is_find = true;
                        break;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(lock_request_list[idx].size() == 0){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    ExclusiveUnlockHashNode_WithWrite(release_node_off, local_hash_nodes_vec[idx], QPType::kLockTable);
                    if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if LockDataId not exist, find next bucket
                // 一个表一定会进入一个哈希桶中
                table_id_t table_id = lock_request_list[idx].begin()->lock_data.table_id_;
                node_id_t node_id = global_meta_man->GetLockTableNode(table_id);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(table_id);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    for(auto lock_data_id : lock_request_list[idx]){
                        if(!InsertSharedLockIntoHashNodeList(local_hash_nodes_vec, 
                                lock_data_id.lock_data, idx, expand_base_off, hold_latch_to_previouse_node_off)){
                            // insert fail
                            ret_lock_fail_data_id.emplace_back(lock_data_id.lock_data);
                        }
                    }
                    // after insert, release latch and write back
                    auto release_idx = idx;
                    auto release_node_off = total_hash_node_offs_vec[release_idx];
                    while(true){
                        ExclusiveUnlockHashNode_WithWrite(release_node_off, local_hash_nodes_vec[idx], QPType::kLockTable);
                        if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                        release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                        release_node_off = total_hash_node_offs_vec[release_idx];
                    }
                }
                else{
                    // alloc next node read buffer and cas buffer
                    NodeOffset next_node_off{node_id, next_off};
                    int new_idx = total_hash_node_offs_vec.size() - 1;
                    pending_hash_node_latch_idx.push_back(new_idx);
                    lock_request_list.push_back(lock_request_list[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(cas_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode)));
                    cas_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }
    assert(shared_lock_item_localaddr_and_remote_offset.size() == last_hold_lock_cnt + lock_data_id_size - ret_lock_fail_data_id.size());
    return ret_lock_fail_data_id;
}


// 这个函数是要对std::vector<LockDataId> lock_data_id上共享锁, 它们的偏移量分别是std::vector<offset_t> node_off
// 这里的offset是可能重复的, 返回No-wait上锁失败的所有LockDataID可以尝试多次
std::vector<LockDataId> DTX::LockShared(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){
    // init
    total_hash_node_offs_vec = node_offs;
    
    assert(pending_hash_node_latch_idx.size() == 0);
    pending_hash_node_latch_idx = std::vector<int>(total_hash_node_offs_vec.size());
    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> cas_bufs_vec(total_hash_node_offs_vec.size(), nullptr);
    // init local_hash_nodes and cas_bufs, and get_pagetable_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
        cas_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        pending_hash_node_latch_idx[i] = i;
    }

    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>
    std::vector<LockDataId> ret_lock_fail_data_id;
    int lock_data_id_size = lock_data_id.size();
    int last_hold_lock_cnt = shared_lock_item_localaddr_and_remote_offset.size();
    shared_lock_item_localaddr_and_remote_offset.reserve(last_hold_lock_cnt + lock_data_id.size());

    int try_cnt = 0;
    while (pending_hash_node_latch_idx.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ExclusiveLockHashNode(yield, QPType::kLockTable, local_hash_nodes_vec, cas_bufs_vec);
        if(++try_cnt > MAX_TRY_LATCH){
            if(pending_hash_node_latch_idx.size() == 1){
                std::cout << "*-* try time out: " << total_hash_node_offs_vec[pending_hash_node_latch_idx[0]].offset << std::endl;
                ExclusiveUnlockHashNode_NoWrite(yield, total_hash_node_offs_vec[pending_hash_node_latch_idx[0]], QPType::kLockTable);
            }
            throw AbortException(tx_id);
        }
        if(try_cnt %5 == 0){
            // 流量控制
            // read now
            char* tmp_read_buf = thread_rdma_buffer_alloc->Alloc(8);
            coro_sched->RDMARead(coro_id, thread_qp_man->GetIndexQPPtrWithNodeID()[0], tmp_read_buf, 0, 8); 
            coro_sched->Yield(yield, coro_id);  
        }
        for(auto idx : succ_node_off_idx ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
            NodeOffset node_off = total_hash_node_offs_vec[idx];
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            bool is_find = false;
            for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                if (lock_node->lock_items[i].key == lock_data_id[idx] && lock_node->lock_items[i].valid == true) {
                    // not exclusive lock
                    if((lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS) == UNLOCKED){
                        // 记录lock table item远程地址
                        NodeOffset item_off{node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node};
                        LockStatusItem lock_status_item = {lock_data_id[idx], local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, item_off}; 
                        shared_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);            
                        // hold_shared_lock_data_id.emplace(lock_data_id[idx]);
                        // lock shared lock
                        lock_node->lock_items[i].lock += 1;
                        // write remote here
                        ExclusiveUnlockHashNode_RemoteWriteItem(item_off.nodeId, item_off.offset, 
                            local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, sizeof(LockItem), QPType::kLockTable);
                    }
                    else{
                        // LockDataId already locked
                        ret_lock_fail_data_id.emplace_back(lock_data_id[idx] );
                    }
                    is_find = true;
                    break;
                }
            }
            if(is_find){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    // 在这里释放latch
                    ExclusiveUnlockHashNode_NoWrite(yield, release_node_off, QPType::kLockTable);
                    if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if LockDataId not exist, find next bucket
                // 一个表一定会进入一个哈希桶中
                table_id_t table_id = lock_data_id[idx].table_id_;
                node_id_t node_id = global_meta_man->GetLockTableNode(table_id);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(table_id);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    if(!InsertSharedLockIntoHashNodeList(local_hash_nodes_vec, 
                            lock_data_id[idx], idx, expand_base_off, hold_latch_to_previouse_node_off)){
                        // insert fail
                        ret_lock_fail_data_id.emplace_back(lock_data_id[idx]);
                    }
                    // after insert, release latch and write back
                    auto release_idx = idx;
                    auto release_node_off = total_hash_node_offs_vec[release_idx];
                    while(true){
                        ExclusiveUnlockHashNode_NoWrite(yield, release_node_off, QPType::kLockTable);
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
                    lock_data_id.push_back(lock_data_id[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(cas_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode)));
                    cas_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }
    assert(shared_lock_item_localaddr_and_remote_offset.size() == last_hold_lock_cnt + lock_data_id_size - ret_lock_fail_data_id.size());
    return ret_lock_fail_data_id;
}

// 这个函数是要对std::vector<LockDataId> lock_data_id上共享锁, 它们的偏移量分别是std::vector<offset_t> node_off
// 这里的offset是可能重复的, 返回No-wait上锁失败的所有LockDataID可以尝试多次
std::vector<LockDataId> DTX::LockExclusiveBatch(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){

    total_hash_node_offs_vec.clear();
    assert(total_hash_node_offs_vec.size() == 0);

    std::vector<std::vector<lock_table_request_list_item>> lock_request_list;
    lock_request_list.reserve(node_offs.size());
    total_hash_node_offs_vec.reserve(node_offs.size());

    // init total_hash_node_offs_vec
    for(int i=0; i<node_offs.size(); i++){
        auto index = std::find(total_hash_node_offs_vec.begin(), total_hash_node_offs_vec.end(), node_offs[i]);
        if(index == total_hash_node_offs_vec.end()){
            // not find
            total_hash_node_offs_vec.push_back(node_offs[i]);
            lock_request_list.push_back(std::vector<lock_table_request_list_item>({{lock_data_id[i], i}}));
        }else{
            // find
            lock_request_list[index - total_hash_node_offs_vec.begin()].push_back({lock_data_id[i], i});
        }
    }
    
    assert(pending_hash_node_latch_idx.size() == 0);
    pending_hash_node_latch_idx = std::vector<int>(total_hash_node_offs_vec.size());
    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> cas_bufs_vec(total_hash_node_offs_vec.size(), nullptr);
    // init local_hash_nodes and cas_bufs, and get_pagetable_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
        cas_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        pending_hash_node_latch_idx[i] = i;
    }

    // std::unordered_set<NodeOffset> hold_node_off_latch;

    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>
    std::vector<LockDataId> ret_lock_fail_data_id;
    int lock_data_id_size = lock_data_id.size();
    int last_hold_lock_cnt = exclusive_lock_item_localaddr_and_remote_offset.size();
    exclusive_lock_item_localaddr_and_remote_offset.reserve(last_hold_lock_cnt + lock_data_id.size());

    while (pending_hash_node_latch_idx.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ExclusiveLockHashNode(yield, QPType::kLockTable, local_hash_nodes_vec, cas_bufs_vec);

        for(auto idx : succ_node_off_idx ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
            NodeOffset node_off = total_hash_node_offs_vec[idx];
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            for(auto it = lock_request_list[idx].begin(); it != lock_request_list[idx].end(); ){
                // find lock item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (lock_node->lock_items[i].key == it->lock_data && lock_node->lock_items[i].valid == true) {
                        // not exclusive lock
                        if(lock_node->lock_items[i].lock == UNLOCKED){
                            // 加入exclusive_lock_item_localaddr_and_remote_offset
                            LockStatusItem lock_status_item = {it->lock_data, local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, 
                                NodeOffset{node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node}};
                            exclusive_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);
                            // hold_exclusive_lock_data_id.emplace(it->lock_data);
                            // lock EXCLUSIVE lock
                            lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;    
                        }
                        else{
                            // LockDataId already locked
                            ret_lock_fail_data_id.emplace_back(it->lock_data);
                        }
                        // erase from lock_request_list
                        lock_request_list[idx].erase(it);
                        is_find = true;
                        break;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(lock_request_list[idx].size() == 0){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    ExclusiveUnlockHashNode_WithWrite(release_node_off, local_hash_nodes_vec[idx], QPType::kLockTable);
                    if(hold_latch_to_previouse_node_off.count(idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if LockDataId not exist, find next bucket
                // 一个表一定会进入一个哈希桶中
                table_id_t table_id = lock_request_list[idx].begin()->lock_data.table_id_;
                node_id_t node_id = global_meta_man->GetLockTableNode(table_id);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(table_id);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    for(auto lock_data_id : lock_request_list[idx]){
                        if(!InsertExclusiveLockIntoHashNodeList(local_hash_nodes_vec, 
                                lock_data_id.lock_data, idx, expand_base_off, hold_latch_to_previouse_node_off)){
                            // insert fail
                            ret_lock_fail_data_id.emplace_back(lock_data_id.lock_data);
                        }
                    }
                    // after insert, release latch and write back
                    auto release_idx = idx;
                    auto release_node_off = total_hash_node_offs_vec[release_idx];
                    while(true){
                        ExclusiveUnlockHashNode_WithWrite(release_node_off, local_hash_nodes_vec[idx], QPType::kLockTable);
                        if(hold_latch_to_previouse_node_off.count(idx) == 0) break;
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
                    lock_request_list.push_back(lock_request_list[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(cas_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode)));
                    cas_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }
    // 这里所有的latch都已经释放了
    // assert(hold_node_off_latch.size() == 0);
    assert(exclusive_lock_item_localaddr_and_remote_offset.size() == last_hold_lock_cnt + lock_data_id_size - ret_lock_fail_data_id.size());
    return ret_lock_fail_data_id;
}

std::vector<LockDataId> DTX::LockExclusive(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){

    // 计时
    // struct timespec tx_lock_time;
    // clock_gettime(CLOCK_REALTIME, &tx_lock_time);
    // init
    total_hash_node_offs_vec = node_offs;
    
    assert(pending_hash_node_latch_idx.size() == 0);
    pending_hash_node_latch_idx = std::vector<int>(total_hash_node_offs_vec.size());
    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> cas_bufs_vec(total_hash_node_offs_vec.size(), nullptr);
    // init local_hash_nodes and cas_bufs, and get_pagetable_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
        cas_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        pending_hash_node_latch_idx[i] = i;
    }
    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>
    std::vector<LockDataId> ret_lock_fail_data_id;
    int lock_data_id_size = lock_data_id.size();
    int last_hold_lock_cnt = exclusive_lock_item_localaddr_and_remote_offset.size();
    exclusive_lock_item_localaddr_and_remote_offset.reserve(last_hold_lock_cnt + lock_data_id.size());

    int try_cnt = 0;
    while (pending_hash_node_latch_idx.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ExclusiveLockHashNode(yield, QPType::kLockTable, local_hash_nodes_vec, cas_bufs_vec);
        if(++try_cnt > MAX_TRY_LATCH){
            if(pending_hash_node_latch_idx.size() == 1){
                std::cout << "*-* try time out: " << total_hash_node_offs_vec[pending_hash_node_latch_idx[0]].offset << std::endl;
                ExclusiveUnlockHashNode_NoWrite(yield, total_hash_node_offs_vec[pending_hash_node_latch_idx[0]], QPType::kLockTable);
            }
            throw AbortException(tx_id);
        }
        if(try_cnt %5 == 0){
            // 流量控制
            // read now
            char* tmp_read_buf = thread_rdma_buffer_alloc->Alloc(8);
            coro_sched->RDMARead(coro_id, thread_qp_man->GetIndexQPPtrWithNodeID()[0], tmp_read_buf, 0, 8); 
            coro_sched->Yield(yield, coro_id);  
        }
        
        for(auto idx : succ_node_off_idx ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes_vec[idx]);
            NodeOffset node_off = total_hash_node_offs_vec[idx];
            // 如果找到, 就从列表中移除
            // find lock item
            bool is_find = false;
            for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                if (lock_node->lock_items[i].key == lock_data_id[idx] && lock_node->lock_items[i].valid == true) {
                    // not exclusive lock
                    if(lock_node->lock_items[i].lock == UNLOCKED){
                        // 加入exclusive_lock_item_localaddr_and_remote_offset
                        NodeOffset item_off{node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node};
                        LockStatusItem lock_status_item = {lock_data_id[idx], local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, item_off};
                        exclusive_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);
                        // hold_exclusive_lock_data_id.emplace(lock_data_id[idx]);
                        // lock EXCLUSIVE lock
                        lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                        // write remote here
                        ExclusiveUnlockHashNode_RemoteWriteItem(item_off.nodeId, item_off.offset, 
                            local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, sizeof(LockItem), QPType::kLockTable);
                    }
                    else if(lock_node->lock_items[i].lock == 1){
                        bool is_find_in_shard_lock = false;
                        for(int i=0; i < shared_lock_item_localaddr_and_remote_offset.size();){
                            if(shared_lock_item_localaddr_and_remote_offset[i].lock_data_id == lock_data_id[idx]){
                                is_find_in_shard_lock = true;
                                shared_lock_item_localaddr_and_remote_offset.erase(shared_lock_item_localaddr_and_remote_offset.begin() + i);
                                NodeOffset item_off{node_off.nodeId, node_off.offset + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node};
                                LockStatusItem lock_status_item = {lock_data_id[idx], local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, item_off};
                                exclusive_lock_item_localaddr_and_remote_offset.push_back(lock_status_item);
                                // lock EXCLUSIVE lock
                                lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                                // write remote here
                                ExclusiveUnlockHashNode_RemoteWriteItem(item_off.nodeId, item_off.offset, 
                                    local_hash_nodes_vec[idx] + (offset_t)&lock_node->lock_items[i] - (offset_t)lock_node, sizeof(LockItem), QPType::kLockTable);
                                break;
                            } else{
                                i++;
                            }
                        }
                        if(!is_find_in_shard_lock){
                            // LockDataId already locked
                            ret_lock_fail_data_id.emplace_back(lock_data_id[idx]);
                        }
                    }
                    else{
                        // LockDataId already locked
                        ret_lock_fail_data_id.emplace_back(lock_data_id[idx]);
                    }
                    is_find = true;
                    break;
                }
            }
            if(is_find){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    ExclusiveUnlockHashNode_NoWrite(yield, release_node_off, QPType::kLockTable);
                    if(hold_latch_to_previouse_node_off.count(idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if LockDataId not exist, find next bucket
                // 一个表一定会进入一个哈希桶中
                table_id_t table_id = lock_data_id[idx].table_id_;
                node_id_t node_id = global_meta_man->GetLockTableNode(table_id);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(table_id);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    if(!InsertExclusiveLockIntoHashNodeList(local_hash_nodes_vec, 
                            lock_data_id[idx], idx, expand_base_off, hold_latch_to_previouse_node_off)){
                        // insert fail
                        ret_lock_fail_data_id.emplace_back(lock_data_id[idx]);
                    }
                    // after insert, release latch and write back
                    auto release_idx = idx;
                    auto release_node_off = total_hash_node_offs_vec[release_idx];
                    while(true){
                        ExclusiveUnlockHashNode_NoWrite(yield, release_node_off, QPType::kLockTable);
                        // ExclusiveUnlockHashNode_WithWrite(release_node_off, local_hash_nodes_vec[idx], QPType::kLockTable);
                        if(hold_latch_to_previouse_node_off.count(idx) == 0) break;
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
                    lock_data_id.push_back(lock_data_id[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(cas_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode)));
                    cas_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }
    // !这里lock_data_id不对
    assert(exclusive_lock_item_localaddr_and_remote_offset.size() == last_hold_lock_cnt + lock_data_id_size - ret_lock_fail_data_id.size());

    // struct timespec tx_lock_end_time;
    // clock_gettime(CLOCK_REALTIME, &tx_lock_end_time);
    // double lock_usec = (tx_lock_end_time.tv_sec - tx_lock_time.tv_sec) * 1000000 + (double)(tx_lock_end_time.tv_nsec - tx_lock_time.tv_nsec) / 1000;
    // printf("lock time: %lf us\n", lock_usec);

    return ret_lock_fail_data_id;
}

// 解除共享锁
bool DTX::UnlockShared(coro_yield_t& yield) {
    // 将shared_lock_item_localaddr_and_remote_offset中的item解锁
    // 修改页表中的item

    for(int i=0; i<shared_lock_item_localaddr_and_remote_offset.size(); i++){
        // debug
        // std::cout << "unlock shared " << shared_lock_item_localaddr_and_remote_offset[i].lock_data_id.table_id_ << " " 
        //     << shared_lock_item_localaddr_and_remote_offset[i].lock_data_id.itemkey_ << "\n";
        char* local_item = shared_lock_item_localaddr_and_remote_offset[i].localaddr;
        NodeOffset node_offset = shared_lock_item_localaddr_and_remote_offset[i].node_off;
        LockItem* item = reinterpret_cast<LockItem*>(local_item);

        RCQP* qp = thread_qp_man->GetRemoteLockQPWithNodeID(node_offset.nodeId);

        char* faa_cnt = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        // !hcy 这里存疑，可能存在问题
        if(!coro_sched->RDMAFAA(coro_id, qp, faa_cnt, node_offset.offset + (offset_t)&(item->lock) - (offset_t)item, SHARED_UNLOCK_TO_BE_ADDED))
            assert(false);
    }
}

// 解除排他锁
bool DTX::UnlockExclusive(coro_yield_t& yield) {
    // 将exclusive_lock_item_localaddr_and_remote_offset中的item解锁
    // 修改页表中的item
    
    for(int i=0; i<exclusive_lock_item_localaddr_and_remote_offset.size(); i++){
        // debug
        // std::cout << "unlock exclusive " << exclusive_lock_item_localaddr_and_remote_offset[i].lock_data_id.table_id_ << " " 
        //     << exclusive_lock_item_localaddr_and_remote_offset[i].lock_data_id.itemkey_ << "\n";

        char* local_item = exclusive_lock_item_localaddr_and_remote_offset[i].localaddr;
        NodeOffset node_offset = exclusive_lock_item_localaddr_and_remote_offset[i].node_off;
        LockItem* item = reinterpret_cast<LockItem*>(local_item);

        RCQP* qp = thread_qp_man->GetRemoteLockQPWithNodeID(node_offset.nodeId);

        char* faa_cnt = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        // !hcy 这里存疑，可能存在问题
        if(!coro_sched->RDMAFAA(coro_id, qp, faa_cnt, node_offset.offset + (offset_t)&(item->lock) - (offset_t)item, EXCLUSIVE_UNLOCK_TO_BE_ADDED))
            assert(false);
    }
}

// // 解除共享锁 
// bool DTX::UnlockShared(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){
//     assert(pending_hash_node_latch_offs.size() == 0);

//     std::unordered_map<NodeOffset, char*> local_hash_nodes;
//     std::unordered_map<NodeOffset, char*> cas_bufs;
//     std::unordered_map<NodeOffset, std::list<LockDataId>> unlock_request_list;

//     // init pending_hash_node_latch_offs
//     for(int i=0; i<node_offs.size(); i++){
//         pending_hash_node_latch_offs.emplace(node_offs[i]);
//         if(local_hash_nodes.count(node_offs[i]) == 0){
//             // Alloc Node Read Buffer
//             local_hash_nodes[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
//         }
//         if(cas_bufs.count(node_offs[i]) == 0){
//             // Alloc latch cas Buffer
//             cas_bufs[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//         }
//         unlock_request_list[node_offs[i]].emplace_back(lock_data_id[i]);
//     }

//     std::unordered_set<NodeOffset> unlock_node_off_with_write;
//     std::unordered_set<NodeOffset> hold_node_off_latch;

//     std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

//     while (pending_hash_node_latch_offs.size()!=0){
//         // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
//         auto succ_node_off = ExclusiveLockHashNode(yield, QPType::kLockTable, local_hash_nodes, cas_bufs);
//         // init hold_node_off_latch
//         for(auto node_off : succ_node_off ){
//             hold_node_off_latch.emplace(node_off);
//         }

//         for(auto node_off : succ_node_off ){
//             // read now
//             LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
//             // 遍历这个node_off上的所有请求即所有的lock_data_id, 
//             // 如果找到, 就从列表中移除
//             for(auto it = unlock_request_list[node_off].begin(); it != unlock_request_list[node_off].end(); ){
//                 // find lock item
//                 bool is_find = false;
//                 for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
//                     if (lock_node->lock_items[i].key == *it && lock_node->lock_items[i].valid == true) {
//                         // lock EXCLUSIVE lock
//                         assert((lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED);
                            
//                         lock_node->lock_items[i].lock--;

//                         // erase from lock_request_list
//                         it = unlock_request_list[node_off].erase(it);
//                         is_find = true;
//                         break;
//                     }
//                 }
//                 // not find
//                 if(!is_find) it++;
//             }

//             // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
//             if(unlock_request_list[node_off].size() == 0){
//                 // release latch and write back
//                 auto release_node_off = node_off;
//                 while(true){
//                     unlock_node_off_with_write.emplace(release_node_off);
//                     if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
//                     release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
//                 }
//             }
//             else{
//                 assert(false);
//             }
//         }
//         // release all latch and write back
//         for (auto node_off : unlock_node_off_with_write){
//             ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes.at(node_off), QPType::kLockTable);
//             hold_node_off_latch.erase(node_off);
//         }
//         unlock_node_off_with_write.clear();
//     }
//     // 这里所有的latch都已经释放了
//     assert(hold_node_off_latch.size() == 0);

//     hold_shared_lock_data_id.clear();
//     hold_shared_lock_node_offs.clear();
    
//     return true;
// }

// // 解除排他锁
// bool DTX::UnlockExclusive(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){
//     assert(pending_hash_node_latch_offs.size() == 0);

//     std::unordered_map<NodeOffset, char*> local_hash_nodes;
//     std::unordered_map<NodeOffset, char*> cas_bufs;
//     std::unordered_map<NodeOffset, std::list<LockDataId>> unlock_request_list;

//     // init pending_hash_node_latch_offs
//     for(int i=0; i<node_offs.size(); i++){
//         pending_hash_node_latch_offs.emplace(node_offs[i]);
//         if(local_hash_nodes.count(node_offs[i]) == 0){
//             // Alloc Node Read Buffer
//             local_hash_nodes[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
//         }
//         if(cas_bufs.count(node_offs[i]) == 0){
//             // Alloc latch cas Buffer
//             cas_bufs[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//         }
//         unlock_request_list[node_offs[i]].emplace_back(lock_data_id[i]);
//     }

//     std::unordered_set<NodeOffset> unlock_node_off_with_write;
//     std::unordered_set<NodeOffset> hold_node_off_latch;

//     std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

//     while (pending_hash_node_latch_offs.size()!=0){
//         // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
//         auto succ_node_off = ExclusiveLockHashNode(yield, QPType::kLockTable, local_hash_nodes, cas_bufs);
//         // init hold_node_off_latch
//         for(auto node_off : succ_node_off ){
//             hold_node_off_latch.emplace(node_off);
//         }

//         for(auto node_off : succ_node_off ){
//             // read now
//             LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
//             // 遍历这个node_off上的所有请求即所有的lock_data_id, 
//             // 如果找到, 就从列表中移除
//             for(auto it = unlock_request_list[node_off].begin(); it != unlock_request_list[node_off].end(); ){
//                 // find lock item
//                 bool is_find = false;
//                 for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
//                     if (lock_node->lock_items[i].key == *it && lock_node->lock_items[i].valid == true) {
//                         // lock EXCLUSIVE lock
//                         assert((lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS) == EXCLUSIVE_LOCKED);
                            
//                         lock_node->lock_items[i].lock = UNLOCKED;

//                         // erase from lock_request_list
//                         it = unlock_request_list[node_off].erase(it);
//                         is_find = true;
//                         break;
//                     }
//                 }
//                 // not find
//                 if(!is_find) it++;
//             }

//             // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
//             if(unlock_request_list[node_off].size() == 0){
//                 // release latch and write back
//                 auto release_node_off = node_off;
//                 while(true){
//                     unlock_node_off_with_write.emplace(release_node_off);
//                     if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
//                     release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
//                 }
//             }
//             else{
//                 assert(false);
//             }
//         }
//         // release all latch and write back
//         for (auto node_off : unlock_node_off_with_write){
//             ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes.at(node_off), QPType::kLockTable);
//             hold_node_off_latch.erase(node_off);
//         }
//         unlock_node_off_with_write.clear();
//     }
//     // 这里所有的latch都已经释放了
//     assert(hold_node_off_latch.size() == 0);

//     hold_exclusive_lock_data_id.clear();
//     hold_exclusive_lock_node_offs.clear();
//     return true;
// }

// ***********************************************************************************
// public functions
// 对表上共享锁
bool DTX::LockSharedOnTable(coro_yield_t& yield, std::vector<table_id_t> table_id) {

    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;
    std::unordered_set<LockDataId> lock_data_id_set; // 去重使用

    batch_lock_data_id.reserve(table_id.size());
    batch_node_off.reserve(table_id.size());

    for(auto table_id : table_id){
        auto lock_data_id = LockDataId(table_id, LockDataType::TABLE);

        // check if hold shared lock in this txn.
        auto iter = std::find_if(shared_lock_item_localaddr_and_remote_offset.begin(), shared_lock_item_localaddr_and_remote_offset.end(), 
            [lock_data_id](LockStatusItem item){return item.lock_data_id == lock_data_id;});
        if(iter != shared_lock_item_localaddr_and_remote_offset.end()) continue;

        if(lock_data_id_set.count(lock_data_id) != 0) continue;
        else lock_data_id_set.emplace(lock_data_id);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockShared(yield, std::move(batch_lock_data_id), std::move(batch_node_off)).size() == 0){
        return true;
    }
    return false;
}

// 对表上排他锁
bool DTX::LockExclusiveOnTable(coro_yield_t& yield, std::vector<table_id_t> table_id){

    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;
    std::unordered_set<LockDataId> lock_data_id_set; // 去重使用

    batch_lock_data_id.reserve(table_id.size());
    batch_node_off.reserve(table_id.size());

    for(auto table_id : table_id){
        auto lock_data_id = LockDataId(table_id, LockDataType::TABLE);
        
        // check if hold shared lock in this txn.
        auto iter = std::find_if(exclusive_lock_item_localaddr_and_remote_offset.begin(), exclusive_lock_item_localaddr_and_remote_offset.end(), 
            [lock_data_id](LockStatusItem item){return item.lock_data_id == lock_data_id;});
        if(iter != exclusive_lock_item_localaddr_and_remote_offset.end()) continue;

        if(lock_data_id_set.count(lock_data_id) != 0) continue;
        else lock_data_id_set.emplace(lock_data_id);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockExclusive(yield, std::move(batch_lock_data_id), std::move(batch_node_off)).size() == 0){
        return true;
    }
    return false;
}

// 对记录上共享锁
bool DTX::LockSharedOnRecord(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){
    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;
    // std::unordered_set<LockDataId> lock_data_id_set; // 去重使用

    batch_lock_data_id.reserve(table_id.size());
    batch_node_off.reserve(table_id.size());

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RECORD);
        
        // if(lock_data_id_set.count(lock_data_id) != 0) continue;
        // else lock_data_id_set.emplace(lock_data_id);
        
        // check if hold shared lock in this txn.
        auto iter = std::find_if(shared_lock_item_localaddr_and_remote_offset.begin(), shared_lock_item_localaddr_and_remote_offset.end(), 
            [lock_data_id](LockStatusItem item){return item.lock_data_id == lock_data_id;});
        if(iter != shared_lock_item_localaddr_and_remote_offset.end()) continue;

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }
    // printf("dtx_lock.cc:1003 batch %ld acquire shared lock %ld key\n", batch_id, batch_lock_data_id.size());

    if(LockShared(yield, std::move(batch_lock_data_id), std::move(batch_node_off)).size() == 0){
        return true;
    }
    return false;
}

// 对记录上排他锁
bool DTX::LockExclusiveOnRecord(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){
    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;
    // std::unordered_set<LockDataId> lock_data_id_set; // 去重使用

    batch_lock_data_id.reserve(table_id.size());
    batch_node_off.reserve(table_id.size());

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RECORD);

        // check if hold shared lock in this txn.
        auto iter = std::find_if(exclusive_lock_item_localaddr_and_remote_offset.begin(), exclusive_lock_item_localaddr_and_remote_offset.end(), 
            [lock_data_id](LockStatusItem item){return item.lock_data_id == lock_data_id;});
        if(iter != exclusive_lock_item_localaddr_and_remote_offset.end()) continue;

        // if(lock_data_id_set.count(lock_data_id) != 0) continue;
        // else lock_data_id_set.emplace(lock_data_id);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }
    // printf("dtx_lock.cc:1043 batch %ld acquire exclusive lock %ld key\n", batch_id, batch_lock_data_id.size());

    if(LockExclusive(yield, std::move(batch_lock_data_id), std::move(batch_node_off)).size() == 0){
        return true;
    }
    return false;
}

// 对范围上共享锁
bool DTX::LockSharedOnRange(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){

    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;
    std::unordered_set<LockDataId> lock_data_id_set; // 去重使用

    batch_lock_data_id.reserve(table_id.size());
    batch_node_off.reserve(table_id.size());

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RANGE);

        // check if hold shared lock in this txn.
        auto iter = std::find_if(shared_lock_item_localaddr_and_remote_offset.begin(), shared_lock_item_localaddr_and_remote_offset.end(), 
            [lock_data_id](LockStatusItem item){return item.lock_data_id == lock_data_id;});
        if(iter != shared_lock_item_localaddr_and_remote_offset.end()) continue;

        if(lock_data_id_set.count(lock_data_id) != 0) continue;
        else lock_data_id_set.emplace(lock_data_id);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockShared(yield, std::move(batch_lock_data_id), std::move(batch_node_off)).size() == 0){
        return true;
    }
    return false;
}

// 对范围上排他锁
bool DTX::LockExclusiveOnRange(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){
    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;
    std::unordered_set<LockDataId> lock_data_id_set; // 去重使用

    batch_lock_data_id.reserve(table_id.size());
    batch_node_off.reserve(table_id.size());

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RECORD);

        // check if hold shared lock in this txn.
        auto iter = std::find_if(exclusive_lock_item_localaddr_and_remote_offset.begin(), exclusive_lock_item_localaddr_and_remote_offset.end(), 
            [lock_data_id](LockStatusItem item){return item.lock_data_id == lock_data_id;});
        if(iter != exclusive_lock_item_localaddr_and_remote_offset.end()) continue;

        if(lock_data_id_set.count(lock_data_id) != 0) continue;
        else lock_data_id_set.emplace(lock_data_id);
        
        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockExclusive(yield, std::move(batch_lock_data_id), std::move(batch_node_off)).size() == 0){
        return true;
    }
    return false;
}

