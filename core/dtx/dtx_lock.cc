#include "dtx/dtx.h"
#include "dtx/rwlock.h"

// 这里锁表与哈希索引实现不同的点在于哈希索引要求索引数据全量存放
// 而锁表只有正在持有的锁是有用的，因此如果出现哈希桶已满的情况
// 可以选择将一个未上锁的数据项移除，并将新的数据项插入
// 这样可以避免哈希桶扩容，减少内存开销

// 这个函数是要在lock_data_id上的桶链上选择一个空闲的位置上锁,
// 不在此函数内释放锁, 因为可能有多个lock_data_id在同一个桶链需要上锁
bool DTX::InsertSharedLockIntoHashNodeList(std::unordered_map<NodeOffset, char*>& local_hash_nodes, 
        LockDataId lockdataid, NodeOffset last_node_off, 
         std::unordered_map<NodeOffset, NodeOffset>& hold_latch_to_previouse_node_off){
    
    offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(lockdataid.table_id_);
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[last_node_off]);

    NodeOffset node_off = last_node_off;
    while(hold_latch_to_previouse_node_off.count(node_off) != 0){
        node_off = hold_latch_to_previouse_node_off.at(node_off);
    }

    while (true) {
        // find lock item
        bool find = false;
        for(int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
            if (lock_node->lock_items[i].lock == UNLOCKED || lock_node->lock_items[i].valid == false) {
                lock_node->lock_items[i].key = lockdataid;
                lock_node->lock_items[i].lock = 1;
                lock_node->lock_items[i].valid = true;
                find = true;
                break;
            }
        }
        if(find) return true;
        // not find
        else{
            auto expand_node_id = lock_node->next_expand_node_id[0];
            if(expand_node_id < 0){
                //TODO: no space for lock
                RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
                return false;
            }
            lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
        }
    }
    return true;
}

bool DTX::InsertExclusiveLockIntoHashNodeList(std::unordered_map<NodeOffset, char*>& local_hash_nodes, 
        LockDataId lockdataid, NodeOffset last_node_off, 
         std::unordered_map<NodeOffset, NodeOffset>& hold_latch_to_previouse_node_off){
    
    offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(lockdataid.table_id_);
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[last_node_off]);

    NodeOffset node_off = last_node_off;
    while(hold_latch_to_previouse_node_off.count(node_off) != 0){
        node_off = hold_latch_to_previouse_node_off.at(node_off);
    }

    while (true) {
        // find lock item
        bool find = false;
        for(int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
            if (lock_node->lock_items[i].lock == UNLOCKED || lock_node->lock_items[i].valid == false) {
                lock_node->lock_items[i].key = lockdataid;
                lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                lock_node->lock_items[i].valid = true;
                find = true;
                break;
            }
        }
        if(find) return true;
        // not find
        else{
            auto expand_node_id = lock_node->next_expand_node_id[0];
            if(expand_node_id < 0){
                //TODO: no space for lock
                RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
                return false;
            }
            lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
        }
    }
    return true;
}

// 这个函数是要对std::vector<LockDataId> lock_data_id上共享锁, 它们的偏移量分别是std::vector<offset_t> node_off
// 这里的offset是可能重复的, 返回No-wait上锁失败的所有LockDataID可以尝试多次
std::vector<LockDataId> DTX::LockShared(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){

    std::vector<LockDataId> ret_lock_fail_data_id;

    assert(pending_hash_node_latch_offs.size() == 0);

    std::unordered_map<NodeOffset, char*> local_hash_nodes;
    std::unordered_map<NodeOffset, char*> cas_bufs;
    std::unordered_map<NodeOffset, std::list<LockDataId>> lock_request_list;

    // init pending_hash_node_latch_offs
    for(int i=0; i<node_offs.size(); i++){
        pending_hash_node_latch_offs.emplace(node_offs[i]);
        if(local_hash_nodes.count(node_offs[i]) == 0){
            // Alloc Node Read Buffer
            local_hash_nodes[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
        }
        if(cas_bufs.count(node_offs[i]) == 0){
            // Alloc latch cas Buffer
            cas_bufs[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        }
        lock_request_list[node_offs[i]].emplace_back(lock_data_id[i]);
    }

    std::unordered_set<NodeOffset> unlock_node_off_with_write;
    // std::unordered_set<NodeOffset> unlock_node_off_no_write;
    std::unordered_set<NodeOffset> hold_node_off_latch;

    std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_offs.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off = ExclusiveLockHashNode(yield, local_hash_nodes, cas_bufs);
        // init hold_node_off_latch
        for(auto node_off : succ_node_off ){
            hold_node_off_latch.emplace(node_off);
        }

        for(auto node_off : succ_node_off ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            for(auto it = lock_request_list[node_off].begin(); it != lock_request_list[node_off].end(); ){
                // find lock item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (lock_node->lock_items[i].key == *it && lock_node->lock_items[i].valid == true) {
                        // not exclusive lock
                        if(lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS == UNLOCKED){
                            // lock shared lock
                            lock_node->lock_items[i].lock += 1;
                        }
                        else{
                            // LockDataId already locked
                            ret_lock_fail_data_id.emplace_back(lock_data_id);
                        }
                        // erase from lock_request_list
                        lock_request_list[node_off].erase(it);
                        is_find = true;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(lock_request_list[node_off].size() == 0){
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
                // if LockDataId not exist, find next bucket
                node_id_t node_id = global_meta_man->GetLockTableNode(lock_request_list[node_off].front().table_id_);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(lock_request_list[node_off].front().table_id_);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    for(auto lock_data_id : lock_request_list[node_off]){
                        if(!InsertSharedLockIntoHashNodeList(local_hash_nodes, lock_data_id, node_off, hold_latch_to_previouse_node_off)){
                            // insert fail
                            ret_lock_fail_data_id.emplace_back(lock_data_id);
                        }
                    }
                    // after insert, release latch and write back
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
                    lock_request_list.emplace(next_node_off, lock_request_list.at(node_off));
                    hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
                    assert(local_hash_nodes.count(next_node_off) == 0);
                    assert(cas_bufs.count(next_node_off) == 0);
                    local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
                    cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
                }
            }
        }
        // release all latch and write back
        for (auto node_off : unlock_node_off_with_write){
            ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes.at(node_off));
            hold_node_off_latch.erase(node_off);
        }
        unlock_node_off_with_write.clear();
    }
    // 这里所有的latch都已经释放了
    assert(hold_node_off_latch.size() == 0);
    return ret_lock_fail_data_id;
}

std::vector<LockDataId> DTX::LockExclusive(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){

    std::vector<LockDataId> ret_lock_fail_data_id;

    assert(pending_hash_node_latch_offs.size() == 0);

    std::unordered_map<NodeOffset, char*> local_hash_nodes;
    std::unordered_map<NodeOffset, char*> cas_bufs;
    std::unordered_map<NodeOffset, std::list<LockDataId>> lock_request_list;

    // init pending_hash_node_latch_offs
    for(int i=0; i<node_offs.size(); i++){
        pending_hash_node_latch_offs.emplace(node_offs[i]);
        if(local_hash_nodes.count(node_offs[i]) == 0){
            // Alloc Node Read Buffer
            local_hash_nodes[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
        }
        if(cas_bufs.count(node_offs[i]) == 0){
            // Alloc latch cas Buffer
            cas_bufs[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        }
        lock_request_list[node_offs[i]].emplace_back(lock_data_id[i]);
    }

    std::unordered_set<NodeOffset> unlock_node_off_with_write;
    // std::unordered_set<NodeOffset> unlock_node_off_no_write;
    std::unordered_set<NodeOffset> hold_node_off_latch;

    std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_offs.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off = ExclusiveLockHashNode(yield, local_hash_nodes, cas_bufs);
        // init hold_node_off_latch
        for(auto node_off : succ_node_off ){
            hold_node_off_latch.emplace(node_off);
        }

        for(auto node_off : succ_node_off ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            for(auto it = lock_request_list[node_off].begin(); it != lock_request_list[node_off].end(); ){
                // find lock item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (lock_node->lock_items[i].key == *it && lock_node->lock_items[i].valid == true) {
                        // not exclusive lock
                        if(lock_node->lock_items[i].lock == UNLOCKED){
                            // lock EXCLUSIVE lock
                            lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                        }
                        else{
                            // LockDataId already locked
                            ret_lock_fail_data_id.emplace_back(lock_data_id);
                        }
                        // erase from lock_request_list
                        lock_request_list[node_off].erase(it);
                        is_find = true;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(lock_request_list[node_off].size() == 0){
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
                // if LockDataId not exist, find next bucket
                node_id_t node_id = global_meta_man->GetLockTableNode(lock_request_list[node_off].front().table_id_);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(lock_request_list[node_off].front().table_id_);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get and insert it
                    for(auto lock_data_id : lock_request_list[node_off]){
                        if(!InsertSharedLockIntoHashNodeList(local_hash_nodes, lock_data_id, node_off, hold_latch_to_previouse_node_off)){
                            // insert fail
                            ret_lock_fail_data_id.emplace_back(lock_data_id);
                        }
                    }
                    // after insert, release latch and write back
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
                    lock_request_list.emplace(next_node_off, lock_request_list.at(node_off));
                    hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
                    assert(local_hash_nodes.count(next_node_off) == 0);
                    assert(cas_bufs.count(next_node_off) == 0);
                    local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
                    cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
                }
            }
        }
        // release all latch and write back
        for (auto node_off : unlock_node_off_with_write){
            ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes.at(node_off));
            hold_node_off_latch.erase(node_off);
        }
        unlock_node_off_with_write.clear();
    }
    // 这里所有的latch都已经释放了
    assert(hold_node_off_latch.size() == 0);
    return ret_lock_fail_data_id;
}

// 解除共享锁 
bool DTX::UnlockShared(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){
    assert(pending_hash_node_latch_offs.size() == 0);

    std::unordered_map<NodeOffset, char*> local_hash_nodes;
    std::unordered_map<NodeOffset, char*> cas_bufs;
    std::unordered_map<NodeOffset, std::list<LockDataId>> lock_request_list;

    // init pending_hash_node_latch_offs
    for(int i=0; i<node_offs.size(); i++){
        pending_hash_node_latch_offs.emplace(node_offs[i]);
        if(local_hash_nodes.count(node_offs[i]) == 0){
            // Alloc Node Read Buffer
            local_hash_nodes[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
        }
        if(cas_bufs.count(node_offs[i]) == 0){
            // Alloc latch cas Buffer
            cas_bufs[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        }
        lock_request_list[node_offs[i]].emplace_back(lock_data_id[i]);
    }

    std::unordered_set<NodeOffset> unlock_node_off_with_write;
    std::unordered_set<NodeOffset> hold_node_off_latch;

    std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_offs.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off = ExclusiveLockHashNode(yield, local_hash_nodes, cas_bufs);
        // init hold_node_off_latch
        for(auto node_off : succ_node_off ){
            hold_node_off_latch.emplace(node_off);
        }

        for(auto node_off : succ_node_off ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            for(auto it = lock_request_list[node_off].begin(); it != lock_request_list[node_off].end(); ){
                // find lock item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (lock_node->lock_items[i].key == *it && lock_node->lock_items[i].valid == true) {
                        // lock EXCLUSIVE lock
                        assert(lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS != EXCLUSIVE_LOCKED);
                            
                        lock_node->lock_items[i].lock--;

                        // erase from lock_request_list
                        lock_request_list[node_off].erase(it);
                        is_find = true;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(lock_request_list[node_off].size() == 0){
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
                // if LockDataId not exist, find next bucket
                node_id_t node_id = global_meta_man->GetLockTableNode(lock_request_list[node_off].front().table_id_);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(lock_request_list[node_off].front().table_id_);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get but couldn't find it
                    for(auto lock_data_id : lock_request_list[node_off]){
                        RDMA_LOG(ERROR) <<  "LockTableStore::UnlockShared: lock item not exist" ;
                    }
                    // after insert, release latch and write back
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
                    lock_request_list.emplace(next_node_off, lock_request_list.at(node_off));
                    hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
                    assert(local_hash_nodes.count(next_node_off) == 0);
                    assert(cas_bufs.count(next_node_off) == 0);
                    local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
                    cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
                }
            }
        }
        // release all latch and write back
        for (auto node_off : unlock_node_off_with_write){
            ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes.at(node_off));
            hold_node_off_latch.erase(node_off);
        }
        unlock_node_off_with_write.clear();
    }
    // 这里所有的latch都已经释放了
    assert(hold_node_off_latch.size() == 0);
    return true;
}

// 解除排他锁
bool DTX::UnlockExclusive(coro_yield_t& yield, std::vector<LockDataId> lock_data_id, std::vector<NodeOffset> node_offs){
    assert(pending_hash_node_latch_offs.size() == 0);

    std::unordered_map<NodeOffset, char*> local_hash_nodes;
    std::unordered_map<NodeOffset, char*> cas_bufs;
    std::unordered_map<NodeOffset, std::list<LockDataId>> lock_request_list;

    // init pending_hash_node_latch_offs
    for(int i=0; i<node_offs.size(); i++){
        pending_hash_node_latch_offs.emplace(node_offs[i]);
        if(local_hash_nodes.count(node_offs[i]) == 0){
            // Alloc Node Read Buffer
            local_hash_nodes[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
        }
        if(cas_bufs.count(node_offs[i]) == 0){
            // Alloc latch cas Buffer
            cas_bufs[node_offs[i]] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        }
        lock_request_list[node_offs[i]].emplace_back(lock_data_id[i]);
    }

    std::unordered_set<NodeOffset> unlock_node_off_with_write;
    std::unordered_set<NodeOffset> hold_node_off_latch;

    std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>

    while (pending_hash_node_latch_offs.size()!=0){
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off = ExclusiveLockHashNode(yield, local_hash_nodes, cas_bufs);
        // init hold_node_off_latch
        for(auto node_off : succ_node_off ){
            hold_node_off_latch.emplace(node_off);
        }

        for(auto node_off : succ_node_off ){
            // read now
            LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_nodes[node_off]);
            // 遍历这个node_off上的所有请求即所有的lock_data_id, 
            // 如果找到, 就从列表中移除
            for(auto it = lock_request_list[node_off].begin(); it != lock_request_list[node_off].end(); ){
                // find lock item
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (lock_node->lock_items[i].key == *it && lock_node->lock_items[i].valid == true) {
                        // lock EXCLUSIVE lock
                        assert(lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS == EXCLUSIVE_LOCKED);
                            
                        lock_node->lock_items[i].lock = UNLOCKED;

                        // erase from lock_request_list
                        lock_request_list[node_off].erase(it);
                        is_find = true;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(lock_request_list[node_off].size() == 0){
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
                // if LockDataId not exist, find next bucket
                node_id_t node_id = global_meta_man->GetLockTableNode(lock_request_list[node_off].front().table_id_);
                auto expand_node_id = lock_node->next_expand_node_id[0];
                offset_t expand_base_off = global_meta_man->GetLockTableExpandBase(lock_request_list[node_off].front().table_id_);
                offset_t next_off = expand_base_off + expand_node_id * sizeof(LockNode);
                if(expand_node_id < 0){
                    // find to the bucket end, here latch is already get but couldn't find it
                    for(auto lock_data_id : lock_request_list[node_off]){
                        RDMA_LOG(ERROR) <<  "LockTableStore::UnlockElciusive: lock item not exist" ;
                    }
                    // after insert, release latch and write back
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
                    lock_request_list.emplace(next_node_off, lock_request_list.at(node_off));
                    hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
                    assert(local_hash_nodes.count(next_node_off) == 0);
                    assert(cas_bufs.count(next_node_off) == 0);
                    local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(LockNode));
                    cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
                }
            }
        }
        // release all latch and write back
        for (auto node_off : unlock_node_off_with_write){
            ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes.at(node_off));
            hold_node_off_latch.erase(node_off);
        }
        unlock_node_off_with_write.clear();
    }
    // 这里所有的latch都已经释放了
    assert(hold_node_off_latch.size() == 0);
    return true;
}

// ***********************************************************************************
// public functions
// 对表上共享锁
bool DTX::LockSharedOnTable(coro_yield_t& yield, std::vector<table_id_t> table_id) {

    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;

    for(auto table_id : table_id){
        auto lock_data_id = LockDataId(table_id, LockDataType::TABLE);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockShared(yield, batch_lock_data_id, batch_node_off).size() == 0){
        return true;
    }
    return false;
}

// 对表上排他锁
bool DTX::LockExclusiveOnTable(coro_yield_t& yield, std::vector<table_id_t> table_id){

    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;

    for(auto table_id : table_id){
        auto lock_data_id = LockDataId(table_id, LockDataType::TABLE);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockExclusive(yield, batch_lock_data_id, batch_node_off).size() == 0){
        return true;
    }
    return false;
}

// 对记录上共享锁
bool DTX::LockSharedOnRecord(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){
    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RECORD);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockShared(yield, batch_lock_data_id, batch_node_off).size() == 0){
        return true;
    }
    return false;
}

// 对记录上排他锁
bool DTX::LockExclusiveOnRecord(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){
    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RECORD);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockExclusive(yield, batch_lock_data_id, batch_node_off).size() == 0){
        return true;
    }
    return false;
}

// 对范围上共享锁
bool DTX::LockSharedOnRange(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){

    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RANGE);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockShared(yield, batch_lock_data_id, batch_node_off).size() == 0){
        return true;
    }
    return false;
}

// 对范围上排他锁
bool DTX::LockExclusiveOnRange(coro_yield_t& yield, std::vector<table_id_t> table_id, std::vector<itemkey_t> key){
    assert(table_id.size() == key.size());
    std::vector<LockDataId> batch_lock_data_id;
    std::vector<NodeOffset> batch_node_off;

    for(int i=0; i<table_id.size(); i++){
        auto lock_data_id = LockDataId(table_id[i], key[i], LockDataType::RECORD);

        auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id[i]);
        auto remote_node_id = global_meta_man->GetLockTableNode(table_id[i]);

        auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);
        
        batch_lock_data_id.emplace_back(lock_data_id);
        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(LockExclusive(yield, batch_lock_data_id, batch_node_off).size() == 0){
        return true;
    }
    return false;
}

// 解除排他锁
bool DTX::UnlockExclusiveLockDataID(coro_yield_t& yield, std::vector<LockDataId> lock_data_id){
    std::vector<NodeOffset> batch_node_off;

    for(int i=0; i<lock_data_id.size(); i++){
        auto lock_table_meta = global_meta_man->GetLockTableMeta(lock_data_id[i].table_id_);
        auto remote_node_id = global_meta_man->GetLockTableNode(lock_data_id[i].table_id_);

        auto hash = MurmurHash64A(lock_data_id[i].Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }

    if(UnlockExclusive(yield, lock_data_id, batch_node_off)){
        return true;
    }
    return false;
}

// 解除共享锁
bool DTX::UnlockSharedLockDataID(coro_yield_t& yield, std::vector<LockDataId> lock_data_id){
    std::vector<NodeOffset> batch_node_off;

    for(int i=0; i<lock_data_id.size(); i++){
        auto lock_table_meta = global_meta_man->GetLockTableMeta(lock_data_id[i].table_id_);
        auto remote_node_id = global_meta_man->GetLockTableNode(lock_data_id[i].table_id_);

        auto hash = MurmurHash64A(lock_data_id[i].Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
        offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

        batch_node_off.emplace_back(NodeOffset{remote_node_id, node_off});
    }
    
    if(UnlockShared(yield, lock_data_id, batch_node_off)){
        return true;
    }
    return false;
}