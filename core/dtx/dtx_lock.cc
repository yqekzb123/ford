#include "dtx/dtx.h"
#include "dtx/rwlock.h"

// 这里锁表与哈希索引实现不同的点在于哈希索引要求索引数据全量存放
// 而锁表只有正在持有的锁是有用的，因此如果出现哈希桶已满的情况
// 可以选择将一个未上锁的数据项移除，并将新的数据项插入
// 这样可以避免哈希桶扩容，减少内存开销

bool DTX::LockShared(LockDataId lock_data_id, offset_t node_off, RCQP* qp){

    // lock hash node bucket
    char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);

    // define first empty lock item & first no lock lock item
    LockItem* first_empty_lock_item = nullptr;
    LockItem* first_nolock_lock_item = nullptr;
    
    // read now
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_node);
    
    // find lock item
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
        if (lock_node->lock_items[i].key == lock_data_id && lock_node->lock_items[i].valid == true) {
            // not exclusive lock
            if(lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS == UNLOCKED){
                // lock shared lock
                lock_node->lock_items[i].lock += 1;
                // release latch and write back
                ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
                return true;
            }
            else{
                // LockDataId already locked
                // release latch and write back
                ExclusiveUnlockHashNode_NoWrite(thread_rdma_buffer_alloc, node_off, this, qp);
                return false;
            }
        }
        if (first_empty_lock_item == nullptr && lock_node->lock_items[i].valid == false) {
            first_empty_lock_item = &lock_node->lock_items[i];
        }
        if (first_nolock_lock_item == nullptr && lock_node->lock_items[i].lock == UNLOCKED) {
            first_nolock_lock_item = &lock_node->lock_items[i];
        }
    }
    
    // lock item not exist & insert new lock item
    if(first_empty_lock_item == nullptr && first_nolock_lock_item == nullptr){
        // release all latch and write back
        ExclusiveUnlockHashNode_NoWrite(thread_rdma_buffer_alloc, node_off, this, qp);
        RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
        return false;
    }
    else if( first_empty_lock_item == nullptr && first_nolock_lock_item != nullptr){
        // lock first no lock lock item
        first_nolock_lock_item->key = lock_data_id;
        first_nolock_lock_item->lock = 1;
        first_nolock_lock_item->valid = true;
        // release all latch and write back
        ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
    }
    else{
        // lock first empty lock item
        first_empty_lock_item->key = lock_data_id;
        first_empty_lock_item->lock = 1;
        first_empty_lock_item->valid = true;
        // release all latch and write back
        ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
    }
    return true;
}

bool DTX::LockExclusive(LockDataId lock_data_id, offset_t node_off, RCQP* qp){

    // lock hash node bucket
    char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);

    // define first empty lock item & first no lock lock item
    LockItem* first_empty_lock_item = nullptr;
    LockItem* first_nolock_lock_item = nullptr;
    
    // read now
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_node);
    
    // find lock item
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
        if (lock_node->lock_items[i].key == lock_data_id && lock_node->lock_items[i].valid == true) {
            // not lock
            if(lock_node->lock_items[i].lock == UNLOCKED){
                // lock EXCLUSIVE lock
                lock_node->lock_items[i].lock = EXCLUSIVE_LOCKED;
                // release latch and write back
                ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
                return true;
            }
            else{
                // LockDataId already locked
                // release latch and write back
                ExclusiveUnlockHashNode_NoWrite(thread_rdma_buffer_alloc, node_off, this, qp);
                return false;
            }
        }
        if (first_empty_lock_item == nullptr && lock_node->lock_items[i].valid == false) {
            first_empty_lock_item = &lock_node->lock_items[i];
        }
        if (first_nolock_lock_item == nullptr && lock_node->lock_items[i].lock == UNLOCKED) {
            first_nolock_lock_item = &lock_node->lock_items[i];
        }
    }
    
    // lock item not exist & insert new lock item
    if(first_empty_lock_item == nullptr && first_nolock_lock_item == nullptr){
        // release all latch and write back
        ExclusiveUnlockHashNode_NoWrite(thread_rdma_buffer_alloc, node_off, this, qp);
        RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
        return false;
    }
    else if( first_empty_lock_item == nullptr && first_nolock_lock_item != nullptr){
        // lock first no lock lock item
        first_nolock_lock_item->key = lock_data_id;
        first_nolock_lock_item->lock = EXCLUSIVE_LOCKED;
        first_nolock_lock_item->valid = true;
        // release all latch and write back
        ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
    }
    else{
        // lock first empty lock item
        first_empty_lock_item->key = lock_data_id;
        first_empty_lock_item->lock = EXCLUSIVE_LOCKED;
        first_empty_lock_item->valid = true;
        // release all latch and write back
        ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
    }
    return true;
}

// 解除共享锁 
bool DTX::UnlockShared(LockDataId lock_data_id, offset_t node_off, RCQP* qp){
    // lock hash node bucket
    char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    
    // read now
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_node);
    
    // find lock item
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
        if (lock_node->lock_items[i].key == lock_data_id && lock_node->lock_items[i].valid == true) {
            assert(lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS == UNLOCKED);
            assert(lock_node->lock_items[i].lock > 0);
            // unlock shared lock
            lock_node->lock_items[i].lock -= 1;
            // release latch and write back
            ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
            return true;
        }
    }

    RDMA_LOG(ERROR) <<  "LockTableStore::UnlockShared: lock item not exist" ;
    assert(false);
    return false;
}

// 解除排他锁
bool DTX::UnlockExclusive(LockDataId lock_data_id, offset_t node_off, RCQP* qp){
    // lock hash node bucket
    char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    
    // read now
    LockNode* lock_node = reinterpret_cast<LockNode*>(local_hash_node);
    
    // find lock item
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
        if (lock_node->lock_items[i].key == lock_data_id && lock_node->lock_items[i].valid == true) {
            assert(lock_node->lock_items[i].lock & MASKED_SHARED_LOCKS == EXCLUSIVE_LOCKED);
            // unlock exclusive lock
            lock_node->lock_items[i].lock = UNLOCKED;
            // release latch and write back
            ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
            return true;
        }
    }
    
    RDMA_LOG(ERROR) <<  "LockTableStore::UnlockShared: lock item not exist" ;
    assert(false);
    return false;
}

// ***********************************************************************************
// public functions
// 对表上共享锁
bool DTX::LockSharedOnTable(table_id_t table_id) {
    auto lock_data_id = LockDataId(table_id, LockDataType::TABLE);

    auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
    auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return LockShared(lock_data_id, node_off, qp);
}

// 对表上排他锁
bool DTX::LockExclusiveOnTable(table_id_t table_id){
    auto lock_data_id = LockDataId(table_id, LockDataType::TABLE);

    auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
    auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return LockExclusive(lock_data_id, node_off, qp);
}

// 对记录上共享锁
bool DTX::LockSharedOnRecord(table_id_t table_id, itemkey_t key){
    auto lock_data_id = LockDataId(table_id, key, LockDataType::RECORD);

    auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
    auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return LockShared(lock_data_id, node_off, qp);
}

// 对记录上排他锁
bool DTX::LockExclusiveOnRecord(table_id_t table_id, itemkey_t key){
    
    auto lock_data_id = LockDataId(table_id, key, LockDataType::RECORD);

    auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
    auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return LockExclusive(lock_data_id, node_off, qp);
}

// 对范围上共享锁
bool DTX::LockSharedOnRange(table_id_t table_id, itemkey_t key){

    auto lock_data_id = LockDataId(table_id, key, LockDataType::RANGE);

    auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
    auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return LockShared(lock_data_id, node_off, qp);
}

// 对范围上排他锁
bool DTX::LockExclusiveOnRange(table_id_t table_id, itemkey_t key){
    auto lock_data_id = LockDataId(table_id, key, LockDataType::RANGE);

    auto lock_table_meta = global_meta_man->GetLockTableMeta(table_id);
    auto remote_node_id = global_meta_man->GetLockTableNode(table_id);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return LockExclusive(lock_data_id, node_off, qp);
}

// 解除排他锁
bool DTX::UnlockExclusiveLockDataID(LockDataId lock_data_id){
    
    auto lock_table_meta = global_meta_man->GetLockTableMeta(lock_data_id.table_id_);
    auto remote_node_id = global_meta_man->GetLockTableNode(lock_data_id.table_id_);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return UnlockExclusive(lock_data_id, node_off, qp);
}

// 解除共享锁
bool DTX::UnlockSharedLockDataID(LockDataId lock_data_id){
    auto lock_table_meta = global_meta_man->GetLockTableMeta(lock_data_id.table_id_);
    auto remote_node_id = global_meta_man->GetLockTableNode(lock_data_id.table_id_);

    auto hash = MurmurHash64A(lock_data_id.Get(), 0xdeadbeef) % lock_table_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = lock_table_meta.base_off + hash * sizeof(LockNode);

    return UnlockShared(lock_data_id, node_off, qp);
}