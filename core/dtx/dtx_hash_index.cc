#include "dtx/dtx.h"

char* ShardLockHashNode(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, DTX* dtx, RCQP* qp){

    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

    while (true) {
        std::shared_ptr<SharedLock_SharedMutex_Batch> doorbell = std::make_shared<SharedLock_SharedMutex_Batch>();
    
        doorbell->SetFAAReq(faa_buf, node_off);
        doorbell->SetReadReq(local_hash_node, node_off, sizeof(IndexNode));  // Read a hash index bucket
        
        if (!doorbell->SendReqs(dtx->coro_sched, qp, dtx->coro_id)) {
            std::cerr << "GetHashIndex get shared mutex sendreqs faild" << std::endl;
            assert(false);
        }
        
        if((*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) >> 56 == 0x00){
            // get lock successfully
            break;
        }
    
        // 探测性FAA失败，FAA(-1)
        if (!dtx->coro_sched->RDMAFAA(dtx->coro_id, qp, faa_buf, node_off, SHARED_UNLOCK_TO_BE_ADDED)){
            assert(false);
        };

        // sleep 30 us
        std::this_thread::sleep_for(std::chrono::microseconds(30));
    }

    return local_hash_node;
}

void ShardUnLockHashNode(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, DTX* dtx, RCQP* qp){
    // Unlock Shared Lock
    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    if (!dtx->coro_sched->RDMAFAA(dtx->coro_id, qp, faa_buf, node_off, SHARED_UNLOCK_TO_BE_ADDED)){
        assert(false);
    };
}

char* ExclusiveLockHashNode(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, DTX* dtx, RCQP* qp){

    char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));

    char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

    while (true) {
        std::shared_ptr<ExclusiveLock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveLock_SharedMutex_Batch>();
    
        doorbell->SetLockReq(cas_buf, node_off);
        doorbell->SetReadReq(local_hash_node, node_off, sizeof(IndexNode));  // Read a hash index bucket
        
        if (!doorbell->SendReqs(dtx->coro_sched, qp, dtx->coro_id)) {
            std::cerr << "GetHashIndex get Exclusive mutex sendreqs faild" << std::endl;
            assert(false);
        }
        
        if( *(lock_t*)cas_buf  == UNLOCKED){
            // get lock successfully
            break;
        }
        // sleep 30 us
        std::this_thread::sleep_for(std::chrono::microseconds(30));
    }

    return local_hash_node;
}

void ExclusiveUnlockHashNode_NoWrite(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, DTX* dtx, RCQP* qp){

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    // release exclusive lock
    if (!dtx->coro_sched->RDMAFAA(dtx->coro_id, qp, faa_buf, node_off, EXCLUSIVE_UNLOCK_TO_BE_ADDED)){
        assert(false);
    };

    if( (*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
        // juage lock legal
        std::cerr << "Unlcok but there is no lock before" << std::endl;
        assert(false);
    }
}

void ExclusiveUnlockHashNode_WithWrite(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, char* write_back_data, DTX* dtx, RCQP* qp){

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

    std::shared_ptr<ExclusiveUnlock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveUnlock_SharedMutex_Batch>();

    // 不写lock，写入后面所有字节
    doorbell->SetWriteReq(write_back_data, node_off + sizeof(lock_t), sizeof(IndexNode)-sizeof(lock_t));  // Read a hash index bucket
    // FAA EXCLUSIVE_UNLOCK_TO_BE_ADDED.
    doorbell->SetUnLockReq(faa_buf, node_off);

    if (!doorbell->SendReqs(dtx->coro_sched, qp, dtx->coro_id)) {
        std::cerr << "GetHashIndex release Exclusive mutex sendreqs faild" << std::endl;
        assert(false);
    }
    
    if( (*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
        // 原值没上锁，出问题
        std::cerr << "Unlcok but there is no lock before" << std::endl;
        assert(false);
    }
}

Rid DTX::GetHashIndex(table_id_t table_id, itemkey_t item_key) {

    // 如果出现初始桶中没有itemkey的情况，似乎无法使用桶尾部的多个指针
    // 并行加多个锁，因为可能会造成死锁, 无法保证按顺序加锁

    auto hash_meta = global_meta_man->GetHashIndexMeta(table_id);
    auto remote_node_id = global_meta_man->GetHashIndexNode(table_id);

    auto hash = MurmurHash64A(item_key, 0xdeadbeef) % hash_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = hash_meta.base_off + hash * sizeof(IndexNode);

    Rid rid_res{-1,-1};
    // shared_lock hash node bucket
    char* local_hash_node = ShardLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    
    while (true) {
        // read now
        IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_node);
        
        // find index item
        for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
            if (index_node->index_items[i].key == item_key && index_node->index_items[i].valid == true) {
                // find
                rid_res = index_node->index_items->rid;
                // Unlock
                ShardUnLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
                return rid_res;
            }
        }
        
        offset_t old_node_off = node_off;
        // not find
        short expand_node_id = index_node->next_expand_node_id[0];
        // index not exist;
        if(expand_node_id < 0){
            return rid_res;
        } 

        node_off = hash_meta.base_off + (hash_meta.bucket_num + expand_node_id) * BUCKET_SIZE;
        // lock next
        char* local_hash_node = ShardLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
        // release now
        ShardUnLockHashNode(thread_rdma_buffer_alloc, old_node_off, this, qp);
    }

}

bool DTX::InsertHashIndex(table_id_t table_id, itemkey_t item_key, Rid rid) {

    auto hash_meta = global_meta_man->GetHashIndexMeta(table_id);
    auto remote_node_id = global_meta_man->GetHashIndexNode(table_id);

    auto hash = MurmurHash64A(item_key, 0xdeadbeef) % hash_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = hash_meta.base_off + hash * sizeof(IndexNode);

    // Exclusive_lock hash node bucket
    char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    
    while (true) {
        // read now
        IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_node);
        
        // find empty index item slot
        for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
            if (index_node->index_items[i].valid == false) {
                // empty, insert
                index_node->index_items[i].key = item_key;
                index_node->index_items[i].rid = rid;
                index_node->index_items[i].valid = true;
                ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
                return true;
            }
        }
        
        offset_t old_node_off = node_off;
        // not find
        short expand_node_id = index_node->next_expand_node_id[0];
        // index not exist;
        if(expand_node_id < 0){
            //TODO: new bucket now
            std::cout << "TODO : new bucket now" << std::endl;
            return false;
        } 

        node_off = hash_meta.base_off + (hash_meta.bucket_num + expand_node_id) * BUCKET_SIZE;
        // lock next
        char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
        // release now
        ExclusiveUnlockHashNode_NoWrite(thread_rdma_buffer_alloc, old_node_off, this, qp);
    }

}

bool DTX::DeleteHashIndex(table_id_t table_id, itemkey_t item_key) {

    auto hash_meta = global_meta_man->GetHashIndexMeta(table_id);
    auto remote_node_id = global_meta_man->GetHashIndexNode(table_id);

    auto hash = MurmurHash64A(item_key, 0xdeadbeef) % hash_meta.bucket_num;

    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    offset_t node_off = hash_meta.base_off + hash * sizeof(IndexNode);

    // Exclusive_lock hash node bucket
    char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    
    while (true) {
        // read now
        IndexNode* index_node = reinterpret_cast<IndexNode*>(local_hash_node);
        
        // find index item
        for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
            if (index_node->index_items[i].key == item_key && index_node->index_items[i].valid == true) {
                // find it
                index_node->index_items[i].valid = false;
                ExclusiveUnlockHashNode_WithWrite(thread_rdma_buffer_alloc, node_off, local_hash_node, this, qp);
                return true;
            }
        }
        
        offset_t old_node_off = node_off;
        // not find
        short expand_node_id = index_node->next_expand_node_id[0];
        // index not exist;
        if(expand_node_id < 0){
            return true;
        } 

        node_off = hash_meta.base_off + (hash_meta.bucket_num + expand_node_id) * BUCKET_SIZE;
        // lock next
        char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
        // release now
        ExclusiveUnlockHashNode_NoWrite(thread_rdma_buffer_alloc, old_node_off, this, qp);
    }

}