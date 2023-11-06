
#pragma once

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