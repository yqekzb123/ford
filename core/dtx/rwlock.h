
#pragma once

#include "dtx/dtx.h"

// char* DTX::ShardLockHashNode(coro_yield_t& yield, RDMABufferAllocator* thread_rdma_buffer_alloc, std::vector<offset_t> node_off, DTX* dtx, RCQP* qp){

//     char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));

//     char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

//     while (true) {
//         std::shared_ptr<SharedLock_SharedMutex_Batch> doorbell = std::make_shared<SharedLock_SharedMutex_Batch>();
    
//         doorbell->SetFAAReq(faa_buf, node_off);
//         doorbell->SetReadReq(local_hash_node, node_off, sizeof(IndexNode));  // Read a hash index bucket
        
//         if (!doorbell->SendReqs(dtx->coro_sched, qp, dtx->coro_id)) {
//             std::cerr << "GetHashIndex get shared mutex sendreqs faild" << std::endl;
//             assert(false);
//         }
        
//         coro_sched->Yield(yield, coro_id);

//         if((*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) >> 56 == 0x00){
//             // get lock successfully
//             break;
//         }
    
//         // 探测性FAA失败，FAA(-1)
//         if (!dtx->coro_sched->RDMAFAA(dtx->coro_id, qp, faa_buf, node_off, SHARED_UNLOCK_TO_BE_ADDED)){
//             assert(false);
//         };

//         coro_sched->Yield(yield, coro_id);
//         // // sleep 30 us
//         // std::this_thread::sleep_for(std::chrono::microseconds(30));
//     }

//     return local_hash_node;
// }

// void DTX::ShardUnLockHashNode(coro_yield_t& yield, RDMABufferAllocator* thread_rdma_buffer_alloc, std::vector<offset_t> node_off, DTX* dtx, RCQP* qp){
//     // Unlock Shared Lock
//     char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//     if (!dtx->coro_sched->RDMAFAA(dtx->coro_id, qp, faa_buf, node_off, SHARED_UNLOCK_TO_BE_ADDED)){
//         assert(false);
//     };
//     // // 切换到其他协程
//     // coro_sched->Yield(yield, coro_id);
// }

// 函数根据DTX中的类pending_hash_node_latch_offs, 对这些桶的上锁，本函数在一次RTT完成
// 返回值为成功获取桶latch的offset
std::vector<NodeOffset> DTX::ExclusiveLockHashNode(coro_yield_t& yield, std::unordered_map<NodeOffset, char*>& local_hash_nodes, 
            std::unordered_map<NodeOffset, char*>& cas_bufs){

    for(auto node_off: pending_hash_node_latch_offs) {
        std::shared_ptr<ExclusiveLock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveLock_SharedMutex_Batch>();
        doorbell->SetLockReq(cas_bufs[node_off], node_off.offset);
        // TODO: 统一IndexNode和其他Node的size大小
        doorbell->SetReadReq(local_hash_nodes[node_off], node_off.offset, sizeof(LockNode));  // Read a hash index bucket
        
        if (!doorbell->SendReqs(coro_sched, thread_qp_man->GetRemoteDataQPWithNodeID(node_off.nodeId), coro_id)) {
            std::cerr << "GetHashIndex get Exclusive mutex sendreqs faild" << std::endl;
            assert(false);
        }
    }
    // 切换到其他协程，等待其他协程释放锁
    coro_sched->Yield(yield, coro_id);

    std::vector<NodeOffset> success_get_latch_off;

    for(auto node_off: pending_hash_node_latch_offs){
        if(*(lock_t*)cas_bufs[node_off] == UNLOCKED){
            // latch successful
            pending_hash_node_latch_offs.erase(node_off);
            success_get_latch_off.push_back(node_off);
        }
    }
    return success_get_latch_off;
}

void DTX::ExclusiveUnlockHashNode_NoWrite(NodeOffset node_off){

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    // release exclusive lock
    if (!coro_sched->RDMAFAA(coro_id, thread_qp_man->GetRemoteDataQPWithNodeID(node_off.nodeId), faa_buf, node_off.offset, EXCLUSIVE_UNLOCK_TO_BE_ADDED)){
        assert(false);
    };

    // // 切换到其他协程
    // coro_sched->Yield(yield, coro_id);

    // if( (*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
    //     // juage lock legal
    //     std::cerr << "Unlcok but there is no latch before" << std::endl;
    //     assert(false);
    // }
}

void DTX::ExclusiveUnlockHashNode_WithWrite(NodeOffset node_off, char* write_back_data){

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

    std::shared_ptr<ExclusiveUnlock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveUnlock_SharedMutex_Batch>();

    // 不写lock，写入后面所有字节
    doorbell->SetWriteReq(write_back_data, node_off.offset + sizeof(lock_t), sizeof(IndexNode)-sizeof(lock_t));  // Read a hash index bucket
    // FAA EXCLUSIVE_UNLOCK_TO_BE_ADDED.
    doorbell->SetUnLockReq(faa_buf, node_off.offset);

    if (!doorbell->SendReqs(coro_sched, thread_qp_man->GetRemoteDataQPWithNodeID(node_off.nodeId), coro_id)) {
        std::cerr << "GetHashIndex release Exclusive mutex sendreqs faild" << std::endl;
        assert(false);
    }
    
    // // 切换到其他协程
    // coro_sched->Yield(yield, coro_id);

    // if( (*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
    //     // 原值没上锁，出问题
    //     std::cerr << "Unlcok but there is no latch before" << std::endl;
    //     assert(false);
    // }
}