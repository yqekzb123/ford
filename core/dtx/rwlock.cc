// Author: huangdund
// Copyright (c) 2023

#include "dtx/dtx.h"

std::vector<int> DTX::ShardLockHashNode(coro_yield_t& yield, QPType qptype, std::vector<char*>& local_hash_nodes, std::vector<char*>& faa_bufs){

    RCQP*const* qp_arr = nullptr;
    switch (qptype){
        case QPType::kHashIndex:
            qp_arr = thread_qp_man->GetIndexQPPtrWithNodeID();
            break;
        case QPType::kLockTable:
            qp_arr = thread_qp_man->GetLockQPPtrWithNodeID();
            break;
        case QPType::kPageTable:
            qp_arr = thread_qp_man->GetPageTableQPPtrWithNodeID();
            break;
        default:
            assert(false);
    }

    for(int i=0; i<pending_hash_node_latch_idx.size(); i++) {
        std::shared_ptr<SharedLock_SharedMutex_Batch> doorbell = std::make_shared<SharedLock_SharedMutex_Batch>();

        NodeOffset node_off = total_hash_node_offs_vec[pending_hash_node_latch_idx[i]];
        doorbell->SetFAAReq(faa_bufs[i], node_off.offset);
        doorbell->SetReadReq(local_hash_nodes[i], node_off.offset, PAGE_SIZE);  // Read a hash index bucket
        
        if (!doorbell->SendReqs(coro_sched, qp_arr[node_off.nodeId], coro_id)) {
            std::cerr << "GetHashIndex get Exclusive mutex sendreqs faild" << std::endl;
            assert(false);
        }
    }
    // 切换到其他协程，等待其他协程释放锁
    coro_sched->Yield(yield, coro_id);

    std::vector<int> success_get_latch_off_idx;

    for (int i=0; i<pending_hash_node_latch_idx.size(); ) {
        if ((*(lock_t*)faa_bufs[i] & MASKED_SHARED_LOCKS) >> 56 == 0x00) {
            success_get_latch_off_idx.push_back(pending_hash_node_latch_idx[i]);
            if(pending_hash_node_latch_idx.size() == 1){
                pending_hash_node_latch_idx.clear();
                break;
            } else{
                pending_hash_node_latch_idx.erase(pending_hash_node_latch_idx.begin() + i); // 擦除元素，并将迭代器更新为下一个元素
            }
        } else {
            // 探测性FAA失败，FAA(-1)
            auto node_off = total_hash_node_offs_vec[pending_hash_node_latch_idx[i]];
            if (!coro_sched->RDMAFAA(coro_id, qp_arr[node_off.nodeId], faa_bufs[i], node_off.offset, SHARED_UNLOCK_TO_BE_ADDED)){
                assert(false);
            };
            i++; // 继续遍历下一个元素
        }
    }

    return success_get_latch_off_idx;
}

void DTX::ShardUnLockHashNode(NodeOffset node_off, QPType qptype){
    RCQP*const* qp_arr = nullptr;
    switch (qptype){
        case QPType::kHashIndex:
            qp_arr = thread_qp_man->GetIndexQPPtrWithNodeID();
            break;
        case QPType::kLockTable:
            qp_arr = thread_qp_man->GetLockQPPtrWithNodeID();
            break;
        case QPType::kPageTable:
            qp_arr = thread_qp_man->GetPageTableQPPtrWithNodeID();
            break;
        default:
            assert(false);
    }

    // Unlock Shared Lock
    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    if (!coro_sched->RDMAFAA(coro_id, qp_arr[node_off.nodeId], faa_buf, node_off.offset, SHARED_UNLOCK_TO_BE_ADDED)){
        assert(false);
    };
}

// 函数根据DTX中的类pending_hash_node_latch_offs, 对这些桶的上锁，本函数在一次RTT完成
// 返回值为成功获取桶latch的offset
std::vector<NodeOffset> DTX::ExclusiveLockHashNode(coro_yield_t& yield, QPType qptype, std::unordered_map<NodeOffset, char*>& local_hash_nodes, 
            std::unordered_map<NodeOffset, char*>& cas_bufs){

    RCQP*const* qp_arr = nullptr;
    switch (qptype){
        case QPType::kHashIndex:
            qp_arr = thread_qp_man->GetIndexQPPtrWithNodeID();
            break;
        case QPType::kLockTable:
            qp_arr = thread_qp_man->GetLockQPPtrWithNodeID();
            break;
        case QPType::kPageTable:
            qp_arr = thread_qp_man->GetPageTableQPPtrWithNodeID();
            break;
        default:
            assert(false);
    }

    for(auto node_off: pending_hash_node_latch_offs) {
        std::shared_ptr<ExclusiveLock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveLock_SharedMutex_Batch>();
        doorbell->SetLockReq(cas_bufs[node_off], node_off.offset);
        doorbell->SetReadReq(local_hash_nodes[node_off], node_off.offset, PAGE_SIZE);  // Read a hash index bucket
        
        if (!doorbell->SendReqs(coro_sched, qp_arr[node_off.nodeId], coro_id)) {
            std::cerr << "GetHashIndex get Exclusive mutex sendreqs faild" << std::endl;
            assert(false);
        }
    }
    // 切换到其他协程，等待其他协程释放锁
    coro_sched->Yield(yield, coro_id);

    std::vector<NodeOffset> success_get_latch_off;

    std::unordered_set<NodeOffset>::iterator it = pending_hash_node_latch_offs.begin();
    while (it != pending_hash_node_latch_offs.end()) {
        if (*(lock_t*)cas_bufs[*it] == UNLOCKED) {
            success_get_latch_off.push_back(*it);
            if(pending_hash_node_latch_offs.size() == 1){
                pending_hash_node_latch_offs.clear();
                break;
            } else{
                it = pending_hash_node_latch_offs.erase(it); // 擦除元素，并将迭代器更新为下一个元素
            }
        } else {
            ++it; // 继续遍历下一个元素
        }
    }
    return success_get_latch_off;
}

void DTX::ExclusiveUnlockHashNode_NoWrite(NodeOffset node_off, QPType qptype){

    RCQP*const* qp_arr = nullptr;
    switch (qptype){
        case QPType::kHashIndex:
            qp_arr = thread_qp_man->GetIndexQPPtrWithNodeID();
            break;
        case QPType::kLockTable:
            qp_arr = thread_qp_man->GetLockQPPtrWithNodeID();
            break;
        case QPType::kPageTable:
            qp_arr = thread_qp_man->GetPageTableQPPtrWithNodeID();
            break;
        default:
            assert(false);
    }

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    // release exclusive lock
    if (!coro_sched->RDMAFAA(coro_id, qp_arr[node_off.nodeId], faa_buf, node_off.offset, EXCLUSIVE_UNLOCK_TO_BE_ADDED)){
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

void DTX::ExclusiveUnlockHashNode_WithWrite(NodeOffset node_off, char* write_back_data, QPType qptype){

    RCQP*const* qp_arr = nullptr;
    switch (qptype){
        case QPType::kHashIndex:
            qp_arr = thread_qp_man->GetIndexQPPtrWithNodeID();
            break;
        case QPType::kLockTable:
            qp_arr = thread_qp_man->GetLockQPPtrWithNodeID();
            break;
        case QPType::kPageTable:
            qp_arr = thread_qp_man->GetPageTableQPPtrWithNodeID();
            break;
        default:
            assert(false);
    }

    char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

    std::shared_ptr<ExclusiveUnlock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveUnlock_SharedMutex_Batch>();

    // 不写lock，写入后面所有字节
    doorbell->SetWriteReq(write_back_data+sizeof(lock_t), node_off.offset+sizeof(lock_t), PAGE_SIZE-sizeof(lock_t));  // Read a hash index bucket
    // FAA EXCLUSIVE_UNLOCK_TO_BE_ADDED.
    doorbell->SetUnLockReq(faa_buf, node_off.offset);

    if (!doorbell->SendReqs(coro_sched, qp_arr[node_off.nodeId], coro_id)) {
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

// // 以下是非batching的上锁函数
// char* ExclusiveLockHashNode(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, DTX* dtx, RCQP* qp){

//     char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(IndexNode));

//     char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

//     while (true) {
//         std::shared_ptr<ExclusiveLock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveLock_SharedMutex_Batch>();
    
//         doorbell->SetLockReq(cas_buf, node_off);
//         doorbell->SetReadReq(local_hash_node, node_off, sizeof(IndexNode));  // Read a hash index bucket
        
//         if (!doorbell->SendReqs(dtx->coro_sched, qp, dtx->coro_id)) {
//             std::cerr << "GetHashIndex get Exclusive mutex sendreqs faild" << std::endl;
//             assert(false);
//         }
        
//         if( *(lock_t*)cas_buf  == UNLOCKED){
//             // get lock successfully
//             break;
//         }
//         // sleep 30 us
//         std::this_thread::sleep_for(std::chrono::microseconds(30));
//     }

//     return local_hash_node;
// }

// void ExclusiveUnlockHashNode_NoWrite(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, DTX* dtx, RCQP* qp){

//     char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//     // release exclusive lock
//     if (!dtx->coro_sched->RDMAFAA(dtx->coro_id, qp, faa_buf, node_off, EXCLUSIVE_UNLOCK_TO_BE_ADDED)){
//         assert(false);
//     };

//     if( (*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
//         // juage lock legal
//         std::cerr << "Unlcok but there is no lock before" << std::endl;
//         assert(false);
//     }
// }

// void ExclusiveUnlockHashNode_WithWrite(RDMABufferAllocator* thread_rdma_buffer_alloc, offset_t node_off, char* write_back_data, DTX* dtx, RCQP* qp){

//     char* faa_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));

//     std::shared_ptr<ExclusiveUnlock_SharedMutex_Batch> doorbell = std::make_shared<ExclusiveUnlock_SharedMutex_Batch>();

//     // 不写lock，写入后面所有字节
//     doorbell->SetWriteReq(write_back_data, node_off + sizeof(lock_t), sizeof(IndexNode)-sizeof(lock_t));  // Read a hash index bucket
//     // FAA EXCLUSIVE_UNLOCK_TO_BE_ADDED.
//     doorbell->SetUnLockReq(faa_buf, node_off);

//     if (!doorbell->SendReqs(dtx->coro_sched, qp, dtx->coro_id)) {
//         std::cerr << "GetHashIndex release Exclusive mutex sendreqs faild" << std::endl;
//         assert(false);
//     }
    
//     if( (*(lock_t*)faa_buf & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
//         // 原值没上锁，出问题
//         std::cerr << "Unlcok but there is no lock before" << std::endl;
//         assert(false);
//     }
// }
