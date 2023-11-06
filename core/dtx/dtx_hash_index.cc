#include "dtx/dtx.h"
#include "dtx/rwlock.h"

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