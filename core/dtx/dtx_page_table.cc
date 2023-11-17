#include "dtx/dtx.h"
#include "dtx/rwlock.h"
#include <future>

// dtx_page_table.cc 用于实现计算节点访问内存节点页表的方法
// 页表的作用是：通过页号PageId找到对应的帧号frame_id_t
// 页表的实现是：哈希表，哈希表的每个桶是一个页表节点PageTableNode，每个桶中存放了多个页表项PageTableItem
// 页表项PageTableItem中存放了页号PageId和帧号frame_id_t
// 页表节点PageTableNode中存放了多个页表项PageTableItem，以及指向下一个页表节点PageTableNode的指针
// 由于hash index实现了给定索引数据项itemkey_t，找到对应的记录Rid(page_id_t, slot_id_t)的功能
// 因此，此文件要实现给定页号PageId，找到对应的帧号frame_id_t的功能
// 注意，每个内存节点都有一个页表，页表管理自己内存节点的所有页
// 因此，计算节点要访问所有内存节点的页表去找到对应的帧号frame_id_t
// 为了提高性能，使用多线程并行访问所有内存节点的页表

PageAddress DTX::GetPageAddrOrAddIntoPageTable(PageId id, bool* need_fetch_from_disk, bool* now_valid, bool is_write) {
    std::vector<RCQP*> qps;
    auto nodes = global_meta_man->GetPageTableNode();

    // 创建多线程并行访问所有内存节点的页表
    // std::vector<std::future<PageAddress>> futures;
    
    // for(int i=0; i<nodes.size(); i++){
    //     futures.push_back(std::async(std::launch::async, [this, id, &nodes, i]{
    //         // 获取页表元数据
    //         auto hash_meta = this->global_meta_man->GetPageTableMeta(nodes[i]);
    //         auto hash = MurmurHash64A(id.Get(), 0xdeadbeef) % hash_meta.bucket_num;
    //         RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(nodes[i]);
    //         // 计算哈希桶的偏移量
    //         offset_t node_off = hash_meta.base_off + hash * sizeof(PageTableNode);

    //         char* local_hash_node = ShardLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
            
    //         PageAddress res = {-1, INVALID_FRAME_ID};

    //         while (true) {
    //             // read now
    //             PageTableNode* page_table_node = reinterpret_cast<PageTableNode*>(local_hash_node);
                
    //             // find page table item
    //             for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
    //                 if (page_table_node->page_table_items[i].page_id == id && page_table_node->page_table_items[i].valid == true) {
                        
    //                     // find
    //                     res = page_table_node->page_table_items[i].page_address;
    //                     // Unlock
    //                     ShardUnLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    //                     return res;
    //                 }
    //             }
                
    //             offset_t old_node_off = node_off;
    //             // not find
    //             short expand_node_id = page_table_node->next_expand_node_id[0];
    //             // page table item not exist;
    //             if(expand_node_id < 0){
    //                 return res;
    //             } 

    //             node_off = hash_meta.base_off + (hash_meta.bucket_num + expand_node_id) * sizeof(PageTableNode);
    //             // lock next
    //             char* local_hash_node = ShardLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
    //             // release now
    //             ShardUnLockHashNode(thread_rdma_buffer_alloc, old_node_off, this, qp);
    //         }

    //         return res;
    //     }));
    // }

    // // 等待所有线程完成
    // for(int i=0; i<futures.size(); i++){
    //     PageAddress res = futures[i].get();
    //     if(res.frame_id != INVALID_FRAME_ID){
    //         return res;
    //     }
    // }
    

    // 这里暂时还没想好怎么实现, 先用单线程实现,
    // 不用多线程实现的原因：多线程并发对hash bucket上锁，可能会导致死锁
    for(int i=0; i<nodes.size(); i++){
        // 获取页表元数据
        auto hash_meta = this->global_meta_man->GetPageTableMeta(nodes[i]);
        auto hash = MurmurHash64A(id.Get(), 0xdeadbeef) % hash_meta.bucket_num;
        RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(nodes[i]);
        // 计算哈希桶的偏移量
        offset_t node_off = hash_meta.base_off + hash * sizeof(PageTableNode);

        char* local_hash_node = ExclusiveLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
        
        PageAddress res = {-1, INVALID_FRAME_ID};
        // 记录空位置
        int empty_pos = -1;

        while (true) {
            // read now
            PageTableNode* page_table_node = reinterpret_cast<PageTableNode*>(local_hash_node);
            
            // find page table item
            for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
                if (page_table_node->page_table_items[i].page_id == id && page_table_node->page_table_items[i].valid == true) {
                    // find
                    res = page_table_node->page_table_items[i].page_address;
                    // Unlock
                    ShardUnLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
                    return res;
                }
            }
            
            offset_t old_node_off = node_off;
            // not find
            short expand_node_id = page_table_node->next_expand_node_id[0];
            // page table item not exist;
            if(expand_node_id < 0){
                return res;
            } 

            node_off = hash_meta.base_off + (hash_meta.bucket_num + expand_node_id) * sizeof(PageTableNode);
            // lock next
            char* local_hash_node = ShardLockHashNode(thread_rdma_buffer_alloc, node_off, this, qp);
            // release now
            ShardUnLockHashNode(thread_rdma_buffer_alloc, old_node_off, this, qp);
        }
    }
    // 所有线程都没有找到，返回-1
    return {-1, INVALID_FRAME_ID};
}