// Author: huangdund
// Copyright (c) 2023

#include "dtx/dtx.h"
#include <chrono>
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

#define BATCH_GET_FREE_PAGE_SIZE 100

PageAddress DTX::GetFreePageSlot(){
    auto nodes = global_meta_man->GetPageTableNode();
    PageAddress res;
    while (true) {    
        for(int i=0; i<nodes.size(); i++){
            RCQP* qp = thread_qp_man->GetRemotePageRingbufferQPWithNodeID(nodes[i]);
            char* faa_cnt_buf = thread_rdma_buffer_alloc->Alloc(sizeof(int64_t));
            char* faa_tail_buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
            
            auto ring_buffer_base_off = global_meta_man->GetFreeRingBase(nodes[i]);
            auto ring_buffer_tail_off = global_meta_man->GetFreeRingTail(nodes[i]);
            auto ring_buffer_cnt_off = global_meta_man->GetFreeRingCnt(nodes[i]);

            free_page_list_mutex->lock();
            if(free_page_list->size() > 0){
                res = free_page_list->front();
                free_page_list->pop_front();
                free_page_list_mutex->unlock();
                return res;
            }
            else{
                auto rc = qp->post_faa(faa_cnt_buf, ring_buffer_cnt_off, -BATCH_GET_FREE_PAGE_SIZE, IBV_SEND_SIGNALED);
                if (rc != SUCC) {
                    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                }
                rc = qp->post_faa(faa_tail_buf, ring_buffer_tail_off, BATCH_GET_FREE_PAGE_SIZE, IBV_SEND_SIGNALED);
                if (rc != SUCC) {
                    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                }
                ibv_wc wc{};
                rc = qp->poll_till_completion(wc, no_timeout);
                if (rc != SUCC) {
                    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                }
                rc = qp->poll_till_completion(wc, no_timeout);
                if (rc != SUCC) {
                    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                }

                if(*(int64_t*)faa_cnt_buf < BATCH_GET_FREE_PAGE_SIZE){
                    // buffer has not enough free page
                    auto rc = qp->post_faa(faa_tail_buf, ring_buffer_tail_off, -BATCH_GET_FREE_PAGE_SIZE, IBV_SEND_SIGNALED);
                    if (rc != SUCC) {
                        RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                    }
                    rc = qp->post_faa(faa_cnt_buf, ring_buffer_cnt_off, BATCH_GET_FREE_PAGE_SIZE, IBV_SEND_SIGNALED);
                    if (rc != SUCC) {
                        RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                    }
                    ibv_wc wc{};
                    rc = qp->poll_till_completion(wc, no_timeout);
                    if (rc != SUCC) {
                        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                    }
                    rc = qp->poll_till_completion(wc, no_timeout);
                    if (rc != SUCC) {
                        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                    }
                    free_page_list_mutex->unlock();
                    continue;
                }
                else{
                    // buffer has enough page
                    char* read_free_page = thread_rdma_buffer_alloc->Alloc(sizeof(RingBufferItem) * BATCH_GET_FREE_PAGE_SIZE);
                    if(*(uint64_t*)faa_tail_buf % MAX_FREE_LIST_BUFFER_SIZE + BATCH_GET_FREE_PAGE_SIZE > MAX_FREE_LIST_BUFFER_SIZE){
                        offset_t read_off_1 = ring_buffer_base_off + (*(uint64_t*)faa_tail_buf % MAX_FREE_LIST_BUFFER_SIZE) * sizeof(RingBufferItem);
                        offset_t read_off_2 = ring_buffer_base_off + 0 * sizeof(RingBufferItem);
                        size_t read_size_1 = sizeof(RingBufferItem) * (MAX_FREE_LIST_BUFFER_SIZE - (*(uint64_t*)faa_tail_buf % MAX_FREE_LIST_BUFFER_SIZE));
                        size_t read_size_2 = sizeof(RingBufferItem) * (BATCH_GET_FREE_PAGE_SIZE - (MAX_FREE_LIST_BUFFER_SIZE - (*(uint64_t*)faa_tail_buf % MAX_FREE_LIST_BUFFER_SIZE)));
                        auto rc = qp->post_send(IBV_WR_RDMA_READ, read_free_page, read_size_1, read_off_1, IBV_SEND_SIGNALED);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                        }
                        rc = qp->post_send(IBV_WR_RDMA_READ, read_free_page + read_size_1, read_size_2, read_off_2, IBV_SEND_SIGNALED);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                        }
                        ibv_wc wc{};
                        rc = qp->poll_till_completion(wc, no_timeout);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                        }
                        rc = qp->poll_till_completion(wc, no_timeout);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                        }

                        // 写回unvalid标志位
                        char* write_free_page = thread_rdma_buffer_alloc->Alloc(sizeof(RingBufferItem) * BATCH_GET_FREE_PAGE_SIZE);
                        memset(write_free_page, 0, sizeof(RingBufferItem) * BATCH_GET_FREE_PAGE_SIZE);
                        rc = qp->post_send(IBV_WR_RDMA_WRITE, write_free_page, read_size_1, read_off_1, IBV_SEND_SIGNALED);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << "GetFreePageThread";
                        }
                        rc = qp->post_send(IBV_WR_RDMA_WRITE, write_free_page + read_size_1, read_size_2, read_off_2, IBV_SEND_SIGNALED);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << "GetFreePageThread";
                        }
                        rc = qp->poll_till_completion(wc, no_timeout);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc << "GetFreePageThread";
                        }
                        rc = qp->poll_till_completion(wc, no_timeout);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc << "GetFreePageThread";
                        }
                    }
                    else{
                        offset_t read_off = ring_buffer_base_off + (*(uint64_t*)faa_tail_buf % MAX_FREE_LIST_BUFFER_SIZE) * sizeof(RingBufferItem);
                        size_t read_size = sizeof(RingBufferItem) * BATCH_GET_FREE_PAGE_SIZE;
                        auto rc = qp->post_send(IBV_WR_RDMA_READ, read_free_page, read_size, read_off, IBV_SEND_SIGNALED);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "GetFreePageThread";
                        }
                        ibv_wc wc{};
                        rc = qp->poll_till_completion(wc, no_timeout);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "GetFreePageThread";
                        }
                        
                        // 写回unvalid标志位
                        char* write_free_page = thread_rdma_buffer_alloc->Alloc(sizeof(RingBufferItem) * BATCH_GET_FREE_PAGE_SIZE);
                        memset(write_free_page, 0, sizeof(RingBufferItem) * BATCH_GET_FREE_PAGE_SIZE);
                        
                        rc = qp->post_send(IBV_WR_RDMA_WRITE, write_free_page, read_size, read_off, IBV_SEND_SIGNALED);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << "GetFreePageThread";
                        }
                        rc = qp->poll_till_completion(wc, no_timeout);
                        if (rc != SUCC) {
                            RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc << "GetFreePageThread";
                        }
                    }
                    for(int i=0; i<BATCH_GET_FREE_PAGE_SIZE; i++){
                        RingBufferItem* item =  reinterpret_cast<RingBufferItem*>(read_free_page + i * sizeof(RingBufferItem));
                        assert(item->valid == true);
                        free_page_list->push_back({item->node_id, item->frame_id});
                    }
                    free_page_list_mutex->unlock();
                }
            }
        }
    }
    return {-1, INVALID_FRAME_ID};
}

PageAddress DTX::InsertPageTableIntoHashNodeList(std::vector<char*>& local_hash_nodes_vec, 
        PageId page_id, bool is_write, int last_idx, int all_page_id_idx,
         std::unordered_map<int, int>& hold_latch_to_previouse_node_off){
    
    int idx = last_idx;
    std::unordered_map<int, int> hold_latch_to_next_node_off;
    while(hold_latch_to_previouse_node_off.count(idx) != 0){
        hold_latch_to_next_node_off[hold_latch_to_previouse_node_off.at(idx)] = idx;
        idx = hold_latch_to_previouse_node_off.at(idx);
    }

    // find empty slot to insert
    PageTableNode* page_table_node = reinterpret_cast<PageTableNode*>(local_hash_nodes_vec[idx]);
    NodeOffset node_off = total_hash_node_offs_vec[idx];
    while (true) {
        // find lock item
        for(int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
            if (page_table_node->page_table_items[i].valid == false) {
                // 在这里记录page table item的本地地址和远程地址
                NodeOffset remote_off = {node_off.nodeId, node_off.offset + (offset_t)&page_table_node->page_table_items[i] - (offset_t)page_table_node};
                page_table_item_localaddr_and_remote_offset[all_page_id_idx] = std::make_pair(
                    local_hash_nodes_vec[idx] + (offset_t)&page_table_node->page_table_items[i] - (offset_t)page_table_node, remote_off);
                page_table_node->page_table_items[i].valid = true;
                page_table_node->page_table_items[i].page_id = page_id;
                // 从BufferPoolManager中获取frame_id
                page_table_node->page_table_items[i].page_address = GetFreePageSlot();
                // 当前页面正在从磁盘读取
                page_table_node->page_table_items[i].page_valid = false;
                if(is_write){
                    page_table_node->page_table_items[i].rwcount = EXCLUSIVE_LOCKED;
                }
                else{
                    page_table_node->page_table_items[i].rwcount = 1;
                }
                ExclusiveUnlockHashNode_RemoteWriteItem(remote_off.nodeId, remote_off.offset, local_hash_nodes_vec[idx] + (offset_t)&page_table_node->page_table_items[i] - (offset_t)page_table_node, 
                        sizeof(PageTableItem), QPType::kPageTable);
                return page_table_node->page_table_items[i].page_address;
            }
        }
        auto expand_node_id = page_table_node->next_expand_node_id[0];
        if(expand_node_id < 0 && idx == last_idx){
            //TODO: no space for lock
            RDMA_LOG(ERROR) <<  "LockTableStore::LockSharedOnTable: lock item bucket is full" ;
            return {-1, INVALID_FRAME_ID};
        }
        idx = hold_latch_to_next_node_off[idx];
        node_off = total_hash_node_offs_vec[idx];
        page_table_node = reinterpret_cast<PageTableNode*>(local_hash_nodes_vec[idx]);
    }
    return {-1, INVALID_FRAME_ID};
}

struct page_table_request_list_item{
    PageId page_id;
    bool is_write;
    int index;
};

std::vector<PageAddress> DTX::GetPageAddrOrAddIntoPageTable(coro_yield_t& yield, std::vector<bool> is_write, 
            std::vector<bool>& need_fetch_from_disk, std::vector<bool>& now_valid, std::vector<int>& all_page_id_idx){
    
    // !这个函数的作用是获取pending_read_all_page_ids中所有page_id对应的page_address, pending应该是已去重的
    assert(pending_read_all_page_ids.size() == is_write.size());
    assert(all_page_id_idx.size() == pending_read_all_page_ids.size());
    
    // 所有访问hash node共用total_hash_node_offs_vec, 因此在本次使用时要清空
    total_hash_node_offs_vec.clear();
    auto nodes = global_meta_man->GetPageTableNode();
    assert(nodes.size() > 0);
    // 计算每个itemkey的hash值和对应的NodeOffset, 
    // 页表和哈希索引或锁表的实现略有不用，因为哈希索引和锁表通过采用分库分表的方式
    // 可以假设一个节点可以存放所有的哈希索引或锁表
    // 而页表管理的页数远远大于哈希索引或锁表的数据项数，因此页表需要分布在多个节点上
    // 在这里，我们需要遍历所有页表可能存在的所有节点来检查页表项是否存在，如果不存在，则需要添加
    
    // 首先，我们需要计算每个page_ids的hash值，然后算出对应的NodeOffset，初始化的NodeOffset的node_id为第一个节点
    // 如果桶链都没有找到，则需要进入下一个节点遍历

    std::vector<std::vector<page_table_request_list_item>> get_pagetable_request_list; // bool 存放的是is_write
    get_pagetable_request_list.reserve(pending_read_all_page_ids.size());
    for(int i=0; i<pending_read_all_page_ids.size(); i++){
        auto hash_meta = global_meta_man->GetPageTableMeta(nodes[0]); // 获取第一个节点的页表元数据
        auto hash = MurmurHash64A(pending_read_all_page_ids[i].Get(), 0xdeadbeef) % hash_meta.bucket_num;
        offset_t offset = hash_meta.base_off + hash * sizeof(PageTableNode);
        NodeOffset node_off{nodes[0], offset};

        auto index = std::find(total_hash_node_offs_vec.begin(), total_hash_node_offs_vec.end(), node_off);
        if(index == total_hash_node_offs_vec.end()){
            // not find
            total_hash_node_offs_vec.emplace_back(node_off);
            get_pagetable_request_list.emplace_back(std::vector<page_table_request_list_item>({{pending_read_all_page_ids[i], is_write[i], i}}));
        }
        else{
            // find
            get_pagetable_request_list[index - total_hash_node_offs_vec.begin()].push_back({pending_read_all_page_ids[i], is_write[i], i});
        }
    }

    assert(pending_hash_node_latch_idx.size() == 0);
    std::vector<char*> local_hash_nodes_vec(total_hash_node_offs_vec.size(), nullptr);
    std::vector<char*> cas_bufs_vec(total_hash_node_offs_vec.size(), nullptr);

    // init local_hash_nodes and cas_bufs, and get_pagetable_request_list , and pending_hash_node_latch_offs
    for(int i=0; i<total_hash_node_offs_vec.size(); i++){
        local_hash_nodes_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
        cas_bufs_vec[i] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        pending_hash_node_latch_idx.push_back(i);
    }
    
    // std::unordered_set<NodeOffset> unlock_node_off_with_write;
    // std::unordered_set<NodeOffset> hold_node_off_latch;
    std::unordered_map<int, int> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>
    std::vector<PageAddress> res(pending_read_all_page_ids.size(), {-1, INVALID_FRAME_ID});

    while (pending_hash_node_latch_idx.size()!=0) {
        // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
        auto succ_node_off_idx = ExclusiveLockHashNode(yield, QPType::kPageTable, local_hash_nodes_vec, cas_bufs_vec);
        // init hold_node_off_latch
        // for(auto node_off : succ_node_off ){
        //     hold_node_off_latch.emplace(node_off);
        // }

        for(auto idx : succ_node_off_idx ){
            // read now
            PageTableNode* page_table_node = reinterpret_cast<PageTableNode*>(local_hash_nodes_vec[idx]);
            NodeOffset node_off = total_hash_node_offs_vec[idx];
            // 遍历这个node_off上的所有请求即所有的page table item 
            // 如果找到, 就从列表中移除
            for(auto it = get_pagetable_request_list[idx].begin(); it != get_pagetable_request_list[idx].end(); ){
                // find empty slot to insert
                bool is_find = false;
                for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
                    if (page_table_node->page_table_items[i].page_id == it->page_id && page_table_node->page_table_items[i].valid == true) {
                        // find, 记录page_address
                        res[it->index] = page_table_node->page_table_items[i].page_address;
                        // std::cout << "find page_id: " << it->first.toString() << " in frame id:" << page_table_node->page_table_items[i].page_address.frame_id << std::endl;
                        // 在这里记录page table item的本地地址和远程地址
                        NodeOffset remote_off = {node_off.nodeId, node_off.offset + (offset_t)&page_table_node->page_table_items[i] - (offset_t)page_table_node};
                        page_table_item_localaddr_and_remote_offset[all_page_id_idx[it->index]] = std::make_pair(
                            local_hash_nodes_vec[idx] + (offset_t)&page_table_node->page_table_items[i] - (offset_t)page_table_node, remote_off);
                        need_fetch_from_disk[it->index] = false;
                        // this page is not valid, because other thread is fetch this page from disk and haven't write back
                        bool write_pagetableitem_back = false;
                        if(page_table_node->page_table_items[i].page_valid == false){
                            now_valid[it->index] = false;
                        }
                        // page is valid now
                        else if(it->is_write == true){
                            //is write & page is valid & wcount == 0
                            if((page_table_node->page_table_items[i].rwcount & MASKED_SHARED_LOCKS) != EXCLUSIVE_LOCKED){
                                now_valid[it->index] = true;
                                page_table_node->page_table_items[i].rwcount |= EXCLUSIVE_LOCKED;
                                write_pagetableitem_back = true;
                            }
                            else{
                                now_valid[it->index] = false;
                            }
                        }
                        else{
                            //is read, read directly
                            now_valid[it->index] = true;
                            page_table_node->page_table_items[i].rwcount++;
                            write_pagetableitem_back = true;
                        }
                        // write remote here
                        if(write_pagetableitem_back){
                            ExclusiveUnlockHashNode_RemoteWriteItem(remote_off.nodeId, remote_off.offset, local_hash_nodes_vec[idx] + (offset_t)&page_table_node->page_table_items[i] - (offset_t)page_table_node, 
                                sizeof(PageTableItem), QPType::kPageTable);
                        }
                        // erase会返回下一个元素的迭代器
                        get_pagetable_request_list[idx].erase(it);
                        is_find = true;
                        break;
                    }
                }
                // not find
                if(!is_find) it++;
            }

            // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
            if(get_pagetable_request_list[idx].size() == 0){
                // release latch and write back
                auto release_idx = idx;
                auto release_node_off = total_hash_node_offs_vec[release_idx];
                while(true){
                    ExclusiveUnlockHashNode_NoWrite(yield, release_node_off, QPType::kPageTable);
                    if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                    release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                    release_node_off = total_hash_node_offs_vec[release_idx];
                }
            }
            else{
                // 存在未处理的请求, 保留latch
                // if PageTable node not exist, find next bucket
                auto expand_node_id = page_table_node->next_expand_node_id[0];
                bool continue_search = false;
                NodeOffset next_node_off;
                if(expand_node_id < 0){
                    // 这个节点搜索完成，搜索下一个节点
                    auto it = std::find(nodes.begin(), nodes.end(), node_off.nodeId);
                    if(it + 1 != nodes.end()){
                        node_id_t new_node_id = *(it+1);
                        auto hash_meta = global_meta_man->GetPageTableMeta(new_node_id); // 获取下一个节点的页表元数据
                        // 这里必须假定新节点的桶数和旧节点的桶数相同，即原节点的桶中所有元素也在新节点的一个桶中
                        auto hash = MurmurHash64A(get_pagetable_request_list[idx][0].page_id.Get(), 0xdeadbeef) % hash_meta.bucket_num;
                        offset_t next_off = hash_meta.base_off + hash * sizeof(PageTableNode);
                        next_node_off = {new_node_id, next_off};
                        continue_search = true;
                    }
                    else{
                        continue_search = false;
                        // 所有节点都搜索完了，但没找到，应该插入以保证缓存一致性(多线程)
                        // 释放所有latch
                        for(auto pagetable_request : get_pagetable_request_list[idx]){
                            PageAddress insert_page_addr = InsertPageTableIntoHashNodeList(local_hash_nodes_vec, pagetable_request.page_id, 
                                pagetable_request.is_write, idx, all_page_id_idx[pagetable_request.index], hold_latch_to_previouse_node_off);
                            if(insert_page_addr.frame_id == INVALID_FRAME_ID || insert_page_addr.node_id < 0){
                                RDMA_LOG(ERROR) << "InsertPageTableIntoHashNodeList failed";
                            }
                            else{
                                res[pagetable_request.index] = insert_page_addr;
                                // std::cout << "insert page_id: " << pagetable_request.first.toString() << " in frame id:" << insert_page_addr.frame_id << std::endl;
                                need_fetch_from_disk[pagetable_request.index] = true;
                                now_valid[pagetable_request.index] = false;
                            }
                        }
                        // release latch and write back
                        auto release_idx = idx;
                        auto release_node_off = total_hash_node_offs_vec[release_idx];
                        while(true){
                            ExclusiveUnlockHashNode_NoWrite(yield, release_node_off, QPType::kPageTable);
                            if(hold_latch_to_previouse_node_off.count(release_idx) == 0) break;
                            release_idx = hold_latch_to_previouse_node_off.at(release_idx);
                            release_node_off = total_hash_node_offs_vec[release_idx];
                        }
                    }
                }
                else{
                    offset_t expand_base_off = global_meta_man->GetPageTableExpandBase(node_off.nodeId);
                    offset_t next_off = expand_base_off + expand_node_id * sizeof(PageTableNode);
                    next_node_off = {node_off.nodeId, next_off};
                    continue_search = true;
                }
                if (continue_search) {
                    total_hash_node_offs_vec.push_back(next_node_off);
                    int new_idx = total_hash_node_offs_vec.size() - 1;
                    pending_hash_node_latch_idx.push_back(new_idx);
                    get_pagetable_request_list.push_back(get_pagetable_request_list[idx]);
                    hold_latch_to_previouse_node_off[new_idx] = idx;
                    assert(local_hash_nodes_vec.size() == total_hash_node_offs_vec.size() - 1);
                    assert(cas_bufs_vec.size() == total_hash_node_offs_vec.size() - 1);
                    local_hash_nodes_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode)));
                    cas_bufs_vec.push_back(thread_rdma_buffer_alloc->Alloc(sizeof(lock_t)));
                }
            }
        }
    }

    // 检查
    for(int i=0; i<pending_read_all_page_ids.size(); i++){
        assert(res[i].frame_id != INVALID_FRAME_ID);
    }
    return res;
}


// void DTX::UnpinPageTable(coro_yield_t& yield, std::vector<PageId> page_ids, std::vector<bool> is_write){

//     auto nodes = global_meta_man->GetPageTableNode();
//     assert(nodes.size() > 0);
//     std::vector<NodeOffset> node_offs;
//     for(int i=0; i<page_ids.size(); i++){
//         auto hash_meta = global_meta_man->GetPageTableMeta(nodes[0]); // 获取第一个节点的页表元数据
//         auto hash = MurmurHash64A(page_ids[i].Get(), 0xdeadbeef) % hash_meta.bucket_num;
//         offset_t node_off = hash_meta.base_off + hash * sizeof(PageTableNode);
//         node_offs.push_back(NodeOffset{nodes[0], node_off});
//     }

//     assert(pending_hash_node_latch_offs.size() == 0);
//     std::unordered_map<NodeOffset, char*> local_hash_nodes;
//     std::unordered_map<NodeOffset, char*> cas_bufs;
//     std::unordered_map<NodeOffset, std::list<std::pair<PageId, bool>>> get_pagetable_request_list; // bool 存放的是is_write
    
//     // init local_hash_nodes and cas_bufs, and get_pagetable_request_list , and pending_hash_node_latch_offs
//     for(int i=0; i<node_offs.size(); i++){
//         auto node_off = node_offs[i];
//         if(local_hash_nodes.find(node_off) == local_hash_nodes.end()){
//             local_hash_nodes[node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
//         }
//         if(cas_bufs.find(node_off) == cas_bufs.end()){
//             cas_bufs[node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//         }
//         get_pagetable_request_list[node_off].push_back(std::make_pair(page_ids[i], is_write[i]));
//         pending_hash_node_latch_offs.emplace(node_off);
//     }
    
//     std::unordered_set<NodeOffset> unlock_node_off_with_write;
//     std::unordered_set<NodeOffset> hold_node_off_latch;
//     std::unordered_map<NodeOffset, NodeOffset> hold_latch_to_previouse_node_off; //维护了反向链表<node_off, previouse_node_off>
//     std::unordered_map<PageId, PageAddress> res;

//     while (pending_hash_node_latch_offs.size()!=0) {
//         // lock hash node bucket, and remove latch successfully from pending_hash_node_latch_offs
//         auto succ_node_off = ExclusiveLockHashNode(yield, QPType::kPageTable, local_hash_nodes, cas_bufs);
//         // init hold_node_off_latch
//         for(auto node_off : succ_node_off ){
//             hold_node_off_latch.emplace(node_off);
//         }

//         for(auto node_off : succ_node_off ){
//             // read now
//             PageTableNode* page_table_node = reinterpret_cast<PageTableNode*>(local_hash_nodes[node_off]);
//             // 遍历这个node_off上的所有请求即所有的page table item 
//             // 如果找到, 就从列表中移除
//             for(auto it = get_pagetable_request_list[node_off].begin(); it != get_pagetable_request_list[node_off].end(); ){
//                 // find empty slot to insert
//                 bool is_find = false;
//                 for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
//                     if (page_table_node->page_table_items[i].page_id == it->first && page_table_node->page_table_items[i].valid == true) {
//                         // unpin it
//                         page_table_node->page_table_items[i].page_valid = true;
//                         if(it->second == true){
//                             //is write
//                             page_table_node->page_table_items[i].rwcount += EXCLUSIVE_UNLOCK_TO_BE_ADDED;
//                             page_table_node->page_table_items[i].last_access_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
//                         }
//                         else{
//                             //is read
//                             page_table_node->page_table_items[i].rwcount--;
//                             page_table_node->page_table_items[i].last_access_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
//                         }
//                         // erase会返回下一个元素的迭代器
//                         it = get_pagetable_request_list[node_off].erase(it);
//                         is_find = true;
//                         break;
//                     }
//                 }
//                 // not find
//                 if(!is_find) it++;
//             }

//             // 如果这个node_off上的所有请求都被处理了, 可以释放这个node_off的latch以及之前所有的latch
//             if(get_pagetable_request_list[node_off].size() == 0){
//                 // release latch and write back
//                 auto release_node_off = node_off;
//                 while(true){
//                     unlock_node_off_with_write.emplace(release_node_off);
//                     if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
//                     release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
//                 }
//             }
//             else{
//                 // 存在未处理的请求, 保留latch
//                 // if PageTable node not exist, find next bucket
//                 auto expand_node_id = page_table_node->next_expand_node_id[0];
//                 bool continue_search = false;
//                 NodeOffset next_node_off;
//                 if(expand_node_id < 0){
//                     // 这个节点搜索完成，搜索下一个节点
//                     auto it = std::find(nodes.begin(), nodes.end(), node_off.nodeId);
//                     if(it + 1 != nodes.end()){
//                         node_id_t new_node_id = *(it+1);
//                         auto hash_meta = global_meta_man->GetPageTableMeta(new_node_id); // 获取下一个节点的页表元数据
//                         // 这里必须假定新节点的桶数和旧节点的桶数相同，即原节点的桶中所有元素也在新节点的一个桶中
//                         auto hash = MurmurHash64A(get_pagetable_request_list[node_off].front().first.Get(), 0xdeadbeef) % hash_meta.bucket_num;
//                         offset_t next_off = hash_meta.base_off + hash * sizeof(PageTableNode);
//                         next_node_off = {new_node_id, next_off};
//                         continue_search = true;
//                     }
//                     else{
//                         continue_search = false;
//                         // 所有节点都搜索完了，但没找到，报错
//                         RDMA_LOG(ERROR) << "UnpinPageTable: page table item not found";
//                         // release latch and write back
//                         auto release_node_off = node_off;
//                         while(true){
//                             unlock_node_off_with_write.emplace(release_node_off);
//                             if(hold_latch_to_previouse_node_off.count(release_node_off) == 0) break;
//                             release_node_off = hold_latch_to_previouse_node_off.at(release_node_off);
//                         }
//                     }
//                 }
//                 else{
//                     offset_t expand_base_off = global_meta_man->GetPageTableExpandBase(node_off.nodeId);
//                     offset_t next_off = expand_base_off + expand_node_id * sizeof(PageTableNode);
//                     next_node_off = {node_off.nodeId, next_off};
//                     continue_search = true;
//                 }
//                 if (continue_search) {
//                     pending_hash_node_latch_offs.emplace(next_node_off);
//                     get_pagetable_request_list.emplace(next_node_off, get_pagetable_request_list.at(node_off));
//                     hold_latch_to_previouse_node_off.emplace(next_node_off, node_off);
//                     assert(local_hash_nodes.count(next_node_off) == 0);
//                     assert(cas_bufs.count(next_node_off) == 0);
//                     local_hash_nodes[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(PageTableNode));
//                     cas_bufs[next_node_off] = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//                 }
//             }
//         }
//         // release all latch and write back
//         for (auto node_off : unlock_node_off_with_write){
//             ExclusiveUnlockHashNode_WithWrite(node_off, local_hash_nodes[node_off], QPType::kPageTable);
//             hold_node_off_latch.erase(node_off);
//         }
//         unlock_node_off_with_write.clear();
//     }
//     // 这里所有的latch都已经释放了
//     assert(hold_node_off_latch.size() == 0);
// }