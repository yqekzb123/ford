// Author: huangdund
// Copyright (c) 2023

#include <brpc/channel.h>
#include "dtx/dtx.h"
#include "worker/global.h"
#include "storage/storage_service.pb.h"
#include "log/record.h"
#include "worker/worker.h"

// 在页表中添加数据项的基本逻辑：
// 1. 如果Fetch的Page在页表中，则需要针对Fetch的类型进行判断是否可以读取，是否需要更改wlatch的状态
// 2. 如果Fetch的Page不在页表中，并且是一个增加record或删除record的操作，则必须需要将该Page加入页表中，防止多个对页面的修改造成的冲突，
// 并将valid置为false，wlatch置为true, 此时如果有其他线程正在读该数据页，则遇到invalid状态会重新执行此函数
// 3. 如果Fetch的Page不在页表中，并且是一个读取Page或update操作，则可以选择性将该Page加入页表中，
// 并将valid置为false，wlatch置为false, 此时如果有其他线程正在读该数据页，则遇到等待valid状态是true之后将修改刷入共享内存池

// 一个在页表中的页面可能处于以下几种状态：
// 1. 该页面已经在共享内存池中，并且没有被增删record的线程获取，此时该页面的wlatch未被占用，可以直接读该数据页
// 2. 该页面已经在共享内存池中，但被增删record的线程获取，此时该页面的wlatch被占用，此时如果是增删操作需要等待wlatch释放
// 3. 该页面还不在共享内存池中，正在由其他线程从磁盘中读取，此时需要等待该页面写完成刷入共享内存池

// 在页表中删除数据项的基本逻辑：
// 如果页面没有正在被插入/删除/更新线程读取，才可以驱逐出缓冲区，否则计算节点刷新数据页会刷到错误的page
// 因此在fetch一个page的时候需要令page的pin count加1
// 只有当页面的pin count为0时，才可以将该页面从页表中删除
// 存在一个后台线程，定期扫描页表，将pin count为0并且超时的页面从页表中删除

// 返回的page_addr_vec中的PageAddress是作为返回值使用，因此传入空vector即可
std::unordered_map<PageId, char*> DTX::FetchPage(coro_yield_t &yield, std::unordered_map<PageId, FetchPageType> ids, batch_id_t request_batch_id){
    std::vector<PageId> page_ids;
    std::unordered_map<PageId, bool> need_fetch_from_disk;
    std::unordered_map<PageId, bool> now_valid;
    std::vector<bool> is_write;
    // to store res
    std::unordered_map<PageId, char*> pages;
    for(auto id : ids){
        page_ids.push_back(id.first);
        if(id.second == FetchPageType::kReadPage || id.second == FetchPageType::kUpdateRecord){
            // 这两种类型的操作，无页面粒度的写入冲突，因此不需要检查wlatch的状态
            // 如果不在页表，则顺便将该页面加入页表，并将valid状态置为false，wlatch状态置为false, rcount+1
            is_write.push_back(false);
        }
        else if(id.second == FetchPageType::kInsertRecord || id.second == FetchPageType::kDeleteRecord ){
            // 如果不在页表，则顺便将该页面加入页表，并将valid状态置为false，wlatch状态置为true, wcount+1
            is_write.push_back(true);
        }
        else{
            assert(false);
        }
    }
    double pagetable_usec = 0;
    double disk_fetch_usec = 0;
    double mem_fetch_usec = 0;
    std::vector<PageAddress> page_addr_vec;
    while(true){
        // 计时1
        timespec msr_start;
        clock_gettime(CLOCK_REALTIME, &msr_start);

        page_addr_vec = GetPageAddrOrAddIntoPageTable(yield, page_ids, need_fetch_from_disk, now_valid, is_write);
        // // for debug
        // for(int i=0; i<page_addr_vec.size(); i++){
        //     std::cout << "*-* FetchPage: table_id:" << page_ids[i].table_id << " page_no: " << page_ids[i].page_no 
        //         << "into page_addr_vec: frame id" << page_addr_vec[i].frame_id 
        //         << " now_valid: " <<  now_valid[page_ids[i]] << " need_fetch_from_disk: " << need_fetch_from_disk[page_ids[i]] << std::endl;
        // }

        // 计时2
        timespec msr_pagetable;
        clock_gettime(CLOCK_REALTIME, &msr_pagetable);
        pagetable_usec += (msr_pagetable.tv_sec - msr_start.tv_sec) * 1000000 + (double)(msr_pagetable.tv_nsec - msr_start.tv_nsec) / 1000;

        std::vector<PageId> new_page_id;
        std::vector<bool> new_is_write;

        storage_service::StorageService_Stub stub(storage_channel);
        storage_service::GetPageRequest request;
        storage_service::GetPageResponse response;
        brpc::Controller cntl;
        request.set_require_batch_id(request_batch_id);
        std::vector<int> need_fetch_idx;
        for(int i=0; i<need_fetch_from_disk.size(); i++) {
            if (need_fetch_from_disk[page_ids[i]]) {
                // 构造request
                // !这里有内存泄漏，暂时先懒得改
                std::string* table_name = new std::string();
                *table_name = global_meta_man->GetTableName(page_ids[i].table_id);
                request.add_page_id();
                request.mutable_page_id(need_fetch_idx.size())->set_allocated_table_name(table_name);
                request.mutable_page_id(need_fetch_idx.size())->set_page_no(page_ids[i].page_no);

                need_fetch_idx.push_back(i);
            }
            else if(now_valid[page_ids[i]] == true){
                // 计时
                timespec msr_mem_fetch;
                clock_gettime(CLOCK_REALTIME, &msr_mem_fetch);

                // 从共享内存池中读取数据页
                auto remote_node_id = page_addr_vec[i].node_id;
                auto frame_id = page_addr_vec[i].frame_id;
                
                auto remote_offset = global_meta_man->GetDataOff(remote_node_id);
                RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
                
                char* page = thread_rdma_buffer_alloc->Alloc(PAGE_SIZE);
                if(!coro_sched->RDMAReadInv(coro_id, qp, page, remote_offset + frame_id * PAGE_SIZE, PAGE_SIZE)){
                    assert(false);
                }
                // std::cout << "ReadPageFromBuffer: " << page_ids[i].table_id << " " << page_ids[i].page_no << " from frame id: " 
                //     << frame_id << std::endl;
                pages.emplace(page_ids[i], page);
                // 记录page的地址和远程地址
                page_data_localaddr_and_remote_offset[page_ids[i]] = std::make_pair(page, page_addr_vec[i]);

                // 计时
                timespec msr_end;
                clock_gettime(CLOCK_REALTIME, &msr_end);
                mem_fetch_usec += (msr_end.tv_sec - msr_mem_fetch.tv_sec) * 1000000 + (double)(msr_end.tv_nsec - msr_mem_fetch.tv_nsec) / 1000;
            }
            else{
                // 如果这里是false，需要反复调用FetchPage直到now_valid为true
                new_page_id.push_back(page_ids[i]);
                new_is_write.push_back(is_write[i]);
            }
        }
        // 在这里从磁盘获取数据页
        if(need_fetch_idx.size() > 0){
            // 计时
            timespec msr_disk_fetch;
            clock_gettime(CLOCK_REALTIME, &msr_disk_fetch);

            stub.GetPage(&cntl, &request, &response, NULL);
            const char *constPage = response.data().c_str();
            int j = 0;
            for(auto i: need_fetch_idx){
                char *page = thread_rdma_buffer_alloc->Alloc(PAGE_SIZE);
                memcpy(page, constPage + (j++) * PAGE_SIZE, PAGE_SIZE);
                pages.emplace(page_ids[i], page);
                // 记录page的地址和远程地址
                page_data_localaddr_and_remote_offset[page_ids[i]] = std::make_pair(page, page_addr_vec[i]);

                // 在这里先将数据页写入共享内存池，以便并行访问
                auto remote_node_id = page_addr_vec[i].node_id;
                auto frame_id = page_addr_vec[i].frame_id;
                auto remote_offset = global_meta_man->GetDataOff(remote_node_id);
                RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
                if(!coro_sched->RDMAWrite(coro_id, qp, page, remote_offset + frame_id * PAGE_SIZE, PAGE_SIZE)){
                    assert(false);
                }

                // 在这里将对应的页表的item的now valid置为true
                char* local_item = page_table_item_localaddr_and_remote_offset[page_ids[i]].first;
                NodeOffset node_offset = page_table_item_localaddr_and_remote_offset[page_ids[i]].second;
                PageTableItem* item = reinterpret_cast<PageTableItem*>(local_item);
                item->page_valid = true;
                qp = thread_qp_man->GetRemotePageTableQPWithNodeID(node_offset.nodeId); 
                if(!coro_sched->RDMAWrite(coro_id, qp, local_item, node_offset.offset, sizeof(PageTableItem))){
                    assert(false);
                };
            }
            // 计时
            timespec msr_end;
            clock_gettime(CLOCK_REALTIME, &msr_end);
            disk_fetch_usec += (msr_end.tv_sec - msr_disk_fetch.tv_sec) * 1000000 + (double)(msr_end.tv_nsec - msr_disk_fetch.tv_nsec) / 1000;
        }
        if(new_page_id.size() == 0){
            break;
        } else{
            page_ids = new_page_id;
            is_write = new_is_write;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // 1ms
    }

    DEBUG_TIME("pagetable_usec: %lf, disk_fetch_usec: %lf, mem_fetch_usec: %lf\n", pagetable_usec, disk_fetch_usec, mem_fetch_usec);
    coro_sched->Yield(yield, coro_id);
    assert(page_data_localaddr_and_remote_offset.size() == pages.size());
    return pages;
}

bool DTX::UnpinPage(coro_yield_t &yield, std::vector<PageId> ids,  std::vector<FetchPageType> types){

    // 1. 根据rids获取对应的page_id
    // 这里先用unordered_map转化，已处理Rid中的重复page_id
    assert(ids.size() == types.size());
    std::unordered_map<PageId, FetchPageType> ids_map;
    for(int i=0; i<ids.size(); i++){
        assert(types[i] == FetchPageType::kReadPage || types[i] == FetchPageType::kUpdateRecord);
        ids_map[ids[i]] = types[i];
    }

    // 修改页表中的item
    for(auto id : ids_map){
        char* local_item = page_table_item_localaddr_and_remote_offset[id.first].first;
        NodeOffset node_offset = page_table_item_localaddr_and_remote_offset[id.first].second;
        PageTableItem* item = reinterpret_cast<PageTableItem*>(local_item);

        RCQP* qp = thread_qp_man->GetRemotePageTableQPWithNodeID(node_offset.nodeId);
        if(id.second == FetchPageType::kReadPage || id.second == FetchPageType::kUpdateRecord){
            // rwcount - 1
            char* faa_cnt = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
            if(!coro_sched->RDMAFAA(coro_id, qp, faa_cnt, node_offset.offset + (offset_t)&(item->rwcount) - (offset_t)item, SHARED_UNLOCK_TO_BE_ADDED))
                assert(false);
        }
        else if(id.second == FetchPageType::kInsertRecord || id.second == FetchPageType::kDeleteRecord){
            // 插入/删除数据页
            char* faa_cnt = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
            if(!coro_sched->RDMAFAA(coro_id, qp, faa_cnt, node_offset.offset + (offset_t)&(item->rwcount) - (offset_t)item, EXCLUSIVE_UNLOCK_TO_BE_ADDED))
                assert(false);
        }
        else{
            assert(false);
        }
    }
    coro_sched->Yield(yield, coro_id);
    return true;
}
    
DataItemPtr DTX::GetDataItemFromPage(table_id_t table_id, char* data, Rid rid){
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    char *slots = bitmap + global_meta_man->GetTableMeta(table_id).bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t));
    DataItemPtr itemPtr = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t)));
    // DataItemPtr itemPtr((DataItem*)(tuple + sizeof(itemkey_t)));
    return itemPtr;
}

// 从数据页中读取数据项
std::vector<DataItemPtr> DTX::FetchTuple(coro_yield_t &yield, std::vector<table_id_t> table_id, std::vector<Rid> rids, std::vector<FetchPageType> types, batch_id_t request_batch_id){
    // 1. 根据rids获取对应的page_id
    // 这里先用unordered_map转化，已处理Rid中的重复page_id
    assert(table_id.size() == rids.size());
    assert(rids.size() == types.size());
    std::unordered_map<PageId, FetchPageType> ids;
    for(int i=0; i<rids.size(); i++){
        PageId page_id;
        page_id.table_id = table_id[i];
        page_id.page_no = rids[i].page_no_;
        assert(types[i] == FetchPageType::kReadPage || types[i] == FetchPageType::kUpdateRecord);
        ids[page_id] = types[i];
    }
    // 2. 根据page_id获取对应的page
    std::unordered_map<PageId, char*> get_pages = FetchPage(yield, ids, request_batch_id);
    // 3. 根据page_id和rids获取对应的data_item
    std::vector<DataItemPtr> data_items;
    for(int i=0; i<rids.size(); i++){
        PageId page_id;
        page_id.table_id = table_id[i];
        page_id.page_no = rids[i].page_no_;
        char* page = get_pages[page_id];
        data_items.push_back(GetDataItemFromPage(table_id[i], page, rids[i]));
    }
    return data_items;
}


bool DTX::WriteTuple(coro_yield_t &yield, std::vector<table_id_t> &table_id, std::vector<Rid> &rids, std::vector<FetchPageType> &types, std::vector<DataItemPtr> &data, batch_id_t request_batch_id){
    assert(table_id.size() == rids.size());
    assert(rids.size() == types.size());

    for(int i=0; i<rids.size(); i++){
        if(types[i] == FetchPageType::kReadPage){
            continue;
        }
        else if(types[i] == FetchPageType::kUpdateRecord){
            // 1. 根据rids获取对应的page_id
            PageId page_id;
            page_id.table_id = table_id[i];
            page_id.page_no = rids[i].page_no_;
            // 2. 根据page_id获取对应的page
            char* page = page_data_localaddr_and_remote_offset[page_id].first;
            PageAddress remote_page_addr = page_data_localaddr_and_remote_offset[page_id].second;
            // 3. 根据page_id和rids获取对应的data_item
            auto hdr_size = sizeof(RmPageHdr) + OFFSET_PAGE_HDR + global_meta_man->GetTableMeta(table_id[i]).bitmap_size_;
            memcpy(page + hdr_size + rids[i].slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t)) + sizeof(itemkey_t), data[i].get(), sizeof(DataItem));
            // 4. 将修改写回到共享内存池
            auto remote_node_id = remote_page_addr.node_id;
            auto frame_id = remote_page_addr.frame_id;

            auto remote_offset = global_meta_man->GetDataOff(remote_node_id);
            RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
            
            if(!coro_sched->RDMAWrite(coro_id, qp, page + hdr_size + rids[i].slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t)), 
                    remote_offset + frame_id * PAGE_SIZE + hdr_size + rids[i].slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t)),
                    sizeof(DataItem)+sizeof(itemkey_t)) ){
                assert(false);
            }
        }
        else{
            assert(false);
        }
    }
    return true;
}