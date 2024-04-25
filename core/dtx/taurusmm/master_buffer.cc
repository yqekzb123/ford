#include "master_buffer.h"
#include "dtx/dtx.h"
#include "worker/global.h"
#include "storage/storage_service.pb.h"
#include "log/record.h"
#include "worker/worker.h"
#include "dtx/exception.h"

bool SubBufferPool::find_victim_page(frame_id_t* frame_id) {
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    bool ret = replacer_->victim(frame_id);
    return ret;
}

void SubBufferPool::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // 1 是不是脏页无所谓，直接扔掉

    // 2 更新page table
    concurrent_page_table_.erase(page->get_page_id());          // 删除页表中原page_id和其对应frame_id
    if (new_page_id.page_no != INVALID_PAGE_ID) {  // 注意INVALID_PAGE_ID不要加到页表
        concurrent_page_table_.insert({new_page_id, new_frame_id});  // 新的page_id和其对应frame_id加到页表
        // concurrent_page_table_[new_page_id] = new_frame_id;   // 新的page_id和其对应frame_id加到页表
    }

    // 3 重置page的data，更新page id
    page->reset_memory();
    page->id_ = new_page_id;
    page->is_valid_ = false;
}

Page* SubBufferPool::fetch_page(PageId page_id) {
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页

    #if OPEN_TIME
        // 计时
        struct timespec tx_start_time;
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
    #endif

    std::unique_lock<std::mutex> lock{latch_};
    
    #if OPEN_TIME
        // 计时
        struct timespec tx_latch_time;
        clock_gettime(CLOCK_REALTIME, &tx_latch_time);
        double tx_latch_usec = (tx_latch_time.tv_sec - tx_start_time.tv_sec) * 1000000000 + (tx_latch_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
        DEBUG_TIME("Latch: %lf us\n", tx_latch_usec);
    #endif
    tbb::concurrent_hash_map<PageId, frame_id_t, PageIdHash>::accessor accessor;

    // 1 该page在页表中存在（说明该page在缓冲池中）
    if (concurrent_page_table_.find(accessor, page_id)) {
        frame_id_t frame_id = accessor->second;  // iter是pair类型，其second是page_id对应的frame_id
        Page *page = &pages_[frame_id];      // 由frame_id得到page
        replacer_->pin(frame_id);            // pin it
        page->pin_count_++;                  // 更新pin_count
        // lock.unlock();
        while(page->is_valid_ == false) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return page;
    }
    // 2 该page在页表中不存在（说明该page不在缓冲池中，而在PageStore中）
    frame_id_t frame_id = INVALID_FRAME_ID;
    // 2.1 没有找到victim page
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    // 2.2 找到victim page，将其data替换为磁盘中该page的内容
    Page *page = &pages_[frame_id];
    update_page(page, page_id, frame_id);  // data置为空，dirty页写入磁盘，然后dirty状态置false
    // 3 固定目标页，更新pin_count_
    replacer_->pin(frame_id);  // pin it
    page->pin_count_ = 1;
    lock.unlock();
    
    // 3 读取目标页到frame
    // rpc read
    storage_service::StorageService_Stub stub(data_channel);
    storage_service::GetPageRequest request;
    storage_service::GetPageResponse* response = new storage_service::GetPageResponse;
    brpc::Controller* cntl = new brpc::Controller;
    request.set_require_batch_id(0);
    std::string* table_name = new std::string();
    *table_name = meta_man->GetTableName(page_id.table_id);
    request.add_page_id();
    request.mutable_page_id(0)->set_allocated_table_name(table_name);
    request.mutable_page_id(0)->set_page_no(page_id.page_no);
    stub.GetPage(cntl, &request, response, NULL);
    memcpy(page->data_, response->data().c_str(), PAGE_SIZE);

    page->is_valid_ = true;

    #if OPEN_TIME
        // 计时
        struct timespec tx_end_time;
        clock_gettime(CLOCK_REALTIME, &tx_end_time);
        double tx_usec = (tx_end_time.tv_sec - tx_latch_time.tv_sec) * 1000000000 + (tx_end_time.tv_nsec - tx_latch_time.tv_nsec) / 1000;
        DEBUG_TIME("Fetch page from disk: %lf us\n", tx_usec);
    #endif
    
    return page;
}

bool SubBufferPool::unpin_page(PageId page_id) {
    std::unique_lock<std::mutex> lock{latch_};
    tbb::concurrent_hash_map<PageId, frame_id_t, PageIdHash>::accessor accessor;
    // 1 该page在页表中不存在
    if (!concurrent_page_table_.find(accessor, page_id)) {
        return false;
    }
    // 2 该page在页表中存在
    frame_id_t frame_id = accessor->second;  // iter是pair类型，其second是page_id对应的frame_id
    Page *page = &pages_[frame_id];      // 由frame_id得到page
    // 2.1 pin_count = 0
    if (page->pin_count_ == 0) {
        return false;
    }
    // 2.2 pin_count > 0
    // 只有pin_count>0才能进行pin_count--，如果pin_count=0之前就直接返回了
    page->pin_count_--;  // 这里特别注意，只有pin_count减到0的时候才让replacer进行unpin
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    return true;
}

Page* MasterBufferPoolManager::fetch_page(PageId page_id){
    // 计算哈希值
    size_t hash_value = page_id.Get() % num_subbufferpools_;
    // 从对应的子缓冲池中获取page
    return sub_buffer_pool[hash_value]->fetch_page(page_id);
}

bool MasterBufferPoolManager::unpin_page(PageId page_id){
    // 计算哈希值
    size_t hash_value = page_id.Get() % num_subbufferpools_;
    // 从对应的子缓冲池中unpin page
    return sub_buffer_pool[hash_value]->unpin_page(page_id);
}
