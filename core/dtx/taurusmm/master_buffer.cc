#include "master_buffer.h"
#include "dtx/dtx.h"
#include "worker/global.h"
#include "storage/storage_service.pb.h"
#include "log/record.h"
#include "worker/worker.h"
#include "dtx/exception.h"

bool MasterBufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    bool ret = replacer_->victim(frame_id);
    return ret;
}

void MasterBufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // 1 是不是脏页无所谓，直接扔掉

    // 2 更新page table
    page_table_.erase(page->get_page_id());          // 删除页表中原page_id和其对应frame_id
    if (new_page_id.page_no != INVALID_PAGE_ID) {  // 注意INVALID_PAGE_ID不要加到页表
        page_table_[new_page_id] = new_frame_id;   // 新的page_id和其对应frame_id加到页表
    }

    // 3 重置page的data，更新page id
    page->reset_memory();
    page->id_ = new_page_id;
}

Page* MasterBufferPoolManager::fetch_page(PageId page_id) {
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页
    std::unique_lock<std::mutex> lock{latch_};
    auto iter = page_table_.find(page_id);
    // 1 该page在页表中存在（说明该page在缓冲池中）
    if (iter != page_table_.end()) {
        frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
        Page *page = &pages_[frame_id];      // 由frame_id得到page
        replacer_->pin(frame_id);            // pin it
        page->pin_count_++;                  // 更新pin_count
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
    
    // 4 固定目标页，更新pin_count_
    replacer_->pin(frame_id);  // pin it
    page->pin_count_ = 1;
    return page;
}

bool MasterBufferPoolManager::unpin_page(PageId page_id) {

    std::unique_lock<std::mutex> lock{latch_};
    auto iter = page_table_.find(page_id);
    // 1 该page在页表中不存在
    if (iter == page_table_.end()) {
        return false;
    }
    // 2 该page在页表中存在
    frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
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

void MasterBufferPoolManager::WLatch(Page* page) {
    page->rwlatch_.lock();
    return;
}

void MasterBufferPoolManager::RLatch(Page* page) {
    page->rwlatch_.lock_shared();
    return;
}

void MasterBufferPoolManager::WUnlatch(Page* page) {
    page->rwlatch_.unlock();
    return;
}

void MasterBufferPoolManager::RUnlatch(Page* page) {
    page->rwlatch_.unlock_shared();
    return;
}
