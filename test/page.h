/*
 * @Description: 
 */
#pragma once

// #include "common/config.h"
// #include "common/rwlatch.h"
#include <string>
#include <cstring>
#include <shared_mutex>
#include <mutex> 
#include <atomic>
#include "base/common.h"

/**
 * @description: Page类声明, Page是rucbase数据块的单位、是负责数据操作Record模块的操作对象，
 * Page对象在磁盘上有文件存储, 若在Buffer中则有帧偏移, 并非特指Buffer或Disk上的数据
 */
class Page {
    friend class BufferPoolManager;
    friend class MasterBufferPoolManager;
    friend class SubBufferPool;

   public:
    
    Page() { reset_memory(); }

    ~Page() = default;

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    void WLatch() {
        rwlatch_.lock();
        return;
    }

    void RLatch() {
        rwlatch_.lock_shared();
        return;
    }

    void WUnlatch() {
        rwlatch_.unlock();
        return;
    }

    void RUnlatch() {
        rwlatch_.unlock_shared();
        return;
    }

   private:
    void reset_memory() { memset(data_, 0, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId id_;

    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};

    /** 脏页判断 */
    bool is_dirty_ = false;

    /** The pin count of this page. */
    std::atomic<int> pin_count_ = 0;

    std::atomic<bool> is_valid_ = false;
    
    // page latch. used to protect the content
    std::shared_mutex rwlatch_;
};