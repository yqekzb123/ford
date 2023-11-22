#pragma once
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <unordered_map>
#include <vector>
#include <mutex>

#include "storage/disk_manager.h"
#include "util/errors.h"
#include "base/page.h"
#include "replacer/clock_replacer.h"
#include "replacer/lru_replacer.h"
#include "replacer/replacer.h"
// 这里！！！！！！！！！！！！！！！！！！！！！！
// 逻辑开始混沌了，，，，
// 注意区分是哪个bufferpool，思路，把现在这个bufferpool放到test里
// 之后重新写一个bufferpool，内存层的
// 磁盘里是没有bufferpool的 也不需要record 这些都是为了测试用的

// 然后在新的bufferpool里面，完成缓冲区驱逐策略的逻辑
// 一个bufferpool里面，只需要有page的data，也不需要pageID这些东西，因为这个是在页表维护的
// 对

class BufferPoolManager {
   private:
    size_t pool_size_;      // buffer_pool中可容纳页面的个数，即帧的个数
    Page *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    std::list<frame_id_t> free_list_;   // 空闲帧编号的链表
    Replacer *replacer_;    // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::mutex latch_;      // 用于共享数据结构的并发控制

   public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size_];
        // 可以被Replacer改变
        if (strcmp(REPLACER_TYPE, "LRU") == 0)
            replacer_ = new LRUReplacer(pool_size_);
        else if (strcmp(REPLACER_TYPE, "CLOCK") == 0)
            replacer_ = new LRUReplacer(pool_size_);
        else {
            replacer_ = new LRUReplacer(pool_size_);
        }
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<frame_id_t>(i));  // static_cast转换数据类型
        }
    }

    ~BufferPoolManager() {
        delete[] pages_;
        delete replacer_;
    }

    /**
     * @description: 将目标页面标记为脏页
     * @param {Page*} page 脏页
     */
    static void mark_dirty(Page* page) { page->is_dirty_ = true; }

   public: 
    Page* fetch_page(PageId page_id);

    bool unpin_page(PageId page_id, bool is_dirty);

    bool flush_page(PageId page_id);

    Page* new_page(PageId* page_id);

    bool delete_page(PageId page_id);

    void flush_all_pages(int fd);

   private:
    bool find_victim_page(frame_id_t* frame_id);

    void update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id);
};