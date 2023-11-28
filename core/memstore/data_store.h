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

// 这里！！！！！！！！！！！！！！！！！！！！！！
// 逻辑开始混沌了，，，，
// 注意区分是哪个bufferpool，思路，把现在这个bufferpool放到test里
// 之后重新写一个bufferpool，内存层的
// 磁盘里是没有bufferpool的 也不需要record 这些都是为了测试用的

// 然后在新的bufferpool里面，完成缓冲区驱逐策略的逻辑
// 一个bufferpool里面，只需要有page的data，也不需要pageID这些东西，因为这个是在页表维护的

class DataStore {
   private:
    size_t pool_size_;      // buffer_pool中可容纳页面的个数，即帧的个数
    MemPage *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    
   public:
    DataStore(size_t pool_size)
        : pool_size_(pool_size) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = new MemPage[pool_size_];
    }

    ~DataStore() {
        delete[] pages_;
    }

   public: 
};