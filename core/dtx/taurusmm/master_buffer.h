#pragma once
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <brpc/channel.h>

#include "connection/meta_manager.h"
#include "util/json_config.h"
#include "storage/disk_manager.h"
#include "util/errors.h"
#include "base/page.h"
#include "page.h"
#include "buffer/replacer/lru_replacer.h"
#include "buffer/replacer/replacer.h"

class MasterBufferPoolManager {
   private:
    size_t pool_size_;      // buffer_pool中可容纳页面的个数，即帧的个数
    Page *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    std::list<frame_id_t> free_list_;   // 空闲帧编号的链表
    Replacer *replacer_;    // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::mutex latch_;      // 用于共享数据结构的并发控制
    brpc::Channel* data_channel;
    MetaManager* meta_man;
   public:
    MasterBufferPoolManager(size_t pool_size, MetaManager* meta_manager)
        : pool_size_(pool_size), meta_man(meta_manager) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size_];
        replacer_ = new LRUReplacer(pool_size_);
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<frame_id_t>(i));  // static_cast转换数据类型
        }

        // Init Brpc channel
        brpc::ChannelOptions options;
        // // brpc::Channel channel;
        // options.use_rdma = false;
        // options.protocol = "baidu_std";
        // options.connection_type = "";
        // options.timeout_ms = 0x7fffffff;
        // options.max_retry = 3;
        std::string storage_config_filepath = "../../../config/compute_node_config.json";
        auto storage_json_config = JsonConfig::load_file(storage_config_filepath);
        auto storage_conf = storage_json_config.get("remote_storage_nodes");
        auto ip = storage_conf.get("remote_storage_node_ips").get(0).get_str();
        auto port = storage_conf.get("remote_storage_node_rpc_port").get(0).get_int64();
        
        std::string storage_node = ip + ":" + std::to_string(port);
        data_channel = new brpc::Channel();
        if(data_channel->Init(storage_node.c_str(), &options) != 0) {
            std::cout << "Fail to initialize channel";
            assert(false);
        }
    }

    ~MasterBufferPoolManager() {
        delete[] pages_;
        delete replacer_;
    }

   public: 
    Page* fetch_page(PageId page_id);

    bool unpin_page(PageId page_id);

    void WLatch(Page* page);
    void RLatch(Page* page);
    void WUnlatch(Page* page);
    void RUnlatch(Page* page);
    
   private:
    bool find_victim_page(frame_id_t* frame_id);

    void update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id);
};