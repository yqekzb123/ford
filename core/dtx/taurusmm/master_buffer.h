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
#include <tbb/concurrent_hash_map.h>

struct page_table_entry {
    frame_id_t frame_id;
    std::mutex mutex;
    page_table_entry(frame_id_t frame_id) : frame_id(frame_id) {};
};

class SubBufferPool{
  private:
    Page *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    tbb::concurrent_hash_map<PageId, frame_id_t, PageIdHash> concurrent_page_table_; // 支持并发的哈希表
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    std::list<frame_id_t> free_list_;   // 空闲帧编号的链表
    Replacer *replacer_;    // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::mutex latch_;      // 用于共享数据结构的并发控制
    brpc::Channel* data_channel;
    MetaManager* meta_man;

  public:
    SubBufferPool(size_t pool_size, MetaManager* meta_manager)
            :meta_man(meta_manager) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size];
        replacer_ = new LRUReplacer(pool_size);
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size; ++i) {
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
    ~SubBufferPool() {
        delete[] pages_;
        delete replacer_;
    }
    Page* fetch_page(PageId page_id);

    bool unpin_page(PageId page_id);

    bool find_victim_page(frame_id_t* frame_id);
    void update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id);
};

class MasterBufferPoolManager {
   private:
    size_t poolsize_per_subbufferpool_;      // buffer_pool中可容纳页面的个数，即帧的个数
    int num_subbufferpools_;                 // buffer_pool的个数
    SubBufferPool** sub_buffer_pool;
   public:
    MasterBufferPoolManager(size_t poolsize_per_subbufferpool, int num_subbufferpools, MetaManager* meta_manager)
        : poolsize_per_subbufferpool_(poolsize_per_subbufferpool), num_subbufferpools_(num_subbufferpools) {
        
        // 为buffer pool分配一块连续的内存空间
        sub_buffer_pool = new SubBufferPool*[num_subbufferpools_];
        for(int i = 0; i < num_subbufferpools_; i++){
            sub_buffer_pool[i] = new SubBufferPool(poolsize_per_subbufferpool_, meta_manager);
        }
    }

    ~MasterBufferPoolManager() {
        delete[] sub_buffer_pool;
    }

   public: 
    Page* fetch_page(PageId page_id);
    bool unpin_page(PageId page_id);
};