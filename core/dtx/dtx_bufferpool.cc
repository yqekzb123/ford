#include <brpc/channel.h>
#include "dtx/dtx.h"
#include "storage/storage_service.pb.h"

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:12348", "IP address of server");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

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

std::unordered_map<PageId, char*> DTX::FetchPage(coro_yield_t &yield, std::unordered_map<PageId, FetchPageType> ids, batch_id_t request_batch_id){
    std::vector<PageId> page_ids;
    std::unordered_map<PageId, bool> need_fetch_from_disk;
    std::unordered_map<PageId, bool> now_valid;
    std::vector<bool> is_write;
    // to store res
    std::vector<char*> pages;
    for(auto id : ids){
        page_ids.push_back(id.first);
        if(id.second == FetchPageType::kReadPage || id.second == FetchPageType::kUpdateRecord){
            // 这两种类型的操作，无页面粒度的写入冲突，因此不需要检查wlatch的状态
            // 如果不在页表，则顺便将该页面加入页表，并将valid状态置为false，wlatch状态置为false, rcount+1
            is_write.push_back(true);
        }
        else if(id.second == FetchPageType::kInsertRecord || id.second == FetchPageType::kDeleteRecord ){
            // 如果不在页表，则顺便将该页面加入页表，并将valid状态置为false，wlatch状态置为true, wcount+1
            is_write.push_back(false);
        }
        else{
            assert(false);
        }
    }
    std::vector<PageAddress> page_addr_vec = GetPageAddrOrAddIntoPageTable(yield, page_ids, need_fetch_from_disk, now_valid, is_write);
    
    for(int i=0; i<need_fetch_from_disk.size(); i++) {
        if (need_fetch_from_disk[page_ids[i]]) {
            // 从磁盘中读取数据页
            brpc::ChannelOptions options;
            brpc::Channel channel;
            
            options.use_rdma = true;
            options.protocol = FLAGS_protocol;
            options.connection_type = FLAGS_connection_type;
            options.timeout_ms = FLAGS_timeout_ms;
            options.max_retry = FLAGS_max_retry;
            if(channel.Init(FLAGS_server.c_str(), &options) != 0) {
                RDMA_LOG(FATAL) << "Fail to initialize channel";
            }
            storage_service::StorageService_Stub stub(&channel);

            storage_service::GetPageRequest request;
            storage_service::GetPageResponse response;
            brpc::Controller cntl;

            request.mutable_page_id()->set_table_name(global_meta_man->GetTableName(page_ids[i].table_id));
            request.mutable_page_id()->set_page_no(page_ids[i].page_no);

            request.set_require_batch_id(request_batch_id);
            
            stub.GetPage(&cntl, &request, &response, NULL);

            const char *constPage = response.data().c_str();
            // 记得释放内存
            char *page = new char[response.data().length() + 1];
            std::strcpy(page, constPage);
            pages.push_back(page);
        }
        else if(now_valid[page_ids[i]] == true){
            // 从共享内存池中读取数据页
            
        }
    }
}

DataItemPtr GetDataItemFromPage(char* page, Rid rid){
    RmPageHandle(&file_hdr_, page);
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
        data_items.push_back(GetDataItemFromPage(page, rids[i]));
    }
}