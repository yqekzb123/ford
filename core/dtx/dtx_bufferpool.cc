#include "dtx/dtx.h"

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

char* DTX::FetchPage(PageId id, FetchPageType type){

    if(type == FetchPageType::kReadPage || type == FetchPageType::kUpdateRecord){
        // 这两种类型的操作，无页面粒度的写入冲突，因此不需要检查wlatch的状态
        // 如果不在页表，则顺便将该页面加入页表，并将valid状态置为false，wlatch状态置为false, pin_count+1
        bool need_fetch_from_disk; bool now_valid;
        PageAddress page_address = GetPageAddrOrAddIntoPageTable(id, &need_fetch_from_disk, &now_valid, false);
        // 如果在页表中
        if(exist == true && now_valid == true){
            // 读取该数据页
            
        }
        else{
            
        }
    }

    // 1. RDMA读页表探测page_id在共享内存池, 如果页表中不存在该page_id, 
    // 则在页表桶中添加该page_id, 并置为invalid状态, 此时如果有其他线程正在读该数据页, 则遇到invalid状态会重新执行此函数
    
    PageAddress page_address = GetPageAddr(id);
    // 2. 如果在共享内存池
    if(page_address.node_id >= 0){
        // 2.1 判断如果Page将进行读操作, 不管页面的wlatch是否被占用，都可以读该数据页

        // 2.2 如果Page将进行写操作, 则需要判断页面的wlatch是否被占用

        // 2.3 如果页面的wlatch被占用, 则需要等待页面的wlatch释放

        // 2.4 如果页面的wlatch未被占用, 则可以直接读该数据页
        
    }
    // 3. 如果不在共享内存池
    else{
        // 3.1 RPC 调用获取刷新数据页的frame_id

        // 3.2 RDMA 删除页表中被替换出的数据项，这里应检查该数据页是否当前为invalid状态，如果是，则需要重新执行此函数

        // 3.3 RDMA 插入页表，将新数据页的<page_id, frame_id>写入页表, 这里需要检查该数据页是否已经在页表中，如果是，则需要重新执行此函数

        // 3.4 RPC 从磁盘中读取数据页
    }
    // 4. 返回数据页的内存地址
}