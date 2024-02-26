//author: huangdund
// Copyrigth (c) 2023
#include "page_table.h"
#include "util/json_config.h"

node_id_t PageTableStore::GetDataStoreMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    RDMA_LOG(ERROR) << "DataStoreMeta inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    RDMA_LOG(ERROR) << "DataStoreMeta creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    RDMA_LOG(ERROR) << "DataStoreMeta connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving hash metadata ----------------- */
  size_t hash_meta_size = (size_t)1024;
  char* recv_buf = (char*)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0) {
    RDMA_LOG(ERROR) << "DataStoreMeta receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  // Get number of meta
  node_id_t machine_id = *((node_id_t*)snooper);
  snooper += sizeof(machine_id);

  uint64_t data_store_ptr = *((uint64_t*)snooper);
  snooper += sizeof(data_store_ptr);

  offset_t base_off = *((offset_t*)snooper);
  snooper += sizeof(base_off);

  uint64_t page_num = *((uint64_t*)snooper);
  snooper += sizeof(page_num);

  size_t node_size = *((size_t*)snooper);
  snooper += sizeof(node_size);

  if (machine_id >= MAX_REMOTE_NODE_NUM) {
    RDMA_LOG(FATAL) << "remote machine id " << machine_id << " exceeds the max machine number";
  }
  // check eof
  uint64_t check = *((uint64_t*)snooper);
  assert(check == MEM_STORE_META_END);

  free(recv_buf);

  // 在这里初始化PageTableStore的类内成员变量
  manager_data_nodes.push_back(machine_id);
  data_node_frame_nums.push_back(page_num);

  return machine_id;
}

void PageTableStore::ReveiveMeta(){
  // Read config json file
  std::string config_filepath = "../../../config/memory_page_table_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  
  auto data_nodes = json_config.get("remote_data_node_nodes");
  auto remote_ips = data_nodes.get("remote_data_node_ips");
  auto remote_meta_ports = data_nodes.get("remote_data_node_meta_ports");  

  // Get remote data node meta via TCP
  for (size_t index = 0; index < remote_ips.size(); index++) {
    std::string remote_ip = remote_ips.get(index).get_str();
    int remote_meta_port = (int)remote_meta_ports.get(index).get_int64();
    RDMA_LOG(INFO) << "get data node meta from " << remote_ip << ", port: " << remote_meta_port;
    node_id_t remote_machine_id = GetDataStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      std::cerr << "Thread " << std::this_thread::get_id() << " GetDataStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    }
  }
  RDMA_LOG(INFO) << "All PageTableStore META received";
}

void PageTableStore::BuildConnectWithRingBuffer(){
  // Read config json file
  std::string config_filepath = "../../../config/memory_page_table_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_page_table_node");
  auto local_port = local_node.get("local_port").get_int64();
  auto local_client_port = local_node.get("local_client_port").get_int64();
  auto local_machine_id = local_node.get("machine_id").get_int64();

  // Using the first RNIC's first port
  RdmaCtrl::DevIdx idx;
  idx.dev_id = 0;
  idx.port_id = 1;
  
  global_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_client_port);
  // Open device
  opened_rnic = global_rdma_ctrl->open_device(idx);

  // Alloc Client RMDA buffer 
  size_t global_mr_size = PER_THREAD_ALLOC_SIZE * 0.2; // 100MB enough
  // Register a buffer to the previous opened device.
  char* global_mr = (char*)malloc(global_mr_size);
  memset(global_mr, 0, global_mr_size);
  RDMA_ASSERT(global_rdma_ctrl->register_memory(CLIENT_MR_ID, global_mr, global_mr_size, opened_rnic));

  rdma_buffer_allocator = new RDMABufferAllocator(global_mr, global_mr + global_mr_size - 1);

  // GetMRMeta
  while (QP::get_remote_mr("127.0.0.1", local_port, SERVER_PAGETABLE_RING_FREE_FRAME_BUFFER_ID, &local_ring_free_frame_buffer_mr) != SUCC) {
    usleep(2000);
  }
  while (QP::get_remote_mr("127.0.0.1", local_port, SERVER_PAGETABLE_ID, &local_page_table_mr) != SUCC) {
    usleep(2000);
  }
  // Build QPs with itself
  // ! 这里的local_mr 疑似被析构了? 
  MemoryAttr local_mr = global_rdma_ctrl->get_local_mr(CLIENT_MR_ID);
  //! 这里create_rc_idx(0, 0)存疑 
  connect_local_ring_qp = global_rdma_ctrl->create_rc_qp(create_rc_idx(local_machine_id, 10085), opened_rnic, &local_mr);
  connect_local_page_table_qp = global_rdma_ctrl->create_rc_qp(create_rc_idx(local_machine_id, 10086), opened_rnic, &local_mr);
  assert(connect_local_ring_qp != nullptr);
  assert(connect_local_page_table_qp != nullptr);
  ConnStatus rc;
  do {
    rc = connect_local_ring_qp->connect("127.0.0.1", local_port);
    if (rc == SUCC) {
      connect_local_ring_qp->bind_remote_mr(local_ring_free_frame_buffer_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
      RDMA_LOG(INFO) << "PageTable QP connected! with local Ring page free buffer";
    }
    usleep(2000);
  } while (rc != SUCC);
  do {
    rc = connect_local_page_table_qp->connect("127.0.0.1", local_port);
    if (rc == SUCC) {
      connect_local_page_table_qp->bind_remote_mr(local_page_table_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
      RDMA_LOG(INFO) << "PageTable QP connected! with local Page Table";
    }
    usleep(2000);
  }while (rc != SUCC);
  stop_victim = false;
}

void PageTableStore::FillFreeListThread() {
  while (stop_victim) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // TODO: 20000是一个可调的参数，可以根据实际情况调整
  while (true){ // 一直循环
    while (stop_victim) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::unique_lock<std::mutex> lock(victim_mutex);
    if(free_list_.size() > MAX_FREE_LIST_VICTIM_SIZE){
      cv.wait(lock);
    }

    for(int i=0; i<* fill_page_count; i++){
      char* cas_buf = rdma_buffer_allocator->Alloc(sizeof(lock_t));
      char* faa_buf = rdma_buffer_allocator->Alloc(sizeof(lock_t));

      offset_t offset = base_off + i * sizeof(PageTableNode);

      // ****************************************************************************************
      // 这里手写了一个RDMACAS + RDMARead的操作，因为这是memstore的操作，不在DTX中，因此无法使用DTX的接口
      // 这里需要注意，这里的RDMACAS因为和本地原子操作和RDMA原子操作不兼容，因此需要使用RDMACAS来加锁
      auto rc = connect_local_page_table_qp->post_cas(cas_buf, offset, UNLOCKED, EXCLUSIVE_LOCKED, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
        // return;
      }
      ibv_wc wc{};
      rc = connect_local_page_table_qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
        // return;
      }
      
      if( *(lock_t*)cas_buf == UNLOCKED){
        
        // std::cout << "lock page table id: " << i << std::endl;
        // 加锁成功
        PageTableNode* node = (PageTableNode*)( i * sizeof(PageTableNode) + page_table_ptr);
        timestamp_t min_timestamp = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() - 5;
        for(int j=0; j<MAX_PAGETABLE_ITEM_NUM_PER_NODE; j++){
          if(node->page_table_items[j].valid == true){
            // 该页面是有效页面
            if(node->page_table_items[j].rwcount == 0 && node->page_table_items[j].last_access_time < min_timestamp){
              // 该页面没有被修改，可以被替换
              // 将该页面放入空闲页面链表中
              // free_list_mutex_[node->page_table_items[j].page_address.node_id].lock();
              // free_list_[node->page_table_items[j].page_address.node_id].push_back(node->page_table_items[j].page_address.frame_id);
              // free_list_mutex_[node->page_table_items[j].page_address.node_id].unlock();

              free_list_mutex_.lock();
              free_list_.push_back({node->page_table_items[j].page_address.node_id, node->page_table_items[j].page_address.frame_id});
              free_list_mutex_.unlock();

              // 将该页面从页表中删除
              node->page_table_items[j].valid = false;

              // debug
              std::cout << "+++remove from page table: " << node->page_table_items[j].page_address.node_id << " " << node->page_table_items[j].page_address.frame_id << std::endl;
            }
          }
        }

        // 释放锁
        rc = connect_local_page_table_qp->post_faa(faa_buf, offset, EXCLUSIVE_UNLOCK_TO_BE_ADDED, IBV_SEND_SIGNALED);
        if (rc != SUCC) {
          RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "VictimPageThread";
          // return;
        }
        rc = connect_local_page_table_qp->poll_till_completion(wc, no_timeout);
        if (rc != SUCC) {
          RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
          // return;
        }
      }
    }
  }
}

void PageTableStore::VictimPageThread(){
  // 这里需要一个线程，不断的将超时页面放入空闲页面链表中
  std::thread victim_page_thread_([this]{
    FillFreeListThread();
  });

  while (stop_victim) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  while (true) {
    // 从空闲页面链表中取出空闲页面或者从页表中将超时的页面替换掉，放入环形缓冲区从head开始
    char* faa_cnt_buf = rdma_buffer_allocator->Alloc(sizeof(int64_t));
    char* faa_head_buf = rdma_buffer_allocator->Alloc(sizeof(uint64_t));
    auto rc = connect_local_ring_qp->post_faa(faa_cnt_buf, ring_buffer_item_num_off, 1, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "VictimPageThread";
    }
    ibv_wc wc{};
    rc = connect_local_ring_qp->poll_till_completion(wc, no_timeout);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
    }

    rc = connect_local_ring_qp->post_faa(faa_head_buf, ring_buffer_head_off, 1, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "VictimPageThread";
    }
    rc = connect_local_ring_qp->poll_till_completion(wc, no_timeout);
    // std::cout << "hcy debug: " << *(int64_t*)faa_cnt_buf << " " << *(int64_t*)faa_head_buf << std::endl; // 确定在这里faa值都已经返回了
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
    }
    // std::cout << "cnt_buf: " << *(int64_t*)faa_cnt_buf << "head_buf: " << *(int64_t*)faa_head_buf << std::endl;

    if(*(int64_t*)faa_cnt_buf >= MAX_FREE_LIST_BUFFER_SIZE -1){
      // buffer is full
      auto rc = connect_local_ring_qp->post_faa(faa_head_buf, ring_buffer_head_off, -1, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "VictimPageThread";
      }
      ibv_wc wc{};
      rc = connect_local_ring_qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
      }
      rc = connect_local_ring_qp->post_faa(faa_cnt_buf, ring_buffer_item_num_off, -1, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "VictimPageThread";
      }
      rc = connect_local_ring_qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "VictimPageThread";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    else{
      // buffer is not full
      // 从free list中取一个元素放入环形缓冲区
      free_list_mutex_.lock();
      // std::cout << "**** debug: " << *(int64_t*)faa_cnt_buf << " " << *(int64_t*)faa_head_buf << std::endl; // 确定在这里faa值都已经返回了
      uint64_t head = (*(uint64_t*)faa_head_buf) % MAX_FREE_LIST_BUFFER_SIZE;
      while (ring_free_frame_buffer_.free_list_buffer_[head].valid == true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      auto free_page = free_list_.front();
      ring_free_frame_buffer_.free_list_buffer_[head] = {free_page.first, free_page.second, true};
      std::cout << "free page: " << free_page.first << " " << free_page.second << "in head: " << head << std::endl;
      free_list_.pop_front();

      if(free_list_.size() < MAX_FREE_LIST_VICTIM_SIZE){
        std::unique_lock<std::mutex> lock(victim_mutex);
        cv.notify_one();
      }

      free_list_mutex_.unlock();
    }
  }
}

// PageAddress PageTableStore::LocalGetPageFrame(PageId page_id) {
//   uint64_t hash = GetHash(page_id);
//   short next_expand_node_id;
//   PageTableNode* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);
//   do {
//     for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
//       if (node->page_table_items[i].page_id == page_id && node->page_table_items[i].valid == true) {
//         // 返回帧号
//         return node->page_table_items[i].page_address;
//       }
//     }
//     // short begin with bucket_num
//     next_expand_node_id = node->next_expand_node_id[0];
//     node = (PageTableNode*)((bucket_num + next_expand_node_id) * sizeof(PageTableNode) + page_table_ptr);
//   } while( next_expand_node_id >= 0);
//   return {-1, INVALID_FRAME_ID};  // failed to found one
// }

// bool PageTableStore::LocalInsertPageTableItem(PageId page_id, PageAddress page_address, MemStoreReserveParam* param) {

//   PageAddress find_exits = LocalGetPageFrame(page_id);
//   // exits same key
//   if(find_exits.frame_id == INVALID_FRAME_ID) return false;

//   uint64_t hash = GetHash(page_id);
//   auto* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);

//   // Find
//   while (true) {
//     for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++){
//       if(node->page_table_items[i].valid == false){
//         node->page_table_items[i].page_id = page_id;
//         node->page_table_items[i].page_address = page_address;
//         node->page_table_items[i].valid = true;
//         return true;
//       }
//     }

//     if (node->next_expand_node_id[0] <= 0) break;
//     node = (PageTableNode*)((bucket_num + node->next_expand_node_id[0]) * sizeof(PageTableNode) + page_table_ptr);
//   }

//   // Allocate
//   // RDMA_LOG(INFO) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum/bucket: " << ITEM_NUM_PER_NODE;
//   assert((uint64_t)param->mem_store_reserve + param->mem_store_reserve_offset <= (uint64_t)param->mem_store_end);
//   auto* new_node = (PageTableNode*)(param->mem_store_reserve + param->mem_store_reserve_offset);
//   param->mem_store_reserve_offset += sizeof(PageTableNode);
//   memset(new_node, 0, sizeof(PageTableNode));
//   new_node->page_table_items[0].page_id = page_id;
//   new_node->page_table_items[0].page_address = page_address;
//   new_node->page_table_items[0].valid = true;
//   new_node->next_expand_node_id[0] = -1;
//   new_node->page_id = node_num;
//   node->next_expand_node_id[0] = node_num - bucket_num;
  
//   node_num++;
//   *fill_page_count = *fill_page_count + 1;
//   return true;
// }

// bool PageTableStore::LocalDeletePageTableItem(PageId page_id) {
//   uint64_t hash = GetHash(page_id);
//   auto* node = (PageTableNode*)(hash * sizeof(PageTableNode) + page_table_ptr);
//   short next_expand_node_id;
//   do{
//     for (int i=0; i<MAX_PAGETABLE_ITEM_NUM_PER_NODE; i++) {
//       if(node->page_table_items[i].page_id == page_id){ 
//         // find it 
//         node->page_table_items[i].valid = 0;
//         return true;
//       }
//     }
//     // short begin with bucket_num
//     next_expand_node_id = node->next_expand_node_id[0];
//     node = (PageTableNode*)((bucket_num + next_expand_node_id) * sizeof(PageTableNode) + page_table_ptr);
//   } while( next_expand_node_id >= 0);
//   return false;
// }
