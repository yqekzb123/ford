#include "rdma_log_buffer.h"
#include "util/json_config.h"

void StorageRDMAMemoryManager::BuildConnectWithLogBuffer(){
  // Read config json file
  std::string config_filepath = "../../../config/storage_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_storage_node");
  auto local_port = local_node.get("local_rdma_port").get_int64();
  auto local_client_port = local_node.get("local_client_port").get_int64();
  auto local_machine_id = local_node.get("machine_id").get_int64();

  // Using the first RNIC's first port
  RdmaCtrl::DevIdx idx;
  idx.dev_id = 0;
  idx.port_id = 1;

  RDMA_LOG(INFO) << "Start initializing RDMA...";
  server_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_port);
  server_rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(
      server_rdma_ctrl->register_memory(SERVER_STORAGE_ID, this->GetMemRegion(), 
        sizeof(StorageMemoryRegion), server_rdma_ctrl->get_device()) == true);
  RDMA_LOG(INFO) << "Register memory success!";

  client_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_client_port);
  // Open device
  opened_rnic = client_rdma_ctrl->open_device(idx);

  // Alloc Client RMDA buffer 
  size_t global_mr_size = PER_THREAD_ALLOC_SIZE * 0.2; // 100MB enough
  // Register a buffer to the previous opened device.
  char* global_mr = (char*)malloc(global_mr_size);
  memset(global_mr, 0, global_mr_size);
  RDMA_ASSERT(client_rdma_ctrl->register_memory(STORAGE_CLINT_ID, global_mr, global_mr_size, opened_rnic));

  rdma_buffer_allocator = new RDMABufferAllocator(global_mr, global_mr + global_mr_size - 1);

  // GetMRMeta
  while (QP::get_remote_mr("127.0.0.1", local_port, SERVER_STORAGE_ID, &local_log_buffer_mr) != SUCC) {
    usleep(2000);
  }
  // Build QPs with itself
  // ! 这里的local_mr 疑似被析构了? 
  MemoryAttr local_mr = client_rdma_ctrl->get_local_mr(STORAGE_CLINT_ID);
  //! 这里create_rc_idx(0, 0)中10010为存储节点特殊标记 
  connect_log_buffer_qp = client_rdma_ctrl->create_rc_qp(create_rc_idx(local_machine_id, 10010), opened_rnic, &local_mr);
    assert(connect_log_buffer_qp != nullptr);
  ConnStatus rc;
  do {
    rc = connect_log_buffer_qp->connect("127.0.0.1", local_port);
    if (rc == SUCC) {
      connect_log_buffer_qp->bind_remote_mr(local_log_buffer_mr);  // Bind the hash mr as the default remote mr for convenient parameter passing
      RDMA_LOG(INFO) << "LogBuffer QP connected! with local Log ring buffer";
    }
    usleep(2000);
  } while (rc != SUCC);
}

void StorageRDMAMemoryManager::AsyncFlush() {
  char* faa_cnt_buf = rdma_buffer_allocator->Alloc(sizeof(int64_t));
  char* faa_tail_buf = rdma_buffer_allocator->Alloc(sizeof(uint64_t));
  while (true) {
    // log_cnt - PAGE_SIZE
    auto rc = connect_log_buffer_qp->post_faa(faa_cnt_buf, log_cnt_off, -PAGE_SIZE, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "AsyncFlush";
    }
    rc = connect_log_buffer_qp->post_faa(faa_tail_buf, log_tail_off, PAGE_SIZE, IBV_SEND_SIGNALED);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "AsyncFlush";
    }

    ibv_wc wc{};
    rc = connect_log_buffer_qp->poll_till_completion(wc, no_timeout);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll faa fail. rc=" << rc << "AsyncFlush";
    }
    rc = connect_log_buffer_qp->poll_till_completion(wc, no_timeout);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll faa fail. rc=" << rc << "AsyncFlush";
    }

    // ! 这里有一个assumption，因为上层计算节点是多线程的faa，但faa之后可能还没有写入数据
    // ! 因此不能faa_cnt的原值大于PageSize就认为可以写入磁盘
    // ! 在这里可以设置，必须原值是10*PageSize才能写入磁盘

    if(*(int64_t*)faa_cnt_buf <= PAGE_SIZE * 10){
      // buffer has not 4K data
      auto rc = connect_log_buffer_qp->post_faa(faa_cnt_buf, log_cnt_off, PAGE_SIZE, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "AsyncFlush";
      }
      rc = connect_log_buffer_qp->post_faa(faa_tail_buf, log_tail_off, -PAGE_SIZE, IBV_SEND_SIGNALED);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "AsyncFlush";
      }
      ibv_wc wc{};
      rc = connect_log_buffer_qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll faa fail. rc=" << rc << "AsyncFlush";
      }
      rc = connect_log_buffer_qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll faa fail. rc=" << rc << "AsyncFlush";
      }
      // RDMA_LOG(INFO) << "log buffer has not 4K data, sleep 1ms";
      usleep(1000); // sleep 1ms
    }
    else{
      // write a page to disk
      char* write_data = log_buffer_->buffer_ + (log_buffer_->tail_  % RDMA_LOG_BUFFER_SIZE);
      log_manager_->write_batch_log_to_disk(write_data, PAGE_SIZE);
    }
  }
}