// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"
#include "util/latency.h"
#include "memstore/addr_store.h"

bool DTX::GetPageAddr(page_id_t id) {
  // If the addr is cached but it is from primary, this impl still reads backup

  node_id_t remote_node_id = global_meta_man->GetPageAddrNodeID(id);

  RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
  // todo: 实现一个以page为粒度处理的本地地址缓存
  auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
  if (offset != NOT_FOUND) {
    // Find the addr in local addr cache
    // hit_local_cache_times++;
    it->remote_offset = offset;
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    pending_direct_ro.emplace_back(DirectRead{.qp = qp, .item = &item, .buf = data_buf, .remote_node = remote_node_id});
    if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
      return false;
    }
  } else {
    // Local cache does not have
    if (!GetRemotePageAddr(id)) {
      return false;
    }
  }
  return true;
}

// 异步的读取页面地址函数
bool DTX::GetRemotePageAddr(page_id_t id) {
  // If the addr is cached but it is from primary, this impl still reads backup

  node_id_t remote_node_id = global_meta_man->GetPageAddrNodeID(id);

  RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
  
  uint64_t bucket_num = global_meta_man->GetPageAddrTableBucketNumWithNodeID(remote_node_id);
  uint64_t idx = MurmurHash64A(id, 0xdeadbeef) % bucket_num;
  offset_t node_off = sizeof(uint64_t) + idx * sizeof(AddrNode);

  char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(AddrNode));
  if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off, sizeof(AddrNode))) {
    return false;
  }
  return true;
}
