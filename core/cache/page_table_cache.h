// Author: huangdund
// Copyright (c) 2024

#pragma once

#include <map>
#include <unordered_map>

#include "base/common.h"
#include "base/page.h"

const offset_t PAGE_ITEM_NOT_FOUND = -1;

// For fast remote address lookup
class PageTableCache {
 public:
  void Insert(node_id_t remote_node_id, PageId page_id, offset_t remote_offset) {
    auto node_search = addr_map.find(remote_node_id);
    if (node_search == addr_map.end()) {
      // There is no such node. Init the node and table
      addr_map[remote_node_id] = std::unordered_map<PageId, offset_t>();
    }
    // The node and table both exist, then insert/update the <key,offset> pair
    addr_map[remote_node_id][page_id] = remote_offset;
  }

  // We know which node to read, but we do not konw whether it is cached before
  offset_t Search(node_id_t remote_node_id, PageId page_id) {
    auto node_search = addr_map.find(remote_node_id);
    if (node_search == addr_map.end()) return PAGE_ITEM_NOT_FOUND;
    auto pageid_search = node_search->second.find(page_id);
    if (pageid_search == node_search->second.end()) return PAGE_ITEM_NOT_FOUND;
    else return pageid_search->second;
  }

  size_t TotalAddrSize() {
    size_t total_size = 0;
    for (auto it = addr_map.begin(); it != addr_map.end(); it++) {
      total_size += sizeof(node_id_t);
      for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
        total_size += sizeof(table_id_t);
      }
    }

    return total_size;
  }

 private:
  std::unordered_map<node_id_t, std::unordered_map<PageId, offset_t>> addr_map; //三层嵌套的地址缓存，节点、表、数据项
};