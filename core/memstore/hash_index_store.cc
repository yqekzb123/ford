#include "hash_index_store.h"

Rid IndexStore::LocalGetIndexRid(itemkey_t key) {
  uint64_t hash = GetHash(key);
  short next_expand_node_id;
  IndexNode* node = (IndexNode*)(hash * sizeof(IndexNode) + index_ptr);
  do {
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
      if (node->index_items[i].key == key && node->index_items[i].valid == true) {
        return node->index_items[i].rid;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (IndexNode*)(next_expand_node_id * sizeof(IndexNode) + expand_region_base_ptr);
  } while( next_expand_node_id >= 0);
  return {INVALID_PAGE_ID, -1};  // failed to found one
}

bool IndexStore::LocalInsertKeyRid(itemkey_t key, const Rid& rid, MemStoreReserveParam* param) {

  Rid find_exits = LocalGetIndexRid(key);
  // exits same key
  if(find_exits.page_no_ != INVALID_PAGE_ID) return false;

  uint64_t hash = GetHash(key);
  auto* node = (IndexNode*)(hash * sizeof(IndexNode) + index_ptr);

  // Find
  while (true) {
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++){
      if(node->index_items[i].valid == false){
        node->index_items[i].key = key;
        node->index_items[i].rid = rid;
        node->index_items[i].valid = true;
        return true;
      }
    }

    if (node->next_expand_node_id[0] <= 0) break;
    node = (IndexNode*)(node->next_expand_node_id[0] * sizeof(IndexNode) + expand_region_base_ptr);
  }

  // Allocate
  // RDMA_LOG(INFO) << "Table " << table_id << " alloc a new bucket for key: " << key << ". Current slotnum/bucket: " << ITEM_NUM_PER_NODE;
  assert((uint64_t)param->mem_store_reserve + param->mem_store_reserve_offset + sizeof(IndexNode) <= (uint64_t)param->mem_store_end);
  auto* new_node = (IndexNode*)(param->mem_store_reserve + param->mem_store_reserve_offset);
  param->mem_store_reserve_offset += sizeof(IndexNode);
  memset(new_node, 0, sizeof(IndexNode));
  new_node->index_items[0].key = key;
  new_node->index_items[0].rid = rid;
  new_node->index_items[0].valid = true;
  new_node->next_expand_node_id[0] = -1;
  new_node->next_expand_node_id[1] = -1;
  new_node->next_expand_node_id[2] = -1;
  new_node->next_expand_node_id[3] = -1;
  new_node->next_expand_node_id[4] = -1;
  new_node->page_id = node_num;
  // node->next_expand_node_id[0] = node_num - bucket_num;
  node->next_expand_node_id[0] = param->mem_store_reserve_offset / sizeof(IndexNode) - 1;
  
  node_num++;
  return true;
}

bool IndexStore::LocalDelete(itemkey_t key) {
  uint64_t hash = GetHash(key);
  auto* node = (IndexNode*)(hash * sizeof(IndexNode) + index_ptr);
  short next_expand_node_id;
  do{
    for (int i=0; i<MAX_RIDS_NUM_PER_NODE; i++) {
      if(node->index_items[i].key == key){ 
        // find it 
        node->index_items[i].valid = 0;
        return true;
      }
    }
    // short begin with bucket_num
    next_expand_node_id = node->next_expand_node_id[0];
    node = (IndexNode*)(next_expand_node_id * sizeof(IndexNode) + expand_region_base_ptr);
  } while( next_expand_node_id >= 0);
  return false;
}