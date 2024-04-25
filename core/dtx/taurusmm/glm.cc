#include "glm.h"
#include <iostream>

// 加锁成功返回-1，失败返回失败的请求的位置
int GLM::GLMLockShared(std::vector<glmlock_request> requests) {
  for (int i=0; i<requests.size(); i++) {
    auto localdata = glm_lock_store.GetLock(requests[i].table_id, requests[i].page_id);
    bool success = localdata->LockShared();
    if (!success) {
      // std::cout << "Lock failed: " << requests[i].table_id << requests[i].page_id << std::endl;
      return i;
    }
  }
  return -1;
}

int GLM::GLMLockExclusive(std::vector<glmlock_request> requests) {
  for (int i=0; i<requests.size(); i++) {
    auto localdata = glm_lock_store.GetLock(requests[i].table_id, requests[i].page_id);
    bool success = localdata->LockExclusive();
    if (!success) {
      // std::cout << "Lock failed: " << requests[i].table_id << requests[i].page_id << std::endl;
      return i;
    }
  }
  return -1;
}

bool GLM::GLMUnLockShared(std::vector<glmlock_request> requests) {
  for (auto& item : requests) {
    auto localdata = glm_lock_store.GetLock(item.table_id, item.page_id);
    localdata->UnlockShared();
  }
  return true;
}

bool GLM::GLMUnLockExclusive(std::vector<glmlock_request> requests) {
  for (auto& item : requests) {
    auto localdata = glm_lock_store.GetLock(item.table_id, item.page_id);
    localdata->UnlockExclusive();
  }
  return true;
}