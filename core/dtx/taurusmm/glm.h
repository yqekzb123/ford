// author: huangchunyue
// 全局锁管理器, 锁的粒度：数据页
#pragma once
#include "base/common.h"
#include <unordered_map>
#include <vector>

class GLMLock{
public:
    table_id_t table_id;
    page_id_t page_id;
    lock_t lock; // 读写锁
    GLMLock() {
        lock = 0;
    }

    GLMLock(table_id_t tid, page_id_t pid) {
        table_id = tid;
        page_id = pid;
        lock = 0;
    }

    bool LockShared() {
        lock_t oldlock = lock;
        lock_t newlock = oldlock + 1;
        
        bool result = ATOM_CAS(lock, oldlock, newlock);
        return result;
    }

    bool LockExclusive() {
        bool result =  ATOM_CAS(lock, UNLOCKED, EXCLUSIVE_LOCKED);
        return result;
    }

    bool UnlockShared() {
        ATOM_SUB_FETCH(lock,1);
    }

    bool UnlockExclusive() {
        lock = UNLOCKED;
    }
};

using GLMLockTable = std::unordered_map<page_id_t, GLMLock*>;

class GLMLockStore{ 
public:  
    GLMLockStore(){
        glm_lock.clear();
    }
    
    GLMLock* GetLock(table_id_t table_id, page_id_t page_id) {
        GLMLock* lock = nullptr;
        GLMLockTable table = glm_lock[table_id];
        lock = table[page_id];
        if (lock == nullptr) {
            // 如果data不存在，则自动创建一个临时的
            lock = new GLMLock(table_id, page_id);
            table.insert(std::make_pair(page_id,lock));
        }
        return lock;
    }

private:
    std::unordered_map<table_id_t,GLMLockTable> glm_lock;
};

struct glmlock_request {
    table_id_t table_id;
    page_id_t page_id;
};

class GLM {
public:
    GLM() {
        glm_lock_store = GLMLockStore();
    }

    int GLMLockShared(std::vector<glmlock_request> requests);
    int GLMLockExclusive(std::vector<glmlock_request> requests);
    bool GLMUnLockShared(std::vector<glmlock_request> requests);
    bool GLMUnLockExclusive(std::vector<glmlock_request> requests);

private:
    GLMLockStore glm_lock_store;
};