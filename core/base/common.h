// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <cstddef>  // For size_t
#include <cstdint>  // For uintxx_t

#include "flags.h"

// Global specification
using tx_id_t = uint64_t;     // Transaction id type
using t_id_t = uint32_t;      // Thread id type
using coro_id_t = int;        // Coroutine id type
using node_id_t = int;        // Machine id type
using mr_id_t = int;          // Memory region id type
using table_id_t = uint64_t;  // Table id type
using itemkey_t = uint64_t;   // Data item key type, used in DB tables
using offset_t = int64_t;     // Offset type. Usually used in remote offset for RDMA
using version_t = uint64_t;   // Version type, used in version checking
using lock_t = uint64_t;      // Lock type, used in remote locking
using lsn_t = uint64_t;       // log sequence number, used for storage_node log storage
using page_id_t = int32_t;    // page id type
using frame_id_t = int32_t;  // frame id type
using batch_id_t = uint64_t;  // batch id type
using slot_offset_t = size_t; // slot offset type
using timestamp_t = int32_t;  // timestamp type, used for transaction concurrency

#define PAGE_SIZE 4096
#define BUFFER_POOL_SIZE 65536 // 256MB

#define LOG_FILE_NAME "LOG_FILE"                                
static constexpr int LOG_REPLAY_BUFFER_SIZE = (10 * PAGE_SIZE);                    // size of a log buffer in byte

// Memory region ids for server's hash store buffer and undo log buffer
// const mr_id_t SERVER_HASH_BUFF_ID = 97;
// const mr_id_t SERVER_LOG_BUFF_ID = 98;
const mr_id_t SERVER_HASH_INDEX_ID = 99;
const mr_id_t SERVER_LOCK_TABLE_ID = 101;
const mr_id_t SERVER_DATA_ID = 102;
const mr_id_t SERVER_PAGETABLE_ID = 103;
const mr_id_t SERVER_PAGETABLE_RING_FREE_FRAME_BUFFER_ID = 104; // !提示：这里还没注册呢

// Memory region ids for client's local_mr
const mr_id_t CLIENT_MR_ID = 100;

// Indicating that memory store metas have been transmitted
const uint64_t MEM_STORE_META_END = 0xE0FF0E0F;

// Node and thread conf
#define BACKUP_DEGREE 2          // Backup memory node number. MUST **NOT** BE SET TO 0
#define MAX_REMOTE_NODE_NUM 100  // Max remote memory node number
#define MAX_DB_TABLE_NUM 15      // Max DB tables
#define MAX_FREE_LIST_BUFFER_SIZE 20480 // 20480*4k = 80M
// Data state
#define STATE_INVISIBLE 0x8000000000000000  // Data cannot be read
#define STATE_LOCKED 1                      // Data cannot be written. Used for serializing transactions
#define STATE_CLEAN 0

// Alias
#define Aligned8 __attribute__((aligned(8)))
#define Aligned4096 __attribute__((aligned(4096)))
#define ALWAYS_INLINE inline __attribute__((always_inline))
#define TID (std::this_thread::get_id())

// Helpful for improving condition prediction hit rate
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)

// invalid value for common identifiers
#define INVALID_LSN -1
#define INVALID_TXN_ID -1
#define INVALID_NODE_ID -1
#define INVALID_BATCH_ID 0
#define INVALID_FRAME_ID -1
#define INVALID_PAGE_ID -1
#define INVALID_TABLE_ID -1

#define PAGE_NO_RM_FILE_HDR 0
#define OFFSET_PAGE_HDR 0
#define OFFSET_NUM_PAGES 4
#define OFFSET_FIRST_FREE_PAGE_NO 12
#define OFFSET_NUM_RECORDS 4
#define OFFSET_NEXT_FREE_PAGE_NO 0
#define OFFSET_BITMAP 8

#define REPLACER_TYPE "LRU"

#define RM_MAX_RECORD_SIZE 512
#define RM_FIRST_RECORD_PAGE 1
#define RM_FILE_HDR_PAGE 0
#define RM_NO_PAGE -1

#define WORKER_EXE_LOCAL_TXN_CNT 10

static constexpr int INVALID_TIMESTAMP = -1;                                  // invalid transaction timestamp
static constexpr int HEADER_PAGE_ID = 0;                                      // the header page id
// static constexpr int LOG_BUFFER_SIZE = (1024 * PAGE_SIZE);                    // size of a log buffer in byte
// static constexpr int BUCKET_SIZE = 50;                                        // size of extendible hash bucket

// 定义哈希桶next指针数组大小
const int NEXT_NODE_COUNT = 5;

static constexpr uint64_t EXCLUSIVE_LOCKED = 0xFF00000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS = 0xFF00000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF; // -1


static constexpr uint64_t ADDR_HASH_BUCKET_NUM = 20000; 


/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) __sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) __sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) __sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) __sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) __sync_sub_and_fetch(&(dest), value)
