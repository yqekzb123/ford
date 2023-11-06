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

// Memory region ids for server's hash store buffer and undo log buffer
const mr_id_t SERVER_HASH_BUFF_ID = 97;
const mr_id_t SERVER_LOG_BUFF_ID = 98;

// Memory region ids for client's local_mr
const mr_id_t CLIENT_MR_ID = 100;

// Indicating that memory store metas have been transmitted
const uint64_t MEM_STORE_META_END = 0xE0FF0E0F;

// Node and thread conf
#define BACKUP_DEGREE 2          // Backup memory node number. MUST **NOT** BE SET TO 0
#define MAX_REMOTE_NODE_NUM 100  // Max remote memory node number
#define MAX_DB_TABLE_NUM 15      // Max DB tables

// Data state
#define STATE_INVISIBLE 0x8000000000000000  // Data cannot be read
#define STATE_LOCKED 1                      // Data cannot be written. Used for serializing transactions
#define STATE_CLEAN 0

// Alias
#define Aligned8 __attribute__((aligned(8)))
#define ALWAYS_INLINE inline __attribute__((always_inline))
#define TID (std::this_thread::get_id())

// Helpful for improving condition prediction hit rate
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)


using frame_id_t = int32_t;  // frame id type, 帧页ID, 页在BufferPool中的存储单元称为帧,一帧对应一页
using page_id_t = int32_t;   // page id type , 页ID
using txn_id_t = int32_t;    // transaction id type
using lsn_t = int32_t;       // log sequence number type
using slot_offset_t = size_t;  // slot offset type
using oid_t = uint16_t;
using timestamp_t = int32_t;  // timestamp type, used for transaction concurrency


static constexpr int INVALID_FRAME_ID = -1;                                   // invalid frame id
static constexpr int INVALID_PAGE_ID = -1;                                    // invalid page id
static constexpr int INVALID_TXN_ID = -1;                                     // invalid transaction id
static constexpr int INVALID_TIMESTAMP = -1;                                  // invalid transaction timestamp
static constexpr int INVALID_LSN = -1;                                        // invalid log sequence number
static constexpr int HEADER_PAGE_ID = 0;                                      // the header page id
static constexpr int PAGE_SIZE = 4096;                                        // size of a data page in byte  4KB
static constexpr int BUFFER_POOL_SIZE = 65536;                                // size of buffer pool 256MB
// static constexpr int BUFFER_POOL_SIZE = 262144;                                // size of buffer pool 1GB
// static constexpr int LOG_BUFFER_SIZE = (1024 * PAGE_SIZE);                    // size of a log buffer in byte
// static constexpr int BUCKET_SIZE = 50;                                        // size of extendible hash bucket

// 定义哈希桶next指针数组大小
const int NEXT_NODE_COUNT = 1;

static constexpr uint64_t EXCLUSIVE_LOCKED = 0xFF00000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS = 0xFF00000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF; // -1


static constexpr uint64_t ADDR_HASH_BUCKET_NUM = 20000; 