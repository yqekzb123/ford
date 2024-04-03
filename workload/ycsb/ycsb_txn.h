// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "ycsb/ycsb_db.h"

#include "worker/global.h"
#include "bench_dtx.h"

/******************** The business logic (Transaction) start ********************/
#define QUERY_CNT 10

enum YCSBTxnType {
    TypeRW,
    TypeScan,
    TypeInsert
};

struct RW {
    DataItemPtr obj[QUERY_CNT];
};

struct Scan {
};

struct Insert {
};

class YCSBDTX : public BenchDTX {
public:
    // !本地执行部分：这个函数中只生成事务，并在本地执行
    bool TxLocalRW(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    // bool TxLocalScan(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    // bool TxLocalInsert(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

    // !batch执行部分：当batch执行完，进行本地计算和处理逻辑
    bool TxReCaculateRW(coro_yield_t& yield);
    // bool TxReCaculateScan(coro_yield_t& yield);
    // bool TxReCaculateInsert(coro_yield_t& yield);
    /******************** The batched business logic (Transaction) end ********************/

    bool TxRW(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    bool TxScan(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    bool TxInsert(YCSB* ycsb_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /******************** The business logic (Transaction) end ********************/

public:
    RW r1;
    Scan s1;
    Insert i1;
    
    YCSBTxnType type;
    bool TxReCaculate(coro_yield_t& yield) {
        switch (type)
        {
        case TypeRW:
            return TxReCaculateRW(yield);
        case TypeScan:
            // return TxReCaculateScan(yield);
        case TypeInsert:
            // return TxReCaculateInsert(yield);
        default:
            break;
        }
    }
    bool StatCommit() {
        // thread_local_commit_times[uint64_t(type)]++;
    }
};

/******************** The business logic (Transaction) end ********************/