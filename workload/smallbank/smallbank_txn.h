// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <memory>

#include "worker/global.h"
#include "dtx/dtx.h"
#include "bench_dtx.h"
#include "smallbank/smallbank_db.h"

/******************** The business logic (Transaction) start ********************/

struct Amalgamate {
    // LVersionPtr sav_obj_0;
    // LVersionPtr chk_obj_0;
    // LVersionPtr chk_obj_1;
    DataItemPtr sav_obj_0;
    DataItemPtr chk_obj_0;
    DataItemPtr chk_obj_1;
};

struct Balance {
    DataItemPtr sav_obj;
    DataItemPtr chk_obj;
};

struct DepositChecking {
    DataItemPtr chk_obj;
};

struct SendPayment {
    DataItemPtr chk_obj_0;
    DataItemPtr chk_obj_1;
};

struct TransactSaving {
    DataItemPtr sav_obj;
};

struct WriteCheck {
    DataItemPtr sav_obj;
    DataItemPtr chk_obj;
};

enum SmallBankTxnType {
    TypeAmalgamate,
    TypeBalance,
    TypeDepositChecking,
    TypeSendPayment,
    TypeTransactSaving,
    TypeWriteCheck
};

class SmallBankDTX : public BenchDTX {
public:
    // !本地执行部分：这个函数中只生成事务，并在本地执行
    bool TxLocalAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Calculate the sum of saving and checking kBalance */
    bool TxLocalBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Add $1.3 to acct_id's checking account */
    bool TxLocalDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool TxLocalSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Add $20 to acct_id's saving's account */
    bool TxLocalTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool TxLocalWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);

    // !batch执行部分：当batch执行完，进行本地计算和处理逻辑
    bool TxReCaculateAmalgamate(coro_yield_t& yield);
    /* Calculate the sum of saving and checking kBalance */
    bool TxReCaculateBalance(coro_yield_t& yield);
    /* Add $1.3 to acct_id's checking account */
    bool TxReCaculateDepositChecking(coro_yield_t& yield);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool TxReCaculateSendPayment(coro_yield_t& yield);
    /* Add $20 to acct_id's saving's account */
    bool TxReCaculateTransactSaving(coro_yield_t& yield);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool TxReCaculateWriteCheck(coro_yield_t& yield);
    /******************** The batched business logic (Transaction) end ********************/

    bool TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Calculate the sum of saving and checking kBalance */
    bool TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Add $1.3 to acct_id's checking account */
    bool TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Add $20 to acct_id's saving's account */
    bool TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
    /******************** The business logic (Transaction) end ********************/

public:
    Amalgamate a1;
    Balance b1;
    DepositChecking d1;
    SendPayment s1;
    TransactSaving t1;
    WriteCheck w1;
    
    SmallBankTxnType type;
    bool TxReCaculate(coro_yield_t& yield) {
        switch (type)
        {
        case TypeAmalgamate:
            return TxReCaculateAmalgamate(yield);
        case TypeBalance:
            return TxReCaculateBalance(yield);
        case TypeDepositChecking:
            return TxReCaculateDepositChecking(yield);
        case TypeSendPayment:
            return TxReCaculateSendPayment(yield);
        case TypeTransactSaving:
            return TxReCaculateTransactSaving(yield);
        case TypeWriteCheck:
            return TxReCaculateWriteCheck(yield);
        default:
            break;
        }
        return false;
    }
    bool StatCommit() {
        // thread_local_commit_times[uint64_t(type)]++;
        return true;
    }
};
