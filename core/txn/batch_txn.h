#pragma once

#include <deque>

#include "base/common.h"
#include "log/log_record.h"
#include "util/debug.h"

class BatchTxnLog {
public:
    BatchTxnLog(){}
    BatchTxnLog(batch_id_t batch_id) {
        batch_id_ = batch_id;
    }
    batch_id_t batch_id_;

    std::deque<LogRecord*> logs;

    std::string get_log_string(){
        std::string str = "";
        for(auto log: logs) {
            char* log_str = new char[log->log_tot_len_];
            log->serialize(log_str);
            // log->format_print();
            str += std::string(log_str, log->log_tot_len_);
            delete[] log_str;
        }
        RDMA_LOG(INFO) << "transaction " << batch_id_ << "'s log: " << str;
        return str;
    }

};