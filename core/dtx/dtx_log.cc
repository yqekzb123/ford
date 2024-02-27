// Author: huangdund
// Copyright (c) 2023

#include <brpc/channel.h>
#include "dtx/dtx.h"
#include "storage/storage_service.pb.h"
#include "log/record.h"
#include "txn/batch_txn.h"

DEFINE_string(log_protocol, "baidu_std", "Protocol type");
DEFINE_string(log_connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(log_server, "127.0.0.1:12348", "IP address of server");
// DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
// DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
// DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

// 发送日志到存储层
void DTX::SendLogToStoragePool(uint64_t bid){
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.use_rdma = false;
    options.protocol = FLAGS_log_protocol;
    options.connection_type = FLAGS_log_connection_type;
    options.timeout_ms = 0x7fffffff;
    options.max_retry = 3;
    if (channel.Init(FLAGS_log_server.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }
    storage_service::StorageService_Stub stub(&channel);
    brpc::Controller cntl;
    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse response;

    batch_txn_log.batch_id_ = bid;

    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(bid, global_meta_man->local_machine_id, tx_id);
    batch_txn_log.logs.push_back(batch_end_log);

    request.set_log(batch_txn_log.get_log_string());

    stub.LogWrite(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to send log: " << cntl.ErrorText();
        return;
    }
    // LOG(INFO) << "Received response from " << cntl.remote_side()
    //           << " to " << cntl.local_side()
    //           << ": " << response.ShortDebugString();
}