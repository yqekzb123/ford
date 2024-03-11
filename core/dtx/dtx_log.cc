// Author: huangdund
// Copyright (c) 2023

#include <brpc/channel.h>
#include "dtx/dtx.h"
#include "storage/storage_service.pb.h"
#include "log/record.h"
#include "txn/batch_txn.h"

// DEFINE_string(log_protocol, "baidu_std", "Protocol type");
// DEFINE_string(log_connection_type, "", "Connection type. Available values: single, pooled, short");
// DEFINE_string(log_server, "127.0.0.1:12348", "IP address of server");
// DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
// DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
// DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

static void LogOnRPCDone(storage_service::LogWriteResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<storage_service::LogWriteResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        // RPC失败了. response里的值是未定义的，勿用。
        LOG(ERROR) << "Fail to send log: " << cntl->ErrorText();
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理。
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}
 

// 发送日志到存储层
void DTX::SendLogToStoragePool(uint64_t bid, brpc::CallId* cid){
    storage_service::StorageService_Stub stub(storage_log_channel);
    brpc::Controller* cntl = new brpc::Controller();
    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse* response = new storage_service::LogWriteResponse();

    batch_txn_log.batch_id_ = bid;

    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(bid, global_meta_man->local_machine_id, tx_id);
    batch_txn_log.logs.push_back(batch_end_log);

    request.set_log(batch_txn_log.get_log_string());

    // 在这里改成异步发送
    *cid = cntl->call_id();

    stub.LogWrite(cntl, &request, response, brpc::NewCallback(LogOnRPCDone, response, cntl));

    // ! 在程序外部同步
}

void DTX::SendLogToStoragePool(uint64_t bid){
    batch_txn_log.batch_id_ = bid;
    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(bid, global_meta_man->local_machine_id, tx_id);
    batch_txn_log.logs.push_back(batch_end_log);

    node_id_t node_id = global_meta_man->GetStorageNodeID();
    RCQP* qp = thread_qp_man->GetRemoteStorageQPWithNodeID(node_id);

    char* faa_cnt_buf = thread_rdma_buffer_alloc->Alloc(sizeof(int64_t));
    char* faa_head_buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
    
    auto log_base_off = global_meta_man->GetStorageLogBase(node_id);
    auto log_head_off = global_meta_man->GetStorageHead(node_id);
    auto log_cnt_off = global_meta_man->GetStorageCnt(node_id);

    // 试探性faa cnt and head
    std::string log_string = batch_txn_log.get_log_string();
    size_t write_size = log_string.size();
    char* log_data = thread_rdma_buffer_alloc->Alloc(write_size);
    memcpy(log_data, log_string.c_str(), write_size);

    while(true){
        auto rc = qp->post_faa(faa_cnt_buf, log_cnt_off, write_size, IBV_SEND_SIGNALED);
        if (rc != SUCC) {
            RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "SendLog";
        }
        rc = qp->post_faa(faa_head_buf, log_head_off, write_size, IBV_SEND_SIGNALED);
        if (rc != SUCC) {
            RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << "SendLog";
        }
        ibv_wc wc{};
        rc = qp->poll_till_completion(wc, no_timeout);
        if (rc != SUCC) {
            RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "SendLog";
        }
        rc = qp->poll_till_completion(wc, no_timeout);
        if (rc != SUCC) {
            RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "SendLog";
        }

        if(*(int64_t*)faa_cnt_buf >= RDMA_LOG_BUFFER_SIZE - PAGE_SIZE){
            // 这里是不能往里写入的，因为单线程刷盘，这里还不能确定已经刷盘，因此可能会覆盖掉之前的日志
            auto rc = qp->post_faa(faa_cnt_buf, log_cnt_off, -write_size, IBV_SEND_SIGNALED);
            if (rc != SUCC) {
                RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "SendLog";
            }
            rc = qp->post_faa(faa_head_buf, log_head_off, -write_size, IBV_SEND_SIGNALED);
            if (rc != SUCC) {
                RDMA_LOG(ERROR) << "client: post faa fail. rc=" << rc << "SendLog";
            }
            ibv_wc wc{};
            rc = qp->poll_till_completion(wc, no_timeout);
            if (rc != SUCC) {
                RDMA_LOG(ERROR) << "client: poll faa fail. rc=" << rc << "SendLog";
            }
            rc = qp->poll_till_completion(wc, no_timeout);
            if (rc != SUCC) {
                RDMA_LOG(ERROR) << "client: poll faa fail. rc=" << rc << "SendLog";
            }
            continue;
        }else{
            auto rc = qp->post_send(IBV_WR_RDMA_WRITE, log_data, write_size, 
                log_base_off + (*(int64_t*)faa_head_buf) % RDMA_LOG_BUFFER_SIZE, IBV_SEND_SIGNALED);
            if (rc != SUCC) {
                RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << "SendLog";
            }
            ibv_wc wc{};
            rc = qp->poll_till_completion(wc, no_timeout);
            if (rc != SUCC) {
                RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << "SendLog";
            }
            break;
        }
    }
}
