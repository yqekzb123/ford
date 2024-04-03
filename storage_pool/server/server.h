#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <brpc/channel.h>

#include "rlib/rdma_ctrl.hpp"
#include "storage/storage_rpc.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
#include "storage/rdma_log_buffer.h"

// Load DB
#include "micro/micro_db.h"
#include "smallbank/smallbank_db.h"
#include "tatp/tatp_db.h"
#include "tpcc/tpcc_db.h"
#include "ycsb/ycsb_db.h"

class Server {
public:
    Server(int machine_id, int local_rdma_port, int local_rpc_port, int local_meta_port, bool use_rdma, int compute_node_num,
            DiskManager* disk_manager, LogManager* log_manager, StorageRDMAMemoryManager* rdma_mem_manager): 
          disk_manager_(disk_manager), log_manager_(log_manager), rdma_mem_manager_(rdma_mem_manager), compute_node_num_(compute_node_num),
          machine_id_(machine_id), local_rdma_port_(local_rdma_port_), local_rpc_port_(local_rpc_port), local_meta_port_(local_meta_port){   
         
        // std::thread rpc_thread([&]{
            //启动事务brpc server
            brpc::Server server;

            storage_service::StoragePoolImpl storage_pool_impl(log_manager_, disk_manager_);
            if (server.AddService(&storage_pool_impl, 
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add service";
            }
            // 监听[0.0.0.0:local_port]
            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, local_rpc_port);

            brpc::ServerOptions options;
            options.use_rdma = use_rdma;

            if (server.Start(point,&options) != 0) {
                LOG(ERROR) << "Fail to start Server";
            }

            SendMeta(machine_id, compute_node_num);

            server.RunUntilAskedToQuit();
        // });
        // rpc_thread.detach();
    }

    ~Server() {}

    void InitRDMA();
    
    void SendMeta(node_id_t machine_id, size_t compute_node_num);

    void SendStorageMeta(char* hash_meta_buffer, size_t& total_meta_size);

    void PrepareStorageMeta(node_id_t machine_id, char** hash_meta_buffer, size_t& total_meta_size);

    void CleanQP();

    bool Run();

private:
    const int machine_id_;
    const int local_rdma_port_;
    const int local_rpc_port_;
    const int local_meta_port_;
    
    int compute_node_num_;
    DiskManager* disk_manager_;
    LogManager* log_manager_;
    StorageRDMAMemoryManager* rdma_mem_manager_;
    RdmaCtrlPtr rdma_ctrl;
};
