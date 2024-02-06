#include "storage_rpc.h"
#include "util/debug.h"

namespace storage_service{

    StoragePoolImpl::StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager)
        :log_manager_(log_manager), disk_manager_(disk_manager){};

    StoragePoolImpl::~StoragePoolImpl(){};

    // 计算层向存储层写日志
    void StoragePoolImpl::LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){
            
        brpc::ClosureGuard done_guard(done);
        RDMA_LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_batch_log_to_disk(request->log());
        
        return;
    };

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        std::string table_name = request->page_id().table_name();
        page_id_t page_no = request->page_id().page_no();
        batch_id_t request_batch_id = request->require_batch_id();
        LogReplay* log_replay = log_manager_->log_replay_;

        RDMA_LOG(INFO) << "handle GetPage request";
        RDMA_LOG(INFO) << "request_batch_id: " << request_batch_id << ", persist_batch_id: " << log_replay->get_persist_batch_id();

        char data[PAGE_SIZE + 1];

        // TODO
        while(log_replay->get_persist_batch_id()+1 < request_batch_id) {
            // wait
            RDMA_LOG(INFO) << "the batch_id requirement is not satisfied...." << "  persist id: "<<
                log_replay->get_persist_batch_id() << "  request id: " << request_batch_id;
            usleep(10);
        }

        RDMA_LOG(INFO) << "the batch_id requirement is satisfied";

        int fd = disk_manager_->open_file(table_name);
        disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);

        response->set_data(std::string(data, PAGE_SIZE));

        disk_manager_->close_file(fd);

        RDMA_LOG(INFO) << "success to GetPage";

        return;
    };
}