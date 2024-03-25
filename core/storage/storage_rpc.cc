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
        // RDMA_LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_batch_log_to_disk(request->log());

        std::unordered_map<std::string, int> table_fd_map;
        for(int i=0; i<request->page_id_size(); i++){
            page_id_t page_no = request->page_id()[i].page_no();
            std::string table_name = request->page_id()[i].table_name();
            int fd;
            if(table_fd_map.find(table_name) == table_fd_map.end()){
                fd = disk_manager_->open_file(table_name);
                table_fd_map[table_name] = fd;
            }
            else{
                fd = table_fd_map[table_name];
            }
            PageId page_id(fd, page_no);
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.lock();
            log_manager_->log_replay_->pageid_batch_count_[page_id].second++;
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.unlock();
        }
        for(auto it = table_fd_map.begin(); it != table_fd_map.end(); it++){
            disk_manager_->close_file(it->second);
        }
        return;
    };

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        std::unordered_map<std::string, int> table_fd_map;
        std::string return_data;
        for(int i=0; i<request->page_id().size(); i++){
            std::string table_name = request->page_id()[i].table_name();
            int fd;
            if(table_fd_map.find(table_name) == table_fd_map.end()){
                fd = disk_manager_->open_file(table_name);
                table_fd_map[table_name] = fd;
            }
            else{
                fd = table_fd_map[table_name];
            }
            page_id_t page_no = request->page_id()[i].page_no();
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            // RDMA_LOG(INFO) << "handle GetPage request";
            // RDMA_LOG(INFO) << "request_batch_id: " << request_batch_id << ", persist_batch_id: " << log_replay->get_persist_batch_id();
            char data[PAGE_SIZE];
            // TODO, 这里逻辑要重新梳理一下
            // while(log_replay->get_persist_batch_id()+1 < request_batch_id) {
            //     // wait
            //     RDMA_LOG(INFO) << "the batch_id requirement is not satisfied...." << "  persist id: "<<
            //         log_replay->get_persist_batch_id() << "  request id: " << request_batch_id;
            //     usleep(10);
            // }

            log_replay->pageid_batch_count_[page_id].first.lock();
            while (log_replay->pageid_batch_count_[page_id].second > 0) {
                // wait
                RDMA_LOG(INFO) << "the log replay queue is has another item...." << "  batch item cnt: "<<
                    log_replay->pageid_batch_count_[page_id].second;
                usleep(10);
            }
            log_replay->pageid_batch_count_[page_id].first.unlock();
            
            disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            return_data.append(std::string(data, PAGE_SIZE));
        }

        response->set_data(return_data);
        for(auto it = table_fd_map.begin(); it != table_fd_map.end(); it++){
            disk_manager_->close_file(it->second);
        }
        // RDMA_LOG(INFO) << "success to GetPage";
        return;
    };
}