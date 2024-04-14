#include "glm_rpc.h"
#include "util/debug.h"

namespace glm_service{

    GLMImpl::GLMImpl(GLM* glm):glm_(glm){};

    GLMImpl::~GLMImpl(){};

    // 获取共享锁
    void GLMImpl::LockPage(::google::protobuf::RpcController* controller,
                       const ::glm_service::LockRequest* request,
                       ::glm_service::LockResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        // // 计时
        // struct timespec tx_start_time;
        // clock_gettime(CLOCK_REALTIME, &tx_start_time);

        std::vector<glmlock_request> requests;
        for(int i=0; i<request->shared_lock_id_size(); i++){
            glmlock_request req;
            req.table_id = request->shared_lock_id(i).table_id();
            req.page_id = request->shared_lock_id(i).page_id();
            requests.push_back(req);
        }
        int pos1 = glm_->GLMLockShared(requests);
        response->set_shared_fail_request_loc(pos1);

        std::vector<glmlock_request> requests2;
        for(int i=0; i<request->exclusive_lock_id_size(); i++){
            glmlock_request req;
            req.table_id = request->exclusive_lock_id(i).table_id();
            req.page_id = request->exclusive_lock_id(i).page_id();
            requests2.push_back(req);
        }
        int pos2 = glm_->GLMLockExclusive(requests2);
        response->set_exclusive_fail_request_loc(pos2);

        // // 计时
        // struct timespec tx_lock_time;
        // clock_gettime(CLOCK_REALTIME, &tx_lock_time);
        // double lock_usec = (tx_lock_time.tv_sec - tx_start_time.tv_sec) * 1000000 + (double)(tx_lock_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
        // printf("Lock time: %lf\n", lock_usec);
        return;
    }

    // 释放共享锁
    void GLMImpl::UnLockPage(::google::protobuf::RpcController* controller,
                       const ::glm_service::UnLockRequest* request,
                       ::glm_service::UnLockResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::vector<glmlock_request> requests;
        for(int i=0; i<request->shared_unlock_id_size(); i++){
            glmlock_request req;
            req.table_id = request->shared_unlock_id(i).table_id();
            req.page_id = request->shared_unlock_id(i).page_id();
            requests.push_back(req);
        }
        bool success = glm_->GLMUnLockShared(requests);

        std::vector<glmlock_request> requests2;
        for(int i=0; i<request->exclusive_unlock_id_size(); i++){
            glmlock_request req;
            req.table_id = request->exclusive_unlock_id(i).table_id();
            req.page_id = request->exclusive_unlock_id(i).page_id();
            requests2.push_back(req);
        }
        bool success2 = glm_->GLMUnLockExclusive(requests2);
        return;
    }
};