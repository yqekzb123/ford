// author: huangchunyue

#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "master_log.pb.h"
#include "base/common.h"

namespace mslog_service{
class MSlogImpl : public MSLogService{  
  public:
    MSlogImpl(std::unordered_map<node_id_t, lsn_t>& node_lsn_map){
      node_lsn_map_ = node_lsn_map;
    }

    virtual ~MSlogImpl(){};

    // 写日志
    virtual void WriteLSNToMS(::google::protobuf::RpcController* controller,
                       const ::mslog_service::MS2MSLogRequest* request,
                       ::mslog_service::MS2MSLogResponse* response,
                       ::google::protobuf::Closure* done){
      brpc::ClosureGuard done_guard(done);
      latch.lock();
      if(node_lsn_map_.find(request->ms_id()) == node_lsn_map_.end()){
        node_lsn_map_[request->ms_id()] = request->lsn();
      } else{
        node_lsn_map_[request->ms_id()] = std::max(node_lsn_map_[request->ms_id()], request->lsn());
      }
      latch.unlock();
    }

  private:
    std::mutex latch;
    std::unordered_map<node_id_t, lsn_t> node_lsn_map_;
  };
}