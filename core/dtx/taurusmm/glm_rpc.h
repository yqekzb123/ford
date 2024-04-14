// author: huangchunyue

#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "glm.pb.h"
#include "glm.h"

namespace glm_service{
class GLMImpl : public GLMService{  
  public:
    GLMImpl(GLM* glm);

    virtual ~GLMImpl();

    // 获取共享锁
    virtual void LockPage(::google::protobuf::RpcController* controller,
                       const ::glm_service::LockRequest* request,
                       ::glm_service::LockResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void UnLockPage(::google::protobuf::RpcController* controller,
                       const ::glm_service::UnLockRequest* request,
                       ::glm_service::UnLockResponse* response,
                       ::google::protobuf::Closure* done);

  private:
    GLM* glm_;
  };
}