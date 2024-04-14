// Author: huangdund
// Copyrigth (c) 2023

#include "glm_rpc.h"

#include <stdlib.h>
#include <unistd.h>

DEFINE_int32(glm_port, 12360, "Port of glm server");

int main(int argc, char* argv[]) { 
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    GLM* glm = new GLM();
    brpc::Server server;
    glm_service::GLMImpl glm_impl(glm);
    if (server.AddService(&glm_impl, 
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
    }
    // 监听[0.0.0.0:local_port]
    butil::EndPoint point;
    point = butil::EndPoint(butil::IP_ANY, FLAGS_glm_port);

    brpc::ServerOptions options;

    if (server.Start(point,&options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }
    server.RunUntilAskedToQuit();
    return 0;
}