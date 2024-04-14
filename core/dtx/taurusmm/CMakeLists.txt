set(TAURUS_SRC
        master_buffer.cc
        master_log_rpc.cc
        master_log.pb.cc
    )

set(GLM_SRC
        glm_rpc.cc
        glm.pb.cc
        glm.cc
    )

add_library(taurus STATIC
    ${TAURUS_SRC}
    )

add_library(glm STATIC
    ${GLM_SRC}
    )

add_executable(glm_server glm_server.cc)

target_link_libraries(glm_server glm ${DYNAMIC_LIB} ${BRPC_LIB})