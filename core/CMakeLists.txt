set(CONNECTION_SRC
        connection/meta_manager.cc
        connection/qp_manager.cc
        )

set(DTX_SRC
        dtx/doorbell.cc
        dtx/dtx_bufferpool.cc
        dtx/dtx.cc
        dtx/dtx_hash_index.cc
        dtx/dtx_lock.cc
        dtx/dtx_local.cc
        dtx/dtx_local_exe_commit.cc
        dtx/dtx_base_exe_commit.cc
        dtx/dtx_page_table.cc
        dtx/structs.cc
        dtx/rwlock.cc
        dtx/dtx_log.cc
        )

set(SCHEDULER_SRC
        scheduler/corotine_scheduler.cc
        )

set(STORAGE_SRC
        storage/storage_service.pb.cc
        storage/storage_rpc.cc
        storage/disk_manager.cc
        )

set(BATCH_SRC
        batch/local_batch.cc)
        
set(LOG_SRC
        log/logreplay.cc
        # log/logparse.cc
        log/log_manager.cc)

set(MEM_SRC
        memstore/page_table.cc)

add_library(ford STATIC
        ${CONNECTION_SRC}
        ${DTX_SRC}
        ${SCHEDULER_SRC}
        ${STORAGE_SRC}
        ${BATCH_SRC}
        ${LOG_SRC}
        ${MEM_SRC}
        )

set_target_properties(ford PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(ford ${DYNAMIC_LIB} ${BRPC_LIB} rlib pthread boost_coroutine boost_context boost_system)