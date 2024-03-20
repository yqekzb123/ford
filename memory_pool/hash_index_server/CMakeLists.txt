# Author: HuangChunyue
# Copyright (c) 2023

set(INDEX_SERVER_SOURCE hash_index_server.cc
        ../../core/storage/disk_manager.cc
        ../../test/record/rm_file_handle.cpp
        ../../test/record/rm_manager.cc
        ../../test/buffer/bufferpool_manager.cpp
        ../../test/buffer/replacer/lru_replacer.cpp

        ../../workload/smallbank/smallbank_db.cc
        ../../workload/micro/micro_db.cc
        ../../workload/tatp/tatp_db.cc
        ../../workload/tpcc/tpcc_db.cc)

add_executable(index_server ${INDEX_SERVER_SOURCE})
target_link_libraries(index_server rlib)
