# Author: HuangChunyue
# Copyright (c) 2023

set(INDEX_SERVER_SOURCE hash_index_server.cc)
add_executable(index_server ${INDEX_SERVER_SOURCE})
# target_link_libraries(zm_mem_pool tatp_db smallbank_db tpcc_db micro_db rlib)
target_link_libraries(index_server rlib smallbank_db)
