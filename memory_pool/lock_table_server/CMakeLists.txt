# Author: HuangChunyue
# Copyright (c) 2023

set(LOCKTABLE_SERVER_SOURCE lock_table_server.cc)
add_executable(lock_table_server ${LOCKTABLE_SERVER_SOURCE})
target_link_libraries(lock_table_server rlib)
