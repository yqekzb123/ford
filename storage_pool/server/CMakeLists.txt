# Author: Ming Zhang
# Copyright (c) 2022

set(SERVER_SOURCE server.cc)
add_executable(storage_pool ${SERVER_SOURCE})
target_link_libraries(storage_pool ford rlib smallbank_db ${DYNAMIC_LIB} ${BRPC_LIB})
