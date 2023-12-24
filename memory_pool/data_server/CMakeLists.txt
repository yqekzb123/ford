# Author: HuangChunyue
# Copyright (c) 2023

set(DATA_SERVER_SOURCE data_server.cc)
add_executable(data_server ${DATA_SERVER_SOURCE})
target_link_libraries(data_server rlib)
