# Author: Ming Zhang
# Copyright (c) 2022

set(SERVER_SOURCE addr_manager.cc)
add_executable(addr_manager ${SERVER_SOURCE})
target_link_libraries(addr_manager smallbank_db rlib)
# target_link_libraries(addr_manager tatp_db smallbank_db tpcc_db micro_db rlib)
