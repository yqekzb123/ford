# Author: Huang Chunyue
# Copyright (c) 2023

set(PAGE_TABLE_SERVER_SOURCE page_table_server.cc)
add_executable(page_table_server ${PAGE_TABLE_SERVER_SOURCE})
target_link_libraries(page_table_server rlib ford)
