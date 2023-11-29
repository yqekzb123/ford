set(RECORD_SRC
        record/rm_file_handle.cpp
        record/rm_manager.cc)

set(BUFFER_SRC
        buffer/bufferpool_manager.cpp
        buffer/replacer/lru_replacer.cpp)

# add_library(storage_test_lib STATIC
#     ${RECORD_SRC}
#     ${BUFFER_SRC}
#     )   

set(STORAGE_TEST_SRC storage_test.cpp)

add_executable(storage_test ${STORAGE_TEST_SRC} ${RECORD_SRC} ${BUFFER_SRC})
target_link_libraries(storage_test pthread ford ${DYNAMIC_LIB} ${BRPC_LIB})