add_library(bustub_lz4 STATIC lz4.c)
target_include_directories(bustub_lz4 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
