set(VECTOR_BENCH_SOURCES vector_bench.cpp)
add_executable(vector-bench ${VECTOR_BENCH_SOURCES})

target_link_libraries(vector-bench bustub)
set_target_properties(vector-bench PROPERTIES OUTPUT_NAME bustub-vector-bench)
