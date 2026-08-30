set(BUSTUB_NODE_SOURCES bustub-node.cpp)
add_executable(bustub-node ${BUSTUB_NODE_SOURCES})
target_link_libraries(bustub-node bustub)
