set(BUSTUB_CLIENT_SOURCES bustub-client.cpp)
add_executable(bustub-client ${BUSTUB_CLIENT_SOURCES})
target_link_libraries(bustub-client bustub)
