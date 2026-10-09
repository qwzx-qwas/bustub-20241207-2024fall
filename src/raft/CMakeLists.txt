add_library(
  bustub_raft
  OBJECT
  kv_state_machine.cpp
  log_store.cpp
  object_storage.cpp
  object_log_store.cpp
  object_snapshot_store.cpp
  persistent_state.cpp
  raft_node.cpp
  rpc_codec.cpp
  snapshot_tasks.cpp
  snapshot_store.cpp
  tcp_transport.cpp
  stable_store.cpp)

set_target_properties(bustub_raft PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)

target_link_libraries(bustub_raft PRIVATE bustub_lz4)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_raft>
  PARENT_SCOPE)
