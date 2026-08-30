add_library(
  bustub_raft
  OBJECT
  kv_state_machine.cpp
  log_store.cpp
  raft_node.cpp
  rpc_codec.cpp
  snapshot_store.cpp
  tcp_transport.cpp
  stable_store.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_raft>
  PARENT_SCOPE)
