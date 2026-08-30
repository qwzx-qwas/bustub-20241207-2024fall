add_library(
  bustub_recovery
  OBJECT
  canonical_snapshot.cpp
  command_log.cpp
  durable_storage.cpp
  log_codec.cpp
  log_manager.cpp
  node_directory.cpp
  single_node_runtime.cpp
  snapshot_manager.cpp
  state_manifest.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_recovery>
  PARENT_SCOPE)
