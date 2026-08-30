add_library(
  bustub_distributed
  OBJECT
  bustub_state_machine.cpp
  client.cpp
  client_protocol.cpp
  command_codec.cpp
  node.cpp
  raft_state_machine.cpp
  sql_command_preparer.cpp
  state_visibility.cpp
  session_table.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_distributed>
  PARENT_SCOPE)
