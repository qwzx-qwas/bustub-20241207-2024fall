add_library(
  bustub_common
  OBJECT
  bustub_instance.cpp
  bustub_ddl.cpp
  byte_codec.cpp
  config.cpp
  sha256.cpp
  util/string_util.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_common>
  PARENT_SCOPE)
