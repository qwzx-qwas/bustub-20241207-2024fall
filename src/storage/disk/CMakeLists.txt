add_library(
    bustub_storage_disk 
    OBJECT
    disk_manager.cpp
    disk_manager_memory.cpp
    disk_scheduler.cpp)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_sources(bustub_storage_disk PRIVATE block_device.cpp io_executor.cpp bootstrap_store.cpp region_manager.cpp metadata_backend.cpp metadata_engine.cpp journal_backend.cpp journal_service.cpp)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_storage_disk>
    PARENT_SCOPE)
