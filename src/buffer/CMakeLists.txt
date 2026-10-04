add_library(
        bustub_buffer
        OBJECT
        buffer_pool_manager.cpp
        file_page_storage.cpp
        frame_arena.cpp
        translation_directory.cpp
        clock_replacer.cpp
        lru_replacer.cpp
        lru_k_replacer.cpp)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_sources(bustub_buffer PRIVATE page_storage.cpp)
endif()

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_buffer>
        PARENT_SCOPE)
