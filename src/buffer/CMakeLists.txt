include(CheckCXXSourceCompiles)
set(CMAKE_CXX_STANDARD 23)
check_cxx_source_compiles("#include <memory>
#include <atomic>
#include <cstdint>
#ifndef __cpp_lib_start_lifetime_as
#error explicit lifetime support required
#endif
static_assert(std::atomic_ref<uint64_t>::is_always_lock_free);
int main() { alignas(uint64_t) unsigned char bytes[8]{};
 auto *p = std::start_lifetime_as_array<uint64_t>(bytes, 1);
 return std::atomic_ref<uint64_t>(*p).load(); }" BUSTUB_HAS_ZERO_PAGE_LIFETIME)
set(CMAKE_CXX_STANDARD 17)
if(NOT BUSTUB_HAS_ZERO_PAGE_LIFETIME)
    message(FATAL_ERROR "F26 requires C++23 std::start_lifetime_as_array and lock-free atomic_ref<uint64_t>. Use a supporting compiler and standard library (for example GCC/libstdc++ 16).")
endif()

# The mmap translation representation needs explicit lifetime management, not
# placement construction that would privately back every probed group.
add_library(bustub_translation OBJECT translation_directory.cpp)
set_target_properties(bustub_translation PROPERTIES CXX_STANDARD 23 CXX_STANDARD_REQUIRED ON)

add_library(
        bustub_buffer
        OBJECT
        buffer_pool_manager.cpp
        file_page_storage.cpp
        frame_arena.cpp
        clock_pro_replacer.cpp)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_sources(bustub_buffer PRIVATE page_storage.cpp)
endif()

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_buffer> $<TARGET_OBJECTS:bustub_translation>
        PARENT_SCOPE)
