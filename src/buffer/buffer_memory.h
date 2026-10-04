// Internal Linux RAM ownership shared by the F26 frame and translation areas.
#pragma once

#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <system_error>

namespace bustub::buffer_memory {

inline auto PageBytes() -> size_t {
  const auto bytes = sysconf(_SC_PAGESIZE);
  if (bytes <= 0) {
    throw std::runtime_error("cannot query OS memory page size");
  }
  return static_cast<size_t>(bytes);
}

inline auto Add(size_t a, size_t b) -> size_t {
  if (b > std::numeric_limits<size_t>::max() - a) {
    throw std::length_error("buffer memory size overflow");
  }
  return a + b;
}

inline auto Multiply(size_t a, size_t b) -> size_t {
  if (a != 0 && b > std::numeric_limits<size_t>::max() / a) {
    throw std::length_error("buffer memory size overflow");
  }
  return a * b;
}

inline auto RoundUp(size_t bytes, size_t alignment) -> size_t {
  const auto remainder = bytes % alignment;
  return remainder == 0 ? bytes : Add(bytes, alignment - remainder);
}

/** Owns one anonymous mapping, including alignment slack; never touches its bytes. */
class Region {
 public:
  static auto MappingBytes(size_t bytes, size_t alignment, size_t os_page) -> size_t {
    const auto common = Multiply(alignment / std::gcd(alignment, os_page), os_page);
    return Add(RoundUp(bytes, os_page), common - os_page);
  }

  Region(size_t bytes, size_t alignment, size_t os_page) : bytes_(MappingBytes(bytes, alignment, os_page)) {
    base_ = mmap(nullptr, bytes_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base_ == MAP_FAILED) {
      throw std::system_error(errno, std::generic_category(), "buffer mmap");
    }
    const auto common = Multiply(alignment / std::gcd(alignment, os_page), os_page);
    data_ = reinterpret_cast<char *>(RoundUp(reinterpret_cast<uintptr_t>(base_), common));
  }
  ~Region() { munmap(base_, bytes_); }
  Region(const Region &) = delete;
  auto operator=(const Region &) -> Region & = delete;
  auto Data() const -> char * { return data_; }
  auto Base() const -> void * { return base_; }
  auto Size() const -> size_t { return bytes_; }

  void UseBasePages() const {
#ifdef MADV_NOHUGEPAGE
    if (madvise(base_, bytes_, MADV_NOHUGEPAGE) == 0) {
      return;
    }
    const auto error = errno;
    // Linux documents a zero-length probe for advice support. Without THP
    // support, ordinary mappings already use base pages. A supported advice
    // failing on our actual mapping is still an error, not a fallback.
    if (error == EINVAL && madvise(nullptr, 0, MADV_NOHUGEPAGE) == -1 && errno == EINVAL) {
      return;
    }
    throw std::system_error(error, std::generic_category(), "disable buffer THP");
#endif
  }

 private:
  size_t bytes_;
  void *base_;
  char *data_;
};

}  // namespace bustub::buffer_memory
