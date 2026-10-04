// Bounded snapshot bytes, independent of files versus ordinary objects.
#pragma once
#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include "recovery/durable_storage.h"
namespace bustub {
using SnapshotAppend = std::function<void(const std::vector<std::byte> &)>;
struct SnapshotInput {
  uint64_t offset_;
  uint64_t size_;
  // The source retains its entire use lease, including gaps between Read calls.
  std::function<std::vector<std::byte>(uint64_t, size_t)> source_;
  auto Read(uint64_t offset, size_t size) const -> std::vector<std::byte> {
    if (offset > size_ || size > size_ - offset) {
      throw std::out_of_range("snapshot range exceeds body");
    }
    auto bytes = source_(offset_ + offset, size);
    if (bytes.size() != size) {
      throw std::runtime_error("snapshot body was truncated");
    }
    return bytes;
  }
  auto Slice(uint64_t offset, uint64_t size) const -> SnapshotInput {
    if (offset > size_ || size > size_ - offset) {
      throw std::out_of_range("snapshot slice exceeds body");
    }
    return {offset_ + offset, size, source_};
  }
};
inline auto FileSnapshotInput(const DurableFileSlice &slice, std::shared_ptr<DurableStorage> storage) -> SnapshotInput {
  if (slice.offset_ > std::numeric_limits<uint64_t>::max() - slice.size_) {
    throw std::invalid_argument("snapshot file range overflows");
  }
  return {slice.offset_, slice.size_, [storage = std::move(storage), path = slice.path_](uint64_t offset, size_t size) {
            return storage->ReadFileRange(path, offset, size);
          }};
}
}  // namespace bustub
