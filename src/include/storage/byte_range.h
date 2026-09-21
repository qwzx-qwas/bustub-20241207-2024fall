//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// byte_range.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <limits>
#include <optional>

namespace bustub {

/**
 * A byte interval [offset, offset + size) with a representable uint64_t end.
 * This value establishes bounds only, not ownership, object identity or durability.
 */
class StorageByteRange {
 public:
  static auto Create(uint64_t offset, uint64_t size) -> std::optional<StorageByteRange> {
    if (size > std::numeric_limits<uint64_t>::max() - offset) {
      return std::nullopt;
    }
    return StorageByteRange(offset, size);
  }

  /** Relative coordinates must stay inside this range; an empty range at its end is valid. */
  auto Subrange(uint64_t relative_offset, uint64_t size) const -> std::optional<StorageByteRange> {
    if (relative_offset > size_ || size > size_ - relative_offset) {
      return std::nullopt;
    }
    // Containment and the parent's checked end make this addition safe.
    return StorageByteRange(offset_ + relative_offset, size);
  }

  auto Offset() const -> uint64_t { return offset_; }
  auto Size() const -> uint64_t { return size_; }

 private:
  StorageByteRange(uint64_t offset, uint64_t size) : offset_(offset), size_(size) {}

  uint64_t offset_;
  uint64_t size_;
};

}  // namespace bustub
