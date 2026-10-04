//===----------------------------------------------------------------------===//
// BusTub — F26 stable frame body memory (Linux).
//===----------------------------------------------------------------------===//
#pragma once

#include <cstddef>
#include <memory>
#include <optional>

#include "common/config.h"

namespace bustub {

struct FrameArenaOptions {
  size_t frame_count_;
  size_t page_bytes_;
  size_t memory_alignment_;  // From the actual backend, not a fixed IO granularity.
  size_t max_bytes_;         // Includes page rounding and alignment slack.
  bool advise_huge_pages_;
};

enum class HugePageAdvice { Disabled, Accepted, Unavailable };

struct FrameArenaLayout {
  size_t frame_count_;
  size_t page_bytes_;
  size_t stride_;
  size_t mapping_bytes_;
  size_t os_page_bytes_;
  HugePageAdvice huge_page_advice_;
  int advice_error_;  // errno when Unavailable; does not invalidate ordinary memory.
};

struct FrameMemoryUsage {
  size_t resident_bytes_;
  size_t anonymous_huge_page_bytes_;
};

struct FrameMemory {
  char *data_;
  size_t size_;      // Database page body.
  size_t capacity_;  // Slot stride, including alignment padding.
};

/**
 * Owns stable anonymous RAM, not page residency or IO permissions. The owner
 * must outlive all returned spans, guards and IO leases. No implicit pre-touch.
 * F26/c supplies pin + content rights before lending these spans to F02.
 */
class FrameArena {
 public:
  explicit FrameArena(const FrameArenaOptions &options);
  ~FrameArena();
  FrameArena(const FrameArena &) = delete;
  auto operator=(const FrameArena &) -> FrameArena & = delete;

  auto Frame(frame_id_t frame) const -> FrameMemory;
  auto Layout() const -> const FrameArenaLayout &;
  /** Linux smaps observation, off the access path. Unknown if unavailable or
   * VMA merging prevents attribution to this arena. Advice is not evidence. */
  auto ObserveMemory() const -> std::optional<FrameMemoryUsage>;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
