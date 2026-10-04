#include "buffer/frame_arena.h"

#include <cstdio>
#include <fstream>
#include <limits>
#include <string>

#include "buffer_memory.h"

namespace bustub {

struct FrameArena::Impl {
  static auto Plan(const FrameArenaOptions &options) -> FrameArenaLayout {
    if (options.frame_count_ == 0 || options.page_bytes_ == 0 || options.memory_alignment_ == 0 ||
        options.frame_count_ > static_cast<size_t>(std::numeric_limits<frame_id_t>::max()) + 1) {
      throw std::invalid_argument("invalid frame arena geometry");
    }
    const auto os_page = buffer_memory::PageBytes();
    const auto stride = buffer_memory::RoundUp(options.page_bytes_, options.memory_alignment_);
    const auto mapping = buffer_memory::Region::MappingBytes(buffer_memory::Multiply(options.frame_count_, stride),
                                                             options.memory_alignment_, os_page);
    if (mapping > options.max_bytes_) {
      throw std::bad_alloc();
    }
    return {options.frame_count_, options.page_bytes_, stride, mapping, os_page, HugePageAdvice::Disabled, 0};
  }

  explicit Impl(const FrameArenaOptions &options)
      : layout_(Plan(options)),
        memory_(buffer_memory::Multiply(layout_.frame_count_, layout_.stride_), options.memory_alignment_,
                layout_.os_page_bytes_) {
    if (!options.advise_huge_pages_) {
      memory_.UseBasePages();
    } else if (madvise(memory_.Base(), memory_.Size(), MADV_HUGEPAGE) != 0) {
      layout_.huge_page_advice_ = HugePageAdvice::Unavailable;
      layout_.advice_error_ = errno;
    } else {
      layout_.huge_page_advice_ = HugePageAdvice::Accepted;
    }
  }

  FrameArenaLayout layout_;
  buffer_memory::Region memory_;
};

FrameArena::FrameArena(const FrameArenaOptions &options) : impl_(std::make_unique<Impl>(options)) {}
FrameArena::~FrameArena() = default;

auto FrameArena::Frame(frame_id_t frame) const -> FrameMemory {
  if (frame < 0 || static_cast<size_t>(frame) >= impl_->layout_.frame_count_) {
    throw std::out_of_range("frame outside arena");
  }
  return {impl_->memory_.Data() + static_cast<size_t>(frame) * impl_->layout_.stride_, impl_->layout_.page_bytes_,
          impl_->layout_.stride_};
}

auto FrameArena::Layout() const -> const FrameArenaLayout & { return impl_->layout_; }

auto FrameArena::ObserveMemory() const -> std::optional<FrameMemoryUsage> {
  std::ifstream input("/proc/self/smaps");
  if (!input) {
    return std::nullopt;
  }
  const auto begin = reinterpret_cast<uintptr_t>(impl_->memory_.Base());
  const auto end = begin + impl_->memory_.Size();
  uintptr_t covered = begin;
  bool inside = false;
  bool have_rss = false;
  bool have_huge = false;
  FrameMemoryUsage usage{};
  std::string line;
  while (std::getline(input, line)) {
    unsigned long from = 0;
    unsigned long to = 0;
    if (std::sscanf(line.c_str(), "%lx-%lx", &from, &to) == 2) {
      if (inside && (!have_rss || !have_huge)) {
        return std::nullopt;
      }
      inside = from < end && to > begin;
      have_rss = false;
      have_huge = false;
      if (inside) {
        // Never attribute an adjacent arena's resident bytes to this one.
        if (from != covered || to > end) {
          return std::nullopt;
        }
        covered = to;
      }
    } else if (inside) {
      size_t kib = 0;
      if (std::sscanf(line.c_str(), "Rss: %zu kB", &kib) == 1) {
        usage.resident_bytes_ += kib * 1024;
        have_rss = true;
      } else if (std::sscanf(line.c_str(), "AnonHugePages: %zu kB", &kib) == 1) {
        usage.anonymous_huge_page_bytes_ += kib * 1024;
        have_huge = true;
      }
    }
  }
  if (input.bad() || covered != end || (inside && (!have_rss || !have_huge))) {
    return std::nullopt;
  }
  return usage;
}

}  // namespace bustub
