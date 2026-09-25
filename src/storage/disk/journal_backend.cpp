//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// journal_backend.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/journal_backend.h"

#include <algorithm>
#include <stdexcept>

namespace bustub {

JournalBackend::JournalBackend(RegionManager &regions, uint64_t segment_bytes)
    : regions_(regions), journal_(regions.Region(RegionKind::Journal)), segment_bytes_(segment_bytes) {
  const auto info = regions_.Describe(journal_);
  if (segment_bytes == 0 || segment_bytes > info.size_ || segment_bytes % info.offset_alignment_ != 0) {
    throw std::invalid_argument("journal segment size is incompatible with this region or device");
  }
  segment_capacity_ = info.size_ / segment_bytes;
}

auto JournalBackend::SegmentCapacity() const -> uint64_t { return segment_capacity_; }

auto JournalBackend::PlanAppend(uint64_t offset, uint64_t size) const -> std::vector<JournalIORequest> {
  const auto capacity = segment_capacity_ * segment_bytes_;
  if (size == 0 || offset > capacity || size > capacity - offset) {
    throw std::invalid_argument("journal append exceeds complete segment capacity");
  }
  std::vector<JournalIORequest> requests;
  while (size != 0) {
    const auto within = offset % segment_bytes_;
    const auto bytes = std::min(size, segment_bytes_ - within);
    requests.push_back({offset / segment_bytes_, IOOperation::Write, within, bytes});
    offset += bytes;
    size -= bytes;
  }
  return requests;
}

auto JournalBackend::Resolve(const std::vector<JournalIORequest> &requests) const -> std::vector<RegionIORequest> {
  std::vector<RegionIORequest> resolved;
  resolved.reserve(requests.size());
  for (const auto &request : requests) {
    if (request.segment_slot_ >= segment_capacity_ || request.size_ == 0 || request.offset_ > segment_bytes_ ||
        request.size_ > segment_bytes_ - request.offset_) {
      throw std::invalid_argument("journal IO is outside a complete segment slot or empty");
    }
    // Complete-slot capacity bounds multiplication; the checked member fits in
    // that slot, so adding its offset cannot overflow or reach the partial tail.
    resolved.push_back(
        {journal_, request.operation_, request.segment_slot_ * segment_bytes_ + request.offset_, request.size_});
  }
  return resolved;
}

auto JournalBackend::TryPrepare(const std::vector<JournalIORequest> &requests, bool flush_after_writes) const
    -> IOPreparation {
  return regions_.TryPrepare(Resolve(requests), flush_after_writes);
}

auto JournalBackend::TryPrepareExternal(const std::vector<JournalIORequest> &requests,
                                        std::vector<IOBufferLease> &leases, bool flush_after_writes) const
    -> IOPreparation {
  return regions_.TryPrepareExternal(Resolve(requests), leases, flush_after_writes);
}

}  // namespace bustub
