//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// journal_backend.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

#include "storage/disk/region_manager.h"

namespace bustub {

struct JournalIORequest {
  uint64_t segment_slot_;
  IOOperation operation_;
  uint64_t offset_;
  uint64_t size_;
};

/**
 * F06 S2: fixed segment slots in this manager's Journal region only.
 * The explicit segment size must meet the device's offset/length alignment.
 * Capacity counts complete addressable slots, not allocated or valid segments.
 * Construction checks geometry only; it does not open or format a Journal.
 *
 * Each member must fit in one slot; a batch may contain members in several
 * slots and exceed one segment's size. S3's F06/F07 will choose positions and
 * fragment logical records. This adapter has no record-size or commit semantics.
 *
 * All ranges are validated before F04/F02 admission. Returned preparations keep
 * F02's buffer, Flush and failure contracts; submit through the original
 * executor. Concurrent preparation is allowed, with conflicts caller-ordered.
 * The manager/executor/device must outlive use. Before replacement or reuse,
 * stop callers and drain accepted IO; destroying this adapter does not cancel it.
 */
class JournalBackend {
 public:
  JournalBackend(RegionManager &regions, uint64_t segment_bytes);
  JournalBackend(const JournalBackend &) = delete;
  auto operator=(const JournalBackend &) -> JournalBackend & = delete;

  auto SegmentCapacity() const -> uint64_t;
  auto TryPrepare(const std::vector<JournalIORequest> &requests, bool flush_after_writes) const -> IOPreparation;
  auto TryPrepareExternal(const std::vector<JournalIORequest> &requests, std::vector<IOBufferLease> &leases,
                          bool flush_after_writes) const -> IOPreparation;

 private:
  auto Resolve(const std::vector<JournalIORequest> &requests) const -> std::vector<RegionIORequest>;
  RegionManager &regions_;
  RegionHandle journal_;
  uint64_t segment_bytes_;
  uint64_t segment_capacity_;
};

}  // namespace bustub
