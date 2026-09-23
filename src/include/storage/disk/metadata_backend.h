//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// metadata_backend.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

#include "common/config.h"
#include "storage/disk/region_manager.h"

namespace bustub {

struct MetadataPageRequest {
  page_id_t page_id_;
  IOOperation operation_;
};

/**
 * F05 S2: fixed whole-page slots in this manager's Metadata region only.
 * Capacity describes addressable slots, not allocated or valid database pages.
 * Page zero is ordinary; partial trailing bytes are not addressable. Construction
 * verifies geometry, not an on-disk B format. S4 must persist/check page size and
 * format before opening a metadata database.
 *
 * All page IDs are validated before F04/F02 admission. IO, buffer permissions,
 * admission, Flush and failure semantics are those of F02; submit returned
 * batches through the original executor. No extra cache, queue or transaction.
 * In particular, the caller (future F07/F08/F09) must enforce WAL before page IO.
 *
 * Concurrent preparation is allowed; cross-batch conflicts are caller-ordered.
 * The manager/executor/device must outlive backend use. Their existing drain and
 * replacement rules apply; destroying this adapter does not cancel accepted IO.
 */
class MetadataBackend {
 public:
  explicit MetadataBackend(RegionManager &regions);
  MetadataBackend(const MetadataBackend &) = delete;
  auto operator=(const MetadataBackend &) -> MetadataBackend & = delete;

  auto PageCapacity() const -> uint64_t;
  auto TryPrepare(const std::vector<MetadataPageRequest> &requests, bool flush_after_writes) const -> IOPreparation;
  auto TryPrepareExternal(const std::vector<MetadataPageRequest> &requests, std::vector<IOBufferLease> &leases,
                          bool flush_after_writes) const -> IOPreparation;

 private:
  auto Resolve(const std::vector<MetadataPageRequest> &requests) const -> std::vector<RegionIORequest>;
  RegionManager &regions_;
  RegionHandle metadata_;
  uint64_t page_capacity_;
};

}  // namespace bustub
