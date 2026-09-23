//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// metadata_backend.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/metadata_backend.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace bustub {

MetadataBackend::MetadataBackend(RegionManager &regions)
    : regions_(regions), metadata_(regions.Region(RegionKind::Metadata)) {
  const auto info = regions_.Describe(metadata_);
  if (info.size_ < BUSTUB_PAGE_SIZE || BUSTUB_PAGE_SIZE % info.offset_alignment_ != 0) {
    throw std::invalid_argument("metadata region cannot support whole BusTub pages on this device");
  }
  page_capacity_ =
      std::min(info.size_ / BUSTUB_PAGE_SIZE, static_cast<uint64_t>(std::numeric_limits<page_id_t>::max()) + 1);
}

auto MetadataBackend::PageCapacity() const -> uint64_t { return page_capacity_; }

auto MetadataBackend::Resolve(const std::vector<MetadataPageRequest> &requests) const -> std::vector<RegionIORequest> {
  std::vector<RegionIORequest> resolved;
  resolved.reserve(requests.size());
  for (const auto &request : requests) {
    if (request.page_id_ < 0 || static_cast<uint64_t>(request.page_id_) >= page_capacity_) {
      throw std::invalid_argument("metadata page is outside the addressable capacity");
    }
    // Capacity was bounded before multiplication, including page_id_t's limit.
    resolved.push_back(
        {metadata_, request.operation_, static_cast<uint64_t>(request.page_id_) * BUSTUB_PAGE_SIZE, BUSTUB_PAGE_SIZE});
  }
  return resolved;
}

auto MetadataBackend::TryPrepare(const std::vector<MetadataPageRequest> &requests, bool flush_after_writes) const
    -> IOPreparation {
  return regions_.TryPrepare(Resolve(requests), flush_after_writes);
}

auto MetadataBackend::TryPrepareExternal(const std::vector<MetadataPageRequest> &requests,
                                         std::vector<IOBufferLease> &leases, bool flush_after_writes) const
    -> IOPreparation {
  return regions_.TryPrepareExternal(Resolve(requests), leases, flush_after_writes);
}

}  // namespace bustub
