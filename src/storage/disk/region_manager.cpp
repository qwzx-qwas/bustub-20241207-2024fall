//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// region_manager.cpp
//
//===----------------------------------------------------------------------===//

#include "storage/disk/region_manager.h"

#include <array>
#include <stdexcept>
#include <utility>

#include "storage/disk/bootstrap_store.h"

namespace bustub {

struct RegionContext {
  IOExecutor *executor_;
  std::array<StorageByteRange, 3> regions_;
};

RegionHandle::RegionHandle(std::shared_ptr<const RegionContext> context, RegionKind kind)
    : context_(std::move(context)), kind_(kind) {}

RegionManager::RegionManager(BootstrapStore &bootstrap) {
  const auto binding = bootstrap.BindRegions();
  context_ = std::make_shared<const RegionContext>(RegionContext{binding.executor_, binding.regions_});
}

auto RegionManager::Region(RegionKind kind) const -> RegionHandle {
  if (static_cast<size_t>(kind) >= context_->regions_.size()) {
    throw std::invalid_argument("unknown storage region");
  }
  return {context_, kind};
}

auto RegionManager::Resolve(const std::vector<RegionIORequest> &requests) const -> std::vector<IORequest> {
  std::vector<IORequest> resolved;
  resolved.reserve(requests.size());
  for (const auto &request : requests) {
    if (request.region_.context_ != context_) {
      throw std::invalid_argument("region handle belongs to another manager binding");
    }
    const auto &region = context_->regions_[static_cast<size_t>(request.region_.kind_)];
    const auto range = region.Subrange(request.offset_, request.size_);
    if (!range || request.size_ == 0) {
      throw std::invalid_argument("IO range is outside its storage region or empty");
    }
    resolved.push_back({request.operation_, *range});
  }
  return resolved;
}

auto RegionManager::TryPrepare(const std::vector<RegionIORequest> &requests, bool flush_after_writes) const
    -> IOPreparation {
  return context_->executor_->TryPrepare(Resolve(requests), flush_after_writes);
}

auto RegionManager::TryPrepareExternal(const std::vector<RegionIORequest> &requests, std::vector<IOBufferLease> &leases,
                                       bool flush_after_writes) const -> IOPreparation {
  return context_->executor_->TryPrepareExternal(Resolve(requests), leases, flush_after_writes);
}

}  // namespace bustub
