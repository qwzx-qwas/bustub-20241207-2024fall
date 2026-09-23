//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// region_manager.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "storage/disk/io_executor.h"

namespace bustub {

class BootstrapStore;
struct RegionContext;
enum class RegionKind { Metadata, Journal, Data };

/** A region in one immutable manager binding; it does not expose a device address. */
class RegionHandle {
 public:
  RegionHandle(const RegionHandle &) = default;
  auto operator=(const RegionHandle &) -> RegionHandle & = default;

 private:
  friend class RegionManager;
  RegionHandle(std::shared_ptr<const RegionContext> context, RegionKind kind);
  std::shared_ptr<const RegionContext> context_;
  RegionKind kind_;
};

struct RegionIORequest {
  RegionHandle region_;
  IOOperation operation_;
  uint64_t offset_;
  uint64_t size_;
};

/**
 * F04: immutable, region-relative addressing on one already-open F03 binding.
 * Construction is serialized with the bootstrap lifecycle. No arbitrary layout
 * or executor can be supplied. Handles from another manager are rejected, even
 * when both managers describe the same device. This is an internal misuse guard,
 * not isolation from hostile code in this process.
 *
 * All ranges are resolved before F02 preparation. The resulting IOBatch keeps
 * F02's ownership, admission and failure contracts; submit it through the same
 * existing IOExecutor. No extra queue, IO, Flush, recovery log or transaction is
 * introduced. Concurrent preparation is allowed; conflicting IO is caller-ordered.
 *
 * The validated layout is copied: BootstrapStore need not outlive this manager.
 * The executor/device must outlive use of all managers and handles. Before device
 * replacement/reformat, stop callers, discard these bindings and drain accepted
 * IO. A handle does not pin a device or authorize reuse after its lifetime ends.
 */
class RegionManager {
 public:
  explicit RegionManager(BootstrapStore &bootstrap);
  RegionManager(const RegionManager &) = delete;
  auto operator=(const RegionManager &) -> RegionManager & = delete;

  auto Region(RegionKind kind) const -> RegionHandle;
  auto TryPrepare(const std::vector<RegionIORequest> &requests, bool flush_after_writes) const -> IOPreparation;
  auto TryPrepareExternal(const std::vector<RegionIORequest> &requests, std::vector<IOBufferLease> &leases,
                          bool flush_after_writes) const -> IOPreparation;

 private:
  auto Resolve(const std::vector<RegionIORequest> &requests) const -> std::vector<IORequest>;
  std::shared_ptr<const RegionContext> context_;
};

}  // namespace bustub
