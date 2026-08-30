//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// snapshot_manager.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/state_visibility.h"
#include "distributed/session_table.h"
#include "recovery/node_directory.h"
#include "recovery/state_manifest.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

struct RecoveredSnapshot {
  StateManifest manifest_;
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<SessionTable> sessions_;
  uint64_t last_applied_{0};
  uint64_t published_applied_index_{0};
};

/** Coordinates canonical capture, immutable publication, fallback, and two-generation retention. */
class SnapshotManager {
 public:
  SnapshotManager(NodeDirectory *node_directory, std::shared_ptr<DurableStorage> storage)
      : node_directory_(node_directory),
        storage_(std::move(storage)),
        manifests_(node_directory == nullptr ? std::filesystem::path{} : node_directory->StateDirectory(), storage_) {}

  auto CreateSnapshot(const Catalog &catalog, const SessionTable &sessions, StateVisibilityLatch *visibility_latch,
                      uint64_t generation, uint64_t last_included_index, uint64_t last_included_term) -> StateManifest;

  auto Recover(const std::function<bool(uint64_t)> &has_bridge_log = {}, size_t buffer_pool_size = 128)
      -> std::unique_ptr<RecoveredSnapshot>;

  /** Delete generations older than the newest two, then notify the log compactor of the new oldest boundary. */
  void PruneToTwo(const std::function<bool(uint64_t)> &can_recover_from,
                  const std::function<void(uint64_t)> &oldest_boundary_advanced);

 private:
  static auto SnapshotDirectoryName(uint64_t generation) -> std::string;

  NodeDirectory *node_directory_;
  std::shared_ptr<DurableStorage> storage_;
  StateManifestStore manifests_;
};

}  // namespace bustub
