//===----------------------------------------------------------------------===//
// BusTub: node-local storage lifecycle and service state (F33/F34).
//===----------------------------------------------------------------------===//
#pragma once

#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

#include "storage/disk/io_executor.h"
#include "storage/disk/metadata_engine.h"

namespace bustub {

enum class NodeStoragePhase { Stopped, Starting, Recovering, Serving, Draining, Failed };
enum class NodeStorageCondition : uint32_t {
  RecoveryFailed = 1U << 0,
  IOFault = 1U << 1,
  ResourcePressure = 1U << 2,
  BootstrapRedundancyLost = 1U << 3,
  BootstrapRepairFailed = 1U << 4
};

struct NodeStorageView {
  uint64_t generation_{0};
  NodeStoragePhase phase_{NodeStoragePhase::Stopped};
  uint32_t conditions_{0};
  std::optional<NodeStorageCondition> primary_condition_;
  bool metadata_read_{false};
  bool metadata_write_{false};
  bool metadata_maintenance_{false};
  std::exception_ptr error_;
  std::exception_ptr repair_error_;
};

struct NodeStorageOptions {
  std::filesystem::path device_path_;
  BlockDeviceOptions device_;
  IOExecutorOptions io_;
  BootstrapIdentity identity_;
  JournalIdentity journal_identity_;
  JournalOptions journal_;
  MetadataOptions metadata_;
  BootstrapRepairOptions repair_;
};

/** Owns one device -> executor -> bootstrap -> B/Journal stack exclusively.
 * S5 exposes local metadata readiness, NOT SQL/Raft readiness. No automatic
 * format, migration, checkpoint on Close, or retry of an uncertain write.
 * Explicit options retain the existing component budgets and IO semantics.
 *
 * Create/Open perform real work on their caller, without holding the state lock
 * across IO. State/Close may run concurrently with startup. Close invalidates
 * publication, drains accepted calls and repair, then shuts down IO and device.
 * A cancelled startup throws NotReady after cleanup. Reopen requires Close and
 * constructs fresh components; old snapshots remain immutable RAM views.
 * Calls may overlap after opening; B still owns transaction ordering/admission.
 * Destruction requires callers to finish; use explicit Close to observe errors.
 */
class NodeStorage {
 public:
  explicit NodeStorage(NodeStorageOptions options);
  ~NodeStorage();
  NodeStorage(const NodeStorage &) = delete;
  auto operator=(const NodeStorage &) -> NodeStorage & = delete;

  void Create(const BootstrapLayout &layout);
  void Open();
  auto State() const -> NodeStorageView;
  auto Read() -> MetadataSnapshot;
  auto Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations) -> JournalResult;
  auto Writeback(size_t max_pages) -> MetadataWritebackResult;
  auto Checkpoint(size_t max_pages) -> MetadataCheckpointResult;
  void Close();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
