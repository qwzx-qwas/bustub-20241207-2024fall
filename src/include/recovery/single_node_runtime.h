//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// single_node_runtime.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "distributed/bustub_state_machine.h"
#include "raft/stable_store.h"
#include "recovery/command_log.h"
#include "recovery/snapshot_manager.h"

namespace bustub {

struct SingleNodeRuntimeOptions {
  size_t buffer_pool_size_{128};
  CommandLogOptions log_options_{};
};

/**
 * Production-shaped term-0 recovery loop used before Raft transport is attached.
 *
 * It is deliberately strict: the working database is always recreated from a validated snapshot, only the durable
 * committed suffix is replayed, and every new command crosses log durability and HARD_STATE commit durability before
 * the public FSM is changed.
 */
class SingleNodeCommandRuntime {
 public:
  static auto Open(const std::filesystem::path &root, std::shared_ptr<DurableStorage> storage = nullptr,
                   SingleNodeRuntimeOptions options = {}) -> std::unique_ptr<SingleNodeCommandRuntime>;

  auto Commit(const TransactionCommandBatch &batch) -> std::vector<std::byte>;
  auto CommitSql(const std::string &sql, uint64_t client_id, uint64_t request_id) -> std::vector<std::byte>;
  auto CreateSnapshot() -> StateManifest;

  auto GetRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
      -> std::optional<std::pair<TupleMeta, Tuple>>;
  auto GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>>;

  auto CatalogForRead() const -> Catalog * { return recovered_->catalog_.get(); }
  auto CommitIndex() const -> uint64_t { return stable_store_->State().commit_index_; }
  auto LastApplied() const -> uint64_t { return state_machine_->LastApplied(); }
  auto PublishedAppliedIndex() const -> uint64_t { return state_machine_->PublishedAppliedIndex(); }
  auto SnapshotGeneration() const -> uint64_t { return recovered_->manifest_.generation_; }
  auto SnapshotBaseIndex() const -> uint64_t { return recovered_->manifest_.last_included_index_; }
  auto LastLogIndex() const -> uint64_t { return command_log_->LastLogIndex(); }

 private:
  SingleNodeCommandRuntime(std::shared_ptr<DurableStorage> storage, std::unique_ptr<NodeDirectory> node_directory,
                           SingleNodeRuntimeOptions options);

  void RecoverOrBootstrap();
  void BootstrapEmptySnapshot();
  auto HasFormalRecoveryState() const -> bool;
  auto HasBridgeLog(uint64_t snapshot_index) const -> bool;
  auto NextSnapshotGeneration() const -> uint64_t;
  auto CommitLocked(const TransactionCommandBatch &batch) -> std::vector<std::byte>;

  std::shared_ptr<DurableStorage> storage_;
  std::unique_ptr<NodeDirectory> node_directory_;
  SingleNodeRuntimeOptions options_;
  std::unique_ptr<StableStore> stable_store_;
  std::unique_ptr<SnapshotManager> snapshot_manager_;
  StateVisibilityLatch visibility_;
  std::unique_ptr<RecoveredSnapshot> recovered_;
  std::unique_ptr<CommandLog> command_log_;
  std::unique_ptr<BusTubStateMachine> state_machine_;
  mutable std::mutex write_mutex_;
};

}  // namespace bustub
