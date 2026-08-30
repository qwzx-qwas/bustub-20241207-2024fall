//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// persistent_state.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/persistent_state.h"

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <utility>

namespace bustub {

auto RecoverRaftPersistentState(const std::filesystem::path &raft_directory,
                                const std::shared_ptr<DurableStorage> &storage,
                                const std::shared_ptr<RaftStateMachine> &state_machine)
    -> RecoveredRaftPersistentState {
  if (raft_directory.empty() || storage == nullptr || state_machine == nullptr || state_machine->LastApplied() != 0) {
    throw std::runtime_error("invalid Raft persistent-state recovery configuration");
  }

  auto stable_store = StableStore::Open(raft_directory, storage);
  auto snapshot_store = SnapshotStore::Open(raft_directory / "snapshots", storage);
  const auto latest_snapshot = snapshot_store->Latest();
  const auto latest_index = latest_snapshot.has_value() ? latest_snapshot->last_included_index_ : 0;
  const auto recovery_snapshot = snapshot_store->OldestRetained();
  const auto recovery_index = recovery_snapshot.has_value() ? recovery_snapshot->last_included_index_ : 0;
  const auto recovery_term = recovery_snapshot.has_value() ? recovery_snapshot->last_included_term_ : 0;
  const auto effective_commit = std::max(stable_store->State().commit_index_, latest_index);

  std::optional<LogStoreRecoveryProbe> probe;
  try {
    probe = LogStore::ProbeRecovery(raft_directory / "log", storage, effective_commit, recovery_index, latest_index);
  } catch (...) {
    if (!latest_snapshot.has_value() || effective_commit != latest_index) {
      throw;
    }
  }

  const bool latest_boundary_matches =
      latest_snapshot.has_value() && probe.has_value() &&
      probe->latest_boundary_term_ == std::optional<uint64_t>{latest_snapshot->last_included_term_};
  bool recovery_boundary_matches = false;
  bool log_already_uses_latest = false;
  if (probe.has_value()) {
    if (!recovery_snapshot.has_value()) {
      recovery_boundary_matches = probe->snapshot_base_index_ == 0 && probe->snapshot_base_term_ == 0;
    } else if (probe->snapshot_base_index_ < recovery_index) {
      recovery_boundary_matches = probe->recovery_boundary_term_ == std::optional<uint64_t>{recovery_term};
    } else if (probe->snapshot_base_index_ == recovery_index) {
      recovery_boundary_matches = probe->snapshot_base_term_ == recovery_term;
    }
    log_already_uses_latest = latest_snapshot.has_value() && probe->snapshot_base_index_ == latest_index &&
                              probe->snapshot_base_term_ == latest_snapshot->last_included_term_;
    recovery_boundary_matches = recovery_boundary_matches || log_already_uses_latest;
  }

  if (!latest_snapshot.has_value()) {
    if (!probe.has_value() || !recovery_boundary_matches) {
      throw std::runtime_error("Raft log has no matching state-machine recovery base");
    }
    return {std::move(stable_store), LogStore::Open(raft_directory / "log", storage, effective_commit),
            std::move(snapshot_store)};
  }

  const bool latest_covers_commit = effective_commit == latest_index;
  if (!latest_covers_commit) {
    // H > S can only be reconstructed by replaying every committed entry in
    // (S, H]. A term mismatch at S means this suffix belongs to another log;
    // neither the snapshot nor a lower commit index may be substituted.
    if (!latest_boundary_matches) {
      throw std::runtime_error("Raft committed suffix does not match the latest snapshot boundary");
    }
  }

  const bool rebuild_from_latest = latest_covers_commit && !latest_boundary_matches;
  const bool promote_latest_recovery = latest_boundary_matches && !recovery_boundary_matches;
  // Validate the complete FSM image before advancing H, cleaning the journal,
  // or pruning a recovery generation. This also keeps RaftNode construction
  // free of durable side effects before snapshot decoding can fail.
  state_machine->InstallSnapshotFile(snapshot_store->PayloadFile(*latest_snapshot), latest_index);
  const auto hard_state = stable_store->State();
  if (hard_state.commit_index_ < latest_index) {
    stable_store->Update(hard_state.current_term_, hard_state.voted_for_, latest_index);
  }

  std::unique_ptr<LogStore> log_store;
  if (rebuild_from_latest) {
    // This is the sole destructive escape hatch. Validate the complete FSM
    // image before advancing H or replacing any pre-install log material.
    log_store = LogStore::RebuildFromVerifiedSnapshot(raft_directory / "log", storage, latest_index, latest_index,
                                                      latest_snapshot->last_included_term_);
    snapshot_store->RetainOnlyLatest();
  } else {
    auto selected_recovery_index = recovery_index;
    auto selected_recovery_term = recovery_term;
    const bool retire_previous = (promote_latest_recovery || log_already_uses_latest) && recovery_index != latest_index;
    if (retire_previous) {
      selected_recovery_index = latest_index;
      selected_recovery_term = latest_snapshot->last_included_term_;
    }
    log_store = LogStore::Open(raft_directory / "log", storage, effective_commit, selected_recovery_index,
                               selected_recovery_term);
    if (retire_previous) {
      snapshot_store->RetainOnlyLatest();
    }
  }
  return {std::move(stable_store), std::move(log_store), std::move(snapshot_store)};
}

}  // namespace bustub
