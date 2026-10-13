//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// persistent_state.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <exception>
#include <filesystem>
#include <memory>

#include "raft/log_store.h"
#include "raft/snapshot_store.h"
#include "raft/stable_store.h"
#include "raft/state_machine.h"
#include "recovery/durable_storage.h"

namespace bustub {

/** The three durable stores after their cross-file recovery oracle has passed. */
struct RecoveredRaftPersistentState {
  std::unique_ptr<StableStore> stable_store_;
  std::unique_ptr<LogStore> log_store_;
  std::unique_ptr<SnapshotStore> snapshot_store_;
};

/** Read-only preparation. Ownership is transferred once to the validated open. */
struct PreparedRaftStores {
  std::unique_ptr<StableStore> stable_;
  std::unique_ptr<SnapshotStore> snapshots_;
  std::unique_ptr<LogStore> log_;
  std::exception_ptr log_error_;
};
auto PrepareRaftStores(const std::shared_ptr<RaftObjectStorage> &objects) -> PreparedRaftStores;
auto RecoverRaftPersistentState(std::shared_ptr<RaftObjectStorage> objects,
                                const std::shared_ptr<RaftStateMachine> &machine, PreparedRaftStores prepared)
    -> RecoveredRaftPersistentState;

/**
 * Recover HARD_STATE, the mutation journal, and published snapshots as one
 * ordered state. The supplied state machine must be empty; a selected latest
 * snapshot is fully installed before any dependent durable repair is allowed.
 */
auto RecoverRaftPersistentState(const std::filesystem::path &raft_directory,
                                const std::shared_ptr<DurableStorage> &storage,
                                const std::shared_ptr<RaftStateMachine> &state_machine) -> RecoveredRaftPersistentState;

/** Object deployment: the explicit namespace and local storage are already open. */
auto RecoverRaftPersistentState(std::shared_ptr<RaftObjectStorage> objects,
                                const std::shared_ptr<RaftStateMachine> &state_machine) -> RecoveredRaftPersistentState;

}  // namespace bustub
