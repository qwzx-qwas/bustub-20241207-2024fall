//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// persistent_state.h
//
//===----------------------------------------------------------------------===//

#pragma once

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

/**
 * Recover HARD_STATE, the mutation journal, and published snapshots as one
 * ordered state. The supplied state machine must be empty; a selected latest
 * snapshot is fully installed before any dependent durable repair is allowed.
 */
auto RecoverRaftPersistentState(const std::filesystem::path &raft_directory,
                                const std::shared_ptr<DurableStorage> &storage,
                                const std::shared_ptr<RaftStateMachine> &state_machine) -> RecoveredRaftPersistentState;

}  // namespace bustub
