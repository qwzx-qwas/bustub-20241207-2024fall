//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_node_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <array>
#include <filesystem>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>

#include "../recovery/power_loss_storage.h"  // NOLINT(build/include_subdir)
#include "common/byte_codec.h"
#include "gtest/gtest.h"
#include "raft/in_memory_raft_transport.h"
#include "raft/persistent_state.h"
#include "raft/raft_node.h"

namespace bustub {

class RaftNodeTestPeer {
 public:
  static void AdvanceDurableCommitWithoutApply(RaftNode *node, uint64_t commit_index) {
    node->PersistHardState(node->hard_state_.current_term_, node->hard_state_.voted_for_, commit_index);
    node->AdvanceLogCommitOrStop(commit_index);
  }

  static void RebaseLogWithoutInstallingStateMachine(RaftNode *node, uint64_t index, uint64_t term) {
    node->InstallLogSnapshotBaseDurably(index, term, true);
  }
};

namespace {

auto MakeFixedElectionTimeoutSource(uint64_t timeout_ms) -> ElectionTimeoutSource {
  return [timeout_ms](uint64_t minimum_ms, uint64_t maximum_ms) {
    if (timeout_ms < minimum_ms || timeout_ms > maximum_ms) {
      throw std::runtime_error("fixed test election timeout is outside the configured interval");
    }
    return timeout_ms;
  };
}

class ThreeNodeKvCluster {
 public:
  explicit ThreeNodeKvCluster(std::string_view suffix)
      : root_(std::filesystem::temp_directory_path() /
              ("bustub-raft-cluster-" + std::to_string(getpid()) + "-" + std::string(suffix))),
        storage_(std::make_shared<PosixDurableStorage>()),
        transport_(std::make_shared<InMemoryRaftTransport>()) {
    storage_->RemoveTree(root_);
    for (size_t offset = 0; offset < nodes_.size(); offset++) {
      const auto id = static_cast<NodeId>(offset + 1);
      const auto raft_directory = root_ / ("node-" + std::to_string(id)) / "raft";
      machines_[offset] = std::make_shared<KvStateMachine>();
      auto stable = StableStore::Open(raft_directory, storage_);
      auto log = LogStore::Open(raft_directory / "log", storage_, 0);
      auto snapshots = SnapshotStore::Open(raft_directory / "snapshots", storage_);
      nodes_[offset] = std::make_unique<RaftNode>(
          RaftNodeConfig{id, {1, 2, 3}, 100, 300, 50, "test-group", MakeFixedElectionTimeoutSource(100U * id)},
          transport_, std::move(stable), std::move(log), machines_[offset], std::move(snapshots));
    }
    for (size_t offset = 0; offset < nodes_.size(); offset++) {
      const auto id = static_cast<NodeId>(offset + 1);
      transport_->Register(
          id, [this, offset](NodeId from, const RaftMessage &message) { nodes_[offset]->Receive(from, message); });
    }
  }

  ~ThreeNodeKvCluster() {
    for (NodeId id = 1; id <= 3; id++) {
      transport_->Unregister(id);
    }
    storage_->RemoveTree(root_);
  }

  auto Node(NodeId id) -> RaftNode & { return *nodes_.at(id - 1); }
  auto Machine(NodeId id) -> KvStateMachine & { return *machines_.at(id - 1); }
  auto Transport() -> InMemoryRaftTransport & { return *transport_; }

  void Restart(NodeId id) {
    const auto offset = static_cast<size_t>(id - 1);
    transport_->Unregister(id);
    nodes_[offset].reset();
    machines_[offset] = std::make_shared<KvStateMachine>();
    const auto raft_directory = root_ / ("node-" + std::to_string(id)) / "raft";
    auto stable = StableStore::Open(raft_directory, storage_);
    auto snapshots = SnapshotStore::Open(raft_directory / "snapshots", storage_);
    const auto latest = snapshots->Latest();
    const auto snapshot_index = latest.has_value() ? latest->last_included_index_ : 0;
    const auto recovery_snapshot = snapshots->OldestRetained();
    const auto snapshot_base_index = recovery_snapshot.has_value() ? recovery_snapshot->last_included_index_ : 0;
    const auto snapshot_base_term = recovery_snapshot.has_value() ? recovery_snapshot->last_included_term_ : 0;
    const auto effective_commit = std::max(stable->State().commit_index_, snapshot_index);
    auto log =
        LogStore::Open(raft_directory / "log", storage_, effective_commit, snapshot_base_index, snapshot_base_term);
    nodes_[offset] = std::make_unique<RaftNode>(
        RaftNodeConfig{id, {1, 2, 3}, 100, 300, 50, "test-group", MakeFixedElectionTimeoutSource(100U * id)},
        transport_, std::move(stable), std::move(log), machines_[offset], std::move(snapshots));
    transport_->Register(
        id, [this, offset](NodeId from, const RaftMessage &message) { nodes_[offset]->Receive(from, message); });
  }

  void ElectOne() {
    Node(1).Tick(100);
    Transport().DeliverAll();
    ASSERT_EQ(Node(1).Role(), RaftRole::LEADER);
  }

  void CorruptLatestSnapshot(NodeId id) {
    const auto snapshot = Node(id).LatestSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    const auto path = root_ / ("node-" + std::to_string(id)) / "raft" / "snapshots" / ("SNAPSHOT-" + [&] {
                        std::ostringstream value;
                        value << std::setw(20) << std::setfill('0') << snapshot->generation_;
                        return value.str();
                      }());
    auto bytes = storage_->ReadFile(path, SnapshotStore::MAX_SNAPSHOT_BYTES + 4096);
    bytes[bytes.size() / 2] ^= std::byte{1};
    storage_->WriteFile(path, bytes);
    storage_->SyncFile(path);
  }

  void SetNodeOnePartitioned(bool partitioned) {
    for (NodeId peer : {NodeId{2}, NodeId{3}}) {
      Transport().SetLinkEnabled(1, peer, !partitioned);
      Transport().SetLinkEnabled(peer, 1, !partitioned);
    }
  }

 private:
  std::filesystem::path root_;
  std::shared_ptr<PosixDurableStorage> storage_;
  std::shared_ptr<InMemoryRaftTransport> transport_;
  std::array<std::shared_ptr<KvStateMachine>, 3> machines_;
  std::array<std::unique_ptr<RaftNode>, 3> nodes_;
};

class FaultInjectedNode {
 public:
  explicit FaultInjectedNode(std::string_view suffix)
      : root_(std::filesystem::temp_directory_path() /
              ("bustub-raft-node-fault-" + std::to_string(getpid()) + "-" + std::string(suffix))),
        storage_(std::make_shared<PowerLossStorage>(root_)),
        transport_(std::make_shared<InMemoryRaftTransport>()),
        machine_(std::make_shared<KvStateMachine>()) {
    storage_->DisableFailure();
    storage_->RemoveTree(root_);
    const auto raft_directory = root_ / "raft";
    node_ = std::make_unique<RaftNode>(
        RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "fault-test", MakeFixedElectionTimeoutSource(100)}, transport_,
        StableStore::Open(raft_directory, storage_), LogStore::Open(raft_directory / "log", storage_, 0), machine_,
        SnapshotStore::Open(raft_directory / "snapshots", storage_));
  }

  ~FaultInjectedNode() {
    node_.reset();
    storage_->DisableFailure();
    storage_->RemoveTree(root_);
  }

  auto Node() -> RaftNode & { return *node_; }
  auto Storage() -> PowerLossStorage & { return *storage_; }
  auto Transport() -> InMemoryRaftTransport & { return *transport_; }

  void ElectReady() {
    node_->Tick(100);
    transport_->Clear();
    node_->Receive(2, RequestVoteResponse{1, true});
    const auto requests = transport_->TakeAll();
    uint64_t request_id = 0;
    for (const auto &envelope : requests) {
      if (envelope.to_ == 2 && std::holds_alternative<AppendEntriesRequest>(envelope.message_)) {
        request_id = std::get<AppendEntriesRequest>(envelope.message_).request_id_;
      }
    }
    if (request_id == 0) {
      throw std::runtime_error("test Leader did not send its NOOP");
    }
    node_->Receive(2, AppendEntriesResponse{1, request_id, true, 1, std::nullopt, 0, std::nullopt});
    transport_->Clear();
    if (!node_->LeaderReady()) {
      throw std::runtime_error("test Leader did not commit its NOOP");
    }
  }

 private:
  std::filesystem::path root_;
  std::shared_ptr<PowerLossStorage> storage_;
  std::shared_ptr<InMemoryRaftTransport> transport_;
  std::shared_ptr<KvStateMachine> machine_;
  std::unique_ptr<RaftNode> node_;
};

struct InstallRecoveryState {
  uint64_t commit_index_;
  uint64_t last_applied_;
  std::optional<std::string> value_;

  friend auto operator==(const InstallRecoveryState &lhs, const InstallRecoveryState &rhs) -> bool {
    return lhs.commit_index_ == rhs.commit_index_ && lhs.last_applied_ == rhs.last_applied_ && lhs.value_ == rhs.value_;
  }
};

auto MakeKvSnapshotPayload() -> std::vector<std::byte> {
  PosixDurableStorage storage;
  const auto root = std::filesystem::temp_directory_path() / ("bustub-raft-install-source-" + std::to_string(getpid()));
  storage.RemoveTree(root);
  storage.CreateDirectories(root);
  KvStateMachine machine;
  machine.Apply(
      {1, 1, 1, EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "installed", "snapshot-value"})});
  const auto path = root / "payload";
  machine.CreateSnapshotFile(path);
  auto payload = storage.ReadFile(path, SnapshotStore::MAX_SNAPSHOT_BYTES);
  storage.RemoveTree(root);
  return payload;
}

auto RunInstallSnapshotCrash(const std::vector<std::byte> &payload, std::optional<StorageFaultPlan> plan)
    -> AtomicDurabilityRun<InstallRecoveryState> {
  const auto root = std::filesystem::temp_directory_path() / ("bustub-raft-install-crash-" + std::to_string(getpid()));
  auto storage = std::make_shared<PowerLossStorage>(root);
  storage->DisableFailure();
  storage->RemoveTree(root);
  const auto raft_directory = root / "raft";
  auto transport = std::make_shared<InMemoryRaftTransport>();
  auto machine = std::make_shared<KvStateMachine>();
  auto node = std::make_unique<RaftNode>(
      RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "install-crash", MakeFixedElectionTimeoutSource(100)}, transport,
      StableStore::Open(raft_directory, storage), LogStore::Open(raft_directory / "log", storage, 0), machine,
      SnapshotStore::Open(raft_directory / "snapshots", storage));
  // The component-level fixture has no NodeDirectory owner, so explicitly make its top-level raft directory entry
  // part of the baseline before exercising child-directory fsync boundaries.
  storage->SyncDirectory(root);
  storage->ResetEventHistory();
  if (plan.has_value()) {
    storage->FailAt(*plan);
  }
  try {
    node->Receive(
        2, InstallSnapshotRequest{1, 2, 1, "snapshot-1", 1, 1, 0, payload.size(), Crc32c(payload), true, payload});
  } catch (...) {
    if (!plan.has_value() || !storage->FaultTriggered()) {
      node.reset();
      machine.reset();
      storage->DisableFailure();
      storage->RemoveTree(root);
      throw;
    }
  }
  const auto events = storage->Events();
  const auto fault_triggered = storage->FaultTriggered();
  node.reset();
  machine.reset();
  storage->PowerLoss();

  auto recovered_machine = std::make_shared<KvStateMachine>();
  auto recovered_state = RecoverRaftPersistentState(raft_directory, storage, recovered_machine);
  auto recovered = std::make_unique<RaftNode>(
      RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "install-crash", MakeFixedElectionTimeoutSource(100)}, transport,
      std::move(recovered_state.stable_store_), std::move(recovered_state.log_store_), recovered_machine,
      std::move(recovered_state.snapshot_store_));
  InstallRecoveryState state{recovered->CommitIndex(), recovered->LastApplied(), recovered_machine->Get("installed")};
  recovered.reset();
  storage->DisableFailure();
  storage->RemoveTree(root);
  return {std::move(state), events, fault_triggered};
}

auto PutEntry(uint64_t index, uint64_t term, std::string key, std::string value) -> ReplicatedLogEntry {
  return {1, index, term, EntryType::KV_COMMAND,
          KvCommandCodec::Encode({1, KvOperation::PUT, std::move(key), std::move(value)})};
}

auto OldRecoverySnapshotPayload() -> std::vector<std::byte> {
  KvStateMachine machine;
  machine.Apply(PutEntry(1, 1, "inventory", "old"));
  return machine.CreateSnapshot();
}

auto LatestRecoverySnapshotPayload() -> std::vector<std::byte> {
  KvStateMachine machine;
  machine.Apply(PutEntry(1, 1, "inventory", "old"));
  machine.Apply(PutEntry(2, 1, "phase", "prepared"));
  machine.Apply(PutEntry(3, 2, "inventory", "snapshot"));
  return machine.CreateSnapshot();
}

auto IntermediateRecoverySnapshotPayload() -> std::vector<std::byte> {
  KvStateMachine machine;
  machine.Apply(PutEntry(1, 1, "inventory", "old"));
  machine.Apply(PutEntry(2, 1, "phase", "prepared"));
  return machine.CreateSnapshot();
}

struct RaftAuthorityFiles {
  std::vector<std::byte> log_;
  std::vector<std::byte> hard_state_;
  std::vector<std::byte> current_;

  friend auto operator==(const RaftAuthorityFiles &lhs, const RaftAuthorityFiles &rhs) -> bool {
    return lhs.log_ == rhs.log_ && lhs.hard_state_ == rhs.hard_state_ && lhs.current_ == rhs.current_;
  }
};

class LiveInstallSnapshotFixture {
 public:
  LiveInstallSnapshotFixture(std::string_view suffix, uint64_t boundary_term)
      : root_(std::filesystem::temp_directory_path() /
              ("bustub-raft-live-install-" + std::to_string(getpid()) + "-" + std::string(suffix))),
        storage_(std::make_shared<PowerLossStorage>(root_)),
        transport_(std::make_shared<InMemoryRaftTransport>()),
        machine_(std::make_shared<KvStateMachine>()) {
    storage_->DisableFailure();
    storage_->RemoveTree(root_);
    const auto raft_directory = RaftDirectory();
    auto log = LogStore::Open(raft_directory / "log", storage_, 0);
    log->Append({PutEntry(1, 1, "inventory", "old"), PutEntry(2, 1, "phase", "prepared"),
                 PutEntry(3, boundary_term, "inventory", "snapshot"), PutEntry(4, 3, "inventory", "suffix")});
    auto stable = StableStore::Open(raft_directory, storage_);
    stable->Update(3, std::nullopt, 1);
    auto snapshots = SnapshotStore::Open(raft_directory / "snapshots", storage_);
    static_cast<void>(snapshots->Publish(1, 1, OldRecoverySnapshotPayload(), true));
    storage_->SyncDirectory(raft_directory);
    storage_->SyncDirectory(root_);
    snapshots.reset();
    stable.reset();
    log.reset();

    snapshots = SnapshotStore::Open(raft_directory / "snapshots", storage_);
    log = LogStore::Open(raft_directory / "log", storage_, 1, 1, 1);
    node_ = std::make_unique<RaftNode>(
        RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "live-install", MakeFixedElectionTimeoutSource(100)}, transport_,
        StableStore::Open(raft_directory, storage_), std::move(log), machine_, std::move(snapshots));
    if (node_->CommitIndex() != 1 || node_->LastApplied() != 1 || machine_->Data() != OldData() ||
        node_->Log().LastLogIndex() != 4) {
      throw std::runtime_error("live InstallSnapshot fixture did not construct its nonempty baseline");
    }
  }

  ~LiveInstallSnapshotFixture() {
    node_.reset();
    storage_->DisableFailure();
    storage_->RemoveTree(root_);
  }

  static auto OldData() -> std::map<std::string, std::string> { return {{"inventory", "old"}}; }

  auto Node() -> RaftNode & { return *node_; }
  auto Machine() -> KvStateMachine & { return *machine_; }
  auto Transport() -> InMemoryRaftTransport & { return *transport_; }

  void AdvanceDurableCommitWithoutApply(uint64_t commit_index) {
    RaftNodeTestPeer::AdvanceDurableCommitWithoutApply(node_.get(), commit_index);
  }

  void RebaseLogWithoutInstallingStateMachine(uint64_t index, uint64_t term) {
    RaftNodeTestPeer::RebaseLogWithoutInstallingStateMachine(node_.get(), index, term);
  }

  void Install(std::string snapshot_id, uint64_t request_id, uint64_t snapshot_index, uint64_t snapshot_term,
               const std::vector<std::byte> &payload) {
    node_->Receive(2, InstallSnapshotRequest{3, 2, request_id, std::move(snapshot_id), snapshot_index, snapshot_term, 0,
                                             payload.size(), Crc32c(payload), true, payload});
  }

  auto AuthorityFiles() -> RaftAuthorityFiles {
    const auto raft_directory = RaftDirectory();
    return {storage_->ReadFile(raft_directory / "log" / "LOG-MUTATIONS", LogStoreOptions::MAXIMUM_JOURNAL_BYTES),
            storage_->ReadFile(raft_directory / "HARD_STATE", 4096),
            storage_->ReadFile(raft_directory / "snapshots" / "CURRENT", 4096)};
  }

 private:
  auto RaftDirectory() const -> std::filesystem::path { return root_ / "raft"; }

  std::filesystem::path root_;
  std::shared_ptr<PowerLossStorage> storage_;
  std::shared_ptr<InMemoryRaftTransport> transport_;
  std::shared_ptr<KvStateMachine> machine_;
  std::unique_ptr<RaftNode> node_;
};

auto TakeOnlyInstallSnapshotResponse(LiveInstallSnapshotFixture *fixture) -> InstallSnapshotResponse {
  auto envelopes = fixture->Transport().TakeAll();
  if (envelopes.size() != 1 || envelopes.front().from_ != 1 || envelopes.front().to_ != 2 ||
      !std::holds_alternative<InstallSnapshotResponse>(envelopes.front().message_)) {
    throw std::runtime_error("live InstallSnapshot did not produce exactly one response to its sender");
  }
  return std::get<InstallSnapshotResponse>(envelopes.front().message_);
}

class CrossFileRecoveryDisk {
 public:
  explicit CrossFileRecoveryDisk(std::string_view suffix)
      : root_(std::filesystem::temp_directory_path() /
              ("bustub-raft-cross-file-" + std::to_string(getpid()) + "-" + std::string(suffix))),
        storage_(std::make_shared<PowerLossStorage>(root_)) {
    storage_->DisableFailure();
    storage_->RemoveTree(root_);
  }

  ~CrossFileRecoveryDisk() {
    storage_->DisableFailure();
    storage_->RemoveTree(root_);
  }

  void Prepare(uint64_t boundary_term, bool include_committed_suffix, uint64_t hard_commit,
               bool invalid_latest_payload = false, uint64_t old_snapshot_term = 1) {
    const auto raft_directory = RaftDirectory();
    auto log = LogStore::Open(raft_directory / "log", storage_, 0);
    std::vector<ReplicatedLogEntry> entries{PutEntry(1, 1, "inventory", "old"), PutEntry(2, 1, "phase", "prepared"),
                                            PutEntry(3, boundary_term, "inventory", "snapshot")};
    if (include_committed_suffix) {
      entries.push_back(PutEntry(4, 3, "inventory", "suffix"));
    }
    log->Append(entries);
    auto stable = StableStore::Open(raft_directory, storage_);
    stable->Update(3, std::nullopt, hard_commit);
    auto snapshots = SnapshotStore::Open(raft_directory / "snapshots", storage_);
    static_cast<void>(snapshots->Publish(1, old_snapshot_term, OldRecoverySnapshotPayload(), true));
    const auto latest_payload = invalid_latest_payload
                                    ? std::vector<std::byte>{std::byte{0x7f}, std::byte{0x01}, std::byte{0x02}}
                                    : LatestRecoverySnapshotPayload();
    static_cast<void>(snapshots->Publish(3, 2, latest_payload, true));
    storage_->SyncDirectory(raft_directory);
    storage_->SyncDirectory(root_);
  }

  auto RaftDirectory() const -> std::filesystem::path { return root_ / "raft"; }
  auto Storage() const -> const std::shared_ptr<PowerLossStorage> & { return storage_; }

 private:
  std::filesystem::path root_;
  std::shared_ptr<PowerLossStorage> storage_;
};

auto OpenRecoveredKvNode(const std::filesystem::path &raft_directory, const std::shared_ptr<PowerLossStorage> &storage,
                         const std::shared_ptr<KvStateMachine> &machine) -> std::unique_ptr<RaftNode> {
  auto recovered = RecoverRaftPersistentState(raft_directory, storage, machine);
  return std::make_unique<RaftNode>(
      RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "cross-file-recovery", MakeFixedElectionTimeoutSource(100)},
      std::make_shared<InMemoryRaftTransport>(), std::move(recovered.stable_store_), std::move(recovered.log_store_),
      machine, std::move(recovered.snapshot_store_));
}

struct RecoveryRepairOracleState {
  uint64_t current_term_;
  uint64_t commit_index_;
  uint64_t last_applied_;
  uint64_t log_base_index_;
  uint64_t log_base_term_;
  uint64_t last_log_index_;
  uint64_t latest_generation_;
  uint64_t oldest_generation_;
  uint64_t latest_index_;
  uint64_t latest_term_;
  bool index_four_present_;
  std::map<std::string, std::string> data_;

  friend auto operator==(const RecoveryRepairOracleState &lhs, const RecoveryRepairOracleState &rhs) -> bool {
    return lhs.current_term_ == rhs.current_term_ && lhs.commit_index_ == rhs.commit_index_ &&
           lhs.last_applied_ == rhs.last_applied_ && lhs.log_base_index_ == rhs.log_base_index_ &&
           lhs.log_base_term_ == rhs.log_base_term_ && lhs.last_log_index_ == rhs.last_log_index_ &&
           lhs.latest_generation_ == rhs.latest_generation_ && lhs.oldest_generation_ == rhs.oldest_generation_ &&
           lhs.latest_index_ == rhs.latest_index_ && lhs.latest_term_ == rhs.latest_term_ &&
           lhs.index_four_present_ == rhs.index_four_present_ && lhs.data_ == rhs.data_;
  }
};

struct RecoveryRepairRun {
  RecoveryRepairOracleState recovered_state_;
  RecoveryRepairOracleState second_reopen_state_;
  StorageEventTopology events_;
  bool fault_triggered_;
  bool latest_installed_before_first_durable_event_;
};

auto ObserveRecoveryRepair(const std::filesystem::path &raft_directory,
                           const std::shared_ptr<PowerLossStorage> &storage) -> RecoveryRepairOracleState {
  auto machine = std::make_shared<KvStateMachine>();
  auto recovered = RecoverRaftPersistentState(raft_directory, storage, machine);
  const auto latest = recovered.snapshot_store_->Latest();
  const auto oldest = recovered.snapshot_store_->OldestRetained();
  if (!latest.has_value() || !oldest.has_value()) {
    throw std::runtime_error("recovery repair did not retain an authoritative snapshot");
  }
  auto node = std::make_unique<RaftNode>(
      RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "recovery-repair-matrix", MakeFixedElectionTimeoutSource(100)},
      std::make_shared<InMemoryRaftTransport>(), std::move(recovered.stable_store_), std::move(recovered.log_store_),
      machine, std::move(recovered.snapshot_store_));
  return {node->CurrentTerm(),
          node->CommitIndex(),
          node->LastApplied(),
          node->Log().SnapshotBaseIndex(),
          node->Log().SnapshotBaseTerm(),
          node->Log().LastLogIndex(),
          latest->generation_,
          oldest->generation_,
          latest->last_included_index_,
          latest->last_included_term_,
          node->Log().EntryAt(4).has_value(),
          machine->Data()};
}

auto RunRecoveryRepairCrash(std::optional<StorageFaultPlan> plan) -> RecoveryRepairRun {
  const auto suffix = plan.has_value() ? plan->Name() : std::string{"complete"};
  CrossFileRecoveryDisk disk("repair-matrix-" + suffix);
  // H=1, latest S=3/T=2, local TermAt(3)=1, previous generation retained,
  // and a real index-4 suffix force the exact-cover verified-rebuild path.
  disk.Prepare(1, true, 1);
  auto baseline_stable = StableStore::Open(disk.RaftDirectory(), disk.Storage());
  auto baseline_snapshots = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
  const auto baseline_latest = baseline_snapshots->Latest();
  const auto baseline_oldest = baseline_snapshots->OldestRetained();
  const auto baseline_log = LogStore::ProbeRecovery(disk.RaftDirectory() / "log", disk.Storage(), 3, 1, 3);
  if (baseline_stable->State().commit_index_ != 1 || !baseline_latest.has_value() ||
      baseline_latest->generation_ != 2 || baseline_latest->last_included_index_ != 3 ||
      baseline_latest->last_included_term_ != 2 || baseline_latest->payload_size_ == 0 ||
      !baseline_oldest.has_value() || baseline_oldest->generation_ != 1 || baseline_oldest->last_included_index_ != 1 ||
      baseline_oldest->payload_size_ == 0 || baseline_log.snapshot_base_index_ != 0 ||
      baseline_log.last_log_index_ != 4 || baseline_log.recovery_boundary_term_ != std::optional<uint64_t>{1} ||
      baseline_log.latest_boundary_term_ != std::optional<uint64_t>{1}) {
    throw std::runtime_error("recovery repair matrix did not construct its required nonempty mismatch baseline");
  }
  baseline_snapshots.reset();
  baseline_stable.reset();
  disk.Storage()->ResetEventHistory();
  if (plan.has_value()) {
    disk.Storage()->FailAt(*plan);
  }

  auto attempt_machine = std::make_shared<KvStateMachine>();
  std::optional<RecoveredRaftPersistentState> attempted;
  bool threw = false;
  try {
    attempted.emplace(RecoverRaftPersistentState(disk.RaftDirectory(), disk.Storage(), attempt_machine));
  } catch (...) {
    threw = true;
    if (!plan.has_value() || !disk.Storage()->FaultTriggered()) {
      throw;
    }
  }
  const std::map<std::string, std::string> latest_data{{"inventory", "snapshot"}, {"phase", "prepared"}};
  const bool latest_installed = attempt_machine->LastApplied() == 3 && attempt_machine->Data() == latest_data;
  const auto events = disk.Storage()->Events();
  const auto fault_triggered = disk.Storage()->FaultTriggered();
  if ((plan.has_value() && (!threw || !fault_triggered)) || (!plan.has_value() && threw)) {
    throw std::runtime_error("recovery repair fault plan did not produce the required outcome");
  }

  attempted.reset();
  attempt_machine.reset();
  disk.Storage()->DisableFailure();
  disk.Storage()->PowerLoss();
  const auto recovered = ObserveRecoveryRepair(disk.RaftDirectory(), disk.Storage());
  disk.Storage()->PowerLoss();
  const auto reopened = ObserveRecoveryRepair(disk.RaftDirectory(), disk.Storage());
  return {recovered, reopened, events, fault_triggered, latest_installed};
}

}  // namespace

TEST(RaftNodeTest, LiveInstallSnapshotFailStopsBeforeAuthorityMutationOnCommittedBoundaryMismatch) {
  LiveInstallSnapshotFixture fixture("committed-mismatch", 1);
  fixture.AdvanceDurableCommitWithoutApply(4);
  ASSERT_EQ(fixture.Node().CommitIndex(), 4);
  ASSERT_EQ(fixture.Node().LastApplied(), 1);
  ASSERT_EQ(fixture.Node().PublishedAppliedIndex(), 1);
  ASSERT_EQ(fixture.Node().Log().TermAt(3), std::optional<uint64_t>{1});
  const auto authority_before = fixture.AuthorityFiles();
  const auto data_before = fixture.Machine().Data();
  const auto applied_before = fixture.Machine().LastApplied();

  EXPECT_THROW(fixture.Install("committed-mismatch", 401, 3, 2, LatestRecoverySnapshotPayload()), std::runtime_error);

  EXPECT_EQ(fixture.Node().Role(), RaftRole::STOPPED);
  EXPECT_EQ(fixture.Node().CommitIndex(), 4);
  EXPECT_EQ(fixture.Node().LastApplied(), 1);
  EXPECT_EQ(fixture.Node().PublishedAppliedIndex(), 1);
  EXPECT_EQ(fixture.Machine().LastApplied(), applied_before);
  EXPECT_EQ(fixture.Machine().Data(), data_before);
  EXPECT_TRUE(fixture.AuthorityFiles() == authority_before);
  EXPECT_EQ(fixture.Transport().Pending(), 0);
}

TEST(RaftNodeTest, LiveInstallSnapshotFailStopsBeforeAuthorityMutationWhenCommittedBoundaryIsMissing) {
  LiveInstallSnapshotFixture fixture("committed-missing", 2);
  fixture.AdvanceDurableCommitWithoutApply(4);
  fixture.RebaseLogWithoutInstallingStateMachine(3, 2);
  ASSERT_EQ(fixture.Node().CommitIndex(), 4);
  ASSERT_EQ(fixture.Node().LastApplied(), 1);
  ASSERT_EQ(fixture.Node().PublishedAppliedIndex(), 1);
  ASSERT_EQ(fixture.Node().Log().SnapshotBaseIndex(), 3);
  ASSERT_EQ(fixture.Node().Log().TermAt(2), std::nullopt);
  ASSERT_TRUE(fixture.Node().Log().EntryAt(4).has_value());
  const auto authority_before = fixture.AuthorityFiles();
  const auto data_before = fixture.Machine().Data();
  const auto applied_before = fixture.Machine().LastApplied();

  EXPECT_THROW(fixture.Install("committed-missing", 402, 2, 1, IntermediateRecoverySnapshotPayload()),
               std::runtime_error);

  EXPECT_EQ(fixture.Node().Role(), RaftRole::STOPPED);
  EXPECT_EQ(fixture.Node().CommitIndex(), 4);
  EXPECT_EQ(fixture.Node().LastApplied(), 1);
  EXPECT_EQ(fixture.Node().PublishedAppliedIndex(), 1);
  EXPECT_EQ(fixture.Machine().LastApplied(), applied_before);
  EXPECT_EQ(fixture.Machine().Data(), data_before);
  EXPECT_TRUE(fixture.AuthorityFiles() == authority_before);
  EXPECT_EQ(fixture.Transport().Pending(), 0);
}

TEST(RaftNodeTest, LiveInstallSnapshotPreservesMatchingCommittedSuffixAndAppliesIt) {
  LiveInstallSnapshotFixture fixture("committed-match", 2);
  fixture.AdvanceDurableCommitWithoutApply(4);
  ASSERT_EQ(fixture.Node().CommitIndex(), 4);
  ASSERT_EQ(fixture.Node().LastApplied(), 1);

  fixture.Install("committed-match", 403, 3, 2, LatestRecoverySnapshotPayload());

  const auto response = TakeOnlyInstallSnapshotResponse(&fixture);
  EXPECT_EQ(response.term_, 3);
  EXPECT_EQ(response.request_id_, 403);
  EXPECT_TRUE(response.success_);
  EXPECT_FALSE(response.stale_);
  EXPECT_TRUE(response.complete_);
  EXPECT_EQ(response.match_index_, 3);
  EXPECT_EQ(fixture.Node().Role(), RaftRole::FOLLOWER);
  EXPECT_EQ(fixture.Node().CommitIndex(), 4);
  EXPECT_EQ(fixture.Node().LastApplied(), 4);
  EXPECT_EQ(fixture.Node().PublishedAppliedIndex(), 4);
  EXPECT_EQ(fixture.Node().Log().SnapshotBaseIndex(), 1);
  EXPECT_EQ(fixture.Node().Log().LastLogIndex(), 4);
  ASSERT_TRUE(fixture.Node().Log().EntryAt(4).has_value());
  EXPECT_EQ(fixture.Node().Log().EntryAt(4)->term_, 3);
  EXPECT_EQ(fixture.Machine().LastApplied(), 4);
  EXPECT_EQ(fixture.Machine().Data(),
            (std::map<std::string, std::string>{{"inventory", "suffix"}, {"phase", "prepared"}}));
  const auto latest = fixture.Node().LatestSnapshot();
  ASSERT_TRUE(latest.has_value());
  EXPECT_EQ(latest->last_included_index_, 3);
  EXPECT_EQ(latest->last_included_term_, 2);
  EXPECT_GT(latest->payload_size_, 0);
}

TEST(RaftNodeTest, LiveInstallSnapshotMayReplaceMismatchedUncommittedSuffixWhenSnapshotCoversCommit) {
  LiveInstallSnapshotFixture fixture("covering-mismatch", 1);
  ASSERT_EQ(fixture.Node().CommitIndex(), 1);
  ASSERT_EQ(fixture.Node().LastApplied(), 1);
  ASSERT_EQ(fixture.Node().Log().TermAt(3), std::optional<uint64_t>{1});

  fixture.Install("covering-mismatch", 404, 3, 2, LatestRecoverySnapshotPayload());

  const auto response = TakeOnlyInstallSnapshotResponse(&fixture);
  EXPECT_EQ(response.term_, 3);
  EXPECT_EQ(response.request_id_, 404);
  EXPECT_TRUE(response.success_);
  EXPECT_FALSE(response.stale_);
  EXPECT_TRUE(response.complete_);
  EXPECT_EQ(response.match_index_, 3);
  EXPECT_EQ(fixture.Node().Role(), RaftRole::FOLLOWER);
  EXPECT_EQ(fixture.Node().CommitIndex(), 3);
  EXPECT_EQ(fixture.Node().LastApplied(), 3);
  EXPECT_EQ(fixture.Node().PublishedAppliedIndex(), 3);
  EXPECT_EQ(fixture.Node().Log().SnapshotBaseIndex(), 3);
  EXPECT_EQ(fixture.Node().Log().SnapshotBaseTerm(), 2);
  EXPECT_EQ(fixture.Node().Log().LastLogIndex(), 3);
  EXPECT_FALSE(fixture.Node().Log().EntryAt(4).has_value());
  EXPECT_EQ(fixture.Machine().LastApplied(), 3);
  EXPECT_EQ(fixture.Machine().Data(),
            (std::map<std::string, std::string>{{"inventory", "snapshot"}, {"phase", "prepared"}}));
  const auto latest = fixture.Node().LatestSnapshot();
  ASSERT_TRUE(latest.has_value());
  EXPECT_EQ(latest->last_included_index_, 3);
  EXPECT_EQ(latest->last_included_term_, 2);
  EXPECT_GT(latest->payload_size_, 0);
}

TEST(RaftNodeTest, LiveInstallSnapshotRejectsInvalidInnerStateBeforeExactCoverAuthorityMutation) {
  LiveInstallSnapshotFixture fixture("exact-cover-invalid-inner", 1);
  fixture.AdvanceDurableCommitWithoutApply(3);
  ASSERT_EQ(fixture.Node().CommitIndex(), 3);
  ASSERT_EQ(fixture.Node().LastApplied(), 1);
  ASSERT_EQ(fixture.Node().PublishedAppliedIndex(), 1);
  ASSERT_EQ(fixture.Node().Log().TermAt(3), std::optional<uint64_t>{1});
  const auto authority_before = fixture.AuthorityFiles();
  const auto data_before = fixture.Machine().Data();
  const auto applied_before = fixture.Machine().LastApplied();
  const std::vector<std::byte> invalid_inner_snapshot{std::byte{0x7f}, std::byte{0x01}, std::byte{0x02}};

  fixture.Install("exact-cover-invalid-inner", 405, 3, 2, invalid_inner_snapshot);

  const auto response = TakeOnlyInstallSnapshotResponse(&fixture);
  EXPECT_EQ(response.term_, 3);
  EXPECT_EQ(response.request_id_, 405);
  EXPECT_FALSE(response.success_);
  EXPECT_FALSE(response.stale_);
  EXPECT_FALSE(response.complete_);
  EXPECT_EQ(fixture.Node().Role(), RaftRole::FOLLOWER);
  EXPECT_EQ(fixture.Node().CommitIndex(), 3);
  EXPECT_EQ(fixture.Node().LastApplied(), 1);
  EXPECT_EQ(fixture.Node().PublishedAppliedIndex(), 1);
  EXPECT_EQ(fixture.Machine().LastApplied(), applied_before);
  EXPECT_EQ(fixture.Machine().Data(), data_before);
  EXPECT_TRUE(fixture.AuthorityFiles() == authority_before);
}

// M3-T05: a fixed seed produces a replayable timeout sequence while retaining the production interval contract.
TEST(RaftNodeTest, SeededElectionTimeoutSourceIsDeterministicAndBounded) {
  auto first = MakeSeededElectionTimeoutSource(0x5eed);
  auto replay = MakeSeededElectionTimeoutSource(0x5eed);
  std::set<uint64_t> observed;
  for (size_t sample = 0; sample < 64; sample++) {
    const auto timeout = first(150, 300);
    EXPECT_EQ(timeout, replay(150, 300));
    EXPECT_GE(timeout, 150);
    EXPECT_LE(timeout, 300);
    observed.insert(timeout);
  }
  EXPECT_GT(observed.size(), 1);
}

TEST(RaftNodeTest, FailedElectionTermPersistenceSendsNoVoteRequest) {
  FaultInjectedNode fixture("failed-election-term");
  fixture.Storage().FailAt({StorageFaultPoint::BEFORE_WRITE, 1});
  EXPECT_THROW(fixture.Node().Tick(100), std::runtime_error);
  EXPECT_TRUE(fixture.Storage().FaultTriggered());
  EXPECT_EQ(fixture.Node().Role(), RaftRole::STOPPED);
  EXPECT_EQ(fixture.Node().CurrentTerm(), 0);
  EXPECT_EQ(fixture.Transport().Pending(), 0);
}

TEST(RaftNodeTest, FailedHigherTermPersistenceSendsNoRpcResponseForEveryEntryPoint) {
  const std::vector<RaftMessage> messages{
      RequestVoteRequest{1, 2, 0, 0},
      RequestVoteResponse{1, true},
      AppendEntriesRequest{1, 2, 1, 0, 0, {}, 0, std::nullopt},
      AppendEntriesResponse{1, 1, false, 0, std::nullopt, 1, std::nullopt},
      InstallSnapshotRequest{1, 2, 1, "snapshot", 1, 1, 0, 1, 0, true, {std::byte{0}}},
      InstallSnapshotResponse{1, 1, true, false, true, 1, 0}};
  for (size_t index = 0; index < messages.size(); index++) {
    SCOPED_TRACE(index);
    FaultInjectedNode fixture("failed-higher-term-" + std::to_string(index));
    fixture.Storage().FailAt({StorageFaultPoint::BEFORE_WRITE, 1});
    EXPECT_THROW(fixture.Node().Receive(2, messages[index]), std::runtime_error);
    EXPECT_TRUE(fixture.Storage().FaultTriggered());
    EXPECT_EQ(fixture.Node().Role(), RaftRole::STOPPED);
    EXPECT_EQ(fixture.Node().CurrentTerm(), 0);
    EXPECT_EQ(fixture.Transport().Pending(), 0);
  }
}

TEST(RaftNodeTest, FailedLocalProposalAppendSendsNothingAndCannotCommit) {
  for (const auto fault : {StorageFaultPoint::BEFORE_WRITE, StorageFaultPoint::AFTER_FSYNC}) {
    SCOPED_TRACE(static_cast<uint32_t>(fault));
    FaultInjectedNode fixture("failed-proposal-append-" + std::to_string(static_cast<uint32_t>(fault)));
    fixture.ElectReady();
    fixture.Storage().FailAt({fault, 1});
    EXPECT_THROW(
        fixture.Node().Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "lost", "value"})),
        std::runtime_error);
    EXPECT_TRUE(fixture.Storage().FaultTriggered());
    EXPECT_EQ(fixture.Node().Role(), RaftRole::STOPPED);
    EXPECT_EQ(fixture.Transport().Pending(), 0);
    EXPECT_EQ(fixture.Node().CommitIndex(), 1);
    EXPECT_EQ(fixture.Node().Log().LastLogIndex(), 1);
    EXPECT_EQ(fixture.Node().Propose(EntryType::KV_COMMAND,
                                     KvCommandCodec::Encode({1, KvOperation::PUT, "later", "must-not-append"})),
              std::nullopt);
  }
}

TEST(RaftNodeTest, ProposalPayloadAdmissionRejectsMalformedAndWrongTypeWithoutAppending) {
  FaultInjectedNode fixture("proposal-payload-admission");
  fixture.ElectReady();
  fixture.Storage().ResetEventHistory();
  const auto valid_kv = KvCommandCodec::Encode({1, KvOperation::PUT, "admission", "must-not-append"});

  EXPECT_THROW(fixture.Node().Propose(EntryType::KV_COMMAND, {std::byte{0x01}, std::byte{0x02}}), std::runtime_error);
  EXPECT_THROW(fixture.Node().Propose(EntryType::COMMAND_BATCH, valid_kv), std::runtime_error);

  EXPECT_EQ(fixture.Node().Role(), RaftRole::LEADER);
  EXPECT_EQ(fixture.Node().CommitIndex(), 1);
  EXPECT_EQ(fixture.Node().LastApplied(), 1);
  EXPECT_EQ(fixture.Node().Log().LastLogIndex(), 1);
  EXPECT_EQ(fixture.Transport().Pending(), 0);
  EXPECT_TRUE(fixture.Storage().Events().empty());
}

TEST(RaftNodeTest, RejectsASecondProposalUntilTheFirstProposalIsResolved) {
  FaultInjectedNode fixture("single-unresolved-proposal");
  fixture.ElectReady();

  ASSERT_EQ(
      fixture.Node().Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "first", "pending"})),
      2);
  const auto messages_after_first = fixture.Transport().Pending();
  ASSERT_GT(messages_after_first, 0);
  EXPECT_THROW(fixture.Node().Propose(EntryType::KV_COMMAND,
                                      KvCommandCodec::Encode({1, KvOperation::PUT, "second", "must-wait"})),
               std::runtime_error);
  EXPECT_EQ(fixture.Node().Role(), RaftRole::LEADER);
  EXPECT_EQ(fixture.Node().CommitIndex(), 1);
  EXPECT_EQ(fixture.Node().Log().LastLogIndex(), 2);
  EXPECT_EQ(fixture.Transport().Pending(), messages_after_first);
}

TEST(RaftNodeTest, InstallSnapshotCrashMatrixRecoversOnlyCompleteOldOrNewState) {
  const auto payload = MakeKvSnapshotPayload();
  const StorageEventTopology expected_events{
      {StorageFaultPoint::BEFORE_WRITE, 1, "raft/HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 1, "raft/HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 1, "raft/HARD_STATE", "raft/HARD_STATE.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 1, "raft", {}},
      {StorageFaultPoint::BEFORE_WRITE, 2, "raft/snapshots/SNAPSHOT-DOWNLOAD.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 2, "raft/snapshots/SNAPSHOT-DOWNLOAD.tmp", {}},
      {StorageFaultPoint::BEFORE_WRITE, 3, "raft/snapshots/SNAPSHOT-00000000000000000001.tmp", {}},
      {StorageFaultPoint::BEFORE_WRITE, 4, "raft/snapshots/SNAPSHOT-00000000000000000001.tmp", {}},
      {StorageFaultPoint::BEFORE_WRITE, 5, "raft/snapshots/SNAPSHOT-00000000000000000001.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 3, "raft/snapshots/SNAPSHOT-00000000000000000001.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 2, "raft/snapshots/SNAPSHOT-00000000000000000001",
       "raft/snapshots/SNAPSHOT-00000000000000000001.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 2, "raft/snapshots", {}},
      {StorageFaultPoint::BEFORE_WRITE, 6, "raft/snapshots/CURRENT.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 4, "raft/snapshots/CURRENT.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 3, "raft/snapshots/CURRENT", "raft/snapshots/CURRENT.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 3, "raft/snapshots", {}},
      {StorageFaultPoint::BEFORE_WRITE, 7, "raft/HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 5, "raft/HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 4, "raft/HARD_STATE", "raft/HARD_STATE.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 4, "raft", {}},
      {StorageFaultPoint::BEFORE_WRITE, 8, "raft/log/LOG-MUTATIONS.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 6, "raft/log/LOG-MUTATIONS.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 5, "raft/log/LOG-MUTATIONS", "raft/log/LOG-MUTATIONS.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 5, "raft/log", {}},
      {StorageFaultPoint::BEFORE_WRITE, 9, "raft/snapshots/SNAPSHOT-DOWNLOAD.tmp", {}},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 6, "raft/snapshots", {}},
  };
  VerifyAtomicDurableTransition(
      InstallRecoveryState{0, 0, std::nullopt}, InstallRecoveryState{1, 1, std::string{"snapshot-value"}},
      expected_events, [&](std::optional<StorageFaultPlan> plan) { return RunInstallSnapshotCrash(payload, plan); });
}

TEST(RaftNodeTest, RecoverPersistentStateNamedPowerLossMatrixConvergesByCrossFileOracle) {
  const StorageEventTopology expected_events{
      {StorageFaultPoint::BEFORE_WRITE, 1, "raft/HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 1, "raft/HARD_STATE.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 1, "raft/HARD_STATE", "raft/HARD_STATE.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 1, "raft", {}},
      {StorageFaultPoint::BEFORE_WRITE, 2, "raft/log/LOG-MUTATIONS.tmp", {}},
      {StorageFaultPoint::AFTER_FSYNC, 2, "raft/log/LOG-MUTATIONS.tmp", {}},
      {StorageFaultPoint::AFTER_RENAME, 2, "raft/log/LOG-MUTATIONS", "raft/log/LOG-MUTATIONS.tmp"},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 2, "raft/log", {}},
      {StorageFaultPoint::BEFORE_WRITE, 3, "raft/snapshots/SNAPSHOT-00000000000000000001", {}},
      {StorageFaultPoint::AFTER_DIR_FSYNC, 3, "raft/snapshots", {}},
  };
  const RecoveryRepairOracleState expected_state{3, 3, 3, 3, 2,     3,
                                                 2, 2, 3, 2, false, {{"inventory", "snapshot"}, {"phase", "prepared"}}};

  const auto complete = RunRecoveryRepairCrash(std::nullopt);
  EXPECT_FALSE(complete.fault_triggered_);
  EXPECT_TRUE(complete.latest_installed_before_first_durable_event_);
  EXPECT_TRUE(complete.recovered_state_ == expected_state);
  EXPECT_TRUE(complete.second_reopen_state_ == expected_state);
  ASSERT_EQ(complete.events_.size(), expected_events.size());
  for (size_t index = 0; index < expected_events.size(); index++) {
    SCOPED_TRACE(index);
    EXPECT_TRUE(complete.events_[index] == expected_events[index]);
  }

  for (size_t index = 0; index < expected_events.size(); index++) {
    const auto &event = expected_events[index];
    const StorageFaultPlan plan{event.point_, event.occurrence_};
    SCOPED_TRACE(plan.Name());
    const auto failed = RunRecoveryRepairCrash(plan);
    EXPECT_TRUE(failed.fault_triggered_);
    EXPECT_TRUE(failed.latest_installed_before_first_durable_event_);
    EXPECT_TRUE(failed.recovered_state_ == expected_state);
    EXPECT_TRUE(failed.second_reopen_state_ == expected_state);
    ASSERT_EQ(failed.events_.size(), index + 1);
    EXPECT_TRUE(std::equal(expected_events.begin(), expected_events.begin() + static_cast<ptrdiff_t>(index + 1),
                           failed.events_.begin()));
  }
}

TEST(RaftNodeTest, CoveringLatestSnapshotNormalizesMismatchedBridgeAfterNamedPruneCrash) {
  CrossFileRecoveryDisk disk("covering-latest");
  disk.Prepare(1, false, 3);
  auto snapshots = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
  disk.Storage()->FailAt({StorageFaultPoint::BEFORE_WRITE, 1});
  EXPECT_THROW(snapshots->RetainOnlyLatest(), std::runtime_error);
  ASSERT_TRUE(disk.Storage()->FaultTriggered());
  ASSERT_EQ(disk.Storage()->Events().size(), 1);
  EXPECT_EQ(disk.Storage()->Events().front(),
            (StorageEvent{StorageFaultPoint::BEFORE_WRITE, 1, "raft/snapshots/SNAPSHOT-00000000000000000001", {}}));
  snapshots.reset();
  disk.Storage()->PowerLoss();

  auto machine = std::make_shared<KvStateMachine>();
  auto node = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), machine);
  EXPECT_EQ(node->CommitIndex(), 3);
  EXPECT_EQ(node->LastApplied(), 3);
  EXPECT_EQ(machine->Get("inventory"), std::optional<std::string>{"snapshot"});
  EXPECT_EQ(machine->Get("phase"), std::optional<std::string>{"prepared"});
  EXPECT_EQ(node->Log().SnapshotBaseIndex(), 3);
  EXPECT_EQ(node->Log().SnapshotBaseTerm(), 2);
  EXPECT_EQ(node->Log().LastLogIndex(), 3);
  node.reset();

  auto retained = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
  ASSERT_TRUE(retained->Latest().has_value());
  ASSERT_TRUE(retained->OldestRetained().has_value());
  EXPECT_EQ(retained->Latest()->generation_, 2);
  EXPECT_EQ(retained->OldestRetained()->generation_, 2);
  retained.reset();

  auto reopened_machine = std::make_shared<KvStateMachine>();
  auto reopened = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), reopened_machine);
  EXPECT_EQ(reopened->CommitIndex(), 3);
  EXPECT_EQ(reopened->LastApplied(), 3);
  EXPECT_EQ(reopened_machine->Get("inventory"), std::optional<std::string>{"snapshot"});
  EXPECT_EQ(reopened->Log().SnapshotBaseIndex(), 3);
  EXPECT_EQ(reopened->Log().LastLogIndex(), 3);
}

TEST(RaftNodeTest, CoveringLatestSnapshotPreservesMatchingRecoveryBridge) {
  CrossFileRecoveryDisk disk("covering-matching");
  disk.Prepare(2, true, 3);
  auto machine = std::make_shared<KvStateMachine>();
  auto node = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), machine);
  EXPECT_EQ(node->CommitIndex(), 3);
  EXPECT_EQ(node->LastApplied(), 3);
  EXPECT_EQ(machine->Get("inventory"), std::optional<std::string>{"snapshot"});
  EXPECT_EQ(node->Log().SnapshotBaseIndex(), 1);
  EXPECT_EQ(node->Log().TermAt(3), std::optional<uint64_t>{2});
  EXPECT_EQ(node->Log().TermAt(4), std::optional<uint64_t>{3});
  EXPECT_EQ(node->Log().LastLogIndex(), 4);

  const auto latest = node->LatestSnapshot();
  ASSERT_TRUE(latest.has_value());
  node.reset();
  auto retained = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
  ASSERT_TRUE(retained->OldestRetained().has_value());
  ASSERT_TRUE(retained->Latest().has_value());
  EXPECT_EQ(retained->OldestRetained()->generation_, 1);
  EXPECT_EQ(retained->Latest()->generation_, 2);
  EXPECT_EQ(retained->Latest()->snapshot_id_, latest->snapshot_id_);
  retained.reset();

  auto reopened_machine = std::make_shared<KvStateMachine>();
  auto reopened = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), reopened_machine);
  EXPECT_EQ(reopened->CommitIndex(), 3);
  EXPECT_EQ(reopened->LastApplied(), 3);
  EXPECT_EQ(reopened_machine->Get("inventory"), std::optional<std::string>{"snapshot"});
  EXPECT_EQ(reopened->Log().SnapshotBaseIndex(), 1);
  EXPECT_EQ(reopened->Log().TermAt(4), std::optional<uint64_t>{3});
  EXPECT_EQ(reopened->Log().LastLogIndex(), 4);
}

TEST(RaftNodeTest, CoveringLatestPromotesMatchingBoundaryWhenPreviousBridgeDisagrees) {
  CrossFileRecoveryDisk disk("matching-latest-bad-previous");
  disk.Prepare(2, true, 3, false, 9);
  auto machine = std::make_shared<KvStateMachine>();
  auto node = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), machine);
  EXPECT_EQ(node->CommitIndex(), 3);
  EXPECT_EQ(node->LastApplied(), 3);
  EXPECT_EQ(machine->Get("inventory"), std::optional<std::string>{"snapshot"});
  EXPECT_EQ(node->Log().SnapshotBaseIndex(), 3);
  EXPECT_EQ(node->Log().SnapshotBaseTerm(), 2);
  EXPECT_EQ(node->Log().TermAt(4), std::optional<uint64_t>{3});
  EXPECT_EQ(node->Log().LastLogIndex(), 4);
  node.reset();

  auto retained = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
  ASSERT_TRUE(retained->Latest().has_value());
  ASSERT_TRUE(retained->OldestRetained().has_value());
  EXPECT_EQ(retained->Latest()->generation_, 2);
  EXPECT_EQ(retained->OldestRetained()->generation_, 2);
  retained.reset();

  auto reopened_machine = std::make_shared<KvStateMachine>();
  auto reopened = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), reopened_machine);
  EXPECT_EQ(reopened->CommitIndex(), 3);
  EXPECT_EQ(reopened->LastApplied(), 3);
  EXPECT_EQ(reopened->Log().TermAt(4), std::optional<uint64_t>{3});
  EXPECT_EQ(reopened->Log().LastLogIndex(), 4);
}

TEST(RaftNodeTest, CommitBeyondLatestPromotesMatchingBoundaryAndReplaysSuffix) {
  CrossFileRecoveryDisk disk("committed-matching-latest-bad-previous");
  disk.Prepare(2, true, 4, false, 9);
  auto machine = std::make_shared<KvStateMachine>();
  auto node = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), machine);
  EXPECT_EQ(node->CommitIndex(), 4);
  EXPECT_EQ(node->LastApplied(), 4);
  EXPECT_EQ(machine->Get("inventory"), std::optional<std::string>{"suffix"});
  EXPECT_EQ(node->Log().SnapshotBaseIndex(), 3);
  EXPECT_EQ(node->Log().SnapshotBaseTerm(), 2);
  EXPECT_EQ(node->Log().TermAt(4), std::optional<uint64_t>{3});

  auto retained = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
  ASSERT_TRUE(retained->Latest().has_value());
  ASSERT_TRUE(retained->OldestRetained().has_value());
  EXPECT_EQ(retained->Latest()->generation_, 2);
  EXPECT_EQ(retained->OldestRetained()->generation_, 2);
}

TEST(RaftNodeTest, CommitBeyondLatestFailsClosedForMismatchedOrMissingSuffix) {
  for (const auto &[suffix, boundary_term, include_suffix] :
       std::array<std::tuple<std::string_view, uint64_t, bool>, 2>{std::tuple{"mismatched", uint64_t{1}, true},
                                                                   std::tuple{"missing", uint64_t{2}, false}}) {
    SCOPED_TRACE(suffix);
    CrossFileRecoveryDisk disk(suffix);
    disk.Prepare(boundary_term, include_suffix, 4);
    const auto log_path = disk.RaftDirectory() / "log" / "LOG-MUTATIONS";
    const auto stable_path = disk.RaftDirectory() / "HARD_STATE";
    const auto current_path = disk.RaftDirectory() / "snapshots" / "CURRENT";
    const auto log_before = disk.Storage()->ReadFile(log_path, static_cast<size_t>(disk.Storage()->FileSize(log_path)));
    const auto stable_before =
        disk.Storage()->ReadFile(stable_path, static_cast<size_t>(disk.Storage()->FileSize(stable_path)));
    const auto current_before =
        disk.Storage()->ReadFile(current_path, static_cast<size_t>(disk.Storage()->FileSize(current_path)));
    auto machine = std::make_shared<KvStateMachine>();
    EXPECT_THROW(RecoverRaftPersistentState(disk.RaftDirectory(), disk.Storage(), machine), std::runtime_error);
    EXPECT_EQ(machine->LastApplied(), 0);
    EXPECT_EQ(disk.Storage()->ReadFile(log_path, static_cast<size_t>(disk.Storage()->FileSize(log_path))), log_before);
    EXPECT_EQ(disk.Storage()->ReadFile(stable_path, static_cast<size_t>(disk.Storage()->FileSize(stable_path))),
              stable_before);
    EXPECT_EQ(disk.Storage()->ReadFile(current_path, static_cast<size_t>(disk.Storage()->FileSize(current_path))),
              current_before);

    auto stable = StableStore::Open(disk.RaftDirectory(), disk.Storage());
    EXPECT_EQ(stable->State().commit_index_, 4);
    auto snapshots = SnapshotStore::Open(disk.RaftDirectory() / "snapshots", disk.Storage());
    ASSERT_TRUE(snapshots->Latest().has_value());
    ASSERT_TRUE(snapshots->OldestRetained().has_value());
    EXPECT_EQ(snapshots->Latest()->last_included_index_, 3);
    EXPECT_EQ(snapshots->OldestRetained()->last_included_index_, 1);
  }
}

TEST(RaftNodeTest, MatchingSuffixStillRejectsInvalidInnerSnapshotBeforeAnyDurableRepair) {
  CrossFileRecoveryDisk disk("invalid-inner-snapshot");
  disk.Prepare(2, true, 4, true);
  const auto log_path = disk.RaftDirectory() / "log" / "LOG-MUTATIONS";
  const auto stable_path = disk.RaftDirectory() / "HARD_STATE";
  const auto current_path = disk.RaftDirectory() / "snapshots" / "CURRENT";
  const auto log_before = disk.Storage()->ReadFile(log_path, static_cast<size_t>(disk.Storage()->FileSize(log_path)));
  const auto stable_before =
      disk.Storage()->ReadFile(stable_path, static_cast<size_t>(disk.Storage()->FileSize(stable_path)));
  const auto current_before =
      disk.Storage()->ReadFile(current_path, static_cast<size_t>(disk.Storage()->FileSize(current_path)));

  auto machine = std::make_shared<KvStateMachine>();
  EXPECT_THROW(RecoverRaftPersistentState(disk.RaftDirectory(), disk.Storage(), machine), std::runtime_error);
  EXPECT_EQ(machine->LastApplied(), 0);
  EXPECT_EQ(disk.Storage()->ReadFile(log_path, static_cast<size_t>(disk.Storage()->FileSize(log_path))), log_before);
  EXPECT_EQ(disk.Storage()->ReadFile(stable_path, static_cast<size_t>(disk.Storage()->FileSize(stable_path))),
            stable_before);
  EXPECT_EQ(disk.Storage()->ReadFile(current_path, static_cast<size_t>(disk.Storage()->FileSize(current_path))),
            current_before);
}

TEST(RaftNodeTest, CommitBeyondLatestReplaysOnlyAProvenMatchingSuffix) {
  CrossFileRecoveryDisk disk("matching-suffix");
  disk.Prepare(2, true, 4);
  auto machine = std::make_shared<KvStateMachine>();
  auto node = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), machine);
  EXPECT_EQ(node->CommitIndex(), 4);
  EXPECT_EQ(node->LastApplied(), 4);
  EXPECT_EQ(machine->Get("inventory"), std::optional<std::string>{"suffix"});
  EXPECT_EQ(machine->Get("phase"), std::optional<std::string>{"prepared"});
  EXPECT_EQ(node->Log().SnapshotBaseIndex(), 1);
  EXPECT_EQ(node->Log().TermAt(3), std::optional<uint64_t>{2});
  EXPECT_EQ(node->Log().TermAt(4), std::optional<uint64_t>{3});
  node.reset();

  auto reopened_machine = std::make_shared<KvStateMachine>();
  auto reopened = OpenRecoveredKvNode(disk.RaftDirectory(), disk.Storage(), reopened_machine);
  EXPECT_EQ(reopened->CommitIndex(), 4);
  EXPECT_EQ(reopened->LastApplied(), 4);
  EXPECT_EQ(reopened_machine->Get("inventory"), std::optional<std::string>{"suffix"});
  EXPECT_EQ(reopened->Log().SnapshotBaseIndex(), 1);
}

TEST(RaftNodeTest, DuplicateVoteOldTermAndLogFreshnessAreFailClosed) {
  FaultInjectedNode fixture("vote-rules");
  fixture.Node().Receive(2, RequestVoteRequest{1, 2, 0, 0});
  auto responses = fixture.Transport().TakeAll();
  ASSERT_EQ(responses.size(), 1);
  ASSERT_TRUE(std::holds_alternative<RequestVoteResponse>(responses[0].message_));
  EXPECT_TRUE(std::get<RequestVoteResponse>(responses[0].message_).vote_granted_);

  fixture.Node().Receive(2, RequestVoteRequest{1, 2, 0, 0});
  fixture.Node().Receive(3, RequestVoteRequest{1, 3, 0, 0});
  responses = fixture.Transport().TakeAll();
  ASSERT_EQ(responses.size(), 2);
  EXPECT_TRUE(std::get<RequestVoteResponse>(responses[0].message_).vote_granted_);
  EXPECT_FALSE(std::get<RequestVoteResponse>(responses[1].message_).vote_granted_);

  fixture.Node().Receive(2, AppendEntriesRequest{0, 2, 10, 0, 0, {}, 0, std::nullopt});
  responses = fixture.Transport().TakeAll();
  ASSERT_EQ(responses.size(), 1);
  EXPECT_FALSE(std::get<AppendEntriesResponse>(responses[0].message_).success_);
  EXPECT_EQ(fixture.Node().CurrentTerm(), 1);

  FaultInjectedNode stale_log("vote-log-freshness");
  stale_log.ElectReady();
  stale_log.Node().Receive(3, RequestVoteRequest{2, 3, 100, 0});
  responses = stale_log.Transport().TakeAll();
  ASSERT_EQ(responses.size(), 1);
  EXPECT_FALSE(std::get<RequestVoteResponse>(responses[0].message_).vote_granted_);
  EXPECT_EQ(stale_log.Node().CurrentTerm(), 2);
}

TEST(RaftNodeTest, SplitVoteAndOutOfOrderResponsesCannotAdvanceState) {
  FaultInjectedNode fixture("split-vote-ordering");
  fixture.Node().Tick(100);
  fixture.Transport().Clear();
  fixture.Node().Receive(2, RequestVoteResponse{1, false});
  fixture.Node().Receive(3, RequestVoteResponse{0, true});
  fixture.Node().Receive(9, RequestVoteResponse{1, true});
  EXPECT_EQ(fixture.Node().Role(), RaftRole::CANDIDATE);
  EXPECT_EQ(fixture.Transport().Pending(), 0);
  fixture.Node().Receive(2, RequestVoteResponse{1, true});
  EXPECT_EQ(fixture.Node().Role(), RaftRole::LEADER);

  auto requests = fixture.Transport().TakeAll();
  uint64_t node_two_request = 0;
  for (const auto &envelope : requests) {
    if (envelope.to_ == 2 && std::holds_alternative<AppendEntriesRequest>(envelope.message_)) {
      node_two_request = std::get<AppendEntriesRequest>(envelope.message_).request_id_;
    }
  }
  ASSERT_GT(node_two_request, 0);
  fixture.Node().Receive(2, AppendEntriesResponse{1, node_two_request - 1, false, 0, std::nullopt, 1, std::nullopt});
  EXPECT_EQ(fixture.Transport().Pending(), 0);
  fixture.Node().Receive(2, AppendEntriesResponse{1, node_two_request, false, 0, std::nullopt, 1, std::nullopt});
  EXPECT_EQ(fixture.Transport().Pending(), 1);
}

// M3-T06: injected fixed timeouts and logical time elect one Leader; production never uses these fixed values.
TEST(RaftNodeTest, ElectionReplicationMajorityCommitAndApply) {
  ThreeNodeKvCluster cluster("election");
  cluster.ElectOne();
  for (NodeId id = 1; id <= 3; id++) {
    EXPECT_EQ(cluster.Node(id).CurrentTerm(), 1);
    EXPECT_EQ(cluster.Node(id).CommitIndex(), 1);
    EXPECT_EQ(cluster.Node(id).LastApplied(), 1);
    EXPECT_EQ(cluster.Node(id).PublishedAppliedIndex(), 1);
    EXPECT_EQ(cluster.Node(id).Log().EntryAt(1)->type_, EntryType::NOOP);
  }

  const auto command = KvCommandCodec::Encode({1, KvOperation::PUT, "alpha", "one"});
  const auto index = cluster.Node(1).Propose(EntryType::KV_COMMAND, command);
  ASSERT_EQ(index, 2);
  cluster.Transport().DeliverAll();
  for (NodeId id = 1; id <= 3; id++) {
    EXPECT_EQ(cluster.Node(id).CommitIndex(), 2);
    EXPECT_EQ(cluster.Node(id).LastApplied(), 2);
    EXPECT_EQ(cluster.Machine(id).Get("alpha"), "one");
  }
}

// M3-T07: an isolated old Leader's uncommitted suffix is atomically replaced by the next Leader.
TEST(RaftNodeTest, OldLeaderConflictingSuffixIsReplaced) {
  ThreeNodeKvCluster cluster("old-leader");
  cluster.ElectOne();
  cluster.SetNodeOnePartitioned(true);

  ASSERT_EQ(
      cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "lost", "minority"})),
      2);
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(1).CommitIndex(), 1);
  EXPECT_FALSE(cluster.Machine(1).Get("lost").has_value());

  cluster.Node(2).Tick(200);
  cluster.Transport().DeliverAll();
  ASSERT_EQ(cluster.Node(2).Role(), RaftRole::LEADER);
  ASSERT_EQ(cluster.Node(2).CurrentTerm(), 2);
  ASSERT_EQ(cluster.Node(2).CommitIndex(), 2);

  cluster.SetNodeOnePartitioned(false);
  cluster.Node(2).Tick(250);
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(1).Role(), RaftRole::FOLLOWER);
  EXPECT_EQ(cluster.Node(1).CurrentTerm(), 2);
  EXPECT_EQ(cluster.Node(1).Log().TermAt(2), 2);
  EXPECT_EQ(cluster.Node(1).Log().EntryAt(2)->type_, EntryType::NOOP);
  EXPECT_FALSE(cluster.Machine(1).Get("lost").has_value());

  ASSERT_EQ(
      cluster.Node(2).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "kept", "majority"})),
      3);
  cluster.Transport().DeliverAll();
  for (NodeId id = 1; id <= 3; id++) {
    EXPECT_EQ(cluster.Node(id).CommitIndex(), 3);
    EXPECT_EQ(cluster.Machine(id).Get("kept"), "majority");
    EXPECT_FALSE(cluster.Machine(id).Get("lost").has_value());
  }
}

// M4-T02: a follower below the compacted base installs the canonical KV snapshot and then resumes log Apply.
TEST(RaftNodeTest, CompactedFollowerCatchesUpBySnapshot) {
  ThreeNodeKvCluster cluster("snapshot-catchup");
  cluster.ElectOne();
  cluster.Transport().SetLinkEnabled(1, 3, false);
  cluster.Transport().SetLinkEnabled(3, 1, false);

  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "a", "one"})),
            2);
  cluster.Transport().DeliverAll();
  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "b", "two"})),
            3);
  cluster.Transport().DeliverAll();
  const auto snapshot = cluster.Node(1).CreateSnapshot();
  EXPECT_EQ(snapshot.last_included_index_, 3);
  EXPECT_EQ(cluster.Node(1).Log().SnapshotBaseIndex(), 3);

  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "c", "three"})),
            4);
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(3).CommitIndex(), 1);

  cluster.Transport().SetLinkEnabled(1, 3, true);
  cluster.Transport().SetLinkEnabled(3, 1, true);
  cluster.Node(1).Tick(150);
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(3).CommitIndex(), 4);
  EXPECT_EQ(cluster.Node(3).LastApplied(), 4);
  EXPECT_EQ(cluster.Node(3).Log().SnapshotBaseIndex(), 3);
  EXPECT_EQ(cluster.Machine(3).Get("a"), "one");
  EXPECT_EQ(cluster.Machine(3).Get("b"), "two");
  EXPECT_EQ(cluster.Machine(3).Get("c"), "three");
}

// A follower fsync may exceed one heartbeat interval. The Leader must retry
// the same in-flight chunk without invalidating its real delayed ACK.
TEST(RaftNodeTest, HeartbeatsCannotStarveMultiChunkSnapshotProgress) {
  ThreeNodeKvCluster cluster("snapshot-heartbeat-progress");
  cluster.ElectOne();
  cluster.Transport().SetLinkEnabled(1, 3, false);
  cluster.Transport().SetLinkEnabled(3, 1, false);

  std::string large_value(192U * 1024U, '\0');
  for (size_t index = 0; index < large_value.size(); index++) {
    large_value[index] = static_cast<char>('a' + index % 23U);
  }
  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND,
                                    KvCommandCodec::Encode({1, KvOperation::PUT, "large", large_value})),
            2);
  cluster.Transport().DeliverAll();
  ASSERT_EQ(cluster.Node(1).CommitIndex(), 2);
  ASSERT_EQ(cluster.Machine(1).Get("large"), large_value);
  const auto snapshot = cluster.Node(1).CreateSnapshot();
  ASSERT_GT(snapshot.payload_size_, 2U * 64U * 1024U);
  ASSERT_EQ(cluster.Node(3).CommitIndex(), 1);

  // Model a prior sender whose ACKs were lost after two real durable chunks.
  // A new transfer starting at zero must consume the follower's returned
  // high-water mark instead of walking the already staged prefix again.
  constexpr size_t chunk_bytes = 64U * 1024U;
  for (uint64_t offset : {uint64_t{0}, uint64_t{chunk_bytes}}) {
    auto data = cluster.Node(1).ReadSnapshotChunk(snapshot, offset, chunk_bytes);
    ASSERT_EQ(data.size(), chunk_bytes);
    cluster.Node(3).Receive(
        1, InstallSnapshotRequest{1, 1, 900 + offset / chunk_bytes, snapshot.snapshot_id_,
                                  snapshot.last_included_index_, snapshot.last_included_term_, offset,
                                  snapshot.payload_size_, snapshot.payload_checksum_, false, std::move(data)});
  }
  EXPECT_EQ(cluster.Transport().Pending(), 0);

  auto take_snapshot_request = [&](NodeId from, NodeId to) -> std::optional<InstallSnapshotRequest> {
    std::optional<InstallSnapshotRequest> result;
    for (const auto &envelope : cluster.Transport().TakeAll()) {
      if (envelope.from_ == from && envelope.to_ == to &&
          std::holds_alternative<InstallSnapshotRequest>(envelope.message_)) {
        EXPECT_FALSE(result.has_value());
        result = std::get<InstallSnapshotRequest>(envelope.message_);
      }
    }
    return result;
  };
  auto take_snapshot_response = [&](NodeId from, NodeId to) -> std::optional<InstallSnapshotResponse> {
    std::optional<InstallSnapshotResponse> result;
    for (const auto &envelope : cluster.Transport().TakeAll()) {
      if (envelope.from_ == from && envelope.to_ == to &&
          std::holds_alternative<InstallSnapshotResponse>(envelope.message_)) {
        EXPECT_FALSE(result.has_value());
        result = std::get<InstallSnapshotResponse>(envelope.message_);
      }
    }
    return result;
  };

  cluster.Transport().SetLinkEnabled(1, 3, true);
  cluster.Transport().SetLinkEnabled(3, 1, true);
  uint64_t logical_time = 150;
  cluster.Node(1).Tick(logical_time);
  auto current = take_snapshot_request(1, 3);
  ASSERT_TRUE(current.has_value());

  size_t chunks = 0;
  uint64_t expected_offset = 0;
  while (true) {
    chunks++;
    ASSERT_LT(chunks, 16);
    ASSERT_EQ(current->snapshot_id_, snapshot.snapshot_id_);
    ASSERT_EQ(current->last_included_index_, snapshot.last_included_index_);
    ASSERT_EQ(current->offset_, expected_offset);
    ASSERT_FALSE(current->data_.empty());

    // This is the production follower path: StageChunk durably appends before
    // emitting the response. Hold that real response at the network boundary.
    cluster.Node(3).Receive(1, *current);
    auto delayed = take_snapshot_response(3, 1);
    ASSERT_TRUE(delayed.has_value());
    ASSERT_TRUE(delayed->success_);

    logical_time += 50;
    cluster.Node(1).Tick(logical_time);
    auto retry = take_snapshot_request(1, 3);
    ASSERT_TRUE(retry.has_value());
    EXPECT_EQ(retry->request_id_, current->request_id_);
    EXPECT_EQ(retry->snapshot_id_, current->snapshot_id_);
    EXPECT_EQ(retry->offset_, current->offset_);
    EXPECT_EQ(retry->done_, current->done_);
    EXPECT_EQ(retry->data_, current->data_);

    std::optional<InstallSnapshotResponse> duplicate_response;
    if (chunks == 1) {
      // Replaying an already durable prefix reports the follower's actual
      // high-water mark through the formal RPC, without appending it twice.
      cluster.Node(3).Receive(1, *retry);
      duplicate_response = take_snapshot_response(3, 1);
      ASSERT_TRUE(duplicate_response.has_value());
      EXPECT_EQ(duplicate_response->request_id_, delayed->request_id_);
      EXPECT_EQ(duplicate_response->next_offset_, delayed->next_offset_);
    }

    const bool complete = delayed->complete_;
    if (complete) {
      // Lose the real COMPLETE ACK after the follower has published the
      // snapshot. The next heartbeat still retries the same final chunk; the
      // follower no longer has staging state, so that retry fails closed.
      cluster.Node(3).Receive(1, *retry);
      auto missing_stage = take_snapshot_response(3, 1);
      ASSERT_TRUE(missing_stage.has_value());
      EXPECT_EQ(missing_stage->request_id_, current->request_id_);
      EXPECT_FALSE(missing_stage->success_);
      cluster.Node(1).Receive(3, *missing_stage);

      // A later heartbeat starts a new transfer at zero. The follower's stale
      // guard reports its independently published index as COMPLETE, allowing
      // the Leader to converge without transferring the image again.
      logical_time += 50;
      cluster.Node(1).Tick(logical_time);
      auto restart = take_snapshot_request(1, 3);
      ASSERT_TRUE(restart.has_value());
      EXPECT_GT(restart->request_id_, current->request_id_);
      EXPECT_EQ(restart->snapshot_id_, snapshot.snapshot_id_);
      EXPECT_EQ(restart->offset_, 0);
      cluster.Node(3).Receive(1, *restart);
      auto stale_complete = take_snapshot_response(3, 1);
      ASSERT_TRUE(stale_complete.has_value());
      EXPECT_EQ(stale_complete->request_id_, restart->request_id_);
      EXPECT_TRUE(stale_complete->success_);
      EXPECT_TRUE(stale_complete->stale_);
      EXPECT_TRUE(stale_complete->complete_);
      EXPECT_EQ(stale_complete->match_index_, snapshot.last_included_index_);
      cluster.Node(1).Receive(3, *stale_complete);

      // The originally lost COMPLETE ACK can arrive arbitrarily late. Its old
      // request identity must not resurrect or mutate a finished transfer.
      cluster.Node(1).Receive(3, *delayed);
      EXPECT_EQ(cluster.Transport().Pending(), 0);
      break;
    }

    cluster.Node(1).Receive(3, *delayed);
    std::optional<InstallSnapshotRequest> next;
    const auto current_end = current->offset_ + current->data_.size();
    ASSERT_GE(delayed->next_offset_, current_end);
    ASSERT_LT(delayed->next_offset_, snapshot.payload_size_);
    expected_offset = delayed->next_offset_;
    if (chunks == 1) {
      EXPECT_EQ(expected_offset, 2U * chunk_bytes);
    }
    next = take_snapshot_request(1, 3);
    ASSERT_TRUE(next.has_value());
    EXPECT_GT(next->request_id_, current->request_id_);
    if (duplicate_response.has_value()) {
      cluster.Node(1).Receive(3, *duplicate_response);
      EXPECT_EQ(cluster.Transport().Pending(), 0);
    }
    current = std::move(next);
  }

  EXPECT_GE(chunks, 3);
  EXPECT_EQ(cluster.Node(3).CommitIndex(), 2);
  EXPECT_EQ(cluster.Node(3).LastApplied(), 2);
  ASSERT_TRUE(cluster.Node(3).LatestSnapshot().has_value());
  EXPECT_EQ(cluster.Node(3).LatestSnapshot()->last_included_index_, 2);
  ASSERT_TRUE(cluster.Machine(3).Get("large").has_value());
  EXPECT_EQ(*cluster.Machine(3).Get("large"), large_value);

  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND,
                                    KvCommandCodec::Encode({1, KvOperation::PUT, "suffix", "after-snapshot"})),
            3);
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(3).LastApplied(), 3);
  EXPECT_EQ(cluster.Machine(3).Get("suffix"), "after-snapshot");
}

// M4-T03: both stale guards preserve CURRENT, logical base, FSM, and published watermarks.
TEST(RaftNodeTest, StaleAndRacingSnapshotCannotRollBackFollower) {
  ThreeNodeKvCluster cluster("stale-snapshot");
  cluster.ElectOne();
  cluster.Transport().SetLinkEnabled(1, 3, false);
  cluster.Transport().SetLinkEnabled(3, 1, false);

  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "x", "two"})),
            2);
  cluster.Transport().DeliverAll();
  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "y", "three"})),
            3);
  cluster.Transport().DeliverAll();
  const auto delayed_entries = cluster.Node(1).Log().Entries(2, 3);
  const auto snapshot = cluster.Node(1).CreateSnapshot();
  ASSERT_GT(snapshot.payload_size_, 1);
  const auto payload = cluster.Node(1).ReadSnapshotChunk(snapshot, 0, static_cast<size_t>(snapshot.payload_size_));
  ASSERT_EQ(payload.size(), snapshot.payload_size_);
  const auto split = payload.size() / 2;

  // The download starts while S=3 is ahead of P=1.
  cluster.Node(3).Receive(1,
                          InstallSnapshotRequest{1,
                                                 1,
                                                 70,
                                                 snapshot.snapshot_id_,
                                                 snapshot.last_included_index_,
                                                 snapshot.last_included_term_,
                                                 0,
                                                 snapshot.payload_size_,
                                                 snapshot.payload_checksum_,
                                                 false,
                                                 {payload.begin(), payload.begin() + static_cast<ptrdiff_t>(split)}});

  // A previously sent AppendEntries catches the follower up before the final chunk is published.
  cluster.Node(3).Receive(1, AppendEntriesRequest{1, 1, 71, 1, 1, delayed_entries, 3, std::nullopt});
  ASSERT_EQ(cluster.Node(3).PublishedAppliedIndex(), 3);
  ASSERT_FALSE(cluster.Node(3).LatestSnapshot().has_value());
  ASSERT_EQ(cluster.Node(3).Log().SnapshotBaseIndex(), 0);

  cluster.Node(3).Receive(1, InstallSnapshotRequest{1,
                                                    1,
                                                    72,
                                                    snapshot.snapshot_id_,
                                                    snapshot.last_included_index_,
                                                    snapshot.last_included_term_,
                                                    split,
                                                    snapshot.payload_size_,
                                                    snapshot.payload_checksum_,
                                                    true,
                                                    {payload.begin() + static_cast<ptrdiff_t>(split), payload.end()}});
  EXPECT_EQ(cluster.Node(3).PublishedAppliedIndex(), 3);
  EXPECT_EQ(cluster.Node(3).LastApplied(), 3);
  EXPECT_EQ(cluster.Node(3).Log().SnapshotBaseIndex(), 0);
  EXPECT_FALSE(cluster.Node(3).LatestSnapshot().has_value());
  EXPECT_EQ(cluster.Machine(3).Get("x"), "two");
  EXPECT_EQ(cluster.Machine(3).Get("y"), "three");

  // A duplicate S == P is rejected by the first guard with the same no-side-effect contract.
  cluster.Node(3).Receive(1, InstallSnapshotRequest{1, 1, 73, snapshot.snapshot_id_, 3, snapshot.last_included_term_, 0,
                                                    snapshot.payload_size_, snapshot.payload_checksum_, true, payload});
  EXPECT_EQ(cluster.Node(3).Log().SnapshotBaseIndex(), 0);
  EXPECT_FALSE(cluster.Node(3).LatestSnapshot().has_value());
  EXPECT_EQ(cluster.Node(3).PublishedAppliedIndex(), 3);

  // A higher-term stale request persists the new term first, but still cannot touch FSM state.
  cluster.Node(3).Receive(2, InstallSnapshotRequest{2, 2, 74, snapshot.snapshot_id_, 3, snapshot.last_included_term_, 0,
                                                    snapshot.payload_size_, snapshot.payload_checksum_, true, payload});
  EXPECT_EQ(cluster.Node(3).CurrentTerm(), 2);
  EXPECT_EQ(cluster.Node(3).PublishedAppliedIndex(), 3);
  EXPECT_EQ(cluster.Node(3).Log().SnapshotBaseIndex(), 0);
  EXPECT_FALSE(cluster.Node(3).LatestSnapshot().has_value());
  EXPECT_EQ(cluster.Machine(3).Get("x"), "two");
}

// M4-T04: every voter reconstructs term, commit, snapshot, and KV state from its own real directory.
TEST(RaftNodeTest, RotatingNodeRestartRecoversDurableState) {
  ThreeNodeKvCluster cluster("restart");
  cluster.ElectOne();
  ASSERT_EQ(
      cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "durable", "yes"})),
      2);
  cluster.Transport().DeliverAll();
  for (NodeId id = 1; id <= 3; id++) {
    const auto snapshot = cluster.Node(id).CreateSnapshot();
    ASSERT_EQ(snapshot.last_included_index_, 2);
  }

  for (NodeId id = 1; id <= 3; id++) {
    cluster.Restart(id);
    EXPECT_EQ(cluster.Node(id).Role(), RaftRole::FOLLOWER);
    EXPECT_EQ(cluster.Node(id).CurrentTerm(), 1);
    EXPECT_EQ(cluster.Node(id).CommitIndex(), 2);
    EXPECT_EQ(cluster.Node(id).LastApplied(), 2);
    EXPECT_EQ(cluster.Node(id).Log().SnapshotBaseIndex(), 2);
    EXPECT_EQ(cluster.Machine(id).Get("durable"), "yes");
  }

  cluster.Node(2).Tick(200);
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(2).Role(), RaftRole::LEADER);
  EXPECT_EQ(cluster.Node(2).CurrentTerm(), 2);
  for (NodeId id = 1; id <= 3; id++) {
    EXPECT_EQ(cluster.Node(id).CommitIndex(), 3);
    EXPECT_EQ(cluster.Node(id).Log().TermAt(3), 2);
    EXPECT_EQ(cluster.Machine(id).Get("durable"), "yes");
  }
}

// M7/E2E-11 component contract: the prior retained snapshot remains usable because the log base stays at its edge.
TEST(RaftNodeTest, CorruptLatestSnapshotRecoversPreviousPlusBridgeLog) {
  ThreeNodeKvCluster cluster("snapshot-fallback-bridge");
  cluster.ElectOne();
  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "a", "two"})),
            2);
  cluster.Transport().DeliverAll();
  ASSERT_EQ(cluster.Node(1).CreateSnapshot().last_included_index_, 2);

  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "b", "three"})),
            3);
  cluster.Transport().DeliverAll();
  ASSERT_EQ(cluster.Node(1).CreateSnapshot().last_included_index_, 3);
  EXPECT_EQ(cluster.Node(1).Log().SnapshotBaseIndex(), 2);

  ASSERT_EQ(cluster.Node(1).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, "c", "four"})),
            4);
  cluster.Transport().DeliverAll();
  cluster.CorruptLatestSnapshot(1);
  cluster.Restart(1);

  EXPECT_EQ(cluster.Node(1).LatestSnapshot()->last_included_index_, 2);
  EXPECT_EQ(cluster.Node(1).CommitIndex(), 4);
  EXPECT_EQ(cluster.Node(1).LastApplied(), 4);
  EXPECT_EQ(cluster.Machine(1).Get("a"), "two");
  EXPECT_EQ(cluster.Machine(1).Get("b"), "three");
  EXPECT_EQ(cluster.Machine(1).Get("c"), "four");
}

// M3-T08: each linearizable read needs its own current-term quorum round; old and ordinary ACKs are unusable.
TEST(RaftNodeTest, ReadIndexUsesUniqueContextAndCannotReuseHeartbeatAcks) {
  ThreeNodeKvCluster cluster("read-index");
  cluster.ElectOne();

  ASSERT_TRUE(cluster.Node(1).StartReadIndex(10));
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(1).TakeReadIndex(10), 1);
  EXPECT_FALSE(cluster.Node(1).TakeReadIndex(10).has_value());
  EXPECT_FALSE(cluster.Node(1).StartReadIndex(10));

  cluster.SetNodeOnePartitioned(true);
  ASSERT_TRUE(cluster.Node(1).StartReadIndex(11));
  cluster.Transport().DeliverAll();
  EXPECT_FALSE(cluster.Node(1).TakeReadIndex(11).has_value());
  cluster.Node(1).Receive(2, AppendEntriesResponse{1, 999, true, 1, std::nullopt, 0, std::nullopt});
  cluster.Node(1).Receive(2, AppendEntriesResponse{1, 1000, true, 1, std::nullopt, 0, 10});
  EXPECT_FALSE(cluster.Node(1).TakeReadIndex(11).has_value());
  cluster.Node(1).CancelReadIndex(11);

  cluster.SetNodeOnePartitioned(false);
  ASSERT_TRUE(cluster.Node(1).StartReadIndex(12));
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(1).TakeReadIndex(12), 1);
}

TEST(RaftNodeTest, OneWayResponseLossCannotCompleteReadIndex) {
  ThreeNodeKvCluster cluster("read-index-one-way-loss");
  cluster.ElectOne();
  cluster.Transport().SetLinkEnabled(2, 1, false);
  cluster.Transport().SetLinkEnabled(3, 1, false);
  ASSERT_TRUE(cluster.Node(1).StartReadIndex(20));
  cluster.Transport().DeliverAll();
  EXPECT_FALSE(cluster.Node(1).TakeReadIndex(20).has_value());

  cluster.Transport().SetLinkEnabled(2, 1, true);
  cluster.Transport().SetLinkEnabled(3, 1, true);
  ASSERT_TRUE(cluster.Node(1).StartReadIndex(21));
  cluster.Transport().DeliverAll();
  EXPECT_EQ(cluster.Node(1).TakeReadIndex(21), 1);
}

}  // namespace bustub
