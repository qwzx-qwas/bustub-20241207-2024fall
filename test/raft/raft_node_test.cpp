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
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>

#include "../recovery/power_loss_storage.h"  // NOLINT(build/include_subdir)
#include "common/byte_codec.h"
#include "gtest/gtest.h"
#include "raft/in_memory_raft_transport.h"
#include "raft/raft_node.h"

namespace bustub {
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

  auto stable = StableStore::Open(raft_directory, storage);
  auto snapshots = SnapshotStore::Open(raft_directory / "snapshots", storage);
  const auto latest = snapshots->Latest();
  const auto latest_index = latest.has_value() ? latest->last_included_index_ : 0;
  const auto recovery = snapshots->OldestRetained();
  const auto recovery_index = recovery.has_value() ? recovery->last_included_index_ : 0;
  const auto recovery_term = recovery.has_value() ? recovery->last_included_term_ : 0;
  const auto effective_commit = std::max(stable->State().commit_index_, latest_index);
  auto recovered_machine = std::make_shared<KvStateMachine>();
  std::unique_ptr<LogStore> log;
  try {
    log = LogStore::Open(raft_directory / "log", storage, effective_commit, recovery_index, recovery_term);
  } catch (...) {
    if (!latest.has_value() || effective_commit != latest->last_included_index_) {
      throw;
    }
    // Mirror production startup: validate the complete state-machine image
    // before an exact Snapshot@commit is allowed to replace a damaged bridge.
    recovered_machine->InstallSnapshotFile(snapshots->PayloadFile(*latest), latest->last_included_index_);
    log = LogStore::RebuildFromVerifiedSnapshot(raft_directory / "log", storage, effective_commit,
                                                latest->last_included_index_, latest->last_included_term_);
    snapshots->RetainOnlyLatest();
  }
  auto recovered = std::make_unique<RaftNode>(
      RaftNodeConfig{1, {1, 2, 3}, 100, 300, 50, "install-crash", MakeFixedElectionTimeoutSource(100)}, transport,
      std::move(stable), std::move(log), recovered_machine, std::move(snapshots));
  InstallRecoveryState state{recovered->CommitIndex(), recovered->LastApplied(), recovered_machine->Get("installed")};
  recovered.reset();
  storage->DisableFailure();
  storage->RemoveTree(root);
  return {std::move(state), events, fault_triggered};
}

}  // namespace

// M3-T04: a fixed seed produces a replayable timeout sequence while retaining the production interval contract.
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

// M3-T05: injected fixed timeouts and logical time elect one Leader; production never uses these fixed values.
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

// M3-T06: an isolated old Leader's uncommitted suffix is atomically replaced by the next Leader.
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
  constexpr size_t CHUNK_BYTES = 64U * 1024U;
  for (uint64_t offset : {uint64_t{0}, uint64_t{CHUNK_BYTES}}) {
    auto data = cluster.Node(1).ReadSnapshotChunk(snapshot, offset, CHUNK_BYTES);
    ASSERT_EQ(data.size(), CHUNK_BYTES);
    cluster.Node(3).Receive(
        1, InstallSnapshotRequest{1, 1, 900 + offset / CHUNK_BYTES, snapshot.snapshot_id_,
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
      EXPECT_EQ(expected_offset, 2U * CHUNK_BYTES);
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

// M6-T01: each linearizable read needs its own current-term quorum round; old and ordinary ACKs are unusable.
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
