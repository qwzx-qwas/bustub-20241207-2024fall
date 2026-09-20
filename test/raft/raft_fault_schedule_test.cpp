//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_fault_schedule_test.cpp
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "raft/persistent_state.h"
#include "raft/raft_node.h"

namespace bustub {
namespace {

// Scenario inspiration: MIT 6.5840 Raft Lab 3A-3D (not a port of the Go tester).
// https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html
// BusTub currently has exactly three voters and one unresolved proposal per leader.
class ScheduledRaftTransport : public RaftTransport {
 public:
  explicit ScheduledRaftTransport(std::function<void(RaftEnvelope)> send) : send_(std::move(send)) {}
  void Send(RaftEnvelope envelope) override { send_(std::move(envelope)); }

 private:
  std::function<void(RaftEnvelope)> send_;
};

class ObservedKvStateMachine : public KvStateMachine {
 public:
  explicit ObservedKvStateMachine(std::function<void(const ReplicatedLogEntry &)> observe)
      : observe_(std::move(observe)) {}

  void Apply(const ReplicatedLogEntry &entry) override {
    if (entry.index_ != LastApplied() + 1) {
      throw std::runtime_error("Apply skipped or repeated an index within one state-machine incarnation");
    }
    observe_(entry);
    KvStateMachine::Apply(entry);
  }

 private:
  std::function<void(const ReplicatedLogEntry &)> observe_;
};

/**
 * All nodes advance on one logical clock. RPC requests AND replies pass through
 * the same seeded drop/duplicate/delay scheduler. Stores use real durable files;
 * crash/restart discards volatile state and reopens those files through production
 * recovery. This does not simulate a torn filesystem operation or a process kill.
 *
 * The oracle retains every observed committed entry across node incarnations and
 * compaction. It checks ordered Apply, agreement at every index, election safety,
 * monotonic term/commit, and a separately replayed map after every delivered RPC.
 */
class FaultScheduleCluster {
 public:
  explicit FaultScheduleCluster(uint64_t seed) : seed_(seed), random_(seed) {
    // mkdtemp also isolates gtest repeats and concurrent invocations of the binary.
    auto pattern = (std::filesystem::temp_directory_path() / "bustub-raft-schedule-XXXXXX").string();
    const auto *directory = mkdtemp(pattern.data());
    if (directory == nullptr) {
      throw std::runtime_error("cannot create Raft schedule test directory");
    }
    root_ = directory;
    storage_ = std::make_shared<PosixDurableStorage>();
    transport_ =
        std::make_shared<ScheduledRaftTransport>([this](RaftEnvelope envelope) { Enqueue(std::move(envelope)); });
    Heal();
    for (NodeId id : IDS) {
      Restart(id);
    }
  }

  ~FaultScheduleCluster() {
    for (auto &node : nodes_) {
      node.reset();
    }
    for (auto &machine : machines_) {
      machine.reset();
    }
    storage_->RemoveTree(root_);
  }

  auto Node(NodeId id) -> RaftNode & { return *nodes_.at(id - 1); }
  auto Machine(NodeId id) -> ObservedKvStateMachine & { return *machines_.at(id - 1); }
  auto Running(NodeId id) const -> bool { return nodes_.at(id - 1) != nullptr; }
  auto Dropped() const -> size_t { return dropped_; }
  auto Duplicated() const -> size_t { return duplicated_; }
  auto Reordered() const -> size_t { return reordered_; }
  auto SnapshotChunks() const -> size_t { return snapshot_chunks_; }
  void SetUnreliable(bool enabled) { unreliable_ = enabled; }

  void Isolate(NodeId id) {
    for (NodeId peer : IDS) {
      if (peer != id) {
        links_[id - 1][peer - 1] = false;
        links_[peer - 1][id - 1] = false;
      }
    }
    // A partition drops in-flight traffic as well as newly sent traffic.
    queue_.erase(
        std::remove_if(queue_.begin(), queue_.end(),
                       [id](const auto &item) { return item.envelope_.from_ == id || item.envelope_.to_ == id; }),
        queue_.end());
  }

  void Heal() {
    for (auto &links : links_) {
      links.fill(true);
    }
  }

  void Crash(NodeId id) {
    // No message belonging to this incarnation can be delivered after a restart.
    queue_.erase(
        std::remove_if(queue_.begin(), queue_.end(),
                       [id](const auto &item) { return item.envelope_.from_ == id || item.envelope_.to_ == id; }),
        queue_.end());
    nodes_[id - 1].reset();
    machines_[id - 1].reset();
    CheckSafety();
  }

  void Restart(NodeId id) {
    Require(!Running(id), "restart requires a stopped node");
    const auto offset = id - 1;
    machines_[offset] = std::make_shared<ObservedKvStateMachine>([this](const ReplicatedLogEntry &entry) {
      const auto [it, inserted] = committed_.emplace(entry.index_, entry);
      Require(it->second == entry, "different entries applied at index " + std::to_string(entry.index_));
      Require(!inserted || entry.index_ == committed_.size(), "committed history has a gap");
    });
    auto recovered = RecoverRaftPersistentState(root_ / std::to_string(id), storage_, machines_[offset]);
    // Each incarnation starts its local monotonic clock at zero; restarting late
    // in a schedule must not expire its election timeout immediately.
    boot_ms_[offset] = now_ms_;
    nodes_[offset] = std::make_unique<RaftNode>(
        RaftNodeConfig{id,
                       {1, 2, 3},
                       150,
                       300,
                       30,
                       "fault-schedule",
                       MakeSeededElectionTimeoutSource(seed_ + 101 * id + 1009 * incarnations_[offset]++)},
        transport_, std::move(recovered.stable_store_), std::move(recovered.log_store_), machines_[offset],
        std::move(recovered.snapshot_store_));
    Node(id).Tick(0);
    CheckSafety();
  }

  void Advance(uint64_t duration_ms) {
    const auto deadline = now_ms_ + duration_ms;
    while (now_ms_ < deadline) {
      now_ms_ += 10;
      for (NodeId id : IDS) {
        if (Running(id)) {
          Node(id).Tick(now_ms_ - boot_ms_[id - 1]);
          CheckSafety();
        }
      }
      Pump();
    }
  }

  auto WaitLeader(const std::vector<NodeId> &eligible = {1, 2, 3}) -> NodeId {
    for (size_t attempt = 0; attempt < 500; attempt++) {
      Advance(10);
      std::optional<NodeId> leader;
      uint64_t highest_term = 0;
      for (NodeId id : eligible) {
        if (Running(id)) {
          highest_term = std::max(highest_term, Node(id).CurrentTerm());
        }
        if (Running(id) && Node(id).LeaderReady()) {
          if (leader.has_value()) {
            leader.reset();
            break;
          }
          leader = id;
        }
      }
      // LeaderReady only fences the current-term NOOP. After healing, an old
      // leader may still have an unresolved proposal or not yet know a higher
      // term. Wait for a leader that can admit the next single-flight write.
      if (leader.has_value() && Node(*leader).CurrentTerm() == highest_term &&
          Node(*leader).Log().LastLogIndex() == Node(*leader).CommitIndex()) {
        return *leader;
      }
    }
    throw std::runtime_error(Diagnostics("no unique ready leader within 5000 logical ms"));
  }

  auto Propose(NodeId leader, const std::string &key, const std::string &value) -> uint64_t {
    const auto index =
        Node(leader).Propose(EntryType::KV_COMMAND, KvCommandCodec::Encode({1, KvOperation::PUT, key, value}));
    Require(index.has_value(), "proposal rejected by selected leader");
    CheckSafety();
    return *index;
  }

  // The command is proposed once. Waiting must never hide failure by resubmitting
  // it at a different index or silently changing the expected replica count.
  void WaitApplied(uint64_t index, const std::vector<NodeId> &replicas) {
    for (size_t attempt = 0; attempt < 500; attempt++) {
      if (std::all_of(replicas.begin(), replicas.end(),
                      [this, index](NodeId id) { return Running(id) && Node(id).LastApplied() >= index; })) {
        return;
      }
      Advance(10);
    }
    throw std::runtime_error(Diagnostics("index " + std::to_string(index) + " did not reach required replicas"));
  }

  auto Submit(NodeId leader, const std::string &key, const std::string &value,
              const std::vector<NodeId> &replicas = {1, 2, 3}) -> uint64_t {
    const auto index = Propose(leader, key, value);
    WaitApplied(index, replicas);
    const auto &entry = committed_.at(index);
    Require(entry.type_ == EntryType::KV_COMMAND &&
                entry.payload_ == KvCommandCodec::Encode({1, KvOperation::PUT, key, value}),
            "a different command committed at the proposed index");
    for (NodeId id : replicas) {
      Require(Machine(id).Get(key) == std::optional<std::string>{value}, "submitted value missing from replica");
    }
    return index;
  }

  void CheckSafety() {
    for (NodeId id : IDS) {
      if (!Running(id)) {
        continue;
      }
      auto &node = Node(id);
      Require(node.CurrentTerm() >= terms_[id - 1], "term regressed after an event or recovery");
      Require(node.CommitIndex() >= commits_[id - 1], "commit index regressed after an event or recovery");
      terms_[id - 1] = node.CurrentTerm();
      commits_[id - 1] = node.CommitIndex();
      if (node.Role() == RaftRole::LEADER) {
        const auto [it, inserted] = leaders_by_term_.emplace(node.CurrentTerm(), id);
        Require(inserted || it->second == id, "two different leaders observed in the same term");
      }
      Require(node.LastApplied() == node.CommitIndex(), "committed state was not applied");
      Require(node.PublishedAppliedIndex() == node.LastApplied(), "published watermark differs from Apply");
      Require(Machine(id).LastApplied() == node.LastApplied(), "state-machine watermark differs from Raft");
      Require(Machine(id).Data() == ModelAt(node.LastApplied()), "state differs from the committed-prefix oracle");
    }
  }

  static constexpr std::array<NodeId, 3> IDS{1, 2, 3};

 private:
  struct ScheduledMessage {
    uint64_t due_ms_;
    RaftEnvelope envelope_;
  };

  auto ModelAt(uint64_t index) const -> std::map<std::string, std::string> {
    std::map<std::string, std::string> expected;
    Require(index <= committed_.size(), "snapshot or Apply exceeds observed committed history");
    for (uint64_t i = 1; i <= index; i++) {
      const auto &entry = committed_.at(i);
      if (entry.type_ == EntryType::NOOP) {
        continue;
      }
      const auto command = KvCommandCodec::Decode(entry.payload_);
      if (command.operation_ == KvOperation::PUT) {
        expected[command.key_] = command.value_;
      } else {
        expected.erase(command.key_);
      }
    }
    return expected;
  }

  void Enqueue(RaftEnvelope envelope) {
    if (!Running(envelope.from_) || !Running(envelope.to_) || !links_[envelope.from_ - 1][envelope.to_ - 1]) {
      return;
    }
    if (unreliable_ && random_() % 100 < 15) {
      dropped_++;
      return;
    }
    if (unreliable_ && random_() % 100 < 10) {
      queue_.push_back({now_ms_ + 10 * (random_() % 7), envelope});
      duplicated_++;
    }
    const auto delay = unreliable_ ? 10 * (random_() % 7) : 0;
    queue_.push_back({now_ms_ + delay, std::move(envelope)});
  }

  void Pump() {
    for (size_t count = 0; count < 10000; count++) {
      const auto next =
          std::find_if(queue_.begin(), queue_.end(), [this](const auto &item) { return item.due_ms_ <= now_ms_; });
      if (next == queue_.end()) {
        return;
      }
      if (next != queue_.begin()) {
        reordered_++;
      }
      auto envelope = std::move(next->envelope_);
      queue_.erase(next);
      if (Running(envelope.to_) && Running(envelope.from_) && links_[envelope.from_ - 1][envelope.to_ - 1]) {
        if (std::holds_alternative<InstallSnapshotRequest>(envelope.message_)) {
          snapshot_chunks_++;
        }
        Node(envelope.to_).Receive(envelope.from_, envelope.message_);
        CheckSafety();
      }
    }
    throw std::runtime_error(Diagnostics("RPC delivery budget exhausted"));
  }

  auto Diagnostics(const std::string &reason) const -> std::string {
    std::ostringstream out;
    out << reason << "; seed=" << seed_ << " logical_ms=" << now_ms_ << " pending=" << queue_.size()
        << " dropped=" << dropped_ << " duplicated=" << duplicated_ << " reordered=" << reordered_;
    for (NodeId id : IDS) {
      out << " [node=" << id;
      if (Running(id)) {
        const auto &node = *nodes_[id - 1];
        out << " term=" << node.CurrentTerm() << " role=" << static_cast<int>(node.Role())
            << " commit=" << node.CommitIndex() << " applied=" << node.LastApplied()
            << " last_log=" << node.Log().LastLogIndex();
      } else {
        out << " stopped";
      }
      out << ']';
    }
    return out.str();
  }

  void Require(bool condition, const std::string &reason) const {
    if (!condition) {
      throw std::runtime_error(Diagnostics(reason));
    }
  }

  uint64_t seed_;
  std::mt19937_64 random_;
  std::filesystem::path root_;
  std::shared_ptr<PosixDurableStorage> storage_;
  std::shared_ptr<ScheduledRaftTransport> transport_;
  std::array<std::shared_ptr<ObservedKvStateMachine>, 3> machines_;
  std::array<std::unique_ptr<RaftNode>, 3> nodes_;
  std::array<std::array<bool, 3>, 3> links_{};
  std::array<uint64_t, 3> terms_{};
  std::array<uint64_t, 3> commits_{};
  std::array<uint64_t, 3> incarnations_{};
  std::array<uint64_t, 3> boot_ms_{};
  std::map<uint64_t, NodeId> leaders_by_term_;
  std::map<uint64_t, ReplicatedLogEntry> committed_;
  std::vector<ScheduledMessage> queue_;
  uint64_t now_ms_{0};
  bool unreliable_{false};
  size_t dropped_{0};
  size_t duplicated_{0};
  size_t reordered_{0};
  size_t snapshot_chunks_{0};
};

auto PeersExcept(NodeId excluded) -> std::vector<NodeId> {
  std::vector<NodeId> peers;
  for (NodeId id : FaultScheduleCluster::IDS) {
    if (id != excluded) {
      peers.push_back(id);
    }
  }
  return peers;
}

TEST(RaftFaultScheduleTest, InitialElectionRemainsStableWithoutFailures) {
  FaultScheduleCluster cluster(5840);
  const auto leader = cluster.WaitLeader();
  const auto term = cluster.Node(leader).CurrentTerm();
  cluster.Submit(leader, "initial", "committed");
  cluster.Advance(3000);
  EXPECT_EQ(cluster.WaitLeader(), leader);
  for (NodeId id : FaultScheduleCluster::IDS) {
    EXPECT_EQ(cluster.Node(id).CurrentTerm(), term);
  }
}

TEST(RaftFaultScheduleTest, NoLeaderWithoutQuorumThenElectionAfterHealing) {
  FaultScheduleCluster cluster(3001);
  for (NodeId id : FaultScheduleCluster::IDS) {
    cluster.Isolate(id);
  }
  cluster.Advance(2000);
  for (NodeId id : FaultScheduleCluster::IDS) {
    EXPECT_NE(cluster.Node(id).Role(), RaftRole::LEADER);
    EXPECT_EQ(cluster.Node(id).CommitIndex(), 0);
  }
  cluster.Heal();
  cluster.Submit(cluster.WaitLeader(), "quorum-restored", "available");
}

TEST(RaftFaultScheduleTest, RepeatedLeaderPartitionsElectAndRejoin) {
  FaultScheduleCluster cluster(824);
  auto leader = cluster.WaitLeader();
  for (size_t round = 0; round < 6; round++) {
    SCOPED_TRACE(round);
    const auto old_leader = leader;
    const auto old_term = cluster.Node(old_leader).CurrentTerm();
    cluster.Isolate(old_leader);
    const auto majority = PeersExcept(old_leader);
    leader = cluster.WaitLeader(majority);
    ASSERT_NE(leader, old_leader);
    ASSERT_GT(cluster.Node(leader).CurrentTerm(), old_term);
    const auto index = cluster.Submit(leader, "round-" + std::to_string(round), "majority", majority);
    cluster.Heal();
    cluster.WaitApplied(index, {1, 2, 3});
    EXPECT_EQ(cluster.Node(old_leader).Role(), RaftRole::FOLLOWER);
    leader = cluster.WaitLeader();
  }
}

TEST(RaftFaultScheduleTest, NoAgreementWithoutMajorityThenRecovery) {
  FaultScheduleCluster cluster(3003);
  const auto leader = cluster.WaitLeader();
  const auto baseline = cluster.Submit(leader, "before", "durable");
  for (NodeId id : FaultScheduleCluster::IDS) {
    cluster.Isolate(id);
  }
  const auto pending = cluster.Propose(leader, "pending", "once");
  ASSERT_GT(pending, baseline);
  cluster.Advance(2000);
  for (NodeId id : FaultScheduleCluster::IDS) {
    EXPECT_EQ(cluster.Node(id).CommitIndex(), baseline);
    EXPECT_FALSE(cluster.Machine(id).Get("pending").has_value());
  }
  cluster.Heal();
  const auto replacement = cluster.WaitLeader();
  // The unacknowledged proposal may survive or be overwritten by the new term.
  // Require progress and preservation of the acknowledged prefix in either case.
  cluster.Submit(replacement, "after", "available");
  for (NodeId id : FaultScheduleCluster::IDS) {
    EXPECT_EQ(cluster.Machine(id).Get("before"), std::optional<std::string>{"durable"});
  }
}

TEST(RaftFaultScheduleTest, DisconnectedFollowerCatchesUpAfterManyCommands) {
  FaultScheduleCluster cluster(3101);
  const auto leader = cluster.WaitLeader();
  const auto lagging = PeersExcept(leader).front();
  const auto majority = PeersExcept(lagging);
  const auto baseline = cluster.Submit(leader, "base", "present");
  cluster.Isolate(lagging);
  uint64_t last = baseline;
  for (size_t i = 0; i < 24; i++) {
    last = cluster.Submit(leader, "key-" + std::to_string(i), std::to_string(i), majority);
  }
  EXPECT_EQ(cluster.Node(lagging).LastApplied(), baseline);
  cluster.Heal();
  cluster.WaitApplied(last, {1, 2, 3});
  EXPECT_EQ(cluster.Machine(lagging).Data(), cluster.Machine(leader).Data());
  cluster.Submit(cluster.WaitLeader(), "rejoined", "yes");
}

TEST(RaftFaultScheduleTest, PartitionedLeaderLosesOnlyItsUncommittedSuffix) {
  FaultScheduleCluster cluster(3102);
  const auto old_leader = cluster.WaitLeader();
  const auto baseline = cluster.Submit(old_leader, "committed", "keep");
  cluster.Isolate(old_leader);
  const auto abandoned = cluster.Propose(old_leader, "minority-only", "discard");
  const auto majority = PeersExcept(old_leader);
  const auto new_leader = cluster.WaitLeader(majority);
  const auto last = cluster.Submit(new_leader, "majority-only", "keep", majority);
  EXPECT_EQ(cluster.Node(old_leader).CommitIndex(), baseline);
  ASSERT_GE(last, abandoned);
  cluster.Heal();
  cluster.WaitApplied(last, {1, 2, 3});
  for (NodeId id : FaultScheduleCluster::IDS) {
    EXPECT_FALSE(cluster.Machine(id).Get("minority-only").has_value());
    EXPECT_EQ(cluster.Machine(id).Get("committed"), std::optional<std::string>{"keep"});
    EXPECT_EQ(cluster.Machine(id).Get("majority-only"), std::optional<std::string>{"keep"});
  }
  EXPECT_EQ(cluster.Node(old_leader).Role(), RaftRole::FOLLOWER);
}

TEST(RaftFaultScheduleTest, FullClusterRestartPreservesSnapshotAndCommittedSuffix) {
  FaultScheduleCluster cluster(3401);
  auto leader = cluster.WaitLeader();
  for (size_t i = 0; i < 6; i++) {
    cluster.Submit(leader, "key-" + std::to_string(i), "before-snapshot");
  }
  for (NodeId id : FaultScheduleCluster::IDS) {
    cluster.Node(id).CreateSnapshot();
  }
  const auto suffix = cluster.Submit(leader, "key-0", "after-snapshot");
  // Repeat cold recovery, preserving the oracle and all per-node high watermarks.
  for (size_t round = 0; round < 2; round++) {
    for (NodeId id : FaultScheduleCluster::IDS) {
      cluster.Crash(id);
    }
    for (NodeId id : FaultScheduleCluster::IDS) {
      cluster.Restart(id);
      EXPECT_GE(cluster.Node(id).LastApplied(), suffix);
      EXPECT_EQ(cluster.Machine(id).Get("key-0"), std::optional<std::string>{"after-snapshot"});
    }
    leader = cluster.WaitLeader();
    cluster.Submit(leader, "restart-" + std::to_string(round), "writable");
  }
}

class RaftSeededFaultTest : public ::testing::TestWithParam<uint64_t> {};

TEST_P(RaftSeededFaultTest, AgreementWithDroppedDuplicatedAndReorderedRpc) {
  FaultScheduleCluster cluster(GetParam());
  cluster.SetUnreliable(true);
  const auto leader = cluster.WaitLeader();
  for (size_t i = 0; i < 20; i++) {
    cluster.Submit(leader, "key-" + std::to_string(i), std::to_string(i));
  }
  EXPECT_GT(cluster.Dropped(), 0);
  EXPECT_GT(cluster.Duplicated(), 0);
  EXPECT_GT(cluster.Reordered(), 0);
}

TEST_P(RaftSeededFaultTest, CrashAndPartitionChurnPreservesAcknowledgedCommands) {
  FaultScheduleCluster cluster(GetParam());
  cluster.SetUnreliable(true);
  auto leader = cluster.WaitLeader();
  for (size_t round = 0; round < 6; round++) {
    SCOPED_TRACE(round);
    cluster.Submit(leader, "acknowledged-" + std::to_string(round), std::to_string(round));
    const auto old_leader = leader;
    const auto majority = PeersExcept(old_leader);
    if (round % 2 == 0) {
      cluster.Crash(old_leader);
    } else {
      cluster.Isolate(old_leader);
      cluster.Propose(old_leader, "uncertain-" + std::to_string(round), "may-be-overwritten");
    }
    leader = cluster.WaitLeader(majority);
    const auto index = cluster.Submit(leader, "replacement-" + std::to_string(round), "committed", majority);
    if (!cluster.Running(old_leader)) {
      cluster.Restart(old_leader);
    }
    cluster.Heal();
    cluster.WaitApplied(index, {1, 2, 3});
    leader = cluster.WaitLeader();
  }
  cluster.SetUnreliable(false);
  cluster.Submit(leader, "final", "converged");
  for (NodeId id : FaultScheduleCluster::IDS) {
    for (size_t round = 0; round < 6; round++) {
      EXPECT_EQ(cluster.Machine(id).Get("acknowledged-" + std::to_string(round)),
                std::optional<std::string>{std::to_string(round)});
    }
  }
}

TEST_P(RaftSeededFaultTest, LaggingFollowerInstallsSnapshotOverUnreliableNetworkThenRestarts) {
  FaultScheduleCluster cluster(GetParam());
  auto leader = cluster.WaitLeader();
  const auto lagging = PeersExcept(leader).front();
  const auto majority = PeersExcept(lagging);
  cluster.Submit(leader, "base", "retained");
  cluster.Crash(lagging);
  // More than two 64 KiB transfer chunks, independent of snapshot metadata size.
  const std::string large_value(150000, 's');
  cluster.Submit(leader, "large", large_value, majority);
  for (size_t round = 0; round < 3; round++) {
    cluster.Submit(leader, "generation", std::to_string(round), majority);
    for (NodeId id : majority) {
      cluster.Node(id).CreateSnapshot();
    }
  }
  const auto last = cluster.Submit(leader, "suffix", "after-compaction", majority);
  const auto base = cluster.Node(leader).Log().SnapshotBaseIndex();
  ASSERT_GT(base, 0);
  cluster.SetUnreliable(true);
  cluster.Restart(lagging);
  ASSERT_LT(cluster.Node(lagging).Log().LastLogIndex(), base);
  cluster.WaitApplied(last, {1, 2, 3});
  EXPECT_GT(cluster.SnapshotChunks(), 2);
  ASSERT_TRUE(cluster.Node(lagging).LatestSnapshot().has_value());
  EXPECT_EQ(cluster.Machine(lagging).Get("large"), std::optional<std::string>{large_value});
  cluster.Crash(lagging);
  cluster.Restart(lagging);
  EXPECT_GE(cluster.Node(lagging).LastApplied(), last);
  EXPECT_EQ(cluster.Machine(lagging).Get("suffix"), std::optional<std::string>{"after-compaction"});
  leader = cluster.WaitLeader();
  cluster.Submit(leader, "after-restart", "available");
}

INSTANTIATE_TEST_SUITE_P(FixedSeeds, RaftSeededFaultTest, ::testing::Values(824ULL, 5840ULL, 202409ULL),
                         [](const ::testing::TestParamInfo<uint64_t> &info) {
                           return "Seed" + std::to_string(info.param);
                         });

}  // namespace
}  // namespace bustub
