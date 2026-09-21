//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_bustub_cluster_test.cpp
//
//===----------------------------------------------------------------------===//

#include <unistd.h>

#include <array>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "distributed/raft_state_machine.h"
#include "gtest/gtest.h"
#include "raft/in_memory_raft_transport.h"
#include "raft/raft_node.h"
#include "type/value_factory.h"

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

class ThreeNodeBusTubCluster {
 public:
  explicit ThreeNodeBusTubCluster(std::string_view suffix)
      : root_(std::filesystem::temp_directory_path() /
              ("bustub-raft-bustub-cluster-" + std::to_string(getpid()) + "-" + std::string(suffix))),
        storage_(std::make_shared<PosixDurableStorage>()),
        transport_(std::make_shared<InMemoryRaftTransport>()) {
    storage_->RemoveTree(root_);
    for (size_t offset = 0; offset < nodes_.size(); offset++) {
      const auto id = static_cast<NodeId>(offset) + NodeId{1};
      directories_[offset] = NodeDirectory::Open(root_ / ("node-" + std::to_string(id)), storage_);
      machines_[offset] = BusTubRaftStateMachine::Open(directories_[offset].get(), storage_, 64);
      const auto raft_directory = directories_[offset]->RaftDirectory();
      nodes_[offset] = std::make_unique<RaftNode>(
          RaftNodeConfig{id, {1, 2, 3}, 100, 300, 50, "test-group", MakeFixedElectionTimeoutSource(100U * id)},
          transport_, StableStore::Open(raft_directory, storage_), LogStore::Open(raft_directory / "log", storage_, 0),
          machines_[offset], SnapshotStore::Open(raft_directory / "snapshots", storage_));
    }
    for (size_t offset = 0; offset < nodes_.size(); offset++) {
      const auto id = static_cast<NodeId>(offset) + NodeId{1};
      transport_->Register(
          id, [this, offset](NodeId from, const RaftMessage &message) { nodes_[offset]->Receive(from, message); });
    }
  }

  ~ThreeNodeBusTubCluster() {
    for (NodeId id = 1; id <= 3; id++) {
      transport_->Unregister(id);
    }
    for (auto &node : nodes_) {
      node.reset();
    }
    for (auto &machine : machines_) {
      machine.reset();
    }
    for (auto &directory : directories_) {
      directory.reset();
    }
    storage_->RemoveTree(root_);
  }

  auto Node(NodeId id) -> RaftNode & { return *nodes_.at(id - 1); }
  auto Machine(NodeId id) -> BusTubRaftStateMachine & { return *machines_.at(id - 1); }
  auto Transport() -> InMemoryRaftTransport & { return *transport_; }

  void Elect(NodeId id, uint64_t now_ms) {
    Node(id).Tick(now_ms);
    Transport().DeliverAll();
    if (Node(id).Role() != RaftRole::LEADER || !Node(id).LeaderReady()) {
      throw std::runtime_error("deterministic BusTub cluster election did not become ready");
    }
  }

  auto Submit(NodeId leader, const std::string &sql, uint64_t client_id, uint64_t request_id)
      -> std::vector<std::byte> {
    const auto request_fingerprint = ComputeWriteIntentFingerprintV1(sql);
    const auto batch = Machine(leader).PrepareSql(sql, client_id, request_id, request_fingerprint);
    Machine(leader).ValidateProposal(batch);
    const auto index = Node(leader).Propose(EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(batch));
    if (!index.has_value()) {
      throw std::runtime_error("ready Leader rejected a BusTub proposal");
    }
    Transport().DeliverAll();
    const auto response = Machine(leader).GetLastResponse(client_id);
    if (!response.has_value() || WriteResponseCodec::Decode(*response).commit_index_ != *index) {
      throw std::runtime_error("BusTub proposal did not publish its committed response");
    }
    return *response;
  }

  auto ProposeWithoutPumping(NodeId leader, const TransactionCommandBatch &batch) -> uint64_t {
    Machine(leader).ValidateProposal(batch);
    const auto index = Node(leader).Propose(EntryType::COMMAND_BATCH, CommandBatchCodec::Encode(batch));
    if (!index.has_value()) {
      throw std::runtime_error("ready Leader rejected an explicitly scheduled BusTub proposal");
    }
    return *index;
  }

  void PartitionNodeOne(bool partitioned) {
    for (NodeId peer : {NodeId{2}, NodeId{3}}) {
      Transport().SetLinkEnabled(1, peer, !partitioned);
      Transport().SetLinkEnabled(peer, 1, !partitioned);
    }
  }

 private:
  std::filesystem::path root_;
  std::shared_ptr<PosixDurableStorage> storage_;
  std::shared_ptr<InMemoryRaftTransport> transport_;
  std::array<std::unique_ptr<NodeDirectory>, 3> directories_;
  std::array<std::shared_ptr<BusTubRaftStateMachine>, 3> machines_;
  std::array<std::unique_ptr<RaftNode>, 3> nodes_;
};

auto Key(int32_t value) -> EncodedPrimaryKeyV1 {
  return PrimaryKeyCodecV1::Encode(ValueFactory::GetIntegerValue(value));
}

void ExpectLogicalState(ThreeNodeBusTubCluster *cluster, NodeId id, uint64_t expected_index) {
  EXPECT_EQ(cluster->Node(id).CommitIndex(), expected_index);
  EXPECT_EQ(cluster->Node(id).LastApplied(), expected_index);
  EXPECT_EQ(cluster->Machine(id).PublishedAppliedIndex(), expected_index);
  const auto catalog = cluster->Machine(id).CatalogSnapshotForRead();
  EXPECT_EQ(catalog.schema_epoch_, 2);
  EXPECT_EQ(catalog.next_table_oid_, 1);
  EXPECT_EQ(catalog.next_index_oid_, 2);
  EXPECT_EQ(catalog.tables_.size(), 1);
  EXPECT_EQ(catalog.indexes_.size(), 2);
  EXPECT_FALSE(cluster->Machine(id).GetRow(0, Key(1)).has_value());
  const auto row = cluster->Machine(id).GetRow(0, Key(2));
  ASSERT_TRUE(row.has_value());
  EXPECT_EQ(row->first.ts_, 5);
  EXPECT_EQ(row->second.GetValue(&catalog.tables_[0].schema_, 1).ToString(), "updated");
}

}  // namespace

// M6-IT01: real BusTub command batches use the Raft durability/commit path and survive a Leader change.
TEST(RaftBusTubClusterTest, NormalReplicationLeaderChangeAndByteExactRetry) {
  ThreeNodeBusTubCluster cluster("leader-change");
  cluster.Elect(1, 100);

  const auto initial_last_index = cluster.Node(1).Log().LastLogIndex();
  const std::string missing_primary_key_sql = "CREATE TABLE missing_pk(value int);";
  EXPECT_THROW(cluster.Machine(1).PrepareSql(missing_primary_key_sql, 700, 1,
                                             ComputeWriteIntentFingerprintV1(missing_primary_key_sql)),
               std::runtime_error);
  EXPECT_EQ(cluster.Node(1).Log().LastLogIndex(), initial_last_index);

  cluster.Submit(1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));", 700, 1);
  const auto before_unique = cluster.Node(1).Log().LastLogIndex();
  const std::string unsupported_unique_sql = "CREATE UNIQUE INDEX invalid_unique ON accounts(name);";
  EXPECT_THROW(cluster.Machine(1).PrepareSql(unsupported_unique_sql, 700, 2,
                                             ComputeWriteIntentFingerprintV1(unsupported_unique_sql)),
               std::runtime_error);
  EXPECT_EQ(cluster.Node(1).Log().LastLogIndex(), before_unique);

  cluster.Submit(1, "INSERT INTO accounts VALUES (2, 'two'), (1, 'one');", 700, 2);
  cluster.Submit(1, "CREATE INDEX accounts_name ON accounts(name);", 700, 3);
  const std::string update_sql = "UPDATE accounts SET name = 'updated' WHERE id <= 2;";
  const auto update_fingerprint = ComputeWriteIntentFingerprintV1(update_sql);
  const auto uncertain = cluster.Submit(1, update_sql, 700, 4);
  EXPECT_EQ(WriteResponseCodec::Decode(uncertain), (WriteResponseV1{1, WriteStatus::COMMITTED, 4, 1, 5}));
  for (NodeId id = 1; id <= 3; id++) {
    EXPECT_EQ(cluster.Machine(id).GetLastResponse(700), uncertain);
    EXPECT_EQ(cluster.Machine(id).GetRow(0, Key(1))->first.ts_, 5);
    EXPECT_EQ(cluster.Machine(id).GetRow(0, Key(2))->first.ts_, 5);
  }

  cluster.PartitionNodeOne(true);
  cluster.Elect(2, 200);
  ASSERT_EQ(cluster.Node(2).CurrentTerm(), 2);
  ASSERT_EQ(cluster.Node(2).CommitIndex(), 6);
  const auto before_retry = cluster.Node(2).Log().LastLogIndex();
  EXPECT_EQ(cluster.Machine(2).ClassifyRequest(700, 4, update_fingerprint), RequestDisposition::RETRY_LAST);
  EXPECT_EQ(cluster.Machine(2).ClassifyRequest(
                700, 4, ComputeWriteIntentFingerprintV1("UPDATE accounts SET name = 'changed' WHERE id <= 2;")),
            RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(cluster.Machine(2).GetLastResponse(700), uncertain);
  EXPECT_EQ(cluster.Node(2).Log().LastLogIndex(), before_retry);

  const auto after_switch = cluster.Submit(2, "DELETE FROM accounts WHERE id = 1;", 700, 5);
  EXPECT_EQ(WriteResponseCodec::Decode(after_switch), (WriteResponseV1{1, WriteStatus::COMMITTED, 5, 2, 7}));
  cluster.PartitionNodeOne(false);
  cluster.Node(2).Tick(250);
  cluster.Transport().DeliverAll();

  for (NodeId id = 1; id <= 3; id++) {
    ExpectLogicalState(&cluster, id, 7);
    EXPECT_EQ(cluster.Machine(id).GetLastResponse(700), after_switch);
  }
  EXPECT_EQ(cluster.Node(1).Role(), RaftRole::FOLLOWER);
  EXPECT_EQ(cluster.Node(1).CurrentTerm(), 2);
}

// M6-IT03: a Leader may acknowledge after one follower has durably appended the entry but before that follower learns
// the new commit index. The replacement Leader's current-term NOOP must apply the inherited entry and its session
// record before it serves the retry.
TEST(RaftBusTubClusterTest, AmbiguousCommitNotificationLossStillRestoresExactOnceSessionOnNewLeader) {
  ThreeNodeBusTubCluster cluster("delayed-commit-notification");
  cluster.Elect(1, 100);

  cluster.Submit(1, "CREATE TABLE counters(id int PRIMARY KEY, balance int);", 901, 1);
  cluster.Submit(1, "INSERT INTO counters VALUES (1, 10);", 901, 2);

  cluster.Transport().SetLinkEnabled(1, 3, false);
  cluster.Transport().SetLinkEnabled(3, 1, false);
  const std::string original_sql = "UPDATE counters SET balance = balance + 7 WHERE id = 1;";
  const auto original_fingerprint = ComputeWriteIntentFingerprintV1(original_sql);
  const auto update_batch = cluster.Machine(1).PrepareSql(original_sql, 901, 3, original_fingerprint);
  ASSERT_EQ(cluster.ProposeWithoutPumping(1, update_batch), 4);

  // AppendEntries(1 -> 2), followed by its acknowledgement (2 -> 1). The latter commits and applies index 4 on the
  // Leader, but its queued leader_commit=4 notification is deliberately lost with the old Leader. The modeled
  // client also receives no response; the test-only server-cache observation below is not returned through a client
  // channel.
  ASSERT_TRUE(cluster.Transport().DeliverOne());
  ASSERT_TRUE(cluster.Transport().DeliverOne());
  ASSERT_EQ(cluster.Node(1).CommitIndex(), 4);
  ASSERT_EQ(cluster.Node(1).LastApplied(), 4);
  ASSERT_EQ(cluster.Node(2).CommitIndex(), 3);
  ASSERT_EQ(cluster.Machine(2).ClassifyRequest(901, 3, original_fingerprint), RequestDisposition::NEW_REQUEST);
  const auto original = cluster.Machine(1).GetLastResponse(901);
  ASSERT_TRUE(original.has_value());
  EXPECT_EQ(WriteResponseCodec::Decode(*original), (WriteResponseV1{1, WriteStatus::COMMITTED, 3, 1, 4}));
  cluster.Transport().Clear();

  cluster.PartitionNodeOne(true);
  cluster.Elect(2, 200);
  ASSERT_EQ(cluster.Node(2).CurrentTerm(), 2);
  ASSERT_EQ(cluster.Node(2).CommitIndex(), 5);
  ASSERT_EQ(cluster.Node(2).LastApplied(), 5);
  const auto before_retry = cluster.Node(2).Log().LastLogIndex();
  EXPECT_EQ(cluster.Machine(2).ClassifyRequest(901, 3, original_fingerprint), RequestDisposition::RETRY_LAST);
  const auto recovered = cluster.Machine(2).GetLastResponse(901);
  ASSERT_TRUE(recovered.has_value());
  EXPECT_EQ(WriteResponseCodec::Decode(*recovered), (WriteResponseV1{1, WriteStatus::COMMITTED, 3, 1, 4}));
  EXPECT_EQ(*recovered, *original);

  const std::string changed_sql = "UPDATE counters SET balance = balance + 70 WHERE id = 1;";
  EXPECT_EQ(cluster.Machine(2).ClassifyRequest(901, 3, ComputeWriteIntentFingerprintV1(changed_sql)),
            RequestDisposition::PAYLOAD_MISMATCH);
  EXPECT_EQ(cluster.Machine(2).GetLastResponse(901), recovered);
  EXPECT_EQ(cluster.Node(2).Log().LastLogIndex(), before_retry);

  const auto catalog = cluster.Machine(2).CatalogSnapshotForRead();
  ASSERT_EQ(catalog.tables_.size(), 1);
  const auto row = cluster.Machine(2).GetRow(0, Key(1));
  ASSERT_TRUE(row.has_value());
  EXPECT_EQ(row->first.ts_, 4);
  EXPECT_EQ(row->second.GetValue(&catalog.tables_[0].schema_, 0).GetAs<int32_t>(), 1);
  EXPECT_EQ(row->second.GetValue(&catalog.tables_[0].schema_, 1).GetAs<int32_t>(), 17);
}

}  // namespace bustub
