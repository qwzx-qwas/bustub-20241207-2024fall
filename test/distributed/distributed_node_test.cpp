//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distributed_node_test.cpp
//
//===----------------------------------------------------------------------===//

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>              // NOLINT(build/c++11)
#include <condition_variable>  // NOLINT(build/c++11)
#include <filesystem>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>  // NOLINT(build/c++11)

#include "distributed/client.h"
#include "distributed/node.h"
#include "gtest/gtest.h"
#include "raft/snapshot_store.h"

namespace bustub {
namespace {

auto AllocateLoopbackPorts(size_t count) -> std::vector<uint16_t> {
  std::set<uint16_t> unique;
  while (unique.size() < count) {
    const auto socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
      throw std::runtime_error("cannot allocate loopback test socket");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(socket_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
      close(socket_fd);
      throw std::runtime_error("cannot reserve loopback test port");
    }
    socklen_t size = sizeof(address);
    if (getsockname(socket_fd, reinterpret_cast<sockaddr *>(&address), &size) != 0) {
      close(socket_fd);
      throw std::runtime_error("cannot inspect loopback test port");
    }
    unique.insert(ntohs(address.sin_port));
    close(socket_fd);
  }
  return {unique.begin(), unique.end()};
}

class ThreeNodeInProcessCluster {
 public:
  explicit ThreeNodeInProcessCluster(uint64_t snapshot_threshold_entries = 10000)
      : root_(std::filesystem::temp_directory_path() / ("bustub-production-node-test-" + std::to_string(getpid()))),
        storage_(std::make_shared<PosixDurableStorage>()) {
    storage_->RemoveTree(root_);
    const auto ports = AllocateLoopbackPorts(6);
    for (size_t offset = 0; offset < 3; offset++) {
      raft_endpoints_[offset] = {"127.0.0.1", ports[offset]};
      client_endpoints_[offset] = {"127.0.0.1", ports[offset + 3]};
    }
    for (size_t offset = 0; offset < 3; offset++) {
      const auto id = static_cast<NodeId>(offset) + NodeId{1};
      std::map<NodeId, DistributedPeerConfig> peers;
      for (size_t peer_offset = 0; peer_offset < 3; peer_offset++) {
        const auto peer_id = static_cast<NodeId>(peer_offset) + NodeId{1};
        if (peer_id != id) {
          peers.emplace(peer_id, DistributedPeerConfig{raft_endpoints_[peer_offset], client_endpoints_[peer_offset]});
        }
      }
      configs_[offset] = {id,
                          "production-test",
                          root_ / ("node-" + std::to_string(id)),
                          raft_endpoints_[offset],
                          client_endpoints_[offset],
                          std::move(peers),
                          200,
                          400,
                          50,
                          10,
                          2000,
                          64,
                          snapshot_threshold_entries};
      nodes_[offset] = DistributedNode::Open(configs_[offset], storage_);
    }
    for (auto &node : nodes_) {
      node->Start();
    }
  }

  ~ThreeNodeInProcessCluster() {
    for (auto &node : nodes_) {
      node.reset();
    }
    storage_->RemoveTree(root_);
  }

  auto AwaitLeader(std::optional<NodeId> excluded = std::nullopt) -> NodeId {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
      for (NodeId id = 1; id <= 3; id++) {
        if ((excluded.has_value() && id == *excluded) || nodes_[id - 1] == nullptr) {
          continue;
        }
        try {
          const auto response =
              DistributedClient::Send(client_endpoints_[id - 1], ClientStatusRequestV1{next_status_request_++}, 300);
          if (response.status_ == ClientResponseStatus::OK && response.leader_ready_ && response.leader_id_ == id &&
              response.commit_index_ >= 1 && response.last_applied_ >= 1) {
            return id;
          }
        } catch (const std::exception &) {
          continue;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    throw std::runtime_error("timed out waiting for a production Leader");
  }

  void AwaitCommit(uint64_t index) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
      bool complete = true;
      for (NodeId id = 1; id <= 3; id++) {
        if (nodes_[id - 1] == nullptr) {
          continue;
        }
        try {
          const auto response =
              DistributedClient::Send(client_endpoints_[id - 1], ClientStatusRequestV1{next_status_request_++}, 300);
          complete &= response.commit_index_ >= index && response.last_applied_ >= index &&
                      response.published_applied_index_ >= index;
        } catch (const std::exception &) {
          complete = false;
        }
      }
      if (complete) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    throw std::runtime_error("timed out waiting for all production nodes to Apply");
  }

  void AwaitSnapshotBase(NodeId id, uint64_t index) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (std::chrono::steady_clock::now() < deadline) {
      try {
        const auto response =
            DistributedClient::Send(client_endpoints_[id - 1], ClientStatusRequestV1{next_status_request_++}, 500);
        if (response.status_ == ClientResponseStatus::OK && response.snapshot_base_index_ >= index &&
            response.published_applied_index_ >= index) {
          return;
        }
      } catch (const std::exception &) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    throw std::runtime_error("timed out waiting for production snapshot base");
  }

  void AwaitSnapshotGenerationCount(NodeId id, size_t expected) {
    const auto snapshot_directory = configs_.at(id - 1).data_directory_ / "raft" / "snapshots";
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (std::chrono::steady_clock::now() < deadline) {
      size_t count = 0;
      for (const auto &entry : storage_->ListDirectory(snapshot_directory)) {
        count += entry.rfind("SNAPSHOT-", 0) == 0 && entry.find(".tmp") == std::string::npos ? 1 : 0;
      }
      if (count == expected) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    throw std::runtime_error("timed out waiting for retained snapshot generations");
  }

  void CorruptLatestSnapshot(NodeId id) {
    const auto snapshot_directory = configs_.at(id - 1).data_directory_ / "raft" / "snapshots";
    std::vector<std::string> snapshots;
    for (const auto &entry : storage_->ListDirectory(snapshot_directory)) {
      if (entry.rfind("SNAPSHOT-", 0) == 0 && entry.find(".tmp") == std::string::npos) {
        snapshots.push_back(entry);
      }
    }
    if (snapshots.size() != 2) {
      throw std::runtime_error("latest-snapshot corruption requires exactly two retained generations");
    }
    const auto path = snapshot_directory / snapshots.back();
    auto bytes = storage_->ReadFile(path, SnapshotStore::MAX_SNAPSHOT_BYTES + 4096);
    bytes[bytes.size() / 2] ^= std::byte{1};
    storage_->WriteFile(path, bytes);
    storage_->SyncFile(path);
  }

  auto LogJournalSize(NodeId id) -> uint64_t {
    return storage_->FileSize(configs_.at(id - 1).data_directory_ / "raft" / "log" / "LOG-MUTATIONS");
  }

  auto AwaitLogJournalGrowth(NodeId id, uint64_t previous_size) -> uint64_t {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
      const auto current_size = LogJournalSize(id);
      if (current_size > previous_size) {
        return current_size;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    throw std::runtime_error("timed out waiting for a durable Raft proposal");
  }

  void CorruptLogJournal(NodeId id) {
    const auto path = configs_.at(id - 1).data_directory_ / "raft" / "log" / "LOG-MUTATIONS";
    auto bytes = storage_->ReadFile(path, static_cast<size_t>(storage_->FileSize(path)));
    if (bytes.empty()) {
      throw std::runtime_error("cannot corrupt an empty Raft log journal");
    }
    bytes[bytes.size() / 2] ^= std::byte{1};
    storage_->WriteFile(path, bytes);
    storage_->SyncFile(path);
  }

  auto Send(NodeId id, const ClientRequestV1 &request) -> ClientResponseV1 {
    return DistributedClient::Send(client_endpoints_.at(id - 1), request, 4000);
  }

  auto LatestSnapshot(NodeId id) -> RaftSnapshot {
    auto snapshots = SnapshotStore::Open(configs_.at(id - 1).data_directory_ / "raft" / "snapshots", storage_);
    const auto latest = snapshots->Latest();
    if (!latest.has_value()) {
      throw std::runtime_error("node has no published snapshot");
    }
    return *latest;
  }

  auto SnapshotCurrentBytes(NodeId id) -> std::vector<std::byte> {
    return storage_->ReadFile(configs_.at(id - 1).data_directory_ / "raft" / "snapshots" / "CURRENT", 4096);
  }

  auto ProbeStaleSnapshotAsPeer(NodeId from, NodeId to, uint64_t term, const RaftSnapshot &snapshot)
      -> InstallSnapshotResponse {
    constexpr size_t chunk_bytes = 64U * 1024U;
    auto snapshots = SnapshotStore::Open(configs_.at(from - 1).data_directory_ / "raft" / "snapshots", storage_);
    auto first_chunk = snapshots->ReadPayloadChunk(snapshot, 0, chunk_bytes);
    constexpr uint64_t request_id = 0xdecafbadULL;
    std::mutex response_mutex;
    std::condition_variable response_cv;
    std::optional<InstallSnapshotResponse> observed;
    TcpRaftTransport probe(from, "production-test", raft_endpoints_.at(from - 1), {{to, raft_endpoints_.at(to - 1)}});
    probe.Start([&](RaftEnvelope envelope) {
      if (envelope.from_ == to && envelope.to_ == from &&
          std::holds_alternative<InstallSnapshotResponse>(envelope.message_)) {
        const auto &response = std::get<InstallSnapshotResponse>(envelope.message_);
        if (response.request_id_ == request_id) {
          {
            std::lock_guard lock(response_mutex);
            observed = response;
          }
          response_cv.notify_all();
        }
      }
    });
    probe.Send(
        {from, to,
         InstallSnapshotRequest{term, from, request_id, snapshot.snapshot_id_, snapshot.last_included_index_,
                                snapshot.last_included_term_, 0, snapshot.payload_size_, snapshot.payload_checksum_,
                                first_chunk.size() == snapshot.payload_size_, std::move(first_chunk)},
         "production-test"});
    {
      std::unique_lock lock(response_mutex);
      static_cast<void>(response_cv.wait_for(lock, std::chrono::seconds(3), [&] { return observed.has_value(); }));
    }
    probe.Stop();
    if (!observed.has_value()) {
      throw std::runtime_error("stale snapshot probe received no InstallSnapshotResponse");
    }
    return *observed;
  }

  void StopAndDestroy(NodeId id) { nodes_.at(id - 1).reset(); }

  void Pause(NodeId id) { nodes_.at(id - 1)->Stop(); }

  void Resume(NodeId id) { nodes_.at(id - 1)->Start(); }

  void StopAll() {
    for (auto &node : nodes_) {
      node.reset();
    }
  }

  void Restart(NodeId id) {
    nodes_.at(id - 1) = DistributedNode::Open(configs_.at(id - 1), storage_);
    nodes_.at(id - 1)->Start();
  }

 private:
  std::filesystem::path root_;
  std::shared_ptr<PosixDurableStorage> storage_;
  std::array<TcpEndpoint, 3> raft_endpoints_;
  std::array<TcpEndpoint, 3> client_endpoints_;
  std::array<DistributedNodeConfig, 3> configs_;
  std::array<std::unique_ptr<DistributedNode>, 3> nodes_;
  uint64_t next_status_request_{1000};
};

}  // namespace

// M6-IT02: three production assemblies communicate only through the stable TCP protocols and real node directories.
TEST(DistributedNodeTest, TcpWriteReadLeaderChangeRetryAndReopenCatchup) {
  ThreeNodeInProcessCluster cluster;
  const auto first_leader = cluster.AwaitLeader();
  const auto follower = first_leader == 1 ? NodeId{2} : NodeId{1};

  const auto routed = cluster.Send(
      follower,
      ClientWriteRequestV1{900, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32), balance int);"});
  EXPECT_EQ(routed.status_, ClientResponseStatus::NOT_LEADER);
  EXPECT_EQ(routed.leader_id_, first_leader);
  EXPECT_FALSE(routed.leader_address_.empty());

  EXPECT_EQ(cluster.Send(first_leader, ClientWriteRequestV1{900, 1, "CREATE TABLE accounts(value int);"}).status_,
            ClientResponseStatus::REJECTED);
  EXPECT_EQ(cluster
                .Send(first_leader,
                      ClientWriteRequestV1{900, 1,
                                           "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32), balance int);"})
                .status_,
            ClientResponseStatus::COMMITTED);
  EXPECT_EQ(
      cluster.Send(first_leader, ClientWriteRequestV1{900, 2, "CREATE UNIQUE INDEX accounts_name ON accounts(name);"})
          .status_,
      ClientResponseStatus::REJECTED);
  EXPECT_EQ(cluster
                .Send(first_leader,
                      ClientWriteRequestV1{900, 2, "INSERT INTO accounts VALUES (2, 'two', 20), (1, 'one', 10);"})
                .status_,
            ClientResponseStatus::COMMITTED);
  EXPECT_EQ(
      cluster.Send(first_leader, ClientWriteRequestV1{900, 3, "CREATE INDEX accounts_name ON accounts(name);"}).status_,
      ClientResponseStatus::COMMITTED);
  const auto uncertain = cluster.Send(
      first_leader, ClientWriteRequestV1{900, 4, "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;"});
  ASSERT_EQ(uncertain.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(WriteResponseCodec::Decode(uncertain.payload_).commit_index_, 5);
  cluster.AwaitCommit(5);

  const auto linear =
      cluster.Send(first_leader, ClientReadRequestV1{100, ClientReadConsistency::LINEARIZABLE,
                                                     "SELECT id, name, balance FROM accounts ORDER BY id;"});
  ASSERT_EQ(linear.status_, ClientResponseStatus::OK);
  ASSERT_TRUE(linear.read_timestamp_.has_value());
  EXPECT_GE(*linear.read_timestamp_, 5);
  EXPECT_GE(*linear.read_timestamp_, linear.commit_index_);
  const auto linear_result = ClientQueryResultCodec::Decode(linear.payload_);
  EXPECT_EQ(linear_result.columns_, (std::vector<std::string>{"accounts.id", "accounts.name", "accounts.balance"}));
  EXPECT_EQ(linear_result.rows_, (std::vector<std::vector<std::string>>{{"1", "one", "17"}, {"2", "two", "27"}}));

  for (NodeId id = 1; id <= 3; id++) {
    const auto stale =
        cluster.Send(id, ClientReadRequestV1{200 + id, ClientReadConsistency::STALE, "SELECT count(*) FROM accounts;"});
    ASSERT_EQ(stale.status_, ClientResponseStatus::OK);
    EXPECT_EQ(stale.read_timestamp_, stale.published_applied_index_);
    EXPECT_EQ(ClientQueryResultCodec::Decode(stale.payload_).rows_, (std::vector<std::vector<std::string>>{{"2"}}));
  }

  cluster.StopAndDestroy(first_leader);
  const auto second_leader = cluster.AwaitLeader(first_leader);
  ASSERT_NE(second_leader, first_leader);
  const auto retry = cluster.Send(
      second_leader, ClientWriteRequestV1{900, 4, "UPDATE accounts SET balance = balance + 7 WHERE id <= 2;"});
  EXPECT_EQ(retry.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(retry.payload_, uncertain.payload_);

  const auto journal_before_mismatch = cluster.LogJournalSize(second_leader);
  const auto changed_retry =
      cluster.Send(second_leader, ClientWriteRequestV1{900, 4, "UPDATE accounts SET balance = 999 WHERE id <= 2;"});
  EXPECT_EQ(changed_retry.status_, ClientResponseStatus::REJECTED);
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(changed_retry.payload_.data()), changed_retry.payload_.size()),
            "request payload does not match request identity");
  EXPECT_EQ(cluster.LogJournalSize(second_leader), journal_before_mismatch);
  const auto unchanged = cluster.Send(
      second_leader,
      ClientReadRequestV1{250, ClientReadConsistency::LINEARIZABLE, "SELECT id, balance FROM accounts ORDER BY id;"});
  ASSERT_EQ(unchanged.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(unchanged.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"1", "17"}, {"2", "27"}}));

  const auto after_switch =
      cluster.Send(second_leader, ClientWriteRequestV1{900, 5, "DELETE FROM accounts WHERE id = 1;"});
  ASSERT_EQ(after_switch.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(WriteResponseCodec::Decode(after_switch.payload_).commit_index_, 7);

  cluster.Restart(first_leader);
  cluster.AwaitCommit(7);
  const auto recovered = cluster.Send(
      first_leader,
      ClientReadRequestV1{300, ClientReadConsistency::STALE, "SELECT id, name, balance FROM accounts ORDER BY id;"});
  ASSERT_EQ(recovered.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(recovered.payload_).columns_,
            (std::vector<std::string>{"accounts.id", "accounts.name", "accounts.balance"}));
  EXPECT_EQ(ClientQueryResultCodec::Decode(recovered.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"2", "two", "27"}}));
}

// E2E-05 TCP prerequisite: an in-process follower receives a canonical snapshot over real TCP, then applies S+1.
TEST(DistributedNodeTest, TcpSnapshotCatchupThenSuffixApply) {
  ThreeNodeInProcessCluster cluster(4);
  const auto leader = cluster.AwaitLeader();
  NodeId lagging_follower = 1;
  while (lagging_follower == leader) {
    lagging_follower++;
  }

  const auto create = cluster.Send(
      leader, ClientWriteRequestV1{901, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"});
  ASSERT_EQ(create.status_, ClientResponseStatus::COMMITTED);
  ASSERT_EQ(WriteResponseCodec::Decode(create.payload_).commit_index_, 2);
  cluster.AwaitCommit(2);
  cluster.StopAndDestroy(lagging_follower);

  ASSERT_EQ(
      cluster.Send(leader, ClientWriteRequestV1{901, 2, "INSERT INTO accounts VALUES (2, 'two'), (1, 'one');"}).status_,
      ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{901, 3, "CREATE INDEX accounts_name ON accounts(name);"}).status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitSnapshotBase(leader, 4);

  const auto suffix =
      cluster.Send(leader, ClientWriteRequestV1{901, 4, "UPDATE accounts SET name = 'updated' WHERE id <= 2;"});
  ASSERT_EQ(suffix.status_, ClientResponseStatus::COMMITTED);
  ASSERT_EQ(WriteResponseCodec::Decode(suffix.payload_).commit_index_, 5);
  cluster.AwaitCommit(5);

  cluster.Restart(lagging_follower);
  cluster.AwaitCommit(5);
  cluster.AwaitSnapshotBase(lagging_follower, 4);

  const auto recovered = cluster.Send(
      lagging_follower, ClientReadRequestV1{400, ClientReadConsistency::STALE,
                                            "SELECT id, name FROM accounts WHERE name = 'updated' ORDER BY id;"});
  const std::string recovered_message(reinterpret_cast<const char *>(recovered.payload_.data()),
                                      recovered.payload_.size());
  ASSERT_EQ(recovered.status_, ClientResponseStatus::OK) << recovered_message;
  EXPECT_EQ(recovered.snapshot_base_index_, 4);
  EXPECT_GE(recovered.published_applied_index_, 5);
  EXPECT_EQ(ClientQueryResultCodec::Decode(recovered.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"1", "updated"}, {"2", "updated"}}));
  // Applying request 4 after Snapshot@4 also proves that requests 1..3 were restored in SessionTable: an empty
  // session table would classify request 4 as a sequence gap and fail-stop before this read could succeed.

  // E2E-15 TCP prerequisite: replay Snapshot@4 only after this Follower has applied suffix index 5.
  const auto delayed_snapshot = cluster.LatestSnapshot(leader);
  ASSERT_EQ(delayed_snapshot.last_included_index_, 4);
  const auto current_before = cluster.SnapshotCurrentBytes(lagging_follower);
  const auto status_before = cluster.Send(lagging_follower, ClientStatusRequestV1{450});
  cluster.StopAndDestroy(leader);
  const auto stale_response =
      cluster.ProbeStaleSnapshotAsPeer(leader, lagging_follower, status_before.term_ + 1, delayed_snapshot);
  EXPECT_EQ(stale_response.term_, status_before.term_ + 1);
  EXPECT_EQ(stale_response.request_id_, 0xdecafbadULL);
  EXPECT_TRUE(stale_response.success_);
  EXPECT_TRUE(stale_response.stale_);
  EXPECT_TRUE(stale_response.complete_);
  EXPECT_GE(stale_response.match_index_, status_before.published_applied_index_);
  EXPECT_EQ(stale_response.next_offset_, 0);
  const auto status_after = cluster.Send(lagging_follower, ClientStatusRequestV1{451});
  EXPECT_EQ(cluster.SnapshotCurrentBytes(lagging_follower), current_before);
  EXPECT_EQ(status_after.snapshot_base_index_, status_before.snapshot_base_index_);
  EXPECT_EQ(status_after.commit_index_, status_before.commit_index_);
  EXPECT_EQ(status_after.last_applied_, status_before.last_applied_);
  EXPECT_EQ(status_after.published_applied_index_, status_before.published_applied_index_);

  cluster.Restart(leader);
  const auto current_leader = cluster.AwaitLeader();
  const auto after_stale =
      cluster.Send(current_leader, ClientWriteRequestV1{901, 5, "INSERT INTO accounts VALUES (3, 'after-stale');"});
  ASSERT_EQ(after_stale.status_, ClientResponseStatus::COMMITTED);
  const auto after_stale_index = WriteResponseCodec::Decode(after_stale.payload_).commit_index_;
  cluster.AwaitCommit(after_stale_index);
  const auto continued =
      cluster.Send(lagging_follower, ClientReadRequestV1{452, ClientReadConsistency::STALE,
                                                         "SELECT id, name FROM accounts WHERE name = 'after-stale';"});
  ASSERT_EQ(continued.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(continued.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"3", "after-stale"}}));
}

// E2E-07 TCP prerequisite: every in-process node rebuilds from Snapshot@S plus a committed suffix, then advances OIDs.
TEST(DistributedNodeTest, FullClusterRestartPreservesSessionsAndContinuesDdl) {
  ThreeNodeInProcessCluster cluster(4);
  auto leader = cluster.AwaitLeader();
  ASSERT_EQ(
      cluster.Send(leader, ClientWriteRequestV1{902, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"})
          .status_,
      ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{902, 2, "INSERT INTO accounts VALUES (1, 'one');"}).status_,
            ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{902, 3, "CREATE INDEX accounts_name ON accounts(name);"}).status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitSnapshotBase(leader, 4);
  const auto before_restart =
      cluster.Send(leader, ClientWriteRequestV1{902, 4, "UPDATE accounts SET name = 'updated' WHERE id = 1;"});
  ASSERT_EQ(before_restart.status_, ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(WriteResponseCodec::Decode(before_restart.payload_).commit_index_);

  cluster.StopAll();
  cluster.Restart(3);
  cluster.Restart(1);
  cluster.Restart(2);
  leader = cluster.AwaitLeader();

  const auto retry =
      cluster.Send(leader, ClientWriteRequestV1{902, 4, "UPDATE accounts SET name = 'updated' WHERE id = 1;"});
  ASSERT_EQ(retry.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(retry.payload_, before_restart.payload_);
  ASSERT_EQ(
      cluster
          .Send(leader, ClientWriteRequestV1{902, 5, "CREATE TABLE ledger(code bigint PRIMARY KEY, note varchar(32));"})
          .status_,
      ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{902, 6, "INSERT INTO ledger VALUES (10, 'ten');"}).status_,
            ClientResponseStatus::COMMITTED);
  const auto continued_ddl =
      cluster.Send(leader, ClientWriteRequestV1{902, 7, "CREATE INDEX ledger_note ON ledger(note);"});
  ASSERT_EQ(continued_ddl.status_, ClientResponseStatus::COMMITTED);
  const auto final_index = WriteResponseCodec::Decode(continued_ddl.payload_).commit_index_;
  cluster.AwaitCommit(final_index);

  for (NodeId id = 1; id <= 3; id++) {
    const auto accounts =
        cluster.Send(id, ClientReadRequestV1{500 + id, ClientReadConsistency::STALE,
                                             "SELECT id, name FROM accounts WHERE name = 'updated';"});
    ASSERT_EQ(accounts.status_, ClientResponseStatus::OK);
    EXPECT_EQ(ClientQueryResultCodec::Decode(accounts.payload_).rows_,
              (std::vector<std::vector<std::string>>{{"1", "updated"}}));
    const auto ledger = cluster.Send(id, ClientReadRequestV1{600 + id, ClientReadConsistency::STALE,
                                                             "SELECT code, note FROM ledger WHERE note = 'ten';"});
    ASSERT_EQ(ledger.status_, ClientResponseStatus::OK);
    EXPECT_EQ(ClientQueryResultCodec::Decode(ledger.payload_).rows_,
              (std::vector<std::vector<std::string>>{{"10", "ten"}}));
  }
}

// E2E-11 TCP prerequisite: with every peer stopped, startup rejects damaged latest state and replays the bridge.
TEST(DistributedNodeTest, CorruptLatestSnapshotFallsBackAndReplaysBridge) {
  ThreeNodeInProcessCluster cluster(2);
  const auto leader = cluster.AwaitLeader();
  ASSERT_EQ(
      cluster.Send(leader, ClientWriteRequestV1{903, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"})
          .status_,
      ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(2);
  constexpr NodeId recovery_node = 1;
  cluster.AwaitSnapshotBase(recovery_node, 2);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{903, 2, "INSERT INTO accounts VALUES (1, 'one');"}).status_,
            ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{903, 3, "CREATE INDEX accounts_name ON accounts(name);"}).status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(4);

  cluster.AwaitSnapshotGenerationCount(recovery_node, 2);
  const auto suffix =
      cluster.Send(leader, ClientWriteRequestV1{903, 4, "UPDATE accounts SET name = 'bridged' WHERE id = 1;"});
  ASSERT_EQ(suffix.status_, ClientResponseStatus::COMMITTED);
  ASSERT_EQ(WriteResponseCodec::Decode(suffix.payload_).commit_index_, 5);
  cluster.AwaitCommit(5);

  cluster.StopAll();
  cluster.CorruptLatestSnapshot(recovery_node);
  cluster.Restart(recovery_node);
  cluster.AwaitCommit(5);
  const auto recovered = cluster.Send(
      recovery_node,
      ClientReadRequestV1{700, ClientReadConsistency::STALE, "SELECT id, name FROM accounts WHERE name = 'bridged';"});
  ASSERT_EQ(recovered.status_, ClientResponseStatus::OK);
  EXPECT_EQ(recovered.snapshot_base_index_, 2);
  EXPECT_EQ(recovered.published_applied_index_, 5);
  EXPECT_EQ(ClientQueryResultCodec::Decode(recovered.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"1", "bridged"}}));
}

// If the fully validated latest snapshot is exactly at H.commit, an older
// damaged bridge is no longer authoritative and can be atomically rebased.
TEST(DistributedNodeTest, FullyCoveringLatestSnapshotRebuildsDamagedBridgeLatestOnly) {
  ThreeNodeInProcessCluster cluster(2);
  const auto leader = cluster.AwaitLeader();
  ASSERT_EQ(
      cluster
          .Send(leader,
                ClientWriteRequestV1{909, 1, "CREATE TABLE fallback_latest(id int PRIMARY KEY, note varchar(32));"})
          .status_,
      ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(2);
  constexpr NodeId recovery_node = 1;
  cluster.AwaitSnapshotBase(recovery_node, 2);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{909, 2, "INSERT INTO fallback_latest VALUES (1, 'from-latest');"})
                .status_,
            ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{909, 3, "CREATE INDEX fallback_note ON fallback_latest(note);"})
                .status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(4);
  cluster.AwaitSnapshotGenerationCount(recovery_node, 2);
  ASSERT_EQ(cluster.LatestSnapshot(recovery_node).last_included_index_, 4);

  cluster.StopAll();
  cluster.CorruptLogJournal(recovery_node);
  ASSERT_NO_THROW(cluster.Restart(recovery_node));
  cluster.AwaitCommit(4);
  cluster.AwaitSnapshotGenerationCount(recovery_node, 1);
  auto recovered =
      cluster.Send(recovery_node,
                   ClientReadRequestV1{710, ClientReadConsistency::STALE,
                                       "SELECT id, note FROM fallback_latest WHERE note = 'from-latest' ORDER BY id;"});
  ASSERT_EQ(recovered.status_, ClientResponseStatus::OK);
  EXPECT_EQ(recovered.snapshot_base_index_, 4);
  EXPECT_EQ(ClientQueryResultCodec::Decode(recovered.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"1", "from-latest"}}));

  // The rebased journal and single retained generation must themselves survive
  // another cold reopen without relying on the now-deleted previous snapshot.
  cluster.StopAndDestroy(recovery_node);
  ASSERT_NO_THROW(cluster.Restart(recovery_node));
  recovered =
      cluster.Send(recovery_node,
                   ClientReadRequestV1{711, ClientReadConsistency::STALE,
                                       "SELECT id, note FROM fallback_latest WHERE note = 'from-latest' ORDER BY id;"});
  ASSERT_EQ(recovered.status_, ClientResponseStatus::OK);
  EXPECT_EQ(recovered.snapshot_base_index_, 4);
  EXPECT_EQ(ClientQueryResultCodec::Decode(recovered.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"1", "from-latest"}}));
}

// E2E-02/E2E-09 TCP prerequisite: former quorum ACKs authorize neither a new write nor ReadIndex after isolation.
TEST(DistributedNodeTest, IsolatedLeaderTimesOutAndItsUncommittedSuffixIsReplaced) {
  ThreeNodeInProcessCluster cluster;
  const auto old_leader = cluster.AwaitLeader();
  ASSERT_EQ(cluster
                .Send(old_leader,
                      ClientWriteRequestV1{904, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"})
                .status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(2);

  std::vector<NodeId> majority;
  for (NodeId id = 1; id <= 3; id++) {
    if (id != old_leader) {
      majority.push_back(id);
      cluster.StopAndDestroy(id);
    }
  }
  const auto isolated_write =
      cluster.Send(old_leader, ClientWriteRequestV1{904, 2, "INSERT INTO accounts VALUES (9, 'must-not-commit');"});
  EXPECT_EQ(isolated_write.status_, ClientResponseStatus::TIMEOUT);
  EXPECT_EQ(isolated_write.commit_index_, 2);
  const auto isolated_read = cluster.Send(
      old_leader, ClientReadRequestV1{800, ClientReadConsistency::LINEARIZABLE, "SELECT count(*) FROM accounts;"});
  EXPECT_EQ(isolated_read.status_, ClientResponseStatus::TIMEOUT);
  EXPECT_FALSE(isolated_read.read_timestamp_.has_value());

  cluster.StopAndDestroy(old_leader);
  cluster.Restart(majority[0]);
  cluster.Restart(majority[1]);
  const auto new_leader = cluster.AwaitLeader(old_leader);
  ASSERT_NE(new_leader, old_leader);
  cluster.Restart(old_leader);
  cluster.AwaitCommit(3);

  const auto after_replacement = cluster.Send(
      old_leader, ClientReadRequestV1{801, ClientReadConsistency::STALE, "SELECT id, name FROM accounts;"});
  ASSERT_EQ(after_replacement.status_, ClientResponseStatus::OK);
  EXPECT_TRUE(ClientQueryResultCodec::Decode(after_replacement.payload_).rows_.empty());

  const auto retry =
      cluster.Send(new_leader, ClientWriteRequestV1{904, 2, "INSERT INTO accounts VALUES (9, 'must-not-commit');"});
  ASSERT_EQ(retry.status_, ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(WriteResponseCodec::Decode(retry.payload_).commit_index_);
}

// A production Leader must never prepare or append a second SQL batch against
// stale state while its first durable proposal is still waiting for a quorum.
TEST(DistributedNodeTest, ConcurrentWritesShareOneUnresolvedProposalGate) {
  ThreeNodeInProcessCluster cluster;
  const auto leader = cluster.AwaitLeader();
  const auto journal_before = cluster.LogJournalSize(leader);

  std::vector<NodeId> followers;
  for (NodeId id = 1; id <= 3; id++) {
    if (id != leader) {
      followers.push_back(id);
      cluster.StopAndDestroy(id);
    }
  }

  ClientResponseV1 first_response;
  ClientResponseV1 duplicate_response;
  ClientResponseV1 second_response;
  std::exception_ptr first_error;
  std::exception_ptr duplicate_error;
  std::exception_ptr second_error;
  std::thread first([&] {
    try {
      first_response = cluster.Send(
          leader, ClientWriteRequestV1{906, 1, "CREATE TABLE first_gate(id int PRIMARY KEY, note varchar(32));"});
    } catch (...) {
      first_error = std::current_exception();
    }
  });
  const auto journal_after_first = cluster.AwaitLogJournalGrowth(leader, journal_before);

  const auto changed_active = cluster.Send(
      leader, ClientWriteRequestV1{906, 1, "CREATE TABLE changed_gate(id int PRIMARY KEY, note varchar(32));"});
  EXPECT_EQ(changed_active.status_, ClientResponseStatus::REJECTED);
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(changed_active.payload_.data()), changed_active.payload_.size()),
            "request payload does not match request identity");
  EXPECT_EQ(cluster.LogJournalSize(leader), journal_after_first);
  const auto changed_table_probe =
      cluster.Send(leader, ClientReadRequestV1{1199, ClientReadConsistency::STALE, "SELECT * FROM changed_gate;"});
  EXPECT_EQ(changed_table_probe.status_, ClientResponseStatus::REJECTED);

  std::thread duplicate([&] {
    try {
      duplicate_response = cluster.Send(
          leader, ClientWriteRequestV1{906, 1, "CREATE TABLE first_gate(id int PRIMARY KEY, note varchar(32));"});
    } catch (...) {
      duplicate_error = std::current_exception();
    }
  });
  std::thread second([&] {
    try {
      second_response = cluster.Send(
          leader, ClientWriteRequestV1{907, 1, "CREATE TABLE second_gate(id int PRIMARY KEY, amount bigint);"});
    } catch (...) {
      second_error = std::current_exception();
    }
  });

  first.join();
  duplicate.join();
  second.join();
  ASSERT_EQ(first_error, nullptr);
  ASSERT_EQ(duplicate_error, nullptr);
  ASSERT_EQ(second_error, nullptr);
  EXPECT_EQ(first_response.status_, ClientResponseStatus::TIMEOUT);
  EXPECT_EQ(duplicate_response.status_, ClientResponseStatus::TIMEOUT);
  EXPECT_EQ(second_response.status_, ClientResponseStatus::TIMEOUT);
  EXPECT_EQ(cluster.LogJournalSize(leader), journal_after_first);

  cluster.Restart(followers.front());
  // One restarted follower is enough to acknowledge the original durable
  // proposal before its election deadline; only then bring back the third node.
  cluster.AwaitCommit(2);
  cluster.Restart(followers.back());
  cluster.AwaitCommit(2);
  const auto first_retry = cluster.Send(
      leader, ClientWriteRequestV1{906, 1, "CREATE TABLE first_gate(id int PRIMARY KEY, note varchar(32));"});
  ASSERT_EQ(first_retry.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(WriteResponseCodec::Decode(first_retry.payload_).commit_index_, 2);
  const auto duplicate_retry = cluster.Send(
      leader, ClientWriteRequestV1{906, 1, "CREATE TABLE first_gate(id int PRIMARY KEY, note varchar(32));"});
  EXPECT_EQ(duplicate_retry.payload_, first_retry.payload_);

  const auto second_retry = cluster.Send(
      leader, ClientWriteRequestV1{907, 1, "CREATE TABLE second_gate(id int PRIMARY KEY, amount bigint);"});
  ASSERT_EQ(second_retry.status_, ClientResponseStatus::COMMITTED);
  EXPECT_EQ(WriteResponseCodec::Decode(second_retry.payload_).commit_index_, 3);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{906, 2, "INSERT INTO first_gate VALUES (11, 'eleven');"}).status_,
            ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{907, 2, "INSERT INTO second_gate VALUES (22, 2200);"}).status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(5);

  const auto first_rows = cluster.Send(
      followers.front(),
      ClientReadRequestV1{1200, ClientReadConsistency::STALE, "SELECT id, note FROM first_gate ORDER BY id;"});
  ASSERT_EQ(first_rows.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(first_rows.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"11", "eleven"}}));
  const auto second_rows = cluster.Send(
      followers.back(),
      ClientReadRequestV1{1201, ClientReadConsistency::STALE, "SELECT id, amount FROM second_gate ORDER BY id;"});
  ASSERT_EQ(second_rows.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(second_rows.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"22", "2200"}}));
}

// Keep the old process object alive so its in-memory ActiveWrite must reconcile
// an overwritten proposal after a later-term Leader commits the same request
// identity with a different valid payload.
TEST(DistributedNodeTest, LiveOldLeaderClearsOverwrittenProposalAfterDifferentPayloadCommitsElsewhere) {
  ThreeNodeInProcessCluster cluster;
  const auto old_leader = cluster.AwaitLeader();
  ASSERT_EQ(cluster
                .Send(old_leader,
                      ClientWriteRequestV1{908, 1, "CREATE TABLE live_heal(id int PRIMARY KEY, note varchar(32));"})
                .status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(2);

  std::vector<NodeId> majority;
  for (NodeId id = 1; id <= 3; id++) {
    if (id != old_leader) {
      majority.push_back(id);
      cluster.StopAndDestroy(id);
    }
  }
  const auto uncertain = cluster.Send(
      old_leader, ClientWriteRequestV1{908, 2, "INSERT INTO live_heal VALUES (7, 'committed-after-heal');"});
  ASSERT_EQ(uncertain.status_, ClientResponseStatus::TIMEOUT);
  ASSERT_EQ(uncertain.commit_index_, 2);

  // Stop networking and ticks without destroying the object or its active gate.
  cluster.Pause(old_leader);
  cluster.Restart(majority.front());
  cluster.Restart(majority.back());
  const auto new_leader = cluster.AwaitLeader(old_leader);
  ASSERT_NE(new_leader, old_leader);
  const std::string winning_sql = "INSERT INTO live_heal VALUES (7, 'winner-after-heal');";
  const auto committed_elsewhere = cluster.Send(new_leader, ClientWriteRequestV1{908, 2, winning_sql});
  ASSERT_EQ(committed_elsewhere.status_, ClientResponseStatus::COMMITTED);
  ASSERT_EQ(WriteResponseCodec::Decode(committed_elsewhere.payload_).commit_index_, 4);

  // Drop any queued index-3-only heartbeat. On restart this Leader still has
  // next_index[old]=3, so its first fresh replication carries 3..4 together and
  // exercises overwrite + Apply + active reconciliation in one Receive path.
  cluster.Pause(new_leader);
  cluster.Resume(old_leader);
  cluster.Resume(new_leader);
  cluster.AwaitCommit(4);
  const auto old_status = cluster.Send(old_leader, ClientStatusRequestV1{1300});
  EXPECT_EQ(old_status.status_, ClientResponseStatus::OK);
  EXPECT_GE(old_status.published_applied_index_, 4);

  const auto journal_before_reject = cluster.LogJournalSize(new_leader);
  const auto rejected_original = cluster.Send(
      new_leader, ClientWriteRequestV1{908, 2, "INSERT INTO live_heal VALUES (7, 'committed-after-heal');"});
  ASSERT_EQ(rejected_original.status_, ClientResponseStatus::REJECTED);
  EXPECT_EQ(
      std::string(reinterpret_cast<const char *>(rejected_original.payload_.data()), rejected_original.payload_.size()),
      "request payload does not match request identity");
  EXPECT_EQ(cluster.LogJournalSize(new_leader), journal_before_reject);
  const auto winning_retry = cluster.Send(new_leader, ClientWriteRequestV1{908, 2, winning_sql});
  EXPECT_EQ(winning_retry.payload_, committed_elsewhere.payload_);
  EXPECT_EQ(cluster.LogJournalSize(new_leader), journal_before_reject);

  const auto healed = cluster.Send(old_leader, ClientReadRequestV1{1301, ClientReadConsistency::STALE,
                                                                   "SELECT id, note FROM live_heal ORDER BY id;"});
  ASSERT_EQ(healed.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(healed.payload_).rows_,
            (std::vector<std::vector<std::string>>{{"7", "winner-after-heal"}}));
}

// E2E-12 TCP prerequisite: real TCP reads racing a large Apply observe only the complete old or new batch.
TEST(DistributedNodeTest, ConcurrentReadsObserveOnlyWholeMultiRowBatch) {
  ThreeNodeInProcessCluster cluster;
  const auto leader = cluster.AwaitLeader();
  NodeId reader_node = leader == 1 ? NodeId{2} : NodeId{1};
  ASSERT_EQ(
      cluster.Send(leader, ClientWriteRequestV1{905, 1, "CREATE TABLE accounts(id int PRIMARY KEY, name varchar(32));"})
          .status_,
      ClientResponseStatus::COMMITTED);

  constexpr size_t row_count = 300;
  std::ostringstream insert;
  insert << "INSERT INTO accounts VALUES ";
  for (size_t id = 1; id <= row_count; id++) {
    if (id != 1) {
      insert << ',';
    }
    insert << '(' << id << ", 'before')";
  }
  insert << ';';
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{905, 2, insert.str()}).status_, ClientResponseStatus::COMMITTED);
  ASSERT_EQ(cluster.Send(leader, ClientWriteRequestV1{905, 3, "CREATE INDEX accounts_name ON accounts(name);"}).status_,
            ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(4);

  std::atomic<bool> update_finished{false};
  std::exception_ptr reader_error;
  std::vector<std::pair<uint64_t, uint64_t>> observations;
  std::thread reader([&] {
    try {
      uint64_t request_id = 900;
      do {
        const auto response =
            cluster.Send(reader_node, ClientReadRequestV1{request_id++, ClientReadConsistency::STALE,
                                                          "SELECT count(*) FROM accounts WHERE name = 'before';"});
        if (response.status_ != ClientResponseStatus::OK || !response.read_timestamp_.has_value()) {
          throw std::runtime_error("concurrent stale read did not return a published timestamp");
        }
        const auto result = ClientQueryResultCodec::Decode(response.payload_);
        if (result.rows_.size() != 1 || result.rows_[0].size() != 1) {
          throw std::runtime_error("concurrent count query returned an invalid shape");
        }
        observations.emplace_back(std::stoull(result.rows_[0][0]), *response.read_timestamp_);
      } while (!update_finished.load() || observations.size() < 2);
    } catch (...) {
      reader_error = std::current_exception();
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  const auto update =
      cluster.Send(leader, ClientWriteRequestV1{905, 4, "UPDATE accounts SET name = 'after' WHERE id <= 300;"});
  update_finished = true;
  reader.join();
  ASSERT_EQ(reader_error, nullptr);
  ASSERT_EQ(update.status_, ClientResponseStatus::COMMITTED);
  cluster.AwaitCommit(WriteResponseCodec::Decode(update.payload_).commit_index_);
  ASSERT_FALSE(observations.empty());
  for (const auto &[count, read_timestamp] : observations) {
    EXPECT_TRUE(count == row_count || count == 0) << "partial batch became visible at read index " << read_timestamp;
  }
  const auto final = cluster.Send(
      reader_node,
      ClientReadRequestV1{999, ClientReadConsistency::STALE, "SELECT count(*) FROM accounts WHERE name = 'after';"});
  ASSERT_EQ(final.status_, ClientResponseStatus::OK);
  EXPECT_EQ(ClientQueryResultCodec::Decode(final.payload_).rows_,
            (std::vector<std::vector<std::string>>{{std::to_string(row_count)}}));
}

}  // namespace bustub
