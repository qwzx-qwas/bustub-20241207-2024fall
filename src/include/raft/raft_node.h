//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_node.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "raft/log_store.h"
#include "raft/snapshot_store.h"
#include "raft/stable_store.h"
#include "raft/state_machine.h"
#include "raft/transport.h"

namespace bustub {

/** Draws one election timeout from the inclusive bounds supplied by RaftNode. */
using ElectionTimeoutSource = std::function<uint64_t(uint64_t, uint64_t)>;

/** Production entropy source. Each returned source owns an independently seeded generator. */
auto MakeRandomElectionTimeoutSource() -> ElectionTimeoutSource;

/** Deterministic source for protocol tests and replayable fault schedules. */
auto MakeSeededElectionTimeoutSource(uint64_t seed) -> ElectionTimeoutSource;

struct RaftNodeConfig {
  NodeId node_id_{0};
  std::vector<NodeId> voters_;
  uint64_t election_timeout_min_ms_{300};
  uint64_t election_timeout_max_ms_{600};
  uint64_t heartbeat_interval_ms_{50};
  std::string group_id_;
  ElectionTimeoutSource election_timeout_source_{MakeRandomElectionTimeoutSource()};
};

/** Single-threaded, explicitly ticked Raft core for one static voter group. */
class RaftNode {
 public:
  RaftNode(RaftNodeConfig config, std::shared_ptr<RaftTransport> transport, std::unique_ptr<StableStore> stable_store,
           std::unique_ptr<LogStore> log_store, std::shared_ptr<RaftStateMachine> state_machine,
           std::unique_ptr<SnapshotStore> snapshot_store = nullptr);

  void Tick(uint64_t now_ms);
  void Receive(NodeId from, const RaftMessage &message);

  /** Returns the locally durable index, or nullopt when this node is not Leader. */
  auto Propose(EntryType type, std::vector<std::byte> payload) -> std::optional<uint64_t>;
  /** Start one non-coalesced current-term quorum probe for a linearizable read. */
  auto StartReadIndex(uint64_t context) -> bool;
  /** Consume a completed probe's read index; nullopt means incomplete or unknown. */
  auto TakeReadIndex(uint64_t context) -> std::optional<uint64_t>;
  void CancelReadIndex(uint64_t context);
  auto CreateSnapshot() -> RaftSnapshot;
  auto ReadSnapshotChunk(const RaftSnapshot &snapshot, uint64_t offset, size_t maximum_size) -> std::vector<std::byte>;

  auto Role() const -> RaftRole { return role_; }
  auto CurrentTerm() const -> uint64_t { return hard_state_.current_term_; }
  auto CommitIndex() const -> uint64_t { return hard_state_.commit_index_; }
  auto LastApplied() const -> uint64_t { return last_applied_; }
  auto PublishedAppliedIndex() const -> uint64_t { return published_applied_index_; }
  /** A Leader serves clients only after its current-term NOOP is committed and applied. */
  auto LeaderReady() const -> bool {
    return role_ == RaftRole::LEADER && leader_barrier_index_ != 0 && last_applied_ >= leader_barrier_index_;
  }
  auto LeaderId() const -> std::optional<NodeId> { return leader_id_; }
  auto Log() const -> const LogStore & { return *log_store_; }
  auto LatestSnapshot() const -> std::optional<RaftSnapshot>;

 private:
  void StartElection();
  void BecomeLeader();
  void ObserveHigherTerm(uint64_t term);
  void PersistHardState(uint64_t term, std::optional<NodeId> voted_for, uint64_t commit_index);
  void FailStop();
  void AppendLogDurably(const std::vector<ReplicatedLogEntry> &entries);
  void ReplaceLogSuffixDurably(uint64_t from_index, const std::vector<ReplicatedLogEntry> &entries);
  void InstallLogSnapshotBaseDurably(uint64_t index, uint64_t term, bool retain_old_suffix);
  void AdvanceLogCommitOrStop(uint64_t committed_index);
  void ResetElectionDeadline();

  void Handle(NodeId from, const RequestVoteRequest &request);
  void Handle(NodeId from, const RequestVoteResponse &response);
  void Handle(NodeId from, const AppendEntriesRequest &request);
  void Handle(NodeId from, const AppendEntriesResponse &response);
  void Handle(NodeId from, const InstallSnapshotRequest &request);
  void Handle(NodeId from, const InstallSnapshotResponse &response);

  void Send(NodeId to, RaftMessage message);
  void SendAppend(NodeId peer, std::optional<uint64_t> read_context = std::nullopt);
  void SendSnapshot(NodeId peer, std::optional<uint64_t> acknowledged_offset = std::nullopt);
  void BroadcastAppend();
  void BroadcastReadIndex(uint64_t context);
  void AdvanceLeaderCommit();
  void ApplyCommitted();
  auto HasMajority(size_t votes) const -> bool;
  auto CandidateLogIsUpToDate(uint64_t last_term, uint64_t last_index) const -> bool;
  auto FirstIndexOfTerm(uint64_t index, uint64_t term) const -> uint64_t;
  auto LastIndexOfTerm(uint64_t term) const -> std::optional<uint64_t>;

  RaftNodeConfig config_;
  std::shared_ptr<RaftTransport> transport_;
  std::unique_ptr<StableStore> stable_store_;
  std::unique_ptr<LogStore> log_store_;
  std::shared_ptr<RaftStateMachine> state_machine_;
  std::unique_ptr<SnapshotStore> snapshot_store_;

  HardState hard_state_;
  RaftRole role_{RaftRole::FOLLOWER};
  std::optional<NodeId> leader_id_;
  uint64_t now_ms_{0};
  uint64_t election_deadline_ms_{0};
  uint64_t heartbeat_deadline_ms_{0};
  uint64_t last_applied_{0};
  uint64_t published_applied_index_{0};
  uint64_t leader_barrier_index_{0};

  std::set<NodeId> votes_received_;
  std::map<NodeId, uint64_t> next_index_;
  std::map<NodeId, uint64_t> match_index_;
  std::map<NodeId, uint64_t> last_request_id_;

  struct SnapshotTransfer {
    RaftSnapshot snapshot_;
    uint64_t offset_{0};
    uint64_t end_offset_{0};
    uint64_t request_id_{0};
  };
  std::map<NodeId, SnapshotTransfer> snapshot_transfers_;

  struct PendingReadIndex {
    uint64_t term_{0};
    std::set<NodeId> acknowledgements_;
  };
  uint64_t highest_read_context_{0};
  std::map<uint64_t, PendingReadIndex> pending_read_indexes_;
  std::map<uint64_t, uint64_t> completed_read_indexes_;
};

}  // namespace bustub
