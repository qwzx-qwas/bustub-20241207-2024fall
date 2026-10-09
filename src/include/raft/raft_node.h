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
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "common/task_executor.h"
#include "raft/log_store.h"
#include "raft/snapshot_store.h"
#include "raft/stable_store.h"
#include "raft/state_machine.h"
#include "raft/transport.h"

namespace bustub {

class RaftNodeTestPeer;
class SnapshotTasks;
class ResourceBudget;

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
  std::shared_ptr<ResourceBudget> memory_budget_{};
  std::function<void()> wake_{};
  size_t append_batch_entries_{128};
  size_t append_batch_bytes_{1024 * 1024};
};

/** Single-threaded, explicitly ticked Raft core for one static voter group. */
class RaftNode {
 public:
  RaftNode(RaftNodeConfig config, std::shared_ptr<RaftTransport> transport, std::unique_ptr<StableStore> stable_store,
           std::unique_ptr<LogStore> log_store, std::shared_ptr<RaftStateMachine> state_machine,
           std::unique_ptr<SnapshotStore> snapshot_store = nullptr);

  ~RaftNode();

  void Tick(uint64_t now_ms);
  void Receive(NodeId from, const RaftMessage &message);

  /** Returns an accepted index; durable/commit/apply progress follows completion. */
  auto Propose(EntryType type, std::vector<std::byte> payload) -> std::optional<uint64_t>;
  /** Independent, prevalidated business dependencies; each entry is still its own command.
   * Moves entries only after acceptance. Returns the first assigned index. */
  auto ProposeBatch(std::vector<ReplicatedLogEntry> &entries) -> std::optional<uint64_t>;
  /** Start one non-coalesced current-term quorum probe for a linearizable read. */
  auto StartReadIndex(uint64_t context) -> bool;
  /** Consume a completed probe's read index; nullopt means incomplete or unknown. */
  auto TakeReadIndex(uint64_t context) -> std::optional<uint64_t>;
  void CancelReadIndex(uint64_t context);
  auto TakeProposalError(uint64_t index) -> std::exception_ptr;
  auto CreateSnapshot() -> bool;
  auto Busy() const -> bool;
  void Poll();
  void Drain();
  auto BusinessTasks() -> TaskExecutor & { return *business_tasks_; }
  auto ReadSnapshotChunk(const RaftSnapshot &snapshot, uint64_t offset, size_t maximum_size) -> std::vector<std::byte>;

  auto Role() const -> RaftRole { return role_; }
  auto CurrentTerm() const -> uint64_t { return hard_state_.current_term_; }
  auto CommitIndex() const -> uint64_t { return hard_state_.commit_index_; }
  auto LastApplied() const -> uint64_t { return last_applied_; }
  auto PublishedAppliedIndex() const -> uint64_t { return published_applied_index_; }
  /** A Leader serves clients only after its current-term NOOP is committed and applied. */
  auto LeaderReady() const -> bool {
    return role_ == RaftRole::LEADER && observed_term_ <= hard_state_.current_term_ && leader_barrier_index_ != 0 &&
           last_applied_ >= leader_barrier_index_;
  }
  auto LeaderId() const -> std::optional<NodeId> { return leader_id_; }
  auto LastLogIndex() const -> uint64_t { return durable_tip_; }
  auto LastLogTerm() const -> uint64_t { return durable_tip_term_; }
  auto SnapshotBaseIndex() const -> uint64_t { return durable_base_; }
  auto Log() const -> const LogStore & { return *log_store_; }
  auto LatestSnapshot() const -> std::optional<RaftSnapshot>;

 private:
  friend class RaftNodeTestPeer;
  class Operation;
  template <typename F>
  auto Slow(F work, bool business = false);
  void Start(Operation operation);
  void HeartbeatWhileBusy();
  void RefreshLogTip();
  auto UpdateLogTip() -> Operation;
  auto Dispatch(NodeId from, RaftMessage message, ResourceCharge charge) -> Operation;
  auto RunTick() -> Operation;
  auto RunProposal(std::vector<ReplicatedLogEntry> entries) -> Operation;
  auto RunSnapshot() -> Operation;

  auto StartElection() -> Operation;
  auto BecomeLeader() -> Operation;
  auto ObserveHigherTerm(uint64_t term) -> Operation;
  auto PersistHardState(uint64_t term, std::optional<NodeId> voted_for, uint64_t commit_index) -> Operation;
  void FailStop();
  auto AppendLogDurably(const std::vector<ReplicatedLogEntry> &entries) -> Operation;
  auto ReplaceLogSuffixDurably(uint64_t from_index, const std::vector<ReplicatedLogEntry> &entries) -> Operation;
  auto InstallLogSnapshotBaseDurably(uint64_t index, uint64_t term, bool retain_old_suffix) -> Operation;
  auto AdvanceLogCommitOrStop(uint64_t committed_index) -> Operation;
  void ResetElectionDeadline();

  auto Handle(NodeId from, const RequestVoteRequest &request) -> Operation;
  auto Handle(NodeId from, const RequestVoteResponse &response) -> Operation;
  auto Handle(NodeId from, const AppendEntriesRequest &request) -> Operation;
  auto Handle(NodeId from, const AppendEntriesResponse &response) -> Operation;
  auto Handle(NodeId from, const InstallSnapshotRequest &request) -> Operation;
  auto Handle(NodeId from, const InstallSnapshotResponse &response) -> Operation;
  auto Handle(NodeId from, const SnapshotOfferRequest &request) -> Operation;
  auto Handle(NodeId from, const SnapshotOfferResponse &response) -> Operation;
  auto CancelIncomingDelta() -> Operation;
  auto PollSnapshotTasks() -> Operation;

  void Send(NodeId to, RaftMessage message);
  auto SendAppend(NodeId peer, std::optional<uint64_t> read_context = std::nullopt) -> Operation;
  auto SendSnapshot(NodeId peer, std::optional<uint64_t> acknowledged_offset = std::nullopt) -> Operation;
  auto BroadcastAppend() -> Operation;
  auto BroadcastReadIndex(uint64_t context) -> Operation;
  auto AdvanceLeaderCommit() -> Operation;
  auto ApplyCommitted() -> Operation;
  auto HasMajority(size_t votes) const -> bool;
  auto CandidateLogIsUpToDate(uint64_t last_term, uint64_t last_index) const -> bool;
  auto FirstIndexOfTerm(uint64_t index, uint64_t term) const -> uint64_t;
  auto LastIndexOfTerm(uint64_t term) const -> std::optional<uint64_t>;

  std::unique_ptr<Operation> operation_;
  std::function<bool()> poll_work_;
  std::unique_ptr<TaskExecutor> storage_tasks_, business_tasks_;
  std::shared_ptr<ResourceAccount> pending_memory_;
  struct PendingMessage {
    NodeId from_;
    RaftMessage message_;
    ResourceCharge charge_;
  };
  std::deque<PendingMessage> pending_messages_;
  uint64_t observed_term_{0};
  uint64_t durable_tip_{0}, durable_tip_term_{0}, durable_base_{0};
  std::optional<RaftSnapshot> latest_snapshot_;
  bool draining_{false};
  bool log_mutating_{false};
  std::map<uint64_t, std::exception_ptr> proposal_errors_;
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
    SnapshotInput input_;
    std::optional<SnapshotDeltaOffer> offer_{std::nullopt};
    std::optional<SnapshotDelta> delta_{std::nullopt};
    std::shared_ptr<InstallSnapshotRequest> chunk_{};
    std::shared_ptr<TaskExecutor::Result<std::shared_ptr<InstallSnapshotRequest>>> work_{};
    bool extended_{true}, compression_{false};
    uint64_t offer_id_{0}, offer_deadline_{0};
    bool offering_{false};
  };
  std::map<NodeId, SnapshotTransfer> snapshot_transfers_;
  struct IncomingDelta {
    uint64_t term_, leader_, session_, deadline_;
    std::string target_;
  };
  std::optional<IncomingDelta> incoming_delta_;
  std::optional<IncomingDelta> incoming_encoding_;
  struct IncomingDecode {
    NodeId from_;
    uint64_t term_;
    uint64_t request_id_;
    uint64_t encoding_session_;
    std::string snapshot_id_;
    std::shared_ptr<TaskExecutor::Result<std::shared_ptr<InstallSnapshotRequest>>> work_;
  };
  std::optional<IncomingDecode> incoming_decode_;
  // Destroyed explicitly before stores and input leases. Workers never access this node.
  std::unique_ptr<SnapshotTasks> snapshot_tasks_;
  // Highest snapshot request observed in this term. Keep it after cancellation
  // so a delayed Offer/full chunk cannot restart an older receive session.
  uint64_t snapshot_request_floor_{0};

  struct PendingReadIndex {
    uint64_t term_{0};
    std::set<NodeId> acknowledgements_;
  };
  uint64_t highest_read_context_{0};
  std::map<uint64_t, PendingReadIndex> pending_read_indexes_;
  std::map<uint64_t, uint64_t> completed_read_indexes_;
};

}  // namespace bustub
