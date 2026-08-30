//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_node.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/raft_node.h"

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace bustub {
namespace {

auto MakeGeneratorBackedElectionTimeoutSource(std::mt19937_64 generator) -> ElectionTimeoutSource {
  auto shared_generator = std::make_shared<std::mt19937_64>(std::move(generator));
  return [shared_generator](uint64_t minimum_ms, uint64_t maximum_ms) {
    if (minimum_ms == 0 || minimum_ms > maximum_ms) {
      throw std::runtime_error("invalid election timeout interval");
    }
    return std::uniform_int_distribution<uint64_t>(minimum_ms, maximum_ms)(*shared_generator);
  };
}

}  // namespace

auto MakeRandomElectionTimeoutSource() -> ElectionTimeoutSource {
  std::random_device entropy;
  std::seed_seq seed{entropy(), entropy(), entropy(), entropy(), entropy(), entropy(), entropy(), entropy()};
  return MakeGeneratorBackedElectionTimeoutSource(std::mt19937_64(seed));
}

auto MakeSeededElectionTimeoutSource(uint64_t seed) -> ElectionTimeoutSource {
  return MakeGeneratorBackedElectionTimeoutSource(std::mt19937_64(seed));
}

RaftNode::RaftNode(RaftNodeConfig config, std::shared_ptr<RaftTransport> transport,
                   std::unique_ptr<StableStore> stable_store, std::unique_ptr<LogStore> log_store,
                   std::shared_ptr<RaftStateMachine> state_machine, std::unique_ptr<SnapshotStore> snapshot_store)
    : config_(std::move(config)),
      transport_(std::move(transport)),
      stable_store_(std::move(stable_store)),
      log_store_(std::move(log_store)),
      state_machine_(std::move(state_machine)),
      snapshot_store_(std::move(snapshot_store)) {
  if (config_.node_id_ == 0 || config_.voters_.size() != 3 || config_.election_timeout_min_ms_ == 0 ||
      config_.election_timeout_min_ms_ >= config_.election_timeout_max_ms_ || config_.heartbeat_interval_ms_ == 0 ||
      !config_.election_timeout_source_ || transport_ == nullptr || stable_store_ == nullptr || log_store_ == nullptr ||
      state_machine_ == nullptr) {
    throw std::runtime_error("invalid static Raft node configuration");
  }
  std::set<NodeId> voters(config_.voters_.begin(), config_.voters_.end());
  if (voters.size() != config_.voters_.size() || voters.count(0) != 0 || voters.count(config_.node_id_) != 1) {
    throw std::runtime_error("invalid static Raft voter set");
  }
  hard_state_ = stable_store_->State();
  if (log_store_->CommittedIndex() < hard_state_.commit_index_) {
    throw std::runtime_error("Raft LogStore is behind HARD_STATE commit index");
  }
  if (log_store_->CommittedIndex() > hard_state_.commit_index_) {
    PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, log_store_->CommittedIndex());
  }
  if (snapshot_store_ != nullptr && snapshot_store_->Latest().has_value()) {
    const auto snapshot = *snapshot_store_->Latest();
    const auto oldest = snapshot_store_->OldestRetained();
    if (!oldest.has_value() || oldest->last_included_index_ != log_store_->SnapshotBaseIndex() ||
        oldest->last_included_term_ != log_store_->SnapshotBaseTerm() ||
        log_store_->TermAt(snapshot.last_included_index_) != std::optional<uint64_t>{snapshot.last_included_term_}) {
      throw std::runtime_error("Raft SnapshotStore and recovery log bridge disagree");
    }
    if (state_machine_->LastApplied() == 0 && snapshot.last_included_index_ != 0) {
      state_machine_->InstallSnapshotFile(snapshot_store_->PayloadFile(snapshot), snapshot.last_included_index_);
    }
  }
  const auto recovered_snapshot_index = snapshot_store_ != nullptr && snapshot_store_->Latest().has_value()
                                            ? snapshot_store_->Latest()->last_included_index_
                                            : log_store_->SnapshotBaseIndex();
  if (state_machine_->LastApplied() != recovered_snapshot_index ||
      state_machine_->LastApplied() > hard_state_.commit_index_) {
    throw std::runtime_error("Raft state machine does not match the recovered snapshot base");
  }
  last_applied_ = state_machine_->LastApplied();
  published_applied_index_ = last_applied_;
  ApplyCommitted();
  ResetElectionDeadline();
}

void RaftNode::ResetElectionDeadline() {
  const auto timeout =
      config_.election_timeout_source_(config_.election_timeout_min_ms_, config_.election_timeout_max_ms_);
  if (timeout < config_.election_timeout_min_ms_ || timeout > config_.election_timeout_max_ms_) {
    throw std::runtime_error("election timeout source returned a value outside its configured interval");
  }
  if (timeout > std::numeric_limits<uint64_t>::max() - now_ms_) {
    throw std::runtime_error("election deadline overflows the logical clock");
  }
  election_deadline_ms_ = now_ms_ + timeout;
}

void RaftNode::PersistHardState(uint64_t term, std::optional<NodeId> voted_for, uint64_t commit_index) {
  try {
    stable_store_->Update(term, voted_for, commit_index);
    hard_state_ = stable_store_->State();
  } catch (...) {
    FailStop();
    throw;
  }
}

void RaftNode::FailStop() {
  role_ = RaftRole::STOPPED;
  leader_id_.reset();
  leader_barrier_index_ = 0;
  votes_received_.clear();
  next_index_.clear();
  match_index_.clear();
  snapshot_transfers_.clear();
  pending_read_indexes_.clear();
  completed_read_indexes_.clear();
}

void RaftNode::AppendLogDurably(const std::vector<ReplicatedLogEntry> &entries) {
  try {
    log_store_->Append(entries);
  } catch (...) {
    // A failed durable append may already have changed the on-disk image. The
    // in-memory log cannot be trusted to choose another index until restart.
    FailStop();
    throw;
  }
}

void RaftNode::ReplaceLogSuffixDurably(uint64_t from_index, const std::vector<ReplicatedLogEntry> &entries) {
  try {
    log_store_->ReplaceSuffix(from_index, entries);
  } catch (...) {
    FailStop();
    throw;
  }
}

void RaftNode::InstallLogSnapshotBaseDurably(uint64_t index, uint64_t term, bool retain_old_suffix) {
  try {
    log_store_->InstallSnapshotBase(index, term, retain_old_suffix);
  } catch (...) {
    FailStop();
    throw;
  }
}

void RaftNode::AdvanceLogCommitOrStop(uint64_t committed_index) {
  try {
    log_store_->AdvanceCommittedIndex(committed_index);
  } catch (...) {
    FailStop();
    throw;
  }
}

void RaftNode::Tick(uint64_t now_ms) {
  if (role_ == RaftRole::STOPPED) {
    return;
  }
  if (now_ms < now_ms_) {
    throw std::runtime_error("Raft logical clock cannot move backwards");
  }
  now_ms_ = now_ms;
  if (role_ == RaftRole::LEADER) {
    if (now_ms_ >= heartbeat_deadline_ms_) {
      BroadcastAppend();
      heartbeat_deadline_ms_ = now_ms_ + config_.heartbeat_interval_ms_;
    }
    return;
  }
  if (role_ != RaftRole::TERM_PERSISTING && now_ms_ >= election_deadline_ms_) {
    StartElection();
  }
}

void RaftNode::StartElection() {
  role_ = RaftRole::TERM_PERSISTING;
  const auto new_term = hard_state_.current_term_ + 1;
  PersistHardState(new_term, config_.node_id_, hard_state_.commit_index_);
  role_ = RaftRole::CANDIDATE;
  leader_id_.reset();
  votes_received_ = {config_.node_id_};
  ResetElectionDeadline();

  RequestVoteRequest request{hard_state_.current_term_, config_.node_id_, log_store_->LastLogIndex(),
                             log_store_->LastLogTerm()};
  for (const auto peer : config_.voters_) {
    if (peer != config_.node_id_) {
      Send(peer, request);
    }
  }
  if (HasMajority(votes_received_.size())) {
    BecomeLeader();
  }
}

void RaftNode::BecomeLeader() {
  if (role_ != RaftRole::CANDIDATE) {
    throw std::runtime_error("only a Candidate can become Leader");
  }
  role_ = RaftRole::LEADER;
  leader_id_ = config_.node_id_;
  next_index_.clear();
  match_index_.clear();
  last_request_id_.clear();
  snapshot_transfers_.clear();
  pending_read_indexes_.clear();
  completed_read_indexes_.clear();
  const auto initial_next = log_store_->LastLogIndex() + 1;
  for (const auto voter : config_.voters_) {
    next_index_[voter] = initial_next;
    match_index_[voter] = voter == config_.node_id_ ? log_store_->LastLogIndex() : 0;
    last_request_id_[voter] = 0;
  }

  const auto noop_index = log_store_->LastLogIndex() + 1;
  AppendLogDurably({ReplicatedLogEntry{1, noop_index, hard_state_.current_term_, EntryType::NOOP, {}}});
  match_index_[config_.node_id_] = noop_index;
  next_index_[config_.node_id_] = noop_index + 1;
  leader_barrier_index_ = noop_index;
  BroadcastAppend();
  heartbeat_deadline_ms_ = now_ms_ + config_.heartbeat_interval_ms_;
}

void RaftNode::ObserveHigherTerm(uint64_t term) {
  if (term <= hard_state_.current_term_) {
    return;
  }
  role_ = RaftRole::TERM_PERSISTING;
  leader_id_.reset();
  leader_barrier_index_ = 0;
  PersistHardState(term, std::nullopt, hard_state_.commit_index_);
  role_ = RaftRole::FOLLOWER;
  votes_received_.clear();
  next_index_.clear();
  match_index_.clear();
  snapshot_transfers_.clear();
  pending_read_indexes_.clear();
  completed_read_indexes_.clear();
  ResetElectionDeadline();
}

void RaftNode::Receive(NodeId from, const RaftMessage &message) {
  if (role_ == RaftRole::STOPPED || from == 0) {
    return;
  }
  std::visit([&](const auto &value) { Handle(from, value); }, message);
}

void RaftNode::Handle(NodeId from, const RequestVoteRequest &request) {
  if (request.term_ > hard_state_.current_term_) {
    ObserveHigherTerm(request.term_);
  }
  bool grant = false;
  if (request.term_ == hard_state_.current_term_ && request.candidate_id_ == from &&
      (hard_state_.voted_for_ == std::nullopt || hard_state_.voted_for_ == request.candidate_id_) &&
      CandidateLogIsUpToDate(request.last_log_term_, request.last_log_index_)) {
    if (hard_state_.voted_for_ != request.candidate_id_) {
      PersistHardState(hard_state_.current_term_, request.candidate_id_, hard_state_.commit_index_);
    }
    grant = true;
    ResetElectionDeadline();
  }
  Send(from, RequestVoteResponse{hard_state_.current_term_, grant});
}

void RaftNode::Handle(NodeId from, const RequestVoteResponse &response) {
  if (response.term_ > hard_state_.current_term_) {
    ObserveHigherTerm(response.term_);
    return;
  }
  if (role_ != RaftRole::CANDIDATE || response.term_ != hard_state_.current_term_ || !response.vote_granted_) {
    return;
  }
  if (std::find(config_.voters_.begin(), config_.voters_.end(), from) == config_.voters_.end()) {
    return;
  }
  votes_received_.insert(from);
  if (HasMajority(votes_received_.size())) {
    BecomeLeader();
  }
}

void RaftNode::Handle(NodeId from, const AppendEntriesRequest &request) {
  if (request.term_ > hard_state_.current_term_) {
    ObserveHigherTerm(request.term_);
  }
  if (request.term_ < hard_state_.current_term_ || request.leader_id_ != from) {
    Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, std::nullopt,
                                     log_store_->LastLogIndex() + 1, std::nullopt});
    return;
  }
  if (role_ != RaftRole::FOLLOWER) {
    role_ = RaftRole::FOLLOWER;
    leader_barrier_index_ = 0;
    pending_read_indexes_.clear();
    completed_read_indexes_.clear();
  }
  leader_id_ = from;
  ResetElectionDeadline();

  const auto local_prev_term = log_store_->TermAt(request.prev_log_index_);
  if (!local_prev_term.has_value()) {
    const auto conflict = request.prev_log_index_ < log_store_->SnapshotBaseIndex()
                              ? log_store_->SnapshotBaseIndex() + 1
                              : log_store_->LastLogIndex() + 1;
    Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, std::nullopt, conflict,
                                     request.read_context_});
    return;
  }
  if (*local_prev_term != request.prev_log_term_) {
    Send(from,
         AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, *local_prev_term,
                               FirstIndexOfTerm(request.prev_log_index_, *local_prev_term), request.read_context_});
    return;
  }

  uint64_t expected_index = request.prev_log_index_ + 1;
  for (const auto &entry : request.entries_) {
    if (entry.index_ != expected_index) {
      Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, std::nullopt,
                                       log_store_->LastLogIndex() + 1, request.read_context_});
      return;
    }
    expected_index++;
  }

  size_t first_new = 0;
  while (first_new < request.entries_.size()) {
    const auto &entry = request.entries_[first_new];
    const auto local_term = log_store_->TermAt(entry.index_);
    if (!local_term.has_value() || *local_term != entry.term_) {
      break;
    }
    first_new++;
  }
  if (first_new < request.entries_.size()) {
    const auto from_index = request.entries_[first_new].index_;
    std::vector<ReplicatedLogEntry> suffix(request.entries_.begin() + static_cast<ptrdiff_t>(first_new),
                                           request.entries_.end());
    if (from_index <= log_store_->LastLogIndex()) {
      ReplaceLogSuffixDurably(from_index, suffix);
    } else {
      AppendLogDurably(suffix);
    }
  }

  const auto match_index = request.prev_log_index_ + request.entries_.size();
  const auto new_commit = std::min(request.leader_commit_, log_store_->LastLogIndex());
  if (new_commit > hard_state_.commit_index_) {
    PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, new_commit);
    AdvanceLogCommitOrStop(new_commit);
    ApplyCommitted();
  }
  Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, true, match_index, std::nullopt, 0,
                                   request.read_context_});
}

void RaftNode::Handle(NodeId from, const AppendEntriesResponse &response) {
  if (response.term_ > hard_state_.current_term_) {
    ObserveHigherTerm(response.term_);
    return;
  }
  if (role_ != RaftRole::LEADER || response.term_ != hard_state_.current_term_ || next_index_.count(from) == 0) {
    return;
  }
  if (response.read_context_.has_value()) {
    const auto pending = pending_read_indexes_.find(*response.read_context_);
    if (pending != pending_read_indexes_.end() && pending->second.term_ == hard_state_.current_term_) {
      pending->second.acknowledgements_.insert(from);
      if (HasMajority(pending->second.acknowledgements_.size())) {
        completed_read_indexes_[pending->first] = hard_state_.commit_index_;
        pending_read_indexes_.erase(pending);
      }
    }
  }
  if (response.success_) {
    match_index_[from] = std::max(match_index_[from], response.match_index_);
    next_index_[from] = std::max(next_index_[from], response.match_index_ + 1);
    const auto transfer = snapshot_transfers_.find(from);
    if (transfer != snapshot_transfers_.end() &&
        response.match_index_ >= transfer->second.snapshot_.last_included_index_) {
      snapshot_transfers_.erase(transfer);
    }
    AdvanceLeaderCommit();
    if (next_index_[from] <= log_store_->LastLogIndex()) {
      SendAppend(from);
    }
    return;
  }
  if (response.request_id_ != last_request_id_[from]) {
    return;
  }
  uint64_t next = response.conflict_index_;
  if (response.conflict_term_.has_value()) {
    if (const auto local = LastIndexOfTerm(*response.conflict_term_); local.has_value()) {
      next = *local + 1;
    }
  }
  next_index_[from] = std::max<uint64_t>(1, std::min(next, log_store_->LastLogIndex() + 1));
  SendAppend(from);
}

void RaftNode::Handle(NodeId from, const InstallSnapshotRequest &request) {
  if (request.term_ > hard_state_.current_term_) {
    ObserveHigherTerm(request.term_);
  }
  if (request.term_ < hard_state_.current_term_ || request.leader_id_ != from || snapshot_store_ == nullptr) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    return;
  }
  if (role_ != RaftRole::FOLLOWER) {
    role_ = RaftRole::FOLLOWER;
    leader_barrier_index_ = 0;
  }
  leader_id_ = from;
  ResetElectionDeadline();

  // First stale guard: do not even retain download state for an obsolete image.
  if (request.offset_ == 0 && request.last_included_index_ <= published_applied_index_) {
    snapshot_store_->CancelStaged(request.snapshot_id_);
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, true, true,
                                       published_applied_index_, 0});
    return;
  }

  SnapshotStageResult stage{SnapshotStageStatus::IN_PROGRESS, 0};
  try {
    stage = snapshot_store_->StageChunk({request.snapshot_id_, request.last_included_index_,
                                         request.last_included_term_, request.offset_, request.total_size_,
                                         request.payload_checksum_, request.done_, request.data_});
  } catch (const std::exception &) {
    snapshot_store_->CancelStaged(request.snapshot_id_);
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    return;
  }
  if (stage.status_ == SnapshotStageStatus::IN_PROGRESS) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, false, false, 0,
                                       stage.next_offset_});
    return;
  }

  // Final stale guard runs in this same single-threaded Apply/Install sequence.
  if (request.last_included_index_ <= published_applied_index_) {
    snapshot_store_->CancelStaged(request.snapshot_id_);
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, true, true,
                                       published_applied_index_, 0});
    return;
  }
  const auto staged = snapshot_store_->Staged(request.snapshot_id_);
  const auto staged_payload = snapshot_store_->StagedPayloadFile(request.snapshot_id_);
  if (!staged.has_value() || !staged_payload.has_value()) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    return;
  }

  const auto preinstall_term = log_store_->TermAt(staged->last_included_index_);
  const bool retain_suffix = preinstall_term == std::optional<uint64_t>{staged->last_included_term_};
  if (hard_state_.commit_index_ > staged->last_included_index_ && !retain_suffix) {
    // The snapshot cannot replace an already-committed suffix unless its
    // boundary is proved to be on the same log. Reject it before publishing
    // CURRENT, advancing HARD_STATE, rebasing the log, or touching the FSM.
    // Cleaning the non-authoritative download is safe; a failed cleanup still
    // leaves the node unable to continue without a restart.
    try {
      snapshot_store_->CancelStaged(request.snapshot_id_);
    } catch (...) {
      FailStop();
      throw;
    }
    FailStop();
    throw std::runtime_error("Raft snapshot boundary cannot preserve the committed suffix");
  }
  try {
    state_machine_->ValidateSnapshotFile(*staged_payload, staged->last_included_index_);
  } catch (const std::exception &) {
    try {
      snapshot_store_->CancelStaged(request.snapshot_id_);
    } catch (...) {
      FailStop();
      throw;
    }
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    return;
  }
  try {
    snapshot_store_->PublishFile(staged->last_included_index_, staged->last_included_term_, staged_payload->path_,
                                 retain_suffix);
    const auto new_commit = std::max(hard_state_.commit_index_, staged->last_included_index_);
    if (new_commit > hard_state_.commit_index_) {
      PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, new_commit);
    }
    const auto recovery_base = snapshot_store_->OldestRetained();
    if (!recovery_base.has_value()) {
      throw std::runtime_error("published Raft snapshot has no recovery base");
    }
    if (recovery_base->last_included_index_ > log_store_->SnapshotBaseIndex()) {
      const bool retain_bridge = log_store_->TermAt(recovery_base->last_included_index_) ==
                                 std::optional<uint64_t>{recovery_base->last_included_term_};
      InstallLogSnapshotBaseDurably(recovery_base->last_included_index_, recovery_base->last_included_term_,
                                    retain_bridge);
    }
    if (log_store_->CommittedIndex() < new_commit) {
      AdvanceLogCommitOrStop(new_commit);
    }
    state_machine_->InstallSnapshotFile(*staged_payload, staged->last_included_index_);
    snapshot_store_->CancelStaged(request.snapshot_id_);
    last_applied_ = staged->last_included_index_;
    published_applied_index_ = staged->last_included_index_;
    ApplyCommitted();
  } catch (...) {
    // Publication may already have changed durable authority. Only restart
    // recovery can safely choose and install the resulting state.
    FailStop();
    throw;
  }
  Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, false, true,
                                     staged->last_included_index_, 0});
}

void RaftNode::Handle(NodeId from, const InstallSnapshotResponse &response) {
  if (response.term_ > hard_state_.current_term_) {
    ObserveHigherTerm(response.term_);
    return;
  }
  const auto transfer = snapshot_transfers_.find(from);
  if (role_ != RaftRole::LEADER || response.term_ != hard_state_.current_term_ || next_index_.count(from) == 0 ||
      transfer == snapshot_transfers_.end() || response.request_id_ != transfer->second.request_id_) {
    return;
  }
  if (!response.success_) {
    snapshot_transfers_.erase(transfer);
    return;
  }
  if (!response.complete_) {
    if (response.stale_ || response.next_offset_ < transfer->second.end_offset_ ||
        response.next_offset_ >= transfer->second.snapshot_.payload_size_) {
      snapshot_transfers_.erase(transfer);
      return;
    }
    SendSnapshot(from, response.next_offset_);
    return;
  }
  if (response.match_index_ < transfer->second.snapshot_.last_included_index_) {
    snapshot_transfers_.erase(transfer);
    return;
  }
  snapshot_transfers_.erase(transfer);
  match_index_[from] = std::max(match_index_[from], response.match_index_);
  next_index_[from] = std::max(next_index_[from], response.match_index_ + 1);
  if (next_index_[from] <= log_store_->LastLogIndex()) {
    SendAppend(from);
  }
}

auto RaftNode::Propose(EntryType type, std::vector<std::byte> payload) -> std::optional<uint64_t> {
  if (!LeaderReady()) {
    return std::nullopt;
  }
  state_machine_->ValidateProposalPayload(type, payload);
  if (log_store_->LastLogIndex() != hard_state_.commit_index_ || last_applied_ != hard_state_.commit_index_ ||
      published_applied_index_ != hard_state_.commit_index_) {
    throw std::runtime_error("V1 allows only one unresolved Raft proposal");
  }
  const auto index = log_store_->LastLogIndex() + 1;
  AppendLogDurably({ReplicatedLogEntry{1, index, hard_state_.current_term_, type, std::move(payload)}});
  match_index_[config_.node_id_] = index;
  next_index_[config_.node_id_] = index + 1;
  BroadcastAppend();
  return index;
}

auto RaftNode::StartReadIndex(uint64_t context) -> bool {
  if (!LeaderReady() || context == 0 || context <= highest_read_context_) {
    return false;
  }
  highest_read_context_ = context;
  pending_read_indexes_.emplace(context,
                                PendingReadIndex{hard_state_.current_term_, std::set<NodeId>{config_.node_id_}});
  BroadcastReadIndex(context);
  return true;
}

auto RaftNode::TakeReadIndex(uint64_t context) -> std::optional<uint64_t> {
  const auto iterator = completed_read_indexes_.find(context);
  if (iterator == completed_read_indexes_.end()) {
    return std::nullopt;
  }
  const auto result = iterator->second;
  completed_read_indexes_.erase(iterator);
  return result;
}

void RaftNode::CancelReadIndex(uint64_t context) {
  pending_read_indexes_.erase(context);
  completed_read_indexes_.erase(context);
}

auto RaftNode::CreateSnapshot() -> RaftSnapshot {
  if (snapshot_store_ == nullptr || last_applied_ != hard_state_.commit_index_ ||
      published_applied_index_ != last_applied_) {
    throw std::runtime_error("Raft node is not at a stable snapshot boundary");
  }
  const auto index = published_applied_index_;
  const auto term = log_store_->TermAt(index);
  if (!term.has_value()) {
    throw std::runtime_error("Raft snapshot term is unavailable before compaction");
  }
  const auto existing = snapshot_store_->Latest();
  if (existing.has_value() && index <= existing->last_included_index_) {
    if (existing->last_included_index_ != index) {
      throw std::runtime_error("Raft state is behind its latest published snapshot");
    }
    return *existing;
  }
  const auto capture = snapshot_store_->PrepareCapturePath();
  try {
    state_machine_->CreateSnapshotFile(capture);
  } catch (...) {
    snapshot_store_->CancelCapture();
    throw;
  }
  try {
    auto snapshot = snapshot_store_->PublishFile(index, *term, capture);
    const auto recovery_base = snapshot_store_->OldestRetained();
    if (!recovery_base.has_value()) {
      throw std::runtime_error("published Raft snapshot has no recovery base");
    }
    if (recovery_base->last_included_index_ > log_store_->SnapshotBaseIndex()) {
      InstallLogSnapshotBaseDurably(recovery_base->last_included_index_, recovery_base->last_included_term_, true);
    }
    return snapshot;
  } catch (...) {
    FailStop();
    try {
      snapshot_store_->CancelCapture();
    } catch (...) {
      // Preserve the publication failure as the primary exception. Startup
      // cleanup owns any leftover capture file.
    }
    throw;
  }
}

auto RaftNode::LatestSnapshot() const -> std::optional<RaftSnapshot> {
  return snapshot_store_ == nullptr ? std::nullopt : snapshot_store_->Latest();
}

auto RaftNode::ReadSnapshotChunk(const RaftSnapshot &snapshot, uint64_t offset, size_t maximum_size)
    -> std::vector<std::byte> {
  if (snapshot_store_ == nullptr) {
    throw std::runtime_error("Raft node has no SnapshotStore");
  }
  return snapshot_store_->ReadPayloadChunk(snapshot, offset, maximum_size);
}

void RaftNode::Send(NodeId to, RaftMessage message) {
  if (role_ == RaftRole::STOPPED || role_ == RaftRole::TERM_PERSISTING) {
    throw std::runtime_error("Raft node cannot send while durable term state is unavailable");
  }
  transport_->Send({config_.node_id_, to, std::move(message), config_.group_id_});
}

void RaftNode::SendAppend(NodeId peer, std::optional<uint64_t> read_context) {
  if (snapshot_transfers_.count(peer) != 0) {
    // Heartbeats retransmit the one in-flight durable chunk with the same
    // request identity. A later heartbeat must not invalidate an ACK that is
    // delayed by the follower's fsync.
    SendSnapshot(peer);
    return;
  }
  auto next = next_index_.at(peer);
  if (next <= log_store_->SnapshotBaseIndex()) {
    SendSnapshot(peer);
    return;
  }
  const auto prev = next - 1;
  const auto prev_term = log_store_->TermAt(prev);
  if (!prev_term.has_value()) {
    throw std::runtime_error("AppendEntries needs a compacted snapshot transfer");
  }
  std::vector<ReplicatedLogEntry> entries;
  if (next <= log_store_->LastLogIndex()) {
    entries = log_store_->Entries(next, log_store_->LastLogIndex());
  }
  const auto request_id = ++last_request_id_[peer];
  Send(peer, AppendEntriesRequest{hard_state_.current_term_, config_.node_id_, request_id, prev, *prev_term,
                                  std::move(entries), hard_state_.commit_index_, read_context});
}

void RaftNode::SendSnapshot(NodeId peer, std::optional<uint64_t> acknowledged_offset) {
  if (snapshot_store_ == nullptr || !snapshot_store_->Latest().has_value()) {
    throw std::runtime_error("Raft Leader has no published snapshot for a compacted follower");
  }
  const auto snapshot = *snapshot_store_->Latest();
  auto transfer = snapshot_transfers_.find(peer);
  if (transfer == snapshot_transfers_.end() || transfer->second.snapshot_.snapshot_id_ != snapshot.snapshot_id_) {
    const auto request_id = ++last_request_id_[peer];
    transfer = snapshot_transfers_.insert_or_assign(peer, SnapshotTransfer{snapshot, 0, 0, request_id}).first;
  } else if (acknowledged_offset.has_value()) {
    if (*acknowledged_offset < transfer->second.end_offset_ || *acknowledged_offset >= snapshot.payload_size_) {
      throw std::runtime_error("follower acknowledged an invalid Raft snapshot offset");
    }
    transfer->second.offset_ = *acknowledged_offset;
    transfer->second.request_id_ = ++last_request_id_[peer];
  }
  constexpr size_t MAX_CHUNK_BYTES = 64U * 1024U;
  const auto offset = transfer->second.offset_;
  const auto chunk_size = static_cast<size_t>(std::min<uint64_t>(MAX_CHUNK_BYTES, snapshot.payload_size_ - offset));
  auto chunk = snapshot_store_->ReadPayloadChunk(snapshot, offset, chunk_size);
  transfer->second.end_offset_ = offset + chunk.size();
  const auto done = transfer->second.end_offset_ == snapshot.payload_size_;
  Send(peer,
       InstallSnapshotRequest{hard_state_.current_term_, config_.node_id_, transfer->second.request_id_,
                              snapshot.snapshot_id_, snapshot.last_included_index_, snapshot.last_included_term_,
                              offset, snapshot.payload_size_, snapshot.payload_checksum_, done, std::move(chunk)});
}

void RaftNode::BroadcastAppend() {
  for (const auto peer : config_.voters_) {
    if (peer != config_.node_id_) {
      SendAppend(peer);
    }
  }
}

void RaftNode::BroadcastReadIndex(uint64_t context) {
  for (const auto peer : config_.voters_) {
    if (peer != config_.node_id_) {
      SendAppend(peer, context);
    }
  }
}

void RaftNode::AdvanceLeaderCommit() {
  for (uint64_t candidate = log_store_->LastLogIndex(); candidate > hard_state_.commit_index_; candidate--) {
    if (log_store_->TermAt(candidate) != std::optional<uint64_t>{hard_state_.current_term_}) {
      continue;
    }
    size_t replicas = 0;
    for (const auto voter : config_.voters_) {
      if (match_index_[voter] >= candidate) {
        replicas++;
      }
    }
    if (!HasMajority(replicas)) {
      continue;
    }
    PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, candidate);
    AdvanceLogCommitOrStop(candidate);
    ApplyCommitted();
    BroadcastAppend();
    return;
  }
}

void RaftNode::ApplyCommitted() {
  while (last_applied_ < hard_state_.commit_index_) {
    const auto next = last_applied_ + 1;
    const auto entry = log_store_->EntryAt(next);
    if (!entry.has_value()) {
      FailStop();
      throw std::runtime_error("committed Raft entry is unavailable for Apply");
    }
    try {
      state_machine_->Apply(*entry);
    } catch (...) {
      FailStop();
      throw;
    }
    last_applied_ = next;
    published_applied_index_ = next;
  }
}

auto RaftNode::HasMajority(size_t votes) const -> bool { return votes >= config_.voters_.size() / 2 + 1; }

auto RaftNode::CandidateLogIsUpToDate(uint64_t last_term, uint64_t last_index) const -> bool {
  const auto local_term = log_store_->LastLogTerm();
  return last_term > local_term || (last_term == local_term && last_index >= log_store_->LastLogIndex());
}

auto RaftNode::FirstIndexOfTerm(uint64_t index, uint64_t term) const -> uint64_t {
  while (index > log_store_->SnapshotBaseIndex()) {
    const auto previous = log_store_->TermAt(index - 1);
    if (!previous.has_value() || *previous != term) {
      break;
    }
    index--;
  }
  return index;
}

auto RaftNode::LastIndexOfTerm(uint64_t term) const -> std::optional<uint64_t> {
  auto index = log_store_->LastLogIndex();
  while (true) {
    const auto value = log_store_->TermAt(index);
    if (value == std::optional<uint64_t>{term}) {
      return index;
    }
    if (index == log_store_->SnapshotBaseIndex()) {
      break;
    }
    index--;
  }
  return std::nullopt;
}

}  // namespace bustub
