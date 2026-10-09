//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// raft_node.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/raft_node.h"

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <limits>
#include <random>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

#include "snapshot_tasks.h"

namespace bustub {
namespace {

auto MakeGeneratorBackedElectionTimeoutSource(std::mt19937_64 generator) -> ElectionTimeoutSource {
  auto shared_generator = std::make_shared<std::mt19937_64>(generator);
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

// Only the serialized protocol owner resumes these frames. A worker never
// resumes a coroutine or changes Raft state.
class RaftNode::Operation {
 public:
  struct promise_type {
    std::exception_ptr error_;
    std::coroutine_handle<> parent_{std::noop_coroutine()};
    auto get_return_object() -> Operation {
      return Operation{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    auto initial_suspend() noexcept -> std::suspend_always { return {}; }
    struct Final {
      auto await_ready() noexcept -> bool { return false; }
      auto await_suspend(std::coroutine_handle<promise_type> handle) noexcept -> std::coroutine_handle<> {
        return handle.promise().parent_;
      }
      void await_resume() noexcept {}
    };
    auto final_suspend() noexcept -> Final { return {}; }
    void return_void() {}
    void unhandled_exception() { error_ = std::current_exception(); }
  };
  explicit Operation(std::coroutine_handle<promise_type> handle) : handle_(handle) {}
  Operation(Operation &&other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  ~Operation() {
    if (handle_) handle_.destroy();
  }
  auto await_ready() -> bool { return false; }
  auto await_suspend(std::coroutine_handle<> parent) -> std::coroutine_handle<> {
    handle_.promise().parent_ = parent;
    return handle_;
  }
  void await_resume() { Check(); }
  void Resume() { handle_.resume(); }
  auto Done() const -> bool { return handle_.done(); }
  void Check() {
    if (handle_.promise().error_) std::rethrow_exception(handle_.promise().error_);
  }

 private:
  std::coroutine_handle<promise_type> handle_;
};

template <typename F>
auto RaftNode::Slow(F work, bool business) {
  struct Awaiter {
    RaftNode *node_;
    F work_;
    bool business_;
    using T = std::invoke_result_t<F>;
    std::shared_ptr<TaskExecutor::Result<T>> result_;
    auto await_ready() -> bool { return false; }
    void await_suspend(std::coroutine_handle<> continuation) {
      auto &executor = business_ ? node_->business_tasks_ : node_->storage_tasks_;
      result_ = executor->Submit(sizeof(T) + sizeof(F), true, std::move(work_));
      node_->poll_work_ = [this, continuation] {
        if (!result_) {
          auto &executor = business_ ? node_->business_tasks_ : node_->storage_tasks_;
          result_ = executor->Submit(sizeof(T) + sizeof(F), true, std::move(work_));
        }
        if (!result_ || !result_->Ready()) return false;
        continuation.resume();
        return true;
      };
    }
    auto await_resume() -> T { return result_->Take(); }
  };
  return Awaiter{this, std::move(work), business, {}};
}

void RaftNode::RefreshLogTip() {
  durable_tip_ = log_store_->LastLogIndex();
  durable_tip_term_ = log_store_->LastLogTerm();
  durable_base_ = log_store_->SnapshotBaseIndex();
  latest_snapshot_ = snapshot_store_ ? snapshot_store_->Latest() : std::nullopt;
}
auto RaftNode::UpdateLogTip() -> Operation {
  const auto values = co_await Slow([&] {
    return std::make_tuple(log_store_->LastLogIndex(), log_store_->LastLogTerm(), log_store_->SnapshotBaseIndex(),
                           snapshot_store_ ? snapshot_store_->Latest() : std::optional<RaftSnapshot>{});
  });
  std::tie(durable_tip_, durable_tip_term_, durable_base_, latest_snapshot_) = values;
}

auto RaftNode::Busy() const -> bool { return operation_ != nullptr || !pending_messages_.empty(); }

void RaftNode::Start(Operation operation) {
  if (operation_) throw std::logic_error("overlapping protocol operations");
  operation_ = std::make_unique<Operation>(std::move(operation));
  operation_->Resume();
  if (operation_->Done()) {
    auto completed = std::move(operation_);
    try {
      completed->Check();
    } catch (...) {
      FailStop();
      throw;
    }
  }
}

void RaftNode::Poll() {
  if (poll_work_) {
    auto poll = std::move(poll_work_);
    if (!poll()) poll_work_ = std::move(poll);
  }
  if (operation_ && operation_->Done()) {
    auto completed = std::move(operation_);
    try {
      completed->Check();
    } catch (...) {
      FailStop();
      throw;
    }
  }
  if (operation_ || role_ == RaftRole::STOPPED) return;
  if (observed_term_ > hard_state_.current_term_) {
    Start(ObserveHigherTerm(observed_term_));
  } else if (!pending_messages_.empty()) {
    auto pending = std::move(pending_messages_.front());
    pending_messages_.pop_front();
    // The coroutine owns its message until the last dependent operation.
    Start(Dispatch(pending.from_, std::move(pending.message_), std::move(pending.charge_)));
  }
}

void RaftNode::Drain() {
  draining_ = true;
  pending_messages_.clear();
  while (operation_) {
    Poll();
    if (operation_) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  business_tasks_->Drain();
  storage_tasks_->Drain();
  if (snapshot_tasks_) snapshot_tasks_->Drain();
  draining_ = false;
}

void RaftNode::HeartbeatWhileBusy() {
  if (role_ != RaftRole::LEADER || observed_term_ > hard_state_.current_term_ || now_ms_ < heartbeat_deadline_ms_)
    return;
  for (auto peer : config_.voters_)
    if (peer != config_.node_id_) {
      Send(peer, AppendEntriesRequest{hard_state_.current_term_,
                                      config_.node_id_,
                                      ++last_request_id_[peer],
                                      durable_tip_,
                                      durable_tip_term_,
                                      {},
                                      hard_state_.commit_index_,
                                      std::nullopt});
    }
  heartbeat_deadline_ms_ = now_ms_ + config_.heartbeat_interval_ms_;
}

auto RaftNode::Dispatch(NodeId from, RaftMessage message, ResourceCharge charge) -> Operation {
  // visit returns the selected suspended operation; message remains in this frame.
  co_await std::visit([&](const auto &value) { return Handle(from, value); }, message);
  co_await PollSnapshotTasks();

  co_return;
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
    stable_store_->Update(hard_state_.current_term_, hard_state_.voted_for_, log_store_->CommittedIndex());
    hard_state_ = stable_store_->State();
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
      state_machine_->LoadSnapshot(snapshot_store_->Input(snapshot), snapshot.last_included_index_);
    }
  }
  const auto recovered_snapshot_index = snapshot_store_ != nullptr && snapshot_store_->Latest().has_value()
                                            ? snapshot_store_->Latest()->last_included_index_
                                            : log_store_->SnapshotBaseIndex();
  const auto local = state_machine_->LocalRecoveryPoint();
  const bool local_matches = local && local->index_ == state_machine_->LastApplied() &&
                             local->index_ >= recovered_snapshot_index &&
                             log_store_->TermAt(local->index_) == std::optional<uint64_t>{local->term_};
  if ((local && !local_matches) || (!local_matches && state_machine_->LastApplied() != recovered_snapshot_index) ||
      state_machine_->LastApplied() > hard_state_.commit_index_) {
    throw std::runtime_error("Raft state machine does not match the recovered snapshot base");
  }
  last_applied_ = state_machine_->LastApplied();
  published_applied_index_ = last_applied_;
  while (last_applied_ < hard_state_.commit_index_) {
    auto entry = log_store_->EntryAt(last_applied_ + 1);
    if (!entry) throw std::runtime_error("committed recovery entry is unavailable");
    state_machine_->Apply(*entry);
    last_applied_ = entry->index_;
    published_applied_index_ = last_applied_;
  }
  storage_tasks_ = std::make_unique<TaskExecutor>(1, 1, config_.memory_budget_, config_.wake_);
  business_tasks_ = std::make_unique<TaskExecutor>(1, 8, config_.memory_budget_, config_.wake_, 1);
  pending_memory_ = ResourceAccount::Create(config_.memory_budget_);
  observed_term_ = hard_state_.current_term_;
  RefreshLogTip();
  ResetElectionDeadline();
  if (snapshot_store_) snapshot_tasks_ = std::make_unique<SnapshotTasks>(config_.memory_budget_);
}

RaftNode::~RaftNode() {
  try {
    Drain();
  } catch (...) { /* failure already stopped protocol; accepted IO is drained below */
  }
  snapshot_tasks_.reset();
  business_tasks_.reset();
  storage_tasks_.reset();
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

auto RaftNode::PersistHardState(uint64_t term, std::optional<NodeId> voted_for, uint64_t commit_index) -> Operation {
  try {
    hard_state_ = co_await Slow([=, this] {
      stable_store_->Update(term, voted_for, commit_index);
      return stable_store_->State();
    });
    observed_term_ = std::max(observed_term_, hard_state_.current_term_);
  } catch (...) {
    FailStop();
    throw;
  }

  co_return;
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

auto RaftNode::AppendLogDurably(const std::vector<ReplicatedLogEntry> &entries) -> Operation {
  try {
    co_await Slow([&] {
      log_store_->Append(entries);
      return true;
    });
    co_await UpdateLogTip();
  } catch (...) {
    // A failed durable append may already have changed the on-disk image. The
    // in-memory log cannot be trusted to choose another index until restart.
    FailStop();
    throw;
  }

  co_return;
}

auto RaftNode::ReplaceLogSuffixDurably(uint64_t from_index, const std::vector<ReplicatedLogEntry> &entries)
    -> Operation {
  log_mutating_ = true;
  try {
    co_await Slow([&] {
      log_store_->ReplaceSuffix(from_index, entries);
      return true;
    });
    co_await UpdateLogTip();
    log_mutating_ = false;
  } catch (...) {
    FailStop();
    throw;
  }

  co_return;
}

auto RaftNode::InstallLogSnapshotBaseDurably(uint64_t index, uint64_t term, bool retain_old_suffix) -> Operation {
  log_mutating_ = true;
  try {
    co_await Slow([&] {
      log_store_->InstallSnapshotBase(index, term, retain_old_suffix);
      return true;
    });
    co_await UpdateLogTip();
    log_mutating_ = false;
  } catch (...) {
    FailStop();
    throw;
  }

  co_return;
}

auto RaftNode::AdvanceLogCommitOrStop(uint64_t committed_index) -> Operation {
  try {
    co_await Slow([&] {
      log_store_->AdvanceCommittedIndex(committed_index);
      return true;
    });
    co_await UpdateLogTip();
  } catch (...) {
    FailStop();
    throw;
  }

  co_return;
}

void RaftNode::Tick(uint64_t now_ms) {
  if (now_ms < now_ms_) throw std::runtime_error("Raft logical clock cannot move backwards");
  now_ms_ = now_ms;
  Poll();
  if (role_ == RaftRole::STOPPED) return;
  if (operation_) {
    HeartbeatWhileBusy();
    return;
  }
  Start(RunTick());
}

auto RaftNode::RunTick() -> Operation {
  co_await PollSnapshotTasks();
  if (incoming_delta_ && now_ms_ >= incoming_delta_->deadline_) co_await CancelIncomingDelta();
  if (role_ == RaftRole::LEADER && observed_term_ <= hard_state_.current_term_) {
    if (now_ms_ >= heartbeat_deadline_ms_) {
      co_await BroadcastAppend();
      std::vector<uint64_t> contexts;
      for (const auto &[context, read] : pending_read_indexes_) contexts.push_back(context);
      for (const auto context : contexts) co_await BroadcastReadIndex(context);
      heartbeat_deadline_ms_ = now_ms_ + config_.heartbeat_interval_ms_;
    }
  } else if (role_ != RaftRole::TERM_PERSISTING && now_ms_ >= election_deadline_ms_) {
    co_await StartElection();
  }
}

auto RaftNode::StartElection() -> Operation {
  co_await CancelIncomingDelta();
  snapshot_request_floor_ = 0;
  incoming_encoding_.reset();
  role_ = RaftRole::TERM_PERSISTING;
  const auto new_term = hard_state_.current_term_ + 1;
  co_await PersistHardState(new_term, config_.node_id_, hard_state_.commit_index_);
  role_ = RaftRole::CANDIDATE;
  leader_id_.reset();
  votes_received_ = {config_.node_id_};
  ResetElectionDeadline();

  RequestVoteRequest request{hard_state_.current_term_, config_.node_id_, durable_tip_, durable_tip_term_};
  for (const auto peer : config_.voters_) {
    if (peer != config_.node_id_) {
      Send(peer, request);
    }
  }
  if (HasMajority(votes_received_.size())) {
    co_await BecomeLeader();
  }

  co_return;
}

auto RaftNode::BecomeLeader() -> Operation {
  if (role_ != RaftRole::CANDIDATE) {
    throw std::runtime_error("only a Candidate can become Leader");
  }
  role_ = RaftRole::LEADER;
  proposal_errors_.clear();  // Reused log indexes must not inherit a prior term's rejection.
  leader_id_ = config_.node_id_;
  next_index_.clear();
  match_index_.clear();
  last_request_id_.clear();
  snapshot_transfers_.clear();
  pending_read_indexes_.clear();
  completed_read_indexes_.clear();
  const auto initial_next = durable_tip_ + 1;
  for (const auto voter : config_.voters_) {
    next_index_[voter] = initial_next;
    match_index_[voter] = voter == config_.node_id_ ? durable_tip_ : 0;
    last_request_id_[voter] = 0;
  }

  const auto noop_index = durable_tip_ + 1;
  const std::vector<ReplicatedLogEntry> noop{{1, noop_index, hard_state_.current_term_, EntryType::NOOP, {}}};
  co_await AppendLogDurably(noop);
  match_index_[config_.node_id_] = noop_index;
  next_index_[config_.node_id_] = noop_index + 1;
  leader_barrier_index_ = noop_index;
  co_await BroadcastAppend();
  heartbeat_deadline_ms_ = now_ms_ + config_.heartbeat_interval_ms_;

  co_return;
}

auto RaftNode::ObserveHigherTerm(uint64_t term) -> Operation {
  if (term <= hard_state_.current_term_) {
    co_return;
  }
  co_await CancelIncomingDelta();
  snapshot_request_floor_ = 0;
  incoming_encoding_.reset();
  role_ = RaftRole::TERM_PERSISTING;
  leader_id_.reset();
  leader_barrier_index_ = 0;
  co_await PersistHardState(term, std::nullopt, hard_state_.commit_index_);
  role_ = RaftRole::FOLLOWER;
  votes_received_.clear();
  next_index_.clear();
  match_index_.clear();
  snapshot_transfers_.clear();
  pending_read_indexes_.clear();
  completed_read_indexes_.clear();
  ResetElectionDeadline();

  co_return;
}

void RaftNode::Receive(NodeId from, const RaftMessage &message) {
  if (role_ == RaftRole::STOPPED || from == config_.node_id_ ||
      std::find(config_.voters_.begin(), config_.voters_.end(), from) == config_.voters_.end())
    return;
  const auto term = std::visit([](const auto &m) { return m.term_; }, message);
  observed_term_ = std::max(observed_term_, term);
  if (const auto *append = std::get_if<AppendEntriesRequest>(&message);
      append && append->leader_id_ == from && term >= hard_state_.current_term_) {
    ResetElectionDeadline();
    // A matching durable prefix can acknowledge a heartbeat during slow work.
    // Any commit advancement still goes through the ordered operation below.
    if (operation_ && !log_mutating_ && term == hard_state_.current_term_ && observed_term_ == term &&
        append->entries_.empty() && append->prev_log_index_ == durable_tip_ &&
        append->prev_log_term_ == durable_tip_term_ && role_ != RaftRole::TERM_PERSISTING) {
      Send(from, AppendEntriesResponse{term, append->request_id_, true, durable_tip_, std::nullopt, 0,
                                       append->read_context_});
    }
  }
  const auto bytes = std::visit(
      [](const auto &m) -> size_t {
        using M = std::decay_t<decltype(m)>;
        size_t size = sizeof(M);
        if constexpr (std::is_same_v<M, AppendEntriesRequest>) {
          for (const auto &entry : m.entries_) size += sizeof(entry) + entry.payload_.size();
        } else if constexpr (std::is_same_v<M, InstallSnapshotRequest>) {
          size += m.data_.size() + m.snapshot_id_.size();
        }
        return size;
      },
      message);
  // Network requests remain retryable until a matching response. Higher terms
  // are remembered even when the bounded payload queue cannot admit a copy.
  if (pending_messages_.size() < 32 && pending_memory_->Reserve(bytes, true)) {
    ResourceCharge charge(pending_memory_, bytes, true);
    pending_messages_.push_back({from, message, std::move(charge)});
  }
  Poll();
}

auto RaftNode::Handle(NodeId from, const RequestVoteRequest &request) -> Operation {
  if (request.term_ > hard_state_.current_term_) {
    co_await ObserveHigherTerm(request.term_);
  }
  bool grant = false;
  if (request.term_ == hard_state_.current_term_ && request.candidate_id_ == from &&
      (hard_state_.voted_for_ == std::nullopt || hard_state_.voted_for_ == request.candidate_id_) &&
      CandidateLogIsUpToDate(request.last_log_term_, request.last_log_index_)) {
    if (hard_state_.voted_for_ != request.candidate_id_) {
      co_await PersistHardState(hard_state_.current_term_, request.candidate_id_, hard_state_.commit_index_);
    }
    grant = true;
    ResetElectionDeadline();
  }
  Send(from, RequestVoteResponse{hard_state_.current_term_, grant});

  co_return;
}

auto RaftNode::Handle(NodeId from, const RequestVoteResponse &response) -> Operation {
  if (response.term_ > hard_state_.current_term_) {
    co_await ObserveHigherTerm(response.term_);
    co_return;
  }
  if (role_ != RaftRole::CANDIDATE || response.term_ != hard_state_.current_term_ || !response.vote_granted_) {
    co_return;
  }
  if (std::find(config_.voters_.begin(), config_.voters_.end(), from) == config_.voters_.end()) {
    co_return;
  }
  votes_received_.insert(from);
  if (HasMajority(votes_received_.size())) {
    co_await BecomeLeader();
  }

  co_return;
}

auto RaftNode::Handle(NodeId from, const AppendEntriesRequest &request) -> Operation {
  if (request.term_ > hard_state_.current_term_) {
    co_await ObserveHigherTerm(request.term_);
  }
  if (request.term_ < hard_state_.current_term_ || request.leader_id_ != from) {
    Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, std::nullopt,
                                     durable_tip_ + 1, std::nullopt});
    co_return;
  }
  if (role_ != RaftRole::FOLLOWER) {
    role_ = RaftRole::FOLLOWER;
    leader_barrier_index_ = 0;
    pending_read_indexes_.clear();
    completed_read_indexes_.clear();
  }
  leader_id_ = from;
  ResetElectionDeadline();

  if (incoming_delta_ && incoming_delta_->term_ == request.term_ && incoming_delta_->leader_ == from) {
    incoming_delta_->deadline_ = election_deadline_ms_;
  }

  const auto local_prev_term = (co_await Slow([&] { return log_store_->TermAt(request.prev_log_index_); }));
  if (!local_prev_term.has_value()) {
    const auto conflict = request.prev_log_index_ < durable_base_ ? durable_base_ + 1 : durable_tip_ + 1;
    Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, std::nullopt, conflict,
                                     request.read_context_});
    co_return;
  }
  if (*local_prev_term != request.prev_log_term_) {
    Send(from, AppendEntriesResponse{
                   hard_state_.current_term_, request.request_id_, false, 0, *local_prev_term,
                   (co_await Slow([&] { return FirstIndexOfTerm(request.prev_log_index_, *local_prev_term); })),
                   request.read_context_});
    co_return;
  }

  uint64_t expected_index = request.prev_log_index_ + 1;
  for (const auto &entry : request.entries_) {
    if (entry.index_ != expected_index) {
      Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, false, 0, std::nullopt,
                                       durable_tip_ + 1, request.read_context_});
      co_return;
    }
    expected_index++;
  }

  size_t first_new = 0;
  while (first_new < request.entries_.size()) {
    const auto &entry = request.entries_[first_new];
    const auto local_term = (co_await Slow([&] { return log_store_->TermAt(entry.index_); }));
    if (!local_term.has_value() || *local_term != entry.term_) {
      break;
    }
    first_new++;
  }
  if (first_new < request.entries_.size()) {
    const auto from_index = request.entries_[first_new].index_;
    std::vector<ReplicatedLogEntry> suffix(request.entries_.begin() + static_cast<ptrdiff_t>(first_new),
                                           request.entries_.end());
    if (from_index <= durable_tip_) {
      co_await ReplaceLogSuffixDurably(from_index, suffix);
    } else {
      co_await AppendLogDurably(suffix);
    }
  }

  const auto match_index = request.prev_log_index_ + request.entries_.size();
  // A heartbeat or partial append proves only this prefix. A longer local
  // suffix may still belong to an old leader and must not be committed yet.
  const auto new_commit = std::min(request.leader_commit_, match_index);
  if (new_commit > hard_state_.commit_index_) {
    co_await PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, new_commit);
    co_await AdvanceLogCommitOrStop(new_commit);
    co_await ApplyCommitted();
  }
  Send(from, AppendEntriesResponse{hard_state_.current_term_, request.request_id_, true, match_index, std::nullopt, 0,
                                   request.read_context_});

  co_return;
}

auto RaftNode::Handle(NodeId from, const AppendEntriesResponse &response) -> Operation {
  if (response.term_ > hard_state_.current_term_) {
    co_await ObserveHigherTerm(response.term_);
    co_return;
  }
  if (role_ != RaftRole::LEADER || response.term_ != hard_state_.current_term_ || next_index_.count(from) == 0) {
    co_return;
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
    co_await AdvanceLeaderCommit();
    if (next_index_[from] <= durable_tip_) {
      co_await SendAppend(from);
    }
    co_return;
  }
  if (response.request_id_ != last_request_id_[from]) {
    co_return;
  }
  uint64_t next = response.conflict_index_;
  if (response.conflict_term_.has_value()) {
    if (const auto local = co_await Slow([&] { return LastIndexOfTerm(*response.conflict_term_); });
        local.has_value()) {
      next = *local + 1;
    }
  }
  next_index_[from] = std::max<uint64_t>(1, std::min(next, durable_tip_ + 1));
  co_await SendAppend(from);

  co_return;
}

auto RaftNode::CancelIncomingDelta() -> Operation {
  if (incoming_delta_) {
    co_await Slow([&] {
      snapshot_store_->CancelStaged(incoming_delta_->target_);
      return true;
    });
    incoming_delta_.reset();
  }

  co_return;
}
auto RaftNode::Handle(NodeId from, const SnapshotOfferRequest &request) -> Operation {
  if (request.term_ > hard_state_.current_term_) co_await ObserveHigherTerm(request.term_);
  auto status = SnapshotOfferStatus::Unsupported;
  if (request.term_ == hard_state_.current_term_ && request.leader_id_ == from && snapshot_store_) {
    role_ = RaftRole::FOLLOWER;
    leader_barrier_index_ = 0;
    leader_id_ = from;
    ResetElectionDeadline();
    if (request.target_.last_included_index_ > published_applied_index_) {
      bool failed = false;
      try {
        if (request.extended_ && incoming_encoding_ && incoming_encoding_->term_ == request.term_ &&
            incoming_encoding_->leader_ == from && incoming_encoding_->session_ == request.request_id_ &&
            incoming_encoding_->target_ == request.target_.snapshot_id_) {
          if (incoming_delta_) incoming_delta_->deadline_ = election_deadline_ms_;
          status = incoming_delta_ && incoming_delta_->session_ == request.request_id_
                       ? SnapshotOfferStatus::Accepted
                       : SnapshotOfferStatus::Unsupported;
        } else if (incoming_delta_ && incoming_delta_->session_ == request.request_id_ &&
                   incoming_delta_->leader_ == from && incoming_delta_->target_ == request.target_.snapshot_id_) {
          incoming_delta_->deadline_ = election_deadline_ms_;
          status = SnapshotOfferStatus::Accepted;
        } else if (request.request_id_ <= snapshot_request_floor_) {
          status = SnapshotOfferStatus::Error;
        } else {
          snapshot_request_floor_ = request.request_id_;
          co_await CancelIncomingDelta();
          if (!request.base_.snapshot_id_.empty() && (co_await Slow([&] {
                return snapshot_store_->BeginDelta(request.target_, request.base_, request.request_id_);
              }))) {
            incoming_delta_ = IncomingDelta{request.term_, from, request.request_id_, election_deadline_ms_,
                                            request.target_.snapshot_id_};
            status = SnapshotOfferStatus::Accepted;
          }
        }
      } catch (const std::exception &) {
        failed = true;
      }
      if (failed) {
        co_await CancelIncomingDelta();
        status = SnapshotOfferStatus::Error;
      }
    }
  }
  if (request.extended_ && request.term_ == hard_state_.current_term_ && request.leader_id_ == from &&
      snapshot_store_ && request.target_.last_included_index_ > published_applied_index_ &&
      status != SnapshotOfferStatus::Error) {
    incoming_encoding_ =
        IncomingDelta{request.term_, from, request.request_id_, election_deadline_ms_, request.target_.snapshot_id_};
  }
  Send(from, SnapshotOfferResponse{hard_state_.current_term_, request.request_id_, status, request.extended_});

  co_return;
}
auto RaftNode::Handle(NodeId from, const SnapshotOfferResponse &response) -> Operation {
  if (response.term_ > hard_state_.current_term_) co_await ObserveHigherTerm(response.term_);
  const auto it = snapshot_transfers_.find(from);
  if (role_ != RaftRole::LEADER || response.term_ != hard_state_.current_term_ || it == snapshot_transfers_.end() ||
      !it->second.offering_ || it->second.offer_id_ != response.request_id_ ||
      it->second.extended_ != response.extended_)
    co_return;
  auto &t = it->second;
  if (response.status_ == SnapshotOfferStatus::Error) {
    snapshot_transfers_.erase(it);
    co_return;
  }
  t.offering_ = false;
  t.compression_ = response.extended_;
  if (response.status_ == SnapshotOfferStatus::Accepted && t.offer_)
    t.delta_ = co_await Slow([&] { return t.offer_->plan_(); });
  t.offer_.reset();
  t.request_id_ = ++last_request_id_[from];
  co_await SendSnapshot(from);

  co_return;
}

auto RaftNode::Handle(NodeId from, const InstallSnapshotRequest &request) -> Operation {
  if (request.term_ > hard_state_.current_term_) {
    co_await ObserveHigherTerm(request.term_);
  }
  if (request.term_ < hard_state_.current_term_ || request.leader_id_ != from || snapshot_store_ == nullptr) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    co_return;
  }
  // Full fallback has a newer request ID than its Offer. An older full chunk
  // must not cancel the delta session that superseded it.
  if (request.delta_session_ == 0 && request.request_id_ < snapshot_request_floor_) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    co_return;
  }
  if (role_ != RaftRole::FOLLOWER) {
    role_ = RaftRole::FOLLOWER;
    leader_barrier_index_ = 0;
  }
  leader_id_ = from;
  ResetElectionDeadline();

  if (request.delta_session_ != 0) {
    if (!incoming_delta_ || incoming_delta_->term_ != request.term_ || incoming_delta_->leader_ != from ||
        incoming_delta_->session_ != request.delta_session_ || incoming_delta_->target_ != request.snapshot_id_) {
      Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
      co_return;
    }
    incoming_delta_->deadline_ = election_deadline_ms_;
  } else {
    snapshot_request_floor_ = std::max(snapshot_request_floor_, request.request_id_);
    co_await CancelIncomingDelta();
  }

  // First stale guard: do not even retain download state for an obsolete image.
  if (request.offset_ == 0 && request.last_included_index_ <= published_applied_index_) {
    co_await Slow([&] {
      snapshot_store_->CancelStaged(request.snapshot_id_);
      return true;
    });
    incoming_delta_.reset();
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, true, true,
                                       published_applied_index_, 0});
    co_return;
  }

  if (request.encoding_ != SnapshotEncoding::Raw) {
    if (!incoming_encoding_ || incoming_encoding_->term_ != request.term_ || incoming_encoding_->leader_ != from ||
        incoming_encoding_->session_ != request.encoding_session_ ||
        incoming_encoding_->target_ != request.snapshot_id_) {
      Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
      co_return;
    }
    // A duplicate waits for the same completion. No ACK until decoding and Stage finish.
    if (incoming_decode_) co_return;
    auto work = snapshot_tasks_->Decode(request);
    if (work)
      incoming_decode_.emplace(IncomingDecode{from, request.term_, request.request_id_, request.encoding_session_,
                                              request.snapshot_id_, std::move(work)});
    co_return;  // Busy admission is retried by the sender; there is no unbounded pending queue.
  }

  const auto staged_result = co_await Slow([&]() -> std::optional<SnapshotStageResult> {
    try {
      return snapshot_store_->StageChunk({request.snapshot_id_, request.last_included_index_,
                                          request.last_included_term_, request.offset_, request.total_size_,
                                          request.payload_checksum_, request.done_, request.data_,
                                          request.delta_session_, request.reuse_});
    } catch (const std::exception &) {
      snapshot_store_->CancelStaged(request.snapshot_id_);
      return std::nullopt;
    }
  });
  if (!staged_result) {
    incoming_delta_.reset();
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    co_return;
  }
  const auto stage = *staged_result;
  if (stage.status_ == SnapshotStageStatus::IN_PROGRESS) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, false, false, 0,
                                       stage.next_offset_});
    co_return;
  }

  // Final stale guard runs in this same single-threaded Apply/Install sequence.
  if (request.last_included_index_ <= published_applied_index_) {
    co_await Slow([&] {
      snapshot_store_->CancelStaged(request.snapshot_id_);
      return true;
    });
    incoming_delta_.reset();
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, true, true,
                                       published_applied_index_, 0});
    co_return;
  }
  const auto staged = co_await Slow([&] { return snapshot_store_->Staged(request.snapshot_id_); });
  const auto staged_payload = co_await Slow([&] { return snapshot_store_->StagedInput(request.snapshot_id_); });
  if (!staged.has_value() || !staged_payload.has_value()) {
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    co_return;
  }

  const auto preinstall_term = (co_await Slow([&] { return log_store_->TermAt(staged->last_included_index_); }));
  const bool retain_suffix = preinstall_term == std::optional<uint64_t>{staged->last_included_term_};
  if (hard_state_.commit_index_ > staged->last_included_index_ && !retain_suffix) {
    // The snapshot cannot replace an already-committed suffix unless its
    // boundary is proved to be on the same log. Reject it before publishing
    // CURRENT, advancing HARD_STATE, rebasing the log, or touching the FSM.
    // Cleaning the non-authoritative download is safe; a failed cleanup still
    // leaves the node unable to continue without a restart.
    try {
      co_await Slow([&] {
        snapshot_store_->CancelStaged(request.snapshot_id_);
        return true;
      });
      incoming_delta_.reset();
    } catch (...) {
      FailStop();
      throw;
    }
    FailStop();
    throw std::runtime_error("Raft snapshot boundary cannot preserve the committed suffix");
  }
  auto prepared = co_await Slow(
      [&]() -> std::unique_ptr<PreparedSnapshot> {
        try {
          return state_machine_->PrepareSnapshot(*staged_payload, staged->last_included_index_);
        } catch (const std::exception &) {
          snapshot_store_->CancelStaged(request.snapshot_id_);
          return {};
        }
      },
      true);
  if (!prepared) {
    incoming_delta_.reset();
    Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, false, false, false, 0, 0});
    co_return;
  }
  try {
    co_await Slow([&] {
      snapshot_store_->PublishStaged(request.snapshot_id_, retain_suffix);
      return true;
    });
    const auto new_commit = std::max(hard_state_.commit_index_, staged->last_included_index_);
    if (new_commit > hard_state_.commit_index_) {
      co_await PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, new_commit);
    }
    const auto recovery_base = co_await Slow([&] { return snapshot_store_->OldestRetained(); });
    if (!recovery_base.has_value()) {
      throw std::runtime_error("published Raft snapshot has no recovery base");
    }
    if (recovery_base->last_included_index_ > durable_base_) {
      const bool retain_bridge =
          (co_await Slow([&] { return log_store_->TermAt(recovery_base->last_included_index_); })) ==
          std::optional<uint64_t>{recovery_base->last_included_term_};
      co_await InstallLogSnapshotBaseDurably(recovery_base->last_included_index_, recovery_base->last_included_term_,
                                             retain_bridge);
    }
    if ((co_await Slow([&] { return log_store_->CommittedIndex(); })) < new_commit) {
      co_await AdvanceLogCommitOrStop(new_commit);
    }
    co_await Slow(
        [&] {
          prepared->Install();
          prepared.reset();
          return true;
        },
        true);
    co_await Slow([&] {
      snapshot_store_->CancelStaged(request.snapshot_id_);
      return true;
    });
    incoming_delta_.reset();
    last_applied_ = staged->last_included_index_;
    published_applied_index_ = staged->last_included_index_;
    co_await UpdateLogTip();
    co_await ApplyCommitted();
  } catch (...) {
    // Publication may already have changed durable authority. Only restart
    // recovery can safely choose and install the resulting state.
    FailStop();
    throw;
  }
  Send(from, InstallSnapshotResponse{hard_state_.current_term_, request.request_id_, true, false, true,
                                     staged->last_included_index_, 0});

  co_return;
}

auto RaftNode::Handle(NodeId from, const InstallSnapshotResponse &response) -> Operation {
  if (response.term_ > hard_state_.current_term_) {
    co_await ObserveHigherTerm(response.term_);
    co_return;
  }
  const auto transfer = snapshot_transfers_.find(from);
  if (role_ != RaftRole::LEADER || response.term_ != hard_state_.current_term_ || next_index_.count(from) == 0 ||
      transfer == snapshot_transfers_.end() || response.request_id_ != transfer->second.request_id_) {
    co_return;
  }
  if (!response.success_) {
    snapshot_transfers_.erase(transfer);
    co_return;
  }
  if (!response.complete_) {
    if (response.stale_ || response.next_offset_ < transfer->second.end_offset_ ||
        response.next_offset_ >= transfer->second.snapshot_.payload_size_) {
      snapshot_transfers_.erase(transfer);
      co_return;
    }
    co_await SendSnapshot(from, response.next_offset_);
    co_return;
  }
  if (response.match_index_ < transfer->second.snapshot_.last_included_index_) {
    snapshot_transfers_.erase(transfer);
    co_return;
  }
  snapshot_transfers_.erase(transfer);
  match_index_[from] = std::max(match_index_[from], response.match_index_);
  next_index_[from] = std::max(next_index_[from], response.match_index_ + 1);
  if (next_index_[from] <= durable_tip_) {
    co_await SendAppend(from);
  }

  co_return;
}

auto RaftNode::Propose(EntryType type, std::vector<std::byte> payload) -> std::optional<uint64_t> {
  std::vector<ReplicatedLogEntry> entries;
  entries.push_back({1, 0, 0, type, std::move(payload)});
  return ProposeBatch(entries);
}

auto RaftNode::ProposeBatch(std::vector<ReplicatedLogEntry> &entries) -> std::optional<uint64_t> {
  if (!LeaderReady() || Busy()) return std::nullopt;
  if (entries.empty() || entries.size() > config_.append_batch_entries_)
    throw std::invalid_argument("invalid proposal batch size");
  const auto first = durable_tip_ + 1;
  for (size_t n = 0; n < entries.size(); ++n) {
    entries[n].index_ = first + n;
    entries[n].term_ = hard_state_.current_term_;
  }
  Start(RunProposal(std::move(entries)));
  return first;
}

auto RaftNode::TakeProposalError(uint64_t index) -> std::exception_ptr {
  auto found = proposal_errors_.find(index);
  if (found == proposal_errors_.end()) return {};
  auto error = found->second;
  proposal_errors_.erase(found);
  return error;
}

auto RaftNode::RunProposal(std::vector<ReplicatedLogEntry> entries) -> Operation {
  try {
    co_await Slow(
        [&] {
          for (const auto &entry : entries) state_machine_->ValidateProposalPayload(entry.type_, entry.payload_);
          return true;
        },
        true);
  } catch (...) {
    for (const auto &entry : entries) proposal_errors_[entry.index_] = std::current_exception();
    co_return;  // Reject the complete batch before changing storage.
  }
  if (!LeaderReady() || observed_term_ != entries.front().term_) co_return;
  co_await AppendLogDurably(entries);
  match_index_[config_.node_id_] = entries.back().index_;
  next_index_[config_.node_id_] = entries.back().index_ + 1;
  co_await BroadcastAppend();
}

auto RaftNode::StartReadIndex(uint64_t context) -> bool {
  if (!LeaderReady() || Busy() || context == 0 || context <= highest_read_context_) {
    return false;
  }
  highest_read_context_ = context;
  pending_read_indexes_.emplace(context,
                                PendingReadIndex{hard_state_.current_term_, std::set<NodeId>{config_.node_id_}});
  Start(BroadcastReadIndex(context));
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

auto RaftNode::CreateSnapshot() -> bool {
  if (Busy()) return false;
  Start(RunSnapshot());
  return true;
}

auto RaftNode::RunSnapshot() -> Operation {
  if (snapshot_store_ == nullptr || last_applied_ != hard_state_.commit_index_ ||
      published_applied_index_ != last_applied_) {
    throw std::runtime_error("Raft node is not at a stable snapshot boundary");
  }
  const auto index = published_applied_index_;
  const auto term = (co_await Slow([&] { return log_store_->TermAt(index); }));
  if (!term.has_value()) {
    throw std::runtime_error("Raft snapshot term is unavailable before compaction");
  }
  const auto existing = latest_snapshot_;
  if (existing.has_value() && index <= existing->last_included_index_) {
    if (existing->last_included_index_ != index) {
      throw std::runtime_error("Raft state is behind its latest published snapshot");
    }
    co_return;
  }
  try {
    co_await Slow(
        [&] {
          try {
            return snapshot_store_->Capture(index, *term, *state_machine_);
          } catch (...) {
            const auto error = std::current_exception();
            try {
              snapshot_store_->CancelCapture();
            } catch (...) {
            }
            std::rethrow_exception(error);
          }
        },
        true);
    const auto recovery_base = co_await Slow([&] { return snapshot_store_->OldestRetained(); });
    if (!recovery_base.has_value()) {
      throw std::runtime_error("published Raft snapshot has no recovery base");
    }
    if (recovery_base->last_included_index_ > durable_base_) {
      // A session keeps its target across publication, but may not pin an
      // unbounded log tail. Cancel only when the normal recovery floor actually
      // crosses that target; the next attempt selects the current latest.
      for (auto it = snapshot_transfers_.begin(); it != snapshot_transfers_.end();) {
        if (it->second.snapshot_.last_included_index_ < recovery_base->last_included_index_)
          it = snapshot_transfers_.erase(it);
        else
          ++it;
      }
      co_await InstallLogSnapshotBaseDurably(recovery_base->last_included_index_, recovery_base->last_included_term_,
                                             true);
    }
    co_await UpdateLogTip();
    co_return;
  } catch (...) {
    FailStop();
    throw;
  }
}

auto RaftNode::LatestSnapshot() const -> std::optional<RaftSnapshot> { return latest_snapshot_; }

auto RaftNode::ReadSnapshotChunk(const RaftSnapshot &snapshot, uint64_t offset, size_t maximum_size)
    -> std::vector<std::byte> {
  if (snapshot_store_ == nullptr) {
    throw std::runtime_error("Raft node has no SnapshotStore");
  }
  return snapshot_store_->ReadPayloadChunk(snapshot, offset, maximum_size);
}

void RaftNode::Send(NodeId to, RaftMessage message) {
  if (role_ == RaftRole::STOPPED || draining_) return;
  if (std::visit([](const auto &m) { return m.term_; }, message) < observed_term_) return;
  if (role_ == RaftRole::TERM_PERSISTING) throw std::logic_error("send before durable term");
  transport_->Send({config_.node_id_, to, std::move(message), config_.group_id_});
}

auto RaftNode::SendAppend(NodeId peer, std::optional<uint64_t> read_context) -> Operation {
  if (snapshot_transfers_.count(peer) != 0) {
    // Heartbeats retransmit the one in-flight durable chunk with the same
    // request identity. A later heartbeat must not invalidate an ACK that is
    // delayed by the follower's fsync.
    co_await SendSnapshot(peer);
    co_return;
  }
  auto next = next_index_.at(peer);
  if (next <= durable_base_) {
    co_await SendSnapshot(peer);
    co_return;
  }
  const auto prev = next - 1;
  const auto prev_term = (co_await Slow([&] { return log_store_->TermAt(prev); }));
  if (!prev_term.has_value()) {
    throw std::runtime_error("AppendEntries needs a compacted snapshot transfer");
  }
  std::vector<ReplicatedLogEntry> entries;
  if (next <= durable_tip_) {
    entries = co_await Slow([&] {
      return log_store_->Entries(next, durable_tip_, config_.append_batch_entries_, config_.append_batch_bytes_);
    });
  }
  const auto request_id = ++last_request_id_[peer];
  Send(peer, AppendEntriesRequest{hard_state_.current_term_, config_.node_id_, request_id, prev, *prev_term,
                                  std::move(entries), hard_state_.commit_index_, read_context});

  co_return;
}

auto RaftNode::SendSnapshot(NodeId peer, std::optional<uint64_t> acknowledged_offset) -> Operation {
  if (snapshot_store_ == nullptr || !latest_snapshot_.has_value()) {
    throw std::runtime_error("Raft Leader has no published snapshot for a compacted follower");
  }
  auto transfer = snapshot_transfers_.find(peer);
  const auto snapshot = transfer == snapshot_transfers_.end() ? *latest_snapshot_ : transfer->second.snapshot_;
  if (transfer == snapshot_transfers_.end()) {
    const auto request_id = ++last_request_id_[peer];
    auto input = co_await Slow([&] { return snapshot_store_->Input(snapshot); });
    SnapshotTransfer t{snapshot, 0, 0, request_id, std::move(input)};
    t.offer_ = co_await Slow([&] { return snapshot_store_->OfferDelta(snapshot); });
    t.offering_ = true;
    t.offer_id_ = request_id;
    t.offer_deadline_ = now_ms_ + config_.heartbeat_interval_ms_;
    transfer = snapshot_transfers_.insert_or_assign(peer, std::move(t)).first;
  } else if (acknowledged_offset) {
    if (*acknowledged_offset < transfer->second.end_offset_ || *acknowledged_offset >= snapshot.payload_size_)
      throw std::runtime_error("follower acknowledged an invalid Raft snapshot offset");
    transfer->second.offset_ = *acknowledged_offset;
    transfer->second.chunk_.reset();
    transfer->second.request_id_ = ++last_request_id_[peer];
  }
  auto &t = transfer->second;
  if (t.offering_) {
    if (now_ms_ >= t.offer_deadline_) {
      if (t.extended_ && t.offer_) {
        t.extended_ = false;
        t.offer_id_ = ++last_request_id_[peer];
        t.offer_deadline_ = now_ms_ + config_.heartbeat_interval_ms_;
      } else {
        t.offering_ = false;
        t.offer_.reset();
        t.request_id_ = ++last_request_id_[peer];
      }
    }
    if (t.offering_) {
      Send(peer, SnapshotOfferRequest{hard_state_.current_term_, config_.node_id_, t.offer_id_, snapshot,
                                      t.offer_ ? t.offer_->base_ : RaftSnapshot{}, t.extended_});
      co_return;
    }
  }
  if (!t.chunk_) {
    if (!t.work_) {
      // Capture immutable inputs only. No worker accesses Raft or mutable Store state.
      t.work_ = snapshot_tasks_->Submit([snapshot, input = t.input_, delta = t.delta_, offset = t.offset_,
                                         term = hard_state_.current_term_, leader = config_.node_id_,
                                         request = t.request_id_, session = t.offer_id_, compress = t.compression_]() {
        constexpr size_t maximum = 64U * 1024U;
        SnapshotChunk chunk;
        if (delta) {
          chunk = delta->chunk_(offset, maximum);
          chunk.delta_session_ = session;
        } else {
          const auto size = std::min<uint64_t>(maximum, snapshot.payload_size_ - offset);
          chunk = {snapshot.snapshot_id_,
                   snapshot.last_included_index_,
                   snapshot.last_included_term_,
                   offset,
                   snapshot.payload_size_,
                   snapshot.payload_checksum_,
                   offset + size == snapshot.payload_size_,
                   input.Read(offset, size)};
        }
        InstallSnapshotRequest result{term,
                                      leader,
                                      request,
                                      chunk.snapshot_id_,
                                      chunk.last_included_index_,
                                      chunk.last_included_term_,
                                      chunk.offset_,
                                      chunk.total_size_,
                                      chunk.payload_checksum_,
                                      chunk.done_,
                                      std::move(chunk.data_),
                                      chunk.delta_session_,
                                      chunk.reuse_};
        if (compress) CompressSnapshotChunk(&result, session);
        return result;
      });
    }
    co_return;
  }
  Send(peer, *t.chunk_);

  co_return;
}

auto RaftNode::PollSnapshotTasks() -> Operation {
  if (incoming_decode_ && incoming_decode_->work_->Ready()) {
    auto task = std::move(*incoming_decode_);
    incoming_decode_.reset();
    if (role_ == RaftRole::FOLLOWER && hard_state_.current_term_ == task.term_ && incoming_encoding_ &&
        incoming_encoding_->leader_ == task.from_ && incoming_encoding_->session_ == task.encoding_session_ &&
        incoming_encoding_->target_ == task.snapshot_id_) {
      std::shared_ptr<InstallSnapshotRequest> decoded;
      try {
        decoded = task.work_->Take();
      } catch (const std::exception &) {
        // Cleanup is resumed below, outside the exception handler.
        incoming_delta_.reset();
        Send(task.from_,
             InstallSnapshotResponse{hard_state_.current_term_, task.request_id_, false, false, false, 0, 0});
      }
      if (!decoded)
        co_await Slow([&] {
          snapshot_store_->CancelStaged(task.snapshot_id_);
          return true;
        });
      if (decoded)
        co_await Handle(task.from_, *decoded);  // Recheck term, offset, session and publication before Stage.
    }
  }
  for (auto it = snapshot_transfers_.begin(); it != snapshot_transfers_.end();) {
    auto &t = it->second;
    if (!t.work_ || !t.work_->Ready()) {
      ++it;
      continue;
    }
    try {
      t.chunk_ = t.work_->Take();
    } catch (...) {
      // A failed immutable body read is a storage error, not a codec-capability fallback.
      FailStop();
      throw;
    }
    t.work_.reset();
    if (role_ != RaftRole::LEADER || t.chunk_->term_ != hard_state_.current_term_ ||
        t.chunk_->request_id_ != t.request_id_ || t.chunk_->snapshot_id_ != t.snapshot_.snapshot_id_) {
      it = snapshot_transfers_.erase(it);
      continue;
    }
    const auto &c = *t.chunk_;
    t.end_offset_ = c.offset_ + (c.reuse_                               ? c.reuse_->length_
                                 : c.encoding_ == SnapshotEncoding::Lz4 ? c.raw_size_
                                                                        : c.data_.size());
    Send(it->first, c);
    ++it;
  }

  co_return;
}

auto RaftNode::BroadcastAppend() -> Operation {
  for (const auto peer : config_.voters_) {
    if (peer != config_.node_id_) {
      co_await SendAppend(peer);
    }
  }

  co_return;
}

auto RaftNode::BroadcastReadIndex(uint64_t context) -> Operation {
  for (const auto peer : config_.voters_) {
    if (peer != config_.node_id_) {
      co_await SendAppend(peer, context);
    }
  }

  co_return;
}

auto RaftNode::AdvanceLeaderCommit() -> Operation {
  for (uint64_t candidate = durable_tip_; candidate > hard_state_.commit_index_; candidate--) {
    if ((co_await Slow([&] { return log_store_->TermAt(candidate); })) !=
        std::optional<uint64_t>{hard_state_.current_term_}) {
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
    co_await PersistHardState(hard_state_.current_term_, hard_state_.voted_for_, candidate);
    co_await AdvanceLogCommitOrStop(candidate);
    co_await ApplyCommitted();
    co_await BroadcastAppend();
    co_return;
  }

  co_return;
}

auto RaftNode::ApplyCommitted() -> Operation {
  while (last_applied_ < hard_state_.commit_index_) {
    const auto next = last_applied_ + 1;
    const auto entry = co_await Slow([&] { return log_store_->EntryAt(next); });
    if (!entry.has_value()) {
      FailStop();
      throw std::runtime_error("committed Raft entry is unavailable for Apply");
    }
    try {
      co_await Slow(
          [&] {
            state_machine_->Apply(*entry);
            return true;
          },
          true);
    } catch (...) {
      FailStop();
      throw;
    }
    last_applied_ = next;
    published_applied_index_ = next;
  }

  co_return;
}

auto RaftNode::HasMajority(size_t votes) const -> bool { return votes >= config_.voters_.size() / 2 + 1; }

auto RaftNode::CandidateLogIsUpToDate(uint64_t last_term, uint64_t last_index) const -> bool {
  const auto local_term = durable_tip_term_;
  return last_term > local_term || (last_term == local_term && last_index >= durable_tip_);
}

auto RaftNode::FirstIndexOfTerm(uint64_t index, uint64_t term) const -> uint64_t {
  while (index > durable_base_) {
    const auto previous = log_store_->TermAt(index - 1);
    if (!previous.has_value() || *previous != term) {
      break;
    }
    index--;
  }
  return index;
}

auto RaftNode::LastIndexOfTerm(uint64_t term) const -> std::optional<uint64_t> {
  auto index = durable_tip_;
  while (true) {
    const auto value = log_store_->TermAt(index);
    if (value == std::optional<uint64_t>{term}) {
      return index;
    }
    if (index == durable_base_) {
      break;
    }
    index--;
  }
  return std::nullopt;
}

}  // namespace bustub
