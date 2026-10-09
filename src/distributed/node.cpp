//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// node.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/node.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>  // NOLINT(build/c++11)
#include <cstring>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "distributed/request_fingerprint.h"
#include "raft/persistent_state.h"
#include "recovery/log_codec.h"

namespace bustub {
namespace {

static_assert(CommandBatchCodec::MAX_ENCODED_BATCH_BYTES == LogCodec::MAX_PAYLOAD_BYTES,
              "an encoded CommandBatch must fit one LogCodec payload");

class SocketGuard {
 public:
  explicit SocketGuard(int socket_fd) : socket_fd_(socket_fd) {}
  ~SocketGuard() {
    if (socket_fd_ >= 0) {
      close(socket_fd_);
    }
  }
  SocketGuard(const SocketGuard &) = delete;
  auto operator=(const SocketGuard &) -> SocketGuard & = delete;
  auto Release() -> int {
    const auto result = socket_fd_;
    socket_fd_ = -1;
    return result;
  }

 private:
  int socket_fd_;
};

void SetSocketTimeout(int socket_fd, uint64_t timeout_ms) {
  timeval timeout{static_cast<time_t>(timeout_ms / 1000), static_cast<suseconds_t>((timeout_ms % 1000) * 1000)};
  setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

auto ReadExact(int socket_fd, std::byte *data, size_t size) -> bool {
  size_t offset = 0;
  while (offset < size) {
    const auto count = recv(socket_fd, data + offset, size - offset, 0);
    if (count > 0) {
      offset += static_cast<size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

auto WriteExact(int socket_fd, const std::byte *data, size_t size) -> bool {
  size_t offset = 0;
  while (offset < size) {
    const auto count = send(socket_fd, data + offset, size - offset, MSG_NOSIGNAL);
    if (count > 0) {
      offset += static_cast<size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

auto OpenListener(TcpEndpoint *endpoint) -> int {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_PASSIVE;
  addrinfo *addresses = nullptr;
  const auto port = std::to_string(endpoint->port_);
  const auto status = getaddrinfo(endpoint->host_.c_str(), port.c_str(), &hints, &addresses);
  if (status != 0) {
    throw std::runtime_error("cannot resolve client listen endpoint " + endpoint->ToString() + ": " +
                             gai_strerror(status));
  }
  int listener = -1;
  for (auto *address = addresses; address != nullptr; address = address->ai_next) {
    const auto candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (candidate < 0) {
      continue;
    }
    SocketGuard guard(candidate);
    int reuse = 1;
    setsockopt(candidate, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (bind(candidate, address->ai_addr, address->ai_addrlen) == 0 && listen(candidate, 128) == 0) {
      listener = guard.Release();
      break;
    }
  }
  freeaddrinfo(addresses);
  if (listener < 0) {
    throw std::runtime_error("cannot bind client endpoint " + endpoint->ToString() + ": " + std::strerror(errno));
  }
  if (endpoint->port_ == 0) {
    sockaddr_storage address{};
    socklen_t size = sizeof(address);
    if (getsockname(listener, reinterpret_cast<sockaddr *>(&address), &size) != 0) {
      close(listener);
      throw std::runtime_error("cannot inspect dynamic client listen endpoint");
    }
    endpoint->port_ = address.ss_family == AF_INET ? ntohs(reinterpret_cast<sockaddr_in *>(&address)->sin_port)
                                                   : ntohs(reinterpret_cast<sockaddr_in6 *>(&address)->sin6_port);
  }
  return listener;
}

auto ErrorPayload(const std::string &message) -> std::vector<std::byte> {
  const auto *begin = reinterpret_cast<const std::byte *>(message.data());
  return {begin, begin + message.size()};
}

}  // namespace

void DistributedNodeConfig::Validate() const {
  if (object_storage_ && (!object_storage_->storage_.objects_ || !object_storage_->storage_.transactions_ ||
                          !object_storage_->pages_ || object_storage_->pages_->pages_per_object_ == 0)) {
    throw std::runtime_error("Raft object deployment requires object IO, transactions and explicit page configuration");
  }
  if (node_id_ == 0 || group_id_.empty() || group_id_.size() > 128 || data_directory_.empty() ||
      raft_listen_.host_.empty() || client_listen_.host_.empty() || peers_.size() != 2 ||
      election_timeout_min_ms_ == 0 || election_timeout_min_ms_ >= election_timeout_max_ms_ ||
      heartbeat_interval_ms_ == 0 || heartbeat_interval_ms_ > (election_timeout_min_ms_ - 1) / 2 ||
      tick_interval_ms_ == 0 || tick_interval_ms_ > heartbeat_interval_ms_ || client_timeout_ms_ == 0 ||
      buffer_pool_size_ == 0 || snapshot_threshold_entries_ == 0 || max_write_requests_ == 0 ||
      max_write_command_bytes_ == 0 || max_write_command_bytes_ > CommandBatchCodec::MAX_ENCODED_BATCH_BYTES ||
      max_write_bytes_ < 4 * max_write_command_bytes_) {
    throw std::runtime_error("invalid distributed node configuration");
  }
  std::set<std::string> raft_addresses{raft_listen_.ToString()};
  std::set<std::string> client_addresses{client_listen_.ToString()};
  for (const auto &[peer_id, peer] : peers_) {
    if (peer_id == 0 || peer_id == node_id_ || peer.raft_endpoint_.host_.empty() || peer.raft_endpoint_.port_ == 0 ||
        peer.client_endpoint_.host_.empty() || peer.client_endpoint_.port_ == 0 ||
        !raft_addresses.insert(peer.raft_endpoint_.ToString()).second ||
        !client_addresses.insert(peer.client_endpoint_.ToString()).second) {
      throw std::runtime_error("invalid or duplicate distributed peer configuration");
    }
  }
}

auto DistributedNode::Open(DistributedNodeConfig config, std::shared_ptr<DurableStorage> storage)
    -> std::unique_ptr<DistributedNode> {
  config.Validate();
  if (storage == nullptr) {
    storage = std::make_shared<PosixDurableStorage>();
  }
  auto result = std::unique_ptr<DistributedNode>(new DistributedNode(std::move(config), std::move(storage)));
  result->Initialize();
  return result;
}

DistributedNode::DistributedNode(DistributedNodeConfig config, std::shared_ptr<DurableStorage> storage)
    : config_(std::move(config)), storage_(std::move(storage)), bound_client_endpoint_(config_.client_listen_) {}

void DistributedNode::Initialize() {
  directory_ = NodeDirectory::Open(config_.data_directory_, storage_);
  std::vector<NodeId> voters{config_.node_id_};
  for (const auto &[peer_id, peer] : config_.peers_) {
    static_cast<void>(peer);
    voters.push_back(peer_id);
  }
  std::sort(voters.begin(), voters.end());
  directory_->EnsureIdentity(config_.node_id_, config_.group_id_, voters);
  if (config_.object_storage_) {
    const auto &deployment = *config_.object_storage_;
    local_storage_ = std::make_shared<NodeStorage>(deployment.storage_);
    local_storage_->Open();
    object_storage_ = RaftObjectStorage::Open(local_storage_, deployment.space_, deployment.raft_);
    object_storage_->EnsureIdentity(config_.node_id_, config_.group_id_, voters);
  }
  if (config_.object_storage_) {
    const auto &deployment = *config_.object_storage_;
    state_machine_ = BusTubRaftStateMachine::OpenObjectPages(
        directory_.get(), storage_, config_.buffer_pool_size_,
        {local_storage_, {deployment.pages_->pages_per_object_, {deployment.space_, 5}}, deployment.pages_->cache_});
  } else {
    state_machine_ = BusTubRaftStateMachine::Open(directory_.get(), storage_, config_.buffer_pool_size_);
  }
  auto recovered = object_storage_ ? RecoverRaftPersistentState(object_storage_, state_machine_)
                                   : RecoverRaftPersistentState(directory_->RaftDirectory(), storage_, state_machine_);

  std::map<NodeId, TcpEndpoint> raft_peers;
  for (const auto &[peer_id, peer] : config_.peers_) {
    raft_peers.emplace(peer_id, peer.raft_endpoint_);
  }
  transport_ = std::make_shared<TcpRaftTransport>(config_.node_id_, config_.group_id_, config_.raft_listen_,
                                                  std::move(raft_peers), 250, 10000,
                                                  local_storage_ ? local_storage_->MemoryBudget() : nullptr);
  write_memory_ = ResourceAccount::Create(local_storage_ ? local_storage_->MemoryBudget() : nullptr);
  raft_node_ = std::make_unique<RaftNode>(
      RaftNodeConfig{config_.node_id_, std::move(voters), config_.election_timeout_min_ms_,
                     config_.election_timeout_max_ms_, config_.heartbeat_interval_ms_, config_.group_id_,
                     MakeRandomElectionTimeoutSource(), local_storage_ ? local_storage_->MemoryBudget() : nullptr,
                     [this] {
                       work_ready_.store(true);
                       state_changed_.notify_all();
                     },
                     config_.object_storage_ ? config_.object_storage_->raft_.max_batch_entries_ : 128,
                     config_.object_storage_ ? config_.object_storage_->raft_.max_batch_bytes_ : 1024U * 1024U},
      transport_, std::move(recovered.stable_store_), std::move(recovered.log_store_), state_machine_,
      std::move(recovered.snapshot_store_));
}

DistributedNode::~DistributedNode() { Stop(); }

void DistributedNode::Start() {
  std::unique_lock lock(mutex_);
  if (running_) {
    throw std::runtime_error("distributed node is already running");
  }
  bound_client_endpoint_ = config_.client_listen_;
  client_listen_fd_ = OpenListener(&bound_client_endpoint_);
  try {
    transport_->Start([this](const RaftEnvelope &envelope) {
      std::lock_guard node_lock(mutex_);
      if (!running_ || fatal_error_ != nullptr) {
        return;
      }
      try {
        raft_node_->Receive(envelope.from_, envelope.message_);
        ReconcileActiveWrite();
      } catch (...) {
        fatal_error_ = std::current_exception();
      }
      state_changed_.notify_all();
    });
    running_ = true;
    tick_thread_ = std::thread([this] { TickLoop(); });
    client_thread_ = std::thread([this] { ClientLoop(); });
    if (object_storage_) {
      std::weak_ptr<RaftObjectStorage> owner = object_storage_;
      local_storage_->SetStoreMaintenance([owner] {
        if (auto store = owner.lock()) store->Collect(1);
      });
    }
  } catch (...) {
    const auto error = std::current_exception();
    const bool started = running_;
    lock.unlock();
    if (started) {
      Stop();
    } else {
      close(client_listen_fd_);
      client_listen_fd_ = -1;
    }
    std::rethrow_exception(error);
  }
}

void DistributedNode::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  state_changed_.notify_all();
  if (client_listen_fd_ >= 0) {
    shutdown(client_listen_fd_, SHUT_RDWR);
  }
  if (tick_thread_.joinable()) {
    tick_thread_.join();
  }
  if (client_thread_.joinable()) {
    client_thread_.join();
  }
  transport_->Stop();
  if (client_listen_fd_ >= 0) {
    close(client_listen_fd_);
    client_listen_fd_ = -1;
  }
  std::lock_guard workers_lock(client_workers_mutex_);
  for (auto &worker : client_workers_) {
    if (worker.thread_.joinable()) {
      worker.thread_.join();
    }
  }
  client_workers_.clear();
  try {
    raft_node_->Drain();
    if (local_storage_) local_storage_->SetStoreMaintenance({});
    state_machine_->DrainCheckpoint();
  } catch (...) {
    std::lock_guard lock(mutex_);
    fatal_error_ = std::current_exception();
  }
}

void DistributedNode::TickLoop() {
  auto last_tick = std::chrono::steady_clock::now();
  while (running_) {
    std::unique_lock lock(mutex_);
    state_changed_.wait_for(lock, std::chrono::milliseconds(config_.tick_interval_ms_),
                            [&] { return !running_ || work_ready_.exchange(false); });
    if (!running_) break;
    if (fatal_error_ == nullptr) {
      try {
        // Keep Raft's logical clock monotonic across Stop()/Start() on the same
        // production assembly. Wall-clock epochs restart; this counter does not.
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick).count();
        logical_now_ms_ += elapsed;
        last_tick += std::chrono::milliseconds(elapsed);
        if (local_storage_) {
          const auto state = local_storage_->State();
          if (state.error_) std::rethrow_exception(state.error_);
          if (state.object_error_) std::rethrow_exception(state.object_error_);
        }
        raft_node_->Tick(logical_now_ms_);
        ReconcileActiveWrite();
        MaybeCreateSnapshot();
      } catch (...) {
        fatal_error_ = std::current_exception();
      }
    }
    state_changed_.notify_all();
  }
}

auto DistributedNode::AcquireWrite(const std::shared_ptr<ActiveWrite> &active) -> bool {
  if (active->claimed_) return true;
  if (active->blocker_ != 0) return false;
  const auto id = active->sequence_;
  const auto &plan = *active->plan_;
  uint64_t blocker = 0;
  if (!catalog_writes_.empty() && *catalog_writes_.begin() < id) blocker = *catalog_writes_.begin();
  if (plan.scope_ == SqlWritePlan::Scope::CATALOG) {
    // DDL waits for every earlier request, including those still analyzing.
    if (writes_.begin()->first != id) blocker = writes_.begin()->first;
    if (blocker == 0) {
      // Later requests already holding keys must also publish before catalog changes.
      for (const auto &[table, state] : table_writes_)
        if (!state.active_.empty()) {
          blocker = *state.active_.begin();
          break;
        }
    }
  } else {
    auto &table = table_writes_.at(plan.table_);
    if (!table.wide_.empty() && *table.wide_.begin() < id) blocker = *table.wide_.begin();
    if (blocker == 0 && plan.scope_ == SqlWritePlan::Scope::TABLE && !table.active_.empty())
      blocker = *table.active_.begin();
    if (blocker == 0 && table.wide_owner_ != 0) blocker = table.wide_owner_;
    if (blocker == 0)
      for (const auto &key : plan.keys_) {
        auto owner = table.keys_.find(key);
        if (owner != table.keys_.end()) {
          blocker = owner->second;
          break;
        }
      }
    if (blocker == 0) {
      table.active_.insert(id);
      if (plan.scope_ == SqlWritePlan::Scope::TABLE) table.wide_owner_ = id;
      for (const auto &key : plan.keys_) table.keys_.emplace(key, id);
    }
  }
  if (blocker != 0) {
    active->blocker_ = blocker;
    writes_.at(blocker)->waiters_.insert(id);
    return false;
  }
  active->claimed_ = true;
  return true;
}

void DistributedNode::ReleaseWrite(const std::shared_ptr<ActiveWrite> &active) {
  const auto id = active->sequence_;
  if (active->blocker_ != 0) {
    auto blocker = writes_.find(active->blocker_);
    if (blocker != writes_.end()) blocker->second->waiters_.erase(id);
    active->blocker_ = 0;
  }
  if (active->plan_) {
    const auto &plan = *active->plan_;
    if (plan.scope_ == SqlWritePlan::Scope::CATALOG)
      catalog_writes_.erase(id);
    else {
      auto &table = table_writes_.at(plan.table_);
      table.wide_.erase(id);
      if (active->claimed_) {
        table.active_.erase(id);
        if (table.wide_owner_ == id) table.wide_owner_ = 0;
        for (const auto &key : plan.keys_) table.keys_.erase(key);
      }
      if (--table.plans_ == 0) table_writes_.erase(plan.table_);
    }
  }
  active->claimed_ = false;
  active->plan_.reset();
  for (auto waiter : active->waiters_) {
    auto found = writes_.find(waiter);
    if (found != writes_.end()) found->second->blocker_ = 0;
  }
  active->waiters_.clear();
}

void DistributedNode::FinishWrite(const std::shared_ptr<ActiveWrite> &active, ClientResponseStatus status,
                                  std::vector<std::byte> bytes) {
  ReleaseWrite(active);
  active->response_ = MakeResponse(active->request_id_, status, std::move(bytes));
  clients_.erase(active->client_id_);
  write_bytes_ -= active->bytes_;
  writes_.erase(active->sequence_);
  // An accepted worker retains active/charge until its actual work ends.
  state_changed_.notify_all();
}

void DistributedNode::ReconcileActiveWrite() {
  // Bounded traversal consumes completion slots, not a pairwise conflict search.
  std::vector<std::shared_ptr<ActiveWrite>> window;
  for (const auto &[id, active] : writes_) window.push_back(active);
  std::vector<std::shared_ptr<ActiveWrite>> ready;
  const size_t maximum_entries = config_.object_storage_ ? config_.object_storage_->raft_.max_batch_entries_ : 128;
  const size_t maximum_bytes =
      config_.object_storage_ ? config_.object_storage_->raft_.max_batch_bytes_ : 1024U * 1024U;
  size_t batch_bytes = 0;
  for (const auto &active : window) {
    if (!raft_node_->LeaderReady() || raft_node_->CurrentTerm() != active->proposal_term_) {
      FinishWrite(active, ClientResponseStatus::NOT_LEADER);
      continue;
    }
    if (active->work_ && active->work_->Ready()) {
      try {
        auto result = active->work_->Take();
        active->work_.reset();
        if (result.disposition_ != RequestDisposition::NEW_REQUEST || result.phase_ == WriteWork::Phase::RESULT) {
          if (result.disposition_ == RequestDisposition::RETRY_LAST) {
            if (WriteResponseCodec::Decode(result.bytes_).commit_index_ > raft_node_->PublishedAppliedIndex()) {
              active->prepared_ = std::move(result);
              continue;
            }
            FinishWrite(active, ClientResponseStatus::COMMITTED, std::move(result.bytes_));
          } else if (active->proposal_index_ != 0) {
            throw std::runtime_error("published proposal has no session result");
          } else {
            FinishWrite(active, ClientResponseStatus::REJECTED,
                        ErrorPayload(result.disposition_ == RequestDisposition::PAYLOAD_MISMATCH
                                         ? "request payload does not match request identity"
                                         : "SQL request id is old or contains a session sequence gap"));
          }
          continue;
        }
        if (result.phase_ == WriteWork::Phase::ANALYZE) {
          active->plan_ = std::make_shared<SqlWritePlan>(std::move(*result.plan_));
          if (active->plan_->scope_ == SqlWritePlan::Scope::CATALOG)
            catalog_writes_.insert(active->sequence_);
          else {
            auto &table = table_writes_[active->plan_->table_];
            ++table.plans_;
            if (active->plan_->scope_ == SqlWritePlan::Scope::TABLE) table.wide_.insert(active->sequence_);
          }
        } else if (result.stale_) {
          ReleaseWrite(active);  // next turn rebinds against the new workspace/schema
        } else {
          active->prepared_ = std::move(result);
        }
      } catch (const std::exception &e) {
        active->work_.reset();
        if (active->proposal_index_ != 0) throw;
        FinishWrite(active, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
        continue;
      }
    }
    if (auto error = raft_node_->TakeProposalError(active->proposal_index_)) {
      try {
        std::rethrow_exception(error);
      } catch (const std::exception &e) {
        FinishWrite(active, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
      }
      continue;
    }
    if (active->prepared_ && active->prepared_->disposition_ == RequestDisposition::RETRY_LAST) {
      if (WriteResponseCodec::Decode(active->prepared_->bytes_).commit_index_ <= raft_node_->PublishedAppliedIndex())
        FinishWrite(active, ClientResponseStatus::COMMITTED, std::move(active->prepared_->bytes_));
      continue;
    }
    if (active->work_) continue;
    if (active->proposal_index_ != 0) {
      if (raft_node_->PublishedAppliedIndex() >= active->proposal_index_) {
        if (active->plan_) ReleaseWrite(active);
        active->work_ = raft_node_->BusinessTasks().Submit(0, true, [state = state_machine_, active] {
          auto disposition =
              state->ClassifyRequest(active->client_id_, active->request_id_, active->request_fingerprint_);
          return WriteWork{WriteWork::Phase::RESULT,
                           disposition,
                           state->GetLastResponse(active->client_id_).value_or(std::vector<std::byte>{}),
                           {},
                           false};
        });
      }
      continue;
    }
    if (active->prepared_) {
      const auto bytes =
          active->prepared_->bytes_.size() + LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES;
      if (bytes > maximum_bytes) {
        FinishWrite(active, ClientResponseStatus::REJECTED,
                    ErrorPayload("prepared command exceeds log batch capacity"));
      } else if (ready.size() < maximum_entries && bytes <= maximum_bytes - batch_bytes) {
        ready.push_back(active);
        batch_bytes += bytes;
      }
      continue;
    }
    if (!active->plan_) {
      if (active->blocker_ != 0) continue;
      if (!catalog_writes_.empty() && *catalog_writes_.begin() < active->sequence_) {
        active->blocker_ = *catalog_writes_.begin();
        writes_.at(active->blocker_)->waiters_.insert(active->sequence_);
        continue;
      }
      // Publish the previous analysis' scope before binding the next statement.
      // In particular INSERT after a queued CREATE must not bind a missing table.
      if (analysis_work_.lock()) continue;
      active->work_ = raft_node_->BusinessTasks().Submit(0, false, [state = state_machine_, active] {
        const auto disposition =
            state->ClassifyRequest(active->client_id_, active->request_id_, active->request_fingerprint_);
        WriteWork result{WriteWork::Phase::ANALYZE, disposition, {}, {}, false};
        if (disposition == RequestDisposition::RETRY_LAST)
          result.bytes_ = *state->GetLastResponse(active->client_id_);
        else if (disposition == RequestDisposition::NEW_REQUEST)
          result.plan_ =
              state->AnalyzeSql(active->sql_, active->client_id_, active->request_id_, active->request_fingerprint_);
        return result;
      });
      analysis_work_ = active->work_;
    } else if (AcquireWrite(active)) {
      active->work_ =
          raft_node_->BusinessTasks().Submit(0, false, [state = state_machine_, active, plan = active->plan_] {
            auto batch = state->PrepareSql(*plan, active->client_id_, active->request_id_, active->request_fingerprint_,
                                           active->command_limit_);
            WriteWork result{WriteWork::Phase::PREPARE, RequestDisposition::NEW_REQUEST, {}, {}, !batch.has_value()};
            if (batch) {
              result.bytes_ = CommandBatchCodec::Encode(*batch);
              if (result.bytes_.size() > active->command_limit_)
                throw std::runtime_error("prepared command exceeds request capacity");
            }
            return result;
          });
    }
  }
  if (!ready.empty() && !raft_node_->Busy()) {
    std::vector<ReplicatedLogEntry> entries;
    for (const auto &active : ready)
      entries.push_back({1, 0, 0, EntryType::COMMAND_BATCH, std::move(active->prepared_->bytes_)});
    const auto first = raft_node_->ProposeBatch(entries);
    if (!first) throw std::logic_error("proposal admission changed within protocol owner");
    for (size_t n = 0; n < ready.size(); ++n) {
      ready[n]->proposal_index_ = *first + n;
      ready[n]->prepared_.reset();
    }
  }
}

void DistributedNode::MaybeCreateSnapshot() {
  const auto latest = raft_node_->LatestSnapshot();
  const auto boundary = latest ? latest->last_included_index_ : 0;
  snapshot_draining_ = raft_node_->LeaderReady() && raft_node_->PublishedAppliedIndex() > boundary &&
                       raft_node_->PublishedAppliedIndex() - boundary >= config_.snapshot_threshold_entries_;
  if (raft_node_->Busy()) return;
  const auto commit = raft_node_->CommitIndex();
  const auto applied = raft_node_->LastApplied();
  const auto published = raft_node_->PublishedAppliedIndex();
  const auto latest_snapshot = raft_node_->LatestSnapshot();
  const auto snapshot_index = latest_snapshot.has_value() ? latest_snapshot->last_included_index_ : 0;
  const auto checkpoint_index = state_machine_->PollCheckpoint();
  if (published > checkpoint_index && published - checkpoint_index >= config_.snapshot_threshold_entries_) {
    if (published == raft_node_->LastLogIndex())
      state_machine_->RequestCheckpoint(published, raft_node_->LastLogTerm());
  }
  if (writes_.empty() && commit == applied && applied == published && raft_node_->LastLogIndex() == commit &&
      published > snapshot_index && published - snapshot_index >= config_.snapshot_threshold_entries_) {
    static_cast<void>(raft_node_->CreateSnapshot());
  }
}

void DistributedNode::ClientLoop() {
  while (running_) {
    ReapClientWorkers();
    pollfd descriptor{client_listen_fd_, POLLIN, 0};
    const auto status = poll(&descriptor, 1, 100);
    if (status < 0 && errno == EINTR) {
      continue;
    }
    if (status <= 0 || !running_) {
      continue;
    }
    const auto connection = accept(client_listen_fd_, nullptr, nullptr);
    if (connection < 0) {
      continue;
    }
    auto finished = std::make_shared<std::atomic<bool>>(false);
    std::thread worker([this, connection, finished] {
      HandleConnection(connection);
      finished->store(true, std::memory_order_release);
    });
    std::lock_guard workers_lock(client_workers_mutex_);
    client_workers_.push_back({std::move(worker), std::move(finished)});
  }
  ReapClientWorkers();
}

void DistributedNode::ReapClientWorkers() {
  std::lock_guard workers_lock(client_workers_mutex_);
  auto worker = client_workers_.begin();
  while (worker != client_workers_.end()) {
    if (!worker->finished_->load(std::memory_order_acquire)) {
      ++worker;
      continue;
    }
    if (worker->thread_.joinable()) {
      worker->thread_.join();
    }
    worker = client_workers_.erase(worker);
  }
}

void DistributedNode::HandleConnection(int socket_fd) {
  SocketGuard guard(socket_fd);
  SetSocketTimeout(socket_fd, config_.client_timeout_ms_);
  try {
    std::vector<std::byte> prefix(ClientProtocolCodec::FRAME_PREFIX_BYTES);
    if (!ReadExact(socket_fd, prefix.data(), prefix.size())) {
      return;
    }
    const auto payload_size = ClientProtocolCodec::PayloadSizeFromPrefix(prefix);
    std::vector<std::byte> frame = prefix;
    frame.resize(prefix.size() + payload_size + sizeof(uint32_t));
    if (!ReadExact(socket_fd, frame.data() + prefix.size(), payload_size + sizeof(uint32_t))) {
      return;
    }
    const auto response = ClientProtocolCodec::EncodeResponse(HandleRequest(ClientProtocolCodec::DecodeRequest(frame)));
    static_cast<void>(WriteExact(socket_fd, response.data(), response.size()));
  } catch (const std::exception &) {
    return;
  }
}

auto DistributedNode::HandleRequest(const ClientRequestV1 &request) -> ClientResponseV1 {
  return std::visit(
      [&](const auto &value) -> ClientResponseV1 {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ClientWriteRequestV1>) {
          return HandleWrite(value);
        } else if constexpr (std::is_same_v<T, ClientReadRequestV1>) {
          return HandleRead(value);
        } else {
          return HandleStatus(value);
        }
      },
      request);
}

auto DistributedNode::MakeResponse(uint64_t request_id, ClientResponseStatus status,
                                   std::vector<std::byte> payload) const -> ClientResponseV1 {
  auto leader = raft_node_->LeaderId();
  std::string leader_address;
  if (leader == config_.node_id_) {
    leader_address = bound_client_endpoint_.ToString();
  } else if (leader.has_value()) {
    const auto peer = config_.peers_.find(*leader);
    if (peer != config_.peers_.end()) {
      leader_address = peer->second.client_endpoint_.ToString();
    }
  }
  if (status == ClientResponseStatus::UNAVAILABLE && payload.empty() && fatal_error_) {
    try {
      std::rethrow_exception(fatal_error_);
    } catch (const std::exception &error) {
      payload = ErrorPayload(error.what());
    } catch (...) {
      payload = ErrorPayload("node stopped after a non-standard exception");
    }
  }
  return {request_id,
          status,
          config_.node_id_,
          raft_node_->LeaderReady(),
          leader,
          std::move(leader_address),
          raft_node_->CurrentTerm(),
          raft_node_->CommitIndex(),
          raft_node_->LastApplied(),
          raft_node_->PublishedAppliedIndex(),
          raft_node_->SnapshotBaseIndex(),
          std::nullopt,
          std::move(payload)};
}

auto DistributedNode::HandleStatus(const ClientStatusRequestV1 &request) -> ClientResponseV1 {
  ClientResponseV1 response;
  std::shared_ptr<NodeStorage> storage;
  {
    std::lock_guard lock(mutex_);
    response = MakeResponse(request.request_id_,
                            fatal_error_ == nullptr ? ClientResponseStatus::OK : ClientResponseStatus::UNAVAILABLE);
    storage = local_storage_;
  }
  if (storage && request.storage_ && response.status_ == ClientResponseStatus::OK) {
    try {
      const auto usage = storage->Usage();
      const auto state = storage->State();
      const auto &d = usage.data_;
      const auto &m = usage.metadata_;
      std::ostringstream json;
      json << "{\"storage_version\":1,\"data_capacity_bytes\":" << d.capacity_bytes_
           << ",\"data_committed_bytes\":" << d.committed_bytes_ << ",\"data_reserved_bytes\":" << d.reserved_bytes_
           << ",\"data_quarantined_bytes\":" << d.quarantined_bytes_ << ",\"data_free_bytes\":" << d.free_bytes_
           << ",\"metadata_descriptors\":" << m.descriptors_ << ",\"metadata_live_bodies\":" << m.live_bodies_
           << ",\"metadata_loads\":" << m.loads_ << ",\"metadata_evictions\":" << m.evictions_
           << ",\"scrub_data_bytes\":" << state.integrity_.data_bytes_
           << ",\"scrub_journal_bytes\":" << state.integrity_.journal_bytes_
           << ",\"scrub_passes\":" << state.integrity_.passes_ << ",\"scrub_yielded\":" << state.integrity_.yielded_
           << "}";
      response.payload_ = ErrorPayload(json.str());
    } catch (const std::exception &e) {
      response.status_ = ClientResponseStatus::UNAVAILABLE;
      response.payload_ = ErrorPayload(e.what());
    }
  }
  return response;
}

auto DistributedNode::HandleWrite(const ClientWriteRequestV1 &request) -> ClientResponseV1 {
  RequestFingerprintV1 fingerprint;
  try {
    fingerprint = ComputeWriteIntentFingerprintV1(request.sql_);
  } catch (const std::exception &e) {
    std::lock_guard lock(mutex_);
    return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
  }
  std::unique_lock lock(mutex_);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_timeout_ms_);
  std::shared_ptr<ActiveWrite> mine;
  for (;;) {
    if (mine && mine->response_) return *mine->response_;
    if (!running_ || fatal_error_) return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
    if (!raft_node_->LeaderReady()) return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
    try {
      ReconcileActiveWrite();
      MaybeCreateSnapshot();
      auto existing = clients_.find(request.client_id_);
      if (existing != clients_.end()) {
        auto active = writes_.at(existing->second);
        if (active->request_id_ == request.request_id_) {
          if (!(active->request_fingerprint_ == fingerprint))
            return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED,
                                ErrorPayload("request payload does not match request identity"));
          mine = active;
        }
      } else if (!mine && !snapshot_draining_ && writes_.size() < config_.max_write_requests_) {
        const auto bytes = 4 * config_.max_write_command_bytes_ + request.sql_.size() * 4;
        if (bytes > config_.max_write_bytes_)
          return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED,
                              ErrorPayload("SQL exceeds write window capacity"));
        if (bytes <= config_.max_write_bytes_ - write_bytes_) {
          bool reserved;
          try {
            reserved = write_memory_->Reserve(bytes, false);
          } catch (const std::invalid_argument &e) {
            return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
          }
          if (reserved) {
            ResourceCharge charge(write_memory_, bytes, false);
            mine = std::make_shared<ActiveWrite>();
            mine->sequence_ = ++next_write_;
            mine->client_id_ = request.client_id_;
            mine->request_id_ = request.request_id_;
            mine->request_fingerprint_ = fingerprint;
            mine->sql_ = request.sql_;
            mine->bytes_ = bytes;
            mine->command_limit_ = config_.max_write_command_bytes_;
            mine->charge_ = std::move(charge);
            mine->proposal_term_ = raft_node_->CurrentTerm();
            clients_.emplace(request.client_id_, mine->sequence_);
            writes_.emplace(mine->sequence_, mine);
            write_bytes_ += bytes;
            ReconcileActiveWrite();
          }
        }
      }
    } catch (...) {
      fatal_error_ = std::current_exception();
      state_changed_.notify_all();
      return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
    }
    if (mine && mine->response_) return *mine->response_;
    if (state_changed_.wait_until(lock, deadline) == std::cv_status::timeout)
      return MakeResponse(request.request_id_, ClientResponseStatus::TIMEOUT);
  }
}

auto DistributedNode::HandleRead(const ClientReadRequestV1 &request) -> ClientResponseV1 {
  std::unique_lock lock(mutex_);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_timeout_ms_);
  const auto term = raft_node_->CurrentTerm();
  const auto linear = request.consistency_ == ClientReadConsistency::LINEARIZABLE;
  const auto context = ++next_read_context_;
  uint64_t read_index = 0;
  bool probe = false, barrier = !linear;
  std::shared_ptr<TaskExecutor::Result<PublishedSqlRead>> work;
  std::optional<PublishedSqlRead> completed;
  for (;;) {
    if (!running_ || fatal_error_) {
      raft_node_->CancelReadIndex(context);
      return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
    }
    if (linear && (!raft_node_->LeaderReady() || raft_node_->CurrentTerm() != term)) {
      raft_node_->CancelReadIndex(context);
      return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
    }
    if (!barrier) {
      if (!probe && !raft_node_->Busy()) probe = raft_node_->StartReadIndex(context);
      if (probe)
        if (auto index = raft_node_->TakeReadIndex(context)) {
          read_index = *index;
          barrier = true;
        }
    }
    if (barrier && raft_node_->PublishedAppliedIndex() >= read_index && !work) {
      work = raft_node_->BusinessTasks().Submit(
          request.sql_.size() + ClientQueryResultCodec::MAX_RESULT_BYTES, false,
          [state = state_machine_, sql = request.sql_, read_index] { return state->ExecuteReadSql(sql, read_index); });
    }
    if (work && work->Ready()) {
      try {
        if (!completed) completed = work->Take();
        if (raft_node_->PublishedAppliedIndex() >= completed->index_) {
          auto response = MakeResponse(request.request_id_, ClientResponseStatus::OK, std::move(completed->payload_));
          response.read_timestamp_ = completed->index_;
          return response;
        }
      } catch (const std::exception &e) {
        return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
      }
    }
    if (state_changed_.wait_until(lock, deadline) == std::cv_status::timeout) {
      raft_node_->CancelReadIndex(context);
      return MakeResponse(request.request_id_, ClientResponseStatus::TIMEOUT);
    }
  }
}

auto DistributedNode::ClientEndpoint() const -> TcpEndpoint {
  std::lock_guard lock(mutex_);
  return bound_client_endpoint_;
}

auto DistributedNode::RaftEndpoint() const -> TcpEndpoint { return transport_->ListenEndpoint(); }

}  // namespace bustub
