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
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>  // NOLINT(build/c++11)
#include <cstring>
#include <iostream>
#include <list>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "common/framed_socket.h"
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
  client_wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (client_wake_fd_ < 0) throw std::runtime_error("cannot open client completion notification");
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
  // Store validation reads are independent of A's private working-space restore.
  // This scoped use of the existing executor drains before any dependent repair.
  std::unique_ptr<TaskExecutor> preparation;
  std::shared_ptr<TaskExecutor::Result<PreparedRaftStores>> stores;
  if (object_storage_) {
    preparation = std::make_unique<TaskExecutor>(1, 1, local_storage_->MemoryBudget(), std::function<void()>{});
    stores = preparation->Submit(4096, true, [objects = object_storage_] { return PrepareRaftStores(objects); });
  }
  if (config_.object_storage_) {
    const auto &deployment = *config_.object_storage_;
    state_machine_ = BusTubRaftStateMachine::OpenObjectPages(
        directory_.get(), storage_, config_.buffer_pool_size_,
        {local_storage_, {deployment.pages_->pages_per_object_, {deployment.space_, 5}}, deployment.pages_->cache_});
  } else {
    state_machine_ = BusTubRaftStateMachine::Open(directory_.get(), storage_, config_.buffer_pool_size_);
  }
  if (preparation) preparation->Drain();
  auto recovered = object_storage_ ? RecoverRaftPersistentState(object_storage_, state_machine_,
                                        stores ? stores->Take() : PrepareRaftStores(object_storage_))
                                   : RecoverRaftPersistentState(directory_->RaftDirectory(), storage_, state_machine_);
  stores.reset();
  preparation.reset();

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
                     [this] { Wake(); },
                     config_.object_storage_ ? config_.object_storage_->raft_.max_batch_entries_ : 128,
                     config_.object_storage_ ? config_.object_storage_->raft_.max_batch_bytes_ : 1024U * 1024U},
      transport_, std::move(recovered.stable_store_), std::move(recovered.log_store_), state_machine_,
      std::move(recovered.snapshot_store_));
}

DistributedNode::~DistributedNode() {
  Stop();
  raft_node_.reset();
  if (client_wake_fd_ >= 0) close(client_wake_fd_);
}

void DistributedNode::NotifyClients() {
  const uint64_t one = 1;
  static_cast<void>(write(client_wake_fd_, &one, sizeof(one)));
}

void DistributedNode::Wake() {
  work_ready_.store(true);
  state_changed_.notify_all();
  NotifyClients();
}

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
      NotifyClients();
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
  Wake();
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
    NotifyClients();
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
    // Requests waiting for a missing session predecessor have no business scope yet.
    // Do not let one of them block the very DDL predecessor it needs.
    for (const auto &[sequence, earlier] : writes_) {
      if (sequence >= id) break;
      if (!earlier->control_ && (earlier->plan_ || earlier->proposal_index_ || earlier->prepared_)) {
        blocker = sequence;
        break;
      }
    }
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
  if (active->control_) {
    control_work_.reset();
  } else {
    auto &requests = clients_.at(active->client_id_);
    requests.erase(active->request_id_);
    if (requests.empty()) clients_.erase(active->client_id_);
  }
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
  // Serializing session creation makes the replicated session-count cap exact,
  // including a batch validated before any of its entries has been applied.
  std::shared_ptr<ActiveWrite> opening;
  for (const auto &active : window)
    if (active->window_ && !active->control_ && active->request_id_ == 1) {
      opening = active;
      break;
    }
  for (const auto &active : window) {
    if (!raft_node_->LeaderReady() || raft_node_->CurrentTerm() != active->proposal_term_) {
      FinishWrite(active, ClientResponseStatus::NOT_LEADER);
      continue;
    }
    if (active->control_) {
      if (active->proposal_index_) {
        if (raft_node_->PublishedAppliedIndex() >= active->proposal_index_)
          FinishWrite(active, ClientResponseStatus::OK);
        else if (auto error = raft_node_->TakeProposalError(active->proposal_index_))
          std::rethrow_exception(error);
        continue;
      }
      if (!active->work_ && !active->prepared_) {
        active->work_ = raft_node_->BusinessTasks().Submit(0, true, [state = state_machine_, active] {
          const auto &control = *active->control_;
          auto [needed, observed] =
              state->InspectSessionControl(control.client_id_, control.acknowledged_through_, control.close_);
          if (!needed)
            return WriteWork{WriteWork::Phase::RESULT, RequestDisposition::NEW_REQUEST, {}, {}, false, observed};
          TransactionCommandBatch batch{3, active->client_id_, active->request_id_, active->request_fingerprint_, 0,
                                        {}};
          batch.session_action_ = control.close_ ? SessionAction::CLOSE : SessionAction::ACK;
          batch.acknowledged_through_ = control.acknowledged_through_;
          return WriteWork{
              WriteWork::Phase::PREPARE, RequestDisposition::NEW_REQUEST, CommandBatchCodec::Encode(batch), {}, false};
        });
      }
      if (active->work_ && active->work_->Ready()) {
        try {
          active->prepared_ = active->work_->Take();
          active->work_.reset();
        } catch (const std::invalid_argument &e) {
          active->work_.reset();
          FinishWrite(active, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
          continue;
        }
      }
      if (active->prepared_ && active->prepared_->phase_ == WriteWork::Phase::RESULT) {
        if (raft_node_->PublishedAppliedIndex() >= active->prepared_->observed_index_)
          FinishWrite(active, ClientResponseStatus::OK);
        continue;
      }
      if (active->prepared_ && !raft_node_->Busy()) {
        std::vector<ReplicatedLogEntry> entries{{1, 0, 0, EntryType::COMMAND_BATCH, active->prepared_->bytes_}};
        const auto first = raft_node_->ProposeBatch(entries);
        if (first) {
          active->proposal_index_ = *first;
          active->prepared_.reset();
        }
      }
      continue;
    }
    if (active->window_ && active->request_id_ == 1 && active != opening) continue;
    // One session prepares against the preceding request's published state.
    if (!active->work_ && !active->plan_ && !active->prepared_ && !active->proposal_index_ &&
        clients_.at(active->client_id_).begin()->first != active->request_id_)
      continue;
    auto finish_result = [&](std::vector<std::byte> bytes) {
      const auto receipt = WriteResponseCodec::Decode(bytes);
      FinishWrite(active,
                  receipt.status_ == WriteStatus::COMMITTED ? ClientResponseStatus::COMMITTED
                                                            : ClientResponseStatus::DURABLE_REJECTED,
                  std::move(bytes));
    };
    auto reject = [&](const std::string &message) {
      TransactionCommandBatch batch{3, active->client_id_, active->request_id_, active->request_fingerprint_, 0, {}};
      batch.session_action_ = SessionAction::REJECT;
      batch.rejection_ = message.substr(0, 512);
      active->prepared_ = WriteWork{
          WriteWork::Phase::PREPARE, RequestDisposition::NEW_REQUEST, CommandBatchCodec::Encode(batch), {}, false};
    };
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
            finish_result(std::move(result.bytes_));
          } else if (active->window_ && (result.disposition_ == RequestDisposition::TOO_OLD ||
                                         result.disposition_ == RequestDisposition::CLOSED)) {
            FinishWrite(active, result.disposition_ == RequestDisposition::CLOSED
                                    ? ClientResponseStatus::SESSION_CLOSED
                                    : ClientResponseStatus::RESULT_EXPIRED);
          } else if (active->proposal_index_ != 0) {
            throw std::runtime_error("published proposal has no session result");
          } else {
            auto status = ClientResponseStatus::REJECTED;
            if (active->window_ && result.disposition_ == RequestDisposition::WINDOW_FULL)
              status = ClientResponseStatus::WINDOW_FULL;
            if (active->window_ && result.disposition_ == RequestDisposition::GAP) status = ClientResponseStatus::GAP;
            FinishWrite(active, status,
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
      } catch (const SqlRequestError &e) {
        active->work_.reset();
        if (active->proposal_index_ != 0) throw;
        if (active->window_)
          reject(e.what());
        else {
          FinishWrite(active, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
          continue;
        }
      } catch (const std::exception &e) {
        active->work_.reset();
        if (active->proposal_index_ != 0 || active->window_) throw;
        FinishWrite(active, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
        continue;
      }
    }
    if (auto error = raft_node_->TakeProposalError(active->proposal_index_)) {
      if (active->window_) std::rethrow_exception(error);
      try {
        std::rethrow_exception(error);
      } catch (const std::exception &e) {
        FinishWrite(active, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
      }
      continue;
    }
    if (active->prepared_ && active->prepared_->disposition_ == RequestDisposition::RETRY_LAST) {
      if (WriteResponseCodec::Decode(active->prepared_->bytes_).commit_index_ <= raft_node_->PublishedAppliedIndex())
        finish_result(std::move(active->prepared_->bytes_));
      continue;
    }
    if (active->work_) continue;
    if (active->proposal_index_ != 0) {
      if (raft_node_->PublishedAppliedIndex() >= active->proposal_index_) {
        if (active->plan_) ReleaseWrite(active);
        active->work_ = raft_node_->BusinessTasks().Submit(0, true, [state = state_machine_, active] {
          auto [disposition, bytes] = state->InspectRequest(active->client_id_, active->request_id_,
                                                            active->request_fingerprint_, active->window_);
          return WriteWork{WriteWork::Phase::RESULT, disposition, std::move(bytes), {}, false};
        });
      }
      continue;
    }
    if (active->prepared_) {
      const auto bytes =
          active->prepared_->bytes_.size() + LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES;
      if (bytes > maximum_bytes) {
        if (active->window_)
          reject("prepared command exceeds log batch capacity");
        else
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
        auto [disposition, bytes] = state->InspectRequest(active->client_id_, active->request_id_,
                                                          active->request_fingerprint_, active->window_);
        WriteWork result{WriteWork::Phase::ANALYZE, disposition, std::move(bytes), {}, false};
        if (disposition == RequestDisposition::NEW_REQUEST)
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
              if (active->window_) batch->format_version_ = 3;
              result.bytes_ = CommandBatchCodec::Encode(*batch);
              if (result.bytes_.size() > active->command_limit_)
                throw SqlRequestError("prepared command exceeds request capacity");
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
  snapshot_draining_ = !raft_node_->SnapshotCaptured() && raft_node_->LeaderReady() &&
                       raft_node_->PublishedAppliedIndex() > boundary &&
                       raft_node_->PublishedAppliedIndex() - boundary >= config_.snapshot_threshold_entries_;
  if (raft_node_->Busy() || raft_node_->SnapshotInProgress()) return;
  const auto commit = raft_node_->CommitIndex();
  const auto applied = raft_node_->LastApplied();
  const auto published = raft_node_->PublishedAppliedIndex();
  const auto latest_snapshot = raft_node_->LatestSnapshot();
  const auto snapshot_index = latest_snapshot.has_value() ? latest_snapshot->last_included_index_ : 0;
  const auto checkpoint_index = state_machine_->PollCheckpoint();
  if (published - snapshot_index < config_.snapshot_threshold_entries_ &&
      published > checkpoint_index && published - checkpoint_index >= config_.snapshot_threshold_entries_) {
    if (published == raft_node_->LastLogIndex())
      state_machine_->RequestCheckpoint(published, raft_node_->LastLogTerm());
  }
  if (writes_.empty() && commit == applied && applied == published && raft_node_->LastLogIndex() == commit &&
      published > snapshot_index && published - snapshot_index >= config_.snapshot_threshold_entries_) {
    static_cast<void>(raft_node_->CreateSnapshot());
  }
}

void DistributedNode::ClientLoop() {
  struct Connection {
    Connection(int fd, std::shared_ptr<ResourceAccount> memory)
        : socket_(fd, ClientProtocolCodec::FRAME_PREFIX_BYTES, std::move(memory)) {}
    FramedSocket socket_;
    std::optional<ClientRequestV1> request_;
    std::optional<RequestState> state_;
    std::optional<ClientResponseV1> response_;
  };
  std::list<Connection> connections;
  while (running_) {
    std::vector<pollfd> descriptors{{client_listen_fd_, POLLIN, 0}, {client_wake_fd_, POLLIN, 0}};
    for (auto &connection : connections)
      descriptors.push_back({connection.socket_.Fd(),
          static_cast<short>(connection.socket_.HasOutput() ? POLLOUT : (!connection.request_ ? POLLIN : 0)), 0});
    const auto ready = poll(descriptors.data(), descriptors.size(), static_cast<int>(config_.tick_interval_ms_));
    if (ready < 0 && errno == EINTR) continue;
    if (ready < 0) { std::lock_guard lock(mutex_); fatal_error_ =
        std::make_exception_ptr(std::runtime_error("client poll failed")); break; }
    if (!running_) break;
    if (descriptors[1].revents & POLLIN) {
      uint64_t count;
      static_cast<void>(read(client_wake_fd_, &count, sizeof(count)));
    }
    size_t position = 2;
    for (auto it = connections.begin(); it != connections.end();) {
      auto &connection = *it;
      const auto events = descriptors[position++].revents;
      bool closed = events & (POLLERR | POLLNVAL);
      // A disconnected caller must not turn an outstanding task into a poll
      // spin. Accepted work keeps its own executor/write-window ownership.
      closed |= (events & POLLHUP) && (connection.request_ || connection.socket_.HasOutput());
      try {
        if (!closed && !connection.request_ && !connection.socket_.HasOutput()) {
          const auto result = connection.socket_.Receive(ClientProtocolCodec::PayloadSizeFromPrefix, 4);
          closed |= result == FramedSocket::Result::Closed;
          if (result == FramedSocket::Result::Ready) {
            connection.request_ = ClientProtocolCodec::DecodeRequest(connection.socket_.Input());
            connection.state_.emplace();
            connection.state_->charge_ = connection.socket_.Consume();
          } else if (connection.socket_.Partial() && std::chrono::steady_clock::now() - connection.socket_.Started() >
                     std::chrono::milliseconds(config_.client_timeout_ms_)) closed = true;
        }
        if (!closed && connection.request_ && !connection.response_ && !connection.socket_.HasOutput())
          connection.response_ = StepRequest(*connection.request_, *connection.state_);
        if (!closed && connection.response_) {
          const auto bytes = 4096 + 3 * connection.response_->payload_.size();
          if (write_memory_->Reserve(bytes, false)) {
            ResourceCharge charge(write_memory_, bytes, false);
            auto output = ClientProtocolCodec::EncodeResponse(*connection.response_);
            charge.ShrinkTo(output.capacity());
            connection.socket_.Reply(std::move(output), std::move(charge));
            connection.response_.reset();
            connection.state_.reset();
          } else if (std::chrono::steady_clock::now() - connection.state_->start_ >
                     std::chrono::milliseconds(config_.client_timeout_ms_)) closed = true;
        }
        if (!closed && connection.socket_.HasOutput()) {
          const auto result = connection.socket_.Send();
          closed |= result == FramedSocket::Result::Closed;
          if (result == FramedSocket::Result::Ready) {
            connection.request_.reset();
          }
          else if (std::chrono::steady_clock::now() - connection.socket_.OutputStarted() >
                   std::chrono::milliseconds(config_.client_timeout_ms_)) closed = true;
        }
      } catch (const std::exception &error) {
        std::clog << "CLIENT_CONNECTION_ERROR " << error.what() << '\n';
        closed = true;
      }
      if (closed) {
        if (connection.state_) {
          std::lock_guard lock(mutex_);
          raft_node_->CancelReadIndex(connection.state_->read_context_);
        }
        it = connections.erase(it);
      } else ++it;
    }
    if (descriptors[0].revents & POLLIN) {
      const auto fd = accept(client_listen_fd_, nullptr, nullptr);
      if (fd >= 0) {
        if (connections.size() < 128) connections.emplace_back(fd, write_memory_);
        else close(fd);
      }
    }
  }
  std::lock_guard lock(mutex_);
  for (auto &connection : connections)
    if (connection.state_) raft_node_->CancelReadIndex(connection.state_->read_context_);
}

auto DistributedNode::HandleRequest(const ClientRequestV1 &request) -> ClientResponseV1 {
  RequestState state;
  for (;;) {
    if (auto response = StepRequest(request, state)) return std::move(*response);
    std::unique_lock lock(mutex_);
    state_changed_.wait_for(lock, std::chrono::milliseconds(config_.tick_interval_ms_));
  }
}

auto DistributedNode::StepRequest(const ClientRequestV1 &request, RequestState &state)
    -> std::optional<ClientResponseV1> {
  if (auto status = std::get_if<ClientStatusRequestV1>(&request)) return HandleStatus(*status);
  std::lock_guard lock(mutex_);
  auto response = std::visit([&](const auto &value) -> std::optional<ClientResponseV1> {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, ClientWriteRequestV1>) return StepWrite(value, state);
    else if constexpr (std::is_same_v<T, ClientSessionRequestV2>) return StepSession(value, state.write_);
    else if constexpr (std::is_same_v<T, ClientReadRequestV1>) return StepRead(value, state);
    else return std::nullopt;
  }, request);
  const auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-state.start_).count();
  const auto id = std::visit([](const auto &value) { return value.request_id_; }, request);
  if (!response && us >= static_cast<int64_t>(config_.client_timeout_ms_ * 1000))
    response = MakeResponse(id, ClientResponseStatus::TIMEOUT);
  if (response && state.read_context_) {
    raft_node_->CancelReadIndex(state.read_context_);
    state.read_context_ = 0;
  }
  return response;
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

auto DistributedNode::StepWrite(const ClientWriteRequestV1 &request, RequestState &state)
    -> std::optional<ClientResponseV1> {
  if (request.window_ && request.acknowledged_through_ && !state.acknowledged_) {
    auto ack = StepSession({request.client_id_, request.request_id_, request.acknowledged_through_, false},
                           state.acknowledgement_);
    if (!ack) return std::nullopt;
    if (ack->status_ != ClientResponseStatus::OK) return ack;
    state.acknowledged_ = true;
    state.acknowledgement_.reset();
  }
  try {
    if (!state.fingerprint_) state.fingerprint_ = ComputeWriteIntentFingerprintV1(request.sql_);
  } catch (const std::exception &e) {
    return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
  }
  const auto &fingerprint = *state.fingerprint_;
  auto &mine = state.write_;
  if (mine && mine->response_) return *mine->response_;
  if (!running_ || fatal_error_) return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
  if (!raft_node_->LeaderReady()) return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
  try {
    ReconcileActiveWrite();
    MaybeCreateSnapshot();
    auto existing = clients_.find(request.client_id_);
    bool can_add = true;
    if (auto control = control_work_.lock();
        control && control->client_id_ == request.client_id_ && control->control_->close_)
      can_add = false;
    if (existing != clients_.end()) {
      auto position = existing->second.find(request.request_id_);
      if (position != existing->second.end()) {
        auto active = writes_.at(position->second);
        if (!(active->request_fingerprint_ == fingerprint) || active->window_ != request.window_)
          return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED,
                              ErrorPayload("request payload does not match request identity"));
        mine = active;
      }
      can_add = can_add && request.window_ && writes_.at(existing->second.begin()->second)->window_ &&
                existing->second.size() < SessionTable::WINDOW_RESULTS;
    }
    if (!mine && can_add && !snapshot_draining_ && writes_.size() < config_.max_write_requests_) {
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
          mine->window_ = request.window_;
          mine->bytes_ = bytes;
          mine->command_limit_ = config_.max_write_command_bytes_;
          mine->charge_ = std::move(charge);
          mine->proposal_term_ = raft_node_->CurrentTerm();
          clients_[request.client_id_].emplace(request.request_id_, mine->sequence_);
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
  return std::nullopt;
}

auto DistributedNode::StepSession(const ClientSessionRequestV2 &request, std::shared_ptr<ActiveWrite> &mine)
    -> std::optional<ClientResponseV1> {
  if (mine && mine->response_) return *mine->response_;
  if (!running_ || fatal_error_) return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
  if (!raft_node_->LeaderReady()) return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
  try {
    ReconcileActiveWrite();
    if (request.close_ && clients_.count(request.client_id_))
      return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED,
                          ErrorPayload("session still has in-flight writes"));
    if (!mine && control_work_.expired() && write_memory_->Reserve(4096, true)) {
      ResourceCharge charge(write_memory_, 4096, true);
      mine = std::make_shared<ActiveWrite>();
      mine->sequence_ = ++next_write_;
      mine->client_id_ = request.client_id_;
      mine->request_id_ = request.request_id_;
      mine->control_ = request;
      mine->window_ = true;
      mine->request_fingerprint_ = ComputeWriteIntentFingerprintV1("session control");
      mine->proposal_term_ = raft_node_->CurrentTerm();
      mine->charge_ = std::move(charge);
      writes_.emplace(mine->sequence_, mine);
      control_work_ = mine;
      ReconcileActiveWrite();
    }
  } catch (...) {
    fatal_error_ = std::current_exception();
    state_changed_.notify_all();
    return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
  }
  if (mine && mine->response_) return *mine->response_;
  return std::nullopt;
}

auto DistributedNode::StepRead(const ClientReadRequestV1 &request, RequestState &state)
    -> std::optional<ClientResponseV1> {
  const bool linear = request.consistency_ == ClientReadConsistency::LINEARIZABLE;
  if (!state.read_started_) {
    state.read_started_ = true;
    state.read_term_ = raft_node_->CurrentTerm();
    state.read_context_ = ++next_read_context_;
    state.read_barrier_ = !linear;
  }
  if (!running_ || fatal_error_) return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
  if (linear && (!raft_node_->LeaderReady() || raft_node_->CurrentTerm() != state.read_term_))
    return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
  if (!state.read_barrier_) {
    if (!state.read_probe_ && !raft_node_->Busy())
      state.read_probe_ = raft_node_->StartReadIndex(state.read_context_);
    if (state.read_probe_) {
      if (auto index = raft_node_->TakeReadIndex(state.read_context_)) {
        state.read_index_ = *index;
        state.read_barrier_ = true;
      }
    }
  }
  if (state.read_barrier_ && raft_node_->PublishedAppliedIndex() >= state.read_index_ && !state.read_) {
    state.read_ = raft_node_->BusinessTasks().Submit(
        request.sql_.size() + ClientQueryResultCodec::MAX_RESULT_BYTES, false,
        [owner = state_machine_, sql = request.sql_, index = state.read_index_] {
          return owner->ExecuteReadSql(sql, index);
        });
  }
  if (state.read_ && state.read_->Ready()) {
    try {
      if (!state.completed_) state.completed_ = state.read_->Take();
      if (raft_node_->PublishedAppliedIndex() >= state.completed_->index_) {
        auto response = MakeResponse(request.request_id_, ClientResponseStatus::OK,
                                     std::move(state.completed_->payload_));
        response.read_timestamp_ = state.completed_->index_;
        return response;
      }
    } catch (const std::exception &error) {
      return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(error.what()));
    }
  }
  return std::nullopt;
}

auto DistributedNode::ClientEndpoint() const -> TcpEndpoint {
  std::lock_guard lock(mutex_);
  return bound_client_endpoint_;
}

auto DistributedNode::RaftEndpoint() const -> TcpEndpoint { return transport_->ListenEndpoint(); }

}  // namespace bustub
