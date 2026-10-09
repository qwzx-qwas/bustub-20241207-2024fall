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
      buffer_pool_size_ == 0 || snapshot_threshold_entries_ == 0) {
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
  raft_node_ = std::make_unique<RaftNode>(
      RaftNodeConfig{config_.node_id_, std::move(voters), config_.election_timeout_min_ms_,
                     config_.election_timeout_max_ms_, config_.heartbeat_interval_ms_, config_.group_id_,
                     MakeRandomElectionTimeoutSource(), local_storage_ ? local_storage_->MemoryBudget() : nullptr,
                     [this] {
                       work_ready_.store(true);
                       state_changed_.notify_all();
                     }},
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

void DistributedNode::ReconcileActiveWrite() {
  if (!active_write_) return;
  auto active = active_write_;
  auto finish = [&](ClientResponseStatus status, std::vector<std::byte> bytes = {}) {
    active->response_ = MakeResponse(active->request_id_, status, std::move(bytes));
    active_write_.reset();
    state_changed_.notify_all();
  };
  // A later leader may replace this index with an unrelated entry. Its
  // publication does not imply that our request must have a session result.
  // Accepted storage work remains owned by Raft; clients retry the same identity.
  if (!raft_node_->LeaderReady() || raft_node_->CurrentTerm() != active->proposal_term_) {
    finish(ClientResponseStatus::NOT_LEADER);
    return;
  }
  if (active->work_ && active->work_->Ready()) {
    try {
      active->prepared_ = active->work_->Take();
      active->work_.reset();
    } catch (const std::exception &e) {
      active->work_.reset();
      if (active->proposal_index_ != 0) throw;
      finish(ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
      return;
    }
  }
  if (raft_node_->Busy()) return;
  if (auto error = raft_node_->TakeProposalError(active->proposal_index_)) {
    try {
      std::rethrow_exception(error);
    } catch (const std::exception &e) {
      finish(ClientResponseStatus::REJECTED, ErrorPayload(e.what()));
    }
    return;
  }
  if (active->prepared_) {
    auto &result = *active->prepared_;
    if (result.disposition_ == RequestDisposition::RETRY_LAST) {
      const auto response = WriteResponseCodec::Decode(result.bytes_);
      if (response.commit_index_ > raft_node_->PublishedAppliedIndex()) return;
      finish(ClientResponseStatus::COMMITTED, std::move(result.bytes_));
      return;
    }
    if (result.disposition_ != RequestDisposition::NEW_REQUEST) {
      finish(ClientResponseStatus::REJECTED,
             ErrorPayload(result.disposition_ == RequestDisposition::PAYLOAD_MISMATCH
                              ? "request payload does not match request identity"
                              : "SQL request id is old or contains a session sequence gap"));
      return;
    }
    if (active->proposal_index_ == 0) {
      if (result.published_ != raft_node_->PublishedAppliedIndex()) {
        finish(ClientResponseStatus::NOT_LEADER);
        return;
      }
      const auto proposed = raft_node_->Propose(EntryType::COMMAND_BATCH, std::move(result.bytes_));
      if (!proposed) {
        finish(ClientResponseStatus::NOT_LEADER);
        return;
      }
      active->proposal_index_ = *proposed;
      active->prepared_.reset();
      return;
    }
    // Query of a published proposal must find the replicated session result.
    throw std::runtime_error("published proposal has no session result");
  }
  if (active->work_) return;
  if (active->proposal_index_ != 0 && raft_node_->PublishedAppliedIndex() >= active->proposal_index_) {
    active->work_ = raft_node_->BusinessTasks().Submit(4096, true, [state = state_machine_, active] {
      const auto disposition =
          state->ClassifyRequest(active->client_id_, active->request_id_, active->request_fingerprint_);
      auto response = state->GetLastResponse(active->client_id_);
      return WriteWork{disposition, response.value_or(std::vector<std::byte>{}), state->PublishedAppliedIndex()};
    });
  }
}

void DistributedNode::MaybeCreateSnapshot() {
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
  if (!static_cast<bool>(active_write_) && commit == applied && applied == published &&
      raft_node_->LastLogIndex() == commit && published > snapshot_index &&
      published - snapshot_index >= config_.snapshot_threshold_entries_) {
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
      if (active_write_ && active_write_->client_id_ == request.client_id_ &&
          active_write_->request_id_ == request.request_id_) {
        if (!(active_write_->request_fingerprint_ == fingerprint))
          return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED,
                              ErrorPayload("request payload does not match request identity"));
        mine = active_write_;
      } else if (!mine && !active_write_ && !raft_node_->Busy() &&
                 raft_node_->LastLogIndex() == raft_node_->CommitIndex() &&
                 raft_node_->PublishedAppliedIndex() == raft_node_->CommitIndex()) {
        auto work = raft_node_->BusinessTasks().Submit(
            request.sql_.size() + 4096, false, [state = state_machine_, request, fingerprint] {
              const auto disposition = state->ClassifyRequest(request.client_id_, request.request_id_, fingerprint);
              if (disposition == RequestDisposition::RETRY_LAST)
                return WriteWork{disposition, *state->GetLastResponse(request.client_id_),
                                 state->PublishedAppliedIndex()};
              if (disposition != RequestDisposition::NEW_REQUEST)
                return WriteWork{disposition, {}, state->PublishedAppliedIndex()};
              auto batch = state->PrepareSql(request.sql_, request.client_id_, request.request_id_, fingerprint);
              return WriteWork{disposition, CommandBatchCodec::Encode(batch), state->PublishedAppliedIndex()};
            });
        if (work) {
          mine = std::make_shared<ActiveWrite>();
          mine->client_id_ = request.client_id_;
          mine->request_id_ = request.request_id_;
          mine->request_fingerprint_ = fingerprint;
          mine->proposal_term_ = raft_node_->CurrentTerm();
          mine->work_ = std::move(work);
          active_write_ = mine;
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
