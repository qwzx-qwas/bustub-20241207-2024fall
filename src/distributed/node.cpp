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
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "raft/persistent_state.h"

namespace bustub {
namespace {

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
  state_machine_ = BusTubRaftStateMachine::Open(directory_.get(), storage_, config_.buffer_pool_size_);
  auto recovered = RecoverRaftPersistentState(directory_->RaftDirectory(), storage_, state_machine_);

  std::map<NodeId, TcpEndpoint> raft_peers;
  for (const auto &[peer_id, peer] : config_.peers_) {
    raft_peers.emplace(peer_id, peer.raft_endpoint_);
  }
  transport_ = std::make_shared<TcpRaftTransport>(config_.node_id_, config_.group_id_, config_.raft_listen_,
                                                  std::move(raft_peers));
  raft_node_ =
      std::make_unique<RaftNode>(RaftNodeConfig{config_.node_id_, std::move(voters), config_.election_timeout_min_ms_,
                                                config_.election_timeout_max_ms_, config_.heartbeat_interval_ms_,
                                                config_.group_id_, MakeRandomElectionTimeoutSource()},
                                 transport_, std::move(recovered.stable_store_), std::move(recovered.log_store_),
                                 state_machine_, std::move(recovered.snapshot_store_));
}

DistributedNode::~DistributedNode() { Stop(); }

void DistributedNode::Start() {
  std::lock_guard lock(mutex_);
  if (running_) {
    throw std::runtime_error("distributed node is already running");
  }
  bound_client_endpoint_ = config_.client_listen_;
  client_listen_fd_ = OpenListener(&bound_client_endpoint_);
  try {
    transport_->Start([this](RaftEnvelope envelope) {
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
  } catch (...) {
    close(client_listen_fd_);
    client_listen_fd_ = -1;
    throw;
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
}

void DistributedNode::TickLoop() {
  while (running_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.tick_interval_ms_));
    std::lock_guard lock(mutex_);
    if (fatal_error_ == nullptr) {
      try {
        // Keep Raft's logical clock monotonic across Stop()/Start() on the same
        // production assembly. Wall-clock epochs restart; this counter does not.
        logical_now_ms_ += config_.tick_interval_ms_;
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
  if (!active_write_.has_value()) {
    return;
  }

  const auto active = *active_write_;
  // In non-Byzantine Raft, (index, term) uniquely identifies the entry: two
  // Leaders cannot exist in one term. Checking the term also avoids copying a
  // potentially multi-megabyte CommandBatch on every tick.
  const bool original_slot_remains =
      raft_node_->Log().TermAt(active.proposal_index_) == std::optional<uint64_t>{active.proposal_term_};
  const auto disposition = state_machine_->ClassifyRequest(active.client_id_, active.request_id_);
  if (disposition == RequestDisposition::RETRY_LAST) {
    const auto response = state_machine_->GetLastResponse(active.client_id_);
    if (!response.has_value()) {
      throw std::runtime_error("committed SessionTable response for the active proposal is unavailable");
    }
    const auto committed = WriteResponseCodec::Decode(*response);
    if (committed.request_id_ != active.request_id_ || committed.commit_index_ > raft_node_->PublishedAppliedIndex()) {
      throw std::runtime_error("active proposal has an invalid or unpublished SessionTable result");
    }
    const auto committed_index = committed.commit_index_;
    if (committed_index != active.proposal_index_ && original_slot_remains) {
      throw std::runtime_error("active proposal remains in the log but its SessionTable result uses another index");
    }
    // Either this exact proposal committed at its original index, or a new
    // Leader overwrote that slot and committed the same exactly-once request at
    // another index. Both outcomes resolve the local gate.
    active_write_.reset();
    state_changed_.notify_all();
    return;
  }
  if (disposition == RequestDisposition::TOO_OLD) {
    // The ordered SessionTable has advanced beyond this request. This also
    // covers a new Leader inheriting and committing the original slot, followed
    // by a later request whose response replaces V1's single cached response.
    active_write_.reset();
    state_changed_.notify_all();
    return;
  }

  if (!original_slot_remains) {
    // A leadership change may overwrite an uncommitted proposal. Clearing the
    // gate permits the request to be retried only after that fact is visible.
    active_write_.reset();
    state_changed_.notify_all();
    return;
  }
  if (raft_node_->CommitIndex() >= active.proposal_index_ || raft_node_->LastApplied() >= active.proposal_index_ ||
      raft_node_->PublishedAppliedIndex() >= active.proposal_index_) {
    throw std::runtime_error("active Raft proposal crossed a commit boundary without a SessionTable result");
  }
}

void DistributedNode::MaybeCreateSnapshot() {
  const auto commit = raft_node_->CommitIndex();
  const auto applied = raft_node_->LastApplied();
  const auto published = raft_node_->PublishedAppliedIndex();
  const auto latest_snapshot = raft_node_->LatestSnapshot();
  const auto snapshot_index = latest_snapshot.has_value() ? latest_snapshot->last_included_index_ : 0;
  if (!active_write_.has_value() && commit == applied && applied == published &&
      raft_node_->Log().LastLogIndex() == commit && published > snapshot_index &&
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
          raft_node_->Log().SnapshotBaseIndex(),
          std::nullopt,
          std::move(payload)};
}

auto DistributedNode::HandleStatus(const ClientStatusRequestV1 &request) -> ClientResponseV1 {
  std::lock_guard lock(mutex_);
  return MakeResponse(request.request_id_,
                      fatal_error_ == nullptr ? ClientResponseStatus::OK : ClientResponseStatus::UNAVAILABLE);
}

auto DistributedNode::HandleWrite(const ClientWriteRequestV1 &request) -> ClientResponseV1 {
  std::unique_lock lock(mutex_);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_timeout_ms_);
  while (true) {
    if (fatal_error_ != nullptr || !running_) {
      return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
    }

    try {
      ReconcileActiveWrite();
    } catch (...) {
      fatal_error_ = std::current_exception();
      state_changed_.notify_all();
      return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
    }
    if (!raft_node_->LeaderReady()) {
      return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
    }

    const auto disposition = state_machine_->ClassifyRequest(request.client_id_, request.request_id_);
    if (disposition == RequestDisposition::RETRY_LAST) {
      const auto response = state_machine_->GetLastResponse(request.client_id_);
      if (!response.has_value()) {
        fatal_error_ = std::make_exception_ptr(std::runtime_error("committed SessionTable response is unavailable"));
        state_changed_.notify_all();
        return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
      }
      try {
        const auto committed = WriteResponseCodec::Decode(*response);
        if (committed.request_id_ != request.request_id_ ||
            committed.commit_index_ > raft_node_->PublishedAppliedIndex()) {
          throw std::runtime_error("SessionTable response is invalid or not yet published");
        }
      } catch (...) {
        fatal_error_ = std::current_exception();
        state_changed_.notify_all();
        return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
      }
      return MakeResponse(request.request_id_, ClientResponseStatus::COMMITTED, *response);
    }
    if (disposition != RequestDisposition::NEW_REQUEST) {
      return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED,
                          ErrorPayload("request ID is old or contains a sequence gap"));
    }

    if (!active_write_.has_value()) {
      const auto commit = raft_node_->CommitIndex();
      if (raft_node_->Log().LastLogIndex() != commit || raft_node_->LastApplied() != commit ||
          raft_node_->PublishedAppliedIndex() != commit) {
        fatal_error_ =
            std::make_exception_ptr(std::runtime_error("Raft write gate is open outside a stable proposal boundary"));
        state_changed_.notify_all();
        return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
      }

      std::vector<std::byte> encoded_batch;
      try {
        const auto batch = state_machine_->PrepareSql(request.sql_, request.client_id_, request.request_id_);
        state_machine_->ValidateProposal(batch);
        encoded_batch = CommandBatchCodec::Encode(batch);
      } catch (const std::exception &error) {
        return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(error.what()));
      }

      const auto proposal_term = raft_node_->CurrentTerm();
      try {
        const auto proposed = raft_node_->Propose(EntryType::COMMAND_BATCH, std::move(encoded_batch));
        if (!proposed.has_value()) {
          return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
        }
        active_write_ = ActiveWrite{request.client_id_, request.request_id_, *proposed, proposal_term};
      } catch (...) {
        // Durable mutation failures are ambiguous: the journal may contain the
        // entry even though in-memory publication failed. Never report these as
        // ordinary SQL rejection or choose another proposal index in this run.
        fatal_error_ = std::current_exception();
        state_changed_.notify_all();
        return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
      }
    }

    if (state_changed_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return MakeResponse(request.request_id_, ClientResponseStatus::TIMEOUT);
    }
  }
}

auto DistributedNode::HandleRead(const ClientReadRequestV1 &request) -> ClientResponseV1 {
  std::unique_lock lock(mutex_);
  if (fatal_error_ != nullptr || raft_node_->Role() == RaftRole::STOPPED) {
    return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
  }

  uint64_t read_index = 0;
  if (request.consistency_ == ClientReadConsistency::LINEARIZABLE) {
    if (!raft_node_->LeaderReady()) {
      return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
    }
    const auto context = ++next_read_context_;
    if (!raft_node_->StartReadIndex(context)) {
      return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
    }
    const auto term = raft_node_->CurrentTerm();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_timeout_ms_);
    while (true) {
      if (fatal_error_ != nullptr || !running_) {
        raft_node_->CancelReadIndex(context);
        return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
      }
      if (!raft_node_->LeaderReady() || raft_node_->CurrentTerm() != term) {
        raft_node_->CancelReadIndex(context);
        return MakeResponse(request.request_id_, ClientResponseStatus::NOT_LEADER);
      }
      if (const auto completed = raft_node_->TakeReadIndex(context); completed.has_value()) {
        read_index = *completed;
        break;
      }
      if (state_changed_.wait_until(lock, deadline) == std::cv_status::timeout) {
        raft_node_->CancelReadIndex(context);
        return MakeResponse(request.request_id_, ClientResponseStatus::TIMEOUT);
      }
    }
  }

  const auto published = raft_node_->PublishedAppliedIndex();
  if (read_index > published) {
    fatal_error_ = std::make_exception_ptr(std::runtime_error("ReadIndex exceeds the published state-machine index"));
    return MakeResponse(request.request_id_, ClientResponseStatus::UNAVAILABLE);
  }
  try {
    auto response = MakeResponse(request.request_id_, ClientResponseStatus::OK,
                                 state_machine_->ExecuteReadSql(request.sql_, published));
    response.read_timestamp_ = published;
    return response;
  } catch (const std::exception &error) {
    return MakeResponse(request.request_id_, ClientResponseStatus::REJECTED, ErrorPayload(error.what()));
  }
}

auto DistributedNode::ClientEndpoint() const -> TcpEndpoint {
  std::lock_guard lock(mutex_);
  return bound_client_endpoint_;
}

auto DistributedNode::RaftEndpoint() const -> TcpEndpoint { return transport_->ListenEndpoint(); }

}  // namespace bustub
