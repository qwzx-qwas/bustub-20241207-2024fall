//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tcp_transport.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/tcp_transport.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>  // NOLINT(build/c++11)
#include <cstring>
#include <limits>
#include <list>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/framed_socket.h"
#include "raft/rpc_codec.h"

namespace bustub {
namespace {

// Conservative peak encoding allowance: body, envelope and framed copies,
// including vector growth. Queue retention is reduced to actual frame capacity.
// Uses message lengths, never serializes once merely to size another encoding.
auto EncodeMemory(const RaftEnvelope &envelope) -> size_t {
  size_t bytes = envelope.group_id_.capacity() + 256;
  std::visit([&](const auto &message) {
    using T = std::decay_t<decltype(message)>;
    if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
      for (const auto &entry : message.entries_) bytes += entry.payload_.capacity() + 128;
    } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
      bytes += message.data_.capacity() + message.snapshot_id_.capacity();
    } else if constexpr (std::is_same_v<T, SnapshotOfferRequest>) {
      bytes += message.target_.snapshot_id_.capacity() + message.base_.snapshot_id_.capacity();
    }
  }, envelope.message_);
  return 4096 + 8 * bytes;
}

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
    const auto value = socket_fd_;
    socket_fd_ = -1;
    return value;
  }

 private:
  int socket_fd_;
};

void SetSocketTimeout(int socket_fd, uint64_t timeout_ms) {
  timeval timeout{static_cast<time_t>(timeout_ms / 1000), static_cast<suseconds_t>((timeout_ms % 1000) * 1000)};
  setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

auto WriteExact(int socket_fd, const std::byte *data, size_t size) -> bool {
  size_t offset = 0;
  while (offset < size) {
    const auto count = send(socket_fd, data + offset, size - offset, MSG_NOSIGNAL);
    if (count > 0) {
      offset += static_cast<size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

auto Resolve(const TcpEndpoint &endpoint, bool passive) -> addrinfo * {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = passive ? AI_PASSIVE : 0;
  addrinfo *addresses = nullptr;
  const auto port = std::to_string(endpoint.port_);
  const auto status =
      getaddrinfo(endpoint.host_.empty() ? nullptr : endpoint.host_.c_str(), port.c_str(), &hints, &addresses);
  if (status != 0) {
    throw std::runtime_error("cannot resolve TCP endpoint " + endpoint.ToString() + ": " + gai_strerror(status));
  }
  return addresses;
}

auto OpenListener(TcpEndpoint *endpoint) -> int {
  addrinfo *addresses = Resolve(*endpoint, true);
  int listener = -1;
  for (auto *address = addresses; address != nullptr; address = address->ai_next) {
    const auto candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (candidate < 0) {
      continue;
    }
    SocketGuard guard(candidate);
    int reuse = 1;
    setsockopt(candidate, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (bind(candidate, address->ai_addr, address->ai_addrlen) != 0 || listen(candidate, 128) != 0) {
      continue;
    }
    listener = guard.Release();
    break;
  }
  freeaddrinfo(addresses);
  if (listener < 0) {
    throw std::runtime_error("cannot bind TCP endpoint " + endpoint->ToString());
  }
  if (endpoint->port_ == 0) {
    sockaddr_storage address{};
    socklen_t size = sizeof(address);
    if (getsockname(listener, reinterpret_cast<sockaddr *>(&address), &size) != 0) {
      close(listener);
      throw std::runtime_error("cannot inspect dynamically bound TCP endpoint");
    }
    if (address.ss_family == AF_INET) {
      endpoint->port_ = ntohs(reinterpret_cast<sockaddr_in *>(&address)->sin_port);
    } else {
      endpoint->port_ = ntohs(reinterpret_cast<sockaddr_in6 *>(&address)->sin6_port);
    }
  }
  return listener;
}

auto ConnectWithTimeout(const TcpEndpoint &endpoint, uint64_t timeout_ms) -> int {
  addrinfo *addresses = nullptr;
  try {
    addresses = Resolve(endpoint, false);
  } catch (const std::exception &) {
    return -1;
  }
  int connected = -1;
  for (auto *address = addresses; address != nullptr; address = address->ai_next) {
    const auto candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (candidate < 0) {
      continue;
    }
    SocketGuard guard(candidate);
    const auto old_flags = fcntl(candidate, F_GETFL, 0);
    if (old_flags < 0 || fcntl(candidate, F_SETFL, old_flags | O_NONBLOCK) != 0) {
      continue;
    }
    auto status = connect(candidate, address->ai_addr, address->ai_addrlen);
    if (status != 0 && errno == EINPROGRESS) {
      pollfd descriptor{candidate, POLLOUT, 0};
      do {
        status = poll(&descriptor, 1, static_cast<int>(timeout_ms));
      } while (status < 0 && errno == EINTR);
      int socket_error = 0;
      socklen_t error_size = sizeof(socket_error);
      if (status <= 0 || getsockopt(candidate, SOL_SOCKET, SO_ERROR, &socket_error, &error_size) != 0 ||
          socket_error != 0) {
        continue;
      }
    } else if (status != 0) {
      continue;
    }
    if (fcntl(candidate, F_SETFL, old_flags) != 0) {
      continue;
    }
    SetSocketTimeout(candidate, timeout_ms);
    int enabled = 1;
    setsockopt(candidate, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
    connected = guard.Release();
    break;
  }
  freeaddrinfo(addresses);
  return connected;
}

}  // namespace

auto TcpEndpoint::Parse(const std::string &value) -> TcpEndpoint {
  const auto separator = value.rfind(':');
  if (separator == std::string::npos || separator == 0 || separator + 1 == value.size() ||
      value.find(':') != separator) {
    throw std::runtime_error("TCP endpoint must use host:port with an IPv4 address or hostname");
  }
  uint32_t port = 0;
  const auto *first = value.data() + separator + 1;
  const auto *last = value.data() + value.size();
  const auto parsed = std::from_chars(first, last, port);
  if (parsed.ec != std::errc() || parsed.ptr != last || port > std::numeric_limits<uint16_t>::max()) {
    throw std::runtime_error("TCP endpoint port is invalid");
  }
  return {value.substr(0, separator), static_cast<uint16_t>(port)};
}

auto TcpEndpoint::ToString() const -> std::string { return host_ + ":" + std::to_string(port_); }

TcpRaftTransport::TcpRaftTransport(NodeId local_node_id, std::string group_id, TcpEndpoint listen_endpoint,
                                   std::map<NodeId, TcpEndpoint> peers, uint64_t connect_timeout_ms,
                                   size_t maximum_pending, std::shared_ptr<ResourceBudget> memory)
    : local_node_id_(local_node_id),
      group_id_(std::move(group_id)),
      listen_endpoint_(std::move(listen_endpoint)),
      peers_(std::move(peers)),
      connect_timeout_ms_(connect_timeout_ms),
      maximum_pending_(maximum_pending),
      memory_(ResourceAccount::Create(std::move(memory))) {
  if (local_node_id_ == 0 || group_id_.empty() || group_id_.size() > 128 || listen_endpoint_.host_.empty() ||
      peers_.empty() || connect_timeout_ms_ == 0 || maximum_pending_ == 0 || peers_.count(local_node_id_) != 0 ||
      peers_.count(0) != 0) {
    throw std::runtime_error("invalid TCP Raft transport configuration");
  }
  for (const auto &[node_id, endpoint] : peers_) {
    senders_.emplace(node_id, std::make_unique<PeerSender>());
    if (node_id == 0 || endpoint.host_.empty() || endpoint.port_ == 0) {
      throw std::runtime_error("invalid TCP Raft peer endpoint");
    }
  }
}

TcpRaftTransport::~TcpRaftTransport() { Stop(); }

void TcpRaftTransport::Start(Receiver receiver) {
  if (!receiver) {
    throw std::runtime_error("TCP Raft transport needs a receiver");
  }
  std::lock_guard lock(mutex_);
  if (running_) {
    throw std::runtime_error("TCP Raft transport is already running");
  }
  listen_fd_ = OpenListener(&listen_endpoint_);
  receiver_ = std::move(receiver);
  running_ = true;
  receive_thread_ = std::thread([this] { ReceiveLoop(); });
  for (auto &[id, sender] : senders_) sender->thread_ = std::thread([this, id] { SendLoop(id); });
}

void TcpRaftTransport::Stop() {
  {
    std::lock_guard lock(mutex_);
    if (!running_) {
      return;
    }
    running_ = false;
    // Once shutdown begins, queued Raft messages are stale and cannot affect
    // safety. Discard them instead of delaying process termination while the
    // send worker attempts every old connection in FIFO order.
    dropped_messages_ += pending_;
    pending_ = 0;
    for (auto &[id, sender] : senders_) {
      sender->frames_.clear();
      if (sender->fd_ >= 0) shutdown(sender->fd_, SHUT_RDWR);
      sender->ready_.notify_one();
    }
  }
  if (listen_fd_ >= 0) {
    shutdown(listen_fd_, SHUT_RDWR);
  }
  if (receive_thread_.joinable()) {
    receive_thread_.join();
  }
  for (auto &[id, sender] : senders_)
    if (sender->thread_.joinable()) sender->thread_.join();
  std::lock_guard lock(mutex_);
  if (listen_fd_ >= 0) {
    close(listen_fd_);
    listen_fd_ = -1;
  }
  receiver_ = nullptr;
}

void TcpRaftTransport::Send(RaftEnvelope envelope) {
  if (envelope.from_ != local_node_id_ || envelope.to_ == local_node_id_ || peers_.count(envelope.to_) == 0 ||
      envelope.group_id_ != group_id_) {
    throw std::runtime_error("invalid or inactive TCP Raft send");
  }
  const auto bytes = EncodeMemory(envelope);
  if (!memory_->Reserve(bytes, false)) {
    ++dropped_messages_;  // Raft retries transport loss; never wait under its protocol lock.
    return;
  }
  ResourceCharge charge(memory_, bytes, false);
  auto frame = RaftRpcCodec::Encode(envelope);
  charge.ShrinkTo(frame.capacity());
  {
    std::lock_guard lock(mutex_);
    // Pair the running-state check with queue insertion so Stop() either
    // clears this message or makes the send fail; no enqueue can occur after
    // the shutdown queue has been discarded.
    if (!running_) {
      throw std::runtime_error("invalid or inactive TCP Raft send");
    }
    auto &sender = *senders_.at(envelope.to_);
    if (pending_ >= maximum_pending_ || sender.frames_.size() >= std::max<size_t>(1, maximum_pending_ / peers_.size())) {
      dropped_messages_++;
      return;
    }
    sender.frames_.push_back({std::move(charge), std::move(frame)});
    ++pending_;
    sender.ready_.notify_one();
  }
}

auto TcpRaftTransport::ListenEndpoint() const -> TcpEndpoint {
  std::lock_guard lock(mutex_);
  return listen_endpoint_;
}

void TcpRaftTransport::ReceiveLoop() {
  std::list<FramedSocket> connections;
  while (running_) {
    std::vector<pollfd> descriptors{{listen_fd_, POLLIN, 0}};
    for (auto &connection : connections) descriptors.push_back({connection.Fd(), POLLIN, 0});
    const auto status = poll(descriptors.data(), descriptors.size(), 100);
    if (status < 0 && errno == EINTR) continue;
    if (status < 0 || !running_) continue;
    size_t position = 1;
    for (auto it = connections.begin(); it != connections.end();) {
      const auto events = descriptors[position++].revents;
      bool closed = events & (POLLERR | POLLNVAL);
      try {
        if (events & (POLLIN | POLLHUP)) {
          auto result = it->Receive(RaftRpcCodec::PayloadSizeFromPrefix, 12);
          closed |= result == FramedSocket::Result::Closed;
          if (result == FramedSocket::Result::Ready) {
            auto envelope = RaftRpcCodec::Decode(it->Input());
            auto charge = it->Consume();
            if (envelope.to_ != local_node_id_ || peers_.count(envelope.from_) == 0 || envelope.group_id_ != group_id_)
              throw std::runtime_error("TCP Raft envelope is not from a configured peer");
            if (running_) receiver_(std::move(envelope));
          }
        }
        if (it->Partial() && std::chrono::steady_clock::now() - it->Started() >
                            std::chrono::milliseconds(connect_timeout_ms_)) closed = true;
      } catch (const std::exception &) {
        ++dropped_messages_;
        closed = true;
      }
      if (closed) it = connections.erase(it); else ++it;
    }
    if (descriptors[0].revents & POLLIN) {
      const auto fd = accept(listen_fd_, nullptr, nullptr);
      if (fd >= 0) {
        if (connections.size() < std::max<size_t>(16, peers_.size() * 4))
          connections.emplace_back(fd, RaftRpcCodec::FRAME_PREFIX_BYTES, memory_);
        else close(fd);
      }
    }
  }
}

void TcpRaftTransport::SendLoop(NodeId peer) {
  auto &sender = *senders_.at(peer);
  for (;;) {
    PendingFrame frame;
    {
      std::unique_lock lock(mutex_);
      sender.ready_.wait(lock, [&] { return !running_ || !sender.frames_.empty(); });
      if (!running_) break;
      frame = std::move(sender.frames_.front());
      sender.frames_.pop_front();
      --pending_;
    }
    if (sender.fd_ >= 0) {
      // This is a one-way stream; EOF is a known closed peer, before any new
      // envelope has been sent. Reconnect here, not after an uncertain write.
      char byte;
      if (recv(sender.fd_, &byte, 1, MSG_PEEK | MSG_DONTWAIT) == 0) {
        std::lock_guard lock(mutex_);
        close(sender.fd_);
        sender.fd_ = -1;
      }
    }
    if (sender.fd_ < 0) {
      const auto fd = ConnectWithTimeout(peers_.at(peer), connect_timeout_ms_);
      std::lock_guard lock(mutex_);
      if (!running_) { if (fd >= 0) close(fd); break; }
      sender.fd_ = fd;
    }
    if (sender.fd_ < 0 || !WriteExact(sender.fd_, frame.bytes_.data(), frame.bytes_.size())) {
      std::lock_guard lock(mutex_);
      ++dropped_messages_;
      if (sender.fd_ >= 0) close(sender.fd_);
      sender.fd_ = -1;
      // Loss is handled by the Raft protocol. Do not replay a stale transport backlog.
      dropped_messages_ += sender.frames_.size();
      pending_ -= sender.frames_.size();
      sender.frames_.clear();
    }
  }
  std::lock_guard lock(mutex_);
  if (sender.fd_ >= 0) close(sender.fd_);
  sender.fd_ = -1;
}

}  // namespace bustub
