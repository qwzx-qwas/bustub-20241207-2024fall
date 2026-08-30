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
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "raft/rpc_codec.h"

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

auto ReadExact(int socket_fd, std::byte *data, size_t size) -> bool {
  size_t offset = 0;
  while (offset < size) {
    const auto count = recv(socket_fd, data + offset, size - offset, 0);
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
                                   size_t maximum_pending)
    : local_node_id_(local_node_id),
      group_id_(std::move(group_id)),
      listen_endpoint_(std::move(listen_endpoint)),
      peers_(std::move(peers)),
      connect_timeout_ms_(connect_timeout_ms),
      maximum_pending_(maximum_pending) {
  if (local_node_id_ == 0 || group_id_.empty() || group_id_.size() > 128 || listen_endpoint_.host_.empty() ||
      peers_.empty() || connect_timeout_ms_ == 0 || maximum_pending_ == 0 || peers_.count(local_node_id_) != 0 ||
      peers_.count(0) != 0) {
    throw std::runtime_error("invalid TCP Raft transport configuration");
  }
  for (const auto &[node_id, endpoint] : peers_) {
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
  send_thread_ = std::thread([this] { SendLoop(); });
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
    dropped_messages_ += outbound_.size();
    outbound_.clear();
  }
  send_cv_.notify_all();
  if (listen_fd_ >= 0) {
    shutdown(listen_fd_, SHUT_RDWR);
  }
  if (receive_thread_.joinable()) {
    receive_thread_.join();
  }
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
  std::lock_guard lock(mutex_);
  if (listen_fd_ >= 0) {
    close(listen_fd_);
    listen_fd_ = -1;
  }
  outbound_.clear();
  receiver_ = nullptr;
}

void TcpRaftTransport::Send(RaftEnvelope envelope) {
  if (envelope.from_ != local_node_id_ || envelope.to_ == local_node_id_ || peers_.count(envelope.to_) == 0 ||
      envelope.group_id_ != group_id_) {
    throw std::runtime_error("invalid or inactive TCP Raft send");
  }
  static_cast<void>(RaftRpcCodec::Encode(envelope));
  {
    std::lock_guard lock(mutex_);
    // Pair the running-state check with queue insertion so Stop() either
    // clears this message or makes the send fail; no enqueue can occur after
    // the shutdown queue has been discarded.
    if (!running_) {
      throw std::runtime_error("invalid or inactive TCP Raft send");
    }
    if (outbound_.size() >= maximum_pending_) {
      dropped_messages_++;
      return;
    }
    outbound_.push_back(std::move(envelope));
  }
  send_cv_.notify_one();
}

auto TcpRaftTransport::ListenEndpoint() const -> TcpEndpoint {
  std::lock_guard lock(mutex_);
  return listen_endpoint_;
}

void TcpRaftTransport::ReceiveLoop() {
  while (running_) {
    pollfd descriptor{listen_fd_, POLLIN, 0};
    auto status = poll(&descriptor, 1, 100);
    if (status < 0 && errno == EINTR) {
      continue;
    }
    if (status <= 0 || !running_) {
      continue;
    }
    const auto connection = accept(listen_fd_, nullptr, nullptr);
    if (connection < 0) {
      continue;
    }
    SocketGuard guard(connection);
    SetSocketTimeout(connection, connect_timeout_ms_);
    try {
      HandleConnection(connection);
    } catch (const std::exception &) {
      dropped_messages_++;
    }
  }
}

void TcpRaftTransport::HandleConnection(int socket_fd) {
  std::vector<std::byte> prefix(RaftRpcCodec::FRAME_PREFIX_BYTES);
  if (!ReadExact(socket_fd, prefix.data(), prefix.size())) {
    throw std::runtime_error("truncated TCP Raft prefix");
  }
  const auto payload_size = RaftRpcCodec::PayloadSizeFromPrefix(prefix);
  std::vector<std::byte> frame = prefix;
  frame.resize(prefix.size() + payload_size + sizeof(uint32_t));
  if (!ReadExact(socket_fd, frame.data() + prefix.size(), payload_size + sizeof(uint32_t))) {
    throw std::runtime_error("truncated TCP Raft frame");
  }
  auto envelope = RaftRpcCodec::Decode(frame);
  if (envelope.to_ != local_node_id_ || peers_.count(envelope.from_) == 0 || envelope.group_id_ != group_id_) {
    throw std::runtime_error("TCP Raft envelope is not from a configured peer");
  }
  Receiver receiver;
  {
    std::lock_guard lock(mutex_);
    receiver = receiver_;
  }
  if (running_ && receiver) {
    receiver(std::move(envelope));
  }
}

void TcpRaftTransport::SendLoop() {
  while (true) {
    RaftEnvelope envelope;
    {
      std::unique_lock lock(mutex_);
      send_cv_.wait(lock, [&] { return !running_ || !outbound_.empty(); });
      if (!running_ && outbound_.empty()) {
        return;
      }
      envelope = std::move(outbound_.front());
      outbound_.pop_front();
    }
    if (!SendOne(envelope)) {
      dropped_messages_++;
    }
  }
}

auto TcpRaftTransport::SendOne(const RaftEnvelope &envelope) -> bool {
  const auto peer = peers_.find(envelope.to_);
  if (peer == peers_.end()) {
    return false;
  }
  const auto socket_fd = ConnectWithTimeout(peer->second, connect_timeout_ms_);
  if (socket_fd < 0) {
    return false;
  }
  SocketGuard guard(socket_fd);
  const auto frame = RaftRpcCodec::Encode(envelope);
  return WriteExact(socket_fd, frame.data(), frame.size());
}

}  // namespace bustub
