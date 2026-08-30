//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// client.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/client.h"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "distributed/session_table.h"

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

 private:
  int socket_fd_;
};

void SetSocketTimeout(int socket_fd, uint64_t timeout_ms) {
  timeval timeout{static_cast<time_t>(timeout_ms / 1000), static_cast<suseconds_t>((timeout_ms % 1000) * 1000)};
  setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

auto Connect(const TcpEndpoint &endpoint, uint64_t timeout_ms) -> int {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo *addresses = nullptr;
  const auto port = std::to_string(endpoint.port_);
  const auto resolved = getaddrinfo(endpoint.host_.c_str(), port.c_str(), &hints, &addresses);
  if (resolved != 0) {
    throw std::runtime_error("cannot resolve client endpoint " + endpoint.ToString() + ": " + gai_strerror(resolved));
  }
  int connected = -1;
  for (auto *address = addresses; address != nullptr; address = address->ai_next) {
    const auto candidate = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (candidate < 0) {
      continue;
    }
    const auto old_flags = fcntl(candidate, F_GETFL, 0);
    if (old_flags < 0 || fcntl(candidate, F_SETFL, old_flags | O_NONBLOCK) != 0) {
      close(candidate);
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
        close(candidate);
        continue;
      }
    } else if (status != 0) {
      close(candidate);
      continue;
    }
    if (fcntl(candidate, F_SETFL, old_flags) != 0) {
      close(candidate);
      continue;
    }
    connected = candidate;
    break;
  }
  freeaddrinfo(addresses);
  if (connected < 0) {
    throw std::runtime_error("cannot connect to client endpoint " + endpoint.ToString());
  }
  SetSocketTimeout(connected, timeout_ms);
  return connected;
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

void ValidateSuccessfulResponse(const ClientRequestV1 &request, const ClientResponseV1 &response, uint64_t request_id) {
  const auto is_write = std::holds_alternative<ClientWriteRequestV1>(request);
  const auto is_read = std::holds_alternative<ClientReadRequestV1>(request);
  if (response.status_ == ClientResponseStatus::COMMITTED) {
    if (!is_write) {
      throw std::runtime_error("distributed client received a committed response for a non-write request");
    }
    const auto committed = WriteResponseCodec::Decode(response.payload_);
    if (committed.request_id_ != request_id) {
      throw std::runtime_error("distributed client committed response request ID does not match envelope");
    }
    return;
  }
  if (response.status_ != ClientResponseStatus::OK) {
    return;
  }
  if (is_write) {
    throw std::runtime_error("distributed client received an OK response for a write request");
  }
  if (is_read) {
    if (!response.read_timestamp_.has_value()) {
      throw std::runtime_error("distributed client read response is missing its read timestamp");
    }
    static_cast<void>(ClientQueryResultCodec::Decode(response.payload_));
    return;
  }
  if (response.read_timestamp_.has_value() || !response.payload_.empty()) {
    throw std::runtime_error("distributed client status response contains read-result state");
  }
}

}  // namespace

auto DistributedClient::Send(const TcpEndpoint &endpoint, const ClientRequestV1 &request, uint64_t timeout_ms)
    -> ClientResponseV1 {
  if (endpoint.host_.empty() || endpoint.port_ == 0 || timeout_ms == 0) {
    throw std::runtime_error("invalid distributed client destination");
  }
  const auto socket_fd = Connect(endpoint, timeout_ms);
  SocketGuard guard(socket_fd);
  const auto request_frame = ClientProtocolCodec::EncodeRequest(request);
  if (!WriteExact(socket_fd, request_frame.data(), request_frame.size())) {
    throw std::runtime_error("distributed client request write failed");
  }
  std::vector<std::byte> prefix(ClientProtocolCodec::FRAME_PREFIX_BYTES);
  if (!ReadExact(socket_fd, prefix.data(), prefix.size())) {
    throw std::runtime_error("distributed client response prefix is unavailable");
  }
  const auto payload_size = ClientProtocolCodec::PayloadSizeFromPrefix(prefix);
  std::vector<std::byte> response = prefix;
  response.resize(prefix.size() + payload_size + sizeof(uint32_t));
  if (!ReadExact(socket_fd, response.data() + prefix.size(), payload_size + sizeof(uint32_t))) {
    throw std::runtime_error("distributed client response is truncated");
  }
  auto decoded = ClientProtocolCodec::DecodeResponse(response);
  const auto request_id = std::visit([](const auto &message) { return message.request_id_; }, request);
  if (decoded.request_id_ != request_id) {
    throw std::runtime_error("distributed client response request ID does not match request");
  }
  ValidateSuccessfulResponse(request, decoded, request_id);
  return decoded;
}

}  // namespace bustub
