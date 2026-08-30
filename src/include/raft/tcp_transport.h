//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tcp_transport.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>  // NOLINT(build/c++11)
#include <string>
#include <thread>  // NOLINT(build/c++11)

#include "raft/transport.h"

namespace bustub {

struct TcpEndpoint {
  std::string host_;
  uint16_t port_{0};

  static auto Parse(const std::string &value) -> TcpEndpoint;
  auto ToString() const -> std::string;
  friend auto operator==(const TcpEndpoint &lhs, const TcpEndpoint &rhs) -> bool {
    return lhs.host_ == rhs.host_ && lhs.port_ == rhs.port_;
  }
};

/**
 * Production Raft transport. Each envelope uses one checksummed TCP frame and one connection. Failed connections are
 * treated as message loss; normal Raft heartbeats retry without blocking the node event loop.
 */
class TcpRaftTransport : public RaftTransport {
 public:
  using Receiver = std::function<void(RaftEnvelope)>;

  TcpRaftTransport(NodeId local_node_id, std::string group_id, TcpEndpoint listen_endpoint,
                   std::map<NodeId, TcpEndpoint> peers, uint64_t connect_timeout_ms = 250,
                   size_t maximum_pending = 10000);
  ~TcpRaftTransport() override;

  void Start(Receiver receiver);
  void Stop();
  void Send(RaftEnvelope envelope) override;

  auto ListenEndpoint() const -> TcpEndpoint;
  auto DroppedMessages() const -> uint64_t { return dropped_messages_.load(); }

 private:
  void ReceiveLoop();
  void SendLoop();
  void HandleConnection(int socket_fd);
  auto SendOne(const RaftEnvelope &envelope) -> bool;

  NodeId local_node_id_;
  std::string group_id_;
  TcpEndpoint listen_endpoint_;
  std::map<NodeId, TcpEndpoint> peers_;
  uint64_t connect_timeout_ms_;
  size_t maximum_pending_;

  mutable std::mutex mutex_;
  std::condition_variable send_cv_;
  std::deque<RaftEnvelope> outbound_;
  Receiver receiver_;
  int listen_fd_{-1};
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> dropped_messages_{0};
  std::thread receive_thread_;
  std::thread send_thread_;
};

}  // namespace bustub
