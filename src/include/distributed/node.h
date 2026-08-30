//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// node.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <string>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "distributed/client_protocol.h"
#include "distributed/raft_state_machine.h"
#include "raft/raft_node.h"
#include "raft/tcp_transport.h"

namespace bustub {

struct DistributedPeerConfig {
  TcpEndpoint raft_endpoint_;
  TcpEndpoint client_endpoint_;
};

struct DistributedNodeConfig {
  NodeId node_id_{0};
  std::string group_id_;
  std::filesystem::path data_directory_;
  TcpEndpoint raft_listen_;
  TcpEndpoint client_listen_;
  std::map<NodeId, DistributedPeerConfig> peers_;
  uint64_t election_timeout_min_ms_{300};
  uint64_t election_timeout_max_ms_{600};
  uint64_t heartbeat_interval_ms_{50};
  uint64_t tick_interval_ms_{10};
  uint64_t client_timeout_ms_{5000};
  size_t buffer_pool_size_{128};
  uint64_t snapshot_threshold_entries_{10000};

  void Validate() const;
};

/** Production assembly for one static BusTub Raft node and its stable client endpoint. */
class DistributedNode {
 public:
  static auto Open(DistributedNodeConfig config, std::shared_ptr<DurableStorage> storage = nullptr)
      -> std::unique_ptr<DistributedNode>;
  ~DistributedNode();

  void Start();
  void Stop();
  auto HandleRequest(const ClientRequestV1 &request) -> ClientResponseV1;

  auto ClientEndpoint() const -> TcpEndpoint;
  auto RaftEndpoint() const -> TcpEndpoint;
  auto IsRunning() const -> bool { return running_.load(); }

 private:
  DistributedNode(DistributedNodeConfig config, std::shared_ptr<DurableStorage> storage);
  void Initialize();
  void TickLoop();
  void ClientLoop();
  void HandleConnection(int socket_fd);
  void MaybeCreateSnapshot();
  void ReapClientWorkers();
  void ReconcileActiveWrite();

  auto HandleWrite(const ClientWriteRequestV1 &request) -> ClientResponseV1;
  auto HandleRead(const ClientReadRequestV1 &request) -> ClientResponseV1;
  auto HandleStatus(const ClientStatusRequestV1 &request) -> ClientResponseV1;
  auto MakeResponse(uint64_t request_id, ClientResponseStatus status, std::vector<std::byte> payload = {}) const
      -> ClientResponseV1;

  DistributedNodeConfig config_;
  std::shared_ptr<DurableStorage> storage_;
  std::unique_ptr<NodeDirectory> directory_;
  std::shared_ptr<TcpRaftTransport> transport_;
  std::shared_ptr<BusTubRaftStateMachine> state_machine_;
  std::unique_ptr<RaftNode> raft_node_;

  mutable std::mutex mutex_;
  std::condition_variable state_changed_;
  std::exception_ptr fatal_error_;
  struct ActiveWrite {
    uint64_t client_id_;
    uint64_t request_id_;
    RequestFingerprintV1 request_fingerprint_;
    uint64_t proposal_index_;
    uint64_t proposal_term_;
  };
  std::optional<ActiveWrite> active_write_;
  uint64_t next_read_context_{0};
  uint64_t logical_now_ms_{0};
  int client_listen_fd_{-1};
  TcpEndpoint bound_client_endpoint_;
  std::atomic<bool> running_{false};
  std::thread tick_thread_;
  std::thread client_thread_;
  std::mutex client_workers_mutex_;
  struct ClientWorker {
    std::thread thread_;
    std::shared_ptr<std::atomic<bool>> finished_;
  };
  std::vector<ClientWorker> client_workers_;
};

}  // namespace bustub
