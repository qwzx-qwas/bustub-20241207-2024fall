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
#include <set>
#include <string>
#include <thread>
#include <unordered_map>  // NOLINT(build/c++11)
#include <vector>

#include "distributed/client_protocol.h"
#include "distributed/raft_state_machine.h"
#include "raft/object_storage.h"
#include "raft/raft_node.h"
#include "raft/tcp_transport.h"

namespace bustub {

struct DistributedPeerConfig {
  TcpEndpoint raft_endpoint_;
  TcpEndpoint client_endpoint_;
};

struct PageCacheDeployment {
  uint32_t pages_per_object_;
  BufferPoolOptions cache_;
};
struct RaftObjectDeployment {
  NodeStorageOptions storage_;
  uint64_t space_;
  RaftObjectOptions raft_;
  // Object nodes require explicit A page geometry/cache budgets from S9.1c.
  std::optional<PageCacheDeployment> pages_{std::nullopt};
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

  // Explicitly provisioned device/namespace. Empty preserves existing file deployments.
  std::optional<RaftObjectDeployment> object_storage_{std::nullopt};

  // The whole window includes blocked, executing and completed-but-unconsumed requests.
  size_t max_write_requests_{32};
  size_t max_write_bytes_{32U * 1024U * 1024U};
  size_t max_write_command_bytes_{1024U * 1024U};

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
  struct ActiveWrite;
  auto AcquireWrite(const std::shared_ptr<ActiveWrite> &active) -> bool;
  void ReleaseWrite(const std::shared_ptr<ActiveWrite> &active);
  void FinishWrite(const std::shared_ptr<ActiveWrite> &active, ClientResponseStatus status,
                   std::vector<std::byte> bytes = {});

  auto HandleWrite(const ClientWriteRequestV1 &request) -> ClientResponseV1;
  auto HandleRead(const ClientReadRequestV1 &request) -> ClientResponseV1;
  auto HandleStatus(const ClientStatusRequestV1 &request) -> ClientResponseV1;
  auto MakeResponse(uint64_t request_id, ClientResponseStatus status, std::vector<std::byte> payload = {}) const
      -> ClientResponseV1;

  DistributedNodeConfig config_;
  std::shared_ptr<DurableStorage> storage_;
  std::shared_ptr<NodeStorage> local_storage_;
  std::shared_ptr<RaftObjectStorage> object_storage_;
  std::unique_ptr<NodeDirectory> directory_;
  std::shared_ptr<TcpRaftTransport> transport_;
  std::shared_ptr<BusTubRaftStateMachine> state_machine_;

  mutable std::mutex mutex_;
  std::condition_variable state_changed_;
  std::exception_ptr fatal_error_;
  struct WriteWork {
    enum class Phase { ANALYZE, PREPARE, RESULT };
    Phase phase_;
    RequestDisposition disposition_{RequestDisposition::NEW_REQUEST};
    std::vector<std::byte> bytes_;
    std::optional<SqlWritePlan> plan_;
    bool stale_{false};
  };
  std::weak_ptr<TaskExecutor::Result<WriteWork>> analysis_work_;
  struct ActiveWrite {
    uint64_t sequence_, client_id_, request_id_;
    RequestFingerprintV1 request_fingerprint_;
    std::string sql_;
    size_t bytes_{0}, command_limit_{0};
    ResourceCharge charge_;
    uint64_t proposal_index_{0}, proposal_term_{0};
    bool claimed_{false};
    uint64_t blocker_{0};
    std::set<uint64_t> waiters_;
    std::shared_ptr<const SqlWritePlan> plan_;
    std::shared_ptr<TaskExecutor::Result<WriteWork>> work_;
    std::optional<WriteWork> prepared_;
    std::optional<ClientResponseV1> response_;
  };
  struct TableWrites {
    size_t plans_{0};
    uint64_t wide_owner_{0};
    std::unordered_map<std::string, uint64_t> keys_;
    std::set<uint64_t> active_, wide_;
  };
  std::map<uint64_t, std::shared_ptr<ActiveWrite>> writes_;
  std::unordered_map<uint64_t, uint64_t> clients_;
  std::unordered_map<table_oid_t, TableWrites> table_writes_;
  std::set<uint64_t> catalog_writes_;
  uint64_t next_write_{0};
  size_t write_bytes_{0};
  std::shared_ptr<ResourceAccount> write_memory_;
  bool snapshot_draining_{false};
  uint64_t next_read_context_{0};
  uint64_t logical_now_ms_{0};
  std::atomic<bool> work_ready_{false};
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
  // Destroy executors before their completion wake-up targets, including when
  // Stop's protocol drain reports a storage failure.
  std::unique_ptr<RaftNode> raft_node_;
};

}  // namespace bustub
