#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "raft/raft_types.h"
#include "storage/disk/resource_budget.h"

namespace bustub {

/** Internal bounded executor. Admission owns both work and its completion cell. */
class SnapshotTasks {
 public:
  explicit SnapshotTasks(std::shared_ptr<ResourceBudget> memory);
  ~SnapshotTasks();
  using Result = std::shared_ptr<InstallSnapshotRequest>;
  using Future = std::optional<std::future<Result>>;
  auto Submit(std::function<InstallSnapshotRequest()> work) -> Future;
  auto Decode(const InstallSnapshotRequest &request) -> Future;

 private:
  void Run();
  auto Reserve() -> std::shared_ptr<ResourceCharge>;
  auto Enqueue(std::shared_ptr<ResourceCharge> charge, std::function<InstallSnapshotRequest()> work) -> Future;
  std::shared_ptr<ResourceAccount> memory_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<std::packaged_task<Result()>> queue_;
  size_t outstanding_{0};
  bool closing_{false};
  std::vector<std::thread> threads_;
};

// Only these worker operations touch compression. The RPC codec preserves encoded bytes.
void CompressSnapshotChunk(InstallSnapshotRequest *request, uint64_t session);
void DecompressSnapshotChunk(InstallSnapshotRequest *request);

}  // namespace bustub
