#pragma once

#include "common/task_executor.h"

#include "raft/raft_types.h"
#include "storage/disk/resource_budget.h"

namespace bustub {

/** Internal bounded executor. Admission owns both work and its completion cell. */
class SnapshotTasks {
 public:
  explicit SnapshotTasks(std::shared_ptr<ResourceBudget> memory);

  void Drain() { tasks_.Drain(); }
  using Result = std::shared_ptr<InstallSnapshotRequest>;
  using Future = std::shared_ptr<TaskExecutor::Result<Result>>;
  auto Submit(std::function<InstallSnapshotRequest()> work) -> Future;
  auto Decode(const InstallSnapshotRequest &request) -> Future;

 private:
  auto Reserve() -> std::shared_ptr<ResourceCharge>;
  auto Enqueue(std::shared_ptr<ResourceCharge> charge, std::function<InstallSnapshotRequest()> work) -> Future;
  std::shared_ptr<ResourceAccount> memory_;
  TaskExecutor tasks_;
};

// Only these worker operations touch compression. The RPC codec preserves encoded bytes.
void CompressSnapshotChunk(InstallSnapshotRequest *request, uint64_t session);
void DecompressSnapshotChunk(InstallSnapshotRequest *request);

}  // namespace bustub
