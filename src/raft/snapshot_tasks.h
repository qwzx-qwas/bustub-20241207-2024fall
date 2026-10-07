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

namespace bustub {

/** Internal bounded executor. Admission owns both work and its completion cell. */
class SnapshotTasks {
 public:
  SnapshotTasks();
  ~SnapshotTasks();
  auto Submit(std::function<InstallSnapshotRequest()> work) -> std::optional<std::future<InstallSnapshotRequest>>;

 private:
  void Run();
  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<std::packaged_task<InstallSnapshotRequest()>> queue_;
  size_t outstanding_{0};
  bool closing_{false};
  std::vector<std::thread> threads_;
};

// Only these worker operations touch compression. The RPC codec preserves encoded bytes.
void CompressSnapshotChunk(InstallSnapshotRequest *request, uint64_t session);
void DecompressSnapshotChunk(InstallSnapshotRequest *request);

}  // namespace bustub
