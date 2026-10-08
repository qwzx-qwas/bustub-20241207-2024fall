// Raft-owned object namespace. The caller opens B/object IO before this context.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/disk/node_storage.h"

namespace bustub {
struct RaftObjectOptions {
  uint64_t segment_bytes_;
  uint64_t io_chunk_bytes_;
  uint64_t max_log_bytes_;
  size_t max_log_entries_;
  size_t max_batch_entries_;
  uint64_t max_batch_bytes_;
  uint64_t max_snapshot_bytes_;
  size_t max_owned_objects_;
};
class ObjectLogStore;
class ObjectSnapshotStore;
class StableStore;
class LogStore;
/** Explicit Create/Open, no migration or format fallback. One context per Raft
 * namespace, shared by the three stores. NodeStorage outlives all sessions.
 * Maintenance is bounded and scheduled by NodeStorage's existing GC role. */
class RaftObjectStorage {
 public:
  static auto Create(std::shared_ptr<NodeStorage> storage, uint64_t space, RaftObjectOptions options)
      -> std::shared_ptr<RaftObjectStorage>;
  static auto Open(std::shared_ptr<NodeStorage> storage, uint64_t space, RaftObjectOptions options)
      -> std::shared_ptr<RaftObjectStorage>;
  /** Bind a provisioned namespace to its static Raft member before recovery. */
  void EnsureIdentity(uint64_t node, const std::string &group, const std::vector<uint64_t> &voters);
  /** Perform at most limit log-cleaning / ownership-retirement steps. Each
   * unmap/trim touches at most one mapping and io_chunk_bytes_ bytes; relocation
   * also observes the existing batch limits. Never remove a leased body.
   * Returns the number of completely removed bodies, not partial steps.
   * One serialized maintenance caller per context. */
  auto Collect(size_t limit) -> size_t;

 private:
  friend class ObjectLogStore;
  friend class ObjectSnapshotStore;
  friend class StableStore;
  friend class LogStore;
  struct State;
  RaftObjectStorage(std::shared_ptr<NodeStorage> storage, uint64_t space, RaftObjectOptions options);
  auto Key(uint64_t number) const -> ObjectKey { return {space_, number}; }
  auto Control(uint64_t owner, uint64_t item) -> std::optional<std::vector<std::byte>>;
  auto Scan(uint64_t owner) -> std::vector<ObjectControlEntry>;
  auto Change(uint64_t owner, uint64_t item, std::optional<std::vector<std::byte>> value) -> ObjectControlMutation;
  void Commit(ObjectTransaction transaction);
  void Check(const std::vector<ObjectControlMutation> &controls) const;
  auto Candidate(uint64_t kind) -> uint64_t;
  auto Own(uint64_t object, uint64_t kind, uint64_t status) -> ObjectControlMutation;
  auto Lease(uint64_t object) -> std::shared_ptr<void>;
  void Append(uint64_t object, const std::vector<std::byte> &bytes);
  auto Read(uint64_t object, uint64_t offset, size_t size) -> std::vector<std::byte>;
  void AttachLog(const std::shared_ptr<ObjectLogStore> &log);
  std::shared_ptr<NodeStorage> storage_;
  uint64_t space_;
  RaftObjectOptions options_;
  std::shared_ptr<State> state_;
  std::mutex allocation_mutex_;
  uint64_t next_object_{16};
  std::atomic<size_t> owned_objects_{0};
  uint64_t gc_cursor_{0};
  std::weak_ptr<ObjectLogStore> log_;
  bool log_turn_{true};
};
}  // namespace bustub
