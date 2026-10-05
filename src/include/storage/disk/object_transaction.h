//===----------------------------------------------------------------------===//
// BusTub: bounded Common object transactions, independent of Raft commit.
//===----------------------------------------------------------------------===//
#pragma once

#include <chrono>  // NOLINT(build/c++11)
#include <memory>
#include <optional>
#include <vector>

#include "storage/disk/io_executor.h"
#include "storage/disk/object_mapping.h"

namespace bustub {
enum class ObjectOperation { Create, Write, Append, Resize, Remove, Unmap };
/** Stable immutable source. Owner retains both memory and permission to read it
 * through Common publication. The caller must not mutate the bytes meanwhile. */
struct ObjectWriteSource {
  const void *data_;
  size_t size_;
  size_t capacity_;
  std::shared_ptr<void> owner_;
};
struct ObjectMutation {
  ObjectOperation operation_;
  ObjectKey object_;
  uint64_t offset_;               // Write offset; Create/Resize length; zero for Append/Remove.
  ObjectSizeMode mode_;           // Used only by Create.
  std::vector<std::byte> bytes_;  // Nonempty only for Write/Append.
  std::optional<ObjectWriteSource> source_{std::nullopt};  // Alternative to owned input.
  uint64_t length_{0};       // Unmap only: remove a range without changing object length or erasing bytes.
  bool common_only_{false};  // Log/snapshot writers avoid another full-body Journal copy.
  auto Size() const -> size_t { return source_ ? source_->size_ : bytes_.size(); }
  auto Data() const -> const void * { return source_ ? source_->data_ : bytes_.data(); }
};
struct ObjectControlMutation {
  ObjectKey owner_;
  uint64_t item_;
  std::optional<std::vector<std::byte>> value_;
};
struct ObjectTransaction {
  // At most one operation per object, and one mutation per control key.
  std::vector<ObjectMutation> objects_;
  std::vector<ObjectControlMutation> controls_;
};
struct ObjectTransactionOptions {
  size_t max_requests_;  // Includes pending, executing and retained result tickets.
  size_t max_operations_;
  uint64_t max_request_bytes_;
  uint64_t max_pending_bytes_;
  std::chrono::milliseconds retry_interval_;
  std::chrono::milliseconds gc_interval_;
  uint64_t deferred_max_bytes_{0};  // Explicit threshold on assembled object bytes; zero selects Common.
  uint64_t deferred_pending_bytes_{0};
  size_t deferred_pending_tasks_{0};
};
struct ObjectTransactionData;
class ObjectTransactionPipeline;
/** Accepted is an in-memory execution handoff, not durable success. A timeout
 * or dropped ticket never cancels submitted IO. Result uses JournalOutcome:
 * Durable means both recovery dependencies and publication are complete;
 * NotCommitted permits a fresh attempt; Indeterminate requires recovery.
 */
class ObjectTransactionTicket {
 public:
  void Wait() const;
  auto WaitFor(std::chrono::milliseconds timeout) const -> bool;
  auto Result() const -> JournalResult;

 private:
  friend class ObjectTransactionPipeline;
  explicit ObjectTransactionTicket(std::shared_ptr<ObjectTransactionData> data);
  std::shared_ptr<ObjectTransactionData> data_;
};
struct ObjectTransactionSubmission {
  IOAdmission admission_;
  std::optional<ObjectTransactionTicket> ticket_;
};
}  // namespace bustub
