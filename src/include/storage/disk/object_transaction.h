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
enum class ObjectOperation { Create, Write, Append, Resize, Remove };
struct ObjectMutation {
  ObjectOperation operation_;
  ObjectKey object_;
  uint64_t offset_;               // Write offset; Create/Resize length; zero for Append/Remove.
  ObjectSizeMode mode_;           // Used only by Create.
  std::vector<std::byte> bytes_;  // Nonempty only for Write/Append.
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
