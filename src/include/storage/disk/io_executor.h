//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// io_executor.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>  // NOLINT(build/c++11)
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "storage/disk/block_device.h"

namespace bustub {

enum class IOOperation { Read, Write };

struct IORequest {
  IOOperation operation_;
  StorageByteRange range_;
};

struct IOExecutorOptions {
  size_t worker_count_;
  size_t max_operations_;
  // Includes alignment padding, prepared/in-flight buffers and retained results.
  size_t max_buffer_bytes_;
};

enum class IOAdmission { Accepted, Full, Stopped };
enum class IOBatchPhase { Prepared, Queued, Executing, Flushing, Draining, Succeeded, Failed };
enum class IOOutcome { NotStarted, Succeeded, Failed };

struct IOOperationResult {
  IOOutcome outcome_{IOOutcome::NotStarted};
  // As in F01: positive syscall returns, not proof the rest of a failed write is unchanged.
  uint64_t completed_bytes_{0};
  std::exception_ptr error_;
};

struct IOBatchResult {
  // Indexed in the original request order, regardless of execution order.
  std::vector<IOOperationResult> operations_;
  std::exception_ptr flush_error_;
  // True only after all member writes and the requested Flush succeed.
  // False does NOT prove that no bytes reached persistent storage.
  bool writes_durable_{false};
};

struct IOBatchData;
class IOExecutor;

/**
 * Exclusive, move-only permission to use an external byte range. ForRead permits
 * device IO to fill RAM; ForWrite permits IO to read immutable RAM. The caller
 * must keep storage alive and enforce that access mode until release runs (pin
 * alone is insufficient). The callback must retain any required owner, must not
 * throw or block, and may run on a worker or the thread destroying an unsubmitted
 * lease. It must not destroy/Shutdown the executor or unlock a thread-owned lock.
 */
class IOBufferLease {
 public:
  static auto ForRead(void *buffer, size_t capacity, std::function<void()> release) -> IOBufferLease;
  static auto ForWrite(const void *buffer, size_t capacity, std::function<void()> release) -> IOBufferLease;
  ~IOBufferLease();
  IOBufferLease(IOBufferLease &&other) noexcept;
  auto operator=(IOBufferLease &&other) noexcept -> IOBufferLease &;
  IOBufferLease(const IOBufferLease &) = delete;
  auto operator=(const IOBufferLease &) -> IOBufferLease & = delete;

 private:
  friend class IOExecutor;
  friend struct IOBatchData;
  IOBufferLease(IOOperation operation, const void *buffer, size_t capacity, std::function<void()> release);
  void Release() noexcept;
  IOOperation operation_;
  const void *buffer_;
  size_t capacity_;
  std::function<void()> release_;
};

/**
 * Move-only owner of the batch's reserved buffers and result slots. Releasing
 * this handle abandons observation, not already accepted IO. Workers retain it
 * until safe. Retained results consume member slots and owned-buffer bytes, but
 * external leases/borrowed capacity are returned before terminal publication.
 * Methods on a live handle may be used concurrently except move/destruction and
 * user access to buffer bytes, which the caller must synchronize.
 */
class IOBatch {
 public:
  ~IOBatch();
  IOBatch(IOBatch &&other) noexcept;
  auto operator=(IOBatch &&other) noexcept -> IOBatch &;
  IOBatch(const IOBatch &) = delete;
  auto operator=(const IOBatch &) -> IOBatch & = delete;

  /**
   * Exactly range.Size() bytes, aligned to this executor's device. Accessible
   * only before Submit or after terminal completion. Stop using all saved
   * pointers before Submit; a wait timeout does not return buffer ownership.
   * External batches never expose addresses through this owned-buffer method.
   */
  auto Buffer(size_t member) -> char *;
  auto Phase() const -> IOBatchPhase;
  void Wait() const;
  auto WaitFor(std::chrono::milliseconds timeout) const -> bool;
  /** Only terminal results are accessible; references live as long as this handle. */
  auto Result() const -> const IOBatchResult &;

 private:
  friend class IOExecutor;
  explicit IOBatch(std::shared_ptr<IOBatchData> data);
  auto Data() const -> IOBatchData &;
  std::shared_ptr<IOBatchData> data_;
};

struct IOPreparation {
  IOAdmission admission_;
  std::optional<IOBatch> batch_;
};

/**
 * F02 S1: trusted, already-resolved device ranges. Object mapping/permissions
 * arrive in S2/S6; this interface is not an untrusted StorageObject API.
 *
 * One existing device must outlive the executor; Close it only after Shutdown.
 * This executor does not format, close, or switch its access mode. Conflicting
 * ranges across batches/executors are ordered by the caller (F17/F26).
 */
class IOExecutor {
 public:
  IOExecutor(BlockDevice &device, const IOExecutorOptions &options);
  /** Explicit external-buffer capacity limit; zero disables external admission.
   * Counts each lease's full capacity as retention pressure, not new RAM usage.
   * The two-argument constructor only enables the existing owned-buffer path.
   * This overload also permits a zero owned-byte limit when external is enabled.
   */
  IOExecutor(BlockDevice &device, const IOExecutorOptions &options, size_t max_external_buffer_bytes);
  ~IOExecutor();
  IOExecutor(const IOExecutor &) = delete;
  auto operator=(const IOExecutor &) -> IOExecutor & = delete;

  /**
   * Non-waiting admission. Reserve all member slots, result storage and aligned
   * buffers before returning Prepared. Empty/invalid/conflicting member ranges
   * throw; Full/Stopped do no IO. System allocation failures propagate.
   * flush_after_writes requires at least one write. It is not a transaction.
   */
  auto TryPrepare(std::vector<IORequest> requests, bool flush_after_writes) -> IOPreparation;
  /**
   * Same range/ordering rules, with one matching lease per request. Accepted
   * consumes leases (empties the vector); every rejection/exception leaves them
   * with the caller. Never falls back to owned buffers. RAM ranges must not
   * overlap if either permission allows filling RAM; cross-batch conflicts are
   * the owner's responsibility. Member slots are shared with owned batches.
   */
  auto TryPrepareExternal(std::vector<IORequest> requests, std::vector<IOBufferLease> &leases, bool flush_after_writes)
      -> IOPreparation;
  /**
   * Accepted transfers buffer access to workers. All capacity was reserved in
   * TryPrepare; no wait for queue space. Stopped leaves the batch Prepared.
   * Allocation failure leaves it Prepared; repeated/cross-executor Submit throws.
   */
  auto TrySubmit(IOBatch &batch) -> IOAdmission;
  /** Stop accepting, drain accepted work and join workers. No forced IO cancellation. */
  void Shutdown();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
