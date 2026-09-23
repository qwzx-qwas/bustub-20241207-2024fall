//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bootstrap_store.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <chrono>  // NOLINT(build/c++11)
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

#include "storage/byte_range.h"

namespace bustub {

class IOExecutor;

struct BootstrapIdentity {
  std::array<uint8_t, 16> storage_;
  std::array<uint8_t, 16> device_;
};

struct BootstrapLayout {
  BootstrapIdentity identity_;
  uint64_t capacity_;
  // Metadata, Journal, data, in that order. These are reserved regions, not objects.
  std::array<StorageByteRange, 3> regions_;
};

enum class BootstrapErrorCode {
  Unformatted,
  Corrupt,
  UnsupportedFormat,
  IdentityMismatch,
  InvalidLayout,
  ConflictingCopies,
  SourceChanged,
  NotEmpty,
  ResourceUnavailable,
  ExecutorStopped
};

/** Format/admission failures; device IO failures retain their original exception. */
class BootstrapError : public std::runtime_error {
 public:
  BootstrapError(BootstrapErrorCode code, const std::string &message) : std::runtime_error(message), code_(code) {}
  auto Code() const -> BootstrapErrorCode { return code_; }

 private:
  BootstrapErrorCode code_;
};

struct BootstrapOpenResult {
  BootstrapLayout layout_;
  bool redundancy_lost_;
};

enum class BootstrapRepairState { Idle, Pending, Running, Succeeded, Failed, Stopped };

struct BootstrapStatus {
  BootstrapRepairState repair_state_{BootstrapRepairState::Idle};
  bool redundancy_lost_{false};
  uint32_t attempts_{0};
  std::exception_ptr last_error_;
};

struct BootstrapRepairOptions {
  uint32_t max_attempts_;
  std::chrono::milliseconds retry_delay_;
  std::chrono::milliseconds admission_timeout_;
};

/**
 * F03 fixed-layout bootstrap. Create writes two 64 KiB slots at 0 and 1 MiB;
 * Open only reads. IDs are explicit, nonzero and compared against both valid
 * copies. Version 1 has immutable layout and no checkpoint/root publication yet.
 *
 * The lifecycle owner serializes Create/Open/StartRepair/Close and excludes all
 * other writers to these slots (including other BootstrapStore instances).
 * Status is concurrently readable. One successful Open fixes this instance's
 * identity/layout. Close joins its one optional repair coordinator, draining
 * accepted IO before returning. It cannot cancel a stalled device operation.
 * The executor and device must outlive this object; close them afterwards.
 */
class BootstrapStore {
 public:
  explicit BootstrapStore(IOExecutor &executor);
  ~BootstrapStore();
  BootstrapStore(const BootstrapStore &) = delete;
  auto operator=(const BootstrapStore &) -> BootstrapStore & = delete;

  /** Both entire slots must be zero. Success requires both writes and Flush.
   * Failure can leave a usable copy; never blindly retry Create or overwrite it.
   * Startup operations fail explicitly if the executor cannot admit their batch.
   */
  void Create(const BootstrapLayout &layout);
  auto Open(const BootstrapIdentity &expected) -> BootstrapOpenResult;
  /** Nonblocking, explicit positive limits. Repeated calls coalesce; a healthy
   * instance needs no coordinator. Failure is observable through Status.
   */
  void StartRepair(const BootstrapRepairOptions &options);
  auto Status() const -> BootstrapStatus;
  void Close();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
