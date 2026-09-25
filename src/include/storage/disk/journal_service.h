//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// journal_service.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <chrono>  // NOLINT(build/c++11)
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/disk/bootstrap_store.h"

namespace bustub {

using JournalIdentity = std::array<uint8_t, 16>;
using JournalRecords = std::vector<std::vector<std::byte>>;

struct JournalOptions {
  uint64_t segment_bytes_;
  uint32_t unit_bytes_;
  uint32_t max_record_bytes_;
  uint64_t max_batch_bytes_;
  uint32_t max_records_per_batch_;
  uint64_t max_group_bytes_;
  size_t max_group_batches_;
  size_t max_pending_batches_;
  uint64_t max_pending_bytes_;
};

enum class JournalErrorCode {
  InvalidFormat,
  IdentityMismatch,
  Corrupt,
  NotEmpty,
  ResourceUnavailable,
  ExecutorStopped
};

class JournalError : public std::runtime_error {
 public:
  JournalError(JournalErrorCode code, const std::string &message) : std::runtime_error(message), code_(code) {}
  auto Code() const -> JournalErrorCode { return code_; }

 private:
  JournalErrorCode code_;
};

enum class JournalAdmission { Accepted, Full, NoSpace, Stopped, Faulted };
enum class JournalOutcome { Durable, NotCommitted, Indeterminate };

struct JournalResult {
  JournalOutcome outcome_;
  // Journal-relative, aligned, exclusive end; begin is also the batch's LSN.
  uint64_t begin_;
  uint64_t end_;
  std::exception_ptr error_;
};

struct JournalTicketData;

/** Retaining a completed ticket still occupies one Journal result slot.
 * Wait timeout or dropping the ticket never cancels accepted IO.
 */
class JournalTicket {
 public:
  JournalTicket(JournalTicket &&) noexcept;
  auto operator=(JournalTicket &&) noexcept -> JournalTicket &;
  ~JournalTicket();
  JournalTicket(const JournalTicket &) = delete;
  auto operator=(const JournalTicket &) -> JournalTicket & = delete;
  void Wait() const;
  auto WaitFor(std::chrono::milliseconds timeout) const -> bool;
  auto Result() const -> JournalResult;

 private:
  friend class JournalService;
  explicit JournalTicket(std::shared_ptr<JournalTicketData> data);
  std::shared_ptr<JournalTicketData> data_;
};

struct JournalSubmission {
  JournalAdmission admission_;
  std::optional<JournalTicket> ticket_;
};

/** S3 append-only, single-copy Journal. No segment reuse/checkpoint/Deferred yet.
 * Sole owner of the bound Journal region. Explicit identity/geometry; Create
 * requires an empty region. Nonzero invalid tails prevent Open, never truncate.
 * The bootstrap/executor/device outlive this instance; drain Journal first.
 * The lifecycle owner serializes Create/Open/Close and keeps replay callbacks
 * non-reentrant. After Create/Open, TryAppend may run concurrently with Close.
 * Callback failure leaves Open failed; discard partially replayed consumer state.
 */
class JournalService {
 public:
  JournalService(BootstrapStore &bootstrap, const JournalIdentity &identity, const JournalOptions &options);
  ~JournalService();
  JournalService(const JournalService &) = delete;
  auto operator=(const JournalService &) -> JournalService & = delete;

  void Create();
  /** Validates the entire stream before replay, then re-Flushes before opening.
   * Replay receives only complete batches, bounded by persisted input limits.
   */
  void Open(const std::function<void(uint64_t, const JournalRecords &)> &replay);
  /** Copies nonempty records into reserved F02 buffers before returning.
   * Full/NoSpace/Stopped/Faulted do no IO; invalid input throws.
   */
  auto TryAppend(const JournalRecords &records) -> JournalSubmission;
  void Close();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
