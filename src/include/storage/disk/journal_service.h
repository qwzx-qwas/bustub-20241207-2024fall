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

struct IOReadBudget;
struct IOBatchResult;

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
  RequestTooLarge,
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
  // Logical LSN range (end exclusive). Legacy format uses physical offsets.
  // New format positions need not be adjacent across segment boundaries.
  uint64_t begin_;
  uint64_t end_;
  std::exception_ptr error_;
};

struct JournalPayloadRef {
  uint64_t batch_;
  uint32_t record_;
  uint32_t bytes_;
};
struct JournalPayloadPin;
struct JournalPayload {
  JournalPayloadRef ref_;
  std::shared_ptr<JournalPayloadPin> retention_;
};
struct JournalPayloadReadData;
class JournalPayloadRead {
 public:
  void Wait() const;
  auto WaitFor(std::chrono::milliseconds timeout) const -> bool;
  auto Bytes() const -> std::vector<std::byte>;
  /** Check the same retained batch without allocating a second payload body. */
  void Verify() const;

 private:
  friend class JournalService;
  explicit JournalPayloadRead(std::shared_ptr<JournalPayloadReadData> data);
  auto Decode(bool copy) const -> std::vector<std::byte>;
  std::shared_ptr<JournalPayloadReadData> data_;
};

struct JournalTicketData;

/** Retaining a completed ticket still occupies one Journal result slot.
 * Wait timeout or dropping the ticket never cancels accepted IO.
 */
class JournalTicket {
 public:
  JournalTicket(JournalTicket &&other) noexcept;
  auto operator=(JournalTicket &&other) noexcept -> JournalTicket &;
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

/** Single-copy Journal. Create uses the recyclable v2 format; Open preserves v1
 * append-only compatibility. B checkpoints retire whole prefix segments in v2.
 * B owns maintenance and persistent payload references. Reads retain complete batches.
 * Sole owner of the bound Journal region. Explicit identity/geometry; Create
 * requires an empty region. Nonzero invalid tails prevent Open, never truncate.
 * Bootstrap/executor/device outlive this instance; drain Journal first. The lifecycle owner serializes
 * Create/Open/Close and keeps replay callbacks non-reentrant. After Create/Open, TryAppend may run concurrently with
 * Close. Callback failure leaves Open failed; discard partially replayed consumer state.
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
  /** F10/F11: the bootstrap owner supplies a trusted complete-batch logical position.
   * Inspect runs during validation, before the entire suffix is known valid:
   * collect a bounded recovery plan only, with no publication or durable effects.
   * Replay starts only after validation and Flush. Prefix payload is not replayed;
   * v2 also verifies segment headers. Control and the complete required suffix
   * (including its unused tail) are checked. A retired entry is rejected.
   */
  void OpenFrom(uint64_t begin, const std::function<void(uint64_t, const JournalRecords &)> &inspect,
                const std::function<void(uint64_t, const JournalRecords &)> &replay);
  /** Copies nonempty records into reserved F02 buffers before returning.
   * Full/NoSpace/Stopped/Faulted do no IO; invalid input throws.
   * Data plus the separate Flush must fit the work class's total IO capacity;
   * otherwise RequestTooLarge is thrown, rather than returning transient Full.
   */
  auto TryAppend(const JournalRecords &records) -> JournalSubmission;
  void Close();

 private:
  friend class MetadataEngine;
  // B is the sole append/maintenance owner and serializes this with its commit.
  auto NextAppendPosition() const -> uint64_t;
  void SetCompletionReserve(uint64_t units);
  auto AppendPayload(const JournalRecords &records, uint64_t reserve_units, bool completion) -> JournalSubmission;
  auto RetainPayload(JournalPayloadRef ref) -> JournalPayload;
  auto ReadPayload(const JournalPayload &payload, IOReadBudget &budget,
                   std::function<void(const IOBatchResult &)> complete, std::function<void()> ready)
      -> JournalPayloadRead;
  auto ScrubNext(uint64_t *cursor, uint64_t through) -> std::optional<JournalPayloadRead>;
  auto AppendCheckpoint(const JournalRecords &records) -> JournalSubmission;
  auto CheckpointRef(uint64_t lsn) const -> MetadataCheckpointRef;
  void RetireBefore(uint64_t checkpoint_lsn);
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
