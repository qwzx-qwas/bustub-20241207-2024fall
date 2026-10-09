//===----------------------------------------------------------------------===//
// BusTub: node-local metadata transactions, independent of the SQL database.
//===----------------------------------------------------------------------===//
#pragma once

#include "storage/disk/resource_budget.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/disk/journal_service.h"

namespace bustub {

struct RegionInfo;

struct MetadataKey {
  uint64_t category_;
  uint64_t owner_;
  uint64_t item_;
};

struct MetadataEntry {
  MetadataKey key_;
  std::vector<std::byte> value_;
};

struct MetadataMutation {
  MetadataKey key_;
  // nullopt removes the key; an empty vector is a present, empty value.
  std::optional<std::vector<std::byte>> value_;
};

/** Body stored once as a Journal record; B stores only its locator. Key category
 * is a caller-owned 56-bit namespace. nullopt retires the durable reference. */
struct MetadataPayloadMutation {
  MetadataKey key_;
  std::optional<std::vector<std::byte>> bytes_;
};
/** Decode the metadata layer's payload envelope after the Journal read completes. */
auto DecodeMetadataPayload(const JournalPayloadRead &read) -> std::vector<std::byte>;

struct MetadataOptions {
  uint32_t page_limit_;
  // Counts immutable published/reader-held and private page images together.
  // Clean, exactly reloadable bodies may be evicted; old overwritten versions remain pinned in RAM.
  uint32_t max_live_pages_;
  uint32_t max_value_bytes_;
  uint64_t max_batch_bytes_;
  std::shared_ptr<ResourceBudget> memory_budget_{};
};

enum class MetadataErrorCode { InvalidFormat, Corrupt, ResourceUnavailable, Conflict, NotReady };

class MetadataError : public std::runtime_error {
 public:
  MetadataError(MetadataErrorCode code, const std::string &message) : std::runtime_error(message), code_(code) {}
  auto Code() const -> MetadataErrorCode { return code_; }

 private:
  MetadataErrorCode code_;
};

/** Commit rejected a stale base before any WAL IO. Callers may rebuild their
 * mutations against a fresh view. This is distinct from a changed object's
 * semantic version conflict, which must not be retried with an obsolete plan. */
class MetadataViewConflict final : public MetadataError {
 public:
  MetadataViewConflict() : MetadataError(MetadataErrorCode::Conflict, "stale or foreign metadata view") {}
};

/** Shared memory, a checkpoint or admitted IO temporarily owns commit capacity.
 * Unlike an oversized batch or exhausted persistent space, this may be retried. */
class MetadataCommitBusy final : public MetadataError {
 public:
  explicit MetadataCommitBusy(const char *message) : MetadataError(MetadataErrorCode::ResourceUnavailable, message) {}
};

enum class MetadataWritebackOutcome { Clean, Durable, Failed };

struct MetadataWritebackResult {
  MetadataWritebackOutcome outcome_;
  // Selected pages, not a count of successful writes after a failure.
  size_t page_count_;
  std::exception_ptr error_;
};

enum class MetadataCheckpointOutcome { Durable, NotPublished, Indeterminate };
struct MetadataCheckpointResult {
  MetadataCheckpointOutcome outcome_;
  std::exception_ptr error_;
};

struct MetadataVersion;

/** Immutable committed view. Concurrent queries require the original engine to
 * remain open. Close rejects new queries and drains admitted calls; copied values
 * remain valid. Retaining overwritten old versions consumes the live-page budget. Scan returns
 * at most limit entries, in unsigned (category, owner, item) order, starting at
 * lower inclusive. Values are owned copies; no internal mutable pages escape.
 */
class MetadataSnapshot {
 public:
  auto Get(const MetadataKey &key) const -> std::optional<std::vector<std::byte>>;
  auto Scan(const MetadataKey &lower, size_t limit) const -> std::vector<MetadataEntry>;
  /** Greatest key <= upper in this immutable view; may belong to another prefix. */
  auto GetFloor(const MetadataKey &upper) const -> std::optional<MetadataEntry>;
  auto Payload(const MetadataKey &key) const -> std::optional<JournalPayload>;

 private:
  friend class MetadataEngine;
  explicit MetadataSnapshot(std::shared_ptr<const MetadataVersion> version);
  std::shared_ptr<const MetadataVersion> version_;
};

/** S4: fixed composite keys, variable values, private pages, one F07 WAL.
 * Owns this bootstrap's B and Journal regions exclusively. Bootstrap/executor/
 * device outlive it. Create/Open/Close are lifecycle-owner serialized; Read and
 * Commit may run concurrently after opening. A writer is serialized, but holds
 * no publication lock while waiting for Journal IO. Stale/foreign base views
 * reject before IO. Mutations in one call form one atomic transaction.
 *
 * Create initializes a new Journal. Open uses the bootstrap checkpoint, final
 * pages and subsequent FULL/PATCH records, or full history if no checkpoint exists.
 * Writeback persists final Metadata slots. New v2 Journals recycle whole prefix
 * segments after checkpoint publication; existing v1 Journals stay append-only.
 * Page/value limits are persisted, checked on Open, and bounded by F05 capacity. max_live_pages and
 * max_batch_bytes are runtime admission limits. Journal admission/encoding
 * rejection throws before this batch writes. Accepted IO returns its actual
 * Durable/NotCommitted/Indeterminate result; any failed accepted commit faults
 * this engine until reopen. Indeterminate is not permission to retry blindly.
 */
class MetadataEngine {
 public:
  MetadataEngine(BootstrapStore &bootstrap, const JournalIdentity &identity, const JournalOptions &journal_options,
                 const MetadataOptions &options);
  ~MetadataEngine();
  MetadataEngine(const MetadataEngine &) = delete;
  auto operator=(const MetadataEngine &) -> MetadataEngine & = delete;

  void Create();
  void Open();
  auto Read() const -> MetadataSnapshot;
  /** Check encoded input limits before the caller starts data IO; not a reservation. */
  void CheckBatch(const std::vector<MetadataMutation> &mutations) const;
  auto Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations) -> JournalResult;
  auto Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations,
              const std::vector<MetadataPayloadMutation> &payloads) -> JournalResult;
  /** Accumulates complete-batch read cost. Completion reports IO errors even if
   * the reader drops its ticket; ready runs after terminal publication. */
  auto ReadPayload(const JournalPayload &payload, IOReadBudget &budget,
                   std::function<void(const IOBatchResult &)> complete, std::function<void()> ready)
      -> JournalPayloadRead;
  /** F09: write at most max_pages committed pages, including their Flush.
   * Called by the maintenance owner, not implicitly on each Commit. Different
   * pages run on F02 workers; only one writeback call is admitted at a time.
   * Read/Commit can proceed during IO. Full/Stopped reject before page IO with
   * ResourceUnavailable/NotReady. Accepted failures return the original error,
   * retain dirty progress and never undo a prior durable Commit. No auto retry.
   * Clean means the observed view needs no writes; it is NOT a global barrier.
   * max_pages must be positive. Close drains an admitted call before returning.
   */
  auto Writeback(size_t max_pages) -> MetadataWritebackResult;
  /** One owner polls a bounded persistent-page/Journal check. False means IO
   * remains in flight; no caller waits here. Uses ordinary F02 credits. */
  auto ScrubStep() -> bool;
  /** F10/F11: excludes new modifications, drains existing work and writes pages
   * in batches of at most max_pages, then persists a checkpoint and its bootstrap
   * reference. Existing snapshots remain readable. A concurrent Commit rejects
   * with ResourceUnavailable. v2 also persists the retirement boundary after the
   * bootstrap reference; v1 retains all logs. No automatic retry.
   * Pre-IO admission errors throw. Page IO failure is NotPublished; an accepted
   * Journal failure also faults B. Bootstrap publication failure is Indeterminate
   * and faults B until reopen. Retirement control failure also isolates B with an
   * Indeterminate result. Durable includes the reference and required control,
   * not only the WAL. Open completes interrupted retirement before becoming ready.
   * Close drains an admitted checkpoint. max_pages must be positive.
   */
  auto Checkpoint(size_t max_pages) -> MetadataCheckpointResult;
  void Close();

 private:
  friend class DataAllocator;
  auto DataRegionInfo() const -> RegionInfo;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
