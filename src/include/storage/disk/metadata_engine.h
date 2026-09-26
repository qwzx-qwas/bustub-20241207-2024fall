//===----------------------------------------------------------------------===//
// BusTub: node-local metadata transactions, independent of the SQL database.
//===----------------------------------------------------------------------===//
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "storage/disk/journal_service.h"

namespace bustub {

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

struct MetadataOptions {
  uint32_t page_limit_;
  // Counts immutable published/reader-held and private page images together.
  // S4 retains the current pages in RAM; eviction/checkpoint arrive in S5.
  uint32_t max_live_pages_;
  uint32_t max_value_bytes_;
  uint64_t max_batch_bytes_;
};

enum class MetadataErrorCode { InvalidFormat, Corrupt, ResourceUnavailable, Conflict, NotReady };

class MetadataError : public std::runtime_error {
 public:
  MetadataError(MetadataErrorCode code, const std::string &message) : std::runtime_error(message), code_(code) {}
  auto Code() const -> MetadataErrorCode { return code_; }

 private:
  MetadataErrorCode code_;
};

enum class MetadataWritebackOutcome { Clean, Durable, Failed };

struct MetadataWritebackResult {
  MetadataWritebackOutcome outcome_;
  // Selected pages, not a count of successful writes after a failure.
  size_t page_count_;
  std::exception_ptr error_;
};

struct MetadataVersion;

/** Immutable committed view. It may outlive the engine and is safe for concurrent
 * reads. Retaining old views consumes the engine's live-page budget. Scan returns
 * at most limit entries, in unsigned (category, owner, item) order, starting at
 * lower inclusive. Values are owned copies; no internal mutable pages escape.
 */
class MetadataSnapshot {
 public:
  auto Get(const MetadataKey &key) const -> std::optional<std::vector<std::byte>>;
  auto Scan(const MetadataKey &lower, size_t limit) const -> std::vector<MetadataEntry>;

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
 * Create initializes a new Journal; Open rebuilds B from its complete batches.
 * Writeback persists final Metadata slots; Open still replays all WAL (no
 * checkpoint or trimming yet). Page/value limits are
 * persisted, checked on Open, and bounded by F05 capacity. max_live_pages and
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
  auto Commit(const MetadataSnapshot &base, const std::vector<MetadataMutation> &mutations) -> JournalResult;
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
  void Close();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bustub
