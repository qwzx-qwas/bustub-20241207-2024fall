//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_store.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <utility>
#include <vector>

#include "recovery/durable_storage.h"
#include "recovery/log_codec.h"

namespace bustub {

struct LogStoreOptions {
  static constexpr size_t MAXIMUM_JOURNAL_BYTES = 512U * 1024U * 1024U;
  size_t maximum_journal_bytes_{512U * 1024U * 1024U};
};

struct LogStoreRecoveryProbe {
  uint64_t snapshot_base_index_{0};
  uint64_t snapshot_base_term_{0};
  uint64_t last_log_index_{0};
  std::optional<uint64_t> recovery_boundary_term_;
  std::optional<uint64_t> latest_boundary_term_;
};

/**
 * A logical Raft log backed by a checksummed mutation journal.
 *
 * Ordinary appends add one durable frame. Suffix replacement and snapshot-base
 * installation atomically publish a canonical journal, ensuring superseded
 * entries cannot reappear if a later frame is damaged.
 */
class LogStore {
 public:
  /** Parse and validate the committed journal without cleaning or rewriting any file. */
  static auto ProbeRecovery(const std::filesystem::path &directory, std::shared_ptr<DurableStorage> storage,
                            uint64_t effective_commit_index, uint64_t recovery_boundary_index,
                            uint64_t latest_boundary_index, LogStoreOptions options = {}) -> LogStoreRecoveryProbe;

  static auto Open(const std::filesystem::path &directory, std::shared_ptr<DurableStorage> storage,
                   uint64_t effective_commit_index, uint64_t published_snapshot_index = 0,
                   uint64_t published_snapshot_term = 0, LogStoreOptions options = {}) -> std::unique_ptr<LogStore>;

  /**
   * Atomically discard an untrusted journal and rebuild the logical base from a
   * snapshot that the caller has already fully validated. This escape hatch is
   * deliberately unavailable when committed log entries would be discarded.
   */
  static auto RebuildFromVerifiedSnapshot(const std::filesystem::path &directory,
                                          std::shared_ptr<DurableStorage> storage, uint64_t effective_commit_index,
                                          uint64_t snapshot_index, uint64_t snapshot_term, LogStoreOptions options = {})
      -> std::unique_ptr<LogStore>;

  /** Each mutator returns only after its complete logical mutation is durable. */
  void Append(const std::vector<ReplicatedLogEntry> &entries);
  void ReplaceSuffix(uint64_t from_index, const std::vector<ReplicatedLogEntry> &new_entries);

  /**
   * Establishes (index, term) as the new logical base. If retain_old_suffix is
   * true, the pre-install TermAt(index) must equal term and all entries > index
   * are kept. Otherwise all old entries are discarded. The caller must first
   * durably commit at least index in HardState; this call advances the cached
   * LogStore commit to index as part of that ordered transition.
   */
  void InstallSnapshotBase(uint64_t index, uint64_t term, bool retain_old_suffix);

  /** Called only after the matching HardState commit update is durable. */
  void AdvanceCommittedIndex(uint64_t committed_index);

  auto SnapshotBaseIndex() const -> uint64_t;
  auto SnapshotBaseTerm() const -> uint64_t;
  auto CommittedIndex() const -> uint64_t;
  auto LastLogIndex() const -> uint64_t;
  auto LastLogTerm() const -> uint64_t;
  auto TermAt(uint64_t index) const -> std::optional<uint64_t>;
  auto EntryAt(uint64_t index) const -> std::optional<ReplicatedLogEntry>;
  auto Entries(uint64_t first_index, uint64_t last_index) const -> std::vector<ReplicatedLogEntry>;

 private:
  enum class MutationType : uint32_t { APPEND = 1, REPLACE_SUFFIX = 2, INSTALL_SNAPSHOT_BASE = 3 };

  struct Mutation {
    MutationType type_;
    uint64_t argument_index_{0};
    uint64_t argument_term_{0};
    bool retain_old_suffix_{false};
    std::vector<ReplicatedLogEntry> entries_;
  };

  enum class DecodeStatus { COMPLETE, TRUNCATED, CORRUPT };

  struct DecodeResult {
    DecodeStatus status_;
    std::optional<Mutation> mutation_;
    size_t bytes_consumed_{0};
  };

  LogStore(std::filesystem::path directory, std::shared_ptr<DurableStorage> storage, uint64_t effective_commit_index,
           LogStoreOptions options)
      : directory_(std::move(directory)),
        journal_path_(directory_ / "LOG-MUTATIONS"),
        journal_temporary_path_(directory_ / "LOG-MUTATIONS.tmp"),
        storage_(std::move(storage)),
        effective_commit_index_(effective_commit_index),
        options_(options) {}

  static auto EncodeMutation(const Mutation &mutation) -> std::vector<std::byte>;
  static auto EncodeMutation(MutationType type, uint64_t argument_index, uint64_t argument_term, bool retain_old_suffix,
                             const std::vector<ReplicatedLogEntry> &entries) -> std::vector<std::byte>;
  static void AppendEncodedMutation(std::vector<std::byte> *output, MutationType type, uint64_t argument_index,
                                    uint64_t argument_term, bool retain_old_suffix,
                                    const std::vector<ReplicatedLogEntry> &entries);
  static auto EncodedMutationSize(const Mutation &mutation) -> size_t;
  static auto EncodedMutationSize(const std::vector<ReplicatedLogEntry> &entries) -> size_t;
  static auto DecodeMutation(const std::vector<std::byte> &bytes, size_t offset) -> DecodeResult;
  void Recover(uint64_t published_snapshot_index, uint64_t published_snapshot_term);
  void ValidateMutation(const Mutation &mutation, bool recovering) const;
  void ApplyMutation(const Mutation &mutation, bool recovering);
  void AppendMutation(const std::vector<ReplicatedLogEntry> &entries);
  void RewriteJournal(uint64_t snapshot_base_index, uint64_t snapshot_base_term,
                      const std::vector<ReplicatedLogEntry> &entries);
  void RewriteJournalFromCurrentState();
  auto TermAtUnlocked(uint64_t index) const -> std::optional<uint64_t>;
  auto LastLogIndexUnlocked() const -> uint64_t;
  void ValidateCommittedRange() const;

  std::filesystem::path directory_;
  std::filesystem::path journal_path_;
  std::filesystem::path journal_temporary_path_;
  std::shared_ptr<DurableStorage> storage_;
  uint64_t effective_commit_index_;
  LogStoreOptions options_;

  mutable std::mutex mutex_;
  uint64_t snapshot_base_index_{0};
  uint64_t snapshot_base_term_{0};
  std::vector<ReplicatedLogEntry> entries_;
};

}  // namespace bustub
