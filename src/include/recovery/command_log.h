//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// command_log.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "recovery/durable_storage.h"
#include "recovery/log_codec.h"

namespace bustub {

struct CommandLogOptions {
  size_t segment_max_bytes_{16U * 1024U * 1024U};
  /**
   * One Append call is one durability batch and therefore stays in one segment.
   * The default admits every individually valid LogCodec frame while bounding
   * the temporary encoding and the largest segment that recovery must read.
   */
  size_t batch_max_bytes_{LogCodec::MAX_PAYLOAD_BYTES + LogCodec::FRAME_HEADER_BYTES +
                          LogCodec::FRAME_BODY_FIXED_BYTES};
};

/** Segmented durable log used by single-node replay and as the Raft LogStore foundation. */
class CommandLog {
 public:
  static auto Open(const std::filesystem::path &directory, std::shared_ptr<DurableStorage> storage,
                   uint64_t effective_commit_index, uint64_t snapshot_base_index = 0, uint64_t snapshot_base_term = 0,
                   CommandLogOptions options = {}) -> std::unique_ptr<CommandLog>;

  /** Returns only after the entire batch crosses one fdatasync boundary. */
  void Append(const std::vector<ReplicatedLogEntry> &entries);

  /** Durably discard a complete, uncommitted suffix while preserving any retained fallback bridge prefix. */
  void TruncateSuffix(uint64_t last_index_to_keep);

  /** Delete only physical segments wholly covered by the oldest retained snapshot. */
  void CompactPrefix(uint64_t compact_through);

  auto LastLogIndex() const -> uint64_t;
  auto SnapshotBaseIndex() const -> uint64_t { return snapshot_base_index_; }
  auto SnapshotBaseTerm() const -> uint64_t { return snapshot_base_term_; }
  auto TermAt(uint64_t index) const -> std::optional<uint64_t>;
  auto EntryAt(uint64_t index) const -> std::optional<ReplicatedLogEntry>;
  auto Entries(uint64_t first_index, uint64_t last_index) const -> std::vector<ReplicatedLogEntry>;

 private:
  struct Segment {
    uint64_t first_index_;
    uint64_t last_index_;
    std::filesystem::path path_;
    size_t size_;
  };

  CommandLog(std::filesystem::path directory, std::shared_ptr<DurableStorage> storage, uint64_t snapshot_base_index,
             uint64_t snapshot_base_term, CommandLogOptions options)
      : directory_(std::move(directory)),
        storage_(std::move(storage)),
        snapshot_base_index_(snapshot_base_index),
        snapshot_base_term_(snapshot_base_term),
        options_(options) {}

  void Recover(uint64_t effective_commit_index);
  static auto SegmentFileName(uint64_t first_index) -> std::string;

  std::filesystem::path directory_;
  std::shared_ptr<DurableStorage> storage_;
  uint64_t snapshot_base_index_;
  uint64_t snapshot_base_term_;
  CommandLogOptions options_;

  mutable std::mutex mutex_;
  std::vector<Segment> segments_;
  std::vector<ReplicatedLogEntry> entries_;
};

}  // namespace bustub
