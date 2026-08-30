//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// snapshot_store.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "recovery/durable_storage.h"

namespace bustub {

struct RaftSnapshot {
  uint32_t format_version_{1};
  uint64_t generation_{0};
  std::string snapshot_id_;
  uint64_t last_included_index_{0};
  uint64_t last_included_term_{0};
  uint64_t payload_size_{0};
  uint32_t payload_checksum_{0};
};

struct SnapshotChunk {
  std::string snapshot_id_;
  uint64_t last_included_index_{0};
  uint64_t last_included_term_{0};
  uint64_t offset_{0};
  uint64_t total_size_{0};
  uint32_t payload_checksum_{0};
  bool done_{false};
  std::vector<std::byte> data_;
};

enum class SnapshotStageStatus { IN_PROGRESS, COMPLETE, DUPLICATE_COMPLETE };

struct SnapshotStageResult {
  SnapshotStageStatus status_;
  /** First byte not yet durably staged for this snapshot. */
  uint64_t next_offset_;
};

/** Versioned, checksummed full-state snapshot publication and chunk staging for the V1 experiment. */
class SnapshotStore {
 public:
  /** Course-scale file safety bound, not an allocation or a production capacity claim. */
  static constexpr uint64_t MAX_SNAPSHOT_BYTES = 1ULL << 30U;
  static constexpr size_t STREAM_CHUNK_BYTES = 1U * 1024U * 1024U;

  static auto Open(std::filesystem::path directory, std::shared_ptr<DurableStorage> storage)
      -> std::unique_ptr<SnapshotStore>;

  auto Latest() const -> std::optional<RaftSnapshot> { return latest_; }
  /** Oldest snapshot that still has to remain recoverable with the local bridge log. */
  auto OldestRetained() const -> std::optional<RaftSnapshot> { return previous_.has_value() ? previous_ : latest_; }
  /** Drop the previous generation after the caller has durably rebased recovery on the verified latest image. */
  void RetainOnlyLatest();
  /** Compatibility helper for small tests. The formal runtime path uses PublishFile. */
  auto Publish(uint64_t last_included_index, uint64_t last_included_term, const std::vector<std::byte> &payload,
               bool retain_previous = true) -> RaftSnapshot;
  auto PublishFile(uint64_t last_included_index, uint64_t last_included_term, const std::filesystem::path &payload_path,
                   bool retain_previous = true) -> RaftSnapshot;

  /** Stable test/transport view: reads at most maximum_size bytes from the immutable payload. */
  auto ReadPayloadChunk(const RaftSnapshot &snapshot, uint64_t offset, size_t maximum_size) -> std::vector<std::byte>;
  auto PayloadFile(const RaftSnapshot &snapshot) -> DurableFileSlice;
  /** Returns a store-owned scratch path that a state machine may fill sequentially. */
  auto PrepareCapturePath() -> std::filesystem::path;
  void CancelCapture();

  auto StageChunk(const SnapshotChunk &chunk) -> SnapshotStageResult;
  auto Staged(std::string_view snapshot_id) const -> std::optional<RaftSnapshot>;
  auto StagedPayloadFile(std::string_view snapshot_id) const -> std::optional<DurableFileSlice>;
  void CancelStaged(std::string_view snapshot_id);

 private:
  struct Download {
    std::string snapshot_id_;
    uint64_t last_included_index_;
    uint64_t last_included_term_;
    uint64_t total_size_;
    uint32_t payload_checksum_;
    bool complete_{false};
    uint64_t received_size_{0};
  };

  struct SnapshotFileView {
    RaftSnapshot snapshot_;
    uint64_t payload_offset_;
  };

  SnapshotStore(std::filesystem::path directory, std::shared_ptr<DurableStorage> storage)
      : directory_(std::move(directory)),
        current_path_(directory_ / "CURRENT"),
        download_path_(directory_ / "SNAPSHOT-DOWNLOAD.tmp"),
        capture_path_(directory_ / "SNAPSHOT-CAPTURE.tmp"),
        storage_(std::move(storage)) {}

  static auto EncodeCurrent(const RaftSnapshot &snapshot, std::string_view file_name, uint32_t file_checksum)
      -> std::vector<std::byte>;
  auto ReadSnapshotFile(uint64_t generation) -> SnapshotFileView;
  void WriteSnapshotFile(const RaftSnapshot &snapshot, const std::filesystem::path &payload_path,
                         const std::filesystem::path &output_path);
  auto ChecksumRange(const std::filesystem::path &path, uint64_t offset, uint64_t size, uint32_t initial_checksum = 0)
      -> uint32_t;
  void PruneSnapshots();
  void Recover();

  std::filesystem::path directory_;
  std::filesystem::path current_path_;
  std::filesystem::path download_path_;
  std::filesystem::path capture_path_;
  std::shared_ptr<DurableStorage> storage_;
  std::optional<RaftSnapshot> latest_;
  std::optional<RaftSnapshot> previous_;
  std::optional<Download> download_;
  std::map<uint64_t, uint64_t> payload_offsets_;
};

}  // namespace bustub
