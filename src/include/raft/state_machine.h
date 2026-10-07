//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// state_machine.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "recovery/durable_storage.h"
#include "recovery/log_codec.h"
#include "recovery/snapshot_stream.h"

namespace bustub {
class NodeStorage;
struct ObjectKey;
struct StateMachineRecoveryPoint {
  uint64_t index_, term_;
};

struct SharedSnapshotResult {
  uint64_t size_;
  uint32_t checksum_;
};
/** Owns a fully built, verified candidate until publication succeeds. Dropping
 * it cancels preparation without touching the active state. Install is called
 * once by the same serialized Raft owner, after durable publication. */
class PreparedSnapshot {
 public:
  virtual ~PreparedSnapshot() = default;
  virtual void Install() = 0;
};

class RaftStateMachine {
 public:
  virtual ~RaftStateMachine() = default;
  /** Read-only admission check that must complete before a proposal is appended. */
  virtual void ValidateProposalPayload(EntryType type, const std::vector<std::byte> &payload) const = 0;
  virtual void Apply(const ReplicatedLogEntry &entry) = 0;
  virtual auto LastApplied() const -> uint64_t = 0;
  /** Optional durable local base, validated against the committed Raft history. */
  virtual auto LocalRecoveryPoint() const -> std::optional<StateMachineRecoveryPoint> { return std::nullopt; }
  virtual void WriteSnapshot(const SnapshotAppend &append) const = 0;
  /** Optional local sharing into an empty, unpublished F25-owned object.
   * nullopt means unsupported, before changing the destination. Errors after
   * admission propagate; they must not trigger a canonical fallback. */
  virtual auto WriteSharedSnapshot(NodeStorage &storage, const ObjectKey &destination, uint64_t index, uint64_t term,
                                   uint64_t limit) -> std::optional<SharedSnapshotResult> {
    return std::nullopt;
  }
  virtual auto PrepareSnapshot(const SnapshotInput &payload, uint64_t index) -> std::unique_ptr<PreparedSnapshot> = 0;
  virtual void ValidateSnapshot(const SnapshotInput &payload, uint64_t index) = 0;
  virtual void LoadSnapshot(const SnapshotInput &payload, uint64_t index) = 0;
  virtual void CreateSnapshotFile(const std::filesystem::path &path) const = 0;
  /** Fully validate a staged snapshot without replacing published state. */
  virtual void ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) = 0;
  virtual void InstallSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) = 0;
};

enum class KvOperation : uint32_t { PUT = 1, DELETE = 2 };

struct KvCommand {
  uint32_t format_version_{1};
  KvOperation operation_{KvOperation::PUT};
  std::string key_;
  std::string value_;
};

class KvCommandCodec {
 public:
  static auto Encode(const KvCommand &command) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> KvCommand;
};

class KvStateMachine : public RaftStateMachine {
 public:
  void ValidateProposalPayload(EntryType type, const std::vector<std::byte> &payload) const override;
  void Apply(const ReplicatedLogEntry &entry) override;
  auto LastApplied() const -> uint64_t override { return last_applied_; }
  /** Small-state conveniences retained for direct unit tests. */
  auto CreateSnapshot() const -> std::vector<std::byte>;
  void InstallSnapshot(const std::vector<std::byte> &payload, uint64_t last_included_index);
  void WriteSnapshot(const SnapshotAppend &append) const override;
  auto PrepareSnapshot(const SnapshotInput &payload, uint64_t index) -> std::unique_ptr<PreparedSnapshot> override;
  void ValidateSnapshot(const SnapshotInput &payload, uint64_t index) override;
  void LoadSnapshot(const SnapshotInput &payload, uint64_t index) override;
  void CreateSnapshotFile(const std::filesystem::path &path) const override;
  void ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) override;
  void InstallSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) override;
  auto Get(std::string_view key) const -> std::optional<std::string>;
  auto Data() const -> const std::map<std::string, std::string> & { return data_; }

 private:
  struct Prepared;
  uint64_t last_applied_{0};
  std::map<std::string, std::string> data_;
};

}  // namespace bustub
