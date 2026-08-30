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

namespace bustub {

class RaftStateMachine {
 public:
  virtual ~RaftStateMachine() = default;
  /** Read-only admission check that must complete before a proposal is appended. */
  virtual void ValidateProposalPayload(EntryType type, const std::vector<std::byte> &payload) const = 0;
  virtual void Apply(const ReplicatedLogEntry &entry) = 0;
  virtual auto LastApplied() const -> uint64_t = 0;
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
  void CreateSnapshotFile(const std::filesystem::path &path) const override;
  void ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) override;
  void InstallSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) override;
  auto Get(std::string_view key) const -> std::optional<std::string>;
  auto Data() const -> const std::map<std::string, std::string> & { return data_; }

 private:
  uint64_t last_applied_{0};
  std::map<std::string, std::string> data_;
};

}  // namespace bustub
