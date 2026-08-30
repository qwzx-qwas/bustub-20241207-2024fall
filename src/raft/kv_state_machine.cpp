//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// kv_state_machine.cpp
//
//===----------------------------------------------------------------------===//

#include <stdexcept>

#include "common/byte_codec.h"
#include "raft/state_machine.h"

namespace bustub {
namespace {

constexpr uint32_t KV_FORMAT_VERSION = 1;
constexpr uint32_t KV_SNAPSHOT_FORMAT_VERSION = 1;
constexpr uint32_t KV_MAX_FIELD_BYTES = 1024U * 1024U;
constexpr uint32_t KV_MAX_SNAPSHOT_ENTRIES = 1000000;
constexpr size_t KV_MAX_SNAPSHOT_BYTES = 128U * 1024U * 1024U;

}  // namespace

auto KvCommandCodec::Encode(const KvCommand &command) -> std::vector<std::byte> {
  if (command.format_version_ != KV_FORMAT_VERSION || command.key_.empty() ||
      command.key_.size() > KV_MAX_FIELD_BYTES || command.value_.size() > KV_MAX_FIELD_BYTES ||
      (command.operation_ != KvOperation::PUT && command.operation_ != KvOperation::DELETE) ||
      (command.operation_ == KvOperation::DELETE && !command.value_.empty())) {
    throw std::runtime_error("invalid KV Raft command");
  }
  ByteWriter writer;
  writer.PutU32(KV_FORMAT_VERSION);
  writer.PutU32(static_cast<uint32_t>(command.operation_));
  writer.PutString(command.key_);
  writer.PutString(command.value_);
  return writer.Take();
}

auto KvCommandCodec::Decode(const std::vector<std::byte> &bytes) -> KvCommand {
  if (bytes.size() > 2U * KV_MAX_FIELD_BYTES + 64U) {
    throw std::runtime_error("KV Raft command is too large");
  }
  ByteReader reader(bytes);
  KvCommand command;
  command.format_version_ = reader.ReadU32();
  command.operation_ = static_cast<KvOperation>(reader.ReadU32());
  command.key_ = reader.ReadString();
  command.value_ = reader.ReadString();
  if (!reader.Empty()) {
    throw std::runtime_error("trailing KV Raft command bytes");
  }
  static_cast<void>(Encode(command));
  return command;
}

void KvStateMachine::ValidateProposalPayload(EntryType type, const std::vector<std::byte> &payload) const {
  if (type != EntryType::KV_COMMAND) {
    throw std::runtime_error("unsupported proposal type for KV state machine");
  }
  static_cast<void>(KvCommandCodec::Decode(payload));
}

void KvStateMachine::Apply(const ReplicatedLogEntry &entry) {
  if (entry.index_ != last_applied_ + 1) {
    throw std::runtime_error("KV state machine apply is not continuous");
  }
  if (entry.type_ == EntryType::NOOP) {
    last_applied_ = entry.index_;
    return;
  }
  if (entry.type_ != EntryType::KV_COMMAND) {
    throw std::runtime_error("unsupported entry type for KV state machine");
  }
  const auto command = KvCommandCodec::Decode(entry.payload_);
  if (command.operation_ == KvOperation::PUT) {
    data_[command.key_] = command.value_;
  } else {
    data_.erase(command.key_);
  }
  last_applied_ = entry.index_;
}

auto KvStateMachine::Get(std::string_view key) const -> std::optional<std::string> {
  const auto item = data_.find(std::string(key));
  if (item == data_.end()) {
    return std::nullopt;
  }
  return item->second;
}

auto KvStateMachine::CreateSnapshot() const -> std::vector<std::byte> {
  if (data_.size() > KV_MAX_SNAPSHOT_ENTRIES) {
    throw std::runtime_error("KV state machine has too many snapshot entries");
  }
  ByteWriter writer;
  writer.PutU32(KV_SNAPSHOT_FORMAT_VERSION);
  writer.PutU64(last_applied_);
  writer.PutU32(static_cast<uint32_t>(data_.size()));
  for (const auto &[key, value] : data_) {
    writer.PutString(key);
    writer.PutString(value);
  }
  return writer.Take();
}

void KvStateMachine::InstallSnapshot(const std::vector<std::byte> &payload, uint64_t last_included_index) {
  ByteReader reader(payload);
  if (reader.ReadU32() != KV_SNAPSHOT_FORMAT_VERSION || reader.ReadU64() != last_included_index) {
    throw std::runtime_error("KV snapshot metadata mismatch");
  }
  const auto count = reader.ReadU32();
  if (count > KV_MAX_SNAPSHOT_ENTRIES) {
    throw std::runtime_error("KV snapshot has too many entries");
  }
  std::map<std::string, std::string> restored;
  std::string previous;
  for (uint32_t index = 0; index < count; index++) {
    auto key = reader.ReadString();
    auto value = reader.ReadString();
    if (key.empty() || key.size() > KV_MAX_FIELD_BYTES || value.size() > KV_MAX_FIELD_BYTES ||
        (index != 0 && key <= previous) || !restored.emplace(key, std::move(value)).second) {
      throw std::runtime_error("invalid or non-canonical KV snapshot entry");
    }
    previous = std::move(key);
  }
  if (!reader.Empty()) {
    throw std::runtime_error("trailing KV snapshot bytes");
  }
  data_ = std::move(restored);
  last_applied_ = last_included_index;
}

void KvStateMachine::CreateSnapshotFile(const std::filesystem::path &path) const {
  PosixDurableStorage storage;
  storage.WriteFile(path, CreateSnapshot());
}

void KvStateMachine::ValidateSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) {
  if (payload.size_ > KV_MAX_SNAPSHOT_BYTES) {
    throw std::runtime_error("KV snapshot exceeds its in-memory teaching-state limit");
  }
  PosixDurableStorage storage;
  const auto bytes = storage.ReadFileRange(payload.path_, payload.offset_, static_cast<size_t>(payload.size_));
  if (bytes.size() != payload.size_) {
    throw std::runtime_error("KV snapshot file was truncated");
  }
  KvStateMachine candidate;
  candidate.InstallSnapshot(bytes, last_included_index);
}

void KvStateMachine::InstallSnapshotFile(const DurableFileSlice &payload, uint64_t last_included_index) {
  if (payload.size_ > KV_MAX_SNAPSHOT_BYTES) {
    throw std::runtime_error("KV snapshot exceeds its in-memory teaching-state limit");
  }
  PosixDurableStorage storage;
  const auto bytes = storage.ReadFileRange(payload.path_, payload.offset_, static_cast<size_t>(payload.size_));
  if (bytes.size() != payload.size_) {
    throw std::runtime_error("KV snapshot file was truncated");
  }
  InstallSnapshot(bytes, last_included_index);
}

}  // namespace bustub
