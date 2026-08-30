//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// snapshot_store.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/snapshot_store.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <iomanip>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> SNAPSHOT_MAGIC{std::byte{'B'}, std::byte{'R'}, std::byte{'S'}, std::byte{'N'},
                                                  std::byte{'A'}, std::byte{'P'}, std::byte{'0'}, std::byte{'1'}};
constexpr std::array<std::byte, 8> CURRENT_MAGIC{std::byte{'B'}, std::byte{'R'}, std::byte{'C'}, std::byte{'U'},
                                                 std::byte{'R'}, std::byte{'R'}, std::byte{'0'}, std::byte{'1'}};
constexpr uint32_t SNAPSHOT_VERSION = 1;
constexpr size_t MAX_SNAPSHOT_ID_BYTES = 1024;
constexpr size_t MAX_SNAPSHOT_METADATA_BYTES = 4096;

auto SnapshotFileName(uint64_t generation) -> std::string {
  std::ostringstream output;
  output << "SNAPSHOT-" << std::setw(20) << std::setfill('0') << generation;
  return output.str();
}

auto SnapshotGeneration(std::string_view file_name) -> std::optional<uint64_t> {
  constexpr std::string_view prefix = "SNAPSHOT-";
  constexpr size_t digits = 20;
  if (file_name.size() != prefix.size() + digits || file_name.substr(0, prefix.size()) != prefix) {
    return std::nullopt;
  }
  uint64_t generation = 0;
  const auto first = file_name.data() + prefix.size();
  const auto last = file_name.data() + file_name.size();
  const auto [end, error] = std::from_chars(first, last, generation);
  if (error != std::errc() || end != last || generation == 0 ||
      SnapshotFileName(generation) != std::string(file_name)) {
    return std::nullopt;
  }
  return generation;
}

auto IsPlainFileName(std::string_view value) -> bool {
  return !value.empty() && value != "." && value != ".." && value.find('/') == std::string_view::npos &&
         value.find('\\') == std::string_view::npos;
}

struct CurrentRecord {
  uint64_t generation_;
  uint64_t index_;
  uint64_t term_;
  std::string snapshot_id_;
  std::string file_name_;
  uint32_t file_checksum_;
};

auto DecodeCurrent(const std::vector<std::byte> &bytes) -> CurrentRecord {
  if (bytes.size() < 32 || bytes.size() > 4096) {
    throw std::runtime_error("invalid Raft snapshot CURRENT size");
  }
  ByteReader reader(bytes);
  if (reader.ReadBytes(CURRENT_MAGIC.size()) != std::vector<std::byte>(CURRENT_MAGIC.begin(), CURRENT_MAGIC.end())) {
    throw std::runtime_error("invalid Raft snapshot CURRENT magic");
  }
  if (reader.ReadU32() != SNAPSHOT_VERSION) {
    throw std::runtime_error("unsupported Raft snapshot CURRENT version");
  }
  const auto protected_size = bytes.size() - sizeof(uint32_t);
  ByteReader checksum(bytes.data() + protected_size, sizeof(uint32_t));
  if (Crc32c(bytes.data(), protected_size) != checksum.ReadU32()) {
    throw std::runtime_error("Raft snapshot CURRENT checksum mismatch");
  }
  CurrentRecord result;
  result.generation_ = reader.ReadU64();
  result.index_ = reader.ReadU64();
  result.term_ = reader.ReadU64();
  result.snapshot_id_ = reader.ReadString();
  result.file_name_ = reader.ReadString();
  result.file_checksum_ = reader.ReadU32();
  if (reader.Offset() != protected_size || result.generation_ == 0 || result.snapshot_id_.empty() ||
      !IsPlainFileName(result.file_name_)) {
    throw std::runtime_error("invalid Raft snapshot CURRENT fields");
  }
  return result;
}

}  // namespace

auto SnapshotStore::ChecksumRange(const std::filesystem::path &path, uint64_t offset, uint64_t size,
                                  uint32_t initial_checksum) -> uint32_t {
  uint32_t checksum = initial_checksum;
  uint64_t consumed = 0;
  while (consumed < size) {
    const auto request = static_cast<size_t>(std::min<uint64_t>(STREAM_CHUNK_BYTES, size - consumed));
    const auto chunk = storage_->ReadFileRange(path, offset + consumed, request);
    if (chunk.size() != request) {
      throw std::runtime_error("snapshot file was truncated during streaming checksum");
    }
    checksum = Crc32cExtend(checksum, chunk.data(), chunk.size());
    consumed += chunk.size();
  }
  return checksum;
}

void SnapshotStore::WriteSnapshotFile(const RaftSnapshot &snapshot, const std::filesystem::path &payload_path,
                                      const std::filesystem::path &output_path) {
  if (snapshot.format_version_ != SNAPSHOT_VERSION || snapshot.generation_ == 0 || snapshot.snapshot_id_.empty() ||
      snapshot.snapshot_id_.size() > MAX_SNAPSHOT_ID_BYTES || snapshot.payload_size_ > MAX_SNAPSHOT_BYTES ||
      storage_->FileSize(payload_path) != snapshot.payload_size_ ||
      (snapshot.last_included_index_ == 0 && snapshot.last_included_term_ != 0)) {
    throw std::runtime_error("invalid streamed Raft snapshot");
  }
  ByteWriter body_header;
  body_header.PutU32(SNAPSHOT_VERSION);
  body_header.PutU64(snapshot.generation_);
  body_header.PutString(snapshot.snapshot_id_);
  body_header.PutU64(snapshot.last_included_index_);
  body_header.PutU64(snapshot.last_included_term_);
  body_header.PutU64(snapshot.payload_size_);
  body_header.PutU32(snapshot.payload_checksum_);

  ByteWriter prefix;
  prefix.PutBytes(SNAPSHOT_MAGIC.data(), SNAPSHOT_MAGIC.size());
  prefix.PutBytes(body_header.Data());
  storage_->WriteFile(output_path, prefix.Data());
  uint32_t body_checksum = Crc32c(body_header.Data());
  uint32_t payload_checksum = 0;
  uint64_t offset = 0;
  while (offset < snapshot.payload_size_) {
    const auto request = static_cast<size_t>(std::min<uint64_t>(STREAM_CHUNK_BYTES, snapshot.payload_size_ - offset));
    const auto chunk = storage_->ReadFileRange(payload_path, offset, request);
    if (chunk.size() != request) {
      throw std::runtime_error("snapshot payload was truncated while publishing");
    }
    storage_->AppendFile(output_path, chunk);
    body_checksum = Crc32cExtend(body_checksum, chunk.data(), chunk.size());
    payload_checksum = Crc32cExtend(payload_checksum, chunk.data(), chunk.size());
    offset += chunk.size();
  }
  if (payload_checksum != snapshot.payload_checksum_) {
    throw std::runtime_error("snapshot payload changed while publishing");
  }
  ByteWriter checksum;
  checksum.PutU32(body_checksum);
  storage_->AppendFile(output_path, checksum.Data());
  storage_->SyncFile(output_path);
}

auto SnapshotStore::ReadSnapshotFile(uint64_t generation) -> SnapshotFileView {
  const auto path = directory_ / SnapshotFileName(generation);
  const auto file_size = storage_->FileSize(path);
  if (file_size < SNAPSHOT_MAGIC.size() + 48 || file_size > MAX_SNAPSHOT_BYTES + MAX_SNAPSHOT_METADATA_BYTES) {
    throw std::runtime_error("invalid Raft snapshot file size");
  }
  const auto prefix =
      storage_->ReadFileRange(path, 0, static_cast<size_t>(std::min<uint64_t>(file_size, MAX_SNAPSHOT_METADATA_BYTES)));
  ByteReader reader(prefix);
  if (reader.ReadBytes(SNAPSHOT_MAGIC.size()) != std::vector<std::byte>(SNAPSHOT_MAGIC.begin(), SNAPSHOT_MAGIC.end())) {
    throw std::runtime_error("invalid Raft snapshot magic");
  }
  RaftSnapshot snapshot;
  snapshot.format_version_ = reader.ReadU32();
  snapshot.generation_ = reader.ReadU64();
  snapshot.snapshot_id_ = reader.ReadString();
  snapshot.last_included_index_ = reader.ReadU64();
  snapshot.last_included_term_ = reader.ReadU64();
  snapshot.payload_size_ = reader.ReadU64();
  snapshot.payload_checksum_ = reader.ReadU32();
  const auto payload_offset = reader.Offset();
  if (snapshot.format_version_ != SNAPSHOT_VERSION || snapshot.generation_ != generation ||
      snapshot.snapshot_id_.empty() || snapshot.snapshot_id_.size() > MAX_SNAPSHOT_ID_BYTES ||
      snapshot.payload_size_ > MAX_SNAPSHOT_BYTES ||
      (snapshot.last_included_index_ == 0 && snapshot.last_included_term_ != 0) ||
      file_size != payload_offset + snapshot.payload_size_ + sizeof(uint32_t)) {
    throw std::runtime_error("invalid Raft snapshot metadata");
  }
  const auto payload_checksum = ChecksumRange(path, payload_offset, snapshot.payload_size_);
  if (payload_checksum != snapshot.payload_checksum_) {
    throw std::runtime_error("Raft snapshot payload checksum mismatch");
  }
  const auto body_checksum =
      ChecksumRange(path, SNAPSHOT_MAGIC.size(), payload_offset - SNAPSHOT_MAGIC.size() + snapshot.payload_size_);
  const auto trailer = storage_->ReadFileRange(path, file_size - sizeof(uint32_t), sizeof(uint32_t));
  ByteReader checksum(trailer);
  if (checksum.ReadU32() != body_checksum) {
    throw std::runtime_error("Raft snapshot file checksum mismatch");
  }
  return {snapshot, payload_offset};
}

auto SnapshotStore::EncodeCurrent(const RaftSnapshot &snapshot, std::string_view file_name, uint32_t file_checksum)
    -> std::vector<std::byte> {
  if (!IsPlainFileName(file_name)) {
    throw std::runtime_error("invalid Raft snapshot file name");
  }
  ByteWriter writer;
  writer.PutBytes(CURRENT_MAGIC.data(), CURRENT_MAGIC.size());
  writer.PutU32(SNAPSHOT_VERSION);
  writer.PutU64(snapshot.generation_);
  writer.PutU64(snapshot.last_included_index_);
  writer.PutU64(snapshot.last_included_term_);
  writer.PutString(snapshot.snapshot_id_);
  writer.PutString(file_name);
  writer.PutU32(file_checksum);
  writer.PutU32(Crc32c(writer.Data()));
  return writer.Take();
}

auto SnapshotStore::Open(std::filesystem::path directory, std::shared_ptr<DurableStorage> storage)
    -> std::unique_ptr<SnapshotStore> {
  if (directory.empty() || storage == nullptr) {
    throw std::runtime_error("invalid Raft SnapshotStore configuration");
  }
  storage->CreateDirectories(directory);
  auto store = std::unique_ptr<SnapshotStore>(new SnapshotStore(std::move(directory), std::move(storage)));
  store->Recover();
  return store;
}

void SnapshotStore::Recover() {
  std::optional<CurrentRecord> current;
  bool current_exists = storage_->Exists(current_path_);
  if (current_exists) {
    try {
      current = DecodeCurrent(storage_->ReadFile(current_path_, 4096));
    } catch (const std::exception &) {
      // A damaged CURRENT is not authoritative. Fully checksummed immutable
      // generations remain eligible in newest-to-oldest order.
    }
  }

  std::vector<uint64_t> generations;
  for (const auto &entry : storage_->ListDirectory(directory_)) {
    if (const auto generation = SnapshotGeneration(entry); generation.has_value()) {
      generations.push_back(*generation);
    }
  }
  std::sort(generations.begin(), generations.end(), std::greater<>());

  const auto read_generation = [&](uint64_t generation, const CurrentRecord *expected) -> RaftSnapshot {
    const auto file_name = SnapshotFileName(generation);
    const auto view = ReadSnapshotFile(generation);
    if (expected != nullptr && (expected->generation_ != generation || expected->file_name_ != file_name ||
                                storage_->ChecksumFile(directory_ / file_name) != expected->file_checksum_)) {
      throw std::runtime_error("published Raft snapshot file checksum mismatch");
    }
    const auto &snapshot = view.snapshot_;
    if (snapshot.generation_ != generation ||
        (expected != nullptr &&
         (snapshot.last_included_index_ != expected->index_ || snapshot.last_included_term_ != expected->term_ ||
          snapshot.snapshot_id_ != expected->snapshot_id_))) {
      throw std::runtime_error("Raft snapshot CURRENT metadata mismatch");
    }
    payload_offsets_[generation] = view.payload_offset_;
    return snapshot;
  };

  if (current.has_value()) {
    try {
      latest_ = read_generation(current->generation_, &*current);
    } catch (const std::exception &) {
      // CURRENT named a damaged generation. It is not retried without the
      // pointer checksum; only an older complete generation may be selected.
    }
  }
  for (const auto generation : generations) {
    if (latest_.has_value() || (current.has_value() && generation >= current->generation_)) {
      continue;
    }
    try {
      latest_ = read_generation(generation, nullptr);
    } catch (const std::exception &) {
      continue;
    }
  }
  if (!latest_.has_value() && !current.has_value()) {
    for (const auto generation : generations) {
      try {
        latest_ = read_generation(generation, nullptr);
        break;
      } catch (const std::exception &) {
        continue;
      }
    }
  }
  if (!latest_.has_value()) {
    if (current_exists || !generations.empty()) {
      throw std::runtime_error("no valid Raft snapshot generation is recoverable");
    }
    return;
  }

  for (const auto generation : generations) {
    if (generation >= latest_->generation_) {
      continue;
    }
    try {
      previous_ = read_generation(generation, nullptr);
      break;
    } catch (const std::exception &) {
      continue;
    }
  }
}

auto SnapshotStore::PrepareCapturePath() -> std::filesystem::path {
  if (storage_->Exists(capture_path_)) {
    storage_->RemoveFile(capture_path_);
  }
  return capture_path_;
}

void SnapshotStore::CancelCapture() {
  if (storage_->Exists(capture_path_)) {
    storage_->RemoveFile(capture_path_);
    storage_->SyncDirectory(directory_);
  }
}

auto SnapshotStore::Publish(uint64_t last_included_index, uint64_t last_included_term,
                            const std::vector<std::byte> &payload, bool retain_previous) -> RaftSnapshot {
  const auto capture = PrepareCapturePath();
  storage_->WriteFile(capture, payload);
  try {
    return PublishFile(last_included_index, last_included_term, capture, retain_previous);
  } catch (...) {
    CancelCapture();
    throw;
  }
}

auto SnapshotStore::PublishFile(uint64_t last_included_index, uint64_t last_included_term,
                                const std::filesystem::path &payload_path, bool retain_previous) -> RaftSnapshot {
  const auto payload_size = storage_->FileSize(payload_path);
  if (payload_size > MAX_SNAPSHOT_BYTES || (last_included_index == 0 && last_included_term != 0) ||
      (latest_.has_value() && last_included_index <= latest_->last_included_index_)) {
    throw std::runtime_error("non-monotonic or invalid Raft snapshot publication");
  }
  const auto old_latest = latest_;
  const auto generation = old_latest.has_value() ? old_latest->generation_ + 1 : 1;
  const auto payload_checksum = storage_->ChecksumFile(payload_path);
  const auto snapshot_id = std::to_string(last_included_index) + "-" + std::to_string(last_included_term) + "-" +
                           std::to_string(payload_checksum);
  RaftSnapshot snapshot{SNAPSHOT_VERSION,   generation,   snapshot_id,     last_included_index,
                        last_included_term, payload_size, payload_checksum};
  const auto file_name = SnapshotFileName(generation);
  const auto temporary = directory_ / (file_name + ".tmp");
  const auto formal = directory_ / file_name;
  WriteSnapshotFile(snapshot, payload_path, temporary);
  const auto file_checksum = storage_->ChecksumFile(temporary);
  storage_->Rename(temporary, formal);
  storage_->SyncDirectory(directory_);

  const auto current_temporary = directory_ / "CURRENT.tmp";
  storage_->WriteFile(current_temporary, EncodeCurrent(snapshot, file_name, file_checksum));
  storage_->SyncFile(current_temporary);
  storage_->Rename(current_temporary, current_path_);
  storage_->SyncDirectory(directory_);
  latest_ = snapshot;
  payload_offsets_[generation] = ReadSnapshotFile(generation).payload_offset_;
  previous_ = retain_previous ? old_latest : std::nullopt;
  PruneSnapshots();
  if (payload_path == capture_path_) {
    CancelCapture();
  }
  return snapshot;
}

auto SnapshotStore::ReadPayloadChunk(const RaftSnapshot &snapshot, uint64_t offset, size_t maximum_size)
    -> std::vector<std::byte> {
  if (snapshot.generation_ == 0 || offset > snapshot.payload_size_ || maximum_size > STREAM_CHUNK_BYTES) {
    throw std::runtime_error("invalid Raft snapshot payload range");
  }
  const auto stored =
      latest_.has_value() && latest_->generation_ == snapshot.generation_
          ? latest_
          : (previous_.has_value() && previous_->generation_ == snapshot.generation_ ? previous_ : std::nullopt);
  const auto payload_offset = payload_offsets_.find(snapshot.generation_);
  if (!stored.has_value() || stored->snapshot_id_ != snapshot.snapshot_id_ ||
      stored->payload_size_ != snapshot.payload_size_ || payload_offset == payload_offsets_.end()) {
    throw std::runtime_error("Raft snapshot payload is not retained locally");
  }
  const auto requested = static_cast<size_t>(
      std::min<uint64_t>(std::min<uint64_t>(maximum_size, STREAM_CHUNK_BYTES), snapshot.payload_size_ - offset));
  return storage_->ReadFileRange(directory_ / SnapshotFileName(snapshot.generation_), payload_offset->second + offset,
                                 requested);
}

auto SnapshotStore::PayloadFile(const RaftSnapshot &snapshot) -> DurableFileSlice {
  const auto payload_offset = payload_offsets_.find(snapshot.generation_);
  const auto stored =
      latest_.has_value() && latest_->generation_ == snapshot.generation_
          ? latest_
          : (previous_.has_value() && previous_->generation_ == snapshot.generation_ ? previous_ : std::nullopt);
  if (!stored.has_value() || stored->snapshot_id_ != snapshot.snapshot_id_ ||
      stored->payload_size_ != snapshot.payload_size_ || payload_offset == payload_offsets_.end()) {
    throw std::runtime_error("Raft snapshot payload is not retained locally");
  }
  return {directory_ / SnapshotFileName(snapshot.generation_), payload_offset->second, snapshot.payload_size_};
}

void SnapshotStore::PruneSnapshots() {
  std::set<uint64_t> retained;
  if (latest_.has_value()) {
    retained.insert(latest_->generation_);
  }
  if (previous_.has_value()) {
    retained.insert(previous_->generation_);
  }
  bool removed = false;
  for (const auto &entry : storage_->ListDirectory(directory_)) {
    const auto generation = SnapshotGeneration(entry);
    if (generation.has_value() && retained.count(*generation) == 0) {
      storage_->RemoveFile(directory_ / entry);
      payload_offsets_.erase(*generation);
      removed = true;
    }
  }
  if (removed) {
    storage_->SyncDirectory(directory_);
  }
}

void SnapshotStore::RetainOnlyLatest() {
  if (!latest_.has_value()) {
    throw std::runtime_error("cannot retain a missing latest Raft snapshot");
  }
  previous_.reset();
  PruneSnapshots();
}

auto SnapshotStore::StageChunk(const SnapshotChunk &chunk) -> SnapshotStageResult {
  if (chunk.snapshot_id_.empty() || chunk.total_size_ > MAX_SNAPSHOT_BYTES || chunk.data_.size() > chunk.total_size_ ||
      chunk.offset_ > chunk.total_size_ - chunk.data_.size() ||
      (chunk.last_included_index_ == 0 && chunk.last_included_term_ != 0)) {
    throw std::runtime_error("invalid Raft snapshot chunk");
  }
  if (!download_.has_value() || download_->snapshot_id_ != chunk.snapshot_id_) {
    if (chunk.offset_ != 0) {
      throw std::runtime_error("Raft snapshot download must start at offset zero");
    }
    if (storage_->Exists(download_path_)) {
      storage_->RemoveFile(download_path_);
      storage_->SyncDirectory(directory_);
    }
    download_ = Download{chunk.snapshot_id_,
                         chunk.last_included_index_,
                         chunk.last_included_term_,
                         chunk.total_size_,
                         chunk.payload_checksum_,
                         false,
                         0};
  }
  auto &download = *download_;
  if (download.last_included_index_ != chunk.last_included_index_ ||
      download.last_included_term_ != chunk.last_included_term_ || download.total_size_ != chunk.total_size_ ||
      download.payload_checksum_ != chunk.payload_checksum_) {
    throw std::runtime_error("Raft snapshot chunk metadata changed during download");
  }
  if (download.complete_) {
    return {SnapshotStageStatus::DUPLICATE_COMPLETE, download.received_size_};
  }
  if (chunk.offset_ < download.received_size_) {
    const auto existing = storage_->ReadFileRange(download_path_, chunk.offset_, chunk.data_.size());
    if (chunk.offset_ + chunk.data_.size() > download.received_size_ || existing != chunk.data_) {
      throw std::runtime_error("conflicting duplicate Raft snapshot chunk");
    }
  } else {
    if (chunk.offset_ != download.received_size_) {
      throw std::runtime_error("out-of-order Raft snapshot chunk");
    }
    storage_->AppendFileDurable(download_path_, chunk.data_);
    download.received_size_ += chunk.data_.size();
  }
  if (chunk.done_) {
    if (download.received_size_ != download.total_size_ ||
        storage_->ChecksumFile(download_path_) != download.payload_checksum_) {
      throw std::runtime_error("completed Raft snapshot download failed validation");
    }
    download.complete_ = true;
    return {SnapshotStageStatus::COMPLETE, download.received_size_};
  }
  if (download.received_size_ == download.total_size_) {
    throw std::runtime_error("final Raft snapshot chunk did not carry done=true");
  }
  return {SnapshotStageStatus::IN_PROGRESS, download.received_size_};
}

auto SnapshotStore::Staged(std::string_view snapshot_id) const -> std::optional<RaftSnapshot> {
  if (!download_.has_value() || !download_->complete_ || download_->snapshot_id_ != snapshot_id) {
    return std::nullopt;
  }
  return RaftSnapshot{SNAPSHOT_VERSION,
                      0,
                      download_->snapshot_id_,
                      download_->last_included_index_,
                      download_->last_included_term_,
                      download_->total_size_,
                      download_->payload_checksum_};
}

auto SnapshotStore::StagedPayloadFile(std::string_view snapshot_id) const -> std::optional<DurableFileSlice> {
  if (!download_.has_value() || !download_->complete_ || download_->snapshot_id_ != snapshot_id) {
    return std::nullopt;
  }
  return DurableFileSlice{download_path_, 0, download_->total_size_};
}

void SnapshotStore::CancelStaged(std::string_view snapshot_id) {
  if (!download_.has_value() || download_->snapshot_id_ != snapshot_id) {
    return;
  }
  if (storage_->Exists(download_path_)) {
    storage_->RemoveFile(download_path_);
    storage_->SyncDirectory(directory_);
  }
  download_.reset();
}

}  // namespace bustub
