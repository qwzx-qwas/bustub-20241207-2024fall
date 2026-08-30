//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_store.cpp
//
//===----------------------------------------------------------------------===//

#include "raft/log_store.h"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <string>

#include "common/byte_codec.h"

namespace bustub {
namespace {

constexpr uint32_t MUTATION_MAGIC = 0x42524d31U;  // "BRM1"
constexpr uint32_t MUTATION_VERSION = 1;
constexpr size_t MUTATION_HEADER_BYTES = 8;
constexpr size_t MUTATION_FIXED_BODY_BYTES = 4 + 4 + 8 + 8 + 1 + 4;
constexpr size_t MUTATION_CHECKSUM_BYTES = 4;
constexpr size_t MINIMUM_MUTATION_FRAME_BYTES =
    MUTATION_HEADER_BYTES + MUTATION_FIXED_BODY_BYTES + MUTATION_CHECKSUM_BYTES;
constexpr size_t MINIMUM_ENCODED_ENTRY_BYTES =
    sizeof(uint32_t) + LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES;

auto IsKnownMutation(uint32_t value) -> bool { return value >= 1 && value <= 3; }

auto IsKnownEntryType(EntryType type) -> bool {
  return type == EntryType::COMMAND_BATCH || type == EntryType::NOOP || type == EntryType::KV_COMMAND;
}

auto CheckedAdd(size_t lhs, size_t rhs, const char *message) -> size_t {
  if (rhs > std::numeric_limits<size_t>::max() - lhs) {
    throw std::runtime_error(message);
  }
  return lhs + rhs;
}

void PutU8(std::vector<std::byte> *output, uint8_t value) { output->push_back(static_cast<std::byte>(value)); }

void PutU32(std::vector<std::byte> *output, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    PutU8(output, static_cast<uint8_t>((value >> shift) & 0xffU));
  }
}

void PutU64(std::vector<std::byte> *output, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    PutU8(output, static_cast<uint8_t>((value >> shift) & 0xffU));
  }
}

auto ValidatedLogFrameSize(const ReplicatedLogEntry &entry) -> size_t {
  if (entry.format_version_ != LogCodec::FORMAT_VERSION || entry.index_ == 0 || !IsKnownEntryType(entry.type_) ||
      entry.payload_.size() > LogCodec::MAX_PAYLOAD_BYTES ||
      (entry.type_ == EntryType::NOOP && !entry.payload_.empty())) {
    throw std::runtime_error("invalid replicated log entry");
  }
  return CheckedAdd(LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES, entry.payload_.size(),
                    "encoded Raft log entry size overflow");
}

void ValidateContinuousEntries(const std::vector<ReplicatedLogEntry> &entries, uint64_t first_index) {
  for (size_t offset = 0; offset < entries.size(); offset++) {
    const auto &entry = entries[offset];
    if (entry.index_ != first_index) {
      throw std::runtime_error("Raft log mutation contains a discontinuous entry sequence");
    }
    static_cast<void>(ValidatedLogFrameSize(entry));
    if (offset + 1 < entries.size()) {
      if (first_index == std::numeric_limits<uint64_t>::max()) {
        throw std::runtime_error("Raft log index space is exhausted");
      }
      first_index++;
    }
  }
}

auto CanonicalJournalSize(const std::vector<ReplicatedLogEntry> &existing, size_t first, size_t count,
                          const std::vector<ReplicatedLogEntry> &appended) -> size_t {
  if (first > existing.size() || count > existing.size() - first || count > std::numeric_limits<uint32_t>::max() ||
      appended.size() > std::numeric_limits<uint32_t>::max() - count) {
    throw std::runtime_error("too many entries in canonical Raft log journal");
  }
  const auto entry_count = count + appended.size();
  size_t suffix_size = MINIMUM_MUTATION_FRAME_BYTES;
  const auto add_entry = [&](const ReplicatedLogEntry &entry) {
    suffix_size = CheckedAdd(suffix_size, sizeof(uint32_t), "canonical Raft log journal size overflow");
    suffix_size = CheckedAdd(suffix_size, ValidatedLogFrameSize(entry), "canonical Raft log journal size overflow");
  };
  for (size_t offset = 0; offset < count; offset++) {
    add_entry(existing[first + offset]);
  }
  for (const auto &entry : appended) {
    add_entry(entry);
  }
  return entry_count == 0
             ? MINIMUM_MUTATION_FRAME_BYTES
             : CheckedAdd(MINIMUM_MUTATION_FRAME_BYTES, suffix_size, "canonical Raft log journal size overflow");
}

void CheckCanonicalJournalLimit(size_t maximum_size, const std::vector<ReplicatedLogEntry> &existing, size_t first,
                                size_t count, const std::vector<ReplicatedLogEntry> &appended) {
  if (CanonicalJournalSize(existing, first, count, appended) > maximum_size) {
    throw std::runtime_error("canonical Raft log journal exceeds the configured maximum");
  }
}

}  // namespace

auto LogStore::EncodedMutationSize(const std::vector<ReplicatedLogEntry> &entries) -> size_t {
  if (entries.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("too many entries in Raft log mutation");
  }
  size_t frame_size = MINIMUM_MUTATION_FRAME_BYTES;
  for (const auto &entry : entries) {
    frame_size = CheckedAdd(frame_size, sizeof(uint32_t), "Raft log mutation size overflow");
    frame_size = CheckedAdd(frame_size, ValidatedLogFrameSize(entry), "Raft log mutation size overflow");
  }
  if (frame_size - MUTATION_HEADER_BYTES > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("Raft log mutation is too large");
  }
  return frame_size;
}

auto LogStore::EncodedMutationSize(const Mutation &mutation) -> size_t {
  return EncodedMutationSize(mutation.entries_);
}

auto LogStore::EncodeMutation(MutationType type, uint64_t argument_index, uint64_t argument_term,
                              bool retain_old_suffix, const std::vector<ReplicatedLogEntry> &entries)
    -> std::vector<std::byte> {
  const auto expected_size = EncodedMutationSize(entries);
  std::vector<std::byte> encoded;
  encoded.reserve(expected_size);
  AppendEncodedMutation(&encoded, type, argument_index, argument_term, retain_old_suffix, entries);
  return encoded;
}

void LogStore::AppendEncodedMutation(std::vector<std::byte> *output, MutationType type, uint64_t argument_index,
                                     uint64_t argument_term, bool retain_old_suffix,
                                     const std::vector<ReplicatedLogEntry> &entries) {
  if (output == nullptr) {
    throw std::runtime_error("Raft log mutation output is null");
  }
  const auto expected_size = EncodedMutationSize(entries);
  const auto frame_start = output->size();
  if (expected_size > output->max_size() - frame_start) {
    throw std::runtime_error("Raft log mutation output size overflow");
  }
  PutU32(output, MUTATION_MAGIC);
  PutU32(output, static_cast<uint32_t>(expected_size - MUTATION_HEADER_BYTES));
  const auto protected_start = output->size();
  PutU32(output, MUTATION_VERSION);
  PutU32(output, static_cast<uint32_t>(type));
  PutU64(output, argument_index);
  PutU64(output, argument_term);
  PutU8(output, retain_old_suffix ? 1 : 0);
  PutU32(output, static_cast<uint32_t>(entries.size()));
  for (const auto &entry : entries) {
    const auto encoded_entry = LogCodec::Encode(entry);
    PutU32(output, static_cast<uint32_t>(encoded_entry.size()));
    output->insert(output->end(), encoded_entry.begin(), encoded_entry.end());
  }
  PutU32(output, Crc32c(output->data() + protected_start, output->size() - protected_start));
  if (output->size() - frame_start != expected_size) {
    throw std::runtime_error("Raft log mutation size calculation mismatch");
  }
}

auto LogStore::EncodeMutation(const Mutation &mutation) -> std::vector<std::byte> {
  return EncodeMutation(mutation.type_, mutation.argument_index_, mutation.argument_term_, mutation.retain_old_suffix_,
                        mutation.entries_);
}

auto LogStore::DecodeMutation(const std::vector<std::byte> &bytes, size_t offset) -> DecodeResult {
  if (offset > bytes.size() || bytes.size() - offset < MUTATION_HEADER_BYTES) {
    return {DecodeStatus::TRUNCATED, std::nullopt, 0};
  }
  try {
    ByteReader header(bytes.data() + offset, bytes.size() - offset);
    if (header.ReadU32() != MUTATION_MAGIC) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    const auto frame_body_size = header.ReadU32();
    if (frame_body_size < MUTATION_FIXED_BODY_BYTES + MUTATION_CHECKSUM_BYTES) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    if (frame_body_size > bytes.size() - offset - MUTATION_HEADER_BYTES) {
      return {DecodeStatus::TRUNCATED, std::nullopt, 0};
    }
    const auto protected_size = static_cast<size_t>(frame_body_size) - MUTATION_CHECKSUM_BYTES;
    const auto *protected_data = bytes.data() + offset + MUTATION_HEADER_BYTES;
    ByteReader checksum(protected_data + protected_size, MUTATION_CHECKSUM_BYTES);
    if (Crc32c(protected_data, protected_size) != checksum.ReadU32()) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }

    ByteReader body(protected_data, protected_size);
    if (body.ReadU32() != MUTATION_VERSION) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    const auto raw_type = body.ReadU32();
    if (!IsKnownMutation(raw_type)) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    Mutation mutation;
    mutation.type_ = static_cast<MutationType>(raw_type);
    mutation.argument_index_ = body.ReadU64();
    mutation.argument_term_ = body.ReadU64();
    const auto retain = body.ReadU8();
    if (retain > 1) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    mutation.retain_old_suffix_ = retain == 1;
    const auto entry_count = body.ReadU32();
    if (entry_count > body.Remaining() / MINIMUM_ENCODED_ENTRY_BYTES) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    // Do not let a hostile count force a large allocation before individual frames have been validated.
    mutation.entries_.reserve(std::min<size_t>(entry_count, 4096));
    for (uint32_t index = 0; index < entry_count; index++) {
      if (body.Remaining() < sizeof(uint32_t)) {
        return {DecodeStatus::CORRUPT, std::nullopt, 0};
      }
      const auto entry_size = body.ReadU32();
      if (entry_size < LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES ||
          entry_size > body.Remaining()) {
        return {DecodeStatus::CORRUPT, std::nullopt, 0};
      }
      auto decoded = LogCodec::DecodeOne(protected_data + body.Offset(), entry_size);
      if (decoded.status_ != LogDecodeStatus::COMPLETE || decoded.bytes_consumed_ != entry_size) {
        return {DecodeStatus::CORRUPT, std::nullopt, 0};
      }
      body.Skip(entry_size);
      mutation.entries_.push_back(std::move(*decoded.entry_));
    }
    if (!body.Empty()) {
      return {DecodeStatus::CORRUPT, std::nullopt, 0};
    }
    return {DecodeStatus::COMPLETE, std::move(mutation), MUTATION_HEADER_BYTES + frame_body_size};
  } catch (const std::exception &) {
    return {DecodeStatus::CORRUPT, std::nullopt, 0};
  }
}

auto LogStore::Open(const std::filesystem::path &directory, std::shared_ptr<DurableStorage> storage,
                    uint64_t effective_commit_index, uint64_t published_snapshot_index,
                    uint64_t published_snapshot_term, LogStoreOptions options) -> std::unique_ptr<LogStore> {
  if (directory.empty() || storage == nullptr || options.maximum_journal_bytes_ < MINIMUM_MUTATION_FRAME_BYTES ||
      options.maximum_journal_bytes_ > LogStoreOptions::MAXIMUM_JOURNAL_BYTES ||
      effective_commit_index < published_snapshot_index ||
      (published_snapshot_index == 0 && published_snapshot_term != 0)) {
    throw std::runtime_error("invalid LogStore configuration");
  }
  storage->CreateDirectories(directory);
  auto store = std::unique_ptr<LogStore>(new LogStore(directory, std::move(storage), effective_commit_index, options));
  store->Recover(published_snapshot_index, published_snapshot_term);
  return store;
}

auto LogStore::RebuildFromVerifiedSnapshot(const std::filesystem::path &directory,
                                           std::shared_ptr<DurableStorage> storage, uint64_t effective_commit_index,
                                           uint64_t snapshot_index, uint64_t snapshot_term, LogStoreOptions options)
    -> std::unique_ptr<LogStore> {
  if (directory.empty() || storage == nullptr || effective_commit_index != snapshot_index ||
      (snapshot_index == 0 && snapshot_term != 0) || options.maximum_journal_bytes_ < MINIMUM_MUTATION_FRAME_BYTES ||
      options.maximum_journal_bytes_ > LogStoreOptions::MAXIMUM_JOURNAL_BYTES) {
    throw std::runtime_error("invalid verified-snapshot LogStore rebuild configuration");
  }
  storage->CreateDirectories(directory);
  auto writer = std::unique_ptr<LogStore>(new LogStore(directory, storage, effective_commit_index, options));
  writer->RewriteJournal(snapshot_index, snapshot_term, {});
  writer.reset();
  return Open(directory, std::move(storage), effective_commit_index, snapshot_index, snapshot_term, options);
}

void LogStore::Recover(uint64_t published_snapshot_index, uint64_t published_snapshot_term) {
  if (storage_->Exists(journal_temporary_path_)) {
    storage_->RemoveFile(journal_temporary_path_);
    storage_->SyncDirectory(directory_);
  }
  if (storage_->Exists(journal_path_)) {
    const auto bytes = storage_->ReadFile(journal_path_, options_.maximum_journal_bytes_);
    size_t offset = 0;
    while (offset < bytes.size()) {
      auto decoded = DecodeMutation(bytes, offset);
      if (decoded.status_ != DecodeStatus::COMPLETE) {
        if (LastLogIndexUnlocked() < effective_commit_index_) {
          throw std::runtime_error("committed Raft log mutation is corrupt or truncated");
        }
        storage_->TruncateFile(journal_path_, offset);
        break;
      }
      ApplyMutation(*decoded.mutation_, true);
      offset += decoded.bytes_consumed_;
    }
  }

  // A published snapshot is not implicit permission to discard a corrupt committed bridge. The caller must use
  // RebuildFromVerifiedSnapshot after independently validating that exact snapshot.
  ValidateCommittedRange();

  if (snapshot_base_index_ > published_snapshot_index ||
      (snapshot_base_index_ == published_snapshot_index && snapshot_base_term_ != published_snapshot_term)) {
    throw std::runtime_error("LogStore base is incompatible with the published snapshot");
  }
  if (snapshot_base_index_ < published_snapshot_index) {
    const bool retain = TermAtUnlocked(published_snapshot_index) == std::optional<uint64_t>{published_snapshot_term};
    std::vector<ReplicatedLogEntry> retained;
    if (retain) {
      const auto first = published_snapshot_index - snapshot_base_index_;
      CheckCanonicalJournalLimit(options_.maximum_journal_bytes_, entries_, static_cast<size_t>(first),
                                 entries_.size() - static_cast<size_t>(first), {});
      retained.assign(entries_.begin() + static_cast<ptrdiff_t>(first), entries_.end());
    } else {
      CheckCanonicalJournalLimit(options_.maximum_journal_bytes_, entries_, 0, 0, {});
    }
    RewriteJournal(published_snapshot_index, published_snapshot_term, retained);
    snapshot_base_index_ = published_snapshot_index;
    snapshot_base_term_ = published_snapshot_term;
    entries_ = std::move(retained);
  }
  ValidateCommittedRange();
}

void LogStore::ValidateMutation(const Mutation &mutation, bool recovering) const {
  switch (mutation.type_) {
    case MutationType::APPEND: {
      if (mutation.argument_index_ != 0 || mutation.argument_term_ != 0 || mutation.retain_old_suffix_ ||
          mutation.entries_.empty()) {
        throw std::runtime_error("invalid append mutation");
      }
      if (LastLogIndexUnlocked() == std::numeric_limits<uint64_t>::max()) {
        throw std::runtime_error("Raft log index space is exhausted");
      }
      ValidateContinuousEntries(mutation.entries_, LastLogIndexUnlocked() + 1);
      break;
    }
    case MutationType::REPLACE_SUFFIX: {
      if (mutation.argument_term_ != 0 || mutation.retain_old_suffix_ ||
          mutation.argument_index_ <= snapshot_base_index_ ||
          (mutation.argument_index_ > LastLogIndexUnlocked() &&
           (LastLogIndexUnlocked() == std::numeric_limits<uint64_t>::max() ||
            mutation.argument_index_ != LastLogIndexUnlocked() + 1)) ||
          (!recovering && mutation.argument_index_ <= effective_commit_index_)) {
        throw std::runtime_error("invalid or committed Raft suffix replacement");
      }
      ValidateContinuousEntries(mutation.entries_, mutation.argument_index_);
      break;
    }
    case MutationType::INSTALL_SNAPSHOT_BASE: {
      const bool canonical_current_base = recovering && !mutation.retain_old_suffix_ && entries_.empty() &&
                                          mutation.argument_index_ == snapshot_base_index_ &&
                                          mutation.argument_term_ == snapshot_base_term_;
      if (!mutation.entries_.empty() || (!canonical_current_base && mutation.argument_index_ <= snapshot_base_index_) ||
          (mutation.argument_index_ == 0 && mutation.argument_term_ != 0)) {
        throw std::runtime_error("invalid snapshot-base mutation");
      }
      if (mutation.retain_old_suffix_ &&
          TermAtUnlocked(mutation.argument_index_) != std::optional<uint64_t>{mutation.argument_term_}) {
        throw std::runtime_error("snapshot-base mutation cannot prove suffix match");
      }
      break;
    }
  }
}

void LogStore::ApplyMutation(const Mutation &mutation, bool recovering) {
  ValidateMutation(mutation, recovering);
  switch (mutation.type_) {
    case MutationType::APPEND:
      entries_.insert(entries_.end(), mutation.entries_.begin(), mutation.entries_.end());
      break;
    case MutationType::REPLACE_SUFFIX:
      entries_.resize(static_cast<size_t>(mutation.argument_index_ - snapshot_base_index_ - 1));
      entries_.insert(entries_.end(), mutation.entries_.begin(), mutation.entries_.end());
      break;
    case MutationType::INSTALL_SNAPSHOT_BASE: {
      std::vector<ReplicatedLogEntry> retained;
      if (mutation.retain_old_suffix_) {
        const auto first = mutation.argument_index_ - snapshot_base_index_;
        retained.assign(entries_.begin() + static_cast<ptrdiff_t>(first), entries_.end());
      }
      snapshot_base_index_ = mutation.argument_index_;
      snapshot_base_term_ = mutation.argument_term_;
      entries_ = std::move(retained);
      break;
    }
  }
}

void LogStore::AppendMutation(const std::vector<ReplicatedLogEntry> &entries) {
  // Invalid mutations must never reach durable storage: recovery cannot know that their caller received an exception.
  if (entries.empty()) {
    throw std::runtime_error("cannot append an empty Raft log batch");
  }
  if (LastLogIndexUnlocked() == std::numeric_limits<uint64_t>::max()) {
    throw std::runtime_error("Raft log index space is exhausted");
  }
  ValidateContinuousEntries(entries, LastLogIndexUnlocked() + 1);
  const bool created_journal = !storage_->Exists(journal_path_);
  const auto current_size = created_journal ? 0 : storage_->FileSize(journal_path_);
  const auto frame_size = EncodedMutationSize(entries);
  if (current_size > options_.maximum_journal_bytes_ ||
      frame_size > options_.maximum_journal_bytes_ - static_cast<size_t>(current_size)) {
    throw std::runtime_error("Raft log journal exceeds the configured maximum");
  }
  const auto frame = EncodeMutation(MutationType::APPEND, 0, 0, false, entries);
  storage_->AppendFileDurable(journal_path_, frame);
  if (created_journal) {
    storage_->SyncDirectory(directory_);
  }
  entries_.insert(entries_.end(), entries.begin(), entries.end());
}

void LogStore::RewriteJournal(uint64_t snapshot_base_index, uint64_t snapshot_base_term,
                              const std::vector<ReplicatedLogEntry> &entries) {
  if (snapshot_base_index == 0 && snapshot_base_term != 0) {
    throw std::runtime_error("invalid canonical Raft log base");
  }
  if (!entries.empty()) {
    if (snapshot_base_index == std::numeric_limits<uint64_t>::max()) {
      throw std::runtime_error("Raft log index space is exhausted");
    }
    ValidateContinuousEntries(entries, snapshot_base_index + 1);
  }
  const std::vector<ReplicatedLogEntry> empty;
  const auto canonical_size = CanonicalJournalSize(entries, 0, entries.size(), empty);
  CheckCanonicalJournalLimit(options_.maximum_journal_bytes_, entries, 0, entries.size(), empty);

  std::vector<std::byte> canonical;
  canonical.reserve(canonical_size);
  AppendEncodedMutation(&canonical, MutationType::INSTALL_SNAPSHOT_BASE, snapshot_base_index, snapshot_base_term, false,
                        empty);
  if (!entries.empty()) {
    AppendEncodedMutation(&canonical, MutationType::APPEND, 0, 0, false, entries);
  }
  storage_->WriteFile(journal_temporary_path_, canonical);
  storage_->SyncFile(journal_temporary_path_);
  storage_->Rename(journal_temporary_path_, journal_path_);
  storage_->SyncDirectory(directory_);
}

void LogStore::RewriteJournalFromCurrentState() { RewriteJournal(snapshot_base_index_, snapshot_base_term_, entries_); }

void LogStore::Append(const std::vector<ReplicatedLogEntry> &entries) {
  std::lock_guard lock(mutex_);
  AppendMutation(entries);
}

void LogStore::ReplaceSuffix(uint64_t from_index, const std::vector<ReplicatedLogEntry> &new_entries) {
  std::lock_guard lock(mutex_);
  if (from_index <= snapshot_base_index_ ||
      (from_index > LastLogIndexUnlocked() &&
       (LastLogIndexUnlocked() == std::numeric_limits<uint64_t>::max() || from_index != LastLogIndexUnlocked() + 1)) ||
      from_index <= effective_commit_index_) {
    throw std::runtime_error("invalid or committed Raft suffix replacement");
  }
  ValidateContinuousEntries(new_entries, from_index);
  const auto retained_count = static_cast<size_t>(from_index - snapshot_base_index_ - 1);
  CheckCanonicalJournalLimit(options_.maximum_journal_bytes_, entries_, 0, retained_count, new_entries);
  std::vector<ReplicatedLogEntry> candidate(entries_.begin(),
                                            entries_.begin() + static_cast<ptrdiff_t>(retained_count));
  candidate.insert(candidate.end(), new_entries.begin(), new_entries.end());
  RewriteJournal(snapshot_base_index_, snapshot_base_term_, candidate);
  entries_ = std::move(candidate);
}

void LogStore::InstallSnapshotBase(uint64_t index, uint64_t term, bool retain_old_suffix) {
  std::lock_guard lock(mutex_);
  const Mutation installation{MutationType::INSTALL_SNAPSHOT_BASE, index, term, retain_old_suffix, {}};
  ValidateMutation(installation, false);
  std::vector<ReplicatedLogEntry> retained;
  if (retain_old_suffix) {
    const auto first = index - snapshot_base_index_;
    CheckCanonicalJournalLimit(options_.maximum_journal_bytes_, entries_, static_cast<size_t>(first),
                               entries_.size() - static_cast<size_t>(first), {});
    retained.assign(entries_.begin() + static_cast<ptrdiff_t>(first), entries_.end());
  } else {
    CheckCanonicalJournalLimit(options_.maximum_journal_bytes_, entries_, 0, 0, {});
  }
  RewriteJournal(index, term, retained);
  snapshot_base_index_ = index;
  snapshot_base_term_ = term;
  entries_ = std::move(retained);
  effective_commit_index_ = std::max(effective_commit_index_, index);
}

void LogStore::AdvanceCommittedIndex(uint64_t committed_index) {
  std::lock_guard lock(mutex_);
  if (committed_index < effective_commit_index_ || committed_index > LastLogIndexUnlocked()) {
    throw std::runtime_error("invalid committed Raft log index");
  }
  effective_commit_index_ = committed_index;
}

auto LogStore::SnapshotBaseIndex() const -> uint64_t {
  std::lock_guard lock(mutex_);
  return snapshot_base_index_;
}

auto LogStore::SnapshotBaseTerm() const -> uint64_t {
  std::lock_guard lock(mutex_);
  return snapshot_base_term_;
}

auto LogStore::CommittedIndex() const -> uint64_t {
  std::lock_guard lock(mutex_);
  return effective_commit_index_;
}

auto LogStore::LastLogIndexUnlocked() const -> uint64_t {
  return entries_.empty() ? snapshot_base_index_ : entries_.back().index_;
}

auto LogStore::LastLogIndex() const -> uint64_t {
  std::lock_guard lock(mutex_);
  return LastLogIndexUnlocked();
}

auto LogStore::LastLogTerm() const -> uint64_t {
  std::lock_guard lock(mutex_);
  return entries_.empty() ? snapshot_base_term_ : entries_.back().term_;
}

auto LogStore::TermAtUnlocked(uint64_t index) const -> std::optional<uint64_t> {
  if (index == snapshot_base_index_) {
    return snapshot_base_term_;
  }
  if (index <= snapshot_base_index_ || index > LastLogIndexUnlocked()) {
    return std::nullopt;
  }
  return entries_[index - snapshot_base_index_ - 1].term_;
}

auto LogStore::TermAt(uint64_t index) const -> std::optional<uint64_t> {
  std::lock_guard lock(mutex_);
  return TermAtUnlocked(index);
}

auto LogStore::EntryAt(uint64_t index) const -> std::optional<ReplicatedLogEntry> {
  std::lock_guard lock(mutex_);
  if (index <= snapshot_base_index_ || index > LastLogIndexUnlocked()) {
    return std::nullopt;
  }
  return entries_[index - snapshot_base_index_ - 1];
}

auto LogStore::Entries(uint64_t first_index, uint64_t last_index) const -> std::vector<ReplicatedLogEntry> {
  std::lock_guard lock(mutex_);
  if (first_index > last_index) {
    return {};
  }
  if (first_index <= snapshot_base_index_ || last_index > LastLogIndexUnlocked()) {
    throw std::out_of_range("requested Raft log range is unavailable");
  }
  const auto first = entries_.begin() + static_cast<ptrdiff_t>(first_index - snapshot_base_index_ - 1);
  const auto last = entries_.begin() + static_cast<ptrdiff_t>(last_index - snapshot_base_index_);
  return {first, last};
}

void LogStore::ValidateCommittedRange() const {
  if (effective_commit_index_ < snapshot_base_index_ || LastLogIndexUnlocked() < effective_commit_index_) {
    throw std::runtime_error("committed Raft log range is unavailable");
  }
}

}  // namespace bustub
