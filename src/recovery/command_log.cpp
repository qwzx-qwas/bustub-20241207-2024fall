//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// command_log.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/command_log.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

namespace bustub {
namespace {

constexpr size_t MINIMUM_LOG_FRAME_BYTES = LogCodec::FRAME_HEADER_BYTES + LogCodec::FRAME_BODY_FIXED_BYTES;

auto CheckedAdd(size_t lhs, size_t rhs, const char *message) -> size_t {
  if (rhs > std::numeric_limits<size_t>::max() - lhs) {
    throw std::runtime_error(message);
  }
  return lhs + rhs;
}

auto EncodedBatchSize(const std::vector<ReplicatedLogEntry> &entries, size_t maximum_size) -> size_t {
  size_t encoded_size = 0;
  for (const auto &entry : entries) {
    if (entry.term_ != 0) {
      throw std::runtime_error("term-0 command log cannot store a nonzero Raft term");
    }
    if (entry.payload_.size() > LogCodec::MAX_PAYLOAD_BYTES) {
      throw std::runtime_error("replicated log entry exceeds the payload limit");
    }
    const auto frame_size =
        CheckedAdd(MINIMUM_LOG_FRAME_BYTES, entry.payload_.size(), "command log batch size overflow");
    encoded_size = CheckedAdd(encoded_size, frame_size, "command log batch size overflow");
    if (encoded_size > maximum_size) {
      throw std::runtime_error("command log append batch exceeds the configured maximum");
    }
  }
  return encoded_size;
}

auto ParseSegmentFirstIndex(const std::filesystem::path &path) -> std::optional<uint64_t> {
  const auto name = path.filename().string();
  constexpr std::string_view prefix = "LOG-";
  if (name.size() != prefix.size() + 20 || name.compare(0, prefix.size(), prefix) != 0) {
    return std::nullopt;
  }
  uint64_t value = 0;
  for (size_t i = prefix.size(); i < name.size(); i++) {
    if (name[i] < '0' || name[i] > '9') {
      return std::nullopt;
    }
    const auto digit = static_cast<uint64_t>(name[i] - '0');
    if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return std::nullopt;
    }
    value = value * 10 + digit;
  }
  return value;
}

}  // namespace

auto CommandLog::SegmentFileName(uint64_t first_index) -> std::string {
  std::ostringstream output;
  output << "LOG-" << std::setw(20) << std::setfill('0') << first_index;
  return output.str();
}

auto CommandLog::Open(const std::filesystem::path &directory, std::shared_ptr<DurableStorage> storage,
                      uint64_t effective_commit_index, uint64_t snapshot_base_index, uint64_t snapshot_base_term,
                      CommandLogOptions options) -> std::unique_ptr<CommandLog> {
  if (directory.empty() || storage == nullptr || options.segment_max_bytes_ < MINIMUM_LOG_FRAME_BYTES ||
      options.batch_max_bytes_ < MINIMUM_LOG_FRAME_BYTES || effective_commit_index < snapshot_base_index ||
      snapshot_base_term != 0) {
    throw std::runtime_error("invalid command log configuration");
  }
  storage->CreateDirectories(directory);
  auto log = std::unique_ptr<CommandLog>(
      new CommandLog(directory, std::move(storage), snapshot_base_index, snapshot_base_term, options));
  log->Recover(effective_commit_index);
  return log;
}

void CommandLog::Recover(uint64_t effective_commit_index) {
  std::vector<std::pair<uint64_t, std::filesystem::path>> files;
  for (const auto &item : std::filesystem::directory_iterator(directory_)) {
    if (!item.is_regular_file()) {
      continue;
    }
    if (auto first_index = ParseSegmentFirstIndex(item.path()); first_index.has_value()) {
      files.emplace_back(*first_index, item.path());
    }
  }
  std::sort(files.begin(), files.end(), [](const auto &lhs, const auto &rhs) { return lhs.first < rhs.first; });

  uint64_t expected_index = files.empty() ? snapshot_base_index_ + 1 : files.front().first;
  if (expected_index > snapshot_base_index_ + 1) {
    if (snapshot_base_index_ + 1 <= effective_commit_index) {
      throw std::runtime_error("committed command log prefix after the snapshot is missing");
    }
    expected_index = snapshot_base_index_ + 1;
  }
  bool discarded_tail = false;
  for (const auto &[declared_first_index, path] : files) {
    if (discarded_tail) {
      storage_->RemoveFile(path);
      continue;
    }
    if (declared_first_index != expected_index) {
      if (expected_index <= effective_commit_index) {
        throw std::runtime_error("committed command log has a missing or misnamed segment");
      }
      storage_->RemoveFile(path);
      discarded_tail = true;
      continue;
    }
    const auto maximum_size = std::max(options_.segment_max_bytes_, options_.batch_max_bytes_);
    const auto bytes = storage_->ReadFile(path, maximum_size);
    size_t offset = 0;
    while (offset < bytes.size()) {
      auto decoded = LogCodec::DecodeOne(bytes, offset);
      if (decoded.status_ != LogDecodeStatus::COMPLETE || decoded.entry_->index_ != expected_index ||
          decoded.entry_->term_ != 0) {
        if (expected_index <= effective_commit_index) {
          throw std::runtime_error("committed command log is corrupt at index " + std::to_string(expected_index));
        }
        storage_->TruncateFile(path, offset);
        discarded_tail = true;
        break;
      }
      if (expected_index > snapshot_base_index_) {
        entries_.push_back(std::move(*decoded.entry_));
      }
      expected_index++;
      offset += decoded.bytes_consumed_;
    }
    if (offset == 0 && bytes.empty()) {
      if (expected_index <= effective_commit_index) {
        throw std::runtime_error("committed command log segment is empty");
      }
      storage_->RemoveFile(path);
      discarded_tail = true;
      continue;
    }
    if (!discarded_tail || offset != 0) {
      const auto kept_size = discarded_tail ? offset : bytes.size();
      if (kept_size != 0) {
        segments_.push_back({declared_first_index, expected_index - 1, path, kept_size});
      }
    }
  }
  if (discarded_tail) {
    storage_->SyncDirectory(directory_);
  }
  if (LastLogIndex() < effective_commit_index) {
    throw std::runtime_error("committed command log suffix is missing");
  }
}

void CommandLog::Append(const std::vector<ReplicatedLogEntry> &entries) {
  std::lock_guard lock(mutex_);
  if (entries.empty()) {
    throw std::runtime_error("cannot append an empty log batch");
  }
  if (entries_.size() > std::numeric_limits<uint64_t>::max() - snapshot_base_index_ ||
      snapshot_base_index_ + entries_.size() == std::numeric_limits<uint64_t>::max()) {
    throw std::runtime_error("command log index space is exhausted");
  }
  const auto encoded_size = EncodedBatchSize(entries, options_.batch_max_bytes_);
  uint64_t expected = snapshot_base_index_ + entries_.size() + 1;
  std::vector<std::byte> encoded;
  encoded.reserve(encoded_size);
  for (size_t entry_offset = 0; entry_offset < entries.size(); entry_offset++) {
    const auto &entry = entries[entry_offset];
    if (entry.index_ != expected) {
      throw std::runtime_error("log append batch is not continuous");
    }
    auto frame = LogCodec::Encode(entry);
    encoded.insert(encoded.end(), frame.begin(), frame.end());
    if (entry_offset + 1 < entries.size() && expected == std::numeric_limits<uint64_t>::max()) {
      throw std::runtime_error("command log index space is exhausted");
    }
    if (entry_offset + 1 < entries.size()) {
      expected++;
    }
  }

  const bool new_segment = segments_.empty() || segments_.back().size_ > options_.segment_max_bytes_ ||
                           encoded.size() > options_.segment_max_bytes_ - segments_.back().size_;
  if (new_segment) {
    const auto path = directory_ / SegmentFileName(entries.front().index_);
    if (storage_->Exists(path)) {
      throw std::runtime_error("refusing to append through an existing segment name");
    }
    storage_->AppendFileDurable(path, encoded);
    storage_->SyncDirectory(directory_);
    segments_.push_back({entries.front().index_, entries.back().index_, path, encoded.size()});
  } else {
    storage_->AppendFileDurable(segments_.back().path_, encoded);
    segments_.back().last_index_ = entries.back().index_;
    segments_.back().size_ += encoded.size();
  }
  entries_.insert(entries_.end(), entries.begin(), entries.end());
}

void CommandLog::TruncateSuffix(uint64_t last_index_to_keep) {
  std::lock_guard lock(mutex_);
  const auto last_index = snapshot_base_index_ + entries_.size();
  if (last_index_to_keep < snapshot_base_index_ || last_index_to_keep > last_index) {
    throw std::runtime_error("invalid command log suffix truncation boundary");
  }
  if (last_index_to_keep == last_index) {
    return;
  }

  std::vector<Segment> kept_segments;
  kept_segments.reserve(segments_.size());
  bool removed_file = false;
  for (const auto &segment : segments_) {
    if (segment.first_index_ > last_index_to_keep) {
      storage_->RemoveFile(segment.path_);
      removed_file = true;
      continue;
    }
    if (segment.last_index_ <= last_index_to_keep) {
      kept_segments.push_back(segment);
      continue;
    }

    const auto maximum_size = std::max(options_.segment_max_bytes_, options_.batch_max_bytes_);
    const auto bytes = storage_->ReadFile(segment.path_, maximum_size);
    size_t offset = 0;
    uint64_t expected = segment.first_index_;
    while (expected <= last_index_to_keep) {
      const auto decoded = LogCodec::DecodeOne(bytes, offset);
      if (decoded.status_ != LogDecodeStatus::COMPLETE || decoded.entry_->index_ != expected) {
        throw std::runtime_error("cannot locate durable command log truncation boundary");
      }
      offset += decoded.bytes_consumed_;
      expected++;
    }
    if (offset == 0) {
      storage_->RemoveFile(segment.path_);
      removed_file = true;
    } else {
      storage_->TruncateFile(segment.path_, offset);
      kept_segments.push_back({segment.first_index_, last_index_to_keep, segment.path_, offset});
    }
  }
  if (removed_file) {
    storage_->SyncDirectory(directory_);
  }
  segments_ = std::move(kept_segments);
  entries_.resize(static_cast<size_t>(last_index_to_keep - snapshot_base_index_));
}

void CommandLog::CompactPrefix(uint64_t compact_through) {
  std::lock_guard lock(mutex_);
  if (compact_through > snapshot_base_index_) {
    throw std::runtime_error("command log compaction exceeds the active snapshot base");
  }
  size_t remove_count = 0;
  while (remove_count < segments_.size() && segments_[remove_count].last_index_ <= compact_through) {
    storage_->RemoveFile(segments_[remove_count].path_);
    remove_count++;
  }
  if (remove_count != 0) {
    storage_->SyncDirectory(directory_);
    segments_.erase(segments_.begin(), segments_.begin() + static_cast<ptrdiff_t>(remove_count));
  }
}

auto CommandLog::LastLogIndex() const -> uint64_t {
  std::lock_guard lock(mutex_);
  return snapshot_base_index_ + entries_.size();
}

auto CommandLog::TermAt(uint64_t index) const -> std::optional<uint64_t> {
  std::lock_guard lock(mutex_);
  if (index == snapshot_base_index_) {
    return snapshot_base_term_;
  }
  if (index <= snapshot_base_index_ || index > snapshot_base_index_ + entries_.size()) {
    return std::nullopt;
  }
  return entries_[index - snapshot_base_index_ - 1].term_;
}

auto CommandLog::EntryAt(uint64_t index) const -> std::optional<ReplicatedLogEntry> {
  std::lock_guard lock(mutex_);
  if (index <= snapshot_base_index_ || index > snapshot_base_index_ + entries_.size()) {
    return std::nullopt;
  }
  return entries_[index - snapshot_base_index_ - 1];
}

auto CommandLog::Entries(uint64_t first_index, uint64_t last_index) const -> std::vector<ReplicatedLogEntry> {
  std::lock_guard lock(mutex_);
  if (first_index > last_index) {
    return {};
  }
  if (first_index <= snapshot_base_index_ || last_index > snapshot_base_index_ + entries_.size()) {
    throw std::out_of_range("requested log range is unavailable");
  }
  const auto first = entries_.begin() + static_cast<ptrdiff_t>(first_index - snapshot_base_index_ - 1);
  const auto last = entries_.begin() + static_cast<ptrdiff_t>(last_index - snapshot_base_index_);
  return {first, last};
}

}  // namespace bustub
