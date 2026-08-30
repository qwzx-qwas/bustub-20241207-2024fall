//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// power_loss_storage.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "recovery/durable_storage.h"

namespace bustub {

/** Stable names used in failure reports and replay plans. */
enum class StorageFaultPoint { BEFORE_WRITE, AFTER_FSYNC, AFTER_RENAME, AFTER_DIR_FSYNC };

inline auto StorageFaultPointName(StorageFaultPoint point) -> std::string_view {
  switch (point) {
    case StorageFaultPoint::BEFORE_WRITE:
      return "before_write";
    case StorageFaultPoint::AFTER_FSYNC:
      return "after_fsync";
    case StorageFaultPoint::AFTER_RENAME:
      return "after_rename";
    case StorageFaultPoint::AFTER_DIR_FSYNC:
      return "after_dir_fsync";
  }
  throw std::runtime_error("unknown storage fault point");
}

struct StorageFaultPlan {
  StorageFaultPoint point_;
  size_t occurrence_;

  auto Name() const -> std::string {
    return std::string(StorageFaultPointName(point_)) + "-" + std::to_string(occurrence_);
  }
};

struct StorageEvent {
  StorageFaultPoint point_;
  size_t occurrence_;
  std::filesystem::path path_;
  std::filesystem::path related_path_;

  friend auto operator==(const StorageEvent &lhs, const StorageEvent &rhs) -> bool {
    return lhs.point_ == rhs.point_ && lhs.occurrence_ == rhs.occurrence_ && lhs.path_ == rhs.path_ &&
           lhs.related_path_ == rhs.related_path_;
  }
};

using StorageEventTopology = std::vector<StorageEvent>;

template <typename State>
struct AtomicDurabilityRun {
  State recovered_state_;
  std::vector<StorageEvent> events_;
  bool fault_triggered_{false};
};

/** Verifies the literal event topology, then applies one old-or-new recovery oracle at every named event. */
template <typename State, typename Scenario>
void VerifyAtomicDurableTransition(const State &old_state, const State &new_state,
                                   const StorageEventTopology &expected_events, Scenario scenario) {
  if (expected_events.empty()) {
    throw std::runtime_error("durable transition expected topology is empty");
  }
  std::map<StorageFaultPoint, size_t> expected_occurrences;
  for (const auto &event : expected_events) {
    if (event.path_.empty() || event.occurrence_ != ++expected_occurrences[event.point_]) {
      throw std::runtime_error("durable transition expected topology has an invalid path or occurrence");
    }
  }
  const auto complete = scenario(std::nullopt);
  if (complete.fault_triggered_ || !(complete.recovered_state_ == new_state)) {
    throw std::runtime_error("uninterrupted durable transition did not recover the new state");
  }
  if (complete.events_.size() != expected_events.size()) {
    throw std::runtime_error("durable transition event count differs from the expected topology");
  }
  for (size_t index = 0; index < expected_events.size(); index++) {
    if (!(complete.events_[index] == expected_events[index])) {
      throw std::runtime_error("durable transition event differs from the expected topology at index " +
                               std::to_string(index));
    }
  }
  for (size_t index = 0; index < expected_events.size(); index++) {
    const auto &event = expected_events[index];
    const StorageFaultPlan plan{event.point_, event.occurrence_};
    const auto failed = scenario(plan);
    if (!failed.fault_triggered_) {
      throw std::runtime_error("named storage fault did not trigger: " + plan.Name());
    }
    if (failed.events_.size() <= index ||
        !std::equal(expected_events.begin(), expected_events.begin() + static_cast<ptrdiff_t>(index + 1),
                    failed.events_.begin())) {
      throw std::runtime_error("faulted transition diverged from the expected event topology before: " + plan.Name());
    }
    if (!(failed.recovered_state_ == old_state) && !(failed.recovered_state_ == new_state)) {
      throw std::runtime_error("recovery produced a mixed state at named storage fault: " + plan.Name());
    }
  }
}

/**
 * Test-only storage that records semantic durability events and restores the last
 * file/directory-synced image when PowerLoss is requested.
 */
class PowerLossStorage : public DurableStorage {
 public:
  explicit PowerLossStorage(std::filesystem::path root) : root_(Absolute(std::move(root))) {}

  void ResetEventHistory() {
    events_.clear();
    event_counts_.clear();
    fault_plan_.reset();
    fault_triggered_ = false;
  }

  void FailAt(StorageFaultPlan plan) {
    ResetEventHistory();
    if (plan.occurrence_ == 0) {
      throw std::runtime_error("storage fault occurrence must be positive");
    }
    fault_plan_ = plan;
  }

  void DisableFailure() { fault_plan_.reset(); }
  auto FaultTriggered() const -> bool { return fault_triggered_; }
  auto Events() const -> const std::vector<StorageEvent> & { return events_; }

  void PowerLoss() {
    fault_plan_.reset();
    auto directories = ReachableDurableDirectories();
    std::map<std::filesystem::path, std::vector<std::byte>> files;
    for (const auto &[relative, bytes] : durable_files_) {
      if (IsDurableParentReachable(relative.parent_path(), directories)) {
        files.emplace(relative, bytes);
      }
    }
    durable_directories_ = std::move(directories);
    durable_files_ = std::move(files);
    delegate_.RemoveTree(root_);
    delegate_.CreateDirectories(root_);
    for (const auto &directory : durable_directories_) {
      delegate_.CreateDirectories(root_ / directory);
    }
    synced_files_.clear();
    for (const auto &[relative, bytes] : durable_files_) {
      const auto path = root_ / relative;
      delegate_.CreateDirectories(path.parent_path());
      delegate_.WriteFile(path, bytes);
      synced_files_.insert(Absolute(path));
    }
  }

  void CreateDirectories(const std::filesystem::path &path) override { delegate_.CreateDirectories(path); }

  void WriteFile(const std::filesystem::path &path, const std::vector<std::byte> &data) override {
    Record(StorageFaultPoint::BEFORE_WRITE, path);
    delegate_.WriteFile(path, data);
    synced_files_.erase(Absolute(path));
  }

  void AppendFile(const std::filesystem::path &path, const std::vector<std::byte> &data) override {
    Record(StorageFaultPoint::BEFORE_WRITE, path);
    delegate_.AppendFile(path, data);
    synced_files_.erase(Absolute(path));
  }

  void AppendFileDurable(const std::filesystem::path &path, const std::vector<std::byte> &data) override {
    Record(StorageFaultPoint::BEFORE_WRITE, path);
    delegate_.AppendFileDurable(path, data);
    synced_files_.insert(Absolute(path));
    CaptureSyncedExistingFile(path);
    Record(StorageFaultPoint::AFTER_FSYNC, path);
  }

  auto ReadFile(const std::filesystem::path &path, size_t maximum_size) -> std::vector<std::byte> override {
    return delegate_.ReadFile(path, maximum_size);
  }

  auto ReadFileRange(const std::filesystem::path &path, uint64_t offset, size_t maximum_size)
      -> std::vector<std::byte> override {
    return delegate_.ReadFileRange(path, offset, maximum_size);
  }

  auto FileSize(const std::filesystem::path &path) -> uint64_t override { return delegate_.FileSize(path); }

  void TruncateFile(const std::filesystem::path &path, uint64_t size) override {
    Record(StorageFaultPoint::BEFORE_WRITE, path);
    delegate_.TruncateFile(path, size);
    synced_files_.insert(Absolute(path));
    CaptureSyncedExistingFile(path);
    Record(StorageFaultPoint::AFTER_FSYNC, path);
  }

  auto ChecksumFile(const std::filesystem::path &path) -> uint32_t override { return delegate_.ChecksumFile(path); }

  void SyncFile(const std::filesystem::path &path) override {
    delegate_.SyncFile(path);
    synced_files_.insert(Absolute(path));
    CaptureSyncedExistingFile(path);
    Record(StorageFaultPoint::AFTER_FSYNC, path);
  }

  void SyncDirectory(const std::filesystem::path &path) override {
    delegate_.SyncDirectory(path);
    CaptureDirectoryEntries(path);
    Record(StorageFaultPoint::AFTER_DIR_FSYNC, path);
  }

  void Rename(const std::filesystem::path &from, const std::filesystem::path &to) override {
    const auto absolute_from = Absolute(from);
    const auto absolute_to = Absolute(to);
    const bool is_directory = std::filesystem::is_directory(absolute_from);
    delegate_.Rename(from, to);
    MoveSyncedPaths(absolute_from, absolute_to);
    if (is_directory) {
      CloneDurableDirectoryContents(absolute_from, absolute_to);
    }
    Record(StorageFaultPoint::AFTER_RENAME, to, from);
  }

  void CopyFile(const std::filesystem::path &from, const std::filesystem::path &to) override {
    Record(StorageFaultPoint::BEFORE_WRITE, to, from);
    delegate_.CopyFile(from, to);
    synced_files_.erase(Absolute(to));
  }

  void RemoveFile(const std::filesystem::path &path) override {
    Record(StorageFaultPoint::BEFORE_WRITE, path);
    delegate_.RemoveFile(path);
    synced_files_.erase(Absolute(path));
  }

  void RemoveTree(const std::filesystem::path &path) override {
    Record(StorageFaultPoint::BEFORE_WRITE, path);
    delegate_.RemoveTree(path);
    EraseSyncedPrefix(Absolute(path));
  }

  auto Exists(const std::filesystem::path &path) const -> bool override { return delegate_.Exists(path); }
  auto ListDirectory(const std::filesystem::path &path) const -> std::vector<std::string> override {
    return delegate_.ListDirectory(path);
  }

 private:
  static auto Absolute(const std::filesystem::path &path) -> std::filesystem::path {
    return std::filesystem::absolute(path).lexically_normal();
  }

  static auto IsWithin(const std::filesystem::path &path, const std::filesystem::path &directory) -> bool {
    const auto relative = path.lexically_relative(directory);
    return !relative.empty() && *relative.begin() != "..";
  }

  auto Relative(const std::filesystem::path &path) const -> std::filesystem::path {
    const auto absolute = Absolute(path);
    if (absolute == root_) {
      return ".";
    }
    return IsWithin(absolute, root_) ? absolute.lexically_relative(root_) : absolute;
  }

  void Record(StorageFaultPoint point, const std::filesystem::path &path,
              const std::filesystem::path &related_path = {}) {
    const auto occurrence = ++event_counts_[point];
    events_.push_back(
        {point, occurrence, Relative(path), related_path.empty() ? related_path : Relative(related_path)});
    if (fault_plan_.has_value() && fault_plan_->point_ == point && fault_plan_->occurrence_ == occurrence) {
      fault_triggered_ = true;
      throw std::runtime_error("injected power loss at " + fault_plan_->Name() + " path=" + Relative(path).string());
    }
  }

  void CaptureSyncedExistingFile(const std::filesystem::path &path) {
    const auto relative = Absolute(path).lexically_relative(root_);
    const auto iterator = durable_files_.find(relative);
    if (iterator == durable_files_.end()) {
      return;
    }
    const auto size = static_cast<size_t>(std::filesystem::file_size(path));
    iterator->second = delegate_.ReadFile(path, size);
  }

  void MoveSyncedPaths(const std::filesystem::path &from, const std::filesystem::path &to) {
    std::set<std::filesystem::path> moved;
    for (const auto &path : synced_files_) {
      if (path == from) {
        moved.insert(to);
      } else if (IsWithin(path, from)) {
        moved.insert(to / path.lexically_relative(from));
      } else {
        moved.insert(path);
      }
    }
    synced_files_ = std::move(moved);
  }

  void EraseSyncedPrefix(const std::filesystem::path &prefix) {
    for (auto iterator = synced_files_.begin(); iterator != synced_files_.end();) {
      if (*iterator == prefix || IsWithin(*iterator, prefix)) {
        iterator = synced_files_.erase(iterator);
      } else {
        ++iterator;
      }
    }
  }

  static auto IsRootRelativeDirectory(const std::filesystem::path &path) -> bool { return path.empty() || path == "."; }

  static auto IsDirectChild(const std::filesystem::path &path, const std::filesystem::path &directory) -> bool {
    const auto parent = path.parent_path();
    return IsRootRelativeDirectory(directory) ? IsRootRelativeDirectory(parent) : parent == directory;
  }

  static auto IsDurableParentReachable(const std::filesystem::path &parent,
                                       const std::set<std::filesystem::path> &directories) -> bool {
    if (IsRootRelativeDirectory(parent)) {
      return true;
    }
    auto prefix = std::filesystem::path{};
    for (const auto &component : parent) {
      prefix /= component;
      if (directories.count(prefix) == 0) {
        return false;
      }
    }
    return true;
  }

  auto ReachableDurableDirectories() const -> std::set<std::filesystem::path> {
    std::vector<std::filesystem::path> ordered(durable_directories_.begin(), durable_directories_.end());
    std::sort(ordered.begin(), ordered.end(), [](const auto &lhs, const auto &rhs) {
      const auto lhs_depth = std::distance(lhs.begin(), lhs.end());
      const auto rhs_depth = std::distance(rhs.begin(), rhs.end());
      return lhs_depth == rhs_depth ? lhs.generic_string() < rhs.generic_string() : lhs_depth < rhs_depth;
    });
    std::set<std::filesystem::path> reachable;
    for (const auto &directory : ordered) {
      if (IsDurableParentReachable(directory.parent_path(), reachable)) {
        reachable.insert(directory);
      }
    }
    return reachable;
  }

  void EraseDurablePrefix(const std::filesystem::path &prefix) {
    for (auto iterator = durable_directories_.begin(); iterator != durable_directories_.end();) {
      if (*iterator == prefix || IsWithin(*iterator, prefix)) {
        iterator = durable_directories_.erase(iterator);
      } else {
        ++iterator;
      }
    }
    for (auto iterator = durable_files_.begin(); iterator != durable_files_.end();) {
      if (iterator->first == prefix || IsWithin(iterator->first, prefix)) {
        iterator = durable_files_.erase(iterator);
      } else {
        ++iterator;
      }
    }
  }

  void CloneDurableDirectoryContents(const std::filesystem::path &from, const std::filesystem::path &to) {
    const auto relative_from = Relative(from);
    const auto relative_to = Relative(to);
    std::vector<std::filesystem::path> directories;
    std::vector<std::pair<std::filesystem::path, std::vector<std::byte>>> files;
    for (const auto &directory : durable_directories_) {
      if (directory != relative_from && IsWithin(directory, relative_from)) {
        directories.push_back(relative_to / directory.lexically_relative(relative_from));
      }
    }
    for (const auto &[path, bytes] : durable_files_) {
      if (IsWithin(path, relative_from)) {
        files.emplace_back(relative_to / path.lexically_relative(relative_from), bytes);
      }
    }
    durable_directories_.insert(directories.begin(), directories.end());
    for (auto &[path, bytes] : files) {
      durable_files_[std::move(path)] = std::move(bytes);
    }
  }

  void CaptureDirectoryEntries(const std::filesystem::path &path) {
    const auto directory = Absolute(path);
    if (directory != root_ && !IsWithin(directory, root_)) {
      throw std::runtime_error("cannot capture a directory outside the power-loss root");
    }
    const auto relative_directory = Relative(directory);
    std::set<std::filesystem::path> current_directories;
    std::set<std::filesystem::path> current_files;
    for (const auto &item : std::filesystem::directory_iterator(directory)) {
      const auto relative = Relative(item.path());
      if (item.is_directory()) {
        current_directories.insert(relative);
      } else if (item.is_regular_file()) {
        current_files.insert(relative);
      }
    }

    std::vector<std::filesystem::path> removed_directories;
    for (const auto &durable : durable_directories_) {
      if (IsDirectChild(durable, relative_directory) && current_directories.count(durable) == 0) {
        removed_directories.push_back(durable);
      }
    }
    for (const auto &removed : removed_directories) {
      EraseDurablePrefix(removed);
    }
    for (auto iterator = durable_files_.begin(); iterator != durable_files_.end();) {
      if (IsDirectChild(iterator->first, relative_directory) && current_files.count(iterator->first) == 0) {
        iterator = durable_files_.erase(iterator);
      } else {
        ++iterator;
      }
    }

    for (const auto &current : current_directories) {
      durable_files_.erase(current);
      durable_directories_.insert(current);
    }
    for (const auto &current : current_files) {
      if (durable_directories_.count(current) != 0) {
        EraseDurablePrefix(current);
      }
      const auto absolute = root_ / current;
      if (synced_files_.count(Absolute(absolute)) != 0) {
        const auto size = static_cast<size_t>(std::filesystem::file_size(absolute));
        durable_files_[current] = delegate_.ReadFile(absolute, size);
      }
    }
  }

  std::filesystem::path root_;
  PosixDurableStorage delegate_;
  std::set<std::filesystem::path> synced_files_;
  std::set<std::filesystem::path> durable_directories_;
  std::map<std::filesystem::path, std::vector<std::byte>> durable_files_;
  std::vector<StorageEvent> events_;
  std::map<StorageFaultPoint, size_t> event_counts_;
  std::optional<StorageFaultPlan> fault_plan_;
  bool fault_triggered_{false};
};

}  // namespace bustub
