//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// snapshot_manager.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/snapshot_manager.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/catalog_snapshot.h"
#include "recovery/canonical_snapshot.h"

namespace bustub {
namespace {

auto ParseGenerationName(std::string_view name, std::string_view prefix) -> std::optional<uint64_t> {
  constexpr size_t digits = 20;
  if (name.size() != prefix.size() + digits || name.substr(0, prefix.size()) != prefix) {
    return std::nullopt;
  }
  uint64_t generation = 0;
  for (size_t offset = prefix.size(); offset < name.size(); offset++) {
    if (name[offset] < '0' || name[offset] > '9') {
      return std::nullopt;
    }
    const auto digit = static_cast<uint64_t>(name[offset] - '0');
    if (generation > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return std::nullopt;
    }
    generation = generation * 10 + digit;
  }
  return generation == 0 ? std::nullopt : std::optional<uint64_t>{generation};
}

auto IsTemporaryStateArtifact(std::string_view name) -> bool {
  const auto has_temporary_suffix = name.size() > 4 && name.substr(name.size() - 4) == ".tmp";
  return name == "CURRENT.tmp" ||
         (has_temporary_suffix && (name.substr(0, 9) == "MANIFEST-" || name.substr(0, 9) == "SNAPSHOT-"));
}

void RemovePath(DurableStorage *storage, const std::filesystem::path &path) {
  std::error_code error;
  if (std::filesystem::is_directory(path, error) && !error) {
    storage->RemoveTree(path);
  } else {
    storage->RemoveFile(path);
  }
}

}  // namespace

auto SnapshotManager::SnapshotDirectoryName(uint64_t generation) -> std::string {
  std::ostringstream output;
  output << "SNAPSHOT-" << std::setw(20) << std::setfill('0') << generation;
  return output.str();
}

auto SnapshotManager::CreateSnapshot(const Catalog &catalog, const SessionTable &sessions,
                                     StateVisibilityLatch *visibility_latch, uint64_t generation,
                                     uint64_t last_included_index, uint64_t last_included_term) -> StateManifest {
  if (node_directory_ == nullptr || storage_ == nullptr || visibility_latch == nullptr || generation == 0 ||
      last_included_term != 0) {
    throw std::runtime_error("invalid snapshot request");
  }
  const auto state_directory = node_directory_->StateDirectory();
  const auto snapshot_name = SnapshotDirectoryName(generation);
  const auto temporary = state_directory / (snapshot_name + ".tmp");
  const auto final = state_directory / snapshot_name;
  if (storage_->Exists(final)) {
    throw std::runtime_error("snapshot generation already exists");
  }
  if (storage_->Exists(temporary)) {
    storage_->RemoveTree(temporary);
  }
  const CanonicalSnapshotPaths paths{temporary / "db.bustub", temporary / "catalog.bin", temporary / "session.bin"};
  CanonicalSnapshotResult built;
  {
    // M1 snapshot barrier: logical files are complete and closed before readers are admitted again. The caller keeps
    // write proposal admission closed until publication finishes, so later working-state changes cannot enter them.
    auto exclusive = visibility_latch->LockExclusive();
    sessions.ValidateSnapshotBoundary(last_included_index, 0);
    storage_->CreateDirectories(temporary);
    built = CanonicalSnapshotBuilder::BuildUnsynced(catalog, sessions, paths, storage_.get());
  }
  const auto database_checksum = storage_->ChecksumFile(paths.database_file_);
  const auto catalog_checksum = storage_->ChecksumFile(paths.catalog_file_);
  const auto session_checksum = storage_->ChecksumFile(paths.session_file_);
  storage_->SyncFile(paths.database_file_);
  storage_->SyncFile(paths.catalog_file_);
  storage_->SyncFile(paths.session_file_);
  storage_->SyncDirectory(temporary);
  storage_->Rename(temporary, final);
  storage_->SyncDirectory(state_directory);

  StateManifest manifest{1,
                         generation,
                         last_included_index,
                         last_included_term,
                         built.catalog_.schema_epoch_,
                         snapshot_name + "/db.bustub",
                         snapshot_name + "/catalog.bin",
                         snapshot_name + "/session.bin",
                         database_checksum,
                         catalog_checksum,
                         session_checksum,
                         built.catalog_.next_table_oid_,
                         built.catalog_.next_index_oid_};
  manifests_.Publish(manifest);
  return manifest;
}

auto SnapshotManager::Recover(const std::function<bool(uint64_t)> &has_bridge_log, size_t buffer_pool_size)
    -> std::unique_ptr<RecoveredSnapshot> {
  if (node_directory_ == nullptr || storage_ == nullptr) {
    throw std::runtime_error("invalid snapshot manager");
  }
  const auto selected = manifests_.SelectRecoveryPoint(has_bridge_log);
  if (!selected.has_value()) {
    return nullptr;
  }
  const auto &manifest = selected->manifest_;
  const auto state_directory = node_directory_->StateDirectory();
  const auto working_directory = node_directory_->WorkingDirectory();
  const auto working_tmp = working_directory / "db.bustub.tmp";
  const auto working = node_directory_->WorkingDatabase();
  storage_->RemoveFile(working_tmp);
  storage_->CopyFile(state_directory / manifest.database_file_, working_tmp);
  storage_->SyncFile(working_tmp);
  storage_->Rename(working_tmp, working);
  storage_->SyncDirectory(working_directory);

  auto recovered = std::make_unique<RecoveredSnapshot>();
  recovered->manifest_ = manifest;
  recovered->disk_manager_ = std::make_unique<DiskManager>(working);
  recovered->buffer_pool_manager_ =
      std::make_unique<BufferPoolManager>(buffer_pool_size, recovered->disk_manager_.get());
  recovered->catalog_ = std::make_unique<Catalog>(recovered->buffer_pool_manager_.get(), nullptr, nullptr);
  const auto catalog_snapshot = CatalogSnapshotCodec::Decode(
      storage_->ReadFile(state_directory / manifest.catalog_file_, CatalogSnapshotCodec::MAX_CATALOG_BYTES));
  ValidateReplicatedCatalogV1(catalog_snapshot);
  CatalogSnapshotCodec::Restore(catalog_snapshot, recovered->catalog_.get(), recovered->buffer_pool_manager_.get(),
                                nullptr);
  recovered->sessions_ = std::make_unique<SessionTable>();
  SessionSnapshotCodec::DecodeInto(storage_->ReadFile(state_directory / manifest.session_file_, 64U * 1024U * 1024U),
                                   recovered->sessions_.get());
  recovered->sessions_->ValidateSnapshotBoundary(manifest.last_included_index_, 0);
  recovered->last_applied_ = manifest.last_included_index_;
  recovered->published_applied_index_ = manifest.last_included_index_;
  return recovered;
}

void SnapshotManager::PruneToTwo(const std::function<bool(uint64_t)> &can_recover_from,
                                 const std::function<void(uint64_t)> &oldest_boundary_advanced) {
  if (node_directory_ == nullptr || storage_ == nullptr) {
    throw std::runtime_error("invalid snapshot manager");
  }
  const auto state_directory = node_directory_->StateDirectory();
  struct ManifestCandidate {
    uint64_t generation_;
    StateManifest manifest_;
    std::filesystem::path path_;
  };
  std::vector<ManifestCandidate> valid;
  std::vector<std::pair<uint64_t, std::filesystem::path>> invalid_manifests;
  std::vector<std::pair<uint64_t, std::filesystem::path>> snapshot_directories;
  std::vector<std::filesystem::path> temporary_artifacts;
  for (const auto &name : storage_->ListDirectory(state_directory)) {
    const auto path = state_directory / name;
    if (IsTemporaryStateArtifact(name)) {
      temporary_artifacts.push_back(path);
      continue;
    }
    if (const auto generation = ParseGenerationName(name, "SNAPSHOT-"); generation.has_value()) {
      snapshot_directories.emplace_back(*generation, path);
      continue;
    }
    const auto generation = ParseGenerationName(name, "MANIFEST-");
    if (!generation.has_value()) {
      continue;
    }
    try {
      auto manifest = StateManifestCodec::Decode(storage_->ReadFile(path, StateManifestCodec::MAX_MANIFEST_BYTES));
      if (manifest.generation_ != *generation || !manifests_.Validate(manifest)) {
        invalid_manifests.emplace_back(*generation, path);
      } else {
        valid.push_back({*generation, std::move(manifest), path});
      }
    } catch (const std::exception &) {
      invalid_manifests.emplace_back(*generation, path);
    }
  }
  std::sort(valid.begin(), valid.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.generation_ > rhs.generation_; });

  size_t retained_count = valid.size();
  if (valid.size() > 2 && (!can_recover_from || can_recover_from(valid[1].manifest_.last_included_index_))) {
    retained_count = 2;
  }

  std::set<std::filesystem::path> retained_snapshot_directories;
  for (size_t index = 0; index < retained_count; index++) {
    retained_snapshot_directories.insert((state_directory / valid[index].manifest_.database_file_).parent_path());
  }

  bool removed = false;
  bool removed_older_generation = false;
  const auto oldest_retained_generation =
      retained_count == 0 ? std::optional<uint64_t>{} : std::optional<uint64_t>{valid[retained_count - 1].generation_};
  std::set<std::filesystem::path> snapshot_removals;
  for (size_t index = retained_count; index < valid.size(); index++) {
    const auto snapshot_directory = (state_directory / valid[index].manifest_.database_file_).parent_path();
    if (retained_snapshot_directories.count(snapshot_directory) == 0) {
      snapshot_removals.insert(snapshot_directory);
    }
    removed_older_generation = true;
  }
  if (!valid.empty()) {
    for (const auto &[generation, path] : snapshot_directories) {
      if (retained_snapshot_directories.count(path) == 0) {
        snapshot_removals.insert(path);
        removed_older_generation = removed_older_generation ||
                                   (oldest_retained_generation.has_value() && generation < *oldest_retained_generation);
      }
    }
  }
  for (const auto &path : snapshot_removals) {
    RemovePath(storage_.get(), path);
    removed = true;
  }

  for (size_t index = retained_count; index < valid.size(); index++) {
    storage_->RemoveFile(valid[index].path_);
    removed = true;
  }
  if (!valid.empty()) {
    for (const auto &[generation, path] : invalid_manifests) {
      storage_->RemoveFile(path);
      removed = true;
      removed_older_generation = removed_older_generation ||
                                 (oldest_retained_generation.has_value() && generation < *oldest_retained_generation);
    }
  }
  for (const auto &path : temporary_artifacts) {
    RemovePath(storage_.get(), path);
    removed = true;
  }
  if (removed) {
    storage_->SyncDirectory(state_directory);
  }
  if (removed_older_generation && retained_count != 0 && oldest_boundary_advanced &&
      (!can_recover_from || can_recover_from(valid[retained_count - 1].manifest_.last_included_index_))) {
    oldest_boundary_advanced(valid[retained_count - 1].manifest_.last_included_index_);
  }
}

}  // namespace bustub
