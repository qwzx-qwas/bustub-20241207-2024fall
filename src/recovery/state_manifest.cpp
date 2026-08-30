//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// state_manifest.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/state_manifest.h"

#include <algorithm>
#include <array>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "catalog/catalog_snapshot.h"
#include "common/byte_codec.h"
#include "distributed/session_table.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> MANIFEST_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'M'},
                                                  std::byte{'A'}, std::byte{'N'}, std::byte{'0'}, std::byte{'1'}};

auto BytesFromString(const std::string &value) -> std::vector<std::byte> {
  const auto *begin = reinterpret_cast<const std::byte *>(value.data());
  return {begin, begin + value.size()};
}

auto StringFromBytes(const std::vector<std::byte> &value) -> std::string {
  return {reinterpret_cast<const char *>(value.data()), value.size()};
}

auto ParseManifestGeneration(const std::string &name) -> std::optional<uint64_t> {
  constexpr std::string_view prefix = "MANIFEST-";
  if (name.size() != prefix.size() + 20 || name.compare(0, prefix.size(), prefix) != 0) {
    return std::nullopt;
  }
  uint64_t generation = 0;
  for (size_t i = prefix.size(); i < name.size(); i++) {
    if (name[i] < '0' || name[i] > '9') {
      return std::nullopt;
    }
    const auto digit = static_cast<uint64_t>(name[i] - '0');
    if (generation > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return std::nullopt;
    }
    generation = generation * 10 + digit;
  }
  if (generation == 0) {
    return std::nullopt;
  }
  return generation;
}

auto IsResolvedWithin(const std::filesystem::path &directory, const std::filesystem::path &candidate) -> bool {
  std::error_code error;
  const auto resolved_directory = std::filesystem::canonical(directory, error);
  if (error) {
    return false;
  }
  const auto resolved_candidate = std::filesystem::canonical(candidate, error);
  if (error) {
    return false;
  }
  auto directory_component = resolved_directory.begin();
  auto candidate_component = resolved_candidate.begin();
  while (directory_component != resolved_directory.end()) {
    if (candidate_component == resolved_candidate.end() || *directory_component != *candidate_component) {
      return false;
    }
    ++directory_component;
    ++candidate_component;
  }
  return candidate_component != resolved_candidate.end();
}

}  // namespace

auto StateManifestCodec::IsSafeRelativePath(const std::string &value) -> bool {
  if (value.empty()) {
    return false;
  }
  const std::filesystem::path path(value);
  if (path.is_absolute() || path.has_root_path() || path.lexically_normal() != path) {
    return false;
  }
  return std::all_of(path.begin(), path.end(),
                     [](const auto &component) { return component != ".." && component != "." && !component.empty(); });
}

auto StateManifestCodec::Encode(const StateManifest &manifest) -> std::vector<std::byte> {
  if (manifest.format_version_ != FORMAT_VERSION || manifest.generation_ == 0 || manifest.last_included_term_ != 0 ||
      !IsSafeRelativePath(manifest.database_file_) || !IsSafeRelativePath(manifest.catalog_file_) ||
      !IsSafeRelativePath(manifest.session_file_) || manifest.database_file_ == manifest.catalog_file_ ||
      manifest.database_file_ == manifest.session_file_ || manifest.catalog_file_ == manifest.session_file_) {
    throw std::runtime_error("invalid state manifest");
  }
  ByteWriter payload;
  payload.PutU64(manifest.generation_);
  payload.PutU64(manifest.last_included_index_);
  payload.PutU64(manifest.last_included_term_);
  payload.PutU64(manifest.schema_epoch_);
  payload.PutString(manifest.database_file_);
  payload.PutString(manifest.catalog_file_);
  payload.PutString(manifest.session_file_);
  payload.PutU32(manifest.database_checksum_);
  payload.PutU32(manifest.catalog_checksum_);
  payload.PutU32(manifest.session_checksum_);
  payload.PutU32(manifest.next_table_oid_);
  payload.PutU32(manifest.next_index_oid_);
  if (payload.Data().size() > MAX_MANIFEST_BYTES) {
    throw std::runtime_error("state manifest exceeds the V1 size limit");
  }

  return EncodeVersionedFrame(
      {MANIFEST_MAGIC.data(), MANIFEST_MAGIC.size(), FORMAT_VERSION, MAX_MANIFEST_BYTES, "state manifest"},
      payload.Data());
}

auto StateManifestCodec::Decode(const std::vector<std::byte> &bytes) -> StateManifest {
  const auto payload = DecodeVersionedFrame(
      {MANIFEST_MAGIC.data(), MANIFEST_MAGIC.size(), FORMAT_VERSION, MAX_MANIFEST_BYTES, "state manifest"}, bytes);
  ByteReader body(payload);
  StateManifest manifest{FORMAT_VERSION,    body.ReadU64(),    body.ReadU64(),    body.ReadU64(), body.ReadU64(),
                         body.ReadString(), body.ReadString(), body.ReadString(), body.ReadU32(), body.ReadU32(),
                         body.ReadU32(),    body.ReadU32(),    body.ReadU32()};
  if (!body.Empty()) {
    throw std::runtime_error("state manifest has trailing bytes");
  }
  // Reuse the encoder's semantic validation without accepting a re-encoded variant.
  static_cast<void>(Encode(manifest));
  return manifest;
}

auto StateManifestStore::ManifestFileName(uint64_t generation) -> std::string {
  std::ostringstream output;
  output << "MANIFEST-" << std::setw(20) << std::setfill('0') << generation;
  return output.str();
}

void StateManifestStore::Publish(const StateManifest &manifest) {
  if (storage_ == nullptr) {
    throw std::runtime_error("manifest store has no durable storage");
  }
  storage_->CreateDirectories(state_directory_);
  if (!Validate(manifest)) {
    throw std::runtime_error("refusing to publish an invalid snapshot manifest");
  }
  const auto name = ManifestFileName(manifest.generation_);
  const auto manifest_tmp = state_directory_ / (name + ".tmp");
  const auto manifest_path = state_directory_ / name;
  storage_->WriteFile(manifest_tmp, StateManifestCodec::Encode(manifest));
  storage_->SyncFile(manifest_tmp);
  storage_->Rename(manifest_tmp, manifest_path);
  storage_->SyncDirectory(state_directory_);

  const auto current_tmp = state_directory_ / "CURRENT.tmp";
  const auto current = state_directory_ / "CURRENT";
  storage_->WriteFile(current_tmp, BytesFromString(name + "\n"));
  storage_->SyncFile(current_tmp);
  storage_->Rename(current_tmp, current);
  storage_->SyncDirectory(state_directory_);
}

auto StateManifestStore::Validate(const StateManifest &manifest) const -> bool {
  try {
    static_cast<void>(StateManifestCodec::Encode(manifest));
    const auto database = state_directory_ / manifest.database_file_;
    const auto catalog = state_directory_ / manifest.catalog_file_;
    const auto session = state_directory_ / manifest.session_file_;
    if (!storage_->Exists(database) || !storage_->Exists(catalog) || !storage_->Exists(session) ||
        !IsResolvedWithin(state_directory_, database) || !IsResolvedWithin(state_directory_, catalog) ||
        !IsResolvedWithin(state_directory_, session) ||
        storage_->ChecksumFile(database) != manifest.database_checksum_ ||
        storage_->ChecksumFile(catalog) != manifest.catalog_checksum_ ||
        storage_->ChecksumFile(session) != manifest.session_checksum_) {
      return false;
    }
    const auto catalog_snapshot =
        CatalogSnapshotCodec::Decode(storage_->ReadFile(catalog, CatalogSnapshotCodec::MAX_CATALOG_BYTES));
    ValidateReplicatedCatalogV1(catalog_snapshot);
    if (catalog_snapshot.schema_epoch_ != manifest.schema_epoch_ ||
        catalog_snapshot.next_table_oid_ != manifest.next_table_oid_ ||
        catalog_snapshot.next_index_oid_ != manifest.next_index_oid_) {
      return false;
    }
    SessionTable sessions;
    SessionSnapshotCodec::DecodeInto(storage_->ReadFile(session, 64U * 1024U * 1024U), &sessions);
    sessions.ValidateSnapshotBoundary(manifest.last_included_index_, 0);
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

auto StateManifestStore::ReadManifest(const std::filesystem::path &path) const -> StateManifest {
  auto manifest = StateManifestCodec::Decode(storage_->ReadFile(path, StateManifestCodec::MAX_MANIFEST_BYTES));
  const auto file_generation = ParseManifestGeneration(path.filename().string());
  if (!file_generation.has_value() || *file_generation != manifest.generation_) {
    throw std::runtime_error("manifest generation does not match its file name");
  }
  return manifest;
}

auto StateManifestStore::SelectRecoveryPoint(const std::function<bool(uint64_t)> &has_bridge_log) const
    -> std::optional<SelectedRecoveryPoint> {
  if (storage_ == nullptr || !storage_->Exists(state_directory_)) {
    return std::nullopt;
  }
  std::vector<std::pair<uint64_t, std::filesystem::path>> manifests;
  for (const auto &item : std::filesystem::directory_iterator(state_directory_)) {
    if (item.is_regular_file()) {
      if (auto generation = ParseManifestGeneration(item.path().filename().string()); generation.has_value()) {
        manifests.emplace_back(*generation, item.path());
      }
    }
  }
  std::sort(manifests.begin(), manifests.end(), [](const auto &lhs, const auto &rhs) { return lhs.first > rhs.first; });

  std::optional<uint64_t> current_generation;
  const auto current_path = state_directory_ / "CURRENT";
  if (storage_->Exists(current_path)) {
    try {
      auto current = StringFromBytes(storage_->ReadFile(current_path, 256));
      if (current.empty() || current.back() != '\n') {
        throw std::runtime_error("CURRENT is not newline terminated");
      }
      current.pop_back();
      current_generation = ParseManifestGeneration(current);
      if (!current_generation.has_value() || current != ManifestFileName(*current_generation)) {
        throw std::runtime_error("CURRENT contains an invalid manifest name");
      }
    } catch (const std::exception &) {
      current_generation = std::nullopt;
    }
  }

  if (current_generation.has_value()) {
    manifests.erase(std::remove_if(manifests.begin(), manifests.end(),
                                   [&](const auto &candidate) { return candidate.first > *current_generation; }),
                    manifests.end());
    std::stable_sort(manifests.begin(), manifests.end(), [&](const auto &lhs, const auto &rhs) {
      const auto lhs_current = lhs.first == *current_generation;
      const auto rhs_current = rhs.first == *current_generation;
      if (lhs_current != rhs_current) {
        return lhs_current;
      }
      return lhs.first > rhs.first;
    });
  }

  for (const auto &[generation, path] : manifests) {
    try {
      auto manifest = ReadManifest(path);
      if (Validate(manifest) && (!has_bridge_log || has_bridge_log(manifest.last_included_index_))) {
        return SelectedRecoveryPoint{std::move(manifest), path};
      }
    } catch (const std::exception &) {
      continue;
    }
  }
  return std::nullopt;
}

}  // namespace bustub
