//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// state_manifest.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "recovery/durable_storage.h"

namespace bustub {

struct StateManifest {
  uint32_t format_version_{1};
  uint64_t generation_{0};
  uint64_t last_included_index_{0};
  uint64_t last_included_term_{0};
  uint64_t schema_epoch_{0};
  std::string database_file_;
  std::string catalog_file_;
  std::string session_file_;
  uint32_t database_checksum_{0};
  uint32_t catalog_checksum_{0};
  uint32_t session_checksum_{0};
  table_oid_t next_table_oid_{0};
  index_oid_t next_index_oid_{0};
};

class StateManifestCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t MAX_MANIFEST_BYTES = 1024U * 1024U;

  static auto Encode(const StateManifest &manifest) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> StateManifest;
  static auto IsSafeRelativePath(const std::string &path) -> bool;
};

struct SelectedRecoveryPoint {
  StateManifest manifest_;
  std::filesystem::path manifest_path_;
};

/** Atomic CURRENT/MANIFEST publication and validated fallback selection. */
class StateManifestStore {
 public:
  StateManifestStore(std::filesystem::path state_directory, std::shared_ptr<DurableStorage> storage)
      : state_directory_(std::move(state_directory)), storage_(std::move(storage)) {}

  static auto ManifestFileName(uint64_t generation) -> std::string;
  void Publish(const StateManifest &manifest);
  auto Validate(const StateManifest &manifest) const -> bool;
  auto SelectRecoveryPoint(const std::function<bool(uint64_t)> &has_bridge_log = {}) const
      -> std::optional<SelectedRecoveryPoint>;

 private:
  auto ReadManifest(const std::filesystem::path &path) const -> StateManifest;

  std::filesystem::path state_directory_;
  std::shared_ptr<DurableStorage> storage_;
};

}  // namespace bustub
