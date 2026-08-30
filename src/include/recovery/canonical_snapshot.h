//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// canonical_snapshot.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <filesystem>

#include "catalog/catalog_snapshot.h"
#include "distributed/session_table.h"
#include "recovery/durable_storage.h"

namespace bustub {

struct CanonicalSnapshotPaths {
  std::filesystem::path database_file_;
  std::filesystem::path catalog_file_;
  std::filesystem::path session_file_;
};

struct CanonicalSnapshotResult {
  CatalogSnapshot catalog_;
  uint64_t row_count_{0};
  uint32_t catalog_checksum_{0};
  uint32_t session_checksum_{0};
};

/**
 * Materializes only latest committed logical rows into a fresh database file.
 * No physical index page from the working database can enter this output.
 * Callers must already hold the snapshot visibility barrier exclusively.
 */
class CanonicalSnapshotBuilder {
 public:
  static auto Build(const Catalog &source_catalog, const SessionTable &sessions, const CanonicalSnapshotPaths &paths,
                    DurableStorage *storage, size_t buffer_pool_size = 128) -> CanonicalSnapshotResult;

  /** Capture complete immutable temp files without issuing their durability syncs. SnapshotManager owns that phase. */
  static auto BuildUnsynced(const Catalog &source_catalog, const SessionTable &sessions,
                            const CanonicalSnapshotPaths &paths, DurableStorage *storage, size_t buffer_pool_size = 128)
      -> CanonicalSnapshotResult;
};

}  // namespace bustub
