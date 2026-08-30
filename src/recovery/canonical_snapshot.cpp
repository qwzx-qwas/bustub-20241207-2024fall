//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// canonical_snapshot.cpp
//
//===----------------------------------------------------------------------===//

#include "recovery/canonical_snapshot.h"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/byte_codec.h"
#include "common/config.h"
#include "storage/disk/disk_manager.h"
#include "storage/table/table_heap.h"

namespace bustub {

namespace {

auto BuildCanonicalSnapshot(const Catalog &source_catalog, const SessionTable &sessions,
                            const CanonicalSnapshotPaths &paths, DurableStorage *storage, size_t buffer_pool_size,
                            bool synchronize) -> CanonicalSnapshotResult {
  if (storage == nullptr || paths.database_file_.empty() || paths.catalog_file_.empty() ||
      paths.session_file_.empty()) {
    throw std::runtime_error("invalid canonical snapshot target");
  }
  if (paths.database_file_.parent_path() != paths.catalog_file_.parent_path() ||
      paths.database_file_.parent_path() != paths.session_file_.parent_path()) {
    throw std::runtime_error("canonical snapshot files must share one snapshot directory");
  }
  if (storage->Exists(paths.database_file_) || storage->Exists(paths.catalog_file_) ||
      storage->Exists(paths.session_file_)) {
    throw std::runtime_error("canonical snapshot target must be new");
  }
  storage->CreateDirectories(paths.database_file_.parent_path());

  CatalogSnapshot catalog_snapshot;
  catalog_snapshot.schema_epoch_ = source_catalog.GetSchemaEpoch();
  catalog_snapshot.next_table_oid_ = source_catalog.GetNextTableOid();
  catalog_snapshot.next_index_oid_ = source_catalog.GetNextIndexOid();
  uint64_t row_count = 0;

  auto table_names = source_catalog.GetTableNames();
  std::sort(table_names.begin(), table_names.end(), [&](const auto &lhs, const auto &rhs) {
    return source_catalog.GetTable(lhs)->oid_ < source_catalog.GetTable(rhs)->oid_;
  });

  std::filesystem::path generated_log;
  {
    DiskManager disk(paths.database_file_);
    generated_log = disk.GetLogFileName();
    BufferPoolManager bpm(buffer_pool_size, &disk);
    for (const auto &table_name : table_names) {
      const auto source_table = source_catalog.GetTable(table_name);
      if (source_table == nullptr || source_table->table_ == nullptr) {
        throw std::runtime_error("cannot materialize a catalog table without a physical heap");
      }
      auto target_table = std::make_unique<TableHeap>(&bpm);
      const auto first_page_id = target_table->GetFirstPageId();
      for (auto iterator = source_table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
        auto [meta, tuple] = iterator.GetTuple();
        if (meta.ts_ < 0 || meta.ts_ >= TXN_START_ID) {
          throw std::runtime_error("canonical snapshot encountered a speculative or invalid row version");
        }
        // A speculative DELETE is still speculative state. Validate every physical tuple before filtering committed
        // tombstones; otherwise an in-flight delete would silently disappear from the canonical image.
        if (meta.is_deleted_) {
          continue;
        }
        if (!target_table->InsertTuple(meta, tuple).has_value()) {
          throw std::runtime_error("canonical snapshot could not materialize a committed tuple");
        }
        row_count++;
      }
      catalog_snapshot.tables_.push_back({source_table->oid_, source_table->name_, source_table->schema_, first_page_id,
                                          source_table->replicated_primary_key_});
      for (const auto &index : source_catalog.GetTableIndexes(table_name)) {
        if (index->constraint_kind_ == IndexConstraintKind::SECONDARY_UNIQUE) {
          throw std::runtime_error("V1 canonical snapshots reject secondary UNIQUE indexes");
        }
        catalog_snapshot.indexes_.push_back({index->index_oid_, source_table->oid_, index->name_, index->key_attrs_,
                                             index->index_type_, index->constraint_kind_});
      }
    }
    bpm.FlushAllPages();
    disk.ShutDown();
  }
  if (synchronize) {
    storage->SyncFile(paths.database_file_);
  }
  // DiskManager's legacy page-log file is not part of the canonical state.
  storage->RemoveFile(generated_log);

  std::sort(catalog_snapshot.tables_.begin(), catalog_snapshot.tables_.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.table_oid_ < rhs.table_oid_; });
  std::sort(catalog_snapshot.indexes_.begin(), catalog_snapshot.indexes_.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.index_oid_ < rhs.index_oid_; });
  const auto catalog_bytes = CatalogSnapshotCodec::Encode(catalog_snapshot);
  const auto session_bytes = SessionSnapshotCodec::Encode(sessions);
  storage->WriteFile(paths.catalog_file_, catalog_bytes);
  storage->WriteFile(paths.session_file_, session_bytes);
  if (synchronize) {
    storage->SyncFile(paths.catalog_file_);
    storage->SyncFile(paths.session_file_);
    storage->SyncDirectory(paths.database_file_.parent_path());
  }

  return {std::move(catalog_snapshot), row_count, Crc32c(catalog_bytes), Crc32c(session_bytes)};
}

}  // namespace

auto CanonicalSnapshotBuilder::Build(const Catalog &source_catalog, const SessionTable &sessions,
                                     const CanonicalSnapshotPaths &paths, DurableStorage *storage,
                                     size_t buffer_pool_size) -> CanonicalSnapshotResult {
  return BuildCanonicalSnapshot(source_catalog, sessions, paths, storage, buffer_pool_size, true);
}

auto CanonicalSnapshotBuilder::BuildUnsynced(const Catalog &source_catalog, const SessionTable &sessions,
                                             const CanonicalSnapshotPaths &paths, DurableStorage *storage,
                                             size_t buffer_pool_size) -> CanonicalSnapshotResult {
  return BuildCanonicalSnapshot(source_catalog, sessions, paths, storage, buffer_pool_size, false);
}

}  // namespace bustub
