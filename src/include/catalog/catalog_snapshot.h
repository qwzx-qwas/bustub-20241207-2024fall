//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_snapshot.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog.h"

namespace bustub {

struct CatalogSnapshotTable {
  table_oid_t table_oid_;
  std::string table_name_;
  Schema schema_;
  page_id_t first_page_id_;
  std::optional<ReplicatedPrimaryKeyDefinition> replicated_primary_key_;
};

struct CatalogSnapshotIndex {
  index_oid_t index_oid_;
  table_oid_t table_oid_;
  std::string index_name_;
  std::vector<uint32_t> key_attrs_;
  IndexType index_type_;
  IndexConstraintKind constraint_kind_;
};

struct CatalogSnapshot {
  uint32_t format_version_{1};
  uint64_t schema_epoch_{0};
  table_oid_t next_table_oid_{0};
  index_oid_t next_index_oid_{0};
  std::vector<CatalogSnapshotTable> tables_;
  std::vector<CatalogSnapshotIndex> indexes_;
};

/** Versioned, checksummed catalog definition codec. It never stores index root page IDs. */
class CatalogSnapshotCodec {
 public:
  static constexpr uint32_t FORMAT_VERSION = 1;
  static constexpr size_t MAX_CATALOG_BYTES = 64U * 1024U * 1024U;

  static auto Capture(const Catalog &catalog) -> CatalogSnapshot;
  static auto Encode(const CatalogSnapshot &snapshot) -> std::vector<std::byte>;
  static auto Decode(const std::vector<std::byte> &bytes) -> CatalogSnapshot;

  /** Reopen table heaps, then rebuild every derived index from table rows. */
  static void Restore(const CatalogSnapshot &snapshot, Catalog *catalog, BufferPoolManager *bpm, Transaction *txn);
};

/** Fail-closed admission check applied before a Catalog is exposed as a replicated V1 state machine. */
void ValidateReplicatedCatalogV1(const CatalogSnapshot &snapshot);

/** Shared V1 physical-index admission check used before proposal, committed Apply, and snapshot restore. */
void ValidateReplicatedIndexV1(const CatalogSnapshotIndex &index, const Schema &table_schema);

/** Shared deterministic derived-index constructor used by recovery and committed FSM Apply. */
auto CreateDerivedIndexFromDefinition(const CatalogSnapshotIndex &record, Catalog *catalog, Transaction *txn)
    -> std::shared_ptr<IndexInfo>;

}  // namespace bustub
