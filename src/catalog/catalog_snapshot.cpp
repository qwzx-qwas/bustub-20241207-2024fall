//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_snapshot.cpp
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog_snapshot.h"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "common/byte_codec.h"
#include "storage/index/generic_key.h"

namespace bustub {
namespace {

constexpr std::array<std::byte, 8> CATALOG_MAGIC{std::byte{'B'}, std::byte{'S'}, std::byte{'T'}, std::byte{'C'},
                                                 std::byte{'A'}, std::byte{'T'}, std::byte{'0'}, std::byte{'1'}};

auto IsKnownType(TypeId type) -> bool {
  switch (type) {
    case TypeId::BOOLEAN:
    case TypeId::TINYINT:
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
    case TypeId::BIGINT:
    case TypeId::DECIMAL:
    case TypeId::VARCHAR:
    case TypeId::TIMESTAMP:
    case TypeId::VECTOR:
      return true;
    default:
      return false;
  }
}

auto IsKnownIndexType(IndexType type) -> bool {
  switch (type) {
    case IndexType::BPlusTreeIndex:
    case IndexType::HashTableIndex:
    case IndexType::STLOrderedIndex:
    case IndexType::STLUnorderedIndex:
    case IndexType::IVFFlatIndex:
    case IndexType::HNSWIndex:
      return true;
    default:
      return false;
  }
}

void ValidateCatalogSnapshotForCodec(const CatalogSnapshot &snapshot) {
  if (snapshot.format_version_ != CatalogSnapshotCodec::FORMAT_VERSION ||
      snapshot.tables_.size() > std::numeric_limits<uint32_t>::max() ||
      snapshot.indexes_.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("unsupported or oversized catalog snapshot");
  }

  std::unordered_map<table_oid_t, const CatalogSnapshotTable *> tables;
  std::unordered_set<std::string> table_names;
  for (const auto &table : snapshot.tables_) {
    if (table.table_name_.empty() || table.first_page_id_ < 0 || table.table_oid_ >= snapshot.next_table_oid_ ||
        table.schema_.GetColumnCount() == 0 || !tables.emplace(table.table_oid_, &table).second ||
        !table_names.emplace(table.table_name_).second) {
      throw std::runtime_error("invalid catalog table definition or next table OID");
    }
    std::unordered_set<std::string> column_names;
    for (const auto &column : table.schema_.GetColumns()) {
      if (column.GetName().empty() || !column_names.emplace(column.GetName()).second ||
          !IsKnownType(column.GetType()) || column.GetStorageSize() == 0) {
        throw std::runtime_error("invalid catalog column definition");
      }
    }
    if (table.replicated_primary_key_.has_value()) {
      const auto &key = *table.replicated_primary_key_;
      if (key.column_oid_ >= table.schema_.GetColumnCount() || key.codec_version_ != 1 ||
          table.schema_.GetColumn(key.column_oid_).GetType() != key.type_ ||
          (key.type_ != TypeId::INTEGER && key.type_ != TypeId::BIGINT && key.type_ != TypeId::VARCHAR)) {
        throw std::runtime_error("unsupported replicated primary-key definition");
      }
    }
  }

  std::unordered_set<index_oid_t> index_oids;
  std::unordered_map<table_oid_t, std::unordered_set<std::string>> index_names;
  std::unordered_map<table_oid_t, uint32_t> primary_counts;
  for (const auto &index : snapshot.indexes_) {
    const auto table = tables.find(index.table_oid_);
    if (index.index_name_.empty() || index.index_oid_ >= snapshot.next_index_oid_ ||
        !index_oids.emplace(index.index_oid_).second || table == tables.end() ||
        !index_names[index.table_oid_].emplace(index.index_name_).second) {
      throw std::runtime_error("invalid catalog index definition or next index OID");
    }
    ValidateReplicatedIndexV1(index, table->second->schema_);
    if (index.constraint_kind_ == IndexConstraintKind::PRIMARY_KEY) {
      const auto &key = table->second->replicated_primary_key_;
      if (!key.has_value() || index.key_attrs_ != std::vector<uint32_t>{key->column_oid_} ||
          ++primary_counts[index.table_oid_] > 1) {
        throw std::runtime_error("catalog primary index does not match its logical key");
      }
    }
  }
}

void EncodeSchema(ByteWriter *writer, const Schema &schema) {
  writer->PutU32(schema.GetColumnCount());
  for (const auto &column : schema.GetColumns()) {
    writer->PutString(column.GetName());
    writer->PutU32(static_cast<uint32_t>(column.GetType()));
    writer->PutU32(column.GetStorageSize());
  }
}

auto DecodeSchema(ByteReader *reader) -> Schema {
  const auto column_count = reader->ReadU32();
  if (column_count == 0 || column_count > 65536) {
    throw std::runtime_error("invalid catalog column count");
  }
  std::vector<Column> columns;
  columns.reserve(column_count);
  for (uint32_t i = 0; i < column_count; i++) {
    auto name = reader->ReadString();
    auto type = static_cast<TypeId>(reader->ReadU32());
    auto length = reader->ReadU32();
    if (name.empty() || !IsKnownType(type)) {
      throw std::runtime_error("invalid catalog column definition");
    }
    if (type == TypeId::VARCHAR || type == TypeId::VECTOR) {
      if (length == 0 || length > std::numeric_limits<uint8_t>::max()) {
        throw std::runtime_error("invalid variable-length catalog column");
      }
      const auto logical_length = type == TypeId::VECTOR ? length / sizeof(double) : length;
      if (logical_length == 0 || (type == TypeId::VECTOR && length % sizeof(double) != 0)) {
        throw std::runtime_error("invalid vector catalog column length");
      }
      columns.emplace_back(std::move(name), type, logical_length);
    } else {
      Column column(std::move(name), type);
      if (column.GetStorageSize() != length) {
        throw std::runtime_error("catalog column length does not match its type");
      }
      columns.emplace_back(std::move(column));
    }
  }
  return Schema(columns);
}

auto MaxKeyBytes(const Schema &key_schema) -> size_t {
  size_t result = key_schema.GetInlinedStorageSize();
  for (const auto &column : key_schema.GetColumns()) {
    if (!column.IsInlined()) {
      result += column.GetStorageSize();
    }
  }
  return result;
}

template <size_t KeySize>
auto RestoreScalarIndex(const CatalogSnapshotIndex &record, Catalog *catalog, Transaction *txn,
                        const std::shared_ptr<TableInfo> &table, const Schema &key_schema)
    -> std::shared_ptr<IndexInfo> {
  using Key = GenericKey<KeySize>;
  using Comparator = GenericComparator<KeySize>;
  return catalog->CreateIndex<Key, RID, Comparator>(txn, record.index_name_, table->name_, table->schema_, key_schema,
                                                    record.key_attrs_, KeySize, HashFunction<Key>{},
                                                    record.constraint_kind_ == IndexConstraintKind::PRIMARY_KEY,
                                                    record.index_type_, record.index_oid_, record.constraint_kind_);
}

auto RestoreIndex(const CatalogSnapshotIndex &record, Catalog *catalog, Transaction *txn,
                  const std::shared_ptr<TableInfo> &table) -> std::shared_ptr<IndexInfo> {
  for (auto attr : record.key_attrs_) {
    if (attr >= table->schema_.GetColumnCount()) {
      throw std::runtime_error("catalog index references an invalid column");
    }
  }
  auto key_schema = Schema::CopySchema(&table->schema_, record.key_attrs_);
  if (record.index_type_ == IndexType::IVFFlatIndex) {
    return catalog->CreateIVFFlatIndex(txn, record.index_name_, table->name_, table->schema_, key_schema,
                                       record.key_attrs_, false, {}, record.index_oid_);
  }
  if (record.index_type_ == IndexType::HNSWIndex) {
    return catalog->CreateHNSWIndex(txn, record.index_name_, table->name_, table->schema_, key_schema,
                                    record.key_attrs_, false, {}, record.index_oid_);
  }

  const auto key_bytes = MaxKeyBytes(key_schema);
  if (key_bytes <= 8) {
    return RestoreScalarIndex<8>(record, catalog, txn, table, key_schema);
  }
  if (key_bytes <= 16) {
    return RestoreScalarIndex<16>(record, catalog, txn, table, key_schema);
  }
  if (key_bytes <= 32) {
    return RestoreScalarIndex<32>(record, catalog, txn, table, key_schema);
  }
  if (key_bytes <= 64) {
    return RestoreScalarIndex<64>(record, catalog, txn, table, key_schema);
  }
  throw std::runtime_error("catalog index key exceeds the V1 maximum size");
}

}  // namespace

auto CreateDerivedIndexFromDefinition(const CatalogSnapshotIndex &record, Catalog *catalog, Transaction *txn)
    -> std::shared_ptr<IndexInfo> {
  if (catalog == nullptr) {
    throw std::runtime_error("invalid catalog index creation target");
  }
  const auto table = catalog->GetTable(record.table_oid_);
  if (table == nullptr) {
    throw std::runtime_error("catalog index references a missing table");
  }
  ValidateReplicatedIndexV1(record, table->schema_);
  return RestoreIndex(record, catalog, txn, table);
}

void ValidateReplicatedIndexV1(const CatalogSnapshotIndex &index, const Schema &table_schema) {
  if (index.key_attrs_.empty() || !IsKnownIndexType(index.index_type_) ||
      (index.constraint_kind_ != IndexConstraintKind::PRIMARY_KEY &&
       index.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY)) {
    throw std::runtime_error("invalid replicated V1 index definition");
  }
  std::unordered_set<uint32_t> attributes;
  bool contains_vector = false;
  for (const auto attribute : index.key_attrs_) {
    if (attribute >= table_schema.GetColumnCount() || !attributes.insert(attribute).second) {
      throw std::runtime_error("replicated V1 index has an invalid or duplicate column");
    }
    contains_vector = contains_vector || table_schema.GetColumn(attribute).GetType() == TypeId::VECTOR;
  }
  const bool is_vector_index =
      index.index_type_ == IndexType::IVFFlatIndex || index.index_type_ == IndexType::HNSWIndex;
  if (is_vector_index) {
    if (index.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY || index.key_attrs_.size() != 1 ||
        !contains_vector) {
      throw std::runtime_error("replicated V1 vector index requires one VECTOR secondary key");
    }
    return;
  }
  if (contains_vector || MaxKeyBytes(Schema::CopySchema(&table_schema, index.key_attrs_)) > 64) {
    throw std::runtime_error("replicated V1 scalar index key exceeds the 64-byte adapter limit");
  }
  if (index.constraint_kind_ == IndexConstraintKind::PRIMARY_KEY && index.index_type_ != IndexType::BPlusTreeIndex) {
    throw std::runtime_error("replicated V1 primary index must use B+Tree semantics");
  }
}

void ValidateReplicatedCatalogV1(const CatalogSnapshot &snapshot) {
  ValidateCatalogSnapshotForCodec(snapshot);
  std::unordered_map<table_oid_t, const CatalogSnapshotTable *> tables;
  std::unordered_map<table_oid_t, uint32_t> primary_index_counts;
  for (const auto &table : snapshot.tables_) {
    if (!tables.emplace(table.table_oid_, &table).second || !table.replicated_primary_key_.has_value()) {
      throw std::runtime_error("replicated V1 catalog table has no unambiguous primary key");
    }
    const auto &primary_key = *table.replicated_primary_key_;
    if (primary_key.column_oid_ >= table.schema_.GetColumnCount() || primary_key.codec_version_ != 1 ||
        table.schema_.GetColumn(primary_key.column_oid_).GetType() != primary_key.type_ ||
        (primary_key.type_ != TypeId::INTEGER && primary_key.type_ != TypeId::BIGINT &&
         primary_key.type_ != TypeId::VARCHAR)) {
      throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
    }
  }
  for (const auto &index : snapshot.indexes_) {
    const auto table = tables.find(index.table_oid_);
    if (table == tables.end()) {
      throw std::runtime_error("replicated V1 catalog index references a missing table");
    }
    if (index.constraint_kind_ == IndexConstraintKind::SECONDARY_UNIQUE) {
      throw std::runtime_error("UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT");
    }
    ValidateReplicatedIndexV1(index, table->second->schema_);
    if (index.constraint_kind_ == IndexConstraintKind::PRIMARY_KEY) {
      const auto column = table->second->replicated_primary_key_->column_oid_;
      if (index.key_attrs_ != std::vector<uint32_t>{column}) {
        throw std::runtime_error("replicated V1 primary index does not match the logical key");
      }
      primary_index_counts[index.table_oid_]++;
    } else if (index.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY) {
      throw std::runtime_error("replicated V1 catalog has an unknown index constraint");
    }
  }
  for (const auto &[table_oid, unused] : tables) {
    if (primary_index_counts[table_oid] != 1) {
      throw std::runtime_error("replicated V1 table must have exactly one primary index");
    }
  }
}

auto CatalogSnapshotCodec::Capture(const Catalog &catalog) -> CatalogSnapshot {
  CatalogSnapshot snapshot;
  snapshot.schema_epoch_ = catalog.GetSchemaEpoch();
  snapshot.next_table_oid_ = catalog.GetNextTableOid();
  snapshot.next_index_oid_ = catalog.GetNextIndexOid();

  auto table_names = catalog.GetTableNames();
  for (const auto &table_name : table_names) {
    auto table = catalog.GetTable(table_name);
    if (table == nullptr || table->table_ == nullptr) {
      throw std::runtime_error("cannot snapshot a catalog table without a physical heap");
    }
    snapshot.tables_.push_back(
        {table->oid_, table->name_, table->schema_, table->table_->GetFirstPageId(), table->replicated_primary_key_});
    for (const auto &index : catalog.GetTableIndexes(table_name)) {
      snapshot.indexes_.push_back({index->index_oid_, table->oid_, index->name_, index->key_attrs_, index->index_type_,
                                   index->constraint_kind_});
    }
  }
  std::sort(snapshot.tables_.begin(), snapshot.tables_.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.table_oid_ < rhs.table_oid_; });
  std::sort(snapshot.indexes_.begin(), snapshot.indexes_.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.index_oid_ < rhs.index_oid_; });
  return snapshot;
}

auto CatalogSnapshotCodec::Encode(const CatalogSnapshot &snapshot) -> std::vector<std::byte> {
  ValidateCatalogSnapshotForCodec(snapshot);
  ByteWriter payload;
  payload.PutU64(snapshot.schema_epoch_);
  payload.PutU32(snapshot.next_table_oid_);
  payload.PutU32(snapshot.next_index_oid_);
  payload.PutU32(static_cast<uint32_t>(snapshot.tables_.size()));
  payload.PutU32(static_cast<uint32_t>(snapshot.indexes_.size()));
  for (const auto &table : snapshot.tables_) {
    payload.PutU32(table.table_oid_);
    payload.PutString(table.table_name_);
    payload.PutU32(static_cast<uint32_t>(table.first_page_id_));
    payload.PutU8(table.replicated_primary_key_.has_value() ? 1 : 0);
    if (table.replicated_primary_key_.has_value()) {
      payload.PutU32(table.replicated_primary_key_->column_oid_);
      payload.PutU32(static_cast<uint32_t>(table.replicated_primary_key_->type_));
      payload.PutU32(table.replicated_primary_key_->codec_version_);
    }
    EncodeSchema(&payload, table.schema_);
  }
  for (const auto &index : snapshot.indexes_) {
    payload.PutU32(index.index_oid_);
    payload.PutU32(index.table_oid_);
    payload.PutString(index.index_name_);
    payload.PutU32(static_cast<uint32_t>(index.index_type_));
    payload.PutU32(static_cast<uint32_t>(index.constraint_kind_));
    payload.PutU32(static_cast<uint32_t>(index.key_attrs_.size()));
    for (auto attr : index.key_attrs_) {
      payload.PutU32(attr);
    }
  }
  if (payload.Data().size() > MAX_CATALOG_BYTES) {
    throw std::runtime_error("catalog snapshot exceeds the V1 size limit");
  }

  return EncodeVersionedFrame(
      {CATALOG_MAGIC.data(), CATALOG_MAGIC.size(), FORMAT_VERSION, MAX_CATALOG_BYTES, "catalog snapshot"},
      payload.Data());
}

auto CatalogSnapshotCodec::Decode(const std::vector<std::byte> &bytes) -> CatalogSnapshot {
  const auto payload = DecodeVersionedFrame(
      {CATALOG_MAGIC.data(), CATALOG_MAGIC.size(), FORMAT_VERSION, MAX_CATALOG_BYTES, "catalog snapshot"}, bytes);

  ByteReader body(payload);
  CatalogSnapshot snapshot;
  snapshot.format_version_ = FORMAT_VERSION;
  snapshot.schema_epoch_ = body.ReadU64();
  snapshot.next_table_oid_ = body.ReadU32();
  snapshot.next_index_oid_ = body.ReadU32();
  const auto table_count = body.ReadU32();
  const auto index_count = body.ReadU32();
  if (table_count > 1000000 || index_count > 1000000) {
    throw std::runtime_error("catalog snapshot contains too many objects");
  }

  std::unordered_set<table_oid_t> table_oids;
  std::unordered_set<std::string> table_names;
  for (uint32_t i = 0; i < table_count; i++) {
    const auto table_oid = body.ReadU32();
    auto table_name = body.ReadString();
    const auto first_page_id = static_cast<page_id_t>(body.ReadU32());
    std::optional<ReplicatedPrimaryKeyDefinition> primary_key;
    const auto has_primary_key = body.ReadU8();
    if (has_primary_key > 1) {
      throw std::runtime_error("invalid replicated primary-key marker");
    }
    if (has_primary_key == 1) {
      primary_key = ReplicatedPrimaryKeyDefinition{body.ReadU32(), static_cast<TypeId>(body.ReadU32()), body.ReadU32()};
    }
    auto schema = DecodeSchema(&body);
    if (table_name.empty() || first_page_id < 0 || !table_oids.emplace(table_oid).second ||
        !table_names.emplace(table_name).second) {
      throw std::runtime_error("invalid or duplicate catalog table definition");
    }
    if (primary_key.has_value()) {
      if (primary_key->column_oid_ >= schema.GetColumnCount() || primary_key->codec_version_ != 1 ||
          schema.GetColumn(primary_key->column_oid_).GetType() != primary_key->type_ ||
          (primary_key->type_ != TypeId::INTEGER && primary_key->type_ != TypeId::BIGINT &&
           primary_key->type_ != TypeId::VARCHAR)) {
        throw std::runtime_error("unsupported replicated primary-key definition");
      }
    }
    snapshot.tables_.push_back({table_oid, std::move(table_name), std::move(schema), first_page_id, primary_key});
  }

  std::unordered_set<index_oid_t> index_oids;
  for (uint32_t i = 0; i < index_count; i++) {
    CatalogSnapshotIndex index{body.ReadU32(),
                               body.ReadU32(),
                               body.ReadString(),
                               {},
                               static_cast<IndexType>(body.ReadU32()),
                               static_cast<IndexConstraintKind>(body.ReadU32())};
    const auto attr_count = body.ReadU32();
    if (attr_count == 0 || attr_count > 65536 || index.index_name_.empty() || !IsKnownIndexType(index.index_type_) ||
        !index_oids.emplace(index.index_oid_).second || table_oids.count(index.table_oid_) == 0) {
      throw std::runtime_error("invalid catalog index definition");
    }
    if (index.constraint_kind_ == IndexConstraintKind::SECONDARY_UNIQUE ||
        (index.constraint_kind_ != IndexConstraintKind::PRIMARY_KEY &&
         index.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY)) {
      throw std::runtime_error("unsupported deferred unique index definition");
    }
    for (uint32_t attr = 0; attr < attr_count; attr++) {
      index.key_attrs_.push_back(body.ReadU32());
    }
    snapshot.indexes_.push_back(std::move(index));
  }
  if (!body.Empty()) {
    throw std::runtime_error("catalog snapshot has trailing bytes");
  }
  ValidateCatalogSnapshotForCodec(snapshot);
  return snapshot;
}

void CatalogSnapshotCodec::Restore(const CatalogSnapshot &snapshot, Catalog *catalog, BufferPoolManager *bpm,
                                   Transaction *txn) {
  if (catalog == nullptr || bpm == nullptr) {
    throw std::runtime_error("invalid catalog restore target");
  }
  ValidateCatalogSnapshotForCodec(snapshot);
  auto tables = snapshot.tables_;
  std::sort(tables.begin(), tables.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.table_oid_ < rhs.table_oid_; });
  page_id_t maximum_table_page_id = INVALID_PAGE_ID;
  std::vector<std::tuple<CatalogSnapshotTable, std::unique_ptr<TableHeap>>> opened_tables;
  opened_tables.reserve(tables.size());
  for (const auto &record : tables) {
    auto heap = TableHeap::Open(bpm, record.first_page_id_);
    maximum_table_page_id = std::max(maximum_table_page_id, heap->GetLastPageId());
    opened_tables.emplace_back(record, std::move(heap));
  }
  bpm->SetNextPageIdForRecovery(maximum_table_page_id == INVALID_PAGE_ID ? 0 : maximum_table_page_id + 1);
  for (auto &[record, heap] : opened_tables) {
    if (catalog->RestoreTable(record.table_name_, record.schema_, record.table_oid_, std::move(heap),
                              record.replicated_primary_key_) == nullptr) {
      throw std::runtime_error("failed to restore catalog table");
    }
  }

  auto indexes = snapshot.indexes_;
  std::sort(indexes.begin(), indexes.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.index_oid_ < rhs.index_oid_; });
  for (const auto &record : indexes) {
    if (CreateDerivedIndexFromDefinition(record, catalog, txn) == nullptr) {
      throw std::runtime_error("failed to rebuild catalog index");
    }
  }
  if (!catalog->RestoreOidAllocators(snapshot.next_table_oid_, snapshot.next_index_oid_)) {
    throw std::runtime_error("catalog snapshot OID allocator is inconsistent with its objects");
  }
  catalog->SetSchemaEpochForRecovery(snapshot.schema_epoch_);
}

}  // namespace bustub
