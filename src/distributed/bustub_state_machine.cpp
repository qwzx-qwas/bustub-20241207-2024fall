//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// bustub_state_machine.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/bustub_state_machine.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>
#include <type_traits>

#include "catalog/catalog_snapshot.h"
#include "common/config.h"

namespace bustub {

BusTubStateMachine::BusTubStateMachine(Catalog *catalog, SessionTable *sessions, StateVisibilityLatch *visibility,
                                       uint64_t snapshot_index)
    : catalog_(catalog),
      sessions_(sessions),
      visibility_(visibility),
      last_applied_(snapshot_index),
      published_applied_index_(snapshot_index) {
  if (catalog_ == nullptr || sessions_ == nullptr || visibility_ == nullptr || snapshot_index >= TXN_START_ID) {
    throw std::runtime_error("invalid BusTub state machine configuration");
  }
}

void BusTubStateMachine::ValidateProposal(const TransactionCommandBatch &batch) const {
  auto shared = visibility_->LockShared();
  if (stopped_) {
    throw std::runtime_error("BusTub state machine is fail-stopped");
  }
  if (sessions_->Classify(batch.client_id_, batch.request_id_) != RequestDisposition::NEW_REQUEST) {
    throw std::runtime_error("proposal session request is not the next new request");
  }
  if (catalog_->GetSchemaEpoch() != batch.expected_start_schema_epoch_) {
    throw std::runtime_error("proposal schema epoch mismatch");
  }
  for (const auto &command : batch.commands_) {
    std::visit(
        [&](const auto &value) {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, CreateTableCommand>) {
            if (value.table_oid_ != catalog_->GetNextTableOid() ||
                value.primary_index_oid_ != catalog_->GetNextIndexOid() ||
                catalog_->GetTable(value.table_name_) != nullptr) {
              throw std::runtime_error("proposal CREATE TABLE name or OID is unavailable");
            }
            const auto schema = DecodeSchema(value.columns_);
            if (value.primary_key_.column_oid_ >= schema.GetColumnCount() ||
                value.primary_key_.codec_version_ != PrimaryKeyCodecV1::FORMAT_VERSION ||
                schema.GetColumn(value.primary_key_.column_oid_).GetType() != value.primary_key_.type_ ||
                value.columns_[value.primary_key_.column_oid_].nullable_ ||
                !PrimaryKeyCodecV1::IsSupported(value.primary_key_.type_)) {
              throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
            }
            ValidateReplicatedIndexV1({value.primary_index_oid_,
                                       value.table_oid_,
                                       "__candidate_primary",
                                       {value.primary_key_.column_oid_},
                                       IndexType::BPlusTreeIndex,
                                       IndexConstraintKind::PRIMARY_KEY},
                                      schema);
          } else if constexpr (std::is_same_v<T, CreateIndexCommand>) {  // NOLINT(readability/braces)
            const auto table = catalog_->GetTable(value.table_oid_);
            if (value.index_oid_ != catalog_->GetNextIndexOid() || table == nullptr ||
                catalog_->GetIndex(value.index_name_, value.table_oid_) != nullptr || value.key_columns_.empty() ||
                value.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY) {
              throw std::runtime_error("proposal CREATE INDEX is invalid or requires deferred uniqueness");
            }
            for (const auto column : value.key_columns_) {
              if (column >= table->schema_.GetColumnCount()) {
                throw std::runtime_error("proposal CREATE INDEX references an invalid column");
              }
            }
            ValidateReplicatedIndexV1({value.index_oid_, value.table_oid_, value.index_name_, value.key_columns_,
                                       value.index_type_, value.constraint_kind_},
                                      table->schema_);
          } else {
            const auto row = LocateRow(value.table_oid_, value.primary_key_);
            if constexpr (std::is_same_v<T, InsertRowCommand>) {
              if (row.has_value()) {
                throw std::runtime_error("proposal INSERT primary key already exists");
              }
              const auto table = catalog_->GetTable(value.table_oid_);
              if (table == nullptr) {
                throw std::runtime_error("proposal INSERT references a missing table");
              }
              const auto tuple = TupleCodecV1::Decode(value.complete_tuple_, table->schema_);
              ValidateTupleKey(table, value.primary_key_, tuple);
            } else {
              if (!row.has_value() || row->meta_.ts_ < 0 ||
                  static_cast<uint64_t>(row->meta_.ts_) != value.expected_old_commit_ts_ ||
                  TupleCodecV1::Encode(row->tuple_, row->table_->schema_) != value.expected_old_tuple_) {
                throw std::runtime_error("proposal mutation old-row precondition mismatch");
              }
              if constexpr (std::is_same_v<T, UpdateRowCommand>) {
                const auto tuple = TupleCodecV1::Decode(value.complete_new_tuple_, row->table_->schema_);
                ValidateTupleKey(row->table_, value.primary_key_, tuple);
              }
            }
          }
        },
        command);
  }
}

void BusTubStateMachine::Apply(const ReplicatedLogEntry &entry) {
  auto exclusive = visibility_->LockExclusive();
  if (stopped_) {
    throw std::runtime_error("BusTub state machine is fail-stopped");
  }
  try {
    if (entry.index_ != last_applied_ + 1 || entry.index_ >= TXN_START_ID) {
      throw std::runtime_error("BusTub state machine Apply is not continuous");
    }
    if (entry.type_ == EntryType::COMMAND_BATCH) {
      ApplyBatch(entry, CommandBatchCodec::Decode(entry.payload_));
    } else if (entry.type_ != EntryType::NOOP) {
      throw std::runtime_error("unsupported committed entry type for BusTub state machine");
    }
    // Publication of every entry, including NOOP, occurs under this same lock.
    published_applied_index_ = entry.index_;
    last_applied_ = entry.index_;
  } catch (...) {
    stopped_ = true;
    throw;
  }
}

void BusTubStateMachine::ApplyBatch(const ReplicatedLogEntry &entry, const TransactionCommandBatch &batch) {
  const auto disposition = sessions_->Classify(batch.client_id_, batch.request_id_);
  if (disposition == RequestDisposition::RETRY_LAST) {
    return;
  }
  if (disposition != RequestDisposition::NEW_REQUEST) {
    throw std::runtime_error("committed SessionTable request is old or contains a sequence gap");
  }
  if (catalog_->GetSchemaEpoch() != batch.expected_start_schema_epoch_) {
    throw std::runtime_error("committed batch schema epoch mismatch");
  }
  for (const auto &command : batch.commands_) {
    ApplyCommand(entry, command);
  }
  const auto response =
      WriteResponseCodec::Encode({1, WriteStatus::COMMITTED, batch.request_id_, entry.term_, entry.index_});
  sessions_->RecordCommitted(batch.client_id_, batch.request_id_, response);
}

void BusTubStateMachine::ApplyCommand(const ReplicatedLogEntry &entry, const ReplicatedCommand &command) {
  std::visit(
      [&](const auto &value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, CreateTableCommand>) {
          ApplyCreateTable(value);
        } else if constexpr (std::is_same_v<T, CreateIndexCommand>) {
          ApplyCreateIndex(value);
        } else if constexpr (std::is_same_v<T, InsertRowCommand>) {
          ApplyInsert(entry, value);
        } else if constexpr (std::is_same_v<T, UpdateRowCommand>) {
          ApplyUpdate(entry, value);
        } else {
          ApplyDelete(entry, value);
        }
      },
      command);
}

auto BusTubStateMachine::DecodeSchema(const std::vector<ReplicatedColumnDefinition> &columns) -> Schema {
  if (columns.empty()) {
    throw std::runtime_error("replicated table must contain a column");
  }
  std::set<std::string> names;
  std::vector<Column> decoded;
  decoded.reserve(columns.size());
  for (const auto &column : columns) {
    if (column.name_.empty() || !names.insert(column.name_).second) {
      throw std::runtime_error("invalid or duplicate replicated column name");
    }
    if (column.type_ == TypeId::VARCHAR || column.type_ == TypeId::VECTOR) {
      if (column.storage_size_ == 0 || column.storage_size_ > std::numeric_limits<uint8_t>::max() ||
          (column.type_ == TypeId::VECTOR && column.storage_size_ % sizeof(double) != 0)) {
        throw std::runtime_error("invalid replicated variable-length column");
      }
      decoded.emplace_back(
          column.name_, column.type_,
          column.type_ == TypeId::VECTOR ? column.storage_size_ / sizeof(double) : column.storage_size_);
    } else {
      Column value(column.name_, column.type_);
      if (value.GetStorageSize() != column.storage_size_) {
        throw std::runtime_error("replicated fixed column storage size mismatch");
      }
      decoded.push_back(std::move(value));
    }
  }
  return Schema(decoded);
}

void BusTubStateMachine::ApplyCreateTable(const CreateTableCommand &command) {
  if (command.table_oid_ != catalog_->GetNextTableOid() || command.primary_index_oid_ != catalog_->GetNextIndexOid()) {
    throw std::runtime_error("replicated CREATE TABLE OID allocator drift");
  }
  const auto schema = DecodeSchema(command.columns_);
  if (command.primary_key_.column_oid_ >= schema.GetColumnCount() || command.primary_key_.codec_version_ != 1 ||
      schema.GetColumn(command.primary_key_.column_oid_).GetType() != command.primary_key_.type_ ||
      command.columns_[command.primary_key_.column_oid_].nullable_ ||
      !PrimaryKeyCodecV1::IsSupported(command.primary_key_.type_)) {
    throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  const auto table =
      catalog_->CreateTableWithOid(nullptr, command.table_name_, schema, command.table_oid_, command.primary_key_);
  if (table == nullptr) {
    throw std::runtime_error("committed CREATE TABLE failed");
  }
  CatalogSnapshotIndex primary{
      command.primary_index_oid_,         command.table_oid_,        "__raft_pk_" + std::to_string(command.table_oid_),
      {command.primary_key_.column_oid_}, IndexType::BPlusTreeIndex, IndexConstraintKind::PRIMARY_KEY};
  if (CreateDerivedIndexFromDefinition(primary, catalog_, nullptr) == nullptr ||
      !catalog_->CommitExplicitIndexOid(command.primary_index_oid_)) {
    throw std::runtime_error("committed primary-index creation failed");
  }
  catalog_->AdvanceSchemaEpoch();
}

void BusTubStateMachine::ApplyCreateIndex(const CreateIndexCommand &command) {
  if (command.index_oid_ != catalog_->GetNextIndexOid() ||
      command.constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY) {
    throw std::runtime_error("replicated CREATE INDEX OID drift or unsupported deferred unique constraint");
  }
  const auto table = catalog_->GetTable(command.table_oid_);
  if (table == nullptr) {
    throw std::runtime_error("committed CREATE INDEX references a missing table");
  }
  for (const auto column : command.key_columns_) {
    if (column >= table->schema_.GetColumnCount()) {
      throw std::runtime_error("committed CREATE INDEX references an invalid column");
    }
  }
  CatalogSnapshotIndex definition{command.index_oid_,   command.table_oid_,  command.index_name_,
                                  command.key_columns_, command.index_type_, command.constraint_kind_};
  if (CreateDerivedIndexFromDefinition(definition, catalog_, nullptr) == nullptr ||
      !catalog_->CommitExplicitIndexOid(command.index_oid_)) {
    throw std::runtime_error("committed CREATE INDEX failed");
  }
  catalog_->AdvanceSchemaEpoch();
}

auto BusTubStateMachine::SortedIndexes(const std::shared_ptr<TableInfo> &table) const
    -> std::vector<std::shared_ptr<IndexInfo>> {
  auto indexes = catalog_->GetTableIndexes(table->name_);
  std::sort(indexes.begin(), indexes.end(),
            [](const auto &lhs, const auto &rhs) { return lhs->index_oid_ < rhs->index_oid_; });
  return indexes;
}

auto BusTubStateMachine::ValidateTableKey(const std::shared_ptr<TableInfo> &table,
                                          const EncodedPrimaryKeyV1 &primary_key) const -> std::shared_ptr<IndexInfo> {
  PrimaryKeyCodecV1::Validate(primary_key);
  if (table == nullptr || !table->replicated_primary_key_.has_value() ||
      table->replicated_primary_key_->codec_version_ != primary_key.codec_version_ ||
      table->replicated_primary_key_->type_ != primary_key.type_) {
    throw std::runtime_error("replicated row primary-key definition mismatch");
  }
  std::shared_ptr<IndexInfo> primary;
  for (const auto &index : SortedIndexes(table)) {
    if (index->constraint_kind_ == IndexConstraintKind::PRIMARY_KEY) {
      if (primary != nullptr ||
          index->key_attrs_ != std::vector<uint32_t>{table->replicated_primary_key_->column_oid_}) {
        throw std::runtime_error("replicated table has an invalid primary index set");
      }
      primary = index;
    } else if (index->constraint_kind_ != IndexConstraintKind::NON_UNIQUE_SECONDARY) {
      throw std::runtime_error("UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT");
    }
  }
  if (primary == nullptr) {
    throw std::runtime_error("replicated table has no primary index");
  }
  return primary;
}

auto BusTubStateMachine::LocateRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
    -> std::optional<LocatedRow> {
  const auto table = catalog_->GetTable(table_oid);
  const auto primary = ValidateTableKey(table, primary_key);
  Tuple key_tuple({PrimaryKeyCodecV1::Decode(primary_key)}, &primary->key_schema_);
  std::vector<RID> rids;
  primary->index_->ScanKey(key_tuple, &rids, nullptr);
  std::optional<LocatedRow> result;
  for (const auto rid : rids) {
    auto [meta, tuple] = table->table_->GetTuple(rid);
    if (meta.is_deleted_) {
      continue;
    }
    if (result.has_value()) {
      throw std::runtime_error("primary index returned multiple live rows");
    }
    result = LocatedRow{table, primary, rid, meta, std::move(tuple)};
  }
  return result;
}

void BusTubStateMachine::ValidateTupleKey(const std::shared_ptr<TableInfo> &table,
                                          const EncodedPrimaryKeyV1 &primary_key, const Tuple &tuple) {
  const auto column = table->replicated_primary_key_->column_oid_;
  if (!(PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, column)) == primary_key)) {
    throw std::runtime_error("replicated tuple does not match its logical primary key");
  }
}

void BusTubStateMachine::InsertAllIndexes(const std::shared_ptr<TableInfo> &table, const Tuple &tuple, RID rid) {
  for (const auto &index : SortedIndexes(table)) {
    const auto key = tuple.KeyFromTuple(table->schema_, index->key_schema_, index->key_attrs_);
    if (!index->index_->InsertEntry(key, rid, nullptr)) {
      throw std::runtime_error("committed derived-index insertion failed");
    }
  }
}

void BusTubStateMachine::DeleteAllIndexes(const std::shared_ptr<TableInfo> &table, const Tuple &tuple, RID rid) {
  for (const auto &index : SortedIndexes(table)) {
    const auto key = tuple.KeyFromTuple(table->schema_, index->key_schema_, index->key_attrs_);
    index->index_->DeleteEntry(key, rid, nullptr);
  }
}

void BusTubStateMachine::ApplyInsert(const ReplicatedLogEntry &entry, const InsertRowCommand &command) {
  if (LocateRow(command.table_oid_, command.primary_key_).has_value()) {
    throw std::runtime_error("committed INSERT primary key already exists");
  }
  const auto table = catalog_->GetTable(command.table_oid_);
  const auto tuple = TupleCodecV1::Decode(command.complete_tuple_, table->schema_);
  ValidateTupleKey(table, command.primary_key_, tuple);
  const auto rid = table->table_->InsertTuple({static_cast<timestamp_t>(entry.index_), false}, tuple);
  if (!rid.has_value()) {
    throw std::runtime_error("committed INSERT failed to allocate a tuple");
  }
  InsertAllIndexes(table, tuple, *rid);
}

void BusTubStateMachine::ApplyUpdate(const ReplicatedLogEntry &entry, const UpdateRowCommand &command) {
  auto row = LocateRow(command.table_oid_, command.primary_key_);
  if (!row.has_value() || row->meta_.ts_ < 0 ||
      static_cast<uint64_t>(row->meta_.ts_) != command.expected_old_commit_ts_ ||
      TupleCodecV1::Encode(row->tuple_, row->table_->schema_) != command.expected_old_tuple_) {
    throw std::runtime_error("committed UPDATE old-row precondition mismatch");
  }
  const auto replacement = TupleCodecV1::Decode(command.complete_new_tuple_, row->table_->schema_);
  ValidateTupleKey(row->table_, command.primary_key_, replacement);
  DeleteAllIndexes(row->table_, row->tuple_, row->rid_);
  row->table_->table_->UpdateTupleMeta({static_cast<timestamp_t>(entry.index_), true}, row->rid_);
  const auto new_rid = row->table_->table_->InsertTuple({static_cast<timestamp_t>(entry.index_), false}, replacement);
  if (!new_rid.has_value()) {
    throw std::runtime_error("committed UPDATE failed to allocate replacement tuple");
  }
  InsertAllIndexes(row->table_, replacement, *new_rid);
}

void BusTubStateMachine::ApplyDelete(const ReplicatedLogEntry &entry, const DeleteRowCommand &command) {
  auto row = LocateRow(command.table_oid_, command.primary_key_);
  if (!row.has_value() || row->meta_.ts_ < 0 ||
      static_cast<uint64_t>(row->meta_.ts_) != command.expected_old_commit_ts_ ||
      TupleCodecV1::Encode(row->tuple_, row->table_->schema_) != command.expected_old_tuple_) {
    throw std::runtime_error("committed DELETE old-row precondition mismatch");
  }
  DeleteAllIndexes(row->table_, row->tuple_, row->rid_);
  row->table_->table_->UpdateTupleMeta({static_cast<timestamp_t>(entry.index_), true}, row->rid_);
}

auto BusTubStateMachine::GetRow(table_oid_t table_oid, const EncodedPrimaryKeyV1 &primary_key) const
    -> std::optional<std::pair<TupleMeta, Tuple>> {
  auto shared = visibility_->LockShared();
  if (stopped_) {
    throw std::runtime_error("BusTub state machine is fail-stopped");
  }
  const auto row = LocateRow(table_oid, primary_key);
  if (!row.has_value()) {
    return std::nullopt;
  }
  return std::pair<TupleMeta, Tuple>{row->meta_, row->tuple_};
}

auto BusTubStateMachine::GetLastResponse(uint64_t client_id) const -> std::optional<std::vector<std::byte>> {
  auto shared = visibility_->LockShared();
  if (stopped_) {
    throw std::runtime_error("BusTub state machine is fail-stopped");
  }
  return sessions_->GetLastResponse(client_id);
}

}  // namespace bustub
