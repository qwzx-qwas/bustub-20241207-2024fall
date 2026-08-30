//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sql_command_preparer.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/sql_command_preparer.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "binder/binder.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/index_statement.h"
#include "catalog/catalog_snapshot.h"
#include "common/enums/statement_type.h"
#include "common/util/string_util.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "execution/plans/values_plan.h"
#include "planner/planner.h"

namespace bustub {
namespace {

auto EvaluatePrivateValuesPlan(const AbstractPlanNodeRef &plan) -> std::vector<Tuple> {
  if (plan->GetType() == PlanType::Values) {
    const auto values = std::dynamic_pointer_cast<const ValuesPlanNode>(plan);
    Schema empty({});
    std::vector<Tuple> tuples;
    tuples.reserve(values->GetValues().size());
    for (const auto &row : values->GetValues()) {
      std::vector<Value> evaluated;
      evaluated.reserve(row.size());
      for (const auto &expression : row) {
        evaluated.push_back(expression->Evaluate(nullptr, empty));
      }
      tuples.emplace_back(std::move(evaluated), &values->OutputSchema());
    }
    return tuples;
  }
  if (plan->GetType() == PlanType::Projection) {
    const auto projection = std::dynamic_pointer_cast<const ProjectionPlanNode>(plan);
    const auto child = projection->GetChildPlan();
    const auto child_tuples = EvaluatePrivateValuesPlan(child);
    std::vector<Tuple> tuples;
    tuples.reserve(child_tuples.size());
    for (const auto &child_tuple : child_tuples) {
      std::vector<Value> values;
      values.reserve(projection->GetExpressions().size());
      for (const auto &expression : projection->GetExpressions()) {
        values.push_back(expression->Evaluate(&child_tuple, child->OutputSchema()));
      }
      tuples.emplace_back(std::move(values), &projection->OutputSchema());
    }
    return tuples;
  }
  throw std::runtime_error("distributed V1 INSERT only supports a private VALUES source");
}

auto PrimaryIndex(const Catalog &catalog, const std::shared_ptr<TableInfo> &table) -> std::shared_ptr<IndexInfo> {
  if (table == nullptr || !table->replicated_primary_key_.has_value()) {
    throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  std::shared_ptr<IndexInfo> primary;
  for (const auto &index : catalog.GetTableIndexes(table->name_)) {
    if (index->constraint_kind_ == IndexConstraintKind::PRIMARY_KEY) {
      if (primary != nullptr ||
          index->key_attrs_ != std::vector<uint32_t>{table->replicated_primary_key_->column_oid_}) {
        throw std::runtime_error("replicated table has an ambiguous primary index");
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

void RejectExistingKey(const std::shared_ptr<TableInfo> &table, const std::shared_ptr<IndexInfo> &primary,
                       const EncodedPrimaryKeyV1 &key) {
  Tuple key_tuple({PrimaryKeyCodecV1::Decode(key)}, &primary->key_schema_);
  std::vector<RID> rids;
  primary->index_->ScanKey(key_tuple, &rids, nullptr);
  for (const auto rid : rids) {
    if (!table->table_->GetTupleMeta(rid).is_deleted_) {
      throw std::runtime_error("proposal INSERT primary key already exists");
    }
  }
}

auto IndexTypeFromName(const std::string &raw_name) -> IndexType {
  const auto name = StringUtil::Lower(raw_name);
  if (name.empty() || name == "bplustree") {
    return IndexType::BPlusTreeIndex;
  }
  if (name == "hash") {
    return IndexType::HashTableIndex;
  }
  if (name == "stl_ordered") {
    return IndexType::STLOrderedIndex;
  }
  if (name == "stl_unordered") {
    return IndexType::STLUnorderedIndex;
  }
  if (name == "ivfflat") {
    return IndexType::IVFFlatIndex;
  }
  if (name == "hnsw") {
    return IndexType::HNSWIndex;
  }
  throw std::runtime_error("unsupported distributed V1 index type");
}

auto PrepareCreateTable(const CreateStatement &statement, Catalog *catalog, uint64_t client_id, uint64_t request_id,
                        const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch)
    -> TransactionCommandBatch {
  if (catalog->GetTable(statement.table_) != nullptr) {
    throw std::runtime_error("CREATE TABLE name already exists");
  }
  if (statement.primary_key_.size() != 1) {
    throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  const Schema schema(statement.columns_);
  const auto primary_column = schema.GetColIdx(statement.primary_key_[0]);
  const auto primary_type = schema.GetColumn(primary_column).GetType();
  if (!PrimaryKeyCodecV1::IsSupported(primary_type)) {
    throw std::runtime_error("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  ValidateReplicatedIndexV1({catalog->GetNextIndexOid(),
                             catalog->GetNextTableOid(),
                             "__candidate_primary",
                             {primary_column},
                             IndexType::BPlusTreeIndex,
                             IndexConstraintKind::PRIMARY_KEY},
                            schema);
  std::vector<ReplicatedColumnDefinition> columns;
  columns.reserve(statement.columns_.size());
  for (uint32_t index = 0; index < statement.columns_.size(); index++) {
    const auto &column = statement.columns_[index];
    columns.push_back({column.GetName(), column.GetType(), column.GetStorageSize(), index != primary_column});
  }
  CreateTableCommand command{catalog->GetNextTableOid(),
                             catalog->GetNextIndexOid(),
                             statement.table_,
                             std::move(columns),
                             {primary_column, primary_type, PrimaryKeyCodecV1::FORMAT_VERSION}};
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, {std::move(command)});
}

auto PrepareCreateIndex(const IndexStatement &statement, Catalog *catalog, uint64_t client_id, uint64_t request_id,
                        const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch)
    -> TransactionCommandBatch {
  if (statement.is_unique_) {
    throw std::runtime_error("UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT");
  }
  if (!statement.options_.empty()) {
    throw std::runtime_error("distributed V1 index build options are not protocol fields");
  }
  const auto table = catalog->GetTable(statement.table_->oid_);
  if (table == nullptr || catalog->GetIndex(statement.index_name_, statement.table_->oid_) != nullptr) {
    throw std::runtime_error("CREATE INDEX table is missing or name already exists");
  }
  std::vector<uint32_t> columns;
  for (const auto &column : statement.cols_) {
    columns.push_back(table->schema_.GetColIdx(column->col_name_.back()));
  }
  CreateIndexCommand command{catalog->GetNextIndexOid(),
                             table->oid_,
                             statement.index_name_,
                             std::move(columns),
                             IndexTypeFromName(statement.index_type_),
                             IndexConstraintKind::NON_UNIQUE_SECONDARY};
  ValidateReplicatedIndexV1({command.index_oid_, command.table_oid_, command.index_name_, command.key_columns_,
                             command.index_type_, command.constraint_kind_},
                            table->schema_);
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, {std::move(command)});
}

auto PrepareInsert(const AbstractPlanNodeRef &plan, Catalog *catalog, uint64_t client_id, uint64_t request_id,
                   const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch) -> TransactionCommandBatch {
  const auto insert = std::dynamic_pointer_cast<const InsertPlanNode>(plan);
  const auto table = catalog->GetTable(insert->GetTableOid());
  const auto primary = PrimaryIndex(*catalog, table);
  std::vector<ReplicatedCommand> commands;
  for (const auto &tuple : EvaluatePrivateValuesPlan(insert->GetChildPlan())) {
    const auto key =
        PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_));
    RejectExistingKey(table, primary, key);
    commands.emplace_back(InsertRowCommand{table->oid_, key, TupleCodecV1::Encode(tuple, table->schema_)});
  }
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, std::move(commands));
}

auto MutationFilter(const AbstractPlanNodeRef &child) -> std::shared_ptr<const FilterPlanNode> {
  if (child->GetType() != PlanType::Filter) {
    throw std::runtime_error("distributed V1 mutation requires a deterministic table filter");
  }
  auto filter = std::dynamic_pointer_cast<const FilterPlanNode>(child);
  if (filter->GetChildPlan()->GetType() != PlanType::SeqScan) {
    throw std::runtime_error("distributed V1 mutation only supports one base table");
  }
  return filter;
}

auto PrepareDelete(const AbstractPlanNodeRef &plan, Catalog *catalog, uint64_t client_id, uint64_t request_id,
                   const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch) -> TransactionCommandBatch {
  const auto deletion = std::dynamic_pointer_cast<const DeletePlanNode>(plan);
  const auto table = catalog->GetTable(deletion->GetTableOid());
  static_cast<void>(PrimaryIndex(*catalog, table));
  const auto filter = MutationFilter(deletion->GetChildPlan());
  std::vector<ReplicatedCommand> commands;
  for (auto iterator = table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
    const auto [meta, tuple] = iterator.GetTuple();
    const auto selected = filter->GetPredicate()->Evaluate(&tuple, filter->GetChildPlan()->OutputSchema());
    if (meta.is_deleted_ || selected.IsNull() || !selected.GetAs<bool>()) {
      continue;
    }
    const auto key =
        PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_));
    commands.emplace_back(DeleteRowCommand{table->oid_, key, static_cast<uint64_t>(meta.ts_),
                                           TupleCodecV1::Encode(tuple, table->schema_)});
  }
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, std::move(commands));
}

auto PrepareUpdate(const AbstractPlanNodeRef &plan, Catalog *catalog, uint64_t client_id, uint64_t request_id,
                   const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch) -> TransactionCommandBatch {
  const auto update = std::dynamic_pointer_cast<const UpdatePlanNode>(plan);
  const auto table = catalog->GetTable(update->GetTableOid());
  static_cast<void>(PrimaryIndex(*catalog, table));
  const auto filter = MutationFilter(update->GetChildPlan());
  std::vector<ReplicatedCommand> commands;
  for (auto iterator = table->table_->MakeIterator(); !iterator.IsEnd(); ++iterator) {
    const auto [meta, tuple] = iterator.GetTuple();
    const auto selected = filter->GetPredicate()->Evaluate(&tuple, filter->GetChildPlan()->OutputSchema());
    if (meta.is_deleted_ || selected.IsNull() || !selected.GetAs<bool>()) {
      continue;
    }
    std::vector<Value> values;
    values.reserve(update->target_expressions_.size());
    for (const auto &expression : update->target_expressions_) {
      values.push_back(expression->Evaluate(&tuple, filter->OutputSchema()));
    }
    Tuple replacement(std::move(values), &table->schema_);
    const auto key =
        PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_));
    if (!(PrimaryKeyCodecV1::Encode(
              replacement.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_)) == key)) {
      throw std::runtime_error("distributed V1 UPDATE cannot modify the primary-key column");
    }
    commands.emplace_back(UpdateRowCommand{table->oid_, key, static_cast<uint64_t>(meta.ts_),
                                           TupleCodecV1::Encode(tuple, table->schema_),
                                           TupleCodecV1::Encode(replacement, table->schema_)});
  }
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, std::move(commands));
}

}  // namespace

auto SqlCommandPreparer::Prepare(const std::string &sql, uint64_t client_id, uint64_t request_id,
                                 const RequestFingerprintV1 &request_fingerprint) const -> TransactionCommandBatch {
  if (catalog_ == nullptr || sql.empty()) {
    throw std::runtime_error("invalid distributed SQL prepare request");
  }
  request_fingerprint.Validate();
  Binder binder(*catalog_);
  binder.ParseAndSave(sql);
  if (binder.statement_nodes_.size() != 1) {
    throw std::runtime_error("distributed V1 accepts exactly one autocommit statement per request");
  }
  auto statement = binder.BindStatement(binder.statement_nodes_[0]);
  const auto schema_epoch = catalog_->GetSchemaEpoch();
  if (statement->type_ == StatementType::CREATE_STATEMENT) {
    return PrepareCreateTable(dynamic_cast<const CreateStatement &>(*statement), catalog_, client_id, request_id,
                              request_fingerprint, schema_epoch);
  }
  if (statement->type_ == StatementType::INDEX_STATEMENT) {
    return PrepareCreateIndex(dynamic_cast<const IndexStatement &>(*statement), catalog_, client_id, request_id,
                              request_fingerprint, schema_epoch);
  }

  Planner planner(*catalog_);
  planner.PlanQuery(*statement);
  if (statement->type_ == StatementType::INSERT_STATEMENT) {
    return PrepareInsert(planner.plan_, catalog_, client_id, request_id, request_fingerprint, schema_epoch);
  }
  if (statement->type_ == StatementType::DELETE_STATEMENT) {
    return PrepareDelete(planner.plan_, catalog_, client_id, request_id, request_fingerprint, schema_epoch);
  }
  if (statement->type_ == StatementType::UPDATE_STATEMENT) {
    return PrepareUpdate(planner.plan_, catalog_, client_id, request_id, request_fingerprint, schema_epoch);
  }
  throw std::runtime_error("distributed SQL prepare only accepts write statements");
}

}  // namespace bustub
