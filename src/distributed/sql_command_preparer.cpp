//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sql_command_preparer.cpp
//
//===----------------------------------------------------------------------===//

#include "distributed/sql_command_preparer.h"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"

#include "binder/binder.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/index_statement.h"
#include "catalog/catalog_snapshot.h"
#include "common/enums/statement_type.h"
#include "common/util/string_util.h"
#include "execution/executor_factory.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
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
  throw SqlRequestError("distributed V1 INSERT only supports a private VALUES source");
}

auto PrimaryIndex(const Catalog &catalog, const std::shared_ptr<TableInfo> &table) -> std::shared_ptr<IndexInfo> {
  if (table == nullptr || !table->replicated_primary_key_.has_value()) {
    throw SqlRequestError("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
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
      throw SqlRequestError("UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT");
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
      throw SqlRequestError("proposal INSERT primary key already exists");
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
  throw SqlRequestError("unsupported distributed V1 index type");
}

// This validator only examines the proposed definition, not stored pages.
// Keep recovery/proposal validation strict; classify user DDL errors at this boundary.
void ValidateRequestedIndex(const CatalogSnapshotIndex &index, const Schema &schema) {
  try {
    ValidateReplicatedIndexV1(index, schema);
  } catch (const std::runtime_error &error) {
    throw SqlRequestError(error.what());
  }
}

auto PrepareCreateTable(const CreateStatement &statement, Catalog *catalog, uint64_t client_id, uint64_t request_id,
                        const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch)
    -> TransactionCommandBatch {
  if (catalog->GetTable(statement.table_) != nullptr) {
    throw SqlRequestError("CREATE TABLE name already exists");
  }
  if (statement.primary_key_.size() != 1) {
    throw SqlRequestError("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  const Schema schema(statement.columns_);
  const auto primary_column = schema.GetColIdx(statement.primary_key_[0]);
  const auto primary_type = schema.GetColumn(primary_column).GetType();
  if (!PrimaryKeyCodecV1::IsSupported(primary_type)) {
    throw SqlRequestError("UNSUPPORTED_REPLICATED_PRIMARY_KEY");
  }
  ValidateRequestedIndex({catalog->GetNextIndexOid(),
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
    throw SqlRequestError("UNSUPPORTED_DEFERRED_UNIQUE_CONSTRAINT");
  }
  if (!statement.options_.empty()) {
    throw SqlRequestError("distributed V1 index build options are not protocol fields");
  }
  const auto table = catalog->GetTable(statement.table_->oid_);
  if (table == nullptr || catalog->GetIndex(statement.index_name_, statement.table_->oid_) != nullptr) {
    throw SqlRequestError("CREATE INDEX table is missing or name already exists");
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
  ValidateRequestedIndex({command.index_oid_, command.table_oid_, command.index_name_, command.key_columns_,
                          command.index_type_, command.constraint_kind_},
                         table->schema_);
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, {std::move(command)});
}

// Bound private command construction while scanning, before Encode allocates a
// second representation. Include each variant/identity, not only row payload.
void ChargeRow(size_t *remaining, size_t bytes) {
  bytes += sizeof(ReplicatedCommand) + 64;
  if (bytes > *remaining) throw SqlRequestError("prepared command exceeds request capacity");
  *remaining -= bytes;
}

auto PrepareInsert(const AbstractPlanNodeRef &plan, const std::vector<Tuple> &values, Catalog *catalog,
                   uint64_t client_id, uint64_t request_id, const RequestFingerprintV1 &request_fingerprint,
                   uint64_t schema_epoch, size_t remaining) -> TransactionCommandBatch {
  const auto insert = std::dynamic_pointer_cast<const InsertPlanNode>(plan);
  const auto table = catalog->GetTable(insert->GetTableOid());
  const auto primary = PrimaryIndex(*catalog, table);
  std::vector<ReplicatedCommand> commands;
  for (const auto &tuple : values) {
    const auto key =
        PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_));
    RejectExistingKey(table, primary, key);
    auto body = TupleCodecV1::Encode(tuple, table->schema_);
    ChargeRow(&remaining, body.size() + key.bytes_.size());
    commands.emplace_back(InsertRowCommand{table->oid_, key, std::move(body)});
  }
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, std::move(commands));
}

auto MutationFilter(const AbstractPlanNodeRef &child) -> std::shared_ptr<const FilterPlanNode> {
  if (child->GetType() != PlanType::Filter) {
    throw SqlRequestError("distributed V1 mutation requires a deterministic table filter");
  }
  auto filter = std::dynamic_pointer_cast<const FilterPlanNode>(child);
  if (filter->GetChildPlan()->GetType() != PlanType::SeqScan) {
    throw SqlRequestError("distributed V1 mutation only supports one base table");
  }
  return filter;
}

// The optimized plan owns the nodes referenced by the executor.
struct MutationCandidates {
  AbstractPlanNodeRef plan_;
  std::unique_ptr<AbstractExecutor> executor_;
  MutationCandidates(const AbstractPlanNodeRef &filter, ExecutorContext *context) {
    Optimizer optimizer(*context->GetCatalog(), false);
    plan_ = optimizer.Optimize(filter);
    executor_ = ExecutorFactory::CreateExecutor(context, plan_);
    executor_->Init();
  }
};

auto PrepareDelete(const AbstractPlanNodeRef &plan, Catalog *catalog, ExecutorContext *context, uint64_t client_id,
                   uint64_t request_id, const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch,
                   size_t remaining) -> TransactionCommandBatch {
  const auto deletion = std::dynamic_pointer_cast<const DeletePlanNode>(plan);
  const auto table = catalog->GetTable(deletion->GetTableOid());
  static_cast<void>(PrimaryIndex(*catalog, table));
  const auto filter = MutationFilter(deletion->GetChildPlan());
  std::vector<ReplicatedCommand> commands;
  auto candidates = MutationCandidates(filter, context);
  Tuple tuple;
  RID rid;
  while (candidates.executor_->Next(&tuple, &rid)) {
    const auto meta = table->table_->GetTupleMeta(rid);
    const auto key =
        PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_));
    auto body = TupleCodecV1::Encode(tuple, table->schema_);
    ChargeRow(&remaining, body.size() + key.bytes_.size());
    commands.emplace_back(DeleteRowCommand{table->oid_, key, static_cast<uint64_t>(meta.ts_), std::move(body)});
  }
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, std::move(commands));
}

auto PrepareUpdate(const AbstractPlanNodeRef &plan, Catalog *catalog, ExecutorContext *context, uint64_t client_id,
                   uint64_t request_id, const RequestFingerprintV1 &request_fingerprint, uint64_t schema_epoch,
                   size_t remaining) -> TransactionCommandBatch {
  const auto update = std::dynamic_pointer_cast<const UpdatePlanNode>(plan);
  const auto table = catalog->GetTable(update->GetTableOid());
  static_cast<void>(PrimaryIndex(*catalog, table));
  const auto filter = MutationFilter(update->GetChildPlan());
  std::vector<ReplicatedCommand> commands;
  auto candidates = MutationCandidates(filter, context);
  Tuple tuple;
  RID rid;
  while (candidates.executor_->Next(&tuple, &rid)) {
    const auto meta = table->table_->GetTupleMeta(rid);
    std::vector<Value> values;
    values.reserve(update->target_expressions_.size());
    for (const auto &expression : update->target_expressions_) {
      values.push_back(expression->Evaluate(&tuple, filter->OutputSchema()));
    }
    Tuple replacement(std::move(values), &table->schema_);
    const auto key =
        PrimaryKeyCodecV1::Encode(tuple.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_));
    const auto replacement_key = replacement.GetValue(&table->schema_, table->replicated_primary_key_->column_oid_);
    if (replacement_key.IsNull() || !(PrimaryKeyCodecV1::Encode(replacement_key) == key)) {
      throw SqlRequestError("distributed V1 UPDATE cannot modify the primary-key column");
    }
    auto old_body = TupleCodecV1::Encode(tuple, table->schema_);
    auto new_body = TupleCodecV1::Encode(replacement, table->schema_);
    ChargeRow(&remaining, old_body.size() + new_body.size() + key.bytes_.size());
    commands.emplace_back(
        UpdateRowCommand{table->oid_, key, static_cast<uint64_t>(meta.ts_), std::move(old_body), std::move(new_body)});
  }
  return CommandBuilder::Build(client_id, request_id, request_fingerprint, schema_epoch, std::move(commands));
}

}  // namespace

// String keys are an in-memory index, using the existing canonical key codec.
// A table has a fixed key type/codec; prefixing the type also makes this explicit.
static auto DependencyKey(const Value &value) -> std::string {
  const auto key = PrimaryKeyCodecV1::Encode(value);
  std::string result(1, static_cast<char>(key.type_));
  result.append(reinterpret_cast<const char *>(key.bytes_.data()), key.bytes_.size());
  return result;
}

static auto FiniteKeys(const AbstractExpressionRef &expression, uint32_t column, TypeId type)
    -> std::optional<std::vector<std::string>> {
  if (auto logic = dynamic_cast<const LogicExpression *>(expression.get())) {
    auto left = FiniteKeys(logic->GetChildAt(0), column, type);
    auto right = FiniteKeys(logic->GetChildAt(1), column, type);
    if (logic->logic_type_ == LogicType::And) return left ? left : right;
    if (!left || !right) return std::nullopt;
    left->insert(left->end(), right->begin(), right->end());
    return left;
  }
  auto equal = dynamic_cast<const ComparisonExpression *>(expression.get());
  if (!equal || equal->comp_type_ != ComparisonType::Equal) return std::nullopt;
  for (size_t side = 0; side < 2; ++side) {
    auto col = dynamic_cast<const ColumnValueExpression *>(equal->GetChildAt(side).get());
    auto constant = dynamic_cast<const ConstantValueExpression *>(equal->GetChildAt(1 - side).get());
    if (col && constant && col->GetTupleIdx() == 0 && col->GetColIdx() == column &&
        constant->val_.GetTypeId() == type && !constant->val_.IsNull())
      return std::vector<std::string>{DependencyKey(constant->val_)};
  }
  return std::nullopt;
}

auto SqlCommandPreparer::Analyze(const std::string &sql, uint64_t client_id, uint64_t request_id,
                                 const RequestFingerprintV1 &request_fingerprint) const -> SqlWritePlan {
  if (client_id == 0 || request_id == 0 || sql.empty())
    throw std::runtime_error("invalid distributed SQL prepare request");
  request_fingerprint.Validate();
  Binder binder(*catalog_);
  binder.ParseAndSave(sql);
  if (binder.statement_nodes_.size() != 1)
    throw SqlRequestError("distributed V1 accepts exactly one autocommit statement per request");
  auto statement = binder.BindStatement(binder.statement_nodes_[0]);
  SqlWritePlan result;
  result.schema_epoch_ = catalog_->GetSchemaEpoch();
  if (statement->type_ == StatementType::CREATE_STATEMENT) {
    result.ddl_ = PrepareCreateTable(dynamic_cast<const CreateStatement &>(*statement), catalog_, client_id, request_id,
                                     request_fingerprint, result.schema_epoch_);
    return result;
  }
  if (statement->type_ == StatementType::INDEX_STATEMENT) {
    result.ddl_ = PrepareCreateIndex(dynamic_cast<const IndexStatement &>(*statement), catalog_, client_id, request_id,
                                     request_fingerprint, result.schema_epoch_);
    return result;
  }
  Planner planner(*catalog_);
  planner.PlanQuery(*statement);
  result.plan_ = planner.plan_;
  if (auto insert = dynamic_cast<const InsertPlanNode *>(result.plan_.get())) {
    result.table_ = insert->GetTableOid();
    result.insert_values_ = EvaluatePrivateValuesPlan(insert->GetChildPlan());
  } else if (auto update = dynamic_cast<const UpdatePlanNode *>(result.plan_.get())) {
    result.table_ = update->GetTableOid();
  } else if (auto deletion = dynamic_cast<const DeletePlanNode *>(result.plan_.get())) {
    result.table_ = deletion->GetTableOid();
  } else {
    throw SqlRequestError("distributed SQL prepare only accepts write statements");
  }
  const auto table = catalog_->GetTable(result.table_);
  static_cast<void>(PrimaryIndex(*catalog_, table));
  const auto column = table->replicated_primary_key_->column_oid_;
  result.scope_ = SqlWritePlan::Scope::KEYS;
  if (result.plan_->GetType() == PlanType::Insert) {
    for (const auto &tuple : result.insert_values_) {
      const auto key = tuple.GetValue(&table->schema_, column);
      if (key.IsNull()) throw SqlRequestError("INSERT primary key cannot be NULL");
      result.keys_.push_back(DependencyKey(key));
    }
  } else {
    auto keys = FiniteKeys(MutationFilter(result.plan_->GetChildAt(0))->GetPredicate(), column,
                           table->schema_.GetColumn(column).GetType());
    if (keys)
      result.keys_ = std::move(*keys);
    else
      result.scope_ = SqlWritePlan::Scope::TABLE;
  }
  std::sort(result.keys_.begin(), result.keys_.end());
  if (result.plan_->GetType() == PlanType::Insert &&
      std::adjacent_find(result.keys_.begin(), result.keys_.end()) != result.keys_.end())
    throw SqlRequestError("INSERT contains duplicate primary keys");
  result.keys_.erase(std::unique(result.keys_.begin(), result.keys_.end()), result.keys_.end());
  return result;
}

auto SqlCommandPreparer::Prepare(const SqlWritePlan &plan, uint64_t client_id, uint64_t request_id,
                                 const RequestFingerprintV1 &fingerprint) const -> TransactionCommandBatch {
  if (plan.schema_epoch_ != catalog_->GetSchemaEpoch()) throw std::runtime_error("SQL plan schema changed");
  if (plan.ddl_) return *plan.ddl_;
  switch (plan.plan_->GetType()) {
    case PlanType::Insert:
      return PrepareInsert(plan.plan_, plan.insert_values_, catalog_, client_id, request_id, fingerprint,
                           plan.schema_epoch_, command_bytes_);
    case PlanType::Delete:
      return PrepareDelete(plan.plan_, catalog_, context_, client_id, request_id, fingerprint, plan.schema_epoch_,
                           command_bytes_);
    case PlanType::Update:
      return PrepareUpdate(plan.plan_, catalog_, context_, client_id, request_id, fingerprint, plan.schema_epoch_,
                           command_bytes_);
    default:
      throw std::runtime_error("invalid SQL write plan");
  }
}

auto SqlCommandPreparer::Prepare(const std::string &sql, uint64_t client_id, uint64_t request_id,
                                 const RequestFingerprintV1 &fingerprint) const -> TransactionCommandBatch {
  return Prepare(Analyze(sql, client_id, request_id, fingerprint), client_id, request_id, fingerprint);
}

}  // namespace bustub
