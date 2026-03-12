#include "execution/executors/vector_index_scan_executor.h"

#include <optional>
#include <vector>

#include "common/exception.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "type/type_id.h"

namespace bustub {

VectorIndexScanExecutor::VectorIndexScanExecutor(ExecutorContext *exec_ctx, const VectorIndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void VectorIndexScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid()).get();
  BUSTUB_ASSERT(index_info_ != nullptr, "vector index not found");

  auto table_info = catalog->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "table for vector index scan not found");

  result_rids_.clear();
  result_idx_ = 0;

  const auto query_value = plan_->GetQueryExpr()->Evaluate(nullptr, table_info->schema_);
  BUSTUB_ASSERT(query_value.GetTypeId() == TypeId::VECTOR, "vector index scan query must evaluate to VECTOR");

  Tuple query_tuple({query_value}, index_info_->index_->GetKeySchema());
  index_info_->index_->SearchKnn(query_tuple, plan_->GetK(), &result_rids_, exec_ctx_->GetTransaction());

  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->GetTableOid(), plan_->GetFilterPredicate());
  }
}

auto VectorIndexScanExecutor::IsTupleVisible(const TableInfo *table_info, RID rid, Tuple *tuple) const -> bool {
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), rid);

  auto undo_logs_opt = CollectUndoLogs(rid, base_meta, base_tuple, undo_link, txn, txn_mgr);
  if (!undo_logs_opt.has_value()) {
    return false;
  }

  std::optional<Tuple> visible_tuple_opt;
  if (undo_logs_opt->empty()) {
    if (base_meta.is_deleted_) {
      return false;
    }
    visible_tuple_opt = base_tuple;
  } else {
    visible_tuple_opt = ReconstructTuple(&table_info->schema_, base_tuple, base_meta, *undo_logs_opt);
  }

  if (!visible_tuple_opt.has_value()) {
    return false;
  }

  *tuple = *visible_tuple_opt;
  return true;
}

auto VectorIndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "table for vector index scan not found");

  while (result_idx_ < result_rids_.size()) {
    *rid = result_rids_[result_idx_++];

    if (!IsTupleVisible(table_info.get(), *rid, tuple)) {
      continue;
    }

    const auto &predicate = plan_->GetFilterPredicate();
    if (predicate != nullptr) {
      const auto value = predicate->Evaluate(tuple, table_info->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }

    return true;
  }

  return false;
}

}  // namespace bustub
