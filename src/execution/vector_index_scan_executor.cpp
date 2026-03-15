#include "execution/executors/vector_index_scan_executor.h"

#include <algorithm>
#include <optional>
#include <unordered_set>
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

  results_.clear();
  result_idx_ = 0;

  const auto query_value = plan_->GetQueryExpr()->Evaluate(nullptr, table_info->schema_);
  BUSTUB_ASSERT(query_value.GetTypeId() == TypeId::VECTOR, "vector index scan query must evaluate to VECTOR");

  Tuple query_tuple({query_value}, index_info_->index_->GetKeySchema());

  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->GetTableOid(), plan_->GetFilterPredicate());
  }
  // 解决补候选问题：
  const auto default_probe_count = index_info_->index_->GetDefaultKnnProbeCount().value_or(1);
  const auto max_probe_count = std::max(default_probe_count, index_info_->index_->GetMaxKnnProbeCount().value_or(default_probe_count));
  // 当前探测多少个ivf列表，或者说多少个probe。
  std::size_t probe_count = std::max<std::size_t>(1, default_probe_count);
  // 当前从索引最多拿多少个候选项
  std::size_t candidate_limit = std::max<std::size_t>(1, plan_->GetK());
  // 防止重复处理同一个RID
  std::unordered_set<RID> seen_rids;
  // 通过检查的候选
  std::vector<std::tuple<double, Tuple, RID>> accepted_candidates;

  while (true) {
    std::vector<RID> round_rids;
    // Ask the index for a progressively wider candidate set when earlier
    // candidates disappear after MVCC reconstruction or filter evaluation.
    index_info_->index_->SearchKnnWithProbe(query_tuple, candidate_limit, probe_count, &round_rids, txn);

    for (const auto &candidate_rid : round_rids) {
      if (!seen_rids.emplace(candidate_rid).second) {
        continue;
      }

      Tuple visible_tuple;
      // 做MVCC可见性检查和重建，如果不可见就跳过这个候选项。
      if (!IsTupleVisible(table_info.get(), candidate_rid, &visible_tuple)) {
        continue;
      }

      if (!ApplyFilter(table_info.get(), visible_tuple)) {
        continue;
      }

      accepted_candidates.emplace_back(EvaluateDistance(table_info.get(), visible_tuple), visible_tuple, candidate_rid);
    }

    if (accepted_candidates.size() >= plan_->GetK()) {
      break;
    }

    const bool can_expand_probe = probe_count < max_probe_count;
    const bool can_expand_limit = round_rids.size() >= candidate_limit;
    if (!can_expand_probe && !can_expand_limit) {
      break;
    }

    if (can_expand_probe) {
      // IVFFlat recall usually improves by probing more lists first.
      probe_count = std::min(max_probe_count, std::max(probe_count + 1, probe_count * 2));
    }
    if (can_expand_limit) {
      // Also over-fetch within the probed lists so filtering does not starve
      // the final top-k result.
      candidate_limit = std::max(candidate_limit + 1, candidate_limit * 2);
    }
  }

  // Re-sort accepted tuples so executor output still matches ORDER BY distance.
  std::sort(accepted_candidates.begin(), accepted_candidates.end(),
            [](const auto &a, const auto &b) { return std::get<0>(a) < std::get<0>(b); });

  const auto keep = std::min(plan_->GetK(), accepted_candidates.size());
  results_.reserve(keep);
  for (std::size_t i = 0; i < keep; i++) {
    results_.emplace_back(std::get<1>(accepted_candidates[i]), std::get<2>(accepted_candidates[i]));
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

auto VectorIndexScanExecutor::ApplyFilter(const TableInfo *table_info, const Tuple &tuple) const -> bool {
  const auto &predicate = plan_->GetFilterPredicate();
  if (predicate == nullptr) {
    return true;
  }

  // Filters are evaluated on the MVCC-visible tuple, not the base table image.
  const auto value = predicate->Evaluate(&tuple, table_info->schema_);
  return !value.IsNull() && value.GetAs<bool>();
}

auto VectorIndexScanExecutor::EvaluateDistance(const TableInfo *table_info, const Tuple &tuple) const -> double {
  if (plan_->GetDistanceExpr() == nullptr) {
    return 0.0;
  }

  return plan_->GetDistanceExpr()->Evaluate(&tuple, table_info->schema_).CastAs(TypeId::DECIMAL).GetAs<double>();
}

auto VectorIndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_idx_ >= results_.size()) {
    return false;
  }

  *tuple = results_[result_idx_].first;
  *rid = results_[result_idx_].second;
  result_idx_++;
  return true;
}

}  // namespace bustub
