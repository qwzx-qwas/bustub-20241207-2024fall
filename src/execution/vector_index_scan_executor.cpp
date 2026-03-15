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

namespace {

/** 作用：校验索引候选携带的键版本是否与当前事务可见 tuple 的索引键一致。 */
auto CandidateMatchesVisibleTuple(const TableInfo *table_info, const IndexInfo *index_info, const Tuple &visible_tuple,
                                  const VectorIndexCandidate &candidate) -> bool {
  const auto visible_key =
      visible_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
  return IsTupleContentEqual(visible_key, candidate.index_key_);
}

}  // namespace

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
  // 作用：执行器只感知统一预算语义，不直接依赖 nprobe 这类索引私有术语。
  auto search_options = index_info_->index_->GetDefaultAnnSearchOptions(plan_->GetK()).value_or(
      AnnSearchOptions{plan_->GetK(), std::max<std::size_t>(1, plan_->GetK()), 1});
  const auto max_search_budget = std::max(search_options.search_budget_,
                                          index_info_->index_->GetMaxAnnSearchBudget().value_or(search_options.search_budget_));
  std::unordered_set<std::uint64_t> seen_candidate_ids;
  std::unordered_set<RID> accepted_rids;
  // 通过检查的候选
  std::vector<std::tuple<double, Tuple, RID>> accepted_candidates;

  while (true) {
    std::vector<VectorIndexCandidate> round_candidates;
    index_info_->index_->SearchVector(query_tuple, search_options, &round_candidates, txn);

    for (const auto &candidate : round_candidates) {
      if (!seen_candidate_ids.emplace(candidate.candidate_id_).second) {
        continue;
      }

      Tuple visible_tuple;
      // 做MVCC可见性检查和重建，如果不可见就跳过这个候选项。
      if (!IsTupleVisible(table_info.get(), candidate.rid_, &visible_tuple)) {
        continue;
      }

      // 只有候选键版本与当前事务可见版本一致时，才认为它不是 stale 命中。
      if (!CandidateMatchesVisibleTuple(table_info.get(), index_info_, visible_tuple, candidate)) {
        continue;
      }

      if (!ApplyFilter(table_info.get(), visible_tuple)) {
        continue;
      }

      if (!accepted_rids.emplace(candidate.rid_).second) {
        continue;
      }

      accepted_candidates.emplace_back(EvaluateDistance(table_info.get(), visible_tuple), visible_tuple, candidate.rid_);
    }

    if (accepted_candidates.size() >= plan_->GetK()) {
      break;
    }

    const bool can_expand_search_budget = search_options.search_budget_ < max_search_budget;
    const bool can_expand_candidate_budget = round_candidates.size() >= search_options.candidate_budget_;
    if (!can_expand_search_budget && !can_expand_candidate_budget) {
      break;
    }

    if (can_expand_search_budget) {
      search_options.search_budget_ =
          std::min(max_search_budget, std::max(search_options.search_budget_ + 1, search_options.search_budget_ * 2));
    }
    if (can_expand_candidate_budget) {
      search_options.candidate_budget_ =
          std::max(search_options.candidate_budget_ + 1, search_options.candidate_budget_ * 2);
    }
  }

  // Re-sort accepted tuples so executor output still matches ORDER BY distance.
  std::sort(accepted_candidates.begin(), accepted_candidates.end(),
            [](const auto &a, const auto &b) {
              if (std::get<0>(a) != std::get<0>(b)) {
                return std::get<0>(a) < std::get<0>(b);
              }
              return std::get<2>(a).Get() < std::get<2>(b).Get();
            });

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
