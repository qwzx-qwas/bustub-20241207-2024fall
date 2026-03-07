#include "execution/executors/vector_knn_scan_executor.h"

#include <algorithm>
#include <tuple>

#include "execution/executor_factory.h"

namespace bustub {
VectorKnnScanExecutor::VectorKnnScanExecutor(ExecutorContext *exec_ctx, const VectorKnnScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void VectorKnnScanExecutor::Init() {
  // Clear previous results and reset cursor.
  results_.clear();
  result_index_ = 0;
  // Initialize child executor to fetch candidates.
  if (child_executor_ == nullptr) {
    child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
  }
  child_executor_->Init();

  // Collect all candidates with their computed distance to query vector.
  std::vector<std::tuple<double, Tuple, RID>> candidates;
  Tuple tuple;
  RID rid;
  const auto &child_schema = child_executor_->GetOutputSchema();
  // 全量候选
  while (child_executor_->Next(&tuple, &rid)) {
    const auto distance_val = plan_->GetDistanceExpr()->Evaluate(&tuple, child_schema).CastAs(TypeId::DECIMAL);
    candidates.emplace_back(distance_val.GetAs<double>(), tuple, rid);
  }
  // 排序
  std::sort(candidates.begin(), candidates.end(),
            [](const auto &a, const auto &b) { return std::get<0>(a) < std::get<0>(b); });
  // top-K
  const auto keep = std::min(plan_->GetK(), candidates.size());
  results_.reserve(keep);
  for (std::size_t i = 0; i < keep; i++) {
    results_.emplace_back(std::get<1>(candidates[i]), std::get<2>(candidates[i]));
  }
}
// 目前是非MVCC版本
auto VectorKnnScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_index_ >= results_.size()) {
    return false;
  }
  *tuple = results_[result_index_].first;
  *rid = results_[result_index_].second;
  result_index_++;
  return true;
}
}
