#include "execution/executors/vector_knn_scan_executor.h"

#include <algorithm>
#include <utility>
#include <tuple>

#include "execution/executor_factory.h"
#include "execution/execution_common.h"
#include "type/type_id.h"

namespace bustub {
VectorKnnScanExecutor::VectorKnnScanExecutor(ExecutorContext *exec_ctx, const VectorKnnScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void VectorKnnScanExecutor::Init() {
  results_.clear();
  result_index_ = 0;
  if (plan_->GetK() == 0) {
    return;
  }

  if (child_executor_ == nullptr) {
    child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
  }
  child_executor_->Init();

  std::vector<std::tuple<double, Tuple, RID>> candidates;
  Tuple tuple;
  RID rid;
  const auto &child_schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&tuple, &rid)) {
    // The child executor already enforces MVCC visibility, so exact KNN only
    // needs to compute distance and keep the best visible tuples.
    const auto distance_val = plan_->GetDistanceExpr()->Evaluate(&tuple, child_schema).CastAs(TypeId::DECIMAL);
    candidates.emplace_back(distance_val.GetAs<double>(), tuple, rid);
  }

  std::sort(candidates.begin(), candidates.end(),
            [](const auto &a, const auto &b) { return std::get<0>(a) < std::get<0>(b); });

  // Materialize the final top-k in ORDER BY distance ASC order.
  const auto keep = std::min(plan_->GetK(), candidates.size());
  results_.reserve(keep);
  for (std::size_t i = 0; i < keep; i++) {
    results_.emplace_back(std::get<1>(candidates[i]), std::get<2>(candidates[i]));
  }
}

auto VectorKnnScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_index_ >= results_.size()) {
    return false;
  }
  *tuple = results_[result_index_].first;
  *rid = results_[result_index_].second;
  result_index_++;
  return true;
}
}  // namespace bustub
