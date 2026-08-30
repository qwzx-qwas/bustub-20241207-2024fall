#pragma once

#include <cstddef>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/vector_index_scan_plan.h"

namespace bustub {

class VectorIndexScanExecutor : public AbstractExecutor {
 public:
  VectorIndexScanExecutor(ExecutorContext *exec_ctx, const VectorIndexScanPlanNode *plan);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  auto IsTupleVisible(const TableInfo *table_info, RID rid, Tuple *tuple) const -> bool;
  auto ApplyFilter(const TableInfo *table_info, const Tuple &tuple) const -> bool;
  auto EvaluateDistance(const TableInfo *table_info, const Tuple &tuple) const -> double;

  const VectorIndexScanPlanNode *plan_;
  IndexInfo *index_info_{nullptr};
  std::vector<std::pair<Tuple, RID>> results_;
  std::size_t result_idx_{0};
};

}  // namespace bustub
