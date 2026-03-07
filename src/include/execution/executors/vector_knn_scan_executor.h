#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/vector_knn_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class VectorKnnScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new VectorKnnScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The vector KNN scan plan to be executed
   */
  VectorKnnScanExecutor(ExecutorContext *exec_ctx, const VectorKnnScanPlanNode *plan);

  /** Initialize the vector KNN scan. */
  void Init() override;

  /**
   * Yield the next tuple from the vector KNN scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the vector KNN scan. */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only). */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

 private:
  /** The vector KNN scan plan node to be executed. */
  const VectorKnnScanPlanNode *plan_;
  /** Child executor from which candidate tuples are obtained. */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Materialized top-k tuples and their RIDs. */
  std::vector<std::pair<Tuple, RID>> results_;
  /** Current read cursor in `results_`. */
  std::size_t result_index_{0};
};

}  // namespace bustub
