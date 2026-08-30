//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include <queue>
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  using TopNEntry = std::pair<SortKey, std::pair<Tuple, RID>>;

  // 为什么需要这个比较器：
  // 因为std::priority_queue必须在类型层面知道“两个元素怎么比较”
  // 而TopN比较规则不是固定的"<"，而是运行时由plan_->GetOrderBy()决定的
  // 所以我需要内置类把这个比较规则封装起来，传给std::priority_queue
  class TopNComparator {
   public:
    explicit TopNComparator(TupleComparator cmp) : cmp_(std::move(cmp)) {}

    auto operator()(const TopNEntry &a, const TopNEntry &b) const -> bool {
      // priority_queue默认把“比较意义上的最大元素”放堆顶
      // 这里要让“更好”的元素被认为更大，这样堆顶就是当前 top-n 里最差的那个
      return cmp_({a.first, a.second.first}, {b.first, b.second.first});  // 注意这里比较的是SortKey
    }

   private:
    TupleComparator cmp_;
  };

  std::priority_queue<TopNEntry, std::vector<TopNEntry>, TopNComparator> heap_;
  // 给Next()用的结果容器，里面存的是已经按正确顺序排好序的 top-n 元素
  std::vector<std::pair<Tuple, RID>> results_;
  size_t result_idx_{0};  // 记录当前输出到results_的哪个位置了
  TupleComparator cmp_;
};
}  // namespace bustub
