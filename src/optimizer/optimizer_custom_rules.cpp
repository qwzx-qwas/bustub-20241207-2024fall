#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"

// Note for 2023 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {
// 查询优化器的“自定义规则入口”：
// 在 Optimizer::OptimizeCustom(...) 里按顺序把一组优化规则应用到执行计划上，然后返回优化后的计划。
auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  // Hash-join conversion can push predicates that reference only one side back above a nested join inside that
  // child. Re-run the join rules to a fixed point so multi-way joins do not leave an inner Cartesian product.
  for (size_t pass = 0; pass < 32; pass++) {
    const auto before = p->ToString(false);
    p = OptimizeMergeFilterNLJ(p);
    p = OptimizeNLJAsIndexJoin(p);
    p = OptimizeNLJAsHashJoin(p);
    if (p->ToString(false) == before) {
      break;
    }
  }
  p = OptimizeVectorKnnScan(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeSeqScanAsIndexScan(p);
  return p;
}

}  // namespace bustub
