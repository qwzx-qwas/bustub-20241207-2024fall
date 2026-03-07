#include "optimizer/optimizer.h"

#include "execution/expressions/vector_distance_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/vector_knn_scan_plan.h"

namespace bustub {

namespace {

auto IsVectorDistanceExpr(const AbstractExpressionRef &expr) -> bool {
  return dynamic_cast<const VectorDistanceExpression *>(expr.get()) != nullptr;
}

}  // namespace

//识别Limit(Sort(child))模式，在单个ORDER BY且Sort的排序表达式是向量距离表达式，可以转换成VectorKnnScanPlanNode以利用向量索引加速查询。
auto Optimizer::OptimizeVectorKnnScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeVectorKnnScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::Limit) {
    return optimized_plan;
  }

  const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
  const auto &child_plan = limit_plan.GetChildPlan();
  if (child_plan->GetType() != PlanType::Sort) {
    return optimized_plan;
  }

  const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
  const auto &order_bys = sort_plan.GetOrderBy();
  if (order_bys.size() != 1) {
    return optimized_plan;
  }

  const auto &[order_type, distance_expr] = order_bys[0];
  if (!(order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) || !IsVectorDistanceExpr(distance_expr)) {
    return optimized_plan;
  }

  return std::make_shared<VectorKnnScanPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(), distance_expr,
                                                 limit_plan.GetLimit());
}

}  // namespace bustub
