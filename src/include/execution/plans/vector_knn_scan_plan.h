#pragma once

#include <string>
#include <utility>

#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * VectorKnnScanPlanNode represents an exact vector KNN operation.
 *
 * When optimizer detects a query pattern like `Limit(Sort(child))` where the
 * sort key is a supported vector distance expression and there is no matching
 * vector index rewrite, it can transform the plan into this specialized node.
 */
class VectorKnnScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new VectorKnnScanPlanNode instance.
   * @param output The output schema
   * @param child The child plan node
   * @param distance_expr Distance expression between query vector and table vector
   * @param k Number of nearest neighbors to keep
   */
  VectorKnnScanPlanNode(SchemaRef output, AbstractPlanNodeRef child, AbstractExpressionRef distance_expr, std::size_t k)
      : AbstractPlanNode(std::move(output), {std::move(child)}), distance_expr_(std::move(distance_expr)), k_(k) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::VectorKnnScan; }

  /** @return The child plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "VectorKnnScan should have exactly one child plan.");
    return GetChildAt(0);
  }

  /** @return The distance expression */
  auto GetDistanceExpr() const -> const AbstractExpressionRef & { return distance_expr_; }

  /** @return Number of nearest neighbors */
  auto GetK() const -> std::size_t { return k_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(VectorKnnScanPlanNode);

  /** Distance expression used by vector KNN scan */
  AbstractExpressionRef distance_expr_;
  /** Top-k limit for nearest neighbors */
  std::size_t k_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

}  // namespace bustub
