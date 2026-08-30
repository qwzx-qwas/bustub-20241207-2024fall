#pragma once

#include <cstddef>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * VectorIndexScanPlanNode represents a vector KNN lookup backed by a vector
 * index. The executor evaluates the query vector once, invokes Index::SearchKnn
 * directly, and then fetches the corresponding table tuples by RID.
 */
class VectorIndexScanPlanNode : public AbstractPlanNode {
 public:
  VectorIndexScanPlanNode(SchemaRef output, table_oid_t table_oid, index_oid_t index_oid,
                          AbstractExpressionRef query_expr, std::size_t k,
                          AbstractExpressionRef distance_expr = nullptr,
                          AbstractExpressionRef filter_predicate = nullptr)
      : AbstractPlanNode(std::move(output), {}),
        table_oid_(table_oid),
        index_oid_(index_oid),
        query_expr_(std::move(query_expr)),
        distance_expr_(std::move(distance_expr)),
        filter_predicate_(std::move(filter_predicate)),
        k_(k) {}

  auto GetType() const -> PlanType override { return PlanType::VectorIndexScan; }

  auto GetTableOid() const -> table_oid_t { return table_oid_; }

  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  auto GetQueryExpr() const -> const AbstractExpressionRef & { return query_expr_; }

  auto GetDistanceExpr() const -> const AbstractExpressionRef & { return distance_expr_; }

  auto GetFilterPredicate() const -> const AbstractExpressionRef & { return filter_predicate_; }

  auto GetK() const -> std::size_t { return k_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(VectorIndexScanPlanNode);

  table_oid_t table_oid_;
  index_oid_t index_oid_;
  AbstractExpressionRef query_expr_;
  AbstractExpressionRef distance_expr_;
  AbstractExpressionRef filter_predicate_;
  std::size_t k_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

}  // namespace bustub
