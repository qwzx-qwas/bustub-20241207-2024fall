#include "optimizer/optimizer.h"

#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/vector_distance_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/vector_index_scan_plan.h"
#include "execution/plans/vector_knn_scan_plan.h"

namespace bustub {

namespace {

auto IsVectorDistanceExpr(const AbstractExpressionRef &expr) -> bool {
  return dynamic_cast<const VectorDistanceExpression *>(expr.get()) != nullptr;
}

auto IsSupportedVectorIndexType(IndexType index_type) -> bool {
  switch (index_type) {
    case IndexType::IVFFlatIndex:
      return true;
    default:
      return false;
  }
}

auto GetVectorDistanceMetric(const VectorDistanceExpression *expr) -> VectorIndexDistanceMetric {
  switch (expr->expr_type_) {
    case VectorDistanceExpressionType::L2Distance:
      return VectorIndexDistanceMetric::L2;
    case VectorDistanceExpressionType::CosineDistance:
      return VectorIndexDistanceMetric::Cosine;
    case VectorDistanceExpressionType::IPDistance:
      return VectorIndexDistanceMetric::InnerProduct;
  }
  UNREACHABLE("unsupported vector distance expression");
}

auto ContainsColumnValueExpr(const AbstractExpressionRef &expr) -> bool {
  if (dynamic_cast<const ColumnValueExpression *>(expr.get()) != nullptr) {
    return true;
  }
  for (const auto &child : expr->GetChildren()) {
    if (ContainsColumnValueExpr(child)) {
      return true;
    }
  }
  return false;
}

auto ExtractIndexedVectorQuery(const AbstractExpressionRef &expr)
    -> std::optional<std::pair<uint32_t, AbstractExpressionRef>> {
  const auto *distance_expr = dynamic_cast<const VectorDistanceExpression *>(expr.get());
  if (distance_expr == nullptr) {
    return std::nullopt;
  }

  const auto &left = distance_expr->GetChildAt(0);
  const auto &right = distance_expr->GetChildAt(1);

  const auto *left_col = dynamic_cast<const ColumnValueExpression *>(left.get());
  if (left_col != nullptr && left_col->GetTupleIdx() == 0 && !ContainsColumnValueExpr(right)) {
    return std::make_pair(left_col->GetColIdx(), right);
  }

  const auto *right_col = dynamic_cast<const ColumnValueExpression *>(right.get());
  if (right_col != nullptr && right_col->GetTupleIdx() == 0 && !ContainsColumnValueExpr(left)) {
    return std::make_pair(right_col->GetColIdx(), left);
  }

  return std::nullopt;
}

auto GetUnderlyingSeqScan(const AbstractPlanNodeRef &plan) -> const SeqScanPlanNode * {
  if (plan->GetType() == PlanType::SeqScan) {
    return dynamic_cast<const SeqScanPlanNode *>(plan.get());
  }
  if (plan->GetType() == PlanType::Filter) {
    // Stage 2 allows `Filter(SeqScan)` to participate in vector index rewrite.
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    return GetUnderlyingSeqScan(filter_plan.GetChildPlan());
  }
  return nullptr;
}

auto GetCombinedFilterPredicate(const AbstractPlanNodeRef &plan) -> AbstractExpressionRef {
  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    return seq_scan.filter_predicate_;
  }
  if (plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    auto child_predicate = GetCombinedFilterPredicate(filter_plan.GetChildPlan());
    if (child_predicate == nullptr) {
      return filter_plan.GetPredicate();
    }
    // Preserve the original filter semantics when multiple filter nodes collapse
    // into a single VectorIndexScan predicate.
    return std::make_shared<LogicExpression>(child_predicate, filter_plan.GetPredicate(), LogicType::And);
  }
  return nullptr;
}

auto FindMatchingVectorIndex(const Catalog &catalog, const SeqScanPlanNode &seq_scan,
                             const AbstractExpressionRef &distance_expr) -> const IndexInfo * {
  auto vector_query = ExtractIndexedVectorQuery(distance_expr);
  if (!vector_query.has_value()) {
    return nullptr;
  }

  const auto *distance = dynamic_cast<const VectorDistanceExpression *>(distance_expr.get());
  BUSTUB_ASSERT(distance != nullptr, "vector distance expression expected");
  const auto required_metric = GetVectorDistanceMetric(distance);
  auto table_info = catalog.GetTable(seq_scan.GetTableOid());
  const auto indexes = catalog.GetTableIndexes(table_info->name_);
  for (const auto &index_info : indexes) {
    if (!IsSupportedVectorIndexType(index_info->index_type_)) {
      continue;
    }
    const auto &key_attrs = index_info->index_->GetKeyAttrs();
    if (key_attrs.size() == 1 && key_attrs[0] == vector_query->first &&
        index_info->index_->GetVectorDistanceMetric() == required_metric) {
      return index_info.get();
    }
  }

  return nullptr;
}

}  // namespace

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

  const auto *seq_scan = GetUnderlyingSeqScan(sort_plan.GetChildPlan());
  if (seq_scan == nullptr) {
    return optimized_plan;
  }

  const auto *matching_index = FindMatchingVectorIndex(catalog_, *seq_scan, distance_expr);
  if (matching_index != nullptr) {
    auto vector_query = ExtractIndexedVectorQuery(distance_expr);
    BUSTUB_ASSERT(vector_query.has_value(), "matching vector index requires an indexed vector query");
    // Stage 2 keeps the filter on the VectorIndexScan node so the executor can
    // re-check candidates after MVCC visibility reconstruction.
    return std::make_shared<VectorIndexScanPlanNode>(optimized_plan->output_schema_, seq_scan->GetTableOid(),
                                                     matching_index->index_oid_, vector_query->second,
                                                     limit_plan.GetLimit(), distance_expr,
                                                     GetCombinedFilterPredicate(sort_plan.GetChildPlan()));
  }

  if (!ExtractIndexedVectorQuery(distance_expr).has_value()) {
    return optimized_plan;
  }

  // No usable vector index: fall back to the exact KNN executor added in stage 1.
  return std::make_shared<VectorKnnScanPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(),
                                                 distance_expr, limit_plan.GetLimit());
}

}  // namespace bustub
